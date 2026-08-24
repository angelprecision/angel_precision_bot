"""P0 regression — broker position unavailability must never become broker-flat truth.

Binding spec:
    docs/pr_specs/p0_broker_position_unavailable_not_flat_20260815.md

Invariant under test:
    BROKER UNAVAILABLE != BROKER FLAT.

The proven production defect was:

    ap/brokers/tradier.py::TradierBroker.list_positions()
        catches every exception from _get() and returns []
      -> ap/exit_safety.py::resolve_exit_broker_truth()
        cannot tell "successful empty" from "unavailable"
        -> broker_truth_open_qty=0, is_fresh_exact=True
      -> ap/order_state_machine.py::APOrderStateMachine.submit_exit()
        marks the local position CLOSED under
        SYNTHETIC_POSITION_STALE_BROKER_FLAT while the broker still
        holds the contract.

These tests drive the REAL adapter, the REAL resolver, and the REAL
submit_exit guard path (no production edits to exit_safety.py or the OSM).
Only ap/brokers/tradier.py::list_positions changes; every assertion below
must hold once transport/auth/HTTP/malformed failures propagate instead of
being swallowed into [].
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))

from ap import exit_safety as exit_safety_mod  # noqa: E402
from ap import order_state_machine as osm_mod  # noqa: E402
from ap.order_state_machine import APOrderStateMachine, OrderStatus  # noqa: E402
from ap.brokers.tradier import TradierBroker, TradierConfig  # noqa: E402
from ap.exit_safety import resolve_exit_broker_truth  # noqa: E402


# ── Real-Tradier production OCC shapes ────────────────────────────────────────
PUT_CONTRACT = "SMCI260626P00032500"   # sym[-9] == 'P'  -> PUT
CALL_CONTRACT = "SMCI260626C00032500"  # sym[-9] == 'C'  -> CALL
OTHER_CONTRACT = "META260626C00520000"


# =============================================================================
# Transport / HTTP fakes for the REAL TradierBroker._get -> list_positions path
# =============================================================================
class FakeResponse:
    """Mimic requests.Response for the exact surface _get() touches."""

    def __init__(self, status_code=200, payload=None, *, content=True, raw_text=""):
        self.status_code = status_code
        self._payload = payload
        self.content = b"{}" if content else b""
        self.text = raw_text

    def raise_for_status(self):
        if self.status_code >= 400:
            err = requests.exceptions.HTTPError(f"HTTP {self.status_code}")
            err.response = self
            raise err

    def json(self):
        return self._payload


def _broker(payload=None, *, status_code=200, exc=None, account_id="ACC-LIVE-1",
            base_url="https://api.tradier.com"):
    """Build a REAL TradierBroker whose session.get is faked.

    This exercises the genuine _get() -> list_positions() code path, including
    raise_for_status() classification, rather than stubbing list_positions.
    """
    b = TradierBroker(TradierConfig(
        base_url=base_url,
        access_token="redacted-test-token",
        account_id=account_id,
    ))

    def fake_get(url, params=None, timeout=None):
        if exc is not None:
            raise exc
        return FakeResponse(status_code=status_code, payload=payload)

    b.session = SimpleNamespace(get=fake_get)
    return b


def _positions_payload(rows):
    """Wrap normalized-ish raw Tradier position dict(s) in the account shape."""
    if isinstance(rows, list):
        node = rows
    else:
        node = rows
    return {"positions": {"position": node}}


def _raw_position(symbol, quantity, cost_basis=1234.0, **extra):
    row = {
        "cost_basis": cost_basis,
        "date_acquired": "2026-06-20T14:41:11.000Z",
        "id": 130089,
        "quantity": quantity,
        "symbol": symbol,
    }
    row.update(extra)
    return row


# =============================================================================
# SECTION A — Adapter contract: successful shapes (cases 1-6)
# =============================================================================
def test_case01_successful_empty_positions_object_returns_empty_list():
    # Tradier "no positions" often serializes positions as an empty object.
    b = _broker({"positions": {}})
    assert b.list_positions() == []


def test_case02a_successful_positions_string_null_is_authoritative_empty():
    # Real Tradier empty shape: {"positions": "null"}.
    b = _broker({"positions": "null"})
    assert b.list_positions() == []


def test_case02b_successful_positions_json_null_is_authoritative_empty():
    # JSON null -> parsed as None; still an authoritative empty snapshot.
    b = _broker({"positions": None})
    assert b.list_positions() == []


def test_case02c_successful_position_node_null_is_authoritative_empty():
    b = _broker({"positions": {"position": None}})
    assert b.list_positions() == []


def test_case02d_no_content_body_is_authoritative_empty():
    # 204 / empty body -> _get returns {} -> authoritative empty.
    b = TradierBroker(TradierConfig(
        base_url="https://api.tradier.com", access_token="t", account_id="ACC-LIVE-1"))
    b.session = SimpleNamespace(
        get=lambda url, params=None, timeout=None: FakeResponse(200, None, content=False))
    assert b.list_positions() == []


def test_case03_single_position_object_normalizes_to_one_row_list():
    b = _broker(_positions_payload(_raw_position(PUT_CONTRACT, 4)))
    rows = b.list_positions()
    assert isinstance(rows, list)
    assert len(rows) == 1
    r = rows[0]
    assert r["symbol"] == PUT_CONTRACT
    assert r["quantity"] == 4.0
    assert r["cost_basis"] == 1234.0
    assert r["side"] == "PUT"
    assert r["raw"]["symbol"] == PUT_CONTRACT  # raw contract preserved


def test_case04_position_list_normalizes_correctly():
    b = _broker(_positions_payload([
        _raw_position(PUT_CONTRACT, 4),
        _raw_position(CALL_CONTRACT, 2),
    ]))
    rows = b.list_positions()
    assert len(rows) == 2
    by_symbol = {r["symbol"]: r for r in rows}
    assert by_symbol[PUT_CONTRACT]["quantity"] == 4.0
    assert by_symbol[PUT_CONTRACT]["side"] == "PUT"
    assert by_symbol[CALL_CONTRACT]["quantity"] == 2.0
    assert by_symbol[CALL_CONTRACT]["side"] == "CALL"


def test_case05_exact_occ_absent_from_nonempty_snapshot_resolves_zero_fresh():
    # Snapshot holds a DIFFERENT contract; resolver for our contract == fresh 0.
    b = _broker(_positions_payload(_raw_position(OTHER_CONTRACT, 9)))
    truth = resolve_exit_broker_truth(broker=b, client_id="jason@example.com", contract=PUT_CONTRACT)
    assert truth["broker_truth_open_qty"] == 0
    assert truth["is_fresh_exact"] is True
    assert truth["audit"]["snapshot_status"] == "contract_absent_open_qty_zero"


def test_case06_exact_occ_present_qty4_resolves_exact_four():
    b = _broker(_positions_payload(_raw_position(PUT_CONTRACT, 4)))
    truth = resolve_exit_broker_truth(broker=b, client_id="jason@example.com", contract=PUT_CONTRACT)
    assert truth["broker_truth_open_qty"] == 4
    assert truth["is_fresh_exact"] is True
    assert truth["audit"]["exact_contract_match"] is True


# =============================================================================
# SECTION B — Adapter contract: UNAVAILABLE must propagate (cases 7-13)
#   In every case list_positions() must RAISE, and the resolver must report
#   broker_truth_open_qty=None, is_fresh_exact=False.
# =============================================================================
@pytest.mark.parametrize("status_code", [401, 403, 429, 500, 502, 503])
def test_case07to10_http_status_propagates_and_resolver_unknown(status_code):
    b = _broker(status_code=status_code, payload={"error": "x"})
    with pytest.raises(requests.exceptions.HTTPError):
        b.list_positions()
    truth = resolve_exit_broker_truth(broker=b, client_id="jason@example.com", contract=PUT_CONTRACT)
    assert truth["broker_truth_open_qty"] is None
    assert truth["is_fresh_exact"] is False
    assert truth["audit"]["snapshot_status"] == "broker_positions_error"


def test_case11_connect_timeout_propagates_and_resolver_unknown():
    b = _broker(exc=requests.exceptions.ConnectTimeout("connect timed out"))
    with pytest.raises(requests.exceptions.Timeout):
        b.list_positions()
    truth = resolve_exit_broker_truth(broker=b, client_id="jason@example.com", contract=PUT_CONTRACT)
    assert truth["broker_truth_open_qty"] is None
    assert truth["is_fresh_exact"] is False


def test_case12_read_timeout_propagates_and_resolver_unknown():
    b = _broker(exc=requests.exceptions.ReadTimeout("read timed out"))
    with pytest.raises(requests.exceptions.Timeout):
        b.list_positions()
    truth = resolve_exit_broker_truth(broker=b, client_id="jason@example.com", contract=PUT_CONTRACT)
    assert truth["broker_truth_open_qty"] is None
    assert truth["is_fresh_exact"] is False


def test_case13_connection_error_propagates_and_resolver_unknown():
    b = _broker(exc=requests.exceptions.ConnectionError("conn refused"))
    with pytest.raises(requests.exceptions.ConnectionError):
        b.list_positions()
    truth = resolve_exit_broker_truth(broker=b, client_id="jason@example.com", contract=PUT_CONTRACT)
    assert truth["broker_truth_open_qty"] is None
    assert truth["is_fresh_exact"] is False


# =============================================================================
# SECTION C — Adapter contract: MALFORMED must raise, never [] (case 14)
#   A malformed SUCCESSFUL payload is not flatness. It must raise a
#   deterministic error, and the resolver must report unknown (None / False).
# =============================================================================
MALFORMED_PAYLOADS = {
    "top_level_json_null": None,
    "top_level_non_dict_list": [1, 2, 3],
    "top_level_non_dict_str": "garbage",
    "positions_node_int": {"positions": 12345},
    "positions_node_str": {"positions": "unexpected"},
    "position_container_int": {"positions": {"position": 42}},
    "position_row_non_dict": {"positions": {"position": ["not-a-dict",
                                                         _raw_position(PUT_CONTRACT, 4)]}},
    "position_qty_unparseable": {"positions": {"position": {"symbol": PUT_CONTRACT,
                                                            "quantity": "abc"}}},
    # Amendment (audit finding): falsy-but-not-authoritative-empty positions
    # node must raise, not silently resolve to SUCCESS_EMPTY.
    "positions_node_empty_list": {"positions": []},
    "positions_node_false": {"positions": False},
    "positions_node_zero": {"positions": 0},
    # Amendment: successful body with no "positions" key at all is not the
    # same as an authoritative empty snapshot — .get(..., {}) must not be
    # allowed to invent flatness for an unrecognized payload shape.
    "positions_key_missing": {"unexpected": "successful-but-wrong-payload"},
    # Amendment: an empty position row carries no truth-bearing contract
    # identity and must not silently normalize to qty=0.
    "position_row_empty_dict": {"positions": {"position": {}}},
    # Amendment: a row with contract identity but no quantity field at all
    # must not default to qty=0 (that is exactly "no exposure" proof).
    "position_row_missing_quantity": {"positions": {"position":
                                                     {"symbol": PUT_CONTRACT}}},
    # Amendment: NaN/inf pass float() without raising and must be rejected
    # explicitly, since they cannot be interpreted as a real position size.
    "position_row_quantity_nan": {"positions": {"position":
                                                 {"symbol": PUT_CONTRACT, "quantity": "nan"}}},
    "position_row_quantity_inf": {"positions": {"position":
                                                 {"symbol": PUT_CONTRACT, "quantity": "inf"}}},
    # Amendment 2 (audit finding): a non-empty positions dict with no
    # "position" key at all was defaulting via .get("position", []) to an
    # empty list, which list_positions() then returned as [] -- silently
    # indistinguishable from an authoritative empty snapshot.
    "positions_dict_missing_position_key": {"positions": {"foo": "bar"}},
}


@pytest.mark.parametrize("name", sorted(MALFORMED_PAYLOADS.keys()))
def test_case14_malformed_payload_raises_and_never_returns_empty(name):
    payload = MALFORMED_PAYLOADS[name]
    b = _broker(payload)
    with pytest.raises(ValueError) as err:
        b.list_positions()
    assert "TRADIER_POSITIONS_PAYLOAD_MALFORMED" in str(err.value)


@pytest.mark.parametrize("name", sorted(MALFORMED_PAYLOADS.keys()))
def test_case14_malformed_payload_resolver_is_unknown_not_zero(name):
    payload = MALFORMED_PAYLOADS[name]
    b = _broker(payload)
    truth = resolve_exit_broker_truth(broker=b, client_id="jason@example.com", contract=PUT_CONTRACT)
    assert truth["broker_truth_open_qty"] is None
    assert truth["is_fresh_exact"] is False
    # Failure class preserved in diagnostics.
    assert "TRADIER_POSITIONS_PAYLOAD_MALFORMED" in str(truth["audit"].get("error") or "")


# =============================================================================
# Integration harness — REAL submit_exit over a faked conn (no OSM edits)
# =============================================================================
class _FakeConn:
    def __init__(self, resolver):
        self._resolver = resolver
        self._row = None
        self._rows = []
        self.queries: list[tuple[str, tuple]] = []

    def execute(self, sql, params=()):
        self.queries.append((sql, tuple(params)))
        payload = self._resolver(sql, tuple(params))
        if isinstance(payload, tuple):
            self._row, self._rows = payload
        else:
            self._row, self._rows = payload, []
        return self

    def fetchone(self):
        return self._row

    def fetchall(self):
        return list(self._rows)


class _FakeConnContext:
    def __init__(self, fake_conn):
        self._fake_conn = fake_conn

    def __enter__(self):
        return self._fake_conn

    def __exit__(self, exc_type, exc, tb):
        return False


class _MockOSM:
    submit_exit = APOrderStateMachine.submit_exit
    update_order_meta = APOrderStateMachine.update_order_meta

    def __init__(self, client_id="jason@example.com"):
        self.client_id = client_id
        self.transitions = []
        self.exit_row = None

    def _get_active_exit_order(self, position_id):
        if self.exit_row and self.exit_row["position_id"] == position_id:
            return dict(self.exit_row)
        return None

    def _get_order(self, local_order_id):
        if self.exit_row and self.exit_row["local_order_id"] == local_order_id:
            return dict(self.exit_row)
        return None

    def create_exit_order(self, **kwargs):
        self.create_exit_kwargs = kwargs
        self.exit_row = {
            "local_order_id": "L-EXIT-001",
            "client_id": self.client_id,
            "position_id": kwargs["position_id"],
            "kind": "EXIT",
            "status": "EXIT_REQUESTED",
            "execution_mode": kwargs["execution_mode"],
            "contract": kwargs["contract"],
            "qty": kwargs["qty"],
            "broker_order_id": "",
            "submitted_ts": None,
            "meta": {},
        }
        return "L-EXIT-001"

    def persist_exit_submit_intent(self, local_order_id, **kwargs):
        if not self.exit_row or self.exit_row["local_order_id"] != local_order_id:
            return False
        key = kwargs["broker_submit_key"]
        self.exit_row["meta"].update({
            "lifecycle_state": "SUBMITTING",
            "submit_intent_at": "2026-08-15T12:00:00+00:00",
            "broker_submit_key": key,
            "broker_submit_payload_hash": kwargs["payload_hash"],
            "current_owner": f"broker_submit:{key}",
        })
        return True

    def transition(self, local_order_id, new_status, **kwargs):
        self.transitions.append((local_order_id, new_status, kwargs))
        if self.exit_row and self.exit_row["local_order_id"] == local_order_id:
            self.exit_row["status"] = new_status
            if kwargs.get("broker_order_id"):
                self.exit_row["broker_order_id"] = kwargs["broker_order_id"]
        return True

    def _resolve_underlying_symbol(self, *, symbol, contract):
        return symbol

    @staticmethod
    def _is_broker_accept_status(status):
        return status in ("open", "pending", "ok", "accepted")

    def _emit_transition_event(self, **kwargs):
        return None

    def _flag_split_brain_order(self, *args, **kwargs):
        return None

    def _lookup_order_by_tag(self, broker, base_url, account_id, tag):
        return None


@pytest.fixture(autouse=True)
def _schema(monkeypatch):
    monkeypatch.setattr(
        exit_safety_mod,
        "_table_columns",
        lambda table: {
            "positions": {"status", "quantity_remaining", "close_source", "entry_ts"},
            "orders": {"client_id", "kind", "status", "contract", "execution_mode",
                       "created_ts", "updated_ts", "last_error"},
        }[table],
    )
    with exit_safety_mod._ALERT_CACHE_LOCK:
        exit_safety_mod._ALERT_CACHE.clear()


def _patch_db(monkeypatch, resolver):
    fake_conn = _FakeConn(resolver)
    monkeypatch.setattr(exit_safety_mod, "conn", lambda: _FakeConnContext(fake_conn))
    monkeypatch.setattr(exit_safety_mod, "run_with_retry", lambda fn, *a, **k: fn())
    monkeypatch.setattr(osm_mod, "conn", lambda: _FakeConnContext(fake_conn))
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn, *a, **k: fn())
    return fake_conn


def _open_position_row(quantity_remaining=1):
    return {
        "status": "OPEN",
        "quantity_remaining": quantity_remaining,
        "close_source": None,
        "entry_ts": "2026-06-25T14:30:00+00:00",
    }


def _integration_broker(*, get_exc=None, get_payload=None, account_id="ACC-LIVE-1"):
    """Real TradierBroker with faked session for both list_positions (get) and
    the exit submission (post)."""
    b = TradierBroker(TradierConfig(
        base_url="https://api.tradier.com",
        access_token="redacted-test-token",
        account_id=account_id,
    ))

    def fake_get(url, params=None, timeout=None):
        if get_exc is not None:
            raise get_exc
        return FakeResponse(200, get_payload)

    def fake_post(url, data=None, headers=None, timeout=None):
        return FakeResponse(200, {"order": {"id": "BO-EXIT-1", "status": "open"}})

    b.session = SimpleNamespace(get=fake_get, post=MagicMock(side_effect=fake_post))
    return b


def _closed_writes(fake_conn):
    return [q for q in fake_conn.queries
            if "positions" in q[0].lower() and "status = 'closed'" in q[0].lower()]


def _synthetic_transitions(osm):
    return [t for t in osm.transitions
            if "SYNTHETIC_POSITION_STALE_BROKER_FLAT" in str(t[2].get("last_error"))]


# =============================================================================
# SECTION D — Integration: unavailable during submit_exit (case 15)
#   The exact former false-truth chain, now proven safe.
# =============================================================================
def test_case15_unavailable_broker_during_submit_exit_never_closes(monkeypatch):
    # Build a concrete 500 HTTPError so list_positions() raises inside resolve.
    def make_get_exc():
        e = requests.exceptions.HTTPError("HTTP 500")
        e.response = FakeResponse(500)
        return e

    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 0},
    )
    broker = _integration_broker(get_exc=make_get_exc())
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=broker,
        position_id="pos-live-open",
        contract=PUT_CONTRACT,
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    # INVARIANT: broker unavailable must NOT terminalize the position.
    assert result.get("reason") != "SYNTHETIC_POSITION_STALE_BROKER_FLAT"
    assert result.get("error") != "SYNTHETIC_POSITION_STALE_BROKER_FLAT"
    assert _closed_writes(fake_conn) == [], "position must not be marked CLOSED on unavailable truth"
    assert _synthetic_transitions(osm) == [], "no synthetic-flat CANCEL on unavailable truth"
    # Fail-OPEN: with truth unknown, the protective exit still proceeds to broker.
    assert broker.session.post.call_count == 1
    assert result.get("ok") is True


@pytest.mark.parametrize("exc_factory", [
    lambda: requests.exceptions.ConnectTimeout("connect timed out"),
    lambda: requests.exceptions.ReadTimeout("read timed out"),
    lambda: requests.exceptions.ConnectionError("conn refused"),
])
def test_case15_transport_unavailable_during_submit_exit_never_closes(monkeypatch, exc_factory):
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 0},
    )
    broker = _integration_broker(get_exc=exc_factory())
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=broker,
        position_id="pos-live-open",
        contract=PUT_CONTRACT,
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result.get("reason") != "SYNTHETIC_POSITION_STALE_BROKER_FLAT"
    assert _closed_writes(fake_conn) == []
    assert _synthetic_transitions(osm) == []
    assert broker.session.post.call_count == 1


def test_case15_malformed_broker_during_submit_exit_never_closes(monkeypatch):
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 0},
    )
    broker = _integration_broker(get_payload={"positions": {"position": 42}})  # malformed
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=broker,
        position_id="pos-live-open",
        contract=PUT_CONTRACT,
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result.get("reason") != "SYNTHETIC_POSITION_STALE_BROKER_FLAT"
    assert _closed_writes(fake_conn) == []
    assert _synthetic_transitions(osm) == []
    assert broker.session.post.call_count == 1


def test_case15_amendment2_missing_position_key_during_submit_exit_never_closes(monkeypatch):
    """Audit finding: {"positions": {"foo": "bar"}} is a non-empty, dict-typed
    positions node with no "position" key. positions.get("position", [])
    was silently defaulting to [] -- indistinguishable from an authoritative
    empty snapshot -- letting a still-open LIVE position resolve as flat and
    reach SYNTHETIC_POSITION_STALE_BROKER_FLAT. Must resolve unknown instead."""
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 0},
    )
    broker = _integration_broker(get_payload={"positions": {"foo": "bar"}})
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=broker,
        position_id="pos-live-open",
        contract=PUT_CONTRACT,
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result.get("reason") != "SYNTHETIC_POSITION_STALE_BROKER_FLAT"
    assert _closed_writes(fake_conn) == []
    assert _synthetic_transitions(osm) == []
    assert broker.session.post.call_count == 1


def test_case15_amendment2_missing_position_key_resolver_is_unknown(monkeypatch):
    broker = _broker({"positions": {"foo": "bar"}})
    truth = resolve_exit_broker_truth(broker=broker, client_id="jason@example.com", contract=PUT_CONTRACT)
    assert truth["broker_truth_open_qty"] is None
    assert truth["is_fresh_exact"] is False


@pytest.mark.parametrize("get_payload,label", [
    # Amendment (audit finding): exact-contract row present but quantity is
    # entirely absent. Must never manufacture "exact matched row, qty=0".
    ({"positions": {"position": {"symbol": PUT_CONTRACT}}}, "missing_quantity"),
    # Amendment: NaN/inf quantity on an exact contract match. float() alone
    # would silently accept these; they must be rejected before reaching the
    # resolver so they can never present as fresh exact zero exposure.
    ({"positions": {"position": {"symbol": PUT_CONTRACT, "quantity": "nan"}}}, "quantity_nan"),
    ({"positions": {"position": {"symbol": PUT_CONTRACT, "quantity": "inf"}}}, "quantity_inf"),
])
def test_case15_amendment_malformed_exact_match_during_submit_exit_never_closes(
        monkeypatch, get_payload, label):
    """The exact false-flat manufacture the audit flagged: an exact-OCC row
    with unusable quantity truth must resolve unknown, not fresh-exact-zero,
    and must never terminalize the position under
    SYNTHETIC_POSITION_STALE_BROKER_FLAT."""
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 0},
    )
    broker = _integration_broker(get_payload=get_payload)
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=broker,
        position_id="pos-live-open",
        contract=PUT_CONTRACT,
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert result.get("reason") != "SYNTHETIC_POSITION_STALE_BROKER_FLAT", label
    assert _closed_writes(fake_conn) == [], label
    assert _synthetic_transitions(osm) == [], label
    assert broker.session.post.call_count == 1, label


@pytest.mark.parametrize("get_payload,label", [
    ({"positions": {"position": {"symbol": PUT_CONTRACT}}}, "missing_quantity"),
    ({"positions": {"position": {"symbol": PUT_CONTRACT, "quantity": "nan"}}}, "quantity_nan"),
    ({"positions": {"position": {"symbol": PUT_CONTRACT, "quantity": "inf"}}}, "quantity_inf"),
])
def test_case15_amendment_malformed_exact_match_resolver_is_unknown(get_payload, label):
    broker = _broker(get_payload)
    truth = resolve_exit_broker_truth(broker=broker, client_id="jason@example.com", contract=PUT_CONTRACT)
    assert truth["broker_truth_open_qty"] is None, label
    assert truth["is_fresh_exact"] is False, label


# =============================================================================
# SECTION E — Integration: authoritative flat cleanup remains reachable (case 16)
# =============================================================================
def test_case16_authoritative_flat_during_submit_exit_still_cleans_up(monkeypatch):
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 0},
    )
    # Successful, authoritative empty snapshot => broker is genuinely flat.
    broker = _integration_broker(get_payload={"positions": "null"})
    osm = _MockOSM()

    result = osm.submit_exit(
        broker=broker,
        position_id="pos-stale-synth",
        contract=PUT_CONTRACT,
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    # Legitimate synthetic-flat cleanup MUST remain reachable on proven flat.
    assert result.get("reason") == "SYNTHETIC_POSITION_STALE_BROKER_FLAT"
    assert len(_closed_writes(fake_conn)) >= 1
    assert len(_synthetic_transitions(osm)) >= 1
    # No broker submission on a proven-flat position.
    assert broker.session.post.call_count == 0
    # The CLOSED write carries the synthetic-flat marker.
    closed = _closed_writes(fake_conn)[0]
    assert "synthetic_position_stale_broker_flat" in str(closed[1])


# =============================================================================
# SECTION F — LIVE / PAPER account + mode isolation (case 17)
# =============================================================================
def test_case17_live_and_paper_accounts_resolve_independently():
    live = _broker(_positions_payload(_raw_position(PUT_CONTRACT, 4)), account_id="ACC-LIVE-1")
    paper = _broker({"positions": "null"}, account_id="ACC-PAPER-9")

    live_truth = resolve_exit_broker_truth(broker=live, client_id="jason@example.com", contract=PUT_CONTRACT)
    paper_truth = resolve_exit_broker_truth(broker=paper, client_id="jose@example.com", contract=PUT_CONTRACT)

    # Live account authoritative long qty. (audit account is normalized lower-case)
    assert live_truth["broker_truth_open_qty"] == 4
    assert live_truth["audit"]["account"] == "acc-live-1"
    # Paper account authoritative empty — its own truth, not the live position.
    assert paper_truth["broker_truth_open_qty"] == 0
    assert paper_truth["is_fresh_exact"] is True
    assert paper_truth["audit"]["account"] == "acc-paper-9"


def test_case17_unavailable_paper_cannot_authorize_flat_for_live(monkeypatch):
    # A live holding must not become invisible because a (different) broker was
    # unavailable. Prove submit_exit on an unavailable broker keeps mode=live and
    # does not close.
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 0},
    )
    broker = _integration_broker(
        get_exc=requests.exceptions.ConnectionError("conn refused"),
        account_id="ACC-LIVE-1",
    )
    osm = _MockOSM(client_id="jason@example.com")

    result = osm.submit_exit(
        broker=broker,
        position_id="pos-live-open",
        contract=PUT_CONTRACT,
        symbol="SMCI",
        direction="PUT",
        qty=1,
        limit_price=1.25,
        execution_mode="live",
    )

    assert _closed_writes(fake_conn) == []
    assert result.get("reason") != "SYNTHETIC_POSITION_STALE_BROKER_FLAT"
    # execution_mode identity preserved end-to-end.
    assert osm.create_exit_kwargs["execution_mode"] == "live"


# =============================================================================
# SECTION G — Diagnostic audit differentiation (case 18)
# =============================================================================
def test_case18_audit_differentiates_error_malformed_and_authoritative_flat(monkeypatch):
    # (a) authoritative flat
    flat = resolve_exit_broker_truth(
        broker=_broker({"positions": "null"}), client_id="c", contract=PUT_CONTRACT)
    assert flat["audit"]["snapshot_status"] == "contract_absent_open_qty_zero"
    assert flat["is_fresh_exact"] is True

    # (b) transport/auth unavailable -> broker_positions_error
    unavail = resolve_exit_broker_truth(
        broker=_broker(status_code=503, payload={"error": "x"}), client_id="c", contract=PUT_CONTRACT)
    assert unavail["audit"]["snapshot_status"] == "broker_positions_error"
    assert unavail["broker_truth_open_qty"] is None

    # (c) malformed successful payload -> broker_positions_error with malformed class
    malformed = resolve_exit_broker_truth(
        broker=_broker({"positions": {"position": 42}}), client_id="c", contract=PUT_CONTRACT)
    assert malformed["audit"]["snapshot_status"] == "broker_positions_error"
    assert "TRADIER_POSITIONS_PAYLOAD_MALFORMED" in str(malformed["audit"].get("error") or "")

    # (d) resolver's structured-malformed branch (defensive: non-list sentinel return)
    class _NonListBroker:
        account_id = "ACC-LIVE-1"

        def list_positions(self):
            return object()  # not a list/dict/None

    sentinel = resolve_exit_broker_truth(
        broker=_NonListBroker(), client_id="c", contract=PUT_CONTRACT)
    assert sentinel["audit"]["snapshot_status"] == "broker_positions_malformed"
    assert sentinel["broker_truth_open_qty"] is None
    assert sentinel["is_fresh_exact"] is False
