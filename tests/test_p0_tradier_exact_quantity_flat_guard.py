"""P0 regression — exact-contract broker quantity must never be coerced into
broker-flat truth.

Binding spec:
    docs/pr_specs/p0_tradier_exact_quantity_flat_guard_20260817.md

Companion / preserved-unbroken spec:
    docs/pr_specs/p0_broker_position_unavailable_not_flat_20260815.md
    (tests/test_p0_broker_position_unavailable_not_flat.py — must remain
    green and unmodified; this file adds coverage, it does not replace it)

Invariant under test:
    BROKER QUANTITY UNCERTAINTY OR CONFLICT MUST NEVER BECOME ZERO EXPOSURE.

The proven production defect, forward-fixing #478 (merged 9a4287a on
main@37f61d0):

    ap/brokers/tradier.py::TradierBroker.list_positions()
        accepted any finite float as a valid quantity — bool (float(False)
        == 0.0), fractional (int(0.5) == 0 downstream), and negative
        (silently coerced to 0 by the long-position helper) all passed
        through as believable broker truth.
      -> ap/exit_safety.py::_extract_long_position_qty()
        collapsed None/negative/short-side quantity to 0 and summed it as
        though it were a confirmed-flat contribution.
      -> ap/exit_safety.py::resolve_exit_broker_truth()
        an exact-contract row matched with quantity=0 (whether from a
        genuinely malformed/negative/fractional/boolean row, OR from a
        well-formed but suspicious EXPLICIT quantity=0 row) produced
        broker_truth_open_qty=0, is_fresh_exact=True — indistinguishable
        from real broker-confirmed flat (contract absence).
      -> ap/order_state_machine.py::APOrderStateMachine.submit_exit()
        could terminalize a still-open LIVE position under
        SYNTHETIC_POSITION_STALE_BROKER_FLAT on manufactured flat truth.

These tests drive the REAL adapter (ap/brokers/tradier.py), the REAL
resolver (ap/exit_safety.py), and the REAL submit_exit guard path
(ap/order_state_machine.py) — no mocking of the logic under test.
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
from ap.order_state_machine import APOrderStateMachine  # noqa: E402
from ap.brokers.tradier import TradierBroker, TradierConfig  # noqa: E402
from ap.exit_safety import resolve_exit_broker_truth, _extract_long_position_qty  # noqa: E402


PUT_CONTRACT = "SMCI260626P00032500"    # sym[-9] == 'P' -> PUT
CALL_CONTRACT = "SMCI260626C00032500"   # sym[-9] == 'C' -> CALL


# =============================================================================
# Transport fakes (mirrors tests/test_p0_broker_position_unavailable_not_flat.py)
# =============================================================================
class FakeResponse:
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


def _broker(payload=None, *, account_id="ACC-LIVE-1"):
    b = TradierBroker(TradierConfig(
        base_url="https://api.tradier.com",
        access_token="redacted-test-token",
        account_id=account_id,
    ))
    b.session = SimpleNamespace(
        get=lambda url, params=None, timeout=None: FakeResponse(200, payload)
    )
    return b


def _integration_broker(*, get_payload=None, account_id="ACC-LIVE-1"):
    """Real TradierBroker with faked session for both list_positions (get)
    and the exit submission (post)."""
    b = TradierBroker(TradierConfig(
        base_url="https://api.tradier.com",
        access_token="redacted-test-token",
        account_id=account_id,
    ))

    def fake_get(url, params=None, timeout=None):
        return FakeResponse(200, get_payload)

    def fake_post(url, data=None, headers=None, timeout=None):
        return FakeResponse(200, {"order": {"id": "BO-EXIT-1", "status": "open"}})

    b.session = SimpleNamespace(get=fake_get, post=MagicMock(side_effect=fake_post))
    return b


def _row(symbol, quantity, **extra):
    row = {
        "cost_basis": 1234.0,
        "date_acquired": "2026-06-20T14:41:11.000Z",
        "id": 130089,
        "quantity": quantity,
        "symbol": symbol,
    }
    row.update(extra)
    return row


def _payload(row_or_rows):
    return {"positions": {"position": row_or_rows}}


# =============================================================================
# DB / OSM integration fakes (mirrors test_p0_broker_position_unavailable_not_flat.py)
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
            "submit_intent_at": "2026-08-17T12:00:00+00:00",
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


def _closed_writes(fake_conn):
    return [q for q in fake_conn.queries
            if "positions" in q[0].lower() and "status = 'closed'" in q[0].lower()]


def _synthetic_transitions(osm):
    return [t for t in osm.transitions
            if "SYNTHETIC_POSITION_STALE_BROKER_FLAT" in str(t[2].get("last_error"))]


# =============================================================================
# SECTION A — Adapter boundary: TradierBroker.list_positions() rejects
# malformed/conflicting exact-contract quantities
# =============================================================================

def test_boolean_quantity_raises_at_adapter():
    b = _broker(_payload(_row(PUT_CONTRACT, False)))
    with pytest.raises(ValueError, match="TRADIER_POSITIONS_PAYLOAD_MALFORMED"):
        b.list_positions()


def test_boolean_true_quantity_raises_at_adapter():
    # float(True) == 1.0 -- an even more dangerous masquerade (looks like a
    # genuine single-contract position) if left unguarded.
    b = _broker(_payload(_row(PUT_CONTRACT, True)))
    with pytest.raises(ValueError, match="TRADIER_POSITIONS_PAYLOAD_MALFORMED"):
        b.list_positions()


def test_fractional_quantity_raises_at_adapter():
    b = _broker(_payload(_row(PUT_CONTRACT, 0.5)))
    with pytest.raises(ValueError, match="TRADIER_POSITIONS_PAYLOAD_MALFORMED"):
        b.list_positions()


def test_negative_integer_quantity_raises_at_adapter():
    b = _broker(_payload(_row(PUT_CONTRACT, -1)))
    with pytest.raises(ValueError, match="TRADIER_POSITIONS_PAYLOAD_CONFLICT"):
        b.list_positions()


def test_negative_fractional_quantity_raises_at_adapter():
    b = _broker(_payload(_row(PUT_CONTRACT, -0.5)))
    # Fractional check runs before sign check; either deterministic error is
    # acceptable as long as it raises and never coerces to a believable qty.
    with pytest.raises(ValueError, match="TRADIER_POSITIONS_PAYLOAD_MALFORMED|TRADIER_POSITIONS_PAYLOAD_CONFLICT"):
        b.list_positions()


def test_explicit_zero_quantity_does_not_raise_at_adapter():
    # Zero is well-formed at the adapter layer -- the adapter has no concept
    # of "the exact contract we're resolving against". The suspicious-zero
    # policy is enforced downstream in the resolver, which does have that
    # context.
    b = _broker(_payload(_row(PUT_CONTRACT, 0)))
    rows = b.list_positions()
    assert len(rows) == 1
    assert rows[0]["quantity"] == 0.0


@pytest.mark.parametrize("quantity,expected", [
    (1, 1.0),
    (2, 2.0),
    ("4", 4.0),
])
def test_valid_integer_quantities_still_normalize(quantity, expected):
    b = _broker(_payload(_row(PUT_CONTRACT, quantity)))
    rows = b.list_positions()
    assert len(rows) == 1
    assert rows[0]["quantity"] == expected


# =============================================================================
# SECTION B — Resolver: resolve_exit_broker_truth() never manufactures
# broker_truth_open_qty=0 / is_fresh_exact=True from a malformed/conflicting
# exact-contract match
# =============================================================================

@pytest.mark.parametrize("quantity,label", [
    (False, "boolean_false"),
    (True, "boolean_true"),
    (0.5, "fractional"),
    (-1, "negative_integer"),
    (-0.5, "negative_fractional"),
    (0, "explicit_zero_exact_match"),
])
def test_malformed_or_conflicting_quantity_resolves_unknown(quantity, label):
    b = _broker(_payload(_row(PUT_CONTRACT, quantity)))
    truth = resolve_exit_broker_truth(
        broker=b, client_id="jason@example.com", contract=PUT_CONTRACT
    )
    assert truth["broker_truth_open_qty"] is None, label
    assert truth["is_fresh_exact"] is False, label


def test_valid_positive_quantity_resolves_fresh_exact():
    b = _broker(_payload(_row(PUT_CONTRACT, 4)))
    truth = resolve_exit_broker_truth(
        broker=b, client_id="jason@example.com", contract=PUT_CONTRACT
    )
    assert truth["broker_truth_open_qty"] == 4
    assert truth["is_fresh_exact"] is True
    assert truth["audit"]["snapshot_status"] == "exact_match"


def test_contract_absent_from_successful_snapshot_still_authoritative_flat():
    # Preserve existing #478 flat semantics: successful snapshot, target OCC
    # absent -> broker_truth_open_qty=0, is_fresh_exact=True. This must NOT
    # regress -- it is required for stale synthetic-position cleanup.
    b = _broker(_payload(_row(CALL_CONTRACT, 2)))  # different contract present
    truth = resolve_exit_broker_truth(
        broker=b, client_id="jason@example.com", contract=PUT_CONTRACT
    )
    assert truth["broker_truth_open_qty"] == 0
    assert truth["is_fresh_exact"] is True
    assert truth["audit"]["snapshot_status"] == "contract_absent_open_qty_zero"


def test_multiple_matched_rows_one_conflicting_makes_whole_resolution_unknown():
    # Two rows exact-match the same contract (e.g. lot-split); one is valid,
    # one is a conflicting zero-quantity row. Fail-closed: the whole
    # resolution must go unknown rather than silently summing only the
    # valid row and hiding the conflict.
    b = _broker(_payload([
        _row(PUT_CONTRACT, 2, id=1),
        _row(PUT_CONTRACT, 0, id=2),
    ]))
    truth = resolve_exit_broker_truth(
        broker=b, client_id="jason@example.com", contract=PUT_CONTRACT
    )
    assert truth["broker_truth_open_qty"] is None
    assert truth["is_fresh_exact"] is False
    assert truth["audit"]["snapshot_status"] == "exact_match_conflict_unknown_quantity"


# =============================================================================
# SECTION C — Defense-in-depth: _extract_long_position_qty() directly
# =============================================================================

def test_extract_long_position_qty_missing_key_is_none():
    assert _extract_long_position_qty({"symbol": PUT_CONTRACT}) is None


def test_extract_long_position_qty_negative_is_none():
    assert _extract_long_position_qty({"quantity": -3}) is None


def test_extract_long_position_qty_boolean_is_none():
    assert _extract_long_position_qty({"quantity": False}) is None
    assert _extract_long_position_qty({"quantity": True}) is None


def test_extract_long_position_qty_fractional_is_none():
    assert _extract_long_position_qty({"quantity": 1.5}) is None


def test_extract_long_position_qty_short_side_is_none():
    assert _extract_long_position_qty({"quantity": 3, "side": "short"}) is None


def test_extract_long_position_qty_explicit_zero_is_int_zero_not_none():
    # 0 is a distinct, deliberate signal -- NOT the same as "could not
    # establish quantity". The resolver treats 0 specially (conflict, not
    # flat authority); this helper must not collapse the two cases.
    result = _extract_long_position_qty({"quantity": 0})
    assert result == 0
    assert result is not None


def test_extract_long_position_qty_valid_positive_is_int():
    assert _extract_long_position_qty({"quantity": 4}) == 4
    assert _extract_long_position_qty({"quantity": "4"}) == 4


# =============================================================================
# SECTION D — Integration: submit_exit() never terminalizes on manufactured
# malformed/conflicting-quantity flat truth
# =============================================================================

@pytest.mark.parametrize("quantity,label", [
    (False, "boolean_false"),
    (True, "boolean_true"),
    (0.5, "fractional"),
    (-1, "negative_integer"),
    (-0.5, "negative_fractional"),
    (0, "explicit_zero_exact_match"),
])
def test_malformed_or_conflicting_quantity_during_submit_exit_never_closes(
        monkeypatch, quantity, label):
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 0},
    )
    broker = _integration_broker(get_payload=_payload(_row(PUT_CONTRACT, quantity)))
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
    assert result.get("error") != "SYNTHETIC_POSITION_STALE_BROKER_FLAT", label
    assert _closed_writes(fake_conn) == [], f"{label}: position must not be marked CLOSED"
    assert _synthetic_transitions(osm) == [], f"{label}: no synthetic-flat CANCEL"
    # Fail-open: with truth unknown/conflicting, the protective exit still
    # proceeds to broker rather than blocking the exit entirely.
    assert broker.session.post.call_count == 1, label
    assert result.get("ok") is True, label


def test_valid_quantity_during_submit_exit_proceeds_normally(monkeypatch):
    """Sanity/preservation: a genuinely valid exact-match quantity must not
    be affected by this hardening -- submit_exit proceeds exactly as before."""
    fake_conn = _patch_db(
        monkeypatch,
        lambda sql, params: _open_position_row() if "FROM positions" in sql else {"rejection_count": 0},
    )
    broker = _integration_broker(get_payload=_payload(_row(PUT_CONTRACT, 1)))
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
    assert result.get("ok") is True


# =============================================================================
# SECTION E — Zero direct broker submit/cancel authority proof
# =============================================================================

def test_resolver_never_calls_broker_submit_or_cancel_directly():
    broker = MagicMock()
    broker.list_positions.return_value = [
        {"symbol": PUT_CONTRACT, "quantity": 0.0, "cost_basis": 0.0, "side": "PUT", "raw": {}}
    ]
    resolve_exit_broker_truth(broker=broker, client_id="jason@example.com", contract=PUT_CONTRACT)
    assert not hasattr(broker, "submit_order") or broker.submit_order.call_count == 0
    assert not hasattr(broker, "cancel_order") or broker.cancel_order.call_count == 0
