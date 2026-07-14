"""P0 OSM broker submit boundary tests.

Requirements covered:
  Req 2 — persist_deferred_broker_ready() diagnostic error codes
    * MATERIALIZATION_COPYBACK_INVALID_PAYLOAD  — invalid input args
    * MATERIALIZATION_COPYBACK_SCHEMA_ERROR     — undefined-column exception
    * MATERIALIZATION_COPYBACK_DB_ERROR         — generic DB exception
    * MATERIALIZATION_COPYBACK_CAS_MISS         — zero-row UPDATE with re-read

  Req 3 — SUBMITTED/ACKNOWLEDGED/PARTIAL_FILL/FILLED must have broker identity
    * SUBMITTED + broker_order_id → idempotent success, no repost
    * SUBMITTED - broker_order_id + successful tag lookup → attach + success
    * SUBMITTED - broker_order_id + no tag result → ENTRY_BROKER_IDENTITY_UNPROVEN
    * FILLED + broker_order_id → idempotent success
    * FILLED - broker_order_id + fill proof → idempotent success
    * FILLED - broker_order_id - fill proof → ENTRY_BROKER_IDENTITY_UNPROVEN

  Req 4 — unknown broker status with broker_order_id
    * Unknown status + broker_order_id → BROKER_STATUS_UNKNOWN_WITH_ID quarantine
    * Unknown status without broker_order_id → normal error classification
    * Normal accepted statuses + broker_order_id → existing SUBMITTED path

Patching discipline: each test patches run_with_retry at the test level only.
_persist_args() produces valid kwargs; caller patches DB as needed.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch, call

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

from ap.order_state_machine import APOrderStateMachine, OrderStatus  # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# Shared fixtures
# ─────────────────────────────────────────────────────────────────────────────

_CLIENT   = "jason@test.com"
_LOID     = "order-aaa-111"
_SIGNAL   = "SIG-001"
_CONTRACT = "NVDA260117C00900000"
_DEFERRED = "DEFERRED:NVDA"
_SYMBOL   = "NVDA"


def _make_osm() -> APOrderStateMachine:
    return APOrderStateMachine(_CLIENT)


def _persist_args(**overrides) -> dict:
    """Valid kwargs for persist_deferred_broker_ready(). Override one field to test failures."""
    defaults: dict[str, Any] = dict(
        owner="watcher-001",
        generation=1,
        signal_id=_SIGNAL,
        execution_mode="paper",
        contract=_CONTRACT,
        limit_price=1.25,
        qty=1,
        reserved_cost=125.0,
        selector_meta={"selector_debug": "test"},
    )
    defaults.update(overrides)
    return defaults


def _order_row(**overrides) -> dict:
    """Complete order row satisfying entry_metadata_guard requirements.

    Guard checks (all must pass on the submit path):
      client_id, execution_mode, signal_id, symbol, direction/side,
      timeframe (non-daily to skip target/stop), score > 0,
      trigger_price > 0, underlying_price > 0.
    All values are supplied in both top-level columns and meta JSON
    so every _first() lookup resolves without falling through to None.
    """
    _meta_blob = json.dumps({
        "direction": "CALL",
        "side": "CALL",
        "timeframe": "5m",
        "score": 0.85,
        "trigger_price": 450.50,
        "underlying_price": 448.20,
        "signal_entry_price": 450.50,
        "pattern": "3-1-2",
        "signal_id": _SIGNAL,
        "symbol": _SYMBOL,
        "execution_mode": "paper",
        "client_id": _CLIENT,
    })
    row: dict[str, Any] = {
        "local_order_id": _LOID,
        "client_id": _CLIENT,
        "kind": "ENTRY",
        "status": "SUBMITTED",
        "broker_order_id": "BID-001",
        "submitted_ts": "2026-07-15T09:30:00Z",
        "execution_mode": "paper",
        "signal_id": _SIGNAL,
        "contract": _CONTRACT,
        "symbol": _SYMBOL,
        "direction": "CALL",
        "side": "CALL",
        "timeframe": "5m",
        "score": 0.85,
        "trigger_price": 450.50,
        "pattern": "3-1-2",
        "qty": 1,
        "limit_price": 1.25,
        "reserved_cost": 125.0,
        "fill_price": None,
        "filled_ts": None,
        "meta": _meta_blob,
    }
    if "meta" in overrides and overrides["meta"] is None:
        # Caller explicitly set meta=None; still need guard fields in the row
        overrides.pop("meta")
    row.update(overrides)
    return row


# ─────────────────────────────────────────────────────────────────────────────
# Req 2 — persist_deferred_broker_ready diagnostic codes
# ─────────────────────────────────────────────────────────────────────────────


class TestCopybackInvalidPayload:
    """MATERIALIZATION_COPYBACK_INVALID_PAYLOAD on bad input — no DB calls needed."""

    def _call(self, osm: APOrderStateMachine, **overrides) -> bool:
        # Invalid-payload branch returns before any run_with_retry call,
        # so no DB patch is required.
        return osm.persist_deferred_broker_ready(_LOID, **_persist_args(**overrides))

    def test_deferred_contract_emits_invalid_payload(self, caplog):
        osm = _make_osm()
        with caplog.at_level(logging.CRITICAL, logger="ap.order_state_machine"):
            result = self._call(osm, contract=_DEFERRED)
        assert result is False
        assert "MATERIALIZATION_COPYBACK_INVALID_PAYLOAD" in caplog.text

    def test_zero_limit_emits_invalid_payload(self, caplog):
        osm = _make_osm()
        with caplog.at_level(logging.CRITICAL, logger="ap.order_state_machine"):
            result = self._call(osm, limit_price=0.0)
        assert result is False
        assert "MATERIALIZATION_COPYBACK_INVALID_PAYLOAD" in caplog.text

    def test_zero_qty_emits_invalid_payload(self, caplog):
        osm = _make_osm()
        with caplog.at_level(logging.CRITICAL, logger="ap.order_state_machine"):
            result = self._call(osm, qty=0)
        assert result is False
        assert "MATERIALIZATION_COPYBACK_INVALID_PAYLOAD" in caplog.text

    def test_missing_owner_emits_invalid_payload(self, caplog):
        osm = _make_osm()
        with caplog.at_level(logging.CRITICAL, logger="ap.order_state_machine"):
            result = self._call(osm, owner="")
        assert result is False
        assert "MATERIALIZATION_COPYBACK_INVALID_PAYLOAD" in caplog.text

    def test_invalid_mode_emits_invalid_payload(self, caplog):
        osm = _make_osm()
        with caplog.at_level(logging.CRITICAL, logger="ap.order_state_machine"):
            result = self._call(osm, execution_mode="unknown")
        assert result is False
        assert "MATERIALIZATION_COPYBACK_INVALID_PAYLOAD" in caplog.text

    def test_invalid_payload_never_mislabeled_schema_or_db_error(self, caplog):
        osm = _make_osm()
        with caplog.at_level(logging.DEBUG, logger="ap.order_state_machine"):
            self._call(osm, contract=_DEFERRED)
        assert "MATERIALIZATION_COPYBACK_SCHEMA_ERROR" not in caplog.text
        assert "MATERIALIZATION_COPYBACK_DB_ERROR" not in caplog.text
        assert "MATERIALIZATION_COPYBACK_CAS_MISS" not in caplog.text


class TestCopybackSchemaError:
    """MATERIALIZATION_COPYBACK_SCHEMA_ERROR on undefined-column DB exception."""

    def test_column_not_exist_exc_emits_schema_error(self, caplog):
        osm = _make_osm()

        def _raise(_fn):
            raise Exception("column contract_selection_status does not exist")

        with patch("ap.order_state_machine.run_with_retry", _raise), \
             caplog.at_level(logging.CRITICAL, logger="ap.order_state_machine"):
            result = osm.persist_deferred_broker_ready(_LOID, **_persist_args())

        assert result is False
        assert "MATERIALIZATION_COPYBACK_SCHEMA_ERROR" in caplog.text
        assert "MATERIALIZATION_COPYBACK_DB_ERROR" not in caplog.text

    def test_undefined_column_keyword_emits_schema_error(self, caplog):
        osm = _make_osm()

        def _raise(_fn):
            raise Exception("column \"contract_selection_status\" of relation \"orders\" does not exist")

        with patch("ap.order_state_machine.run_with_retry", _raise), \
             caplog.at_level(logging.CRITICAL, logger="ap.order_state_machine"):
            result = osm.persist_deferred_broker_ready(_LOID, **_persist_args())

        assert result is False
        assert "MATERIALIZATION_COPYBACK_SCHEMA_ERROR" in caplog.text

    def test_schema_error_never_mislabeled_cas_miss(self, caplog):
        osm = _make_osm()

        def _raise(_fn):
            raise Exception("column contract_selection_status does not exist")

        with patch("ap.order_state_machine.run_with_retry", _raise), \
             caplog.at_level(logging.DEBUG, logger="ap.order_state_machine"):
            osm.persist_deferred_broker_ready(_LOID, **_persist_args())

        assert "MATERIALIZATION_COPYBACK_CAS_MISS" not in caplog.text


class TestCopybackDbError:
    """MATERIALIZATION_COPYBACK_DB_ERROR on generic (non-schema) DB exception."""

    def test_connection_error_emits_db_error(self, caplog):
        osm = _make_osm()

        def _raise(_fn):
            raise Exception("connection reset by peer")

        with patch("ap.order_state_machine.run_with_retry", _raise), \
             caplog.at_level(logging.CRITICAL, logger="ap.order_state_machine"):
            result = osm.persist_deferred_broker_ready(_LOID, **_persist_args())

        assert result is False
        assert "MATERIALIZATION_COPYBACK_DB_ERROR" in caplog.text
        assert "MATERIALIZATION_COPYBACK_SCHEMA_ERROR" not in caplog.text

    def test_db_error_never_mislabeled_cas_miss(self, caplog):
        osm = _make_osm()

        def _raise(_fn):
            raise Exception("connection reset by peer")

        with patch("ap.order_state_machine.run_with_retry", _raise), \
             caplog.at_level(logging.DEBUG, logger="ap.order_state_machine"):
            osm.persist_deferred_broker_ready(_LOID, **_persist_args())

        assert "MATERIALIZATION_COPYBACK_CAS_MISS" not in caplog.text


class TestCopybackCasMiss:
    """MATERIALIZATION_COPYBACK_CAS_MISS when UPDATE returns zero rows."""

    def _rwr_zero_then_row(self, reread_row):
        """run_with_retry that returns 0 (UPDATE miss), then reread_row (SELECT)."""
        state = {"calls": 0}
        def _rwr(fn):
            state["calls"] += 1
            if state["calls"] == 1:
                return 0          # the UPDATE
            return reread_row     # the re-read SELECT
        return _rwr

    def test_zero_row_update_emits_cas_miss(self, caplog):
        osm = _make_osm()
        reread = {
            "status": "PENDING_TRIGGER",
            "broker_order_id": None,
            "submitted_ts": None,
            "execution_mode": "paper",
            "signal_id": _SIGNAL,
            "lifecycle_state": "MATERIALIZING",
            "materialization_owner": "watcher-OLD",
            "materialization_generation": "2",
            "recovery_submit_owner": "",
            "submit_intent_at": "",
        }
        with patch("ap.order_state_machine.run_with_retry", self._rwr_zero_then_row(reread)), \
             caplog.at_level(logging.CRITICAL, logger="ap.order_state_machine"):
            result = osm.persist_deferred_broker_ready(_LOID, **_persist_args())

        assert result is False
        assert "MATERIALIZATION_COPYBACK_CAS_MISS" in caplog.text

    def test_cas_miss_logs_broker_id_presence(self, caplog):
        osm = _make_osm()
        reread = {
            "status": "SUBMITTED",
            "broker_order_id": "BID-999",   # already submitted concurrently
            "submitted_ts": "2026-07-15T09:30:00Z",
            "execution_mode": "paper",
            "signal_id": _SIGNAL,
            "lifecycle_state": "SUBMITTED",
            "materialization_owner": "",
            "materialization_generation": "1",
            "recovery_submit_owner": "",
            "submit_intent_at": "2026-07-15T09:29:59Z",
        }
        with patch("ap.order_state_machine.run_with_retry", self._rwr_zero_then_row(reread)), \
             caplog.at_level(logging.CRITICAL, logger="ap.order_state_machine"):
            osm.persist_deferred_broker_ready(_LOID, **_persist_args())

        assert "MATERIALIZATION_COPYBACK_CAS_MISS" in caplog.text
        assert "broker_order_id_present=True" in caplog.text
        assert "submitted_ts_present=True" in caplog.text

    def test_cas_miss_never_terminalized(self, caplog):
        """persist_deferred_broker_ready must NEVER call transition() on CAS miss."""
        osm = _make_osm()
        transition_calls: list = []
        original_transition = osm.transition

        def _record_transition(*args, **kwargs):
            transition_calls.append((args, kwargs))
            return False

        osm.transition = _record_transition  # type: ignore[assignment]

        with patch("ap.order_state_machine.run_with_retry", self._rwr_zero_then_row(None)), \
             caplog.at_level(logging.CRITICAL, logger="ap.order_state_machine"):
            osm.persist_deferred_broker_ready(_LOID, **_persist_args())

        assert transition_calls == [], (
            "persist_deferred_broker_ready must not terminalize on CAS miss; "
            "caller owns the terminal decision"
        )

    def test_cas_miss_not_mislabeled_schema_or_db_error(self, caplog):
        osm = _make_osm()
        with patch("ap.order_state_machine.run_with_retry", self._rwr_zero_then_row(None)), \
             caplog.at_level(logging.DEBUG, logger="ap.order_state_machine"):
            osm.persist_deferred_broker_ready(_LOID, **_persist_args())

        assert "MATERIALIZATION_COPYBACK_SCHEMA_ERROR" not in caplog.text
        assert "MATERIALIZATION_COPYBACK_DB_ERROR" not in caplog.text


class TestCopybackSuccess:
    """Successful rowcount > 0 returns True without any error log."""

    def test_rowcount_one_returns_true(self, caplog):
        osm = _make_osm()

        def _rwr(_fn):
            return 1  # simulate a successful UPDATE rowcount

        with patch("ap.order_state_machine.run_with_retry", _rwr), \
             caplog.at_level(logging.DEBUG, logger="ap.order_state_machine"):
            result = osm.persist_deferred_broker_ready(_LOID, **_persist_args())

        assert result is True
        assert "MATERIALIZATION_COPYBACK" not in caplog.text


# ─────────────────────────────────────────────────────────────────────────────
# Req 3 — submit_existing_entry: submitted-like state must have broker identity
# ─────────────────────────────────────────────────────────────────────────────


class _BrokerNoTag:
    """Broker whose tag lookup always returns no matches."""
    base_url = "https://sandbox.tradier.com"
    account_id = "VA00000000"

    def __init__(self):
        self.session = MagicMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"orders": {"order": []}}
        self.session.get.return_value = resp
        self.session.post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"order": {"id": "BID-SHOULD-NOT-APPEAR", "status": "open"}},
        )


class _BrokerWithTag:
    """Broker whose tag lookup returns a real broker_order_id."""
    base_url = "https://sandbox.tradier.com"
    account_id = "VA00000000"
    FOUND_ID = "BID-RECOVERED-001"

    def __init__(self):
        self.session = MagicMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "orders": {
                "order": [{"id": self.FOUND_ID, "tag": _LOID, "status": "open"}]
            }
        }
        self.session.get.return_value = resp
        self.session.post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"order": {"id": "BID-SHOULD-NOT-APPEAR", "status": "open"}},
        )


def _run_submit(osm: APOrderStateMachine, broker, row: dict) -> dict:
    """Patch _get_order to return row and call submit_existing_entry."""
    with patch.object(osm, "_get_order", return_value=row):
        return osm.submit_existing_entry(local_order_id=_LOID, broker=broker)


class TestSubmittedIdempotency:
    """SUBMITTED/ACKNOWLEDGED/PARTIAL_FILL + broker_order_id → idempotent success."""

    def test_submitted_with_broker_id_returns_ok_true(self):
        osm = _make_osm()
        row = _order_row(status="SUBMITTED", broker_order_id="BID-001")
        result = _run_submit(osm, _BrokerNoTag(), row)
        assert result["ok"] is True
        assert result["broker_order_id"] == "BID-001"
        assert result["status"] == "SUBMITTED"

    def test_acknowledged_with_broker_id_returns_ok_true(self):
        osm = _make_osm()
        row = _order_row(status="ACKNOWLEDGED", broker_order_id="BID-002")
        result = _run_submit(osm, _BrokerNoTag(), row)
        assert result["ok"] is True
        assert result["broker_order_id"] == "BID-002"

    def test_partial_fill_with_broker_id_returns_ok_true(self):
        osm = _make_osm()
        row = _order_row(status="PARTIAL_FILL", broker_order_id="BID-003")
        result = _run_submit(osm, _BrokerNoTag(), row)
        assert result["ok"] is True

    def test_submitted_with_broker_id_never_reposts(self):
        """Idempotent path must never send a new POST to Tradier."""
        osm = _make_osm()
        broker = _BrokerNoTag()
        row = _order_row(status="SUBMITTED", broker_order_id="BID-001")
        _run_submit(osm, broker, row)
        broker.session.post.assert_not_called()


class TestSubmittedMissingBrokerIdentity:
    """SUBMITTED without broker_order_id → tag recovery or ENTRY_BROKER_IDENTITY_UNPROVEN."""

    def test_submitted_no_broker_id_tag_found_attaches_and_returns_ok(self):
        """Tag lookup recovers broker ID → attach_broker_identity, return ok=True."""
        osm = _make_osm()
        broker = _BrokerWithTag()
        row = _order_row(status="SUBMITTED", broker_order_id=None)
        attach_calls: list = []

        def _fake_attach(loid, *, broker_order_id, current_status):
            attach_calls.append((loid, broker_order_id, current_status))
            return True

        with patch.object(osm, "_attach_broker_identity_if_missing", _fake_attach):
            result = _run_submit(osm, broker, row)

        assert result["ok"] is True
        assert result["broker_order_id"] == _BrokerWithTag.FOUND_ID
        assert result.get("reconciled_by_tag") is True
        assert len(attach_calls) == 1
        assert attach_calls[0][0] == _LOID

    def test_submitted_no_broker_id_tag_found_never_reposts(self):
        """Tag recovery must not repost; broker.session.post must be uncalled."""
        osm = _make_osm()
        broker = _BrokerWithTag()
        row = _order_row(status="SUBMITTED", broker_order_id=None)
        with patch.object(osm, "_attach_broker_identity_if_missing", return_value=True):
            _run_submit(osm, broker, row)
        broker.session.post.assert_not_called()

    def test_submitted_no_broker_id_no_tag_returns_identity_unproven(self):
        """No tag match → ENTRY_BROKER_IDENTITY_UNPROVEN, ok=False."""
        osm = _make_osm()
        row = _order_row(status="SUBMITTED", broker_order_id=None)
        result = _run_submit(osm, _BrokerNoTag(), row)
        assert result["ok"] is False
        assert result["error"] == "ENTRY_BROKER_IDENTITY_UNPROVEN"
        assert result.get("reconciliation_required") is True
        assert result["broker_order_id"] is None

    def test_acknowledged_no_broker_id_no_tag_returns_identity_unproven(self):
        osm = _make_osm()
        row = _order_row(status="ACKNOWLEDGED", broker_order_id=None)
        result = _run_submit(osm, _BrokerNoTag(), row)
        assert result["ok"] is False
        assert result["error"] == "ENTRY_BROKER_IDENTITY_UNPROVEN"

    def test_identity_unproven_never_reposts(self):
        """POST must be zero when identity is unproven."""
        osm = _make_osm()
        broker = _BrokerNoTag()
        row = _order_row(status="SUBMITTED", broker_order_id=None)
        _run_submit(osm, broker, row)
        broker.session.post.assert_not_called()


class TestFilledIdentity:
    """FILLED requires broker_order_id OR durable fill proof."""

    def test_filled_with_broker_id_returns_ok(self):
        osm = _make_osm()
        row = _order_row(status="FILLED", broker_order_id="BID-001",
                         fill_price=1.30, filled_ts="2026-07-15T09:31:00Z")
        result = _run_submit(osm, _BrokerNoTag(), row)
        assert result["ok"] is True

    def test_filled_no_broker_id_but_fill_price_proof_returns_ok(self):
        osm = _make_osm()
        row = _order_row(status="FILLED", broker_order_id=None,
                         fill_price=1.30, filled_ts=None)
        result = _run_submit(osm, _BrokerNoTag(), row)
        assert result["ok"] is True

    def test_filled_no_broker_id_but_filled_ts_proof_returns_ok(self):
        osm = _make_osm()
        row = _order_row(status="FILLED", broker_order_id=None,
                         fill_price=None, filled_ts="2026-07-15T09:31:00Z")
        result = _run_submit(osm, _BrokerNoTag(), row)
        assert result["ok"] is True

    def test_filled_no_broker_id_no_proof_returns_identity_unproven(self):
        osm = _make_osm()
        row = _order_row(status="FILLED", broker_order_id=None,
                         fill_price=None, filled_ts=None)
        result = _run_submit(osm, _BrokerNoTag(), row)
        assert result["ok"] is False
        assert result["error"] == "ENTRY_BROKER_IDENTITY_UNPROVEN"
        assert result.get("reconciliation_required") is True

    def test_filled_identity_unproven_never_reposts(self):
        osm = _make_osm()
        broker = _BrokerNoTag()
        row = _order_row(status="FILLED", broker_order_id=None,
                         fill_price=None, filled_ts=None)
        _run_submit(osm, broker, row)
        broker.session.post.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# Req 4 — unknown broker status + broker_order_id → quarantine, not discard
# ─────────────────────────────────────────────────────────────────────────────


def _broker_post(*, status_code: int = 200, broker_id: str | None, status_str: str):
    """Build a mock broker whose POST returns the given broker response."""
    broker = MagicMock()
    broker.base_url = "https://sandbox.tradier.com"
    broker.account_id = "VA00000000"
    order_dict: dict = {"status": status_str}
    if broker_id:
        order_dict["id"] = broker_id
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = {"order": order_dict}
    broker.session = MagicMock()
    broker.session.post.return_value = resp
    # Tag lookup always returns nothing
    get_resp = MagicMock()
    get_resp.status_code = 200
    get_resp.json.return_value = {"orders": {"order": []}}
    broker.session.get.return_value = get_resp
    return broker


def _pending_row(**overrides) -> dict:
    """PENDING_TRIGGER row ready for submit_existing_entry."""
    row = _order_row(
        status="PENDING_TRIGGER",
        broker_order_id=None,
        submitted_ts=None,
        fill_price=None,
        filled_ts=None,
        meta=None,
    )
    row.update(overrides)
    return row


def _run_from_pending(osm, broker, row=None) -> dict:
    """Run submit_existing_entry from a PENDING_TRIGGER row with all DB writes mocked."""
    if row is None:
        row = _pending_row()
    flag_calls: list = []

    def _flag(loid, *, broker_order_id, error_msg):
        flag_calls.append({"loid": loid, "bid": broker_order_id, "msg": error_msg})

    with patch.object(osm, "_get_order", side_effect=[row, row, row, row]), \
         patch.object(osm, "transition", return_value=True), \
         patch.object(osm, "update_order_meta", return_value=True), \
         patch.object(osm, "_flag_split_brain_order", side_effect=_flag), \
         patch("ap.order_state_machine.run_with_retry", lambda fn: fn()):
        result = osm.submit_existing_entry(local_order_id=_LOID, broker=broker)
    result["_flag_split_brain_calls"] = flag_calls
    return result


class TestUnknownBrokerStatusWithId:
    """Req 4: unknown status + broker_order_id → quarantine, never discard."""

    def test_unknown_status_with_id_enters_split_brain(self, caplog):
        osm = _make_osm()
        broker = _broker_post(broker_id="BID-UNKNOWN-001", status_str="mystery_status")

        with caplog.at_level(logging.CRITICAL, logger="ap.order_state_machine"):
            result = _run_from_pending(osm, broker)

        assert result["ok"] is False
        assert result.get("split_brain") is True
        assert result.get("reconciliation_required") is True
        assert result["broker_order_id"] == "BID-UNKNOWN-001"
        assert "BROKER_STATUS_UNKNOWN_WITH_ID" in caplog.text
        assert len(result["_flag_split_brain_calls"]) == 1

    def test_unknown_status_with_id_preserves_broker_id(self):
        """Broker ID must survive in the return dict — never discarded."""
        osm = _make_osm()
        broker = _broker_post(broker_id="BID-PRESERVE-ME", status_str="weird_status")
        result = _run_from_pending(osm, broker)
        assert result["broker_order_id"] == "BID-PRESERVE-ME"

    def test_unknown_status_with_id_no_replacement_submit(self):
        """After quarantine, no second POST is issued."""
        osm = _make_osm()
        broker = _broker_post(broker_id="BID-ONCE", status_str="bizarre_status")
        _run_from_pending(osm, broker)
        # Exactly one POST — the original attempt that returned unknown status
        assert broker.session.post.call_count == 1

    def test_unknown_status_without_id_does_not_quarantine(self):
        """No broker_order_id → normal error classification, no split_brain."""
        osm = _make_osm()
        broker = _broker_post(broker_id=None, status_str="mystery_status_no_id")
        result = _run_from_pending(osm, broker)
        assert result.get("split_brain") is not True
        assert result.get("reconciliation_required") is not True
        assert not result["_flag_split_brain_calls"]

    def test_accepted_status_with_id_does_not_quarantine(self):
        """Normal accepted status + broker_order_id → clean SUBMITTED path."""
        osm = _make_osm()
        broker = _broker_post(broker_id="BID-ACCEPTED", status_str="open")
        result = _run_from_pending(osm, broker)
        assert result["ok"] is True
        assert result["status"] == OrderStatus.SUBMITTED
        assert result["broker_order_id"] == "BID-ACCEPTED"
        assert result.get("split_brain") is not True

    def test_pending_status_with_id_does_not_quarantine(self):
        """'pending' is an accepted status — must not trigger quarantine."""
        osm = _make_osm()
        broker = _broker_post(broker_id="BID-PENDING", status_str="pending")
        result = _run_from_pending(osm, broker)
        assert result["ok"] is True
        assert result.get("split_brain") is not True


# ─────────────────────────────────────────────────────────────────────────────
# Regression preservation
# ─────────────────────────────────────────────────────────────────────────────


class TestNormalEntryRegressions:
    """Standard PENDING_TRIGGER entries must still reach Tradier after changes."""

    def test_preselected_pending_trigger_reaches_broker_post(self):
        osm = _make_osm()
        broker = _broker_post(broker_id="BID-STANDARD", status_str="open")
        row = _pending_row()

        with patch.object(osm, "_get_order", side_effect=[row, row, row, row]), \
             patch.object(osm, "transition", return_value=True), \
             patch.object(osm, "update_order_meta", return_value=True), \
             patch("ap.order_state_machine.run_with_retry", lambda fn: fn()):
            result = osm.submit_existing_entry(local_order_id=_LOID, broker=broker)

        assert result["ok"] is True
        assert result["broker_order_id"] == "BID-STANDARD"
        broker.session.post.assert_called_once()

    def test_deferred_placeholder_never_reaches_broker(self):
        """DEFERRED: contract is hard-blocked before any POST."""
        osm = _make_osm()
        broker = _broker_post(broker_id="BID-SHOULD-NOT-HAPPEN", status_str="open")
        row = _pending_row(contract=_DEFERRED)

        with patch.object(osm, "_get_order", return_value=row), \
             patch.object(osm, "transition", return_value=True), \
             patch.object(osm, "update_order_meta", return_value=True), \
             patch("ap.order_state_machine.run_with_retry", lambda fn: fn()):
            result = osm.submit_existing_entry(local_order_id=_LOID, broker=broker)

        assert result["ok"] is False
        assert "DEFERRED_CONTRACT_BLOCKED" in (result.get("error") or "")
        broker.session.post.assert_not_called()
