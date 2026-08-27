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
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch, call

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

from ap.order_state_machine import (  # noqa: E402
    APOrderStateMachine,
    OrderStatus,
)
from ap.broker_submit_identity import canonical_broker_submit_key  # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# Shared fixtures
# ─────────────────────────────────────────────────────────────────────────────

_CLIENT   = "jason@test.com"
_LOID     = "123e4567-e89b-12d3-a456-426614174000"
_TAG      = "123e4567-e89b-12d3-a456-42661417"
_SIGNAL   = "SIG-001"
_CONTRACT = "NVDA260117C00900000"
_DEFERRED = "DEFERRED:NVDA"
_SYMBOL   = "NVDA"


def _make_osm() -> APOrderStateMachine:
    return APOrderStateMachine(_CLIENT)


def _patch_copyback_retry(retry_fn):
    """Patch the exact globals used by persist_deferred_broker_ready().

    Full-suite imports can leave multiple module aliases around this legacy
    OSM surface. Patching the method globals keeps these diagnostics tests
    bound to the function under test instead of whichever module alias pytest
    imported first.
    """
    method_globals = APOrderStateMachine.persist_deferred_broker_ready.__globals__
    return patch.dict(method_globals, {"run_with_retry": retry_fn})


def test_real_uuid_uses_exact_canonical_32_character_broker_tag():
    assert len(_LOID) == 36
    assert len(_TAG) == 32
    assert canonical_broker_submit_key(_LOID) == _TAG


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

        with _patch_copyback_retry(_raise), \
             caplog.at_level(logging.CRITICAL, logger="ap.order_state_machine"):
            result = osm.persist_deferred_broker_ready(_LOID, **_persist_args())

        assert result is False
        assert "MATERIALIZATION_COPYBACK_SCHEMA_ERROR" in caplog.text
        assert "MATERIALIZATION_COPYBACK_DB_ERROR" not in caplog.text

    def test_undefined_column_keyword_emits_schema_error(self, caplog):
        osm = _make_osm()

        def _raise(_fn):
            raise Exception("column \"contract_selection_status\" of relation \"orders\" does not exist")

        with _patch_copyback_retry(_raise), \
             caplog.at_level(logging.CRITICAL, logger="ap.order_state_machine"):
            result = osm.persist_deferred_broker_ready(_LOID, **_persist_args())

        assert result is False
        assert "MATERIALIZATION_COPYBACK_SCHEMA_ERROR" in caplog.text

    def test_schema_error_never_mislabeled_cas_miss(self, caplog):
        osm = _make_osm()

        def _raise(_fn):
            raise Exception("column contract_selection_status does not exist")

        with _patch_copyback_retry(_raise), \
             caplog.at_level(logging.DEBUG, logger="ap.order_state_machine"):
            osm.persist_deferred_broker_ready(_LOID, **_persist_args())

        assert "MATERIALIZATION_COPYBACK_CAS_MISS" not in caplog.text


class TestCopybackDbError:
    """MATERIALIZATION_COPYBACK_DB_ERROR on generic (non-schema) DB exception."""

    def test_connection_error_emits_db_error(self, caplog):
        osm = _make_osm()

        def _raise(_fn):
            raise Exception("connection reset by peer")

        with _patch_copyback_retry(_raise), \
             caplog.at_level(logging.CRITICAL, logger="ap.order_state_machine"):
            result = osm.persist_deferred_broker_ready(_LOID, **_persist_args())

        assert result is False
        assert "MATERIALIZATION_COPYBACK_DB_ERROR" in caplog.text
        assert "MATERIALIZATION_COPYBACK_SCHEMA_ERROR" not in caplog.text

    def test_db_error_never_mislabeled_cas_miss(self, caplog):
        osm = _make_osm()

        def _raise(_fn):
            raise Exception("connection reset by peer")

        with _patch_copyback_retry(_raise), \
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
        with _patch_copyback_retry(self._rwr_zero_then_row(reread)), \
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
        with _patch_copyback_retry(self._rwr_zero_then_row(reread)), \
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

        with _patch_copyback_retry(self._rwr_zero_then_row(None)), \
             caplog.at_level(logging.CRITICAL, logger="ap.order_state_machine"):
            osm.persist_deferred_broker_ready(_LOID, **_persist_args())

        assert transition_calls == [], (
            "persist_deferred_broker_ready must not terminalize on CAS miss; "
            "caller owns the terminal decision"
        )

    def test_cas_miss_not_mislabeled_schema_or_db_error(self, caplog):
        osm = _make_osm()
        with _patch_copyback_retry(self._rwr_zero_then_row(None)), \
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

        with _patch_copyback_retry(_rwr), \
             caplog.at_level(logging.DEBUG, logger="ap.order_state_machine"):
            result = osm.persist_deferred_broker_ready(_LOID, **_persist_args())

        assert result is True
        assert "MATERIALIZATION_COPYBACK" not in caplog.text


def test_owned_broker_ready_terminal_cas_loses_to_submit_intent_without_overwrite():
    osm = _make_osm()
    seen = {}

    class _Cursor:
        rowcount = 0

        def execute(self, sql, params):
            seen["sql"] = sql
            seen["params"] = params
            return self

    class _Conn:
        def __enter__(self):
            return _Cursor()

        def __exit__(self, *_args):
            return False

    osm.transition = MagicMock()
    method_globals = APOrderStateMachine.terminalize_owned_broker_ready_materialization.__globals__
    with patch.dict(
        method_globals,
        {
            "conn": lambda: _Conn(),
            "run_with_retry": lambda fn, *a, **k: fn(),
        },
    ):
        result = osm.terminalize_owned_broker_ready_materialization(
            _LOID,
            reason="late_gate_rejected",
            terminal_status="EXPIRED",
            owner="watcher-001",
            generation=7,
            retry_attempt=2,
            client_id=_CLIENT,
            execution_mode="paper",
            expected_direction="CALL",
            expected_contract_symbol=_CONTRACT,
            diagnostics={"failure_stage": "late_gate"},
        )

    assert result is False
    assert "BROKER_READY" in seen["sql"]
    assert "SELECTED" in seen["sql"]
    assert "broker_ready" in seen["sql"]
    assert "materialization_owner" in seen["sql"]
    assert "materialization_generation" in seen["sql"]
    assert "retry_attempt" in seen["sql"]
    assert "submit_intent_at" in seen["sql"]
    assert "broker_submit_key" in seen["sql"]
    assert "recovery_submit_owner" in seen["sql"]
    assert "current_owner" in seen["sql"]
    assert "direction" in seen["sql"]
    assert "contract" in seen["sql"]
    osm.transition.assert_not_called()


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
                "order": [{"id": self.FOUND_ID, "tag": _TAG, "status": "open"}]
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

        def _fake_attach(
            loid, *, broker_order_id, current_status, current_execution_mode,
            durable_mode_authority=False,
        ):
            attach_calls.append(
                (loid, broker_order_id, current_status, current_execution_mode)
            )
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

    def test_tag_recovery_attach_cas_miss_fails_closed(self):
        osm = _make_osm()
        broker = _BrokerWithTag()
        row = _order_row(status="SUBMITTED", broker_order_id=None)
        with patch.object(osm, "_attach_broker_identity_if_missing", return_value=False):
            result = _run_submit(osm, broker, row)
        assert result["ok"] is False
        assert result["error"] == "ENTRY_BROKER_IDENTITY_PERSIST_FAILED"
        assert result["broker_order_id"] == _BrokerWithTag.FOUND_ID
        assert result.get("reconciliation_required") is True
        assert result.get("reconciled_by_tag") is not True
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


def _run_from_pending(
    osm, broker, row=None, *, plan=None, bypass_entry_guards=False,
) -> dict:
    """Run submit_existing_entry from a PENDING_TRIGGER row with all DB writes mocked."""
    if row is None:
        row = _pending_row()
    flag_calls: list = []

    def _meta_dict():
        raw = row.get("meta") or {}
        if isinstance(raw, str):
            raw = json.loads(raw)
        return dict(raw)

    def _persist_intent(_loid, **kwargs):
        meta = _meta_dict()
        meta.update({
            "lifecycle_state": "SUBMITTING",
            "submit_intent_at": "2026-07-15T09:30:00Z",
            "broker_submit_key": kwargs["broker_submit_key"],
            "broker_submit_payload_hash": kwargs["payload_hash"],
            "current_owner": f"broker_submit:{kwargs['broker_submit_key']}",
        })
        row["meta"] = json.dumps(meta)
        return True

    def _transition(_loid, new_status, **fields):
        row["status"] = str(new_status)
        row.update(fields)
        return True

    def _update_meta(_loid, patch_value):
        meta = _meta_dict()
        meta.update(patch_value)
        row["meta"] = json.dumps(meta)
        return True

    def _flag(
        loid, *, broker_order_id, execution_mode, error_msg,
        durable_mode_authority=False,
    ):
        flag_calls.append({
            "loid": loid,
            "bid": broker_order_id,
            "mode": execution_mode,
            "msg": error_msg,
        })
        row.update({
            "status": "SUBMITTED",
            "broker_order_id": broker_order_id,
            "submitted_ts": "2026-07-15T09:30:01Z",
            "last_error": f"SPLIT_BRAIN:{error_msg}",
        })
        _update_meta(loid, {
            "lifecycle_state": "SUBMITTED",
            "split_brain_quarantine": True,
            "reconciliation_required": True,
            "broker_order_id": broker_order_id,
        })
        return True

    with patch.object(osm, "_get_order", side_effect=lambda _loid: dict(row)), \
         patch.object(osm, "persist_entry_submit_intent", side_effect=_persist_intent), \
         patch.object(osm, "persist_deferred_submit_intent", side_effect=_persist_intent), \
         patch.object(osm, "persist_materialized_submit_intent", side_effect=_persist_intent), \
         patch.object(osm, "transition", side_effect=_transition), \
         patch.object(osm, "update_order_meta", side_effect=_update_meta), \
         patch.object(osm, "_flag_split_brain_order", side_effect=_flag):
        if bypass_entry_guards:
            original_submit = getattr(
                APOrderStateMachine,
                "_entry_metadata_guard_original_submit_existing",
                None,
            )
            if original_submit is None:
                result = osm.submit_existing_entry(
                    local_order_id=_LOID, broker=broker, plan=plan,
                )
            else:
                result = original_submit(
                    osm, local_order_id=_LOID, broker=broker, plan=plan,
                )
        else:
            result = osm.submit_existing_entry(
                local_order_id=_LOID, broker=broker, plan=plan,
            )
    result["_flag_split_brain_calls"] = flag_calls
    result["_durable_row"] = dict(row)
    return result


class TestExistingBrokerProofGate:
    def test_pending_prior_submit_intent_without_tag_never_posts(self):
        osm = _make_osm()
        broker = _BrokerNoTag()
        row = _pending_row()
        meta = json.loads(row["meta"])
        meta.update({
            "lifecycle_state": "SUBMITTING",
            "submit_intent_at": "2026-07-15T09:29:59Z",
            "broker_submit_key": _TAG,
            "current_owner": f"broker_submit:{_TAG}",
        })
        row["meta"] = json.dumps(meta)
        result = _run_from_pending(osm, broker, row)
        assert result["ok"] is False
        assert result["error"] == "ENTRY_PRIOR_SUBMIT_PROOF_RECONCILIATION_REQUIRED"
        assert result.get("reconciliation_required") is True
        broker.session.post.assert_not_called()

    def test_pending_row_with_existing_broker_id_never_posts(self):
        osm = _make_osm()
        broker = _BrokerNoTag()
        row = _pending_row(broker_order_id="BID-EXISTING")
        result = _run_from_pending(osm, broker, row)
        assert result["ok"] is True
        assert result["broker_order_id"] == "BID-EXISTING"
        assert result.get("reconciled_by_tag") is True
        broker.session.post.assert_not_called()


class TestAmbiguousBrokerResponseRecovery:
    @staticmethod
    def _broker_with_lookup():
        broker = MagicMock()
        broker.base_url = "https://sandbox.tradier.com"
        broker.account_id = "VA00000000"
        lookup = MagicMock()
        lookup.status_code = 200
        lookup.json.return_value = {
            "orders": {"order": [{"id": "BID-AMBIGUOUS", "tag": _TAG}]}
        }
        broker.session.get.return_value = lookup
        return broker

    def test_ambiguous_json_uses_canonical_tag_and_posts_once(self):
        osm = _make_osm()
        broker = self._broker_with_lookup()
        response = MagicMock(status_code=200)
        response.json.side_effect = ValueError("truncated json")
        broker.session.post.return_value = response
        result = _run_from_pending(osm, broker)
        assert result["ok"] is True
        assert result["broker_order_id"] == "BID-AMBIGUOUS"
        assert result["_durable_row"]["status"] == "SUBMITTED"
        assert result["_durable_row"]["broker_order_id"] == "BID-AMBIGUOUS"
        broker.session.post.assert_called_once()
        broker.session.get.assert_called_once()

    def test_read_timeout_uses_canonical_tag_and_posts_once(self):
        from requests.exceptions import ReadTimeout

        osm = _make_osm()
        broker = self._broker_with_lookup()
        broker.session.post.side_effect = ReadTimeout("response lost after POST")
        result = _run_from_pending(osm, broker)
        assert result["ok"] is True
        assert result["broker_order_id"] == "BID-AMBIGUOUS"
        assert result["_durable_row"]["status"] == "SUBMITTED"
        assert result["_durable_row"]["broker_order_id"] == "BID-AMBIGUOUS"
        broker.session.post.assert_called_once()
        broker.session.get.assert_called_once()

    def test_multiple_live_tag_matches_fail_closed(self):
        osm = _make_osm()
        broker = self._broker_with_lookup()
        broker.session.get.return_value.json.return_value = {
            "orders": {"order": [
                {"id": "BID-A", "tag": _TAG, "status": "open"},
                {"id": "BID-B", "tag": _TAG, "status": "pending"},
            ]}
        }
        assert osm._lookup_order_by_tag(
            broker, broker.base_url, broker.account_id, _LOID,
        ) is None

    def test_one_live_and_one_terminal_tag_match_adopts_live_identity(self):
        osm = _make_osm()
        broker = self._broker_with_lookup()
        broker.session.get.return_value.json.return_value = {
            "orders": {"order": [
                {"id": "BID-OLD", "tag": _TAG, "status": "canceled"},
                {"id": "BID-LIVE", "tag": _TAG, "status": "open"},
            ]}
        }
        assert osm._lookup_order_by_tag(
            broker, broker.base_url, broker.account_id, _LOID,
        ) == "BID-LIVE"

    @pytest.mark.parametrize(
        "failure_kind",
        ["json", "read_timeout", "connection_error", "request_exception", "unexpected"],
    )
    def test_ambiguous_without_tag_proof_never_reposts(self, failure_kind):
        from requests.exceptions import ConnectionError, ReadTimeout, RequestException

        osm = _make_osm()
        broker = _broker_post(broker_id="UNUSED", status_str="open")
        if failure_kind == "json":
            response = MagicMock(status_code=200)
            response.json.side_effect = ValueError("truncated json")
            broker.session.post.return_value = response
        else:
            error_type = {
                "read_timeout": ReadTimeout,
                "connection_error": ConnectionError,
                "request_exception": RequestException,
                "unexpected": RuntimeError,
            }[failure_kind]
            broker.session.post.side_effect = error_type("response lost after POST")

        result = _run_from_pending(osm, broker)

        assert result["ok"] is False
        assert result["reconciliation_required"] is True
        assert result["identity_quarantine"] is True
        assert result["error"].startswith("BROKER_AMBIGUOUS_")
        if failure_kind == "unexpected":
            assert result["error"].startswith("BROKER_AMBIGUOUS_UNEXPECTED_")
        durable_meta = json.loads(result["_durable_row"]["meta"])
        assert result["_durable_row"]["status"] == "PENDING_TRIGGER"
        assert durable_meta["lifecycle_state"] == "SUBMITTING"
        assert durable_meta["broker_submit_key"] == _TAG
        broker.session.post.assert_called_once()
        broker.session.get.assert_called_once()

    @pytest.mark.parametrize("broker_status", ["open", "mystery_status", ""])
    def test_clean_2xx_without_id_reconciles_then_retains_intent_and_never_reposts(
        self, broker_status,
    ):
        osm = _make_osm()
        broker = _broker_post(broker_id=None, status_str=broker_status)
        row = _pending_row()

        first = _run_from_pending(osm, broker, row)

        assert first["ok"] is False
        assert first["error"] == (
            "BROKER_AMBIGUOUS_2XX_MISSING_ID_RECONCILIATION_REQUIRED:"
            f"status={broker_status or 'empty'}"
        )
        assert first["reconciliation_required"] is True
        assert first["identity_quarantine"] is True
        assert first["_durable_row"]["status"] == "PENDING_TRIGGER"
        durable_meta = json.loads(first["_durable_row"]["meta"])
        assert durable_meta["submit_intent_at"]
        assert durable_meta["broker_submit_key"] == _TAG
        assert broker.session.post.call_args.kwargs["data"]["tag"] == _TAG
        broker.session.post.assert_called_once()
        broker.session.get.assert_called_once()

        second = _run_from_pending(osm, broker, row)
        assert second["ok"] is False
        assert second["reconciliation_required"] is True
        assert broker.session.post.call_count == 1


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
        assert result["status"] == "SUBMITTED"
        assert result.get("quarantine_persisted") is True
        durable = result["_durable_row"]
        assert durable["status"] == "SUBMITTED"
        assert durable["broker_order_id"] == "BID-UNKNOWN-001"
        assert durable["last_error"].startswith("SPLIT_BRAIN:")
        assert json.loads(durable["meta"])["split_brain_quarantine"] is True
        assert "BROKER_STATUS_UNKNOWN_WITH_ID" in caplog.text
        assert len(result["_flag_split_brain_calls"]) == 1

    def test_unknown_status_with_id_preserves_broker_id(self):
        """Broker ID must survive in the return dict — never discarded."""
        osm = _make_osm()
        broker = _broker_post(broker_id="BID-PRESERVE-ME", status_str="weird_status")
        result = _run_from_pending(osm, broker)
        assert result["broker_order_id"] == "BID-PRESERVE-ME"

    def test_unknown_status_with_id_no_replacement_submit(self):
        """A second invocation after quarantine performs zero replacement POSTs."""
        osm = _make_osm()
        broker = _broker_post(broker_id="BID-ONCE", status_str="bizarre_status")
        row = _pending_row()
        first = _run_from_pending(osm, broker, row)
        second = _run_from_pending(osm, broker, row)
        assert first["_durable_row"]["status"] == "SUBMITTED"
        assert first["_durable_row"]["broker_order_id"] == "BID-ONCE"
        assert second["ok"] is False
        assert second["error"] == "ENTRY_SPLIT_BRAIN_QUARANTINED"
        assert broker.session.post.call_count == 1

    def test_unknown_status_without_id_holds_for_identity_reconciliation(self):
        """A clean 2xx without identity is ambiguous, never submit-eligible."""
        osm = _make_osm()
        broker = _broker_post(broker_id=None, status_str="mystery_status_no_id")
        result = _run_from_pending(osm, broker)
        assert result.get("split_brain") is not True
        assert result.get("reconciliation_required") is True
        assert result.get("identity_quarantine") is True
        assert result["error"].startswith("BROKER_AMBIGUOUS_2XX_MISSING_ID")
        assert result["_durable_row"]["status"] == OrderStatus.PENDING_TRIGGER
        assert json.loads(result["_durable_row"]["meta"])["lifecycle_state"] == "SUBMITTING"
        assert broker.session.post.call_count == 1
        broker.session.get.assert_called_once()
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

    @pytest.mark.parametrize("execution_mode", ["live", "paper"])
    def test_materialized_deferred_first_submit_is_exactly_once_and_durable(
        self, execution_mode,
    ):
        osm = _make_osm()
        broker = _broker_post(
            broker_id=f"BID-DEFERRED-{execution_mode.upper()}",
            status_str="open",
        )
        row = _pending_row(execution_mode=execution_mode)
        meta = json.loads(row["meta"])
        meta.update({
            "execution_mode": execution_mode,
            "contract_deferred": False,
            "materialization_generation": 7,
            "materialization_entry_path": "DEFERRED_BREACH_MATERIALIZATION",
            "lifecycle_state": "BROKER_READY",
            "broker_ready": True,
        })
        row["meta"] = json.dumps(meta)
        result = _run_from_pending(osm, broker, row)
        durable = result["_durable_row"]
        durable_meta = json.loads(durable["meta"])
        assert result["ok"] is True
        assert broker.session.post.call_count == 1
        assert broker.session.post.call_args.kwargs["data"]["tag"] == _TAG
        assert durable["status"] == "SUBMITTED"
        assert durable["broker_order_id"] == f"BID-DEFERRED-{execution_mode.upper()}"
        assert durable["submitted_ts"]
        assert durable_meta["submit_intent_at"]
        assert durable_meta["broker_submit_key"] == _TAG

    def test_preselected_pending_trigger_reaches_broker_post(self):
        osm = _make_osm()
        broker = _broker_post(broker_id="BID-STANDARD", status_str="open")
        row = _pending_row()
        result = _run_from_pending(osm, broker, row)

        assert result["ok"] is True
        assert result["broker_order_id"] == "BID-STANDARD"
        broker.session.post.assert_called_once()

    @pytest.mark.parametrize("execution_mode", ["live", "paper"])
    def test_ordinary_entry_does_not_use_materialization_mode_gate(self, execution_mode):
        """Ordinary rows retain the pre-#524 submit seam despite malformed meta mode."""
        osm = _make_osm()
        broker = _broker_post(broker_id=f"BID-{execution_mode.upper()}", status_str="open")
        row = _pending_row(execution_mode=execution_mode)
        meta = json.loads(row["meta"])
        meta["execution_mode"] = "staging"
        row["meta"] = json.dumps(meta)

        result = _run_from_pending(
            osm, broker, row, bypass_entry_guards=True,
        )

        assert result["ok"] is True
        assert result["error"] is None
        assert result["error"] != "MATERIALIZATION_EXECUTION_MODE_UNPROVEN"
        assert broker.session.post.call_count == 1

    def test_ordinary_submit_intent_uses_column_only_mode_cas(self, monkeypatch):
        """The real ordinary intent CAS must not apply deferred meta fencing."""
        osm = _make_osm()
        executed_sql = []

        class _Cursor:
            rowcount = 1

        class _Conn:
            rowcount = 1

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def execute(self, sql, _params):
                executed_sql.append(sql)
                return _Cursor()

        method_globals = APOrderStateMachine.persist_entry_submit_intent.__globals__
        monkeypatch.setitem(method_globals, "conn", lambda: _Conn())
        monkeypatch.setitem(
            method_globals,
            "run_with_retry",
            lambda fn, *args, **kwargs: fn(),
        )

        assert osm.persist_entry_submit_intent(
            _LOID,
            current_status="PENDING_TRIGGER",
            execution_mode="live",
            signal_id=_SIGNAL,
            contract=_CONTRACT,
            qty=1,
            limit_price=1.25,
            payload_hash="ordinary-payload-hash",
            broker_submit_key=_TAG,
        ) is True
        assert len(executed_sql) == 1
        assert "LOWER(COALESCE(execution_mode,'')) = %s" in executed_sql[0]
        assert "meta->>'execution_mode'" not in executed_sql[0]

    def test_deferred_entry_keeps_strict_mode_gate(self):
        osm = _make_osm()
        broker = _broker_post(broker_id="BID-SHOULD-NOT-APPEAR", status_str="open")
        row = _pending_row(execution_mode="")
        meta = json.loads(row["meta"])
        meta.pop("execution_mode", None)
        meta.update({
            "contract_deferred": False,
            "materialization_generation": 7,
            "materialization_entry_path": "DEFERRED_BREACH_MATERIALIZATION",
            "lifecycle_state": "BROKER_READY",
            "broker_ready": True,
        })
        row["meta"] = json.dumps(meta)

        result = _run_from_pending(
            osm, broker, row, bypass_entry_guards=True,
        )

        assert result["ok"] is False
        assert result["error"] == "MATERIALIZATION_EXECUTION_MODE_UNPROVEN"
        broker.session.post.assert_not_called()

    @pytest.mark.parametrize("execution_mode", ["live", "paper"])
    def test_materialized_deferred_blank_column_uses_meta_mode(self, execution_mode):
        osm = _make_osm()
        broker = _broker_post(broker_id=f"BID-META-{execution_mode.upper()}", status_str="open")
        row = _pending_row(execution_mode="")
        meta = json.loads(row["meta"])
        meta.update({
            "execution_mode": execution_mode,
            "contract_deferred": False,
            "materialization_generation": 7,
            "materialization_entry_path": "DEFERRED_BREACH_MATERIALIZATION",
            "lifecycle_state": "BROKER_READY",
            "broker_ready": True,
        })
        row["meta"] = json.dumps(meta)

        result = _run_from_pending(
            osm, broker, row, bypass_entry_guards=True,
        )

        assert result["ok"] is True
        assert broker.session.post.call_count == 1

    @pytest.mark.parametrize("execution_mode", ["live", "paper"])
    def test_recovery_deferred_blank_column_uses_meta_mode(self, execution_mode):
        osm = _make_osm()
        broker = _broker_post(broker_id=f"BID-RECOVERY-{execution_mode.upper()}", status_str="open")
        row = _pending_row(execution_mode="")
        owner = "recovery-submit:owner"
        meta = json.loads(row["meta"])
        meta.update({
            "execution_mode": execution_mode,
            "contract_deferred": False,
            "materialization_generation": 7,
            "materialization_entry_path": "DEFERRED_BREACH_MATERIALIZATION",
            "lifecycle_state": "BROKER_READY",
            "broker_ready": True,
            "recovery_submit_owner": owner,
        })
        row["meta"] = json.dumps(meta)
        plan = SimpleNamespace(metadata={
            "recovery_submit_fenced": True,
            "recovery_submit_owner": owner,
            "recovery_submit_generation": 7,
        })

        result = _run_from_pending(
            osm, broker, row, plan=plan, bypass_entry_guards=True,
        )

        assert result["ok"] is True
        assert broker.session.post.call_count == 1

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


def test_real_postgres_broker_ready_intent_quarantine_and_reconciler(monkeypatch):
    """Production-shaped end-to-end submit ownership seam on real PostgreSQL."""
    from contextlib import contextmanager
    import hashlib
    import uuid

    import psycopg2
    import psycopg2.extras

    from ap_reconciler import APBrokerReconciler

    database_url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "")
    if not database_url:
        pytest.skip("disposable PostgreSQL URL not configured")

    schema = f"osm_p0_{uuid.uuid4().hex}"

    class _Wrapper:
        def __init__(self, connection, cursor):
            self.connection = connection
            self.cursor = cursor

        @property
        def rowcount(self):
            return self.cursor.rowcount

        def execute(self, sql, params=None):
            self.cursor.execute(sql, params)
            return self

        def fetchone(self):
            return self.cursor.fetchone()

        def fetchall(self):
            return self.cursor.fetchall()

    @contextmanager
    def _pg_conn():
        connection = psycopg2.connect(database_url)
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute(f'SET search_path TO "{schema}"')
            yield _Wrapper(connection, cursor)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()

    admin = psycopg2.connect(database_url)
    admin.autocommit = True
    try:
        with admin.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{schema}"')
            cursor.execute(
                f"""
                CREATE TABLE "{schema}".orders (
                    local_order_id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    position_id TEXT,
                    plan_id TEXT,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    broker_order_id TEXT,
                    submitted_ts TIMESTAMPTZ,
                    filled_ts TIMESTAMPTZ,
                    fill_price NUMERIC,
                    execution_mode TEXT NOT NULL,
                    signal_id TEXT,
                    contract TEXT,
                    contract_selection_status TEXT,
                    symbol TEXT,
                    direction TEXT,
                    side TEXT,
                    timeframe TEXT,
                    score NUMERIC,
                    trigger_price NUMERIC,
                    pattern TEXT,
                    qty INTEGER,
                    limit_price NUMERIC,
                    reserved_cost NUMERIC,
                    meta JSONB,
                    last_error TEXT,
                    created_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )

        method_globals = APOrderStateMachine.persist_deferred_broker_ready.__globals__
        monkeypatch.setitem(method_globals, "conn", _pg_conn)
        monkeypatch.setitem(method_globals, "run_with_retry", lambda fn, *a, **k: fn())
        import ap.db as db_module
        monkeypatch.setattr(db_module, "conn", _pg_conn)
        monkeypatch.setattr(db_module, "run_with_retry", lambda fn, *a, **k: fn())

        accepted_id = "223e4567-e89b-12d3-a456-426614174000"
        accepted_tag = canonical_broker_submit_key(accepted_id)
        accepted_owner = "watcher-accepted"
        unknown_owner = "watcher-unknown"

        def _materializing_meta(*, owner: str, generation: int) -> dict:
            return {
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
                "contract_deferred": True,
                "lifecycle_state": "MATERIALIZING",
                "materialization_owner": owner,
                "materialization_generation": generation,
            }

        def _insert_materializing(local_order_id: str, *, owner: str, generation: int):
            with _pg_conn() as c:
                c.execute(
                    """
                    INSERT INTO orders (
                        local_order_id, client_id, kind, status, execution_mode,
                        signal_id, contract, symbol, direction, side, timeframe,
                        score, trigger_price, pattern, qty, limit_price,
                        reserved_cost, meta
                    ) VALUES (
                        %s,%s,'ENTRY','PENDING_TRIGGER','paper',%s,%s,%s,
                        'CALL','CALL','5m',0.85,450.50,'3-1-2',1,0.01,1.00,%s::jsonb
                    )
                    """,
                    (
                        local_order_id,
                        _CLIENT,
                        _SIGNAL,
                        _DEFERRED,
                        _SYMBOL,
                        json.dumps(_materializing_meta(owner=owner, generation=generation)),
                    ),
                )

        def _assert_committed_intent_then(response, local_order_id: str, expected_tag: str):
            def _post(*_args, **kwargs):
                order_data = kwargs["data"]
                expected_hash = hashlib.sha256(
                    json.dumps(
                        order_data,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest()
                with _pg_conn() as c:
                    c.execute(
                        "SELECT * FROM orders WHERE local_order_id=%s AND client_id=%s",
                        (local_order_id, _CLIENT),
                    )
                    before_response = dict(c.fetchone())
                intent_meta = before_response["meta"]
                assert before_response["status"] == "PENDING_TRIGGER"
                assert before_response["broker_order_id"] is None
                assert before_response["submitted_ts"] is None
                assert intent_meta["lifecycle_state"] == "SUBMITTING"
                assert intent_meta["submit_intent_at"]
                assert intent_meta["broker_submit_key"] == expected_tag
                assert intent_meta["broker_submit_payload_hash"] == expected_hash
                assert intent_meta["current_owner"] == f"broker_submit:{expected_tag}"
                return response

            return _post

        _insert_materializing(accepted_id, owner=accepted_owner, generation=8)
        _insert_materializing(_LOID, owner=unknown_owner, generation=7)

        osm = _make_osm()
        for local_order_id, owner, generation in (
            (accepted_id, accepted_owner, 8),
            (_LOID, unknown_owner, 7),
        ):
            assert osm.persist_deferred_broker_ready(
                local_order_id,
                owner=owner,
                generation=generation,
                signal_id=_SIGNAL,
                execution_mode="paper",
                contract=_CONTRACT,
                limit_price=1.25,
                qty=1,
                reserved_cost=125.0,
                selector_meta={"selector_debug": "postgres_acceptance"},
            )

        # Accepted broker truth: the POST callback opens a separate connection,
        # proving submit intent and its exact payload hash committed before any
        # broker response exists.
        accepted_broker = _broker_post(
            broker_id="BID-PG-ACCEPTED",
            status_str="open",
        )
        accepted_response = accepted_broker.session.post.return_value
        accepted_broker.session.post.side_effect = _assert_committed_intent_then(
            accepted_response,
            accepted_id,
            accepted_tag,
        )
        accepted = osm.submit_existing_entry(
            local_order_id=accepted_id,
            broker=accepted_broker,
        )
        assert accepted["ok"] is True
        assert accepted["broker_order_id"] == "BID-PG-ACCEPTED"
        accepted_broker.session.post.assert_called_once()
        assert accepted_broker.session.post.call_args.kwargs["data"]["tag"] == accepted_tag

        with _pg_conn() as c:
            c.execute(
                "SELECT * FROM orders WHERE local_order_id=%s AND client_id=%s",
                (accepted_id, _CLIENT),
            )
            accepted_durable = dict(c.fetchone())
        assert accepted_durable["contract"] == _CONTRACT
        assert float(accepted_durable["limit_price"]) == 1.25
        assert accepted_durable["contract_selection_status"] == "CONTRACT_SELECTED"
        assert accepted_durable["status"] == "SUBMITTED"
        assert accepted_durable["broker_order_id"] == "BID-PG-ACCEPTED"
        assert accepted_durable["submitted_ts"] is not None
        assert accepted_durable["meta"]["submit_intent_at"]
        assert accepted_durable["meta"]["broker_submit_key"] == accepted_tag
        assert accepted_durable["meta"]["submit_completed_at"]

        # Unknown broker status with a real ID is quarantined after the same
        # durable-before-POST proof and cannot issue a replacement submit.
        unknown_broker = _broker_post(
            broker_id="BID-PG-UNKNOWN",
            status_str="unexpected_broker_state",
        )
        unknown_response = unknown_broker.session.post.return_value
        unknown_broker.session.post.side_effect = _assert_committed_intent_then(
            unknown_response,
            _LOID,
            _TAG,
        )
        result = osm.submit_existing_entry(local_order_id=_LOID, broker=unknown_broker)
        assert result["ok"] is False
        assert result["quarantine_persisted"] is True
        assert unknown_broker.session.post.call_count == 1
        assert unknown_broker.session.post.call_args.kwargs["data"]["tag"] == _TAG

        with _pg_conn() as c:
            c.execute(
                "SELECT * FROM orders WHERE local_order_id=%s AND client_id=%s",
                (_LOID, _CLIENT),
            )
            durable = dict(c.fetchone())
        assert durable["contract"] == _CONTRACT
        assert float(durable["limit_price"]) == 1.25
        assert durable["contract_selection_status"] == "CONTRACT_SELECTED"
        assert durable["status"] == "SUBMITTED"
        assert durable["broker_order_id"] == "BID-PG-UNKNOWN"
        assert durable["submitted_ts"] is not None
        assert durable["meta"]["submit_intent_at"]
        assert durable["meta"]["broker_submit_key"] == _TAG
        assert durable["meta"]["split_brain_quarantine"] is True

        rows = db_module.get_open_orders_for_reconcile(
            client_id=_CLIENT,
            execution_mode="paper",
        )
        assert {row["local_order_id"] for row in rows} == {accepted_id, _LOID}

        second = osm.submit_existing_entry(local_order_id=_LOID, broker=unknown_broker)
        assert second["ok"] is False
        assert second["error"] == "ENTRY_SPLIT_BRAIN_QUARANTINED"
        assert unknown_broker.session.post.call_count == 1

        # Exercise the actual reconciler, not the resolver directly. Exact-ID
        # broker truth clears only the quarantined row through the fenced OSM CAS.
        reconcile_broker = MagicMock()
        reconcile_broker.get_order.side_effect = (
            lambda broker_order_id: {"id": broker_order_id, "status": "open"}
        )
        with patch.object(APBrokerReconciler, "_register_health", return_value=None):
            reconciler = APBrokerReconciler(
                broker=reconcile_broker,
                client_id=_CLIENT,
                osm=osm,
                pm=MagicMock(),
                execution_mode="paper",
            )
        summary: dict = {}
        with patch.object(reconciler, "_check_ghost_fills", return_value=None):
            reconciler._reconcile_orders(summary)
        assert summary["orders_checked"] == 2
        assert summary["split_brain_resolved"] == 1
        assert reconcile_broker.get_order.call_count == 2
        assert osm.get_split_brain_orders(execution_mode="paper") == []

        with _pg_conn() as c:
            c.execute(
                "SELECT last_error, meta FROM orders WHERE local_order_id=%s AND client_id=%s",
                (_LOID, _CLIENT),
            )
            reconciled = dict(c.fetchone())
        assert reconciled["last_error"] == "RECONCILED_SPLIT_BRAIN:broker_status=open"
        assert reconciled["meta"]["split_brain_quarantine"] is False
        assert reconciled["meta"]["reconciliation_required"] is False
        assert reconciled["meta"]["split_brain_resolved_by"] == "ap_reconciler"

        # Fresh submitted/no-ID rows share a signal ID but remain isolated by
        # local ID + client + execution mode. No signal-level alias may attach
        # identity or permit Jason to submit Jose's row.
        jason_identity_id = "323e4567-e89b-12d3-a456-426614174000"
        jose_identity_id = "423e4567-e89b-12d3-a456-426614174000"
        jose_client = "jose@test.com"
        shared_signal_id = "SIG-SHARED-IDENTITY-FENCE"
        identity_meta = json.dumps({"lifecycle_state": "SUBMITTED"})
        with _pg_conn() as c:
            for local_order_id, client_id in (
                (jason_identity_id, _CLIENT),
                (jose_identity_id, jose_client),
            ):
                c.execute(
                    """
                    INSERT INTO orders (
                        local_order_id, client_id, kind, status, execution_mode,
                        signal_id, contract, symbol, qty, limit_price, meta
                    ) VALUES (%s,%s,'ENTRY','SUBMITTED','paper',%s,%s,%s,1,1.25,%s::jsonb)
                    """,
                    (
                        local_order_id,
                        client_id,
                        shared_signal_id,
                        _CONTRACT,
                        _SYMBOL,
                        identity_meta,
                    ),
                )

        jose = APOrderStateMachine(jose_client)
        assert not osm._attach_broker_identity_if_missing(
            jason_identity_id,
            broker_order_id="BID-WRONG-MODE",
            current_status="SUBMITTED",
            current_execution_mode="live",
        )
        assert not jose._attach_broker_identity_if_missing(
            jason_identity_id,
            broker_order_id="BID-WRONG-CLIENT",
            current_status="SUBMITTED",
            current_execution_mode="paper",
        )
        assert osm._attach_broker_identity_if_missing(
            jason_identity_id,
            broker_order_id="BID-JASON-PAPER",
            current_status="SUBMITTED",
            current_execution_mode="paper",
        )

        cross_client_broker = _broker_post(
            broker_id="BID-MUST-NOT-POST",
            status_str="open",
        )
        cross_client_result = osm.submit_existing_entry(
            local_order_id=jose_identity_id,
            broker=cross_client_broker,
        )
        assert cross_client_result["ok"] is False
        assert cross_client_result["error"] == "existing_entry_order_not_found"
        cross_client_broker.session.post.assert_not_called()
        assert jose._attach_broker_identity_if_missing(
            jose_identity_id,
            broker_order_id="BID-JOSE-PAPER",
            current_status="SUBMITTED",
            current_execution_mode="paper",
        )

        with _pg_conn() as c:
            c.execute(
                """
                SELECT local_order_id, client_id, execution_mode, signal_id, broker_order_id
                FROM orders
                WHERE local_order_id IN (%s, %s)
                ORDER BY local_order_id
                """,
                (jason_identity_id, jose_identity_id),
            )
            identity_rows = [dict(row) for row in c.fetchall()]
        assert [row["signal_id"] for row in identity_rows] == [
            shared_signal_id,
            shared_signal_id,
        ]
        assert identity_rows[0]["broker_order_id"] == "BID-JASON-PAPER"
        assert identity_rows[1]["broker_order_id"] == "BID-JOSE-PAPER"
    finally:
        with admin.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()
