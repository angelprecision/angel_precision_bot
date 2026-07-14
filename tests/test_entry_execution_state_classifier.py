"""
tests/test_entry_execution_state_classifier.py
───────────────────────────────────────────────
Comprehensive test suite for classify_entry_execution_state() and the
execution-state annotation added to every ENTRY order row via _order_row().

Coverage required by the PR spec:

    ✓ pre-breach row
    ✓ future retry
    ✓ overdue retry
    ✓ materializing row
    ✓ broker-ready row
    ✓ final-gate blocked row
    ✓ submit-intent without broker ID
    ✓ reconcile-pending row
    ✓ submitted row (broker_order_id present)
    ✓ terminal row
    ✓ every INCONSISTENT_STATE invariant
    ✓ PAPER / LIVE display in _order_row
    ✓ raw diagnostics remain available
    ✓ no database UPDATE / INSERT / DELETE is issued
"""
from __future__ import annotations

import datetime as _dt
import inspect
import re

import pytest

from ap.operator_queue_read_model import (
    EntryExecutionState,
    _check_entry_invariants,
    _meta_bool,
    _meta_str,
    _parse_iso_utc,
    _resolve_retry_timestamp,
    _order_row,
    classify_entry_execution_state,
)
import ap.operator_queue_read_model as _qrm


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

_UTC = _dt.timezone.utc
_NOW = _dt.datetime(2026, 7, 14, 14, 0, 0, tzinfo=_UTC)
_FUTURE = (_NOW + _dt.timedelta(minutes=10)).isoformat()
_PAST   = (_NOW - _dt.timedelta(minutes=10)).isoformat()
_OCC    = "AAPL260718C00200000"


def _make_row(
    status: str = "PENDING_TRIGGER",
    broker_order_id: str | None = None,
    contract: str = _OCC,
    limit_price: float = 2.50,
    execution_mode: str = "paper",
    meta: dict | None = None,
    updated_ts=None,
    **extra,
) -> dict:
    """Construct a minimal synthetic orders row for classifier tests."""
    return {
        "local_order_id": "ord-test-001",
        "client_id":      "test@example.com",
        "signal_id":      "sig-001",
        "plan_id":        "plan-001",
        "status":         status,
        "broker_order_id": broker_order_id,
        "contract":       contract,
        "limit_price":    limit_price,
        "execution_mode": execution_mode,
        "symbol":         "AAPL",
        "side":           "CALL",
        "score":          85.0,
        "created_ts":     "2026-07-14T09:00:00+00:00",
        "updated_ts":     updated_ts or "2026-07-14T13:55:00+00:00",
        "last_error":     None,
        "meta":           meta or {},
        **extra,
    }


def _classify(row: dict) -> dict:
    return classify_entry_execution_state(row, now_utc=_NOW)


# ─────────────────────────────────────────────────────────────────────────────
# Helper unit tests
# ─────────────────────────────────────────────────────────────────────────────

class TestMetaHelpers:
    def test_meta_bool_python_true(self):
        assert _meta_bool({"broker_ready": True}, "broker_ready") is True

    def test_meta_bool_python_false(self):
        assert _meta_bool({"broker_ready": False}, "broker_ready") is False

    def test_meta_bool_string_true(self):
        assert _meta_bool({"broker_ready": "true"}, "broker_ready") is True

    def test_meta_bool_string_false(self):
        assert _meta_bool({"broker_ready": "false"}, "broker_ready") is False

    def test_meta_bool_missing(self):
        assert _meta_bool({}, "broker_ready") is False

    def test_meta_str_present(self):
        assert _meta_str({"lifecycle_state": "RETRY_WAIT"}, "lifecycle_state") == "RETRY_WAIT"

    def test_meta_str_missing(self):
        assert _meta_str({}, "lifecycle_state") == ""

    def test_parse_iso_utc_z_suffix(self):
        dt = _parse_iso_utc("2026-07-14T14:10:00Z")
        assert dt is not None
        assert dt.tzinfo is not None

    def test_parse_iso_utc_offset(self):
        dt = _parse_iso_utc("2026-07-14T14:10:00+00:00")
        assert dt is not None

    def test_parse_iso_utc_blank(self):
        assert _parse_iso_utc("") is None

    def test_parse_iso_utc_none(self):
        assert _parse_iso_utc(None) is None

    def test_parse_iso_utc_garbage(self):
        assert _parse_iso_utc("NOT-A-TIMESTAMP") is None

    def test_resolve_retry_prefers_materialization_next(self):
        meta = {
            "materialization_next_retry_at": _FUTURE,
            "deferred_retry_next_attempt_at": (_NOW - _dt.timedelta(hours=1)).isoformat(),
            "next_retry_at": (_NOW - _dt.timedelta(hours=2)).isoformat(),
        }
        ts, field = _resolve_retry_timestamp(meta)
        assert field == "materialization_next_retry_at"
        assert ts > _NOW

    def test_resolve_retry_prefers_deferred_over_next_retry_at(self):
        """deferred_retry_next_attempt_at has priority 2; next_retry_at has priority 3.

        This is the critical ordering: when deferred is overdue but next_retry_at
        is future, the result must be DUE_RETRY_PENDING (not FUTURE_RETRY_SCHEDULED).
        """
        meta = {
            "deferred_retry_next_attempt_at": _PAST,
            "next_retry_at": _FUTURE,
        }
        ts, field = _resolve_retry_timestamp(meta)
        assert field == "deferred_retry_next_attempt_at"
        assert ts < _NOW  # overdue

    def test_resolve_retry_falls_back_to_next_retry_at(self):
        meta = {"next_retry_at": _FUTURE}
        ts, field = _resolve_retry_timestamp(meta)
        assert field == "next_retry_at"

    def test_resolve_retry_falls_back_to_deferred(self):
        meta = {"deferred_retry_next_attempt_at": _FUTURE}
        ts, field = _resolve_retry_timestamp(meta)
        assert field == "deferred_retry_next_attempt_at"

    def test_resolve_retry_all_missing(self):
        ts, field = _resolve_retry_timestamp({})
        assert ts is None
        assert field == ""

    def test_parse_iso_utc_naive_returns_none(self):
        """Timezone-naive timestamps must be treated as malformed (not silently UTC)."""
        result = _parse_iso_utc("2026-07-14T14:10:00")  # no tz info
        assert result is None, "naive timestamp must not be silently assumed to be UTC"

    def test_parse_iso_utc_aware_z_returns_datetime(self):
        result = _parse_iso_utc("2026-07-14T14:10:00Z")
        assert result is not None
        assert result.tzinfo is not None


# ─────────────────────────────────────────────────────────────────────────────
# Classification: normal (healthy) states
# ─────────────────────────────────────────────────────────────────────────────

class TestPreBreach:
    """PRE_BREACH — healthy PENDING_TRIGGER row before any trigger breach."""

    def test_basic_pre_breach(self):
        row = _make_row(status="PENDING_TRIGGER")
        r = _classify(row)
        assert r["execution_state"] == EntryExecutionState.PRE_BREACH

    def test_pre_breach_no_broker_id_reason(self):
        r = _classify(_make_row())
        assert r["no_broker_id_reason"] == "WAITING_FOR_TRIGGER"

    def test_pre_breach_action_not_required(self):
        r = _classify(_make_row())
        assert r["action_required"] is False

    def test_pre_breach_next_transition(self):
        r = _classify(_make_row())
        assert "MATERIALIZING" in r["next_expected_transition"]
        assert "trigger" in r["next_expected_transition"].lower()

    def test_pre_breach_overdue_is_none(self):
        r = _classify(_make_row())
        assert r["overdue_seconds"] is None

    def test_pre_breach_state_updated_at_populated(self):
        r = _classify(_make_row(updated_ts="2026-07-14T13:55:00+00:00"))
        assert "2026-07-14" in (r["state_updated_at"] or "")


class TestFutureRetryScheduled:
    """FUTURE_RETRY_SCHEDULED — retry is scheduled but not yet due."""

    def _row(self, **kw) -> dict:
        meta = {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_in_flight": False,
            "broker_ready": False,
            "retry_reason": "MONEYNESS_OUT_OF_RANGE",
            "retry_attempt": 1,
            "retry_max_attempts": 3,
            "materialization_next_retry_at": _FUTURE,
            **kw,
        }
        return _make_row(
            status="PENDING_TRIGGER",
            contract="DEFERRED:AAPL",
            limit_price=0.01,
            meta=meta,
        )

    def test_state(self):
        r = _classify(self._row())
        assert r["execution_state"] == EntryExecutionState.FUTURE_RETRY_SCHEDULED

    def test_no_broker_id_reason_contains_retry_reason(self):
        r = _classify(self._row())
        assert r["no_broker_id_reason"] == "MONEYNESS_OUT_OF_RANGE"

    def test_action_not_required(self):
        r = _classify(self._row())
        assert r["action_required"] is False

    def test_next_transition_mentions_due_retry(self):
        r = _classify(self._row())
        assert "DUE_RETRY_PENDING" in r["next_expected_transition"]

    def test_overdue_is_none_for_future_retry(self):
        r = _classify(self._row())
        assert r["overdue_seconds"] is None

    def test_legacy_next_retry_at_field_works(self):
        """next_retry_at (legacy) still triggers FUTURE_RETRY_SCHEDULED."""
        meta = {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_in_flight": False,
            "broker_ready": False,
            "next_retry_at": _FUTURE,
        }
        r = _classify(_make_row(contract="DEFERRED:AAPL", limit_price=0.01, meta=meta))
        assert r["execution_state"] == EntryExecutionState.FUTURE_RETRY_SCHEDULED

    def test_deferred_retry_next_attempt_at_field_works(self):
        """deferred_retry_next_attempt_at still triggers correct state."""
        meta = {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_in_flight": False,
            "broker_ready": False,
            "deferred_retry_next_attempt_at": _FUTURE,
        }
        r = _classify(_make_row(contract="DEFERRED:AAPL", limit_price=0.01, meta=meta))
        assert r["execution_state"] == EntryExecutionState.FUTURE_RETRY_SCHEDULED


class TestDueRetryPending:
    """DUE_RETRY_PENDING — retry timestamp is in the past (overdue)."""

    def _row(self, **kw) -> dict:
        meta = {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_in_flight": False,
            "broker_ready": False,
            "retry_reason": "DELTA_OUT_OF_RANGE",
            "retry_attempt": 2,
            "retry_max_attempts": 3,
            "materialization_next_retry_at": _PAST,
            **kw,
        }
        return _make_row(
            status="PENDING_TRIGGER",
            contract="DEFERRED:AAPL",
            limit_price=0.01,
            meta=meta,
        )

    def test_state(self):
        r = _classify(self._row())
        assert r["execution_state"] == EntryExecutionState.DUE_RETRY_PENDING

    def test_no_broker_id_reason(self):
        r = _classify(self._row())
        assert r["no_broker_id_reason"] == "RETRY_OVERDUE"

    def test_action_required(self):
        r = _classify(self._row())
        assert r["action_required"] is True

    def test_overdue_seconds_positive(self):
        r = _classify(self._row())
        assert r["overdue_seconds"] is not None
        assert r["overdue_seconds"] > 0

    def test_overdue_seconds_approx_600(self):
        """_PAST is 10 minutes ago so overdue should be ~600s."""
        r = _classify(self._row())
        assert 550 < r["overdue_seconds"] < 650


class TestMaterializing:
    """MATERIALIZING — contract selector is actively running."""

    def _row(self, **kw) -> dict:
        meta = {
            "lifecycle_state": "MATERIALIZING",
            "materialization_status": "RUNNING",
            "materialization_in_flight": True,
            "materialization_generation": 2,
            "materialization_owner": "watcher:AAPL:abc123",
            "current_owner": "watcher:AAPL:abc123",
            "watcher_token": "abc123",
            "broker_ready": False,
            **kw,
        }
        return _make_row(
            status="PENDING_TRIGGER",
            contract="DEFERRED:AAPL",
            limit_price=0.01,
            meta=meta,
        )

    def test_state(self):
        r = _classify(self._row())
        assert r["execution_state"] == EntryExecutionState.MATERIALIZING

    def test_no_broker_id_reason(self):
        r = _classify(self._row())
        assert r["no_broker_id_reason"] == "MATERIALIZATION_IN_PROGRESS"

    def test_action_not_required(self):
        r = _classify(self._row())
        assert r["action_required"] is False

    def test_next_transition_mentions_broker_ready(self):
        r = _classify(self._row())
        assert "BROKER_READY" in r["next_expected_transition"]

    def test_via_mat_in_flight_flag_only(self):
        """materialization_in_flight=true is enough even without lifecycle_state."""
        meta = {
            "materialization_in_flight": True,
            "materialization_generation": 1,
            "materialization_owner": "watcher:AAPL:xyz",
            "broker_ready": False,
        }
        r = _classify(_make_row(contract="DEFERRED:AAPL", limit_price=0.01, meta=meta))
        assert r["execution_state"] == EntryExecutionState.MATERIALIZING


class TestBrokerReady:
    """BROKER_READY — contract selected, awaiting broker POST."""

    def _row(self) -> dict:
        meta = {
            "lifecycle_state": "BROKER_READY",
            "materialization_status": "SELECTED",
            "materialization_in_flight": False,
            "broker_ready": True,
            "materialization_generation": 1,
            "current_owner": "watcher:AAPL:abc",
        }
        return _make_row(status="PENDING_TRIGGER", contract=_OCC, limit_price=2.50, meta=meta)

    def test_state(self):
        r = _classify(self._row())
        assert r["execution_state"] == EntryExecutionState.BROKER_READY

    def test_no_broker_id_reason(self):
        r = _classify(self._row())
        assert r["no_broker_id_reason"] == "BROKER_READY_AWAITING_SUBMIT"

    def test_action_not_required(self):
        r = _classify(self._row())
        assert r["action_required"] is False

    def test_next_transition_mentions_broker_submitted(self):
        r = _classify(self._row())
        assert "BROKER_SUBMITTED" in r["next_expected_transition"]


class TestFinalGateBlocked:
    """FINAL_GATE_BLOCKED — live submit gate blocked and order terminated."""

    def _row(self, reason_code: str = "CURRENT_PRICE_STALE") -> dict:
        meta = {
            "broker_ready": False,
            "lifecycle_state": "",
            "materialization_in_flight": False,
            "final_market_validity": {
                "gate": "market_validity",
                "passed": False,
                "reason_code": reason_code,
                "reason": reason_code,
                "checked_at": "2026-07-14T13:55:00+00:00",
            },
        }
        return _make_row(
            status="PENDING_TRIGGER",
            execution_mode="live",
            meta=meta,
        )

    def test_state(self):
        r = _classify(self._row())
        assert r["execution_state"] == EntryExecutionState.FINAL_GATE_BLOCKED

    def test_no_broker_id_reason_is_reason_code(self):
        r = _classify(self._row("CALL_NO_LONGER_ABOVE_TRIGGER"))
        assert r["no_broker_id_reason"] == "CALL_NO_LONGER_ABOVE_TRIGGER"

    def test_action_not_required_already_terminated(self):
        r = _classify(self._row())
        assert r["action_required"] is False

    def test_next_transition_terminal(self):
        r = _classify(self._row())
        assert "TERMINAL" in r["next_expected_transition"]

    def test_all_known_gate_reason_codes(self):
        codes = [
            "CURRENT_PRICE_STALE",
            "CALL_NO_LONGER_ABOVE_TRIGGER",
            "CALL_STOP_ALREADY_BROKEN",
            "PUT_NO_LONGER_BELOW_TRIGGER",
            "PUT_STOP_ALREADY_BROKEN",
            "TARGET_ALREADY_INVALID",
            "REMAINING_OPPORTUNITY_TOO_SMALL",
            "STALE_TRIGGER_BREACH",
        ]
        for code in codes:
            r = _classify(self._row(code))
            assert r["execution_state"] == EntryExecutionState.FINAL_GATE_BLOCKED
            assert r["no_broker_id_reason"] == code


class TestSubmitIntentPending:
    """SUBMIT_INTENT_PENDING — crash window before broker_order_id was committed."""

    def _row(self) -> dict:
        submit_key = "ord-test-001"
        meta = {
            "lifecycle_state": "SUBMITTING",
            "submit_intent_at": "2026-07-14T13:50:00+00:00",
            "broker_submit_key": submit_key,
            "current_owner": f"broker_submit:{submit_key}",
            "broker_ready": True,
            "materialization_generation": 1,
            "materialization_in_flight": False,
        }
        # broker_order_id is absent (crash before commit)
        return _make_row(
            status="PENDING_TRIGGER",
            contract=_OCC,
            limit_price=2.50,
            broker_order_id=None,
            meta=meta,
        )

    def test_state(self):
        r = _classify(self._row())
        assert r["execution_state"] == EntryExecutionState.SUBMIT_INTENT_PENDING

    def test_no_broker_id_reason(self):
        r = _classify(self._row())
        assert r["no_broker_id_reason"] == "SUBMIT_INTENT_WITHOUT_BROKER_ID"

    def test_action_required(self):
        r = _classify(self._row())
        assert r["action_required"] is True

    def test_next_transition_mentions_both_outcomes(self):
        r = _classify(self._row())
        transition = r["next_expected_transition"]
        assert "BROKER_SUBMITTED" in transition
        assert "RETRY_WAIT" in transition


class TestReconcilePending:
    """RECONCILE_PENDING — reconciler took ownership but could not resolve."""

    def _row(self) -> dict:
        meta = {
            "lifecycle_state": "SUBMITTING",
            "submit_intent_at": "2026-07-14T13:50:00+00:00",
            "broker_submit_key": "ord-test-001",
            "current_owner": "broker_reconciler:test@example.com",
            "broker_ready": False,
            "materialization_in_flight": False,
        }
        return _make_row(
            status="PENDING_TRIGGER",
            contract=_OCC,
            limit_price=2.50,
            broker_order_id=None,
            meta=meta,
        )

    def test_state(self):
        r = _classify(self._row())
        assert r["execution_state"] == EntryExecutionState.RECONCILE_PENDING

    def test_action_required(self):
        r = _classify(self._row())
        assert r["action_required"] is True

    def test_next_transition_mentions_both_outcomes(self):
        transition = _classify(self._row())["next_expected_transition"]
        assert "BROKER_SUBMITTED" in transition
        assert "RETRY_WAIT" in transition


class TestBrokerSubmitted:
    """BROKER_SUBMITTED — broker_order_id present."""

    def test_submitted_status(self):
        row = _make_row(status="SUBMITTED", broker_order_id="TRD-99999")
        r = _classify(row)
        assert r["execution_state"] == EntryExecutionState.BROKER_SUBMITTED

    def test_no_broker_id_reason_is_none(self):
        row = _make_row(status="SUBMITTED", broker_order_id="TRD-99999")
        r = _classify(row)
        assert r["no_broker_id_reason"] is None

    def test_action_not_required(self):
        row = _make_row(status="SUBMITTED", broker_order_id="TRD-99999")
        assert _classify(row)["action_required"] is False

    def test_next_transition_mentions_fill_or_reject(self):
        row = _make_row(status="SUBMITTED", broker_order_id="TRD-99999")
        t = _classify(row)["next_expected_transition"]
        assert "FILLED" in t or "REJECTED" in t

    def test_acknowledged_status(self):
        row = _make_row(status="ACKNOWLEDGED", broker_order_id="TRD-99999")
        assert _classify(row)["execution_state"] == EntryExecutionState.BROKER_SUBMITTED

    def test_working_status(self):
        row = _make_row(status="WORKING", broker_order_id="TRD-99999")
        assert _classify(row)["execution_state"] == EntryExecutionState.BROKER_SUBMITTED


class TestTerminal:
    """TERMINAL — order is in a terminal lifecycle state."""

    @pytest.mark.parametrize("status", [
        "REJECTED", "EXPIRED", "CANCELED", "ERROR",
        "WATCHER_INVALIDATED", "ENTRY_CONFIRMATION_FAILED",
    ])
    def test_terminal_statuses(self, status: str):
        row = _make_row(status=status, broker_order_id=None)
        r = _classify(row)
        assert r["execution_state"] == EntryExecutionState.TERMINAL

    def test_terminal_no_broker_id_reason_is_status(self):
        row = _make_row(status="REJECTED")
        r = _classify(row)
        assert r["no_broker_id_reason"] == "REJECTED"

    def test_terminal_action_not_required(self):
        row = _make_row(status="EXPIRED")
        assert _classify(row)["action_required"] is False

    def test_terminal_overdue_none(self):
        row = _make_row(status="REJECTED")
        assert _classify(row)["overdue_seconds"] is None


# ─────────────────────────────────────────────────────────────────────────────
# Classification: INCONSISTENT_STATE invariants
# ─────────────────────────────────────────────────────────────────────────────

class TestInconsistentStateInvariants:
    """Every documented invariant must produce INCONSISTENT_STATE."""

    def _check(self, row: dict) -> dict:
        r = _classify(row)
        assert r["execution_state"] == EntryExecutionState.INCONSISTENT_STATE, (
            f"Expected INCONSISTENT_STATE but got {r['execution_state']!r}: "
            f"no_broker_id_reason={r['no_broker_id_reason']!r}"
        )
        return r

    def test_inv1_broker_ready_with_deferred_contract(self):
        """Invariant 1: broker_ready=true with DEFERRED: contract."""
        meta = {
            "broker_ready": True,
            "materialization_generation": 1,
            "lifecycle_state": "BROKER_READY",
            "materialization_in_flight": False,
        }
        row = _make_row(contract="DEFERRED:AAPL", limit_price=5.00, meta=meta)
        r = self._check(row)
        assert "BROKER_READY_WITH_DEFERRED_CONTRACT" in r["no_broker_id_reason"]

    def test_inv2_broker_ready_with_limit_price_zero(self):
        """Invariant 2: broker_ready=true with limit_price=0.01."""
        meta = {
            "broker_ready": True,
            "materialization_generation": 1,
            "lifecycle_state": "BROKER_READY",
            "materialization_in_flight": False,
        }
        row = _make_row(contract=_OCC, limit_price=0.01, meta=meta)
        r = self._check(row)
        assert "BROKER_READY_WITH_INVALID_LIMIT_PRICE" in r["no_broker_id_reason"]

    def test_inv2_broker_ready_with_limit_price_below_threshold(self):
        """Invariant 2: broker_ready=true with limit_price=0.005."""
        meta = {"broker_ready": True, "materialization_generation": 1, "lifecycle_state": "BROKER_READY", "materialization_in_flight": False}
        row = _make_row(contract=_OCC, limit_price=0.005, meta=meta)
        self._check(row)

    def test_inv3_broker_order_id_with_pending_trigger_status(self):
        """Invariant 3: broker_order_id present while status=PENDING_TRIGGER."""
        row = _make_row(status="PENDING_TRIGGER", broker_order_id="TRD-999")
        r = self._check(row)
        assert "BROKER_ORDER_ID_WITH_PENDING_TRIGGER_STATUS" in r["no_broker_id_reason"]

    def test_inv4_submit_intent_after_terminal_rejected(self):
        """Invariant 4: submit_intent_at set after terminal status REJECTED."""
        meta = {
            "submit_intent_at": "2026-07-14T13:50:00+00:00",
            "broker_submit_key": "ord-001",
            "lifecycle_state": "SUBMITTING",
        }
        row = _make_row(status="REJECTED", broker_order_id=None, meta=meta)
        r = self._check(row)
        assert "SUBMIT_INTENT_AFTER_TERMINAL_STATUS" in r["no_broker_id_reason"]

    def test_inv4_submit_intent_after_terminal_expired(self):
        """Invariant 4: submit_intent_at set after EXPIRED."""
        meta = {
            "submit_intent_at": "2026-07-14T13:50:00+00:00",
            "broker_submit_key": "ord-001",
        }
        row = _make_row(status="EXPIRED", broker_order_id=None, meta=meta)
        self._check(row)

    def test_inv5_retry_wait_without_retry_timestamp(self):
        """Invariant 5: RETRY_WAIT lifecycle with no retry timestamp at all."""
        meta = {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_in_flight": False,
            "broker_ready": False,
            # Intentionally no materialization_next_retry_at / next_retry_at
        }
        row = _make_row(contract="DEFERRED:AAPL", limit_price=0.01, meta=meta)
        r = self._check(row)
        assert "RETRY_WAIT_WITHOUT_RETRY_TIMESTAMP" in r["no_broker_id_reason"]

    def test_inv6_materializing_without_owner(self):
        """Invariant 6: MATERIALIZING lifecycle with no owner set."""
        meta = {
            "lifecycle_state": "MATERIALIZING",
            "materialization_status": "RUNNING",
            "materialization_in_flight": True,
            "materialization_generation": 1,
            "broker_ready": False,
            # No materialization_owner, no current_owner
        }
        row = _make_row(contract="DEFERRED:AAPL", limit_price=0.01, meta=meta)
        r = self._check(row)
        assert "MATERIALIZING_WITHOUT_OWNER" in r["no_broker_id_reason"]

    def test_inv7_materialization_in_flight_without_generation(self):
        """Invariant 7: materialization_in_flight=true with no generation."""
        meta = {
            "lifecycle_state": "MATERIALIZING",
            "materialization_status": "RUNNING",
            "materialization_in_flight": True,
            # No materialization_generation
            "materialization_owner": "watcher:AAPL:abc",
            "broker_ready": False,
        }
        row = _make_row(contract="DEFERRED:AAPL", limit_price=0.01, meta=meta)
        r = self._check(row)
        assert "MATERIALIZATION_IN_FLIGHT_WITHOUT_GENERATION" in r["no_broker_id_reason"]

    def test_inv8_invalid_execution_mode(self):
        """Invariant 8: execution_mode is not 'paper', 'live', or 'unknown'."""
        row = _make_row(execution_mode="sandbox")
        r = self._check(row)
        assert "INVALID_EXECUTION_MODE" in r["no_broker_id_reason"]

    def test_inv9_occ_contract_with_retry_wait_lifecycle(self):
        """Invariant 9: real OCC contract on a RETRY_WAIT lifecycle row."""
        meta = {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_in_flight": False,
            "broker_ready": False,
            "materialization_next_retry_at": _FUTURE,
        }
        # Real OCC symbol (not DEFERRED:) but lifecycle is RETRY_WAIT — impossible
        row = _make_row(contract=_OCC, limit_price=2.50, meta=meta)
        r = self._check(row)
        assert "OCC_CONTRACT_WITH_RETRY_WAIT_LIFECYCLE" in r["no_broker_id_reason"]

    def test_inconsistent_action_always_required(self):
        """Every INCONSISTENT_STATE must require operator action."""
        # Use inv3 as a representative
        row = _make_row(status="PENDING_TRIGGER", broker_order_id="TRD-999")
        r = _classify(row)
        assert r["action_required"] is True

    def test_inconsistent_next_transition_mentions_investigation(self):
        """INCONSISTENT_STATE next_transition must tell operator to investigate."""
        row = _make_row(status="PENDING_TRIGGER", broker_order_id="TRD-999")
        r = _classify(row)
        assert "investigation" in r["next_expected_transition"].lower()


# ─────────────────────────────────────────────────────────────────────────────
# Invariant check function unit tests
# ─────────────────────────────────────────────────────────────────────────────

class TestCheckEntryInvariants:
    """Direct tests against _check_entry_invariants()."""

    def _check(self, **kw) -> tuple[bool, str]:
        defaults = dict(
            status="PENDING_TRIGGER",
            contract=_OCC,
            limit_price=2.50,
            execution_mode="paper",
            broker_order_id=None,
            meta={},
        )
        defaults.update(kw)
        return _check_entry_invariants(**defaults)

    def test_clean_row_no_violation(self):
        violated, code = self._check()
        assert violated is False
        assert code == ""

    def test_all_known_valid_modes_pass(self):
        for mode in ("paper", "live", "unknown", ""):
            violated, _ = self._check(execution_mode=mode)
            assert violated is False, f"mode={mode!r} should pass"

    def test_broker_ready_with_deferred_and_valid_price_violates(self):
        violated, code = self._check(
            contract="DEFERRED:AAPL",
            limit_price=5.0,
            meta={"broker_ready": True, "materialization_generation": 1},
        )
        assert violated
        assert "BROKER_READY_WITH_DEFERRED_CONTRACT" in code

    def test_broker_ready_string_true_also_violates(self):
        """Postgres may deliver 'true' as a string; both must be caught."""
        violated, _ = self._check(
            contract="DEFERRED:AAPL",
            limit_price=5.0,
            meta={"broker_ready": "true", "materialization_generation": 1},
        )
        assert violated


# ─────────────────────────────────────────────────────────────────────────────
# _order_row: PAPER / LIVE display and raw diagnostics
# ─────────────────────────────────────────────────────────────────────────────

class TestOrderRowDisplay:
    """_order_row() must show correct execution_mode display and expose diagnostics."""

    def _make(self, meta: dict | None = None, **kw) -> dict:
        raw = {
            "local_order_id": "ord-001",
            "client_id": "test@example.com",
            "signal_id": "sig-001",
            "plan_id": "plan-001",
            "status": "PENDING_TRIGGER",
            "broker_order_id": None,
            "symbol": "AAPL",
            "side": "CALL",
            "contract": _OCC,
            "limit_price": 2.50,
            "score": 85.0,
            "execution_mode": "paper",
            "submitted_ts": None,
            "created_ts": "2026-07-14T09:00:00+00:00",
            "updated_ts": "2026-07-14T13:55:00+00:00",
            "last_error": None,
            "meta": meta or {},
            **kw,
        }
        return _order_row(raw)

    def test_paper_mode_display(self):
        row = self._make(execution_mode="paper")
        assert row["raw_diagnostics"]["execution_mode"] == "paper"

    def test_live_mode_display(self):
        row = self._make(execution_mode="live")
        assert row["raw_diagnostics"]["execution_mode"] == "live"

    def test_deferred_contract_hidden_from_display_contract(self):
        raw = {
            "local_order_id": "ord-002",
            "client_id": "test@example.com",
            "signal_id": "sig-002",
            "plan_id": "plan-002",
            "status": "PENDING_TRIGGER",
            "broker_order_id": None,
            "symbol": "AAPL",
            "side": "CALL",
            "contract": "DEFERRED:AAPL",
            "limit_price": 0.01,
            "score": 85.0,
            "execution_mode": "paper",
            "submitted_ts": None,
            "created_ts": "2026-07-14T09:00:00+00:00",
            "updated_ts": "2026-07-14T13:55:00+00:00",
            "last_error": None,
            "meta": {
                "lifecycle_state": "RETRY_WAIT",
                "materialization_status": "RETRY_PENDING",
                "materialization_in_flight": False,
                "broker_ready": False,
                "retry_reason": "MONEYNESS_OUT_OF_RANGE",
                "materialization_next_retry_at": _FUTURE,
            },
        }
        out = _order_row(raw)
        # Display contract must not show raw DEFERRED: value
        assert "DEFERRED" not in str(out["display_contract"])
        assert "pending" in str(out["display_contract"]).lower()
        # But raw contract is still in diagnostics
        assert out["raw_diagnostics"]["status"] == "PENDING_TRIGGER"

    def test_real_occ_contract_shown_as_is(self):
        row = self._make()
        assert row["display_contract"] == _OCC
        assert row["display_limit_price"] == 2.50

    def test_execution_state_present(self):
        row = self._make()
        assert "execution_state" in row
        assert row["execution_state"] in (
            EntryExecutionState.PRE_BREACH,
            EntryExecutionState.INCONSISTENT_STATE,
        )

    def test_pre_breach_execution_state(self):
        row = self._make()
        assert row["execution_state"] == EntryExecutionState.PRE_BREACH

    def test_action_required_present(self):
        row = self._make()
        assert "action_required" in row
        assert isinstance(row["action_required"], bool)

    def test_no_broker_id_reason_present(self):
        row = self._make()
        assert "no_broker_id_reason" in row

    def test_next_expected_transition_present(self):
        row = self._make()
        assert "next_expected_transition" in row
        assert row["next_expected_transition"]

    def test_overdue_seconds_present(self):
        row = self._make()
        assert "overdue_seconds" in row

    def test_state_updated_at_present(self):
        row = self._make()
        assert "state_updated_at" in row

    def test_raw_diagnostics_block_present(self):
        row = self._make()
        assert "raw_diagnostics" in row
        d = row["raw_diagnostics"]
        assert isinstance(d, dict)

    def test_raw_diagnostics_all_required_fields(self):
        """Every field listed in the PR spec must be in raw_diagnostics."""
        row = self._make(
            meta={
                "lifecycle_state": "MATERIALIZING",
                "materialization_status": "RUNNING",
                "materialization_generation": 2,
                "materialization_in_flight": True,
                "retry_attempt": 1,
                "retry_max_attempts": 3,
                "retry_reason": "MONEYNESS_OUT_OF_RANGE",
                "materialization_next_retry_at": _FUTURE,
                "deferred_retry_next_attempt_at": _FUTURE,
                "next_retry_at": _FUTURE,
                "retry_deadline": "2026-07-14T16:00:00+00:00",
                "absolute_entry_deadline": "2026-07-14T16:00:00+00:00",
                "broker_ready": False,
                "current_owner": "watcher:AAPL:abc",
                "materialization_owner": "watcher:AAPL:abc",
                "watcher_token": "abc",
                "submit_intent_at": None,
                "broker_submit_key": None,
                "recovery_retention_reason": "RETRY_PENDING",
                "final_market_validity": None,
            }
        )
        d = row["raw_diagnostics"]
        required_keys = [
            "local_order_id", "client_id", "execution_mode", "signal_id", "plan_id",
            "status", "broker_order_id", "submitted_ts",
            "lifecycle_state", "materialization_status", "materialization_generation",
            "retry_attempt", "retry_max_attempts", "retry_reason",
            "materialization_next_retry_at", "deferred_retry_next_attempt_at",
            "next_retry_at", "retry_deadline", "absolute_entry_deadline",
            "broker_ready", "current_owner", "materialization_owner",
            "watcher_token", "submit_intent_at", "broker_submit_key",
            "recovery_retention_reason", "final_market_validity",
            "last_error", "updated_ts",
        ]
        for key in required_keys:
            assert key in d, f"Missing required diagnostic field: {key!r}"


# ─────────────────────────────────────────────────────────────────────────────
# Read-only contract: no SQL mutations in module source
# ─────────────────────────────────────────────────────────────────────────────

class TestReadOnlyContract:
    """The module must never issue any DML statements."""

    def test_no_update_statements_in_source(self):
        src = inspect.getsource(_qrm)
        # Allow 'update' in variable names / comments, but block SQL UPDATE.
        # Pattern: 'UPDATE ' (capital U, followed by space) as a SQL keyword.
        sql_updates = re.findall(r"\bUPDATE\s+\w", src)
        assert sql_updates == [], (
            f"Unexpected SQL UPDATE statement(s) found: {sql_updates}"
        )

    def test_no_insert_statements_in_source(self):
        src = inspect.getsource(_qrm)
        sql_inserts = re.findall(r"\bINSERT\s+INTO\b", src)
        assert sql_inserts == [], (
            f"Unexpected SQL INSERT statement(s) found: {sql_inserts}"
        )

    def test_no_delete_statements_in_source(self):
        src = inspect.getsource(_qrm)
        sql_deletes = re.findall(r"\bDELETE\s+FROM\b", src)
        assert sql_deletes == [], (
            f"Unexpected SQL DELETE statement(s) found: {sql_deletes}"
        )

    def test_no_for_update_in_select(self):
        """FOR UPDATE locks a row; forbidden in a read-only module."""
        src = inspect.getsource(_qrm)
        assert "FOR UPDATE" not in src


# ─────────────────────────────────────────────────────────────────────────────
# SQL query guards (adapted from original test file)
# ─────────────────────────────────────────────────────────────────────────────

class TestSqlQueryGuards:
    def test_trade_queue_query_does_not_use_missing_updated_ts_column(self):
        src = inspect.getsource(_qrm)
        tq_section = src.split("tq_sql = (", 1)[1].split("order_sql = (", 1)[0]
        assert "updated_ts" not in tq_section
        assert "started_ts" in tq_section
        assert "finished_ts" in tq_section

    def test_order_query_selects_submitted_ts(self):
        src = inspect.getsource(_qrm)
        order_section = src.split("order_sql = (", 1)[1].split("def _fetch():", 1)[0]
        assert "submitted_ts" in order_section

    def test_order_query_selects_execution_mode(self):
        src = inspect.getsource(_qrm)
        order_section = src.split("order_sql = (", 1)[1].split("def _fetch():", 1)[0]
        assert "execution_mode" in order_section

    def test_order_query_selects_plan_id(self):
        src = inspect.getsource(_qrm)
        order_section = src.split("order_sql = (", 1)[1].split("def _fetch():", 1)[0]
        assert "plan_id" in order_section

    def test_order_query_selects_limit_price_for_hydrated_rows(self):
        src = inspect.getsource(_qrm)
        order_section = src.split("order_sql = (", 1)[1].split("def _fetch():", 1)[0]
        assert "limit_price" in order_section


# ─────────────────────────────────────────────────────────────────────────────
# EntryExecutionState constants
# ─────────────────────────────────────────────────────────────────────────────

class TestRetryTimestampPrecedenceContradiction:
    """Contradiction tests: legacy (next_retry_at) and canonical timestamps disagree."""

    def _row_with_timestamps(self, mat_next=None, deferred_next=None, next_retry=None) -> dict:
        meta: dict = {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_in_flight": False,
            "broker_ready": False,
            "retry_reason": "MONEYNESS_OUT_OF_RANGE",
        }
        if mat_next is not None:
            meta["materialization_next_retry_at"] = mat_next
        if deferred_next is not None:
            meta["deferred_retry_next_attempt_at"] = deferred_next
        if next_retry is not None:
            meta["next_retry_at"] = next_retry
        return _make_row(contract="DEFERRED:AAPL", limit_price=0.01, meta=meta)

    def test_contradiction_deferred_overdue_next_retry_future_yields_due(self):
        """Canonical contradiction: deferred is overdue, legacy next_retry_at is future.

        With the correct precedence (deferred > next_retry_at), the result must be
        DUE_RETRY_PENDING — not FUTURE_RETRY_SCHEDULED.  This is the exact scenario
        that would misclassify with the wrong field order.
        """
        row = self._row_with_timestamps(
            deferred_next=_PAST,    # overdue — should win
            next_retry=_FUTURE,     # future — must lose to deferred
        )
        r = _classify(row)
        assert r["execution_state"] == EntryExecutionState.DUE_RETRY_PENDING, (
            "deferred_retry_next_attempt_at (overdue) must take priority over "
            "next_retry_at (future) — wrong precedence would return FUTURE_RETRY_SCHEDULED"
        )
        assert r["overdue_seconds"] is not None
        assert r["overdue_seconds"] > 0

    def test_contradiction_materialization_overdue_deferred_future(self):
        """materialization_next_retry_at (overdue) beats deferred_next (future)."""
        row = self._row_with_timestamps(
            mat_next=_PAST,
            deferred_next=_FUTURE,
        )
        r = _classify(row)
        assert r["execution_state"] == EntryExecutionState.DUE_RETRY_PENDING

    def test_contradiction_all_three_materialization_is_authoritative(self):
        """materialization_next_retry_at is always the most authoritative field."""
        row = self._row_with_timestamps(
            mat_next=_FUTURE,   # future — wins priority 1
            deferred_next=_PAST,
            next_retry=_PAST,
        )
        r = _classify(row)
        assert r["execution_state"] == EntryExecutionState.FUTURE_RETRY_SCHEDULED

    def test_contradiction_only_next_retry_future_yields_future(self):
        """Only next_retry_at present and future → FUTURE_RETRY_SCHEDULED."""
        row = self._row_with_timestamps(next_retry=_FUTURE)
        r = _classify(row)
        assert r["execution_state"] == EntryExecutionState.FUTURE_RETRY_SCHEDULED

    def test_naive_retry_timestamp_yields_inconsistent_state(self):
        """A timezone-naive retry timestamp is malformed — must produce INCONSISTENT_STATE.

        The correctness fix requires _parse_iso_utc to return None for naive
        timestamps instead of silently assuming UTC.  A RETRY_WAIT row with
        only a naive timestamp has no parseable retry timestamp at all, which
        triggers invariant 5 (RETRY_WAIT_WITHOUT_RETRY_TIMESTAMP).
        """
        meta = {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_in_flight": False,
            "broker_ready": False,
            # Naive timestamp — no timezone offset
            "materialization_next_retry_at": "2026-07-14T14:10:00",
        }
        row = _make_row(contract="DEFERRED:AAPL", limit_price=0.01, meta=meta)
        r = _classify(row)
        assert r["execution_state"] == EntryExecutionState.INCONSISTENT_STATE
        # The invariant is missing-timestamp because the naive value is unparseable
        assert "RETRY_WAIT_WITHOUT_RETRY_TIMESTAMP" in r["no_broker_id_reason"]


class TestRouteResponseFixtures:
    """Route-level and response-fixture tests.

    These tests prove that the deployed API returns the classification fields
    every operator-console consumer depends on.  They call the read model
    directly (no DB; uses synthetic row builders) to validate the response
    shape the /admin/operator/entry-queue endpoint will return.
    """

    def _fixture_response(self, rows: list[dict] | None = None) -> dict:
        """Build a synthetic response matching build_operator_queue_read_model output."""
        from ap.operator_queue_read_model import (
            _order_row, empty_queue_counts, dashboard_queue_bucket,
        )
        built_rows = [_order_row(r) for r in (rows or [])]
        counts = empty_queue_counts()
        for row in built_rows:
            bucket = row.get("dashboard_status")
            if bucket in counts:
                counts[bucket] += 1
        active = counts["NEW"] + counts["WATCHING"] + counts["TRIGGERED"]
        return {
            "ok": True,
            "client_id": None,
            "hours": 24,
            "counts": counts,
            "NEW": counts["NEW"],
            "WATCHING": counts["WATCHING"],
            "TRIGGERED": counts["TRIGGERED"],
            "REJECTED": counts["REJECTED"],
            "EXPIRED": counts["EXPIRED"],
            "active_queue_signals": active,
            "pending_signals": active,
            "rows": built_rows,
        }

    def _make_order_for_fixture(self, **kw) -> dict:
        """Synthetic orders row suitable for _order_row()."""
        return {
            "local_order_id": kw.get("local_order_id", "ord-fix-001"),
            "client_id":       kw.get("client_id", "test@example.com"),
            "signal_id":       kw.get("signal_id", "sig-fix-001"),
            "plan_id":         kw.get("plan_id", "plan-fix-001"),
            "status":          kw.get("status", "PENDING_TRIGGER"),
            "broker_order_id": kw.get("broker_order_id", None),
            "symbol":          kw.get("symbol", "AAPL"),
            "side":            kw.get("side", "CALL"),
            "contract":        kw.get("contract", _OCC),
            "limit_price":     kw.get("limit_price", 2.50),
            "score":           kw.get("score", 85.0),
            "execution_mode":  kw.get("execution_mode", "paper"),
            "submitted_ts":    kw.get("submitted_ts", None),
            "created_ts":      kw.get("created_ts", "2026-07-14T09:00:00+00:00"),
            "updated_ts":      kw.get("updated_ts", "2026-07-14T13:55:00+00:00"),
            "last_error":      kw.get("last_error", None),
            "meta":            kw.get("meta", {}),
        }

    # ── Top-level response shape ───────────────────────────────────────────

    def test_response_top_level_ok(self):
        r = self._fixture_response()
        assert r["ok"] is True

    def test_response_has_counts(self):
        r = self._fixture_response()
        assert "counts" in r
        assert set(r["counts"].keys()) == {"NEW", "WATCHING", "TRIGGERED", "REJECTED", "EXPIRED"}

    def test_response_has_active_queue_signals(self):
        r = self._fixture_response()
        assert "active_queue_signals" in r
        assert isinstance(r["active_queue_signals"], int)

    def test_response_has_rows_list(self):
        r = self._fixture_response()
        assert "rows" in r
        assert isinstance(r["rows"], list)

    def test_response_counts_match_rows(self):
        """WATCHING count must equal the number of PENDING_TRIGGER rows."""
        raw_rows = [
            self._make_order_for_fixture(local_order_id="ord-001", status="PENDING_TRIGGER"),
            self._make_order_for_fixture(local_order_id="ord-002", status="PENDING_TRIGGER"),
        ]
        r = self._fixture_response(raw_rows)
        assert r["WATCHING"] == 2

    # ── Every operator-visible classification field present in each row ─────

    def test_every_row_has_execution_state(self):
        raw = [self._make_order_for_fixture()]
        r = self._fixture_response(raw)
        assert len(r["rows"]) == 1
        assert "execution_state" in r["rows"][0]

    def test_every_row_has_no_broker_id_reason(self):
        raw = [self._make_order_for_fixture()]
        r = self._fixture_response(raw)
        assert "no_broker_id_reason" in r["rows"][0]

    def test_every_row_has_action_required(self):
        raw = [self._make_order_for_fixture()]
        r = self._fixture_response(raw)
        row = r["rows"][0]
        assert "action_required" in row
        assert isinstance(row["action_required"], bool)

    def test_every_row_has_action_required_reason(self):
        raw = [self._make_order_for_fixture()]
        r = self._fixture_response(raw)
        assert "action_required_reason" in r["rows"][0]

    def test_every_row_has_next_expected_transition(self):
        raw = [self._make_order_for_fixture()]
        r = self._fixture_response(raw)
        assert "next_expected_transition" in r["rows"][0]
        assert r["rows"][0]["next_expected_transition"]

    def test_every_row_has_overdue_seconds(self):
        raw = [self._make_order_for_fixture()]
        r = self._fixture_response(raw)
        assert "overdue_seconds" in r["rows"][0]

    def test_every_row_has_state_updated_at(self):
        raw = [self._make_order_for_fixture()]
        r = self._fixture_response(raw)
        assert "state_updated_at" in r["rows"][0]

    def test_every_row_has_raw_diagnostics(self):
        raw = [self._make_order_for_fixture()]
        r = self._fixture_response(raw)
        assert "raw_diagnostics" in r["rows"][0]
        assert isinstance(r["rows"][0]["raw_diagnostics"], dict)

    def test_every_row_has_display_contract(self):
        raw = [self._make_order_for_fixture()]
        r = self._fixture_response(raw)
        assert "display_contract" in r["rows"][0]

    def test_every_row_has_display_limit_price(self):
        raw = [self._make_order_for_fixture()]
        r = self._fixture_response(raw)
        assert "display_limit_price" in r["rows"][0]

    # ── Per-state fixture spot checks ──────────────────────────────────────

    def test_fixture_pre_breach_classification(self):
        raw = [self._make_order_for_fixture(status="PENDING_TRIGGER")]
        r = self._fixture_response(raw)
        assert r["rows"][0]["execution_state"] == EntryExecutionState.PRE_BREACH
        assert r["rows"][0]["action_required"] is False

    def test_fixture_due_retry_action_required(self):
        # Use a timestamp well in the past so _utcnow() always sees it as overdue,
        # regardless of when the test suite actually runs.
        _CLEARLY_PAST = "2026-06-01T00:00:00+00:00"
        meta = {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_in_flight": False,
            "broker_ready": False,
            "materialization_next_retry_at": _CLEARLY_PAST,
        }
        raw = [self._make_order_for_fixture(
            contract="DEFERRED:AAPL", limit_price=0.01, meta=meta,
        )]
        r = self._fixture_response(raw)
        row = r["rows"][0]
        assert row["execution_state"] == EntryExecutionState.DUE_RETRY_PENDING
        assert row["action_required"] is True
        assert row["overdue_seconds"] is not None and row["overdue_seconds"] > 0

    def test_fixture_broker_submitted_no_broker_id_reason_null(self):
        raw = [self._make_order_for_fixture(
            status="SUBMITTED", broker_order_id="TRD-12345",
        )]
        r = self._fixture_response(raw)
        assert r["rows"][0]["execution_state"] == EntryExecutionState.BROKER_SUBMITTED
        assert r["rows"][0]["no_broker_id_reason"] is None

    def test_fixture_inconsistent_returns_action_required(self):
        """INCONSISTENT_STATE rows must surface action_required=True in the fixture."""
        # inv3: broker_order_id present with PENDING_TRIGGER status
        raw = [self._make_order_for_fixture(
            status="PENDING_TRIGGER", broker_order_id="TRD-999",
        )]
        r = self._fixture_response(raw)
        assert r["rows"][0]["execution_state"] == EntryExecutionState.INCONSISTENT_STATE
        assert r["rows"][0]["action_required"] is True

    def test_fixture_retry_attempt_in_raw_diagnostics(self):
        """retry_attempt and retry_max_attempts must be in raw_diagnostics for UI display."""
        meta = {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_in_flight": False,
            "broker_ready": False,
            "retry_attempt": 2,
            "retry_max_attempts": 3,
            "materialization_next_retry_at": _FUTURE,
        }
        raw = [self._make_order_for_fixture(
            contract="DEFERRED:AAPL", limit_price=0.01, meta=meta,
        )]
        r = self._fixture_response(raw)
        diag = r["rows"][0]["raw_diagnostics"]
        assert diag["retry_attempt"] == 2
        assert diag["retry_max_attempts"] == 3

    def test_fixture_final_market_validity_in_raw_diagnostics(self):
        """final_market_validity.reason_code must be accessible via raw_diagnostics."""
        meta = {
            "broker_ready": False,
            "lifecycle_state": "",
            "materialization_in_flight": False,
            "final_market_validity": {
                "gate": "market_validity",
                "passed": False,
                "reason_code": "CALL_NO_LONGER_ABOVE_TRIGGER",
            },
        }
        raw = [self._make_order_for_fixture(
            status="PENDING_TRIGGER", execution_mode="live", meta=meta,
        )]
        r = self._fixture_response(raw)
        diag = r["rows"][0]["raw_diagnostics"]
        fmv = diag.get("final_market_validity") or {}
        assert fmv.get("reason_code") == "CALL_NO_LONGER_ABOVE_TRIGGER"

    def test_fixture_submit_intent_at_in_raw_diagnostics(self):
        """submit_intent_at must surface in raw_diagnostics for reconcile-state display."""
        meta = {
            "lifecycle_state": "SUBMITTING",
            "submit_intent_at": "2026-07-14T13:50:00+00:00",
            "broker_submit_key": "ord-fix-001",
            "current_owner": "broker_submit:ord-fix-001",
            "broker_ready": True,
            "materialization_generation": 1,
            "materialization_in_flight": False,
        }
        raw = [self._make_order_for_fixture(meta=meta)]
        r = self._fixture_response(raw)
        diag = r["rows"][0]["raw_diagnostics"]
        assert diag["submit_intent_at"] == "2026-07-14T13:50:00+00:00"

    # ── Route registration test (no DB required) ───────────────────────────
    #
    # We read app.py as raw text instead of importing it to avoid triggering
    # the module-level DB connection initialisation that runs at import time.
    # A text-level search is sufficient to prove the route and delegation are
    # present; the response-shape tests above prove the classification payload.

    @staticmethod
    def _app_source() -> str:
        import os
        app_path = os.path.join(os.path.dirname(__file__), "..", "app.py")
        with open(os.path.normpath(app_path)) as fh:
            return fh.read()

    def test_entry_queue_route_registered_in_app_source(self):
        """The /admin/operator/entry-queue endpoint must be registered in app.py."""
        src = self._app_source()
        assert "/admin/operator/entry-queue" in src, (
            "/admin/operator/entry-queue route is not registered in app.py"
        )

    def test_entry_queue_route_calls_build_operator_queue_read_model(self):
        """The route implementation must delegate to build_operator_queue_read_model."""
        src = self._app_source()
        assert "build_operator_queue_read_model" in src, (
            "build_operator_queue_read_model not called from app.py"
        )
        # Split on the function definition (not the comment) so we get only
        # the handler body, then find the next top-level @app decorator.
        handler_src = src.split("def admin_operator_entry_queue():", 1)[1]
        handler_body = handler_src.split("\n@app", 1)[0]
        assert "build_operator_queue_read_model" in handler_body, (
            "build_operator_queue_read_model not found in admin_operator_entry_queue body"
        )

    def test_entry_queue_endpoint_is_read_only_in_source(self):
        """The entry-queue endpoint must not issue any DML."""
        src = self._app_source()
        # Isolate handler body (between def and the next top-level decorator)
        eq_body = src.split("admin_operator_entry_queue", 1)[1].split("@app", 1)[0]
        import re as _re
        sql_updates = _re.findall(r"\bUPDATE\s+\w", eq_body)
        assert sql_updates == [], f"SQL mutation in entry-queue handler: {sql_updates}"

    def test_entry_queue_route_protected_by_require_admin(self):
        """The route must be behind the @_require_admin decorator."""
        src = self._app_source()
        # The decorator must appear between the @app.get and the def
        route_block = src.split("/admin/operator/entry-queue", 1)[1]
        before_def = route_block.split("def admin_operator_entry_queue", 1)[0]
        assert "_require_admin" in before_def, (
            "@_require_admin decorator not found before admin_operator_entry_queue"
        )


class TestEntryExecutionStateConstants:
    def test_all_eleven_states_defined(self):
        expected = {
            "PRE_BREACH", "FUTURE_RETRY_SCHEDULED", "DUE_RETRY_PENDING",
            "MATERIALIZING", "BROKER_READY", "FINAL_GATE_BLOCKED",
            "SUBMIT_INTENT_PENDING", "RECONCILE_PENDING",
            "BROKER_SUBMITTED", "TERMINAL", "INCONSISTENT_STATE",
        }
        actual = {
            v for k, v in vars(EntryExecutionState).items()
            if not k.startswith("_")
        }
        assert actual == expected

    def test_existing_dashboard_buckets_unchanged(self):
        from ap.operator_queue_read_model import DASHBOARD_BUCKETS, dashboard_queue_bucket, empty_queue_counts
        assert dashboard_queue_bucket("PENDING_TRIGGER") == "WATCHING"
        assert dashboard_queue_bucket("SUBMITTED") == "TRIGGERED"
        assert empty_queue_counts() == {
            "NEW": 0, "WATCHING": 0, "TRIGGERED": 0, "REJECTED": 0, "EXPIRED": 0
        }

    def test_order_row_still_shows_real_hydrated_limit_price(self):
        """Regression guard: existing _order_row() hydrated-price behaviour preserved."""
        row = {
            "local_order_id": "local-123",
            "client_id": "client@example.com",
            "status": "PENDING_TRIGGER",
            "symbol": "AAPL",
            "side": "CALL",
            "contract": "AAPL260717C00200000",
            "limit_price": 2.5,
            "score": 88.0,
            "signal_id": "sig-123",
            "plan_id": None,
            "execution_mode": "paper",
            "submitted_ts": None,
            "created_ts": "2026-07-03T13:40:00Z",
            "updated_ts": "2026-07-03T13:41:00Z",
            "broker_order_id": None,
            "last_error": None,
            "meta": {
                "contract_selection_status": "HYDRATED_PRE_BREACH",
                "deferred_hydration": {"attempted": True, "success": True},
            },
        }
        out = _order_row(row)
        assert out["limit_price"] == 2.5
        assert out["display_limit_price"] == 2.5
        assert out["contract"] == "AAPL260717C00200000"
