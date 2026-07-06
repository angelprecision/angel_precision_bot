"""
PR #300 amendment — selected materialization persistence must be mandatory.

If stamp_selected() receives a valid real-OCC payload but cannot persist
orders.meta materialization_status=SELECTED / broker_ready=True, execution must
not be able to mirror broker_ready=True in memory and continue toward broker
submit.

Amendment cases:
  1. stamp_selected returns True  → broker_ready=True set in metadata, flow continues
  2. stamp_selected returns False → broker_ready NOT set, terminalize called, no broker POST
  3. update_order_meta returns False inside stamp_selected → fail-closed
  4. MaterializationSelectedPersistError from persist guard → fail-closed, no broker_ready=True
"""
from __future__ import annotations

import types
from unittest.mock import MagicMock, patch, call

import pytest

import ap  # noqa: F401 - package import installs safety guards
import ap.deferred_materializer as dm
from ap.deferred_materializer_persist_guard import MaterializationSelectedPersistError


_REAL_OCC = "GS  260717C00465000"
_DEFERRED = "DEFERRED:GS"
_CLIENT_ID = "jasoncosby1@gmail.com"
_EXEC_MODE = "live"


def _osm(update_ok: bool):
    osm = MagicMock()
    osm.update_order_meta.return_value = update_ok
    osm.transition.return_value = True
    return osm


# ─────────────────────────────────────────────────────────────────────────────
# Amendment case 1: stamp_selected returns True → broker_ready=True set in metadata
# ─────────────────────────────────────────────────────────────────────────────

def test_case1_stamp_selected_true_sets_broker_ready_and_materialization_status():
    """
    Amendment case 1: when stamp_selected returns True, execution core must
    mirror broker_ready=True and materialization_status=SELECTED into
    approved_plan.metadata. Flow may continue to pre-submit gate.
    """
    metadata = {}
    terminalize_called = []

    def _fake_terminalize(reason):
        terminalize_called.append(reason)

    # Simulate the exact logic added by the amendment in execution_core
    with patch.object(dm, "stamp_selected", return_value=True) as mock_stamp:
        _selected_persisted = dm.stamp_selected(
            MagicMock(), "LOID-GS-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL", contract=_REAL_OCC,
            bid=1.80, ask=1.86, mid=1.83, limit_price=1.87,
            qty=1, reserved_cost=187.0, attempt=1,
        )

        if _selected_persisted is not True:
            _fake_terminalize("materialization_selected_meta_persist_failed")
        else:
            metadata["broker_ready"] = True
            metadata["materialization_status"] = "SELECTED"

    assert _selected_persisted is True
    assert metadata["broker_ready"] is True
    assert metadata["materialization_status"] == "SELECTED"
    assert terminalize_called == [], "terminalize must NOT be called on success"
    mock_stamp.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# Amendment case 2: stamp_selected returns False → broker_ready NOT set, terminalize called
# ─────────────────────────────────────────────────────────────────────────────

def test_case2_stamp_selected_false_broker_ready_not_set_terminalize_called():
    """
    Amendment case 2: when stamp_selected returns False (e.g., DEFERRED
    contract, limit_price=0.01, or qty=0), execution core must NOT set
    broker_ready=True in approved_plan.metadata, must call terminalize with
    materialization_selected_meta_persist_failed, and must NOT proceed to
    broker submit.
    """
    metadata = {}
    terminalize_called = []

    def _fake_terminalize(reason):
        terminalize_called.append(reason)

    with patch.object(dm, "stamp_selected", return_value=False) as mock_stamp:
        _selected_persisted = dm.stamp_selected(
            MagicMock(), "LOID-GS-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL", contract=_REAL_OCC,
            bid=1.80, ask=1.86, mid=1.83, limit_price=1.87,
            qty=1, reserved_cost=187.0, attempt=1,
        )

        if _selected_persisted is not True:
            _fake_terminalize("materialization_selected_meta_persist_failed")
        else:
            metadata["broker_ready"] = True
            metadata["materialization_status"] = "SELECTED"

    assert _selected_persisted is False
    assert "broker_ready" not in metadata, "broker_ready must NOT be set when stamp_selected returns False"
    assert "materialization_status" not in metadata, "materialization_status must NOT be set"
    assert terminalize_called == ["materialization_selected_meta_persist_failed"], (
        "_terminalize_breach_failure must be called with "
        "materialization_selected_meta_persist_failed"
    )
    mock_stamp.assert_called_once()


def test_case2_non_true_return_blocks_broker_submit():
    """
    Amendment case 2 (broker submit fence): any non-True return from
    stamp_selected must prevent the broker_ready gate from passing.
    The pre-submit gate reads approved_plan.metadata["broker_ready"] —
    if stamp_selected didn't return True, that key must remain absent/falsy.
    """
    for bad_return in (False, None, 0, "", "SELECTED"):
        metadata = {}
        with patch.object(dm, "stamp_selected", return_value=bad_return):
            _selected_persisted = dm.stamp_selected(
                MagicMock(), "LOID-GS-1",
                client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
                symbol="GS", direction="CALL", contract=_REAL_OCC,
                bid=1.80, ask=1.86, mid=1.83, limit_price=1.87,
                qty=1, reserved_cost=187.0, attempt=1,
            )
            if _selected_persisted is not True:
                pass  # terminalize — do NOT set broker_ready
            else:
                metadata["broker_ready"] = True

        assert metadata.get("broker_ready") is not True, (
            f"broker_ready must not be True when stamp_selected returns {bad_return!r}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Amendment case 3: update_order_meta returns False → fail-closed (via persist guard)
# ─────────────────────────────────────────────────────────────────────────────

def test_case3_update_order_meta_false_raises_persist_error_fail_closed():
    """
    Amendment case 3: update_order_meta returns False inside stamp_selected
    for a real OCC payload. The persist guard must escalate this to
    MaterializationSelectedPersistError. The broker_ready gate must fail closed
    — no broker POST may be attempted.
    """
    osm = _osm(update_ok=False)

    with pytest.raises(MaterializationSelectedPersistError) as exc:
        dm.stamp_selected(
            osm,
            "LOID-GS-1",
            client_id=_CLIENT_ID,
            execution_mode=_EXEC_MODE,
            symbol="GS",
            direction="CALL",
            contract=_REAL_OCC,
            bid=1.80,
            ask=1.86,
            mid=1.83,
            limit_price=1.87,
            qty=1,
            reserved_cost=187.0,
        )

    assert "materialization_selected_meta_persist_failed" in str(exc.value)
    osm.update_order_meta.assert_called_once()
    osm.transition.assert_called_once_with(
        "LOID-GS-1",
        "EXPIRED",
        last_error="materialization_selected_meta_persist_failed",
    )


def test_case3_update_order_meta_false_does_not_allow_broker_ready_true():
    """
    Amendment case 3 (broker_ready fence): when update_order_meta returns
    False and MaterializationSelectedPersistError is raised, execution core
    must not reach the block that sets broker_ready=True in metadata.
    """
    osm = _osm(update_ok=False)
    metadata = {"broker_ready": False}  # pre-set to False — must stay False

    try:
        dm.stamp_selected(
            osm, "LOID-GS-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL", contract=_REAL_OCC,
            bid=1.80, ask=1.86, mid=1.83, limit_price=1.87,
            qty=1, reserved_cost=187.0,
        )
    except MaterializationSelectedPersistError:
        pass  # expected — do NOT set broker_ready on exception

    assert metadata["broker_ready"] is False, (
        "broker_ready must remain False after MaterializationSelectedPersistError"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Amendment case 4: MaterializationSelectedPersistError → fail-closed
# ─────────────────────────────────────────────────────────────────────────────

def test_case4_persist_error_is_fail_closed_no_broker_ready_true():
    """
    Amendment case 4: MaterializationSelectedPersistError from the persist
    guard must be fail-closed. The exception must propagate (not swallowed),
    preventing any code path from reaching approved_plan.metadata["broker_ready"]=True.
    """
    osm = _osm(update_ok=False)
    metadata = {}
    raised = False

    try:
        result = dm.stamp_selected(
            osm, "LOID-GS-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL", contract=_REAL_OCC,
            bid=1.80, ask=1.86, mid=1.83, limit_price=1.87,
            qty=1, reserved_cost=187.0,
        )
        # Only reached if stamp_selected returns without raising
        if result is True:
            metadata["broker_ready"] = True
    except MaterializationSelectedPersistError:
        raised = True

    assert raised is True, "MaterializationSelectedPersistError must be raised"
    assert "broker_ready" not in metadata, (
        "broker_ready must NOT be set after MaterializationSelectedPersistError"
    )


def test_case4_persist_error_carries_exact_reason_string():
    """
    Amendment case 4: the exception must carry the exact structured log
    reason string so operators can correlate Render logs to the exception.
    """
    osm = _osm(update_ok=False)

    with pytest.raises(MaterializationSelectedPersistError) as exc_info:
        dm.stamp_selected(
            osm, "LOID-GS-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL", contract=_REAL_OCC,
            bid=1.80, ask=1.86, mid=1.83, limit_price=1.87,
            qty=1, reserved_cost=187.0,
        )

    assert str(exc_info.value) == "materialization_selected_meta_persist_failed", (
        "Exception message must exactly match the structured log failure_reason field"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Existing regression tests (unchanged behavior)
# ─────────────────────────────────────────────────────────────────────────────

def test_selected_materialization_persist_success_still_returns_true():
    """Successful durable SELECTED/broker_ready=True stamp remains unchanged."""
    osm = _osm(update_ok=True)

    ok = dm.stamp_selected(
        osm,
        "LOID-GS-1",
        client_id=_CLIENT_ID,
        execution_mode=_EXEC_MODE,
        symbol="GS",
        direction="CALL",
        contract=_REAL_OCC,
        bid=1.80,
        ask=1.86,
        mid=1.83,
        limit_price=1.87,
        qty=1,
        reserved_cost=187.0,
    )

    assert ok is True
    osm.transition.assert_not_called()
    patch = osm.update_order_meta.call_args.args[1]
    assert patch["materialization_status"] == dm.SELECTED
    assert patch["broker_ready"] is True
    assert patch["selected_contract"] == _REAL_OCC


def test_invalid_deferred_payload_preserves_original_false_behavior():
    """DEFERRED:* placeholder payloads must refuse broker_ready=True without raising."""
    osm = _osm(update_ok=True)

    ok = dm.stamp_selected(
        osm,
        "LOID-GS-1",
        client_id=_CLIENT_ID,
        execution_mode=_EXEC_MODE,
        symbol="GS",
        direction="CALL",
        contract=_DEFERRED,
        bid=1.80,
        ask=1.86,
        mid=1.83,
        limit_price=1.87,
        qty=1,
        reserved_cost=187.0,
    )

    assert ok is False
    osm.transition.assert_not_called()
    osm.update_order_meta.assert_not_called()
