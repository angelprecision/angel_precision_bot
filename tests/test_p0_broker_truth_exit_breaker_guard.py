from __future__ import annotations

from ap.exit_circuit_breaker_broker_truth_guard import (
    FLAT_REASON,
    _is_broker_truth_repair_context,
    apply_broker_truth_exit_breaker_bypass,
)


def _breaker_result():
    return {
        "blocked": True,
        "reason": "exit_circuit_breaker_tripped",
        "position_state": {"blocked": False, "quantity_remaining": 1},
        "circuit_breaker": {
            "blocked": True,
            "reason": "exit_circuit_breaker_tripped",
            "rejection_count": 5,
            "threshold": 5,
        },
    }


def test_broker_truth_repair_bypasses_exit_circuit_breaker():
    result = apply_broker_truth_exit_breaker_bypass(
        _breaker_result(),
        {
            "position_id": "broker-repair-client@example.com-VZ260717C00040000",
            "client_id": "client@example.com",
            "execution_mode": "live",
            "contract": "VZ260717C00040000",
            "broker_truth_open_qty": 1,
            "allow_missing_position_with_broker_truth": True,
        },
    )
    assert result["blocked"] is False
    assert result["reason"] is None
    assert result["p0_broker_truth_circuit_breaker_bypass"] is True
    assert result["p0_broker_truth_open_qty"] == 1


def test_broker_truth_guard_does_not_bypass_normal_position():
    result = apply_broker_truth_exit_breaker_bypass(
        _breaker_result(),
        {
            "position_id": "pos-normal",
            "client_id": "client@example.com",
            "execution_mode": "live",
            "contract": "VZ260717C00040000",
            "broker_truth_open_qty": 1,
            "allow_missing_position_with_broker_truth": False,
        },
    )
    assert result["blocked"] is True
    assert result["reason"] == "exit_circuit_breaker_tripped"


def test_broker_truth_repair_qty_zero_marks_flat_not_breaker_loop():
    result = apply_broker_truth_exit_breaker_bypass(
        _breaker_result(),
        {
            "position_id": "broker-repair-client@example.com-VZ260717C00040000",
            "client_id": "client@example.com",
            "execution_mode": "live",
            "contract": "VZ260717C00040000",
            "broker_truth_open_qty": 0,
            "allow_missing_position_with_broker_truth": True,
        },
    )
    assert result["blocked"] is True
    assert result["reason"] == FLAT_REASON
    assert result["p0_broker_truth_position_flat"] is True
    assert result["p0_broker_truth_open_qty"] == 0


def test_broker_truth_repair_context_classifier():
    assert _is_broker_truth_repair_context({"position_id": "pos-normal", "allow_missing_position_with_broker_truth": False}) is False
    assert _is_broker_truth_repair_context({"position_id": "broker-repair-client@example.com-XYZ", "allow_missing_position_with_broker_truth": False}) is True
    assert _is_broker_truth_repair_context({"position_id": "pos-normal", "allow_missing_position_with_broker_truth": True}) is True
