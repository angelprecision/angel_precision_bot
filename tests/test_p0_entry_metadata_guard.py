from __future__ import annotations

import json
from copy import deepcopy
from types import SimpleNamespace

import pytest

from ap.entry_metadata_guard import (
    MISSING_PATTERN,
    MISSING_SIGNAL_ID,
    UNKNOWN_EXECUTION_MODE,
    ZERO_SCORE,
    ZERO_TRIGGER,
    ZERO_UNDERLYING,
    validate_entry_metadata,
)


def _shaped_signal(**overrides):
    signal = {
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "signal_id": "sig-wfc-call-1d",
        "canonical_signal_id": "WFC:CALL:1d:2026-06-26",
        "ticker": "WFC",
        "symbol": "WFC",
        "side": "CALL",
        "direction": "CALL",
        "timeframe": "1d",
        "pattern": "1-2-2U",
        "pattern_id": "1-2-2U:WFC:1d",
        "score": 82.4,
        "entry_trigger": 77.62,
        "underlying_entry": 77.88,
        "target_price": 81.50,
        "stop_price": 75.10,
    }
    signal.update(overrides)
    return signal


def _queue_payload_without_mode(**overrides):
    signal = _shaped_signal(**overrides)
    signal.pop("execution_mode", None)
    return signal


def _install_or_skip():
    try:
        import ap
        ap.install_entry_metadata_safety_guards()
        return ap
    except Exception as exc:
        pytest.skip(f"runtime guard install unavailable in minimal CI: {type(exc).__name__}: {exc}")


def test_score_zero_blocks():
    result = validate_entry_metadata(plan=_shaped_signal(score=0))
    assert not result.ok
    assert result.reason == ZERO_SCORE


def test_pattern_blank_blocks():
    result = validate_entry_metadata(plan=_shaped_signal(pattern="", pattern_id=""))
    assert not result.ok
    assert result.reason == MISSING_PATTERN


def test_trigger_zero_blocks():
    result = validate_entry_metadata(plan=_shaped_signal(entry_trigger=0))
    assert not result.ok
    assert result.reason == ZERO_TRIGGER


def test_underlying_zero_blocks():
    result = validate_entry_metadata(plan=_shaped_signal(underlying_entry=0))
    assert not result.ok
    assert result.reason == ZERO_UNDERLYING


def test_execution_mode_unknown_blocks():
    result = validate_entry_metadata(plan=_shaped_signal(execution_mode="unknown"))
    assert not result.ok
    assert result.reason == UNKNOWN_EXECUTION_MODE


def test_missing_signal_id_blocks_when_canonical_missing_too():
    result = validate_entry_metadata(plan=_shaped_signal(signal_id="", canonical_signal_id=""))
    assert not result.ok
    assert result.reason == MISSING_SIGNAL_ID


def test_fully_shaped_signal_passes_unchanged():
    signal = _shaped_signal()
    before = deepcopy(signal)
    result = validate_entry_metadata(plan=signal)
    assert result.ok
    assert result.reason is None
    assert signal == before


def test_nested_daily_trigger_stop_target_shape_passes_unchanged():
    signal = _shaped_signal(entry_trigger=None, target_price=None, stop_price=None, trigger={"entry": 77.62, "stop": 75.10, "pt1": 81.50})
    before = deepcopy(signal)
    result = validate_entry_metadata(plan=signal)
    assert result.ok
    assert result.reason is None
    assert signal == before


def test_real_queue_payload_shape_passes_when_runtime_mode_is_valid():
    payload = _queue_payload_without_mode()
    result = validate_entry_metadata(plan=payload, client_id="jasoncosby1@gmail.com", execution_mode="paper")
    assert result.ok
    assert result.reason is None


def test_real_queue_payload_shape_still_blocks_without_safe_runtime_mode():
    payload = _queue_payload_without_mode()
    result = validate_entry_metadata(plan=payload, client_id="jasoncosby1@gmail.com", execution_mode=None)
    assert not result.ok
    assert result.reason == UNKNOWN_EXECUTION_MODE


def test_plan_mode_passes_without_execution_mode_attr():
    plan = SimpleNamespace(**_queue_payload_without_mode(mode="PAPER"))
    result = validate_entry_metadata(plan=plan, client_id="jasoncosby1@gmail.com")
    assert result.ok


def test_guard_installed_on_master_control_and_osm_classes():
    _install_or_skip()
    from ap.order_state_machine import APOrderStateMachine
    from ap_master_control import APMasterControl

    assert getattr(APMasterControl, "_entry_metadata_guard_installed", False) is True
    assert getattr(APOrderStateMachine, "_entry_metadata_guard_installed", False) is True


def test_master_control_uses_valid_runtime_mode_before_contract_selection():
    _install_or_skip()
    from ap_master_control import APMasterControl

    mc = APMasterControl(mode="paper", client_id="jasoncosby1@gmail.com")
    decision = mc.evaluate(_queue_payload_without_mode(score=0), client_id="jasoncosby1@gmail.com")

    assert decision.ok is False
    assert decision.stage == "metadata_validation"
    assert decision.reason == ZERO_SCORE


def test_submit_existing_entry_blocks_before_broker_submit_or_order_mutation(monkeypatch):
    _install_or_skip()
    from ap.order_state_machine import APOrderStateMachine, OrderStatus

    osm = APOrderStateMachine("jasoncosby1@gmail.com")
    order_row = {
        "local_order_id": "local-1",
        "client_id": "jasoncosby1@gmail.com",
        "kind": "ENTRY",
        "status": OrderStatus.PENDING_TRIGGER,
        "signal_id": "sig-wfc-call-1d",
        "canonical_signal_id": "WFC:CALL:1d:2026-06-26",
        "symbol": "WFC",
        "contract": "WFC260717C00080000",
        "direction": "CALL",
        "execution_mode": "paper",
        "score": 0,
        "trigger_price": 77.62,
        "stop_underlying": 75.10,
        "target_underlying": 81.50,
        "pattern": "1-2-2U",
        "timeframe": "1d",
        "limit_price": 1.25,
        "qty": 1,
        "meta": json.dumps({"underlying_entry": 77.88}),
    }

    monkeypatch.setattr(osm, "_get_order", lambda local_order_id: order_row)

    def _should_not_submit(*args, **kwargs):
        raise AssertionError("broker submit must not be called on invalid metadata")

    def _should_not_transition(*args, **kwargs):
        raise AssertionError("metadata guard must not mutate the order row")

    monkeypatch.setattr(osm, "_submit_order_with_retry", _should_not_submit)
    monkeypatch.setattr(osm, "transition", _should_not_transition)

    result = osm.submit_existing_entry(local_order_id="local-1", broker=object(), plan=None, limit_price=1.25)

    assert result["ok"] is False
    assert result["metadata_blocked"] is True
    assert result["error"] == ZERO_SCORE
    assert result["status"] == OrderStatus.PENDING_TRIGGER
