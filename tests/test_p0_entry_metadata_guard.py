from __future__ import annotations

import json
from copy import deepcopy

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
    result = validate_entry_metadata(
        plan=_shaped_signal(signal_id="", canonical_signal_id="")
    )

    assert not result.ok
    assert result.reason == MISSING_SIGNAL_ID


def test_fully_shaped_signal_passes_unchanged():
    signal = _shaped_signal()
    before = deepcopy(signal)

    result = validate_entry_metadata(plan=signal)

    assert result.ok
    assert result.reason is None
    assert signal == before


def test_guard_installed_on_osm_class():
    from ap.order_state_machine import APOrderStateMachine

    assert getattr(APOrderStateMachine, "_entry_metadata_guard_installed", False) is True


def test_submit_existing_entry_blocks_before_broker_submit_or_order_mutation(monkeypatch):
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

    result = osm.submit_existing_entry(
        local_order_id="local-1",
        broker=object(),
        plan=None,
        limit_price=1.25,
    )

    assert result["ok"] is False
    assert result["metadata_blocked"] is True
    assert result["error"] == ZERO_SCORE
    assert result["status"] == OrderStatus.PENDING_TRIGGER
