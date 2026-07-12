from __future__ import annotations

import json
from copy import deepcopy
from types import SimpleNamespace

import pytest

from ap.entry_metadata_guard import (
    ENTRY_GEOMETRY_CALL_INVALID,
    ENTRY_GEOMETRY_CHANGED_AFTER_ARM,
    ENTRY_GEOMETRY_CONFLICT,
    ENTRY_GEOMETRY_MISSING,
    ENTRY_GEOMETRY_NONNUMERIC,
    ENTRY_GEOMETRY_PUT_INVALID,
    ENTRY_GEOMETRY_STOP_EQUALS_TARGET,
    ENTRY_GEOMETRY_STOP_EQUALS_TRIGGER,
    ENTRY_GEOMETRY_TARGET_EQUALS_TRIGGER,
    ENTRY_PATTERN_SIDE_CONFLICT,
    LIVE_FAILED_DIRECTION_PATTERN_BLOCKED,
    LIVE_TIMEFRAME_NOT_ALLOWED,
    MISSING_SIGNAL_ID,
    MISSING_TIMEFRAME,
    UNKNOWN_EXECUTION_MODE,
    ZERO_SCORE,
    ZERO_TRIGGER,
    ZERO_UNDERLYING,
    _allow_deferred_overnight_watcher_create,
    _mark_deferred_watcher_data_pending,
    validate_entry_metadata,
    validate_entry_strategy_truth,
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


def test_pattern_blank_is_non_blocking():
    result = validate_entry_metadata(plan=_shaped_signal(pattern="", pattern_id=""))
    assert result.ok
    assert result.reason is None


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


def test_strategy_truth_valid_call_passes():
    result = validate_entry_strategy_truth(plan=_shaped_signal())
    assert result.ok
    assert result.details["trigger"] == 77.62
    assert result.details["stop"] == 75.10
    assert result.details["target"] == 81.50


def test_strategy_truth_valid_put_passes():
    result = validate_entry_strategy_truth(plan=_shaped_signal(
        side="PUT",
        direction="PUT",
        pattern="1-2_2D",
        entry_trigger=77.62,
        stop_price=80.10,
        target_price=74.50,
    ))
    assert result.ok


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"target_price": 76.50}, ENTRY_GEOMETRY_CALL_INVALID),
        ({"stop_price": 78.10}, ENTRY_GEOMETRY_CALL_INVALID),
        ({"stop_price": 77.62}, ENTRY_GEOMETRY_STOP_EQUALS_TRIGGER),
        ({"target_price": 77.62}, ENTRY_GEOMETRY_TARGET_EQUALS_TRIGGER),
        ({"stop_price": 81.50}, ENTRY_GEOMETRY_STOP_EQUALS_TARGET),
        ({"target_price": None, "trigger": {"entry": 77.62, "stop": 75.10}}, ENTRY_GEOMETRY_MISSING),
        ({"target_price": "not-a-number"}, ENTRY_GEOMETRY_NONNUMERIC),
    ],
)
def test_strategy_truth_call_geometry_blocks(overrides, reason):
    result = validate_entry_strategy_truth(plan=_shaped_signal(**overrides))
    assert not result.ok
    assert result.reason == reason


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"target_price": 78.20}, ENTRY_GEOMETRY_PUT_INVALID),
        ({"stop_price": 76.40}, ENTRY_GEOMETRY_PUT_INVALID),
    ],
)
def test_strategy_truth_put_geometry_blocks(overrides, reason):
    signal = _shaped_signal(
        side="PUT",
        direction="PUT",
        pattern="1-2_2D",
        stop_price=80.10,
        target_price=74.50,
    )
    signal.update(overrides)
    result = validate_entry_strategy_truth(plan=signal)
    assert not result.ok
    assert result.reason == reason


def test_strategy_truth_rejects_conflicting_authoritative_geometry():
    order = _shaped_signal(target_price=81.50)
    plan = SimpleNamespace(**_shaped_signal(target_price=82.00))
    result = validate_entry_strategy_truth(order=order, plan=plan)
    assert not result.ok
    assert result.reason == ENTRY_GEOMETRY_CONFLICT
    assert result.details["field"] == "target"


def test_strategy_truth_pattern_side_conflicts_block():
    result = validate_entry_strategy_truth(plan=_shaped_signal(
        side="PUT",
        direction="PUT",
        pattern="1-2_2U",
        stop_price=80.10,
        target_price=74.50,
    ))
    assert not result.ok
    assert result.reason == ENTRY_PATTERN_SIDE_CONFLICT

    result = validate_entry_strategy_truth(plan=_shaped_signal(
        side="CALL",
        direction="CALL",
        pattern="1-2_2D",
    ))
    assert not result.ok
    assert result.reason == ENTRY_PATTERN_SIDE_CONFLICT


def test_strategy_truth_ambiguous_pattern_not_rejected_by_parser():
    result = validate_entry_strategy_truth(plan=_shaped_signal(pattern="3-2-2"))
    assert result.ok


def test_failed_direction_blocks_in_live_and_paper_follows_policy(monkeypatch):
    live = validate_entry_strategy_truth(plan=_shaped_signal(pattern="FAILED_DIR_2U_30min+60min"))
    assert not live.ok
    assert live.reason == LIVE_FAILED_DIRECTION_PATTERN_BLOCKED

    paper_signal = _shaped_signal(execution_mode="paper", pattern="FAILED_DIR_2U_30min+60min")
    paper = validate_entry_strategy_truth(plan=paper_signal)
    assert paper.ok

    monkeypatch.setenv("PAPER_BLOCK_FAILED_DIRECTION_PATTERNS", "1")
    paper_blocked = validate_entry_strategy_truth(plan=paper_signal)
    assert not paper_blocked.ok
    assert paper_blocked.reason == LIVE_FAILED_DIRECTION_PATTERN_BLOCKED


def test_live_timeframe_policy_blocks_intraday_and_allows_daily_weekly(monkeypatch):
    monkeypatch.delenv("LIVE_ALLOWED_ENTRY_TIMEFRAMES", raising=False)
    assert validate_entry_strategy_truth(plan=_shaped_signal(timeframe="1d")).ok
    assert validate_entry_strategy_truth(plan=_shaped_signal(timeframe="overnight")).ok
    assert validate_entry_strategy_truth(plan=_shaped_signal(timeframe="weekly")).ok

    blocked = validate_entry_strategy_truth(plan=_shaped_signal(timeframe="15m"))
    assert not blocked.ok
    assert blocked.reason == LIVE_TIMEFRAME_NOT_ALLOWED


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


def test_weekly_consolidation_scanner_payload_passes_with_explicit_scanner_timeframe_clues():
    payload = {
        "side": "PUT",
        "score": 65,
        "symbol": "WBD",
        "ticker": "WBD",
        "trigger": {
            "pt1": 26.25,
            "pt2": None,
            "pt3": None,
            "side": "PUT",
            "stop": 26.88,
            "entry": 26.57,
            "source": "scanner_consolidation_v3_weekly",
            "strike": 27,
            "expiry_hint": "WEEKLY",
            "current_price": 26.66,
        },
        "ev_score": 65,
        "rr_ratio": 1.5,
        "win_rate": 0.65,
        "direction": "PUT",
        "signal_id": "2026-07-01:1-1:WBD:Weekly:PUT",
        "avg_return": 1.5,
        "pattern_id": "1-1",
        "avg_opt_ret": 400,
        "n_occurrences": 5,
        "timestamp_iso": "2026-07-01T12:31:32.416050Z",
        "confidence_tag": "standard_pool",
        "confidence_score": 49,
        "backtest_match_source": "FALLBACK_DEFAULT",
    }

    before = deepcopy(payload)
    result = validate_entry_metadata(
        plan=payload,
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
    )

    assert result.ok
    assert result.reason is None
    assert payload == before


def test_missing_timeframe_still_blocks_when_no_scanner_timeframe_clue_exists():
    payload = _queue_payload_without_mode(
        timeframe="",
        signal_id="sig-without-timeframe-clue",
        canonical_signal_id="sig-without-timeframe-clue",
        underlying_entry=None,
        trigger={
            "entry": 77.62,
            "stop": 75.10,
            "pt1": 81.50,
            "current_price": 77.88,
            "source": "scanner_unknown",
        },
    )

    result = validate_entry_metadata(
        plan=payload,
        client_id="jasoncosby1@gmail.com",
        execution_mode="paper",
    )

    assert not result.ok
    assert result.reason == MISSING_TIMEFRAME


def test_random_weekly_text_without_valid_scanner_clue_still_fails_missing_timeframe():
    payload = _queue_payload_without_mode(
        timeframe="",
        time_horizon="",
        signal_id="sig-random-text-only",
        canonical_signal_id="sig-random-text-only",
        pattern="weekly fake note",
        underlying_entry=None,
        trigger={
            "entry": 77.62,
            "stop": 75.10,
            "pt1": 81.50,
            "current_price": 77.88,
            "source": "scanner_unknown",
            "comment": "this note says weekly but is not a scanner timeframe clue",
            "expiry_hint": "",
        },
    )
    before = deepcopy(payload)

    result = validate_entry_metadata(
        plan=payload,
        client_id="jasoncosby1@gmail.com",
        execution_mode="paper",
    )

    assert not result.ok
    assert result.reason == MISSING_TIMEFRAME
    assert payload == before


def test_deferred_overnight_watcher_handoff_still_fails_strict_validation_without_underlying():
    plan = SimpleNamespace(
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        signal_id="REEVAL:95451d1d-c357-4b97-8863-e9ae6a45c9b6:731354",
        ticker="CVS",
        symbol="CVS",
        side="CALL",
        direction="CALL",
        timeframe="1d",
        score=70.0,
        trigger_price=104.67,
        target_underlying=105.93,
        stop_underlying=102.69,
        contract_symbol="DEFERRED:CVS",
        metadata={"contract_deferred": True, "overnight": True},
    )

    result = validate_entry_metadata(plan=plan, client_id="jasoncosby1@gmail.com", execution_mode="live")

    assert not result.ok
    assert result.reason == ZERO_UNDERLYING


def test_deferred_overnight_watcher_create_carveout_requires_pending_trigger():
    plan = SimpleNamespace(
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        signal_id="REEVAL:95451d1d-c357-4b97-8863-e9ae6a45c9b6:731354",
        ticker="CVS",
        symbol="CVS",
        side="CALL",
        direction="CALL",
        timeframe="1d",
        score=70.0,
        trigger_price=104.67,
        target_underlying=105.93,
        stop_underlying=102.69,
        contract_symbol="DEFERRED:CVS",
        metadata={"contract_deferred": True, "overnight": True},
    )

    assert _allow_deferred_overnight_watcher_create(
        plan=plan,
        caller_meta=plan.metadata,
        initial_status="PENDING_TRIGGER",
    )
    assert not _allow_deferred_overnight_watcher_create(
        plan=plan,
        caller_meta=plan.metadata,
        initial_status="CREATED",
    )


def test_deferred_overnight_watcher_marker_is_not_execution_ready():
    plan = SimpleNamespace(metadata={"contract_deferred": True, "overnight": True})
    caller_meta = {"contract_deferred": True, "overnight": True}

    _mark_deferred_watcher_data_pending(plan, caller_meta, ZERO_UNDERLYING)

    assert plan.metadata["metadata_validation_status"] == "DATA_PENDING"
    assert plan.metadata["underlying_data_pending"] is True
    assert plan.metadata["allowed_for_watcher"] is True
    assert plan.metadata["allowed_for_execution"] is False
    assert caller_meta["allowed_for_execution"] is False


def test_submit_metadata_still_blocks_deferred_order_without_underlying():
    order_row = {
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "signal_id": "REEVAL:95451d1d-c357-4b97-8863-e9ea0e6a74e0:abc123",
        "symbol": "CVS",
        "direction": "CALL",
        "timeframe": "1d",
        "score": 70.0,
        "trigger_price": 104.67,
        "target_underlying": 105.93,
        "stop_underlying": 102.69,
        "meta": json.dumps({
            "contract_deferred": True,
            "overnight": True,
            "underlying_data_pending": True,
            "allowed_for_execution": False,
        }),
    }

    result = validate_entry_metadata(order=order_row, client_id="jasoncosby1@gmail.com", execution_mode="live")

    assert not result.ok
    assert result.reason == ZERO_UNDERLYING


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


def _valid_submit_order(**overrides):
    row = {
        "local_order_id": "local-geometry",
        "client_id": "jasoncosby1@gmail.com",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "signal_id": "sig-wfc-call-1d",
        "canonical_signal_id": "WFC:CALL:1d:2026-06-26",
        "symbol": "WFC",
        "contract": "WFC260717C00080000",
        "direction": "CALL",
        "execution_mode": "live",
        "score": 80,
        "trigger_price": 77.62,
        "stop_underlying": 75.10,
        "target_underlying": 81.50,
        "pattern": "1-2_2U",
        "timeframe": "1d",
        "limit_price": 1.25,
        "qty": 1,
        "meta": json.dumps({
            "underlying_entry": 77.88,
        }),
    }
    row.update(overrides)
    return row


def test_submit_existing_entry_blocks_invalid_geometry_before_submit_intent_or_broker(monkeypatch):
    _install_or_skip()
    from ap.order_state_machine import APOrderStateMachine, OrderStatus

    osm = APOrderStateMachine("jasoncosby1@gmail.com")
    order_row = _valid_submit_order(target_underlying=76.50)
    terminalized = {}

    monkeypatch.setattr(osm, "_get_order", lambda local_order_id: order_row)
    monkeypatch.setattr(osm, "terminalize_deferred_breach", lambda local_order_id, **kwargs: terminalized.setdefault("kwargs", kwargs) or True)

    def _should_not_submit(*args, **kwargs):
        raise AssertionError("broker submit must not be called for invalid geometry")

    def _should_not_write_submit_intent(*args, **kwargs):
        raise AssertionError("submit intent must not be written for invalid geometry")

    monkeypatch.setattr(osm, "_submit_order_with_retry", _should_not_submit)
    monkeypatch.setattr(osm, "update_order_meta", _should_not_write_submit_intent)
    monkeypatch.setattr(osm, "persist_materialized_submit_intent", _should_not_write_submit_intent)
    monkeypatch.setattr(osm, "persist_deferred_submit_intent", _should_not_write_submit_intent)

    result = osm.submit_existing_entry(
        local_order_id="local-geometry",
        broker=object(),
        plan=None,
        limit_price=1.25,
    )

    assert result["ok"] is False
    assert result["strategy_truth_blocked"] is True
    assert result["error"] == ENTRY_GEOMETRY_CALL_INVALID
    assert result["terminalized"] is True
    assert terminalized["kwargs"]["reason_code"] == ENTRY_GEOMETRY_CALL_INVALID
    assert terminalized["kwargs"]["terminal_status"] == OrderStatus.ERROR


def test_direct_submit_boundary_detects_geometry_changed_after_arm(monkeypatch):
    _install_or_skip()
    from ap.order_state_machine import APOrderStateMachine

    osm = APOrderStateMachine("jasoncosby1@gmail.com")
    order_row = _valid_submit_order(target_underlying=76.50)
    armed_plan = SimpleNamespace(
        contract_symbol="WFC260717C00080000",
        contracts=1,
        signal_id="sig-wfc-call-1d",
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        ticker="WFC",
        side="CALL",
        trigger_price=77.62,
        stop_underlying=75.10,
        target_underlying=81.50,
        pattern="1-2_2U",
        timeframe="1d",
        metadata={"underlying_entry": 77.88},
    )

    monkeypatch.setattr(osm, "_get_order", lambda local_order_id: order_row)
    monkeypatch.setattr(osm, "terminalize_deferred_breach", lambda *args, **kwargs: True)

    def _should_not_submit(*args, **kwargs):
        raise AssertionError("broker submit must not be called after geometry corruption")

    monkeypatch.setattr(osm, "_submit_order_with_retry", _should_not_submit)

    original = getattr(APOrderStateMachine, "_entry_metadata_guard_original_submit_existing", None)
    submit = original.__get__(osm, APOrderStateMachine) if original else osm.submit_existing_entry
    result = submit(
        local_order_id="local-geometry",
        broker=object(),
        plan=armed_plan,
        limit_price=1.25,
    )

    assert result["ok"] is False
    assert result["strategy_truth_blocked"] is True
    assert result["error"] == ENTRY_GEOMETRY_CHANGED_AFTER_ARM
