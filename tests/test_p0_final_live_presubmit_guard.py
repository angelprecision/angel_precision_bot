from dataclasses import dataclass, field

from ap_entry_confirmation import check_entry_confirmation


@dataclass
class Plan:
    ticker: str = "WFC"
    signal_id: str = "sig-test"
    client_id: str = "jasoncosby1@gmail.com"
    execution_mode: str = "live"
    side: str = "CALL"
    timeframe: str = "1d"
    pattern: str = "1-2-2U"
    trigger_price: float = 79.97
    stop_underlying: float = 79.40
    target_underlying: float = 81.25
    limit_price: float = 1.50
    metadata: dict = field(default_factory=dict)


def _run(plan, *, direction="CALL", current=80.05, live_ask=1.55, decision_price=1.50, sandbox=False):
    return check_entry_confirmation(
        plan=plan,
        direction=direction,
        trigger_price=plan.trigger_price,
        live_bid=round(live_ask - 0.05, 2),
        live_ask=live_ask,
        live_quote_age_ms=1000,
        underlying_last=current,
        decision_option_price=decision_price,
        score=72,
        tier="B",
        timeframe=plan.timeframe,
        sandbox_mode=sandbox,
    )


def test_live_wfc_stale_call_target_already_reached_blocks():
    plan = Plan(
        side="CALL",
        trigger_price=77.62,
        stop_underlying=76.90,
        target_underlying=78.56,
        limit_price=1.47,
    )

    result = _run(plan, direction="CALL", current=83.985, live_ask=1.50, decision_price=1.47)

    assert not result.passed
    assert result.fail_reason == "STALE_TARGET_ALREADY_REACHED_CALL"
    guard = result.metadata["live_final_entry_guard"]
    assert guard["client_id"] == "jasoncosby1@gmail.com"
    assert guard["execution_mode"] == "live"
    assert guard["trigger_price"] == 77.62
    assert guard["current_underlying"] == 83.985
    assert guard["target_underlying"] == 78.56


def test_live_wfc_premium_drift_blocks_above_eight_percent():
    plan = Plan(
        side="PUT",
        trigger_price=83.00,
        stop_underlying=84.00,
        target_underlying=81.00,
        limit_price=1.47,
    )

    result = _run(plan, direction="PUT", current=82.75, live_ask=1.68, decision_price=1.47)

    assert not result.passed
    assert result.fail_reason == "LIVE_PREMIUM_DRIFT_TOO_HIGH"
    guard = result.metadata["live_final_entry_guard"]
    assert round(guard["premium_drift_pct"], 4) == round(((1.68 - 1.47) / 1.47) * 100, 4)
    assert guard["selector_reference_price"] == 1.47
    assert guard["submit_ask"] == 1.68


def test_live_put_target_already_reached_blocks():
    plan = Plan(
        side="PUT",
        trigger_price=83.00,
        stop_underlying=84.25,
        target_underlying=81.50,
        limit_price=1.20,
    )

    result = _run(plan, direction="PUT", current=81.25, live_ask=1.22, decision_price=1.20)

    assert not result.passed
    assert result.fail_reason == "STALE_TARGET_ALREADY_REACHED_PUT"


def test_valid_live_clean_call_near_trigger_passes_without_legacy_confirmation_required():
    plan = Plan(
        side="CALL",
        trigger_price=79.97,
        stop_underlying=79.20,
        target_underlying=81.00,
        limit_price=1.50,
        metadata={"quality_mode_result": {"gate_status": "DISABLED"}},
    )

    result = _run(plan, direction="CALL", current=80.05, live_ask=1.55, decision_price=1.50)

    assert result.passed
    assert result.fail_reason is None
    assert result.metadata["confirmation_required"] is False
    guard = result.metadata["live_final_entry_guard"]
    assert guard["passed"] is True
    assert guard["quality_mode_disabled_replaced_by_final_guard"] is True
    assert guard["client_id"] == "jasoncosby1@gmail.com"
    assert guard["execution_mode"] == "live"


def test_paper_keeps_existing_fail_open_behavior_when_legacy_confirmation_not_required():
    plan = Plan(
        side="CALL",
        trigger_price=77.62,
        stop_underlying=76.90,
        target_underlying=78.56,
        limit_price=1.47,
    )

    result = _run(plan, direction="CALL", current=83.985, live_ask=1.68, decision_price=1.47, sandbox=True)

    assert result.passed
    assert result.metadata["confirmation_required"] is False
    assert result.metadata["live_final_entry_guard"]["skipped"] is True
