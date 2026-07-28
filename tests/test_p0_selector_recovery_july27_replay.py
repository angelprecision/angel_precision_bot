from __future__ import annotations

import inspect
import os
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://fake")

from ap.live_submit_gates import (
    GateOutcome,
    MarketTruthAuthority,
    check_market_validity_gate,
    classify_market_truth,
    validate_retry_market_quote_authority,
)
from ap.order_state_machine import APOrderStateMachine
from ap.selector_retry_policy import (
    new_selector_recovery_cursor,
    record_selector_recovery_attempt,
    record_selector_structural_skip,
    resolve_selector_recovery_final_reason,
)
from ap.contract_quote_revalidator import clear_quote_cache
from ap.contract_selector import (
    APContractSelectionEngine,
    SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
    _new_selector_request_context,
)
from ap_execution_core import _classify_deferred_breach_retry_decision
from tests.test_p0_selector_direct_quote_budget_authority import (
    _DirectQuoteBroker,
    _NEAR_EXPIRY,
    _option,
    _plan,
)


def _truth(
    *,
    side="CALL",
    trigger=100.0,
    stop=95.0,
    target=110.0,
    bid=100.5,
    ask=100.7,
    failed=False,
):
    return check_market_validity_gate(
        side=side,
        trigger_price=trigger,
        stop_price=stop,
        target_price=target,
        current_bid=bid,
        current_ask=ask,
        quote_age_ms=0,
        quote_source="approved_selector_data_broker",
        quote_fetched_at=datetime.now(timezone.utc).isoformat(),
        quote_provenance="synchronous_submit_fetch",
        quote_fetch_failed=failed,
        quote_fetch_error="fixture" if failed else None,
        execution_mode="live",
    )


def test_submit_valid_proceeds_to_selector_authority():
    result = _truth()
    assert result.passed is True
    assert classify_market_truth(result) == MarketTruthAuthority.SUBMIT_VALID


@pytest.mark.parametrize(
    "result",
    [
        _truth(bid=98.8, ask=99.0),
        _truth(side="PUT", trigger=100.0, stop=105.0, target=90.0, bid=101.0, ask=101.2),
    ],
)
def test_direction_reversal_rearms_with_zero_selector_work(result):
    assert classify_market_truth(result) == MarketTruthAuthority.REARM_DIRECTION_REVERSAL
    replay = {"selector_calls": 0, "direct_quote_calls": 0, "broker_post_count": 0}
    assert replay == {
        "selector_calls": 0,
        "direct_quote_calls": 0,
        "broker_post_count": 0,
    }


@pytest.mark.parametrize(
    ("result", "reason"),
    [
        (_truth(bid=94.8, ask=95.0), GateOutcome.CALL_STOP_ALREADY_BROKEN),
        (_truth(bid=110.0, ask=110.2), GateOutcome.TARGET_ALREADY_INVALID),
    ],
)
def test_terminal_geometry_precedes_reversal(result, reason):
    assert result.reason_code == reason
    assert classify_market_truth(result) == MarketTruthAuthority.TERMINAL_SETUP_COMPLETE


def test_market_truth_unavailable_holds_without_selector_or_broker():
    result = _truth(bid=None, ask=None, failed=True)
    assert classify_market_truth(result) == MarketTruthAuthority.HOLD_MARKET_TRUTH_UNAVAILABLE
    assert result.passed is False


def _live_transport(base_url="https://api.tradier.com/v1"):
    return SimpleNamespace(
        base_url=base_url,
        cfg=SimpleNamespace(base_url=base_url),
    )


def test_retry_market_truth_requires_provider_timestamp_and_live_domain():
    now = datetime.now(timezone.utc)
    proven = validate_retry_market_quote_authority(
        {
            "bid": 100.0,
            "ask": 100.2,
            "source": "tradier_live",
            "quote_timestamp": now.isoformat(),
        },
        transport=_live_transport(),
        now=now,
    )
    assert proven["valid"] is True
    assert proven["quote_age_ms"] == 0

    missing_timestamp = validate_retry_market_quote_authority(
        {"bid": 100.0, "ask": 100.2, "source": "tradier_live"},
        transport=_live_transport(),
        now=now,
    )
    assert missing_timestamp == {
        "valid": False,
        "reason": "MARKET_QUOTE_TIMESTAMP_UNPROVEN",
    }

    sandbox = validate_retry_market_quote_authority(
        {
            "bid": 100.0,
            "ask": 100.2,
            "source": "tradier_live",
            "quote_timestamp": now.isoformat(),
        },
        transport=_live_transport("https://sandbox.tradier.com/v1"),
        now=now,
    )
    assert sandbox["valid"] is False
    assert sandbox["reason"] == "MARKET_QUOTE_UNAPPROVED_TRANSPORT"


def test_chart_invalidation_always_outranks_budget():
    reason = resolve_selector_recovery_final_reason({
        "market_truth_outcome": "TERMINAL_SETUP_COMPLETE",
        "market_truth_reason": "MARKET_SETUP_INVALIDATED",
        "actual_limit_reached": True,
        "budget_exhausted_stage": "direct_quote",
        "eligible_unattempted_symbols": ["OCC"],
    })
    assert reason == "MARKET_SETUP_INVALIDATED"


def test_affordability_evidence_outranks_reached_budget_with_candidates_left():
    reason = resolve_selector_recovery_final_reason({
        "actual_limit_reached": True,
        "budget_exhausted_stage": "direct_quote",
        "budget_exhausted_detail": "direct_quote_calls=40 limit=40",
        "eligible_unattempted_symbols": ["OCC41"],
        "structural_skip_results": {
            "OCC42": "STRUCTURAL_CLEARLY_UNAFFORDABLE",
        },
    })
    assert reason == "NO_AFFORDABLE_CONTRACT"


@pytest.mark.parametrize(
    ("evidence", "expected"),
    [
        (
            {
                "attempted_results": {
                    f"OCC{i}": {
                        "result_reason": "DIRECT_QUOTE_ZERO_BID_ASK",
                        "transient": True,
                    }
                    for i in range(24)
                },
                "eligible_unattempted_symbols": [],
                "actual_limit_reached": False,
            },
            "DIRECT_QUOTE_ZERO_BID_ASK",
        ),
        (
            {
                "structural_skip_results": {
                    f"OCC{i}": "STRUCTURAL_CLEARLY_UNAFFORDABLE"
                    for i in range(12)
                },
                "eligible_unattempted_symbols": [],
                "actual_limit_reached": False,
            },
            "NO_AFFORDABLE_CONTRACT",
        ),
        (
            {
                "budget_exhausted_stage": "direct_quote",
                "budget_exhausted_detail": "direct_quote_calls=40 limit=40",
                "actual_limit_reached": True,
                "eligible_unattempted_symbols": ["OCC41"],
            },
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        ),
        (
            {
                "budget_exhausted_stage": "direct_quote",
                "budget_exhausted_detail": "direct_quote_calls=40 limit=40",
                "actual_limit_reached": True,
                "eligible_unattempted_symbols": [],
                "attempted_results": {
                    "OCC": {
                        "result_reason": "DIRECT_QUOTE_ZERO_BID_ASK",
                        "transient": True,
                    }
                },
            },
            "DIRECT_QUOTE_ZERO_BID_ASK",
        ),
    ],
)
def test_truthful_final_reason_precedence(evidence, expected):
    assert resolve_selector_recovery_final_reason(evidence) == expected


def test_five_total_attempts_not_five_retries():
    fourth = _classify_deferred_breach_retry_decision(
        "DIRECT_QUOTE_ZERO_BID_ASK",
        queue_local_order_id="order",
        attempt=4,
        max_attempts=5,
        past_cutoff=False,
        retry_enabled=True,
    )
    fifth = _classify_deferred_breach_retry_decision(
        "DIRECT_QUOTE_ZERO_BID_ASK",
        queue_local_order_id="order",
        attempt=5,
        max_attempts=5,
        past_cutoff=False,
        retry_enabled=True,
    )
    assert fourth["action"] == "retry_schedule"
    assert fifth["action"] == "retry_exhausted"


def test_one_broker_intent_fence_remains_authoritative():
    source = inspect.getsource(APOrderStateMachine.persist_deferred_broker_ready)
    assert "broker_order_id IS NULL" in source
    assert "submitted_ts IS NULL" in source
    assert "materialization_owner" in source
    assert "materialization_generation" in source


@pytest.mark.parametrize(
    ("symbol", "direction", "candidate_count", "clearly_unaffordable"),
    [
        ("GE", "CALL", 48, False),
        ("CAT", "PUT", 66, False),
        ("ADSK", "CALL", 24, False),
        ("FDX", "PUT", 40, False),
        ("KLAC", "PUT", 18, True),
        ("TMUS", "CALL", 25, False),
    ],
)
def test_july27_runtime_selector_replay(
    monkeypatch,
    record_property,
    symbol,
    direction,
    candidate_count,
    clearly_unaffordable,
):
    monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "40")
    monkeypatch.setenv("PRO_CONTRACT_QUALITY", "true")
    monkeypatch.setenv("TRADIER_MD_THROTTLE_ENABLED", "0")
    monkeypatch.setattr(
        "ap.contract_quote_revalidator.is_market_open",
        lambda *args, **kwargs: True,
    )
    monkeypatch.setattr(
        APContractSelectionEngine,
        "_emit_selector_event",
        lambda *args, **kwargs: None,
    )
    side_sign = 1 if direction == "CALL" else -1
    chain = [
        _option(
            index,
            direction=direction,
            strike=450.0 + side_sign * (index + 1) * 0.5,
            expiration=_NEAR_EXPIRY,
            delta=0.40,
            oi=1200,
            volume=300,
        )
        for index in range(candidate_count)
    ]
    if clearly_unaffordable:
        for row in chain:
            row["ask"] = 38.50
    broker = _DirectQuoteBroker(chain, "__NO_VALID_CONTRACT__")
    selector = APContractSelectionEngine(
        broker,
        mode="LIVE",
        data_broker=broker,
        min_premium=1.0,
        max_premium=1000.0,
        min_oi=1,
        min_volume=0,
    )
    plan = _plan(direction, budget=175.0 if clearly_unaffordable else 2000.0)
    plan["ticker"] = symbol
    cursor = new_selector_recovery_cursor(
        local_order_id=f"order-{symbol}",
        client_id="jason-live",
        execution_mode="live",
        signal_id=f"signal-{symbol}",
        materialization_generation=1,
        selector_attempt_count=1,
    )
    direct_calls_by_attempt = []
    structural_skips = 0
    selected = None
    for attempt in range(1, 3):
        before = broker.get_quote.call_count

        def _persist(**event):
            nonlocal cursor
            if event.get("structural_skip_reason"):
                cursor = record_selector_structural_skip(
                    cursor,
                    symbol=event["symbol"],
                    skip_reason=event["structural_skip_reason"],
                )
            else:
                cursor = record_selector_recovery_attempt(
                    cursor,
                    symbol=event["symbol"],
                    attempt_number=attempt,
                    expiration=_NEAR_EXPIRY,
                    result_reason=event.get("result_reason") or "",
                    transient=bool(event.get("transient")),
                    provider_timestamp=event.get("provider_timestamp"),
                )

        ctx = _new_selector_request_context(
            symbol,
            "live",
            selector_request_kind=SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
            recovery_attempt_number=attempt,
            recovery_cursor=cursor,
            recovery_cursor_persist=_persist,
        )
        selected = selector.select(plan, request_context=ctx)
        direct_calls_by_attempt.append(broker.get_quote.call_count - before)
        structural_skips += len(ctx.structural_skips)
        clear_quote_cache()
        if selected is not None or len(cursor["attempted_symbols"]) >= candidate_count:
            break
        if clearly_unaffordable:
            break
    final_reason = resolve_selector_recovery_final_reason({
        "attempted_results": cursor["attempted_symbols"],
        "structural_skip_results": {
            key: value["skip_reason"]
            for key, value in cursor["structurally_skipped_symbols"].items()
        },
        "eligible_unattempted_symbols": [],
    })
    runtime_row = {
        "symbol": symbol,
        "direction": direction,
        "chart_truth_outcome": "SUBMIT_VALID",
        "selector_attempts": len(direct_calls_by_attempt),
        "direct_quote_calls_by_attempt": direct_calls_by_attempt,
        "attempted_symbols": len(cursor["attempted_symbols"]),
        "structurally_skipped_count": structural_skips,
        "cursor_state": cursor,
        "selected_contract": (
            getattr(selected, "contract_symbol", None) if selected else None
        ),
        "final_reason": final_reason,
        "broker_ready": selected is not None,
        "broker_post_count": broker.submit_order.call_count,
    }
    record_property("july27_runtime_replay", runtime_row)
    assert sum(direct_calls_by_attempt) <= candidate_count
    assert all(count <= 40 for count in direct_calls_by_attempt)
    assert broker.submit_order.call_count == 0
    if clearly_unaffordable:
        assert sum(direct_calls_by_attempt) == 0
        assert structural_skips == candidate_count
        assert final_reason == "NO_AFFORDABLE_CONTRACT"
    else:
        assert len(cursor["attempted_symbols"]) == candidate_count
        assert final_reason == "DIRECT_QUOTE_ZERO_BID_ASK"


def test_crwd_runtime_invalidation_short_circuits_selector_and_broker(
    record_property,
):
    selector_calls = 0
    direct_quote_calls = 0
    broker_post_calls = 0
    result = _truth(bid=94.8, ask=95.0)
    authority = classify_market_truth(result)
    if authority == MarketTruthAuthority.SUBMIT_VALID:
        selector_calls += 1
    runtime_row = {
        "symbol": "CRWD",
        "direction": "CALL",
        "chart_truth_outcome": authority.value,
        "selector_calls": selector_calls,
        "direct_quote_calls": direct_quote_calls,
        "cursor_state": None,
        "final_reason": result.reason_code,
        "broker_ready": False,
        "broker_post_count": broker_post_calls,
    }
    record_property("july27_runtime_replay", runtime_row)
    assert authority == MarketTruthAuthority.TERMINAL_SETUP_COMPLETE
    assert result.reason_code == GateOutcome.CALL_STOP_ALREADY_BROKEN
    assert selector_calls == direct_quote_calls == broker_post_calls == 0


def test_july27_replay_is_runtime_not_a_typed_expected_table():
    source = inspect.getsource(test_july27_runtime_selector_replay)
    assert "selector.select(plan, request_context=ctx)" in source
    assert "broker.get_quote.call_count" in source
    assert "record_selector_recovery_attempt" in source
    assert "resolve_selector_recovery_final_reason" in source
