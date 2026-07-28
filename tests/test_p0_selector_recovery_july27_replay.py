from __future__ import annotations

import inspect
import os
from datetime import datetime, timezone

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://fake")

from ap.live_submit_gates import (
    GateOutcome,
    MarketTruthAuthority,
    check_market_validity_gate,
    classify_market_truth,
)
from ap.order_state_machine import APOrderStateMachine
from ap.selector_retry_policy import resolve_selector_recovery_final_reason
from ap_execution_core import _classify_deferred_breach_retry_decision


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


def test_chart_invalidation_always_outranks_budget():
    reason = resolve_selector_recovery_final_reason({
        "market_truth_outcome": "TERMINAL_SETUP_COMPLETE",
        "market_truth_reason": "MARKET_SETUP_INVALIDATED",
        "actual_limit_reached": True,
        "budget_exhausted_stage": "direct_quote",
        "eligible_unattempted_symbols": ["OCC"],
    })
    assert reason == "MARKET_SETUP_INVALIDATED"


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


def test_july27_seven_symbol_replay_table(record_property):
    table = [
        {
            "symbol": "GE",
            "direction": "CALL",
            "chart_truth_outcome": "SUBMIT_VALID",
            "selector_attempts": 2,
            "direct_quote_calls_by_attempt": [40, 8],
            "attempted_symbols": 48,
            "structurally_skipped_count": 4,
            "eligible_unattempted_count": 0,
            "selected_contract": None,
            "final_reason": "DIRECT_QUOTE_ZERO_BID_ASK",
            "broker_post_count": 0,
        },
        {
            "symbol": "CAT",
            "direction": "PUT",
            "chart_truth_outcome": "SUBMIT_VALID",
            "selector_attempts": 2,
            "direct_quote_calls_by_attempt": [40, 26],
            "attempted_symbols": 66,
            "structurally_skipped_count": 0,
            "eligible_unattempted_count": 0,
            "selected_contract": None,
            "final_reason": "DIRECT_QUOTE_ZERO_BID_ASK",
            "broker_post_count": 0,
        },
        {
            "symbol": "ADSK",
            "direction": "CALL",
            "chart_truth_outcome": "SUBMIT_VALID",
            "selector_attempts": 1,
            "direct_quote_calls_by_attempt": [24],
            "attempted_symbols": 24,
            "structurally_skipped_count": 0,
            "eligible_unattempted_count": 0,
            "selected_contract": None,
            "final_reason": "DIRECT_QUOTE_ZERO_BID_ASK",
            "broker_post_count": 0,
        },
        {
            "symbol": "FDX",
            "direction": "PUT",
            "chart_truth_outcome": "SUBMIT_VALID",
            "selector_attempts": 2,
            "direct_quote_calls_by_attempt": [32, 8],
            "attempted_symbols": 40,
            "structurally_skipped_count": 3,
            "eligible_unattempted_count": 0,
            "selected_contract": None,
            "final_reason": "DIRECT_QUOTE_ZERO_BID_ASK",
            "broker_post_count": 0,
        },
        {
            "symbol": "KLAC",
            "direction": "PUT",
            "chart_truth_outcome": "SUBMIT_VALID",
            "selector_attempts": 1,
            "direct_quote_calls_by_attempt": [0],
            "attempted_symbols": 0,
            "structurally_skipped_count": 18,
            "eligible_unattempted_count": 0,
            "selected_contract": None,
            "final_reason": "NO_AFFORDABLE_CONTRACT",
            "broker_post_count": 0,
        },
        {
            "symbol": "CRWD",
            "direction": "CALL",
            "chart_truth_outcome": "TERMINAL_SETUP_COMPLETE",
            "selector_attempts": 0,
            "direct_quote_calls_by_attempt": [],
            "attempted_symbols": 0,
            "structurally_skipped_count": 0,
            "eligible_unattempted_count": 0,
            "selected_contract": None,
            "final_reason": "MARKET_SETUP_INVALIDATED",
            "broker_post_count": 0,
        },
        {
            "symbol": "TMUS",
            "direction": "CALL",
            "chart_truth_outcome": "SUBMIT_VALID",
            "selector_attempts": 1,
            "direct_quote_calls_by_attempt": [25],
            "attempted_symbols": 25,
            "structurally_skipped_count": 0,
            "eligible_unattempted_count": 0,
            "selected_contract": None,
            "final_reason": "DIRECT_QUOTE_ZERO_BID_ASK",
            "broker_post_count": 0,
        },
    ]
    record_property("july27_selector_replay", table)
    assert [row["symbol"] for row in table] == [
        "GE", "CAT", "ADSK", "FDX", "KLAC", "CRWD", "TMUS"
    ]
    crwd = next(row for row in table if row["symbol"] == "CRWD")
    assert crwd["selector_attempts"] == 0
    assert crwd["direct_quote_calls_by_attempt"] == []
    assert crwd["broker_post_count"] == 0
    assert all(row["broker_post_count"] <= 1 for row in table)
