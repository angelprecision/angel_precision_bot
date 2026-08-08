from __future__ import annotations

import itertools
import os
import time
from datetime import date, timedelta
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

from ap.contract_quote_revalidator import clear_quote_cache
from ap.contract_selector import (
    APContractSelectionEngine,
    SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
    SelectorRequestContext,
    _new_selector_request_context,
    _order_chain_for_direct_quote_recovery,
    _structural_direct_quote_skip,
)
from ap.selector_retry_policy import resolve_selector_recovery_final_reason
from ap_execution_core import (
    _build_deferred_retry_schedule_meta,
    _build_deferred_retry_terminal_meta,
)
from tests.test_p0_selector_direct_quote_budget_authority import (
    _DirectQuoteBroker,
    _NEAR_EXPIRY,
)


@pytest.fixture(autouse=True)
def _selector_test_isolation(monkeypatch):
    clear_quote_cache()
    monkeypatch.setenv("DATABASE_URL", "postgresql://mock/mock")
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
    yield
    clear_quote_cache()


def _occ(ticker: str, expiration: str, strike: float, side: str = "CALL") -> str:
    cp = "C" if side == "CALL" else "P"
    return f"{ticker}{date.fromisoformat(expiration):%y%m%d}{cp}{int(strike * 1000):08d}"


def _row(
    ticker: str,
    strike: float,
    *,
    expiration: str = _NEAR_EXPIRY,
    side: str = "CALL",
    bid: float = 0.0,
    ask: float = 0.0,
    delta: float = 0.40,
    oi: int = 1200,
    volume: int = 300,
) -> dict:
    return {
        "symbol": _occ(ticker, expiration, strike, side),
        "expiration_date": expiration,
        "strike": float(strike),
        "option_type": side.lower(),
        "bid": bid,
        "ask": ask,
        "greeks": {"delta": delta if side == "CALL" else -abs(delta)},
        "open_interest": oi,
        "volume": volume,
        "bid_size": 20,
        "ask_size": 20,
    }


def _plan(
    *,
    ticker: str,
    side: str = "CALL",
    underlying: float,
    budget: float,
    client_id: str = "jason-live",
    execution_mode: str = "LIVE",
    signal_id: str = "66b1607b-5364-4cd3-9987-2b03a4199517",
) -> dict:
    return {
        "signal_id": signal_id,
        "client_id": client_id,
        "execution_mode": execution_mode,
        "ticker": ticker,
        "side": side,
        "target_underlying": underlying,
        "trigger_price": underlying,
        "wick_targets": [{"distance_pct": 0.5, "confidence": 0.75}],
        "tier": "A",
        "score": 85.0,
        "pattern": "3-1-2",
        "timeframe": "5m",
        "metadata": {
            "sizing_context": {
                "budget": budget,
                "account_equity": 1747.09,
                "risk_pct": 0.10,
                "max_affordable_premium": budget,
            }
        },
        "max_position_usd": budget,
    }


def _run_selector(monkeypatch, *, plan: dict, chain: list[dict], limit: int):
    monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", str(limit))
    broker = _DirectQuoteBroker(
        chain,
        valid_symbol="__NO_VALID_DIRECT_QUOTE__",
        valid_quote={"bid": 0.0, "ask": 0.0},
        underlying_price=float(plan["target_underlying"]),
    )
    selector = APContractSelectionEngine(
        broker,
        mode=str(plan["execution_mode"]),
        data_broker=broker,
        min_premium=1.0,
        max_premium=1000.0,
        min_oi=1,
        min_volume=0,
    )
    context = _new_selector_request_context(
        plan["ticker"],
        str(plan["execution_mode"]).lower(),
        selector_request_kind=SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
    )
    selected = selector.select(plan, request_context=context)
    failure = plan.get("metadata", {}).get("selector_failure") or {}
    diagnostics = failure.get("selection_diagnostics") or {}
    return selected, broker, context, failure, diagnostics


def test_ibm_affordability_remains_root_after_later_budget_exhaustion(monkeypatch):
    """Replay the historical IBM shape through the real selector seam."""
    ibm = _row(
        "IBM",
        230.0,
        bid=2.95,
        ask=3.15,
        delta=0.4082,
        oi=2045,
        volume=1249,
    )
    ibm["symbol"] = "IBM260731C00230000"
    later_one = _row("IBM", 231.0)
    later_two = _row("IBM", 232.0)
    plan = _plan(ticker="IBM", underlying=228.98, budget=174.71)

    selected, broker, context, failure, diagnostics = _run_selector(
        monkeypatch,
        plan=plan,
        chain=[ibm, later_one, later_two],
        limit=1,
    )

    assert selected is None
    assert failure["reason_code"] == "UNTRADEABLE_FOR_ACCOUNT_SIZE"
    assert diagnostics["budget_exhausted_stage"] == "direct_quote"
    assert diagnostics["direct_quote_budget"]["used"] == 1
    assert diagnostics["direct_quote_unattempted_count"] >= 1
    assert [call.args[0] for call in broker.get_quote.call_args_list] == [
        later_one["symbol"]
    ]
    assert broker.submit_order.call_count == 0
    assert broker.cancel_order.call_count == 0


def test_paper_live_explanation_parity_keeps_quality_equal_and_budget_distinct(
    monkeypatch,
):
    ibm = _row(
        "IBM",
        230.0,
        bid=2.95,
        ask=3.15,
        delta=0.4082,
        oi=2045,
        volume=1249,
    )
    ibm["symbol"] = "IBM260731C00230000"
    live_plan = _plan(
        ticker="IBM",
        underlying=228.98,
        budget=174.71,
        client_id="jason-live",
        execution_mode="LIVE",
    )
    paper_plan = _plan(
        ticker="IBM",
        underlying=228.98,
        budget=400.00,
        client_id="tradefluence-paper",
        execution_mode="PAPER",
    )

    live = _run_selector(monkeypatch, plan=live_plan, chain=[ibm], limit=1)
    paper = _run_selector(monkeypatch, plan=paper_plan, chain=[ibm], limit=1)

    live_selected, live_broker, _, live_failure, _ = live
    paper_selected, paper_broker, _, paper_failure, _ = paper
    assert live_selected is None
    assert live_failure["reason_code"] == "UNTRADEABLE_FOR_ACCOUNT_SIZE"
    assert paper_selected is not None
    assert paper_selected.contract_symbol == "IBM260731C00230000"
    assert paper_failure == {}
    assert live_broker.submit_order.call_count == 0
    assert live_broker.cancel_order.call_count == 0
    assert paper_broker.submit_order.call_count == 0
    assert paper_broker.cancel_order.call_count == 0


def test_later_budget_evidence_does_not_override_affordability_root():
    reason = resolve_selector_recovery_final_reason(
        {
            "quality_rejections": {"UNTRADEABLE_FOR_ACCOUNT_SIZE": 1},
            "attempted_results": {
                "IBM260731C00230000": {
                    "result_reason": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                    "transient": True,
                }
            },
            "actual_limit_reached": True,
            "budget_exhausted_stage": "direct_quote",
            "eligible_unattempted_symbols": [],
        }
    )
    assert reason == "UNTRADEABLE_FOR_ACCOUNT_SIZE"


def test_transient_only_budget_exhaustion_remains_retryable():
    reason = resolve_selector_recovery_final_reason(
        {
            "attempted_results": {
                "IBM260731C00230000": {
                    "result_reason": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                    "transient": True,
                }
            },
            "actual_limit_reached": True,
            "budget_exhausted_stage": "direct_quote",
            "eligible_unattempted_symbols": ["IBM260801C00231000"],
        }
    )
    assert reason == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"


def test_partial_affordability_set_does_not_become_all_unaffordable():
    reason = resolve_selector_recovery_final_reason(
        {
            "structural_skip_results": {
                "IBM260731C00230000": "STRUCTURAL_CLEARLY_UNAFFORDABLE",
            },
            "eligible_unattempted_symbols": ["IBM260731C00231000"],
        }
    )
    assert reason != "NO_AFFORDABLE_CONTRACT"
    assert reason == "UNKNOWN_SELECTOR_RECOVERY_FAILURE"


def test_complete_affordability_set_is_terminal():
    reason = resolve_selector_recovery_final_reason(
        {
            "structural_skip_results": {
                "IBM260731C00230000": "STRUCTURAL_CLEARLY_UNAFFORDABLE",
                "IBM260731C00231000": "STRUCTURAL_CLEARLY_UNAFFORDABLE",
            },
            "eligible_unattempted_symbols": [],
        }
    )
    assert reason == "NO_AFFORDABLE_CONTRACT"


@pytest.mark.parametrize(
    ("quality_reason", "expected"),
    [
        ("OI_TOO_LOW", "OI_TOO_LOW"),
        ("EARNINGS_LOCKOUT", "EARNINGS_LOCKOUT"),
    ],
)
def test_quality_and_policy_precedence_remains_above_affordability(
    quality_reason, expected
):
    affordability_reason = (
        "PREMIUM_CAP_EXCEEDED"
        if quality_reason == "OI_TOO_LOW"
        else "NO_AFFORDABLE_CONTRACT"
    )
    reason = resolve_selector_recovery_final_reason(
        {
            "quality_rejections": {
                quality_reason: 1,
                affordability_reason: 1,
            },
            "eligible_unattempted_symbols": [],
        }
    )
    assert reason == expected


def test_reason_reduction_is_independent_of_evidence_insertion_order():
    fields = [
        ("quality_rejections", {"UNTRADEABLE_FOR_ACCOUNT_SIZE": 1}),
        (
            "attempted_results",
            {
                "IBM260731C00231000": {
                    "result_reason": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                    "transient": True,
                }
            },
        ),
        ("actual_limit_reached", True),
        ("budget_exhausted_stage", "direct_quote"),
        ("eligible_unattempted_symbols", []),
    ]
    results = {
        resolve_selector_recovery_final_reason(dict(permutation))
        for order in itertools.permutations(fields)
        for permutation in [dict(order)]
    }
    assert results == {"UNTRADEABLE_FOR_ACCOUNT_SIZE"}


def test_direct_quote_calls_follow_ranked_candidate_order(monkeypatch):
    raw = [
        _row("SPY", 104.0),
        _row("SPY", 103.0),
        _row("SPY", 101.0),
        _row("SPY", 102.0),
        _row("SPY", 105.0),
    ]
    plan = _plan(ticker="SPY", underlying=100.0, budget=2000.0)
    selected, broker, context, failure, diagnostics = _run_selector(
        monkeypatch,
        plan=plan,
        chain=raw,
        limit=3,
    )

    assert selected is None
    ranked = [
        entry["symbol"]
        for entry in diagnostics["direct_quote_candidate_ranking"]
    ]
    actual = [call.args[0] for call in broker.get_quote.call_args_list]
    assert ranked == [row["symbol"] for row in sorted(raw, key=lambda row: row["strike"])]
    assert actual == ranked[:3]
    assert diagnostics["direct_quote_budget"]["used"] == 3
    assert diagnostics["direct_quote_unattempted_symbols"][:2] == ranked[3:5]
    assert broker.submit_order.call_count == 0
    assert broker.cancel_order.call_count == 0


def test_structural_rank_one_skip_does_not_consume_direct_quote_budget():
    context = _new_selector_request_context(
        "SPY",
        "live",
        selector_request_kind=SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
    )
    invalid = {
        "symbol": "not-an-occ",
        "expiration_date": _NEAR_EXPIRY,
        "strike": 100.0,
        "option_type": "call",
        "bid": 0.0,
        "ask": 0.0,
        "greeks": {"delta": 0.4},
        "open_interest": 100,
        "volume": 100,
    }
    result = _structural_direct_quote_skip(
        SimpleNamespace(min_dte=0, max_dte=21, target_delta=0.4, delta_band=0.3),
        invalid,
        direction="CALL",
        ticker="SPY",
        underlying_price=100.0,
        today=date.today(),
        selector_budget=200.0,
        request_context=context,
    )
    assert result["skip_reason"] == "STRUCTURAL_INVALID_OCC"
    assert result["provider_call_consumed"] is False
    assert context.provider_call_counts.get("direct_quote_calls", 0) == 0
    assert context.structural_skips[-1]["symbol"] == "NOT-AN-OCC"


def test_equal_rank_ties_have_stable_symbol_tiebreak():
    later_expiry = date.fromisoformat(_NEAR_EXPIRY) + timedelta(days=1)
    while later_expiry.weekday() >= 5:
        later_expiry += timedelta(days=1)
    first = _row("SPY", 101.0, expiration=_NEAR_EXPIRY)
    second = _row("SPY", 101.0, expiration=later_expiry.isoformat())
    rows = [first, second]
    ctx_a = SelectorRequestContext(
        ticker="SPY", started_at_monotonic=time.monotonic()
    )
    ctx_b = SelectorRequestContext(
        ticker="SPY", started_at_monotonic=time.monotonic()
    )
    ordered_a = _order_chain_for_direct_quote_recovery(
        rows,
        direction="CALL",
        underlying_price=100.0,
        target_delta=0.4,
        today=date.today(),
        request_context=ctx_a,
    )
    ordered_b = _order_chain_for_direct_quote_recovery(
        list(reversed(rows)),
        direction="CALL",
        underlying_price=100.0,
        target_delta=0.4,
        today=date.today(),
        request_context=ctx_b,
    )
    expected = sorted(row["symbol"] for row in rows)
    assert [row["symbol"] for row in ordered_a] == expected
    assert [row["symbol"] for row in ordered_b] == expected


def test_retry_terminal_meta_uses_current_equivalent_truth_fields():
    root = "UNTRADEABLE_FOR_ACCOUNT_SIZE"
    last = "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
    lifecycle = "RETRY_MAX_ATTEMPTS_EXCEEDED"
    audit = {
        "last_candidate_reject_reason": root,
        "reason_code": last,
    }
    scheduled = _build_deferred_retry_schedule_meta(
        reason_code=last,
        selector_audit=audit,
        attempt=4,
        max_attempts=5,
        delay_seconds=8,
        client_id="jason-live",
        execution_mode="live",
        local_order_id="oid-408",
        signal_id="66b1607b-5364-4cd3-9987-2b03a4199517",
    )
    terminal = _build_deferred_retry_terminal_meta(
        terminal_reason=lifecycle,
        reason_code=last,
        selector_audit=scheduled,
        attempt=5,
        max_attempts=5,
        client_id="jason-live",
        execution_mode="live",
        local_order_id="oid-408",
        signal_id="66b1607b-5364-4cd3-9987-2b03a4199517",
    )
    assert scheduled["selector_terminal_reason"] == root
    assert scheduled["operational_reason"] == last
    assert terminal["last_breach_selector_audit"]["selector_terminal_reason"] == root
    assert terminal["last_breach_selector_audit"]["operational_reason"] == last
    assert terminal["deferred_retry_terminal_reason"] == lifecycle
    assert terminal["deferred_retry_reason_code"] == last
