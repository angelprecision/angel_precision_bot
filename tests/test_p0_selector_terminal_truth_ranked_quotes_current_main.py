from __future__ import annotations

import itertools
import os
import time
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

import ap_execution_core as ec_mod
from ap.contract_quote_revalidator import clear_quote_cache
from ap.contract_selector import (
    APContractSelectionEngine,
    SELECTOR_REQUEST_KIND_ORDINARY,
    SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
    SelectorRequestContext,
    _new_selector_request_context,
    _order_chain_for_direct_quote_recovery,
    _structural_direct_quote_skip,
)
from ap_execution_core import APExecutionCore
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
        lambda self, plan, stage, decision, reason_code, explanation, **kwargs: (
            self._set_last_failure({
                "stage": str(stage or ""),
                "reason_code": str(reason_code or "") or "UNKNOWN_REJECTION",
                "explanation": str(explanation or ""),
            })
            if str(decision or "").upper() == "REJECT"
            else None
        ),
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


def _run_selector(
    monkeypatch,
    *,
    plan: dict,
    chain: list[dict],
    limit: int,
    request_kind: str = SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
    valid_symbol: str = "__NO_VALID_DIRECT_QUOTE__",
    valid_quote: dict | None = None,
):
    monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", str(limit))
    broker = _DirectQuoteBroker(
        chain,
        valid_symbol=valid_symbol,
        valid_quote=valid_quote or {"bid": 0.0, "ask": 0.0},
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
        selector_request_kind=request_kind,
    )
    selected = selector.select(plan, request_context=context)
    failure = plan.get("metadata", {}).get("selector_failure") or {}
    diagnostics = failure.get("selection_diagnostics") or {}
    return selected, broker, context, failure, diagnostics


def _execution_plan(
    *,
    execution_mode: str = "LIVE",
    breach_attempt_count: int = 0,
    budget: float = 2000.0,
    ticker: str = "SPY",
    underlying: float = 100.0,
    side: str = "CALL",
) -> SimpleNamespace:
    return SimpleNamespace(
        ticker=ticker,
        side=side,
        contract_symbol=f"DEFERRED:{ticker}",
        limit_price=0.01,
        contracts=1,
        max_position_usd=budget,
        trigger_price=underlying,
        signal_id="sig-pr408-execution-core",
        client_id=(
            "jasoncosby1@gmail.com"
            if execution_mode.upper() == "LIVE"
            else "tradefluencehq@gmail.com"
        ),
        execution_mode=execution_mode,
        metadata={
            "contract_deferred": True,
            "queue_id": 408,
            "breach_attempt_count": breach_attempt_count,
            "sizing_context": {
                "budget": budget,
                "account_equity": 10000.0,
                "risk_pct": 0.20,
                "max_affordable_premium": budget,
            },
        },
    )


def _execution_watched(
    execution_mode: str = "LIVE",
    *,
    ticker: str = "SPY",
    trigger_price: float = 100.0,
    local_order_id: str = "local-pr408-execution-core",
) -> SimpleNamespace:
    client_id = (
        "jasoncosby1@gmail.com"
        if execution_mode.upper() == "LIVE"
        else "tradefluencehq@gmail.com"
    )
    return SimpleNamespace(
        ticker=ticker,
        trigger_price=trigger_price,
        signal={
            "signal_id": "sig-pr408-execution-core",
            "client_id": client_id,
            "local_order_id": local_order_id,
            "queue_id": 408,
            "contract_deferred": True,
            "score": 85,
        },
    )


def _execution_core(selector, broker, execution_mode: str = "LIVE") -> APExecutionCore:
    core = APExecutionCore.__new__(APExecutionCore)
    core.paper = execution_mode.upper() == "PAPER"
    core.mode = execution_mode.upper()
    core.execution_mode = execution_mode.upper()
    core.email = (
        "jasoncosby1@gmail.com"
        if execution_mode.upper() == "LIVE"
        else "tradefluencehq@gmail.com"
    )
    core.client_id = core.email
    core.contract_selector = selector
    core.order_state_machine = MagicMock()
    core.order_state_machine.expire_pending_entry.return_value = True
    core.order_state_machine.transition.return_value = True
    core.order_state_machine.update_order_meta.return_value = True
    core.order_state_machine.schedule_deferred_materialization_retry.return_value = True
    core.order_state_machine.terminalize_deferred_breach.return_value = False
    core.store = MagicMock()
    core.entry_watcher = MagicMock()
    core.exit_eng = MagicMock()
    core.tracker = MagicMock()
    core.position_manager = MagicMock()
    core.broker = broker
    return core


class _ChainFailureBroker(_DirectQuoteBroker):
    def _session_get(self, url, *, params=None, headers=None, timeout=None):
        if "options/chains" in url:
            raise RuntimeError("simulated chain provider outage")
        return super()._session_get(
            url,
            params=params,
            headers=headers,
            timeout=timeout,
        )


def _actual_selector(broker, execution_mode: str = "LIVE") -> APContractSelectionEngine:
    return APContractSelectionEngine(
        broker,
        mode=execution_mode,
        data_broker=broker,
        min_premium=1.0,
        max_premium=1000.0,
        min_oi=1,
        min_volume=0,
    )


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


def test_execution_core_ibm_affordability_terminalizes_without_retry_owner_reschedule(
    monkeypatch,
):
    """Run the historical IBM affordability result through the real owner."""
    monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "1")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_ENABLED", "1")
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "2359")
    monkeypatch.setattr(APExecutionCore, "_breach_risk_check", lambda self, watched: True)
    monkeypatch.setattr(
        "ap.queue.write_deferred_breach_last_error",
        lambda *args, **kwargs: None,
    )

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
    broker = _DirectQuoteBroker(
        [ibm, later_one, later_two],
        valid_symbol="__NO_VALID_DIRECT_QUOTE__",
        valid_quote={"bid": 0.0, "ask": 0.0},
        underlying_price=228.98,
    )
    selector = _actual_selector(broker, execution_mode="LIVE")
    selector.select = MagicMock(wraps=selector.select)
    core = _execution_core(selector, broker, execution_mode="LIVE")
    plan = _execution_plan(
        ticker="IBM",
        underlying=228.98,
        budget=174.71,
        breach_attempt_count=0,
    )
    monkeypatch.setattr(
        APExecutionCore,
        "_recover_plan_for_revalidation",
        lambda self, watched: plan,
    )
    thread_factory = MagicMock()
    thread_factory.return_value = MagicMock()
    monkeypatch.setattr(ec_mod.threading, "Thread", thread_factory)

    result = core._on_entry_trigger(
        _execution_watched(
            "LIVE",
            ticker="IBM",
            trigger_price=228.98,
            local_order_id="local-pr408-ibm",
        )
    )

    assert result["disposition"] == "TERMINAL_DURABLE"
    assert selector.select.call_count == 1
    assert [call.args[0] for call in broker.get_quote.call_args_list] == [
        later_one["symbol"]
    ]
    core.order_state_machine.schedule_deferred_materialization_retry.assert_not_called()
    thread_factory.return_value.start.assert_not_called()

    terminal_updates = [
        call.args[1]
        for call in core.order_state_machine.update_order_meta.call_args_list
        if "last_breach_selector_audit" in call.args[1]
    ]
    assert terminal_updates
    audit = terminal_updates[-1]["last_breach_selector_audit"]
    assert audit["reason_code"] == "UNTRADEABLE_FOR_ACCOUNT_SIZE"
    assert audit["canonical_selector_reason"] == "UNTRADEABLE_FOR_ACCOUNT_SIZE"
    assert audit["last_observed_selector_reason"] == "UNTRADEABLE_FOR_ACCOUNT_SIZE"
    assert audit["selector_terminal_reason"] == "UNTRADEABLE_FOR_ACCOUNT_SIZE"
    assert audit["operational_reason"] == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
    assert audit["selection_diagnostics"]["direct_quote_budget"]["used"] == 1
    tradeability = audit["best_rejected_candidate"]["tradeability_diag"]
    assert tradeability["budget"] == pytest.approx(174.71)
    assert tradeability["premium_per_contract_usd"] == pytest.approx(315.0)
    core.order_state_machine.expire_pending_entry.assert_called_once()
    broker.submit_order.assert_not_called()
    broker.cancel_order.assert_not_called()


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


@pytest.mark.parametrize("execution_mode", ["LIVE", "PAPER"])
def test_ordinary_select_equal_ranked_contracts_use_stable_quote_order_with_cap(
    monkeypatch,
    execution_mode,
):
    later_expiry = date.fromisoformat(_NEAR_EXPIRY) + timedelta(days=1)
    while later_expiry.weekday() >= 5:
        later_expiry += timedelta(days=1)
    near = _row("SPY", 101.0, expiration=_NEAR_EXPIRY)
    later = _row("SPY", 101.0, expiration=later_expiry.isoformat())
    expected = min(near["symbol"], later["symbol"])
    plan = _plan(
        ticker="SPY",
        underlying=100.0,
        budget=2000.0,
        client_id=(
            "jasoncosby1@gmail.com"
            if execution_mode == "LIVE"
            else "tradefluencehq@gmail.com"
        ),
        execution_mode=execution_mode,
    )

    selected, broker, context, failure, _ = _run_selector(
        monkeypatch,
        plan=plan,
        chain=[later, near],
        limit=1,
        request_kind=SELECTOR_REQUEST_KIND_ORDINARY,
        valid_symbol=expected,
        valid_quote={
            "bid": 1.10,
            "ask": 1.14,
            "volume": 300,
            "open_interest": 1200,
        },
    )

    assert selected is not None
    assert selected.contract_symbol == expected
    assert failure == {}
    assert [call.args[0] for call in broker.get_quote.call_args_list] == [expected]
    assert context.selector_request_kind == SELECTOR_REQUEST_KIND_ORDINARY
    assert context.provider_call_counts["direct_quote_calls"] == 1
    assert [row["symbol"] for row in context.direct_quote_candidate_ranking] == [
        expected,
        later["symbol"] if expected == near["symbol"] else near["symbol"],
    ]


def test_duplicate_occ_rows_resolve_quality_before_quote_budget_and_stay_stable(
    monkeypatch,
):
    # The same OCC arrives once with a zero/stale chain quote and once with a
    # usable quote but weaker liquidity and unrelated provider metadata. The
    # usable financial representation must win quality resolution regardless
    # of payload order; the duplicate must not consume a second direct quote.
    duplicate_a = _row("SPY", 101.0, oi=5000, volume=1000)
    duplicate_a["_provider_index"] = 0
    duplicate_a["provider_metadata"] = {"source": "feed-a", "page": 1}
    duplicate_b = dict(duplicate_a)
    duplicate_b["_provider_index"] = 1
    duplicate_b["symbol"] = f" {duplicate_a['symbol'].lower()} "
    duplicate_b["bid"] = 1.10
    duplicate_b["ask"] = 1.11
    duplicate_b["open_interest"] = 500
    duplicate_b["volume"] = 100
    duplicate_b["provider_metadata"] = {"source": "feed-b", "page": 99}
    next_rank = _row("SPY", 102.0)
    valid_symbol = duplicate_a["symbol"]
    plan = _plan(ticker="SPY", underlying=100.0, budget=2000.0)
    observed = []

    for chain in ([duplicate_a, next_rank, duplicate_b], [duplicate_b, next_rank, duplicate_a]):
        clear_quote_cache()
        selected, broker, context, failure, _ = _run_selector(
            monkeypatch,
            plan=plan,
            chain=list(chain),
            limit=1,
            valid_symbol=valid_symbol,
            valid_quote={
                "bid": 0.0,
                "ask": 0.0,
            },
        )
        assert selected is not None
        assert selected.contract_symbol == valid_symbol
        assert failure == {}
        assert [call.args[0] for call in broker.get_quote.call_args_list] == [
            valid_symbol
        ]
        assert context.direct_quote_duplicate_symbols == [valid_symbol]
        assert context.provider_call_counts["direct_quote_calls"] == 1
        observed.append(
            (
                [row["symbol"] for row in context.direct_quote_candidate_ranking],
                [call.args[0] for call in broker.get_quote.call_args_list],
                context.provider_call_counts["direct_quote_calls"],
            )
        )

    assert observed[0] == observed[1]
    assert len(observed[0][0]) == len(observed[1][0])
    assert observed[0][2] == observed[1][2] == 1


@pytest.mark.parametrize(
    ("breach_attempt_count", "expected_disposition"),
    [(0, "RETRY_WAIT"), (5, "TERMINAL_DURABLE")],
)
def test_execution_core_real_selector_failure_retries_or_terminalizes_with_truth_fields(
    monkeypatch,
    breach_attempt_count,
    expected_disposition,
):
    monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "1")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_ENABLED", "1")
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_DELAY_SECONDS", "1")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "2359")
    monkeypatch.setattr(APExecutionCore, "_breach_risk_check", lambda self, watched: True)
    monkeypatch.setattr(
        "ap.queue.write_deferred_breach_last_error",
        lambda *args, **kwargs: None,
    )

    # Every row is a real OCC candidate with a zero chain quote. The actual
    # selector reaches the production chain-row failure path, and the outer
    # execution core must preserve that reason while retrying or terminalizing.
    chain = [_row("SPY", strike) for strike in (101.0, 102.0, 103.0)]
    broker = _DirectQuoteBroker(
        chain,
        valid_symbol="__NO_VALID_DIRECT_QUOTE__",
        valid_quote={"bid": 0.0, "ask": 0.0},
        underlying_price=100.0,
    )
    selector = _actual_selector(broker, execution_mode="LIVE")
    core = _execution_core(selector, broker, execution_mode="LIVE")
    plan = _execution_plan(breach_attempt_count=breach_attempt_count)
    monkeypatch.setattr(
        APExecutionCore,
        "_recover_plan_for_revalidation",
        lambda self, watched: plan,
    )
    thread_factory = MagicMock()
    thread_factory.return_value = MagicMock()
    monkeypatch.setattr(ec_mod.threading, "Thread", thread_factory)

    result = core._on_entry_trigger(_execution_watched("LIVE"))

    assert result["disposition"] == expected_disposition
    core.order_state_machine.submit_existing_entry.assert_not_called()
    assert broker.submit_order.call_count == 0
    assert broker.cancel_order.call_count == 0

    if expected_disposition == "RETRY_WAIT":
        core.order_state_machine.schedule_deferred_materialization_retry.assert_called_once()
        thread_factory.return_value.start.assert_not_called()
        schedule_call = core.order_state_machine.schedule_deferred_materialization_retry.call_args
        selector_failure = schedule_call.kwargs["selector_failure"]
        assert selector_failure["reason_code"] == (
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )
        assert selector_failure["canonical_selector_reason"] == (
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )
        assert selector_failure["last_observed_selector_reason"] == (
            "CHAIN_ROW_ZERO_BID_ASK"
        )
        assert selector_failure["selector_terminal_reason"] == (
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )
        assert selector_failure["operational_reason"] == (
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )
        assert selector_failure["retry_class"] == "OPERATIONAL_REQUEST_BUDGET"
        assert selector_failure["materialization_outcome"] == (
            "RETRY_LATER_SELECTOR_BUDGET"
        )
    else:
        thread_factory.return_value.start.assert_not_called()
        terminal_updates = [
            call.args[1]
            for call in core.order_state_machine.update_order_meta.call_args_list
            if "last_breach_selector_audit" in call.args[1]
        ]
        assert terminal_updates
        selector_failure = terminal_updates[-1]["last_breach_selector_audit"]
        assert selector_failure["reason_code"] == (
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )
        assert selector_failure["canonical_selector_reason"] == (
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )
        assert selector_failure["operational_reason"] == (
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )
        core.order_state_machine.expire_pending_entry.assert_called_once()


def test_execution_core_real_selector_provider_failure_terminalizes_without_fake_failure_payload(
    monkeypatch,
):
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_ENABLED", "1")
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "2359")
    monkeypatch.setattr(APExecutionCore, "_breach_risk_check", lambda self, watched: True)
    monkeypatch.setattr(
        "ap.queue.write_deferred_breach_last_error",
        lambda *args, **kwargs: None,
    )
    broker = _ChainFailureBroker(
        [],
        valid_symbol="__NO_VALID_DIRECT_QUOTE__",
        valid_quote={"bid": 0.0, "ask": 0.0},
        underlying_price=100.0,
    )
    selector = _actual_selector(broker, execution_mode="LIVE")
    core = _execution_core(selector, broker, execution_mode="LIVE")
    plan = _execution_plan(breach_attempt_count=5)
    monkeypatch.setattr(
        APExecutionCore,
        "_recover_plan_for_revalidation",
        lambda self, watched: plan,
    )
    thread_factory = MagicMock()
    thread_factory.return_value = MagicMock()
    monkeypatch.setattr(ec_mod.threading, "Thread", thread_factory)

    result = core._on_entry_trigger(_execution_watched("LIVE"))

    assert result["disposition"] == "TERMINAL_DURABLE"
    assert selector.get_last_failure()["reason_code"] == "CHAIN_PROVIDER_ERROR"
    terminal_updates = [
        call.args[1]
        for call in core.order_state_machine.update_order_meta.call_args_list
        if "last_breach_selector_audit" in call.args[1]
    ]
    assert terminal_updates
    selector_failure = terminal_updates[-1]["last_breach_selector_audit"]
    assert selector_failure["canonical_selector_reason"] == "CHAIN_PROVIDER_ERROR"
    assert selector_failure["last_observed_selector_reason"] == "CHAIN_PROVIDER_ERROR"
    assert selector_failure["selector_terminal_reason"] == "CHAIN_PROVIDER_ERROR"
    assert selector_failure["operational_reason"] is None
    thread_factory.return_value.start.assert_not_called()
    core.order_state_machine.expire_pending_entry.assert_called_once()
    broker.submit_order.assert_not_called()
    broker.cancel_order.assert_not_called()


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
