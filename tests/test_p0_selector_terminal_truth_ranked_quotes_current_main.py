from __future__ import annotations

import copy
import itertools
import os
import time
from datetime import date, datetime, timedelta, timezone
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
from ap.selector_retry_policy import (
    load_selector_recovery_cursor,
    new_selector_recovery_cursor,
    record_selector_recovery_attempt,
    resolve_selector_recovery_final_reason,
)
from ap_execution_core import (
    _build_deferred_retry_schedule_meta,
    _build_deferred_retry_terminal_meta,
)
import ap.contract_selector as selector_mod
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
    recovery_cursor: dict | None = None,
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
        recovery_cursor=recovery_cursor,
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


class _ReleaseReplayOSM:
    """Small durable-row harness for the immutable PR #439 replay."""

    def __init__(self, row: dict):
        self.client_id = str(row["client_id"])
        self.row = copy.deepcopy(row)
        self.claim_calls = []
        self.cursor_calls = []
        self.schedule_calls = []
        self.submit_existing_entry = MagicMock()
        self.expire_pending_entry = MagicMock(return_value=False)
        self.terminalize_deferred_breach = MagicMock(return_value=False)
        self.transition = MagicMock(return_value=True)

    def get_order(self, local_order_id):
        assert local_order_id == self.row["local_order_id"]
        return copy.deepcopy(self.row)

    def update_order_meta(self, local_order_id, patch):
        assert local_order_id == self.row["local_order_id"]
        self.row.setdefault("meta", {}).update(copy.deepcopy(patch or {}))
        return True

    def claim_deferred_materialization(self, local_order_id, **kwargs):
        assert local_order_id == self.row["local_order_id"]
        self.claim_calls.append(copy.deepcopy(kwargs))
        meta = self.row.setdefault("meta", {})
        target_generation = int(
            kwargs.get("new_generation")
            if kwargs.get("new_generation") is not None
            else kwargs.get("generation") or 0
        )
        expected_previous = target_generation - 1
        if int(meta.get("materialization_generation") or 0) != expected_previous:
            return False
        if str(meta.get("lifecycle_state") or "") not in {"", "RETRY_WAIT"}:
            return False
        meta.update({
            "lifecycle_state": "MATERIALIZING",
            "materialization_status": "RUNNING",
            "materialization_in_flight": True,
            "materialization_owner": kwargs["owner"],
            "watcher_token": kwargs["owner"],
            "materialization_generation": target_generation,
            "broker_ready": False,
        })
        # Mirror the production claim's aligned durable attempt counters. The
        # release replay asserts the callback's pre-claim counter fencing.
        if kwargs.get("retry_attempt") is not None:
            retry_attempt = int(kwargs["retry_attempt"])
            meta["retry_attempt"] = retry_attempt
            meta["breach_attempt_count"] = retry_attempt
            meta["materialization_attempts"] = retry_attempt
        return True

    def persist_selector_recovery_cursor(self, local_order_id, **kwargs):
        assert local_order_id == self.row["local_order_id"]
        self.cursor_calls.append(copy.deepcopy(kwargs))
        meta = self.row.setdefault("meta", {})
        if (
            meta.get("lifecycle_state") != "MATERIALIZING"
            or meta.get("materialization_owner") != kwargs["owner"]
            or int(meta.get("materialization_generation") or 0)
            != int(kwargs["generation"])
        ):
            return False
        meta["selector_recovery_cursor_v1"] = copy.deepcopy(kwargs["cursor"])
        return True

    def schedule_deferred_materialization_retry(self, local_order_id, **kwargs):
        assert local_order_id == self.row["local_order_id"]
        self.schedule_calls.append(copy.deepcopy(kwargs))
        meta = self.row.setdefault("meta", {})
        if (
            meta.get("lifecycle_state") != "MATERIALIZING"
            or meta.get("materialization_owner") != kwargs["owner"]
            or int(meta.get("materialization_generation") or 0)
            != int(kwargs["generation"])
        ):
            return False
        meta.update({
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_in_flight": False,
            "materialization_owner": "",
            "watcher_token": "",
            "materialization_lease_until": "",
            "materialization_generation": int(kwargs["generation"]),
            "retry_reason": kwargs["reason_code"],
            "materialization_reason": kwargs["reason_code"],
            "retry_attempt": int(kwargs["attempt"]),
            "breach_attempt_count": int(kwargs["attempt"]),
            "materialization_attempts": int(kwargs["attempt"]),
            "retry_max_attempts": int(kwargs["max_attempts"]),
            "next_retry_at": kwargs["next_retry_at"],
            "materialization_next_retry_at": kwargs["next_retry_at"],
            "retry_owner": kwargs["owner"],
            "current_owner": kwargs["owner"],
            "materialization_outcome": kwargs["selector_failure"].get(
                "materialization_outcome"
            ),
            "materialization_detail": kwargs["selector_failure"].get(
                "materialization_detail"
            ),
            "entry_path": kwargs["selector_failure"].get("entry_path"),
            "materialization_selector_failure": copy.deepcopy(
                kwargs["selector_failure"]
            ),
            "broker_ready": False,
        })
        if isinstance(kwargs.get("selector_recovery_cursor"), dict):
            meta["selector_recovery_cursor_v1"] = copy.deepcopy(
                kwargs["selector_recovery_cursor"]
            )
        return True


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


def test_pr439_retryable_quote_evidence_outranks_unrelated_structural_moneyness():
    reason = resolve_selector_recovery_final_reason(
        {
            "structural_skip_results": {
                "FAR_OTM": "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            },
            "attempted_results": {
                "NEAR_ATM": {
                    "result_reason": "DIRECT_QUOTE_ZERO_BID_ASK",
                    "transient": True,
                },
            },
            "eligible_unattempted_symbols": [],
        }
    )
    assert reason == "DIRECT_QUOTE_ZERO_BID_ASK"


def test_pr439_full_structural_moneyness_set_remains_terminal():
    reason = resolve_selector_recovery_final_reason(
        {
            "structural_skip_results": {
                "FAR_OTM_1": "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
                "FAR_OTM_2": "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            },
            "eligible_unattempted_symbols": [],
        }
    )
    assert reason == "MONEYNESS_OUT_OF_RANGE"


def test_pr439_structural_moneyness_waits_for_unattempted_candidate_accounting():
    reason = resolve_selector_recovery_final_reason(
        {
            "structural_skip_results": {
                "FAR_OTM": "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            },
            "eligible_unattempted_symbols": ["NEAR_ATM"],
        }
    )
    assert reason == "UNKNOWN_SELECTOR_RECOVERY_FAILURE"


def test_pr439_candidate_scoped_quality_does_not_veto_retryable_candidate():
    reason = resolve_selector_recovery_final_reason(
        {
            "candidate_outcomes": {
                "NEAR_LOW_OI": "OI_TOO_LOW",
                "NEAR_ZERO_QUOTE": "DIRECT_QUOTE_ZERO_BID_ASK",
                "FAR_OTM": "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            },
            "structural_skip_results": {
                "FAR_OTM": "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            },
            "candidate_accounting_complete": True,
            "eligible_unattempted_symbols": [],
        }
    )
    assert reason == "DIRECT_QUOTE_ZERO_BID_ASK"


def test_pr439_candidate_scoped_terminal_quality_requires_complete_accounting():
    reason = resolve_selector_recovery_final_reason(
        {
            "candidate_outcomes": {
                "NEAR_LOW_OI": "OI_TOO_LOW",
                "FAR_OTM": "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            },
            "structural_skip_results": {
                "FAR_OTM": "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            },
            "candidate_accounting_complete": True,
            "eligible_unattempted_symbols": [],
        }
    )
    assert reason == "OI_TOO_LOW"


def test_pr439_candidate_scoped_structural_reason_requires_complete_accounting():
    reason = resolve_selector_recovery_final_reason(
        {
            "candidate_outcomes": {
                "FAR_OTM": "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            },
            "structural_skip_results": {
                "FAR_OTM": "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            },
            "candidate_accounting_complete": False,
            "eligible_unattempted_symbols": [],
        }
    )
    assert reason == "UNKNOWN_SELECTOR_RECOVERY_FAILURE"


@pytest.mark.parametrize(
    ("old_reason", "current_reason", "expected"),
    [
        (
            "DIRECT_QUOTE_ZERO_BID_ASK",
            "OI_TOO_LOW",
            "OI_TOO_LOW",
        ),
        (
            "OI_TOO_LOW",
            "DIRECT_QUOTE_ZERO_BID_ASK",
            "DIRECT_QUOTE_ZERO_BID_ASK",
        ),
    ],
)
def test_pr439_disappeared_cursor_symbol_cannot_vote_on_current_reason(
    old_reason, current_reason, expected
):
    """A refreshed chain owns the final reason; the cursor remains audit data."""
    old_symbol = "SPY260801C00101000"
    current_symbol = "SPY260801C00102000"
    reason = resolve_selector_recovery_final_reason(
        {
            "attempted_results": {
                old_symbol: {
                    "result_reason": old_reason,
                    "transient": old_reason != "OI_TOO_LOW",
                },
            },
            "candidate_outcomes": {current_symbol: current_reason},
            "current_candidate_universe": [current_symbol],
            "candidate_accounting_complete": True,
            "eligible_unattempted_symbols": [],
        }
    )
    assert reason == expected


@pytest.mark.parametrize(
    ("old_reason", "current_reason", "expected"),
    [
        (
            "DIRECT_QUOTE_ZERO_BID_ASK",
            "OI_TOO_LOW",
            "OI_TOO_LOW",
        ),
        (
            "OI_TOO_LOW",
            "DIRECT_QUOTE_ZERO_BID_ASK",
            "DIRECT_QUOTE_ZERO_BID_ASK",
        ),
    ],
)
def test_pr439_disappeared_cursor_symbol_stays_ignored_after_reload_restart(
    old_reason, current_reason, expected
):
    """Reloaded durable history must not regain current-reason authority."""
    old_symbol = "SPY260801C00101000"
    current_symbol = "SPY260801C00102000"
    cursor = new_selector_recovery_cursor(
        local_order_id="oid-pr439-restart",
        client_id="jason-live",
        execution_mode="live",
        signal_id="sig-pr439-restart",
        materialization_generation=1,
        selector_attempt_count=1,
    )
    cursor = record_selector_recovery_attempt(
        cursor,
        symbol=old_symbol,
        attempt_number=1,
        expiration=_NEAR_EXPIRY,
        result_reason=old_reason,
        transient=old_reason != "OI_TOO_LOW",
    )
    reloaded, load_reason = load_selector_recovery_cursor(
        cursor,
        local_order_id="oid-pr439-restart",
        client_id="jason-live",
        execution_mode="LIVE",
        signal_id="sig-pr439-restart",
        materialization_generation=1,
        selector_attempt_count=1,
    )
    assert load_reason is None
    assert old_symbol in reloaded["attempted_symbols"]

    reason = resolve_selector_recovery_final_reason(
        {
            "attempted_results": reloaded["attempted_symbols"],
            "candidate_outcomes": {current_symbol: current_reason},
            "current_candidate_universe": [current_symbol],
            "candidate_accounting_complete": True,
            "eligible_unattempted_symbols": [],
        }
    )
    assert reason == expected


@pytest.mark.parametrize(
    ("old_reason", "current_row", "expected"),
    [
        (
            "DIRECT_QUOTE_ZERO_BID_ASK",
            _row("SPY", 102.0, bid=1.10, ask=1.14, oi=0, volume=0),
            "OI_TOO_LOW",
        ),
        (
            "OI_TOO_LOW",
            _row("SPY", 102.0, bid=0.0, ask=0.0, oi=1200, volume=300),
            "DIRECT_QUOTE_ZERO_BID_ASK",
        ),
    ],
)
def test_pr439_selector_callsite_keeps_cursor_audit_but_filters_final_reason(
    monkeypatch, old_reason, current_row, expected
):
    """The production selector seam applies the current-universe filter."""
    plan = _plan(ticker="SPY", underlying=100.0, budget=2000.0)
    old_symbol = _row("SPY", 101.0)["symbol"]
    cursor = new_selector_recovery_cursor(
        local_order_id="oid-pr439-callsite",
        client_id=plan["client_id"],
        execution_mode=plan["execution_mode"],
        signal_id=plan["signal_id"],
        materialization_generation=1,
        selector_attempt_count=1,
    )
    cursor = record_selector_recovery_attempt(
        cursor,
        symbol=old_symbol,
        attempt_number=1,
        expiration=_NEAR_EXPIRY,
        result_reason=old_reason,
        transient=old_reason == "DIRECT_QUOTE_ZERO_BID_ASK",
    )

    selected, broker, context, failure, _ = _run_selector(
        monkeypatch,
        plan=plan,
        chain=[current_row],
        limit=1,
        recovery_cursor=cursor,
    )

    assert selected is None
    assert failure["reason_code"] == expected
    assert old_symbol in context.recovery_cursor["attempted_symbols"]
    assert broker.submit_order.call_count == 0
    assert broker.cancel_order.call_count == 0


def test_pr439_pro_quality_unavailable_recovery_owns_candidate_reason(monkeypatch):
    """The real PRO-quality reduction keeps a retryable recovery disposition.

    The recovery result is supplied at the direct-quote seam so the test
    exercises the production branch that chooses between the pre-recovery PRO
    quality reason and the later recovery reason; it does not mock the
    candidate accounting or final reducer.
    """
    calls = []

    monkeypatch.setattr(
        selector_mod,
        "_pro_contract_quality",
        lambda opt, ticker, dte: ("REJECT", "low_oi_0"),
    )

    def _unavailable(*args, **kwargs):
        calls.append(args[1]["symbol"])
        return {
            "action": "REJECT_UNAVAILABLE",
            "reason_code": "DIRECT_QUOTE_UNAVAILABLE",
            "direct_quote_used": False,
            "opt_updated": None,
            "audit": {},
        }

    monkeypatch.setattr(selector_mod, "_revalidate_direct", _unavailable)
    candidate = _row("SPY", 101.0, bid=0.0, ask=0.0, oi=0, volume=0)
    selected, broker, context, failure, diagnostics = _run_selector(
        monkeypatch,
        plan=_plan(ticker="SPY", underlying=100.0, budget=2000.0),
        chain=[candidate],
        limit=1,
    )

    assert selected is None
    assert calls == [candidate["symbol"]]
    assert failure["reason_code"] == "DIRECT_QUOTE_UNAVAILABLE"
    assert diagnostics["candidate_outcomes"] == {
        candidate["symbol"]: "DIRECT_QUOTE_UNAVAILABLE"
    }
    assert diagnostics["candidate_accounting"] == {
        "universe_count": 1,
        "accounted_count": 1,
        "complete": True,
    }
    assert broker.submit_order.call_count == 0
    assert broker.cancel_order.call_count == 0


def test_pr439_pro_quality_partial_pass_without_authoritative_quote_fails_closed(
    monkeypatch,
):
    """A partial PASS result cannot restore the earlier terminal quality veto."""
    monkeypatch.setattr(
        selector_mod,
        "_pro_contract_quality",
        lambda opt, ticker, dte: ("REJECT", "low_oi_0"),
    )
    monkeypatch.setattr(
        selector_mod,
        "_revalidate_direct",
        lambda *args, **kwargs: {
            "action": "PASS",
            "reason_code": "DIRECT_QUOTE_UNAVAILABLE",
            "direct_quote_used": False,
            "opt_updated": None,
            "audit": {},
        },
    )
    candidate = _row("SPY", 101.0, bid=0.0, ask=0.0, oi=0, volume=0)
    selected, broker, context, failure, diagnostics = _run_selector(
        monkeypatch,
        plan=_plan(ticker="SPY", underlying=100.0, budget=2000.0),
        chain=[candidate],
        limit=1,
    )

    assert selected is None
    assert failure["reason_code"] == "DIRECT_QUOTE_UNAVAILABLE"
    assert diagnostics["candidate_outcomes"][candidate["symbol"]] == (
        "DIRECT_QUOTE_UNAVAILABLE"
    )
    assert broker.submit_order.call_count == 0
    assert broker.cancel_order.call_count == 0


def test_pr439_pro_quality_retryable_peer_beats_terminal_quality_peer(monkeypatch):
    """A retryable PRO recovery candidate cannot be vetoed by a terminal peer."""
    pro_calls = {}

    def _pro_quality(opt, ticker, dte):
        pro_calls[opt["symbol"]] = pro_calls.get(opt["symbol"], 0) + 1
        return "REJECT", "low_oi_0"

    monkeypatch.setattr(
        selector_mod,
        "_pro_contract_quality",
        _pro_quality,
    )
    retryable_symbol = _row("SPY", 101.0, bid=0.0, ask=0.0, oi=0, volume=0)["symbol"]
    terminal_symbol = _row("SPY", 102.0, bid=0.0, ask=0.0, oi=0, volume=0)["symbol"]

    def _recover(*args, **kwargs):
        opt = args[1]
        if opt["symbol"] == retryable_symbol:
            return {
                "action": "REJECT_UNAVAILABLE",
                "reason_code": "DIRECT_QUOTE_UNAVAILABLE",
                "direct_quote_used": False,
                "opt_updated": None,
                "audit": {},
            }
        patched = dict(opt)
        patched.update({"bid": 1.10, "ask": 1.14})
        return {
            "action": "PASS",
            "reason_code": "DIRECT_QUOTE_RECOVERED_CHAIN_ZERO",
            "direct_quote_used": True,
            "opt_updated": patched,
            "audit": {"direct_bid": 1.10, "direct_ask": 1.14},
        }

    monkeypatch.setattr(selector_mod, "_revalidate_direct", _recover)
    candidates = [
        _row("SPY", 101.0, bid=0.0, ask=0.0, oi=0, volume=0),
        _row("SPY", 102.0, bid=0.0, ask=0.0, oi=0, volume=0),
    ]
    selected, broker, context, failure, diagnostics = _run_selector(
        monkeypatch,
        plan=_plan(ticker="SPY", underlying=100.0, budget=2000.0),
        chain=candidates,
        limit=2,
    )

    assert selected is None
    assert failure["reason_code"] == "DIRECT_QUOTE_UNAVAILABLE"
    assert diagnostics["candidate_outcomes"] == {
        retryable_symbol: "DIRECT_QUOTE_UNAVAILABLE",
        terminal_symbol: "OI_TOO_LOW",
    }
    assert diagnostics["candidate_accounting"] == {
        "universe_count": 2,
        "accounted_count": 2,
        "complete": True,
    }
    assert broker.submit_order.call_count == 0
    assert broker.cancel_order.call_count == 0


def test_pr439_successful_pro_recovery_then_fresh_terminal_quality_is_terminal(
    monkeypatch,
):
    """Fresh authoritative quote evidence permits a genuine quality terminal."""
    monkeypatch.setattr(
        selector_mod,
        "_pro_contract_quality",
        lambda opt, ticker, dte: ("REJECT", "low_oi_0"),
    )

    def _recover(*args, **kwargs):
        opt = dict(args[1])
        opt.update({"bid": 1.10, "ask": 1.14})
        return {
            "action": "PASS",
            "reason_code": "DIRECT_QUOTE_RECOVERED_CHAIN_ZERO",
            "direct_quote_used": True,
            "opt_updated": opt,
            "audit": {"direct_bid": 1.10, "direct_ask": 1.14},
        }

    monkeypatch.setattr(selector_mod, "_revalidate_direct", _recover)
    candidates = [
        _row("SPY", 101.0, bid=0.0, ask=0.0, oi=0, volume=0),
        _row("SPY", 102.0, bid=0.0, ask=0.0, oi=0, volume=0),
    ]
    selected, broker, context, failure, diagnostics = _run_selector(
        monkeypatch,
        plan=_plan(ticker="SPY", underlying=100.0, budget=2000.0),
        chain=candidates,
        limit=2,
    )

    assert selected is None
    assert failure["reason_code"] == "OI_TOO_LOW"
    assert set(diagnostics["candidate_outcomes"].values()) == {"OI_TOO_LOW"}
    assert diagnostics["candidate_accounting"]["complete"] is True
    assert broker.submit_order.call_count == 0
    assert broker.cancel_order.call_count == 0


@pytest.mark.parametrize(
    ("candidate_outcomes", "expected"),
    [
        ({"A": "OI_TOO_LOW", "B": "SPREAD_TOO_WIDE"}, "OI_TOO_LOW"),
        ({"A": "DIRECT_QUOTE_UNAVAILABLE", "B": "OI_TOO_LOW"}, "DIRECT_QUOTE_UNAVAILABLE"),
        ({"A": "OI_TOO_LOW", "B": "DIRECT_QUOTE_ZERO_BID_ASK"}, "DIRECT_QUOTE_ZERO_BID_ASK"),
        ({"A": "DIRECT_QUOTE_UNAVAILABLE", "B": "DIRECT_QUOTE_ZERO_BID_ASK"}, "DIRECT_QUOTE_UNAVAILABLE"),
        ({"A": "OI_TOO_LOW", "B": "OI_TOO_LOW"}, "OI_TOO_LOW"),
    ],
)
def test_pr439_candidate_scoped_authority_matrix(candidate_outcomes, expected):
    """The reducer applies retryable authority across every candidate ordering."""
    assert resolve_selector_recovery_final_reason({
        "candidate_outcomes": candidate_outcomes,
        "candidate_accounting_complete": True,
        "eligible_unattempted_symbols": [],
    }) == expected


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
    duplicate_b["open_interest"] = duplicate_a["open_interest"]
    duplicate_b["volume"] = duplicate_a["volume"]
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


def test_conflicting_valid_duplicate_occ_quotes_use_one_authority_and_fail_closed(
    monkeypatch,
):
    """A cheap duplicate must not make an unaffordable OCC appear affordable."""
    duplicate_a = _row(
        "SPY", 101.0, bid=1.45, ask=1.50, oi=5000, volume=1000
    )
    duplicate_a["_provider_index"] = 0
    duplicate_a["provider_metadata"] = {"source": "feed-a", "page": 1}
    duplicate_b = dict(duplicate_a)
    duplicate_b["_provider_index"] = 1
    duplicate_b["symbol"] = f" {duplicate_a['symbol'].lower()} "
    duplicate_b["bid"] = 2.95
    duplicate_b["ask"] = 3.15
    duplicate_b["open_interest"] = 1
    duplicate_b["volume"] = 1
    duplicate_b["provider_metadata"] = {"source": "feed-b", "page": 99}
    interleaver = _row("SPY", 102.0)
    later = _row("SPY", 103.0)
    valid_symbol = duplicate_a["symbol"]
    execution_mode = "LIVE"
    observed = []

    chains = (
        [duplicate_a, interleaver, duplicate_b, later],
        [duplicate_b, interleaver, duplicate_a, later],
        [interleaver, duplicate_a, later, duplicate_b],
    )
    for chain in chains:
        clear_quote_cache()
        plan = _plan(
            ticker="SPY",
            underlying=100.0,
            budget=174.71,
            client_id=(
                "jasoncosby1@gmail.com"
                if execution_mode == "LIVE"
                else "tradefluencehq@gmail.com"
            ),
            execution_mode=execution_mode,
        )
        selected, broker, context, failure, diagnostics = _run_selector(
            monkeypatch,
            plan=plan,
            chain=list(chain),
            limit=1,
            request_kind=SELECTOR_REQUEST_KIND_ORDINARY,
            valid_symbol=valid_symbol,
            valid_quote={
                "bid": 2.95,
                "ask": 3.15,
                "volume": 300,
                "open_interest": 1200,
            },
        )

        assert selected is None
        assert failure["reason_code"] == "UNTRADEABLE_FOR_ACCOUNT_SIZE"
        assert failure["canonical_selector_reason"] == (
            "UNTRADEABLE_FOR_ACCOUNT_SIZE"
        )
        assert failure["operational_reason"] == (
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )
        assert diagnostics["direct_quote_budget"]["used"] == 1
        assert diagnostics["duplicate_quote_authority_symbols"] == [valid_symbol]
        assert diagnostics["duplicate_quote_conflicts"][0]["authority"] == (
            "DIRECT_QUOTE_NORMALIZED_OCC"
        )
        assert [call.args[0] for call in broker.get_quote.call_args_list] == [
            valid_symbol
        ]
        assert broker.submit_order.call_count == 0
        assert broker.cancel_order.call_count == 0
        best = failure["best_rejected_candidate"]
        assert best["tradeability_diag"]["premium_per_contract_usd"] == 315.0
        observed.append(
            (
                failure["reason_code"],
                failure["canonical_selector_reason"],
                failure["operational_reason"],
                diagnostics["direct_quote_budget"]["used"],
                best["tradeability_diag"]["premium_per_contract_usd"],
            )
        )

    assert observed == [observed[0]] * len(observed)


def test_small_duplicate_ask_boundary_requires_authority_in_all_payload_orders(
    monkeypatch,
):
    """A ten-cent ask gap can still change LIVE affordability."""
    duplicate_a = _row(
        "SPY", 101.0, bid=1.45, ask=1.50, oi=1200, volume=300
    )
    duplicate_b = dict(duplicate_a)
    duplicate_b["symbol"] = f" {duplicate_a['symbol'].lower()} "
    duplicate_b["bid"] = 1.55
    duplicate_b["ask"] = 1.60
    duplicate_b["provider_metadata"] = {"source": "untrusted-feed", "page": 2}
    interleaver = _row("SPY", 102.0)
    later = _row("SPY", 103.0)
    valid_symbol = duplicate_a["symbol"]
    observed = []

    for chain in (
        [duplicate_a, interleaver, duplicate_b, later],
        [duplicate_b, interleaver, duplicate_a, later],
        [interleaver, duplicate_a, later, duplicate_b],
    ):
        clear_quote_cache()
        plan = _plan(
            ticker="SPY",
            underlying=100.0,
            budget=155.0,
            client_id="jasoncosby1@gmail.com",
            execution_mode="LIVE",
        )
        selected, broker, context, failure, diagnostics = _run_selector(
            monkeypatch,
            plan=plan,
            chain=list(chain),
            limit=1,
            request_kind=SELECTOR_REQUEST_KIND_ORDINARY,
            valid_symbol=valid_symbol,
            valid_quote={
                "bid": 1.55,
                "ask": 1.60,
                "volume": 300,
                "open_interest": 1200,
            },
        )

        assert selected is None
        assert failure["reason_code"] == "UNTRADEABLE_FOR_ACCOUNT_SIZE"
        assert failure["canonical_selector_reason"] == (
            "UNTRADEABLE_FOR_ACCOUNT_SIZE"
        )
        assert failure["operational_reason"] == (
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )
        assert failure["best_rejected_candidate"]["tradeability_diag"][
            "premium_per_contract_usd"
        ] == 160.0
        assert diagnostics["direct_quote_budget"]["used"] == 1
        assert diagnostics["duplicate_quote_conflict_dimensions"][valid_symbol] == [
            "price"
        ]
        assert [call.args[0] for call in broker.get_quote.call_args_list] == [
            valid_symbol
        ]
        assert broker.submit_order.call_count == 0
        assert broker.cancel_order.call_count == 0
        observed.append(
            (
                failure["reason_code"],
                failure["operational_reason"],
                diagnostics["direct_quote_budget"]["used"],
                failure["best_rejected_candidate"]["tradeability_diag"][
                    "premium_per_contract_usd"
                ],
            )
        )

    assert observed == [observed[0]] * len(observed)


def test_liquidity_only_duplicate_conflict_uses_authoritative_direct_liquidity(
    monkeypatch,
):
    """Duplicate OI/volume cannot be resolved by the higher-liquidity row."""
    high_liquidity = _row(
        "SPY", 101.0, bid=1.45, ask=1.50, oi=1200, volume=300
    )
    low_liquidity = dict(high_liquidity)
    low_liquidity["symbol"] = f" {high_liquidity['symbol'].lower()} "
    low_liquidity["open_interest"] = 10
    low_liquidity["volume"] = 1
    low_liquidity["provider_metadata"] = {"source": "feed-b", "page": 7}
    interleaver = _row("SPY", 102.0)
    valid_symbol = high_liquidity["symbol"]
    observed = []

    for chain in (
        [high_liquidity, interleaver, low_liquidity],
        [low_liquidity, interleaver, high_liquidity],
    ):
        clear_quote_cache()
        plan = _plan(
            ticker="SPY",
            underlying=100.0,
            budget=2000.0,
            client_id="jasoncosby1@gmail.com",
            execution_mode="LIVE",
        )
        selected, broker, context, failure, diagnostics = _run_selector(
            monkeypatch,
            plan=plan,
            chain=list(chain),
            limit=1,
            request_kind=SELECTOR_REQUEST_KIND_ORDINARY,
            valid_symbol=valid_symbol,
            valid_quote={
                "bid": 1.45,
                "ask": 1.50,
                "volume": 300,
                "open_interest": 1200,
            },
        )

        assert selected is not None
        assert selected.contract_symbol == valid_symbol
        assert selected.open_interest == 1200
        assert selected.volume == 300
        assert failure == {}
        assert context.duplicate_quote_conflict_dimensions[valid_symbol] == (
            "open_interest",
            "volume",
        )
        assert [call.args[0] for call in broker.get_quote.call_args_list] == [
            valid_symbol
        ]
        observed.append(
            (
                selected.contract_symbol,
                selected.open_interest,
                selected.volume,
                context.provider_call_counts["direct_quote_calls"],
            )
        )

    assert observed == [observed[0]] * len(observed)


def test_duplicate_liquidity_conflict_with_price_only_direct_quote_fails_closed(
    monkeypatch,
):
    """Price authority without disputed OI/volume must not launder liquidity."""
    high_liquidity = _row(
        "SPY", 101.0, bid=1.45, ask=1.50, oi=1200, volume=300
    )
    low_liquidity = dict(high_liquidity)
    low_liquidity["symbol"] = f" {high_liquidity['symbol'].lower()} "
    low_liquidity["open_interest"] = 10
    low_liquidity["volume"] = 1
    interleaver = _row("SPY", 102.0)
    valid_symbol = high_liquidity["symbol"]
    observed = []

    for chain in (
        [high_liquidity, interleaver, low_liquidity],
        [low_liquidity, interleaver, high_liquidity],
    ):
        clear_quote_cache()
        plan = _plan(
            ticker="SPY",
            underlying=100.0,
            budget=2000.0,
            client_id="jasoncosby1@gmail.com",
            execution_mode="LIVE",
        )
        selected, broker, context, failure, diagnostics = _run_selector(
            monkeypatch,
            plan=plan,
            chain=list(chain),
            limit=1,
            request_kind=SELECTOR_REQUEST_KIND_ORDINARY,
            valid_symbol=valid_symbol,
            valid_quote={"bid": 1.45, "ask": 1.50},
        )

        assert selected is None
        assert failure["reason_code"] == "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
        assert failure["canonical_selector_reason"] == (
            "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
        )
        assert diagnostics["duplicate_quote_authority_failures"][valid_symbol] == (
            "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
        )
        assert diagnostics["direct_quote_budget"]["used"] == 1
        assert [call.args[0] for call in broker.get_quote.call_args_list] == [
            valid_symbol
        ]
        assert broker.submit_order.call_count == 0
        assert broker.cancel_order.call_count == 0
        observed.append(
            (
                failure["reason_code"],
                diagnostics["direct_quote_budget"]["used"],
            )
        )

    assert observed == [observed[0]] * len(observed)


@pytest.mark.parametrize("execution_mode", ["LIVE", "PAPER"])
@pytest.mark.parametrize("conflict_shape", ["in_band_vs_out_of_band", "missing_vs_valid"])
def test_duplicate_delta_conflict_fails_closed_independent_of_payload_order(
    monkeypatch,
    execution_mode,
    conflict_shape,
):
    """Contradictory duplicate Greeks never inherit the favorable row."""
    valid = _row(
        "SPY", 101.0, bid=1.45, ask=1.50, delta=0.39, oi=1200, volume=300
    )
    conflicting = dict(valid)
    conflicting["symbol"] = f" {valid['symbol'].lower()} "
    conflicting["greeks"] = (
        {"delta": 0.79}
        if conflict_shape == "in_band_vs_out_of_band"
        else {}
    )
    interleaver = _row("SPY", 102.0)
    valid_symbol = valid["symbol"]
    observed = []

    for chain in (
        [valid, conflicting, interleaver],
        [conflicting, valid, interleaver],
        [valid, interleaver, conflicting],
        [conflicting, interleaver, valid],
    ):
        clear_quote_cache()
        plan = _plan(
            ticker="SPY",
            underlying=100.0,
            budget=2000.0,
            execution_mode=execution_mode,
            client_id=(
                "jasoncosby1@gmail.com"
                if execution_mode == "LIVE"
                else "tradefluencehq@gmail.com"
            ),
        )
        selected, broker, context, failure, diagnostics = _run_selector(
            monkeypatch,
            plan=plan,
            chain=list(chain),
            limit=1,
            request_kind=SELECTOR_REQUEST_KIND_ORDINARY,
            valid_symbol=valid_symbol,
            # Price and liquidity authority are available, but the deployed
            # Tradier quote lane requests greeks=false. Even a stray greeks
            # field must not be treated as reliable direct-delta authority.
            valid_quote={
                "bid": 1.45,
                "ask": 1.50,
                "volume": 300,
                "open_interest": 1200,
                "greeks": {"delta": 0.40},
            },
        )

        assert selected is None
        assert failure["reason_code"] == "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
        assert failure["canonical_selector_reason"] == (
            "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
        )
        assert failure["data_failure"] is True
        assert failure["quality_failure"] is False
        assert context.duplicate_quote_conflict_dimensions[valid_symbol] == (
            "delta",
        )
        assert diagnostics["duplicate_quote_conflict_dimensions"][valid_symbol] == [
            "delta"
        ]
        assert diagnostics["duplicate_quote_authority_failures"][valid_symbol] == (
            "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
        )
        assert [call.args[0] for call in broker.get_quote.call_args_list] == [
            valid_symbol
        ]
        assert broker.submit_order.call_count == 0
        assert broker.cancel_order.call_count == 0
        observed.append(
            (
                failure["reason_code"],
                failure["data_failure"],
                failure["quality_failure"],
                tuple(diagnostics["duplicate_quote_conflict_dimensions"][valid_symbol]),
            )
        )

    assert observed == [observed[0]] * len(observed)


def test_execution_core_duplicate_liquidity_conflict_schedules_one_durable_retry(
    monkeypatch,
):
    """The real selector-to-owner LIVE path preserves data truth and retries."""
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

    high_liquidity = _row(
        "SPY", 101.0, bid=1.45, ask=1.50, oi=1200, volume=300
    )
    low_liquidity = dict(high_liquidity)
    low_liquidity["symbol"] = f" {high_liquidity['symbol'].lower()} "
    low_liquidity["open_interest"] = 10
    low_liquidity["volume"] = 1
    valid_symbol = high_liquidity["symbol"]
    broker = _DirectQuoteBroker(
        [high_liquidity, low_liquidity],
        valid_symbol=valid_symbol,
        valid_quote={"bid": 1.45, "ask": 1.50},
        underlying_price=100.0,
    )
    selector = _actual_selector(broker, execution_mode="LIVE")
    core = _execution_core(selector, broker, execution_mode="LIVE")
    plan = _execution_plan(breach_attempt_count=0)
    monkeypatch.setattr(
        APExecutionCore,
        "_recover_plan_for_revalidation",
        lambda self, watched: plan,
    )
    thread_factory = MagicMock()
    thread_factory.return_value = MagicMock()
    monkeypatch.setattr(ec_mod.threading, "Thread", thread_factory)

    result = core._on_entry_trigger(_execution_watched("LIVE"))

    assert result["disposition"] == "RETRY_WAIT"
    core.order_state_machine.schedule_deferred_materialization_retry.assert_called_once()
    schedule_call = (
        core.order_state_machine.schedule_deferred_materialization_retry.call_args
    )
    selector_failure = schedule_call.kwargs["selector_failure"]
    assert selector_failure["reason_code"] == "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
    assert selector_failure["canonical_selector_reason"] == (
        "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
    )
    assert selector_failure["selector_terminal_reason"] == (
        "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
    )
    assert selector_failure["data_failure"] is True
    assert selector_failure["quality_failure"] is False
    assert selector_failure["reason_code"] != "CONTRACT_SELECTION_QUALITY_REJECT"
    core.order_state_machine.submit_existing_entry.assert_not_called()
    core.order_state_machine.expire_pending_entry.assert_not_called()
    thread_factory.return_value.start.assert_not_called()
    assert broker.submit_order.call_count == 0
    assert broker.cancel_order.call_count == 0


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


@pytest.mark.parametrize("execution_mode", ["LIVE", "PAPER"])
def test_execution_core_mixed_retryable_quote_and_structural_moneyness_schedules_retry(
    monkeypatch,
    execution_mode,
):
    """The real deferred selector/owner seam preserves retryable quote truth."""
    monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "40")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_ENABLED", "1")
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_DELAY_SECONDS", "1")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "2359")
    monkeypatch.setattr(APExecutionCore, "_breach_risk_check", lambda self, watched: True)
    monkeypatch.setattr(
        "ap.queue.write_deferred_breach_last_error",
        lambda *args, **kwargs: None,
    )

    near_atm = _row("SPY", 101.0)
    far_otm = _row("SPY", 130.0)
    broker = _DirectQuoteBroker(
        [near_atm, far_otm],
        valid_symbol="__NO_VALID_DIRECT_QUOTE__",
        valid_quote={"bid": 0.0, "ask": 0.0},
        underlying_price=100.0,
    )
    selector = _actual_selector(broker, execution_mode=execution_mode)
    core = _execution_core(selector, broker, execution_mode=execution_mode)
    plan = _execution_plan(
        breach_attempt_count=0,
        ticker="SPY",
        underlying=100.0,
        execution_mode=execution_mode,
    )
    monkeypatch.setattr(
        APExecutionCore,
        "_recover_plan_for_revalidation",
        lambda self, watched: plan,
    )
    thread_factory = MagicMock()
    thread_factory.return_value = MagicMock()
    monkeypatch.setattr(ec_mod.threading, "Thread", thread_factory)

    local_order_id = f"local-pr439-mixed-evidence-{execution_mode.lower()}"
    result = core._on_entry_trigger(
        _execution_watched(
            execution_mode,
            ticker="SPY",
            trigger_price=100.0,
            local_order_id=local_order_id,
        )
    )

    assert result["disposition"] == "RETRY_WAIT"
    core.order_state_machine.schedule_deferred_materialization_retry.assert_called_once()
    selector_failure = (
        core.order_state_machine.schedule_deferred_materialization_retry.call_args
        .kwargs["selector_failure"]
    )
    assert selector_failure["reason_code"] == "DIRECT_QUOTE_ZERO_BID_ASK"
    assert selector_failure["canonical_selector_reason"] == (
        "DIRECT_QUOTE_ZERO_BID_ASK"
    )
    assert selector_failure["execution_mode"] == execution_mode
    assert selector_failure["data_failure"] is True
    assert selector_failure["quality_failure"] is False
    assert selector_failure["reason_code"] != "MONEYNESS_OUT_OF_RANGE"
    assert selector_failure["selection_diagnostics"]["structural_skips"]
    assert selector_failure["selection_diagnostics"]["direct_quote_attempted_symbols"] == [
        near_atm["symbol"]
    ]
    assert selector_failure["selection_diagnostics"]["direct_quote_unattempted_count"] == 0
    core.order_state_machine.submit_existing_entry.assert_not_called()
    core.order_state_machine.expire_pending_entry.assert_not_called()
    thread_factory.return_value.start.assert_not_called()
    broker.submit_order.assert_not_called()
    broker.cancel_order.assert_not_called()

    cursor_calls = core.order_state_machine.persist_selector_recovery_cursor.call_args_list
    assert cursor_calls
    cursor_call = cursor_calls[-1]
    assert cursor_call.args[0] == local_order_id
    assert cursor_call.kwargs["signal_id"] == plan.signal_id
    assert cursor_call.kwargs["execution_mode"] == plan.execution_mode
    cursor = cursor_call.kwargs["cursor"]
    assert cursor["attempted_symbols"][near_atm["symbol"]]["result_reason"] == (
        "DIRECT_QUOTE_ZERO_BID_ASK"
    )
    assert cursor["structurally_skipped_symbols"][far_otm["symbol"]]["skip_reason"] == (
        "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"
    )


@pytest.mark.parametrize("execution_mode", ["LIVE", "PAPER"])
def test_execution_core_candidate_scoped_quality_does_not_veto_retryable_quote(
    monkeypatch,
    execution_mode,
):
    """A terminal OI result on one candidate cannot expire a retryable peer."""
    monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "40")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_ENABLED", "1")
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_DELAY_SECONDS", "1")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "2359")
    monkeypatch.setattr(APExecutionCore, "_breach_risk_check", lambda self, watched: True)
    monkeypatch.setattr(
        "ap.queue.write_deferred_breach_last_error",
        lambda *args, **kwargs: None,
    )

    near_low_oi = _row(
        "SPY",
        101.0,
        bid=1.10,
        ask=1.14,
        oi=0,
        volume=0,
    )
    near_zero_quote = _row("SPY", 102.0)
    far_otm = _row("SPY", 130.0)
    broker = _DirectQuoteBroker(
        [near_low_oi, near_zero_quote, far_otm],
        valid_symbol="__NO_VALID_DIRECT_QUOTE__",
        valid_quote={"bid": 0.0, "ask": 0.0},
        underlying_price=100.0,
    )
    selector = _actual_selector(broker, execution_mode=execution_mode)
    core = _execution_core(selector, broker, execution_mode=execution_mode)
    plan = _execution_plan(
        breach_attempt_count=0,
        ticker="SPY",
        underlying=100.0,
        execution_mode=execution_mode,
    )
    monkeypatch.setattr(
        APExecutionCore,
        "_recover_plan_for_revalidation",
        lambda self, watched: plan,
    )
    thread_factory = MagicMock()
    thread_factory.return_value = MagicMock()
    monkeypatch.setattr(ec_mod.threading, "Thread", thread_factory)

    local_order_id = f"local-pr439-candidate-scoped-{execution_mode.lower()}"
    result = core._on_entry_trigger(
        _execution_watched(
            execution_mode,
            ticker="SPY",
            trigger_price=100.0,
            local_order_id=local_order_id,
        )
    )

    assert result["disposition"] == "RETRY_WAIT"
    core.order_state_machine.schedule_deferred_materialization_retry.assert_called_once()
    selector_failure = (
        core.order_state_machine.schedule_deferred_materialization_retry.call_args
        .kwargs["selector_failure"]
    )
    assert selector_failure["reason_code"] == "DIRECT_QUOTE_ZERO_BID_ASK"
    assert selector_failure["data_failure"] is True
    assert selector_failure["quality_failure"] is False
    assert selector_failure["selection_diagnostics"]["candidate_accounting"] == {
        "universe_count": 3,
        "accounted_count": 3,
        "complete": True,
    }
    assert selector_failure["top_reject_buckets"]["OI_TOO_LOW"] == 1
    assert selector_failure["top_reject_buckets"]["DIRECT_QUOTE_ZERO_BID_ASK"] == 1
    assert selector_failure["selection_diagnostics"]["structural_skips"]
    core.order_state_machine.expire_pending_entry.assert_not_called()
    core.order_state_machine.submit_existing_entry.assert_not_called()
    thread_factory.return_value.start.assert_not_called()
    broker.submit_order.assert_not_called()
    broker.cancel_order.assert_not_called()


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


def test_pr439_immutable_release_replay_preserves_retry_ownership_and_blocks_broker(
    monkeypatch,
):
    """Replay one incident-shaped request through durable retry and restart."""
    monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "40")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_ENABLED", "1")
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_DELAY_SECONDS", "1")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "2359")
    monkeypatch.setenv("SELECTOR_DURABLE_RECOVERY_CURSOR_ENABLED", "1")
    risk_calls = []
    monkeypatch.setattr(
        APExecutionCore,
        "_breach_risk_check",
        lambda self, watched: risk_calls.append(watched.signal.get("signal_id")) or True,
    )
    monkeypatch.setattr(
        APExecutionCore,
        "_recover_plan_for_revalidation",
        lambda self, watched: watched.signal.get("_approved_plan") or plan,
    )
    monkeypatch.setattr(
        "ap.queue.write_deferred_breach_last_error",
        lambda *args, **kwargs: None,
    )

    near_zero = _row("SPY", 101.0)
    low_oi = _row("SPY", 102.0, bid=1.10, ask=1.14, oi=0, volume=0)
    far_otm = _row("SPY", 130.0)
    chain = [near_zero, low_oi, far_otm]
    broker = _DirectQuoteBroker(
        chain,
        valid_symbol="__NO_VALID_DIRECT_QUOTE__",
        valid_quote={"bid": 0.0, "ask": 0.0},
        underlying_price=100.0,
    )
    selector = _actual_selector(broker, execution_mode="LIVE")
    core = _execution_core(selector, broker, execution_mode="LIVE")
    plan = _execution_plan(
        execution_mode="LIVE",
        breach_attempt_count=0,
        budget=2000.0,
        ticker="SPY",
        underlying=100.0,
    )
    plan.signal_id = "sig-pr439-release-replay"
    local_order_id = "local-pr439-release-replay"
    watched = _execution_watched(
        "LIVE",
        ticker="SPY",
        trigger_price=100.0,
        local_order_id=local_order_id,
    )
    watched.signal["signal_id"] = plan.signal_id
    watched.signal["_approved_plan"] = plan
    now = datetime.now(timezone.utc)
    row = {
        "local_order_id": local_order_id,
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "signal_id": plan.signal_id,
        "plan_id": "plan-pr439-release-replay",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "symbol": "SPY",
        "direction": "CALL",
        "score": 85.0,
        "tier": "A",
        "trigger_price": 100.0,
        "stop_underlying": 98.0,
        "target_underlying": 103.0,
        "pattern": "3-1-2",
        "timeframe": "5m",
        "contract": "DEFERRED:SPY",
        "qty": 1,
        "limit_price": 0.01,
        "reserved_cost": 1.0,
        "meta": {
            "lifecycle_state": "",
            "materialization_status": "WAITING_FOR_TRIGGER",
            "materialization_generation": 0,
            "retry_attempt": 0,
            "breach_attempt_count": 0,
            "materialization_attempts": 0,
            "retry_max_attempts": 5,
            "broker_ready": False,
            "trigger_crossed_at": now.isoformat(),
            "observed_underlying_price": 100.0,
            "trigger_price": 100.0,
            "client_id": "jasoncosby1@gmail.com",
            "execution_mode": "live",
            "canonical_signal_id": plan.signal_id,
        },
    }
    osm = _ReleaseReplayOSM(row)
    core.order_state_machine = osm

    first = core._on_entry_trigger(watched)
    assert first["disposition"] == "RETRY_WAIT"
    assert first["reason_code"] == "DIRECT_QUOTE_ZERO_BID_ASK"
    assert len(osm.schedule_calls) == 1
    schedule = osm.schedule_calls[0]
    selector_failure = schedule["selector_failure"]
    diagnostics = selector_failure["selection_diagnostics"]
    assert selector_failure["reason_code"] == "DIRECT_QUOTE_ZERO_BID_ASK"
    assert diagnostics["candidate_accounting"] == {
        "universe_count": 3,
        "accounted_count": 3,
        "complete": True,
    }
    assert diagnostics["candidate_outcomes"][near_zero["symbol"]] == (
        "DIRECT_QUOTE_ZERO_BID_ASK"
    )
    assert diagnostics["candidate_outcomes"][low_oi["symbol"]] == "OI_TOO_LOW"
    assert diagnostics["structural_skips"]
    assert diagnostics["candidate_outcomes"][far_otm["symbol"]] == (
        "MONEYNESS_OUT_OF_RANGE"
    )
    assert any(
        item.get("symbol") == far_otm["symbol"]
        and item.get("skip_reason") == "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"
        for item in diagnostics["structural_skips"]
    )
    assert schedule["signal_id"] == plan.signal_id
    assert str(schedule["execution_mode"]).lower() == "live"
    assert osm.row["client_id"] == "jasoncosby1@gmail.com"
    assert osm.row["execution_mode"] == "live"
    assert osm.row["signal_id"] == plan.signal_id
    assert osm.row["local_order_id"] == local_order_id
    assert osm.row["meta"]["materialization_generation"] == 1
    assert osm.row["meta"]["retry_attempt"] == 1
    assert osm.row["meta"]["lifecycle_state"] == "RETRY_WAIT"
    assert osm.row["meta"]["materialization_status"] == "RETRY_PENDING"
    assert osm.row["meta"]["broker_ready"] is False
    assert osm.row["meta"]["selector_recovery_cursor_v1"]
    first_cursor = osm.row["meta"]["selector_recovery_cursor_v1"]
    assert first_cursor["local_order_id"] == local_order_id
    assert first_cursor["signal_id"] == plan.signal_id
    assert first_cursor["execution_mode"] == "live"
    assert first_cursor["materialization_generation"] == 1
    assert first_cursor["selector_attempt_count"] == 1
    assert broker.submit_order.call_count == 0
    assert broker.cancel_order.call_count == 0
    osm.submit_existing_entry.assert_not_called()

    # Simulate the immutable row becoming due before a fresh process starts.
    due = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    osm.row["meta"]["next_retry_at"] = due
    osm.row["meta"]["materialization_next_retry_at"] = due

    restart_broker = _DirectQuoteBroker(
        chain,
        valid_symbol="__NO_VALID_DIRECT_QUOTE__",
        valid_quote={"bid": 0.0, "ask": 0.0},
        underlying_price=100.0,
    )

    def _restart_quote(symbol):
        if str(symbol).upper() == "SPY":
            return {
                "bid": 99.99,
                "ask": 100.01,
                "last": 100.0,
                "provider_timestamp": datetime.now(timezone.utc).isoformat(),
            }
        return {"bid": 0.0, "ask": 0.0}

    restart_broker.get_quote.side_effect = _restart_quote
    restart_selector = _actual_selector(restart_broker, execution_mode="LIVE")
    restarted = _execution_core(restart_selector, restart_broker, execution_mode="LIVE")
    restarted.order_state_machine = osm
    callback_results = []
    _original_callback = restarted._on_entry_trigger

    def _capture_callback(watched_signal):
        result = _original_callback(watched_signal)
        callback_results.append(result)
        return result

    restarted._on_entry_trigger = _capture_callback
    second = restarted.resume_deferred_materialization_retry(
        local_order_id=local_order_id,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="recovery:pr439-release-replay",
    )

    assert second["disposition"] == "RETRY_WAIT"
    assert len(callback_results) == 1
    assert callback_results[0]["disposition"] == "RETRY_WAIT"
    assert callback_results[0]["reason_code"] == "DIRECT_QUOTE_ZERO_BID_ASK"
    assert len(risk_calls) >= 2
    assert len(osm.claim_calls) == 2
    assert len(osm.schedule_calls) == 2
    assert osm.claim_calls[-1]["owner"] == "recovery:pr439-release-replay"
    assert osm.claim_calls[-1]["new_generation"] == 2
    assert osm.claim_calls[-1]["retry_attempt"] == 2
    assert osm.schedule_calls[-1]["signal_id"] == plan.signal_id
    assert str(osm.schedule_calls[-1]["execution_mode"]).lower() == "live"
    assert osm.row["local_order_id"] == local_order_id
    assert osm.row["client_id"] == "jasoncosby1@gmail.com"
    assert osm.row["execution_mode"] == "live"
    assert osm.row["signal_id"] == plan.signal_id
    assert osm.row["meta"]["materialization_generation"] == 2
    assert osm.row["meta"]["lifecycle_state"] == "RETRY_WAIT"
    assert osm.row["meta"]["broker_ready"] is False
    second_cursor = osm.row["meta"]["selector_recovery_cursor_v1"]
    assert second_cursor["local_order_id"] == local_order_id
    assert second_cursor["signal_id"] == plan.signal_id
    assert second_cursor["execution_mode"] == "live"
    assert second_cursor["materialization_generation"] == 2
    assert second_cursor["selector_attempt_count"] == 2
    assert len(osm.cursor_calls) >= 2
    assert all(call["signal_id"] == plan.signal_id for call in osm.cursor_calls)
    assert all(str(call["execution_mode"]).lower() == "live" for call in osm.cursor_calls)
    assert all(call["generation"] in {1, 2} for call in osm.cursor_calls)
    assert broker.submit_order.call_count == 0
    assert broker.cancel_order.call_count == 0
    assert restart_broker.submit_order.call_count == 0
    assert restart_broker.cancel_order.call_count == 0
    restarted.order_state_machine.submit_existing_entry.assert_not_called()
