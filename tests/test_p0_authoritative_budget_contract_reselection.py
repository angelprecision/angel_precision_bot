from __future__ import annotations

import threading
import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

from ap_execution_core import APExecutionCore, _classify_deferred_breach_retry_decision
from ap_master_control import APMasterControl, ControlDecision
from tests.test_p0_selector_terminal_truth_ranked_quotes_current_main import (
    _execution_core,
    _execution_plan,
    _execution_watched,
    _plan,
    _row,
    _run_selector,
)


def test_final_cap_exposes_authoritative_budget_for_reselection():
    master = APMasterControl.__new__(APMasterControl)
    master.mode = "live"
    master.max_position_pct = 0.10
    master.max_total_capital_pct = 0.40
    master.max_sector_pct = 0.25
    master.max_ticker_pct = 0.20
    master.pending_capital_fail_closed_live = False
    master._is_live_mode = lambda: True
    master._current_mode = lambda: "LIVE"
    master._equity_snapshot = MagicMock(return_value=(1660.0, -500.0))
    master._get_snapshot = MagicMock(return_value={
        "_snapshot_ok": True,
        "_snapshot_ts": "2026-08-13T16:00:00Z",
        "capital_deployed": 0.0,
        "open_positions": [],
        "closing_positions": [],
    })
    master._pending_capital_from_snapshot_or_db = MagicMock(return_value=0.0)
    master._get_pending_capital_breakdown = MagicMock(return_value={})
    master._sector_capital_deployed = MagicMock(return_value=0.0)
    master._ticker_capital_deployed = MagicMock(return_value=0.0)
    master._log_capital_utilization = MagicMock()
    master._kill_switch_fn = lambda: False
    master._block = lambda signal_id, ticker, client_id, stage, reason, reason_code="", meta=None: ControlDecision(
        ok=False,
        stage=stage,
        reason=reason,
        reason_code=reason_code,
        signal_id=signal_id,
        ticker=ticker,
        client_id=client_id,
        context=dict(meta or {}),
    )
    plan = SimpleNamespace(
        ticker="HOOD",
        signal_id="sig-pr444-budget",
        client_id="jason-live",
        execution_mode="LIVE",
        contract_symbol="HOOD260619C00220000",
        limit_price=1.69,
        contracts=1,
        max_position_usd=169.0,
        metadata={
            "local_order_id": "local-pr444-budget",
            "sizing_context": {"selector_budget": 169.0},
        },
    )

    decision = master.revalidate_exposure(plan, client_id="jason-live")

    assert not decision.ok
    assert decision.reason_code == "ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP"
    assert decision.context["decision"] == "CONTRACT_RESELECT_REQUIRED_BUDGET_DRIFT"
    assert decision.context["effective_contract_budget"] == pytest.approx(166.0)
    assert decision.context["prior_contract_cost"] == pytest.approx(169.0)


def test_selector_receives_final_budget_and_keeps_quality_gates(monkeypatch):
    expensive = _row(
        "SPY", 101.0, bid=1.60, ask=1.69, delta=0.40, oi=1200, volume=300
    )
    cheaper = _row(
        "SPY", 102.0, bid=1.45, ask=1.50, delta=0.40, oi=1200, volume=300
    )
    selected, broker, _context, failure, _diagnostics = _run_selector(
        monkeypatch,
        plan=_plan(ticker="SPY", underlying=100.0, budget=166.0),
        chain=[expensive, cheaper],
        limit=2,
    )

    assert selected is not None
    assert selected.contract_symbol == cheaper["symbol"]
    assert selected.premium_per_contract == pytest.approx(150.0)
    assert not failure
    assert broker.submit_order.call_count == 0


def test_budget_drift_invalidates_only_contract_and_preserves_identity():
    local_order_id = "local-pr444-identity"
    signal_id = "sig-pr444-identity"
    client_id = "jason-live"
    plan = SimpleNamespace(
        ticker="SPY",
        contract_symbol="SPY260619C00101000",
        limit_price=1.69,
        contracts=1,
        max_position_usd=169.0,
        signal_id=signal_id,
        client_id=client_id,
        execution_mode="LIVE",
        metadata={"local_order_id": local_order_id},
    )
    context = {
        "decision": "CONTRACT_RESELECT_REQUIRED_BUDGET_DRIFT",
        "client_id": client_id,
        "execution_mode": "live",
        "signal_id": signal_id,
        "local_order_id": local_order_id,
        "prior_contract": plan.contract_symbol,
        "prior_contract_cost": 169.0,
        "effective_contract_budget": 166.0,
        "authoritative_max_premium": 166.0,
        "per_position_cap": 166.0,
        "remaining_total_capacity": 664.0,
    }
    master = MagicMock()
    master._kill_switch_fn = lambda: False
    master.revalidate_exposure.return_value = SimpleNamespace(
        ok=False,
        reason="contract over final cap",
        reason_code="ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP",
        context=context,
    )

    core = APExecutionCore.__new__(APExecutionCore)
    core.mode = "LIVE"
    core.execution_mode = "LIVE"
    core.email = "wrong-fallback@example.com"
    core.client_id = client_id
    core.master_control = master
    core.order_state_machine = MagicMock()
    core.order_state_machine.update_order_meta.return_value = True
    core.store = MagicMock()
    core.position_manager = None
    core._pos_lock = threading.Lock()
    core._position_count = 0
    core._max_positions = 7
    core._kill_switch = False
    core._recover_plan_for_revalidation = MagicMock(return_value=plan)
    core._emit_breach_diag = MagicMock()

    watched = SimpleNamespace(
        ticker="SPY",
        signal={
            "signal_id": signal_id,
            "local_order_id": local_order_id,
            "client_id": client_id,
        },
    )

    assert APExecutionCore._breach_risk_check(core, watched) is True
    master.revalidate_exposure.assert_called_once_with(plan, client_id=client_id)
    assert plan.contract_symbol == "DEFERRED:SPY"
    assert plan.limit_price == pytest.approx(0.01)
    assert plan.max_position_usd == pytest.approx(166.0)
    assert plan.metadata["budget_reselection"]["prior_contract"] == (
        "SPY260619C00101000"
    )
    assert plan.metadata["contract_deferred"] is True
    core.order_state_machine.update_order_meta.assert_called_once()


def test_affordability_retry_is_bounded_only_for_budget_reselection():
    common = {
        "queue_local_order_id": "local-pr444-retry",
        "attempt": 1,
        "max_attempts": 3,
        "past_cutoff": False,
        "retry_enabled": True,
    }
    retry = _classify_deferred_breach_retry_decision(
        "UNTRADEABLE_FOR_ACCOUNT_SIZE",
        **common,
        budget_reselection=True,
    )
    terminal = _classify_deferred_breach_retry_decision(
        "UNTRADEABLE_FOR_ACCOUNT_SIZE",
        **common,
        budget_reselection=False,
    )

    assert retry["action"] == "retry_schedule"
    assert terminal["action"] == "terminal_quality"


def test_no_affordable_quality_contract_waits_without_submit(monkeypatch):
    class NoAffordableSelector:
        def select(self, _plan, request_context=None):
            return None

        def get_last_failure(self):
            return {
                "stage": "affordability_gate",
                "reason_code": "UNTRADEABLE_FOR_ACCOUNT_SIZE",
                "explanation": "quality candidates exceed final budget",
            }

        def get_last_dte_ladder_audit(self):
            return {}

    plan = _execution_plan(execution_mode="LIVE", budget=166.0)
    local_order_id = "local-pr444-no-cheaper"
    plan.contract_symbol = "DEFERRED:SPY"
    plan.limit_price = 0.01
    plan.metadata.update({
        "local_order_id": local_order_id,
        "budget_reselection": {
            "decision": "CONTRACT_RESELECT_REQUIRED_BUDGET_DRIFT",
            "client_id": plan.client_id,
            "execution_mode": "live",
            "signal_id": plan.signal_id,
            "local_order_id": local_order_id,
            "prior_contract": "SPY260619C00101000",
            "prior_contract_cost": 169.0,
            "authoritative_max_premium": 166.0,
            "effective_contract_budget": 166.0,
            "reselection_attempt": 1,
        },
    })
    core = _execution_core(NoAffordableSelector(), SimpleNamespace(
        cfg=SimpleNamespace(base_url="https://api.tradier.com"),
    ), execution_mode="LIVE")
    core._breach_risk_check = MagicMock(return_value=True)
    core._recover_plan_for_revalidation = MagicMock(return_value=plan)
    core.order_state_machine.get_order.return_value = {}
    core.order_state_machine.schedule_deferred_materialization_retry.return_value = True
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_ENABLED", "1")
    monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "2")
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "2359")
    monkeypatch.setattr(
        "ap.queue.write_deferred_breach_last_error",
        lambda *args, **kwargs: None,
    )

    result = core._on_entry_trigger(
        _execution_watched("LIVE", ticker="SPY", local_order_id=local_order_id)
    )

    assert result["disposition"] == "RETRY_WAIT"
    assert result["reason_code"] == "UNTRADEABLE_FOR_ACCOUNT_SIZE"
    core.order_state_machine.schedule_deferred_materialization_retry.assert_called_once()
    core.order_state_machine.submit_existing_entry.assert_not_called()
    core.order_state_machine.expire_pending_entry.assert_not_called()


def test_successful_reselection_runs_final_cap_check_before_submit(monkeypatch):
    plan = _execution_plan(execution_mode="PAPER", budget=166.0)
    plan.contract_symbol = "DEFERRED:SPY"
    plan.limit_price = 0.01
    plan.metadata.update({
        "budget_reselection": {
            "decision": "CONTRACT_RESELECT_REQUIRED_BUDGET_DRIFT",
            "client_id": plan.client_id,
            "execution_mode": "paper",
            "signal_id": plan.signal_id,
            "local_order_id": "local-pr444-final",
            "prior_contract": "SPY260619C00101000",
            "prior_contract_cost": 169.0,
            "authoritative_max_premium": 166.0,
            "effective_contract_budget": 166.0,
            "reselection_attempt": 1,
        },
    })
    selected = SimpleNamespace(
        contract_symbol="SPY260619C00102000",
        execution_price_per_share=1.50,
        ask=1.50,
        bid=1.45,
        mid=1.475,
        affordable_contracts=1,
        premium_per_contract=150.0,
        expiration_date="2026-08-21",
        strike=102.0,
        option_type="call",
        dte=8,
        delta=0.40,
        open_interest=1200,
        volume=300,
        candidate_audit={},
    )
    broker = SimpleNamespace(
        cfg=SimpleNamespace(base_url="https://api.tradier.com"),
        get_quote=MagicMock(return_value={"bid": 1.45, "ask": 1.50, "last": 1.48}),
        submit_order=MagicMock(),
    )
    selector = MagicMock()
    selector.select.return_value = selected
    selector.get_last_failure.return_value = None
    selector.get_last_dte_ladder_audit.return_value = {}
    core = _execution_core(selector, broker, execution_mode="PAPER")
    core.master_control = MagicMock()
    core.order_state_machine.client_id = plan.client_id
    core.order_state_machine.execution_mode = "PAPER"
    core.master_control.revalidate_exposure.return_value = SimpleNamespace(ok=True)
    core._breach_risk_check = MagicMock(return_value=True)
    core._recover_plan_for_revalidation = MagicMock(return_value=plan)
    core.order_state_machine.claim_deferred_materialization.return_value = True
    core.order_state_machine.persist_selector_recovery_cursor.return_value = True
    core.order_state_machine.persist_deferred_broker_ready.return_value = True
    core.order_state_machine.submit_existing_entry.return_value = {
        "ok": True,
        "local_order_id": "local-pr444-final",
        "broker_order_id": "paper-pr444-final",
    }
    durable_row = {
        "status": "PENDING_TRIGGER",
        "contract": selected.contract_symbol,
        "limit_price": 1.52,
        "qty": 1,
        "reserved_cost": 152.0,
        "client_id": plan.client_id,
        "execution_mode": "paper",
        "signal_id": plan.signal_id,
        "meta": {},
    }
    core.order_state_machine.get_order.return_value = durable_row

    def _update_meta(_local_order_id, patch):
        durable_row["meta"].update(patch or {})
        return True

    core.order_state_machine.update_order_meta.side_effect = _update_meta
    monkeypatch.setattr(
        "ap.queue.write_deferred_breach_last_error",
        lambda *args, **kwargs: None,
    )

    result = core._on_entry_trigger(
        _execution_watched(
            "PAPER",
            ticker="SPY",
            local_order_id="local-pr444-final",
        )
    )

    assert core.master_control.revalidate_exposure.call_count == 1
    assert core.master_control.revalidate_exposure.call_args.kwargs["client_id"] == (
        plan.client_id
    )
    assert selector.select.call_count == 1
    assert core.order_state_machine.submit_existing_entry.call_count == 1, (
        result,
        core.order_state_machine.method_calls,
    )
    assert result is None or result.get("disposition") in {"SUBMITTED", "KEEP_WATCHER"}
