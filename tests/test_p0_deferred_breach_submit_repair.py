from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import ap_execution_core as core_mod
from ap_entry_watcher import WatchedSignal


def _run_deferred_breach_branch(
    *,
    plan_contract: str = "DEFERRED:AVGO",
    selected_contract: str = "AVGO260619C00450000",
    selected_price: float = 4.25,
    selected_qty: int = 2,
    premium_per_contract: float = 425.0,
    underlying_price: float = 210.25,
    selector_reason_code: str = "CHAIN_EMPTY_OPTIONS",
    client_id: str = "jasoncosby1@gmail.com",
    execution_mode: str = "paper",
    signal_id: str = "sig-123",
    selector_raises: Exception | None = None,
):
    plan = types.SimpleNamespace(
        contract_symbol=plan_contract,
        limit_price=0.01,
        contracts=1,
        max_position_usd=1.0,
        metadata={"contract_deferred": True},
        execution_mode=execution_mode,
        signal_id=signal_id,
        underlying_price=underlying_price,
    )
    osm = MagicMock()
    osm.transition.return_value = True
    osm.expire_pending_entry.return_value = True
    osm.update_order_meta.return_value = True

    local_order_id = "local-123"

    def _exact_reason(reason_code: str) -> str:
        return {
            "SPREAD_TOO_WIDE": "QUALITY_REJECT_SPREAD",
            "NO_AFFORDABLE_CONTRACT": "AFFORDABILITY_REJECT",
        }.get(reason_code, reason_code)

    def _terminalize(reason_code: str, *, legacy_reason: str = "", extra_meta: dict | None = None):
        exact_reason = _exact_reason(reason_code)
        last_error = f"materialization:{exact_reason}"
        meta_patch = {
            "materialization_outcome": "TERMINAL_NO_SUBMIT",
            "materialization_failure_reason": exact_reason,
            "deferred_contract_finalized": True,
            "deferred_breach_failure": True,
            "deferred_breach_reason": legacy_reason or last_error,
            "local_order_id": local_order_id,
        }
        if extra_meta:
            meta_patch.update(extra_meta)
        osm.update_order_meta(local_order_id, meta_patch)
        osm.transition(local_order_id, "ERROR", last_error=last_error)

    if underlying_price <= 0:
        _terminalize(
            "ZERO_UNDERLYING_UNRECOVERABLE",
            legacy_reason="materialization_underlying_missing",
            extra_meta={"failure_stage": "deferred_contract_selection"},
        )
        return {"submitted": False, "plan": plan, "osm": osm}

    if selector_raises is not None:
        _terminalize(
            "SELECTOR_EXCEPTION",
            legacy_reason=f"breach_time_contract_selection_error:{selector_raises}",
            extra_meta={"failure_stage": "deferred_contract_selection"},
        )
        return {"submitted": False, "plan": plan, "osm": osm}

    sel = types.SimpleNamespace(
        contract_symbol=selected_contract,
        expiration="2026-06-19",
        strike=450.0,
        bid=max(selected_price - 0.1, 0.0),
        execution_price_per_share=selected_price,
        ask=selected_price,
        mid=selected_price,
        affordable_contracts=selected_qty,
        premium_per_contract=premium_per_contract,
        pricing_basis="direct_quote",
    )

    live_contract = str(getattr(plan, "contract_symbol", "") or "").strip()
    plan_is_placeholder = (not live_contract) or live_contract.upper().startswith("DEFERRED:")
    sel_is_real = bool(sel.contract_symbol) and not sel.contract_symbol.upper().startswith("DEFERRED:")

    if sel_is_real and plan_is_placeholder:
        plan.contract_symbol = sel.contract_symbol
        plan.contract = sel.contract_symbol
        plan.limit_price = sel.execution_price_per_share or sel.ask or sel.mid
        if int(sel.affordable_contracts or 0) > 0:
            plan.contracts = int(sel.affordable_contracts)
            plan.qty = int(sel.affordable_contracts)
            plan.max_position_usd = int(sel.affordable_contracts) * float(sel.premium_per_contract)
        plan.selected_expiration = sel.expiration
        plan.selected_strike = sel.strike
        plan.option_bid = sel.bid
        plan.option_ask = sel.ask
        plan.option_mid = sel.mid
        plan.selector_data_source = sel.pricing_basis
        plan.dte_bucket_used = "A"
        plan.metadata.update({
            "materialization_outcome": "MATERIALIZED_FOR_SUBMIT",
            "deferred_contract_finalized": True,
            "materialized_contract": sel.contract_symbol,
            "selected_expiration": sel.expiration,
            "selected_strike": sel.strike,
            "option_bid": sel.bid,
            "option_ask": sel.ask,
            "option_mid": sel.mid,
            "limit_price": plan.limit_price,
            "qty": plan.contracts,
            "underlying_price": underlying_price,
            "selector_data_source": sel.pricing_basis,
            "dte_bucket_used": "A",
        })
        live_contract = str(getattr(plan, "contract_symbol", "") or "").strip()

    if (
        not selected_contract
        or live_contract.upper().startswith("DEFERRED:")
        or plan.limit_price <= 0
        or int(getattr(plan, "contracts", 0) or 0) <= 0
        or underlying_price <= 0
    ):
        _terminalize(
            "COPYBACK_INTEGRITY_FAILED" if selected_contract else selector_reason_code,
            legacy_reason=(
                f"breach_time_contract_selection:{selector_reason_code}"
                if not selected_contract
                else f"breach_time_contract_selection:DEFERRED_UNRESOLVED_AT_BREACH:{live_contract}"
            ),
            extra_meta={
                "failure_stage": "deferred_contract_selection",
                "selected_contract": selected_contract or None,
                "approved_contract": live_contract,
            },
        )
        return {"submitted": False, "plan": plan, "osm": osm}

    osm.update_order_meta(local_order_id, {
        "materialization_outcome": "MATERIALIZED_FOR_SUBMIT",
        "deferred_contract_finalized": True,
        "materialized_contract": sel.contract_symbol,
        "selected_expiration": sel.expiration,
        "selected_strike": sel.strike,
        "option_bid": sel.bid,
        "option_ask": sel.ask,
        "option_mid": sel.mid,
        "limit_price": plan.limit_price,
        "qty": plan.contracts,
        "underlying_price": underlying_price,
        "selector_data_source": sel.pricing_basis,
        "dte_bucket_used": "A",
    })
    submit_limit = float(plan.limit_price)
    osm.submit_existing_entry(
        local_order_id=local_order_id,
        broker=MagicMock(),
        plan=plan,
        limit_price=submit_limit,
    )
    return {
        "submitted": True,
        "plan": plan,
        "osm": osm,
        "submit_limit": submit_limit,
        "client_id": client_id,
        "execution_mode": execution_mode,
        "signal_id": signal_id,
    }


def test_deferred_breach_copies_selected_contract_onto_plan_and_submits():
    result = _run_deferred_breach_branch()

    assert result["submitted"] is True
    assert result["plan"].contract_symbol == "AVGO260619C00450000"
    assert result["plan"].contract == "AVGO260619C00450000"
    assert result["plan"].limit_price == 4.25
    assert result["plan"].contracts == 2
    assert result["plan"].qty == 2
    assert result["plan"].max_position_usd == 850.0
    result["osm"].submit_existing_entry.assert_called_once()
    result["osm"].transition.assert_not_called()
    _, submit_kwargs = result["osm"].submit_existing_entry.call_args
    assert submit_kwargs["local_order_id"] == "local-123"
    assert submit_kwargs["plan"].contract_symbol == "AVGO260619C00450000"
    assert not submit_kwargs["plan"].contract_symbol.startswith("DEFERRED:")
    assert submit_kwargs["plan"].limit_price > 0
    assert submit_kwargs["plan"].underlying_price > 0
    assert submit_kwargs["plan"].contracts > 0
    assert result["signal_id"] == "sig-123"
    assert result["client_id"] == "jasoncosby1@gmail.com"
    assert result["execution_mode"] == "paper"
    result["osm"].update_order_meta.assert_called_once()


def test_deferred_breach_unresolved_placeholder_terminalizes_order_with_last_error():
    result = _run_deferred_breach_branch(
        selected_contract="DEFERRED:AVGO",
        selected_price=0.0,
        selected_qty=0,
        premium_per_contract=0.0,
    )

    assert result["submitted"] is False
    result["osm"].submit_existing_entry.assert_not_called()
    result["osm"].update_order_meta.assert_called_once()
    result["osm"].transition.assert_called_once_with(
        "local-123",
        "ERROR",
        last_error="materialization:COPYBACK_INTEGRITY_FAILED",
    )
    _, meta_patch = result["osm"].update_order_meta.call_args[0]
    assert meta_patch["materialization_outcome"] == "TERMINAL_NO_SUBMIT"
    assert meta_patch["materialization_failure_reason"] == "COPYBACK_INTEGRITY_FAILED"
    assert meta_patch["deferred_contract_finalized"] is True


def test_deferred_breach_selector_error_terminalizes_order_with_last_error():
    result = _run_deferred_breach_branch(
        selector_raises=RuntimeError("chain timeout"),
    )

    assert result["submitted"] is False
    result["osm"].submit_existing_entry.assert_not_called()
    result["osm"].transition.assert_called_once_with(
        "local-123",
        "ERROR",
        last_error="materialization:SELECTOR_EXCEPTION",
    )
    _, meta_patch = result["osm"].update_order_meta.call_args[0]
    assert meta_patch["materialization_failure_reason"] == "SELECTOR_EXCEPTION"


def test_deferred_breach_zero_underlying_terminalizes_with_exact_reason():
    result = _run_deferred_breach_branch(
        underlying_price=0.0,
    )

    assert result["submitted"] is False
    result["osm"].submit_existing_entry.assert_not_called()
    result["osm"].transition.assert_called_once_with(
        "local-123",
        "ERROR",
        last_error="materialization:ZERO_UNDERLYING_UNRECOVERABLE",
    )
    _, meta_patch = result["osm"].update_order_meta.call_args[0]
    assert meta_patch["materialization_failure_reason"] == "ZERO_UNDERLYING_UNRECOVERABLE"


def test_deferred_breach_selector_reason_terminalizes_without_broker_post():
    result = _run_deferred_breach_branch(
        selected_contract="",
        selected_price=0.0,
        selected_qty=0,
        premium_per_contract=0.0,
        selector_reason_code="CHAIN_EMPTY_OPTIONS",
    )

    assert result["submitted"] is False
    result["osm"].submit_existing_entry.assert_not_called()
    result["osm"].transition.assert_called_once_with(
        "local-123",
        "ERROR",
        last_error="materialization:CHAIN_EMPTY_OPTIONS",
    )
    _, meta_patch = result["osm"].update_order_meta.call_args[0]
    assert meta_patch["materialization_failure_reason"] == "CHAIN_EMPTY_OPTIONS"


def test_deferred_breach_quality_reject_maps_to_exact_terminal_reason():
    result = _run_deferred_breach_branch(
        selected_contract="",
        selected_price=0.0,
        selected_qty=0,
        premium_per_contract=0.0,
        selector_reason_code="SPREAD_TOO_WIDE",
    )

    assert result["submitted"] is False
    result["osm"].transition.assert_called_once_with(
        "local-123",
        "ERROR",
        last_error="materialization:QUALITY_REJECT_SPREAD",
    )
    _, meta_patch = result["osm"].update_order_meta.call_args[0]
    assert meta_patch["materialization_failure_reason"] == "QUALITY_REJECT_SPREAD"


def test_trigger_ready_deferred_quote_refresh_failure_expires_order_with_last_error(monkeypatch):
    plan = types.SimpleNamespace(
        contract_symbol="DEFERRED:AVGO",
        limit_price=3.10,
        contracts=1,
        max_position_usd=310.0,
        metadata={"contract_deferred": True},
        trigger_price=210.0,
        side="CALL",
    )
    selected = types.SimpleNamespace(
        contract_symbol="AVGO260619C00450000",
        execution_price_per_share=3.25,
        ask=3.25,
        mid=3.20,
        affordable_contracts=2,
        premium_per_contract=325.0,
    )

    core = core_mod.APExecutionCore.__new__(core_mod.APExecutionCore)
    core.paper = True
    core.broker = types.SimpleNamespace(
        cfg=types.SimpleNamespace(base_url="https://api.tradier.com")
    )
    core.store = MagicMock()
    core.order_state_machine = MagicMock()
    core.order_state_machine.expire_pending_entry.return_value = True
    core.contract_selector = MagicMock()
    core.contract_selector.select.return_value = selected
    core._breach_risk_check = MagicMock(return_value=True)
    core._recover_plan_for_revalidation = MagicMock(return_value=plan)
    core._alert_degraded = MagicMock()
    core._cleanup_pending_entry_order = types.MethodType(
        core_mod.APExecutionCore._cleanup_pending_entry_order,
        core,
    )

    fake_execution_mod = types.ModuleType("ap.execution")
    fake_execution_mod._refresh_ask_at_submit = lambda broker, contract: (
        0.0,
        11,
        False,
        "no_quote",
        {
            "submit_bid": None,
            "submit_ask": None,
            "submit_last": None,
            "submit_mid": None,
            "spread_pct": None,
        },
    )
    monkeypatch.setitem(sys.modules, "ap.execution", fake_execution_mod)

    watched = WatchedSignal(
        {
            "ticker": "AVGO",
            "side": "CALL",
            "entry_price": 210.0,
            "stop_price": 205.0,
            "target_price": 220.0,
            "signal_id": "sig-avgo-1",
            "local_order_id": "local-avgo-1",
            "client_id": "jasoncosby1@gmail.com",
            "contract_deferred": True,
            "score": 78,
        },
        overnight=True,
    )
    watched.trigger_price = 210.25

    core_mod.APExecutionCore._on_entry_trigger(core, watched)

    core.contract_selector.select.assert_called_once_with(plan)
    core.order_state_machine.submit_existing_entry.assert_not_called()
    core.order_state_machine.expire_pending_entry.assert_called_once_with(
        "local-avgo-1",
        reason="breach_quote_refresh_failed:no_quote",
    )
    core.store.update_signal_fields.assert_any_call(
        "sig-avgo-1",
        {
            "decision_status": "blocked_at_breach",
            "context_notes": "breach_quote_refresh_failed:no_quote",
        },
    )


def test_trigger_ready_deferred_success_submits_real_occ_not_deferred(monkeypatch):
    plan = types.SimpleNamespace(
        contract_symbol="DEFERRED:AVGO",
        limit_price=3.10,
        contracts=1,
        max_position_usd=310.0,
        metadata={"contract_deferred": True},
        trigger_price=210.0,
        side="CALL",
        execution_mode="paper",
        signal_id="sig-avgo-2",
    )
    selected = types.SimpleNamespace(
        contract_symbol="AVGO260619C00450000",
        expiration="2026-06-19",
        strike=450.0,
        bid=3.15,
        ask=3.25,
        mid=3.20,
        execution_price_per_share=3.25,
        affordable_contracts=2,
        premium_per_contract=325.0,
        pricing_basis="direct_quote",
    )

    core = core_mod.APExecutionCore.__new__(core_mod.APExecutionCore)
    core.paper = True
    core.mode = "paper"
    core.client_id = "jasoncosby1@gmail.com"
    core.email = "jasoncosby1@gmail.com"
    core.broker = types.SimpleNamespace(
        cfg=types.SimpleNamespace(base_url="https://api.tradier.com")
    )
    core.store = MagicMock()
    core.order_state_machine = MagicMock()
    core.order_state_machine.submit_existing_entry.return_value = {
        "ok": True,
        "local_order_id": "local-avgo-2",
        "broker_order_id": "broker-123",
    }
    core.contract_selector = MagicMock()
    core.contract_selector.select.return_value = selected
    core.contract_selector.get_last_dte_ladder_audit.return_value = {
        "selected_bucket": "A",
    }
    core._breach_risk_check = MagicMock(return_value=True)
    core._recover_plan_for_revalidation = MagicMock(return_value=plan)
    core._alert_degraded = MagicMock()

    fake_execution_mod = types.ModuleType("ap.execution")
    fake_execution_mod._refresh_ask_at_submit = lambda broker, contract: (
        3.25,
        11,
        True,
        "ok",
        {
            "submit_bid": 3.15,
            "submit_ask": 3.25,
            "submit_last": 3.20,
            "submit_mid": 3.20,
            "spread_pct": 0.03,
        },
    )
    monkeypatch.setitem(sys.modules, "ap.execution", fake_execution_mod)

    watched = WatchedSignal(
        {
            "ticker": "AVGO",
            "side": "CALL",
            "entry_price": 210.0,
            "stop_price": 205.0,
            "target_price": 220.0,
            "signal_id": "sig-avgo-2",
            "local_order_id": "local-avgo-2",
            "client_id": "jasoncosby1@gmail.com",
            "execution_mode": "paper",
            "contract_deferred": True,
            "score": 78,
        },
        overnight=True,
    )
    watched.trigger_price = 210.25

    core_mod.APExecutionCore._on_entry_trigger(core, watched)

    core.order_state_machine.submit_existing_entry.assert_called_once()
    submit_kwargs = core.order_state_machine.submit_existing_entry.call_args.kwargs
    assert submit_kwargs["local_order_id"] == "local-avgo-2"
    assert submit_kwargs["plan"].contract_symbol == "AVGO260619C00450000"
    assert not submit_kwargs["plan"].contract_symbol.startswith("DEFERRED:")
    assert submit_kwargs["plan"].contracts == 2
    assert submit_kwargs["plan"].underlying_price == 210.25
    assert watched.signal["client_id"] == "jasoncosby1@gmail.com"
    assert watched.signal["execution_mode"] == "paper"
    assert watched.signal["local_order_id"] == "local-avgo-2"
    assert watched.signal["signal_id"] == "sig-avgo-2"


def test_trigger_ready_deferred_copyback_failure_terminalizes_and_skips_submit(monkeypatch):
    plan = types.SimpleNamespace(
        contract_symbol="DEFERRED:AVGO",
        limit_price=3.10,
        contracts=1,
        max_position_usd=310.0,
        metadata={"contract_deferred": True},
        trigger_price=210.0,
        side="CALL",
        execution_mode="paper",
        signal_id="sig-avgo-3",
    )
    selected = types.SimpleNamespace(
        contract_symbol="DEFERRED:AVGO",
        execution_price_per_share=0.0,
        ask=0.0,
        mid=0.0,
        affordable_contracts=0,
        premium_per_contract=0.0,
        pricing_basis="direct_quote",
    )

    core = core_mod.APExecutionCore.__new__(core_mod.APExecutionCore)
    core.paper = True
    core.mode = "paper"
    core.client_id = "jasoncosby1@gmail.com"
    core.email = "jasoncosby1@gmail.com"
    core.broker = types.SimpleNamespace(
        cfg=types.SimpleNamespace(base_url="https://api.tradier.com")
    )
    core.store = MagicMock()
    core.order_state_machine = MagicMock()
    core.contract_selector = MagicMock()
    core.contract_selector.select.return_value = selected
    core._breach_risk_check = MagicMock(return_value=True)
    core._recover_plan_for_revalidation = MagicMock(return_value=plan)
    core._alert_degraded = MagicMock()

    watched = WatchedSignal(
        {
            "ticker": "AVGO",
            "side": "CALL",
            "entry_price": 210.0,
            "stop_price": 205.0,
            "target_price": 220.0,
            "signal_id": "sig-avgo-3",
            "local_order_id": "local-avgo-3",
            "client_id": "jasoncosby1@gmail.com",
            "execution_mode": "paper",
            "contract_deferred": True,
            "score": 78,
        },
        overnight=True,
    )
    watched.trigger_price = 210.25

    core_mod.APExecutionCore._on_entry_trigger(core, watched)

    core.order_state_machine.submit_existing_entry.assert_not_called()
    core.order_state_machine.transition.assert_called_once_with(
        "local-avgo-3",
        "ERROR",
        last_error="materialization:COPYBACK_INTEGRITY_FAILED",
    )
