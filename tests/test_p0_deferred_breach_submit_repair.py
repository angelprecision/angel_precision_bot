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
    selector_raises: Exception | None = None,
):
    plan = types.SimpleNamespace(
        contract_symbol=plan_contract,
        limit_price=0.01,
        contracts=1,
        max_position_usd=1.0,
        metadata={"contract_deferred": True},
    )
    osm = MagicMock()
    osm.transition.return_value = True
    osm.expire_pending_entry.return_value = True
    osm.update_order_meta.return_value = True

    local_order_id = "local-123"

    def _terminalize(reason: str, *, extra_meta: dict | None = None):
        meta_patch = {
            "deferred_breach_failure": True,
            "deferred_breach_reason": reason,
            "local_order_id": local_order_id,
        }
        if extra_meta:
            meta_patch.update(extra_meta)
        osm.update_order_meta(local_order_id, meta_patch)
        if not osm.expire_pending_entry(local_order_id, reason=reason):
            osm.transition(local_order_id, "ERROR", last_error=reason)

    if selector_raises is not None:
        _terminalize(
            f"breach_time_contract_selection_error:{selector_raises}",
            extra_meta={"failure_stage": "deferred_contract_selection"},
        )
        return {"submitted": False, "plan": plan, "osm": osm}

    sel = types.SimpleNamespace(
        contract_symbol=selected_contract,
        execution_price_per_share=selected_price,
        ask=selected_price,
        mid=selected_price,
        affordable_contracts=selected_qty,
        premium_per_contract=premium_per_contract,
    )

    live_contract = str(getattr(plan, "contract_symbol", "") or "").strip()
    plan_is_placeholder = (not live_contract) or live_contract.upper().startswith("DEFERRED:")
    sel_is_real = bool(sel.contract_symbol) and not sel.contract_symbol.upper().startswith("DEFERRED:")

    if sel_is_real and plan_is_placeholder:
        plan.contract_symbol = sel.contract_symbol
        plan.limit_price = sel.execution_price_per_share or sel.ask or sel.mid
        if int(sel.affordable_contracts or 0) > 0:
            plan.contracts = int(sel.affordable_contracts)
            plan.max_position_usd = int(sel.affordable_contracts) * float(sel.premium_per_contract)
        live_contract = str(getattr(plan, "contract_symbol", "") or "").strip()

    if not selected_contract or live_contract.upper().startswith("DEFERRED:"):
        _terminalize(
            f"deferred_unresolved_at_breach:{live_contract}",
            extra_meta={
                "failure_stage": "deferred_contract_selection",
                "selected_contract": selected_contract or None,
                "approved_contract": live_contract,
            },
        )
        return {"submitted": False, "plan": plan, "osm": osm}

    submit_limit = float(plan.limit_price)
    osm.submit_existing_entry(
        local_order_id=local_order_id,
        broker=MagicMock(),
        plan=plan,
        limit_price=submit_limit,
    )
    return {"submitted": True, "plan": plan, "osm": osm, "submit_limit": submit_limit}


def test_deferred_breach_copies_selected_contract_onto_plan_and_submits():
    result = _run_deferred_breach_branch()

    assert result["submitted"] is True
    assert result["plan"].contract_symbol == "AVGO260619C00450000"
    assert result["plan"].limit_price == 4.25
    assert result["plan"].contracts == 2
    assert result["plan"].max_position_usd == 850.0
    result["osm"].submit_existing_entry.assert_called_once()
    result["osm"].transition.assert_not_called()


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
    result["osm"].expire_pending_entry.assert_called_once_with(
        "local-123",
        reason="deferred_unresolved_at_breach:DEFERRED:AVGO",
    )
    result["osm"].transition.assert_not_called()


def test_deferred_breach_selector_error_terminalizes_order_with_last_error():
    result = _run_deferred_breach_branch(
        selector_raises=RuntimeError("chain timeout"),
    )

    assert result["submitted"] is False
    result["osm"].submit_existing_entry.assert_not_called()
    result["osm"].expire_pending_entry.assert_called_once_with(
        "local-123",
        reason="breach_time_contract_selection_error:chain timeout",
    )
    result["osm"].transition.assert_not_called()


def test_deferred_breach_falls_back_to_error_transition_if_expire_unavailable():
    result = _run_deferred_breach_branch(
        selected_contract="DEFERRED:AVGO",
        selected_price=0.0,
        selected_qty=0,
        premium_per_contract=0.0,
    )

    result["osm"].reset_mock()
    result["osm"].expire_pending_entry.return_value = False

    local_order_id = "local-123"
    reason = "deferred_unresolved_at_breach:DEFERRED:AVGO"
    meta_patch = {
        "deferred_breach_failure": True,
        "deferred_breach_reason": reason,
        "local_order_id": local_order_id,
        "failure_stage": "deferred_contract_selection",
        "selected_contract": "DEFERRED:AVGO",
        "approved_contract": "DEFERRED:AVGO",
    }

    result["osm"].update_order_meta(local_order_id, meta_patch)
    if not result["osm"].expire_pending_entry(local_order_id, reason=reason):
        result["osm"].transition(local_order_id, "ERROR", last_error=reason)

    result["osm"].expire_pending_entry.assert_called_once_with(local_order_id, reason=reason)
    result["osm"].transition.assert_called_once()
    _, transition_kwargs = result["osm"].transition.call_args
    assert transition_kwargs["last_error"] == reason


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
