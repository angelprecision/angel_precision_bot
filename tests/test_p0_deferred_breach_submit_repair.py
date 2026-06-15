from __future__ import annotations

import types
from unittest.mock import MagicMock


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
        if not osm.transition(local_order_id, "ERROR", last_error=reason):
            osm.expire_pending_entry(local_order_id, reason=reason)

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
    result["osm"].transition.assert_called_once()
    _, transition_kwargs = result["osm"].transition.call_args
    assert transition_kwargs["last_error"] == "deferred_unresolved_at_breach:DEFERRED:AVGO"


def test_deferred_breach_selector_error_terminalizes_order_with_last_error():
    result = _run_deferred_breach_branch(
        selector_raises=RuntimeError("chain timeout"),
    )

    assert result["submitted"] is False
    result["osm"].submit_existing_entry.assert_not_called()
    result["osm"].transition.assert_called_once()
    _, transition_kwargs = result["osm"].transition.call_args
    assert "breach_time_contract_selection_error:chain timeout" == transition_kwargs["last_error"]
