"""P0 PR #474 deferred final-cost revalidation."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

from ap_execution_core import (
    APExecutionCore,
    _build_deferred_final_revalidation_plan,
    _classify_deferred_plan,
    _revalidate_deferred_final_cost,
    _validate_deferred_final_identity,
)
from ap_entry_watcher import WatchedSignal

_REPO = Path(__file__).resolve().parents[1]
_CORE = (_REPO / "ap_execution_core.py").read_text()
_P0 = (_REPO / ".github/workflows/p0_regression.yml").read_text()


def _plan(
    *,
    contract="SPY260821P00775000",
    price=1.45,
    qty=1,
    client_id="jasoncosby1@gmail.com",
    mode="LIVE",
    bootstrap=False,
):
    return SimpleNamespace(
        ticker="SPY",
        side="PUT",
        signal_id="sig-474",
        client_id=client_id,
        execution_mode=mode.lower(),
        mode=mode,
        contract_symbol=contract,
        limit_price=price,
        contracts=qty,
        max_position_usd=round(price * qty * 100.0, 2),
        metadata={
            "contract_deferred": contract.upper().startswith("DEFERRED:"),
            "sizing_context": {"bootstrap_mode": bootstrap},
        },
        selector_metadata={
            "premium_per_contract": round(price * 100.0, 2),
            "execution_price_per_share": price,
        },
        selector_execution_price=price,
    )


def _row(client_id="jasoncosby1@gmail.com", mode="LIVE"):
    return {
        "local_order_id": "local-474",
        "client_id": client_id,
        "execution_mode": mode,
        "signal_id": "sig-474",
        "status": "PENDING_TRIGGER",
        "contract": "DEFERRED:SPY",
        "broker_order_id": None,
    }


def _signal(client_id="jasoncosby1@gmail.com", mode="LIVE"):
    return {
        "local_order_id": "local-474",
        "client_id": client_id,
        "execution_mode": mode,
        "signal_id": "sig-474",
        "ticker": "SPY",
        "side": "PUT",
    }


class _CapMC:
    def __init__(self, cap):
        self.cap = float(cap)
        self.calls = []

    def revalidate_exposure(self, plan, client_id="default"):
        self.calls.append((plan, client_id))
        cost = float(plan.max_position_usd)
        return SimpleNamespace(
            ok=cost <= self.cap,
            reason="" if cost <= self.cap else "ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP",
        )


class _BootstrapMC:
    """Mimic the bootstrap mutation relevant to current Master Control."""

    def __init__(self, cap):
        self.cap = float(cap)
        self.seen_ppc = None

    def revalidate_exposure(self, plan, client_id="default"):
        if (plan.metadata.get("sizing_context") or {}).get("bootstrap_mode"):
            self.seen_ppc = float(plan.selector_metadata["premium_per_contract"])
            plan.contracts = 1
            plan.max_position_usd = self.seen_ppc
        cost = float(plan.max_position_usd)
        return SimpleNamespace(ok=cost <= self.cap, reason="" if cost <= self.cap else "CAP")


class _RaiseMC:
    def revalidate_exposure(self, plan, client_id="default"):
        raise RuntimeError("mc unavailable")


def test_blank_contract_is_not_deferred_authority_without_provenance():
    explicit = _plan(contract="")
    explicit.metadata["contract_deferred"] = True
    assert _classify_deferred_plan(explicit, {}) == (True, False)

    prefixed = _plan(contract="DEFERRED:SPY")
    prefixed.metadata = {}
    assert _classify_deferred_plan(prefixed, {}) == (True, False)

    malformed = _plan(contract="")
    malformed.metadata = {}
    assert _classify_deferred_plan(malformed, {}) == (False, True)


def test_final_identity_exact_shape_passes():
    result = _validate_deferred_final_identity(
        order_row=_row(),
        approved_plan=_plan(),
        signal=_signal(),
        expected_local_order_id="local-474",
        runtime_client_ids=("jasoncosby1@gmail.com",),
        runtime_modes=("LIVE",),
    )
    assert result["ok"] is True
    assert result["client_id"] == "jasoncosby1@gmail.com"
    assert result["execution_mode"] == "live"


def test_final_identity_client_mode_and_local_order_mismatches_block():
    client = _validate_deferred_final_identity(
        order_row=_row(),
        approved_plan=_plan(client_id="other@example.com"),
        signal=_signal(),
        expected_local_order_id="local-474",
        runtime_client_ids=("jasoncosby1@gmail.com",),
        runtime_modes=("LIVE",),
    )
    assert client["reason_code"] == "DEFERRED_FINAL_IDENTITY_CLIENT_MISMATCH"

    mode = _validate_deferred_final_identity(
        order_row=_row(mode="LIVE"),
        approved_plan=_plan(mode="PAPER"),
        signal=_signal(mode="LIVE"),
        expected_local_order_id="local-474",
        runtime_client_ids=("jasoncosby1@gmail.com",),
        runtime_modes=("LIVE",),
    )
    assert mode["reason_code"] == "DEFERRED_FINAL_IDENTITY_MODE_MISMATCH"

    local = _validate_deferred_final_identity(
        order_row=_row(),
        approved_plan=_plan(),
        signal=_signal(),
        expected_local_order_id="wrong-order",
        runtime_client_ids=("jasoncosby1@gmail.com",),
        runtime_modes=("LIVE",),
    )
    assert local["reason_code"] == "DEFERRED_FINAL_IDENTITY_LOCAL_ORDER_MISMATCH"


def test_revalidation_proxy_uses_final_price_without_corrupting_selector_evidence():
    original = _plan(price=1.40, qty=2)
    proxy = _build_deferred_final_revalidation_plan(
        original, submit_limit=1.59, qty=2
    )
    assert proxy is not original
    assert proxy.limit_price == 1.59
    assert proxy.max_position_usd == 318.00
    assert proxy.selector_metadata["premium_per_contract"] == 159.00
    assert original.limit_price == 1.40
    assert original.max_position_usd == 280.00
    assert original.selector_metadata["premium_per_contract"] == 140.00


def test_selector_price_under_cap_but_final_submit_price_over_cap_blocks():
    plan = _plan(price=1.45, qty=1)
    mc = _CapMC(160.00)
    result = _revalidate_deferred_final_cost(
        mc,
        plan,
        client_id="jasoncosby1@gmail.com",
        execution_mode="LIVE",
        submit_limit=1.64,
        qty=1,
    )
    assert result["ok"] is False
    assert result["actual_cost"] == 164.00
    assert "ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP" in result["reason_code"]
    assert len(mc.calls) == 1
    seen_plan, seen_client = mc.calls[0]
    assert seen_client == "jasoncosby1@gmail.com"
    assert seen_plan.limit_price == 1.64
    assert seen_plan.max_position_usd == 164.00


def test_final_submit_price_under_cap_passes_exact_cost():
    mc = _CapMC(160.00)
    result = _revalidate_deferred_final_cost(
        mc,
        _plan(price=1.45),
        client_id="jasoncosby1@gmail.com",
        execution_mode="LIVE",
        submit_limit=1.54,
        qty=1,
    )
    assert result["ok"] is True
    assert result["final_qty"] == 1
    assert result["actual_cost"] == 154.00
    assert result["mc_seen_cost"] == 154.00


def test_bootstrap_clamp_uses_final_submit_price_not_selector_price():
    plan = _plan(price=1.40, qty=3, bootstrap=True)
    mc = _BootstrapMC(160.00)
    result = _revalidate_deferred_final_cost(
        mc,
        plan,
        client_id="jasoncosby1@gmail.com",
        execution_mode="LIVE",
        submit_limit=1.59,
        qty=3,
    )
    assert result["ok"] is True
    assert mc.seen_ppc == 159.00
    assert result["final_qty"] == 1
    assert result["actual_cost"] == 159.00
    assert plan.selector_metadata["premium_per_contract"] == 140.00
    assert plan.contracts == 3
    assert plan.max_position_usd == 420.00


def test_live_mc_exception_fails_closed_paper_preserves_fail_open():
    live = _revalidate_deferred_final_cost(
        _RaiseMC(),
        _plan(),
        client_id="jasoncosby1@gmail.com",
        execution_mode="LIVE",
        submit_limit=1.50,
        qty=1,
    )
    assert live["ok"] is False
    assert live["reason_code"] == "DEFERRED_FINAL_EXPOSURE_REVALIDATION_ERROR"

    paper = _revalidate_deferred_final_cost(
        _RaiseMC(),
        _plan(mode="PAPER"),
        client_id="paper@example.com",
        execution_mode="PAPER",
        submit_limit=1.50,
        qty=1,
    )
    assert paper["ok"] is True
    assert paper["actual_cost"] == 150.00
    assert paper["reason_code"] == "PAPER_DEFERRED_FINAL_REVALIDATION_ERROR_FAIL_OPEN"


def _breach_harness(plan):
    core = APExecutionCore.__new__(APExecutionCore)
    core.mode = "LIVE"
    core.paper = False
    core.email = "jasoncosby1@gmail.com"
    core.client_id = "jasoncosby1@gmail.com"
    core._kill_switch = False
    core._max_positions = 7
    core.store = MagicMock()
    core._current_open_position_count = lambda: 0
    core._current_pending_entry_count = lambda: 0
    core.master_control = MagicMock()
    core.master_control._kill_switch_fn = lambda: False
    core.master_control.revalidate_exposure.return_value = SimpleNamespace(ok=True, reason="")
    watched = WatchedSignal(
        {
            "ticker": "SPY",
            "side": "PUT",
            "signal_id": "sig-474",
            "local_order_id": "local-474",
            "client_id": "jasoncosby1@gmail.com",
            "execution_mode": "LIVE",
            "entry_price": 775.43,
            "stop": 779.44,
            "target": 770.00,
            "_approved_plan": plan,
        }
    )
    return core, watched


def test_real_breach_check_skips_only_proven_deferred_placeholder_cost():
    deferred = _plan(contract="DEFERRED:SPY")
    deferred.max_position_usd = 164.305
    core, watched = _breach_harness(deferred)
    assert core._breach_risk_check(watched) is True
    core.master_control.revalidate_exposure.assert_not_called()

    malformed = _plan(contract="")
    malformed.metadata = {}
    core2, watched2 = _breach_harness(malformed)
    assert core2._breach_risk_check(watched2) is True
    assert core2.master_control.revalidate_exposure.call_count == 1


def test_money_path_order_is_final_price_then_mc_then_broker_ready_then_submit():
    on_trigger = _CORE.index("def _on_entry_trigger(")
    refresh = _CORE.index("submit_limit = round(_submit_ask + _ask_cross, 2)", on_trigger)
    final_mc = _CORE.index("final deferred capital authority at broker-ready economics", refresh)
    broker_ready = _CORE.index("persist_deferred_broker_ready", final_mc)
    submit = _CORE.index(".submit_existing_entry(", broker_ready)
    assert refresh < final_mc < broker_ready < submit


def test_final_mc_block_uses_submit_limit_and_has_zero_broker_authority():
    marker = "final deferred capital authority at broker-ready economics"
    start = _CORE.index(marker)
    end = _CORE.index("# Build the entry pricing audit", start)
    block = _CORE[start:end]
    assert "submit_limit=float(submit_limit)" in block
    assert "_revalidate_deferred_final_cost(" in block
    assert "_plan_limit" not in block
    for forbidden in (
        "persist_deferred_broker_ready",
        ".submit_existing_entry(",
        "broker.submit",
        "broker.cancel",
        "proof_trades",
        "create_position",
    ):
        assert forbidden not in block
    assert "return _terminalize_deferred_breach_failure(" in block


def test_selector_cost_is_deterministic_and_focused_test_is_in_ci():
    assert "float(_sel_price_candidate)" in _CORE
    assert "* int(_sel_qty)" in _CORE
    assert "approved_plan.max_position_usd = _sel_qty * _prem_per_contract" not in _CORE
    assert "float(_sel_price_candidate) * 1 * 100.0" in _CORE
    assert "tests/test_p0_deferred_real_cost_revalidation.py" in _P0
