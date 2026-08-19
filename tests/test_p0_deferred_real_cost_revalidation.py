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


# ──────────────────────────────────────────────────────────────────────────────
# PR #474 AMENDMENT — REQUIRED CHANGE 3, 4, 5
# Jason AAPL production replay, mirror over-cap block, and missing-proof guard.
# ──────────────────────────────────────────────────────────────────────────────

def _aapl_plan(
    *,
    contract="DEFERRED:AAPL",
    placeholder_price=0.01,
    placeholder_max_usd=165.811,
    client_id="jasoncosby1@gmail.com",
    mode="LIVE",
):
    """Exact Jason AAPL deferred plan shape from production incident."""
    p = SimpleNamespace(
        ticker="AAPL",
        side="PUT",
        signal_id="sig-aapl-live",
        client_id=client_id,
        execution_mode=mode.lower(),
        mode=mode,
        contract_symbol=contract,
        limit_price=placeholder_price,
        contracts=1,
        # 165.811 is the reservation/selector budget, NOT the actual contract cost.
        max_position_usd=placeholder_max_usd,
        metadata={
            "contract_deferred": True,
            "sizing_context": {"bootstrap_mode": False},
        },
        selector_metadata={
            "premium_per_contract": round(placeholder_price * 100.0, 2),
            "execution_price_per_share": placeholder_price,
        },
        selector_execution_price=placeholder_price,
    )
    return p


def _aapl_row(client_id="jasoncosby1@gmail.com", mode="LIVE"):
    return {
        "local_order_id": "local-aapl-live",
        "client_id": client_id,
        "execution_mode": mode,
        "signal_id": "sig-aapl-live",
        "status": "PENDING_TRIGGER",
        "contract": "DEFERRED:AAPL",
        "broker_order_id": None,
    }


# ── REQUIRED CHANGE 3: Jason AAPL production replay — actual cost $140 passes ──

def test_jason_aapl_deferred_actual_cost_140_under_cap_passes():
    """
    Regression: placeholder reservation $165.811 must NOT trigger
    ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP when real selected contract
    costs $140 (1.40 × 1 × 100).

    Verifies the original production failure is resolved:
    - MC does NOT see 165.811
    - MC sees exactly $140.00
    - result is approved
    - MC called exactly once (at final authority, not at breach-time)
    """
    placeholder = _aapl_plan()
    assert placeholder.max_position_usd == 165.811  # reservation, not real cost

    # Per-position cap ≈ $166; actual contract $140 is under cap.
    mc = _CapMC(cap=166.00)

    result = _revalidate_deferred_final_cost(
        mc,
        placeholder,
        client_id="jasoncosby1@gmail.com",
        execution_mode="LIVE",
        submit_limit=1.40,   # real executable OCC price
        qty=1,
    )

    # Must pass.
    assert result["ok"] is True, f"Expected approval, got: {result}"
    assert result["final_qty"] == 1

    # MC must have seen exactly $140, NOT $165.811.
    assert result["actual_cost"] == 140.00
    assert result["mc_seen_cost"] == 140.00

    # MC was called exactly once with the real contract cost.
    assert len(mc.calls) == 1
    seen_plan, seen_client = mc.calls[0]
    assert seen_client == "jasoncosby1@gmail.com"
    assert seen_plan.limit_price == 1.40
    assert seen_plan.max_position_usd == 140.00

    # Original plan must be unmodified (proxy pattern).
    assert placeholder.max_position_usd == 165.811
    assert placeholder.limit_price == 0.01


# ── REQUIRED CHANGE 4: Mirror — actual cost $180 over cap blocks ──

def test_jason_aapl_deferred_actual_cost_180_over_cap_blocks():
    """
    Inverse production-shape test: same $165.811 placeholder, but real
    selected contract costs $180 (1.80 × 1 × 100) which exceeds the ~$166 cap.

    This proves #474 does NOT weaken capital risk — it changes the *timing*
    of the capital authority from fake pre-materialization economics to the
    real broker-ready economics.

    Expected: MC blocks, zero broker POST, correct reason code.
    """
    placeholder = _aapl_plan()
    assert placeholder.max_position_usd == 165.811

    mc = _CapMC(cap=166.00)

    result = _revalidate_deferred_final_cost(
        mc,
        placeholder,
        client_id="jasoncosby1@gmail.com",
        execution_mode="LIVE",
        submit_limit=1.80,   # over the per-position cap
        qty=1,
    )

    # Must block.
    assert result["ok"] is False, f"Expected block, got: {result}"
    assert "ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP" in result["reason_code"]

    # MC saw real cost $180, not the $165.811 reservation.
    # actual_cost reflects the proxy plan's max_position_usd (the real cost).
    assert result["actual_cost"] == 180.00
    # mc_seen_cost is only populated on the ok=True path; on block, mc_reason carries context.
    assert "mc_reason" in result

    # Confirm one authoritative call, correct identity.
    assert len(mc.calls) == 1
    _, seen_client = mc.calls[0]
    assert seen_client == "jasoncosby1@gmail.com"

    # Original plan untouched.
    assert placeholder.max_position_usd == 165.811


# ── REQUIRED CHANGE 5: Missing handoff proof fails closed ──

def test_deferred_live_missing_handoff_proof_fails_closed():
    """
    Audit finding regression: a deferred LIVE entry where the handoff
    snapshot was never captured (captured=False or absent) must FAIL CLOSED
    before reaching broker-ready CAS or submit.

    Verifies the source-code invariant:
    - requires_final_cost_revalidation = bool(_deferred) is present
    - DEFERRED_FINAL_HANDOFF_PROOF_MISSING terminates execution before
      persist_deferred_broker_ready and submit_existing_entry
    - There is no branch where both early MC skip AND final MC skip can occur.
    """
    core_text = _CORE

    # 1. Explicit requires_final_cost_revalidation flag must exist.
    assert "requires_final_cost_revalidation = bool(_deferred)" in core_text, (
        "requires_final_cost_revalidation flag missing — invariant not explicit"
    )

    # 2. DEFERRED_FINAL_HANDOFF_PROOF_MISSING reason code must exist in source.
    assert "DEFERRED_FINAL_HANDOFF_PROOF_MISSING" in core_text, (
        "DEFERRED_FINAL_HANDOFF_PROOF_MISSING reason code missing"
    )

    # 3. The weak conditional that was the audit finding must be gone.
    assert 'if _deferred and _handoff_snapshot.get("captured")' not in core_text, (
        "Weak conditional 'if _deferred and _handoff_snapshot.get(\"captured\")' still "
        "present — audit finding not fixed; missing-proof path can bypass final MC"
    )

    # 4. The proof-missing guard must appear BEFORE broker-ready and submit
    #    within the _on_entry_trigger function scope.
    on_trigger = core_text.index("def _on_entry_trigger(")
    proof_missing_pos = core_text.index("DEFERRED_FINAL_HANDOFF_PROOF_MISSING", on_trigger)
    broker_ready_pos  = core_text.index("persist_deferred_broker_ready", proof_missing_pos)
    submit_pos        = core_text.index(".submit_existing_entry(", broker_ready_pos)
    assert proof_missing_pos < broker_ready_pos < submit_pos, (
        "DEFERRED_FINAL_HANDOFF_PROOF_MISSING guard does not precede "
        "persist_deferred_broker_ready and submit_existing_entry"
    )

    # 5. The fail-closed block must contain a terminalize return and zero broker POST.
    start = core_text.index("requires_final_cost_revalidation = bool(_deferred)", on_trigger)
    # Narrow window: from the flag to the identity block.
    end   = core_text.index(
        "# Handoff proof is confirmed present", start
    )
    guard_block = core_text[start:end]
    assert "return _terminalize_deferred_breach_failure(" in guard_block, (
        "Fail-closed terminalize not found in the proof-missing guard block"
    )
    assert "broker_post_count: 0" in guard_block or '"broker_post_count": 0' in guard_block, (
        "broker_post_count=0 diagnostic missing from proof-missing guard"
    )
    # No broker calls inside the guard block.
    for forbidden in ("broker.submit", "broker.cancel", ".submit_existing_entry(",
                      "persist_deferred_broker_ready"):
        assert forbidden not in guard_block, (
            f"Forbidden broker call '{forbidden}' found inside the missing-proof guard"
        )


def test_requires_final_cost_revalidation_flag_is_explicit_in_source():
    """
    Structural invariant: the requires_final_cost_revalidation flag must
    exist and derive from _deferred with no other dependency, making the
    mandatory-authority requirement self-documenting and refactor-resistant.
    """
    assert "requires_final_cost_revalidation = bool(_deferred)" in _CORE


def test_proof_missing_guard_is_in_ci():
    """The amended test file must be registered in the P0 CI workflow."""
    assert "tests/test_p0_deferred_real_cost_revalidation.py" in _P0
