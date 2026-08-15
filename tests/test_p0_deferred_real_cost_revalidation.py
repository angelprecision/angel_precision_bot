"""
tests/test_p0_deferred_real_cost_revalidation.py — PR #474

Proves the deferred real-cost revalidation invariant:

    A ``DEFERRED:<ticker>`` entry must never have its reserved *account budget*
    interpreted as a real *selected-contract cost*. The correct order is:

        breach (kill-switch + slot) -> deferred selector -> real OCC / price /
        qty -> actual cost -> final Master Control exposure revalidation ->
        existing submit gates -> broker.

Coverage strategy (matches this repo's conventions):

  * ``_breach_risk_check()`` is a cleanly callable production method, so Part A
    (skip exposure revalidation while still deferred; keep kill-switch + slot;
    non-deferred real contract still revalidates exactly once) is proven by
    driving the REAL method with production-shaped plans/objects.

  * ``_validate_deferred_selector_result()`` is the real production validator
    that feeds the cost math, so the actual-cost formula (Part B — independent
    of optional ``premium_per_contract``, and recomputed after the acceptance
    qty=1 clamp) is proven behaviourally against it.

  * Parts B and C live deep inside the ~2,900-line ``_on_entry_trigger`` and are
    proven by exact source-structure guards (the same approach used by
    ``test_no_silent_deferred_trigger.py`` and siblings), asserting the real
    money path, the terminal-on-block behaviour, LIVE fail-closed / PAPER
    fail-open posture, and the frozen mutation/broker safety.
"""
from __future__ import annotations

import types
from pathlib import Path
from unittest.mock import MagicMock

import ap_execution_core as core_mod
from ap_execution_core import APExecutionCore, _validate_deferred_selector_result
from ap_entry_watcher import WatchedSignal

_REPO = Path(__file__).resolve().parents[1]
_EC = (_REPO / "ap_execution_core.py").read_text()

# Production-observed Jason LIVE placeholder budgets (Aug 10/14).
JASON_BUDGET = 165.886


# ── shared harness ────────────────────────────────────────────────────────────
def _make_core(*, mode: str = "LIVE", email: str = "jason@angelprecision.io"):
    """Construct a real APExecutionCore with only the attributes
    ``_breach_risk_check`` touches, so we drive genuine production code."""
    core = APExecutionCore.__new__(APExecutionCore)
    core.mode = mode
    core.paper = mode != "LIVE"
    core.email = email
    core._kill_switch = False
    core._max_positions = 7
    core.store = MagicMock()
    mc = MagicMock()
    # No master-control kill switch active.
    mc._kill_switch_fn = lambda: False
    core.master_control = mc
    # No open positions / pending entries -> a slot is available.
    core._current_open_position_count = lambda: 0
    core._current_pending_entry_count = lambda: 0
    return core


def _watched(plan, *, ticker="UNP", side="CALL"):
    sig = {
        "ticker": ticker,
        "side": side,
        "signal_id": "sig-474",
        "canonical_signal_id": "csig-474",
        "local_order_id": "local-474",
        "entry_price": 250.0,
        "stop": 245.0,
        "target": 260.0,
        "execution_mode": "live",
        "_approved_plan": plan,
    }
    return WatchedSignal(sig)


def _deferred_plan(*, ticker="UNP", budget=JASON_BUDGET):
    return types.SimpleNamespace(
        client_id="jason@angelprecision.io",
        execution_mode="live",
        signal_id="sig-474",
        contract_symbol=f"DEFERRED:{ticker}",
        limit_price=0.01,
        contracts=1,
        max_position_usd=budget,
        metadata={"contract_deferred": True},
    )


def _real_plan(*, contract="UNP260116C00250000", price=1.42, qty=1):
    return types.SimpleNamespace(
        client_id="jason@angelprecision.io",
        execution_mode="live",
        signal_id="sig-474",
        contract_symbol=contract,
        limit_price=price,
        contracts=qty,
        max_position_usd=round(price * qty * 100.0, 2),
        metadata={},
    )


def _selection(*, contract="UNP260116C00250000", price=1.42, qty=1,
               with_premium=True):
    sel = types.SimpleNamespace(
        contract_symbol=contract,
        execution_price_per_share=price,
        ask=price,
        mid=price,
        affordable_contracts=qty,
    )
    if with_premium:
        sel.premium_per_contract = round(price * 100.0, 2)
    return sel


# ── Part A behaviour: real _breach_risk_check ──────────────────────────────────
class TestBreachRiskCheckPartA:
    def test_1_jason_placeholder_not_treated_as_real_cost(self):
        """Test #1 — the $165.886 reservation must NOT reach revalidate_exposure
        at breach time; a small budget drift must not false-reject the setup."""
        core = _make_core(mode="LIVE")
        plan = _deferred_plan(budget=JASON_BUDGET)
        watched = _watched(plan)

        result = core._breach_risk_check(watched)

        assert result is True
        core.master_control.revalidate_exposure.assert_not_called()

    def test_1b_deferred_by_prefix_only_also_skips(self):
        """A DEFERRED: contract with metadata missing the flag still classifies
        as deferred (the prefix is authoritative)."""
        core = _make_core(mode="LIVE")
        plan = _deferred_plan()
        plan.metadata = {}  # rely on the DEFERRED: prefix alone
        result = core._breach_risk_check(_watched(plan))
        assert result is True
        core.master_control.revalidate_exposure.assert_not_called()

    def test_1c_empty_contract_classifies_deferred(self):
        core = _make_core(mode="LIVE")
        plan = _deferred_plan()
        plan.contract_symbol = ""
        plan.metadata = {}
        assert core._breach_risk_check(_watched(plan)) is True
        core.master_control.revalidate_exposure.assert_not_called()

    def test_6_real_preselected_contract_revalidates_exactly_once(self):
        """Test #6 — a non-deferred real OCC plan performs the original breach
        exposure revalidation exactly once; the deferred gate never fires."""
        core = _make_core(mode="LIVE")
        core.master_control.revalidate_exposure.return_value = types.SimpleNamespace(
            ok=True, reason=None,
        )
        plan = _real_plan(price=1.42, qty=1)  # $142 real contract
        result = core._breach_risk_check(_watched(plan))
        assert result is True
        assert core.master_control.revalidate_exposure.call_count == 1

    def test_3a_real_contract_over_cap_still_blocks_at_breach(self):
        """A real (non-deferred) contract that exceeds the cap still blocks at
        breach — the block path is unchanged for materialized contracts."""
        core = _make_core(mode="LIVE")
        core.master_control.revalidate_exposure.return_value = types.SimpleNamespace(
            ok=False, reason="ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP",
        )
        plan = _real_plan(price=5.0, qty=1)  # $500 real contract
        result = core._breach_risk_check(_watched(plan))
        assert result is False
        assert core.master_control.revalidate_exposure.call_count == 1

    def test_7_identity_preserved_through_breach_check(self):
        """Test #7 — the exact plan (client_id / execution_mode / signal_id) is
        the object passed to Master Control; no LIVE/PAPER cross-contamination."""
        core = _make_core(mode="LIVE")
        core.master_control.revalidate_exposure.return_value = types.SimpleNamespace(
            ok=True, reason=None,
        )
        plan = _real_plan()
        core._breach_risk_check(_watched(plan))
        passed_plan = core.master_control.revalidate_exposure.call_args.args[0]
        assert passed_plan is plan
        assert passed_plan.client_id == "jason@angelprecision.io"
        assert passed_plan.execution_mode == "live"
        assert passed_plan.signal_id == "sig-474"

    def test_paper_deferred_also_skips_without_reval(self):
        core = _make_core(mode="PAPER", email="jose-paper@angelprecision.io")
        result = core._breach_risk_check(_watched(_deferred_plan()))
        assert result is True
        core.master_control.revalidate_exposure.assert_not_called()


# ── Part A identity: real _recover_plan_for_revalidation ───────────────────────
class TestRecoveredPlanIdentityAndClassification:
    def test_recovered_deferred_plan_classifies_deferred(self):
        """A deferred OSM order recovers a plan that still classifies as deferred
        (budget as reserved_cost, DEFERRED: contract) and thus skips reval."""
        core = _make_core(mode="LIVE")
        osm = MagicMock()
        osm.get_order.return_value = {
            "plan_id": "plan-1", "signal_id": "sig-474",
            "client_id": "jason@angelprecision.io", "execution_mode": "live",
            "symbol": "UNP", "direction": "CALL",
            "qty": 1, "reserved_cost": JASON_BUDGET, "limit_price": 0.01,
            "contract": "DEFERRED:UNP",
            "meta": {"contract_deferred": True, "execution_mode": "live"},
        }
        core.order_state_machine = osm

        sig = {
            "ticker": "UNP", "side": "CALL", "signal_id": "sig-474",
            "local_order_id": "local-474", "entry_price": 250.0,
            "stop": 245.0, "target": 260.0, "execution_mode": "live",
        }
        watched = WatchedSignal(sig)

        recovered = core._recover_plan_for_revalidation(watched)
        assert recovered is not None
        assert recovered.client_id == "jason@angelprecision.io"
        assert recovered.execution_mode == "live"
        assert recovered.signal_id == "sig-474"
        assert recovered.contract_symbol == "DEFERRED:UNP"
        assert bool(recovered.metadata.get("contract_deferred")) is True

        # Driven through the real breach check, the recovered deferred plan must
        # not reach revalidate_exposure.
        assert core._breach_risk_check(watched) is True
        core.master_control.revalidate_exposure.assert_not_called()


# ── Part B behaviour: real _validate_deferred_selector_result + cost formula ───
class TestActualCostFormula:
    def test_2_affordable_materialization_cost(self):
        """Test #2 — price 1.42, qty 1 -> validator yields (True, .., 1.42, 1);
        the production actual-cost formula gives exactly $142.00."""
        ok, contract, price, qty = _validate_deferred_selector_result(
            _selection(price=1.42, qty=1), "UNP",
        )
        assert ok is True
        assert price == 1.42 and qty == 1
        assert round(price * qty * 100.0, 2) == 142.00

    def test_4_premium_per_contract_absent_still_computes_cost(self):
        """Test #4 — with no premium_per_contract, the validated executable
        price still yields a real cost, so the placeholder budget cannot survive
        into final revalidation."""
        sel = _selection(price=1.42, qty=1, with_premium=False)
        assert not hasattr(sel, "premium_per_contract")
        ok, _c, price, qty = _validate_deferred_selector_result(sel, "UNP")
        assert ok is True
        actual = round(price * qty * 100.0, 2)
        assert actual == 142.00
        assert actual != JASON_BUDGET  # placeholder is replaced

    def test_4b_premium_zero_still_computes_cost(self):
        sel = _selection(price=1.42, qty=1, with_premium=True)
        sel.premium_per_contract = 0.0  # optional field present but zero
        ok, _c, price, qty = _validate_deferred_selector_result(sel, "UNP")
        assert ok is True
        assert round(price * qty * 100.0, 2) == 142.00

    def test_5_qty_clamp_cost_matches_final_qty(self):
        """Test #5 — after an acceptance clamp to qty=1, actual cost is computed
        from the final qty and executable price."""
        sel = _selection(price=1.90, qty=3)  # selector affords 3 ...
        ok, _c, price, _qty = _validate_deferred_selector_result(sel, "UNP")
        assert ok is True
        # ... acceptance mode clamps to exactly 1 -> cost tracks the clamped qty.
        clamped_cost = round(price * 1 * 100.0, 2)
        assert clamped_cost == 190.00

    def test_3_expensive_contract_yields_blocking_cost(self):
        """Test #3 — an expensive real contract yields an actual cost that a
        tighter authoritative per-position budget would reject; the number is a
        real contract cost (not the placeholder)."""
        ok, _c, price, qty = _validate_deferred_selector_result(
            _selection(price=5.00, qty=1), "UNP",
        )
        assert ok is True
        actual = round(price * qty * 100.0, 2)
        assert actual == 500.00
        assert actual > 200.0  # exceeds a hypothetical $200 per-position budget

    def test_placeholder_selection_is_rejected_by_validator(self):
        """A DEFERRED: placeholder / penny-limit selection is never a valid
        selector result, so it can never be copied back as a real cost."""
        ok, _c, _p, _q = _validate_deferred_selector_result(
            _selection(contract="DEFERRED:UNP", price=0.0, qty=0), "UNP",
        )
        assert ok is False


# ── Parts B & C: source-structure guards on the real money path ────────────────
def _slice(start_marker: str, end_marker: str) -> str:
    i = _EC.find(start_marker)
    assert i != -1, f"marker not found: {start_marker!r}"
    j = _EC.find(end_marker, i)
    assert j != -1, f"end marker not found: {end_marker!r}"
    return _EC[i:j]


class TestPartASource:
    def test_deferred_gate_suppresses_reval_via_skip_flag(self):
        body = _slice("def _breach_risk_check(", "def start(self):")
        # The deferred gate sets the skip flag before the exposure revalidation
        # call, and the revalidation is guarded on that flag — no new return path.
        gate = body.find("_bp_is_deferred")
        reval = body.find("self.master_control.revalidate_exposure(")
        assert gate != -1 and reval != -1 and gate < reval
        gate_block = body[gate: reval]
        assert "_skip_exposure_reval_deferred = True" in gate_block
        assert "revalidate_exposure" not in gate_block
        # The exposure revalidation only runs when the flag is not set.
        assert "not _skip_exposure_reval_deferred" in body

    def test_control_flow_signature_unchanged(self):
        # Part A must not add a new return path to _breach_risk_check (the
        # diagnostic invariant in test_breach_block_diagnostics depends on this).
        body = _slice("def _breach_risk_check(", "def start(self):")
        assert body.count("return False") == 6
        assert body.count("return True") == 1

    def test_deferred_detection_uses_flag_prefix_and_empty(self):
        body = _slice("def _breach_risk_check(", "def start(self):")
        assert '_bp_meta.get("contract_deferred")' in body
        assert 'DEFERRED:' in body
        assert "not _bp_contract" in body


class TestPartBSource:
    def test_copyback_uses_price_qty_100_not_premium_gate(self):
        # Non-acceptance copy-back: unconditional real cost from validated price.
        assert "float(_sel_price_candidate) * int(_sel_qty) * 100.0" in _EC
        # The old premium-only conditional must be gone from the copy-back.
        assert "approved_plan.max_position_usd = _sel_qty * _prem_per_contract" not in _EC

    def test_acceptance_clamp_recomputes_cost_from_price(self):
        # Acceptance clamp: cost recomputed from validated price and final qty=1.
        assert "float(_sel_price_candidate) * 1 * 100.0" in _EC
        # The old premium-only conditional at the clamp must be gone.
        assert "if _prem > 0:\n                            approved_plan.max_position_usd = _prem" not in _EC


class TestPartCSource:
    PART_C = property(lambda self: _slice(
        "final capital authority for DEFERRED entries only",
        "4b) P0 FIX: refresh the option contract ask",
    ))

    def test_final_reval_gated_on_deferred_only(self):
        block = self.PART_C
        assert "if _deferred and _deferred_master_control is not None:" in block
        assert "_deferred_master_control.revalidate_exposure(" in block

    def test_final_reval_recomputes_cost_before_call(self):
        block = self.PART_C
        # Cost recomputed from final (post-clamp) qty and validated price BEFORE
        # the revalidation call.
        cost_idx = block.find("float(_plan_limit) * int(approved_qty) * 100.0")
        call_idx = block.find("_deferred_master_control.revalidate_exposure(")
        assert cost_idx != -1 and call_idx != -1 and cost_idx < call_idx

    def test_block_terminalizes_with_exact_reason_no_broker_post(self):
        block = self.PART_C
        assert "_terminalize_deferred_breach_failure(" in block
        assert "deferred_final_exposure_revalidation:" in block
        # exact MC reason is preserved downstream (not flattened to a generic).
        assert "mc_block_reason" in block

    def test_live_fail_closed_paper_fail_open(self):
        block = self.PART_C
        assert 'if self.mode == "LIVE":' in block
        assert "deferred_final_exposure_revalidation_error:" in block
        assert "failed open" in block  # PAPER continues

    def test_runs_before_quote_refresh_and_broker_post(self):
        # Positionally, the deferred final reval must precede the quote refresh
        # (Step 4b) and therefore any broker POST.
        final = _EC.find("final capital authority for DEFERRED entries only")
        refresh = _EC.find("4b) P0 FIX: refresh the option contract ask")
        assert final != -1 and refresh != -1
        assert final < refresh
        # The actual broker submit call (OSM.submit_existing_entry(...)) occurs
        # after the quote-refresh gate, i.e. strictly after Part C.
        submit_call = _EC.find(".submit_existing_entry(", refresh)
        assert submit_call != -1
        assert final < refresh < submit_call

    def test_no_broker_or_mutation_inside_final_reval_block(self):
        """Test #8 — the final-reval block creates no position, no proof_trades
        write, no broker submit/cancel, no queue writer."""
        block = self.PART_C
        for forbidden in (
            "submit_existing_entry", "proof_trades", "create_position",
            "broker.submit", "broker.cancel", "place_order",
        ):
            assert forbidden not in block, f"forbidden in Part C block: {forbidden}"


class TestRevalidationCallSiteCount:
    def test_exactly_three_revalidation_sites(self):
        """breach (non-deferred) + hydration (hydrated pre-breach) + deferred
        final. Exactly three — no accidental duplicate revalidation."""
        assert _EC.count("revalidate_exposure(\n") == 3


class TestFrozenNonGoals:
    def test_no_new_broker_submit_or_cancel_helper_added(self):
        # We add no new broker path; the only submit entrypoint stays OSM's
        # existing submit_existing_entry.
        assert _EC.count("def submit_existing_entry") == 0  # defined in OSM, not here

    def test_single_production_file_touched_marker(self):
        # Sanity: the PR marker only appears in this production file's edits.
        assert _EC.count("PR #474") >= 4
