"""
P0 — Paper Static Capital Precheck: Split Per-Trade vs Total-Exposure Gate (PR #173)
======================================================================================

PROBLEM (observed 2026-06-22):
  Paper accounts were rejecting valid signals with messages like:
    jose.vasquez4011@gmail.com:  capital_limit (projected $10997 > $10024)
    tradefluencehq@gmail.com:   capital_limit (projected $2690 > $1960)

  The PAPER non-bootstrap static-estimate branch compared total projected
  portfolio exposure against per_trade_budget (a single-trade limit) because
  max_capital was aliased to per_trade_budget. This recreated the old single-cap
  bug: any signal arriving when existing exposure exceeded one trade budget was
  rejected even when the portfolio cap had plenty of room.

FIX (PR #173):
  The PAPER non-bootstrap else-branch now uses two separate gates:
    Gate 1 — per-trade estimate: estimated_new_cost_pre > per_trade_budget
    Gate 2 — total exposure:     current_total_exposure + estimated_new_cost_pre > total_capital_cap

  The old `projected_total > max_capital` (where max_capital == per_trade_budget) is removed.

SAFETY INVARIANTS (unchanged by this PR):
  * No quality/score/OI/spread gate weakened.
  * No broker submit/cancel touched.
  * No order/position mutation touched.
  * No live handoff/cron touched.
  * Only the PAPER static capital precheck math changes.
  * _pending_capital_from_snapshot_or_db preserved; no open_positions() bypass.

TEST CASES (from PR spec):
  T1  PAPER non-bootstrap, zero deployed/pending, cost below per-trade budget
      → passes capital precheck (not blocked by either gate)
  T2  PAPER non-bootstrap, existing exposure == one per-trade budget, total cap has room
      → passes (production regression: the old code blocked this; new code does not)
  T3  PAPER non-bootstrap, single-trade estimate > per-trade budget
      → CAPITAL_LIMIT_PER_TRADE_ESTIMATE
  T4  PAPER non-bootstrap, projected total > total_capital_cap
      → CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED
  T5  LIVE mode: affordability flow unchanged; paper gate reason codes never fire in LIVE
  T6  _pending_capital_from_snapshot_or_db still called; exposure not bypassed

KEY NUMERIC ASSUMPTIONS (at MAX_TRADE_USD=1800 env default):
  AAPL  premium $1.50,  score 70 → fraction 0.65, budget $1,170
        _base_contracts = max(2, min(10, int(1170/150))) = max(2, 7) = 7
        estimated_new_cost_pre = 7 × 100 × $1.50 = $1,050

  NVDA  premium $4.50,  score 85 → fraction 1.00, budget $1,800
        _base_contracts = max(2, min(10, int(1800/450))) = max(2, 4) = 4
        estimated_new_cost_pre = 4 × 100 × $4.50 = $1,800
"""
from __future__ import annotations

import inspect
import os
import sys
from unittest.mock import MagicMock

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import ap_master_control as mc

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------
BOOTSTRAP_THRESHOLD = int(os.getenv("BOOTSTRAP_TRADES_THRESHOLD", "20"))
# Contract cost constants — must match _estimate_premium() + _base_contracts() at defaults
AAPL_PREMIUM   = 1.50
AAPL_CONTRACTS = 7       # _base_contracts(70.0, 1.50) at MAX_TRADE_USD=1800
AAPL_COST_PRE  = AAPL_CONTRACTS * 100 * AAPL_PREMIUM   # $1,050

NVDA_PREMIUM   = 4.50
NVDA_CONTRACTS = 4       # _base_contracts(85.0, 4.50) at MAX_TRADE_USD=1800
NVDA_COST_PRE  = NVDA_CONTRACTS * 100 * NVDA_PREMIUM   # $1,800


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_paper_mc(
    *,
    account_equity: float = 25_000.0,
    max_capital_pct: float = 0.10,
    capital_deployed: float = 0.0,
    pending_capital: float = 0.0,
) -> mc.APMasterControl:
    """Build a PAPER (non-bootstrap) APMasterControl with all external
    dependencies stubbed.  total_trades is above BOOTSTRAP_THRESHOLD so
    bootstrap_mode=False and the PAPER static precheck else-branch is active."""
    m = mc.APMasterControl(
        mode="paper",
        score_floor=60.0,
        context_floor=0.0,
        account_equity=account_equity,
        max_capital_pct=max_capital_pct,
        pending_capital_fail_closed_live=False,
        require_snapshot_freshness_live=False,
    )
    # Snapshot with total_trades above threshold → bootstrap_mode=False.
    m._get_snapshot = MagicMock(return_value={
        "_snapshot_ok":              True,
        "_snapshot_ts":              "2026-06-22T13:00:00Z",
        "_snapshot_age_sec":         0.1,
        "open_count":                0,
        "pending_entries":           0,
        "capital_deployed":          capital_deployed,
        "position_capital_deployed": capital_deployed,
        "calls_open":                0,
        "puts_open":                 0,
        "filled_unreconciled_calls": 0,
        "filled_unreconciled_puts":  0,
        "trades_today":              0,
        "open_positions":            [],
        "closing_positions":         [],
        "realized_pnl_today":        0.0,
        "total_trades":              BOOTSTRAP_THRESHOLD + 5,  # non-bootstrap
        "ticker_open_counts":        {},
        "ticker_pending_counts":     {},
    })
    # Stub pending-capital helper to our controlled value.
    m._pending_capital_from_snapshot_or_db = MagicMock(return_value=pending_capital)
    # Stub kill-switch inactive.
    m._kill_switch_fn = lambda: False
    # Stub sector/ticker deployed to 0 so those gates do not block.
    m._sector_capital_deployed = MagicMock(return_value=0.0)
    m._ticker_capital_deployed = MagicMock(return_value=0.0)
    # Disable noisy helpers that call Supabase / broker.
    m._log_capital_utilization = MagicMock()
    m._equity_snapshot = MagicMock(return_value=(account_equity, -500.0))
    m.feedback_loop = None
    m.tier_engine = None
    m.sizer = None
    m._signal_store = None
    m._idem_store_signal = MagicMock(return_value=True)
    m._persist_dedup = MagicMock(return_value=None)
    m._options_intelligence = None
    m._fetch_intel = MagicMock(return_value={
        "available": False, "score": 0.0, "reason": "intel_disabled_in_test",
        "contracts": 0, "setup": {}, "breakdown": {},
    })
    return m


def _make_live_mc(
    *,
    account_equity: float = 25_000.0,
    max_capital_pct: float = 0.10,
    capital_deployed: float = 0.0,
    pending_capital: float = 0.0,
) -> mc.APMasterControl:
    """Build a LIVE (post-bootstrap) APMasterControl with all external
    dependencies stubbed."""
    m = mc.APMasterControl(
        mode="live",
        score_floor=60.0,
        context_floor=0.0,
        account_equity=account_equity,
        max_capital_pct=max_capital_pct,
        pending_capital_fail_closed_live=False,
        require_snapshot_freshness_live=False,
    )
    m._get_snapshot = MagicMock(return_value={
        "_snapshot_ok":              True,
        "_snapshot_ts":              "2026-06-22T13:00:00Z",
        "_snapshot_age_sec":         0.1,
        "open_count":                0,
        "pending_entries":           0,
        "capital_deployed":          capital_deployed,
        "position_capital_deployed": capital_deployed,
        "calls_open":                0,
        "puts_open":                 0,
        "filled_unreconciled_calls": 0,
        "filled_unreconciled_puts":  0,
        "trades_today":              0,
        "open_positions":            [],
        "closing_positions":         [],
        "realized_pnl_today":        0.0,
        "total_trades":              BOOTSTRAP_THRESHOLD + 5,  # post-bootstrap
        "ticker_open_counts":        {},
        "ticker_pending_counts":     {},
    })
    m._pending_capital_from_snapshot_or_db = MagicMock(return_value=pending_capital)
    m._kill_switch_fn = lambda: False
    m._sector_capital_deployed = MagicMock(return_value=0.0)
    m._ticker_capital_deployed = MagicMock(return_value=0.0)
    m._log_capital_utilization = MagicMock()
    m._equity_snapshot = MagicMock(return_value=(account_equity, -500.0))
    m.feedback_loop = None
    m.tier_engine = None
    m.sizer = None
    m._signal_store = None
    m._idem_store_signal = MagicMock(return_value=True)
    m._persist_dedup = MagicMock(return_value=None)
    m._options_intelligence = None
    m._fetch_intel = MagicMock(return_value={
        "available": False, "score": 0.0, "reason": "intel_disabled_in_test",
        "contracts": 0, "setup": {}, "breakdown": {},
    })
    return m


def _aapl_signal(score: float = 70.0) -> dict:
    return {
        "signal_id":  "test-sig-aapl-pr173",
        "ticker":     "AAPL",
        "symbol":     "AAPL",
        "side":       "CALL",
        "direction":  "CALL",
        "score":      score,
        "ev_score":   score,
        "tier":       "B",
        "pattern":    "2-2",
        "timeframe":  "1d",
        "trigger":    {"entry": 200.0, "stop": 198.0, "pt1": 205.0},
    }


def _nvda_signal(score: float = 85.0) -> dict:
    return {
        "signal_id":  "test-sig-nvda-pr173",
        "ticker":     "NVDA",
        "symbol":     "NVDA",
        "side":       "CALL",
        "direction":  "CALL",
        "score":      score,
        "ev_score":   score,
        "tier":       "A",
        "pattern":    "2-2",
        "timeframe":  "1d",
        "trigger":    {"entry": 900.0, "stop": 895.0, "pt1": 910.0},
    }


def _paper_capital_block(decision) -> bool:
    """True iff the block originated from the PAPER static precheck gates (PR #173)."""
    return decision.reason_code in {
        "CAPITAL_LIMIT_PER_TRADE_ESTIMATE",
        "CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED",
    }


# ---------------------------------------------------------------------------
# Source-level invariants — verify the new code shape exists in evaluate()
# ---------------------------------------------------------------------------

class TestPR173SourceInvariants:
    """Fail-fast checks: if the source doesn't contain the new gates the
    behavioural tests below could silently pass for the wrong reason."""

    def test_per_trade_estimate_reason_code_in_evaluate_source(self):
        src = inspect.getsource(mc.APMasterControl.evaluate)
        assert "CAPITAL_LIMIT_PER_TRADE_ESTIMATE" in src, (
            "PR #173: CAPITAL_LIMIT_PER_TRADE_ESTIMATE must exist in evaluate() "
            "(Gate 1 of the PAPER static precheck)"
        )

    def test_total_exposure_cap_reason_code_in_evaluate_source(self):
        src = inspect.getsource(mc.APMasterControl.evaluate)
        assert "CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED" in src, (
            "PR #173: CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED must exist in evaluate() "
            "(Gate 2 of the PAPER static precheck)"
        )

    def test_old_single_gate_string_absent(self):
        """The old `capital_limit (projected $X > $Y)` string that used
        max_capital (alias for per_trade_budget) must be gone from the
        PAPER precheck else-branch."""
        src = inspect.getsource(mc.APMasterControl.evaluate)
        assert 'f"capital_limit (projected ${projected_total:.0f} > ${max_capital:.0f})"' not in src, (
            "PR #173: old single-gate reason string must be removed from evaluate(). "
            "It compared total exposure to per_trade_budget — the root cause of PR #173."
        )

    def test_per_trade_budget_and_total_cap_both_present(self):
        """Both split-cap variables must be computed in evaluate()."""
        src = inspect.getsource(mc.APMasterControl.evaluate)
        assert "per_trade_budget" in src
        assert "total_capital_cap" in src

    def test_pending_capital_helper_still_in_evaluate(self):
        """_pending_capital_from_snapshot_or_db must not have been removed
        and replaced with a direct open_positions() call."""
        src = inspect.getsource(mc.APMasterControl.evaluate)
        assert "_pending_capital_from_snapshot_or_db" in src, (
            "PR #173 must NOT remove _pending_capital_from_snapshot_or_db from evaluate(). "
            "Removing it would bypass pending/fill-unreconciled exposure accounting."
        )


# ---------------------------------------------------------------------------
# T1 — Zero deployed/pending; cost below per-trade budget → passes precheck
# ---------------------------------------------------------------------------

class TestT1ZeroDeployedPassesCapitalPrecheck:
    """
    equity=$25,000  max_capital_pct=0.10
    → per_trade_budget=$2,500   total_capital_cap=$10,000
    capital_deployed=0  pending=0  → current_total_exposure=$0

    AAPL score=70: estimated_new_cost_pre=$1,050
      Gate 1: $1,050 > $2,500?  No → pass
      Gate 2: ($0 + $1,050) > $10,000?  No → pass

    Expected: capital precheck does NOT block (no capital-limit reason code).
    """

    def test_per_trade_gate_does_not_fire(self):
        m = _make_paper_mc(
            account_equity=25_000.0, max_capital_pct=0.10,
            capital_deployed=0.0, pending_capital=0.0,
        )
        decision = m.evaluate(_aapl_signal(score=70.0))
        assert decision.reason_code != "CAPITAL_LIMIT_PER_TRADE_ESTIMATE", (
            f"T1: Gate 1 must not fire when estimated_new_cost_pre (${AAPL_COST_PRE:.0f}) "
            f"< per_trade_budget ($2,500). Got reason={decision.reason!r}"
        )

    def test_total_exposure_gate_does_not_fire(self):
        m = _make_paper_mc(
            account_equity=25_000.0, max_capital_pct=0.10,
            capital_deployed=0.0, pending_capital=0.0,
        )
        decision = m.evaluate(_aapl_signal(score=70.0))
        assert decision.reason_code != "CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED", (
            f"T1: Gate 2 must not fire when projected ($0 + ${AAPL_COST_PRE:.0f}) "
            f"< total_cap ($10,000). Got reason={decision.reason!r}"
        )

    def test_neither_paper_capital_gate_fires(self):
        m = _make_paper_mc(
            account_equity=25_000.0, max_capital_pct=0.10,
            capital_deployed=0.0, pending_capital=0.0,
        )
        decision = m.evaluate(_aapl_signal(score=70.0))
        assert not _paper_capital_block(decision), (
            f"T1: neither paper capital gate should fire with zero exposure and "
            f"affordable signal. Got reason_code={decision.reason_code!r}"
        )


# ---------------------------------------------------------------------------
# T2 — Existing exposure == one per-trade budget; total cap has room → passes
# ---------------------------------------------------------------------------

class TestT2ExistingExposureEqualsOneBudgetTotalCapHasRoom:
    """
    PRODUCTION REGRESSION — the exact failure observed 2026-06-22:
      jose.vasquez4011@gmail.com: capital_limit (projected $10997 > $10024)
      tradefluencehq@gmail.com:  capital_limit (projected $2690 > $1960)

    The old code:
      projected_total = deployed + pending + estimated_cost
      if projected_total > max_capital:   ← max_capital == per_trade_budget ← BUG

    equity=$25,000  max_capital_pct=0.10
    → per_trade_budget=$2,500   total_capital_cap=$10,000
    capital_deployed=$2,500  pending=0  → current_total_exposure=$2,500

    AAPL score=70: estimated_new_cost_pre=$1,050
      OLD: projected_total = $2,500 + 0 + $1,050 = $3,550 > $2,500 → BLOCKED ❌
      NEW Gate 1: $1,050 > $2,500?  No → pass
      NEW Gate 2: ($2,500 + $1,050 = $3,550) > $10,000?  No → pass ✓
    """

    def test_not_blocked_when_total_cap_has_room(self):
        m = _make_paper_mc(
            account_equity=25_000.0, max_capital_pct=0.10,
            capital_deployed=2_500.0, pending_capital=0.0,
        )
        decision = m.evaluate(_aapl_signal(score=70.0))
        assert not _paper_capital_block(decision), (
            f"T2 (production regression): must NOT block when total_cap has room. "
            f"Old single-gate would have blocked here "
            f"($2,500 + ${AAPL_COST_PRE:.0f} = ${2500 + AAPL_COST_PRE:.0f} > $2,500). "
            f"Got reason_code={decision.reason_code!r} reason={decision.reason!r}"
        )

    def test_old_reason_string_absent(self):
        """Guard against the old reason format reappearing."""
        m = _make_paper_mc(
            account_equity=25_000.0, max_capital_pct=0.10,
            capital_deployed=2_500.0, pending_capital=0.0,
        )
        decision = m.evaluate(_aapl_signal(score=70.0))
        reason = (decision.reason or "").lower()
        assert "capital_limit (projected" not in reason, (
            "T2: old single-gate reason string 'capital_limit (projected …)' must not appear. "
            f"Got: {decision.reason!r}"
        )

    def test_pending_exposure_plus_deployed_still_within_total_cap_passes(self):
        """capital_deployed=$1,000, pending=$500 → exposure=$1,500.
        $1,500 + $1,050 = $2,550 < $10,000 → should pass Gate 2."""
        m = _make_paper_mc(
            account_equity=25_000.0, max_capital_pct=0.10,
            capital_deployed=1_000.0, pending_capital=500.0,
        )
        decision = m.evaluate(_aapl_signal(score=70.0))
        assert not _paper_capital_block(decision), (
            f"T2c: deployed=$1,000 + pending=$500 + estimated=${AAPL_COST_PRE:.0f} "
            f"= ${1000 + 500 + AAPL_COST_PRE:.0f} < $10,000 total_cap. "
            f"Capital precheck must not block. Got reason_code={decision.reason_code!r}"
        )


# ---------------------------------------------------------------------------
# T3 — Single-trade estimate > per-trade budget → CAPITAL_LIMIT_PER_TRADE_ESTIMATE
# ---------------------------------------------------------------------------

class TestT3PerTradeBudgetExceeded:
    """
    equity=$5,000  max_capital_pct=0.10
    → per_trade_budget=$500   total_capital_cap=$2,000
    capital_deployed=0  pending=0

    NVDA score=85: estimated_new_cost_pre=$1,800
      Gate 1: $1,800 > $500?  Yes → CAPITAL_LIMIT_PER_TRADE_ESTIMATE
    """

    def test_blocks_with_per_trade_estimate_reason_code(self):
        m = _make_paper_mc(
            account_equity=5_000.0, max_capital_pct=0.10,
            capital_deployed=0.0, pending_capital=0.0,
        )
        decision = m.evaluate(_nvda_signal(score=85.0))
        assert not decision.ok, (
            f"T3: must block when estimated_new_cost_pre (${NVDA_COST_PRE:.0f}) "
            f"> per_trade_budget ($500)"
        )
        assert decision.reason_code == "CAPITAL_LIMIT_PER_TRADE_ESTIMATE", (
            f"T3: expected CAPITAL_LIMIT_PER_TRADE_ESTIMATE, got {decision.reason_code!r}"
        )

    def test_reason_string_identifies_per_trade_gate(self):
        m = _make_paper_mc(
            account_equity=5_000.0, max_capital_pct=0.10,
            capital_deployed=0.0, pending_capital=0.0,
        )
        decision = m.evaluate(_nvda_signal(score=85.0))
        assert decision.reason_code == "CAPITAL_LIMIT_PER_TRADE_ESTIMATE"
        reason = (decision.reason or "").lower()
        assert "capital_limit_per_trade_estimate" in reason, (
            f"T3: reason string must name the per-trade gate. Got: {decision.reason!r}"
        )

    def test_reason_string_contains_estimated_amount(self):
        m = _make_paper_mc(
            account_equity=5_000.0, max_capital_pct=0.10,
            capital_deployed=0.0, pending_capital=0.0,
        )
        decision = m.evaluate(_nvda_signal(score=85.0))
        assert decision.reason_code == "CAPITAL_LIMIT_PER_TRADE_ESTIMATE"
        assert "estimated=" in decision.reason or "estimated=$" in decision.reason, (
            f"T3: reason string must include estimated cost. Got: {decision.reason!r}"
        )

    def test_zero_existing_exposure_does_not_prevent_per_trade_block(self):
        """Gate 1 fires on estimated_new_cost_pre alone regardless of deployed capital."""
        m = _make_paper_mc(
            account_equity=5_000.0, max_capital_pct=0.10,
            capital_deployed=0.0, pending_capital=0.0,
        )
        decision = m.evaluate(_nvda_signal(score=85.0))
        assert decision.reason_code == "CAPITAL_LIMIT_PER_TRADE_ESTIMATE", (
            "T3d: Gate 1 must fire even when deployed=0, pending=0 — "
            "it is about single-trade size, not total exposure."
        )


# ---------------------------------------------------------------------------
# T4 — Projected total > total_capital_cap → CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED
# ---------------------------------------------------------------------------

class TestT4TotalCapExceeded:
    """
    equity=$25,000  max_capital_pct=0.10
    → per_trade_budget=$2,500   total_capital_cap=$10,000
    capital_deployed=$9,500  pending=0  → current_total_exposure=$9,500
    (remaining_total_cap=$500 > 0 → early hard-block does NOT fire)

    AAPL score=70: estimated_new_cost_pre=$1,050
      Gate 1: $1,050 > $2,500?  No → pass
      Gate 2: ($9,500 + $1,050 = $10,550) > $10,000?  Yes → CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED
    """

    def test_blocks_with_total_exposure_cap_reason_code(self):
        m = _make_paper_mc(
            account_equity=25_000.0, max_capital_pct=0.10,
            capital_deployed=9_500.0, pending_capital=0.0,
        )
        decision = m.evaluate(_aapl_signal(score=70.0))
        assert not decision.ok, (
            f"T4: must block when projected (${9500 + AAPL_COST_PRE:.0f}) "
            f"> total_capital_cap ($10,000)"
        )
        assert decision.reason_code == "CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED", (
            f"T4: expected CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED, got {decision.reason_code!r}"
        )

    def test_reason_string_identifies_total_cap_gate(self):
        m = _make_paper_mc(
            account_equity=25_000.0, max_capital_pct=0.10,
            capital_deployed=9_500.0, pending_capital=0.0,
        )
        decision = m.evaluate(_aapl_signal(score=70.0))
        reason = (decision.reason or "").lower()
        assert "capital_limit_total_exposure_cap_reached" in reason, (
            f"T4: reason string must name the total-exposure gate. Got: {decision.reason!r}"
        )

    def test_gate1_does_not_fire_gate2_does(self):
        """Verify Gate 1 is NOT the reason (single-trade cost is affordable);
        Gate 2 is the one that blocks because the portfolio is saturated."""
        m = _make_paper_mc(
            account_equity=25_000.0, max_capital_pct=0.10,
            capital_deployed=9_500.0, pending_capital=0.0,
        )
        decision = m.evaluate(_aapl_signal(score=70.0))
        assert decision.reason_code != "CAPITAL_LIMIT_PER_TRADE_ESTIMATE", (
            f"T4c: Gate 1 (per-trade) must NOT fire when estimated_cost (${AAPL_COST_PRE:.0f}) "
            f"< per_trade_budget ($2,500). Gate 2 should be the reason. "
            f"Got reason_code={decision.reason_code!r}"
        )
        assert decision.reason_code == "CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED"

    def test_pending_capital_counted_toward_exposure_gate(self):
        """Pending capital is part of current_total_exposure and must push
        the projection over the cap even when deployed alone would not.

        capital_deployed=$8,700  pending=$500  → exposure=$9,200
        Gate 2: $9,200 + $1,050 = $10,250 > $10,000 → block
        Without pending: $8,700 + $1,050 = $9,750 < $10,000 → pass (not blocked)
        """
        m = _make_paper_mc(
            account_equity=25_000.0, max_capital_pct=0.10,
            capital_deployed=8_700.0, pending_capital=500.0,
        )
        decision = m.evaluate(_aapl_signal(score=70.0))
        assert not decision.ok
        assert decision.reason_code == "CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED", (
            f"T4d: pending capital must count toward Gate 2. "
            f"Without pending, $8,700 + ${AAPL_COST_PRE:.0f} = ${8700 + AAPL_COST_PRE:.0f} "
            f"< $10,000 (would NOT block). With pending=$500: "
            f"${8700 + 500 + AAPL_COST_PRE:.0f} > $10,000 (should block). "
            f"Got reason_code={decision.reason_code!r}"
        )


# ---------------------------------------------------------------------------
# T5 — LIVE affordability flow unchanged; paper gate codes never fire in LIVE
# ---------------------------------------------------------------------------

class TestT5LiveAffordabilityFlowUnchanged:
    """
    LIVE mode must use _use_affordability_flow=True (the if-branch), not the
    else-branch where the new PAPER gates live.  The new reason codes
    CAPITAL_LIMIT_PER_TRADE_ESTIMATE and CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED
    must never be produced by the LIVE path in evaluate().
    """

    def test_paper_reason_codes_absent_in_live_evaluate_run(self):
        """End-to-end: LIVE evaluate() must never return the paper-specific
        codes from this branch, even when capital_deployed would trip the
        paper Gate 2 if mode were PAPER."""
        m = _make_live_mc(
            account_equity=25_000.0, max_capital_pct=0.10,
            capital_deployed=9_500.0, pending_capital=0.0,
        )
        decision = m.evaluate(_aapl_signal(score=70.0))
        assert decision.reason_code != "CAPITAL_LIMIT_PER_TRADE_ESTIMATE", (
            "T5: LIVE evaluate() must not produce CAPITAL_LIMIT_PER_TRADE_ESTIMATE "
            "(that code lives in the PAPER static-estimate else-branch only)"
        )
        assert decision.reason_code != "CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED", (
            "T5: LIVE evaluate() with remaining_total_cap > 0 must not produce "
            "CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED from the paper static-estimate "
            "else-branch (revalidate_exposure may use a different code path)"
        )

    def test_use_affordability_flow_variable_gated_on_live_mode(self):
        """Source-level: _use_affordability_flow must reference _is_live_mode()
        so LIVE always uses the affordability path."""
        src = inspect.getsource(mc.APMasterControl.evaluate)
        assert "_use_affordability_flow" in src, (
            "T5: _use_affordability_flow flag must remain in evaluate()"
        )
        assert "_is_live_mode" in src, (
            "T5: _is_live_mode() must be part of the _use_affordability_flow condition"
        )

    def test_paper_gate_codes_only_in_else_branch_not_live_branch(self):
        """Source order: CAPITAL_LIMIT_PER_TRADE_ESTIMATE must appear AFTER the
        _use_affordability_flow variable is declared, confirming it is inside the
        else (PAPER) branch, not the if (LIVE/bootstrap) branch."""
        src = inspect.getsource(mc.APMasterControl.evaluate)
        flow_idx = src.index("_use_affordability_flow")
        per_trade_idx = src.index("CAPITAL_LIMIT_PER_TRADE_ESTIMATE")
        assert per_trade_idx > flow_idx, (
            "T5: CAPITAL_LIMIT_PER_TRADE_ESTIMATE must appear after the "
            "_use_affordability_flow declaration (i.e., in the else/PAPER branch)"
        )


# ---------------------------------------------------------------------------
# T6 — _pending_capital_from_snapshot_or_db still called; exposure not bypassed
# ---------------------------------------------------------------------------

class TestT6PendingCapitalStillCounted:
    """
    The PR #173 fix must NOT replace _pending_capital_from_snapshot_or_db with a
    raw open_positions() call or any other shortcut. Pending orders and
    fill-unreconciled exposure must continue to be counted by the existing helper.
    """

    def test_pending_capital_helper_called_during_paper_evaluate(self):
        """_pending_capital_from_snapshot_or_db must be invoked when evaluate()
        runs for a PAPER non-bootstrap client."""
        m = _make_paper_mc(
            account_equity=25_000.0, max_capital_pct=0.10,
            capital_deployed=0.0, pending_capital=0.0,
        )
        m.evaluate(_aapl_signal(score=70.0))
        assert m._pending_capital_from_snapshot_or_db.called, (
            "T6: _pending_capital_from_snapshot_or_db must be called during PAPER "
            "evaluate() — removing it would bypass pending/fill-unreconciled accounting."
        )

    def test_pending_capital_return_value_influences_gate2(self):
        """Non-zero pending capital must contribute to current_total_exposure and
        can cause Gate 2 to fire even when deployed alone would not.

        deployed=$8,700 + pending=$500 = exposure=$9,200
        Gate 2: $9,200 + $1,050 = $10,250 > $10,000 → blocked

        Control: deployed=$8,700 + pending=$0 = $9,750 < $10,000 → NOT blocked
        """
        # With pending=$500 → Gate 2 fires.
        m_with_pending = _make_paper_mc(
            account_equity=25_000.0, max_capital_pct=0.10,
            capital_deployed=8_700.0, pending_capital=500.0,
        )
        d_with = m_with_pending.evaluate(_aapl_signal(score=70.0))
        assert d_with.reason_code == "CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED", (
            f"T6b(with pending): expected Gate 2 block, got {d_with.reason_code!r}"
        )

        # Without pending → Gate 2 does NOT fire.
        m_without_pending = _make_paper_mc(
            account_equity=25_000.0, max_capital_pct=0.10,
            capital_deployed=8_700.0, pending_capital=0.0,
        )
        d_without = m_without_pending.evaluate(_aapl_signal(score=70.0))
        assert d_without.reason_code != "CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED", (
            f"T6b(no pending): Gate 2 must NOT fire when deployed_only + cost "
            f"= ${8700 + AAPL_COST_PRE:.0f} < $10,000 total_cap. "
            f"Got reason_code={d_without.reason_code!r}"
        )

    def test_pending_capital_not_replaced_by_direct_db_call(self):
        """Source guard: evaluate() must continue to call
        _pending_capital_from_snapshot_or_db, not a raw position lookup."""
        src = inspect.getsource(mc.APMasterControl.evaluate)
        assert "_pending_capital_from_snapshot_or_db" in src, (
            "T6c: _pending_capital_from_snapshot_or_db must remain in evaluate(). "
            "Removing it would bypass pending-order / fill-unreconciled exposure accounting."
        )
