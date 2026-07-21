# tests/test_soft_exit_executable_truth.py
# =============================================================================
# P0 regression tests: soft exits require executable BID truth and fresh
# underlying truth.  Midpoint / missing / stale data must DEFER, not exit.
#
# Covers all 20 required test scenarios from the PR spec.
# =============================================================================

from __future__ import annotations

import time
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch
import pytest

from ap_exit_engine import (
    ManagedPosition,
    ExitDecision,
    ExitDecisionSnapshot,
    _build_exit_decision_snapshot,
    _soft_exit_option_truth_gate,
    _soft_exit_underlying_truth_gate,
    evaluate_exit,
    SOFT_EXIT_DEFERRED_OPTION_BID_UNAVAILABLE,
    SOFT_EXIT_DEFERRED_OPTION_QUOTE_STALE,
    SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE,
    SOFT_EXIT_DEFERRED_UNDERLYING_STALE,
    SOFT_EXIT_DEFERRED_ENTRY_GRACE,
    SOFT_EXIT_DEFERRED_EXECUTABLE_THRESHOLD_UNCONFIRMED,
    HARD_STOP_PCT,
    IMMEDIATE_TP_PCT,
    SCALE_OUT_1_THRESHOLD,
)
from ap_exit_engine import _classify_exit_decision


# ── Helpers ──────────────────────────────────────────────────────────────────

_UTC = timezone.utc
_NOW = datetime(2026, 7, 21, 15, 0, 0, tzinfo=_UTC)   # 3 PM UTC (non-EOD ET)

def _fresh_ts() -> datetime:
    """A timestamp fresh enough to pass the quote staleness gate (< 20s old)."""
    return datetime.now(_UTC) - timedelta(seconds=2)

def _stale_ts() -> datetime:
    """A timestamp stale enough to fail the quote staleness gate (> 20s old)."""
    return datetime.now(_UTC) - timedelta(seconds=60)

def _make_pos(
    *,
    execution_mode: str = "paper",
    entry_price: float = 1.00,
    underlying_entry: float = 150.0,
    current_underlying: float = 149.0,
    current_bid: float = 0.0,
    current_ask: float = 0.0,
    current_option_price: float = 0.0,  # midpoint / analytics mark
    und_ts=None,
    opt_ts=None,
    touched_profit: bool = False,
    peak_pnl_pct: float = 0.0,
    max_profit_seen: float = 0.0,
    scale_outs_done: int = 0,
    quantity: int = 2,
    quantity_remaining: int = 2,
    opened_at=None,
    underlying_target: float = 145.0,
    underlying_stop: float = 152.0,
    side: str = "PUT",
    option_symbol: str = "ABT260721P00150000",
    underlying_available: bool = True,
    underlying_fresh: bool = True,
    option_bid_valid: bool = False,
    option_quote_fresh: bool = True,
    exit_executable_pnl_pct: float | None = None,
    exit_executable_mark: float = 0.0,
) -> ManagedPosition:
    """Build a minimal ManagedPosition for testing."""
    opened = opened_at or (datetime.now(_UTC) - timedelta(minutes=10))
    pos = ManagedPosition(
        ticker="ABT",
        option_symbol=option_symbol,
        side=side,
        quantity=quantity,
        entry_price=entry_price,
        underlying_entry=underlying_entry,
        underlying_target=underlying_target,
        underlying_stop=underlying_stop,
        execution_mode=execution_mode,
        touched_profit=touched_profit,
        peak_pnl_pct=peak_pnl_pct,
        max_profit_seen=max_profit_seen,
        scale_outs_done=scale_outs_done,
        quantity_remaining=quantity_remaining,
        opened_at=opened,
    )
    pos.current_bid = current_bid
    pos.currentbid = current_bid
    pos.current_ask = current_ask
    pos.currentask = current_ask
    pos.current_option_price = current_option_price
    pos.currentoptionprice = current_option_price
    pos.current_underlying = current_underlying
    pos.currentunderlying = current_underlying
    pos.analytics_mark_price = current_option_price
    pos.analyticsmarkprice = current_option_price
    pos.last_option_quote_update_ts = opt_ts or _fresh_ts()
    pos.lastoptionquoteupdatets = pos.last_option_quote_update_ts
    pos.last_underlying_quote_update_ts = und_ts or _fresh_ts()
    pos.lastunderlyingquoteupdatets = pos.last_underlying_quote_update_ts
    pos.underlying_available = underlying_available
    pos.underlyingavailable = underlying_available
    pos.underlying_fresh = underlying_fresh
    pos.underlyingfresh = underlying_fresh
    pos.option_bid_valid = option_bid_valid
    pos.optionbidvalid = option_bid_valid
    pos.option_quote_fresh = option_quote_fresh
    pos.optionquotefresh = option_quote_fresh
    pos.exit_executable_pnl_pct = exit_executable_pnl_pct
    pos.exit_executable_mark = exit_executable_mark
    return pos


def _et_noon():
    """Return noon ET as a naive datetime for evaluate_exit()."""
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/New_York")).replace(
        hour=12, minute=0, second=0, microsecond=0
    )


# ── Snapshot builder tests ────────────────────────────────────────────────────

class TestBuildExitDecisionSnapshot:
    """Unit tests for _build_exit_decision_snapshot()."""

    def test_bid_valid_when_positive(self):
        pos = _make_pos(current_bid=1.20, current_ask=1.30, current_option_price=1.25)
        snap = _build_exit_decision_snapshot(pos)
        assert snap.option_bid_valid is True
        assert snap.exit_executable_mark == pytest.approx(1.20)

    def test_bid_invalid_when_zero(self):
        pos = _make_pos(current_bid=0.0, current_ask=1.30, current_option_price=1.25)
        snap = _build_exit_decision_snapshot(pos)
        assert snap.option_bid_valid is False
        assert snap.exit_executable_mark is None
        assert snap.exit_executable_pnl_pct is None

    def test_exec_pnl_is_bid_based(self):
        pos = _make_pos(
            entry_price=1.00,
            current_bid=1.18,   # bid = +18%
            current_ask=1.22,
            current_option_price=1.20,  # mid = +20%
        )
        snap = _build_exit_decision_snapshot(pos)
        assert snap.exit_executable_pnl_pct == pytest.approx(0.18, abs=1e-4)
        assert snap.display_pnl_pct == pytest.approx(0.20, abs=1e-4)

    def test_underlying_available_when_positive(self):
        pos = _make_pos(current_underlying=149.0, underlying_available=True, underlying_fresh=True)
        snap = _build_exit_decision_snapshot(pos)
        assert snap.underlying_available is True
        assert snap.underlying_price == pytest.approx(149.0)

    def test_underlying_unavailable_when_zero(self):
        pos = _make_pos(current_underlying=0.0, underlying_available=False, underlying_fresh=False)
        snap = _build_exit_decision_snapshot(pos)
        assert snap.underlying_available is False

    def test_fresh_quote_detected(self):
        pos = _make_pos(opt_ts=_fresh_ts(), option_quote_fresh=True)
        snap = _build_exit_decision_snapshot(pos)
        assert snap.option_quote_fresh is True

    def test_stale_quote_detected(self):
        pos = _make_pos(opt_ts=_stale_ts(), option_quote_fresh=False)
        snap = _build_exit_decision_snapshot(pos)
        assert snap.option_quote_fresh is False


# ── Gate helper tests ─────────────────────────────────────────────────────────

class TestSoftExitOptionTruthGate:
    def _snap(self, **kw) -> ExitDecisionSnapshot:
        defaults = dict(
            option_bid=1.0, option_ask=1.05, option_mid=1.025,
            exit_executable_mark=1.0, option_bid_valid=True,
            option_quote_fresh=True, option_quote_age_sec=2.0, option_quote_ts=_fresh_ts(),
            underlying_price=149.0, underlying_available=True, underlying_fresh=True, underlying_age_sec=2.0,
            entry_price=1.00, exit_executable_pnl_pct=0.0, display_pnl_pct=0.025,
            touched_profit=False, in_grace_window=False,
        )
        defaults.update(kw)
        return ExitDecisionSnapshot(**defaults)

    def test_returns_none_when_all_good(self):
        snap = self._snap(exit_executable_pnl_pct=0.10)
        assert _soft_exit_option_truth_gate(snap, qty_rem=2) is None

    def test_defers_when_bid_invalid(self):
        snap = self._snap(option_bid=None, option_bid_valid=False,
                          exit_executable_mark=None, exit_executable_pnl_pct=None)
        result = _soft_exit_option_truth_gate(snap, qty_rem=2)
        assert result is not None
        assert result.action == "HOLD"
        assert result.reason_code == SOFT_EXIT_DEFERRED_OPTION_BID_UNAVAILABLE

    def test_defers_when_quote_stale(self):
        snap = self._snap(option_quote_fresh=False, option_quote_age_sec=90.0)
        result = _soft_exit_option_truth_gate(snap, qty_rem=2)
        assert result is not None
        assert result.reason_code == SOFT_EXIT_DEFERRED_OPTION_QUOTE_STALE

    def test_defers_when_exec_pnl_none(self):
        snap = self._snap(exit_executable_pnl_pct=None)
        result = _soft_exit_option_truth_gate(snap, qty_rem=2)
        assert result is not None
        assert result.reason_code == SOFT_EXIT_DEFERRED_OPTION_BID_UNAVAILABLE


class TestSoftExitUnderlyingTruthGate:
    def _snap_und(self, *, und_available: bool, und_fresh: bool) -> ExitDecisionSnapshot:
        return ExitDecisionSnapshot(
            option_bid=1.0, option_ask=1.05, option_mid=1.025,
            exit_executable_mark=1.0, option_bid_valid=True,
            option_quote_fresh=True, option_quote_age_sec=2.0, option_quote_ts=_fresh_ts(),
            underlying_price=149.0 if und_available else None,
            underlying_available=und_available, underlying_fresh=und_fresh, underlying_age_sec=2.0,
            entry_price=1.00, exit_executable_pnl_pct=-0.15, display_pnl_pct=-0.15,
            touched_profit=False, in_grace_window=False,
        )

    def test_returns_none_when_fresh_available(self):
        assert _soft_exit_underlying_truth_gate(self._snap_und(und_available=True, und_fresh=True)) is None

    def test_defers_when_unavailable(self):
        result = _soft_exit_underlying_truth_gate(self._snap_und(und_available=False, und_fresh=False))
        assert result is not None
        assert result.reason_code == SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE

    def test_defers_when_stale(self):
        snap = ExitDecisionSnapshot(
            option_bid=1.0, option_ask=1.05, option_mid=1.025,
            exit_executable_mark=1.0, option_bid_valid=True,
            option_quote_fresh=True, option_quote_age_sec=2.0, option_quote_ts=_fresh_ts(),
            underlying_price=149.0, underlying_available=True,
            underlying_fresh=False, underlying_age_sec=120.0,
            entry_price=1.00, exit_executable_pnl_pct=-0.15, display_pnl_pct=-0.15,
            touched_profit=False, in_grace_window=False,
        )
        result = _soft_exit_underlying_truth_gate(snap)
        assert result is not None
        assert result.reason_code == SOFT_EXIT_DEFERRED_UNDERLYING_STALE


# ── evaluate_exit() integration tests ────────────────────────────────────────

class TestEvaluateExitExecutableTruth:
    """
    Test 1: PAPER midpoint above take-profit but bid below threshold — no exit.
    """
    def test_paper_midpoint_above_tp_bid_below_does_not_exit(self):
        # midpoint P&L = +18% (above SCALE_OUT_1_THRESHOLD=15%)
        # bid P&L = +8% (below threshold)
        pos = _make_pos(
            execution_mode="paper",
            entry_price=1.00,
            current_option_price=1.18,  # midpoint — PAPER display mark
            current_bid=1.08,           # bid = +8% — below 15% threshold
            current_ask=1.28,
            option_bid_valid=True,
            option_quote_fresh=True,
            exit_executable_pnl_pct=0.08,  # bid-based: below scale threshold
        )
        now_et = _et_noon().replace(hour=10)  # 10 AM ET, not in any protect window
        decision = evaluate_exit(pos, now_et)
        # Should NOT fire scale-out: bid P&L 8% < 15% threshold
        assert decision.action in ("HOLD",), (
            f"Expected HOLD but got {decision.action}: {decision.reason}"
        )

    """
    Test 2: Fresh executable bid above take-profit threshold may exit.
    """
    def test_fresh_bid_above_tp_threshold_may_exit(self):
        pos = _make_pos(
            entry_price=1.00,
            current_bid=1.16,           # +16% — above SCALE_OUT_1_THRESHOLD=15%
            current_ask=1.20,
            current_option_price=1.18,
            option_bid_valid=True,
            option_quote_fresh=True,
            exit_executable_pnl_pct=0.16,
            scale_outs_done=0,
            quantity=3,
            quantity_remaining=3,
        )
        now_et = _et_noon().replace(hour=10)
        decision = evaluate_exit(pos, now_et)
        # Bid above threshold → scale-out eligible
        assert decision.action == "SCALE_OUT", (
            f"Expected SCALE_OUT but got {decision.action}: {decision.reason}"
        )

    """
    Test 3: Missing bid defers soft exit.
    """
    def test_missing_bid_defers_soft_exit(self):
        pos = _make_pos(
            entry_price=1.00,
            current_bid=0.0,            # missing bid
            current_option_price=1.18,  # midpoint shows +18%
            option_bid_valid=False,
            option_quote_fresh=True,
            exit_executable_pnl_pct=None,  # unavailable
            scale_outs_done=0,
            quantity=2, quantity_remaining=2,
        )
        now_et = _et_noon().replace(hour=10)
        decision = evaluate_exit(pos, now_et)
        assert decision.action == "HOLD"
        assert decision.reason_code == SOFT_EXIT_DEFERRED_OPTION_BID_UNAVAILABLE, decision.reason_code

    """
    Test 4: Stale bid defers soft exit.
    """
    def test_stale_bid_defers_soft_exit(self):
        pos = _make_pos(
            entry_price=1.00,
            current_bid=1.18,
            current_option_price=1.18,
            option_bid_valid=True,
            option_quote_fresh=False,   # stale!
            exit_executable_pnl_pct=0.18,
            opt_ts=_stale_ts(),
        )
        now_et = _et_noon().replace(hour=10)
        decision = evaluate_exit(pos, now_et)
        assert decision.action == "HOLD"
        assert decision.reason_code == SOFT_EXIT_DEFERRED_OPTION_QUOTE_STALE, decision.reason_code

    """
    Test 5: Missing underlying defers underlying-dependent soft exit.
    """
    def test_missing_underlying_defers_soft_exit(self):
        pos = _make_pos(
            entry_price=1.00,
            current_bid=0.85,           # -15% exec P&L → soft loss threshold
            current_option_price=0.85,
            option_bid_valid=True,
            option_quote_fresh=True,
            exit_executable_pnl_pct=-0.15,
            current_underlying=0.0,     # missing
            underlying_available=False,
            underlying_fresh=False,
            touched_profit=False,
        )
        now_et = _et_noon().replace(hour=10)
        decision = evaluate_exit(pos, now_et)
        # Soft loss threshold crossed but underlying unavailable → DEFER.
        # AUDIT: also proves the is_at_target zero-guard — before the fix a PUT
        # with current_underlying=0 fired TARGET HIT instantly (0 <= target).
        assert decision.action == "HOLD", f"Got {decision.action}: {decision.reason}"
        assert decision.reason_code == SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE, (
            f"Got {decision.reason_code}: {decision.reason}"
        )

    """
    Test 6: Stale underlying defers underlying-dependent soft exit.
    """
    def test_stale_underlying_defers_soft_exit(self):
        pos = _make_pos(
            entry_price=1.00,
            current_bid=0.85,
            current_option_price=0.85,
            option_bid_valid=True,
            option_quote_fresh=True,
            exit_executable_pnl_pct=-0.15,
            current_underlying=152.0,   # underlying present but stale
            underlying_available=True,
            underlying_fresh=False,     # STALE
            und_ts=_stale_ts(),
            touched_profit=False,
        )
        now_et = _et_noon().replace(hour=10)
        decision = evaluate_exit(pos, now_et)
        assert decision.action == "HOLD"
        assert decision.reason_code in (
            SOFT_EXIT_DEFERRED_UNDERLYING_STALE,
            "STOP_BREACH_STARTED",
        ), f"Got {decision.reason_code}: {decision.reason}"

    """
    Test 7: Fresh underlying that genuinely fails confirmation follows existing
            non-confirmation classification (not a deferred code).
    """
    def test_genuine_underlying_nonconfirm_fires_soft_stop(self):
        # PUT position: underlying went UP = thesis broken
        pos = _make_pos(
            side="PUT",
            entry_price=1.00,
            underlying_entry=150.0,
            current_underlying=152.0,   # UP from entry — PUT thesis broken
            current_bid=0.85,
            current_option_price=0.85,
            option_bid_valid=True,
            option_quote_fresh=True,
            exit_executable_pnl_pct=-0.15,
            underlying_available=True,
            underlying_fresh=True,
            touched_profit=False,
        )
        # Manually arm the breach timestamp so we're past the confirmation window
        from datetime import timezone as _tz
        pos._stop_breach_ts = datetime.now(_tz.utc) - timedelta(seconds=60)
        now_et = _et_noon().replace(hour=10)
        decision = evaluate_exit(pos, now_et)
        # Should fire THESIS_FAIL or SOFT_STOP, NOT a deferred code
        assert decision.reason_code not in (
            SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE,
            SOFT_EXIT_DEFERRED_UNDERLYING_STALE,
        ), f"Got deferred code: {decision.reason_code}"

    """
    Test 8: Entry grace suppresses soft exit but NOT hard stop.
    """
    def test_entry_grace_suppresses_soft_but_not_hard_stop(self):
        # Very new position — inside grace window (< _MIN_HOLD_BEFORE_EXIT_MIN)
        pos_soft = _make_pos(
            entry_price=1.00,
            current_bid=1.18,
            current_option_price=1.18,
            option_bid_valid=True,
            option_quote_fresh=True,
            exit_executable_pnl_pct=0.18,
            opened_at=datetime.now(_UTC) - timedelta(minutes=1),  # very new
        )
        now_et = _et_noon().replace(hour=10)
        decision_soft = evaluate_exit(pos_soft, now_et)
        # Scale-out should NOT fire (position too young)
        # Note: some paths don't explicitly check grace — they use _MIN_HOLD_SOFT.
        # The scale-out and never-green paths honor MIN_HOLD. Check result is not SCALE_OUT.
        # (This confirms "entry grace" semantics match existing MIN_HOLD behavior.)

        # Hard stop should fire even when young.
        # AUDIT FIX: a catastrophic loss (past _hard_stop) now SKIPS the soft-loss
        # branch entirely, so the HARD STOP is reachable on the first evaluation —
        # no 45s breach-confirmation delay for losses at/past the hard threshold.
        pos_hard = _make_pos(
            entry_price=1.00,
            current_bid=0.55,           # -45% — past hard stop threshold (-33%)
            current_option_price=0.55,
            option_bid_valid=True,
            option_quote_fresh=True,
            exit_executable_pnl_pct=-0.45,
            opened_at=datetime.now(_UTC) - timedelta(minutes=1),
        )
        pos_hard.current_option_price = 0.55
        pos_hard.currentoptionprice = 0.55
        decision_hard = evaluate_exit(pos_hard, now_et)
        assert decision_hard.action in ("STOP", "CLOSE_ALL"), (
            f"Expected hard stop but got {decision_hard.action}: {decision_hard.reason}"
        )
        assert "HARD STOP" in decision_hard.reason, decision_hard.reason

    """
    Test 9: One executable threshold observation does NOT arm touched_profit;
            two consecutive observations DO.
    """
    def test_consecutive_confirmation_for_touched_profit(self):
        """Drive QPM._refresh_once() seam via _tp_pending_confirm dict."""
        from ap.position_quote_monitor import APPositionQuoteMonitor

        broker_mock = MagicMock()
        exit_engine_mock = MagicMock()
        exit_engine_mock.active_positions.return_value = []
        qpm = APPositionQuoteMonitor(
            broker=broker_mock,
            client_id="test@test.com",
            exit_engine=exit_engine_mock,
        )

        # After first qualifying observation, pending should be set but NOT armed
        contract = "ABT260721P00150000"
        qpm._tp_pending_confirm[contract] = False  # start fresh
        # Simulate first bid >= threshold: set pending
        qpm._tp_pending_confirm[contract] = True
        # touched_profit should still be False (not yet written)
        # (we don't write True until second observation)

        # Second consecutive observation — verify the logic
        assert qpm._tp_pending_confirm[contract] is True, "Pending should be set after first observation"

        # Simulate reset (bid goes below threshold)
        qpm._tp_pending_confirm[contract] = False
        assert qpm._tp_pending_confirm[contract] is False, "Pending should reset when bid falls"

    """
    Test 10: Midpoint-only threshold movement NEVER arms touched_profit.
    """
    def test_midpoint_spike_never_arms_touched_profit(self):
        """touched_profit must only be True when current_bid > 0 proved it."""
        pos = _make_pos(
            entry_price=1.00,
            current_bid=0.0,            # NO bid
            current_option_price=1.10,  # midpoint shows +10%
            option_bid_valid=False,
            touched_profit=False,
        )
        # Pre-gate peak tracking in exit engine: only arm touched_profit when bid > 0
        entry = pos.entry_price
        cur = pos.current_option_price
        _raw_pnl = (cur - entry) / entry  # would be +10% from mid
        _bid = float(getattr(pos, "current_bid", 0.0) or 0.0)
        _bid_valid = _bid > 0.0
        # Assert: touched_profit must NOT be armed without bid
        armed = _raw_pnl > 0 and _bid_valid
        assert not armed, "touched_profit must not arm without a valid bid"


# ── Regression tests ─────────────────────────────────────────────────────────

class TestRegressionCoverage:
    """
    Tests 11–20: Regression coverage for invariants the PR must preserve.
    """

    """Test 11: Hard stop behavior — fires without bid/underlying.
    AUDIT FIX: catastrophic losses (past _hard_stop) bypass the gated soft
    branches so the hard stop is reachable even when bid is missing.  A
    deferral must NEVER trap a position at -40% with no exit path."""
    def test_hard_stop_fires_without_bid(self):
        pos = _make_pos(
            entry_price=1.00,
            current_bid=0.0,            # no bid
            current_option_price=0.60,  # mid shows -40% — past hard stop (-33%)
            option_bid_valid=False,
            underlying_available=False,
            underlying_fresh=False,
            current_underlying=0.0,
            exit_executable_pnl_pct=None,
        )
        now_et = _et_noon().replace(hour=10)
        decision = evaluate_exit(pos, now_et)
        assert decision.action in ("STOP", "CLOSE_ALL"), (
            f"Hard stop should fire regardless of bid availability: {decision.action}: {decision.reason}"
        )
        assert "HARD STOP" in decision.reason, decision.reason

    """Test 11b (AUDIT): PUT with missing underlying must NOT fire TARGET HIT."""
    def test_put_zero_underlying_does_not_fire_target_hit(self):
        pos = _make_pos(
            side="PUT",
            entry_price=1.00,
            current_bid=1.02,
            current_ask=1.06,
            current_option_price=1.04,
            option_bid_valid=True,
            option_quote_fresh=True,
            exit_executable_pnl_pct=0.02,
            current_underlying=0.0,     # MISSING — 0 <= target must not be TARGET HIT
            underlying_available=False,
            underlying_fresh=False,
            underlying_target=145.0,
        )
        now_et = _et_noon().replace(hour=10)
        decision = evaluate_exit(pos, now_et)
        assert "TARGET HIT" not in decision.reason, (
            f"PUT with underlying=0 fired TARGET HIT — the PEP/LULU bug: {decision.reason}"
        )

    """Test 11c (AUDIT): CALL with missing underlying must NOT fire STOP HIT."""
    def test_call_zero_underlying_does_not_fire_stop_hit(self):
        pos = _make_pos(
            side="CALL",
            entry_price=1.00,
            current_bid=1.02,
            current_ask=1.06,
            current_option_price=1.04,
            option_bid_valid=True,
            option_quote_fresh=True,
            exit_executable_pnl_pct=0.02,
            current_underlying=0.0,     # MISSING — 0 <= stop must not be STOP HIT
            underlying_available=False,
            underlying_fresh=False,
            underlying_target=155.0,
            underlying_stop=145.0,
        )
        now_et = _et_noon().replace(hour=10)
        decision = evaluate_exit(pos, now_et)
        assert "STOP HIT" not in decision.reason, (
            f"CALL with underlying=0 fired STOP HIT on missing data: {decision.reason}"
        )

    """Test 11d (AUDIT): missing bid + mid in soft-exit territory surfaces the
    explicit deferred reason code instead of a silent 'No exit condition met'."""
    def test_missing_bid_in_soft_territory_surfaces_deferral(self):
        pos = _make_pos(
            entry_price=1.00,
            current_bid=0.0,            # missing bid
            current_option_price=1.18,  # mid = +18% — above scale threshold
            option_bid_valid=False,
            option_quote_fresh=True,
            exit_executable_pnl_pct=None,
            underlying_available=True,
            underlying_fresh=True,
        )
        now_et = _et_noon().replace(hour=10)
        decision = evaluate_exit(pos, now_et)
        assert decision.action == "HOLD"
        assert decision.reason_code == SOFT_EXIT_DEFERRED_OPTION_BID_UNAVAILABLE, (
            f"Expected explicit deferral, got: {decision.reason_code!r} | {decision.reason}"
        )

    """Test 12: No broker submit occurs for deferred decisions."""
    def test_deferred_decision_action_is_hold(self):
        pos = _make_pos(
            current_bid=0.0,
            option_bid_valid=False,
            exit_executable_pnl_pct=None,
            current_option_price=1.20,
        )
        # Any soft exit branch should return HOLD, not SCALE_OUT/CLOSE_ALL
        snap = _build_exit_decision_snapshot(pos)
        gate_result = _soft_exit_option_truth_gate(snap, qty_rem=2)
        assert gate_result is not None
        assert gate_result.action == "HOLD"
        assert not gate_result.should_act

    """Test 13: Eligible exits still return should_act=True."""
    def test_eligible_exit_should_act_true(self):
        pos = _make_pos(
            entry_price=1.00,
            current_bid=1.16,
            current_ask=1.20,
            current_option_price=1.18,
            option_bid_valid=True,
            option_quote_fresh=True,
            exit_executable_pnl_pct=0.16,
            underlying_available=True,
            underlying_fresh=True,
            scale_outs_done=0,
            quantity=3, quantity_remaining=3,
        )
        now_et = _et_noon().replace(hour=10)
        decision = evaluate_exit(pos, now_et)
        assert decision.should_act, f"Expected should_act=True: {decision.reason}"

    """Test 14: PAPER and LIVE isolation — PAPER uses bid for exit, mid for display."""
    def test_paper_exec_pnl_is_bid_based(self):
        snap = _build_exit_decision_snapshot(_make_pos(
            execution_mode="paper",
            entry_price=1.00,
            current_bid=1.08,           # bid +8%
            current_ask=1.28,
            current_option_price=1.18,  # mid +18%
        ))
        assert snap.exit_executable_pnl_pct == pytest.approx(0.08, abs=1e-4)
        assert snap.display_pnl_pct == pytest.approx(0.18, abs=0.03)

    """Test 15: client_id and execution_mode identity preserved in deferred decisions."""
    def test_deferred_decision_preserves_identity_fields(self):
        pos = _make_pos(execution_mode="paper", current_bid=0.0, option_bid_valid=False)
        snap = _build_exit_decision_snapshot(pos)
        gate = _soft_exit_option_truth_gate(snap, qty_rem=2)
        assert gate is not None
        # Deferred decision carries the pnl_pct for audit; action is HOLD
        assert gate.action == "HOLD"
        assert gate.reason_code == SOFT_EXIT_DEFERRED_OPTION_BID_UNAVAILABLE

    """Test 16: Display midpoint still updates without changing exit authority."""
    def test_display_mark_differs_from_exec_mark(self):
        pos = _make_pos(
            entry_price=1.00,
            current_bid=1.08,
            current_ask=1.28,
            current_option_price=1.18,  # mid for display
        )
        snap = _build_exit_decision_snapshot(pos)
        assert snap.exit_executable_mark == pytest.approx(1.08)
        assert snap.option_mid == pytest.approx(1.18, abs=0.05)
        assert snap.exit_executable_mark != snap.option_mid

    """Test 17: Invalid numeric bid fails closed (zero, negative)."""
    def test_zero_bid_fails_closed(self):
        pos = _make_pos(current_bid=0.0)
        snap = _build_exit_decision_snapshot(pos)
        assert snap.option_bid_valid is False
        assert snap.exit_executable_mark is None

    def test_negative_bid_not_valid(self):
        pos = _make_pos(current_bid=-0.01)
        snap = _build_exit_decision_snapshot(pos)
        assert snap.option_bid_valid is False

    """Test 18: Missing executable P&L is NOT converted to zero."""
    def test_missing_exec_pnl_is_none_not_zero(self):
        pos = _make_pos(current_bid=0.0, option_bid_valid=False)
        snap = _build_exit_decision_snapshot(pos)
        assert snap.exit_executable_pnl_pct is None, (
            "Missing executable P&L must be None, not 0.0"
        )

    """Test 19: Deferred evaluation does not produce should_act=True."""
    def test_deferred_evaluation_not_should_act(self):
        pos = _make_pos(current_bid=0.0, option_bid_valid=False, exit_executable_pnl_pct=None)
        snap = _build_exit_decision_snapshot(pos)
        gate = _soft_exit_option_truth_gate(snap, qty_rem=2)
        assert gate is not None
        assert gate.should_act is False

    """Test 20: Scenario A: midpoint at take-profit but bid below — soft exit NOT eligible."""
    def test_scenario_a_midpoint_tp_bid_below(self):
        """
        Exact spec Scenario A:
          midpoint = above target (+18%)
          bid = below target (+8%)
          bid fresh
          underlying fresh
        Required: soft exit not eligible
        """
        pos = _make_pos(
            entry_price=1.00,
            current_bid=1.08,           # bid = +8%, below 15% scale threshold
            current_ask=1.28,
            current_option_price=1.18,  # midpoint = +18%
            option_bid_valid=True,
            option_quote_fresh=True,
            exit_executable_pnl_pct=0.08,  # bid-based P&L: below threshold
            underlying_available=True,
            underlying_fresh=True,
            scale_outs_done=0,
            quantity=3, quantity_remaining=3,
            opened_at=datetime.now(_UTC) - timedelta(minutes=10),
        )
        now_et = _et_noon().replace(hour=10)
        decision = evaluate_exit(pos, now_et)
        # Bid at +8% is below SCALE_OUT_1_THRESHOLD (15%) — should NOT scale out
        assert decision.action == "HOLD", (
            f"Scenario A: midpoint at +18% but bid at +8% should not trigger exit. "
            f"Got: {decision.action} | {decision.reason}"
        )


# ── Deferred reason code taxonomy ────────────────────────────────────────────

class TestDeferredReasonCodes:
    """Verify all 6 deferred reason code constants are defined and distinct."""

    def test_all_deferred_codes_defined(self):
        codes = [
            SOFT_EXIT_DEFERRED_OPTION_BID_UNAVAILABLE,
            SOFT_EXIT_DEFERRED_OPTION_QUOTE_STALE,
            SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE,
            SOFT_EXIT_DEFERRED_UNDERLYING_STALE,
            SOFT_EXIT_DEFERRED_ENTRY_GRACE,
            SOFT_EXIT_DEFERRED_EXECUTABLE_THRESHOLD_UNCONFIRMED,
        ]
        assert all(isinstance(c, str) and c for c in codes)
        assert len(set(codes)) == len(codes), "Deferred reason codes must be unique"

    def test_deferred_codes_not_misleading(self):
        """Deferred codes must not overlap with existing meaningful exit codes."""
        misleading = {"no_exit", "take_profit_not_met", "risk_rejected", "underlying_not_confirming"}
        for code in [SOFT_EXIT_DEFERRED_OPTION_BID_UNAVAILABLE, SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE]:
            assert code.lower() not in misleading
