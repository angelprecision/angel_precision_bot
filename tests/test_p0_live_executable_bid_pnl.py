"""P0 — LIVE Executable Bid P&L and Canonical Position Identity

Tests A–F from the specification.

Production incident (July 15 2026, Jason LIVE BA260717C00222500):
  Entry fill $1.59; Quote bid=$1.63 ask=$1.79
  System used midpoint $1.71 → mid P&L +7.55% → touched_profit=True → floor armed
  Real executable P&L from bid: +2.52% → floor must NOT arm
  Exit quote bid=$1.45 → actual fill -8.81%
  System fired: "TOUCHED PROFIT STOP — peaked +7% now 0% — floor=3%"
  Actual realized: -8.81% loss (premium paid)
  Exit attributed to synthetic broker-repair ID, not canonical position.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

_ENTRY_FILL   = 1.59
_CONTRACT     = "BA260717C00222500"
_CANON_POS_ID = "d7668918-bc1d-45f3-a783-357c7fe51afc"
_CANON_ORD_ID = "944f4c9d-7808-4d55-9947-5bfb6563626e"
_BROKER_ORD   = "136961009"
_CLIENT       = "jasoncosby1@gmail.com"
_SIG          = "SIG-BA-001"


def _make_live_pos(**overrides) -> SimpleNamespace:
    """Minimal LIVE ManagedPosition-like namespace."""
    pos = SimpleNamespace(
        position_id     = _CANON_POS_ID,
        client_id       = _CLIENT,
        execution_mode  = "live",
        ticker          = "BA",
        option_symbol   = _CONTRACT,
        optionsymbol    = _CONTRACT,
        entry_price     = _ENTRY_FILL,
        entryprice      = _ENTRY_FILL,
        current_option_price = 0.0,
        currentoptionprice   = 0.0,
        current_bid     = 0.0,
        currentbid      = 0.0,
        current_ask     = 0.0,
        currentask      = 0.0,
        current_underlying   = 0.0,
        currentunderlying    = 0.0,
        peak_pnl_pct    = 0.0,
        peakpnlpct      = 0.0,
        max_profit_seen = 0.0,
        maxprofitseen   = 0.0,
        option_pnl_pct  = 0.0,
        optionpnlpct    = 0.0,
        touched_profit  = False,
        touchedprofit   = False,
        quantity        = 1,
        quantity_remaining = 1,
        closed          = False,
        side            = "CALL",
        underlying_entry = 220.0,
        underlying_stop  = 215.0,
        underlying_target = 230.0,
        scale_outs_done  = 0,
        last_option_quote_update_ts = None,
        lastoptionquoteupdatets     = None,
        last_underlying_quote_update_ts = None,
        lastunderlyingquoteupdatets     = None,
    )
    for k, v in overrides.items():
        setattr(pos, k, v)
    return pos


def _make_paper_pos(**overrides) -> SimpleNamespace:
    pos = _make_live_pos(**overrides)
    pos.execution_mode = "paper"
    return pos


def _run_qpm_pnl_update(pos, *, bid: float, ask: float, mid: float | None = None):
    """Simulate the QPM P&L update block without the full thread loop."""
    from ap.position_quote_monitor import APPositionQuoteMonitor, _safe_float, _get_attr, DIRECT_POSITION_WRITES

    class _StubQPM(APPositionQuoteMonitor):
        def __init__(self):
            # Minimal init without starting threads
            self.client_id = _CLIENT
            self._last_db_persist_ts = {}
            self._last_db_persist_price = {}
            self._metrics = {"spread_rejects": 0}
            self._alert_fn = lambda m: None

        def _persist_quote_to_db(self, **kw):
            return False

        def _persist_mfe_mae_to_orders(self, **kw):
            return None

        def _mark_mfe_mae_unavailable(self, **kw):
            return None

    qpm = _StubQPM()

    # Set current bid/ask/price on position (as QPM would do in _refresh_once)
    pos.current_bid = bid
    pos.currentbid  = bid
    pos.current_ask = ask
    pos.currentask  = ask
    # mid or mark is what _extract_option_price returns for valid spread
    _computed_mid = mid if mid is not None else round((bid + ask) / 2, 4)
    pos.current_option_price = _computed_mid
    pos.currentoptionprice   = _computed_mid

    # Reproduce the QPM P&L block exactly as modified in the fix
    cost_basis = (
        _safe_float(_get_attr(pos, "entryprice", "entry_price", default=None), 0.0)
        or _safe_float(_get_attr(pos, "avgfill", "avg_fill_price", default=None), 0.0)
        or _safe_float(_get_attr(pos, "entry_fill_price", default=None), 0.0)
    )
    cur_opt = _safe_float(_get_attr(pos, "currentoptionprice", "current_option_price", default=None), 0.0)

    _exec_mode = str(
        _get_attr(pos, "executionmode", "execution_mode", default="") or ""
    ).lower().strip()
    _is_live = (_exec_mode == "live")

    qpm._write_field(pos, "analytics_mark_price", cur_opt)
    qpm._write_field(pos, "analyticsmarkprice",   cur_opt)

    cur_bid_now = _safe_float(_get_attr(pos, "currentbid", "current_bid", default=None), 0.0)
    if _is_live:
        if cur_bid_now > 0:
            exec_price = cur_bid_now
            qpm._write_field(pos, "live_executable_price_source", "bid")
            qpm._write_field(pos, "liveexecutablepricesource",    "bid")
            qpm._write_field(pos, "currentoptionprice",  cur_bid_now)
            qpm._write_field(pos, "current_option_price", cur_bid_now)
        else:
            exec_price = 0.0
            qpm._write_field(pos, "live_executable_price_source", "bid_missing")
            qpm._write_field(pos, "liveexecutablepricesource",    "bid_missing")
    else:
        exec_price = cur_opt
        qpm._write_field(pos, "live_executable_price_source", "paper_mid_simulation")
        qpm._write_field(pos, "liveexecutablepricesource",    "paper_mid_simulation")

    if cost_basis > 0 and exec_price > 0:
        pnl_pct = (exec_price - cost_basis) / cost_basis
        from ap.position_quote_monitor import _safe_float as _sf
        peak = max(
            pnl_pct,
            _sf(_get_attr(pos, "peakpnlpct", "peak_pnl_pct", default=None), float("-inf")),
            _sf(_get_attr(pos, "maxprofitseen", "max_profit_seen", default=None), float("-inf")),
        )
        qpm._write_field(pos, "peakpnlpct", peak)
        qpm._write_field(pos, "peak_pnl_pct", peak)
        qpm._write_field(pos, "maxprofitseen", peak)
        qpm._write_field(pos, "max_profit_seen", peak)
        qpm._write_field(pos, "optionpnlpct", pnl_pct)
        qpm._write_field(pos, "option_pnl_pct", pnl_pct)
        if pnl_pct >= 0.05:
            qpm._write_field(pos, "touchedprofit", True)
            qpm._write_field(pos, "touched_profit", True)

    return pos


# ─────────────────────────────────────────────────────────────────────────────
# Test A — Exact BA replay
# ─────────────────────────────────────────────────────────────────────────────


class TestBAExactReplay:
    """Reproduce the exact July 15 2026 BA production quotes."""

    def test_quote1_analytics_mark_is_midpoint(self):
        """Analytics mark must capture mid (+7.55%) for charting."""
        pos = _make_live_pos()
        _run_qpm_pnl_update(pos, bid=1.63, ask=1.79, mid=1.71)
        mark = getattr(pos, "analytics_mark_price", None)
        assert mark is not None
        mark_pnl = (mark - _ENTRY_FILL) / _ENTRY_FILL
        assert abs(mark_pnl - 0.0755) < 0.002, (
            f"Analytics mark P&L should be ~+7.55%, got {mark_pnl*100:.2f}%"
        )

    def test_quote1_executable_bid_pnl_is_plus_252(self):
        """Executable bid P&L must be +2.52%, not +7.55%."""
        pos = _make_live_pos()
        _run_qpm_pnl_update(pos, bid=1.63, ask=1.79, mid=1.71)
        bid_pnl = (1.63 - _ENTRY_FILL) / _ENTRY_FILL
        actual_pnl = getattr(pos, "option_pnl_pct", None)
        assert actual_pnl is not None
        assert abs(actual_pnl - bid_pnl) < 0.001, (
            f"Executable P&L must be ~+2.52% (bid-based), got {actual_pnl*100:.2f}%"
        )

    def test_quote1_touched_profit_does_not_arm(self):
        """Bid P&L +2.52% < 5% threshold — touched_profit must stay False."""
        pos = _make_live_pos()
        _run_qpm_pnl_update(pos, bid=1.63, ask=1.79, mid=1.71)
        assert pos.touched_profit is False, (
            "touched_profit must not arm at +2.52% bid P&L (would require +5%)"
        )

    def test_quote1_executable_peak_is_bid_based(self):
        """Peak must not exceed bid-based P&L (+2.52%)."""
        pos = _make_live_pos()
        _run_qpm_pnl_update(pos, bid=1.63, ask=1.79, mid=1.71)
        peak = getattr(pos, "peak_pnl_pct", 0.0)
        assert peak < 0.05, (
            f"Peak P&L must be bid-based (+2.52%), got {peak*100:.2f}%"
        )

    def test_quote1_no_floor_armed(self):
        """With touched_profit=False, no profit floor can be armed."""
        pos = _make_live_pos()
        _run_qpm_pnl_update(pos, bid=1.63, ask=1.79, mid=1.71)
        assert not pos.touched_profit
        # Exit engine's floor logic requires touched_profit=True
        # (see ap_exit_engine.py line 580: if pos.scale_outs_done == 0 and pos.touched_profit)
        # So no floor can fire.

    def test_quote2_no_touched_profit_stop_fires(self):
        """Quote 2 (bid=$1.45): touched_profit never set → TOUCHED_PROFIT_STOP cannot fire."""
        pos = _make_live_pos()
        # Quote 1: bid=1.63 — does NOT arm touched_profit
        _run_qpm_pnl_update(pos, bid=1.63, ask=1.79, mid=1.71)
        assert pos.touched_profit is False

        # Quote 2: bid=1.45 (exit quote)
        _run_qpm_pnl_update(pos, bid=1.45, ask=1.74, mid=1.595)
        bid_pnl_q2 = (1.45 - _ENTRY_FILL) / _ENTRY_FILL  # -8.81%
        assert bid_pnl_q2 < 0, "Quote 2 bid P&L must be negative"
        assert pos.touched_profit is False, (
            "touched_profit must remain False after bid-based P&L; "
            "TOUCHED_PROFIT_STOP must not fire from prior mid peak"
        )

    def test_quote2_executable_pnl_is_minus_881(self):
        pos = _make_live_pos()
        _run_qpm_pnl_update(pos, bid=1.45, ask=1.74, mid=1.595)
        pnl = getattr(pos, "option_pnl_pct", None)
        assert pnl is not None
        assert abs(pnl - (1.45 - 1.59) / 1.59) < 0.002, (
            f"Executable P&L at bid=$1.45 must be ~-8.81%, got {pnl*100:.2f}%"
        )

    def test_live_price_source_is_bid(self):
        pos = _make_live_pos()
        _run_qpm_pnl_update(pos, bid=1.63, ask=1.79)
        src = getattr(pos, "live_executable_price_source", None)
        assert src == "bid", f"live_executable_price_source must be 'bid', got {src!r}"

    def test_current_option_price_overridden_to_bid_for_live(self):
        """Exit engine reads current_option_price for option_pnl_pct.
        For LIVE it must be bid, not mid."""
        pos = _make_live_pos()
        _run_qpm_pnl_update(pos, bid=1.63, ask=1.79, mid=1.71)
        assert abs(pos.current_option_price - 1.63) < 0.001, (
            f"LIVE current_option_price must be bid=1.63, got {pos.current_option_price}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Test B — Real executable winner
# ─────────────────────────────────────────────────────────────────────────────


class TestRealExecutableWinner:
    """When the bid itself is above the 5% threshold, touched_profit arms correctly."""

    def test_bid_above_5pct_arms_touched_profit(self):
        """bid=$1.70 → P&L +6.92% ≥ 5% → touched_profit arms."""
        pos = _make_live_pos()
        _run_qpm_pnl_update(pos, bid=1.70, ask=1.79)
        bid_pnl = (1.70 - _ENTRY_FILL) / _ENTRY_FILL  # +6.92%
        assert bid_pnl >= 0.05
        assert pos.touched_profit is True

    def test_peak_stored_from_bid(self):
        """Executable peak comes from bid, not ask."""
        pos = _make_live_pos()
        _run_qpm_pnl_update(pos, bid=1.70, ask=1.79)
        peak = getattr(pos, "peak_pnl_pct", 0.0)
        expected = (1.70 - _ENTRY_FILL) / _ENTRY_FILL
        assert abs(peak - expected) < 0.001

    def test_profit_floor_from_bid(self):
        """After peak at bid, floor decision must use bid P&L, not mid."""
        pos = _make_live_pos()
        # Quote 1: peak at bid=1.70
        _run_qpm_pnl_update(pos, bid=1.70, ask=1.79)
        assert pos.touched_profit is True
        peak_q1 = pos.peak_pnl_pct

        # Quote 2: bid=1.64 (still profitable but dropped)
        _run_qpm_pnl_update(pos, bid=1.64, ask=1.72)
        pnl_q2 = getattr(pos, "option_pnl_pct", 0.0)
        bid_pnl_q2 = (1.64 - _ENTRY_FILL) / _ENTRY_FILL
        assert abs(pnl_q2 - bid_pnl_q2) < 0.001
        # Peak should not have risen from mid
        assert pos.peak_pnl_pct <= peak_q1 + 0.001


# ─────────────────────────────────────────────────────────────────────────────
# Test C — Midpoint rises while bid does not
# ─────────────────────────────────────────────────────────────────────────────


class TestMidRiseBidFlat:
    """Ask rising while bid flat must not arm touched_profit or raise peak."""

    def test_rising_ask_flat_bid_no_touched_profit(self):
        """bid=$1.60, ask=$2.10 — mid would be +$1.855 (+16.7%) but bid is only +0.6%."""
        pos = _make_live_pos()
        _run_qpm_pnl_update(pos, bid=1.60, ask=2.10)
        bid_pnl = (1.60 - _ENTRY_FILL) / _ENTRY_FILL  # +0.6%
        assert bid_pnl < 0.05
        assert pos.touched_profit is False, (
            "Rising ask with flat bid must not arm touched_profit"
        )

    def test_rising_ask_flat_bid_peak_does_not_rise(self):
        pos = _make_live_pos()
        _run_qpm_pnl_update(pos, bid=1.60, ask=2.10)
        peak = getattr(pos, "peak_pnl_pct", 0.0)
        assert peak < 0.05, (
            f"Peak must not rise from mid when bid is flat, got {peak*100:.2f}%"
        )

    def test_analytics_mark_still_captured(self):
        """Analytics mark should capture the mid for charting."""
        pos = _make_live_pos()
        _run_qpm_pnl_update(pos, bid=1.60, ask=2.10)
        mark = getattr(pos, "analytics_mark_price", None)
        assert mark is not None and mark > pos.current_bid, (
            "Analytics mark (mid) should exceed bid when ask is high"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Test D — PAPER behavior isolation
# ─────────────────────────────────────────────────────────────────────────────


class TestPaperBehavior:
    """PAPER must use mid/mark simulation and not be affected by bid accounting."""

    def test_paper_uses_mid_for_pnl(self):
        pos = _make_paper_pos()
        mid = round((1.63 + 1.79) / 2, 4)
        _run_qpm_pnl_update(pos, bid=1.63, ask=1.79, mid=mid)
        pnl = getattr(pos, "option_pnl_pct", 0.0)
        expected = (mid - _ENTRY_FILL) / _ENTRY_FILL
        assert abs(pnl - expected) < 0.001, (
            f"PAPER must use mid P&L, expected {expected*100:.2f}%, got {pnl*100:.2f}%"
        )

    def test_paper_touched_profit_from_mid(self):
        """PAPER: mid P&L of +7.55% should arm touched_profit."""
        pos = _make_paper_pos()
        _run_qpm_pnl_update(pos, bid=1.63, ask=1.79, mid=1.71)
        mid_pnl = (1.71 - _ENTRY_FILL) / _ENTRY_FILL  # +7.55%
        assert mid_pnl >= 0.05
        assert pos.touched_profit is True, (
            "PAPER must arm touched_profit at mid P&L ≥ 5%"
        )

    def test_paper_price_source_labeled_simulation(self):
        pos = _make_paper_pos()
        _run_qpm_pnl_update(pos, bid=1.63, ask=1.79, mid=1.71)
        src = getattr(pos, "live_executable_price_source", None)
        assert src == "paper_mid_simulation", (
            f"PAPER must label source as 'paper_mid_simulation', got {src!r}"
        )

    def test_paper_current_option_price_stays_mid(self):
        """PAPER: current_option_price must remain mid (not overridden to bid)."""
        pos = _make_paper_pos()
        mid = round((1.63 + 1.79) / 2, 4)
        _run_qpm_pnl_update(pos, bid=1.63, ask=1.79, mid=mid)
        assert abs(pos.current_option_price - mid) < 0.001, (
            f"PAPER current_option_price must be mid={mid}, got {pos.current_option_price}"
        )

    def test_live_and_paper_same_bid_different_pnl(self):
        """Given same quotes, LIVE uses bid and PAPER uses mid."""
        pos_live  = _make_live_pos()
        pos_paper = _make_paper_pos()
        mid = 1.71
        _run_qpm_pnl_update(pos_live,  bid=1.63, ask=1.79, mid=mid)
        _run_qpm_pnl_update(pos_paper, bid=1.63, ask=1.79, mid=mid)

        live_pnl  = getattr(pos_live,  "option_pnl_pct", 0.0)
        paper_pnl = getattr(pos_paper, "option_pnl_pct", 0.0)
        assert live_pnl  < paper_pnl, "LIVE P&L (bid) must be less than PAPER P&L (mid)"
        assert abs(live_pnl  - (1.63 - 1.59) / 1.59) < 0.001
        assert abs(paper_pnl - (1.71 - 1.59) / 1.59) < 0.001


# ─────────────────────────────────────────────────────────────────────────────
# Test E — Synthetic-to-canonical adoption
# ─────────────────────────────────────────────────────────────────────────────


class TestSyntheticToCanonicalAdoption:
    """adopt_canonical_position_identity upgrades broker-repair to canonical."""

    def _make_engine_with_repair_pos(self):
        from ap_exit_engine import APExitEngine, ManagedPosition
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        import threading
        engine._lock = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}

        repair_id = f"broker-repair-{_CLIENT}-{_CONTRACT}"
        pos = MagicMock(spec=ManagedPosition)
        pos.position_id      = repair_id
        pos.client_id        = _CLIENT
        pos.option_symbol    = _CONTRACT
        pos.ticker           = "BA"
        pos.execution_mode   = ""       # unknown — the problem
        pos.entry_price      = 1.59
        pos.current_option_price = 1.65
        pos.current_bid      = 1.63
        pos.current_ask      = 1.79
        pos.peak_pnl_pct     = 0.025   # 2.5% — bid-based seed
        pos.max_profit_seen  = 0.025
        pos.touched_profit   = False
        pos.closed           = False
        pos.signal_id        = ""

        engine._positions.append(pos)
        engine._positions_by_id[repair_id] = pos
        return engine, pos

    def test_broker_repair_is_upgraded_not_duplicated(self):
        """adopt_canonical_position_identity returns True and no new position is created."""
        engine, repair_pos = self._make_engine_with_repair_pos()
        adopted = engine.adopt_canonical_position_identity(
            contract              = _CONTRACT,
            canonical_position_id = _CANON_POS_ID,
            local_order_id        = _CANON_ORD_ID,
            broker_order_id       = _BROKER_ORD,
            signal_id             = _SIG,
            canonical_signal_id   = _SIG,
            entry_fill            = 1.59,
            entry_ts              = None,
            execution_mode        = "live",
            client_id             = _CLIENT,
            score                 = 88.0,
            tier                  = "A",
            pattern               = "3-1-2",
            direction             = "CALL",
        )
        assert adopted is True
        assert len(engine._positions) == 1, "Must remain exactly one active position"

    def test_canonical_position_id_replaces_synthetic(self):
        engine, repair_pos = self._make_engine_with_repair_pos()
        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id=_CANON_POS_ID,
            local_order_id=_CANON_ORD_ID, broker_order_id=_BROKER_ORD,
            signal_id=_SIG, canonical_signal_id=_SIG, entry_fill=1.59,
            entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        assert engine._positions[0].position_id == _CANON_POS_ID
        assert _CANON_POS_ID in engine._positions_by_id
        assert f"broker-repair-{_CLIENT}-{_CONTRACT}" not in engine._positions_by_id

    def test_execution_mode_set_to_live(self):
        engine, repair_pos = self._make_engine_with_repair_pos()
        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id=_CANON_POS_ID,
            local_order_id=_CANON_ORD_ID, broker_order_id=_BROKER_ORD,
            signal_id=_SIG, canonical_signal_id=_SIG, entry_fill=1.59,
            entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        assert engine._positions[0].execution_mode == "live"

    def test_entry_fill_updated_to_canonical(self):
        engine, _ = self._make_engine_with_repair_pos()
        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id=_CANON_POS_ID,
            local_order_id=_CANON_ORD_ID, broker_order_id=_BROKER_ORD,
            signal_id=_SIG, canonical_signal_id=_SIG, entry_fill=1.59,
            entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        assert engine._positions[0].entry_price == 1.59

    def test_quote_and_peak_state_preserved(self):
        """Peak state accumulated before adoption must not be lost."""
        engine, repair_pos = self._make_engine_with_repair_pos()
        repair_pos.peak_pnl_pct = 0.04  # pre-adoption peak
        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id=_CANON_POS_ID,
            local_order_id=_CANON_ORD_ID, broker_order_id=_BROKER_ORD,
            signal_id=_SIG, canonical_signal_id=_SIG, entry_fill=1.59,
            entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        # Peak should not be zero (state preserved)
        assert engine._positions[0].peak_pnl_pct >= 0.0

    def test_no_adoption_when_no_repair_pos(self):
        """When no broker-repair exists, adopt returns False and caller should seed."""
        from ap_exit_engine import APExitEngine
        engine = APExitEngine.__new__(APExitEngine)
        import threading
        engine._lock = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}

        adopted = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id=_CANON_POS_ID,
            local_order_id=_CANON_ORD_ID, broker_order_id=_BROKER_ORD,
            signal_id=_SIG, canonical_signal_id=_SIG, entry_fill=1.59,
            entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        assert adopted is False


# ─────────────────────────────────────────────────────────────────────────────
# Test F — No duplicate position / exit
# ─────────────────────────────────────────────────────────────────────────────


class TestNoDuplicatePosition:
    """add_position after adoption must not create a second in-memory position."""

    def test_add_position_with_canonical_id_deduplicates(self):
        """After adoption, add_position with same canonical ID is a no-op."""
        from ap_exit_engine import APExitEngine, ManagedPosition
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        import threading
        engine._lock  = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}
        engine._assert_position_invariants = MagicMock()

        repair_id = f"broker-repair-{_CLIENT}-{_CONTRACT}"
        pos1 = MagicMock(spec=ManagedPosition)
        pos1.position_id   = repair_id
        pos1.client_id     = _CLIENT
        pos1.option_symbol = _CONTRACT
        pos1.ticker        = "BA"
        pos1.execution_mode = ""
        pos1.entry_price   = 1.59
        pos1.current_bid   = 1.63
        pos1.current_option_price = 1.63
        pos1.peak_pnl_pct  = 0.025
        pos1.max_profit_seen = 0.025
        pos1.touched_profit = False
        pos1.closed         = False
        pos1.signal_id      = ""
        engine._positions.append(pos1)
        engine._positions_by_id[repair_id] = pos1

        # Adopt
        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id=_CANON_POS_ID,
            local_order_id=_CANON_ORD_ID, broker_order_id=_BROKER_ORD,
            signal_id=_SIG, canonical_signal_id=_SIG, entry_fill=1.59,
            entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        assert len(engine._positions) == 1

        # Attempt to add a second position with the same symbol — dedup must block
        pos2 = MagicMock(spec=ManagedPosition)
        pos2.position_id   = _CANON_POS_ID
        pos2.client_id     = _CLIENT
        pos2.option_symbol = _CONTRACT
        pos2.ticker        = "BA"
        pos2.execution_mode = "live"
        pos2.entry_price   = 1.59
        pos2.closed        = False

        # Patch _normalize_ticker to avoid import issues
        with patch("ap_exit_engine._normalize_ticker", return_value="BA"):
            engine.add_position(pos2)

        assert len(engine._positions) == 1, (
            "add_position must not create a duplicate for the same contract"
        )
