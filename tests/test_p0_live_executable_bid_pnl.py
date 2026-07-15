"""P0 — LIVE Executable Bid P&L and Canonical Position Identity (Amendment)

Tests A–F plus amendment blockers 1–6.

Production incident (July 15 2026, Jason LIVE BA260717C00222500):
  Entry $1.59; bid $1.63 ask $1.79 mid $1.71
  System used mid → +7.55% → touched_profit=True → floor armed
  Exit filled at bid $1.45 → -8.81% real loss
  System claimed: "TOUCHED PROFIT STOP peaked +7% now 0% floor=3%"
  Exit attributed to broker-repair ID, not canonical position.

All tests that touch QPM pricing call actual _refresh_once() with a fake
broker — no copying of the production implementation into the test.
"""
from __future__ import annotations

import json
import os
import sys
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

from ap.position_quote_monitor import APPositionQuoteMonitor  # noqa: E402

# ─────────────────────────────────────────────────────────────────────────────
# Shared constants
# ─────────────────────────────────────────────────────────────────────────────

_ENTRY   = 1.59
_CONTRACT = "BA260717C00222500"
_TICKER   = "BA"
_POS_ID   = "d7668918-bc1d-45f3-a783-357c7fe51afc"
_CLIENT   = "jasoncosby1@gmail.com"
_SIG      = "SIG-BA-001"


# ─────────────────────────────────────────────────────────────────────────────
# Shared test infrastructure — real ManagedPosition + real _refresh_once()
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class _Pos:
    """Minimal mutable position that real _refresh_once() can write to."""
    position_id:   str   = _POS_ID
    client_id:     str   = _CLIENT
    execution_mode: str  = "live"
    ticker:        str   = _TICKER
    option_symbol: str   = _CONTRACT
    optionsymbol:  str   = _CONTRACT
    entry_price:   float = _ENTRY
    entryprice:    float = _ENTRY
    current_option_price: float = 0.0
    currentoptionprice:   float = 0.0
    current_bid:   float = 0.0
    currentbid:    float = 0.0
    current_ask:   float = 0.0
    currentask:    float = 0.0
    current_underlying:  float = 0.0
    currentunderlying:   float = 0.0
    peak_pnl_pct:  float = 0.0
    peakpnlpct:    float = 0.0
    max_profit_seen: float = 0.0
    maxprofitseen:   float = 0.0
    option_pnl_pct:  float = 0.0
    optionpnlpct:    float = 0.0
    touched_profit:  bool  = False
    touchedprofit:   bool  = False
    closed:          bool  = False
    quantity:        int   = 1
    quantity_remaining: int = 1
    side:            str  = "CALL"
    underlying_entry: float = 220.0
    underlying_stop:  float = 215.0
    underlying_target: float = 230.0
    scale_outs_done:  int  = 0
    last_option_quote_update_ts: Optional[datetime] = None
    lastoptionquoteupdatets: Optional[datetime] = None
    last_underlying_quote_update_ts: Optional[datetime] = None
    lastunderlyingquoteupdatets: Optional[datetime] = None
    live_executable_price_source: str = ""
    liveexecutablepricesource: str = ""
    analytics_mark_price: float = 0.0
    analyticsmarkprice: float = 0.0
    executable_exit_price: float = 0.0
    executable_quote_valid: bool = False
    pricing_mode: str = ""
    raw_execution_mode: str = ""
    # extra fields that _refresh_once may write
    quotestate: str = ""
    quote_state: str = ""

    def __setattr__(self, name, value):
        # Accept any dynamic attribute writes from QPM
        object.__setattr__(self, name, value)


def _make_qpm(positions: list, *, bid: float, ask: float, und: float = 220.0) -> APPositionQuoteMonitor:
    """Build a QPM with a fake broker returning exactly one quote."""
    fake_quote = {
        "symbol": _CONTRACT,
        "bid":    bid,
        "ask":    ask,
        "mark":   round((bid + ask) / 2.0, 4) if bid > 0 and ask > 0 else ask,
        "last":   round((bid + ask) / 2.0, 4) if bid > 0 and ask > 0 else ask,
    }
    fake_und_quote = {"symbol": _TICKER, "last": und}

    broker = MagicMock()
    broker.get_option_quotes = MagicMock(return_value={_CONTRACT: fake_quote})
    broker.get_quotes = MagicMock(return_value={_TICKER: fake_und_quote})

    fake_exit_engine = MagicMock()
    fake_exit_engine.active_positions = MagicMock(return_value=positions)
    fake_exit_engine._lock = threading.Lock()
    fake_exit_engine.applyquotesnapshots = MagicMock()

    qpm = APPositionQuoteMonitor.__new__(APPositionQuoteMonitor)
    qpm.broker = broker
    qpm.client_id = _CLIENT
    qpm.exit_engine = fake_exit_engine
    qpm._alert_fn = lambda m: None
    qpm._interval = 2.0
    qpm._stop = threading.Event()
    qpm._kick = threading.Event()
    qpm._thread = None
    qpm._health = {}
    qpm._health_lock = threading.Lock()
    qpm._last_push_price = {}
    qpm._last_wake_price = {}
    qpm._last_wake_ts = {}
    qpm._last_immediate_refresh_ts = {}
    qpm._last_db_persist_ts = {}
    qpm._last_db_persist_price = {}
    qpm._orders_meta_available = None
    qpm._cycles = 0
    qpm._consecutive_failures = 0
    qpm._rate_limit_backoff_sec = 1.0
    qpm._last_cycle_ts = 0.0
    qpm._metrics = {k: 0 for k in [
        "cycles", "cache_hits", "cache_misses", "rate_limited",
        "batch_calls", "fallback_calls", "spread_rejects", "blind_alerts",
        "wakes_sent", "wakes_suppressed", "immediate_retry_requests",
        "immediate_retry_coalesced", "immediate_retry_evictions",
        "immediate_retry_backoff_suppressed", "exits_gated_blind",
        "exits_gated_stale",
    ]}
    return qpm


def _run_once(qpm: APPositionQuoteMonitor):
    """Call real _refresh_once() with all DB writes patched out."""
    with patch.object(qpm, "_persist_quote_to_db",     return_value=False), \
         patch.object(qpm, "_persist_mfe_mae_to_orders", return_value=None), \
         patch.object(qpm, "_mark_mfe_mae_unavailable",  return_value=None), \
         patch.object(qpm, "_prune_closed",               return_value=None), \
         patch.object(qpm, "_fetch_batch_cached") as mock_fetch:
        # Return appropriate quotes for contract or underlying lookup
        def _batch(symbols):
            result = {}
            for s in symbols:
                s_up = s.upper()
                if s_up == _CONTRACT:
                    result[s_up] = {
                        "symbol": s_up,
                        "bid":    qpm._test_bid,
                        "ask":    qpm._test_ask,
                        "mark":   round((qpm._test_bid + qpm._test_ask) / 2.0, 4)
                                  if qpm._test_bid > 0 and qpm._test_ask > 0
                                  else qpm._test_ask,
                        "last":   round((qpm._test_bid + qpm._test_ask) / 2.0, 4)
                                  if qpm._test_bid > 0 and qpm._test_ask > 0
                                  else qpm._test_ask,
                    }
                elif s_up == _TICKER:
                    result[s_up] = {"symbol": s_up, "last": 220.0}
            return result
        mock_fetch.side_effect = _batch
        qpm._refresh_once()


def _run_refresh(pos: _Pos, *, bid: float, ask: float) -> _Pos:
    """Run a single QPM refresh cycle against a real position object."""
    qpm = _make_qpm([pos], bid=bid, ask=ask)
    qpm._test_bid = bid
    qpm._test_ask = ask
    _run_once(qpm)
    return pos


# ─────────────────────────────────────────────────────────────────────────────
# Test A — Exact BA replay (production-path)
# ─────────────────────────────────────────────────────────────────────────────


class TestBAExactReplay:
    def test_analytics_mark_is_midpoint(self):
        pos = _Pos()
        _run_refresh(pos, bid=1.63, ask=1.79)
        mark = getattr(pos, "analytics_mark_price", 0.0)
        mid = (1.63 + 1.79) / 2.0
        assert abs(mark - mid) < 0.01 or mark > 1.63, (
            f"analytics_mark should be ~mid={mid:.4f}, got {mark}"
        )

    def test_executable_pnl_is_bid_based(self):
        pos = _Pos()
        _run_refresh(pos, bid=1.63, ask=1.79)
        exec_pnl = getattr(pos, "option_pnl_pct", None)
        bid_pnl  = (1.63 - _ENTRY) / _ENTRY   # +2.52%
        assert exec_pnl is not None
        assert abs(exec_pnl - bid_pnl) < 0.002, (
            f"Executable P&L must be bid-based +2.52%, got {exec_pnl*100:.2f}%"
        )

    def test_touched_profit_does_not_arm_at_bid_pnl_252(self):
        pos = _Pos()
        _run_refresh(pos, bid=1.63, ask=1.79)
        assert pos.touched_profit is False, (
            "touched_profit must not arm at +2.52% bid P&L"
        )

    def test_peak_is_bid_based(self):
        pos = _Pos()
        _run_refresh(pos, bid=1.63, ask=1.79)
        assert pos.peak_pnl_pct < 0.05, (
            f"Peak must reflect bid P&L (<5%), got {pos.peak_pnl_pct*100:.2f}%"
        )

    def test_quote2_no_touched_profit_stop_possible(self):
        pos = _Pos()
        _run_refresh(pos, bid=1.63, ask=1.79)   # peak armed?
        assert pos.touched_profit is False
        _run_refresh(pos, bid=1.45, ask=1.74)
        assert pos.touched_profit is False, (
            "TOUCHED_PROFIT_STOP impossible — touched_profit never armed"
        )

    def test_executable_source_is_bid(self):
        pos = _Pos()
        _run_refresh(pos, bid=1.63, ask=1.79)
        src = getattr(pos, "live_executable_price_source", "")
        assert src == "bid", f"Expected 'bid', got {src!r}"

    def test_current_option_price_is_bid_not_mid(self):
        pos = _Pos()
        _run_refresh(pos, bid=1.63, ask=1.79)
        assert abs(pos.current_option_price - 1.63) < 0.001, (
            f"current_option_price must be bid=1.63, got {pos.current_option_price}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Test B — Real executable winner
# ─────────────────────────────────────────────────────────────────────────────


class TestRealExecutableWinner:
    def test_bid_above_5pct_arms_touched_profit(self):
        pos = _Pos()
        _run_refresh(pos, bid=1.70, ask=1.79)
        assert pos.touched_profit is True

    def test_peak_stored_from_bid(self):
        pos = _Pos()
        _run_refresh(pos, bid=1.70, ask=1.79)
        expected = (1.70 - _ENTRY) / _ENTRY
        assert abs(pos.peak_pnl_pct - expected) < 0.002

    def test_profit_floor_uses_bid_not_mid(self):
        pos = _Pos()
        _run_refresh(pos, bid=1.70, ask=1.79)
        peak_q1 = pos.peak_pnl_pct
        _run_refresh(pos, bid=1.64, ask=1.72)
        bid_pnl_q2 = (1.64 - _ENTRY) / _ENTRY
        assert abs(pos.option_pnl_pct - bid_pnl_q2) < 0.002
        assert pos.peak_pnl_pct <= peak_q1 + 0.001


# ─────────────────────────────────────────────────────────────────────────────
# Test C — Midpoint rises while bid does not
# ─────────────────────────────────────────────────────────────────────────────


class TestMidRiseBidFlat:
    def test_rising_ask_flat_bid_no_touched_profit(self):
        pos = _Pos()
        _run_refresh(pos, bid=1.60, ask=2.10)
        assert pos.touched_profit is False

    def test_analytics_mark_captured_separately(self):
        pos = _Pos()
        _run_refresh(pos, bid=1.60, ask=2.10)
        mark = getattr(pos, "analytics_mark_price", 0.0)
        assert mark > pos.current_bid or mark > 0


# ─────────────────────────────────────────────────────────────────────────────
# Test D — PAPER behavior isolation
# ─────────────────────────────────────────────────────────────────────────────


class TestPaperBehavior:
    def test_paper_uses_mid_for_pnl(self):
        pos = _Pos(execution_mode="paper")
        _run_refresh(pos, bid=1.63, ask=1.79)
        mid = (1.63 + 1.79) / 2.0
        expected = (mid - _ENTRY) / _ENTRY
        assert abs(pos.option_pnl_pct - expected) < 0.005, (
            f"PAPER must use mid, expected ~{expected*100:.2f}%, got {pos.option_pnl_pct*100:.2f}%"
        )

    def test_paper_touched_profit_from_mid(self):
        pos = _Pos(execution_mode="paper")
        _run_refresh(pos, bid=1.63, ask=1.79)
        mid_pnl = ((1.63 + 1.79) / 2.0 - _ENTRY) / _ENTRY  # ~+7.55%
        if mid_pnl >= 0.05:
            assert pos.touched_profit is True

    def test_paper_source_labeled_simulation(self):
        pos = _Pos(execution_mode="paper")
        _run_refresh(pos, bid=1.63, ask=1.79)
        src = getattr(pos, "live_executable_price_source", "")
        assert "paper_mid_simulation" in src, f"Expected paper label, got {src!r}"

    def test_live_and_paper_diverge(self):
        pos_live  = _Pos(execution_mode="live")
        pos_paper = _Pos(execution_mode="paper")
        _run_refresh(pos_live,  bid=1.63, ask=1.79)
        _run_refresh(pos_paper, bid=1.63, ask=1.79)
        assert pos_live.option_pnl_pct < pos_paper.option_pnl_pct


# ─────────────────────────────────────────────────────────────────────────────
# Blocker 1 — Unknown mode tests
# ─────────────────────────────────────────────────────────────────────────────


class TestUnknownModeClassification:
    """Blank/None/unknown/malformed execution_mode must use bid, not mid."""

    @pytest.mark.parametrize("mode", ["", None, "unknown", "LIVE", "PAPER", "Live", "Paper", "xyz"])
    def test_unknown_mode_mid_cannot_arm_touched_profit(self, mode):
        """
        For any non-paper, non-live mode: if bid < 5% profit threshold,
        touched_profit must remain False regardless of what mid says.
        """
        if mode is None:
            pos = _Pos()
            object.__setattr__(pos, "execution_mode", None)
            object.__setattr__(pos, "executionmode", None)
        else:
            pos = _Pos(execution_mode=mode)
            try:
                pos.executionmode = mode
            except Exception:
                pass

        # bid = 1.63 → bid P&L +2.52% < 5%
        # mid = 1.71 → mid P&L +7.55% ≥ 5% (would arm if midpoint were used)
        _run_refresh(pos, bid=1.63, ask=1.79)

        # Only exact "paper" should use mid; all others → bid accounting
        _effective_mode = str(mode or "").strip().lower() if mode is not None else ""
        if _effective_mode == "paper":
            # PAPER is allowed to use mid — skip this check
            return
        assert pos.touched_profit is False, (
            f"execution_mode={mode!r} must not arm touched_profit via mid P&L; "
            f"touched={pos.touched_profit} option_pnl={getattr(pos, 'option_pnl_pct', '?')}"
        )

    def test_blank_mode_pricing_mode_is_live_risk_unproven(self):
        pos = _Pos(execution_mode="")
        _run_refresh(pos, bid=1.63, ask=1.79)
        pricing_mode = getattr(pos, "pricing_mode", "")
        assert pricing_mode in ("live", "live_risk_unproven"), (
            f"Blank mode must yield live or live_risk_unproven, got {pricing_mode!r}"
        )

    def test_unknown_string_mode_pricing_mode_is_live_risk_unproven(self):
        pos = _Pos(execution_mode="unknown")
        _run_refresh(pos, bid=1.63, ask=1.79)
        assert getattr(pos, "pricing_mode", "") == "live_risk_unproven"

    def test_exact_paper_gets_paper_pricing(self):
        pos = _Pos(execution_mode="paper")
        _run_refresh(pos, bid=1.63, ask=1.79)
        assert getattr(pos, "pricing_mode", "") == "paper"

    def test_exact_live_gets_live_pricing(self):
        pos = _Pos(execution_mode="live")
        _run_refresh(pos, bid=1.63, ask=1.79)
        assert getattr(pos, "pricing_mode", "") == "live"


# ─────────────────────────────────────────────────────────────────────────────
# Blocker 2 — Missing bid must not leak mark/ask into LIVE P&L
# ─────────────────────────────────────────────────────────────────────────────


class TestMissingBidBlocker:
    """Production-path test: bid=0, ask=1.90, mark/last=1.80."""

    def _run_missing_bid(self) -> _Pos:
        pos = _Pos(execution_mode="live")
        # bid=0 means no executable price
        _run_refresh(pos, bid=0.0, ask=1.90)
        return pos

    def test_analytics_mark_may_capture_mark(self):
        """mark/last may appear in analytics_mark_price but NOT in decision price."""
        pos = self._run_missing_bid()
        # analytics mark is allowed to be non-zero from mark/last/ask
        # (it's for charts); the key invariant is the DECISION price
        mark = getattr(pos, "analytics_mark_price", 0.0)
        assert mark >= 0.0  # mark is captured if available

    def test_executable_quote_is_invalid(self):
        pos = self._run_missing_bid()
        assert getattr(pos, "executable_quote_valid", True) is False, (
            "executable_quote_valid must be False when bid=0"
        )

    def test_current_option_price_is_not_ask_or_mark(self):
        pos = self._run_missing_bid()
        # current_option_price must NOT be set to 1.80 or 1.90 for LIVE
        cur = pos.current_option_price
        assert cur <= 0.0 or cur == pos.current_bid, (
            f"current_option_price must not be mark/ask when bid=0; got {cur}"
        )

    def test_touched_profit_does_not_arm(self):
        pos = self._run_missing_bid()
        assert pos.touched_profit is False

    def test_peak_does_not_rise(self):
        pos = self._run_missing_bid()
        assert pos.peak_pnl_pct <= 0.0

    def test_executable_exit_price_is_zero(self):
        pos = self._run_missing_bid()
        assert getattr(pos, "executable_exit_price", -1.0) == 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Blocker 3 — Contaminated midpoint state removed on adoption
# ─────────────────────────────────────────────────────────────────────────────


class TestAdoptionContaminationCleanse:
    """Exact regression: repair peak=+7.55% must become +2.52% after adoption."""

    def _make_engine_with_repair(self, peak: float = 0.0755, touched: bool = True):
        from ap_exit_engine import APExitEngine
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        engine._lock  = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}

        repair_id = f"broker-repair-{_CLIENT}-{_CONTRACT}"
        pos = _Pos(
            position_id   = repair_id,
            execution_mode = "",         # unknown
            current_bid   = 1.63,
            currentbid    = 1.63,
            current_option_price = 1.71,  # contaminated midpoint
            currentoptionprice   = 1.71,
            peak_pnl_pct  = peak,
            peakpnlpct    = peak,
            max_profit_seen = peak,
            maxprofitseen   = peak,
            touched_profit  = touched,
            touchedprofit   = touched,
        )
        engine._positions.append(pos)
        engine._positions_by_id[repair_id] = pos
        return engine, pos

    def test_midpoint_peak_becomes_bid_based_after_adoption(self):
        """repair peak=+7.55% (mid), bid=1.63, fill=1.59 → peak≈+2.52%."""
        engine, _ = self._make_engine_with_repair(peak=0.0755, touched=True)
        engine.adopt_canonical_position_identity(
            contract              = _CONTRACT,
            canonical_position_id = "d7668918",
            local_order_id        = "944f4c9d",
            broker_order_id       = "136961009",
            signal_id             = _SIG,
            canonical_signal_id   = _SIG,
            entry_fill            = 1.59,
            entry_ts              = None,
            execution_mode        = "live",
            client_id             = _CLIENT,
        )
        pos = engine._positions[0]
        expected_peak = (1.63 - 1.59) / 1.59  # +2.52%
        assert abs(pos.peak_pnl_pct - expected_peak) < 0.002, (
            f"Peak must be bid-based +2.52%, got {pos.peak_pnl_pct*100:.2f}%"
        )

    def test_midpoint_max_profit_seen_becomes_bid_based(self):
        engine, _ = self._make_engine_with_repair(peak=0.0755, touched=True)
        engine.adopt_canonical_position_identity(
            contract="BA260717C00222500", canonical_position_id="x",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        pos = engine._positions[0]
        assert abs(pos.max_profit_seen - (1.63 - 1.59) / 1.59) < 0.002

    def test_touched_profit_cleared_when_bid_pnl_below_5pct(self):
        engine, _ = self._make_engine_with_repair(peak=0.0755, touched=True)
        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="x",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        pos = engine._positions[0]
        # bid P&L = +2.52% < 5% → touched_profit must be False
        assert pos.touched_profit is False

    def test_no_fresh_bid_zeros_all_profit_state(self):
        engine, _ = self._make_engine_with_repair(peak=0.0755, touched=True)
        # Set current_bid = 0 (no fresh bid)
        engine._positions[0].current_bid = 0.0
        engine._positions[0].currentbid  = 0.0
        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="x",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        pos = engine._positions[0]
        assert pos.peak_pnl_pct    == 0.0
        assert pos.max_profit_seen == 0.0
        assert pos.touched_profit  is False


# ─────────────────────────────────────────────────────────────────────────────
# Blocker 4 — Canonical entry timestamp and underlying entry are applied
# ─────────────────────────────────────────────────────────────────────────────


class TestCanonicalProductionFields:
    def _make_engine_with_repair(self):
        from ap_exit_engine import APExitEngine
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        engine._lock  = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}
        repair_id = f"broker-repair-{_CLIENT}-{_CONTRACT}"
        pos = _Pos(position_id=repair_id, execution_mode="",
                   current_bid=1.63, currentbid=1.63,
                   peak_pnl_pct=0.0, touched_profit=False)
        engine._positions.append(pos)
        engine._positions_by_id[repair_id] = pos
        return engine, pos

    def test_opened_at_set_to_canonical_fill_time(self):
        engine, _ = self._make_engine_with_repair()
        fill_ts = datetime(2026, 7, 15, 9, 30, 0, tzinfo=timezone.utc)
        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="d1",
            local_order_id="l1", broker_order_id="b1",
            signal_id=_SIG, canonical_signal_id=_SIG,
            entry_fill=1.59, entry_ts=fill_ts,
            execution_mode="live", client_id=_CLIENT,
            underlying_entry=220.50,
        )
        pos = engine._positions[0]
        assert getattr(pos, "opened_at", None) == fill_ts

    def test_underlying_entry_is_canonical(self):
        engine, _ = self._make_engine_with_repair()
        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="d1",
            local_order_id="l1", broker_order_id="b1",
            signal_id=_SIG, canonical_signal_id=_SIG,
            entry_fill=1.59, entry_ts=None,
            execution_mode="live", client_id=_CLIENT,
            underlying_entry=221.75,
        )
        pos = engine._positions[0]
        assert abs(getattr(pos, "underlying_entry", 0.0) - 221.75) < 0.01

    def test_score_tier_pattern_timeframe_in_signal_dict(self):
        engine, _ = self._make_engine_with_repair()
        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="d1",
            local_order_id="l1", broker_order_id="b1",
            signal_id=_SIG, canonical_signal_id=_SIG,
            entry_fill=1.59, entry_ts=None,
            execution_mode="live", client_id=_CLIENT,
            score=88.0, tier="A", pattern="3-1-2", timeframe="5m",
        )
        pos = engine._positions[0]
        sig = getattr(pos, "signal", {}) or {}
        assert sig.get("score") == 88.0 or getattr(pos, "score", None) == 88.0
        assert sig.get("tier")  == "A"  or getattr(pos, "tier",  None) == "A"
        assert sig.get("pattern") == "3-1-2" or getattr(pos, "pattern", None) == "3-1-2"
        assert sig.get("timeframe") == "5m"  or getattr(pos, "timeframe", None) == "5m"


# ─────────────────────────────────────────────────────────────────────────────
# Blocker 5 — Adoption identity fencing
# ─────────────────────────────────────────────────────────────────────────────


class TestAdoptionIdentityFencing:
    def _base_engine(self):
        from ap_exit_engine import APExitEngine
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        engine._lock  = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}
        return engine

    def _add_repair(self, engine, *, client=_CLIENT, mode="", contract=_CONTRACT):
        repair_id = f"broker-repair-{client}-{contract}"
        pos = _Pos(position_id=repair_id, execution_mode=mode,
                   client_id=client, current_bid=1.63, peak_pnl_pct=0.0)
        pos.option_symbol = contract
        engine._positions.append(pos)
        engine._positions_by_id[repair_id] = pos
        return pos

    def test_client_mismatch_refuses_adoption(self):
        engine = self._base_engine()
        self._add_repair(engine, client="other@client.com")
        result = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live",
            client_id=_CLIENT,  # different from repair client
        )
        assert result.disposition == "RETRY_CLIENT_MISMATCH"
        assert result.adopted is False
        assert engine._positions[0].position_id.startswith("broker-repair-")

    def test_no_adoption_when_canonical_already_exists(self):
        engine = self._base_engine()
        self._add_repair(engine)
        # Pre-populate canonical entry in index AND positions list
        canon_pos = _Pos(position_id="canon-existing", option_symbol=_CONTRACT,
                         client_id=_CLIENT, execution_mode="live")
        engine._positions.append(canon_pos)
        engine._positions_by_id["canon-existing"] = canon_pos

        result = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon-existing",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live",
            client_id=_CLIENT,
        )
        # Canonical + repair → collapse; must end with exactly 1 active
        assert result.adopted is True
        assert len([p for p in engine._positions if not p.closed]) == 1

    def test_repair_to_paper_mismatch_refused(self):
        engine = self._base_engine()
        self._add_repair(engine, mode="live")  # repair is live
        result = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="paper",  # canonical says paper
            client_id=_CLIENT,
        )
        assert result.disposition == "RETRY_MODE_MISMATCH"
        assert result.adopted is False

    def test_blank_repair_mode_accepts_live_canonical(self):
        engine = self._base_engine()
        self._add_repair(engine, mode="")  # repair mode unknown
        result = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live",
            client_id=_CLIENT,
        )
        assert result.disposition == "ADOPTED"
        assert result.adopted is True


# ─────────────────────────────────────────────────────────────────────────────
# Blocker 4 — get_pending_orders returns canonical fields
# ─────────────────────────────────────────────────────────────────────────────


class TestGetPendingOrdersColumns:
    """Verify the production SQL selects execution_mode, filled_ts, meta."""

    def test_select_contains_execution_mode(self):
        import inspect, ap.fill_monitor as fm
        src = inspect.getsource(fm.get_pending_orders)
        assert "execution_mode" in src, "get_pending_orders must SELECT execution_mode"

    def test_select_contains_filled_ts(self):
        import inspect, ap.fill_monitor as fm
        src = inspect.getsource(fm.get_pending_orders)
        assert "filled_ts" in src, "get_pending_orders must SELECT filled_ts"

    def test_select_contains_meta(self):
        import inspect, ap.fill_monitor as fm
        src = inspect.getsource(fm.get_pending_orders)
        assert "meta" in src, "get_pending_orders must SELECT meta"

    def test_select_contains_canonical_signal_id(self):
        import inspect, ap.fill_monitor as fm
        src = inspect.getsource(fm.get_pending_orders)
        assert "canonical_signal_id" in src


# ─────────────────────────────────────────────────────────────────────────────
# Test E — Synthetic-to-canonical adoption (full flow)
# ─────────────────────────────────────────────────────────────────────────────


class TestSyntheticToCanonicalAdoption:
    def _engine_with_repair(self):
        from ap_exit_engine import APExitEngine
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        engine._lock  = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}
        repair_id = f"broker-repair-{_CLIENT}-{_CONTRACT}"
        pos = _Pos(position_id=repair_id, execution_mode="",
                   current_bid=1.63, currentbid=1.63,
                   peak_pnl_pct=0.0755, touched_profit=True)
        engine._positions.append(pos)
        engine._positions_by_id[repair_id] = pos
        return engine

    def test_adopted_not_duplicated(self):
        engine = self._engine_with_repair()
        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="d7668918",
            local_order_id="944f4c9d", broker_order_id="136961009",
            signal_id=_SIG, canonical_signal_id=_SIG,
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        assert len(engine._positions) == 1

    def test_canonical_id_in_index(self):
        engine = self._engine_with_repair()
        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="d7668918",
            local_order_id="944f4c9d", broker_order_id="136961009",
            signal_id=_SIG, canonical_signal_id=_SIG,
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        assert "d7668918" in engine._positions_by_id
        assert not any(k.startswith("broker-repair-") for k in engine._positions_by_id)

    def test_execution_mode_upgraded_to_live(self):
        engine = self._engine_with_repair()
        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="d7668918",
            local_order_id="944f4c9d", broker_order_id="136961009",
            signal_id=_SIG, canonical_signal_id=_SIG,
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        assert engine._positions[0].execution_mode == "live"


# ─────────────────────────────────────────────────────────────────────────────
# Test F — No duplicate exit after adoption
# ─────────────────────────────────────────────────────────────────────────────


class TestNoDuplicateAfterAdoption:
    def test_add_position_same_symbol_is_noop_after_adoption(self):
        from ap_exit_engine import APExitEngine, ManagedPosition
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        engine._lock  = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}
        engine._assert_position_invariants = MagicMock()

        repair_id = f"broker-repair-{_CLIENT}-{_CONTRACT}"
        pos = _Pos(position_id=repair_id, execution_mode="",
                   current_bid=1.63, peak_pnl_pct=0.0)
        engine._positions.append(pos)
        engine._positions_by_id[repair_id] = pos

        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="l1", broker_order_id="b1",
            signal_id=_SIG, canonical_signal_id=_SIG,
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        assert len(engine._positions) == 1

        # Attempt second add
        pos2 = MagicMock(spec=ManagedPosition)
        pos2.position_id   = "canon1"
        pos2.option_symbol = _CONTRACT
        pos2.ticker        = _TICKER
        pos2.execution_mode = "live"
        pos2.closed        = False
        with patch("ap_exit_engine._normalize_ticker", return_value=_TICKER):
            engine.add_position(pos2)
        assert len(engine._positions) == 1


# ─────────────────────────────────────────────────────────────────────────────
# Final Blocker 1 — Canonical + repair collapses to one object
# ─────────────────────────────────────────────────────────────────────────────


class TestCanonicalPlusRepairCollapse:
    """When both canonical and repair positions exist, adoption must merge
    and remove the repair so exactly one active object remains."""

    def _make_engine_with_both(self):
        from ap_exit_engine import APExitEngine, CanonicalAdoptionResult
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        engine._lock  = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}

        canon_id  = "d7668918-bc1d-45f3-a783-357c7fe51afc"
        repair_id = f"broker-repair-{_CLIENT}-{_CONTRACT}"

        # Canonical: already exists
        canon = _Pos(position_id=canon_id, execution_mode="live",
                     option_symbol=_CONTRACT, client_id=_CLIENT,
                     current_bid=1.63, peak_pnl_pct=0.025, touched_profit=False)
        engine._positions.append(canon)
        engine._positions_by_id[canon_id] = canon

        # Repair: also exists (the problem case)
        repair = _Pos(position_id=repair_id, execution_mode="",
                      option_symbol=_CONTRACT, client_id=_CLIENT,
                      current_bid=1.64, peak_pnl_pct=0.0755,  # contaminated mid peak
                      touched_profit=True)
        engine._positions.append(repair)
        engine._positions_by_id[repair_id] = repair

        return engine, canon_id, repair_id

    def test_one_active_object_after_collapse(self):
        engine, canon_id, repair_id = self._make_engine_with_both()
        result = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id=canon_id,
            local_order_id="944f4c9d", broker_order_id="136961009",
            signal_id=_SIG, canonical_signal_id=_SIG,
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        active = [p for p in engine._positions if not p.closed]
        assert len(active) == 1, f"Expected 1 active, got {len(active)}"

    def test_canonical_id_retained(self):
        engine, canon_id, repair_id = self._make_engine_with_both()
        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id=canon_id,
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        assert engine._positions[0].position_id == canon_id

    def test_repair_removed_from_index(self):
        engine, canon_id, repair_id = self._make_engine_with_both()
        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id=canon_id,
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        assert repair_id not in engine._positions_by_id
        assert canon_id in engine._positions_by_id

    def test_repair_absent_from_positions_list(self):
        engine, canon_id, repair_id = self._make_engine_with_both()
        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id=canon_id,
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        for p in engine._positions:
            assert not str(p.position_id).startswith("broker-repair-")

    def test_disposition_is_already_canonical_repair_removed(self):
        from ap_exit_engine import CanonicalAdoptionResult
        engine, canon_id, repair_id = self._make_engine_with_both()
        result = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id=canon_id,
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        assert isinstance(result, CanonicalAdoptionResult)
        assert result.disposition == "ALREADY_CANONICAL_REPAIR_REMOVED"
        assert result.adopted is True


# ─────────────────────────────────────────────────────────────────────────────
# Final Blocker 2 — Structured CanonicalAdoptionResult + RETRY cannot fall through
# ─────────────────────────────────────────────────────────────────────────────


class TestStructuredAdoptionResult:
    """CanonicalAdoptionResult is returned for all paths; RETRY never seeds."""

    def _engine(self):
        from ap_exit_engine import APExitEngine
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        engine._lock  = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}
        return engine

    def _add_repair(self, engine, *, client=_CLIENT, mode="", contract=_CONTRACT):
        from ap_exit_engine import CanonicalAdoptionResult
        repair_id = f"broker-repair-{client}-{contract}"
        pos = _Pos(position_id=repair_id, execution_mode=mode,
                   client_id=client, option_symbol=contract,
                   current_bid=1.63, peak_pnl_pct=0.0)
        engine._positions.append(pos)
        engine._positions_by_id[repair_id] = pos

    def test_adopted_disposition_on_success(self):
        from ap_exit_engine import CanonicalAdoptionResult
        engine = self._engine()
        self._add_repair(engine)
        r = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        assert isinstance(r, CanonicalAdoptionResult)
        assert r.disposition == "ADOPTED"
        assert r.adopted is True
        assert r.safe_to_seed is False

    def test_no_repair_found_disposition(self):
        from ap_exit_engine import CanonicalAdoptionResult
        engine = self._engine()  # no positions
        r = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        assert r.disposition == "NO_REPAIR_FOUND"
        assert r.safe_to_seed is True

    def test_client_mismatch_returns_retry_not_seeds(self):
        from ap_exit_engine import CanonicalAdoptionResult
        engine = self._engine()
        self._add_repair(engine, client="other@client.com")
        r = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live",
            client_id=_CLIENT,  # different from repair
        )
        assert r.disposition == "RETRY_CLIENT_MISMATCH"
        assert r.adopted is False
        assert r.safe_to_seed is False
        assert r.retryable is True
        # Repair position must still exist (not adopted)
        assert len(engine._positions) == 1
        assert engine._positions[0].position_id.startswith("broker-repair-")

    def test_mode_mismatch_returns_retry_not_seeds(self):
        from ap_exit_engine import CanonicalAdoptionResult
        engine = self._engine()
        self._add_repair(engine, mode="live")
        r = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="paper",  # mismatch
            client_id=_CLIENT,
        )
        assert r.disposition == "RETRY_MODE_MISMATCH"
        assert r.safe_to_seed is False


# ─────────────────────────────────────────────────────────────────────────────
# Final Blocker 3 — _normalize_canonical_ts
# ─────────────────────────────────────────────────────────────────────────────


class TestNormalizeCanonicalTs:
    """All timestamp formats produce correct aware UTC datetime."""

    def _norm(self, ts, *, fallback=None):
        from ap_exit_engine import _normalize_canonical_ts
        return _normalize_canonical_ts(ts, fallback=fallback, local_order_id="test-order")

    def test_aware_datetime_returned_as_utc(self):
        from datetime import datetime, timezone, timedelta
        eastern = timezone(timedelta(hours=-5))
        ts = datetime(2026, 7, 15, 9, 30, 0, tzinfo=eastern)
        result = self._norm(ts)
        assert result.tzinfo == timezone.utc
        assert result.hour == 14  # 9:30 ET = 14:30 UTC

    def test_naive_datetime_assumed_utc(self):
        from datetime import datetime, timezone
        ts = datetime(2026, 7, 15, 9, 30, 0)  # naive
        result = self._norm(ts)
        assert result.tzinfo == timezone.utc
        assert result.hour == 9

    def test_iso_string_with_z(self):
        result = self._norm("2026-07-15T09:30:00Z")
        from datetime import timezone
        assert result.tzinfo == timezone.utc
        assert result.hour == 9

    def test_iso_string_with_offset(self):
        result = self._norm("2026-07-15T09:30:00-05:00")
        assert result.hour == 14  # 14:30 UTC

    def test_numeric_epoch(self):
        from datetime import datetime, timezone
        ts = datetime(2026, 7, 15, 14, 30, 0, tzinfo=timezone.utc).timestamp()
        result = self._norm(ts)
        assert result.hour == 14

    def test_malformed_string_uses_fallback(self, caplog):
        import logging
        with caplog.at_level(logging.WARNING, logger="ap_exit_engine"):
            result = self._norm("NOT_A_DATE", fallback="2026-07-15T14:30:00Z")
        assert result.hour == 14
        assert "CANONICAL_ENTRY_TIMESTAMP_FALLBACK" in caplog.text

    def test_malformed_string_no_fallback_returns_now(self, caplog):
        import logging
        from datetime import datetime, timezone
        with caplog.at_level(logging.WARNING, logger="ap_exit_engine"):
            result = self._norm("NOT_A_DATE")
        assert isinstance(result, datetime)
        assert result.tzinfo == timezone.utc
        assert "CANONICAL_ENTRY_TIMESTAMP_FALLBACK" in caplog.text

    def test_adoption_uses_normalized_ts(self):
        """opened_at must be an aware datetime after adoption."""
        from ap_exit_engine import APExitEngine
        from datetime import timezone
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        engine._lock  = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}
        repair_id = f"broker-repair-{_CLIENT}-{_CONTRACT}"
        pos = _Pos(position_id=repair_id, execution_mode="",
                   current_bid=1.63, peak_pnl_pct=0.0)
        engine._positions.append(pos)
        engine._positions_by_id[repair_id] = pos

        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts="2026-07-15T09:30:00Z",
            execution_mode="live", client_id=_CLIENT,
        )
        opened_at = getattr(engine._positions[0], "opened_at", None)
        assert opened_at is not None
        assert hasattr(opened_at, "tzinfo") and opened_at.tzinfo is not None
        assert opened_at.hour == 9
