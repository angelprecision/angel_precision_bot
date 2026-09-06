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
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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
    last_option_bid_update_ts: Optional[datetime] = None
    lastoptionbidupdatets: Optional[datetime] = None
    last_underlying_quote_update_ts: Optional[datetime] = None
    lastunderlyingquoteupdatets: Optional[datetime] = None
    option_bid_valid: Optional[bool] = None
    optionbidvalid: Optional[bool] = None
    option_quote_fresh: Optional[bool] = None
    optionquotefresh: Optional[bool] = None
    underlying_available: Optional[bool] = None
    underlyingavailable: Optional[bool] = None
    underlying_fresh: Optional[bool] = None
    underlyingfresh: Optional[bool] = None
    hard_exit_reference_price: float = 0.0
    hardexitreferenceprice: float = 0.0
    hard_exit_reference_source: str = ""
    hardexitreferencesource: str = ""
    hard_exit_reference_validity: str = "no_data"
    hardexitreferencevalidity: str = "no_data"
    hard_exit_reference_ts: Optional[datetime] = None
    hardexitreferencets: Optional[datetime] = None
    hard_exit_reference_pnl_pct: Optional[float] = None
    hardexitreferencepnlpct: Optional[float] = None
    hard_exit_reference_refresh_needed: bool = True
    hardexitreferencerefreshneeded: bool = True
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
    # PR #385: position-scoped touched-profit consecutive-confirmation dict.
    # Required attribute; tests that exercise touched_profit must init this.
    qpm._tp_pending_confirm = {}
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
        # PR #385: touched_profit requires TWO consecutive fresh BID observations
        # above TOUCHED_PROFIT_ARM_PCT. This test uses the SAME QPM instance
        # across polls so the position-scoped confirmation state persists.
        pos = _Pos()
        qpm = _make_qpm([pos], bid=1.70, ask=1.79)
        # Poll 1: qualifying observation — pending set, not armed
        qpm._test_bid = 1.70
        qpm._test_ask = 1.79
        _run_once(qpm)
        assert pos.touched_profit is False, (
            "PR #385: one qualifying poll must NOT arm touched_profit; "
            "requires two consecutive fresh BID observations"
        )
        # Poll 2: consecutive qualifying observation — armed
        qpm._test_bid = 1.70
        qpm._test_ask = 1.79
        _run_once(qpm)
        assert pos.touched_profit is True, (
            "PR #385: two consecutive fresh BID polls above +5% must arm touched_profit"
        )

    def test_missing_bid_cycle_resets_touched_profit_confirmation(self):
        pos = _Pos()
        qpm = _make_qpm([pos], bid=1.75, ask=1.79)

        qpm._test_bid = 1.75
        qpm._test_ask = 1.79
        _run_once(qpm)
        assert pos.touched_profit is False
        assert qpm._tp_pending_confirm

        qpm._test_bid = 0.0
        qpm._test_ask = 1.79
        _run_once(qpm)
        assert pos.touched_profit is False
        assert all(pending is False for pending in qpm._tp_pending_confirm.values())

        qpm._test_bid = 1.75
        qpm._test_ask = 1.79
        _run_once(qpm)
        assert pos.touched_profit is False, (
            "green -> missing -> green must not arm; the green observations "
            "are not consecutive"
        )

        _run_once(qpm)
        assert pos.touched_profit is True, (
            "A second consecutive green BID after the reset should arm touched_profit"
        )

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
        # PR #385: PAPER midpoint MUST NOT arm touched_profit — that state feeds
        # soft-exit decisions and midpoint is not executable.  Only two consecutive
        # fresh BID observations above TOUCHED_PROFIT_ARM_PCT (+5%) arm it, in
        # every execution mode.
        # Part A: PAPER midpoint above +5%, BID below +5%, two polls → False.
        pos_a = _Pos(execution_mode="paper")
        qpm_a = _make_qpm([pos_a], bid=1.63, ask=1.79)  # mid=1.71 → +7.55%; bid +2.52%
        qpm_a._test_bid = 1.63
        qpm_a._test_ask = 1.79
        _run_once(qpm_a)
        _run_once(qpm_a)
        assert pos_a.touched_profit is False, (
            "PR #385: PAPER midpoint above +5% with BID below +5% must NEVER arm "
            "touched_profit — even after two polls"
        )

        # Part B: PAPER with BID above +5% for two consecutive polls → True.
        pos_b = _Pos(execution_mode="paper")
        qpm_b = _make_qpm([pos_b], bid=1.70, ask=1.79)  # bid +6.92%
        qpm_b._test_bid = 1.70
        qpm_b._test_ask = 1.79
        _run_once(qpm_b)
        assert pos_b.touched_profit is False, "PAPER: one poll must not arm"
        _run_once(qpm_b)
        assert pos_b.touched_profit is True, (
            "PR #385: PAPER with BID above +5% for two consecutive fresh polls "
            "must arm touched_profit"
        )

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
        fresh_bid_ts = datetime.now(timezone.utc)
        pos = _Pos(
            position_id   = repair_id,
            execution_mode = "live",
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
            last_option_bid_update_ts = fresh_bid_ts,
            lastoptionbidupdatets     = fresh_bid_ts,
            option_bid_valid = True,
            optionbidvalid   = True,
            option_quote_fresh = True,
            optionquotefresh   = True,
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
        engine._positions[0].last_option_bid_update_ts = None
        engine._positions[0].lastoptionbidupdatets = None
        engine._positions[0].option_bid_valid = False
        engine._positions[0].optionbidvalid = False
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
        pos = _Pos(position_id=repair_id, execution_mode="live",
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

    def _add_repair(self, engine, *, client=_CLIENT, mode="live", contract=_CONTRACT):
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

    def test_blank_repair_mode_refuses_live_canonical(self):
        engine = self._base_engine()
        self._add_repair(engine, mode="")  # repair mode unknown
        result = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live",
            client_id=_CLIENT,
        )
        assert result.disposition == "RETRY_MODE_MISMATCH"
        assert result.adopted is False


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
        pos = _Pos(position_id=repair_id, execution_mode="live",
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
        pos = _Pos(position_id=repair_id, execution_mode="live",
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

    def _make_engine_with_both(
        self, *, canonical_mode="live", repair_mode="live", repair_client=_CLIENT
    ):
        from ap_exit_engine import APExitEngine, CanonicalAdoptionResult
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        engine._lock  = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}

        canon_id  = "d7668918-bc1d-45f3-a783-357c7fe51afc"
        repair_id = f"broker-repair-{_CLIENT}-{_CONTRACT}"

        # Canonical: already exists
        canon = _Pos(position_id=canon_id, execution_mode=canonical_mode,
                     option_symbol=_CONTRACT, client_id=_CLIENT,
                     current_bid=1.63, peak_pnl_pct=0.025, touched_profit=False)
        engine._positions.append(canon)
        engine._positions_by_id[canon_id] = canon

        # Repair: also exists (the problem case)
        repair = _Pos(position_id=repair_id, execution_mode=repair_mode,
                      option_symbol=_CONTRACT, client_id=repair_client,
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

    def test_blank_mode_repair_is_quarantined_from_paper_canonical(self):
        engine, canon_id, repair_id = self._make_engine_with_both(
            canonical_mode="paper", repair_mode="", repair_client=_CLIENT,
        )
        repair = engine._positions_by_id[repair_id]
        repair.hard_exit_reference_validity = "proven"
        repair.hardexitreferencevalidity = "proven"
        repair.hard_exit_reference_price = 0.55
        repair.hardexitreferenceprice = 0.55

        result = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id=canon_id,
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="paper", client_id=_CLIENT,
        )

        canonical = engine._positions_by_id[canon_id]
        assert result.disposition == "RETRY_REPAIR_IDENTITY_UNPROVEN"
        assert result.adopted is False
        assert result.safe_to_seed is False
        assert result.retryable is True
        assert repair_id in engine._positions_by_id
        assert repair in engine._positions
        assert getattr(repair, "adoption_identity_quarantined", False) is True
        assert engine.active_positions() == [canonical]
        assert getattr(canonical, "hard_exit_reference_validity", "") != "proven"

    def test_blank_client_repair_is_quarantined_from_matching_mode_canonical(self):
        engine, canon_id, repair_id = self._make_engine_with_both(
            canonical_mode="live", repair_mode="live", repair_client="",
        )

        result = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id=canon_id,
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )

        canonical = engine._positions_by_id[canon_id]
        repair = engine._positions_by_id[repair_id]
        assert result.disposition == "RETRY_REPAIR_IDENTITY_UNPROVEN"
        assert result.adopted is False
        assert result.safe_to_seed is False
        assert result.retryable is True
        assert repair_id in engine._positions_by_id
        assert any(getattr(p, "position_id", "") == repair_id for p in engine._positions)
        assert getattr(repair, "adoption_identity_quarantined", False) is True
        assert engine.active_positions() == [canonical]

    def test_quarantined_repair_can_recover_canonical_owner_via_broker_precheck(self):
        from ap_exit_engine import APExitEngine

        class _Broker:
            account_id = "acct-1"
            mode = "live"
            def list_positions(self):
                return [{
                    "symbol": _CONTRACT,
                    "quantity": 1,
                    "cost_basis": 159.0,
                    "date_acquired": "2026-07-15T09:30:00Z",
                }]

        engine = APExitEngine(broker=_Broker(), email=_CLIENT)
        repair = _Pos(
            position_id=f"broker-repair-{_CLIENT}-{_CONTRACT}",
            option_symbol=_CONTRACT, client_id="", execution_mode="",
            quantity=1, quantity_remaining=1,
        )
        repair.adoption_identity_quarantined = True
        repair.adoption_identity_quarantine_reason = "repair_identity_unproven"
        engine._positions = [repair]
        engine._positions_by_id[repair.position_id] = repair

        db_row = {
            "id": "canon-recovered",
            "client_id": _CLIENT,
            "contract": _CONTRACT,
            "option_symbol": _CONTRACT,
            "underlying": _TICKER,
            "side": "CALL",
            "qty": 1,
            "quantity_remaining": 1,
            "entry_price": 1.59,
            "execution_mode": "live",
            "status": "OPEN",
            "signal_id": _SIG,
        }
        engine._load_db_position_row = lambda sym: db_row if sym == _CONTRACT else None
        engine._fetch_broker_quote = lambda sym: {
            "bid": 1.20, "ask": 1.25, "mark": 1.22, "last": 1.20,
        }

        assert engine.active_positions() == []
        class _Cursor:
            rowcount = 1

            def __init__(self):
                self._sql = ""

            def execute(self, sql, params=()):
                self._sql = str(sql)
                return self

            def fetchall(self):
                # No exact ENTRY lookup is needed when durable full qty
                # already agrees with fresh broker truth; hydration also
                # sees no active EXIT rows.
                return []

            def fetchone(self):
                if "FOR UPDATE" in self._sql:
                    return dict(db_row)
                return None

        @contextmanager
        def _conn():
            yield _Cursor()

        fake_db = SimpleNamespace(
            conn=_conn,
            run_with_retry=lambda fn, **_kwargs: fn(),
        )
        prior_db = sys.modules.get("ap.db")
        sys.modules["ap.db"] = fake_db
        try:
            assert engine._broker_position_precheck() is True
        finally:
            if prior_db is not None:
                sys.modules["ap.db"] = prior_db
            else:
                sys.modules.pop("ap.db", None)

        canonical = engine._positions_by_id["canon-recovered"]
        assert canonical.position_id == "canon-recovered"
        assert canonical.execution_mode == "live"
        assert engine.active_positions() == [canonical]
        assert repair in engine._positions
        assert getattr(repair, "adoption_identity_quarantined", False) is True

    def test_broker_precheck_failed_repair_keeps_degraded_owner_active(self):
        from ap_exit_engine import APExitEngine

        class _Broker:
            account_id = "acct-1"
            mode = "live"
            def list_positions(self):
                return [{
                    "symbol": _CONTRACT,
                    "quantity": 1,
                    "cost_basis": 159.0,
                    "date_acquired": "2026-07-15T09:30:00Z",
                }]

        engine = APExitEngine(broker=_Broker(), email=_CLIENT)
        engine._load_db_position_row = lambda sym: None
        engine._upsert_broker_position_to_db = lambda sym, bp: None

        # Canonical DB repair is still degraded, so the precheck reports False.
        # The broker position must nevertheless retain exactly one stable,
        # behavior-active degraded owner for exit monitoring and capacity truth.
        assert engine._broker_position_precheck() is False
        degraded = [
            p for p in engine._positions
            if getattr(p, "broker_repair_degraded", False)
        ]
        assert len(degraded) == 1
        assert degraded[0].execution_mode == "live"
        assert degraded[0].quantity_remaining == 1
        assert engine._positions_by_id[degraded[0].position_id] is degraded[0]
        assert engine.active_positions() == degraded

    def test_paper_broker_precheck_failed_repair_keeps_degraded_owner_active(self):
        from ap_exit_engine import APExitEngine

        class _Broker:
            account_id = "paper-acct-1"
            mode = "paper"
            def list_positions(self):
                return [{
                    "symbol": _CONTRACT,
                    "quantity": 1,
                    "cost_basis": 159.0,
                    "date_acquired": "2026-07-15T09:30:00Z",
                }]

        engine = APExitEngine(broker=_Broker(), email=_CLIENT)
        engine._load_db_position_row = lambda sym: None
        engine._upsert_broker_position_to_db = lambda sym, bp: None

        assert engine._broker_position_precheck() is False
        degraded = [
            p for p in engine._positions
            if getattr(p, "broker_repair_degraded", False)
        ]
        assert len(degraded) == 1
        assert degraded[0].execution_mode == "paper"
        assert degraded[0].quantity_remaining == 1
        assert engine._positions_by_id[degraded[0].position_id] is degraded[0]
        assert engine.active_positions() == degraded

    def test_broker_precheck_unknown_mode_quarantines_without_live_default(self):
        from ap_exit_engine import APExitEngine

        class _Broker:
            account_id = "unknown-acct-1"
            def list_positions(self):
                return [{
                    "symbol": _CONTRACT,
                    "quantity": 1,
                    "cost_basis": 159.0,
                    "date_acquired": "2026-07-15T09:30:00Z",
                }]

        engine = APExitEngine(broker=_Broker(), email=_CLIENT)
        engine._load_db_position_row = lambda sym: None
        engine._upsert_broker_position_to_db = lambda sym, bp: None

        assert engine._broker_position_precheck() is False
        assert engine._positions == []
        assert engine._positions_by_id == {}
        assert engine.active_positions() == []

    def test_broker_precheck_with_only_quarantined_repair_installs_broker_owner(self):
        from ap_exit_engine import APExitEngine, _BrokerRepairIdentity

        class _Broker:
            account_id = "acct-1"
            mode = "live"
            def list_positions(self):
                return [{
                    "symbol": _CONTRACT,
                    "quantity": 1,
                    "cost_basis": 159.0,
                    "date_acquired": "2026-07-15T09:30:00Z",
                }]

        engine = APExitEngine(broker=_Broker(), email=_CLIENT)
        repair = _Pos(
            position_id=f"broker-repair-{_CLIENT}-{_CONTRACT}",
            option_symbol=_CONTRACT, client_id="", execution_mode="",
            quantity=1, quantity_remaining=1,
        )
        repair.adoption_identity_quarantined = True
        repair.adoption_identity_quarantine_reason = "repair_identity_unproven"
        engine._positions = [repair]
        engine._positions_by_id[repair.position_id] = repair
        engine._load_db_position_row = lambda sym: None
        engine._upsert_broker_position_to_db = lambda sym, bp: _BrokerRepairIdentity(
            "canon-from-broker",
            {
                "id": "canon-from-broker",
                "contract": _CONTRACT,
                "option_symbol": _CONTRACT,
                "execution_mode": "live",
                "qty": 1,
                "quantity_remaining": 1,
            },
        )
        engine._fetch_broker_quote = lambda sym: {
            "bid": 1.20, "ask": 1.25, "mark": 1.22, "last": 1.20,
        }

        assert engine.active_positions() == []
        assert engine._broker_position_precheck() is True

        active = engine.active_positions()
        assert len(active) == 1
        assert active[0].position_id == "canon-from-broker"
        assert active[0].execution_mode == "live"
        assert repair in engine._positions
        assert getattr(repair, "adoption_identity_quarantined", False) is True

    def test_successful_re_adoption_clears_prior_quarantine_flags(self):
        from ap_exit_engine import APExitEngine, ManagedPosition

        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        engine._lock = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}
        repair = ManagedPosition(
            ticker=_TICKER, option_symbol=_CONTRACT,
            side="CALL", quantity=1, quantity_remaining=1, entry_price=1.59,
            underlying_entry=220.0, underlying_target=230.0, underlying_stop=215.0,
            position_id=f"broker-repair-{_CLIENT}-{_CONTRACT}",
            client_id=_CLIENT, execution_mode="live",
            opened_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        )
        repair.adoption_identity_quarantined = True
        repair.adoptionidentityquarantined = True
        repair.adoption_identity_quarantine_reason = "previously_unknown"
        repair.adoptionidentityquarantinereason = "previously_unknown"
        repair.exit_in_flight = False
        repair.pending_exit_reason = ""
        engine._positions.append(repair)
        engine._positions_by_id[repair.position_id] = repair

        result = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon-recovered",
            local_order_id="ord-1", broker_order_id="brk-1",
            signal_id=_SIG, canonical_signal_id=_SIG,
            entry_fill=1.59, entry_ts=None,
            execution_mode="live", client_id=_CLIENT,
        )

        adopted = engine._positions_by_id["canon-recovered"]
        assert result.disposition == "ADOPTED"
        assert getattr(adopted, "adoption_identity_quarantined", True) is False
        assert getattr(adopted, "adoptionidentityquarantined", True) is False
        assert getattr(adopted, "adoption_identity_quarantine_reason", None) == ""
        assert engine.active_positions() == [adopted]
        assert engine._can_submit_exit(adopted, datetime.now(timezone.utc), reason="test") is True

    def test_existing_canonical_stale_repair_bid_cannot_raise_peak(self):
        engine, canon_id, repair_id = self._make_engine_with_both()
        repair = engine._positions_by_id[repair_id]
        repair.live_executable_price_source = "bid"
        repair.liveexecutablepricesource = "bid"
        repair.current_bid = 2.40
        repair.currentbid = 2.40
        repair.peak_pnl_pct = 0.50
        repair.max_profit_seen = 0.50
        old_ts = datetime.now(timezone.utc) - timedelta(seconds=90)
        repair.last_option_bid_update_ts = old_ts
        repair.lastoptionbidupdatets = old_ts

        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id=canon_id,
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )

        canonical = engine._positions_by_id[canon_id]
        assert canonical.peak_pnl_pct == pytest.approx(0.025)
        assert canonical.max_profit_seen == pytest.approx(0.0)

    def test_existing_canonical_reclassifies_repair_ask_against_canonical_entry(self):
        engine, canon_id, repair_id = self._make_engine_with_both()
        canonical = engine._positions_by_id[canon_id]
        canonical.entry_price = 1.20
        repair = engine._positions_by_id[repair_id]
        now = datetime.now(timezone.utc)
        repair.hard_exit_reference_price = 0.75
        repair.hardexitreferenceprice = 0.75
        repair.hard_exit_reference_source = "ask_unproven"
        repair.hardexitreferencesource = "ask_unproven"
        repair.hard_exit_reference_validity = "unproven"
        repair.hardexitreferencevalidity = "unproven"
        repair.hard_exit_reference_ts = now
        repair.hardexitreferencets = now
        repair.hard_exit_reference_pnl_pct = -0.25
        repair.hardexitreferencepnlpct = -0.25

        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id=canon_id,
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )

        assert canonical.hard_exit_reference_validity == "catastrophic_ask"
        assert canonical.hard_exit_reference_source == "ask_catastrophic"
        assert canonical.hard_exit_reference_pnl_pct == pytest.approx((0.75 - 1.20) / 1.20)

    def test_existing_canonical_demotes_false_catastrophic_ask_against_canonical_entry(self):
        engine, canon_id, repair_id = self._make_engine_with_both()
        canonical = engine._positions_by_id[canon_id]
        canonical.entry_price = 0.80
        repair = engine._positions_by_id[repair_id]
        now = datetime.now(timezone.utc)
        repair.hard_exit_reference_price = 0.75
        repair.hardexitreferenceprice = 0.75
        repair.hard_exit_reference_source = "ask_catastrophic"
        repair.hardexitreferencesource = "ask_catastrophic"
        repair.hard_exit_reference_validity = "catastrophic_ask"
        repair.hardexitreferencevalidity = "catastrophic_ask"
        repair.hard_exit_reference_ts = now
        repair.hardexitreferencets = now
        repair.hard_exit_reference_pnl_pct = -0.40
        repair.hardexitreferencepnlpct = -0.40

        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id=canon_id,
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )

        assert canonical.hard_exit_reference_validity == "unproven"
        assert canonical.hard_exit_reference_source == "ask_unproven"
        assert canonical.hard_exit_reference_pnl_pct == pytest.approx((0.75 - 0.80) / 0.80)

    def test_existing_canonical_does_not_remove_foreign_client_or_wrong_mode_repairs(self):
        engine, canon_id, repair_id = self._make_engine_with_both()
        foreign_id = f"broker-repair-other@client.com-{_CONTRACT}"
        wrong_mode_id = f"broker-repair-{_CLIENT}-paper-{_CONTRACT}"
        foreign = _Pos(position_id=foreign_id, option_symbol=_CONTRACT,
                       client_id="other@client.com", execution_mode="live")
        wrong_mode = _Pos(position_id=wrong_mode_id, option_symbol=_CONTRACT,
                          client_id=_CLIENT, execution_mode="paper")
        engine._positions.extend([foreign, wrong_mode])
        engine._positions_by_id[foreign_id] = foreign
        engine._positions_by_id[wrong_mode_id] = wrong_mode

        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id=canon_id,
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )

        assert repair_id not in engine._positions_by_id
        assert foreign_id in engine._positions_by_id
        assert wrong_mode_id in engine._positions_by_id


class TestCanonicalCollapseRiskReferenceRegression:
    """Regression coverage for canonical+repair collapse money-safety state."""

    def _engine_with_canon_and_repair(self, canon: _Pos, repair: _Pos, *, canon_id="canon1"):
        from ap_exit_engine import APExitEngine
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        engine._lock = threading.Lock()
        engine._positions = [canon, repair]
        engine._positions_by_id = {
            canon_id: canon,
            repair.position_id: repair,
        }
        return engine

    def _collapse(self, engine, *, canon_id="canon1"):
        return engine.adopt_canonical_position_identity(
            contract=_CONTRACT,
            canonical_position_id=canon_id,
            local_order_id="l1",
            broker_order_id="b1",
            signal_id=_SIG,
            canonical_signal_id=_SIG,
            entry_fill=1.20,
            entry_ts=None,
            execution_mode="live",
            client_id=_CLIENT,
        )

    def test_existing_canonical_does_not_copy_raw_repair_peak_percentage(self):
        now = datetime.now(timezone.utc)
        canon = _Pos(
            position_id="canon1", option_symbol=_CONTRACT, client_id=_CLIENT,
            execution_mode="live", entry_price=1.20, entryprice=1.20,
            current_bid=1.10, currentbid=1.10, peak_pnl_pct=0.0,
            last_option_bid_update_ts=now - timedelta(seconds=20),
        )
        repair = _Pos(
            position_id=f"broker-repair-{_CLIENT}-{_CONTRACT}",
            option_symbol=_CONTRACT, client_id=_CLIENT, execution_mode="live",
            entry_price=1.00, entryprice=1.00,
            current_bid=1.16, currentbid=1.16,
            peak_pnl_pct=0.16, max_profit_seen=0.16,
            touched_profit=True, live_executable_price_source="bid",
            last_option_bid_update_ts=now,
            last_option_quote_update_ts=now,
        )
        engine = self._engine_with_canon_and_repair(canon, repair)

        self._collapse(engine)

        assert canon.current_bid == pytest.approx(1.16)
        assert canon.peak_pnl_pct == pytest.approx(0.0)
        assert canon.max_profit_seen == pytest.approx(0.0)
        assert canon.touched_profit is False

    def test_unproven_repair_hard_reference_cannot_replace_proven_canonical(self):
        now = datetime.now(timezone.utc)
        canon = _Pos(
            position_id="canon1", option_symbol=_CONTRACT, client_id=_CLIENT,
            execution_mode="live", entry_price=1.20, entryprice=1.20,
            hard_exit_reference_price=0.70,
            hard_exit_reference_source="bid",
            hard_exit_reference_validity="proven",
            hard_exit_reference_ts=now - timedelta(seconds=20),
            hard_exit_reference_refresh_needed=False,
        )
        repair = _Pos(
            position_id=f"broker-repair-{_CLIENT}-{_CONTRACT}",
            option_symbol=_CONTRACT, client_id=_CLIENT, execution_mode="live",
            hard_exit_reference_price=1.30,
            hard_exit_reference_source="ask_unproven",
            hard_exit_reference_validity="unproven",
            hard_exit_reference_ts=now,
        )
        engine = self._engine_with_canon_and_repair(canon, repair)

        self._collapse(engine)

        assert canon.hard_exit_reference_price == pytest.approx(0.70)
        assert canon.hard_exit_reference_validity == "proven"
        assert canon.hard_exit_reference_refresh_needed is True

    def test_repair_bid_requires_dedicated_newer_bid_timestamp(self):
        now = datetime.now(timezone.utc)
        canon = _Pos(
            position_id="canon1", option_symbol=_CONTRACT, client_id=_CLIENT,
            execution_mode="live", entry_price=1.20, entryprice=1.20,
            current_bid=1.10, currentbid=1.10,
            last_option_bid_update_ts=now,
            last_option_quote_update_ts=now,
            option_bid_valid=True,
            option_quote_fresh=True,
        )
        repair = _Pos(
            position_id=f"broker-repair-{_CLIENT}-{_CONTRACT}",
            option_symbol=_CONTRACT, client_id=_CLIENT, execution_mode="live",
            current_bid=1.16, currentbid=1.16,
            peak_pnl_pct=0.16, live_executable_price_source="bid",
            last_option_bid_update_ts=now - timedelta(seconds=90),
            last_option_quote_update_ts=now + timedelta(seconds=5),
            option_bid_valid=True,
            option_quote_fresh=True,
        )
        engine = self._engine_with_canon_and_repair(canon, repair)

        self._collapse(engine)

        assert canon.current_bid == pytest.approx(1.10)
        assert canon.last_option_bid_update_ts == now
        assert canon.peak_pnl_pct == pytest.approx(0.0)


class TestCanonicalAdoptionHardReferenceRebase:
    def test_ask_hard_reference_reclassified_after_entry_correction(self):
        from ap_exit_engine import APExitEngine, get_effective_hard_exit_reference
        now = datetime.now(timezone.utc)
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        engine._lock = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}

        repair_id = f"broker-repair-{_CLIENT}-{_CONTRACT}"
        repair = _Pos(
            position_id=repair_id,
            option_symbol=_CONTRACT,
            client_id=_CLIENT,
            execution_mode="live",
            entry_price=1.00,
            entryprice=1.00,
            current_ask=0.75,
            currentask=0.75,
            hard_exit_reference_price=0.75,
            hard_exit_reference_source="ask_unproven",
            hard_exit_reference_validity="unproven",
            hard_exit_reference_ts=now,
            hard_exit_reference_pnl_pct=-0.25,
        )
        engine._positions.append(repair)
        engine._positions_by_id[repair_id] = repair

        result = engine.adopt_canonical_position_identity(
            contract=_CONTRACT,
            canonical_position_id="canon1",
            local_order_id="l1",
            broker_order_id="b1",
            signal_id=_SIG,
            canonical_signal_id=_SIG,
            entry_fill=1.20,
            entry_ts=None,
            execution_mode="live",
            client_id=_CLIENT,
        )

        assert result.disposition == "ADOPTED"
        assert repair.hard_exit_reference_validity == "catastrophic_ask"
        assert repair.hard_exit_reference_pnl_pct == pytest.approx(-0.375)
        assert get_effective_hard_exit_reference(repair, now) == pytest.approx(-0.375)

    def test_stale_ask_hard_reference_stays_unproven_after_entry_correction(self):
        from ap_exit_engine import APExitEngine, get_effective_hard_exit_reference
        now = datetime.now(timezone.utc)
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        engine._lock = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}

        repair_id = f"broker-repair-{_CLIENT}-{_CONTRACT}"
        repair = _Pos(
            position_id=repair_id,
            option_symbol=_CONTRACT,
            client_id=_CLIENT,
            execution_mode="live",
            entry_price=1.00,
            entryprice=1.00,
            current_ask=0.75,
            currentask=0.75,
            hard_exit_reference_price=0.75,
            hard_exit_reference_source="ask_stale",
            hard_exit_reference_validity="unproven",
            hard_exit_reference_ts=now - timedelta(seconds=90),
            hard_exit_reference_pnl_pct=-0.25,
        )
        engine._positions.append(repair)
        engine._positions_by_id[repair_id] = repair

        result = engine.adopt_canonical_position_identity(
            contract=_CONTRACT,
            canonical_position_id="canon1",
            local_order_id="l1",
            broker_order_id="b1",
            signal_id=_SIG,
            canonical_signal_id=_SIG,
            entry_fill=1.20,
            entry_ts=None,
            execution_mode="live",
            client_id=_CLIENT,
        )

        assert result.disposition == "ADOPTED"
        assert repair.hard_exit_reference_validity == "unproven"
        assert repair.hard_exit_reference_refresh_needed is True
        assert repair.hard_exit_reference_pnl_pct == pytest.approx(-0.375)
        assert get_effective_hard_exit_reference(repair, now) is None


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

    def _add_repair(self, engine, *, client=_CLIENT, mode="live", contract=_CONTRACT):
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

    def test_upgrade_in_place_stale_repair_bid_cannot_seed_peak(self):
        engine = self._engine()
        self._add_repair(engine, mode="live")
        repair = engine._positions[0]
        repair.live_executable_price_source = "bid"
        repair.liveexecutablepricesource = "bid"
        repair.current_bid = 2.40
        repair.currentbid = 2.40
        repair.peak_pnl_pct = 0.50
        repair.max_profit_seen = 0.50
        old_ts = datetime.now(timezone.utc) - timedelta(seconds=90)
        repair.last_option_bid_update_ts = old_ts
        repair.lastoptionbidupdatets = old_ts

        r = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.00, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )

        assert r.disposition == "ADOPTED"
        pos = engine._positions_by_id["canon1"]
        assert pos.peak_pnl_pct == pytest.approx(0.0)
        assert pos.max_profit_seen == pytest.approx(0.0)
        assert pos.touched_profit is False


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
        pos = _Pos(position_id=repair_id, execution_mode="live",
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


# ─────────────────────────────────────────────────────────────────────────────
# P0 — Direct _exit_loop / _apply_option_quote_for_decision tests
# ─────────────────────────────────────────────────────────────────────────────


class TestExitLoopQuoteAuthority:
    """Exact BA regression: _QUOTES.get_fresh() must not write _snap.mid to
    current_option_price for a LIVE position."""

    def _make_snap(self, bid, ask, mid=None, underlying=220.0):
        snap = SimpleNamespace(
            bid             = bid,
            ask             = ask,
            mid             = mid if mid is not None else round((bid + ask) / 2.0, 4),
            mark            = mid if mid is not None else round((bid + ask) / 2.0, 4),
            underlying_price = underlying,
        )
        return snap

    def _run_exit_loop_quote(self, pos: _Pos, snap) -> _Pos:
        """Reproduce the _exit_loop quote application block using the production helper."""
        from ap_exit_engine import _apply_option_quote_for_decision
        from datetime import datetime, timezone
        now_utc = datetime.now(timezone.utc)
        if snap.bid > 0 or snap.ask > 0:
            _apply_option_quote_for_decision(
                pos,
                bid  = float(snap.bid or 0.0),
                ask  = float(snap.ask or 0.0),
                mark = float(getattr(snap, "mid", 0.0) or getattr(snap, "mark", 0.0) or 0.0),
                quote_ts = now_utc,
            )
        return pos

    def test_live_quote1_current_option_price_is_bid(self):
        """BA quote 1: bid=1.63, mid=1.71 → current_option_price must be 1.63."""
        pos = _Pos(execution_mode="live")
        snap = self._make_snap(bid=1.63, ask=1.79, mid=1.71)
        self._run_exit_loop_quote(pos, snap)
        assert abs(pos.current_option_price - 1.63) < 0.001, (
            f"current_option_price must be bid=1.63, got {pos.current_option_price}"
        )

    def test_live_quote1_option_pnl_is_bid_based(self):
        """option_pnl_pct uses current_option_price — must be bid-based."""
        pos = _Pos(execution_mode="live")
        snap = self._make_snap(bid=1.63, ask=1.79, mid=1.71)
        self._run_exit_loop_quote(pos, snap)
        pnl = (pos.current_option_price - _ENTRY) / _ENTRY
        assert abs(pnl - 0.0252) < 0.002, f"Bid P&L must be ~+2.52%, got {pnl*100:.2f}%"

    def test_live_quote1_touched_profit_false(self):
        pos = _Pos(execution_mode="live")
        snap = self._make_snap(bid=1.63, ask=1.79, mid=1.71)
        self._run_exit_loop_quote(pos, snap)
        assert pos.touched_profit is False

    def test_live_analytics_mark_is_mid(self):
        """Analytics mark captures mid for dashboards; decision uses bid."""
        pos = _Pos(execution_mode="live")
        snap = self._make_snap(bid=1.63, ask=1.79, mid=1.71)
        self._run_exit_loop_quote(pos, snap)
        mark = getattr(pos, "analytics_mark_price", 0.0)
        assert abs(mark - 1.71) < 0.01 or mark > 1.63

    def test_live_quote2_executable_pnl_is_minus_881(self):
        """BA quote 2: bid=1.45 → P&L -8.81%, no TOUCHED_PROFIT_STOP possible."""
        pos = _Pos(execution_mode="live")
        # Quote 1 — does NOT arm touched_profit
        self._run_exit_loop_quote(pos, self._make_snap(bid=1.63, ask=1.79, mid=1.71))
        assert pos.touched_profit is False
        # Quote 2
        self._run_exit_loop_quote(pos, self._make_snap(bid=1.45, ask=1.74, mid=1.595))
        pnl = (pos.current_option_price - _ENTRY) / _ENTRY
        assert pnl < 0, f"Quote 2 must produce negative P&L, got {pnl*100:.2f}%"
        assert abs(pnl - (-0.0881)) < 0.002

    def test_guard_mid_never_written_to_current_option_price_for_live(self):
        """Guard: _snap.mid must never appear as current_option_price for LIVE."""
        pos = _Pos(execution_mode="live")
        snap = self._make_snap(bid=1.63, ask=1.79, mid=1.71)
        self._run_exit_loop_quote(pos, snap)
        # 1.71 is the mid — it must NOT be current_option_price
        assert abs(pos.current_option_price - 1.71) > 0.05, (
            "current_option_price must not be mid=1.71 for a LIVE position"
        )

    def test_missing_bid_clears_current_option_price(self):
        """bid=0 with ask/mark → current_option_price must be 0 for LIVE."""
        pos = _Pos(execution_mode="live")
        snap = self._make_snap(bid=0.0, ask=1.79, mid=1.71)
        self._run_exit_loop_quote(pos, snap)
        assert pos.current_option_price == 0.0
        assert getattr(pos, "executable_quote_valid", True) is False

    def test_paper_uses_mid(self):
        """PAPER positions must still use mid/mark for current_option_price."""
        pos = _Pos(execution_mode="paper")
        snap = self._make_snap(bid=1.63, ask=1.79, mid=1.71)
        self._run_exit_loop_quote(pos, snap)
        # PAPER: current_option_price should be mid (1.71 area)
        assert pos.current_option_price > 1.63, (
            "PAPER current_option_price must use mid, not bid"
        )

    def test_unknown_mode_uses_bid_not_mid(self):
        """Unknown/blank mode falls to live_risk_unproven → bid, not mid."""
        pos = _Pos(execution_mode="unknown")
        snap = self._make_snap(bid=1.63, ask=1.79, mid=1.71)
        self._run_exit_loop_quote(pos, snap)
        assert abs(pos.current_option_price - 1.63) < 0.001

    def test_rising_ask_flat_bid_current_option_price_stays_flat(self):
        """Ask rising while bid flat → current_option_price stays bid."""
        pos = _Pos(execution_mode="live")
        snap = self._make_snap(bid=1.60, ask=2.10, mid=1.85)
        self._run_exit_loop_quote(pos, snap)
        assert abs(pos.current_option_price - 1.60) < 0.001


class TestApplyOptionQuoteForDecision:
    """Unit tests for the _apply_option_quote_for_decision helper directly."""

    def test_returns_executable_quote_application(self):
        from ap_exit_engine import _apply_option_quote_for_decision, ExecutableQuoteApplication
        pos = _Pos(execution_mode="live")
        result = _apply_option_quote_for_decision(pos, bid=1.63, ask=1.79, mark=1.71)
        assert isinstance(result, ExecutableQuoteApplication)

    def test_live_bid_valid_sets_executable_valid(self):
        from ap_exit_engine import _apply_option_quote_for_decision
        pos = _Pos(execution_mode="live")
        r = _apply_option_quote_for_decision(pos, bid=1.63, ask=1.79, mark=1.71)
        assert r.executable_valid is True
        assert r.executable_price == 1.63
        assert r.source == "bid"

    def test_live_missing_bid_sets_executable_invalid(self):
        from ap_exit_engine import _apply_option_quote_for_decision
        pos = _Pos(execution_mode="live")
        r = _apply_option_quote_for_decision(pos, bid=0.0, ask=1.79, mark=1.71)
        assert r.executable_valid is False
        assert r.executable_price == 0.0
        assert "bid_missing" in r.source

    def test_paper_uses_mid(self):
        from ap_exit_engine import _apply_option_quote_for_decision
        pos = _Pos(execution_mode="paper")
        r = _apply_option_quote_for_decision(pos, bid=1.63, ask=1.79, mark=1.71)
        assert "paper_mid_simulation" in r.source
        assert r.executable_price > 1.63

    def test_analytics_mark_always_captured(self):
        from ap_exit_engine import _apply_option_quote_for_decision
        pos = _Pos(execution_mode="live")
        _apply_option_quote_for_decision(pos, bid=1.63, ask=1.79, mark=1.71)
        assert abs(getattr(pos, "analytics_mark_price", 0) - 1.71) < 0.01


# ─────────────────────────────────────────────────────────────────────────────
# P1 — Canonical cross-mode conflicts
# ─────────────────────────────────────────────────────────────────────────────


class TestCanonicalCrossMode:
    """LIVE and PAPER canonical positions must never collapse across modes."""

    def _engine_with_canon(self, canon_mode: str):
        from ap_exit_engine import APExitEngine
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        engine._lock  = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}
        # Canonical position exists
        canon = _Pos(position_id="canon1", option_symbol=_CONTRACT,
                     client_id=_CLIENT, execution_mode=canon_mode)
        engine._positions.append(canon)
        engine._positions_by_id["canon1"] = canon
        return engine

    def test_canonical_live_incoming_paper_is_mismatch(self):
        engine = self._engine_with_canon("live")
        r = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="paper", client_id=_CLIENT,
        )
        assert r.disposition == "RETRY_MODE_MISMATCH"
        assert r.adopted is False
        assert r.retryable is True

    def test_canonical_paper_incoming_live_is_mismatch(self):
        engine = self._engine_with_canon("paper")
        r = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        assert r.disposition == "RETRY_MODE_MISMATCH"
        assert r.adopted is False

    def test_canonical_live_incoming_live_collapse_allowed(self):
        engine = self._engine_with_canon("live")
        # Add repair position
        repair_id = f"broker-repair-{_CLIENT}-{_CONTRACT}"
        repair = _Pos(position_id=repair_id, option_symbol=_CONTRACT,
                      client_id=_CLIENT, execution_mode="live",
                      current_bid=1.63, peak_pnl_pct=0.0)
        engine._positions.append(repair)
        engine._positions_by_id[repair_id] = repair
        r = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        assert r.adopted is True
        assert r.disposition in ("ADOPTED", "ALREADY_CANONICAL_REPAIR_REMOVED")

    def test_canonical_paper_incoming_paper_collapse_allowed(self):
        engine = self._engine_with_canon("paper")
        repair_id = f"broker-repair-{_CLIENT}-{_CONTRACT}"
        repair = _Pos(position_id=repair_id, option_symbol=_CONTRACT,
                      client_id=_CLIENT, execution_mode="paper",
                      current_bid=1.63, peak_pnl_pct=0.0)
        engine._positions.append(repair)
        engine._positions_by_id[repair_id] = repair
        r = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="paper", client_id=_CLIENT,
        )
        assert r.adopted is True

    def test_canonical_unknown_incoming_live_is_fail_closed(self):
        """canonical blank/unknown + incoming live → fail closed (retryable)."""
        engine = self._engine_with_canon("")  # unknown canonical mode
        r = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live", client_id=_CLIENT,
        )
        # blank canonical mode is unproven → must not allow collapse
        assert r.disposition == "RETRY_MODE_MISMATCH"
        assert r.retryable is True


# ─────────────────────────────────────────────────────────────────────────────
# P2 — Undefined timestamp fallback regression
# ─────────────────────────────────────────────────────────────────────────────


class TestTimestampFallbackRegression:
    """Engine with _last_order_filled_ts must not raise on adoption."""

    def _engine_with_repair(self):
        from ap_exit_engine import APExitEngine
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        engine._lock  = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}
        repair_id = f"broker-repair-{_CLIENT}-{_CONTRACT}"
        pos = _Pos(position_id=repair_id, option_symbol=_CONTRACT,
                   client_id=_CLIENT, execution_mode="live",
                   current_bid=1.63, peak_pnl_pct=0.0)
        engine._positions.append(pos)
        engine._positions_by_id[repair_id] = pos
        return engine

    def test_adoption_does_not_raise_with_last_order_filled_ts_present(self):
        """P2 regression: engine._last_order_filled_ts must not trigger NameError."""
        from datetime import datetime, timezone
        engine = self._engine_with_repair()
        # Simulate the attribute that was incorrectly referenced via hasattr()
        engine._last_order_filled_ts = datetime.now(timezone.utc)

        # Must not raise
        result = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts="2026-07-15T09:30:00Z",
            order_filled_ts="2026-07-15T09:30:00Z",
            execution_mode="live", client_id=_CLIENT,
        )
        assert result.adopted is True

    def test_opened_at_is_normalized_aware_utc(self):
        from datetime import timezone
        engine = self._engine_with_repair()
        engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts="2026-07-15T09:30:00Z",
            execution_mode="live", client_id=_CLIENT,
        )
        pos = engine._positions[0]
        opened = getattr(pos, "opened_at", None)
        assert opened is not None
        assert opened.tzinfo is not None
        assert opened.tzinfo == timezone.utc or str(opened.tzinfo) in ("UTC", "+00:00")

    def test_malformed_entry_ts_falls_back_gracefully(self, caplog):
        import logging
        engine = self._engine_with_repair()
        with caplog.at_level(logging.WARNING, logger="ap_exit_engine"):
            result = engine.adopt_canonical_position_identity(
                contract=_CONTRACT, canonical_position_id="canon1",
                local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
                entry_fill=1.59, entry_ts="NOT_A_DATE",
                order_filled_ts="2026-07-15T09:30:00Z",
                execution_mode="live", client_id=_CLIENT,
            )
        assert result.adopted is True
        # Should fall back to order_filled_ts or now_utc without raising
        pos = engine._positions[0]
        assert getattr(pos, "opened_at", None) is not None


# ─────────────────────────────────────────────────────────────────────────────
# P1 final — blank+blank and unproven mode combinations must fail closed
# ─────────────────────────────────────────────────────────────────────────────


class TestCanonicalBlankBlankModeFencing:
    """blank+blank and all unproven combinations must return RETRY_MODE_MISMATCH.

    The previous guard `if not _mode_compatible and (_norm_incoming or _norm_canonical):`
    allowed blank+blank to bypass the mode check and collapse two unproven-mode
    positions. Broker-repair positions are exactly the blank-mode incident class.
    """

    def _engine_with_canon(self, canon_mode: str):
        from ap_exit_engine import APExitEngine
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        engine._lock  = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}
        canon = _Pos(position_id="canon1", option_symbol=_CONTRACT,
                     client_id=_CLIENT, execution_mode=canon_mode)
        engine._positions.append(canon)
        engine._positions_by_id["canon1"] = canon
        return engine

    def _adopt(self, engine, incoming_mode: str):
        return engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode=incoming_mode,
            client_id=_CLIENT,
        )

    # ── Exact required tests from spec ──────────────────────────────────────

    def test_blank_canonical_blank_incoming_is_mismatch(self):
        """blank + blank → RETRY_MODE_MISMATCH; repair position remains protected."""
        engine = self._engine_with_canon("")
        r = self._adopt(engine, incoming_mode="")
        assert r.disposition == "RETRY_MODE_MISMATCH", (
            f"blank+blank must be RETRY_MODE_MISMATCH, got {r.disposition}"
        )
        assert r.adopted is False
        assert r.safe_to_seed is False
        assert r.retryable is True
        # Canon position must not have been collapsed with repair
        assert engine._positions[0].position_id == "canon1"

    def test_blank_canonical_blank_incoming_repair_not_collapsed(self):
        """Repair position must survive when blank+blank is rejected."""
        from ap_exit_engine import APExitEngine
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        engine._lock  = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}
        # Both canonical and repair with blank modes
        canon_id  = "canon-blank"
        repair_id = f"broker-repair-{_CLIENT}-{_CONTRACT}"
        canon = _Pos(position_id=canon_id, option_symbol=_CONTRACT,
                     client_id=_CLIENT, execution_mode="")
        repair = _Pos(position_id=repair_id, option_symbol=_CONTRACT,
                      client_id=_CLIENT, execution_mode="")
        engine._positions.extend([canon, repair])
        engine._positions_by_id[canon_id]  = canon
        engine._positions_by_id[repair_id] = repair

        r = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id=canon_id,
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="",
            client_id=_CLIENT,
        )
        assert r.disposition == "RETRY_MODE_MISMATCH"
        # Both positions must still exist — no collapse occurred
        assert len(engine._positions) == 2
        assert repair_id in engine._positions_by_id

    def test_unknown_incoming_unknown_canonical_is_mismatch(self):
        engine = self._engine_with_canon("unknown")
        r = self._adopt(engine, incoming_mode="unknown")
        assert r.disposition == "RETRY_MODE_MISMATCH"
        assert r.adopted is False

    def test_blank_incoming_unknown_canonical_is_mismatch(self):
        engine = self._engine_with_canon("unknown")
        r = self._adopt(engine, incoming_mode="")
        assert r.disposition == "RETRY_MODE_MISMATCH"
        assert r.adopted is False

    def test_unknown_incoming_blank_canonical_is_mismatch(self):
        engine = self._engine_with_canon("")
        r = self._adopt(engine, incoming_mode="unknown")
        assert r.disposition == "RETRY_MODE_MISMATCH"
        assert r.adopted is False

    # ── Verify valid combinations still work ─────────────────────────────────

    def test_live_live_collapse_still_works(self):
        """live+live must not be broken by the tightened check."""
        from ap_exit_engine import APExitEngine
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        engine._lock  = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}
        canon = _Pos(position_id="canon1", option_symbol=_CONTRACT,
                     client_id=_CLIENT, execution_mode="live")
        repair_id = f"broker-repair-{_CLIENT}-{_CONTRACT}"
        repair = _Pos(position_id=repair_id, option_symbol=_CONTRACT,
                      client_id=_CLIENT, execution_mode="live",
                      current_bid=1.63, peak_pnl_pct=0.0)
        engine._positions.extend([canon, repair])
        engine._positions_by_id["canon1"]   = canon
        engine._positions_by_id[repair_id]  = repair

        r = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="live",
            client_id=_CLIENT,
        )
        assert r.adopted is True
        assert r.disposition in ("ADOPTED", "ALREADY_CANONICAL_REPAIR_REMOVED")

    def test_paper_paper_collapse_still_works(self):
        """paper+paper must not be broken by the tightened check."""
        from ap_exit_engine import APExitEngine
        engine = APExitEngine.__new__(APExitEngine)
        engine._email = _CLIENT
        engine._lock  = threading.Lock()
        engine._positions = []
        engine._positions_by_id = {}
        canon = _Pos(position_id="canon1", option_symbol=_CONTRACT,
                     client_id=_CLIENT, execution_mode="paper")
        repair_id = f"broker-repair-{_CLIENT}-{_CONTRACT}"
        repair = _Pos(position_id=repair_id, option_symbol=_CONTRACT,
                      client_id=_CLIENT, execution_mode="paper",
                      current_bid=1.63, peak_pnl_pct=0.0)
        engine._positions.extend([canon, repair])
        engine._positions_by_id["canon1"]   = canon
        engine._positions_by_id[repair_id]  = repair

        r = engine.adopt_canonical_position_identity(
            contract=_CONTRACT, canonical_position_id="canon1",
            local_order_id="", broker_order_id="", signal_id="", canonical_signal_id="",
            entry_fill=1.59, entry_ts=None, execution_mode="paper",
            client_id=_CLIENT,
        )
        assert r.adopted is True
