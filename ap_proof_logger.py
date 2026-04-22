# ap_proof_logger.py — Angel Precision Proof System Logger
# =============================================================================
# Records every trade and generates daily summaries.
# This is your raw truth layer — the foundation of everything you'll sell.
#
# PRODUCTION FIXES APPLIED:
#   1. Upsert key: date,client_email,system_version (not just date,client_email)
#   2. trades_executed incremented at position OPEN, not close
#   3. generate_daily_summary() builds from Supabase, not only _trades_today
#      (survives Render restarts cleanly)
#   4. Day bucketing uses ET timezone, not UTC string slice (no midnight boundary bugs)
#   5. generate_10day_proof() filters to last 10 distinct trading days
#   6. 10-day proof exposes median, avg hold time, worst trade, exit reason breakdown
#   7. _trades_today is cache only — Supabase is always source of truth
#
# FUNNEL COUNTER UPDATE:
#   Added watcher lifecycle fields: watcher_sent, watcher_expired,
#   watcher_invalidated, watcher_triggered, order_failed, rejected_score
# =============================================================================

from __future__ import annotations

import logging
import threading
from datetime import datetime, date, timezone, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

log = logging.getLogger("ap.proof_logger")
ET  = ZoneInfo("America/New_York")

SYSTEM_VERSION = "v2"   # bump this to reset client-facing performance history


# =============================================================================
# INTRADAY FUNNEL COUNTER
# =============================================================================

class FunnelCounter:
    """
    Tracks signal funnel stats throughout the trading day.
    Reset automatically at start of each new ET trading day.
    Shared across all modules — increment when gates fire.

    Import and use anywhere:
        from ap_proof_logger import funnel
        funnel.inc("signals_received")
        funnel.inc("context_blocked")
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._day  = None
        self.reset()

    def reset(self):
        with self._lock:
            self._day                  = datetime.now(ET).date()
            # ── intake ────────────────────────────────────────────────
            self.signals_received      = 0
            # ── score gate ────────────────────────────────────────────
            self.rejected_score        = 0
            self.passed_score          = 0
            # ── context gate ──────────────────────────────────────────
            self.context_blocked       = 0
            self.passed_context        = 0
            # ── tier classify ─────────────────────────────────────────
            self.shadow_tracked        = 0
            # ── ranking queue ─────────────────────────────────────────
            self.sector_capped         = 0
            self.queue_expired         = 0
            # ── watcher ───────────────────────────────────────────────
            self.watcher_sent          = 0
            self.watcher_expired       = 0
            self.watcher_invalidated   = 0
            self.watcher_triggered     = 0
            # ── options gate ──────────────────────────────────────────
            self.options_rejected      = 0
            # ── execution ─────────────────────────────────────────────
            self.order_failed          = 0
            self.trades_executed       = 0   # incremented at position OPEN
            # ── market context ────────────────────────────────────────
            self.regime                = "unknown"
            self.was_trend_day         = False

    def _check_day_reset(self):
        """Auto-reset if ET date has rolled over."""
        today = datetime.now(ET).date()
        if self._day and today != self._day:
            log.info(f"[FUNNEL] New trading day {today} — auto-resetting funnel counter")
            self.reset()

    def inc(self, field: str, by: int = 1):
        self._check_day_reset()
        with self._lock:
            current = getattr(self, field, 0)
            setattr(self, field, current + by)

    def set_regime(self, regime: str, trend_day: bool = False):
        with self._lock:
            self.regime        = regime
            self.was_trend_day = trend_day

    def snapshot(self) -> dict:
        self._check_day_reset()
        with self._lock:
            return {
                "signals_received":      self.signals_received,
                "rejected_score":        self.rejected_score,
                "passed_score_filter":   self.passed_score,
                "context_blocked":       self.context_blocked,
                "passed_context_filter": self.passed_context,
                "shadow_tracked":        self.shadow_tracked,
                "sector_capped":         self.sector_capped,
                "queue_expired":         self.queue_expired,
                "watcher_sent":          self.watcher_sent,
                "watcher_expired":       self.watcher_expired,
                "watcher_invalidated":   self.watcher_invalidated,
                "watcher_triggered":     self.watcher_triggered,
                "options_rejected":      self.options_rejected,
                "order_failed":          self.order_failed,
                "trades_executed":       self.trades_executed,
                "regime":                self.regime,
                "was_trend_day":         self.was_trend_day,
            }


# Global funnel counter — import and increment anywhere in the pipeline
funnel = FunnelCounter()


# =============================================================================
# HELPERS
# =============================================================================

def _to_et_date(ts_iso: str) -> Optional[date]:
    """Convert UTC ISO timestamp string to ET date. Handles midnight boundary correctly."""
    try:
        dt_utc = datetime.fromisoformat(ts_iso.replace("Z", "+00:00"))
        return dt_utc.astimezone(ET).date()
    except Exception:
        return None

def _median(values: list) -> Optional[float]:
    if not values: return None
    s = sorted(values)
    n = len(s)
    return (s[n//2] if n % 2 == 1 else (s[n//2-1] + s[n//2]) / 2)

def _max_equity_drawdown(pnls: list) -> float:
    """True peak-to-trough on cumulative P&L sequence."""
    peak = running = max_dd = 0.0
    for p in pnls:
        running += p
        if running > peak: peak = running
        dd = peak - running
        if dd > max_dd: max_dd = dd
    return round(max_dd, 2)


# =============================================================================
# PROOF LOGGER
# =============================================================================

class APProofLogger:
    """
    Logs trade outcomes to Supabase and generates daily proof summaries.

    Every number on the client dashboard comes from Supabase — not local memory.
    _trades_today is a convenience cache only; generate_daily_summary() always
    rebuilds from Supabase so it survives Render restarts cleanly.

    Usage:
        proof = APProofLogger(supabase_client, client_email="user@email.com")

        # When a position OPENS:
        proof.log_position_opened(ticker, side, tier, score, contracts)

        # When a position CLOSES:
        proof.log_trade(ticker=..., ...)

        # At EOD (4:30 PM ET):
        proof.generate_daily_summary()
    """

    def __init__(
        self,
        supabase_client = None,
        client_email:  str = "",
        mode:          str = "paper",
    ):
        self.sb    = supabase_client
        self.email = client_email
        self.mode  = mode
        self._trades_cache: list[dict] = []   # cache only — not source of truth
        self._lock = threading.Lock()

    # ── Increment executed at OPEN ─────────────────────────────────────────

    def log_position_opened(
        self,
        ticker:          str,
        side:            str,
        tier:            str,
        score:           float,
        contracts:       int,
        synthetic_entry: bool = False,
    ):
        """
        Call this when a position is entered (order filled).
        This is when trades_executed should increment — not at close.
        synthetic_entry=True means the fill was simulated (paper continuity fallback),
        not broker-confirmed. Only broker-backed opens count as proof.
        """
        if not synthetic_entry:
            funnel.inc("trades_executed")
        log.info(
            f"[PROOF] OPENED {ticker} {side} {tier} score={score:.0f} qty={contracts}"
            f" {'[SYNTHETIC]' if synthetic_entry else '[BROKER]'}"
        )

    # ── TRADE LOGGER (called at close) ────────────────────────────────────────

    def log_trade(
        self,
        ticker:              str,
        pattern:             str,
        side:                str,
        timeframe:           str,
        score:               float,
        tier:                str,
        context_score:       float,
        setup_status:        str,
        entry_trigger:       float,
        entry_option_price:  float,
        exit_option_price:   float,
        underlying_entry:    float,
        underlying_exit:     float,
        contracts:           int,
        exit_reason:         str,
        option_pnl_pct:      float,
        underlying_pnl_pct:  float,
        win:                 bool,
        spread_pct:          float   = 0.0,
        chain_grade:         str     = "",
        opened_at:           Optional[datetime] = None,
        closed_at:           Optional[datetime] = None,
        synthetic_entry:     bool = False,
    ) -> dict:
        now = datetime.now(timezone.utc)
        row = {
            "client_email":       self.email,
            "mode":               self.mode,
            "system_version":     SYSTEM_VERSION,
            "opened_at":          (opened_at or now).isoformat(),
            "closed_at":          (closed_at or now).isoformat(),
            "ticker":             ticker,
            "pattern":            pattern,
            "side":               side,
            "timeframe":          timeframe,
            "score":              round(score, 1),
            "tier":               tier,
            "context_score":      round(context_score, 1),
            "setup_status":       setup_status,
            "entry_trigger":      round(entry_trigger, 4),
            "entry_option_price": round(entry_option_price, 4),
            "exit_option_price":  round(exit_option_price, 4),
            "underlying_entry":   round(underlying_entry, 4),
            "underlying_exit":    round(underlying_exit, 4),
            "contracts":          contracts,
            "exit_reason":        exit_reason,
            "option_pnl_pct":     round(option_pnl_pct, 2),
            "underlying_pnl_pct": round(underlying_pnl_pct, 3),
            "win":                win,
            "spread_pct":         round(spread_pct, 4),
            "chain_grade":        chain_grade,
            "synthetic_entry":    bool(synthetic_entry),
        }

        # Cache for convenience — not source of truth
        with self._lock:
            self._trades_cache.append(row)

        log.info(
            f"[PROOF] CLOSED {ticker} {side} {tier} | "
            f"P&L={option_pnl_pct:+.1f}% | {'WIN' if win else 'LOSS'} | "
            f"{exit_reason} | score={score:.0f} ctx={context_score:.0f}"
        )

        # Write to Supabase immediately — this is the source of truth
        if self.sb:
            try:
                self.sb.table("proof_trades").insert(row).execute()
                log.debug(f"[PROOF] {ticker} written to Supabase")
            except Exception as e:
                log.error(f"[PROOF] Supabase write failed: {e}")

        return row

    # ── Daily summary rebuilt from Supabase ───────────────────────────────

    def generate_daily_summary(self, trade_date: Optional[date] = None) -> dict:
        """
        Generate and store daily proof summary.
        Builds from Supabase for that ET trading day — survives restarts cleanly.
        Call from /eod endpoint or scheduler at 4:30 PM ET.
        """
        today    = trade_date or datetime.now(ET).date()
        snapshot = funnel.snapshot()

        trades = self._fetch_trades_for_date(today)

        if not trades:
            log.info(f"[PROOF] No trades found in Supabase for {today} — generating zero summary")

        broker_trades = [t for t in trades if not t.get("synthetic_entry")]
        synth_trades  = [t for t in trades if t.get("synthetic_entry")]
        wins   = [t for t in broker_trades if t.get("win")]
        losses = [t for t in broker_trades if not t.get("win")]
        a_plus = [t for t in trades if t.get("tier") == "A+"]
        a_tier = [t for t in trades if t.get("tier") == "A"]
        b_tier = [t for t in trades if t.get("tier") == "B"]

        pnls      = [float(t.get("option_pnl_pct", 0)) for t in broker_trades]
        win_pnls  = [float(t.get("option_pnl_pct", 0)) for t in wins]
        loss_pnls = [float(t.get("option_pnl_pct", 0)) for t in losses]
        win_rate  = round(len(wins) / len(broker_trades) * 100, 1) if broker_trades else 0

        summary = {
            "date":                  str(today),
            "client_email":          self.email,
            "mode":                  self.mode,
            "system_version":        SYSTEM_VERSION,

            # Funnel
            "signals_received":      snapshot["signals_received"],
            "passed_score_filter":   snapshot["passed_score_filter"],
            "passed_context_filter": snapshot["passed_context_filter"],
            "context_blocked":       snapshot["context_blocked"],
            "options_rejected":      snapshot["options_rejected"],
            "queue_expired":         snapshot["queue_expired"],
            "sector_capped":         snapshot["sector_capped"],
            "shadow_tracked":        snapshot["shadow_tracked"],
            "trades_executed":       snapshot["trades_executed"],
            "synthetic_executed":     len(synth_trades),

            # Provenance split
            "broker_backed_trades":   len(broker_trades),
            "synthetic_trades":       len(synth_trades),
            # Results (broker-backed only)
            "wins":                  len(wins),
            "losses":                len(losses),
            "win_rate":              win_rate,
            "gross_pnl_pct":         round(sum(pnls), 2),
            "avg_win_pct":           round(sum(win_pnls) / len(win_pnls), 2) if win_pnls else 0,
            "avg_loss_pct":          round(sum(loss_pnls) / len(loss_pnls), 2) if loss_pnls else 0,
            "max_single_win_pct":    round(max(win_pnls), 2) if win_pnls else 0,
            "max_single_loss_pct":   round(min(loss_pnls), 2) if loss_pnls else 0,

            # Tier breakdown
            "a_plus_count":          len(a_plus),
            "a_plus_wins":           sum(1 for t in a_plus if t.get("win")),
            "a_plus_win_rate":       round(sum(1 for t in a_plus if t.get("win")) / len(a_plus) * 100, 1) if a_plus else None,
            "a_count":               len(a_tier),
            "a_wins":                sum(1 for t in a_tier if t.get("win")),
            "a_win_rate":            round(sum(1 for t in a_tier if t.get("win")) / len(a_tier) * 100, 1) if a_tier else None,
            "b_shadow_count":        len(b_tier),
            "b_shadow_wins":         sum(1 for t in b_tier if t.get("win")),

            # Market context
            "regime":                snapshot["regime"],
            "was_trend_day":         snapshot["was_trend_day"],
        }

        log.info(
            f"[PROOF] Daily summary {today} | "
            f"trades={len(trades)} wins={len(wins)} WR={win_rate}% "
            f"P&L={summary['gross_pnl_pct']:+.1f}% | A+:{len(a_plus)} A:{len(a_tier)} | "
            f"funnel: {snapshot['signals_received']} → "
            f"{snapshot['context_blocked']} blocked → {snapshot['trades_executed']} executed"
        )

        if self.sb:
            try:
                self.sb.table("proof_daily_summary") \
                    .upsert(summary, on_conflict="date,client_email,system_version") \
                    .execute()
                log.info("[PROOF] Daily summary written to Supabase")
            except Exception as e:
                log.error(f"[PROOF] Daily summary write failed: {e}")

        funnel.reset()

        return summary

    def _fetch_trades_for_date(self, trade_date: date) -> list[dict]:
        """Fetch all v2 trades for this client that closed on trade_date ET."""
        if not self.sb:
            with self._lock:
                return [t for t in self._trades_cache
                        if _to_et_date(t.get("closed_at", "")) == trade_date]

        try:
            day_start_et = datetime(trade_date.year, trade_date.month, trade_date.day,
                                    0, 0, 0, tzinfo=ET)
            day_end_et   = day_start_et + timedelta(days=1)
            utc_start    = day_start_et.astimezone(timezone.utc).isoformat()
            utc_end      = day_end_et.astimezone(timezone.utc).isoformat()

            res = self.sb.table("proof_trades") \
                .select("*") \
                .eq("client_email", self.email) \
                .eq("system_version", SYSTEM_VERSION) \
                .gte("closed_at", utc_start) \
                .lt("closed_at", utc_end) \
                .order("closed_at", desc=False) \
                .execute()
            return res.data or []
        except Exception as e:
            log.error(f"[PROOF] Supabase fetch for {trade_date} failed: {e}")
            with self._lock:
                return [t for t in self._trades_cache
                        if _to_et_date(t.get("closed_at", "")) == trade_date]

    # ── 10-day proof ──────────────────────────────────────────────────────

    def generate_10day_proof(self) -> dict:
        """Generate the client-facing 10-day proof summary."""
        if not self.sb:
            return {"status": "no_supabase"}

        try:
            days_res = self.sb.table("proof_daily_summary") \
                .select("date") \
                .eq("client_email", self.email) \
                .eq("system_version", SYSTEM_VERSION) \
                .eq("mode", self.mode) \
                .order("date", desc=True) \
                .limit(10) \
                .execute()
            days = [r["date"] for r in (days_res.data or [])]

            if not days:
                return {"status": "no_data"}

            days.sort()
            date_start = days[0]
            date_end   = days[-1]

            start_et = datetime.fromisoformat(date_start + "T00:00:00").replace(tzinfo=ET)
            end_et   = datetime.fromisoformat(date_end + "T23:59:59").replace(tzinfo=ET)

            res = self.sb.table("proof_trades") \
                .select("*") \
                .eq("client_email", self.email) \
                .eq("system_version", SYSTEM_VERSION) \
                .eq("mode", self.mode) \
                .gte("closed_at", start_et.astimezone(timezone.utc).isoformat()) \
                .lte("closed_at", end_et.astimezone(timezone.utc).isoformat()) \
                .order("closed_at", desc=False) \
                .execute()
            trades = res.data or []

        except Exception as e:
            log.error(f"[PROOF] 10-day fetch failed: {e}")
            return {"status": "error", "message": str(e)}

        if not trades:
            return {"status": "no_data"}

        wins   = [t for t in trades if t.get("win")]
        losses = [t for t in trades if not t.get("win")]
        pnls   = [float(t.get("option_pnl_pct", 0)) for t in trades]
        a_plus = [t for t in trades if t.get("tier") == "A+"]
        a_tier = [t for t in trades if t.get("tier") == "A"]

        median_ret = _median(pnls)

        hold_times = []
        for t in trades:
            hm = t.get("hold_minutes")
            if hm is not None:
                hold_times.append(float(hm))
            elif t.get("opened_at") and t.get("closed_at"):
                try:
                    o = datetime.fromisoformat(t["opened_at"].replace("Z", "+00:00"))
                    c = datetime.fromisoformat(t["closed_at"].replace("Z", "+00:00"))
                    hold_times.append((c - o).total_seconds() / 60)
                except Exception:
                    pass
        avg_hold_min = round(sum(hold_times) / len(hold_times), 1) if hold_times else None

        exit_counts = {"TARGET HIT": 0, "STOP HIT": 0, "THETA/TIME": 0, "PROFIT PROTECT": 0}
        for t in trades:
            r = (t.get("exit_reason") or "").upper()
            if "TARGET" in r:       exit_counts["TARGET HIT"] += 1
            elif "STOP" in r:       exit_counts["STOP HIT"] += 1
            elif "PROTECT" in r:    exit_counts["PROFIT PROTECT"] += 1
            else:                   exit_counts["THETA/TIME"] += 1

        max_dd      = _max_equity_drawdown(pnls)
        worst_trade = min(pnls) if pnls else 0

        sorted_trades = sorted(trades, key=lambda t: float(t.get("option_pnl_pct", 0)))
        top_3    = sorted_trades[-3:][::-1]
        bottom_3 = sorted_trades[:3]

        total = len(trades)
        return {
            "period":              f"{date_start} → {date_end}",
            "trading_days":        len(days),
            "total_trades":        total,
            "wins":                len(wins),
            "losses":              len(losses),
            "win_rate":            round(len(wins) / total * 100, 1) if total else 0,
            "avg_return":          round(sum(pnls) / total, 1) if total else 0,
            "median_return":       round(median_ret, 1) if median_ret is not None else None,
            "total_return":        round(sum(pnls), 1),
            "max_equity_drawdown": max_dd,
            "worst_single_trade":  round(worst_trade, 1),
            "avg_hold_minutes":    avg_hold_min,
            "exit_breakdown":      exit_counts,
            "exit_pct": {
                k: round(v / total * 100, 1) for k, v in exit_counts.items()
            } if total else {},
            "a_plus_count":        len(a_plus),
            "a_plus_win_rate":     round(sum(1 for t in a_plus if t.get("win")) / len(a_plus) * 100, 1) if a_plus else None,
            "a_count":             len(a_tier),
            "a_win_rate":          round(sum(1 for t in a_tier if t.get("win")) / len(a_tier) * 100, 1) if a_tier else None,
            "top_trades":          top_3,
            "worst_trades":        bottom_3,
            "mode":                self.mode,
            "system_version":      SYSTEM_VERSION,
            "statement": (
                "All trades were executed automatically by the Angel Precision execution engine "
                "under rule-based conditions. No manual intervention. Every entry required a "
                "minimum score of 85/100, live context confirmation, and a two-poll breach "
                "confirmation before execution."
            ),
        }

    # ── Discord EOD report ─────────────────────────────────────────────────

    def discord_eod_report(self, summary: dict, webhook_url: str = "") -> str:
        if not summary:
            return ""

        n        = summary.get("trades_executed", 0)
        wins     = summary.get("wins", 0)
        wr       = summary.get("win_rate", 0)
        pnl      = summary.get("gross_pnl_pct", 0)
        received = summary.get("signals_received", 0)
        blocked  = summary.get("context_blocked", 0)
        rejected = summary.get("options_rejected", 0)
        expired  = summary.get("queue_expired", 0)
        ap_wr    = summary.get("a_plus_win_rate")
        a_wr     = summary.get("a_win_rate")
        regime   = summary.get("regime", "unknown")
        trend    = "trend day" if summary.get("was_trend_day") else "normal day"

        report = (
            f"📊 **ANGEL PRECISION — EOD PROOF REPORT**\n"
            f"`{summary.get('date')}` | {regime} · {trend} | Mode: {summary.get('mode','paper').upper()} | v2\n"
            f"{'─'*44}\n"
            f"**Results**\n"
            f"> Trades: **{n}** | Wins: {wins} | WR: **{wr:.1f}%** | P&L: **{pnl:+.1f}%**\n"
            f"\n**Signal Funnel**\n"
            f"> {received} received → {blocked} context blocked → "
            f"{rejected} options rejected → {expired} expired → **{n} executed**\n"
        )

        if ap_wr is not None:
            report += (
                f"\n**Tier Results**\n"
                f"> A+ WR: **{ap_wr:.1f}%** ({summary.get('a_plus_count',0)} trades) | "
                f"A WR: **{a_wr:.1f}%** ({summary.get('a_count',0)} trades)\n"
            )

        report += f"{'─'*44}"

        if webhook_url:
            try:
                import requests
                requests.post(webhook_url, json={"content": report}, timeout=5)
            except Exception as e:
                log.warning(f"Discord EOD report failed: {e}")

        return report
