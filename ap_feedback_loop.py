# ap_feedback_loop.py — Angel Precision Trade Outcome Feedback Loop
# =============================================================================
# Gap 4 fix: Every trade that closes feeds back into the scoring model.
#
# What it records per trade:
#   - Ticker, pattern, timeframe, side
#   - Signal score at entry
#   - Entry trigger price, option entry price
#   - Exit price, exit reason, P&L
#   - Market context at entry (regime, time, volume)
#   - Whether it hit target, stop, or time exit
#
# What it does with that data:
#   1. Writes to Supabase `signal_outcomes` table (already in your schema)
#   2. Updates a running stats table per (ticker, pattern, timeframe, side)
#   3. Flags any setup whose LIVE win rate diverges from backtest by >20%
#      → Sends Discord alert: "BA 1-1 CALL live WR = 43% vs backtest 80% — REVIEW"
#   4. Auto-downgrades setup confidence when live data contradicts backtest
#
# This means:
#   Month 1: bot trades on backtest scores
#   Month 3: bot is trading on LIVE scores — backtest was just the starting point
#   Month 6: setups that don't perform in real life are automatically demoted
#   Month 12: the model is entirely self-correcting
# =============================================================================

from __future__ import annotations

import json
import logging
import threading
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("ap.feedback_loop")

# Minimum live trades before live WR overrides backtest WR
MIN_LIVE_TRADES_TO_OVERRIDE = 10
DIVERGENCE_ALERT_THRESHOLD  = 0.20   # alert if live WR differs from backtest by 20%+
AUTO_DOWNGRADE_THRESHOLD    = 0.25   # auto-demote if live WR is 25%+ below backtest


# =============================================================================
# TRADE OUTCOME RECORDER
# =============================================================================

class APFeedbackLoop:
    """
    Records trade outcomes and updates the live performance model.

    Usage:
        feedback = APFeedbackLoop(supabase_client, discord_webhook_url)

        # When a trade closes:
        feedback.record_outcome(
            signal=original_signal_dict,
            entry_option_price=1.85,
            exit_option_price=5.40,
            exit_reason="TARGET HIT",
            underlying_entry=855.0,
            underlying_exit=892.0,
            contracts=2,
        )
    """

    def __init__(self, supabase_client=None, discord_webhook_url: str = ""):
        self.sb          = supabase_client
        self.webhook_url = discord_webhook_url
        self._lock       = threading.Lock()
        self._live_stats: dict[str, dict] = {}   # key = "TICKER:PATTERN:TF:SIDE"

    # ── RECORD A CLOSED TRADE ─────────────────────────────────────────────────

    def record_outcome(
        self,
        signal:               dict,
        entry_option_price:   float,
        exit_option_price:    float,
        exit_reason:          str,
        underlying_entry:     float,
        underlying_exit:      float,
        contracts:            int = 1,
        context_notes:        str = "",
    ):
        """Call this every time a position closes."""
        ticker    = signal.get("ticker", "")
        pattern   = signal.get("pattern", "")
        side      = signal.get("side", "CALL")
        timeframe = signal.get("timeframe", "1d")
        score     = float(signal.get("score", 0) or 0)
        backtest_wr = float(signal.get("win_rate", 0) or 0)

        # P&L
        if entry_option_price > 0:
            option_pnl_pct = (exit_option_price - entry_option_price) / entry_option_price * 100
        else:
            option_pnl_pct = 0.0

        if underlying_entry > 0:
            underlying_pnl_pct = (underlying_exit - underlying_entry) / underlying_entry * 100
            if side == "PUT":
                underlying_pnl_pct = -underlying_pnl_pct
        else:
            underlying_pnl_pct = 0.0

        win            = option_pnl_pct > 0
        hit_target     = "TARGET" in exit_reason.upper()
        hit_stop       = "STOP" in exit_reason.upper()
        time_exit      = any(x in exit_reason.upper() for x in ["EOD", "THETA", "PROTECT"])

        outcome = {
            "ticker":             ticker,
            "pattern":            pattern,
            "side":               side,
            "timeframe":          timeframe,
            "signal_score":       round(score, 1),
            "backtest_win_rate":  backtest_wr,
            "entry_option_price": round(entry_option_price, 4),
            "exit_option_price":  round(exit_option_price, 4),
            "option_pnl_pct":     round(option_pnl_pct, 2),
            "underlying_entry":   round(underlying_entry, 4),
            "underlying_exit":    round(underlying_exit, 4),
            "underlying_pnl_pct": round(underlying_pnl_pct, 3),
            "contracts":          contracts,
            "win":                win,
            "hit_target":         hit_target,
            "hit_stop":           hit_stop,
            "time_exit":          time_exit,
            "exit_reason":        exit_reason,
            "context_notes":      context_notes,
            "closed_at":          datetime.now(timezone.utc).isoformat(),
        }

        log.info(
            f"[{ticker}] {pattern} {side} CLOSED | "
            f"P&L={option_pnl_pct:+.1f}% | {'WIN' if win else 'LOSS'} | {exit_reason}"
        )

        # 1. Write to Supabase
        self._write_to_supabase(outcome)

        # 2. Update live stats
        self._update_live_stats(ticker, pattern, timeframe, side, win, option_pnl_pct, backtest_wr)

        # 3. Send Discord notification
        self._notify_discord(outcome)

    # ── SUPABASE WRITE ────────────────────────────────────────────────────────

    def _write_to_supabase(self, outcome: dict):
        if not self.sb:
            log.debug("No Supabase client — outcome logged locally only")
            return
        try:
            self.sb.table("signal_outcomes").insert(outcome).execute()
            log.debug(f"[{outcome['ticker']}] Outcome written to Supabase")
        except Exception as e:
            log.error(f"Supabase write failed: {e}")

    # ── LIVE STATS UPDATE ─────────────────────────────────────────────────────

    def _update_live_stats(
        self,
        ticker: str, pattern: str, timeframe: str, side: str,
        win: bool, pnl_pct: float, backtest_wr: float,
    ):
        key = f"{ticker}:{pattern}:{timeframe}:{side}"
        with self._lock:
            if key not in self._live_stats:
                self._live_stats[key] = {
                    "ticker": ticker, "pattern": pattern,
                    "timeframe": timeframe, "side": side,
                    "trades": 0, "wins": 0, "total_pnl": 0.0,
                    "backtest_wr": backtest_wr,
                    "live_wr": None, "status": "LEARNING",
                }
            s = self._live_stats[key]
            s["trades"]    += 1
            s["wins"]      += int(win)
            s["total_pnl"] += pnl_pct
            s["live_wr"]    = s["wins"] / s["trades"]

            # Check for divergence
            if s["trades"] >= MIN_LIVE_TRADES_TO_OVERRIDE and backtest_wr > 0:
                divergence = backtest_wr / 100.0 - s["live_wr"]

                if divergence >= AUTO_DOWNGRADE_THRESHOLD:
                    s["status"] = "DOWNGRADED"
                    log.warning(
                        f"[{ticker}] {pattern} {side} AUTO-DOWNGRADED — "
                        f"live WR={s['live_wr']*100:.1f}% vs backtest {backtest_wr:.1f}% "
                        f"(divergence={divergence*100:.1f}%)"
                    )
                    self._alert_divergence(s, divergence, "DOWNGRADED")

                elif divergence >= DIVERGENCE_ALERT_THRESHOLD:
                    s["status"] = "WATCH"
                    self._alert_divergence(s, divergence, "WATCH")

                elif s["live_wr"] > backtest_wr / 100.0 + 0.10:
                    s["status"] = "OUTPERFORMING"
                    log.info(
                        f"[{ticker}] {pattern} {side} OUTPERFORMING — "
                        f"live WR={s['live_wr']*100:.1f}% vs backtest {backtest_wr:.1f}%"
                    )
                else:
                    s["status"] = "CONFIRMED"

    def _alert_divergence(self, stats: dict, divergence: float, level: str):
        """Send Discord alert when live WR diverges from backtest."""
        if not self.webhook_url:
            return
        try:
            import requests
            emoji = "🔴" if level == "DOWNGRADED" else "🟡"
            action = "STOP TRADING THIS SETUP" if level == "DOWNGRADED" else "MONITOR CLOSELY"
            msg = (
                f"{emoji} **LIVE vs BACKTEST DIVERGENCE**\n"
                f"> Setup: **{stats['ticker']}** {stats['pattern']} "
                f"**{stats['side']}** [{stats['timeframe']}]\n"
                f"> Backtest WR: `{stats['backtest_wr']:.1f}%`\n"
                f"> Live WR:     `{stats['live_wr']*100:.1f}%` ({stats['trades']} trades)\n"
                f"> Divergence:  `{divergence*100:.1f}%`\n"
                f"> Status:      **{level}** — {action}"
            )
            requests.post(self.webhook_url, json={"content": msg}, timeout=5)
        except Exception as e:
            log.warning(f"Divergence alert failed: {e}")

    # ── DISCORD TRADE NOTIFICATION ────────────────────────────────────────────

    def _notify_discord(self, outcome: dict):
        if not self.webhook_url:
            return
        try:
            import requests
            win_emoji = "✅" if outcome["win"] else "❌"
            exit_emoji = {"TARGET": "🎯", "STOP": "🛑", "THETA": "⏰", "EOD": "⏰", "PROTECT": "🔒"}.get(
                next((k for k in ["TARGET","STOP","THETA","EOD","PROTECT"] if k in outcome["exit_reason"].upper()), ""), "📊"
            )
            pnl = outcome["option_pnl_pct"]
            msg = (
                f"{win_emoji} **{outcome['ticker']}** {outcome['pattern']} "
                f"**{outcome['side']}** [{outcome['timeframe']}] CLOSED\n"
                f"> Option P&L: **{pnl:+.1f}%** | {exit_emoji} {outcome['exit_reason']}\n"
                f"> Score at entry: {outcome['signal_score']} | "
                f"Underlying: {outcome['underlying_pnl_pct']:+.3f}%"
            )
            requests.post(self.webhook_url, json={"content": msg}, timeout=5)
        except Exception as e:
            log.debug(f"Discord notify failed: {e}")

    # ── REPORTS ───────────────────────────────────────────────────────────────

    def get_live_stats(self) -> list[dict]:
        """Return all live performance stats, sorted by divergence."""
        with self._lock:
            stats = list(self._live_stats.values())
        return sorted(stats, key=lambda x: x["trades"], reverse=True)

    def get_downgraded_setups(self) -> list[str]:
        """Return setup keys that have been auto-downgraded."""
        with self._lock:
            return [k for k, v in self._live_stats.items() if v["status"] == "DOWNGRADED"]

    def daily_summary(self) -> str:
        """Build a daily P&L summary string for Discord."""
        stats = self.get_live_stats()
        if not stats:
            return "No trades recorded today."

        total_trades = sum(s["trades"] for s in stats)
        total_wins   = sum(s["wins"] for s in stats)
        total_pnl    = sum(s["total_pnl"] for s in stats)
        overall_wr   = total_wins / total_trades * 100 if total_trades > 0 else 0

        lines = [
            "📊 **ANGEL PRECISION — DAILY PERFORMANCE**",
            f"> Trades: {total_trades} | Wins: {total_wins} | WR: **{overall_wr:.1f}%**",
            f"> Total Option P&L: **{total_pnl:+.1f}%**",
        ]
        downgraded = self.get_downgraded_setups()
        if downgraded:
            lines.append(f"> ⚠️ Downgraded setups: {len(downgraded)} — review needed")
        return "\n".join(lines)
