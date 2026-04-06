# ap_execution_core.py — Angel Precision Execution Core
# =============================================================================
# Ties all 4 execution upgrades together into one clean interface.
# This is what ClientRunner calls — it replaces the raw exit_manager_loop.
#
# Flow for every signal received:
#
#   SIGNAL IN → EntryWatcher parks it → watches for breach
#       ↓ (breach confirmed)
#   OptionsIntelligence validates chain → selects contract → approves/rejects
#       ↓ (approved)
#   Broker places order → position created
#       ↓
#   ExitEngine monitors position → time-aware exits → P&L tracking
#       ↓ (position closed)
#   FeedbackLoop records outcome → updates live stats → alerts on divergence
#
# All 4 components run as background threads. Thread-safe. Per-client isolated.
# =============================================================================

from __future__ import annotations

import os
import logging
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from ap_entry_watcher       import APEntryWatcher, WatchedSignal
from ap_options_intelligence import evaluate_contract, chain_health_report
from ap_exit_engine          import APExitEngine, ManagedPosition
from ap_feedback_loop        import APFeedbackLoop
from ap_tier_engine          import APTierEngine, APShadowTracker, Tier

log = logging.getLogger("ap.execution_core")
ET  = ZoneInfo("America/New_York")

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
BOT_MODE            = os.getenv("BOT_MODE", "PAPER").upper()   # PAPER or LIVE


class APExecutionCore:
    """
    One instance per client (per ClientRunner thread).
    Manages the full lifecycle: signal → watch → enter → manage → exit → record.
    """

    def __init__(self, broker, supabase_client=None, email: str = ""):
        self.broker    = broker
        self.email     = email
        self.paper     = BOT_MODE != "LIVE"

        # The 4 components
        self.watcher   = APEntryWatcher(broker)
        self.exit_eng  = APExitEngine(broker)
        self.feedback  = APFeedbackLoop(supabase_client, DISCORD_WEBHOOK_URL)

        # Wire callbacks
        self.watcher.on_trigger    = self._on_entry_trigger
        self.watcher.on_expire     = lambda w: log.info(f"[{w.ticker}] Signal expired — no breach")
        self.watcher.on_invalidate = lambda w: log.info(f"[{w.ticker}] Signal invalidated — wrong direction first")

        self.exit_eng.on_exit  = self._on_position_close
        self.exit_eng.on_scale = self._on_position_scale

        self.tier_engine   = APTierEngine()
        self.shadow        = APShadowTracker(supabase_client, DISCORD_WEBHOOK_URL)

        log.info(f"APExecutionCore initialized for {email} | Mode: {'PAPER' if self.paper else 'LIVE'}")

    def start(self):
        """Start all background engines."""
        self.watcher.start()
        self.exit_eng.start()
        log.info(f"[{self.email}] Execution core started")

    def stop(self):
        self.watcher.stop()
        self.exit_eng.stop()

    def receive_signal(self, signal: dict):
        """
        Entry point for scanner signals.
        Called when the /signal endpoint receives a new signal from the scanner.
        """
        ticker = signal.get("ticker", "")
        score  = float(signal.get("score", 0) or 0)
        grade  = signal.get("grade", "")

        log.info(
            f"[{ticker}] Signal received | "
            f"{signal.get('pattern')} {signal.get('side')} [{signal.get('timeframe')}] | "
            f"Score={score} [{grade}] | auto={signal.get('auto_execute')}"
        )

        # Gate: minimum score — 85 is the floor, 90 is auto-execute
        if score < 85:
            log.info(f"[{ticker}] Rejected — score {score} below 85 minimum (raise your bar)")
            return

        # Gate: context score hard block
        # Even a high-scoring signal gets killed if market conditions are wrong
        context_score = float(signal.get("score_breakdown", {}).get("real_time_ctx", 20) or 20)
        context_minimum = 12.0   # out of 20 — below this, conditions are too poor
        if context_score < context_minimum:
            log.info(
                f"[{ticker}] CONTEXT BLOCKED — context_score={context_score:.1f}/20 "
                f"below {context_minimum} minimum. Signal score={score} but tape is wrong."
            )
            return

        # Gate: auto_execute=False means paper tracking only
        if not signal.get("auto_execute", False) and not self.paper:
            # auto_execute=True only when score >= 90
            log.info(f"[{ticker}] Grade {grade} (score={score:.1f}) — not auto-execute tier, paper tracking")
            return

        # Add to entry watcher — it will wait for the actual breach
        added = self.watcher.add_signal(signal)
        if not added:
            log.info(f"[{ticker}] Signal not added to watcher (EOD or duplicate)")

    # ── CALLBACK: Entry Breach Confirmed ─────────────────────────────────────

    def _on_entry_trigger(self, watched: WatchedSignal):
        """
        Called by entry watcher when breach is confirmed.
        Now we run the options intelligence check and place the order.
        """
        sig    = watched.signal
        ticker = watched.ticker
        side   = watched.side

        log.info(f"[{ticker}] Entry triggered at ${watched.trigger_price:.2f} — evaluating options chain")

        # Fetch option chain from broker
        try:
            chain, expiration = self._fetch_0dte_chain(ticker, side)
        except Exception as e:
            log.error(f"[{ticker}] Chain fetch failed: {e}")
            return

        if not chain:
            log.warning(f"[{ticker}] Empty chain — cannot enter")
            return

        # Chain health sanity check
        health = chain_health_report(chain, side, watched.trigger_price)
        if not health["tradeable"]:
            log.warning(
                f"[{ticker}] Chain health FAILED — "
                f"avg_spread={health['avg_spread']}% "
                f"vol={health['total_volume']} — skipping"
            )
            return

        # Options intelligence gate
        decision = evaluate_contract(
            chain=chain,
            direction=side,
            underlying_price=watched.trigger_price,
            expiration=expiration,
            signal_score=watched.score,
        )

        if not decision.approved:
            log.warning(f"[{ticker}] Options gate REJECTED: {decision.rejection_reason}")
            return

        # Calculate contracts to buy based on:
        #   1. Score tier (base contracts)
        #   2. Options intelligence size_modifier (spread/IV/liquidity)
        #   3. Feedback loop size_modifier (live vs backtest performance)
        base_contracts    = self._get_base_contracts(watched.score)
        spread_modifier   = decision.size_modifier
        feedback_modifier = self.feedback.get_size_modifier(
            ticker   = watched.ticker,
            pattern  = sig.get("pattern", ""),
            timeframe= sig.get("timeframe", "1d"),
            side     = watched.side,
        )
        setup_status = self.feedback.get_setup_status(
            watched.ticker, sig.get("pattern",""), sig.get("timeframe","1d"), watched.side
        )
        # Hard block downgraded setups even if they re-score high
        if setup_status == "DOWNGRADED" and not self.paper:
            log.warning(
                f"[{ticker}] BLOCKED — setup is DOWNGRADED in live performance data. "
                f"Live WR diverged >25% from backtest. Resolve before trading."
            )
            return

        combined_modifier = spread_modifier * feedback_modifier
        contracts = max(1, round(base_contracts * combined_modifier))
        log.info(
            f"[{ticker}] Sizing: base={base_contracts} × spread={spread_modifier:.2f} "
            f"× feedback={feedback_modifier:.2f} → {contracts} contracts "
            f"[setup status: {setup_status}]"
        )

        log.info(
            f"[{ticker}] Options gate APPROVED [{decision.grade}] — "
            f"buying {contracts}x {decision.symbol} @ ${decision.mid_price:.2f} "
            f"spread={decision.spread_pct*100:.1f}% IV={decision.iv*100:.0f}% "
            f"size_modifier={decision.size_modifier}"
        )

        # Place the order
        if self.paper:
            log.info(f"[{ticker}] PAPER MODE — simulating order fill at ${decision.mid_price:.2f}")
            fill_price = decision.mid_price
        else:
            try:
                fill_price = self._place_option_order(
                    symbol=decision.symbol,
                    contracts=contracts,
                    side="buy_to_open",
                    limit_price=decision.mid_price,
                )
            except Exception as e:
                log.error(f"[{ticker}] Order placement failed: {e}")
                return

        if not fill_price:
            return

        # Register with exit engine
        # Pull context for trend-day exit relaxation
        score_breakdown = sig.get("score_breakdown", {})
        regime          = sig.get("spy_trend", "neutral")
        is_trend_day    = bool(sig.get("is_trend_day", False))
        trend_direction = regime  # "uptrend" / "downtrend"

        pos = ManagedPosition(
            ticker=ticker,
            option_symbol=decision.symbol,
            side=side,
            quantity=contracts,
            entry_price=fill_price,
            underlying_entry=watched.trigger_price,
            underlying_target=watched.target_price,
            underlying_stop=watched.stop_level,
            is_trend_day=is_trend_day,
            trend_direction=trend_direction,
        )
        pos.current_option_price   = fill_price
        pos.current_underlying     = watched.trigger_price

        # Attach original signal for feedback loop
        pos.signal = sig  # type: ignore[attr-defined]

        self.exit_eng.add_position(pos)

        log.info(
            f"[{ticker}] {'PAPER' if self.paper else 'LIVE'} POSITION OPEN | "
            f"{contracts}x {decision.symbol} @ ${fill_price:.2f} | "
            f"target=${watched.target_price} stop=${watched.stop_level}"
        )

    # ── CALLBACK: Position Closed ─────────────────────────────────────────────

    def _on_position_close(self, pos: ManagedPosition, decision):
        """Called when exit engine fully closes a position."""
        if self.paper:
            log.info(f"[{pos.ticker}] PAPER CLOSE | P&L={pos.option_pnl_pct*100:+.1f}% | {decision.reason}")
            exit_price = pos.current_option_price
        else:
            try:
                exit_price = self._place_option_order(
                    symbol=pos.option_symbol,
                    contracts=pos.quantity_remaining,
                    side="sell_to_close",
                    limit_price=pos.current_option_price,
                )
            except Exception as e:
                log.error(f"[{pos.ticker}] Close order failed: {e}")
                exit_price = pos.current_option_price

        # Record outcome in feedback loop + shadow tracker
        sig = getattr(pos, "signal", {})
        self.feedback.record_outcome(
            signal=sig,
            entry_option_price=pos.entry_price,
            exit_option_price=exit_price or pos.current_option_price,
            exit_reason=decision.reason,
            underlying_entry=pos.underlying_entry,
            underlying_exit=pos.current_underlying,
            contracts=pos.quantity,
            context_notes=f"mode={'paper' if self.paper else 'live'}",
        )
        # Record to shadow tracker for tier comparison reporting
        tier = sig.get("tier", Tier.A_PLUS)
        opt_pnl = ((exit_price or pos.current_option_price) - pos.entry_price) / pos.entry_price * 100
        self.shadow.record_live_outcome(tier, opt_pnl)

    def _on_position_scale(self, pos: ManagedPosition, decision):
        """Called on partial (scale-out) exits."""
        log.info(
            f"[{pos.ticker}] SCALE OUT {decision.quantity}x | "
            f"P&L={pos.option_pnl_pct*100:+.1f}% | {decision.reason}"
        )
        if not self.paper:
            try:
                self._place_option_order(
                    symbol=pos.option_symbol,
                    contracts=decision.quantity,
                    side="sell_to_close",
                    limit_price=pos.current_option_price,
                )
            except Exception as e:
                log.error(f"[{pos.ticker}] Scale order failed: {e}")

    # ── BROKER HELPERS ────────────────────────────────────────────────────────

    def _fetch_0dte_chain(self, ticker: str, side: str) -> tuple[list, str]:
        """Fetch today's option chain from Tradier."""
        from datetime import date
        today = date.today().strftime("%Y-%m-%d")

        resp = self.broker.session.get(
            f"{self.broker.base_url}/v1/markets/options/chains",
            params={"symbol": ticker, "expiration": today, "greeks": "true"},
            headers={"Accept": "application/json"},
            timeout=10,
        )
        data    = resp.json()
        options = data.get("options", {}).get("option", [])
        if isinstance(options, dict):
            options = [options]
        return options or [], today

    def _get_base_contracts(self, score: float) -> int:
        """
        Base contract count by score tier.
        Score thresholds match the tightened system:
          90+ = A+: full position
          85–89 = A: standard position
          below 85: should not reach here (blocked at gate)
        """
        if score >= 95: return 4    # exceptional — max
        if score >= 90: return 3    # A+ — full size
        if score >= 85: return 2    # A  — standard
        return 1                    # safety net — should be blocked before here

    def _place_option_order(
        self, symbol: str, contracts: int, side: str, limit_price: float
    ) -> Optional[float]:
        """Place a limit order via Tradier. Returns fill price or None."""
        try:
            resp = self.broker.session.post(
                f"{self.broker.base_url}/v1/accounts/{self.broker.account_id}/orders",
                data={
                    "class":    "option",
                    "symbol":   symbol.split()[0] if " " in symbol else symbol[:6],
                    "option_symbol": symbol,
                    "side":     side,
                    "quantity": contracts,
                    "type":     "limit",
                    "price":    round(limit_price, 2),
                    "duration": "day",
                },
                headers={"Accept": "application/json"},
                timeout=10,
            )
            data = resp.json()
            order = data.get("order", {})
            status = order.get("status", "")
            log.info(f"Order placed: {status} | {symbol} x{contracts} @ ${limit_price:.2f}")
            return limit_price if status in ("ok", "filled", "pending") else None
        except Exception as e:
            log.error(f"Order error: {e}")
            return None
