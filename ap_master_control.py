# ap_master_control.py — Angel Precision Master Control
# =============================================================================
# THE SINGLE AUTHORITY for whether a signal may become a trade.
#
# Gate order (evaluate()):
#   A. Kill switch / mode / daily stop
#   B. Position snapshot (open_count, capital, pending entries, ticker dedupe)
#   C. Hard risk limits (capital%, directional bias, daily loss, trade count)
#   D. Score floor
#   E. Context floor
#   F. Tier classification
#   G. Intelligence pipeline (optional enrichment)
#   H. Feedback modifier
#   I. Build ApprovedExecutionPlan
# =============================================================================

from __future__ import annotations

import uuid
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Any

log = logging.getLogger("ap.master_control")


# =============================================================================
# APPROVED EXECUTION PLAN
# =============================================================================

@dataclass
class ApprovedExecutionPlan:
    """
    Canonical contract between intelligence and execution.
    Created once by master control. Never mutated by execution modules.
    """
    plan_id:           str
    signal_id:         str
    client_id:         str
    ticker:            str
    side:              str              # "CALL" | "PUT"
    direction:         str
    pattern:           str
    timeframe:         str
    contracts:         int
    max_position_usd:  float
    tier:              str
    score:             float
    intel_score:       float
    confidence_bucket: str

    trigger_type:      str              # "breach" | "immediate"
    trigger_price:     Optional[float]
    stop_underlying:   Optional[float]
    target_underlying: Optional[float]

    contract_symbol:   Optional[str]   = None
    limit_price:       Optional[float] = None
    mode:              str             = "paper"
    paper_sim:         bool            = True
    reasoning:         str             = ""
    intel_available:   bool            = False
    stage:             str             = "APPROVED"
    created_at:        str             = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    metadata:          dict            = field(default_factory=dict)

    def to_signal_dict(self) -> dict:
        return {
            "signal_id":      self.signal_id,
            "ticker":         self.ticker,
            "symbol":         self.ticker,
            "side":           self.side,
            "direction":      self.direction,
            "pattern":        self.pattern,
            "pattern_id":     self.pattern,
            "timeframe":      self.timeframe,
            "score":          self.score,
            "ev_score":       self.score,
            "tier":           self.tier,
            "confidence_tag": self.confidence_bucket,
            "trigger": {
                "entry": self.trigger_price,
                "stop":  self.stop_underlying,
                "pt1":   self.target_underlying,
            },
            "_approved_plan": self,
        }


# =============================================================================
# CONTROL DECISION
# =============================================================================

@dataclass
class ControlDecision:
    ok:        bool
    stage:     str
    reason:    str              = ""
    plan:      Optional[ApprovedExecutionPlan] = None
    signal_id: str              = ""
    ticker:    str              = ""
    client_id: str              = "default"


# =============================================================================
# MASTER CONTROL
# =============================================================================

class APMasterControl:
    """
    Single decision authority for all trading decisions.

    Constructor args:
        mode             — "PAPER" | "LIVE"
        score_floor      — minimum signal score (default 60)
        context_floor    — minimum real_time_ctx score (default 6.0)
        max_positions    — max concurrent active positions (default 7)
        max_capital_pct  — max capital% deployed before blocking (default 0.40)
        max_calls        — max active CALL positions (default 5)
        max_puts         — max active PUT positions (default 5)
        max_trades_today — max entries per day (default 10)
        max_daily_loss   — max realized loss today before kill (default -500)
        account_equity   — account size for capital% calculation (default 25000)
        position_manager — APPositionManager instance (required for gates)
        supabase_client  — optional Supabase client
        signal_store     — optional signal status store
        tier_engine      — optional tier engine
        feedback_loop    — optional feedback loop
    """

    def __init__(
        self,
        *,
        mode:             str   = "paper",
        score_floor:      float = 60.0,
        context_floor:    float = 6.0,
        max_positions:    int   = 7,
        max_capital_pct:  float = 0.40,     # block if deployed > 40% of equity
        max_calls:        int   = 5,         # max simultaneous CALL positions
        max_puts:         int   = 5,         # max simultaneous PUT positions
        max_trades_today: int   = 10,        # entries per day
        max_daily_loss:   float = -500.0,    # stop trading if PnL < this
        account_equity:   float = 25000.0,   # used for capital% math
        position_manager  = None,
        supabase_client   = None,
        signal_store      = None,
        tier_engine       = None,
        feedback_loop     = None,
    ):
        self.mode             = mode.upper()
        self.paper            = (self.mode != "LIVE")
        self.score_floor      = score_floor
        self.context_floor    = context_floor
        self.max_positions    = max_positions
        self.max_capital_pct  = max_capital_pct
        self.max_calls        = max_calls
        self.max_puts         = max_puts
        self.max_trades_today = max_trades_today
        self.max_daily_loss   = max_daily_loss
        self.account_equity   = account_equity

        self.pm       = position_manager   # APPositionManager
        self.sb       = supabase_client
        self.store    = signal_store
        self.tier_eng = tier_engine
        self.feedback = feedback_loop

        # Runtime callbacks (wired from execution core if pm not provided)
        self._kill_switch_fn = None
        self._mode_fn        = None

        # Session dedup cache
        self._seen_signals: set = set()

        log.info(
            f"APMasterControl initialized | mode={self.mode} | "
            f"score_floor={self.score_floor} | ctx_floor={self.context_floor} | "
            f"max_pos={self.max_positions} | max_cap={self.max_capital_pct*100:.0f}% | "
            f"max_calls={self.max_calls} | max_puts={self.max_puts} | "
            f"max_trades_today={self.max_trades_today} | "
            f"max_daily_loss=${self.max_daily_loss}"
        )

    def wire(self, *, kill_switch_fn=None, mode_fn=None):
        """Wire runtime callbacks from execution core."""
        if kill_switch_fn: self._kill_switch_fn = kill_switch_fn
        if mode_fn:        self._mode_fn        = mode_fn

    def set_account_equity(self, equity: float):
        """Update account equity (call after broker balance fetch)."""
        self.account_equity = float(equity)

    # =========================================================================
    # PUBLIC ENTRY POINT
    # =========================================================================

    def evaluate(self, signal: dict, client_id: str = "default") -> ControlDecision:
        """
        The ONLY place that decides whether a signal becomes a trade.
        Returns ControlDecision(ok=True, plan=...) on approval.
        Returns ControlDecision(ok=False, ...) on any block.
        """
        ticker    = signal.get("ticker", signal.get("symbol", "?"))
        score     = float(signal.get("score", 0) or 0)
        signal_id = str(signal.get("signal_id") or uuid.uuid4())
        signal["signal_id"] = signal_id

        log.info(f"[{ticker}] evaluate | score={score:.1f} | client={client_id}")

        # ── A. SYSTEM GATES ───────────────────────────────────────────────────

        if self._kill_switch_fn and self._kill_switch_fn():
            return self._block(signal_id, ticker, client_id,
                               "blocked_system", "kill_switch_active")

        current_mode = (self._mode_fn() if self._mode_fn else self.mode).upper()
        if current_mode == "READ_ONLY":
            return self._block(signal_id, ticker, client_id,
                               "blocked_system", "mode_read_only")

        # Dedupe
        dedup_key = f"{signal_id}:{client_id}"
        if dedup_key in self._seen_signals:
            return self._block(signal_id, ticker, client_id,
                               "blocked_system", "duplicate_signal")
        self._seen_signals.add(dedup_key)

        # ── B. POSITION SNAPSHOT ──────────────────────────────────────────────

        snap = self._get_snapshot(client_id)

        # ── C. HARD RISK LIMITS ───────────────────────────────────────────────

        # Max concurrent positions (OPEN + CLOSING)
        if snap["open_count"] >= self.max_positions:
            return self._block(signal_id, ticker, client_id, "blocked_risk",
                               f"max_positions ({snap['open_count']}/{self.max_positions})")

        # Pending entry orders count against available slots
        effective_count = snap["open_count"] + snap["pending_entries"]
        if effective_count >= self.max_positions:
            return self._block(signal_id, ticker, client_id, "blocked_risk",
                               f"max_positions_with_pending ({effective_count}/{self.max_positions})")

        # Capital % limit
        max_capital = self.account_equity * self.max_capital_pct
        if snap["capital_deployed"] >= max_capital:
            return self._block(signal_id, ticker, client_id, "blocked_risk",
                               f"capital_limit (${snap['capital_deployed']:.0f} >= "
                               f"${max_capital:.0f} = {self.max_capital_pct*100:.0f}% of ${self.account_equity:.0f})")

        # Directional bias limits
        side = signal.get("side", signal.get("direction", "CALL")).upper()
        if side == "CALL" and snap["calls_open"] >= self.max_calls:
            return self._block(signal_id, ticker, client_id, "blocked_risk",
                               f"max_calls ({snap['calls_open']}/{self.max_calls})")
        if side == "PUT" and snap["puts_open"] >= self.max_puts:
            return self._block(signal_id, ticker, client_id, "blocked_risk",
                               f"max_puts ({snap['puts_open']}/{self.max_puts})")

        # Daily trade count
        if snap["trades_today"] >= self.max_trades_today:
            return self._block(signal_id, ticker, client_id, "blocked_risk",
                               f"max_trades_today ({snap['trades_today']}/{self.max_trades_today})")

        # Daily loss limit
        if snap["realized_pnl_today"] <= self.max_daily_loss:
            return self._block(signal_id, ticker, client_id, "blocked_risk",
                               f"daily_loss_limit (${snap['realized_pnl_today']:.2f} <= ${self.max_daily_loss:.2f})")

        # Ticker dedupe — already holding or pending entry in this ticker
        if snap["open_tickers"] and ticker.upper() in snap["open_tickers"]:
            return self._block(signal_id, ticker, client_id, "blocked_risk",
                               f"ticker_already_active ({ticker})")

        # Pending entry for this ticker (order in-flight, no position yet)
        if self.pm:
            try:
                if self.pm.has_pending_entry(ticker):
                    return self._block(signal_id, ticker, client_id, "blocked_risk",
                                       f"pending_entry_exists ({ticker})")
            except Exception as e:
                log.warning(f"[{ticker}] has_pending_entry check failed: {e}")

        # ── D. SCORE GATE ─────────────────────────────────────────────────────

        if score < self.score_floor:
            self._store_update(signal_id, "rejected",
                               f"score {score:.1f} < floor {self.score_floor}")
            return self._block(signal_id, ticker, client_id, "blocked_score",
                               f"score_below_floor ({score:.1f}<{self.score_floor})")

        # ── E. CONTEXT GATE ───────────────────────────────────────────────────

        score_breakdown = signal.get("score_breakdown") or {}
        if "real_time_ctx" in score_breakdown:
            ctx = float(score_breakdown.get("real_time_ctx", 0) or 0)
            if ctx < self.context_floor:
                self._store_update(signal_id, "context_blocked",
                                   f"ctx={ctx:.1f} < floor {self.context_floor}")
                return self._block(signal_id, ticker, client_id, "blocked_score",
                                   f"context_below_floor (ctx={ctx:.1f}<{self.context_floor})")

        # ── F. TIER CLASSIFICATION ────────────────────────────────────────────

        try:
            from ap_tier_engine import Tier
            tier = Tier.from_score(score)
        except Exception:
            tier = self._fallback_tier(score)

        if tier in ("REJECT", "reject"):
            self._store_update(signal_id, "rejected", f"tier=REJECT score={score:.1f}")
            return self._block(signal_id, ticker, client_id, "blocked_score",
                               f"tier_reject (score={score:.1f})")

        if tier in ("SHADOW", "shadow"):
            self._store_update(signal_id, "shadow", f"tier=SHADOW score={score:.1f}")
            return ControlDecision(
                ok=False, stage="shadow",
                reason=f"shadow_track score={score:.1f}",
                signal_id=signal_id, ticker=ticker, client_id=client_id,
            )

        # ── G. INTELLIGENCE PIPELINE ──────────────────────────────────────────

        intel         = self._run_intelligence(signal)
        intel_score   = float(intel.get("score", 0))
        intel_approve = intel.get("approved", True)
        intel_reason  = intel.get("reasoning", "")
        intel_avail   = intel.get("_available", False)

        if intel_avail and not intel_approve:
            self._store_update(signal_id, "rejected",
                               f"intel_blocked: {intel_reason[:100]}")
            return self._block(signal_id, ticker, client_id, "blocked_intel",
                               f"intel_rejected: {intel_reason[:80]}")

        intel_contracts = int(intel.get("contracts", 1) or 1)

        # ── H. FEEDBACK MODIFIER ──────────────────────────────────────────────

        feedback_mod = 1.0
        setup_status = "LEARNING"
        if self.feedback:
            try:
                feedback_mod = self.feedback.get_size_modifier(
                    ticker=ticker,
                    pattern=signal.get("pattern", ""),
                    timeframe=signal.get("timeframe", "1d"),
                    side=signal.get("side", "CALL"),
                )
                setup_status = self.feedback.get_setup_status(
                    ticker, signal.get("pattern", ""),
                    signal.get("timeframe", "1d"), signal.get("side", "CALL"),
                )
                if setup_status == "DOWNGRADED" and not self.paper:
                    return self._block(signal_id, ticker, client_id, "blocked_intel",
                                       "setup_downgraded_live_blocked")
            except Exception as e:
                log.warning(f"[{ticker}] Feedback modifier failed: {e}")

        # ── I. SIZING ─────────────────────────────────────────────────────────

        if str(tier).upper() == "B":
            contracts = 1
        else:
            tier_mult = 1.0 if str(tier).upper() == "A+" else 0.6
            base      = self._base_contracts(score)
            contracts = max(1, round(base * feedback_mod * tier_mult))
            if intel_avail and intel_contracts > 0:
                contracts = min(contracts, intel_contracts)

        # ── J. BUILD APPROVED PLAN ────────────────────────────────────────────

        _trigger     = signal.get("trigger") or {}
        entry_price  = signal.get("entry_price") or _trigger.get("entry")
        stop_price   = signal.get("stop_price")  or _trigger.get("stop")
        target_price = (signal.get("target_price") or
                        _trigger.get("pt1") or _trigger.get("pt2"))
        trigger_type = "breach" if entry_price else "immediate"

        plan = ApprovedExecutionPlan(
            plan_id           = str(uuid.uuid4()),
            signal_id         = signal_id,
            client_id         = client_id,
            ticker            = ticker,
            side              = signal.get("side", "CALL"),
            direction         = signal.get("direction", signal.get("side", "CALL")),
            pattern           = signal.get("pattern", signal.get("pattern_id", "")),
            timeframe         = signal.get("timeframe", "1d"),
            contracts         = contracts,
            max_position_usd  = contracts * 100 * 5.0,
            tier              = str(tier),
            score             = score,
            intel_score       = intel_score,
            confidence_bucket = signal.get("confidence_tag", "standard_pool"),
            trigger_type      = trigger_type,
            trigger_price     = float(entry_price) if entry_price else None,
            stop_underlying   = float(stop_price) if stop_price else None,
            target_underlying = float(target_price) if target_price else None,
            mode              = "live" if not self.paper else "paper",
            paper_sim         = self.paper,
            reasoning         = (intel_reason or
                                 f"tier={tier} score={score:.1f} "
                                 f"feedback={feedback_mod:.2f} setup={setup_status}"),
            intel_available   = intel_avail,
            stage             = "APPROVED",
            metadata          = {
                "setup_status":    setup_status,
                "feedback_mod":    feedback_mod,
                "intel_result":    intel,
                "snapshot_at_eval": {
                    "open_count":       snap["open_count"],
                    "capital_deployed": snap["capital_deployed"],
                    "pending_entries":  snap["pending_entries"],
                    "calls_open":       snap["calls_open"],
                    "puts_open":        snap["puts_open"],
                },
            },
        )

        self._store_update(signal_id, "queued", timestamp_flag="queued_at")

        log.info(
            f"[{ticker}] ✅ APPROVED | tier={tier} score={score:.1f} "
            f"contracts={contracts} trigger={trigger_type} "
            f"entry={entry_price} stop={stop_price} target={target_price} | "
            f"intel={'✓' if intel_avail else '—'} feedback={feedback_mod:.2f} | "
            f"open={snap['open_count']} cap=${snap['capital_deployed']:.0f} "
            f"pending={snap['pending_entries']}"
        )

        return ControlDecision(
            ok=True, stage="approved", reason="",
            plan=plan, signal_id=signal_id,
            ticker=ticker, client_id=client_id,
        )

    # =========================================================================
    # PRIVATE HELPERS
    # =========================================================================

    def _get_snapshot(self, client_id: str) -> dict:
        """Pull position snapshot. Falls back to zeros if pm unavailable."""
        if self.pm:
            try:
                return self.pm.snapshot()
            except Exception as e:
                log.warning(f"snapshot() failed: {e} — using zeros")
        return {
            "open_count": 0, "open_tickers": set(),
            "calls_open": 0, "puts_open": 0,
            "capital_deployed": 0.0,
            "pending_entries": 0, "pending_exits": 0,
            "trades_today": 0, "realized_pnl_today": 0.0,
            "open_positions": [], "closing_positions": [],
        }

    def _run_intelligence(self, signal: dict) -> dict:
        try:
            from intelligence_bridge import run_intelligence_check, INTELLIGENCE_AVAILABLE
            if not INTELLIGENCE_AVAILABLE:
                return {"approved": True, "score": 0, "contracts": 1,
                        "reasoning": "intel_unavailable", "_available": False}
            _trigger = signal.get("trigger") or {}
            price = (signal.get("entry_price") or _trigger.get("entry") or
                     signal.get("current_price") or 100.0)
            result = run_intelligence_check(signal, underlying_price=float(price))
            result["_available"] = True
            return result
        except Exception as e:
            log.debug(f"Intelligence unavailable: {e}")
            return {"approved": True, "score": 0, "contracts": 1,
                    "reasoning": f"intel_error: {e}", "_available": False}

    def _fallback_tier(self, score: float) -> str:
        """Tier classification if ap_tier_engine not importable."""
        if score >= 85: return "A+"
        if score >= 75: return "A"
        if score >= 65: return "B"
        if score >= 55: return "SHADOW"
        return "REJECT"

    def _base_contracts(self, score: float) -> int:
        if score >= 95: return 4
        if score >= 90: return 3
        if score >= 85: return 2
        return 1

    def _block(self, signal_id, ticker, client_id, stage, reason) -> ControlDecision:
        log.info(f"[{ticker}] BLOCKED | stage={stage} | reason={reason}")
        return ControlDecision(
            ok=False, stage=stage, reason=reason,
            signal_id=signal_id, ticker=ticker, client_id=client_id,
        )

    def _store_update(self, signal_id: str, status: str,
                      context_notes: str = "", timestamp_flag: str = ""):
        if not self.store:
            return
        try:
            if timestamp_flag:
                self.store.update_status(signal_id, status,
                                         timestamp_flag=timestamp_flag)
            else:
                self.store.update_status(signal_id, status,
                                         context_notes=context_notes)
        except Exception as e:
            log.debug(f"store_update failed: {e}")

    def reset_session(self):
        """Call at start of each trading day to clear dedup cache."""
        self._seen_signals.clear()
        log.info("MasterControl session reset")
