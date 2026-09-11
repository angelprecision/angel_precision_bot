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


# H7 — Canonical exit classification.
# The exit engine writes free-text reasons ("TARGET HIT -- ...", "HARD STOP
# -- ...", "RUNNER TRAIL EXIT -- ..."). Proof metrics need clean buckets so
# the 28/30-trade cycle can be measured as: win-rate, breakeven-rate, true
# loss-rate, hard-stop count, average win, average loss. This is a PURE
# function over (reason, pnl, win) — it changes no exit behavior, only adds
# a derived label to the proof row.
EXIT_BUCKETS = (
    "WIN_BASE_HIT",             # target/scale/profit-lock win at/near base target
    "WIN_RUNNER",               # runner trailed out above base — the big winners
    "BREAKEVEN_SAVE",           # closed within +/-3% — no real gain or loss
    "TARGET_HIT_OPTION_LOSS",   # underlying target hit but the option contract lost money
    "SOFT_LOSS",                # losing exit but not the hard stop (managed cut)
    "HARD_STOP",                # hit the hard stop threshold
    "EOD_CLOSE",                # forced flat at end of day
    "MANUAL_EXIT",              # operator/admin force-exit
    "RECONCILED_CLOSE",         # reconciler/quarantine evidence-based close
    "UNCLASSIFIED",             # fell through — should be ~0; investigate if not
)


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def is_target_hit_option_loss(exit_reason: str, option_pnl_pct: float) -> bool:
    """True when the underlying target was hit but the option trade lost money.

    This is reporting/intelligence taxonomy only. It does not change exit,
    broker, order, handoff, queue, or position behavior.
    """
    r = (exit_reason or "").upper().replace("_", " ")
    return "TARGET HIT" in r and _safe_float(option_pnl_pct) < 0.0


def classify_exit(exit_reason: str, option_pnl_pct: float, win: bool) -> str:
    """Map a raw exit reason + P&L into one canonical EXIT_BUCKETS value.

    Order matters: explicit-cause reasons (manual, EOD, hard stop, reconcile)
    take priority over P&L-shape inference (runner vs base vs breakeven), except
    target-hit option losses, which must never be treated as a clean target win.
    """
    r = (exit_reason or "").upper()
    pnl = _safe_float(option_pnl_pct)

    # 0. Underlying target success is not proof success when the option loses.
    if is_target_hit_option_loss(exit_reason, pnl):
        return "TARGET_HIT_OPTION_LOSS"

    # 1. Explicit operational causes — independent of P&L sign.
    if "MANUAL" in r or "ADMIN_FORCE" in r or "ADMIN FORCE" in r or "FORCE_EXIT" in r:
        return "MANUAL_EXIT"
    if "QUARANTINE" in r or "RECONCIL" in r or "GHOST" in r:
        return "RECONCILED_CLOSE"
    if "EOD" in r or "FORCE CLOSE" in r or "MARKET CLOSED" in r:
        return "EOD_CLOSE"
    if "HARD STOP" in r or "HARD_STOP" in r:
        return "HARD_STOP"

    # 2. Breakeven band — within +/-3% is neither a real win nor loss.
    # BREAKEVEN_BAND: matches BREAKEVEN_BAND_PCT env var in execution_core.
    # Both pnl and band are in PERCENTAGE form (e.g. -25.0 = -25%).
    import os as _os
    _band = float(_os.getenv("BREAKEVEN_BAND_PCT", "-2.0"))  # e.g. -2.0 = -2%
    if _band <= pnl <= 3.0:
        return "BREAKEVEN_SAVE"

    # 3. Winners — split base vs runner.
    if win or pnl > 3.0:
        if "RUNNER" in r or "TRAIL" in r and pnl >= 25.0:
            return "WIN_RUNNER"
        if "RUNNER" in r:
            return "WIN_RUNNER"
        # Trailing/profit-lock that still captured a big move counts as runner.
        if pnl >= 25.0:
            return "WIN_RUNNER"
        return "WIN_BASE_HIT"

    # 4. Losers that are not the hard stop = managed soft loss.
    if pnl < -3.0:
        return "SOFT_LOSS"

    return "UNCLASSIFIED"


def normalize_trade_outcome(exit_reason: str, option_pnl_pct: float, win: bool) -> dict:
    """Normalize proof-trade outcome fields before persistence/training.

    Future records that say TARGET HIT while the option P&L is negative become
    non-wins with a dedicated diagnostic/bucket. Historical rows are not changed.
    """
    target_hit_option_loss = is_target_hit_option_loss(exit_reason, option_pnl_pct)
    normalized_win = False if target_hit_option_loss else bool(win)
    return {
        "win": normalized_win,
        "exit_bucket": classify_exit(exit_reason, option_pnl_pct, normalized_win),
        "target_hit_option_loss": target_hit_option_loss,
    }


def is_positive_training_label(trade: dict) -> bool:
    """Return whether a proof trade is safe to use as a positive training label."""
    if not isinstance(trade, dict):
        return False
    if trade.get("target_hit_option_loss") or trade.get("exit_bucket") == "TARGET_HIT_OPTION_LOSS":
        return False
    return bool(trade.get("win")) and _safe_float(trade.get("option_pnl_pct")) >= 0.0


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
            # Operational health counters
            self.scale_outs_executed   = 0
            self.small_win_locks       = 0
            self.no_contract_skipped   = 0
            self.reconciler_corrections = 0
            self.sentinel_alerts       = 0
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
                "scale_outs_executed":   self.scale_outs_executed,
                "small_win_locks":       self.small_win_locks,
                "no_contract_skipped":   self.no_contract_skipped,
                "reconciler_corrections": self.reconciler_corrections,
                "sentinel_alerts":       self.sentinel_alerts,
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

def _resolve_entry_execution_mode(local_order_id: str) -> str:
    """Resolve the execution mode for a proof_trades row from the ORIGINATING
    entry order — the source of truth stamped at entry creation time.

    Reads orders.execution_mode (preferred) then orders.meta.execution_mode.
    Returns 'live' or 'paper' when the entry order carries a valid mode, else
    'unknown'. Never guesses 'live': a missing/absent mode is always 'unknown'.
    """
    if not local_order_id:
        return "unknown"
    try:
        from ap.db import get_order_by_id
        order = get_order_by_id(local_order_id)
    except Exception as e:
        log.debug("execution_mode lookup failed for %s: %s", local_order_id, e)
        return "unknown"
    if not order:
        return "unknown"
    mode = str(order.get("execution_mode") or "").lower().strip()
    if mode not in ("live", "paper"):
        meta = order.get("meta")
        if isinstance(meta, str):
            try:
                import json as _json
                meta = _json.loads(meta)
            except Exception:
                meta = None
        if isinstance(meta, dict):
            mode = str(meta.get("execution_mode") or "").lower().strip()
    return mode if mode in ("live", "paper") else "unknown"


# ── proof_trades.execution_mode schema guard ──────────────────────────────
# The proof logger writes proof_trades.execution_mode on every close, and
# live-only performance accounting depends on it. If the column is missing the
# insert silently falls back to a degraded write (or fails entirely), and live
# clients would lose execution-mode attribution. This guard verifies the column
# exists BEFORE the first insert and surfaces a loud CRITICAL otherwise. The
# result is cached so we probe the catalog at most once per process.
_EXECUTION_MODE_COLUMN_OK: Optional[bool] = None


def proof_trades_execution_mode_present(supabase_client) -> Optional[bool]:
    """Return True if proof_trades.execution_mode exists, False if confirmed
    missing, or None if it could not be determined (no client / probe error).

    Cached after the first definitive answer. A probe error returns None and is
    NOT cached, so a transient failure can be re-checked on a later call.
    """
    global _EXECUTION_MODE_COLUMN_OK
    if _EXECUTION_MODE_COLUMN_OK is not None:
        return _EXECUTION_MODE_COLUMN_OK
    if supabase_client is None:
        return None
    try:
        # PostgREST: selecting a non-existent column raises; selecting it with
        # limit(1) is a cheap existence probe that does not depend on any rows.
        supabase_client.table("proof_trades").select("execution_mode").limit(1).execute()
        _EXECUTION_MODE_COLUMN_OK = True
        return True
    except Exception as e:
        emsg = str(e).lower()
        if any(k in emsg for k in ("column", "execution_mode", "schema", "does not exist", "42703")):
            _EXECUTION_MODE_COLUMN_OK = False
            return False
        # Non-schema/transient error — do not cache, report unknown.
        log.debug("proof_trades.execution_mode probe inconclusive: %s", e)
        return None


def ensure_proof_trades_schema(supabase_client) -> bool:
    """Startup guard: confirm proof_trades.execution_mode exists before the proof
    logger begins inserting it. Logs CRITICAL with the migration to run if the
    column is missing. Returns True when present, False when confirmed missing,
    True (optimistic) when the check is inconclusive so we never block trading on
    a transient catalog read.
    """
    present = proof_trades_execution_mode_present(supabase_client)
    if present is False:
        log.critical(
            "[PROOF] SCHEMA GUARD FAILED — proof_trades.execution_mode is MISSING. "
            "Live-only performance attribution will be lost. Run migration "
            "migrations/20260606_orders_execution_mode.sql before live trading."
        )
        return False
    if present is True:
        log.info("[PROOF] schema guard OK — proof_trades.execution_mode present.")
        return True
    log.warning("[PROOF] schema guard inconclusive — could not verify proof_trades.execution_mode.")
    return True


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
        position_id:         str  = "",
        local_order_id:      str  = "",
        execution_mode:      str  = "",
        fill_timestamp_quality: str = "",
        # Adaptive exit pricing slippage fields (filled by exit engine)
        exit_bid:            float = 0.0,
        exit_ask:            float = 0.0,
        exit_mid:            float = 0.0,
        exit_limit_placed:   float = 0.0,
        exit_fill_price:     float = 0.0,
        slippage_vs_mid:     float = 0.0,
        slippage_vs_bid:     float = 0.0,
        exit_pricing_tier:   str  = "",
        exit_attempt:        int  = 0,
        seconds_to_fill:     float = 0.0,
    ) -> dict:
        now = datetime.now(timezone.utc)
        # execution_mode is COPIED from the originating entry order (source of
        # truth stamped at entry creation), NOT recomputed from self.mode — the
        # client may have switched modes while the position was open. Missing →
        # 'unknown' (never guess 'live').
        # execution_mode precedence (Requirement 6):
        #   1. Originating order mode is authoritative (stamped at entry creation).
        #      A client may switch modes while a position is open; the reconciler's
        #      current runtime mode must not overwrite a valid order-level mode.
        #   2. Explicit caller mode is used only when the originating order is
        #      unavailable or returns an invalid/unknown value.
        #   3. Both missing/invalid → "unknown". Never guess "live".
        _explicit_mode = str(execution_mode or "").strip().lower()
        _origin_mode   = _resolve_entry_execution_mode(local_order_id)
        _execution_mode = (
            _origin_mode   if _origin_mode   in ("live", "paper") else
            _explicit_mode if _explicit_mode in ("live", "paper") else
            "unknown"
        )
        _outcome = normalize_trade_outcome(exit_reason, option_pnl_pct, win)
        _win = _outcome["win"]
        row = {
            "client_email":       self.email,
            "mode":               self.mode,
            "execution_mode":     _execution_mode,
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
            "exit_bucket":        _outcome["exit_bucket"],
            "option_pnl_pct":     round(option_pnl_pct, 2),
            "underlying_pnl_pct": round(underlying_pnl_pct, 3),
            "win":                _win,
            "target_hit_option_loss": _outcome["target_hit_option_loss"],
            "spread_pct":         round(spread_pct, 4),
            "chain_grade":        chain_grade,
            "synthetic_entry":    bool(synthetic_entry),
            "position_id":        position_id or None,
            "local_order_id":     local_order_id or None,
            # Adaptive exit pricing — how we priced vs what we got
            "exit_bid":           round(exit_bid, 4) if exit_bid else None,
            "exit_ask":           round(exit_ask, 4) if exit_ask else None,
            "exit_mid":           round(exit_mid, 4) if exit_mid else None,
            "exit_limit_placed":  round(exit_limit_placed, 4) if exit_limit_placed else None,
            "exit_fill_price":    round(exit_fill_price, 4) if exit_fill_price else None,
            "slippage_vs_mid":    round(slippage_vs_mid, 4) if slippage_vs_mid else None,
            "slippage_vs_bid":    round(slippage_vs_bid, 4) if slippage_vs_bid else None,
            "exit_pricing_tier":  exit_pricing_tier or None,
            "exit_attempt":       exit_attempt if exit_attempt else None,
            "seconds_to_fill":    round(seconds_to_fill, 1) if seconds_to_fill else None,
        }
        # Keep chronology quality in the proof row whenever the caller has an
        # explicit classification.  Legacy exact callers omit the key so the
        # existing schema fallback remains compatible; a non-exact close must
        # not be silently persisted without its quality marker.
        if str(fill_timestamp_quality or "").strip():
            row["fill_timestamp_quality"] = str(fill_timestamp_quality).strip()

        # Cache for convenience — not source of truth
        with self._lock:
            self._trades_cache.append(row)

        log.info(
            f"[PROOF] CLOSED {ticker} {side} {tier} | "
            f"P&L={option_pnl_pct:+.1f}% | {'WIN' if _win else 'LOSS'} | "
            f"{exit_reason} | score={score:.0f} ctx={context_score:.0f}"
        )

        # Write to Supabase immediately — this is the source of truth.
        # Four-stage fallback so trades are NEVER silently lost on column drift:
        #   Stage 1: full row (all fields including diagnostic + slippage tracking)
        #   Stage 2: strip target-hit diagnostic if the migration has not run yet
        #   Stage 3: strip slippage + exit_bucket (columns that may not exist yet)
        #   Stage 4: core-only rows (guaranteed columns — minimal but never lost)
        _DIAGNOSTIC_COLS = {"target_hit_option_loss"}
        _SLIPPAGE_COLS = {
            "exit_bid", "exit_ask", "exit_mid", "exit_limit_placed",
            "exit_fill_price", "slippage_vs_mid", "slippage_vs_bid",
            "exit_pricing_tier", "exit_attempt", "seconds_to_fill",
        }
        _CORE_COLS = {
            "client_email", "mode", "execution_mode", "system_version",
            "opened_at", "closed_at",
            "ticker", "pattern", "side", "timeframe", "score", "tier",
            "entry_option_price", "exit_option_price", "contracts",
            "exit_reason", "option_pnl_pct", "underlying_pnl_pct", "win",
            "synthetic_entry", "position_id", "local_order_id",
            "fill_timestamp_quality",
        }
        # ── Persistence-status tracking ──────────────────────────────────────
        # _persisted is set True only after a confirmed Supabase insert.
        # _persistence_error captures the last failure reason.
        # These fields are added to the returned result dict but are NEVER
        # included in any Supabase insert payload.
        _persisted:         bool            = False
        _persistence_error: str | None      = None

        if self.sb:
            try:
                self.sb.table("proof_trades").insert(row).execute()
                _persisted = True
                log.debug("[PROOF] %s written to Supabase (full row)", ticker)
            except Exception as e1:
                emsg1 = str(e1).lower()
                if not any(k in emsg1 for k in ("column", "schema", "field", "violat", "null", "type")):
                    _persistence_error = str(e1)
                    log.error("[PROOF] Supabase write failed (non-schema error): %s", e1)
                else:
                    # Stage 2: strip diagnostic-only columns that may not exist yet.
                    _stage2 = {k: v for k, v in row.items() if k not in _DIAGNOSTIC_COLS}
                    try:
                        self.sb.table("proof_trades").insert(_stage2).execute()
                        _persisted = True
                        log.warning(
                            "[PROOF] %s written without target_hit_option_loss diagnostic — "
                            "run migration migrations/20260626_target_hit_option_loss_diagnostic.sql",
                            ticker,
                        )
                    except Exception as e2:
                        emsg2 = str(e2).lower()
                        if not any(k in emsg2 for k in ("column", "schema", "field", "violat", "null", "type")):
                            _persistence_error = str(e2)
                            log.error("[PROOF] Supabase write failed stage-2 (non-schema): %s", e2)
                        else:
                            # Stage 3: strip slippage columns + exit_bucket + diagnostics
                            _stage3 = {k: v for k, v in row.items()
                                       if k not in _SLIPPAGE_COLS and k != "exit_bucket" and k not in _DIAGNOSTIC_COLS}
                            try:
                                self.sb.table("proof_trades").insert(_stage3).execute()
                                _persisted = True
                                log.warning(
                                    "[PROOF] %s written without slippage columns — "
                                    "run proof_trades migration to add: %s",
                                    ticker,
                                    ", ".join(sorted(_SLIPPAGE_COLS | {"exit_bucket"} | _DIAGNOSTIC_COLS)),
                                )
                            except Exception as e3:
                                emsg3 = str(e3).lower()
                                if not any(k in emsg3 for k in ("column", "schema", "field")):
                                    _persistence_error = str(e3)
                                    log.error("[PROOF] Supabase write failed stage-3 (non-schema): %s", e3)
                                else:
                                    # Stage 4: core columns only — guaranteed minimal write
                                    _stage4 = {k: v for k, v in row.items() if k in _CORE_COLS}
                                    try:
                                        self.sb.table("proof_trades").insert(_stage4).execute()
                                        _persisted = True
                                        log.warning(
                                            "[PROOF] %s written with CORE COLUMNS ONLY — "
                                            "proof_trades schema is significantly out of date. "
                                            "Run all migrations immediately.",
                                            ticker,
                                        )
                                    except Exception as e4:
                                        _persistence_error = str(e4)
                                        log.error(
                                            "[PROOF] ALL WRITE ATTEMPTS FAILED for %s — "
                                            "TRADE WILL NOT APPEAR IN PROOF. Error: %s",
                                            ticker, e4,
                                        )
        else:
            _persistence_error = "missing_supabase_client"

        # Return trade fields plus private persistence metadata.
        # Callers that only read trade fields are unaffected.
        result = dict(row)
        result["_proof_persisted"]         = _persisted
        result["_proof_persistence_error"] = _persistence_error
        return result

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

        exit_counts = {
            "TARGET HIT": 0,
            "TARGET HIT OPTION LOSS": 0,
            "STOP HIT": 0,
            "THETA/TIME": 0,
            "PROFIT PROTECT": 0,
        }
        for t in trades:
            r = (t.get("exit_reason") or "").upper()
            if t.get("target_hit_option_loss") or t.get("exit_bucket") == "TARGET_HIT_OPTION_LOSS":
                exit_counts["TARGET HIT OPTION LOSS"] += 1
            elif "TARGET" in r:
                exit_counts["TARGET HIT"] += 1
            elif "STOP" in r:
                exit_counts["STOP HIT"] += 1
            elif "PROTECT" in r:
                exit_counts["PROFIT PROTECT"] += 1
            else:
                exit_counts["THETA/TIME"] += 1

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
