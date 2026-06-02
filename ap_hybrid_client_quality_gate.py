"""
ap_hybrid_client_quality_gate.py
=================================
P0: Hybrid Client Quality Gate

Protects client accounts from low-quality signals while keeping
real B-tier daily winners flowing.  Called by ap_master_control.evaluate()
just before the final approval — if this gate blocks, the signal is
TERMINAL: no sizing, no order creation, no re-approval.

Two client lanes
----------------
DAILY_CLIENT  — 1d/daily timeframe, score ≥ 70, A/A+/B tier,
                pattern in DAILY_CLIENT_PATTERN_WHITELIST
INTRADAY_CLIENT — 15m/30m/60m, score ≥ 70, A/A+/B tier,
                  pattern in INTRADAY_CLIENT_PATTERN_WHITELIST,
                  FAILED_DIR quarantined unless ALLOW_FAILED_DIR_CLIENT=true

Environment flags (all read at call-time — hot-reloadable)
-----------------------------------------------------------
HYBRID_CLIENT_QUALITY_MODE       = true
MIN_CLIENT_SCORE                 = 70
ALLOW_CLIENT_TIER_B              = true
DAILY_CLIENT_PATTERN_WHITELIST   = 2-3,3-2-2,1-2_2D
INTRADAY_CLIENT_PATTERN_WHITELIST= (empty by default)
ALLOW_FAILED_DIR_CLIENT          = false
ENTRY_CONFIRM_SECONDS            = 45
MAX_CLIENT_TRADES_PER_DAY        = 5
MAX_CLIENT_DAILY_TRADES          = 3
MAX_CLIENT_INTRADAY_TRADES       = 2
MAX_CLIENT_SYMBOL_TRADES_PER_DAY = 1
MAX_PRE_ENTRY_OPTION_FADE_PCT    = 8
MAX_PRE_ENTRY_UNDERLYING_REVERSAL_PCT = 0.25
"""

from __future__ import annotations
import os
import logging
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

_DAILY_TIMEFRAMES  = {"1d", "daily", "1D", "D", "d"}
_INTRADAY_TIMEFRAMES = {"15m", "30m", "60m", "15", "30", "60",
                        "15min", "30min", "60min"}
_ALLOWED_TIERS     = {"A", "A+", "B"}

# ── Config helpers ────────────────────────────────────────────────────────────

def _env_bool(key: str, default: bool = False) -> bool:
    return os.getenv(key, str(default)).strip().lower() in ("true", "1", "yes")

def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except (TypeError, ValueError):
        return default

def _env_set(key: str, default: str = "") -> set[str]:
    raw = os.getenv(key, default).strip()
    if not raw:
        return set()
    return {p.strip() for p in raw.split(",") if p.strip()}


# ── Gate decision ─────────────────────────────────────────────────────────────

@dataclass
class HybridGateDecision:
    allowed: bool
    quality_lane: str           # DAILY_CLIENT / INTRADAY_CLIENT / BLOCKED
    block_reason: Optional[str] # None if allowed
    metadata: dict = field(default_factory=dict)

    @property
    def reason_code(self) -> Optional[str]:
        return self.block_reason

    def to_meta(self) -> dict:
        return {
            "hybrid_client_quality_mode": True,
            "quality_lane":               self.quality_lane,
            "client_eligible":            self.allowed,
            "block_reason":               self.block_reason,
            **self.metadata,
        }


def _block(reason: str, lane: str, meta: dict) -> HybridGateDecision:
    log.info("[HYBRID_GATE] BLOCKED lane=%s reason=%s", lane, reason)
    return HybridGateDecision(allowed=False, quality_lane=lane,
                              block_reason=reason, metadata=meta)

def _allow(lane: str, meta: dict) -> HybridGateDecision:
    log.info("[HYBRID_GATE] ALLOWED lane=%s", lane)
    return HybridGateDecision(allowed=True, quality_lane=lane,
                              block_reason=None, metadata=meta)


# ── Main gate ─────────────────────────────────────────────────────────────────

def evaluate_client_quality_gate(
    signal: dict,
    client_id: str,
    snapshot: dict,
    live_quote: Optional[dict] = None,
    underlying_price: Optional[float] = None,
) -> HybridGateDecision:
    """
    Evaluate whether a signal is eligible for client fan-out.

    Parameters
    ----------
    signal          Full signal/plan dict (all scoring + meta fields)
    client_id       Client identifier for per-client caps
    snapshot        Current session snapshot: trades_today, open_count,
                    pending_entries, daily_trades, intraday_trades,
                    symbol_trades dict
    live_quote      Optional live option quote dict: bid, ask, mid, mark
    underlying_price Optional current underlying price

    Returns
    -------
    HybridGateDecision — TERMINAL if not allowed; must not be re-approved
    """

    # ── 0. Is gate enabled? ───────────────────────────────────────────────
    if not _env_bool("HYBRID_CLIENT_QUALITY_MODE"):
        return HybridGateDecision(
            allowed=True,
            quality_lane="GATE_DISABLED",
            block_reason=None,
            metadata={"hybrid_client_quality_mode": False},
        )

    # ── Read config (hot from env) ────────────────────────────────────────
    min_score        = _env_float("MIN_CLIENT_SCORE", 70.0)
    allow_tier_b     = _env_bool("ALLOW_CLIENT_TIER_B", True)
    daily_whitelist  = _env_set("DAILY_CLIENT_PATTERN_WHITELIST", "2-3,3-2-2,1-2_2D")
    intra_whitelist  = _env_set("INTRADAY_CLIENT_PATTERN_WHITELIST", "")
    allow_failed_dir = _env_bool("ALLOW_FAILED_DIR_CLIENT", False)
    entry_confirm_s  = _env_float("ENTRY_CONFIRM_SECONDS", 45.0)
    max_total        = int(_env_float("MAX_CLIENT_TRADES_PER_DAY", 5))
    max_daily        = int(_env_float("MAX_CLIENT_DAILY_TRADES", 3))
    max_intra        = int(_env_float("MAX_CLIENT_INTRADAY_TRADES", 2))
    max_sym          = int(_env_float("MAX_CLIENT_SYMBOL_TRADES_PER_DAY", 1))
    max_fade         = _env_float("MAX_PRE_ENTRY_OPTION_FADE_PCT", 8.0)
    max_reversal     = _env_float("MAX_PRE_ENTRY_UNDERLYING_REVERSAL_PCT", 0.25)

    # ── Extract signal fields ─────────────────────────────────────────────
    ticker    = str(signal.get("symbol") or signal.get("ticker") or "?").upper()
    direction = str(signal.get("direction") or signal.get("side") or "CALL").upper()
    timeframe = str(signal.get("timeframe") or "1d").strip()
    pattern   = str(signal.get("pattern")   or "").strip()
    tier      = str(signal.get("tier")      or "").strip().upper()
    score     = float(signal.get("score")   or 0)
    stop_ul   = _safe_float(signal.get("stop_underlying"))
    trig_ul   = _safe_float(signal.get("trigger_price"))
    target_ul = _safe_float(signal.get("target_underlying"))
    spread_pct= _safe_float(signal.get("spread_pct") or signal.get("spread_pct_at_submit"))

    # Base metadata attached to every gate decision
    base_meta = {
        "symbol":          ticker,
        "direction":       direction,
        "timeframe":       timeframe,
        "pattern":         pattern,
        "tier":            tier,
        "score":           score,
        "trigger_price":   trig_ul,
        "stop_underlying": stop_ul,
        "target_underlying": target_ul,
        "spread_pct":      spread_pct,
        "entry_confirm_seconds": entry_confirm_s,
        "geometry_valid":  None,
    }

    lane = "BLOCKED"  # will be set per-path

    # ── 1. Score hard gate ────────────────────────────────────────────────
    if score < min_score:
        return _block("client_score_below_70", lane,
                      {**base_meta, "min_score": min_score})

    # ── 2. Tier gate ─────────────────────────────────────────────────────
    allowed_tiers = _ALLOWED_TIERS if allow_tier_b else {"A", "A+"}
    if tier and tier not in allowed_tiers:
        return _block("client_tier_block", lane,
                      {**base_meta, "allowed_tiers": list(allowed_tiers)})

    # ── 3. Determine lane ─────────────────────────────────────────────────
    tf_norm = timeframe.lower().replace("min", "m").replace(" ", "")
    is_daily   = (timeframe in _DAILY_TIMEFRAMES or
                  tf_norm in {"1d", "daily", "d"})
    is_intraday= (timeframe in _INTRADAY_TIMEFRAMES or
                  tf_norm in {"15m", "30m", "60m"})

    if is_daily:
        lane = "DAILY_CLIENT"
    elif is_intraday:
        lane = "INTRADAY_CLIENT"
    else:
        # Timeframe not in any client lane
        return _block("client_timeframe_not_in_lane", lane,
                      {**base_meta, "reason": f"timeframe={timeframe} not daily or intraday"})

    base_meta["quality_lane"] = lane

    # ── 4. FAILED_DIR quarantine (intraday only) ─────────────────────────
    if lane == "INTRADAY_CLIENT" and not allow_failed_dir:
        if "FAILED_DIR" in pattern.upper():
            return _block("client_intraday_failed_dir_quarantine", lane,
                          {**base_meta, "quarantined_pattern": pattern})

    # ── 5. Pattern whitelist gate ─────────────────────────────────────────
    if lane == "DAILY_CLIENT":
        if daily_whitelist and pattern not in daily_whitelist:
            # Allow empty daily_whitelist to pass all (backwards compat)
            pass  # whitelist is enforced only if non-empty
        if daily_whitelist:
            # Check if pattern matches any whitelist entry (prefix or exact)
            if not any(pattern == w or pattern.startswith(w)
                       for w in daily_whitelist):
                return _block("client_daily_pattern_not_whitelisted", lane,
                              {**base_meta,
                               "whitelist": list(daily_whitelist),
                               "pattern": pattern})

    elif lane == "INTRADAY_CLIENT":
        # Intraday whitelist is empty by default → block ALL intraday for clients
        if not intra_whitelist:
            return _block("client_intraday_no_whitelist", lane,
                          {**base_meta,
                           "reason": "INTRADAY_CLIENT_PATTERN_WHITELIST is empty — no intraday patterns approved for clients yet"})
        if not any(pattern == w or pattern.startswith(w)
                   for w in intra_whitelist):
            return _block("client_intraday_pattern_not_whitelisted", lane,
                          {**base_meta, "whitelist": list(intra_whitelist)})

    # ── 6. Trigger/stop geometry hard gate ───────────────────────────────
    geometry_valid = True
    geometry_reason = None
    if trig_ul is not None and stop_ul is not None:
        if direction == "CALL":
            if stop_ul >= trig_ul:
                geometry_valid = False
                geometry_reason = (
                    f"CALL stop_underlying ({stop_ul}) >= trigger_price ({trig_ul}) — "
                    "stop is on the wrong side of trigger"
                )
        elif direction == "PUT":
            if stop_ul <= trig_ul:
                geometry_valid = False
                geometry_reason = (
                    f"PUT stop_underlying ({stop_ul}) <= trigger_price ({trig_ul}) — "
                    "stop is on the wrong side of trigger"
                )
    base_meta["geometry_valid"] = geometry_valid
    if not geometry_valid:
        return _block("invalid_trigger_stop_geometry", lane,
                      {**base_meta, "geometry_reason": geometry_reason})

    # ── 7. Pre-entry option fade check ───────────────────────────────────
    # If a live quote is available, check the option hasn't faded too much
    # from the decision price before the watcher arms
    if live_quote:
        entry_price = _safe_float(signal.get("entry_option_price") or
                                   signal.get("limit_price"))
        current_mid = _safe_float(live_quote.get("mid") or
                                   live_quote.get("mark") or
                                   live_quote.get("ask"))
        if entry_price and current_mid and entry_price > 0:
            fade_pct = (entry_price - current_mid) / entry_price * 100
            base_meta["quote_age_ms"]  = live_quote.get("age_ms")
            base_meta["option_fade_pct"] = round(fade_pct, 2)
            if fade_pct > max_fade:
                return _block("entry_confirm_failed_option_fade", lane,
                              {**base_meta,
                               "fade_pct": round(fade_pct, 2),
                               "max_fade_pct": max_fade})

    # ── 8. Pre-entry underlying reversal check ────────────────────────────
    if underlying_price is not None and trig_ul is not None and trig_ul > 0:
        reversal_pct = abs(underlying_price - trig_ul) / trig_ul * 100
        base_meta["underlying_reversal_pct"] = round(reversal_pct, 2)
        if direction == "CALL" and underlying_price < trig_ul * (1 - max_reversal / 100):
            return _block("entry_confirm_failed_underlying_reversal", lane,
                          {**base_meta,
                           "underlying_price": underlying_price,
                           "reversal_pct": round(reversal_pct, 2)})
        if direction == "PUT"  and underlying_price > trig_ul * (1 + max_reversal / 100):
            return _block("entry_confirm_failed_underlying_reversal", lane,
                          {**base_meta,
                           "underlying_price": underlying_price,
                           "reversal_pct": round(reversal_pct, 2)})

    # ── 9. Per-client caps ────────────────────────────────────────────────
    trades_today   = int(snapshot.get("trades_today",    0) or 0)
    daily_trades   = int(snapshot.get("daily_trades",    0) or 0)
    intraday_trades= int(snapshot.get("intraday_trades", 0) or 0)
    symbol_trades  = dict(snapshot.get("symbol_trades",  {}) or {})
    sym_today      = int(symbol_trades.get(ticker.upper(), 0))

    if trades_today >= max_total:
        return _block("client_daily_cap_reached", lane,
                      {**base_meta,
                       "trades_today": trades_today, "max": max_total})

    if lane == "DAILY_CLIENT" and daily_trades >= max_daily:
        return _block("client_daily_cap_reached", lane,
                      {**base_meta,
                       "daily_trades": daily_trades, "max_daily": max_daily})

    if lane == "INTRADAY_CLIENT" and intraday_trades >= max_intra:
        return _block("client_intraday_cap_reached", lane,
                      {**base_meta,
                       "intraday_trades": intraday_trades, "max_intra": max_intra})

    if sym_today >= max_sym:
        return _block("client_symbol_duplicate_block", lane,
                      {**base_meta,
                       "symbol_trades_today": sym_today, "max_symbol": max_sym})

    # ── 10. All gates passed — inject confirmation config ─────────────────
    base_meta["confirmation_required"]  = True
    base_meta["confirmation_seconds"]   = entry_confirm_s
    base_meta["max_pre_entry_option_fade_pct"] = max_fade
    base_meta["max_pre_entry_underlying_reversal_pct"] = max_reversal

    log.info(
        "[HYBRID_GATE] PASS | client=%s ticker=%s lane=%s score=%.1f tier=%s pattern=%s",
        client_id, ticker, lane, score, tier, pattern,
    )
    return _allow(lane, base_meta)


def _safe_float(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None
