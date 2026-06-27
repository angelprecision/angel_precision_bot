from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Optional

from ap_historical_edge_guard import (
    block_reason_for_untrusted_historical_edge,
    evaluate_historical_edge,
)

log = logging.getLogger(__name__)

_DAILY_TIMEFRAMES = {"1d", "daily", "1D", "D", "d"}
_INTRADAY_TIMEFRAMES = {"15m", "30m", "60m", "15", "30", "60", "15min", "30min", "60min"}
_ALLOWED_TIERS = {"A", "A+", "B"}


def _env_bool(key: str, default: bool = False) -> bool:
    return os.getenv(key, str(default)).strip().lower() in ("true", "1", "yes", "on")


def _env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except (TypeError, ValueError):
        return default


def _env_set(key: str, default: str = "") -> set[str]:
    raw = os.getenv(key, default).strip()
    return {p.strip() for p in raw.split(",") if p.strip()} if raw else set()


def _safe_float(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _resolve_score(signal: dict) -> tuple[float, str]:
    for key in ("score", "score_total", "merge_score", "scanner_score", "intel_score"):
        v = signal.get(key)
        if v is not None:
            try:
                f = float(v)
                if f > 0:
                    return f, key
            except (TypeError, ValueError):
                pass
    for key in ("score", "score_total", "merge_score", "scanner_score", "intel_score"):
        v = signal.get(key)
        if v is not None:
            try:
                return float(v), key
            except (TypeError, ValueError):
                pass
    return 0.0, "missing"


def _resolve_pattern(signal: dict) -> tuple[str, str]:
    for key in ("pattern", "pattern_id", "setup_combo", "pattern_family"):
        v = str(signal.get(key) or "").strip()
        if v:
            return v, key
    return "", "missing"


def _resolve_timeframe(signal: dict) -> tuple[Optional[str], str]:
    for key in ("timeframe", "time_horizon"):
        v = str(signal.get(key) or "").strip()
        if v:
            return v, key
    return None, "missing"


def _resolve_tier(signal: dict) -> tuple[str, str, bool]:
    for key in ("tier", "score_grade"):
        v = str(signal.get(key) or "").strip().upper()
        if v:
            return v, key, False
    return "", "missing", True


@dataclass
class HybridGateDecision:
    allowed: bool
    quality_lane: str
    block_reason: Optional[str]
    metadata: dict = field(default_factory=dict)

    @property
    def reason_code(self) -> Optional[str]:
        return self.block_reason

    def to_meta(self) -> dict:
        return {
            "hybrid_client_quality_mode": True,
            "quality_lane": self.quality_lane,
            "client_eligible": self.allowed,
            "block_reason": self.block_reason,
            **self.metadata,
        }


def _block(reason: str, lane: str, meta: dict) -> HybridGateDecision:
    log.info("[HYBRID_GATE] BLOCKED lane=%s reason=%s", lane, reason)
    return HybridGateDecision(False, lane, reason, meta)


def _allow(lane: str, meta: dict) -> HybridGateDecision:
    log.info("[HYBRID_GATE] ALLOWED lane=%s", lane)
    return HybridGateDecision(True, lane, None, meta)


def evaluate_client_quality_gate(
    signal: dict,
    client_id: str,
    snapshot: dict,
    live_quote: Optional[dict] = None,
    underlying_price: Optional[float] = None,
) -> HybridGateDecision:
    if not _env_bool("HYBRID_CLIENT_QUALITY_MODE"):
        return HybridGateDecision(True, "GATE_DISABLED", None, {"hybrid_client_quality_mode": False})

    min_score = _env_float("MIN_CLIENT_SCORE", 70.0)
    allow_tier_b = _env_bool("ALLOW_CLIENT_TIER_B", True)
    daily_whitelist = _env_set("DAILY_CLIENT_PATTERN_WHITELIST", "2-3,3-2-2,1-2_2D")
    intra_whitelist = _env_set("INTRADAY_CLIENT_PATTERN_WHITELIST", "")
    allow_failed_dir = _env_bool("ALLOW_FAILED_DIR_CLIENT", False)
    entry_confirm_s = _env_float("ENTRY_CONFIRM_SECONDS", 45.0)
    max_total = int(_env_float("MAX_CLIENT_TRADES_PER_DAY", 5))
    max_daily = int(_env_float("MAX_CLIENT_DAILY_TRADES", 3))
    max_intra = int(_env_float("MAX_CLIENT_INTRADAY_TRADES", 2))
    max_sym = int(_env_float("MAX_CLIENT_SYMBOL_TRADES_PER_DAY", 1))
    max_fade = _env_float("MAX_PRE_ENTRY_OPTION_FADE_PCT", 8.0)
    max_reversal = _env_float("MAX_PRE_ENTRY_UNDERLYING_REVERSAL_PCT", 0.25)

    ticker = str(signal.get("symbol") or signal.get("ticker") or "?").upper()
    direction = str(signal.get("direction") or signal.get("side") or "CALL").upper()
    raw_score, score_src = _resolve_score(signal)
    historical_edge = evaluate_historical_edge(signal, raw_score=raw_score)
    score = historical_edge.effective_score
    historical_edge_block_reason = block_reason_for_untrusted_historical_edge(historical_edge)
    pattern, pat_src = _resolve_pattern(signal)
    timeframe, tf_src = _resolve_timeframe(signal)
    tier, tier_src, tier_missing = _resolve_tier(signal)

    stop_ul = _safe_float(signal.get("stop_underlying"))
    trig_ul = _safe_float(signal.get("trigger_price"))
    target_ul = _safe_float(signal.get("target_underlying"))
    spread_pct = _safe_float(signal.get("spread_pct") or signal.get("spread_pct_at_submit"))

    raw_score_fields = {k: signal.get(k) for k in ("score", "score_total", "merge_score", "scanner_score", "intel_score") if signal.get(k) is not None}
    raw_pattern_fields = {k: signal.get(k) for k in ("pattern", "pattern_id", "setup_combo", "pattern_family") if signal.get(k) is not None}

    base_meta = {
        "symbol": ticker,
        "direction": direction,
        "timeframe": timeframe,
        "pattern": pattern,
        "tier": tier,
        "score": score,
        "raw_score": raw_score,
        "score_after_historical_edge": score,
        "trigger_price": trig_ul,
        "stop_underlying": stop_ul,
        "target_underlying": target_ul,
        "spread_pct": spread_pct,
        "entry_confirm_seconds": entry_confirm_s,
        "geometry_valid": None,
        "tier_missing": tier_missing,
        "resolved_score_source": score_src,
        "resolved_pattern_source": pat_src,
        "resolved_timeframe_source": tf_src,
        "resolved_tier_source": tier_src,
        "raw_score_fields": raw_score_fields,
        "raw_pattern_fields": raw_pattern_fields,
        **historical_edge.to_metadata(),
    }

    lane = "BLOCKED"
    if historical_edge_block_reason and score < min_score:
        return _block(historical_edge_block_reason, lane, {**base_meta, "min_score": min_score})
    if score < min_score:
        return _block("client_score_below_70", lane, {**base_meta, "min_score": min_score})

    allowed_tiers = _ALLOWED_TIERS if allow_tier_b else {"A", "A+"}
    if not tier_missing and tier and tier not in allowed_tiers:
        return _block("client_tier_block", lane, {**base_meta, "allowed_tiers": list(allowed_tiers)})

    if timeframe is None:
        return _block("client_timeframe_missing", lane, {**base_meta, "reason": "timeframe not found in signal or time_horizon"})

    tf_norm = timeframe.lower().replace("min", "m").replace(" ", "")
    is_daily = timeframe in _DAILY_TIMEFRAMES or tf_norm in {"1d", "daily", "d"}
    is_intraday = timeframe in _INTRADAY_TIMEFRAMES or tf_norm in {"15m", "30m", "60m"}
    if is_daily:
        lane = "DAILY_CLIENT"
    elif is_intraday:
        lane = "INTRADAY_CLIENT"
    else:
        return _block("client_timeframe_not_in_lane", lane, {**base_meta, "reason": f"timeframe={timeframe} not daily or intraday"})
    base_meta["quality_lane"] = lane

    if lane == "INTRADAY_CLIENT" and not allow_failed_dir and "FAILED_DIR" in pattern.upper():
        return _block("client_intraday_failed_dir_quarantine", lane, {**base_meta, "quarantined_pattern": pattern})

    if lane == "DAILY_CLIENT" and daily_whitelist:
        if not any(pattern == w or pattern.startswith(w) for w in daily_whitelist):
            return _block("client_daily_pattern_not_whitelisted", lane, {**base_meta, "whitelist": list(daily_whitelist), "pattern": pattern})
    elif lane == "INTRADAY_CLIENT":
        if not intra_whitelist:
            return _block("client_intraday_no_whitelist", lane, {**base_meta, "reason": "INTRADAY_CLIENT_PATTERN_WHITELIST is empty"})
        if not any(pattern == w or pattern.startswith(w) for w in intra_whitelist):
            return _block("client_intraday_pattern_not_whitelisted", lane, {**base_meta, "whitelist": list(intra_whitelist)})

    geometry_valid = True
    geometry_reason = None
    if trig_ul is not None and stop_ul is not None:
        if direction == "CALL" and stop_ul >= trig_ul:
            geometry_valid = False
            geometry_reason = f"CALL stop_underlying ({stop_ul}) >= trigger_price ({trig_ul})"
        elif direction == "PUT" and stop_ul <= trig_ul:
            geometry_valid = False
            geometry_reason = f"PUT stop_underlying ({stop_ul}) <= trigger_price ({trig_ul})"
    base_meta["geometry_valid"] = geometry_valid
    if not geometry_valid:
        return _block("invalid_trigger_stop_geometry", lane, {**base_meta, "geometry_reason": geometry_reason})

    if live_quote:
        entry_price = _safe_float(signal.get("entry_option_price") or signal.get("limit_price"))
        current_mid = _safe_float(live_quote.get("mid") or live_quote.get("mark") or live_quote.get("ask"))
        if entry_price and current_mid and entry_price > 0:
            fade_pct = (entry_price - current_mid) / entry_price * 100
            base_meta["quote_age_ms"] = live_quote.get("age_ms")
            base_meta["option_fade_pct"] = round(fade_pct, 2)
            if fade_pct > max_fade:
                return _block("entry_confirm_failed_option_fade", lane, {**base_meta, "fade_pct": round(fade_pct, 2), "max_fade_pct": max_fade})

    if underlying_price is not None and trig_ul is not None and trig_ul > 0:
        reversal_pct = abs(underlying_price - trig_ul) / trig_ul * 100
        base_meta["underlying_reversal_pct"] = round(reversal_pct, 2)
        if direction == "CALL" and underlying_price < trig_ul * (1 - max_reversal / 100):
            return _block("entry_confirm_failed_underlying_reversal", lane, {**base_meta, "underlying_price": underlying_price, "reversal_pct": round(reversal_pct, 2)})
        if direction == "PUT" and underlying_price > trig_ul * (1 + max_reversal / 100):
            return _block("entry_confirm_failed_underlying_reversal", lane, {**base_meta, "underlying_price": underlying_price, "reversal_pct": round(reversal_pct, 2)})

    trades_today = int(snapshot.get("total_trades", snapshot.get("trades_today", 0)) or 0)
    daily_trades = int(snapshot.get("daily_trades", 0) or 0)
    intraday_trades = int(snapshot.get("intraday_trades", 0) or 0)
    symbol_trades = dict(snapshot.get("symbol_trades", {}) or {})
    sym_today = int(symbol_trades.get(ticker.upper(), 0))

    if trades_today >= max_total:
        return _block("client_total_daily_cap_reached", lane, {**base_meta, "trades_today": trades_today, "max": max_total})
    if lane == "DAILY_CLIENT" and daily_trades >= max_daily:
        return _block("client_daily_lane_cap_reached", lane, {**base_meta, "daily_trades": daily_trades, "max_daily": max_daily})
    if lane == "INTRADAY_CLIENT" and intraday_trades >= max_intra:
        return _block("client_intraday_cap_reached", lane, {**base_meta, "intraday_trades": intraday_trades, "max_intra": max_intra})
    if sym_today >= max_sym:
        return _block("client_symbol_duplicate_block", lane, {**base_meta, "symbol_trades_today": sym_today, "max_symbol": max_sym})

    base_meta["confirmation_required"] = True
    base_meta["confirmation_seconds"] = entry_confirm_s
    base_meta["max_pre_entry_option_fade_pct"] = max_fade
    base_meta["max_pre_entry_underlying_reversal_pct"] = max_reversal

    log.info(
        "[HYBRID_GATE] PASS | client=%s ticker=%s lane=%s score=%.1f raw=%.1f src=%s historical_edge_valid=%s",
        client_id,
        ticker,
        lane,
        score,
        raw_score,
        score_src,
        historical_edge.historical_edge_valid,
    )
    return _allow(lane, base_meta)
