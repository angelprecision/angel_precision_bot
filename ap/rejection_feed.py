# ap/rejection_feed.py -- Angel Precision | Discord Rejection Feed
# =============================================================================
# Posts rejected signals / blocked decisions to Discord with concise context.
#
# Design goals:
#   - Non-blocking: Discord failure must NEVER crash trading.
#   - Low-noise: lightweight cooldown prevents identical spam bursts.
#   - Explainable: stage, reason_code, score, pattern, thresholds/details.
#   - Company-grade timestamps: always America/New_York.
#
# Wire points:
#   - queue._dispatch() after every rejected _mark_job()
#   - master_control blocked decisions, if you want Discord mirroring
#   - contract_selector when no eligible contract survives
#   - execution_core chain health failures / broker submit failures
#
# Important:
#   This is NOT the canonical audit store. decision_events is canonical.
#   This is a human-facing alert/feed layer.
# =============================================================================

from __future__ import annotations

import hashlib
import json
import logging
import os
import threading
import time
from collections import Counter
from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

import requests

log = logging.getLogger("ap.rejection_feed")

ET = ZoneInfo("America/New_York")

DISCORD_WEBHOOK_REJECTIONS = (os.getenv("DISCORD_WEBHOOK_REJECTIONS", "") or "").strip()
REJECTION_FEED_ENABLED = bool(DISCORD_WEBHOOK_REJECTIONS)

# Discord safety
_REQUEST_TIMEOUT_SEC = float(os.getenv("REJECTION_FEED_TIMEOUT_SEC", "8"))
_DEDUP_WINDOW_SEC = int(os.getenv("REJECTION_FEED_DEDUP_WINDOW_SEC", "90"))
_MAX_DETAIL_ITEMS = int(os.getenv("REJECTION_FEED_MAX_DETAIL_ITEMS", "8"))
_MAX_CONTENT_CHARS = 1900  # Discord limit is 2000; leave headroom.

# In-memory anti-spam cache. Key -> last_sent_epoch
_DEDUP_CACHE: dict[str, float] = {}
_DEDUP_LOCK = threading.Lock()


# =============================================================================
# LABELS / TAXONOMY
# =============================================================================

_STAGE_EMOJI = {
    "master_control":    "🚦",
    "blocked_system":    "🧯",
    "blocked_risk":      "🧱",
    "blocked_score":     "📉",
    "blocked_intel":     "🧠",
    "contract_filter":   "📋",
    "contract_rank":     "📊",
    "selector_entry":    "🧾",
    "chain_health":      "🔗",
    "risk_sizing":       "💰",
    "capital_util":      "🏦",
    "entry_validation":  "⏱️",
    "broker_submit":     "📤",
    "dedup":             "🔄",
    "entry_cutoff":      "⏰",
    "iv_gate":           "📈",
    "earnings":          "📅",
    "fill_monitor":      "✅",
    "order_monitor":     "👁️",
    "exit_decision":     "🚪",
    "system_alert":      "🚨",
}

_REASON_LABELS = {
    "duplicate_signal_id":             "🔁 Duplicate signal already in flight",
    "DEDUP_BLOCK":                     "🔁 Duplicate setup / signal blocked",
    "no_contract_found":               "📭 No eligible contracts — spreads too wide or no chain",
    "NO_ELIGIBLE_CONTRACTS":           "📭 No eligible contracts",
    "chain_health_failed":             "🔗 Chain health failed — spread / volume / chain data issue",
    "CHAIN_HEALTH_FAILED":             "🔗 Chain health failed",
    "SPREAD_TOO_WIDE":                 "📏 Spread too wide",
    "OI_TOO_LOW":                      "💧 Open interest too low",
    "CHAIN_EMPTY":                     "🧱 Empty options chain",
    "CHAIN_FETCH_FAILED":              "🌐 Chain fetch failed",
    "QUOTE_FETCH_FAILED":              "📉 Quote fetch failed",
    "QUOTE_ZERO_BID_ASK":              "🪫 Quote zero bid/ask",
    "DIRECT_QUOTE_ZERO_BID_ASK":       "🪫 Direct quote zero bid/ask",
    "CHAIN_ROW_ZERO_BID_ASK":          "🪫 Chain row zero bid/ask",
    "BID_BELOW_MIN":                   "🪙 Bid below minimum",
    "NO_VALID_PLAYBOOK_DTE_CONTRACT":  "📅 No valid playbook DTE contract",
    "VOLUME_TOO_LOW":                  "📉 Volume too low",
    "DELTA_OUT_OF_RANGE":              "🎯 Delta out of acceptable range",
    "DTE_OUT_OF_RANGE":                "📆 Expiration/DTE out of range",
    "PREMIUM_CAP_EXCEEDED":            "💸 Premium too expensive",
    "NO_AFFORDABLE_CONTRACT":          "💸 No affordable contract",
    "DEEP_OTM":                        "🎯 Strike too far out of the money",
    "IV_RANK_TOO_HIGH":                "📈 IV rank too high — premium too expensive",
    "EARNINGS_LOCKOUT":                "📅 Earnings blackout window",
    "CAPITAL_UTIL_BLOCK":              "🏦 Capital utilization cap reached",
    "SECTOR_CAP_BLOCK":                "🏦 Sector exposure cap reached",
    "TICKER_CAP_BLOCK":                "🏦 Ticker exposure cap reached",
    "POSITION_LIMIT_REACHED":          "📊 Max open positions reached",
    "MAX_CALLS":                       "📊 Max calls reached",
    "MAX_PUTS":                        "📊 Max puts reached",
    "DAILY_STOP_ACTIVE":               "🛑 Daily loss limit hit",
    "SCORE_BELOW_THRESHOLD":           "📉 Signal score below floor",
    "KILL_SWITCH_ACTIVE":              "🧯 Kill switch active",
    "entry_cutoff: too_late_in_session": "⏰ Too late in session — entry cutoff hit",
    "overnight_signal_no_watcher":     "🌙 Overnight signal — no entry watcher available",
    "ORDER_REJECTED":                  "📤 Broker rejected order",
    "ORDER_CANCELED":                  "📤 Broker canceled order",
    "ORDER_EXPIRED":                   "📤 Broker order expired",
    "BROKER_FILL_CHECK_ERROR":         "⚠️ Broker fill check failed",
}


def _now_et() -> datetime:
    return datetime.now(ET)


def _label_reason(reason: str) -> str:
    if not reason:
        return "unknown"
    return _REASON_LABELS.get(reason, _REASON_LABELS.get(reason.upper(), reason))


def _safe_str(value: Any, max_len: int = 180) -> str:
    text = str(value)
    text = text.replace("\x00", "").strip()
    if len(text) > max_len:
        return text[: max_len - 1] + "…"
    return text


def _format_value(value: Any) -> str:
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".")
    if isinstance(value, (dict, list, tuple)):
        try:
            return _safe_str(json.dumps(value, default=str), max_len=220)
        except Exception:
            return _safe_str(value, max_len=220)
    return _safe_str(value, max_len=220)


def _dedup_key(
    *,
    ticker: str,
    side: str,
    stage: str,
    reason: str,
    client_id: str,
    details: Optional[dict],
) -> str:
    payload = {
        "ticker": (ticker or "").upper(),
        "side": (side or "").upper(),
        "stage": stage or "",
        "reason": reason or "",
        "client_id": client_id or "",
        # Only include compact detail identity. Values often change by pennies;
        # this prevents total spam while still allowing distinct failure types.
        "detail_keys": sorted((details or {}).keys()),
    }
    raw = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _should_send(key: str) -> bool:
    now = time.time()
    with _DEDUP_LOCK:
        # Opportunistic cleanup
        stale_before = now - (_DEDUP_WINDOW_SEC * 3)
        for k, ts in list(_DEDUP_CACHE.items()):
            if ts < stale_before:
                _DEDUP_CACHE.pop(k, None)

        last = _DEDUP_CACHE.get(key)
        if last and now - last < _DEDUP_WINDOW_SEC:
            return False

        _DEDUP_CACHE[key] = now
        return True


def _send_discord_message(content: str) -> None:
    if not REJECTION_FEED_ENABLED:
        return

    if len(content) > _MAX_CONTENT_CHARS:
        content = content[: _MAX_CONTENT_CHARS - 1] + "…"

    try:
        resp = requests.post(
            DISCORD_WEBHOOK_REJECTIONS,
            json={"content": content},
            timeout=_REQUEST_TIMEOUT_SEC,
        )
        if resp.status_code >= 300:
            log.debug(
                "Rejection feed Discord returned HTTP %s: %s",
                resp.status_code,
                resp.text[:250] if getattr(resp, "text", None) else "",
            )
    except Exception as e:
        log.debug("Rejection feed post failed (non-critical): %s", e)


# =============================================================================
# PUBLIC API
# =============================================================================

def post_rejection(
    ticker: str,
    side: str,
    stage: str,
    reason: str,
    score: float = 0.0,
    pattern: str = "",
    details: dict | None = None,
    client_id: str = "",
    *,
    reason_code: str | None = None,
    dedup: bool = True,
) -> None:
    """
    Post a human-readable rejection/block to Discord.

    Non-blocking: all exceptions are swallowed so rejection-feed failures never
    crash trading.

    reason_code can be passed when you have canonical observability codes.
    reason remains supported for older callers.
    """
    if not REJECTION_FEED_ENABLED:
        return

    try:
        canonical_reason = reason_code or reason or "unknown"

        if dedup:
            key = _dedup_key(
                ticker=ticker,
                side=side,
                stage=stage,
                reason=canonical_reason,
                client_id=client_id,
                details=details,
            )
            if not _should_send(key):
                return

        stage_emoji = _STAGE_EMOJI.get(stage, "❌")
        reason_label = _label_reason(canonical_reason)
        side_clean = (side or "—").upper()
        side_emoji = "🟢" if side_clean == "CALL" else "🔴" if side_clean == "PUT" else "⚪"

        lines = [
            f"{stage_emoji} **REJECTED** | {side_emoji} **{(ticker or '?').upper()}** {side_clean}",
            f"Stage: `{_safe_str(stage or 'unknown')}`",
            f"Reason: {reason_label}",
        ]

        if reason_code:
            lines.append(f"Code: `{_safe_str(reason_code)}`")
        elif reason:
            lines.append(f"Code: `{_safe_str(reason)}`")

        if client_id:
            lines.append(f"Client: `{_safe_str(client_id, 80)}`")
        if score:
            lines.append(f"Score: `{float(score):.1f}`")
        if pattern:
            lines.append(f"Pattern: `{_safe_str(pattern, 80)}`")

        if details:
            lines.append("Details:")
            shown = 0
            for k, v in details.items():
                if shown >= _MAX_DETAIL_ITEMS:
                    lines.append(f"• `+{len(details) - shown}` more detail(s)")
                    break
                lines.append(f"• `{_safe_str(k, 50)}`: `{_format_value(v)}`")
                shown += 1

        lines.append(f"`{_now_et().strftime('%Y-%m-%d %H:%M:%S ET')}`")

        _send_discord_message("\n".join(lines))

    except Exception as e:
        log.debug("post_rejection failed (non-critical): %s", e)


def post_chain_health_fail(
    ticker: str,
    spread_pct: float,
    volume: int,
    max_spread: float,
    min_volume: int,
    expiry: str = "",
    side: str = "—",
    client_id: str = "",
) -> None:
    """
    Specific helper for chain health failures.
    Called from execution_core when chain health check fails.
    """
    post_rejection(
        ticker=ticker,
        side=side,
        stage="chain_health",
        reason="chain_health_failed",
        reason_code="CHAIN_HEALTH_FAILED",
        client_id=client_id,
        details={
            "spread_pct": f"{spread_pct:.1f}% (max {max_spread:.1f}%)",
            "volume": f"{volume} (min {min_volume})",
            "expiry": expiry or "n/a",
        },
    )


def post_no_contracts(
    ticker: str,
    side: str,
    chain_size: int,
    top_rejections: dict,
    score: float = 0.0,
    pattern: str = "",
    client_id: str = "",
) -> None:
    """
    Specific helper for NO_ELIGIBLE_CONTRACTS.
    Called from contract_selector when nothing passes quality gates.
    """
    try:
        top = ", ".join(
            f"{k}({v})"
            for k, v in sorted((top_rejections or {}).items(), key=lambda x: -x[1])[:3]
        )
    except Exception:
        top = "unavailable"

    post_rejection(
        ticker=ticker,
        side=side,
        stage="contract_filter",
        reason="no_contract_found",
        reason_code="NO_ELIGIBLE_CONTRACTS",
        score=score,
        pattern=pattern,
        client_id=client_id,
        details={
            "chain_size": chain_size,
            "top_rejections": top or "none",
        },
    )


def post_master_control_block(
    ticker: str,
    side: str,
    stage: str,
    reason: str,
    score: float = 0.0,
    pattern: str = "",
    client_id: str = "",
    details: dict | None = None,
) -> None:
    """
    Specific helper for master control blocks.
    Called from queue._dispatch() or master_control blocked decision mirrors.
    """
    post_rejection(
        ticker=ticker,
        side=side,
        stage=stage or "master_control",
        reason=reason,
        score=score,
        pattern=pattern,
        client_id=client_id,
        details=details,
    )


def post_decision_event_rejection(event: dict) -> None:
    """
    Optional bridge: post directly from a decision_events-shaped dict.

    Expected keys:
      symbol, side/direction, stage, reason_code, explanation, score,
      setup_type/pattern, client_id, inputs, thresholds, context
    """
    if not event:
        return

    inputs = event.get("inputs") or {}
    thresholds = event.get("thresholds") or {}
    context = event.get("context") or {}

    details: dict[str, Any] = {}
    explanation = event.get("explanation")
    if explanation:
        details["explanation"] = explanation
    if inputs:
        details["inputs"] = inputs
    if thresholds:
        details["thresholds"] = thresholds
    if context:
        details["context"] = context

    post_rejection(
        ticker=event.get("symbol") or event.get("ticker") or "?",
        side=event.get("side") or event.get("direction") or inputs.get("side") or "—",
        stage=event.get("stage") or "unknown",
        reason=event.get("reason_code") or event.get("reason") or "unknown",
        reason_code=event.get("reason_code"),
        score=float(inputs.get("score") or event.get("score") or 0),
        pattern=event.get("setup_type") or event.get("pattern") or "",
        client_id=event.get("client_id") or "",
        details=details,
    )


# =============================================================================
# DAILY SUMMARY
# =============================================================================

def post_daily_rejection_summary(rejections: list[dict]) -> None:
    """
    Post end-of-day rejection summary to Discord.

    Pass list of dicts with keys:
      ticker, side, stage, reason/reason_code, score
    """
    if not REJECTION_FEED_ENABLED or not rejections:
        return

    try:
        reason_counts = Counter(
            (r.get("reason_code") or r.get("reason") or "unknown")
            for r in rejections
        )
        stage_counts = Counter(r.get("stage", "unknown") for r in rejections)
        ticker_counts = Counter((r.get("ticker") or r.get("symbol") or "?").upper() for r in rejections)

        lines = [
            f"📊 **Daily Rejection Summary** | {len(rejections)} total blocks",
            "",
            "**By Reason:**",
        ]

        for reason, count in reason_counts.most_common(8):
            label = _label_reason(reason)
            lines.append(f"• `{count}x` {label}")

        lines.append("")
        lines.append("**By Stage:**")
        for stage, count in stage_counts.most_common(6):
            emoji = _STAGE_EMOJI.get(stage, "❌")
            lines.append(f"• `{count}x` {emoji} `{stage}`")

        lines.append("")
        lines.append("**Most Blocked Symbols:**")
        for ticker, count in ticker_counts.most_common(8):
            lines.append(f"• `{count}x` **{ticker}**")

        lines.append(f"\n`{_now_et().strftime('%Y-%m-%d %H:%M:%S ET')}`")

        _send_discord_message("\n".join(lines))

    except Exception as e:
        log.debug("Daily summary post failed (non-critical): %s", e)


def post_system_note(message: str, *, stage: str = "system_alert", client_id: str = "") -> None:
    """
    Lightweight operational note for deployment / smoke-test status.
    Use sparingly.
    """
    if not REJECTION_FEED_ENABLED:
        return
    try:
        emoji = _STAGE_EMOJI.get(stage, "ℹ️")
        suffix = f"\nClient: `{_safe_str(client_id)}`" if client_id else ""
        _send_discord_message(
            f"{emoji} **SYSTEM NOTE**\n{_safe_str(message, 1500)}{suffix}\n"
            f"`{_now_et().strftime('%Y-%m-%d %H:%M:%S ET')}`"
        )
    except Exception as e:
        log.debug("System note post failed (non-critical): %s", e)
