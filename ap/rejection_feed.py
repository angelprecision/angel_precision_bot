# ap/rejection_feed.py -- Angel Precision | Discord Rejection Feed
# =============================================================================
# Posts every rejected signal to Discord with full context:
#   - Ticker, side, score, pattern
#   - Stage where it was rejected
#   - Exact reason and threshold that caused rejection
#   - What the values were vs what was needed
#
# Wire into queue._dispatch() after every REJECTED _mark_job() call
# Wire into execution_core after chain health fails
# Wire into contract_selector after every NO_ELIGIBLE_CONTRACTS
# =============================================================================

from __future__ import annotations

import logging
import os
import json
import requests
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("ap.rejection_feed")

DISCORD_WEBHOOK_REJECTIONS = (os.getenv("DISCORD_WEBHOOK_REJECTIONS", "") or "").strip()
REJECTION_FEED_ENABLED = bool(DISCORD_WEBHOOK_REJECTIONS)

# Emoji map by stage
_STAGE_EMOJI = {
    "master_control":    "🚦",
    "contract_filter":   "📋",
    "contract_rank":     "📊",
    "chain_health":      "🔗",
    "risk_sizing":       "💰",
    "capital_util":      "🏦",
    "entry_validation":  "⏱️",
    "broker_submit":     "📤",
    "dedup":             "🔄",
    "entry_cutoff":      "⏰",
    "iv_gate":           "📈",
    "earnings":          "📅",
}

_REASON_LABELS = {
    "duplicate_signal_id":        "🔁 Duplicate signal already in flight",
    "no_contract_found":          "📭 No eligible contracts — spreads too wide or no chain",
    "chain_health_failed":        "🔗 Chain health failed — spread % or volume too low",
    "SPREAD_TOO_WIDE":            "📏 Spread too wide",
    "OI_TOO_LOW":                 "💧 Open interest too low",
    "PREMIUM_CAP_EXCEEDED":       "💸 Premium too expensive",
    "DEEP_OTM":                   "🎯 Strike too far out of the money",
    "IV_RANK_TOO_HIGH":           "📈 IV rank too high — expensive premium",
    "EARNINGS_LOCKOUT":           "📅 Earnings blackout window",
    "CAPITAL_UTIL_BLOCK":         "🏦 Capital utilization cap reached",
    "POSITION_LIMIT_REACHED":     "📊 Max open positions reached",
    "DAILY_STOP_ACTIVE":          "🛑 Daily loss limit hit",
    "SCORE_BELOW_THRESHOLD":      "📉 Signal score below floor",
    "entry_cutoff: too_late_in_session": "⏰ Too late in session — past 3:15 PM ET",
    "overnight_signal_no_watcher": "🌙 Overnight signal — no entry watcher available",
}


def post_rejection(
    ticker: str,
    side: str,
    stage: str,
    reason: str,
    score: float = 0.0,
    pattern: str = "",
    details: dict | None = None,
    client_id: str = "",
) -> None:
    """
    Post a rejection to Discord.
    Call this every time a signal is blocked — anywhere in the stack.
    Non-blocking: all exceptions swallowed so rejections never crash the bot.
    """
    if not REJECTION_FEED_ENABLED:
        return

    try:
        stage_emoji = _STAGE_EMOJI.get(stage, "❌")
        reason_label = _REASON_LABELS.get(reason, reason)
        side_emoji = "🟢" if side == "CALL" else "🔴" if side == "PUT" else "⚪"

        lines = [
            f"{stage_emoji} **REJECTED** | {side_emoji} **{ticker}** {side}",
            f"   Stage: `{stage}`",
            f"   Reason: {reason_label}",
        ]

        if score:
            lines.append(f"   Score: {score}")
        if pattern:
            lines.append(f"   Pattern: `{pattern}`")
        if details:
            for k, v in details.items():
                lines.append(f"   {k}: `{v}`")

        lines.append(f"   `{datetime.now().strftime('%H:%M:%S ET')}`")

        msg = "\n".join(lines)

        requests.post(
            DISCORD_WEBHOOK_REJECTIONS,
            json={"content": msg},
            timeout=5,
        )
    except Exception as e:
        log.debug("Rejection feed post failed (non-critical): %s", e)


def post_chain_health_fail(
    ticker: str,
    spread_pct: float,
    volume: int,
    max_spread: float,
    min_volume: int,
    expiry: str = "",
) -> None:
    """
    Specific helper for chain health failures.
    Called from execution_core when chain health check fails.
    """
    post_rejection(
        ticker=ticker,
        side="—",
        stage="chain_health",
        reason="chain_health_failed",
        details={
            "spread":     f"{spread_pct:.1f}% (max {max_spread:.0f}%)",
            "volume":     f"{volume} (min {min_volume})",
            "expiry":     expiry,
        },
    )


def post_no_contracts(
    ticker: str,
    side: str,
    chain_size: int,
    top_rejections: dict,
    score: float = 0.0,
    pattern: str = "",
) -> None:
    """
    Specific helper for NO_ELIGIBLE_CONTRACTS.
    Called from contract_selector when nothing passes quality gates.
    """
    top = ", ".join(
        f"{k}({v})" for k, v in
        sorted(top_rejections.items(), key=lambda x: -x[1])[:3]
    )
    post_rejection(
        ticker=ticker,
        side=side,
        stage="contract_filter",
        reason="no_contract_found",
        score=score,
        pattern=pattern,
        details={
            "chain_size":      str(chain_size),
            "top_rejections":  top or "none",
        },
    )


def post_master_control_block(
    ticker: str,
    side: str,
    stage: str,
    reason: str,
    score: float = 0.0,
    pattern: str = "",
) -> None:
    """
    Specific helper for master control blocks.
    Called from queue._dispatch() on every REJECTED job.
    """
    post_rejection(
        ticker=ticker,
        side=side,
        stage=stage,
        reason=reason,
        score=score,
        pattern=pattern,
    )


# =============================================================================
# DAILY SUMMARY (call at EOD or on demand)
# =============================================================================

def post_daily_rejection_summary(rejections: list[dict]) -> None:
    """
    Post end-of-day rejection summary to Discord.
    Pass list of dicts with keys: ticker, side, stage, reason, score
    """
    if not REJECTION_FEED_ENABLED or not rejections:
        return

    try:
        from collections import Counter
        reason_counts = Counter(r.get("reason", "unknown") for r in rejections)
        stage_counts  = Counter(r.get("stage", "unknown") for r in rejections)

        lines = [
            f"📊 **Daily Rejection Summary** | {len(rejections)} total blocks",
            "",
            "**By Reason:**",
        ]
        for reason, count in reason_counts.most_common(8):
            label = _REASON_LABELS.get(reason, reason)
            lines.append(f"   {count}x {label}")

        lines.append("")
        lines.append("**By Stage:**")
        for stage, count in stage_counts.most_common(5):
            emoji = _STAGE_EMOJI.get(stage, "❌")
            lines.append(f"   {count}x {emoji} {stage}")

        lines.append(f"\n`{datetime.now().strftime('%Y-%m-%d %H:%M ET')}`")

        requests.post(
            DISCORD_WEBHOOK_REJECTIONS,
            json={"content": "\n".join(lines)},
            timeout=10,
        )
    except Exception as e:
        log.debug("Daily summary post failed (non-critical): %s", e)
