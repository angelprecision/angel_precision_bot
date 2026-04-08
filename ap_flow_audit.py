# ap_flow_audit.py — Angel Precision Trade Flow Debug Audit
# =============================================================================
# Answers the one question that matters when the bot isn't trading:
# WHERE IS IT DYING?
#
# Sources:
#   1. ap_signals Supabase table  — status counts per decision_status
#   2. funnel.snapshot()          — in-memory gate counters (reset daily)
#
# Usage:
#   from ap_flow_audit import flow_audit
#   report = flow_audit(supabase_client=sb)
#   print(report["table"])          # plain text choke-point table
#   print(report["discord"])        # Discord-formatted version
#   print(report["bottleneck"])     # "OPTIONS_GATE" / "SCORE_GATE" / etc.
#
# Flask endpoint (add to app.py):
#   @app.route("/debug/flow")
#   def debug_flow():
#       from ap_flow_audit import flow_audit
#       return jsonify(flow_audit(supabase_client=sb))
#
# Standalone (run after market):
#   python ap_flow_audit.py
# =============================================================================

from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional

log = logging.getLogger("ap.flow_audit")
ET  = ZoneInfo("America/New_York")


# =============================================================================
# STEP DEFINITIONS — the full signal lifecycle in order
# =============================================================================

# Each step: (key, label, source)
# source = "supabase" | "funnel" | "derived"
STEPS = [
    ("received",              "Signals received",              "supabase"),
    ("legacy_routed",         "Legacy routed",                 "supabase"),
    ("rejected_score",        "Rejected — score too low",      "derived"),   # received - passed_score
    ("context_blocked",       "Rejected — context blocked",    "supabase"),
    ("shadow",                "Shadow tracked (B-tier)",       "supabase"),
    ("dropped",               "Dropped (sector cap / recheck)","supabase"),
    ("queued",                "Queued (A/A+)",                 "supabase"),
    ("queue_expired",         "Expired in queue",              "funnel"),
    ("watching",              "Sent to watcher",               "supabase"),
    ("expired",               "Expired — no breach",           "supabase"),
    ("invalidated",           "Invalidated — wrong direction", "supabase"),
    ("triggered",             "Breach confirmed",              "supabase"),
    ("options_rejected",      "Rejected — options gate",       "funnel"),
    ("requeued_after_trigger","Requeued (positions full)",     "supabase"),
    ("executed",              "Executed (order placed)",       "supabase"),
    ("order_failed",          "Order failed",                  "funnel"),
    ("closed",                "Closed",                        "supabase"),
]


# =============================================================================
# FETCH FROM SUPABASE
# =============================================================================

def _fetch_supabase_counts(sb, hours_back: int = 24) -> dict[str, int]:
    """
    Count rows in ap_signals by decision_status for the last N hours.
    Also counts rows where context_notes contains 'sector_cap' or 'context_recheck'
    to break down the 'dropped' bucket.
    """
    since = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
    counts: dict[str, int] = {}

    try:
        res = sb.table("ap_signals") \
            .select("decision_status, context_notes") \
            .gte("created_at", since) \
            .execute()
        rows = res.data or []
    except Exception as e:
        log.warning("ap_signals fetch failed: %s", e)
        return counts

    for row in rows:
        status = (row.get("decision_status") or "unknown").lower()
        counts[status] = counts.get(status, 0) + 1

    # Break down 'dropped' further using context_notes
    sector_cap_count     = sum(1 for r in rows
                               if "sector_cap" in (r.get("context_notes") or ""))
    context_recheck_count = sum(1 for r in rows
                                if "context_recheck_fail" in (r.get("context_notes") or ""))
    counts["_sector_cap_drop"]     = sector_cap_count
    counts["_context_recheck_drop"] = context_recheck_count

    # Count score rejections from context_notes
    score_reject_count = sum(1 for r in rows
                             if r.get("decision_status") == "rejected"
                             and "score" in (r.get("context_notes") or "").lower())
    counts["_score_rejected"] = score_reject_count

    return counts


# =============================================================================
# MERGE FUNNEL + SUPABASE
# =============================================================================

def _build_counts(sb=None, hours_back: int = 24) -> dict[str, int]:
    """Merge in-memory funnel snapshot with Supabase counts."""
    try:
        from ap_proof_logger import funnel
        snap = funnel.snapshot()
    except Exception:
        snap = {}

    supabase_counts: dict[str, int] = {}
    if sb:
        supabase_counts = _fetch_supabase_counts(sb, hours_back)

    # Merge: prefer Supabase for status-based counts, funnel for gate-level counts
    merged: dict[str, int] = {}

    # From Supabase
    merged["received"]               = supabase_counts.get("received", 0)
    merged["legacy_routed"]          = supabase_counts.get("legacy_routed", 0)
    merged["context_blocked"]        = supabase_counts.get("context_blocked", 0)
    merged["shadow"]                 = supabase_counts.get("shadow", 0)
    merged["dropped"]                = supabase_counts.get("dropped", 0)
    merged["queued"]                 = supabase_counts.get("queued", 0)
    merged["watching"]               = supabase_counts.get("watching", 0)
    merged["expired"]                = supabase_counts.get("expired", 0)
    merged["invalidated"]            = supabase_counts.get("invalidated", 0)
    merged["triggered"]              = supabase_counts.get("triggered", 0)
    merged["requeued_after_trigger"] = supabase_counts.get("requeued_after_trigger", 0)
    merged["executed"]               = supabase_counts.get("executed", 0)
    merged["closed"]                 = supabase_counts.get("closed", 0)

    # From funnel (in-memory, more granular for gates)
    merged["queue_expired"]   = snap.get("queue_expired", 0)
    merged["options_rejected"] = snap.get("options_rejected", 0)
    merged["order_failed"]    = snap.get("order_failed", 0)

    # Derived: score rejections
    # If Supabase has the breakdown, use it; else derive from funnel
    supabase_score_rejected = supabase_counts.get("_score_rejected", 0)
    funnel_score_rejected   = snap.get("signals_received", 0) - snap.get("passed_score_filter", 0)
    merged["rejected_score"] = max(supabase_score_rejected, max(funnel_score_rejected, 0))

    # Drop breakdown detail
    merged["_sector_cap_drop"]      = supabase_counts.get("_sector_cap_drop", 0)
    merged["_context_recheck_drop"] = supabase_counts.get("_context_recheck_drop", 0)

    # Regime context
    merged["_regime"]       = snap.get("regime", "unknown")
    merged["_trend_day"]    = snap.get("was_trend_day", False)

    return merged


# =============================================================================
# CHOKE-POINT IDENTIFIER
# =============================================================================

def _find_bottleneck(counts: dict[str, int]) -> tuple[str, str]:
    """
    Identify the single biggest choke point.
    Returns (key, plain-english label).
    """
    received = counts.get("received", 0)
    if received == 0:
        return "NO_SIGNALS", "No signals received — check scanner → execution_core connection"

    drops = {
        "SCORE_GATE":      counts.get("rejected_score", 0),
        "CONTEXT_GATE":    counts.get("context_blocked", 0),
        "SHADOW_TIER":     counts.get("shadow", 0),
        "SECTOR_CAP":      counts.get("_sector_cap_drop", 0),
        "CONTEXT_RECHECK": counts.get("_context_recheck_drop", 0),
        "QUEUE_EXPIRY":    counts.get("queue_expired", 0),
        "WATCHER_EXPIRY":  counts.get("expired", 0),
        "WATCHER_INVALID": counts.get("invalidated", 0),
        "OPTIONS_GATE":    counts.get("options_rejected", 0),
        "REQUEUE_FULL":    counts.get("requeued_after_trigger", 0),
        "ORDER_FAILURE":   counts.get("order_failed", 0),
    }

    biggest_key = max(drops, key=lambda k: drops[k])
    biggest_val = drops[biggest_key]

    if biggest_val == 0:
        return "NONE", "No dominant choke point — flow looks healthy"

    labels = {
        "SCORE_GATE":      f"Score gate killing most signals — floor may be too high",
        "CONTEXT_GATE":    f"Context gate blocking most signals — regime filter too strict or market choppy",
        "SHADOW_TIER":     f"Most signals landing in B-tier shadow — edge scores need improvement",
        "SECTOR_CAP":      f"Sector cap dropping signals — correlated market moves causing cap hits",
        "CONTEXT_RECHECK": f"Context re-check killing signals after queue — conditions degrading",
        "QUEUE_EXPIRY":    f"Queue expiring signals — signals queued but no slots opening in 120s",
        "WATCHER_EXPIRY":  f"Watcher expiring — price never breaching trigger level",
        "WATCHER_INVALID": f"Watcher invalidating — price going wrong direction before trigger",
        "OPTIONS_GATE":    f"Options gate rejecting — spread/IV/liquidity failing at execution",
        "REQUEUE_FULL":    f"Positions full at breach — MAX_POSITIONS cap too low for flow",
        "ORDER_FAILURE":   f"Orders failing at broker — Tradier API or account issue",
    }

    return biggest_key, labels.get(biggest_key, biggest_key)


# =============================================================================
# FORMAT OUTPUT
# =============================================================================

def _pct(part: int, whole: int) -> str:
    if whole == 0:
        return "  —  "
    return f"{part/whole*100:5.1f}%"


def _build_table(counts: dict[str, int], bottleneck_key: str) -> str:
    received = counts.get("received", 0)

    rows = [
        ("received",              "Signals received",               received),
        ("legacy_routed",         "  ↳ Legacy routed",              counts.get("legacy_routed", 0)),
        ("rejected_score",        "  ✗ Score too low",              counts.get("rejected_score", 0)),
        ("context_blocked",       "  ✗ Context blocked",            counts.get("context_blocked", 0)),
        ("shadow",                "  → Shadow (B-tier)",            counts.get("shadow", 0)),
        ("dropped",               "  ✗ Dropped (sector/recheck)",   counts.get("dropped", 0)),
        ("queued",                "  → Queued (A/A+)",              counts.get("queued", 0)),
        ("queue_expired",         "    ✗ Expired in queue",         counts.get("queue_expired", 0)),
        ("watching",              "  → Sent to watcher",            counts.get("watching", 0)),
        ("expired",               "    ✗ Expired (no breach)",      counts.get("expired", 0)),
        ("invalidated",           "    ✗ Invalidated (wrong dir)",  counts.get("invalidated", 0)),
        ("triggered",             "  → Breach confirmed",           counts.get("triggered", 0)),
        ("options_rejected",      "    ✗ Options gate rejected",    counts.get("options_rejected", 0)),
        ("requeued_after_trigger","    ↺ Requeued (pos full)",      counts.get("requeued_after_trigger", 0)),
        ("executed",              "  → ORDER PLACED",               counts.get("executed", 0)),
        ("order_failed",          "    ✗ Order failed",             counts.get("order_failed", 0)),
        ("closed",                "  ✓ CLOSED",                     counts.get("closed", 0)),
    ]

    lines = [
        "═" * 58,
        "  ANGEL PRECISION — SIGNAL FLOW AUDIT",
        f"  Regime: {counts.get('_regime','?')} | "
        f"Trend day: {counts.get('_trend_day', False)}",
        "─" * 58,
        f"  {'STEP':<38} {'COUNT':>6}  {'OF RECV':>7}",
        "─" * 58,
    ]

    for key, label, count in rows:
        flag = " ◄◄ CHOKE" if key == bottleneck_key and count > 0 else ""
        lines.append(f"  {label:<38} {count:>6}  {_pct(count, received):>7}{flag}")

    lines += [
        "═" * 58,
    ]

    # Drop breakdown detail if relevant
    sc = counts.get("_sector_cap_drop", 0)
    cr = counts.get("_context_recheck_drop", 0)
    if sc or cr:
        lines += [
            "  Dropped breakdown:",
            f"    Sector cap:       {sc}",
            f"    Context recheck:  {cr}",
            "─" * 58,
        ]

    return "\n".join(lines)


def _build_discord(counts: dict[str, int], bottleneck_key: str, bottleneck_label: str) -> str:
    received = counts.get("received", 0)
    executed = counts.get("executed", 0)
    closed   = counts.get("closed", 0)

    def row(label, count, emoji=""):
        pct_str = f"`{count/received*100:.1f}%`" if received > 0 else "`—`"
        return f"> {emoji} {label}: **{count}** ({pct_str})"

    lines = [
        "📡 **ANGEL PRECISION — FLOW AUDIT**",
        f"`Regime: {counts.get('_regime','?')}` | "
        f"`Trend: {counts.get('_trend_day', False)}`",
        "─" * 36,
        row("Signals received",        received,                            "📥"),
        row("Score gate killed",        counts.get("rejected_score", 0),    "❌"),
        row("Context gate killed",      counts.get("context_blocked", 0),   "❌"),
        row("B-tier shadow",            counts.get("shadow", 0),            "⚪"),
        row("Dropped (cap/recheck)",    counts.get("dropped", 0),           "❌"),
        row("Queued (A/A+)",            counts.get("queued", 0),            "🟡"),
        row("Watcher expired",          counts.get("expired", 0),           "⏰"),
        row("Watcher invalidated",      counts.get("invalidated", 0),       "↩️"),
        row("Breach confirmed",         counts.get("triggered", 0),         "🎯"),
        row("Options gate killed",      counts.get("options_rejected", 0),  "❌"),
        row("Orders placed",            executed,                            "✅"),
        row("Orders failed",            counts.get("order_failed", 0),      "🚨"),
        row("Positions closed",         closed,                              "💰"),
        "─" * 36,
        f"🔎 **Biggest choke:** `{bottleneck_key}`",
        f"> {bottleneck_label}",
    ]

    return "\n".join(lines)


# =============================================================================
# MAIN AUDIT FUNCTION
# =============================================================================

def flow_audit(
    supabase_client=None,
    hours_back: int = 24,
    send_discord: bool = False,
    discord_webhook: str = "",
) -> dict:
    """
    Run a full signal flow audit.

    Args:
        supabase_client: Supabase client (uses ap_signals table)
        hours_back:      Look back window in hours (default 24 = today's session)
        send_discord:    Whether to post the report to Discord
        discord_webhook: Discord webhook URL (or set DISCORD_WEBHOOK_URL env var)

    Returns:
        {
          "counts":      dict of raw counts per step,
          "bottleneck":  key of the biggest choke point,
          "diagnosis":   plain-english diagnosis,
          "table":       formatted plain-text table,
          "discord":     Discord-formatted version,
          "timestamp":   ISO timestamp,
        }
    """
    counts             = _build_counts(supabase_client, hours_back)
    bottleneck_key, bottleneck_label = _find_bottleneck(counts)
    table              = _build_table(counts, bottleneck_key)
    discord_msg        = _build_discord(counts, bottleneck_key, bottleneck_label)

    print(table)

    if send_discord:
        webhook = discord_webhook or os.getenv("DISCORD_WEBHOOK_URL", "").strip()
        if webhook:
            try:
                import requests
                requests.post(webhook, json={"content": discord_msg}, timeout=10)
            except Exception as e:
                log.warning("Discord send failed: %s", e)

    return {
        "counts":      counts,
        "bottleneck":  bottleneck_key,
        "diagnosis":   bottleneck_label,
        "table":       table,
        "discord":     discord_msg,
        "timestamp":   datetime.now(timezone.utc).isoformat(),
    }


# =============================================================================
# STANDALONE — run after market
# =============================================================================

if __name__ == "__main__":
    import json

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

    sb_url = os.getenv("SUPABASE_URL", "")
    sb_key = os.getenv("SUPABASE_SERVICE_KEY", "")

    sb = None
    if sb_url and sb_key:
        from supabase import create_client
        sb = create_client(sb_url, sb_key)
        print("✅ Connected to Supabase\n")
    else:
        print("⚠️  No Supabase — showing in-memory funnel only\n")

    result = flow_audit(
        supabase_client = sb,
        hours_back      = 24,
        send_discord    = bool(os.getenv("DISCORD_WEBHOOK_URL")),
    )

    print(f"\nBottleneck: {result['bottleneck']}")
    print(f"Diagnosis:  {result['diagnosis']}")
