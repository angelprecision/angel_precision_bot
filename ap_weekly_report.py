# ap_weekly_report.py — Angel Precision Weekly Performance Report
# =============================================================================
# Run every Friday after market close (or on demand).
#
# Pulls from Supabase signal_outcomes table and generates:
#   1. Tier comparison: A+ vs A vs B shadow
#   2. Win rate + avg return by setup family
#   3. Win rate by time-of-day
#   4. Win rate by chain quality grade
#   5. Drawdown analysis
#   6. Graduating B-tier setups (78–84 that are performing well)
#   7. Setups to remove (consistently losing)
#   8. Next week recommendations
#
# Output: Discord embed + saved to /workspace/reports/weekly_YYYY-MM-DD.txt
# =============================================================================

from __future__ import annotations

import os
import json
import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import Optional

log = logging.getLogger("ap.weekly_report")

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
REPORT_DIR = "/home/user/workspace/reports"


def _safe_pct(wins: int, total: int) -> str:
    if total == 0: return "N/A"
    return f"{wins/total*100:.1f}%"

def _safe_avg(values: list) -> str:
    if not values: return "N/A"
    return f"{sum(values)/len(values):+.1f}%"

def _grade_band(score: float) -> str:
    if score >= 90: return "A+"
    if score >= 85: return "A"
    if score >= 78: return "B"
    return "REJ"


def generate_weekly_report(
    supabase_client=None,
    days_back: int = 7,
    send_discord: bool = True,
    save_file: bool = True,
) -> str:

    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=days_back)).isoformat()
    week_label = now.strftime("%Y-%m-%d")

    # ── Fetch outcomes ────────────────────────────────────────────────────────
    outcomes = []
    if supabase_client:
        try:
            res = supabase_client.table("signal_outcomes") \
                .select("*") \
                .gte("closed_at", since) \
                .execute()
            outcomes = res.data or []
        except Exception as e:
            log.error(f"Supabase fetch failed: {e}")

    if not outcomes:
        return f"No outcomes recorded in the past {days_back} days."

    total   = len(outcomes)
    wins    = sum(1 for o in outcomes if o.get("win"))
    overall_wr = wins / total * 100

    # ── SECTION 1: Tier comparison ────────────────────────────────────────────
    by_tier: dict[str, list] = defaultdict(list)
    for o in outcomes:
        score = float(o.get("signal_score", 0) or 0)
        tier  = _grade_band(score)
        by_tier[tier].append(o)

    tier_section = ["**📊 PERFORMANCE BY TIER**"]
    for tier in ["A+", "A", "B"]:
        trades = by_tier.get(tier, [])
        if not trades: continue
        t_wins   = sum(1 for t in trades if t.get("win"))
        t_wr     = _safe_pct(t_wins, len(trades))
        t_avg    = _safe_avg([t.get("option_pnl_pct", 0) for t in trades])
        t_target = sum(1 for t in trades if t.get("hit_target"))
        paper    = sum(1 for t in trades if "paper" in str(t.get("context_notes","")))
        emoji    = {"A+":"🔴","A":"🟡","B":"⚪"}.get(tier,"")
        live_lbl = f"({paper} paper)" if paper > 0 else ""
        tier_section.append(
            f"{emoji} **{tier}**: {len(trades)} trades {live_lbl} | "
            f"WR: **{t_wr}** | Avg opt return: **{t_avg}** | "
            f"Target hits: {t_target}/{len(trades)}"
        )

    # ── SECTION 2: By setup family ────────────────────────────────────────────
    by_pattern: dict[str, list] = defaultdict(list)
    for o in outcomes:
        key = f"{o.get('pattern','?')} {o.get('side','?')}"
        by_pattern[key].append(o)

    pattern_section = ["**🔬 BY SETUP FAMILY** (≥3 trades)"]
    ranked = sorted(
        [(k, v) for k, v in by_pattern.items() if len(v) >= 3],
        key=lambda x: sum(1 for t in x[1] if t.get("win")) / len(x[1]),
        reverse=True
    )
    for pat, trades in ranked[:10]:
        t_wins = sum(1 for t in trades if t.get("win"))
        t_wr   = _safe_pct(t_wins, len(trades))
        t_avg  = _safe_avg([t.get("option_pnl_pct", 0) for t in trades])
        pattern_section.append(f"  `{pat:25s}` n={len(trades):3d} | WR: {t_wr:6s} | Avg: {t_avg}")

    # ── SECTION 3: By time of day ─────────────────────────────────────────────
    by_tod: dict[str, list] = defaultdict(list)
    for o in outcomes:
        closed = o.get("closed_at","")
        try:
            from zoneinfo import ZoneInfo
            ET = ZoneInfo("America/New_York")
            dt_et = datetime.fromisoformat(closed.replace("Z","+00:00")).astimezone(ET)
            h = dt_et.hour
            if h < 10:   window = "Pre-open"
            elif h < 11: window = "Open (9:30-10)"
            elif h < 12: window = "Mid-morning"
            elif h < 14: window = "Midday"
            elif h < 15: window = "Afternoon"
            else:        window = "Power hour"
        except:
            window = "Unknown"
        by_tod[window].append(o)

    tod_section = ["**⏰ BY TIME OF DAY**"]
    for window in ["Open (9:30-10)","Mid-morning","Midday","Afternoon","Power hour"]:
        trades = by_tod.get(window, [])
        if not trades: continue
        t_wins = sum(1 for t in trades if t.get("win"))
        t_wr   = _safe_pct(t_wins, len(trades))
        tod_section.append(f"  `{window:20s}` n={len(trades):3d} | WR: {t_wr}")

    # ── SECTION 4: By chain quality ───────────────────────────────────────────
    # Inferred from spread — stored in context_notes or scored
    # Use underlying_pnl_pct as proxy (tight spread = easier to get full move)
    high_pnl  = [o for o in outcomes if abs(float(o.get("underlying_pnl_pct",0))) >= 0.5]
    low_pnl   = [o for o in outcomes if abs(float(o.get("underlying_pnl_pct",0))) < 0.3]

    chain_section = ["**⛓ CHAIN QUALITY PROXY**"]
    for label, bucket in [("Strong move (≥0.5% underlying)", high_pnl),
                           ("Weak move  (<0.3% underlying)", low_pnl)]:
        if bucket:
            bw = sum(1 for t in bucket if t.get("win"))
            bwr = _safe_pct(bw, len(bucket))
            chain_section.append(f"  {label} | n={len(bucket)} | WR: {bwr}")

    # ── SECTION 5: Drawdown ───────────────────────────────────────────────────
    pnls = [float(o.get("option_pnl_pct",0)) for o in outcomes]
    peak = 0.0
    max_dd = 0.0
    running = 0.0
    for p in pnls:
        running += p
        if running > peak: peak = running
        dd = peak - running
        if dd > max_dd: max_dd = dd

    dd_section = [
        "**📉 DRAWDOWN**",
        f"  Max drawdown (sum of option returns): **{max_dd:.1f}%**",
        f"  Losing streak: **{_max_losing_streak(outcomes)}** consecutive",
    ]

    # ── SECTION 6: Graduating / Demoting ────────────────────────────────────
    b_trades = by_tier.get("B", [])
    grad_section = []
    if b_trades:
        b_wins = [t for t in b_trades if t.get("win")]
        b_high = [t for t in b_wins if float(t.get("signal_score",0)) >= 82]
        grad_section = [
            "**🟢 B-TIER ANALYSIS**",
            f"  B trades tracked: {len(b_trades)} | B win rate: {_safe_pct(len(b_wins), len(b_trades))}",
        ]
        if b_high:
            grad_section.append(f"  Potential A promotions (score 82–84, winning): {len(b_high)}")
            for t in b_high[:3]:
                grad_section.append(
                    f"    → {t.get('ticker')} {t.get('pattern')} {t.get('side')} "
                    f"score={t.get('signal_score')} ret={t.get('option_pnl_pct'):+.1f}%"
                )

    # ── SECTION 7: Recommendations ────────────────────────────────────────────
    a_plus_trades = by_tier.get("A+", [])
    a_trades      = by_tier.get("A",  [])
    a_plus_wr = (sum(1 for t in a_plus_trades if t.get("win")) / len(a_plus_trades)
                 if a_plus_trades else 0)
    a_wr      = (sum(1 for t in a_trades if t.get("win")) / len(a_trades)
                 if a_trades else 0)
    b_wr_raw  = (sum(1 for t in b_trades if t.get("win")) / len(b_trades)
                 if b_trades else 0)

    recs = ["**🎯 NEXT WEEK RECOMMENDATIONS**"]
    if a_plus_wr >= 0.70:
        recs.append("  ✅ A+ tier performing — maintain current threshold (90)")
    elif a_plus_wr < 0.50 and len(a_plus_trades) >= 5:
        recs.append("  ⚠️ A+ win rate below 50% — review setup family quality or context engine")

    if a_wr >= 0.65:
        recs.append("  ✅ A tier healthy — 60% size appropriate")
    elif a_wr < a_plus_wr - 0.15:
        recs.append("  ⚠️ A tier significantly below A+ — consider raising A threshold to 88")

    if b_wr_raw > a_wr + 0.10:
        recs.append("  🟡 B tier outperforming A — threshold may be too conservative, review")
    elif b_wr_raw < 0.45:
        recs.append("  ✅ B tier weaker than live tiers — threshold is correctly set")

    # ── ASSEMBLE FULL REPORT ──────────────────────────────────────────────────
    sections = [
        f"📈 **ANGEL PRECISION — WEEKLY REPORT** | `{week_label}`",
        f"`{total} total trades | Overall WR: {overall_wr:.1f}% | {wins} wins / {total-wins} losses`",
        "═" * 48,
        "\n".join(tier_section),
        "",
        "\n".join(pattern_section),
        "",
        "\n".join(tod_section),
        "",
        "\n".join(chain_section),
        "",
        "\n".join(dd_section),
    ]
    if grad_section:
        sections += ["", "\n".join(grad_section)]
    sections += ["", "\n".join(recs), "═" * 48]

    report = "\n".join(sections)

    # Save to file
    if save_file:
        os.makedirs(REPORT_DIR, exist_ok=True)
        path = f"{REPORT_DIR}/weekly_{week_label}.txt"
        with open(path, "w") as f:
            f.write(report)
        log.info(f"Weekly report saved to {path}")

    # Send to Discord
    if send_discord and DISCORD_WEBHOOK_URL:
        _send_discord_report(report)

    return report


def _max_losing_streak(outcomes: list) -> int:
    streak = max_streak = 0
    for o in sorted(outcomes, key=lambda x: x.get("closed_at","")):
        if not o.get("win"):
            streak += 1
            max_streak = max(max_streak, streak)
        else:
            streak = 0
    return max_streak


def _send_discord_report(report: str):
    try:
        import requests
        chunks = [report[i:i+1900] for i in range(0, len(report), 1900)]
        for chunk in chunks:
            requests.post(DISCORD_WEBHOOK_URL, json={"content": chunk}, timeout=10)
    except Exception as e:
        log.warning(f"Discord report send failed: {e}")


if __name__ == "__main__":
    # Demo with mock data
    print(generate_weekly_report(supabase_client=None, send_discord=False, save_file=False))
    print("\n(No Supabase connected — run with real client to get live data)")
