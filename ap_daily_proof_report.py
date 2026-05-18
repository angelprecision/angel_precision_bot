#!/usr/bin/env python3
"""
ap_daily_proof_report.py — Angel Precision Daily Execution Report

Runs at end of each trading day (or on demand) to produce the proof
document needed to verify the system is working and to show clients.

Usage:
    python3 ap_daily_proof_report.py              # today
    python3 ap_daily_proof_report.py --date 2026-05-19
    python3 ap_daily_proof_report.py --post-discord

Output: printed to stdout + saved to /tmp/ap_daily_report_YYYYMMDD.json
"""

import os, sys, json, argparse
from datetime import datetime, date, timezone, timedelta

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://jhawzqnhcihevkhehogm.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
DISCORD_WEBHOOK = os.getenv("DISCORD_PROOF_WEBHOOK", "")

def sb_get(table, params=""):
    import urllib.request, urllib.parse
    url = f"{SUPABASE_URL}/rest/v1/{table}?{params}"
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    })
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())

def build_report(report_date: date) -> dict:
    day_start = f"{report_date}T00:00:00+00:00"
    day_end   = f"{report_date}T23:59:59+00:00"
    dt_filter = f"gte.{day_start}&created_at=lte.{day_end}"

    # ── Signals generated ──────────────────────────────────────────────────
    try:
        signals = sb_get("ap_signals", f"created_at={dt_filter}&select=signal_id,ticker,side,decision_status")
    except Exception as e:
        signals = []
        print(f"  [warn] ap_signals query failed: {e}", file=sys.stderr)

    # ── Orders (entries attempted, filled, canceled, expired) ─────────────
    try:
        orders = sb_get("orders", f"created_ts={dt_filter}&select=id,client_id,status,symbol,kind,limit_price,fill_price,qty,created_ts,updated_ts,signal_id")
    except Exception as e:
        orders = []
        print(f"  [warn] orders query failed: {e}", file=sys.stderr)

    entry_orders   = [o for o in orders if (o.get("kind") or "").upper() == "ENTRY"]
    exit_orders    = [o for o in orders if (o.get("kind") or "").upper() == "EXIT"]
    filled_entries = [o for o in entry_orders if o.get("status") in ("FILLED", "ACKNOWLEDGED")]
    canceled       = [o for o in entry_orders if o.get("status") in ("CANCELED",)]
    expired        = [o for o in entry_orders if o.get("status") in ("EXPIRED",)]
    stale_canceled = [o for o in canceled if "STALE_ENTRY_CANCEL" in str(o.get("context_notes",""))]

    # ── Proof trades (closed positions) ───────────────────────────────────
    try:
        proof = sb_get("proof_trades", f"closed_at={dt_filter}&select=ticker,side,option_pnl_pct,win,exit_reason,exit_bucket,client_email,seconds_to_fill,slippage_vs_mid,exit_pricing_tier,exit_attempt")
    except Exception as e:
        proof = []
        print(f"  [warn] proof_trades query failed: {e}", file=sys.stderr)

    winners = [t for t in proof if t.get("win")]
    losers  = [t for t in proof if not t.get("win") and t.get("option_pnl_pct") is not None]
    avg_win  = sum(float(t["option_pnl_pct"] or 0) for t in winners) / len(winners) * 100 if winners else 0
    avg_loss = sum(float(t["option_pnl_pct"] or 0) for t in losers)  / len(losers)  * 100 if losers  else 0

    # ── Client sync ────────────────────────────────────────────────────────
    clients_in_orders = list(set(o.get("client_id","") for o in entry_orders if o.get("client_id")))
    clients_in_proof  = list(set(t.get("client_email","") for t in proof if t.get("client_email")))

    # ── Exit quality ───────────────────────────────────────────────────────
    bucket_counts = {}
    for t in proof:
        b = t.get("exit_bucket") or "UNCLASSIFIED"
        bucket_counts[b] = bucket_counts.get(b, 0) + 1

    manual_exits = bucket_counts.get("MANUAL_EXIT", 0)
    fill_delays  = [t for t in proof if (t.get("seconds_to_fill") or 0) > 45]

    # ── Decision events (rejections) ───────────────────────────────────────
    try:
        rejections = sb_get("decision_events",
            f"timestamp={dt_filter}&decision=eq.REJECT&select=client_id,symbol,reason_code,stage")
    except Exception:
        rejections = []

    # ── Assemble report ────────────────────────────────────────────────────
    report = {
        "date":                     str(report_date),
        "mode":                     os.getenv("BOT_MODE", "paper").upper(),
        "generated_at":             datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "signals": {
            "total":                len(signals),
            "watching":             sum(1 for s in signals if s.get("decision_status") == "WATCHING"),
            "armed":                sum(1 for s in signals if s.get("decision_status") == "armed"),
            "rejected":             sum(1 for s in signals if s.get("decision_status") == "rejected"),
        },
        "entries": {
            "attempted":            len(entry_orders),
            "filled":               len(filled_entries),
            "canceled":             len(canceled),
            "expired":              len(expired),
            "stale_canceled":       len(stale_canceled),
            "fill_rate_pct":        round(len(filled_entries) / max(len(entry_orders), 1) * 100, 1),
        },
        "exits": {
            "total":                len(exit_orders),
        },
        "proof_trades": {
            "total_closed":         len(proof),
            "winners":              len(winners),
            "losers":               len(losers),
            "win_rate_pct":         round(len(winners) / max(len(proof), 1) * 100, 1),
            "avg_winner_pct":       round(avg_win, 1),
            "avg_loser_pct":        round(avg_loss, 1),
            "manual_exits":         manual_exits,
            "exit_buckets":         bucket_counts,
            "slow_fills_gt45s":     len(fill_delays),
        },
        "client_sync": {
            "clients_with_orders":  clients_in_orders,
            "clients_with_proof":   clients_in_proof,
            "sync_ok":              set(clients_in_orders) == set(clients_in_proof) or not proof,
        },
        "rejections": {
            "total":                len(rejections),
            "by_reason":            {},
        },
        "health": {
            "clean_run":            (
                manual_exits == 0 and
                len(fill_delays) == 0 and
                len(expired) == 0
            ),
            "target_fills_met":     len(filled_entries) >= 5,
            "stale_orders_clean":   len(expired) == 0,
        },
        "trades": [
            {
                "ticker":    t.get("ticker"),
                "side":      t.get("side"),
                "pnl_pct":   round(float(t.get("option_pnl_pct") or 0) * 100, 1),
                "win":       t.get("win"),
                "bucket":    t.get("exit_bucket"),
                "reason":    (t.get("exit_reason") or "")[:60],
                "client":    (t.get("client_email") or "")[:20],
                "fill_secs": t.get("seconds_to_fill"),
                "slip_mid":  t.get("slippage_vs_mid"),
            }
            for t in proof
        ],
    }

    # Rejection breakdown
    for r in rejections:
        code = r.get("reason_code") or "unknown"
        report["rejections"]["by_reason"][code] = \
            report["rejections"]["by_reason"].get(code, 0) + 1

    return report

def print_report(r: dict):
    h = r["health"]
    p = r["proof_trades"]
    e = r["entries"]
    cs = r["client_sync"]

    print()
    print("=" * 65)
    print(f"  ANGEL PRECISION — DAILY EXECUTION REPORT")
    print(f"  Date: {r['date']}  |  Mode: {r['mode']}")
    print(f"  Generated: {r['generated_at']}")
    print("=" * 65)
    print()
    print(f"  SIGNALS       {r['signals']['total']:>4} generated  |  "
          f"{r['signals']['watching']} watching  |  {r['signals']['rejected']} rejected")
    print()
    print(f"  ENTRIES       {e['attempted']:>4} attempted  |  "
          f"{e['filled']} filled ({e['fill_rate_pct']}%)  |  "
          f"{e['canceled']} canceled  |  {e['expired']} expired")
    if e['stale_canceled']:
        print(f"                {e['stale_canceled']} stale orders correctly auto-canceled")
    print()
    print(f"  CLOSED        {p['total_closed']:>4} positions  |  "
          f"{p['winners']}W / {p['losers']}L  |  "
          f"win={p['win_rate_pct']}%")
    print(f"  P&L           avg winner: {p['avg_winner_pct']:+.1f}%  |  "
          f"avg loser: {p['avg_loser_pct']:+.1f}%")
    if p['manual_exits']:
        print(f"  ⚠️  MANUAL EXITS: {p['manual_exits']} (should be 0 for clean proof)")
    if p['slow_fills_gt45s']:
        print(f"  ⚠️  SLOW EXIT FILLS: {p['slow_fills_gt45s']} took >45s")
    print()
    print(f"  EXIT BUCKETS  {json.dumps(p['exit_buckets'])}")
    print()
    print(f"  CLIENT SYNC   {cs['clients_with_orders']}  ←  orders")
    print(f"                {cs['clients_with_proof']}  ←  proof trades")
    print(f"                {'✅ IN SYNC' if cs['sync_ok'] else '❌ OUT OF SYNC'}")
    print()
    print(f"  REJECTIONS    {r['rejections']['total']} total  |  {r['rejections']['by_reason']}")
    print()

    if r["trades"]:
        print("  TRADES ─────────────────────────────────────────────────────")
        for t in r["trades"]:
            icon = "✅" if t["win"] else "❌"
            slip = f" slip={t['slip_mid']:+.3f}" if t.get("slip_mid") is not None else ""
            fill = f" fill={t['fill_secs']:.0f}s" if t.get("fill_secs") else ""
            print(f"  {icon} {t['ticker']:6} {t['side']:4}  "
                  f"{t['pnl_pct']:+6.1f}%  {(t['bucket'] or 'N/A'):20}{slip}{fill}")
            print(f"     {t['reason']}")
        print()

    print("  HEALTH CHECK ───────────────────────────────────────────────")
    hc = [
        ("5+ fills/day target",     h["target_fills_met"]),
        ("No stale expired orders",  h["stale_orders_clean"]),
        ("No manual exits",          p["manual_exits"] == 0),
        ("Client sync",              cs["sync_ok"]),
        ("Clean run",                h["clean_run"]),
    ]
    for label, ok in hc:
        print(f"  {'✅' if ok else '❌'} {label}")
    print("=" * 65)
    print()

def post_discord(r: dict):
    import urllib.request
    p = r["proof_trades"]; e = r["entries"]
    text = (
        f"**Angel Precision — {r['date']} ({r['mode']})**\n"
        f"Signals: {r['signals']['total']} | Entries: {e['filled']}/{e['attempted']} filled\n"
        f"Closed: {p['total_closed']} | {p['winners']}W/{p['losers']}L | "
        f"Win={p['win_rate_pct']}% | AvgW={p['avg_winner_pct']:+.1f}% AvgL={p['avg_loser_pct']:+.1f}%\n"
        f"Client sync: {'✅' if r['client_sync']['sync_ok'] else '❌'} | "
        f"Manual exits: {'✅ 0' if p['manual_exits']==0 else f'❌ {p[\"manual_exits\"]}'} | "
        f"Stale: {'✅ 0' if e['expired']==0 else f'❌ {e[\"expired\"]}'}"
    )
    data = json.dumps({"content": text}).encode()
    req = urllib.request.Request(DISCORD_WEBHOOK, data=data,
        headers={"Content-Type": "application/json"}, method="POST")
    urllib.request.urlopen(req, timeout=5)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AP Daily Proof Report")
    parser.add_argument("--date", default=str(date.today()),
        help="Report date YYYY-MM-DD (default today)")
    parser.add_argument("--post-discord", action="store_true",
        help="Post summary to Discord proof webhook")
    parser.add_argument("--json", action="store_true",
        help="Print raw JSON instead of formatted report")
    args = parser.parse_args()

    if not SUPABASE_KEY:
        print("ERROR: SUPABASE_SERVICE_KEY not set", file=sys.stderr); sys.exit(1)

    report_date = date.fromisoformat(args.date)
    print(f"Building report for {report_date}...")
    report = build_report(report_date)

    out_path = f"/tmp/ap_daily_report_{str(report_date).replace('-','')}.json"
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"Saved to {out_path}")

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print_report(report)

    if args.post_discord and DISCORD_WEBHOOK:
        post_discord(report)
        print("Posted to Discord.")
    elif args.post_discord:
        print("No DISCORD_PROOF_WEBHOOK set — skipping Discord post.")

    # Exit 1 if health checks fail (useful for CI / alerting)
    sys.exit(0 if report["health"]["clean_run"] else 1)
