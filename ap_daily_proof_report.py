#!/usr/bin/env python3
"""
ap_daily_proof_report.py — Angel Precision Daily Execution Report

Exit codes: 0=clean, 1=health fail, 2=REPORT INVALID (query failed)
"""
import os, sys, json, argparse, urllib.request, urllib.parse
from datetime import datetime, date, timezone

SUPABASE_URL    = os.getenv("SUPABASE_URL", "https://jhawzqnhcihevkhehogm.supabase.co")
SUPABASE_KEY    = os.getenv("SUPABASE_SERVICE_KEY", "")
DISCORD_WEBHOOK = os.getenv("DISCORD_PROOF_WEBHOOK", "")
DEBUG = False

def sb_get(table, filters, select, label=""):
    # filters must be a list of (key, value) tuples to support duplicate
    # column names (e.g. two created_at params: gte.X and lte.Y).
    # A dict would deduplicate them — PostgREST requires both separately.
    param_list = [("select", select)] + list(filters)
    qs  = urllib.parse.urlencode(param_list)
    url = f"{SUPABASE_URL}/rest/v1/{table}?{qs}"
    if DEBUG:
        print(f"  [DEBUG] {label or table}", file=sys.stderr)
        print(f"  [DEBUG] GET .../{table}?{qs}", file=sys.stderr)
    req = urllib.request.Request(url, headers={
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
            if DEBUG:
                print(f"  [DEBUG] -> {len(data)} rows", file=sys.stderr)
            return data
    except urllib.error.HTTPError as e:
        body = ""
        try: body = e.read().decode("utf-8", errors="replace")
        except Exception: pass
        raise RuntimeError(f"HTTP {e.code}: {body[:600]}") from e
    except Exception as e:
        raise RuntimeError(str(e)) from e

def build_report(report_date, debug=False):
    global DEBUG
    DEBUG = debug
    s = f"{report_date}T00:00:00+00:00"
    e2 = f"{report_date}T23:59:59+00:00"
    errs = []

    def fetch(table, col, select, label):
        try:
            return sb_get(table,
                [(col, f"gte.{s}"), (col, f"lte.{e2}")],
                select, label)
        except RuntimeError as ex:
            errs.append(f"{table}: {ex}")
            return None

    signals  = fetch("ap_signals",   "created_at", "signal_id,ticker,side,decision_status", "ap_signals")
    orders_r = fetch("orders",        "created_ts", "id,client_id,status,symbol,kind,limit_price,fill_price,qty,created_ts", "orders")
    proof    = fetch("proof_trades",  "closed_at",  "ticker,side,option_pnl_pct,win,exit_reason,exit_bucket,client_email,seconds_to_fill,slippage_vs_mid,exit_pricing_tier", "proof_trades")
    try:
        rejs = sb_get("decision_events",
            [("timestamp", f"gte.{s}"), ("timestamp", f"lte.{e2}"),
             ("decision", "eq.REJECT")],
            "client_id,symbol,reason_code,stage", "decision_events")
    except Exception:
        rejs = []

    def safe(lst, fn):
        return fn(lst) if lst is not None else None

    entries  = [o for o in (orders_r or []) if (o.get("kind") or "").upper() == "ENTRY"]
    filled   = [o for o in entries if o.get("status") in ("FILLED","ACKNOWLEDGED")]
    canceled = [o for o in entries if o.get("status") == "CANCELED"]
    expired  = [o for o in entries if o.get("status") == "EXPIRED"]

    winners  = [t for t in (proof or []) if t.get("win")]
    losers   = [t for t in (proof or []) if not t.get("win") and t.get("option_pnl_pct") is not None]
    avg_win  = sum(float(t["option_pnl_pct"] or 0)*100 for t in winners)/len(winners) if winners else 0.0
    avg_loss = sum(float(t["option_pnl_pct"] or 0)*100 for t in losers )/len(losers)  if losers  else 0.0

    buckets = {}
    for t in (proof or []):
        b = t.get("exit_bucket") or "UNCLASSIFIED"
        buckets[b] = buckets.get(b, 0) + 1

    manual_exits = buckets.get("MANUAL_EXIT", 0)
    slow_fills   = [t for t in (proof or []) if (t.get("seconds_to_fill") or 0) > 45]
    c_orders     = list(set(o.get("client_id","") for o in entries if o.get("client_id")))
    c_proof      = list(set(t.get("client_email","") for t in (proof or []) if t.get("client_email")))
    rej_by       = {}
    for r in (rejs or []):
        k = r.get("reason_code") or "unknown"
        rej_by[k] = rej_by.get(k, 0) + 1

    data_valid = not errs
    return {
        "date":         str(report_date),
        "mode":         os.getenv("BOT_MODE", "paper").upper(),
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "data_valid":   data_valid,
        "query_errors": errs,
        "signals":  {
            "total":    len(signals) if signals is not None else "QUERY_FAILED",
            "watching": sum(1 for s2 in (signals or []) if s2.get("decision_status")=="WATCHING"),
            "rejected": sum(1 for s2 in (signals or []) if s2.get("decision_status")=="rejected"),
        },
        "entries": {
            "attempted":   len(entries),
            "filled":      len(filled),
            "canceled":    len(canceled),
            "expired":     len(expired),
            "fill_rate":   f"{round(len(filled)/max(len(entries),1)*100,1)}%" if orders_r is not None else "N/A",
        },
        "proof_trades": {
            "total_closed":   len(proof) if proof is not None else "QUERY_FAILED",
            "winners":        len(winners),
            "losers":         len(losers),
            "win_rate_pct":   round(len(winners)/max(len(proof or []),1)*100,1),
            "avg_winner_pct": round(avg_win, 1),
            "avg_loser_pct":  round(avg_loss, 1),
            "manual_exits":   manual_exits,
            "exit_buckets":   buckets,
            "slow_fills_gt45s": len(slow_fills),
        },
        "client_sync": {
            "clients_orders": c_orders,
            "clients_proof":  c_proof,
            "sync_ok": set(c_orders) == set(c_proof) or not (proof or []),
        },
        "rejections":   {"total": len(rejs or []), "by_reason": rej_by},
        "health": {
            "data_valid":       data_valid,
            "target_fills_met": len(filled) >= 5,
            "no_stale_expired": len(expired) == 0,
            "no_manual_exits":  manual_exits == 0,
            "client_sync_ok":   set(c_orders) == set(c_proof) or not (proof or []),
            "clean_run":        data_valid and manual_exits == 0 and not slow_fills and not expired,
        },
        "trades": [
            {"ticker": t.get("ticker"), "side": t.get("side"),
             "pnl_pct": round(float(t.get("option_pnl_pct") or 0)*100, 1),
             "win": t.get("win"), "bucket": t.get("exit_bucket"),
             "reason": (t.get("exit_reason") or "")[:60],
             "client": (t.get("client_email") or "")[:20],
             "fill_secs": t.get("seconds_to_fill"),
             "slip_mid": t.get("slippage_vs_mid")}
            for t in (proof or [])
        ],
    }, errs

def print_report(r, errs):
    print()
    print("=" * 65)
    print("  ANGEL PRECISION — DAILY EXECUTION REPORT")
    print(f"  Date: {r['date']}  |  Mode: {r['mode']}")
    print(f"  Generated: {r['generated_at']}")
    if not r["data_valid"]:
        print()
        print("  ❌❌ REPORT INVALID — DATA QUERIES FAILED ❌❌")
        print("  This report CANNOT be used as proof.")
    print("=" * 65)
    if errs:
        print()
        print("  QUERY ERRORS:")
        for err in errs:
            print(f"  ❌ {err}")
        print()
        print("  Possible causes: wrong table name, wrong column name,")
        print("  timestamp encoding, bad date filter, missing schema.")
        print("  Run with --debug for exact URLs and Supabase error bodies.")
        if not r["data_valid"]:
            print()
            print("  Stopping — no metrics available.")
            print("=" * 65); return
    p  = r["proof_trades"]
    e  = r["entries"]
    cs = r["client_sync"]
    h  = r["health"]
    print()
    print(f"  SIGNALS      {r['signals']['total']:>5}  |  {r['signals']['watching']} watching  |  {r['signals']['rejected']} rejected")
    print(f"  ENTRIES      {e['attempted']:>5}  |  {e['filled']} filled ({e['fill_rate']})  |  {e['canceled']} canceled  |  {e['expired']} expired")
    print(f"  CLOSED       {p['total_closed']:>5}  |  {p['winners']}W / {p['losers']}L  |  win={p['win_rate_pct']}%")
    print(f"  P&L          avg W: {p['avg_winner_pct']:+.1f}%  avg L: {p['avg_loser_pct']:+.1f}%")
    if p["manual_exits"]: print(f"  ⚠️  MANUAL EXITS: {p['manual_exits']} (should be 0)")
    if p["slow_fills_gt45s"]: print(f"  ⚠️  SLOW EXIT FILLS: {p['slow_fills_gt45s']} took >45s")
    print(f"  EXIT BUCKETS {json.dumps(p['exit_buckets'])}")
    print(f"  CLIENT SYNC  orders={cs['clients_orders']}  proof={cs['clients_proof']}  {'✅' if cs['sync_ok'] else '❌ OUT OF SYNC'}")
    print(f"  REJECTIONS   {r['rejections']['total']}  {r['rejections']['by_reason']}")
    if r["trades"]:
        print()
        print("  TRADES ─────────────────────────────────────────────────────")
        for t in r["trades"]:
            icon = "✅" if t["win"] else "❌"
            sl   = f" slip={t['slip_mid']:+.3f}" if t.get("slip_mid") is not None else ""
            fl   = f" {t['fill_secs']:.0f}s" if t.get("fill_secs") else ""
            print(f"  {icon} {t['ticker']:6} {t['side']:4} {t['pnl_pct']:+6.1f}%  {(t['bucket'] or ''):20}{sl}{fl}")
            if t.get("reason"): print(f"       {t['reason']}")
    print()
    print("  HEALTH CHECK")
    for label, ok in [
        ("❌ DATA QUERY FAILURE" if not h["data_valid"] else "Data queries OK", h["data_valid"]),
        ("5+ fills/day target",     h["target_fills_met"]),
        ("No stale expired orders", h["no_stale_expired"]),
        ("No manual exits",         h["no_manual_exits"]),
        ("Client sync",             h["client_sync_ok"]),
        ("Clean run",               h["clean_run"]),
    ]:
        print(f"  {'✅' if ok else '❌'} {label}")
    print("=" * 65); print()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=str(date.today()))
    p.add_argument("--post-discord", action="store_true")
    p.add_argument("--json",  action="store_true")
    p.add_argument("--debug", action="store_true", help="Print exact URLs and Supabase error bodies")
    p.add_argument("--debug", action="store_true")
    args = p.parse_args()
    if not SUPABASE_KEY:
        print("ERROR: SUPABASE_SERVICE_KEY not set"); sys.exit(2)
    rd = date.fromisoformat(args.date)
    print(f"Building report for {rd}...")
    report, errs = build_report(rd, debug=args.debug)
    out = f"/tmp/ap_daily_report_{str(rd).replace('-','')}.json"
    with open(out,"w") as f: json.dump(report, f, indent=2)
    print(f"Saved: {out}")
    if args.json: print(json.dumps(report, indent=2))
    else: print_report(report, errs)
    if errs: sys.exit(2)
    elif not report["health"]["clean_run"]: sys.exit(1)
    else: sys.exit(0)
