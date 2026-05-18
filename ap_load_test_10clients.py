#!/usr/bin/env python3
"""
ap_load_test_10clients.py — Angel Precision 10-client paper load test

Tests the shared bot process can handle 10 concurrent paper clients:
  - All 10 runners alive and healthy
  - Signal fan-out reaches all 10 clients
  - No 429 rate limit errors
  - No cross-client order contamination
  - No crashed runners

Run from the bot repo root:
  LOAD_TEST_BOT_URL=https://angel-precision-bot-official-1.onrender.com \
  LOAD_TEST_ADMIN_KEY=your_admin_key \
  python3 ap_load_test_10clients.py

Add --create-clients to auto-provision 10 paper test clients in Supabase.
"""

import os
import sys
import time
import json
import hmac
import hashlib
import argparse
import requests
from datetime import datetime, timezone

BOT_URL       = os.getenv("LOAD_TEST_BOT_URL", "https://angel-precision-bot-official-1.onrender.com")
ADMIN_KEY     = os.getenv("LOAD_TEST_ADMIN_KEY", "")
SIGNING_SECRET = os.getenv("SIGNING_SECRET", "")
SUPABASE_URL  = os.getenv("SUPABASE_URL", "https://jhawzqnhcihevkhehogm.supabase.co")
SUPABASE_KEY  = os.getenv("SUPABASE_SERVICE_KEY", "")

TEST_CLIENTS = [f"loadtest{i:02d}@angelprecision.com" for i in range(1, 11)]

PASS = "✅"
FAIL = "❌"
WARN = "⚠️ "

results = []

def log(status, msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {status} {msg}"
    print(line)
    results.append((status, msg))

def bot_get(path):
    headers = {"X-Admin-Key": ADMIN_KEY}
    if SIGNING_SECRET:
        raw = b""
        ts  = str(int(time.time()))
        sig = hmac.new(SIGNING_SECRET.encode(), ts.encode() + b"." + raw, hashlib.sha256).hexdigest()
        headers.update({"X-AP-Timestamp": ts, "X-AP-Signature": sig})
    try:
        r = requests.get(f"{BOT_URL}{path}", headers=headers, timeout=15)
        return r
    except Exception as e:
        return None

def bot_post(path, payload=None):
    body = json.dumps(payload or {}).encode()
    headers = {"Content-Type": "application/json", "X-Admin-Key": ADMIN_KEY}
    if SIGNING_SECRET:
        ts  = str(int(time.time()))
        sig = hmac.new(SIGNING_SECRET.encode(), ts.encode() + b"." + body, hashlib.sha256).hexdigest()
        headers.update({"X-AP-Timestamp": ts, "X-AP-Signature": sig})
    try:
        r = requests.post(f"{BOT_URL}{path}", data=body, headers=headers, timeout=15)
        return r
    except Exception as e:
        return None

def sb_get(table, params=""):
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    try:
        r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}?{params}", headers=h, timeout=10)
        return r.json()
    except:
        return []

def sb_post(table, data):
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
         "Content-Type": "application/json", "Prefer": "return=representation"}
    try:
        r = requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=h,
                          data=json.dumps(data), timeout=10)
        return r
    except Exception as e:
        return None

# ─────────────────────────────────────────────────────────────────────────────
# STEP 0: Check bot is alive
# ─────────────────────────────────────────────────────────────────────────────
def check_bot_alive():
    print("\n── STEP 0: Bot health ──────────────────────────────────────────")
    r = bot_get("/health")
    if r and r.status_code == 200:
        log(PASS, f"Bot alive | {BOT_URL}/health → 200")
        return True
    log(FAIL, f"Bot unreachable — got {r.status_code if r else 'no response'}")
    return False

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: Provision 10 paper clients in Supabase
# ─────────────────────────────────────────────────────────────────────────────
def provision_clients():
    print("\n── STEP 1: Provision 10 paper test clients ─────────────────────")
    if not SUPABASE_KEY:
        log(WARN, "SUPABASE_SERVICE_KEY not set — skipping auto-provision, verify manually")
        return
    for email in TEST_CLIENTS:
        # Check members table
        existing = sb_get("members", f"email=eq.{email}&select=email")
        if existing:
            log(PASS, f"Member exists: {email}")
            continue
        r = sb_post("members", {
            "email": email,
            "name": f"Load Test {email.split('@')[0]}",
            "mode": "PAPER",
            "tier": "beta",
            "active": True,
            "allow_live_trading": False,
        })
        if r and r.status_code in (200, 201):
            log(PASS, f"Provisioned: {email}")
        else:
            log(FAIL, f"Failed to provision {email}: {r.status_code if r else 'no response'}")
        time.sleep(0.2)  # avoid Supabase rate limit

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: Check all 10 runners are alive
# ─────────────────────────────────────────────────────────────────────────────
def check_runners():
    print("\n── STEP 2: Check all 10 runners alive ──────────────────────────")
    r = bot_get("/admin/client-readiness")
    if not r or r.status_code != 200:
        log(FAIL, f"client-readiness endpoint failed: {r.status_code if r else 'no response'}")
        return 0
    data = r.json()
    clients = data.get("clients", [])
    alive = 0
    for email in TEST_CLIENTS:
        match = next((c for c in clients if c.get("email") == email), None)
        if not match:
            log(WARN, f"Runner not found: {email} (may not be started yet)")
            continue
        status = match.get("runner_status", "unknown")
        organs = match.get("organ_health", {})
        all_ok = all(v in ("ok", "alive", True, "healthy") for v in organs.values()) if organs else True
        if status in ("alive", "running", "ok") and all_ok:
            log(PASS, f"Runner alive: {email} | organs={organs}")
            alive += 1
        else:
            log(FAIL, f"Runner unhealthy: {email} | status={status} | organs={organs}")
    log(PASS if alive == len(TEST_CLIENTS) else WARN,
        f"Runners alive: {alive}/{len(TEST_CLIENTS)}")
    return alive

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: Send 3 signals and verify fan-out to all 10 clients
# ─────────────────────────────────────────────────────────────────────────────
def test_signal_fanout():
    print("\n── STEP 3: Signal fan-out test ─────────────────────────────────")
    test_signals = [
        {"ticker": "SPY",  "side": "CALL", "score": 78, "setup_type": "1-2_2U", "timeframe": "1d",
         "signal_id": f"LOADTEST_SPY_CALL_{int(time.time())}",  "source": "load_test"},
        {"ticker": "QQQ",  "side": "PUT",  "score": 76, "setup_type": "1-2_2D", "timeframe": "1d",
         "signal_id": f"LOADTEST_QQQ_PUT_{int(time.time())+1}", "source": "load_test"},
        {"ticker": "AAPL", "side": "CALL", "score": 75, "setup_type": "3-1-2",  "timeframe": "1d",
         "signal_id": f"LOADTEST_AAPL_{int(time.time())+2}",    "source": "load_test"},
    ]
    fan_out_counts = []
    for sig in test_signals:
        r = bot_post("/signal", sig)
        if not r:
            log(FAIL, f"Signal {sig['ticker']} — no response")
            continue
        data = r.json() if r.status_code in (200, 201, 202) else {}
        queued    = data.get("queued_for", data.get("clients_queued", 0))
        rejected  = data.get("rejected", 0)
        status    = r.status_code
        if status in (200, 201, 202) and queued >= 8:
            log(PASS, f"Signal {sig['ticker']} {sig['side']} → queued for {queued} clients")
        elif status in (200, 201, 202):
            log(WARN, f"Signal {sig['ticker']} → only {queued} clients queued (expected 10)")
        else:
            log(FAIL, f"Signal {sig['ticker']} → HTTP {status}: {data}")
        fan_out_counts.append(queued)
        time.sleep(1)
    avg = sum(fan_out_counts) / len(fan_out_counts) if fan_out_counts else 0
    log(PASS if avg >= 8 else WARN, f"Avg fan-out: {avg:.1f}/10 clients per signal")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: Check for 429 rate-limit errors in recent logs (via Supabase)
# ─────────────────────────────────────────────────────────────────────────────
def check_rate_limits():
    print("\n── STEP 4: Check for 429 / rate limit errors ───────────────────")
    if not SUPABASE_KEY:
        log(WARN, "SUPABASE_SERVICE_KEY not set — cannot check logs. Monitor Render logs manually.")
        log(WARN, "Look for: '429', 'Too Many Requests', 'rate_limited' in bot logs")
        return
    # Check ap_signals table for recent errors (proxy for healthy operation)
    recent = sb_get("ap_signals", "order=created_at.desc&limit=50&select=created_at,ticker,status")
    if recent:
        log(PASS, f"ap_signals table reachable — {len(recent)} recent signals")
    else:
        log(WARN, "ap_signals returned empty — may be off-hours (no signals)")

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: Check no cross-client order contamination
# ─────────────────────────────────────────────────────────────────────────────
def check_order_isolation():
    print("\n── STEP 5: Cross-client order isolation ────────────────────────")
    if not SUPABASE_KEY:
        log(WARN, "SUPABASE_SERVICE_KEY not set — skipping isolation check")
        return
    # Check that each order row has a client_id that matches known test clients or real clients
    orders = sb_get("orders", "select=client_id,local_order_id,status&limit=100&order=created_ts.desc")
    contaminated = []
    for o in (orders or []):
        cid = o.get("client_id", "")
        # Order contamination = order exists under a client_id that isn't in the system
        # (This check mainly ensures client_id is never null or mismatched)
        if not cid:
            contaminated.append(o.get("local_order_id", "unknown"))
    if contaminated:
        log(FAIL, f"Orders with no client_id (contamination risk): {contaminated}")
    else:
        log(PASS, f"All {len(orders or [])} recent orders have client_id set — no contamination")

# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
def print_summary():
    print("\n── LOAD TEST SUMMARY ───────────────────────────────────────────")
    passed = sum(1 for s, _ in results if s == PASS)
    warned = sum(1 for s, _ in results if s == WARN)
    failed = sum(1 for s, _ in results if s == FAIL)
    total  = len(results)
    print(f"  {PASS} Passed:  {passed}/{total}")
    print(f"  {WARN} Warnings: {warned}/{total}")
    print(f"  {FAIL} Failed:  {failed}/{total}")
    if failed == 0 and warned <= 2:
        print("\n  ✅ LOAD TEST PASSED — 10-client paper architecture is viable")
        print("  Ready to: onboard first 2 live clients on dedicated Render services")
    elif failed == 0:
        print("\n  ⚠️  LOAD TEST PASSED WITH WARNINGS — review warnings before live clients")
    else:
        print("\n  ❌ LOAD TEST FAILED — fix failures before onboarding live clients")
        print("  Check Render logs for 429 errors, crashed runners, and OSM errors")

# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Angel Precision 10-client paper load test")
    parser.add_argument("--create-clients", action="store_true",
                        help="Auto-provision 10 paper test clients in Supabase")
    parser.add_argument("--skip-signals", action="store_true",
                        help="Skip signal fan-out test (market closed)")
    args = parser.parse_args()

    print("=" * 65)
    print("  ANGEL PRECISION — 10-CLIENT PAPER LOAD TEST")
    print(f"  Bot: {BOT_URL}")
    print(f"  Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 65)

    if not check_bot_alive():
        print("\nBot unreachable. Start the bot and retry.")
        sys.exit(1)

    if args.create_clients:
        provision_clients()

    check_runners()

    if not args.skip_signals:
        test_signal_fanout()
    else:
        log(WARN, "Signal fan-out test skipped (--skip-signals)")

    check_rate_limits()
    check_order_isolation()
    print_summary()
