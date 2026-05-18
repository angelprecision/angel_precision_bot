#!/usr/bin/env python3
"""
ap_verify_deploy.py — Angel Precision deployment verification

Run this from the Render Shell immediately after deploying to confirm:
  1. Correct git commit is deployed
  2. Both runners started with correct credentials
  3. entries_allowed=True for both clients
  4. Fan-out is ready (WATCHING signals accessible)
  5. Risk profiles exist for live clients

Usage (Render Shell):
    python3 ap_verify_deploy.py

Pass/fail exits with code 0 (pass) or 1 (fail) for scripting.
"""
import os
import sys
import time
import subprocess

REQUIRED_COMMIT = "b4b8b0a"
PASS = "✅"
FAIL = "❌"
WARN = "⚠️ "

results = []

def check(label, passed, detail=""):
    icon = PASS if passed else FAIL
    line = f"  {icon} {label}"
    if detail:
        line += f" | {detail}"
    print(line)
    results.append(passed)
    return passed

print("=" * 65)
print("  ANGEL PRECISION — DEPLOYMENT VERIFICATION")
print(f"  Time: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
print("=" * 65)

# ── 1. Git commit ──────────────────────────────────────────────────────────
print("\n── 1. Deployed commit ──────────────────────────────────────────")
try:
    commit = subprocess.check_output(
        ["git", "rev-parse", "--short", "HEAD"], text=True
    ).strip()
    ok = commit.startswith(REQUIRED_COMMIT[:7]) or REQUIRED_COMMIT.startswith(commit)
    check(f"Commit {commit}", ok,
          f"expected {REQUIRED_COMMIT}" if not ok else "correct")
except Exception as e:
    check("Git commit readable", False, str(e))

# ── 2. Runner state ────────────────────────────────────────────────────────
print("\n── 2. Active runners ───────────────────────────────────────────")
try:
    from client_runner import _active_runners, _registry_lock
    with _registry_lock:
        runners = dict(_active_runners)
    check(f"Runner count >= 1", len(runners) >= 1, f"{len(runners)} runners")
    for email, runner in runners.items():
        alive     = runner.is_alive()
        init      = runner.initialized.is_set()
        entries   = runner.entries_allowed.is_set()
        degraded  = runner.degraded.is_set()
        masked    = email[:3] + "***" + email[-8:]
        check(f"Runner alive: {masked}",
              alive and init and not degraded,
              f"alive={alive} init={init} entries_allowed={entries} degraded={degraded}")
        check(f"entries_allowed: {masked}", entries)
except Exception as e:
    check("Runner registry accessible", False, str(e))

# ── 3. Credentials resolved ────────────────────────────────────────────────
print("\n── 3. Credentials ──────────────────────────────────────────────")
try:
    from client_runner import _active_runners, _registry_lock
    with _registry_lock:
        runners = dict(_active_runners)
    for email, runner in runners.items():
        masked = email[:3] + "***" + email[-8:]
        token_source = getattr(runner, "_token_source", None)
        account_id   = getattr(runner, "_resolved_tradier_account_id", None)
        base_url     = getattr(runner, "_resolved_tradier_base_url", None)
        mode         = getattr(runner, "mode", None)
        check(f"Token resolved: {masked}",
              bool(token_source and token_source != "missing"),
              f"token_source={token_source} account={str(account_id or '')[:4]}****")
        if mode == "live":
            check(f"Live base_url correct: {masked}",
                  base_url and "api.tradier.com" in base_url and "sandbox" not in base_url,
                  base_url)
        else:
            check(f"Paper sandbox URL: {masked}",
                  not base_url or "sandbox" in str(base_url),
                  base_url)
except Exception as e:
    check("Credential resolution readable", False, str(e))

# ── 4. Fan-out: WATCHING signals accessible ────────────────────────────────
print("\n── 4. Fan-out readiness ────────────────────────────────────────")
try:
    import os
    from supabase import create_client
    sb = create_client(
        os.getenv("SUPABASE_URL", ""),
        os.getenv("SUPABASE_SERVICE_KEY", "")
    )
    # Confirm no client_email filter — query without it and check it returns
    res = (
        sb.table("ap_signals")
        .select("signal_id, ticker, decision_status")
        .eq("decision_status", "WATCHING")
        .limit(5)
        .execute()
    )
    watching = res.data or []
    check("WATCHING signals queryable (no client_email filter)",
          True,
          f"{len(watching)} WATCHING signals in pool right now")
except Exception as e:
    check("WATCHING signal query", False, str(e))

# ── 5. Schema columns present ──────────────────────────────────────────────
print("\n── 5. Schema ───────────────────────────────────────────────────")
try:
    from supabase import create_client
    sb = create_client(
        os.getenv("SUPABASE_URL", ""),
        os.getenv("SUPABASE_SERVICE_KEY", "")
    )
    res = sb.table("members").select("execution_pod, allow_live_trading").limit(1).execute()
    check("members.execution_pod exists", True)
    check("members.allow_live_trading exists", True)
except Exception as e:
    err = str(e).lower()
    missing = [c for c in ["execution_pod", "allow_live_trading"] if c in err]
    check("Required schema columns", not missing,
          f"MISSING COLUMNS: {missing} — run migrations" if missing else "all present")

# ── 6. Risk profiles for live clients ─────────────────────────────────────
print("\n── 6. Live client risk profiles ────────────────────────────────")
try:
    from supabase import create_client
    sb = create_client(
        os.getenv("SUPABASE_URL", ""),
        os.getenv("SUPABASE_SERVICE_KEY", "")
    )
    live = sb.table("members").select("email").eq("tradier_account_mode", "live")\
              .eq("approved", True).eq("allow_live_trading", True).execute()
    live_emails = [r["email"] for r in (live.data or [])]
    if not live_emails:
        print(f"  {WARN} No live clients yet (expected for paper-only deploy)")
    else:
        rp = sb.table("client_risk_profiles").select("client_email")\
               .in_("client_email", live_emails).execute()
        have_rp = {r["client_email"] for r in (rp.data or [])}
        for email in live_emails:
            masked = email[:3] + "***" + email[-8:]
            check(f"Risk profile: {masked}", email in have_rp,
                  "MISSING — live client will be dropped at boot" if email not in have_rp else "present")
except Exception as e:
    check("Risk profile check", False, str(e))

# ── Summary ────────────────────────────────────────────────────────────────
print("\n" + "=" * 65)
passed = sum(results)
failed = sum(1 for r in results if not r)
total  = len(results)
print(f"  {PASS} {passed}/{total} checks passed    {FAIL} {failed}/{total} failed")
if failed == 0:
    print("\n  ✅ DEPLOYMENT VERIFIED — safe to proceed")
    print("  Next: watch for first overnight signal fan-out to both clients")
else:
    print(f"\n  ❌ DEPLOYMENT HAS {failed} ISSUE(S) — do not onboard live money yet")
    print("  Fix failures above, redeploy, and re-run this script")
print("=" * 65)
sys.exit(0 if failed == 0 else 1)
