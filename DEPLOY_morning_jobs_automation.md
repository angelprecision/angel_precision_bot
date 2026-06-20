# Deploy Requirements — Live Morning Jobs Automation (PR #163)

This automation has **two deploy-time requirements** that must be satisfied for
it to work correctly and safely. Read this before merging/deploying.

---

## 1. REQUIRED: Apply the run-lock migration before relying on idempotency

```
migrations/2026_06_20_handoff_run_locks.sql
```

This migration creates `public.handoff_run_locks`, the table that enforces
single execution when Render Cron (primary) and the GitHub Actions backup could
both target the same job window.

**Apply it on Supabase before the first market open that uses the automation:**

```bash
psql "$DATABASE_URL" -f migrations/2026_06_20_handoff_run_locks.sql
# or run the SQL in the Supabase SQL editor
```

Verify:

```sql
\d public.handoff_run_locks          -- table exists, run_key is PRIMARY KEY
```

### What happens if the table does NOT exist

The run-lock helper (`ap_handoff_run_lock.py`) **fails open**: if the table is
missing or the DB errors, `try_acquire_run_lock()` returns `True` and the job
runs anyway. This means:

- Jobs are NOT blocked by a missing table (safe — automation still functions).
- BUT duplicate-execution protection is **inactive** until the table exists.

So a missing migration degrades to the prior endpoint-level idempotency
(`paper_rescue_restart_guard` WHERE-guard, `release_after_hours_deferred`
status-transition guard, `morning_handoff_audit` idempotent re-arm). That is
still safe, but the cross-scheduler run-lock that prevents a Render+GHA double
on `morning_handoff_audit` is not protecting you until the table is created.

**Bottom line:** apply the migration to get the duplicate-Render/GHA protection
the PR advertises. Until then, automation works but relies on the weaker
endpoint-level idempotency.

---

## 2. REQUIRED: Set cron service env vars on Render

The scheduler-agnostic runner (`ap/scripts/live_morning_jobs.py`) reads:

| Env var          | Purpose                                   |
|------------------|-------------------------------------------|
| `BOT_URL`        | Bot admin base URL (the live pod)         |
| `SIGNING_SECRET` | HMAC signing secret shared with the bot   |
| `MORNING_JOB`    | Set per cron service (see `render.yaml`)   |
| `TZ`             | `America/New_York` (process clock only)   |

These are declared `sync: false` in `render.yaml` and must be set in the Render
dashboard for each cron service (secrets are not committed).

For the GitHub Actions backup, the same values are read from repo secrets
`AP_BOT_URL` and `AP_SIGNING_SECRET`.

---

## 3. OPTIONAL: Live auto-release of after-hours-deferred rows (OFF by default)

`morning_recovery` runs `release_after_hours_deferred` scoped to **paper clients
only** unless you explicitly opt in:

```
ENABLE_LIVE_AUTO_RELEASE_AFTER_OPEN=true
```

Set this in the `ap-morning-recovery-*` Render cron service env ONLY if you want
the live client's after-hours-deferred rows auto-released at open. Leaving it
unset (the default) means live rows are never auto-released by the scheduler —
the operator must release them manually via the dashboard / admin endpoint.

---

## 4. Scheduling facts (already handled in config, listed for awareness)

- **Render cron is UTC.** `TZ` does not affect when cron fires (only the process
  clock). `render.yaml` therefore uses DST-safe dual UTC schedules
  (13:xx for EDT, 14:xx for EST) per ET window. The script's `should_run_now()`
  ET-window guard runs only the season-correct tick and skips the other.
- **GitHub Actions is backup-only**, running once at a later recovery window
  (09:50 ET = 13:50/14:50 UTC) and executing only the idempotent
  `morning_recovery` batch — it does not race the Render primary windows.

---

## Recommended rollout order

1. Apply the migration on Supabase (step 1) and verify the table exists.
2. Set the Render cron env vars (step 2). Do NOT set
   `ENABLE_LIVE_AUTO_RELEASE_AFTER_OPEN` yet.
3. Create the **paper-relevant** Render cron services first; leave live cron
   services uncreated or paused.
4. Watch one full market open on paper. Confirm the structured logs show the
   jobs firing on schedule and the run-lock acquiring/skipping as expected.
5. Only then create/enable the live cron services.
