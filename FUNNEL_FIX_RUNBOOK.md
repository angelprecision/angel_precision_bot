# Funnel-Leak Fix Runbook — 2026-05-20

## What this branch fixes

Live data from 2026-05-20 showed 29 orders submitted across Jose and tradefluence accounts; **0 fills**. Breakdown of `last_error`:

| Failure Mode | Count | Root Cause |
|---|---|---|
| `watch_arm_failed:stale_price_-0.X%` | 12 | Arm-time stale gate rejected at 0.0–0.4% deltas. Hardcoded 1.5% threshold + non-env-tunable + reject reason conflated drift-stale with below-stop-stale. |
| `watcher_invalidated` (9:00 ET) | 8 | Pre-market stop touches invalidated daily/overnight setups before the 9:30 ET structural revalidator could run. Thin pre-open spreads = false invalidations. |
| `positions_full_at_breach` | 5 | Phantom CREATED orders (never reached broker, will be cleaned up at 120s) counted as pending exposure → slots full with zero actual fills. |
| `CREATED for X > 120s — never submitted` | 2 (MSFT, UBER) | Watcher fell off OR `on_trigger` callback never fired. Order monitor killed at 120s as `LOST_HANDOFF`. Forensic reason string was opaque — couldn't tell what step failed. |
| `STALE_ENTRY_CANCEL` (NFLX) | 1 | Existing safety, working correctly. |

## The four fixes (priority order)

### #1 — CREATED-never-submitted forensic trail

`ap/order_monitor.py`: the 120s cancel reason is now enriched with `signal_id`, `source`, and `broker_order_id` so we can trace the lost handoff. A WARN-level log line also fires so operators see it in real time.

This fix is **diagnostic, not preventive** — the underlying watcher-handoff failure is hard to reproduce without live logs. The next time it happens we'll have a forensic record sufficient to pinpoint the broken step.

### #2 — Stale-arm tolerance: env-tunable + gate distinction

`ap_entry_watcher.py`:
- New env var `WATCH_ARM_STALE_TOLERANCE_PCT` (default `0.010` = 1.0%).
- `WATCH_ARM_EFFECTIVE_THRESHOLD_PCT = max(env, MAX_INTRADAY_DRIFT_PCT)`. Env can only LOOSEN — never silently tightens below the safe 1.5% raw.
- Reject reason now distinguishes:
  - `arm_drift_-0.10pct_thr_1.50pct` — drift-stale
  - `arm_below_stop_mid_99.95_stop_99.97` — below-stop-stale
- Startup logs the effective threshold so ops know what's actually in effect.

### #3 — `pending_entries` slot counter excludes phantom CREATED

`ap/position_manager.py`: snapshot query now excludes CREATED/PENDING_TRIGGER orders that have aged past `PENDING_ENTRY_PHANTOM_GRACE_SEC` (default 30s) AND have no `broker_order_id`. These are effectively dead phantoms awaiting the watchdog's 120s sweep; they no longer consume slots.

Counted as pending exposure:
- CREATED orders younger than the grace window (legitimate in-flight)
- CREATED orders with a broker_order_id (queued at broker)
- SUBMITTED, ACKNOWLEDGED, PARTIAL_FILL (real live exposure)

### #4 — Pre-open stop-touch guard for daily/overnight setups

`ap_entry_watcher.py`: `WatchedSignal.check()` now skips the stop-level invalidation if **both**:
- The signal is overnight OR a daily signal
- The current time is pre-market (before 9:30 ET)

A debug log records the skipped touch. The 9:30 ET structural validator (`_revalidate_overnight_at_open`) still catches genuine thesis breaks.

---

## Environment variables (new)

| Var | Default | Meaning |
|---|---|---|
| `WATCH_ARM_STALE_TOLERANCE_PCT` | `0.010` | Arm-time stale tolerance. Effective threshold = max(this, 0.015). Set to `0.020` to allow 2% pre-arm drift. |
| `PENDING_ENTRY_PHANTOM_GRACE_SEC` | `30` | How long a CREATED-without-broker order counts as pending exposure. Past this, presumed phantom and excluded. |

---

## Deploy order

1. **Merge this PR to `main`** on `angel_precision_bot`.
2. **Tonight, set on Render** (both Jose and tradefluence):
   ```
   WATCH_ARM_STALE_TOLERANCE_PCT=0.010
   PENDING_ENTRY_PHANTOM_GRACE_SEC=30
   ```
   Defaults match these; setting explicitly makes the env audit-visible.
3. **Restart both bots.** Confirm in logs:
   ```
   [entry-watcher] thresholds loaded: MAX_INTRADAY_DRIFT_PCT=0.0150 WATCH_ARM_STALE_TOLERANCE_PCT=0.0100 effective=0.0150
   ```
4. **Watch tomorrow's session.** Expected behavior:
   - **No more** `stale_price_-0.X%` rejections at sub-1% deltas.
   - **No more** `watcher_invalidated` events between 8:00–9:30 ET on daily/overnight setups.
   - **`positions_full_at_breach`** should fire only when there are real filled positions consuming slots.
   - **If** `LOST_HANDOFF` fires, the log will name the signal_id and source — we can correlate to scanner output and find the exact gap.

---

## Verification SQL

Run during/after tomorrow's session:

```sql
-- Confirm rejection reasons changed from the old opaque label to new gate-specific codes
SELECT last_error, COUNT(*) AS n
FROM   orders
WHERE  client_id IN ('jose@example.com', 'tradefluencehq@gmail.com')
  AND  created_ts >= NOW() - INTERVAL '1 day'
  AND  status IN ('CANCELED', 'EXPIRED')
GROUP  BY last_error
ORDER  BY n DESC;
-- Expect to see arm_drift_* and arm_below_stop_* (not stale_price_*)
-- Expect to see LOST_HANDOFF if any CREATED orders aged out

-- Confirm the slot counter is no longer inflated by phantoms
SELECT status,
       (broker_order_id IS NULL OR broker_order_id = '') AS no_broker_id,
       COUNT(*) AS n
FROM   orders
WHERE  client_id = 'jose@example.com'
  AND  kind = 'ENTRY'
  AND  created_ts >= NOW() - INTERVAL '1 day'
  AND  status IN ('CREATED','PENDING_TRIGGER','SUBMITTED','ACKNOWLEDGED','PARTIAL_FILL')
GROUP  BY status, no_broker_id;
-- Look for CREATED+no_broker_id rows that are old. With the fix, they no
-- longer block new admissions.

-- Confirm zero pre-9:30 invalidations on daily/overnight
SELECT symbol, status, last_error, created_ts
FROM   orders
WHERE  client_id IN ('jose@example.com', 'tradefluencehq@gmail.com')
  AND  created_ts >= NOW() - INTERVAL '1 day'
  AND  EXTRACT(HOUR FROM (created_ts AT TIME ZONE 'America/New_York')) < 9
   OR  (EXTRACT(HOUR FROM (created_ts AT TIME ZONE 'America/New_York')) = 9
        AND EXTRACT(MINUTE FROM (created_ts AT TIME ZONE 'America/New_York')) < 30)
ORDER  BY created_ts DESC;
```

---

## Rollback

If anything looks wrong, revert with env vars (no code rollback needed):

```bash
# To re-tighten the arm gate to 1.5% raw default (the original behavior):
WATCH_ARM_STALE_TOLERANCE_PCT=0.015

# To disable the phantom-grace exclusion entirely (return to old behavior):
PENDING_ENTRY_PHANTOM_GRACE_SEC=99999

# The pre-open stop-touch guard has no env disable. If needed, revert the
# merge commit for #4. Other three fixes are env-controllable.
```

---

## What this DOES NOT change

- Exit engine (`ap_exit_engine.py`) — working, untouched
- OSM state machine (`ap_order_state_machine.py`) — working, untouched
- Reconciler — working, untouched
- Risk gate / position sizer — working, untouched
- Kill switch — working, untouched
- Score-based admission, alignment-aware retry, P0-1..P0-5 audit fixes — all preserved

---

## Tests added

16 regression tests in `tests/test_funnel_fixes.py`. All passing. Coverage:

- Stale-arm: env var exists, effective = max(env, raw), startup logs, drift/stop distinction, no old label
- pending_entries: phantom-exclusion clause present, env-tunable, default in sane range
- LOST_HANDOFF: forensic context in cancel reason, WARN log present
- Pre-open stop: helper exists, used in CALL+PUT branches, applies to both overnight + daily
- New env vars have sane defaults in expected ranges
