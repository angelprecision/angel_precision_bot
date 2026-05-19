# AUDIT PHASE-2 Runbook — Queue, Slots, and Alignment-Aware Retry

## What this branch fixes (and what it leaves alone)

Three problems the user identified after observing live trading on 2026-05-19:

1. **Slot burn on cancel.** A signal that gets submitted to the broker but cancels (or times out) permanently consumed one of the 5 daily slots, blocking later signals — including overnight setups — for the rest of the day.
2. **First-N-wins admission.** The trade queue ordered by `created_ts ASC`. The first 5 signals always won, regardless of quality. A low-liquidity 72-score signal admitted before an AAPL 92-score signal burned the slot the AAPL signal needed.
3. **Limit orders that don't chase.** The bot posts a limit (e.g. MSFT $2.17), the option drifts up, and at 75s a `MISSED_MOVE_CANCEL` fires. There is no re-peg attempt. But blind retries are dangerous: every trade that canceled today (per user observation) would have been a loss if filled later — so retry must require thesis alignment, not just elapsed time.

What this branch does NOT change:
- The exit engine (`ap_exit_engine.py`)
- The reconciler (`ap_reconciler.py`)
- The order state machine (transition rules)
- The kill switch
- The risk gate / position sizer
- The HMAC + cookie auth introduced in the prior audit pass

---

## Code changes summary

### A. Slot accounting (`ap/execution.py`, `ap/config.py`, `ap/admin_api.py`, `ap/startup_guard.py`, `ap/scripts/initialize_client.py`)
- Daily-trade cap gate now reads `_count_active_entry_orders_today(client_id)` which counts only orders with status in `{ACK, ACKNOWLEDGED, SUBMITTED, PARTIAL_FILL, FILLED}`. CANCELED/REJECTED/EXPIRED/ERROR auto-free their slot — no decrement bookkeeping needed.
- `MAX_TRADES_PER_DAY` default raised from **5 to 12**.
- `MAX_CONCURRENT_POSITIONS` default raised from **2 to 4** (matches the lifted daily cap; previous 2/7 mismatch caused conflict warnings at startup).
- New-client onboarding (`initialize_client.py`, `admin_api.py`) defaults updated to 12 / 4.
- One-off SQL migration (`migrations/20260519_phase2_lift_caps.sql`) provided to bump existing approved + active clients to the new caps. **Review the SELECT first, then uncomment the UPDATE.**

### B. Score-based admission + opt-in preemption (`ap/queue.py`, `ap/execution.py`)
- `_claim_one_job()` ORDER BY changed from `created_ts ASC` to `COALESCE(NULLIF(payload->>'score','')::numeric, 65) DESC, created_ts ASC`. Best-score-wins, with arrival time as the tiebreaker.
- New `_try_preempt_for_higher_score()` helper: when the daily cap is full but some slots are held by **pre-submitted** entries (status `CREATED` or `PENDING_TRIGGER`, waiting on a price trigger), a higher-scored incoming signal can preempt the lowest-scored pre-submitted entry. Gated by `PREEMPT_PRE_SUBMITTED_ORDERS` env flag (default OFF) and `PREEMPT_SCORE_DELTA` minimum (default 10). Only cancels pre-broker orders — never racing a SUBMITTED order against a possible fill.
- `audit(... 'SLOT_PREEMPTED', ...)` event emitted on every preemption.

### C. Alignment-aware retry (`ap/retry_engine.py`, `ap/order_monitor.py`)
- New module `ap/retry_engine.py` with `decide_repeg()` and `apply_repeg()`.
- Three gates ALL must pass for a re-peg:
  1. **TIME**: max 2 attempts per order, 30s minimum between attempts
  2. **PROXIMITY**: option mid within 8% of our limit (else "runaway_quote")
  3. **ALIGNMENT**: for a CALL, underlying still ≥ signal_entry × 0.998; for a PUT, ≤ × 1.002 (else "stale_thesis_call" / "stale_thesis_put")
- If all three pass, new limit = current_limit + 0.50 × (current_option_mid − current_limit). Move halfway to market, not all the way.
- `order_monitor.py` now calls `_try_repeg` BEFORE the `MISSED_MOVE_CANCEL` block. If the engine declines (any gate fails), the existing cancel logic runs unchanged.

### D. Order metadata (`ap/db.py`, `migrations/20260519_phase2_orders_meta.sql`)
- New `orders.meta` JSONB column (idempotent migration).
- `insert_order(...)` and `update_order(...)` now accept `meta=` and (update_order) `limit_price=`. Backward-compatible — both are optional kwargs.
- `execution.process_signal` populates `meta` at order creation with: `score`, `ticker`, `signal_entry_price`, `signal_id`, `source`, `repeg_attempts=0`, `last_repeg_ts=0`.

---

## Kill switches (in order from least to most disruptive)

If anything looks wrong in production, use these env vars on Render (no code
rollback needed):

| To disable... | Set... | Effect |
|---|---|---|
| Just the re-peg ladder | `REPEG_ENABLED=0` | `decide_repeg()` returns ok=False with reason="repeg_disabled". `order_monitor` falls through to the existing MISSED_MOVE_CANCEL behavior. Slot accounting and score ordering still active. |
| Score-based preemption | `PREEMPT_PRE_SUBMITTED_ORDERS=0` | No pre-broker entries get canceled when a higher-scored signal arrives. Already the default for the first rollout day. |
| Lift back to legacy caps | `MAX_TRADES_PER_DAY=5`, `MAX_CONCURRENT_POSITIONS=2`, `MAX_POSITIONS=2` | Returns to pre-PHASE2 caps. Slot accounting (orders-table-truth) still active; the queue still orders by score — only the cap shrinks. |
| Whole admission improvement | All three above | The bot reverts to behaving like the pre-PHASE2 build except slot accounting stays correct. Score ordering is automatic via the existing SQL and harmless if scores are missing. |

For a deeper rollback, revert the merge commit on `main`.

## Environment variables (new + changed defaults)

| Var | Default | Meaning |
|---|---|---|
| `MAX_TRADES_PER_DAY` | **12** (was 5) | Daily entry cap. Per-client overrides via `clients.max_trades_per_day`. |
| `MAX_CONCURRENT_POSITIONS` | **4** (was 2) | Hard concurrent-open cap. |
| `MAX_POSITIONS` | **4** (was 7) | Same number, used by execution sizer. Must match the above. |
| `PREEMPT_PRE_SUBMITTED_ORDERS` | `0` | Set `1` to enable score-based preemption of CREATED/PENDING_TRIGGER entries. Recommend leaving OFF until 24h of observed clean admission with the new ordering. |
| `PREEMPT_SCORE_DELTA` | `10` | Incoming signal must beat the lowest pre-submitted by at least this score delta. |
| `REPEG_ENABLED` | `1` | Master switch for `ap/retry_engine.decide_repeg`. |
| `REPEG_MAX_ATTEMPTS` | `2` | Per-order re-peg cap. |
| `REPEG_INTERVAL_SECS` | `30` | Min seconds between re-pegs on same order. |
| `REPEG_PROXIMITY_PCT` | `0.08` | If option mid > limit × (1+this), thesis is gone — refuse re-peg. |
| `REPEG_UNDERLYING_DRIFT_PCT` | `0.002` | Underlying drift tolerance for alignment gate (0.2% by default). |
| `REPEG_STEP_FRACTION` | `0.50` | How much of the limit-vs-current gap to close on each re-peg. |

---

## Deploy order (do exactly this)

1. **Merge this PR to `main`** on `angel_precision_bot`.
2. **Apply the migrations on Supabase** in this order:
   - `migrations/20260519_phase2_orders_meta.sql` (adds `orders.meta` JSONB)
   - `migrations/20260519_phase2_lift_caps.sql` — first run the SELECT, confirm the affected clients, then uncomment and run the UPDATE.
3. **Set env vars on Render** (deploy these together with the merge):
   ```
   MAX_TRADES_PER_DAY=12
   MAX_CONCURRENT_POSITIONS=4
   MAX_POSITIONS=4
   REPEG_ENABLED=1
   PREEMPT_PRE_SUBMITTED_ORDERS=0    # leave OFF for the first day
   ```
4. **Deploy.** Tail logs and look for:
   - `Position limits consistent: 4` (startup guard agrees)
   - `SECRET_FINGERPRINTS_SHA8` line (sanity)
   - A `REPEG_DECLINED` line on any stale-entry cancel that would have otherwise fired without explanation — that's the new code working in observe mode.
5. **24-hour shakedown.** Observe one full session. Expect to see:
   - More signals admitted (12 cap, not 5)
   - Some `SLOT_PREEMPT` paths logged but inert (because flag is off)
   - On real stale-entry events: `REPEG_DECLINED stale_thesis_call` (the MSFT case) or `REPEG_APPLIED ... new_limit=X` (aligned)
6. **After 24h of clean behavior**, flip `PREEMPT_PRE_SUBMITTED_ORDERS=1` and redeploy. Phase-2 is then fully on.

---

## Verifying it works (real-time)

### Slot fix (PR A)
```sql
-- This query is the authoritative daily cap. Run it during the day.
SELECT COUNT(*) AS slots_used
FROM   orders
WHERE  client_id = 'tradefluencehq@gmail.com'
  AND  COALESCE(kind, 'ENTRY') = 'ENTRY'
  AND  created_ts >= date_trunc('day', NOW() AT TIME ZONE 'UTC')
  AND  status IN ('ACK','ACKNOWLEDGED','SUBMITTED','PARTIAL_FILL','FILLED');

-- And the comparison for what client_state THINKS:
SELECT trades_taken_today FROM client_state WHERE client_id = 'tradefluencehq@gmail.com';

-- These two may diverge after cancels — the SQL query above is the truth.
```

### Score ordering (PR B)
After a few signals enqueue:
```sql
SELECT signal_id,
       payload->>'ticker'                            AS ticker,
       (payload->>'score')::numeric                  AS score,
       status,
       created_ts
FROM   trade_queue
WHERE  client_id = 'tradefluencehq@gmail.com'
  AND  status = 'NEW'
ORDER  BY COALESCE(NULLIF(payload->>'score','')::numeric, 65) DESC,
          created_ts ASC;
-- The first row is what the worker will claim next.
```

### Re-peg (PR C)
```bash
# Log greps after a stale-entry event:
grep "REPEG_DECLINED"  bot.log    # alignment gates rejecting bad chases
grep "REPEG_APPLIED"   bot.log    # successful re-pegs
grep "stale_thesis"    bot.log    # the MSFT-style refusal we want
```

---

## Rollback

If anything goes sideways:

```bash
# Code rollback
git checkout main && git push origin main --force-with-lease  # (only if needed)

# Env rollback (Render dashboard)
MAX_TRADES_PER_DAY=5
MAX_CONCURRENT_POSITIONS=2
MAX_POSITIONS=2
REPEG_ENABLED=0
PREEMPT_PRE_SUBMITTED_ORDERS=0
```

The `orders.meta` column is forward-compatible — old code ignores it. No DB rollback needed.

---

## Test suite

```
tests/test_retry_engine.py             16 tests covering all three gates + MSFT replay
tests/test_queue_score_ordering.py      2 tests guarding the ORDER BY change
```

Add to CI: ensures CI fails if anyone reverts the ORDER BY or weakens the retry gates.
