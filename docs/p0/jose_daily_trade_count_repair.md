# Jose daily trade-count repair

**Scope:** Jose Vasquez — PAPER account only.
**DO NOT run this procedure for Jason, Tradefluence, or any LIVE account.**
Runtime code does not rewrite `client_state`. Run the read-only verification
first and do not substitute a different client without separate evidence of the
same corruption.

---

## 1. Recompute from broker-confirmed ENTRY fills (PAPER)

```sql
WITH session_bounds AS (
  SELECT
    date_trunc('day', now() AT TIME ZONE 'America/New_York')
      AT TIME ZONE 'America/New_York' AS start_utc,
    (date_trunc('day', now() AT TIME ZONE 'America/New_York') + interval '1 day')
      AT TIME ZONE 'America/New_York' AS end_utc
), filled_entries AS (
  SELECT CASE
    WHEN coalesce(broker_order_id, '') <> '' THEN 'broker:' || broker_order_id
    WHEN coalesce(local_order_id, '') <> '' THEN 'local:' || local_order_id
  END AS canonical_entry_identity
  FROM orders, session_bounds
  WHERE client_id = 'jose.vasquez4011@gmail.com'
    AND lower(coalesce(execution_mode, '')) = 'paper'
    AND upper(coalesce(kind, '')) = 'ENTRY'
    AND coalesce(filled_qty, 0) > 0
    AND upper(coalesce(status, '')) IN
      ('PARTIAL_FILL', 'PARTIALLY_FILLED', 'FILLED')
    AND filled_ts >= start_utc
    AND filled_ts < end_utc
)
SELECT count(DISTINCT canonical_entry_identity) AS verified_trades_today
FROM filled_entries
WHERE canonical_entry_identity IS NOT NULL;
```

Record the returned value and the current Eastern session date. Do not use
positions, proof rows, contracts, or quantities as the count.

---

## 2. Preview the exact client-state row before writing

```sql
SELECT client_id, mode, day_key, trades_taken_today, updated_at
FROM client_state
WHERE client_id = 'jose.vasquez4011@gmail.com'
  AND upper(coalesce(mode, '')) = 'PAPER';
```

**This must return exactly one row.** If it returns zero rows, stop — the
state row is missing or the mode is wrong; do not proceed. If it returns more
than one row, stop and escalate. Jason and Tradefluence must not appear in
the result.

---

## 3. Apply only after operator approval

Replace `<verified_count>` with the integer result from step 1.

```sql
BEGIN;

UPDATE client_state
SET trades_taken_today = <verified_count>,
    day_key = to_char(now() AT TIME ZONE 'America/New_York', 'YYYY-MM-DD'),
    updated_at = now()
WHERE client_id = 'jose.vasquez4011@gmail.com'
  AND upper(coalesce(mode, '')) = 'PAPER'
RETURNING client_id, mode, day_key, trades_taken_today, updated_at;
```

**Required before `COMMIT`:**

* The `RETURNING` clause must emit exactly one row.
* `client_id` must be `jose.vasquez4011@gmail.com`.
* `mode` must be `PAPER`.
* `trades_taken_today` must equal `<verified_count>`.
* `day_key` must equal today's Eastern session date.

If `RETURNING` emits zero rows or more than one row, execute `ROLLBACK`
immediately. Do not commit.

```sql
-- On any unexpected result:
ROLLBACK;
```

This procedure does not update positions, orders, proof trades, official
performance history, Jason, or Tradefluence.
