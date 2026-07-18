# Jose daily trade-count repair

This is an explicit operator procedure. Runtime code does not rewrite
`client_state`. Run the read-only verification first and do not substitute a
different client without separate evidence of the same corruption.

## 1. Recompute from broker-confirmed ENTRY fills

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
    AND lower(coalesce(execution_mode, '')) = 'live'
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

## 2. Preview the exact client-state correction

```sql
SELECT client_id, mode, day_key, trades_taken_today, updated_at
FROM client_state
WHERE client_id = 'jose.vasquez4011@gmail.com';
```

Confirm this returns exactly one Jose row. Jason and Tradefluence must not
appear in either the predicate or result.

## 3. Apply only after operator approval

Replace `<verified_count>` with the result from step 1.

```sql
BEGIN;

UPDATE client_state
SET trades_taken_today = <verified_count>,
    day_key = to_char(now() AT TIME ZONE 'America/New_York', 'YYYY-MM-DD'),
    updated_at = now()
WHERE client_id = 'jose.vasquez4011@gmail.com';

SELECT client_id, mode, day_key, trades_taken_today, updated_at
FROM client_state
WHERE client_id = 'jose.vasquez4011@gmail.com';

-- COMMIT only after the SELECT proves the exact Jose row and expected count.
-- Otherwise execute ROLLBACK.
```

This procedure does not update positions, orders, proof trades, official
performance history, Jason, or Tradefluence.
