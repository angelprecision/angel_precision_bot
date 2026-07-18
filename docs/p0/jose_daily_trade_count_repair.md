# Jose daily trade-count repair

**Scope:** Jose Vasquez — PAPER account only.
This procedure must not be run for Jason, Tradefluence, or any LIVE account
without a separate evidence review and dedicated runbook.

---

## 1. Recompute from broker-confirmed PAPER ENTRY fills

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
    WHEN coalesce(local_order_id,  '') <> '' THEN 'local:'  || local_order_id
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

Record the returned value and the current Eastern session date.
Do not use positions, proof rows, contracts, or quantities as the count.

---

## 2. Preview the exact PAPER client-state row

```sql
SELECT client_id, mode, day_key, trades_taken_today, updated_at
FROM client_state
WHERE client_id = 'jose.vasquez4011@gmail.com'
  AND lower(coalesce(mode, '')) = 'paper';
```

**This must return exactly one row.**
If it returns zero rows, stop — do not proceed to step 3.
If it returns more than one row, stop — escalate before any write.
Jason and Tradefluence must not appear in the result.

---

## 3. Apply only after operator approval

Replace `<verified_count>` with the integer from step 1.

```sql
BEGIN;

UPDATE client_state
SET trades_taken_today = <verified_count>,
    day_key            = to_char(now() AT TIME ZONE 'America/New_York', 'YYYY-MM-DD'),
    updated_at         = now()
WHERE client_id = 'jose.vasquez4011@gmail.com'
  AND lower(coalesce(mode, '')) = 'paper'
RETURNING client_id, mode, day_key, trades_taken_today, updated_at;
```

**Required outcome:** the `RETURNING` clause must emit exactly one row showing
Jose's client_id, `mode = paper`, today's `day_key`, and the expected count.

* If `RETURNING` emits zero rows → `ROLLBACK` immediately. The WHERE predicate
  did not match. Do not commit.
* If `RETURNING` emits more than one row → `ROLLBACK` immediately. Multiple
  mode rows exist for Jose. Do not commit.
* If `RETURNING` emits exactly one row with the expected values → `COMMIT`.

---

## Prohibition

This SQL must not be executed for:
- Any `execution_mode = 'live'` or `mode = 'live'` predicate for Jose
- Jason's client_id (`jason@...`)
- Tradefluence's client_id (`tradefluencehq@...`)
- Any client other than `jose.vasquez4011@gmail.com`

A separate evidence review and dedicated runbook is required before this
procedure may be adapted for any other client or mode.
