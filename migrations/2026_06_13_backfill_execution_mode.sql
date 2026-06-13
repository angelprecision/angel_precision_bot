-- migrations/2026_06_13_backfill_execution_mode.sql
--
-- PR B: Backfill execution_mode on historical orders.
--
-- WHY:
--   ap/position_manager.snapshot() filters orders by execution_mode to match
--   the runtime mode. Historical rows from before the execution_mode column
--   was added have execution_mode IS NULL and get logged as
--   SNAPSHOT_ORPHAN_FILLED_IGNORED on every snapshot call. The current paper
--   service logs show null_mode_count=32 (Jose) and =53 (tradefluencehq) on
--   every signal — 85+ rows scanned and ignored per signal per client. This
--   is harmless but noisy and slow.
--
-- WHAT:
--   Set execution_mode from the client's current tradier_account_mode in
--   members. This is the historically-correct mapping: clients have not
--   switched modes, so their historical orders match their current mode.
--
-- SAFETY:
--   - Only updates rows where execution_mode IS NULL (idempotent)
--   - Only joins on email matches in members (skips orphan client_ids)
--   - DOES NOT touch the execution_mode of any row that already has a value
--   - Wrapped in a transaction so partial application is impossible
--
-- ROLLBACK:
--   If needed, set execution_mode back to NULL where last_error contains
--   'BACKFILL_TAG_2026_06_13' — but we do not write that tag (no audit
--   trail change). The migration is one-way; rollback is not expected.
--
-- VERIFICATION:
--   Before:
--     SELECT execution_mode, count(*)
--     FROM public.orders
--     GROUP BY execution_mode ORDER BY count(*) DESC;
--   After running this migration the NULL bucket should drop by 85+ rows
--   per active client.

BEGIN;

-- Backfill paper-mode orders
UPDATE public.orders o
SET execution_mode = 'paper'
FROM public.members m
WHERE o.client_id = m.email
  AND o.execution_mode IS NULL
  AND lower(m.tradier_account_mode) = 'paper';

-- Backfill live-mode orders
UPDATE public.orders o
SET execution_mode = 'live'
FROM public.members m
WHERE o.client_id = m.email
  AND o.execution_mode IS NULL
  AND lower(m.tradier_account_mode) = 'live';

-- Report what's left as NULL (orphan rows without a matching member)
-- This is informational only — orphan rows will continue to be ignored by
-- snapshot's per-mode filter, which is the correct safe behavior.
DO $$
DECLARE
    null_remaining int;
BEGIN
    SELECT count(*) INTO null_remaining
    FROM public.orders
    WHERE execution_mode IS NULL;
    RAISE NOTICE 'execution_mode NULL rows remaining after backfill: %', null_remaining;
END $$;

COMMIT;
