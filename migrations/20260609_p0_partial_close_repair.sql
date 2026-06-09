-- =============================================================================
-- P0 MIGRATION: Repair CLOSED positions with quantity_remaining > 0
-- File: migrations/20260609_p0_partial_close_repair.sql
-- Author: Angel Precision Intelligence
-- Date: 2026-06-09
--
-- Context
-- -------
-- The reconciler was setting status='CLOSED' unconditionally on auto-close,
-- ignoring quantity_remaining.  This produced rows where status=CLOSED but
-- quantity_remaining > 0 — invalid state that hides live broker exposure.
--
-- Known affected rows (jasoncosby1@gmail.com):
--   PG:   qty=7 remaining=5  UNH: qty=12 remaining=5
--   AAPL: qty=5 remaining=3  BA:  qty=5  remaining=3
--   C:    qty=7 remaining=5
--
-- This migration performs a SAFE one-time repair:
--   1. Rows where broker is flat (exit evidence exists or options expired):
--      → set quantity_remaining = 0, close_source = 'CLOSED_REPAIR'
--      → status stays CLOSED (correct terminal state)
--
--   2. Rows where no exit evidence can be determined from DB alone:
--      → set close_source = 'MANUAL_REVIEW_REQUIRED' and add a flag in meta
--      → OPERATOR MUST VERIFY against broker before this run is complete
--
-- The runtime reconciler (_repair_closed_positions_with_remaining_qty) handles
-- ongoing broker-truth verification.  This migration handles the historical debt.
--
-- Safe to run multiple times (idempotent via WHERE clause).
-- =============================================================================

BEGIN;

-- ── Step 1: Audit — snapshot all bad rows before touching anything ────────────
-- This view is ephemeral (transaction-scoped CTE) to give a pre-repair count.
WITH bad_rows AS (
    SELECT
        id,
        client_id,
        COALESCE(option_symbol, contract)   AS option_contract,
        qty,
        quantity_remaining,
        status,
        close_source,
        exit_ts,
        exit_price
    FROM positions
    WHERE UPPER(status) = 'CLOSED'
      AND COALESCE(quantity_remaining, 0) > 0
)
SELECT
    COUNT(*)                                AS total_bad_rows,
    COUNT(*) FILTER (WHERE exit_price IS NOT NULL AND exit_price > 0)
                                            AS rows_with_exit_price,
    COUNT(*) FILTER (WHERE exit_price IS NULL OR exit_price = 0)
                                            AS rows_without_exit_price
FROM bad_rows;

-- ── Step 2: For rows that have an exit_price (broker confirmed at time of close)
--           set quantity_remaining = 0 and mark source CLOSED_REPAIR.
--           These are genuinely closed — the remaining_qty column was just
--           not zeroed due to the bug.
UPDATE positions
SET
    quantity_remaining = 0,
    close_source       = 'CLOSED_REPAIR',
    updated_at         = NOW()
WHERE UPPER(status) = 'CLOSED'
  AND COALESCE(quantity_remaining, 0) > 0
  AND exit_price IS NOT NULL
  AND exit_price > 0;

-- ── Step 3: For rows with NO exit_price — broker truth could not be inferred.
--           Mark for operator manual review WITHOUT changing status or qty.
--           Operator must check Tradier before deciding CLOSED vs PARTIAL.
UPDATE positions
SET
    close_source = 'MANUAL_REVIEW_REQUIRED',
    updated_at   = NOW()
WHERE UPPER(status) = 'CLOSED'
  AND COALESCE(quantity_remaining, 0) > 0
  AND (exit_price IS NULL OR exit_price = 0)
  AND close_source != 'MANUAL_REVIEW_REQUIRED';  -- idempotent

-- ── Step 4: Verification — count remaining bad rows post-repair ───────────────
-- After this migration, rows with exit_price should be 0.
-- Rows without exit_price remain for manual review.
SELECT
    close_source,
    COUNT(*)           AS count,
    SUM(quantity_remaining) AS total_remaining_qty
FROM positions
WHERE UPPER(status) = 'CLOSED'
  AND COALESCE(quantity_remaining, 0) > 0
GROUP BY close_source
ORDER BY count DESC;

COMMIT;

-- =============================================================================
-- POST-MIGRATION OPERATOR CHECKLIST
-- =============================================================================
-- 1. Run the SELECT above to see rows still flagged MANUAL_REVIEW_REQUIRED.
-- 2. For each: check Tradier broker account for the option contract.
--    a. Broker is flat → UPDATE positions SET quantity_remaining=0 WHERE id=<id>;
--    b. Broker holds contracts → UPDATE positions
--                                SET status='PARTIAL',
--                                    quantity_remaining=<broker_qty>,
--                                    close_source='PARTIAL_CLOSE_REPAIR'
--                                WHERE id=<id>;
-- 3. After all manual_review rows are resolved, the bad state is fully cleared.
-- 4. Deploy the code fix (PR p0-reconciler-partial-close-fix) to prevent recurrence.
-- =============================================================================
