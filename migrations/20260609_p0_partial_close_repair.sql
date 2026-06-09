-- =============================================================================
-- P0 MIGRATION: Audit + broker-safe repair of CLOSED positions with remaining qty
-- File: migrations/20260609_p0_partial_close_repair.sql
-- Author: Angel Precision Intelligence
-- Date: 2026-06-09 (revised)
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
-- Safety principle
-- ----------------
-- exit_price IS NOT NULL does NOT prove broker-flat.
-- It may only prove a partial exit or scale-out occurred.
-- The ONLY safe automatic zeroing is for contracts that have already expired
-- (OCC expiry date is in the past), because expired options are always flat.
-- Everything else requires manual broker verification before touching state.
--
-- What this migration does
-- ------------------------
--  STEP 1  Audit-only SELECT — shows all bad rows with full context.
--          Run this first, review output before proceeding.
--
--  STEP 2  Auto-repair only expired contracts:
--          expiry_date < NOW() → quantity_remaining=0, close_source=CLOSED_REPAIR
--          Safe: expired options cannot be held by any broker.
--
--  STEP 3  Flag everything else as MANUAL_REVIEW_REQUIRED WITHOUT changing
--          status or quantity_remaining. Operator must check Tradier for each row.
--
--  STEP 4  Post-repair verification SELECT.
--
-- The runtime reconciler (_repair_closed_positions_with_remaining_qty) handles
-- live broker-truth verification going forward. This migration handles historical debt.
--
-- Safe to run multiple times (idempotent WHERE clauses throughout).
-- =============================================================================

-- ── STEP 1: AUDIT — review this output before running Steps 2–3 ──────────────
-- Run this block alone first. Confirm the rows look as expected.
-- Pay attention to expiry_date: expired contracts are auto-repairable.
-- Non-expired contracts need broker verification.
SELECT
    id,
    client_id,
    COALESCE(option_symbol, contract)                       AS option_contract,
    qty,
    quantity_remaining,
    status,
    close_source,
    exit_ts,
    exit_price,
    expiry_date,
    CASE
        WHEN expiry_date IS NOT NULL AND expiry_date < NOW()
            THEN 'EXPIRED — safe to auto-zero'
        WHEN exit_price IS NOT NULL AND exit_price > 0
            THEN 'HAS_EXIT_PRICE — needs broker verify before zeroing'
        ELSE
            'NO_EXIT_PRICE — needs broker verify before zeroing'
    END                                                     AS repair_recommendation
FROM positions
WHERE UPPER(status) = 'CLOSED'
  AND COALESCE(quantity_remaining, 0) > 0
ORDER BY client_id, expiry_date NULLS LAST, entry_ts DESC;

-- =============================================================================
-- STOP HERE. Review the audit output above.
-- Only proceed to Steps 2–3 after reviewing each row.
-- =============================================================================

BEGIN;

-- ── STEP 2: Auto-repair EXPIRED contracts only ────────────────────────────────
-- Expired options cannot be held by any broker. Zero their quantity_remaining.
-- This is the ONLY safe automatic fix — no broker call needed.
--
-- Condition: expiry_date column must exist and be in the past.
-- If your schema uses a different column name (e.g. expiration_date, exp_date),
-- update the column name below before running.
UPDATE positions
SET
    quantity_remaining = 0,
    close_source       = 'CLOSED_REPAIR',
    updated_at         = NOW()
WHERE UPPER(status) = 'CLOSED'
  AND COALESCE(quantity_remaining, 0) > 0
  AND expiry_date IS NOT NULL
  AND expiry_date < NOW();

-- ── STEP 3: Flag non-expired rows for manual broker verification ──────────────
-- Do NOT change status or quantity_remaining here.
-- Operator must check Tradier for each flagged row before resolving.
--
-- Rows already flagged are skipped (idempotent).
UPDATE positions
SET
    close_source = 'MANUAL_REVIEW_REQUIRED',
    updated_at   = NOW()
WHERE UPPER(status) = 'CLOSED'
  AND COALESCE(quantity_remaining, 0) > 0
  AND (expiry_date IS NULL OR expiry_date >= NOW())
  AND close_source != 'MANUAL_REVIEW_REQUIRED';  -- idempotent

-- ── STEP 4: Post-repair verification ─────────────────────────────────────────
SELECT
    close_source,
    COUNT(*)                    AS row_count,
    SUM(quantity_remaining)     AS total_remaining_qty,
    MIN(expiry_date)            AS earliest_expiry,
    MAX(expiry_date)            AS latest_expiry
FROM positions
WHERE UPPER(status) = 'CLOSED'
  AND COALESCE(quantity_remaining, 0) > 0
GROUP BY close_source
ORDER BY row_count DESC;

COMMIT;

-- =============================================================================
-- POST-MIGRATION OPERATOR CHECKLIST
-- =============================================================================
-- After running, you will have two categories of rows remaining:
--
-- A. CLOSED_REPAIR rows (quantity_remaining=0)
--    These are expired contracts — no further action needed.
--
-- B. MANUAL_REVIEW_REQUIRED rows (quantity_remaining still > 0)
--    For each row: check Tradier broker account for the option contract.
--
--    Case 1 — Broker is flat (contract not held):
--      UPDATE positions
--      SET    quantity_remaining = 0,
--             close_source       = 'CLOSED_REPAIR',
--             updated_at         = NOW()
--      WHERE  id = '<row_id>';
--
--    Case 2 — Broker still holds the contract:
--      UPDATE positions
--      SET    status             = 'PARTIAL',
--             quantity_remaining = <broker_qty>,
--             close_source       = 'PARTIAL_CLOSE_REPAIR',
--             updated_at         = NOW()
--      WHERE  id = '<row_id>';
--      -- The runtime reconciler will then re-seed the exit engine for this row.
--
--    Case 3 — Broker API unavailable / cannot verify:
--      Leave as MANUAL_REVIEW_REQUIRED.
--      The runtime _repair_closed_positions_with_remaining_qty() will attempt
--      broker verification on every reconciler cycle until resolved.
--
-- C. Final check — no normal rows should remain:
--    SELECT id, client_id, contract, status, quantity_remaining, close_source
--    FROM   positions
--    WHERE  UPPER(status) = 'CLOSED'
--      AND  COALESCE(quantity_remaining, 0) > 0
--      AND  close_source NOT IN ('MANUAL_REVIEW_REQUIRED', 'CLOSED_REPAIR');
--    -- This should return 0 rows after all operator steps are complete.
-- =============================================================================
