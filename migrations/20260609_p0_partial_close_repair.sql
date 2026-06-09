-- =============================================================================
-- P0 MIGRATION: Audit + broker-safe repair of CLOSED positions with remaining qty
-- File: migrations/20260609_p0_partial_close_repair.sql
-- Author: Angel Precision Intelligence
-- Date: 2026-06-09 (revised v3)
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
-- Safety principles
-- -----------------
-- 1. positions.expiry_date does NOT exist in this schema.
--    Expiry is derived from the OCC contract symbol string (YYMMDD chars 7-12).
--    PostgreSQL regex: SUBSTRING(contract FROM '\d{6}(?=[CP])') → 'YYMMDD'
--    Converted to date: TO_DATE(match, 'YYMMDD')
--
-- 2. exit_price IS NOT NULL does NOT prove broker-flat.
--    It may only prove a partial exit or scale-out occurred.
--
-- 3. The ONLY safe automatic zeroing is for EXPIRED contracts:
--    An expired option cannot be held by any broker.
--    All other rows require manual broker verification.
--
-- Safe rule
-- ---------
--   expired contract (OCC expiry < today)   → auto-zero quantity_remaining=0
--   all other CLOSED+remaining>0 rows       → MANUAL_REVIEW_REQUIRED
--                                             status and quantity_remaining UNCHANGED
--
-- The runtime reconciler (_repair_closed_positions_with_remaining_qty) handles
-- live broker-truth verification for MANUAL_REVIEW_REQUIRED rows going forward.
--
-- Safe to run multiple times (idempotent WHERE clauses throughout).
-- =============================================================================

-- ── HELPER: OCC expiry extraction ────────────────────────────────────────────
-- OCC symbol format: {root}{YYMMDD}{C|P}{8-digit-strike}
-- Example: PG260620C00155000 → expiry string '260620' → date 2026-06-20
-- Regex captures the 6-digit sequence immediately before a C or P.
-- If the contract does not match OCC format, TO_DATE returns NULL safely.

-- ── STEP 1: AUDIT — run this first, review before proceeding ─────────────────
-- Shows all bad rows with expiry derived from OCC symbol.
-- Review the repair_action column for each row before running Steps 2-3.
SELECT
    id,
    client_id,
    COALESCE(option_symbol, contract)                           AS option_contract,
    qty,
    quantity_remaining,
    status,
    close_source,
    exit_ts,
    exit_price,
    -- Derive expiry from OCC symbol — no expiry_date column in schema
    TO_DATE(
        SUBSTRING(COALESCE(option_symbol, contract) FROM '\d{6}(?=[CP])'),
        'YYMMDD'
    )                                                           AS derived_expiry,
    CASE
        WHEN TO_DATE(
                SUBSTRING(COALESCE(option_symbol, contract) FROM '\d{6}(?=[CP])'),
                'YYMMDD'
             ) < CURRENT_DATE
            THEN 'EXPIRED — safe to auto-zero quantity_remaining'
        WHEN TO_DATE(
                SUBSTRING(COALESCE(option_symbol, contract) FROM '\d{6}(?=[CP])'),
                'YYMMDD'
             ) IS NULL
            THEN 'NON-OCC SYMBOL — manual review required'
        ELSE
            'NOT YET EXPIRED — broker verify required before touching'
    END                                                         AS repair_action
FROM positions
WHERE UPPER(status) = 'CLOSED'
  AND COALESCE(quantity_remaining, 0) > 0
ORDER BY client_id, derived_expiry NULLS LAST, entry_ts DESC;

-- =============================================================================
-- STOP. Review the audit output above.
-- Only proceed to Steps 2-3 after confirming each row's repair_action.
-- =============================================================================

BEGIN;

-- ── STEP 2: Auto-repair EXPIRED contracts only ────────────────────────────────
-- Expired options cannot be held by any broker — safe to zero without a broker call.
-- Condition: OCC-derived expiry date is before today (CURRENT_DATE).
-- Non-OCC symbols produce NULL from TO_DATE and are excluded by IS NOT NULL.
UPDATE positions
SET
    quantity_remaining = 0,
    close_source       = 'CLOSED_REPAIR',
    updated_at         = NOW()
WHERE UPPER(status) = 'CLOSED'
  AND COALESCE(quantity_remaining, 0) > 0
  AND TO_DATE(
        SUBSTRING(COALESCE(option_symbol, contract) FROM '\d{6}(?=[CP])'),
        'YYMMDD'
      ) IS NOT NULL
  AND TO_DATE(
        SUBSTRING(COALESCE(option_symbol, contract) FROM '\d{6}(?=[CP])'),
        'YYMMDD'
      ) < CURRENT_DATE;

-- ── STEP 3: Flag all remaining rows for manual broker verification ────────────
-- DO NOT change status or quantity_remaining here.
-- Covers: not-yet-expired contracts, non-OCC symbols, any unresolved row.
-- Idempotent: rows already flagged are skipped.
UPDATE positions
SET
    close_source = 'MANUAL_REVIEW_REQUIRED',
    updated_at   = NOW()
WHERE UPPER(status) = 'CLOSED'
  AND COALESCE(quantity_remaining, 0) > 0
  AND close_source != 'MANUAL_REVIEW_REQUIRED';  -- idempotent

-- ── STEP 4: Post-repair verification ─────────────────────────────────────────
SELECT
    close_source,
    COUNT(*)                                                    AS row_count,
    SUM(quantity_remaining)                                     AS total_remaining_qty,
    MIN(TO_DATE(
        SUBSTRING(COALESCE(option_symbol, contract) FROM '\d{6}(?=[CP])'),
        'YYMMDD'
    ))                                                          AS earliest_derived_expiry,
    MAX(TO_DATE(
        SUBSTRING(COALESCE(option_symbol, contract) FROM '\d{6}(?=[CP])'),
        'YYMMDD'
    ))                                                          AS latest_derived_expiry
FROM positions
WHERE UPPER(status) = 'CLOSED'
  AND COALESCE(quantity_remaining, 0) > 0
GROUP BY close_source
ORDER BY row_count DESC;

COMMIT;

-- =============================================================================
-- POST-MIGRATION OPERATOR CHECKLIST
-- =============================================================================
-- After running, two categories of rows may remain:
--
-- A. CLOSED_REPAIR (quantity_remaining=0)
--    Expired contracts — no further action needed.
--
-- B. MANUAL_REVIEW_REQUIRED (quantity_remaining still > 0)
--    For each row: check the option contract in Tradier broker account.
--
--    Case 1 — Broker is flat (contract not held, or confirmed expired):
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
--      -- The runtime reconciler will re-seed the exit engine for this row.
--
--    Case 3 — Broker API unavailable / cannot verify:
--      Leave as MANUAL_REVIEW_REQUIRED.
--      The runtime _repair_closed_positions_with_remaining_qty() will retry
--      broker verification on every reconciler cycle until resolved.
--
-- C. Final check — confirm no unresolved rows remain:
--    SELECT id, client_id, contract, status, quantity_remaining, close_source
--    FROM   positions
--    WHERE  UPPER(status) = 'CLOSED'
--      AND  COALESCE(quantity_remaining, 0) > 0
--      AND  close_source NOT IN ('MANUAL_REVIEW_REQUIRED', 'CLOSED_REPAIR');
--    -- Should return 0 rows after all operator steps are complete.
-- =============================================================================
