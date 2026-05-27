-- =============================================================================
-- Rollback for: 2026_05_26_proof_trades_trigger_vs_realized.sql
-- Run only if you need to revert the trigger-vs-realized columns.
-- Data in the dropped columns will be LOST.
-- =============================================================================

BEGIN;

ALTER TABLE proof_trades DROP COLUMN IF EXISTS trigger_pnl_pct;
ALTER TABLE proof_trades DROP COLUMN IF EXISTS trigger_option_price;
ALTER TABLE proof_trades DROP COLUMN IF EXISTS trigger_underlying;
ALTER TABLE proof_trades DROP COLUMN IF EXISTS trigger_reason_code;
ALTER TABLE proof_trades DROP COLUMN IF EXISTS realized_pnl_pct;

COMMIT;
