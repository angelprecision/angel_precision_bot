-- =============================================================================
-- Migration: 2026_05_26_proof_trades_trigger_vs_realized.sql
-- Purpose:   Add columns to proof_trades for the trigger-vs-realized labeling
--            fix. Captures what the exit engine SAW at decision time separately
--            from the broker fill RESULT.
--
-- Motivation: Forensic finding on 2026-05-26 of SPY 0DTE PUT exit:
--   - Exit engine read option mark = $0.73 at decision time (theta decay)
--   - DEEP_LOSS_STOP fired with trigger PnL = -37%
--   - Limit submitted, actual broker fill = $1.15 (better than trigger)
--   - Realized PnL = -0.9% (breakeven, within -2% band)
--   - But the dashboard showed LOSS with exit_reason "-37%" string
--
-- These columns let the dashboard show: "Triggered at -37% → Filled at -0.9%"
-- while preserving exit_reason as the trigger evidence.
--
-- Idempotent: uses IF NOT EXISTS for safe re-runs.
-- Reversible: ROLLBACK file shipped separately.
-- Safe: all columns nullable, all existing rows continue to work.
-- =============================================================================

BEGIN;

-- trigger_pnl_pct: exit-engine estimated PnL at decision time (PERCENTAGE, e.g. -37.0)
ALTER TABLE proof_trades
  ADD COLUMN IF NOT EXISTS trigger_pnl_pct numeric NULL;

-- trigger_option_price: option mark/mid used by exit engine at decision time
ALTER TABLE proof_trades
  ADD COLUMN IF NOT EXISTS trigger_option_price numeric NULL;

-- trigger_underlying: underlying ticker price at decision time
ALTER TABLE proof_trades
  ADD COLUMN IF NOT EXISTS trigger_underlying numeric NULL;

-- trigger_reason_code: machine-readable exit reason code at decision time
-- (e.g. "DEEP_LOSS_STOP", "THESIS_FAIL_SOFT_STOP", "TARGET_HIT", "RUNNER_TRAIL")
ALTER TABLE proof_trades
  ADD COLUMN IF NOT EXISTS trigger_reason_code text NULL;

-- realized_pnl_pct: broker-fill-based PnL (PERCENTAGE).
-- Functionally an alias of option_pnl_pct; kept as an explicit column so
-- downstream readers can semantically distinguish "trigger evidence" from
-- "realized result" without conditional logic.
ALTER TABLE proof_trades
  ADD COLUMN IF NOT EXISTS realized_pnl_pct numeric NULL;

-- Comments for schema documentation
COMMENT ON COLUMN proof_trades.trigger_pnl_pct IS
  'Exit-engine PnL percentage at decision time (e.g. -37.0). May differ '
  'from option_pnl_pct/realized_pnl_pct when the broker fill is better '
  'than the limit submitted (protective exits during quote collapse).';
COMMENT ON COLUMN proof_trades.trigger_option_price IS
  'Option mark/mid used by exit engine when the exit decision fired.';
COMMENT ON COLUMN proof_trades.trigger_underlying IS
  'Underlying price at exit decision time.';
COMMENT ON COLUMN proof_trades.trigger_reason_code IS
  'Machine-readable exit reason code (DEEP_LOSS_STOP, HARD_STOP, '
  'THESIS_FAIL_SOFT_STOP, TARGET_HIT, RUNNER_TRAIL, etc.). Pairs with '
  'exit_reason which is the human-readable form.';
COMMENT ON COLUMN proof_trades.realized_pnl_pct IS
  'Broker-fill PnL percentage. Authoritative trade result. Same as '
  'option_pnl_pct; kept as explicit alias for semantic clarity.';

COMMIT;
