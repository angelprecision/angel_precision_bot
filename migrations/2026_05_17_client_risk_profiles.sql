-- ============================================================================
-- H3: Per-client risk profiles
-- ============================================================================
-- Adds nullable per-client risk override columns to the clients table.
--
-- SAFETY: every column is NULLABLE with NO default. A NULL means
-- "use the global env default" — so running this migration changes the
-- behavior of ZERO existing clients. Behavior only changes for a client
-- when an operator explicitly sets one of these columns for that client.
--
-- Run once in the Supabase SQL editor.
-- ============================================================================

ALTER TABLE clients
  ADD COLUMN IF NOT EXISTS max_capital_pct          double precision,
  ADD COLUMN IF NOT EXISTS max_sector_pct           double precision,
  ADD COLUMN IF NOT EXISTS max_ticker_pct           double precision,
  ADD COLUMN IF NOT EXISTS max_calls                integer,
  ADD COLUMN IF NOT EXISTS max_puts                 integer,
  ADD COLUMN IF NOT EXISTS score_floor              double precision,
  ADD COLUMN IF NOT EXISTS context_floor            double precision,
  ADD COLUMN IF NOT EXISTS daily_profit_target_usd  double precision,  -- FLOOR not a cap: success milestone, bot keeps trading after
  ADD COLUMN IF NOT EXISTS entries_enabled          boolean DEFAULT true;

-- ----------------------------------------------------------------------------
-- Example: conservative beta profile for a $5K client (Jay).
-- 10% max capital  = ~1 position at a time on a $5K account
-- 1 call / 1 put    = single directional exposure
-- score_floor 70    = only A-tier setups
-- $225 daily target = stop opening new entries once hit (H8 quota logic)
-- ----------------------------------------------------------------------------
-- UPDATE clients SET
--   max_capital_pct         = 0.10,
--   max_sector_pct          = 0.10,
--   max_ticker_pct          = 0.10,
--   max_calls               = 1,
--   max_puts                = 1,
--   score_floor             = 70,
--   daily_profit_target_usd = 225   -- floor: clear $225 then keep going
-- WHERE client_id = 'jose.vasquez4011@gmail.com';

-- Verify
-- SELECT client_id, max_capital_pct, max_sector_pct, max_ticker_pct,
--        max_calls, max_puts, score_floor, context_floor,
--        daily_profit_target_usd, entries_enabled
-- FROM clients ORDER BY client_id;
