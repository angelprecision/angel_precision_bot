-- Migration: 2026_05_31_sizer_threshold_columns.sql
--
-- Adds per-client sizer threshold columns to the clients table.
-- These let operators set correct throttle/stop values per client via the
-- admin panel instead of Render env vars — eliminating the env-var misordering
-- that produced "Sizer thresholds misordered" warnings.
--
-- Semantics (BOTH values must be negative):
--   throttle_threshold_usd : first risk signal / smaller loss  (e.g. -1500)
--   stop_threshold_usd     : hard stop       / larger loss     (e.g. -2000)
--
-- A NULL in either column means "fall back to the global env var".
-- A CHECK constraint enforces correct ordering at the DB level so an
-- operator cannot accidentally enter them reversed.
--
-- Apply in Supabase SQL editor (or psql):
--   \i sql/2026_05_31_sizer_threshold_columns.sql

ALTER TABLE clients
  ADD COLUMN IF NOT EXISTS throttle_threshold_usd NUMERIC(12,2) DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS stop_threshold_usd      NUMERIC(12,2) DEFAULT NULL;

-- Both must be negative when set.
ALTER TABLE clients
  DROP CONSTRAINT IF EXISTS chk_throttle_negative,
  DROP CONSTRAINT IF EXISTS chk_stop_negative,
  DROP CONSTRAINT IF EXISTS chk_thresholds_ordered;

ALTER TABLE clients
  ADD CONSTRAINT chk_throttle_negative
    CHECK (throttle_threshold_usd IS NULL OR throttle_threshold_usd < 0),
  ADD CONSTRAINT chk_stop_negative
    CHECK (stop_threshold_usd IS NULL OR stop_threshold_usd < 0),
  -- stop must be MORE negative than throttle (further from zero).
  -- throttle=-1500, stop=-2000 → stop < throttle → valid.
  -- throttle=-2000, stop=-1500 → stop > throttle → DB rejects.
  ADD CONSTRAINT chk_thresholds_ordered
    CHECK (
      throttle_threshold_usd IS NULL
      OR stop_threshold_usd IS NULL
      OR stop_threshold_usd <= throttle_threshold_usd
    );

-- Correct values for known clients (set based on their account equity).
-- jasoncosby1: equity ~$100K → throttle=-1500 (~1.5%), stop=-3000 (~3%)
-- tradefluencehq: set to similar conservative defaults.
-- Adjust these to match the actual equity of each client.

UPDATE clients SET
  throttle_threshold_usd = -1500.00,
  stop_threshold_usd     = -3000.00
WHERE client_id IN ('jasoncosby1@gmail.com', 'tradefluencehq@gmail.com')
  AND throttle_threshold_usd IS NULL;

COMMENT ON COLUMN clients.throttle_threshold_usd IS
  'Per-client sizer throttle threshold in USD (negative). First risk signal that halves sizing. NULL = use THROTTLE_THRESHOLD env var. Must be less negative than stop_threshold_usd.';

COMMENT ON COLUMN clients.stop_threshold_usd IS
  'Per-client sizer hard stop in USD (negative). Blocks all new entries when daily drawdown reaches this level. NULL = use STOP_THRESHOLD env var. Must be more negative than throttle_threshold_usd.';
