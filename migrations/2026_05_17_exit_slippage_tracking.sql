-- ============================================================================
-- Adaptive exit pricing — slippage tracking columns on proof_trades
-- ============================================================================
-- These columns capture what the exit engine saw (bid/ask/mid at decision
-- time), what it placed (limit_placed), what it filled at, and the slippage
-- vs mid and vs bid. This is the scoreboard for adaptive exit pricing.
--
-- All nullable — existing rows are unaffected. New rows populate going forward.
-- Run once in Supabase SQL editor.
-- ============================================================================

ALTER TABLE proof_trades
  ADD COLUMN IF NOT EXISTS exit_bid           double precision,
  ADD COLUMN IF NOT EXISTS exit_ask           double precision,
  ADD COLUMN IF NOT EXISTS exit_mid           double precision,
  ADD COLUMN IF NOT EXISTS exit_limit_placed  double precision,
  ADD COLUMN IF NOT EXISTS exit_fill_price    double precision,
  ADD COLUMN IF NOT EXISTS slippage_vs_mid    double precision,
  ADD COLUMN IF NOT EXISTS slippage_vs_bid    double precision,
  ADD COLUMN IF NOT EXISTS exit_pricing_tier  text,
  ADD COLUMN IF NOT EXISTS exit_attempt       integer,
  ADD COLUMN IF NOT EXISTS seconds_to_fill    double precision;

-- ── Proof query: how well is the adaptive pricing working? ─────────────────
-- SELECT
--   exit_pricing_tier,
--   exit_bucket,
--   count(*)                               AS trades,
--   round(avg(slippage_vs_mid)::numeric,3) AS avg_slip_vs_mid,
--   round(avg(slippage_vs_bid)::numeric,3) AS avg_slip_vs_bid,
--   round(avg(seconds_to_fill)::numeric,1) AS avg_fill_secs,
--   round(avg(exit_attempt)::numeric,2)    AS avg_retries,
--   round(avg(option_pnl_pct)::numeric,1)  AS avg_pnl_pct
-- FROM proof_trades
-- WHERE exit_pricing_tier IS NOT NULL
-- GROUP BY exit_pricing_tier, exit_bucket
-- ORDER BY trades DESC;
