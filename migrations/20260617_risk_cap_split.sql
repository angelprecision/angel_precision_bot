-- =============================================================================
-- Migration: 20260617_risk_cap_split.sql
--
-- PR #155 — Split per-position cap from total portfolio exposure cap.
--
-- Adds two nullable columns to client_risk_profiles so operators can configure
-- the split-cap model per client without touching global env vars.
--
-- Semantics:
--   max_position_pct     — per-trade budget as a fraction of account equity.
--                          Example: 0.10 → each trade can use up to 10% of equity.
--                          NULL = fall back to max_capital_pct.
--
--   max_total_capital_pct— total active+pending portfolio exposure cap as a
--                          fraction of equity.
--                          Example: 0.40 → total live exposure ≤ 40% of equity.
--                          NULL = fall back to DEFAULT_MAX_TOTAL_CAPITAL_PCT env
--                                 (hard-coded default: 0.40). Never unlimited.
--
-- SAFETY:
--   Both columns are NULLABLE with NO default. A NULL means "use the fallback
--   resolution in APMasterControl.__init__". Running this migration changes the
--   behavior of ZERO existing clients. Behavior only changes for a client when
--   an operator explicitly sets one of these columns.
--
-- CONSTRAINT:
--   max_total_capital_pct must be >= max_position_pct when both are set.
--   Enforced at the DB level. APMasterControl also clamps this defensively at
--   runtime in case a row pre-dates the constraint or is loaded differently.
--
-- Run in Supabase SQL editor (project: jhawzqnhcihevkhehogm):
--   Paste and execute this file in the SQL editor, or run via psql.
-- =============================================================================

ALTER TABLE client_risk_profiles
  ADD COLUMN IF NOT EXISTS max_position_pct      double precision DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS max_total_capital_pct double precision DEFAULT NULL;

-- Constraint: total exposure cap must be >= per-position cap when both are set.
-- Prevents an operator from setting max_total_capital_pct=0.05 and
-- max_position_pct=0.10, which would make every trade immediately blocked.
ALTER TABLE client_risk_profiles
  DROP CONSTRAINT IF EXISTS chk_total_cap_gte_position_cap;

ALTER TABLE client_risk_profiles
  ADD CONSTRAINT chk_total_cap_gte_position_cap
    CHECK (
      max_total_capital_pct IS NULL
      OR max_position_pct IS NULL
      OR max_total_capital_pct >= max_position_pct
    );

-- Column comments for schema clarity.
COMMENT ON COLUMN client_risk_profiles.max_position_pct IS
  'Per-trade budget cap as a fraction of account equity (e.g. 0.10 = 10%). '
  'NULL = fall back to max_capital_pct. Used by APMasterControl as per_trade_budget = equity * max_position_pct.';

COMMENT ON COLUMN client_risk_profiles.max_total_capital_pct IS
  'Total active+pending portfolio exposure cap as a fraction of equity (e.g. 0.40 = 40%). '
  'NULL = fall back to DEFAULT_MAX_TOTAL_CAPITAL_PCT env (default 0.40). '
  'Must be >= max_position_pct when both are set. Never silently unlimited.';

-- =============================================================================
-- Deployment SQL for Jason (live client).
-- Run AFTER deploying the code change (PR #155) so MC can read the new fields.
--
-- Before this update:
--   max_capital_pct=0.10 acts as BOTH per-trade cap and total cap.
--   equity=$1987, one fill of $183 → next selector budget = $15.72.
--
-- After this update:
--   per_trade_budget = 1987.24 * 0.10 = $198.72   (per trade, unchanged)
--   total_capital_cap = 1987.24 * 0.40 = $794.90  (total portfolio)
--   existing exposure $183 → remaining_total_cap = $611.90
--   selector_budget = min(198.72, 611.90) = $198.72  ← full budget restored
-- =============================================================================

-- UPDATE client_risk_profiles
-- SET
--   max_position_pct      = 0.10,
--   max_total_capital_pct = 0.40
-- WHERE client_email = 'jasoncosby1@gmail.com';

-- Verify after applying:
-- SELECT client_email, max_capital_pct, max_position_pct, max_total_capital_pct
-- FROM   client_risk_profiles
-- ORDER  BY client_email;
