-- =============================================================================
-- PR-B -- Daily Option Chain Archiving
-- =============================================================================
-- Additive archive table only. This table is not read by trading execution and
-- does not mutate orders, positions, proof_trades, trade_queue, or broker state.
--
-- Verified before this migration: no existing repository reference to
-- option_chain_snapshots.
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS option_chain_snapshots (
  id BIGSERIAL PRIMARY KEY,
  snapshot_date DATE NOT NULL,
  ticker TEXT NOT NULL,
  expiration DATE NOT NULL,
  snapshot_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  underlying_price NUMERIC,
  chain JSONB NOT NULL,
  atm_iv NUMERIC,
  expected_move_1d NUMERIC,
  expected_move_to_exp NUMERIC,
  source TEXT NOT NULL DEFAULT 'tradier',
  UNIQUE (snapshot_date, ticker, expiration)
);

CREATE INDEX IF NOT EXISTS idx_ocs_ticker_date
ON option_chain_snapshots (ticker, snapshot_date);

COMMIT;
