-- Live Authorization Gate + execution_mode persistence.
--
-- This migration is REPO-TRACKED in the bot so the bot is self-contained: the
-- proof logger (ap_proof_logger.py) inserts proof_trades.execution_mode on every
-- close, so the bot repo must be able to provision every column it writes
-- without depending on the dashboard-backend migration running first.
--
-- Columns:
--   orders.execution_mode           — stamped on an ENTRY order at CREATION time
--                                     ('live' when broker base_url is live,
--                                     'paper' when sandbox). The ENTRY order's
--                                     stored mode is the SOURCE OF TRUTH for
--                                     proof_trades, since a client may switch
--                                     modes while a position is open.
--   proof_trades.execution_mode     — copied from the originating entry order on
--                                     close (NULL/missing → 'unknown'). Drives
--                                     live-only performance accounting.
--   proof_trades.broker_reconciled  — whether the closed trade has been
--                                     reconciled against broker fills.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS works on Postgres 9.6+. Safe to re-run
-- and safe to run in either order relative to the dashboard-backend migration
-- (2026_06_05_client_authorizations.sql), which defines the same proof_trades
-- columns identically.

ALTER TABLE orders
  ADD COLUMN IF NOT EXISTS execution_mode TEXT;

-- Allowed execution_mode values: 'paper', 'live', 'unknown'.
ALTER TABLE proof_trades
  ADD COLUMN IF NOT EXISTS execution_mode TEXT;

ALTER TABLE proof_trades
  ADD COLUMN IF NOT EXISTS broker_reconciled BOOLEAN DEFAULT FALSE;

-- Index supporting live-only performance scans (execution_mode + closed_at).
CREATE INDEX IF NOT EXISTS idx_proof_trades_execution_mode
  ON proof_trades (execution_mode);

-- Quick sanity check — all three columns must be present after this runs.
SELECT table_name, column_name, data_type
FROM   information_schema.columns
WHERE  (table_name = 'orders'       AND column_name = 'execution_mode')
   OR  (table_name = 'proof_trades' AND column_name IN ('execution_mode', 'broker_reconciled'))
ORDER  BY table_name, column_name;
