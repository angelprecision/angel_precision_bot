-- Live Authorization Gate + execution_mode persistence.
--
-- Adds orders.execution_mode — the execution mode stamped on an ENTRY order at
-- CREATION time ('live' when the broker base_url is live, 'paper' when sandbox).
-- The ENTRY order's stored mode is the SOURCE OF TRUTH for proof_trades, since a
-- client may switch modes while a position is open. proof_trades.execution_mode
-- is copied from the originating entry order on close (NULL/missing → 'unknown').
--
-- Idempotent: ADD COLUMN IF NOT EXISTS works on Postgres 9.6+.

ALTER TABLE orders
  ADD COLUMN IF NOT EXISTS execution_mode TEXT;

-- Quick sanity check
SELECT column_name, data_type, column_default
FROM   information_schema.columns
WHERE  table_name = 'orders'
  AND  column_name = 'execution_mode';
