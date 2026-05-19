-- AUDIT PHASE-2: add `meta` JSONB column to orders for re-peg state +
-- signal-alignment data.
--
-- Stored fields (set by execution.process_signal at order creation):
--   signal_entry_price : underlying price at signal time (for alignment gate)
--   score              : signal score (used for preemption ordering)
--   ticker             : underlying ticker (denormalized for fast filtering)
--
-- Stored fields (set by retry_engine.apply_repeg):
--   repeg_attempts     : int, count of re-pegs applied so far
--   last_repeg_ts      : float unix seconds, when the last re-peg happened
--   last_repeg_reason  : str, last decision reason for audit
--
-- Idempotent: ADD COLUMN IF NOT EXISTS works on Postgres 9.6+.

ALTER TABLE orders
  ADD COLUMN IF NOT EXISTS meta JSONB DEFAULT '{}'::jsonb;

-- Optional partial index for fast preemption candidate lookup
-- (lowest-score pre-submitted entries today).
CREATE INDEX IF NOT EXISTS idx_orders_preempt_lookup
  ON orders (client_id, status, created_ts)
  WHERE status IN ('CREATED', 'PENDING_TRIGGER');

-- Quick sanity check
SELECT column_name, data_type, column_default
FROM   information_schema.columns
WHERE  table_name = 'orders'
  AND  column_name = 'meta';
