-- P0 2026-08-11: align positions schema with QPM / exit-engine hard-ref persistence.
--
-- Incident:
--   ap.position_quote_monitor._persist_quote_to_db() writes
--   positions.meta->hard_exit_reference, and ap_exit_engine.seed_from_db()
--   restores that durable hard-exit reference on restart. Production had no
--   positions.meta column, so QPM emitted a repeating `column "meta" does not
--   exist` failure loop while quote/P&L/hard-ref persistence silently failed.
--
-- Idempotent and safe for environments where the column may already exist.

ALTER TABLE positions
  ADD COLUMN IF NOT EXISTS meta JSONB;

UPDATE positions
SET meta = '{}'::jsonb
WHERE meta IS NULL;

ALTER TABLE positions
  ALTER COLUMN meta SET DEFAULT '{}'::jsonb;

ALTER TABLE positions
  ALTER COLUMN meta SET NOT NULL;

COMMENT ON COLUMN positions.meta IS
  'Durable position metadata, including QPM hard_exit_reference restart authority.';
