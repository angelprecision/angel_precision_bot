-- migration: fill integrity guardrails
-- Run in Supabase SQL editor.
-- Two parts: audit table first, constraint second (delayed until writer is patched).

-- ── Part 1: broker_order_audit table ─────────────────────────────────────────
-- Stores raw Tradier order responses keyed per client+broker_order_id.
-- Created once; subsequent runs are no-ops (IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS broker_order_audit (
  id               bigserial PRIMARY KEY,
  broker           text      NOT NULL DEFAULT 'tradier',
  client_id        text      NOT NULL,
  local_order_id   text,
  broker_order_id  text      NOT NULL,
  endpoint         text,
  raw_response     jsonb     NOT NULL,
  normalized       jsonb,
  captured_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_broker_order_audit_client_order
  ON broker_order_audit (client_id, broker_order_id);

CREATE INDEX IF NOT EXISTS idx_broker_order_audit_captured_at
  ON broker_order_audit (captured_at DESC);


-- ── Part 2: mark today's two bad rows ────────────────────────────────────────
-- These ENTRY orders are FILLED with filled_qty=0 and fill_price IS NULL.
-- We flag them for manual reconciliation without touching fill_price or filled_qty.

UPDATE orders
SET
  last_error = 'ENTRY_FILLED_WITHOUT_EXEC_FIELDS__RECONCILE_REQUIRED',
  updated_ts = now(),
  meta = COALESCE(meta, '{}'::jsonb) || jsonb_build_object(
    'audit_status',                'ENTRY_FILL_UNPROVEN',
    'missing_exec_quantity',       true,
    'missing_avg_fill_price',      true,
    'requires_broker_order_detail', true,
    'flagged_at',                  now()::text
  )
WHERE kind    = 'ENTRY'
  AND status  = 'FILLED'
  AND filled_qty IS NOT DISTINCT FROM 0
  AND fill_price IS NULL;


-- ── Part 3: constraint — ADD AFTER ap_reconciler.py is deployed ──────────────
-- NOT VALID: enforces on new rows only; skips historical bad rows.
-- Run this ONLY after the bot writer is patched so new fills can pass the check.

-- ALTER TABLE orders
--   ADD CONSTRAINT orders_filled_requires_exec_data
--   CHECK (
--     status NOT IN ('FILLED', 'EXIT_FILLED')
--     OR (
--       filled_qty  IS NOT NULL AND filled_qty  > 0
--       AND fill_price IS NOT NULL AND fill_price > 0
--     )
--   ) NOT VALID;
