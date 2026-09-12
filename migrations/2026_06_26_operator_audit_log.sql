-- ─────────────────────────────────────────────────────────────────────────────
-- Migration: 2026_06_26_operator_audit_log.sql
-- PR: feat/operator-manual-close-endpoint
--
-- Creates the operator_audit_log table that records every manual operator
-- intervention on a position (manual close, force close, price corrections).
--
-- Run BEFORE deploying the /admin/operator/manual-close endpoint.
-- Idempotent — safe to re-run.
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS operator_audit_log (
    id                BIGSERIAL    PRIMARY KEY,
    event_type        TEXT         NOT NULL,
    client_id         TEXT         NOT NULL,
    position_id       TEXT,
    contract          TEXT,
    true_fill_price   NUMERIC(12, 4),
    tradier_order_id  TEXT,
    reason            TEXT,
    realized_pnl      NUMERIC(12, 2),
    realized_pnl_pct  NUMERIC(10, 4),
    operator_note     TEXT,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_operator_audit_log_client_id
    ON operator_audit_log (client_id);

CREATE INDEX IF NOT EXISTS idx_operator_audit_log_created_at
    ON operator_audit_log (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_operator_audit_log_position_id
    ON operator_audit_log (position_id)
    WHERE position_id IS NOT NULL;

-- RLS: service role only — anon key gets nothing
ALTER TABLE operator_audit_log ENABLE ROW LEVEL SECURITY;

-- Verify
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'operator_audit_log'
ORDER BY ordinal_position;
