-- migrations/20260625_handoff_run_locks_schema_fix.sql
-- PR #182 — Fix handoff_run_locks schema collision

BEGIN;

-- =============================================================================
-- ROOT CAUSE
--   Two independent systems both used the table name `handoff_run_locks`:
--
--   System 1 (job dedup lock — ap_handoff_run_lock.py):
--     Created by migrations/2026_06_20_handoff_run_locks.sql — APPLIED first.
--     Schema: run_key TEXT PRIMARY KEY, trade_date, job_name, client_scope, ...
--     Purpose: prevents duplicate Render/GHA runs of the same morning job window.
--
--   System 2 (per-client stage tracker — ap/morning_handoff.py):
--     migrations/20260622_morning_handoff_run_locks.sql used CREATE TABLE IF
--     NOT EXISTS → SILENT NO-OP because the table already existed from System 1.
--     Expected schema: client_id, execution_mode, trading_date, stage, ...
--     Purpose: tracks which handoff stages each client completed today.
--
--   Result: morning_handoff.py crashed with
--     `column "client_id" does not exist FROM handoff_run_locks`
--   because it was querying the System 1 table with System 2 column names.
--
-- FIX
--   1. Rename System 1 table to handoff_job_locks (new canonical name).
--      Data preserved. ap_handoff_run_lock.py updated to use the new name.
--   2. Create handoff_run_locks fresh with the schema morning_handoff.py expects.
--      This is what 20260622_morning_handoff_run_locks.sql should have done.
--
-- APPLY
--   psql "$DATABASE_URL" -f migrations/20260625_handoff_run_locks_schema_fix.sql
-- VERIFY
--   \d public.handoff_job_locks
--   \d public.handoff_run_locks
-- =============================================================================

-- ── Step 1: Rename existing table ────────────────────────────────────────────
-- Preserves all run_key dedup data. ap_handoff_run_lock.py is updated in the
-- same PR to reference handoff_job_locks.
ALTER TABLE IF EXISTS public.handoff_run_locks
    RENAME TO handoff_job_locks;

-- Rename indexes so they stay legible after the table rename.
-- PostgreSQL renames the table but indexes keep their original names.
ALTER INDEX IF EXISTS idx_handoff_run_locks_date
    RENAME TO idx_handoff_job_locks_date;

ALTER INDEX IF EXISTS idx_handoff_run_locks_status_acquired
    RENAME TO idx_handoff_job_locks_status_acquired;

-- ── Step 2: Create handoff_run_locks with the schema morning_handoff.py needs ─
-- This is the table that 20260622_morning_handoff_run_locks.sql tried to create
-- but couldn't because the table already existed with the wrong schema.
CREATE TABLE IF NOT EXISTS public.handoff_run_locks (
    client_id       TEXT        NOT NULL,
    execution_mode  TEXT        NOT NULL,
    trading_date    DATE        NOT NULL,
    stage           TEXT        NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'pending',
    last_run_at     TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_error      TEXT,
    details         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (client_id, execution_mode, trading_date, stage)
);

CREATE INDEX IF NOT EXISTS idx_handoff_run_locks_trading_date
    ON public.handoff_run_locks (trading_date);

CREATE INDEX IF NOT EXISTS idx_handoff_run_locks_client_status
    ON public.handoff_run_locks (client_id, status);

COMMENT ON TABLE public.handoff_job_locks IS
    'Job-level dedup lock. Prevents duplicate Render/GHA runs of the same '
    'morning job window. PK: run_key. Used by ap_handoff_run_lock.py.';

COMMENT ON TABLE public.handoff_run_locks IS
    'Per-client stage tracker for morning handoff. Tracks which stages each '
    'client completed today. PK: (client_id, execution_mode, trading_date, stage). '
    'Used by ap/morning_handoff.py.';

COMMIT;
