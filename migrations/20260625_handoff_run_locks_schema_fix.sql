-- migrations/20260625_handoff_run_locks_schema_fix.sql
-- PR #182 — Fix handoff_run_locks schema collision (safe, conditional, idempotent)
--
-- ROOT CAUSE
--   migrations/2026_06_20_handoff_run_locks.sql created handoff_run_locks with
--   run_key TEXT PRIMARY KEY (for ap_handoff_run_lock.py job-dedup locks).
--   migrations/20260622_morning_handoff_run_locks.sql used CREATE TABLE IF NOT
--   EXISTS → silent no-op. morning_handoff.py expected client_id / stage columns
--   that never existed. Production crash: column "client_id" does not exist.
--
-- WHAT THIS MIGRATION DOES
--   1. Renames handoff_run_locks → handoff_job_locks ONLY when:
--        - handoff_run_locks has run_key (old schema), AND
--        - handoff_run_locks does NOT already have client_id (new schema), AND
--        - handoff_job_locks does not already exist.
--      Raises an exception if the rename cannot be done safely.
--   2. Creates handoff_run_locks with client_id schema IF it does not exist.
--   3. Asserts the final state is correct. Raises if anything is wrong.
--
-- IDEMPOTENT: safe to re-run.
-- ATOMIC: runs in a single transaction.
--
-- APPLY
--   psql "$DATABASE_URL" -f migrations/20260625_handoff_run_locks_schema_fix.sql
-- VERIFY
--   \d public.handoff_job_locks   -- run_key PK
--   \d public.handoff_run_locks   -- client_id PK
-- =============================================================================

BEGIN;

-- ── Step 1: Conditional rename ───────────────────────────────────────────────

DO $$
DECLARE
    v_run_has_run_key   boolean;
    v_run_has_client_id boolean;
    v_job_locks_exists  boolean;
BEGIN

    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'handoff_run_locks'
          AND column_name  = 'run_key'
    ) INTO v_run_has_run_key;

    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'handoff_run_locks'
          AND column_name  = 'client_id'
    ) INTO v_run_has_client_id;

    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name   = 'handoff_job_locks'
    ) INTO v_job_locks_exists;

    -- Case A: old run_key schema present, client_id absent → rename.
    IF v_run_has_run_key AND NOT v_run_has_client_id THEN

        IF v_job_locks_exists THEN
            RAISE EXCEPTION
                'Cannot rename handoff_run_locks → handoff_job_locks: '
                'handoff_job_locks already exists. '
                'Inspect both tables and resolve manually before re-running.';
        END IF;

        ALTER TABLE public.handoff_run_locks RENAME TO handoff_job_locks;

        ALTER INDEX IF EXISTS idx_handoff_run_locks_date
            RENAME TO idx_handoff_job_locks_date;
        ALTER INDEX IF EXISTS idx_handoff_run_locks_status_acquired
            RENAME TO idx_handoff_job_locks_status_acquired;

        RAISE NOTICE 'Renamed handoff_run_locks → handoff_job_locks (run_key schema preserved).';

    -- Case B: client_id already present → table already has the new schema.
    ELSIF v_run_has_client_id THEN
        RAISE NOTICE 'handoff_run_locks already has client_id — rename skipped.';

    -- Case C: table absent entirely → nothing to rename.
    ELSIF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name   = 'handoff_run_locks'
    ) THEN
        RAISE NOTICE 'handoff_run_locks does not exist — rename not needed.';

    -- Case D: table exists but has neither run_key nor client_id → unknown state.
    ELSE
        RAISE EXCEPTION
            'handoff_run_locks exists but has neither run_key nor client_id. '
            'Unknown schema state — manual inspection required before re-running.';
    END IF;

END $$;

-- ── Step 2: Create handoff_run_locks with client_id schema (idempotent) ──────

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
    'Job-level dedup lock. PK: run_key. Used by ap_handoff_run_lock.py.';

COMMENT ON TABLE public.handoff_run_locks IS
    'Per-client stage tracker for morning handoff. '
    'PK: (client_id, execution_mode, trading_date, stage). '
    'Used by ap/morning_handoff.py.';

-- ── Step 3: Final assertions — fail loudly if schema is wrong ─────────────────

DO $$
DECLARE
    v_job_has_run_key        boolean;
    v_run_has_client_id      boolean;
    v_run_has_execution_mode boolean;
    v_run_has_trading_date   boolean;
    v_run_has_stage          boolean;
BEGIN

    -- handoff_job_locks: must have run_key if the table exists
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'handoff_job_locks'
          AND column_name  = 'run_key'
    ) INTO v_job_has_run_key;

    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'handoff_job_locks'
    ) AND NOT v_job_has_run_key THEN
        RAISE EXCEPTION
            'ASSERTION FAILED: public.handoff_job_locks exists but has no run_key column. '
            'Migration ended in an unexpected state.';
    END IF;

    -- handoff_run_locks: must have client_id, execution_mode, trading_date, stage
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'handoff_run_locks'
          AND column_name  = 'client_id'
    ) INTO v_run_has_client_id;

    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'handoff_run_locks'
          AND column_name  = 'execution_mode'
    ) INTO v_run_has_execution_mode;

    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'handoff_run_locks'
          AND column_name  = 'trading_date'
    ) INTO v_run_has_trading_date;

    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = 'handoff_run_locks'
          AND column_name  = 'stage'
    ) INTO v_run_has_stage;

    IF NOT v_run_has_client_id THEN
        RAISE EXCEPTION
            'ASSERTION FAILED: public.handoff_run_locks missing client_id.';
    END IF;

    IF NOT v_run_has_execution_mode THEN
        RAISE EXCEPTION
            'ASSERTION FAILED: public.handoff_run_locks missing execution_mode.';
    END IF;

    IF NOT v_run_has_trading_date THEN
        RAISE EXCEPTION
            'ASSERTION FAILED: public.handoff_run_locks missing trading_date.';
    END IF;

    IF NOT v_run_has_stage THEN
        RAISE EXCEPTION
            'ASSERTION FAILED: public.handoff_run_locks missing stage.';
    END IF;

    RAISE NOTICE
        'PR #182 schema assertions passed: '
        'handoff_job_locks=run_key, handoff_run_locks=client_id+execution_mode+trading_date+stage.';

END $$;

COMMIT;
