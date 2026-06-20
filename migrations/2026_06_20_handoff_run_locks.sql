-- Idempotency run-lock for market-open morning jobs.
--
-- ⚠ DEPLOY REQUIREMENT: This migration MUST be applied on Supabase before the
-- run-lock provides duplicate-Render/GHA protection. See
-- DEPLOY_morning_jobs_automation.md. If the table is absent, the run-lock helper
-- FAILS OPEN (jobs still run, but cross-scheduler dedup on morning_handoff_audit
-- is inactive until the table exists).
--
-- Apply:  psql "$DATABASE_URL" -f migrations/2026_06_20_handoff_run_locks.sql
-- Verify: \d public.handoff_run_locks
--
-- Render Cron (primary), the GitHub Actions backup, and operator manual
-- triggers can all fire the same job window. This table enforces single
-- execution server-side: the first caller to INSERT a given run_key wins;
-- all others get ON CONFLICT DO NOTHING (rowcount=0) and skip.
--
-- run_key format:
--   morning_job:{trade_date}:{job_name}:{execution_mode}:{client_scope_hash}
--
-- This is REAL enforcement, not log-only job_window_key. The unique PRIMARY
-- KEY on run_key is the lock.

CREATE TABLE IF NOT EXISTS public.handoff_run_locks (
    run_key        TEXT PRIMARY KEY,
    trade_date     DATE        NOT NULL,
    job_name       TEXT        NOT NULL,
    execution_mode TEXT        NOT NULL,
    client_scope   TEXT        NOT NULL,
    triggered_by   TEXT        NOT NULL DEFAULT 'unknown',  -- render_cron | github_backup | manual_operator
    status         TEXT        NOT NULL DEFAULT 'running',  -- running | completed | failed
    acquired_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at   TIMESTAMPTZ,
    result_summary JSONB
);

CREATE INDEX IF NOT EXISTS idx_handoff_run_locks_date
    ON public.handoff_run_locks (trade_date);

CREATE INDEX IF NOT EXISTS idx_handoff_run_locks_status_acquired
    ON public.handoff_run_locks (status, acquired_at);
