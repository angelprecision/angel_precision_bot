CREATE TABLE IF NOT EXISTS handoff_run_locks (
    client_id TEXT NOT NULL,
    execution_mode TEXT NOT NULL,
    trading_date DATE NOT NULL,
    stage TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    last_run_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_error TEXT,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (client_id, execution_mode, trading_date, stage)
);
