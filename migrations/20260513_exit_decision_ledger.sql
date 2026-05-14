-- migrations/20260513_exit_decision_ledger.sql
-- Safe to run multiple times (IF NOT EXISTS throughout).
-- Run in Supabase SQL editor.

CREATE TABLE IF NOT EXISTS exit_decision_ledger (
    id                          BIGSERIAL PRIMARY KEY,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_type                  TEXT NOT NULL,
    client_id                   TEXT,
    position_id                 TEXT,
    signal_id                   TEXT,
    ticker                      TEXT,
    contract                    TEXT,
    side                        TEXT,
    quantity_remaining          INTEGER DEFAULT 0,
    scale_outs_done             INTEGER DEFAULT 0,
    entry_price                 NUMERIC,
    current_option_price        NUMERIC,
    current_bid                 NUMERIC,
    current_ask                 NUMERIC,
    current_underlying          NUMERIC,
    option_pnl_pct              NUMERIC,
    peak_pnl_pct                NUMERIC,
    max_profit_seen             NUMERIC,
    touched_profit              BOOLEAN DEFAULT FALSE,
    quote_state                 TEXT,
    quote_opt_age_sec           NUMERIC,
    quote_und_age_sec           NUMERIC,
    last_quote_update_ts        TIMESTAMPTZ,
    decision_action             TEXT,
    decision_qty                INTEGER DEFAULT 0,
    decision_reason             TEXT,
    decision_reason_code        TEXT,
    decision_urgency            TEXT,
    decision_pnl_pct            NUMERIC,
    exit_in_flight              BOOLEAN DEFAULT FALSE,
    pending_exit_action         TEXT,
    pending_exit_reason         TEXT,
    pending_exit_qty            INTEGER DEFAULT 0,
    pending_exit_local_order_id TEXT,
    pending_exit_broker_order_id TEXT,
    local_order_id              TEXT,
    broker_order_id             TEXT,
    broker_status               TEXT,
    fill_qty                    INTEGER DEFAULT 0,
    fill_price                  NUMERIC,
    error                       TEXT,
    metadata                    JSONB NOT NULL DEFAULT '{}'::jsonb,
    payload                     JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_exit_ledger_position_created
    ON exit_decision_ledger (position_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_exit_ledger_client_created
    ON exit_decision_ledger (client_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_exit_ledger_event_created
    ON exit_decision_ledger (event_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_exit_ledger_action_created
    ON exit_decision_ledger (decision_action, created_at DESC);

COMMENT ON TABLE exit_decision_ledger IS
'Exit audit trail: quote state, P&L, decision, submit, broker, fill, error for every protected exit path.';
