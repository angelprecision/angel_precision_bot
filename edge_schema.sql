-- edge_schema.sql -- Edge Intelligence tables for Angel Precision Bot
-- =============================================================================
-- Run once against Supabase Postgres. Safe to re-run (IF NOT EXISTS everywhere).
-- =============================================================================

-- Full trade record with all fields needed for edge analysis
CREATE TABLE IF NOT EXISTS ap_trade_log (
    trade_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    signal_id           UUID,
    client_id           TEXT NOT NULL,
    date                DATE NOT NULL DEFAULT CURRENT_DATE,
    entry_ts            TIMESTAMPTZ,
    exit_ts             TIMESTAMPTZ,

    -- Setup identity
    ticker              TEXT NOT NULL,
    direction           TEXT NOT NULL,  -- CALL / PUT
    timeframe           TEXT,
    setup_combo         TEXT,           -- e.g. "2-1_2U", "1-2_2D"
    scanner_type        TEXT,           -- e.g. "scanner_212", "scanner_322"
    score               FLOAT,
    tier                TEXT,

    -- Time context
    time_bucket         TEXT,           -- "09:30-10:00", "10:00-11:00", etc.
    day_of_week         TEXT,
    market_regime       TEXT,           -- trend_up, trend_down, chop, gap_up, gap_down
    vol_regime          TEXT,           -- low, normal, high

    -- Execution
    trigger_price       FLOAT,
    underlying_entry    FLOAT,
    contract_symbol     TEXT,
    contract_entry_price FLOAT,
    contracts           INT,
    planned_stop        FLOAT,
    planned_target      FLOAT,

    -- Outcome
    underlying_exit     FLOAT,
    contract_exit_price FLOAT,
    gross_pnl           FLOAT,
    net_pnl             FLOAT,
    return_pct          FLOAT,
    r_multiple          FLOAT,          -- (pnl / risk_per_contract)
    win_flag            SMALLINT,       -- 1=win, 0=loss
    mfe_pct             FLOAT,          -- max favorable excursion %
    mae_pct             FLOAT,          -- max adverse excursion %
    bars_held           INT,
    exit_reason         TEXT,           -- target_hit, stop_hit, time_stop, eod, manual

    -- Quality
    broker_fill_confirmed BOOLEAN DEFAULT FALSE,
    slippage_pct        FLOAT,
    spread_at_entry     FLOAT,

    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Edge summary per bucket (recomputed nightly)
CREATE TABLE IF NOT EXISTS ap_edge_buckets (
    bucket_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id           TEXT NOT NULL,
    bucket_type         TEXT NOT NULL,  -- ticker, timeframe, setup, time_of_day, combo
    bucket_key          TEXT NOT NULL,  -- e.g. "NVDA", "1d", "2-1_2U", "09:30-10:00"
    n_trades            INT DEFAULT 0,
    win_rate            FLOAT,
    avg_win             FLOAT,
    avg_loss            FLOAT,
    profit_factor       FLOAT,
    expectancy          FLOAT,
    edge_score          FLOAT,
    max_drawdown        FLOAT,
    median_return       FLOAT,
    std_dev_return      FLOAT,
    status              TEXT DEFAULT 'watch',  -- promote, watch, downgrade, kill
    last_computed_at    TIMESTAMPTZ DEFAULT NOW(),
    last_reviewed_at    TIMESTAMPTZ,
    UNIQUE(client_id, bucket_type, bucket_key)
);

-- Production whitelist — only active rows get auto-traded
CREATE TABLE IF NOT EXISTS ap_whitelist (
    whitelist_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    client_id           TEXT NOT NULL,
    ticker              TEXT,
    timeframe           TEXT,
    setup_combo         TEXT,
    direction           TEXT,
    time_bucket         TEXT,
    status              TEXT DEFAULT 'watch',  -- active, watch, killed
    edge_score          FLOAT DEFAULT 0,
    n_trades            INT DEFAULT 0,
    last_reviewed_at    TIMESTAMPTZ DEFAULT NOW(),
    notes               TEXT,
    UNIQUE(client_id, ticker, timeframe, setup_combo, direction, time_bucket)
);

CREATE INDEX IF NOT EXISTS idx_trade_log_client_date ON ap_trade_log(client_id, date);
CREATE INDEX IF NOT EXISTS idx_trade_log_ticker ON ap_trade_log(ticker, direction, timeframe);
CREATE INDEX IF NOT EXISTS idx_edge_buckets_client ON ap_edge_buckets(client_id, bucket_type, status);
CREATE INDEX IF NOT EXISTS idx_whitelist_active ON ap_whitelist(client_id, status);
