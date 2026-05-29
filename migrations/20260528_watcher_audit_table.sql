-- watcher_audit: canonical queryable history for every watcher block,
-- invalidation, rearm event, and OSM persist decision.
-- Lives alongside orders.meta["watcher_audit"] (per-order JSON snapshot).
-- This table is the analytics surface; orders.meta is the order-scoped context.

CREATE TABLE IF NOT EXISTS public.watcher_audit (
    id                              BIGSERIAL PRIMARY KEY,

    -- order / signal linkage
    local_order_id                  TEXT,                        -- FK-like, not enforced (order may be archived)
    signal_id                       TEXT,
    client_id                       TEXT,

    -- instrument
    symbol                          TEXT,
    underlying_price                NUMERIC(12, 4),              -- current_underlying at evaluation time

    -- signal quality
    score                           NUMERIC(6, 2),
    tier                            TEXT,
    direction                       TEXT,                        -- CALL / PUT
    timeframe                       TEXT,
    pattern                         TEXT,

    -- prices at evaluation
    trigger_price                   NUMERIC(12, 4),
    stop_price                      NUMERIC(12, 4),
    current_bid                     NUMERIC(12, 4),
    current_ask                     NUMERIC(12, 4),
    current_mid                     NUMERIC(12, 4),
    arm_price                       NUMERIC(12, 4),
    distance_to_trigger_pct         NUMERIC(10, 6),
    distance_to_stop_pct            NUMERIC(10, 6),

    -- watcher decision
    trigger_type                    TEXT,                        -- arm_time, intraday_check, rearm_check, etc.
    reason_code                     TEXT,                        -- arm_below_stop, arm_drift, rearm_reclaimed, etc.
    raw_reason                      TEXT,
    arm_condition                   TEXT,
    stop_condition                  TEXT,

    -- rearm config snapshot at evaluation time
    rearm_enabled                   BOOLEAN,
    rearm_only_daily_or_overnight   BOOLEAN,
    rearm_window_sec                INTEGER,
    rearm_min_score                 NUMERIC(6, 2),
    rearm_tolerance_pct             NUMERIC(8, 6),
    rearm_max_attempts              INTEGER,

    -- rearm eligibility breakdown
    score_ok                        BOOLEAN,
    tier_ok                         BOOLEAN,
    is_rearm_eligible               BOOLEAN,

    -- lifecycle outcome (derived from reason_code for fast filtering)
    rearmed                         BOOLEAN DEFAULT FALSE,       -- rearm_reclaimed
    expired                         BOOLEAN DEFAULT FALSE,       -- rearm_window_expired, overnight_too_far, etc.
    permanently_rejected            BOOLEAN DEFAULT FALSE,       -- arm_drift, low-score below_stop

    -- full payload for drill-down
    full_payload                    JSONB,

    evaluated_at                    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for the queries you'll actually run
CREATE INDEX IF NOT EXISTS watcher_audit_symbol_idx        ON public.watcher_audit (symbol, evaluated_at DESC);
CREATE INDEX IF NOT EXISTS watcher_audit_reason_code_idx   ON public.watcher_audit (reason_code, evaluated_at DESC);
CREATE INDEX IF NOT EXISTS watcher_audit_local_order_id_idx ON public.watcher_audit (local_order_id) WHERE local_order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS watcher_audit_signal_id_idx     ON public.watcher_audit (signal_id) WHERE signal_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS watcher_audit_rearm_eligible_idx ON public.watcher_audit (is_rearm_eligible, evaluated_at DESC);
