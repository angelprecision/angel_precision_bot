-- migrations/20260528_watcher_audit_table.sql
-- watcher_audit: canonical queryable history for every watcher block,
-- invalidation, rearm event, and OSM persist decision.
--
-- IDEMPOTENT — safe to re-run.
-- CREATE TABLE IF NOT EXISTS handles the fresh-install case.
-- Individual ALTER TABLE ADD COLUMN IF NOT EXISTS handle the case where
-- an earlier partial version of this table exists.

CREATE TABLE IF NOT EXISTS public.watcher_audit (
    id                              BIGSERIAL PRIMARY KEY,
    local_order_id                  TEXT,
    signal_id                       TEXT,
    client_id                       TEXT,
    symbol                          TEXT,
    underlying_price                NUMERIC(12, 4),
    score                           NUMERIC(6, 2),
    tier                            TEXT,
    direction                       TEXT,
    timeframe                       TEXT,
    pattern                         TEXT,
    trigger_price                   NUMERIC(12, 4),
    stop_price                      NUMERIC(12, 4),
    current_bid                     NUMERIC(12, 4),
    current_ask                     NUMERIC(12, 4),
    current_mid                     NUMERIC(12, 4),
    arm_price                       NUMERIC(12, 4),
    distance_to_trigger_pct         NUMERIC(10, 6),
    distance_to_stop_pct            NUMERIC(10, 6),
    trigger_type                    TEXT,
    reason_code                     TEXT,
    raw_reason                      TEXT,
    arm_condition                   TEXT,
    stop_condition                  TEXT,
    rearm_enabled                   BOOLEAN,
    rearm_only_daily_or_overnight   BOOLEAN,
    rearm_window_sec                INTEGER,
    rearm_min_score                 NUMERIC(6, 2),
    rearm_tolerance_pct             NUMERIC(8, 6),
    rearm_max_attempts              INTEGER,
    score_ok                        BOOLEAN,
    tier_ok                         BOOLEAN,
    is_rearm_eligible               BOOLEAN,
    rearmed                         BOOLEAN DEFAULT FALSE,
    expired                         BOOLEAN DEFAULT FALSE,
    permanently_rejected            BOOLEAN DEFAULT FALSE,
    full_payload                    JSONB,
    evaluated_at                    TIMESTAMPTZ DEFAULT NOW()
);

-- ── Idempotent column additions ───────────────────────────────────────────
-- Safe to run even if the table was created from an earlier partial migration.
-- evaluated_at is the column that triggered the 42703 error; it is listed
-- first so a partial re-run still fixes the immediate production error.
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS evaluated_at                  TIMESTAMPTZ DEFAULT NOW();
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS full_payload                  JSONB;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS permanently_rejected          BOOLEAN DEFAULT FALSE;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS expired                       BOOLEAN DEFAULT FALSE;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS rearmed                       BOOLEAN DEFAULT FALSE;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS is_rearm_eligible             BOOLEAN;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS tier_ok                       BOOLEAN;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS score_ok                      BOOLEAN;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS rearm_max_attempts            INTEGER;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS rearm_tolerance_pct           NUMERIC(8, 6);
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS rearm_min_score               NUMERIC(6, 2);
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS rearm_window_sec              INTEGER;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS rearm_only_daily_or_overnight BOOLEAN;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS rearm_enabled                 BOOLEAN;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS stop_condition                TEXT;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS arm_condition                 TEXT;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS raw_reason                    TEXT;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS reason_code                   TEXT;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS trigger_type                  TEXT;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS distance_to_stop_pct          NUMERIC(10, 6);
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS distance_to_trigger_pct       NUMERIC(10, 6);
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS arm_price                     NUMERIC(12, 4);
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS current_mid                   NUMERIC(12, 4);
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS current_ask                   NUMERIC(12, 4);
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS current_bid                   NUMERIC(12, 4);
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS stop_price                    NUMERIC(12, 4);
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS trigger_price                 NUMERIC(12, 4);
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS pattern                       TEXT;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS timeframe                     TEXT;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS direction                     TEXT;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS tier                          TEXT;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS score                         NUMERIC(6, 2);
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS underlying_price              NUMERIC(12, 4);
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS symbol                        TEXT;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS client_id                     TEXT;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS signal_id                     TEXT;
ALTER TABLE public.watcher_audit ADD COLUMN IF NOT EXISTS local_order_id                TEXT;

-- ── Indexes ───────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS watcher_audit_symbol_idx
    ON public.watcher_audit (symbol, evaluated_at DESC);
CREATE INDEX IF NOT EXISTS watcher_audit_reason_code_idx
    ON public.watcher_audit (reason_code, evaluated_at DESC);
CREATE INDEX IF NOT EXISTS watcher_audit_local_order_id_idx
    ON public.watcher_audit (local_order_id) WHERE local_order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS watcher_audit_signal_id_idx
    ON public.watcher_audit (signal_id) WHERE signal_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS watcher_audit_rearm_eligible_idx
    ON public.watcher_audit (is_rearm_eligible, evaluated_at DESC);
