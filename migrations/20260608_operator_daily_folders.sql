-- =============================================================================
-- PR #91 — Daily Operator Folders -> Weekly Archive Pipeline
-- =============================================================================
-- Two tables (additive only):
--
--   operator_daily_folders
--     One row per trading day. Each row holds every operator dashboard
--     section as a JSONB blob, plus section_errors / missing_sections /
--     source_status so partial days are honest.
--
--   weekly_rollups
--     One row per ISO week. Built by aggregating operator_daily_folders
--     rows (NOT by re-running live queries). Surfaces days_present /
--     days_missing / section_errors_by_day so the weekly archive cannot
--     silently swallow a missing day.
--
-- Rerunnable / idempotent:
--   * CREATE TABLE IF NOT EXISTS
--   * Every column add uses ADD COLUMN IF NOT EXISTS
--   * Every index uses CREATE INDEX IF NOT EXISTS
--
-- Trading state is NOT touched. This migration is archive/reporting only.
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. operator_daily_folders
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS operator_daily_folders (
    folder_date              DATE        PRIMARY KEY,
    iso_week                 TEXT        NOT NULL,
    generated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_status            TEXT        NOT NULL DEFAULT 'ok',
    command_center           JSONB       NOT NULL DEFAULT '{}'::jsonb,
    scanner_truth            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    intelligence_truth       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    execution_truth          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    live_execution_journal   JSONB       NOT NULL DEFAULT '{}'::jsonb,
    trade_volume_funnel      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    missed_winner_report     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    client_parity            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    ghost_impact             JSONB       NOT NULL DEFAULT '{}'::jsonb,
    score_regression         JSONB       NOT NULL DEFAULT '{}'::jsonb,
    ticker_truth             JSONB       NOT NULL DEFAULT '{}'::jsonb,
    client_balances          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    rejected_setups          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    failed_orders            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    closed_trades            JSONB       NOT NULL DEFAULT '{}'::jsonb,
    summary                  JSONB       NOT NULL DEFAULT '{}'::jsonb,
    section_errors           JSONB       NOT NULL DEFAULT '{}'::jsonb,
    missing_sections         JSONB       NOT NULL DEFAULT '[]'::jsonb,
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Defensive: if the table already existed in an older shape, ensure every
-- column we now require exists. Each ADD is a no-op when present.
ALTER TABLE operator_daily_folders
    ADD COLUMN IF NOT EXISTS iso_week                TEXT,
    ADD COLUMN IF NOT EXISTS generated_at            TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS source_status           TEXT,
    ADD COLUMN IF NOT EXISTS command_center          JSONB,
    ADD COLUMN IF NOT EXISTS scanner_truth           JSONB,
    ADD COLUMN IF NOT EXISTS intelligence_truth      JSONB,
    ADD COLUMN IF NOT EXISTS execution_truth         JSONB,
    ADD COLUMN IF NOT EXISTS live_execution_journal  JSONB,
    ADD COLUMN IF NOT EXISTS trade_volume_funnel     JSONB,
    ADD COLUMN IF NOT EXISTS missed_winner_report    JSONB,
    ADD COLUMN IF NOT EXISTS client_parity           JSONB,
    ADD COLUMN IF NOT EXISTS ghost_impact            JSONB,
    ADD COLUMN IF NOT EXISTS score_regression        JSONB,
    ADD COLUMN IF NOT EXISTS ticker_truth            JSONB,
    ADD COLUMN IF NOT EXISTS client_balances         JSONB,
    ADD COLUMN IF NOT EXISTS rejected_setups         JSONB,
    ADD COLUMN IF NOT EXISTS failed_orders           JSONB,
    ADD COLUMN IF NOT EXISTS closed_trades           JSONB,
    ADD COLUMN IF NOT EXISTS summary                 JSONB,
    ADD COLUMN IF NOT EXISTS section_errors          JSONB,
    ADD COLUMN IF NOT EXISTS missing_sections        JSONB,
    ADD COLUMN IF NOT EXISTS updated_at              TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_operator_daily_folders_iso_week
    ON operator_daily_folders (iso_week);

CREATE INDEX IF NOT EXISTS idx_operator_daily_folders_folder_date
    ON operator_daily_folders (folder_date DESC);

-- ---------------------------------------------------------------------------
-- 2. weekly_rollups  (BACKWARD-COMPATIBLE)
-- ---------------------------------------------------------------------------
-- IMPORTANT BACK-COMPAT NOTE
--
-- `weekly_rollups` may already exist in production from an earlier PR
-- with a different shape. We MUST NOT drop or replace its existing
-- primary key, and we MUST NOT rename or remove any pre-existing
-- columns the current weekly dashboard reads.
--
-- The strategy:
--   1. For FRESH installs, create the table with id BIGSERIAL PK
--      (NOT iso_week PK) so we never collide with an older PK shape.
--   2. For LEGACY installs where the table already exists, every PR91
--      column — INCLUDING iso_week — is added with ADD COLUMN
--      IF NOT EXISTS. Existing columns / PK are left untouched.
--   3. The conflict target for upserts is the UNIQUE PARTIAL INDEX
--      on (iso_week) WHERE iso_week IS NOT NULL — NOT the primary
--      key. This lets ON CONFLICT work without touching the existing
--      PK and tolerates legacy rows whose iso_week is null.
--   4. Best-effort backfill of iso_week from week_start is bounded by
--      `WHERE iso_week IS NULL` so it never overwrites a real value
--      and never errors if week_start is missing on a legacy row.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS weekly_rollups (
    id                       BIGSERIAL   PRIMARY KEY,
    iso_week                 TEXT,
    week_start               DATE,
    week_end                 DATE,
    generated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    days_present             JSONB       NOT NULL DEFAULT '[]'::jsonb,
    days_missing             JSONB       NOT NULL DEFAULT '[]'::jsonb,
    daily_index              JSONB       NOT NULL DEFAULT '[]'::jsonb,
    section_errors_by_day    JSONB       NOT NULL DEFAULT '{}'::jsonb,
    missing_sections_by_day  JSONB       NOT NULL DEFAULT '{}'::jsonb,
    source_status_by_day     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    totals                   JSONB       NOT NULL DEFAULT '{}'::jsonb,
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Defensive ADDs for both fresh + legacy installs. EVERY PR91 column
-- (including iso_week) is added IF NOT EXISTS. Nothing is renamed, no
-- existing column is altered, no existing PK is dropped.
ALTER TABLE weekly_rollups
    ADD COLUMN IF NOT EXISTS iso_week                TEXT,
    ADD COLUMN IF NOT EXISTS week_start              DATE,
    ADD COLUMN IF NOT EXISTS week_end                DATE,
    ADD COLUMN IF NOT EXISTS generated_at            TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS days_present            JSONB,
    ADD COLUMN IF NOT EXISTS days_missing            JSONB,
    ADD COLUMN IF NOT EXISTS daily_index             JSONB,
    ADD COLUMN IF NOT EXISTS section_errors_by_day   JSONB,
    ADD COLUMN IF NOT EXISTS missing_sections_by_day JSONB,
    ADD COLUMN IF NOT EXISTS source_status_by_day    JSONB,
    ADD COLUMN IF NOT EXISTS totals                  JSONB,
    ADD COLUMN IF NOT EXISTS updated_at              TIMESTAMPTZ;

-- Unique partial index on iso_week. This is the ON CONFLICT target for
-- PR91 upserts and does NOT change the primary key. The partial WHERE
-- clause means legacy rows with NULL iso_week do not collide with each
-- other or block this index from being created.
CREATE UNIQUE INDEX IF NOT EXISTS uq_weekly_rollups_iso_week
    ON weekly_rollups (iso_week)
    WHERE iso_week IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_weekly_rollups_week_start
    ON weekly_rollups (week_start DESC);

-- ---------------------------------------------------------------------------
-- 2b. Best-effort iso_week backfill
-- ---------------------------------------------------------------------------
-- For legacy rows that have a week_start but no iso_week, derive the
-- ISO label from the date. Bounded by `iso_week IS NULL` so re-running
-- never overwrites a real value, and bounded by `week_start IS NOT NULL`
-- so legacy rows without a week_start are left intact (no crash, no
-- bogus value).
--
-- to_char(date, 'IYYY-"W"IW') produces e.g. '2026-W23' — the same
-- format PR91's ap_operator_daily_folders.iso_week_string emits.
UPDATE weekly_rollups
   SET iso_week = to_char(week_start, 'IYYY"-W"IW')
 WHERE iso_week IS NULL
   AND week_start IS NOT NULL;

COMMIT;

-- =============================================================================
-- Optional sanity checks (run separately):
--   SELECT count(*) AS days FROM operator_daily_folders;
--   SELECT iso_week, source_status FROM operator_daily_folders ORDER BY folder_date DESC LIMIT 14;
--   SELECT iso_week, jsonb_array_length(days_present) AS present,
--          jsonb_array_length(days_missing) AS missing
--     FROM weekly_rollups ORDER BY week_start DESC LIMIT 8;
-- =============================================================================
