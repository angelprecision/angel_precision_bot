-- migration: client_signal_opportunities table
-- PR1: Client Opportunity Ledger
-- PR81 FINAL AMENDMENT §1: canonical_signal_id is the true unique key.
-- Run in Supabase SQL editor before deploying the bot update.
--
-- This migration is SAFE to re-run on an existing table:
--   1. Creates the table if missing.
--   2. Backfills canonical_signal_id from signal_id where NULL.
--   3. Resolves duplicate (canonical_signal_id, client_id) rows by keeping the
--      most-progressed row (FILLED > BROKER_* > ORDER_CREATED > PREFLIGHT_* > MISSED/SKIPPED > CREATED).
--   4. Enforces canonical_signal_id NOT NULL.
--   5. Drops the legacy (signal_id, client_id) unique index.
--   6. Adds the canonical (canonical_signal_id, client_id) unique index.
--   7. Adds the new lifecycle columns added by PR81 amendments.
--   8. Reporting indexes.

CREATE TABLE IF NOT EXISTS client_signal_opportunities (
  id                         bigserial PRIMARY KEY,
  signal_id                  text      NOT NULL,
  canonical_signal_id        text,
  plan_id                    text,
  client_id                  text      NOT NULL,
  symbol                     text,
  direction                  text,
  side                       text,
  timeframe                  text,
  pattern                    text,
  score                      numeric,
  tier                       text,
  scanner_type               text,
  scanner_name               text,
  signal_created_at          timestamptz,
  client_eligibility_status  text      DEFAULT 'CLIENT_ELIGIBLE',
  opportunity_status         text      NOT NULL DEFAULT 'CREATED',
  miss_stage                 text,
  miss_reason                text,
  order_local_id             text,
  broker_order_id            text,
  position_id                text,
  quote_age_seconds          numeric,
  spread_pct                 numeric,
  buying_power_snapshot      numeric,
  cap_snapshot               jsonb,
  kill_switch_state          boolean,
  entries_paused_state       boolean,
  entry_confirmation_result  text,
  preflight_enforced         boolean,
  execution_continued        boolean,
  would_block_reason         text,
  retry_status               text,
  retry_reason               text,
  created_at                 timestamptz NOT NULL DEFAULT now(),
  updated_at                 timestamptz NOT NULL DEFAULT now(),
  metadata                   jsonb
);

-- Ensure new amendment columns exist on previously-deployed tables.
ALTER TABLE client_signal_opportunities
  ADD COLUMN IF NOT EXISTS preflight_enforced  boolean,
  ADD COLUMN IF NOT EXISTS execution_continued boolean,
  ADD COLUMN IF NOT EXISTS would_block_reason  text;

-- ── §1.1 Backfill canonical_signal_id ────────────────────────────────────────
UPDATE client_signal_opportunities
   SET canonical_signal_id = signal_id
 WHERE canonical_signal_id IS NULL OR canonical_signal_id = '';

-- ── §1.2 Resolve duplicate (canonical_signal_id, client_id) rows ─────────────
-- Final Amendment v2 §2: terminals must rank ABOVE non-terminals so dedup
-- never deletes a terminal truth (FILLED, BROKER_REJECTED, EXPIRED, CANCELED,
-- MISSED, CLIENT_SKIPPED, INTERNAL_ERROR, WATCHER_INVALIDATED,
-- ENTRY_CONFIRMATION_FAILED) in favor of a preliminary status
-- (PREFLIGHT_*, ORDER_CREATED, WATCHER_ARMED, BROKER_SUBMITTED, BROKER_ACKED).
-- Tie-break: prefer the more recently updated row.
WITH ranked AS (
  SELECT
    id,
    ROW_NUMBER() OVER (
      PARTITION BY canonical_signal_id, client_id
      ORDER BY
        CASE opportunity_status
          -- Terminals (all outrank every non-terminal)
          WHEN 'FILLED'                    THEN 1000
          WHEN 'BROKER_REJECTED'           THEN  990
          WHEN 'EXPIRED'                   THEN  980
          WHEN 'CANCELED'                  THEN  970
          WHEN 'MISSED'                    THEN  960
          WHEN 'CLIENT_SKIPPED'            THEN  950
          WHEN 'INTERNAL_ERROR'            THEN  940
          WHEN 'WATCHER_INVALIDATED'       THEN  930
          WHEN 'ENTRY_CONFIRMATION_FAILED' THEN  920
          -- Non-terminal progress
          WHEN 'BROKER_ACKED'              THEN   90
          WHEN 'BROKER_SUBMITTED'          THEN   80
          WHEN 'WATCHER_ARMED'             THEN   70
          WHEN 'ORDER_CREATED'             THEN   60
          WHEN 'PREFLIGHT_PASSED'          THEN   50
          WHEN 'PREFLIGHT_WARNING'         THEN   40
          WHEN 'CLIENT_ELIGIBLE'           THEN   20
          WHEN 'CREATED'                   THEN   10
          ELSE                                     0
        END DESC,
        updated_at DESC NULLS LAST,
        id DESC
    ) AS rn
  FROM client_signal_opportunities
)
DELETE FROM client_signal_opportunities
 WHERE id IN (SELECT id FROM ranked WHERE rn > 1);

-- ── §1.3 Enforce canonical_signal_id NOT NULL ────────────────────────────────
ALTER TABLE client_signal_opportunities
  ALTER COLUMN canonical_signal_id SET NOT NULL;

-- ── §1.4 Drop legacy unique index on (signal_id, client_id) ──────────────────
DROP INDEX IF EXISTS idx_cso_signal_client;

-- ── §1.5 Add canonical unique index ──────────────────────────────────────────
CREATE UNIQUE INDEX IF NOT EXISTS
  idx_cso_canonical_client
  ON client_signal_opportunities (canonical_signal_id, client_id);

-- ── Reporting indexes ────────────────────────────────────────────────────────
-- signal_id remains the latest raw execution signal id (non-unique lookup).
CREATE INDEX IF NOT EXISTS idx_cso_signal_id
  ON client_signal_opportunities (signal_id);
CREATE INDEX IF NOT EXISTS idx_cso_client_id
  ON client_signal_opportunities (client_id);
CREATE INDEX IF NOT EXISTS idx_cso_symbol_created
  ON client_signal_opportunities (symbol, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_cso_status
  ON client_signal_opportunities (opportunity_status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_cso_miss_reason
  ON client_signal_opportunities (miss_reason) WHERE miss_reason IS NOT NULL;
