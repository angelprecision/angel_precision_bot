-- =============================================================================
-- P0 — Client Parity: Canonical Signal ID + Client Opportunity Ledger
-- =============================================================================
-- Fixes the client parity visibility bug where the same market opportunity
-- (e.g. UNH CALL, VZ PUT, META CALL on 2026-06-03) is fragmented across
-- multiple per-client signal_ids and cannot be audited as one event.
--
-- This migration is INSTRUMENTATION + LIFECYCLE SAFETY ONLY.
-- It does NOT change scoring, sizing, contract selection, exits, or entry
-- confirmation thresholds. It does NOT add peer-retry auto-submit logic.
--
-- All statements are idempotent. Safe to run multiple times.
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1. orders.canonical_signal_id
-- -----------------------------------------------------------------------------
-- For a plain UUID signal_id, canonical_signal_id == signal_id.
-- For a REEVAL:<uuid>:<hex6> wrapped signal_id, canonical_signal_id == <uuid>.
-- This is the value of build_canonical_signal_id() at insert time, persisted
-- to allow grouping the same opportunity across all client rows.
-- -----------------------------------------------------------------------------
ALTER TABLE orders
  ADD COLUMN IF NOT EXISTS canonical_signal_id TEXT;

-- -----------------------------------------------------------------------------
-- 2. Backfill existing rows
-- -----------------------------------------------------------------------------
-- Strip the trailing :<hex> suffix for REEVAL:<uuid>:<hex>; leave plain UUIDs
-- and any other signal_id values unchanged. Only touch rows where the column
-- is currently NULL so the migration is safe to re-run.
-- -----------------------------------------------------------------------------
UPDATE orders
SET canonical_signal_id =
    CASE
        WHEN signal_id ~ '^REEVAL:[0-9a-fA-F-]{36}:[0-9a-fA-F]+$'
            THEN regexp_replace(signal_id, ':[^:]+$', '')
        ELSE signal_id
    END
WHERE canonical_signal_id IS NULL
  AND signal_id IS NOT NULL;

-- -----------------------------------------------------------------------------
-- 3. Index for parity audits across clients
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_orders_canonical_signal_client
    ON orders (canonical_signal_id, client_id, created_ts);

-- -----------------------------------------------------------------------------
-- 4. client_signal_opportunities — the parity ledger
-- -----------------------------------------------------------------------------
-- One row per (canonical_signal_id, client_id). Tracks every active eligible
-- client's outcome for a given canonical opportunity. UNIQUE constraint
-- prevents accidental duplicate rows from racing inserts.
--
-- This PR creates the table. Subsequent PRs will populate it through the
-- lifecycle stages (CREATED -> CLIENT_ELIGIBLE -> ... -> FILLED/CANCELED).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS client_signal_opportunities (
    id                          BIGSERIAL PRIMARY KEY,
    canonical_signal_id         TEXT        NOT NULL,
    signal_id                   TEXT,
    plan_id                     TEXT,
    client_id                   TEXT        NOT NULL,
    symbol                      TEXT        NOT NULL,
    direction                   TEXT        NOT NULL,
    side                        TEXT,
    timeframe                   TEXT,
    pattern                     TEXT,
    score                       DOUBLE PRECISION,
    tier                        TEXT,
    scanner_type                TEXT,
    scanner_name                TEXT,
    opportunity_status          TEXT        NOT NULL DEFAULT 'CREATED',
    client_eligibility_status   TEXT,
    miss_stage                  TEXT,
    miss_reason                 TEXT,
    block_reason                TEXT,
    contract                    TEXT,
    qty                         INTEGER,
    limit_price                 NUMERIC,
    quote_bid                   NUMERIC,
    quote_ask                   NUMERIC,
    quote_mid                   NUMERIC,
    quote_ts                    TIMESTAMPTZ,
    quote_age_seconds           DOUBLE PRECISION,
    spread_pct                  DOUBLE PRECISION,
    order_id                    BIGINT,
    order_local_id              TEXT,
    broker_order_id             TEXT,
    position_id                 TEXT,
    submitted_ts                TIMESTAMPTZ,
    filled_ts                   TIMESTAMPTZ,
    retry_count                 INTEGER     DEFAULT 0,
    retry_status                TEXT,
    retry_reason                TEXT,
    fanout_batch_id             TEXT,
    created_ts                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_ts                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata                    JSONB       DEFAULT '{}'::jsonb,
    UNIQUE (canonical_signal_id, client_id)
);

-- Lookup indexes
CREATE INDEX IF NOT EXISTS idx_cso_canonical
    ON client_signal_opportunities (canonical_signal_id);
CREATE INDEX IF NOT EXISTS idx_cso_client_created
    ON client_signal_opportunities (client_id, created_ts DESC);
CREATE INDEX IF NOT EXISTS idx_cso_status_created
    ON client_signal_opportunities (opportunity_status, created_ts DESC);

COMMIT;

-- =============================================================================
-- Sanity checks (run as separate statements — these do not modify state)
-- =============================================================================
-- SELECT column_name, data_type FROM information_schema.columns
--  WHERE table_name='orders' AND column_name='canonical_signal_id';
--
-- SELECT count(*) AS total_orders,
--        count(canonical_signal_id) AS with_canonical
--   FROM orders;
--
-- SELECT count(*) AS opp_rows FROM client_signal_opportunities;
