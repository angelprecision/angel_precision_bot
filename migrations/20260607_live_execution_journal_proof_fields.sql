-- =============================================================================
-- PR #90 — Live Execution Truth Layer (Tradier Exit Proof Lock)
-- =============================================================================
-- Adds only the proof fields that don't already exist. Existing columns are
-- left UNTOUCHED:
--   * proof_trades.execution_mode   (added by 20260606_orders_execution_mode.sql)
--   * proof_trades.broker_reconciled (added by 20260606_orders_execution_mode.sql)
--   * proof_trades.synthetic_entry  (written by ap_proof_logger; if missing the
--                                    proof_logger upserts it on first close)
--   * proof_trades.local_order_id   (written by ap_proof_logger)
--   * proof_trades.exit_fill_price  (written by ap_proof_logger)
--
-- New columns required for the Tradier Exit Proof Lock per the PR spec:
--
--   broker_entry_order_id            -- broker-assigned id for the ENTRY order
--   broker_exit_order_id             -- broker-assigned id for the EXIT order
--   broker_entry_fill_ts             -- broker timestamp of entry fill
--   broker_exit_fill_ts              -- broker timestamp of exit fill
--   broker_entry_filled_qty          -- broker-reported filled quantity (entry)
--   broker_exit_filled_qty           -- broker-reported filled quantity (exit)
--   entry_price_source               -- which path produced the entry fill price
--   exit_price_source                -- which path produced the exit fill price
--   official_live_performance_eligible
--                                    -- conjunction of every rule in the PR
--                                       spec; default false (conservative).
--
-- Allowed *_price_source values (string enum, app-enforced):
--   TRADIER_ENTRY_FILL
--   TRADIER_EXIT_FILL
--   PAPER_BROKER_FILL
--   MANUAL_REPAIR_TRADIER_FILL
--   MISSING_BROKER_ENTRY_FILL
--   MISSING_BROKER_EXIT_FILL
--   LEGACY_UNKNOWN
--
-- Migration is fully idempotent and rerunnable: every column is added with
-- IF NOT EXISTS, every backfill statement is bounded to rows where the new
-- column IS NULL, and the conservative backfill never marks a legacy row as
-- official.
-- =============================================================================

BEGIN;

-- ---------------------------------------------------------------------------
-- 1a. Defensive: ensure every column the journal READS from proof_trades
--     actually exists. These may have been added by earlier migrations or by
--     ap_proof_logger at runtime, but on a fresh clone or out-of-order apply
--     they may be absent. ADD COLUMN IF NOT EXISTS is a no-op if present, so
--     this is safe and idempotent. Without these guards the journal query
--     LEFT JOIN would raise UndefinedColumn on schemas missing any one.
-- ---------------------------------------------------------------------------
ALTER TABLE proof_trades
    ADD COLUMN IF NOT EXISTS execution_mode    TEXT,
    ADD COLUMN IF NOT EXISTS broker_reconciled BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS synthetic_entry   BOOLEAN,
    ADD COLUMN IF NOT EXISTS local_order_id    TEXT,
    ADD COLUMN IF NOT EXISTS exit_fill_price   NUMERIC;

-- ---------------------------------------------------------------------------
-- 1b. Add new proof-lock columns (additive only; defaults conservative).
-- ---------------------------------------------------------------------------
ALTER TABLE proof_trades
    ADD COLUMN IF NOT EXISTS broker_entry_order_id              TEXT,
    ADD COLUMN IF NOT EXISTS broker_exit_order_id               TEXT,
    ADD COLUMN IF NOT EXISTS broker_entry_fill_ts               TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS broker_exit_fill_ts                TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS broker_entry_filled_qty            NUMERIC,
    ADD COLUMN IF NOT EXISTS broker_exit_filled_qty             NUMERIC,
    ADD COLUMN IF NOT EXISTS entry_price_source                 TEXT,
    ADD COLUMN IF NOT EXISTS exit_price_source                  TEXT,
    ADD COLUMN IF NOT EXISTS official_live_performance_eligible BOOLEAN
        DEFAULT FALSE;

-- ---------------------------------------------------------------------------
-- 2. Conservative backfill for legacy rows
-- ---------------------------------------------------------------------------
-- Spec rule: legacy live rows must NOT become official automatically. Even
-- when broker_*_order_id and fill prices exist, we cannot prove they came
-- from a Tradier fill without a recorded source — so we mark them as
-- LEGACY_UNKNOWN and leave official=false. Only rows that future writers
-- explicitly source as TRADIER_*_FILL or MANUAL_REPAIR_TRADIER_FILL will
-- ever be eligible for official.
--
-- The backfill is double-bounded (column IS NULL guard + per-side guard)
-- so re-running this migration is a no-op for already-stamped rows.
-- ---------------------------------------------------------------------------

-- entry_price_source backfill
UPDATE proof_trades
SET entry_price_source = 'LEGACY_UNKNOWN'
WHERE entry_price_source IS NULL;

-- exit_price_source backfill
UPDATE proof_trades
SET exit_price_source = 'LEGACY_UNKNOWN'
WHERE exit_price_source IS NULL;

-- official_live_performance_eligible: stays at default FALSE for every
-- legacy row. Do NOT compute this here — the read-side classifier in
-- ap/operator/live_execution_journal.py is the single source of truth so
-- the rule can evolve without a migration rewrite. The DB default of
-- FALSE is the safety floor.

-- ---------------------------------------------------------------------------
-- 3. Indexes for the journal query path
-- ---------------------------------------------------------------------------
-- The journal filters on (execution_mode, closed_at) and joins by
-- canonical_signal_id / signal_id back to orders. Both already-indexed
-- columns are reused; the only new index covers broker reconciliation
-- look-ups for the exit-proof classifier.
CREATE INDEX IF NOT EXISTS idx_proof_trades_broker_exit_order
    ON proof_trades (broker_exit_order_id)
    WHERE broker_exit_order_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_proof_trades_official_eligible
    ON proof_trades (official_live_performance_eligible, closed_at DESC);

-- ---------------------------------------------------------------------------
-- 4. Join indexes for the journal query path
-- ---------------------------------------------------------------------------
-- The journal joins:
--   orders o (ENTRY) -> proof_trades p ON p.local_order_id = o.local_order_id
--   orders o         -> orders exit_o  ON exit_o.position_id = ...
--                                     AND exit_o.kind = 'EXIT'
--   orders o         -> client_signal_opportunities cso
--                       ON cso.canonical_signal_id = COALESCE(...)
--                       AND cso.client_id = o.client_id
-- All five indexes are created IF NOT EXISTS so this is a no-op when they
-- already exist from earlier migrations.
CREATE INDEX IF NOT EXISTS idx_proof_trades_local_order_id
    ON proof_trades (local_order_id)
    WHERE local_order_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_orders_local_order_id
    ON orders (local_order_id)
    WHERE local_order_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_orders_created_ts
    ON orders (created_ts DESC);

CREATE INDEX IF NOT EXISTS idx_orders_canonical_signal_id
    ON orders (canonical_signal_id)
    WHERE canonical_signal_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_cso_canonical_client
    ON client_signal_opportunities (canonical_signal_id, client_id);

COMMIT;

-- =============================================================================
-- Optional sanity checks (run separately):
--   SELECT count(*) AS legacy_proofs
--     FROM proof_trades WHERE entry_price_source='LEGACY_UNKNOWN';
--   SELECT count(*) AS official
--     FROM proof_trades WHERE official_live_performance_eligible=TRUE;
-- =============================================================================
