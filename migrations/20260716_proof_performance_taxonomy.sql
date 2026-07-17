-- P0 — Performance taxonomy and training eligibility
--
-- Purpose:
--   Keep LIVE-official, LIVE-unreconciled, PAPER, and unknown proof rows in
--   separate accounting classes. Legacy/PAPER rows never become learning input
--   automatically. The migration is additive and idempotent.

BEGIN;

-- Defensive prerequisites make this migration safe even when an older database
-- missed the earlier proof-lock migrations. Defaults remain conservative.
ALTER TABLE proof_trades
    ADD COLUMN IF NOT EXISTS execution_mode TEXT,
    ADD COLUMN IF NOT EXISTS official_live_performance_eligible BOOLEAN
        DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS performance_taxonomy TEXT NOT NULL
        DEFAULT 'UNKNOWN_QUARANTINED',
    ADD COLUMN IF NOT EXISTS training_eligible BOOLEAN NOT NULL
        DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS taxonomy_reason TEXT,
    ADD COLUMN IF NOT EXISTS quote_domain_consistent BOOLEAN;

-- Conservative classification only. No legacy row is promoted to LIVE_OFFICIAL
-- unless its existing proof-lock boolean is already explicitly true.
UPDATE proof_trades
SET performance_taxonomy = CASE
        WHEN official_live_performance_eligible IS TRUE
          AND LOWER(COALESCE(execution_mode, '')) = 'live'
            THEN 'LIVE_OFFICIAL'
        WHEN LOWER(COALESCE(execution_mode, '')) = 'live'
            THEN 'LIVE_UNRECONCILED'
        WHEN LOWER(COALESCE(execution_mode, '')) = 'paper'
            THEN 'PAPER_UNVERIFIED'
        ELSE 'UNKNOWN_QUARANTINED'
    END,
    training_eligible = CASE
        WHEN official_live_performance_eligible IS TRUE
          AND LOWER(COALESCE(execution_mode, '')) = 'live'
            THEN TRUE
        ELSE FALSE
    END,
    taxonomy_reason = CASE
        WHEN official_live_performance_eligible IS TRUE
          AND LOWER(COALESCE(execution_mode, '')) = 'live'
            THEN 'existing_tradier_exit_proof_lock_passed'
        WHEN LOWER(COALESCE(execution_mode, '')) = 'live'
            THEN 'live_row_missing_complete_broker_proof'
        WHEN LOWER(COALESCE(execution_mode, '')) = 'paper'
            THEN 'paper_execution_excluded_from_live_learning'
        ELSE 'execution_mode_or_originating_entry_identity_unknown'
    END
WHERE performance_taxonomy = 'UNKNOWN_QUARANTINED'
   OR performance_taxonomy IS NULL;

CREATE INDEX IF NOT EXISTS idx_proof_trades_training_eligible_closed
    ON proof_trades (training_eligible, closed_at DESC);

CREATE INDEX IF NOT EXISTS idx_proof_trades_performance_taxonomy_closed
    ON proof_trades (performance_taxonomy, closed_at DESC);

COMMIT;
