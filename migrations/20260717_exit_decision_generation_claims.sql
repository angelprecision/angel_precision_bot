-- Durable one-decision-per-exit-generation fence.
-- Apply before deploying PR #361.

BEGIN;

CREATE TABLE IF NOT EXISTS exit_decision_generation_claims (
    generation_key       TEXT PRIMARY KEY,
    client_id            TEXT NOT NULL,
    position_id          TEXT NOT NULL,
    remaining_qty        INTEGER NOT NULL CHECK (remaining_qty >= 0),
    requested_qty        INTEGER CHECK (requested_qty > 0),
    exit_generation      BIGINT NOT NULL CHECK (exit_generation > 0),
    decision_action      TEXT,
    decision_reason_code TEXT,
    claim_state          TEXT NOT NULL DEFAULT 'CLAIMED',
    local_order_id       TEXT,
    broker_order_id      TEXT,
    last_error           TEXT,
    released_at          TIMESTAMPTZ,
    claimed_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE exit_decision_generation_claims
    ADD COLUMN IF NOT EXISTS claim_state TEXT NOT NULL DEFAULT 'CLAIMED';
ALTER TABLE exit_decision_generation_claims
    ADD COLUMN IF NOT EXISTS local_order_id TEXT;
ALTER TABLE exit_decision_generation_claims
    ADD COLUMN IF NOT EXISTS broker_order_id TEXT;
ALTER TABLE exit_decision_generation_claims
    ADD COLUMN IF NOT EXISTS last_error TEXT;
ALTER TABLE exit_decision_generation_claims
    ADD COLUMN IF NOT EXISTS released_at TIMESTAMPTZ;
ALTER TABLE exit_decision_generation_claims
    ADD COLUMN IF NOT EXISTS requested_qty INTEGER CHECK (requested_qty > 0);

UPDATE exit_decision_generation_claims
SET claim_state = COALESCE(NULLIF(claim_state, ''), 'CLAIMED')
WHERE claim_state IS NULL OR claim_state = '';

CREATE INDEX IF NOT EXISTS idx_exit_decision_generation_position
    ON exit_decision_generation_claims
       (client_id, position_id, exit_generation DESC);

ALTER TABLE exit_decision_generation_claims ENABLE ROW LEVEL SECURITY;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
        REVOKE ALL ON TABLE exit_decision_generation_claims FROM anon;
    END IF;

    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
        REVOKE ALL ON TABLE exit_decision_generation_claims FROM authenticated;
    END IF;
END
$$;

COMMIT;
