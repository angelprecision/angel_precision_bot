-- Durable one-decision-per-exit-generation fence.
-- Apply before deploying PR #361.

BEGIN;

CREATE TABLE IF NOT EXISTS exit_decision_generation_claims (
    generation_key       TEXT PRIMARY KEY,
    client_id            TEXT NOT NULL,
    position_id          TEXT NOT NULL,
    remaining_qty        INTEGER NOT NULL CHECK (remaining_qty >= 0),
    exit_generation      BIGINT NOT NULL CHECK (exit_generation > 0),
    decision_action      TEXT,
    decision_reason_code TEXT,
    claimed_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_exit_decision_generation_position
    ON exit_decision_generation_claims
       (client_id, position_id, exit_generation DESC);

ALTER TABLE exit_decision_generation_claims ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE exit_decision_generation_claims FROM anon, authenticated;

COMMIT;
