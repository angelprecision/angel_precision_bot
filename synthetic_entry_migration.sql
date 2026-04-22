-- synthetic_entry provenance migration
-- Run once in Supabase SQL editor.
-- Safe: IF NOT EXISTS + DEFAULT false means all historical rows get false.

ALTER TABLE proof_trades
ADD COLUMN IF NOT EXISTS synthetic_entry boolean NOT NULL DEFAULT false;

ALTER TABLE signal_outcomes
ADD COLUMN IF NOT EXISTS synthetic_entry boolean NOT NULL DEFAULT false;

-- Index so reports can efficiently split broker vs synthetic
CREATE INDEX IF NOT EXISTS idx_proof_trades_synthetic
    ON proof_trades (client_email, synthetic_entry, closed_at);

CREATE INDEX IF NOT EXISTS idx_signal_outcomes_synthetic
    ON signal_outcomes (synthetic_entry, closed_at);
