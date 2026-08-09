-- Preserve economic EXIT quantity separately from position-generation identity.
-- Existing rows remain nullable and fail closed during broker-owned recovery;
-- every new/released claim writes an exact positive requested_qty.

BEGIN;

ALTER TABLE exit_decision_generation_claims
    ADD COLUMN IF NOT EXISTS requested_qty INTEGER CHECK (requested_qty > 0);

COMMIT;
