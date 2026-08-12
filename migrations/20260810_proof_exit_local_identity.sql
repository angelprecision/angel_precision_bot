-- =============================================================================
-- PR #428 — Explicit EXIT local identity in proof_trades
-- =============================================================================
-- proof_trades.local_order_id is the originating ENTRY local-order bridge.
-- Keep the EXIT local-order identity in its own nullable column so reconciled
-- proof cannot corrupt the ENTRY -> proof journal join.
--
-- Additive and idempotent. Apply before enabling LIVE reconciled proof writes.
-- =============================================================================

ALTER TABLE proof_trades
    ADD COLUMN IF NOT EXISTS exit_local_order_id TEXT;

CREATE INDEX IF NOT EXISTS idx_proof_trades_exit_local_order_id
    ON proof_trades (exit_local_order_id)
    WHERE exit_local_order_id IS NOT NULL;
