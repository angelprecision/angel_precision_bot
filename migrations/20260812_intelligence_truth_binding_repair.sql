-- PR #432 — intelligence truth repair.
--
-- This migration is intentionally transaction-control free.  The migration
-- runner owns the transaction boundary.  It repairs only the evidence plane:
-- no proof economics, P&L, order, position, queue, or broker state is
-- created or modified here.

CREATE TABLE IF NOT EXISTS public.blocked_signal_counterfactuals (
    id BIGSERIAL PRIMARY KEY,
    signal_id TEXT NOT NULL,
    canonical_signal_id TEXT,
    client_id TEXT NOT NULL,
    execution_mode TEXT NOT NULL,
    ticker TEXT NOT NULL,
    direction TEXT NOT NULL,
    block_stage TEXT NOT NULL,
    block_reason TEXT NOT NULL,
    reason_code TEXT,
    blocked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    entry_ref NUMERIC,
    target_ref NUMERIC,
    stop_ref NUMERIC,
    resolution TEXT,
    hypothetical_r NUMERIC,
    resolved_at TIMESTAMPTZ,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (signal_id, client_id, execution_mode)
);

CREATE INDEX IF NOT EXISTS blocked_signal_counterfactuals_blocked_at_idx
    ON public.blocked_signal_counterfactuals (blocked_at DESC);

CREATE INDEX IF NOT EXISTS blocked_signal_counterfactuals_pending_idx
    ON public.blocked_signal_counterfactuals (execution_mode, client_id, blocked_at)
    WHERE resolution IS NULL OR resolution = '';

CREATE TABLE IF NOT EXISTS public.ap_intelligence_outcome_bindings (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id UUID NOT NULL,
    proof_trade_id BIGINT NOT NULL,
    client_id TEXT NOT NULL,
    execution_mode TEXT NOT NULL,
    originating_local_order_id TEXT NOT NULL,
    canonical_signal_id TEXT NOT NULL,
    phase TEXT NOT NULL,
    profile_version TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    binding_method TEXT NOT NULL,
    binding_version TEXT NOT NULL,
    bound_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (snapshot_id)
);

CREATE INDEX IF NOT EXISTS idx_ap_intelligence_outcome_bindings_proof_trade
    ON public.ap_intelligence_outcome_bindings (proof_trade_id);

CREATE INDEX IF NOT EXISTS idx_ap_intelligence_outcome_bindings_identity
    ON public.ap_intelligence_outcome_bindings (
        client_id,
        execution_mode,
        originating_local_order_id
    );
