BEGIN;

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

COMMIT;
