-- Immutable daily intelligence selection and post-selection outcome ledger.
-- Selection rows contain only PRETRIGGER evidence. Outcomes are written later
-- and never participate in ranking. The bot role writes through direct
-- Postgres; anon/authenticated Data API roles have no access.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS public.ap_daily_ranking_runs (
  id UUID PRIMARY KEY,
  client_id TEXT NOT NULL,
  execution_mode TEXT NOT NULL CHECK (upper(execution_mode) IN ('PAPER', 'LIVE')),
  session_date DATE NOT NULL,
  policy_version TEXT NOT NULL,
  policy_frozen_at TIMESTAMPTZ NOT NULL,
  selection_frozen_at TIMESTAMPTZ NOT NULL,
  source_count INTEGER NOT NULL CHECK (source_count >= 0),
  eligible_count INTEGER NOT NULL CHECK (eligible_count >= 0),
  selected_count INTEGER NOT NULL CHECK (selected_count BETWEEN 0 AND 10),
  evaluation_count INTEGER NOT NULL CHECK (evaluation_count BETWEEN 0 AND 7),
  feed_limit INTEGER NOT NULL DEFAULT 10 CHECK (feed_limit = 10),
  evaluation_limit INTEGER NOT NULL DEFAULT 7 CHECK (evaluation_limit = 7),
  status TEXT NOT NULL DEFAULT 'FROZEN' CHECK (status = 'FROZEN'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (client_id, execution_mode, session_date, policy_version)
);

CREATE TABLE IF NOT EXISTS public.ap_daily_opportunity_rankings (
  id UUID PRIMARY KEY,
  ranking_run_id UUID NOT NULL REFERENCES public.ap_daily_ranking_runs(id) ON DELETE RESTRICT,
  score_snapshot_id UUID NOT NULL REFERENCES public.ap_intelligence_snapshots(id) ON DELETE RESTRICT,
  client_id TEXT NOT NULL,
  execution_mode TEXT NOT NULL CHECK (upper(execution_mode) IN ('PAPER', 'LIVE')),
  session_date DATE NOT NULL,
  policy_version TEXT NOT NULL,
  canonical_signal_id TEXT NOT NULL,
  signal_id TEXT,
  ticker TEXT NOT NULL,
  side TEXT NOT NULL,
  opportunity_rank INTEGER NOT NULL CHECK (opportunity_rank BETWEEN 1 AND 10),
  selected_for_feed BOOLEAN NOT NULL DEFAULT TRUE CHECK (selected_for_feed IS TRUE),
  selected_for_trade_evaluation BOOLEAN NOT NULL DEFAULT FALSE,
  policy_score NUMERIC NOT NULL CHECK (policy_score BETWEEN 0 AND 100),
  score_input_hash TEXT NOT NULL,
  score_integrity_hash TEXT NOT NULL,
  scored_at TIMESTAMPTZ NOT NULL,
  data_as_of TIMESTAMPTZ NOT NULL,
  context JSONB NOT NULL DEFAULT '{}'::jsonb,
  frozen_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (ranking_run_id, canonical_signal_id),
  UNIQUE (ranking_run_id, opportunity_rank)
);

CREATE TABLE IF NOT EXISTS public.ap_daily_opportunity_outcomes (
  id UUID PRIMARY KEY,
  ranking_id UUID NOT NULL REFERENCES public.ap_daily_opportunity_rankings(id) ON DELETE RESTRICT,
  outcome_class TEXT NOT NULL CHECK (outcome_class IN ('LIVE_OFFICIAL', 'PAPER_DIAGNOSTIC', 'COUNTERFACTUAL_DIAGNOSTIC')),
  eligible_for_promotion BOOLEAN NOT NULL DEFAULT FALSE,
  proof_trade_id TEXT,
  position_id TEXT,
  local_order_id TEXT,
  return_fraction NUMERIC NOT NULL CHECK (return_fraction >= -1.0),
  win BOOLEAN NOT NULL,
  known_at TIMESTAMPTZ NOT NULL,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (
    (outcome_class = 'LIVE_OFFICIAL' AND eligible_for_promotion IS TRUE)
    OR (outcome_class <> 'LIVE_OFFICIAL' AND eligible_for_promotion IS FALSE)
  ),
  UNIQUE (ranking_id, outcome_class),
  UNIQUE (proof_trade_id, outcome_class)
);

CREATE INDEX IF NOT EXISTS idx_ap_daily_rankings_client_feed
  ON public.ap_daily_opportunity_rankings
  (client_id, execution_mode, session_date DESC, opportunity_rank);

CREATE INDEX IF NOT EXISTS idx_ap_daily_outcomes_promotion
  ON public.ap_daily_opportunity_outcomes (eligible_for_promotion, known_at DESC)
  WHERE eligible_for_promotion IS TRUE;

CREATE OR REPLACE FUNCTION public.ap_reject_immutable_intelligence_ledger_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'daily intelligence selection/outcome ledgers are append-only';
END;
$$;

DROP TRIGGER IF EXISTS ap_daily_ranking_runs_immutable ON public.ap_daily_ranking_runs;
CREATE TRIGGER ap_daily_ranking_runs_immutable
  BEFORE UPDATE OR DELETE ON public.ap_daily_ranking_runs
  FOR EACH ROW EXECUTE FUNCTION public.ap_reject_immutable_intelligence_ledger_mutation();

DROP TRIGGER IF EXISTS ap_daily_opportunity_rankings_immutable ON public.ap_daily_opportunity_rankings;
CREATE TRIGGER ap_daily_opportunity_rankings_immutable
  BEFORE UPDATE OR DELETE ON public.ap_daily_opportunity_rankings
  FOR EACH ROW EXECUTE FUNCTION public.ap_reject_immutable_intelligence_ledger_mutation();

DROP TRIGGER IF EXISTS ap_daily_opportunity_outcomes_immutable ON public.ap_daily_opportunity_outcomes;
CREATE TRIGGER ap_daily_opportunity_outcomes_immutable
  BEFORE UPDATE OR DELETE ON public.ap_daily_opportunity_outcomes
  FOR EACH ROW EXECUTE FUNCTION public.ap_reject_immutable_intelligence_ledger_mutation();

ALTER TABLE public.ap_daily_ranking_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ap_daily_opportunity_rankings ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ap_daily_opportunity_outcomes ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
    REVOKE ALL ON public.ap_daily_ranking_runs,
      public.ap_daily_opportunity_rankings,
      public.ap_daily_opportunity_outcomes FROM anon;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
    REVOKE ALL ON public.ap_daily_ranking_runs,
      public.ap_daily_opportunity_rankings,
      public.ap_daily_opportunity_outcomes FROM authenticated;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') THEN
    GRANT SELECT, INSERT ON public.ap_daily_ranking_runs,
      public.ap_daily_opportunity_rankings,
      public.ap_daily_opportunity_outcomes TO service_role;
  END IF;
END $$;

COMMIT;
