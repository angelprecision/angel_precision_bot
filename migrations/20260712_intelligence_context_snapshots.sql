CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS ap_intelligence_snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id TEXT NOT NULL,
  execution_mode TEXT NOT NULL,
  canonical_signal_id TEXT NOT NULL,
  signal_id TEXT,
  local_order_id TEXT,
  phase TEXT NOT NULL,
  context_revision INTEGER NOT NULL DEFAULT 1,
  profile_version TEXT NOT NULL,
  parent_snapshot_id UUID REFERENCES ap_intelligence_snapshots(id),
  input_hash TEXT NOT NULL,
  config_hash TEXT,
  git_commit TEXT,
  data_as_of TIMESTAMPTZ,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  status TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (phase IN ('PRETRIGGER', 'PREOPEN', 'BREACH', 'CONTRACT_SELECTED')),
  CHECK (status IN ('COMPLETE', 'PARTIAL', 'STALE', 'UNAVAILABLE', 'ERROR'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_ap_intel_snapshots_identity
ON ap_intelligence_snapshots (
  client_id,
  lower(execution_mode),
  canonical_signal_id,
  COALESCE(NULLIF(BTRIM(local_order_id), ''), '__none__'),
  phase,
  context_revision,
  profile_version
);

CREATE INDEX IF NOT EXISTS idx_ap_intel_snapshots_latest_phase
ON ap_intelligence_snapshots (
  client_id,
  lower(execution_mode),
  canonical_signal_id,
  phase,
  context_revision DESC,
  computed_at DESC
);

CREATE INDEX IF NOT EXISTS idx_ap_intel_snapshots_parent
ON ap_intelligence_snapshots (parent_snapshot_id);

CREATE TABLE IF NOT EXISTS ap_intelligence_jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id TEXT NOT NULL,
  execution_mode TEXT NOT NULL,
  canonical_signal_id TEXT NOT NULL,
  signal_id TEXT,
  local_order_id TEXT,
  phase TEXT NOT NULL,
  context_revision INTEGER NOT NULL DEFAULT 1,
  profile_version TEXT NOT NULL,
  input_hash TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'PENDING',
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  claimed_at TIMESTAMPTZ,
  claim_owner TEXT,
  claim_expires_at TIMESTAMPTZ,
  last_error_code TEXT,
  last_error_detail TEXT,
  snapshot_id UUID REFERENCES ap_intelligence_snapshots(id),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (phase IN ('PRETRIGGER', 'PREOPEN', 'BREACH', 'CONTRACT_SELECTED')),
  CHECK (status IN ('PENDING', 'RUNNING', 'RETRY_PENDING', 'COMPLETED', 'FAILED_TERMINAL'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_ap_intel_jobs_identity
ON ap_intelligence_jobs (
  client_id,
  lower(execution_mode),
  canonical_signal_id,
  COALESCE(NULLIF(BTRIM(local_order_id), ''), '__none__'),
  phase,
  context_revision,
  profile_version
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_ap_intel_jobs_same_input
ON ap_intelligence_jobs (
  client_id,
  lower(execution_mode),
  canonical_signal_id,
  COALESCE(NULLIF(BTRIM(local_order_id), ''), '__none__'),
  phase,
  profile_version,
  input_hash
);

CREATE INDEX IF NOT EXISTS idx_ap_intel_jobs_due_claim
ON ap_intelligence_jobs (
  client_id, lower(execution_mode), status, next_attempt_at, claim_expires_at
);

CREATE INDEX IF NOT EXISTS idx_ap_intel_jobs_active_due_claim
ON ap_intelligence_jobs (
  client_id, lower(execution_mode), next_attempt_at, claim_expires_at
)
WHERE status IN ('PENDING', 'RETRY_PENDING', 'RUNNING');

CREATE INDEX IF NOT EXISTS idx_ap_intel_jobs_identity_lookup
ON ap_intelligence_jobs (
  client_id,
  lower(execution_mode),
  canonical_signal_id,
  phase,
  context_revision
);

CREATE INDEX IF NOT EXISTS idx_ap_intel_jobs_recovery_lookup
ON ap_intelligence_jobs (
  client_id,
  lower(execution_mode),
  signal_id,
  phase,
  COALESCE(NULLIF(BTRIM(local_order_id), ''), '__none__')
);

ALTER TABLE ap_intelligence_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE ap_intelligence_jobs ENABLE ROW LEVEL SECURITY;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') THEN
    EXECUTE 'REVOKE ALL ON ap_intelligence_snapshots, ap_intelligence_jobs FROM anon';
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') THEN
    EXECUTE 'REVOKE ALL ON ap_intelligence_snapshots, ap_intelligence_jobs FROM authenticated';
  END IF;
END $$;
