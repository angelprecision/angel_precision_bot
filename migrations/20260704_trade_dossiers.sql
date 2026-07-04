CREATE TABLE IF NOT EXISTS trade_dossiers (
  dossier_id UUID PRIMARY KEY,
  signal_id TEXT,
  canonical_signal_id TEXT,
  client_id TEXT NOT NULL,
  execution_mode TEXT NOT NULL,
  trade_date DATE NOT NULL,
  ticker TEXT NOT NULL,
  direction TEXT,
  strategy TEXT,
  timeframe TEXT,
  dossier_status TEXT NOT NULL DEFAULT 'BUILDING',
  case_quality_score NUMERIC,
  case_grade TEXT,
  decision TEXT,
  decision_reason TEXT,
  primary_strength TEXT,
  primary_risk TEXT,
  dossier JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  schema_version TEXT NOT NULL DEFAULT 'v1',
  git_commit TEXT,
  config_hash TEXT,
  UNIQUE (canonical_signal_id, client_id, execution_mode, schema_version)
);

CREATE INDEX IF NOT EXISTS idx_trade_dossiers_client_date
ON trade_dossiers (client_id, execution_mode, trade_date);

CREATE INDEX IF NOT EXISTS idx_trade_dossiers_ticker_date
ON trade_dossiers (ticker, trade_date);

CREATE INDEX IF NOT EXISTS idx_trade_dossiers_status
ON trade_dossiers (dossier_status);
