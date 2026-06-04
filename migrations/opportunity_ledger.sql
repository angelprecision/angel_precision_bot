-- migration: client_signal_opportunities table
-- PR1: Client Opportunity Ledger
-- Run in Supabase SQL editor before deploying the bot update.

CREATE TABLE IF NOT EXISTS client_signal_opportunities (
  id                         bigserial PRIMARY KEY,
  signal_id                  text      NOT NULL,
  canonical_signal_id        text,
  plan_id                    text,
  client_id                  text      NOT NULL,
  symbol                     text,
  direction                  text,
  side                       text,
  timeframe                  text,
  pattern                    text,
  score                      numeric,
  tier                       text,
  scanner_type               text,
  scanner_name               text,
  signal_created_at          timestamptz,
  client_eligibility_status  text      DEFAULT 'CLIENT_ELIGIBLE',
  opportunity_status         text      NOT NULL DEFAULT 'CREATED',
  miss_stage                 text,
  miss_reason                text,
  order_local_id             text,
  broker_order_id            text,
  position_id                text,
  quote_age_seconds          numeric,
  spread_pct                 numeric,
  buying_power_snapshot      numeric,
  cap_snapshot               jsonb,
  kill_switch_state          boolean,
  entries_paused_state       boolean,
  entry_confirmation_result  text,
  retry_status               text,
  retry_reason               text,
  created_at                 timestamptz NOT NULL DEFAULT now(),
  updated_at                 timestamptz NOT NULL DEFAULT now(),
  metadata                   jsonb
);

-- Idempotency constraint — one row per signal+client
CREATE UNIQUE INDEX IF NOT EXISTS
  idx_cso_signal_client
  ON client_signal_opportunities (signal_id, client_id);

-- Reporting indexes
CREATE INDEX IF NOT EXISTS idx_cso_client_id
  ON client_signal_opportunities (client_id);
CREATE INDEX IF NOT EXISTS idx_cso_symbol_created
  ON client_signal_opportunities (symbol, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_cso_status
  ON client_signal_opportunities (opportunity_status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_cso_miss_reason
  ON client_signal_opportunities (miss_reason) WHERE miss_reason IS NOT NULL;
