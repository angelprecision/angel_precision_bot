-- Migration: create public.watcher_decision_audit
-- Replaces the broken watcher_audit inserts (old table uses UUID client_id/order_id;
-- current orders schema uses text client_id / bigint order row id / text local_order_id).
-- The legacy public.watcher_audit table is NOT dropped — it stays as historical audit.
-- orders.meta.watcher_audit remains the primary per-order embedded proof.

CREATE TABLE IF NOT EXISTS public.watcher_decision_audit (
    id                      uuid            PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at              timestamptz     NOT NULL DEFAULT now(),

    -- order / signal identity
    local_order_id          text,
    order_row_id            bigint,
    signal_id               text,
    canonical_signal_id     text,
    plan_id                 text,
    client_id               text,                        -- text email, not UUID

    -- execution context
    execution_mode          text,
    symbol                  text,
    contract                text,
    direction               text,
    pattern                 text,
    timeframe               text,
    tier                    text,
    score                   numeric,

    -- watcher decision
    decision                text,
    reason_code             text,
    raw_reason              text,

    -- prices at decision time
    trigger_price           numeric,
    stop_price              numeric,
    target_price            numeric,
    current_underlying      numeric,
    current_bid             numeric,
    current_ask             numeric,
    current_mid             numeric,

    -- quote source proof (should always show api.tradier.com after P0 fix)
    watcher_quote_source    text,
    watcher_sandbox_mode    boolean,
    watcher_quote_base_url  text,
    quote_age_ms            integer,

    -- full watcher_audit payload JSONB for complete traceability
    payload                 jsonb
);

-- Indexes
CREATE INDEX IF NOT EXISTS watcher_decision_audit_created_at_idx
    ON public.watcher_decision_audit (created_at DESC);

CREATE INDEX IF NOT EXISTS watcher_decision_audit_client_symbol_idx
    ON public.watcher_decision_audit (client_id, symbol, created_at DESC);

CREATE INDEX IF NOT EXISTS watcher_decision_audit_signal_id_idx
    ON public.watcher_decision_audit (signal_id);

CREATE INDEX IF NOT EXISTS watcher_decision_audit_canonical_signal_id_idx
    ON public.watcher_decision_audit (canonical_signal_id);

CREATE INDEX IF NOT EXISTS watcher_decision_audit_local_order_id_idx
    ON public.watcher_decision_audit (local_order_id);

COMMENT ON TABLE public.watcher_decision_audit IS
    'Queryable watcher decision proof (trigger/stop/invalidation). '
    'Uses text client_id and text local_order_id matching current orders schema. '
    'Replaces failed inserts into old public.watcher_audit (UUID fields). '
    'Verify post-deploy: SELECT execution_mode, watcher_quote_source, '
    'watcher_sandbox_mode, watcher_quote_base_url, count(*) FROM '
    'watcher_decision_audit GROUP BY 1,2,3,4 ORDER BY 5 DESC; '
    'Expected: paper|tradier_live|false|https://api.tradier.com';
