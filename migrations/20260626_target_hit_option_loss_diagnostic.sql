-- P1 reporting/intelligence-only diagnostic.
-- Adds a nullable future-record marker for exits where the underlying target
-- was hit but the option contract realized a negative P&L.
--
-- Safety:
-- - No historical row rewrite/backfill.
-- - No default, so existing rows remain untouched/null.
-- - No broker/order/position/queue/handoff behavior impact.

alter table public.proof_trades
    add column if not exists target_hit_option_loss boolean;

comment on column public.proof_trades.target_hit_option_loss is
    'Reporting diagnostic: true when exit_reason contains TARGET HIT but option_pnl_pct is negative. Future rows only; do not treat as winning proof/training label.';
