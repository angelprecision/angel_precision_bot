alter table public.proof_trades add column if not exists trade_id bigint;
alter table public.proof_trades add column if not exists signal_id text;
alter table public.proof_trades add column if not exists status text;
