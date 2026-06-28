alter table proof_trades add column if not exists trade_id bigint;
alter table proof_trades add column if not exists signal_id text;
alter table proof_trades add column if not exists status text;
