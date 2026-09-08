-- PR #518 — downstream truth fields for manual/external close recovery.
--
-- The lifecycle guard is intentionally fail-closed when these fields are not
-- present.  Keep this migration additive and runner-compatible: the migration
-- runner owns the transaction, so this file must not contain BEGIN/COMMIT.

ALTER TABLE public.proof_trades
    ADD COLUMN IF NOT EXISTS exit_local_order_id TEXT;

-- These columns are already written/read by the core order and queue paths,
-- but are included here so a fresh or partially-provisioned bot database can
-- be brought to the same contract before LIVE schema attestation.
ALTER TABLE public.orders
    ADD COLUMN IF NOT EXISTS filled_ts TIMESTAMPTZ;

ALTER TABLE public.trade_queue
    ADD COLUMN IF NOT EXISTS finished_ts TIMESTAMPTZ;
