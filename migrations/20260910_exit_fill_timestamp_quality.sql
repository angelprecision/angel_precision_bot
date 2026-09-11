ALTER TABLE public.positions
  ADD COLUMN IF NOT EXISTS exit_observed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS exit_timestamp_quality TEXT;

ALTER TABLE public.proof_trades
  ADD COLUMN IF NOT EXISTS fill_timestamp_quality TEXT;

COMMENT ON COLUMN public.positions.exit_observed_at IS
  'Broker order observation boundary; never an exact execution timestamp.';
COMMENT ON COLUMN public.positions.exit_timestamp_quality IS
  'Quality of exit chronology: exact_execution or order_update_not_exact_execution.';
