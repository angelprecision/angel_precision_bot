-- P0: durable bounded winner-pullback recovery state.
--
-- The exit engine already reads/writes positions.meta for quote and protective
-- state.  Keep this migration idempotent for installations where that column
-- was not included in the original positions table definition.
ALTER TABLE public.positions
    ADD COLUMN IF NOT EXISTS meta JSONB DEFAULT '{}'::jsonb;

COMMENT ON COLUMN public.positions.meta IS
    'Non-destructive JSONB position metadata, including exit protection state';
