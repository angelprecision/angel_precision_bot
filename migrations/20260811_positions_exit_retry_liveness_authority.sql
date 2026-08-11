-- PR #423: durable JSONB authority for replacement lifecycle and exit-fill
-- consumption.  The migration is deliberately additive and idempotent so an
-- existing positions row keeps its economic fields unchanged.

ALTER TABLE public.positions
    ADD COLUMN IF NOT EXISTS meta JSONB;

UPDATE public.positions
SET meta = '{}'::jsonb
WHERE meta IS NULL;

ALTER TABLE public.positions
    ALTER COLUMN meta SET DEFAULT '{}'::jsonb,
    ALTER COLUMN meta SET NOT NULL;
