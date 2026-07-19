-- P0: make position execution identity durable and attestable.
--
-- The column is intentionally nullable for historical rows whose mode cannot
-- be proven.  New application writes fail closed unless they persist an exact
-- PAPER/LIVE mode.  Historical rows are backfilled only when one unique
-- originating ENTRY order can be matched by durable local or broker order ID.

SET LOCAL lock_timeout = '5s';

ALTER TABLE public.positions
    ADD COLUMN IF NOT EXISTS execution_mode TEXT;

WITH uniquely_proven_mode AS (
    SELECT
        p.id AS position_id,
        MIN(LOWER(TRIM(o.execution_mode))) AS execution_mode
    FROM public.positions AS p
    JOIN public.orders AS o
      ON o.client_id = p.client_id
     AND UPPER(COALESCE(o.kind, '')) = 'ENTRY'
     AND (
          (p.local_order_id IS NOT NULL AND o.local_order_id = p.local_order_id)
          OR
          (p.broker_order_id IS NOT NULL AND o.broker_order_id = p.broker_order_id)
     )
    WHERE LOWER(TRIM(COALESCE(o.execution_mode, ''))) IN ('paper', 'live')
    GROUP BY p.id
    HAVING COUNT(DISTINCT o.id) = 1
       AND COUNT(DISTINCT LOWER(TRIM(o.execution_mode))) = 1
)
UPDATE public.positions AS p
SET execution_mode = proven.execution_mode
FROM uniquely_proven_mode AS proven
WHERE p.id = proven.position_id
  AND NULLIF(TRIM(p.execution_mode), '') IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'public.positions'::regclass
          AND conname = 'positions_execution_mode_valid'
    ) THEN
        ALTER TABLE public.positions
            ADD CONSTRAINT positions_execution_mode_valid
            CHECK (
                execution_mode IS NULL
                OR LOWER(TRIM(execution_mode)) IN ('paper', 'live')
            ) NOT VALID;
    END IF;
END
$$;

ALTER TABLE public.positions
    VALIDATE CONSTRAINT positions_execution_mode_valid;

COMMENT ON COLUMN public.positions.execution_mode IS
    'Durable execution identity: paper or live. NULL means historical identity is unproven and must remain quarantined.';
