-- PR #407 rollout repair: bind legacy confirmed-trigger timestamps to the
-- lifecycle that already owns the order row.
--
-- This is a data migration in the existing orders.meta JSONB contract.  It
-- does not add a column, change order status, submit/cancel an order, or
-- clear trigger_crossed_at.  Run it before enabling PR #407 recovery.
--
-- Safety contract:
--   * only ENTRY rows with an existing trigger_crossed_at are candidates;
--   * an existing trigger_crossed_at_provenance value is never overwritten,
--     including an incomplete or mismatched value;
--   * all four identity values must be present and internally consistent;
--   * materialization_generation is intentionally not fabricated here; it is
--     a separate deferred-materialization CAS field, not ordinary queue
--     confirmed-trigger provenance;
--   * rows that cannot be proven remain unchanged and are reported by the
--     existing RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN gate.
--
-- The canonical expression below matches ap_canonical_signal.build_canonical_signal_id:
-- REEVAL:<uuid>:<hex> -> REEVAL:<uuid>; all other signal IDs are unchanged.

WITH candidates AS (
    SELECT
        o.local_order_id,
        o.meta,
        BTRIM(o.local_order_id::text) AS local_order_id_norm,
        LOWER(BTRIM(o.client_id::text)) AS client_id_norm,
        LOWER(BTRIM(COALESCE(o.execution_mode::text, ''))) AS execution_mode_norm,
        BTRIM(o.signal_id::text) AS signal_id_norm,
        NULLIF(BTRIM(o.canonical_signal_id), '') AS column_canonical_id,
        NULLIF(BTRIM(o.meta->>'canonical_signal_id'), '') AS meta_canonical_id,
        NULLIF(BTRIM(o.meta->>'client_id'), '') AS meta_client_id,
        NULLIF(BTRIM(o.meta->>'execution_mode'), '') AS meta_execution_mode,
        CASE
            WHEN BTRIM(o.signal_id::text) ~
                '^REEVAL:[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}:[0-9a-fA-F]+$'
                THEN regexp_replace(BTRIM(o.signal_id::text), ':[^:]+$', '')
            ELSE BTRIM(o.signal_id::text)
        END AS derived_canonical_id
    FROM orders AS o
    WHERE UPPER(COALESCE(o.kind, '')) = 'ENTRY'
      AND jsonb_typeof(COALESCE(o.meta, '{}'::jsonb)) = 'object'
      AND (o.meta ? 'trigger_crossed_at')
      AND NULLIF(BTRIM(o.meta->>'trigger_crossed_at'), '') IS NOT NULL
      AND NOT (o.meta ? 'trigger_crossed_at_provenance')
), eligible AS (
    SELECT
        c.local_order_id,
        c.meta,
        c.local_order_id_norm,
        c.client_id_norm,
        c.execution_mode_norm,
        c.derived_canonical_id AS canonical_signal_id
    FROM candidates AS c
    WHERE NULLIF(c.local_order_id_norm, '') IS NOT NULL
      AND NULLIF(c.client_id_norm, '') IS NOT NULL
      AND c.execution_mode_norm IN ('live', 'paper')
      AND NULLIF(c.signal_id_norm, '') IS NOT NULL
      AND NULLIF(c.derived_canonical_id, '') IS NOT NULL
      -- A pre-existing canonical value is usable only when it agrees with
      -- the canonical authority derived from signal_id.
      AND (c.column_canonical_id IS NULL OR c.column_canonical_id = c.derived_canonical_id)
      AND (c.meta_canonical_id IS NULL OR c.meta_canonical_id = c.derived_canonical_id)
      -- Top-level order identity is authoritative; conflicting metadata is
      -- not silently repaired.
      AND (c.meta_client_id IS NULL OR LOWER(BTRIM(c.meta_client_id)) = c.client_id_norm)
      AND (c.meta_execution_mode IS NULL OR LOWER(BTRIM(c.meta_execution_mode)) = c.execution_mode_norm)
)
UPDATE orders AS o
SET meta = COALESCE(o.meta, '{}'::jsonb) || jsonb_build_object(
    'trigger_crossed_at_provenance',
    jsonb_build_object(
        'canonical_signal_id', e.canonical_signal_id,
        'client_id', e.client_id_norm,
        'execution_mode', e.execution_mode_norm,
        'local_order_id', e.local_order_id_norm
    )
)
FROM eligible AS e
WHERE o.local_order_id = e.local_order_id
  AND NOT (o.meta ? 'trigger_crossed_at_provenance');
