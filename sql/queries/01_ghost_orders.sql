-- =============================================================================
-- GHOST ORDERS — read-only report (Supabase SQL editor version)
-- =============================================================================
-- Paste-and-run. No parameters to substitute. Adjust the literals at the
-- bottom if you want a different window.
--
-- "Ghost" = ENTRY order, PENDING_TRIGGER, never sent to broker, stale.
-- This is the same query the /admin/operator/ghost-orders endpoint runs
-- internally — just inlined with literal values for the editor.
--
-- Knobs (change inline below):
--   24       → stale-hours threshold
--   LIMIT    → safety cap
-- =============================================================================

SELECT
  local_order_id,
  client_id,
  meta->>'plan_id'                              AS plan_id,
  meta->>'signal_id'                            AS signal_id,
  symbol,
  contract,
  COALESCE(meta->>'direction', meta->>'side')   AS direction,
  qty,
  limit_price,
  NULLIF(meta->>'reserved_cost', '')::numeric   AS reserved_cost,
  NULLIF(meta->>'trigger_price', '')::numeric   AS trigger_price,
  score,
  tier,
  meta->>'pattern'                              AS pattern,
  meta->>'timeframe'                            AS timeframe,
  last_error,
  created_ts,
  updated_ts,
  EXTRACT(EPOCH FROM (NOW() - updated_ts)) / 3600.0 AS stale_hours_age
FROM orders
WHERE kind = 'ENTRY'
  AND status = 'PENDING_TRIGGER'
  AND broker_order_id IS NULL
  AND updated_ts < NOW() - INTERVAL '24 hours'
ORDER BY updated_ts ASC
LIMIT 500;
