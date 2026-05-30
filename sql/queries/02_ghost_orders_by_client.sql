-- =============================================================================
-- GHOST ORDERS — filtered to a single client
-- =============================================================================
-- Same shape as 01_ghost_orders.sql but scoped to one client_id so you can
-- look at main vs jason vs jose individually.
--
-- Replace the literal client_id value below before running.
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
  AND client_id = 'tradefluencehq@gmail.com'   -- <<< change this
ORDER BY updated_ts ASC
LIMIT 500;
