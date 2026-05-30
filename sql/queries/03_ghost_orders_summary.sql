-- =============================================================================
-- GHOST ORDERS — per-client summary
-- =============================================================================
-- One row per client: how many ghosts, total reserved capital, oldest age.
-- Same staleness rule as 01.
-- =============================================================================

SELECT
  client_id,
  COUNT(*)                                                    AS ghost_orders,
  ROUND(SUM(reserved_cost)::numeric, 2)                       AS total_reserved_capital,
  ROUND(MAX(stale_hours_age)::numeric, 2)                     AS oldest_age_hours
FROM (
  SELECT
    client_id,
    NULLIF(meta->>'reserved_cost', '')::numeric  AS reserved_cost,
    EXTRACT(EPOCH FROM (NOW() - updated_ts)) / 3600.0 AS stale_hours_age
  FROM orders
  WHERE kind = 'ENTRY'
    AND status = 'PENDING_TRIGGER'
    AND broker_order_id IS NULL
    AND updated_ts < NOW() - INTERVAL '24 hours'
) g
GROUP BY client_id
ORDER BY ghost_orders DESC, oldest_age_hours DESC;
