-- =============================================================================
-- SIGNAL LEDGER — only signals where clients differ
-- =============================================================================
-- Per canonical signal: how many distinct contracts, qtys, and outcomes did
-- the different clients see? Surfaces fan-out mismatches you should
-- investigate (different contract per client, different qty, etc.).
-- =============================================================================

SELECT
  canonical_signal_id,
  MIN(symbol)                          AS symbol,
  COUNT(DISTINCT client_id)            AS n_clients,
  COUNT(DISTINCT contract)             AS n_distinct_contracts,
  COUNT(DISTINCT qty)                  AS n_distinct_qtys,
  COUNT(DISTINCT ledger_bucket)        AS n_distinct_outcomes,
  STRING_AGG(DISTINCT ledger_bucket, ', ' ORDER BY ledger_bucket) AS buckets_seen,
  STRING_AGG(DISTINCT client_id, ', ' ORDER BY client_id) AS clients,
  MIN(order_created_ts)                AS earliest_ts,
  MAX(order_updated_ts)                AS latest_ts
FROM ap_multi_account_signal_ledger
WHERE order_created_ts > NOW() - INTERVAL '24 hours'
GROUP BY canonical_signal_id
HAVING
     COUNT(DISTINCT contract)      > 1
  OR COUNT(DISTINCT qty)           > 1
  OR COUNT(DISTINCT ledger_bucket) > 1
ORDER BY latest_ts DESC
LIMIT 200;
