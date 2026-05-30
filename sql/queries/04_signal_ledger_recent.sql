-- =============================================================================
-- SIGNAL LEDGER — recent window (Supabase SQL editor version)
-- =============================================================================
-- Read-only view of the multi-account signal ledger for the last N hours.
-- Same data the /admin/operator/signal-ledger endpoint returns, paste-ready.
--
-- REQUIRES: ap_multi_account_signal_ledger view applied to the DB
-- (sql/views/ap_multi_account_signal_ledger.sql).
-- =============================================================================

SELECT
  canonical_signal_id,
  client_id,
  symbol,
  direction,
  score,
  tier,
  timeframe,
  ledger_bucket,
  order_status,
  contract,
  qty,
  filled_qty,
  watcher_reason_code,
  mode,
  quote_domain_mismatch_possible,
  last_error,
  order_created_ts,
  order_updated_ts
FROM ap_multi_account_signal_ledger
WHERE order_created_ts > NOW() - INTERVAL '24 hours'
ORDER BY canonical_signal_id, order_created_ts DESC
LIMIT 1000;
