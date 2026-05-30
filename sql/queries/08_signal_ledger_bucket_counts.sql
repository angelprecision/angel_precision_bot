-- =============================================================================
-- SIGNAL LEDGER — bucket counts over a window
-- =============================================================================
-- One-row-per-bucket summary. Same numbers the dashboard cards show.
-- =============================================================================

SELECT
  ledger_bucket,
  COUNT(*) AS n
FROM ap_multi_account_signal_ledger
WHERE order_created_ts > NOW() - INTERVAL '24 hours'
GROUP BY ledger_bucket
ORDER BY n DESC;
