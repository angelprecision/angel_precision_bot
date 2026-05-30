-- =============================================================================
-- SIGNAL LEDGER — drill into one canonical signal
-- =============================================================================
-- All client rows for a specific signal id. Use when 05_mismatches surfaces
-- something suspicious and you want the full per-client picture.
--
-- Replace the literal canonical_signal_id below before running.
-- =============================================================================

SELECT
  client_id,
  symbol,
  direction,
  ledger_bucket,
  order_status,
  contract,
  qty,
  filled_qty,
  fill_price,
  limit_price,
  reserved_cost,
  watcher_reason_code,
  watcher_raw_reason,
  mode,
  quote_domain_mismatch_possible,
  last_error,
  signal_context_notes,
  order_created_ts,
  order_updated_ts
FROM ap_multi_account_signal_ledger
WHERE canonical_signal_id = '00000000-0000-0000-0000-000000000000'   -- <<< replace
ORDER BY order_created_ts ASC;
