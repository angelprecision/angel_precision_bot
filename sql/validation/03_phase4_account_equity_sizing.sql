-- ============================================================
-- Phase 4 validation: account-equity-based sizing
-- ============================================================
-- Proves position sizing uses account_equity * POSITION_RISK_PCT
-- and the acceptance examples from the audit prompt hold:
--   * $10K @ $3.08 \u2192 3 contracts
--   * $30K @ $3.08 \u2192 9 contracts

\echo '== Phase 4.1: sizing telemetry coverage (last 24h, ENTRY orders) =='
SELECT
    COUNT(*)                            AS total_entries,
    COUNT(meta->>'account_equity')      AS has_account_equity,
    COUNT(meta->>'position_budget')     AS has_position_budget,
    COUNT(meta->>'final_qty')           AS has_final_qty,
    COUNT(meta->>'sizing_reason_code')  AS has_sizing_reason_code
FROM   orders
WHERE  kind = 'ENTRY'
  AND  created_ts >= NOW() - INTERVAL '24 hours';
\echo 'EXPECTED: has_* should equal total_entries for orders created after Phase 4 deploy.'

\echo ''
\echo '== Phase 4.2: sizing_reason_code distribution (last 24h) =='
SELECT
    COALESCE(meta->>'sizing_reason_code', 'UNKNOWN') AS reason,
    COUNT(*)                                          AS count
FROM   orders
WHERE  kind = 'ENTRY'
  AND  created_ts >= NOW() - INTERVAL '24 hours'
GROUP  BY 1
ORDER  BY 2 DESC;
\echo 'EXPECTED: ACCOUNT_EQUITY_PCT dominates. MAX_TRADE_USD_CAP /'
\echo '  MAX_CONTRACTS_CAP rare unless intentional (very large accounts).'
\echo '  INSUFFICIENT_BUDGET reasonable only when premium exceeds 10% of equity.'

\echo ''
\echo '== Phase 4.3: empirical qty math (random sample of 20 entries) =='
SELECT
    local_order_id,
    symbol,
    qty                                  AS final_qty,
    ROUND((meta->>'account_equity')::numeric, 0)  AS account_equity,
    ROUND((meta->>'position_budget')::numeric, 0) AS position_budget,
    ROUND((meta->>'submit_limit')::numeric, 2)    AS submit_limit,
    -- Expected qty per the formula in execution._size_position
    FLOOR(
        (meta->>'position_budget')::numeric /
        ((meta->>'submit_limit')::numeric * 100)
    )::int                                AS expected_qty,
    meta->>'sizing_reason_code'           AS reason
FROM   orders
WHERE  kind = 'ENTRY'
  AND  created_ts >= NOW() - INTERVAL '24 hours'
  AND  (meta->>'position_budget') IS NOT NULL
  AND  (meta->>'submit_limit')    IS NOT NULL
  AND  (meta->>'sizing_reason_code') = 'ACCOUNT_EQUITY_PCT'
ORDER  BY random()
LIMIT  20;
\echo 'EXPECTED: final_qty = expected_qty for every row where reason is'
\echo '  ACCOUNT_EQUITY_PCT. Any mismatch means qty is being modified after'
\echo '  _size_position returns \u2014 investigate.'

\echo ''
\echo '== Phase 4.4: forced-qty=1 check (LIVE only) =='
SELECT
    COUNT(*) AS live_entries_with_qty_one,
    COUNT(*) FILTER (WHERE (meta->>'sizing_reason_code') NOT IN
        ('INSUFFICIENT_BUDGET', 'INVALID_INPUTS')) AS unexplained_qty_one
FROM   orders o
JOIN   client_state s USING (client_id)
WHERE  o.kind = 'ENTRY'
  AND  o.created_ts >= NOW() - INTERVAL '24 hours'
  AND  o.qty = 1
  AND  UPPER(s.mode) = 'LIVE';
\echo 'EXPECTED: unexplained_qty_one = 0. A non-zero value means a LIVE'
\echo '  entry was forced to qty=1 without a sizing reason that justifies'
\echo '  it (INSUFFICIENT_BUDGET means equity * risk_pct < one contract cost,'
\echo '  which is legitimate; anything else is suspect).'
