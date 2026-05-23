-- ============================================================
-- Phase 7 validation: exit pricing + multi-contract partial integrity
-- ============================================================
-- Read-only sanity checks on the positions / exits already in production.
-- Phase 7 didn't change code; these queries surface anomalies that would
-- indicate the verified properties have been bypassed.

\echo '== Phase 7.1: exit prices are within option range, not stock range =='
-- Heuristic: if an exit row has fill_price > 50, it almost certainly used
-- the underlying stock price by mistake. Real option premiums are < $30
-- for the tickers we trade.
SELECT
    p.client_id,
    p.id              AS position_id,
    p.contract,
    p.avg_fill        AS entry_avg_fill,
    p.exit_avg_fill   AS exit_avg_fill,
    p.status,
    p.closed_ts
FROM   positions p
WHERE  p.exit_avg_fill > 50
  AND  p.closed_ts >= NOW() - INTERVAL '30 days'
ORDER  BY p.closed_ts DESC
LIMIT  50;
\echo 'EXPECTED: zero rows. Any row here is an exit priced against the'
\echo '  underlying stock price (broker would have rejected the order at'
\echo '  that absurd price, so look at last_error too).'

\echo ''
\echo '== Phase 7.2: multi-contract positions exit fully =='
SELECT
    p.client_id,
    p.id              AS position_id,
    p.contract,
    p.qty             AS original_qty,
    p.qty_remaining   AS remaining_after_close,
    p.status,
    p.closed_ts
FROM   positions p
WHERE  p.qty > 1
  AND  p.status = 'CLOSED'
  AND  p.qty_remaining > 0
  AND  p.closed_ts >= NOW() - INTERVAL '7 days'
ORDER  BY p.closed_ts DESC
LIMIT  50;
\echo 'EXPECTED: zero rows. CLOSED with qty_remaining > 0 means a multi-'
\echo '  contract exit submitted qty=1 (instead of qty=N) and the position'
\echo '  is now stuck open at the broker.'

\echo ''
\echo '== Phase 7.3: exit fill_price sanity vs entry fill_price =='
-- A successful exit's fill should be within +/- 80% of entry for tight stops
-- (>80% in either direction is rare and may indicate option contract
-- mis-routing). This is a wide tolerance \u2014 just looking for outliers.
SELECT
    p.client_id,
    p.id            AS position_id,
    p.contract,
    p.qty,
    p.avg_fill      AS entry_fill,
    p.exit_avg_fill AS exit_fill,
    ROUND((p.exit_avg_fill / NULLIF(p.avg_fill, 0) - 1) * 100, 1) AS exit_vs_entry_pct,
    p.status,
    p.closed_ts
FROM   positions p
WHERE  p.status = 'CLOSED'
  AND  p.avg_fill > 0
  AND  p.exit_avg_fill > 0
  AND  ABS(p.exit_avg_fill / p.avg_fill - 1) > 0.80
  AND  p.closed_ts >= NOW() - INTERVAL '7 days'
ORDER  BY ABS(p.exit_avg_fill / p.avg_fill - 1) DESC
LIMIT  20;
\echo 'EXPECTED: small list with clear winners/losers in the tail. Many'
\echo '  +5000% rows would indicate exits hitting stock prices.'

\echo ''
\echo '== Phase 7.4: scale-out / partial exit tracking =='
SELECT
    p.client_id,
    p.id                   AS position_id,
    p.contract,
    p.qty                  AS original_qty,
    p.qty_remaining,
    p.status,
    -- Count partial exit orders linked to this position
    (SELECT COUNT(*) FROM orders o
       WHERE o.position_id = p.id::text
         AND o.kind = 'EXIT'
         AND o.status IN ('FILLED', 'PARTIAL_FILL')) AS exit_orders_count
FROM   positions p
WHERE  p.qty > 1
  AND  p.closed_ts >= NOW() - INTERVAL '7 days'
ORDER  BY p.closed_ts DESC
LIMIT  20;
\echo 'EXPECTED: exit_orders_count >= 1 for every closed multi-contract'
\echo '  position. exit_orders_count > 1 indicates scale-out worked (good).'
\echo '  exit_orders_count = 0 on a CLOSED position is a bug.'
