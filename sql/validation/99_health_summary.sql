-- ============================================================
-- Post-deploy health summary (one-pager)
-- ============================================================
-- Run this first after deploy. Everything that follows in
-- 01-06 *.sql is a drill-down on what this surfaces.

\echo '== Entry funnel by reason_bucket (last 24h) =='
WITH bucketed AS (
    SELECT
        CASE
            WHEN UPPER(status) = 'FILLED'        THEN '01_filled'
            WHEN UPPER(status) IN ('PARTIAL_FILL','PARTIAL') THEN '02_partial'
            WHEN UPPER(status) = 'REJECTED'      THEN '03_rejected'
            WHEN UPPER(status) = 'EXPIRED'       THEN '04_expired'
            WHEN UPPER(status) IN ('CANCELED','CANCELLED') AND
                 LOWER(COALESCE(last_error,'')) ~ 'entry_max_age_(normal|aplus)_reached'
                                                  THEN '04_expired'
            WHEN UPPER(status) IN ('CANCELED','CANCELLED') AND
                 LOWER(COALESCE(last_error,'')) ~
                 '(thesis_invalid|spread_wide|runaway_quote|positions_full|lost_handoff|risk_gate_blocked)'
                                                  THEN '05_canceled_signal_dead'
            WHEN UPPER(status) IN ('CANCELED','CANCELLED') AND
                 LOWER(COALESCE(last_error,'')) ~
                 '(stale_entry_timeout|missed_move|broker_transient_error)'
                                                  THEN '06_canceled_signal_alive'
            WHEN UPPER(status) IN ('CANCELED','CANCELLED')  THEN '07_canceled_other'
            WHEN UPPER(status) IN ('ACK','SUBMITTED','NEW','PENDING','OPEN','PENDING_FILL')
                                                  THEN '08_pending'
            ELSE '09_unknown'
        END AS bucket
    FROM   orders
    WHERE  kind = 'ENTRY'
      AND  created_ts >= NOW() - INTERVAL '24 hours'
)
SELECT
    bucket,
    COUNT(*) AS entries,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM   bucketed
GROUP  BY bucket
ORDER  BY bucket;

\echo ''
\echo '== Fill latency p50 / p95 (filled entries, last 24h) =='
SELECT
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (p.opened_ts - o.created_ts))) AS p50_sec,
    PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (p.opened_ts - o.created_ts))) AS p95_sec,
    COUNT(*) AS sample_size
FROM       orders o
JOIN       positions p ON p.contract = o.contract AND p.client_id = o.client_id
WHERE      o.kind = 'ENTRY'
  AND      o.status = 'FILLED'
  AND      o.created_ts >= NOW() - INTERVAL '24 hours'
  AND      p.opened_ts IS NOT NULL
  AND      p.opened_ts >= o.created_ts;

\echo ''
\echo '== Sizing summary (last 24h, ACCOUNT_EQUITY_PCT only) =='
SELECT
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY (meta->>'final_qty')::int) AS p50_qty,
    PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY (meta->>'final_qty')::int) AS p95_qty,
    PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY (meta->>'position_budget')::numeric) AS p50_budget,
    PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY (meta->>'position_budget')::numeric) AS p95_budget,
    COUNT(*) AS sample_size
FROM   orders
WHERE  kind = 'ENTRY'
  AND  created_ts >= NOW() - INTERVAL '24 hours'
  AND  (meta->>'sizing_reason_code') = 'ACCOUNT_EQUITY_PCT';

\echo ''
\echo '== Retry effectiveness (last 7d, only orders with retry_attempt >= 1) =='
SELECT
    (meta->>'retry_attempts')::int AS retry_attempt,
    COUNT(*) FILTER (WHERE status = 'FILLED') AS filled,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'FILLED') / NULLIF(COUNT(*),0), 1) AS fill_rate_pct
FROM   orders
WHERE  kind = 'ENTRY'
  AND  (meta->>'retry_attempts')::int >= 1
  AND  created_ts >= NOW() - INTERVAL '7 days'
GROUP  BY 1
ORDER  BY 1;

\echo ''
\echo '== Read-only role check =='
SELECT current_user, session_user, current_setting('default_transaction_read_only') AS read_only;
\echo 'EXPECTED: read_only = on (or current_user is a read-only role).'
