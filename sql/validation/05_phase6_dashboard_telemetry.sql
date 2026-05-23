-- ============================================================
-- Phase 6 validation: dashboard telemetry projection
-- ============================================================
-- Proves the 23-field telemetry view can be assembled for every entry
-- by joining orders + positions and reading meta. The dashboard backend
-- will call ap.entry_telemetry.compute_entry_telemetry() in code; these
-- queries replicate the same fields in pure SQL so ops can sanity-check.

\echo '== Phase 6.1: telemetry view materialized in SQL (last 50 entries) =='
SELECT
    o.client_id,
    o.local_order_id,
    o.broker_order_id,
    o.symbol,
    o.contract,
    o.direction,
    o.status,
    (o.meta->>'score')::numeric                  AS score,

    -- Attempt counters
    COALESCE((o.meta->>'entry_attempt')::int,   0) AS entry_attempt,
    COALESCE((o.meta->>'repeg_attempts')::int,  0) AS repeg_attempt,
    COALESCE((o.meta->>'retry_attempts')::int,  0) AS retry_attempt,

    -- reason_bucket (mirrors ap.entry_telemetry.derive_reason_bucket)
    CASE
        WHEN UPPER(o.status) = 'FILLED'        THEN 'filled'
        WHEN UPPER(o.status) IN ('PARTIAL_FILL','PARTIAL') THEN 'partial'
        WHEN UPPER(o.status) = 'REJECTED'      THEN 'rejected'
        WHEN UPPER(o.status) = 'EXPIRED'       THEN 'expired'
        WHEN UPPER(o.status) IN ('CANCELED','CANCELLED') AND
             LOWER(COALESCE(o.last_error,'')) ~ 'entry_max_age_(normal|aplus)_reached'
                                                  THEN 'expired'
        WHEN UPPER(o.status) IN ('CANCELED','CANCELLED') AND
             LOWER(COALESCE(o.last_error,'')) ~
             '(thesis_invalid|spread_wide|runaway_quote|positions_full|lost_handoff|risk_gate_blocked|kill_switch_active|read_only_mode|client_inactive)'
                                                  THEN 'canceled_signal_dead'
        WHEN UPPER(o.status) IN ('CANCELED','CANCELLED') AND
             LOWER(COALESCE(o.last_error,'')) ~
             '(stale_entry_timeout|missed_move|broker_transient_error|unfilled_at_ladder_top)'
                                                  THEN 'canceled_signal_alive'
        WHEN UPPER(o.status) IN ('CANCELED','CANCELLED')  THEN 'canceled_other'
        WHEN UPPER(o.status) IN ('ACK','ACKED','SUBMITTED','NEW','PENDING','OPEN','PENDING_FILL','PENDING_TRIGGER','CREATED','OK','ACCEPTED','ACKNOWLEDGED')
                                                  THEN 'pending'
        ELSE 'unknown'
    END                                          AS reason_bucket,
    o.last_error                                 AS cancel_reason_detail,

    -- Submit-time pricing (Phase 3)
    (o.meta->>'selector_ask')::numeric           AS selector_ask,
    (o.meta->>'submit_ask')::numeric             AS submit_ask,
    COALESCE(
        (o.meta->>'submit_limit')::numeric,
        o.limit_price
    )                                            AS submit_limit,
    p.avg_fill                                   AS fill_price,

    -- Timing
    (o.meta->>'quote_age_ms')::int               AS quote_age_ms,
    CASE WHEN p.opened_ts IS NOT NULL AND o.created_ts IS NOT NULL
         THEN EXTRACT(EPOCH FROM (p.opened_ts - o.created_ts))
         ELSE NULL
    END                                          AS seconds_to_fill,

    -- Sizing (Phase 4)
    (o.meta->>'account_equity')::numeric         AS account_equity,
    (o.meta->>'position_budget')::numeric        AS position_budget,
    COALESCE((o.meta->>'final_qty')::int, o.qty) AS final_qty,
    o.meta->>'sizing_reason_code'                AS sizing_reason_code
FROM       orders o
LEFT  JOIN positions p ON p.contract = o.contract
                      AND p.client_id = o.client_id
                      AND p.status IN ('OPEN','CLOSED','CLOSING','PENDING')
WHERE      o.kind = 'ENTRY'
ORDER  BY  o.created_ts DESC
LIMIT  50;
\echo 'EXPECTED: every column populated for entries created after deploy.'
\echo '  fill_price NULL on pending; seconds_to_fill NULL on unfilled.'

\echo ''
\echo '== Phase 6.2: reason_bucket histogram (last 7d) =='
SELECT
    CASE
        WHEN UPPER(o.status) = 'FILLED'        THEN 'filled'
        WHEN UPPER(o.status) IN ('PARTIAL_FILL','PARTIAL') THEN 'partial'
        WHEN UPPER(o.status) = 'REJECTED'      THEN 'rejected'
        WHEN UPPER(o.status) = 'EXPIRED'       THEN 'expired'
        WHEN UPPER(o.status) IN ('CANCELED','CANCELLED') AND
             LOWER(COALESCE(o.last_error,'')) ~ 'entry_max_age_(normal|aplus)_reached'
                                                  THEN 'expired'
        WHEN UPPER(o.status) IN ('CANCELED','CANCELLED') AND
             LOWER(COALESCE(o.last_error,'')) ~
             '(thesis_invalid|spread_wide|runaway_quote|positions_full|lost_handoff|risk_gate_blocked|kill_switch_active)'
                                                  THEN 'canceled_signal_dead'
        WHEN UPPER(o.status) IN ('CANCELED','CANCELLED') AND
             LOWER(COALESCE(o.last_error,'')) ~
             '(stale_entry_timeout|missed_move|broker_transient_error)'
                                                  THEN 'canceled_signal_alive'
        WHEN UPPER(o.status) IN ('CANCELED','CANCELLED')  THEN 'canceled_other'
        WHEN UPPER(o.status) IN ('ACK','SUBMITTED','NEW','PENDING','OPEN','PENDING_FILL')
                                                  THEN 'pending'
        ELSE 'unknown'
    END AS reason_bucket,
    COUNT(*) AS entries
FROM   orders o
WHERE  o.kind = 'ENTRY'
  AND  o.created_ts >= NOW() - INTERVAL '7 days'
GROUP  BY 1
ORDER  BY 2 DESC;
\echo 'EXPECTED: filled is the largest bucket. canceled_signal_alive should'
\echo '  be small after retries land. Lots of canceled_other = miscategorized'
\echo '  reasons; expand the LIKE patterns above.'
