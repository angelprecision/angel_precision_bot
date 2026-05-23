-- ============================================================
-- Phase 2 validation: adaptive autocancel
-- ============================================================
-- Proves the 25s re-evaluate + 90s/120s ceiling behavior is live.
--
-- Expected after Phase 2 is deployed:
--   * ENTRY_REEVAL events appear in audit_log for orders that sit
--     between 25s and the ceiling without filling.
--   * ENTRY_MAX_AGE_NORMAL_REACHED fires at ~90s for non-A+ orders.
--   * ENTRY_MAX_AGE_APLUS_REACHED  fires at ~120s for A+ orders (score >= 85).
--   * No hard cancel fires before 25s (other than for the explicit
--     immediate-cancel reasons in the spec).
--
-- All read-only.

\echo '== Phase 2.1: ENTRY_REEVAL event count by hour (last 24h) =='
SELECT
    date_trunc('hour', ts) AS hour_utc,
    COUNT(*) AS reeval_events
FROM   audit_log
WHERE  event = 'ENTRY_REEVAL'
  AND  ts >= NOW() - INTERVAL '24 hours'
GROUP  BY 1
ORDER  BY 1
LIMIT  48;

\echo ''
\echo '== Phase 2.2: ENTRY_MAX_AGE_*_REACHED counts (last 24h) =='
SELECT
    event,
    COUNT(*) AS hits,
    MIN(ts)  AS first_hit,
    MAX(ts)  AS last_hit
FROM   audit_log
WHERE  event IN ('ENTRY_MAX_AGE_NORMAL_REACHED', 'ENTRY_MAX_AGE_APLUS_REACHED')
  AND  ts >= NOW() - INTERVAL '24 hours'
GROUP  BY event;

\echo ''
\echo '== Phase 2.3: cancels-before-25s should ONLY be in the immediate-cancel set =='
SELECT
    COALESCE(meta->>'cancel_reason_detail', last_error, 'UNKNOWN') AS reason,
    COUNT(*) AS cancels
FROM   orders
WHERE  kind = 'ENTRY'
  AND  status = 'CANCELED'
  AND  created_ts >= NOW() - INTERVAL '24 hours'
  AND  EXTRACT(EPOCH FROM (updated_ts - created_ts)) < 25
GROUP  BY 1
ORDER  BY 2 DESC
LIMIT  20;
\echo 'EXPECTED reasons (only): thesis_invalid, spread_wide, runaway_quote,'
\echo '  runaway_quote_at_submit, positions_full, lost_handoff,'
\echo '  risk_gate_blocked, kill_switch_active. Any OTHER reason here means'
\echo '  Phase 2 is being bypassed for some path \u2014 investigate.'

\echo ''
\echo '== Phase 2.4: A+ orders should get the 120s ceiling, not 90s =='
SELECT
    CASE WHEN (meta->>'score')::numeric >= 85 THEN 'aplus' ELSE 'normal' END AS tier,
    COUNT(*) FILTER (WHERE last_error LIKE '%ENTRY_MAX_AGE_NORMAL_REACHED%') AS hit_90s,
    COUNT(*) FILTER (WHERE last_error LIKE '%ENTRY_MAX_AGE_APLUS_REACHED%')  AS hit_120s,
    COUNT(*) AS total_in_tier
FROM   orders
WHERE  kind = 'ENTRY'
  AND  status = 'CANCELED'
  AND  created_ts >= NOW() - INTERVAL '24 hours'
GROUP  BY 1;
\echo 'EXPECTED: aplus row \u2192 hit_90s should be 0, hit_120s > 0.'
\echo '          normal row \u2192 hit_120s should be 0, hit_90s > 0.'
\echo 'A non-zero off-diagonal means tier classification is wrong.'
