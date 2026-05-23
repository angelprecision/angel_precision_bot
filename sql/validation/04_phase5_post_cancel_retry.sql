-- ============================================================
-- Phase 5 validation: post-cancel retry
-- ============================================================
-- Once order_monitor is wired to call evaluate_retry() (separate PR),
-- these queries prove the retry logic is behaving as specified:
--   * Max 2 retries per signal
--   * Skip reasons in NON_RETRYABLE_REASONS always ABORT
--   * Retried orders carry retry_attempt >= 1
--   * No infinite loops

\echo '== Phase 5.1: ENTRY_RETRY_* event counts (last 24h) =='
SELECT
    event,
    COUNT(*) AS count
FROM   audit_log
WHERE  event IN ('ENTRY_RETRY_ARMED', 'ENTRY_RETRY_SUBMITTED', 'ENTRY_RETRY_ABORTED')
  AND  ts >= NOW() - INTERVAL '24 hours'
GROUP  BY 1
ORDER  BY 2 DESC;
\echo 'EXPECTED: ARMED >= SUBMITTED >= ABORTED is fine \u2014 some armed retries'
\echo '  fail at submit-time (e.g. quote ran). ABORTED counts the skip path.'

\echo ''
\echo '== Phase 5.2: retry_attempt distribution =='
SELECT
    COALESCE((meta->>'retry_attempts')::int, 0) AS retry_attempt,
    COUNT(*) AS orders
FROM   orders
WHERE  kind = 'ENTRY'
  AND  created_ts >= NOW() - INTERVAL '24 hours'
GROUP  BY 1
ORDER  BY 1;
\echo 'EXPECTED: 0 dominates (first attempts), 1 and 2 small, NO rows with'
\echo '  retry_attempt > 2. Anything above 2 means ENTRY_RETRY_MAX_ATTEMPTS'
\echo '  is being bypassed.'

\echo ''
\echo '== Phase 5.3: cancel-reason -> retry-decision matrix (last 7d) =='
SELECT
    COALESCE(meta->>'retry_cancel_reason', meta->>'cancel_reason_detail', last_error, 'UNKNOWN') AS cancel_reason,
    COUNT(*) AS retries,
    COUNT(*) FILTER (WHERE status = 'FILLED') AS retries_filled,
    COUNT(*) FILTER (WHERE status = 'CANCELED') AS retries_canceled_again
FROM   orders
WHERE  kind = 'ENTRY'
  AND  (meta->>'retry_attempts')::int >= 1
  AND  created_ts >= NOW() - INTERVAL '7 days'
GROUP  BY 1
ORDER  BY 2 DESC;
\echo 'EXPECTED: cancel reasons here should ONLY be retryable ones'
\echo '  (entry_max_age_normal_reached, entry_max_age_aplus_reached,'
\echo '   stale_entry_timeout, missed_move, broker_transient_error,'
\echo '   unfilled_at_ladder_top). Any thesis_invalid / runaway_quote /'
\echo '   spread_wide / positions_full / lost_handoff / risk_gate_blocked'
\echo '   appearing here means the skip-list is being violated.'

\echo ''
\echo '== Phase 5.4: retry success rate =='
SELECT
    (meta->>'retry_attempts')::int AS attempt,
    COUNT(*) FILTER (WHERE status = 'FILLED')   AS filled,
    COUNT(*) FILTER (WHERE status = 'CANCELED') AS canceled,
    COUNT(*) FILTER (WHERE status = 'REJECTED') AS rejected,
    COUNT(*) AS total,
    ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'FILLED') / NULLIF(COUNT(*),0), 1) AS fill_rate_pct
FROM   orders
WHERE  kind = 'ENTRY'
  AND  (meta->>'retry_attempts')::int >= 1
  AND  created_ts >= NOW() - INTERVAL '7 days'
GROUP  BY 1
ORDER  BY 1;
\echo 'EXPECTED: fill_rate_pct should be > 30% on retries; otherwise the'
\echo '  retry is converting cycles into noise. Adjust SUBMIT_CHASE_BAND_PCT'
\echo '  / ENTRY_RETRY_ALIGNMENT_DRIFT_PCT if fill rate is too low.'
