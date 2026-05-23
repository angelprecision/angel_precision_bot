-- ============================================================
-- Phase 3 validation: submit-time ask refresh + chase-band guard
-- ============================================================
-- Proves the new telemetry fields are populating and the
-- RUNAWAY_QUOTE_AT_SUBMIT block is working.
--
-- Expected after Phase 3 is deployed:
--   * Every entry order created after deploy has selector_ask /
--     submit_ask / submit_limit / quote_age_ms / entry_attempt in meta.
--   * Average quote_age_ms is small (< 200ms typical).
--   * RUNAWAY_QUOTE_AT_SUBMIT events appear sometimes during volatile
--     moves (and never on quiet days).

\echo '== Phase 3.1: telemetry coverage (last 24h, ENTRY orders) =='
SELECT
    COUNT(*)                                                          AS total_entries,
    COUNT(meta->>'selector_ask')                                      AS has_selector_ask,
    COUNT(meta->>'submit_ask')                                        AS has_submit_ask,
    COUNT(meta->>'submit_limit')                                      AS has_submit_limit,
    COUNT(meta->>'quote_age_ms')                                      AS has_quote_age_ms,
    COUNT(meta->>'entry_attempt')                                     AS has_entry_attempt
FROM   orders
WHERE  kind = 'ENTRY'
  AND  created_ts >= NOW() - INTERVAL '24 hours';
\echo 'EXPECTED: has_* counts should equal total_entries (every new entry'
\echo '  must carry the Phase 3 fields). Any gap means a code path is'
\echo '  bypassing process_signal and inserting orders directly.'

\echo ''
\echo '== Phase 3.2: quote refresh latency distribution =='
SELECT
    width_bucket((meta->>'quote_age_ms')::int, 0, 1000, 10) AS bucket,
    COUNT(*) AS orders_in_bucket
FROM   orders
WHERE  kind = 'ENTRY'
  AND  created_ts >= NOW() - INTERVAL '24 hours'
  AND  (meta->>'quote_age_ms') IS NOT NULL
GROUP  BY 1
ORDER  BY 1;
\echo 'EXPECTED: most mass in low buckets (<200ms). Long tails suggest'
\echo '  broker quote endpoint is slow \u2014 not a code bug.'

\echo ''
\echo '== Phase 3.3: RUNAWAY_QUOTE_AT_SUBMIT block counts (last 7d) =='
SELECT
    date_trunc('day', ts) AS day,
    COUNT(*) AS runaway_blocks
FROM   audit_log
WHERE  event = 'RUNAWAY_QUOTE_AT_SUBMIT'
  AND  ts >= NOW() - INTERVAL '7 days'
GROUP  BY 1
ORDER  BY 1;
\echo 'EXPECTED: non-zero on volatile days; zero on quiet days is fine.'

\echo ''
\echo '== Phase 3.4: selector vs submit ask drift (filled entries, 7d) =='
SELECT
    AVG(((meta->>'submit_ask')::numeric / NULLIF((meta->>'selector_ask')::numeric,0) - 1) * 100)   AS avg_gap_pct,
    STDDEV(((meta->>'submit_ask')::numeric / NULLIF((meta->>'selector_ask')::numeric,0) - 1) * 100) AS stddev_gap_pct,
    MIN(((meta->>'submit_ask')::numeric / NULLIF((meta->>'selector_ask')::numeric,0) - 1) * 100)    AS min_gap_pct,
    MAX(((meta->>'submit_ask')::numeric / NULLIF((meta->>'selector_ask')::numeric,0) - 1) * 100)    AS max_gap_pct,
    COUNT(*) AS sample_size
FROM   orders
WHERE  kind = 'ENTRY'
  AND  status IN ('FILLED', 'PARTIAL_FILL')
  AND  created_ts >= NOW() - INTERVAL '7 days'
  AND  (meta->>'submit_ask')   IS NOT NULL
  AND  (meta->>'selector_ask') IS NOT NULL;
\echo 'EXPECTED: avg close to 0%, max < 8% (SUBMIT_CHASE_BAND_PCT default).'
\echo '  A max > 8% means the chase band was violated somewhere.'
