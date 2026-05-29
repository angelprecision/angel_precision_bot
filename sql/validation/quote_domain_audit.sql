-- ============================================================================
-- QUOTE-DOMAIN AUDIT — verification queries (read-only)
-- Run after a monitored paper session to separate signal quality from
-- Tradier sandbox quote-domain fill distortion.
-- ============================================================================

-- A. Paper no-fill but contract went green.
--    These are the orders the quote-domain mismatch most likely cost us:
--    submitted in paper, never filled, but the contract traded above our limit
--    afterward. (max_contract_price_after_submit / MFE_pct require the
--    after-trigger tracking from spec item 4 — shown here as meta fields;
--    rows where they are null just haven't been backfilled yet.)
SELECT
  o.symbol,
  o.contract,
  o.meta->>'submit_ask'                          AS submit_ask,
  o.limit_price                                  AS submit_limit,
  o.meta->>'paper_fill_mode'                     AS paper_fill_mode,
  o.meta->>'paper_submitted_type'                AS paper_submitted_type,
  o.status                                        AS broker_status,
  o.meta->>'max_contract_price_after_submit'     AS max_price_after,
  o.meta->>'mfe_pct'                             AS mfe_pct,
  o.meta->>'went_green_after_trigger'           AS went_green,
  o.meta->>'quote_domain_mismatch_possible'      AS quote_domain_mismatch
FROM orders o
WHERE o.kind = 'ENTRY'
  AND o.meta->>'mode' = 'PAPER'
  AND o.status IN ('CANCELED', 'REJECTED', 'ACK')   -- never reached FILLED
  AND o.created_ts > NOW() - INTERVAL '2 days'
ORDER BY o.created_ts DESC;

-- B. Watcher reject source — which Tradier env did the watcher evaluate against?
SELECT
  o.symbol,
  o.meta->'watcher_audit'->>'score'              AS score,
  o.meta->'watcher_audit'->>'tier'               AS tier,
  o.meta->'watcher_audit'->>'timeframe'          AS timeframe,
  o.meta->'watcher_audit'->>'reason_code'        AS reason_code,
  o.meta->'watcher_audit'->>'current_mid'        AS current_mid,
  o.meta->'watcher_audit'->>'watcher_quote_source'   AS watcher_quote_source,
  o.meta->'watcher_audit'->>'watcher_quote_base_url' AS watcher_quote_base_url,
  o.meta->'watcher_audit'->>'watcher_sandbox_mode'   AS watcher_sandbox_mode
FROM orders o
WHERE o.kind = 'ENTRY'
  AND o.meta->'watcher_audit' IS NOT NULL
  AND o.created_ts > NOW() - INTERVAL '2 days'
ORDER BY o.created_ts DESC;

-- C. Quote mismatch evidence — selector source vs submit source vs broker env.
--    A row where selector_quote_source = tradier_live but submit/broker is
--    sandbox is the exact distortion: contract chosen on live quotes, filled
--    against delayed sandbox quotes.
SELECT
  o.symbol,
  o.contract,
  o.meta->>'selector_quote_source'    AS selector_quote_source,
  o.meta->>'selector_quote_base_url'  AS selector_quote_base_url,
  o.meta->>'submit_quote_source'      AS submit_quote_source,
  o.meta->>'submit_quote_base_url'    AS submit_quote_base_url,
  o.meta->>'tradier_sandbox_mode'     AS tradier_sandbox_mode,
  o.meta->>'selector_ask'             AS selector_ask,
  o.meta->>'submit_ask'               AS submit_ask,
  o.meta->>'submit_limit'             AS submit_limit,
  o.meta->>'quote_domain_mismatch_possible' AS mismatch_possible,
  o.status                            AS broker_status,
  o.last_error                        AS broker_no_fill_reason
FROM orders o
WHERE o.kind = 'ENTRY'
  AND o.meta->>'quote_domain_mismatch_possible' = 'true'
  AND o.created_ts > NOW() - INTERVAL '2 days'
ORDER BY o.created_ts DESC;

-- D. Paper fill-mode effectiveness — did marketable_limit / market improve fills?
SELECT
  o.meta->>'paper_fill_mode'      AS paper_fill_mode,
  o.meta->>'paper_submitted_type' AS submitted_type,
  COUNT(*)                                                 AS n,
  COUNT(*) FILTER (WHERE o.status = 'FILLED')              AS filled,
  ROUND(100.0 * COUNT(*) FILTER (WHERE o.status='FILLED') / NULLIF(COUNT(*),0), 1) AS fill_pct,
  ROUND(AVG((o.meta->>'paper_cushion_applied')::numeric), 3) AS avg_cushion
FROM orders o
WHERE o.kind = 'ENTRY'
  AND o.meta->>'mode' = 'PAPER'
  AND o.created_ts > NOW() - INTERVAL '7 days'
GROUP BY 1, 2
ORDER BY n DESC;
