-- ============================================================================
-- Multi-Account Signal Ledger (Item 4) — READ-ONLY view
-- One row per (canonical signal, client/account, ENTRY order) so we can answer:
--   "For signal X: what happened on main, Jason, Jose?"
--
-- REEVAL JOIN: orders.signal_id may be wrapped as REEVAL:<uuid>:<hex> while
-- ap_signals.signal_id is the canonical UUID. We normalize with split_part
-- (position 2 = the UUID between the 1st and 2nd colon) before joining, so
-- re-evaluation orders attach to their original signal row.
-- ============================================================================

-- ── STEP 0 (run first; DO NOT GUESS the schema) ─────────────────────────────
-- Confirm the real columns before relying on this view. If ap_signals uses a
-- timestamp column other than created_at (e.g. inserted_at), or orders lacks
-- last_error / broker_order_id, adjust the SELECT to the real names.
--
--   SELECT column_name FROM information_schema.columns
--   WHERE table_name='orders' ORDER BY ordinal_position;
--
--   SELECT column_name FROM information_schema.columns
--   WHERE table_name='ap_signals' ORDER BY ordinal_position;
--
-- Verified from code (ap/order_state_machine.py INSERT + transition UPDATE):
--   orders has: local_order_id, client_id, plan_id, signal_id, kind, status,
--   symbol, contract, direction, qty, limit_price, reserved_cost, filled_qty,
--   fill_price, score, tier, trigger_price, stop_underlying, target_underlying,
--   pattern, timeframe, meta, broker_order_id, last_error, submitted_ts,
--   created_ts, updated_ts.
-- Verified from ap_signal_store.py: ap_signals has signal_id (uuid),
--   client_email, system_version, ticker, pattern, timeframe, side, score,
--   context_score, tier, decision_status, entry_trigger, stop_price,
--   target_price, underlying_at_signal, signal_payload, score_breakdown,
--   context_notes. Timestamp assumed created_at (Supabase default) — verify.
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW ap_multi_account_signal_ledger AS
SELECT
  COALESCE(
    CASE
      WHEN o.signal_id LIKE 'REEVAL:%' THEN split_part(o.signal_id, ':', 2)
      ELSE o.signal_id
    END,
    s.signal_id::text
  )                                                        AS canonical_signal_id,
  o.signal_id                                              AS order_signal_id,
  o.client_id,
  o.local_order_id,
  o.plan_id,
  COALESCE(o.symbol, s.ticker)                             AS symbol,
  COALESCE(o.direction, s.side)                            AS direction,
  COALESCE(o.pattern, s.pattern)                           AS pattern,
  COALESCE(o.timeframe, s.timeframe)                       AS timeframe,
  COALESCE(o.score, s.score)                               AS score,
  COALESCE(o.tier, s.tier)                                 AS tier,
  s.decision_status                                        AS signal_decision_status,
  s.context_notes                                          AS signal_context_notes,
  o.kind                                                   AS order_kind,
  o.status                                                 AS order_status,
  o.contract,
  o.qty,
  o.filled_qty,
  o.limit_price,
  o.reserved_cost,
  o.trigger_price,
  o.stop_underlying,
  o.target_underlying,
  o.last_error,
  o.broker_order_id,
  o.meta->>'selected_contract'                             AS selected_contract,
  o.meta->>'trigger_type'                                  AS trigger_type,
  o.meta->>'mode'                                          AS mode,
  o.meta->>'paper_fill_mode'                               AS paper_fill_mode,
  o.meta->>'quote_domain_mismatch_possible'               AS quote_domain_mismatch_possible,
  o.meta->'watcher_audit'->>'reason_code'                  AS watcher_reason_code,
  o.meta->'watcher_audit'->>'raw_reason'                   AS watcher_raw_reason,
  o.meta->'watcher_audit'->>'current_bid'                  AS watcher_current_bid,
  o.meta->'watcher_audit'->>'current_ask'                  AS watcher_current_ask,
  o.meta->'watcher_audit'->>'current_mid'                  AS watcher_current_mid,
  o.meta->'watcher_audit'->>'trigger_price'                AS watcher_trigger_price,
  o.meta->'watcher_audit'->>'stop_price'                   AS watcher_stop_price,
  o.meta->'watcher_audit'->>'distance_to_trigger_pct'      AS watcher_distance_to_trigger_pct,
  o.meta->'watcher_audit'->>'distance_to_stop_pct'         AS watcher_distance_to_stop_pct,
  o.meta->'watcher_audit'->>'watcher_quote_source'         AS watcher_quote_source,
  o.meta->'watcher_audit'->>'watcher_quote_base_url'       AS watcher_quote_base_url,
  o.meta->'selector_candidate_audit'                       AS selector_candidate_audit,
  s.created_at                                             AS signal_created_at,
  o.created_ts                                             AS order_created_ts,
  o.updated_ts                                             AS order_updated_ts,
  CASE
    WHEN o.local_order_id IS NULL                                              THEN 'NO_ORDER_FOR_CLIENT'
    WHEN o.status = 'PENDING_TRIGGER' AND o.broker_order_id IS NULL            THEN 'PENDING_TRIGGER_NO_BROKER'
    WHEN o.status IN ('ACK','SUBMITTED') AND o.broker_order_id IS NOT NULL     THEN 'BROKER_SUBMITTED'
    WHEN o.status IN ('FILLED','PARTIALLY_FILLED','PARTIAL_FILL')              THEN 'FILLED_OR_PARTIAL'
    WHEN o.status IN ('CANCELED','CANCELLED','EXPIRED','REJECTED')             THEN 'TERMINAL_NO_FILL'
    ELSE 'OTHER'
  END                                                      AS ledger_bucket
FROM orders o
LEFT JOIN ap_signals s
  ON s.signal_id::text =
     CASE
       WHEN o.signal_id LIKE 'REEVAL:%' THEN split_part(o.signal_id, ':', 2)
       ELSE o.signal_id
     END
WHERE o.kind = 'ENTRY';

-- ── ACCEPTANCE CHECK 8: prove REEVAL normalization works ────────────────────
-- A REEVAL-wrapped order must resolve to the same canonical_signal_id as the
-- bare UUID, and should join to the ap_signals row (signal_decision_status
-- non-null when the signal exists).
--
--   SELECT order_signal_id, canonical_signal_id, client_id, order_status,
--          ledger_bucket, signal_decision_status
--   FROM ap_multi_account_signal_ledger
--   WHERE order_signal_id LIKE 'REEVAL:%'
--   ORDER BY order_created_ts DESC
--   LIMIT 20;
