-- =============================================================================
-- PR5 — Live proof reconciliation: execution_mode normalization
-- Date: 2026-06-20
--
-- PROBLEM: execution_mode casing is inconsistent across tables, which causes
-- classify_official() check #1 ("execution_mode must be 'live'") to fail for
-- real live trades:
--     orders.execution_mode                 = 'live'   (correct)
--     watcher_decision_audit.execution_mode = 'LIVE'   (uppercase)
--     proof_trades.execution_mode           = 'unknown'/'live'/NULL (mixed)
--
-- This migration normalizes the canonical lowercase form WITHOUT changing any
-- trade's actual mode. It only fixes CASING and the 'unknown' sentinel where the
-- joined order proves the true mode.
--
-- SAFETY:
--   * Idempotent — safe to re-run.
--   * Touches only the execution_mode text column. No P&L, no prices, no
--     eligibility flags are modified here (eligibility is recomputed by the
--     repair script using the existing classify_official rules).
--   * Wrapped in a transaction. Review the SELECT counts before COMMIT.
--
-- RUN THIS YOURSELF after review. Do NOT auto-apply to live.
-- Recommended: run the two SELECT previews first, then the UPDATEs.
-- =============================================================================

BEGIN;

-- ── Preview 1: current casing distribution (run before updating) ────────────
-- SELECT 'orders' AS tbl, execution_mode, COUNT(*) FROM orders GROUP BY execution_mode
-- UNION ALL
-- SELECT 'watcher_decision_audit', execution_mode, COUNT(*) FROM watcher_decision_audit GROUP BY execution_mode
-- UNION ALL
-- SELECT 'proof_trades', execution_mode, COUNT(*) FROM proof_trades GROUP BY execution_mode
-- ORDER BY tbl, execution_mode;

-- ── 1. watcher_decision_audit: lowercase the casing ─────────────────────────
-- Safe to normalize broadly: watcher_decision_audit is observability and
-- casing does not change semantics. This is the table where 'LIVE'/'live'
-- inconsistency originated.
UPDATE watcher_decision_audit
SET    execution_mode = lower(execution_mode)
WHERE  execution_mode IS NOT NULL
  AND  execution_mode <> lower(execution_mode);

-- ── 2. proof_trades: EVIDENCE-BACKED normalization only ─────────────────────
-- AMENDMENT (review #6): do NOT broadly lowercase proof_trades.execution_mode.
-- Only set it when we can JOIN the row to a real order whose execution_mode is
-- 'live' (or 'paper'). This refuses to mark any row as 'live' without an order
-- that proves it. Rows with no joinable order (no local_order_id, or
-- local_order_id that doesn't match an orders row) are LEFT UNTOUCHED — the
-- repair script will handle them case-by-case with safety guards.
UPDATE proof_trades pt
SET    execution_mode = lower(o.execution_mode)
FROM   orders o
WHERE  pt.local_order_id IS NOT NULL
  AND  pt.local_order_id = o.local_order_id
  AND  o.execution_mode IS NOT NULL
  AND  o.execution_mode IN ('live', 'paper', 'LIVE', 'PAPER')
  AND  (pt.execution_mode IS NULL
        OR pt.execution_mode = ''
        OR pt.execution_mode = 'unknown'
        OR pt.execution_mode <> lower(o.execution_mode));

-- ── Preview 2: post-update distribution (run before COMMIT) ─────────────────
-- SELECT 'proof_trades' AS tbl, execution_mode, COUNT(*) FROM proof_trades GROUP BY execution_mode;

-- Review the above, then:
--   COMMIT;   -- to apply
-- or
--   ROLLBACK; -- to abort
COMMIT;
