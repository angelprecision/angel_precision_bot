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
UPDATE watcher_decision_audit
SET    execution_mode = lower(execution_mode)
WHERE  execution_mode IS NOT NULL
  AND  execution_mode <> lower(execution_mode);

-- ── 2. proof_trades: lowercase existing non-null modes ──────────────────────
UPDATE proof_trades
SET    execution_mode = lower(execution_mode)
WHERE  execution_mode IS NOT NULL
  AND  execution_mode <> lower(execution_mode);

-- ── 3. proof_trades: resolve 'unknown'/NULL from the joined order ───────────
-- Only sets the mode when the linked order unambiguously proves it. Uses
-- local_order_id first; this is the safe, evidence-based fill (no guessing).
UPDATE proof_trades pt
SET    execution_mode = lower(o.execution_mode)
FROM   orders o
WHERE  pt.local_order_id IS NOT NULL
  AND  pt.local_order_id = o.local_order_id
  AND  o.execution_mode IS NOT NULL
  AND  (pt.execution_mode IS NULL
        OR pt.execution_mode = 'unknown'
        OR pt.execution_mode = '');

-- ── Preview 2: post-update distribution (run before COMMIT) ─────────────────
-- SELECT 'proof_trades' AS tbl, execution_mode, COUNT(*) FROM proof_trades GROUP BY execution_mode;

-- Review the above, then:
--   COMMIT;   -- to apply
-- or
--   ROLLBACK; -- to abort
COMMIT;
