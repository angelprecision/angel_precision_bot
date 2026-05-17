-- ============================================================================
-- H7: Canonical exit classification bucket
-- ============================================================================
-- Adds exit_bucket to proof_trades so the proof cycle can be measured as
-- win-rate / breakeven-rate / true-loss-rate / hard-stop count, with average
-- win and average loss per bucket.
--
-- SAFETY: nullable column, no default. The proof logger writes it going
-- forward; if this migration is not yet applied the logger falls back to
-- inserting without the column (proof logging is never lost).
--
-- Run once in the Supabase SQL editor.
-- ============================================================================

ALTER TABLE proof_trades
  ADD COLUMN IF NOT EXISTS exit_bucket text;

-- ----------------------------------------------------------------------------
-- Backfill existing rows from exit_reason + option_pnl_pct + win.
-- Mirrors ap_proof_logger.classify_exit() exactly. Safe to re-run.
-- ----------------------------------------------------------------------------
UPDATE proof_trades SET exit_bucket =
  CASE
    WHEN upper(coalesce(exit_reason,'')) LIKE '%MANUAL%'
      OR upper(coalesce(exit_reason,'')) LIKE '%ADMIN FORCE%'
      OR upper(coalesce(exit_reason,'')) LIKE '%FORCE_EXIT%'      THEN 'MANUAL_EXIT'
    WHEN upper(coalesce(exit_reason,'')) LIKE '%QUARANTINE%'
      OR upper(coalesce(exit_reason,'')) LIKE '%RECONCIL%'
      OR upper(coalesce(exit_reason,'')) LIKE '%GHOST%'            THEN 'RECONCILED_CLOSE'
    WHEN upper(coalesce(exit_reason,'')) LIKE '%EOD%'
      OR upper(coalesce(exit_reason,'')) LIKE '%FORCE CLOSE%'
      OR upper(coalesce(exit_reason,'')) LIKE '%MARKET CLOSED%'    THEN 'EOD_CLOSE'
    WHEN upper(coalesce(exit_reason,'')) LIKE '%HARD STOP%'
      OR upper(coalesce(exit_reason,'')) LIKE '%HARD_STOP%'        THEN 'HARD_STOP'
    WHEN coalesce(option_pnl_pct,0) BETWEEN -3.0 AND 3.0           THEN 'BREAKEVEN_SAVE'
    WHEN coalesce(win,false) = true OR coalesce(option_pnl_pct,0) > 3.0 THEN
      CASE
        WHEN upper(coalesce(exit_reason,'')) LIKE '%RUNNER%'       THEN 'WIN_RUNNER'
        WHEN coalesce(option_pnl_pct,0) >= 25.0                    THEN 'WIN_RUNNER'
        ELSE 'WIN_BASE_HIT'
      END
    WHEN coalesce(option_pnl_pct,0) < -3.0                         THEN 'SOFT_LOSS'
    ELSE 'UNCLASSIFIED'
  END
WHERE exit_bucket IS NULL;

-- Proof summary by bucket
-- SELECT exit_bucket,
--        count(*)                         AS trades,
--        round(avg(option_pnl_pct)::numeric, 1) AS avg_pnl_pct,
--        round(min(option_pnl_pct)::numeric, 1) AS worst,
--        round(max(option_pnl_pct)::numeric, 1) AS best
-- FROM proof_trades
-- GROUP BY exit_bucket ORDER BY trades DESC;
