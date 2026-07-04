-- =============================================================================
-- PR-G: Volatility-Scaled Exit Ladder — optional metadata column
--
-- Adds positions.vol_exit_meta (JSONB, nullable). This column carries the
-- IV/delta/premium snapshot captured at contract selection, used ONLY by the
-- vol_scaled exit ladder (ap/exit_ladder.py) when it is explicitly enabled.
--
-- DORMANCY GUARANTEE: this column is a SECOND independent dormancy layer on
-- top of the env-flag guard. With the flags off (default), the value is
-- written but never read. Applying this migration alone changes NO behavior.
--
-- Safe to apply anytime: additive, nullable, no backfill, no default, no
-- index. Existing rows get NULL. ADD COLUMN IF NOT EXISTS is idempotent.
-- =============================================================================
ALTER TABLE positions
    ADD COLUMN IF NOT EXISTS vol_exit_meta JSONB;
