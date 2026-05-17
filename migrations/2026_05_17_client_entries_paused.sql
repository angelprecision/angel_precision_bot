-- ============================================================================
-- H5: Per-client entries pause
-- ============================================================================
-- Adds entries_paused to client_state. When true, that ONE client's new
-- entries are blocked by its master_control while exits keep running and
-- every other client is unaffected. Distinct from kill_switch (which is a
-- full READ_ONLY protective stop).
--
-- SAFETY: boolean DEFAULT false — running this pauses no one. The bot reads
-- it fresh (5s cache) so an operator toggle takes effect with no restart.
--
-- Run once in the Supabase SQL editor.
-- ============================================================================

ALTER TABLE client_state
  ADD COLUMN IF NOT EXISTS entries_paused boolean DEFAULT false;

-- Operator quick reference:
--   Pause a client:   bot POST /admin/client/<email>/pause_entries
--   Resume a client:  bot POST /admin/client/<email>/resume_entries
--   (or directly)     UPDATE client_state SET entries_paused = true
--                       WHERE client_id = '<email>';

-- SELECT client_id, kill_switch, entries_paused, mode, realized_pnl_today
-- FROM client_state ORDER BY client_id;
