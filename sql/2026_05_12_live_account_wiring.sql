-- 2026_05_12_live_account_wiring.sql
-- Angel Precision: Tradier live/paper account wiring for members table
-- Run this in Supabase SQL Editor.

ALTER TABLE members
ADD COLUMN IF NOT EXISTS tradier_live_access_token text,
ADD COLUMN IF NOT EXISTS tradier_live_account_id text,
ADD COLUMN IF NOT EXISTS tradier_paper_access_token text,
ADD COLUMN IF NOT EXISTS tradier_paper_account_id text,
ADD COLUMN IF NOT EXISTS tradier_account_mode text DEFAULT 'paper',
ADD COLUMN IF NOT EXISTS tradier_connected boolean DEFAULT false,
ADD COLUMN IF NOT EXISTS tradier_connected_at timestamptz,
ADD COLUMN IF NOT EXISTS tradier_access_token text,
ADD COLUMN IF NOT EXISTS tradier_account_id text,
ADD COLUMN IF NOT EXISTS tradier_base_url text DEFAULT 'https://sandbox.tradier.com';

-- Optional helper: switch one member's active Tradier account to LIVE.
-- Replace the email with the target member email.
-- IMPORTANT: tradier_live_account_id must be the real live Tradier account id.
-- UPDATE members
-- SET
--   tradier_account_mode = 'live',
--   tradier_connected = true,
--   tradier_connected_at = NOW(),
--   tradier_account_id = tradier_live_account_id,
--   tradier_access_token = tradier_live_access_token,
--   tradier_base_url = 'https://api.tradier.com'
-- WHERE email = '<client-email>';

-- Optional helper: switch one member's active Tradier account back to PAPER/SANDBOX.
-- UPDATE members
-- SET
--   tradier_account_mode = 'paper',
--   tradier_connected = true,
--   tradier_connected_at = NOW(),
--   tradier_account_id = tradier_paper_account_id,
--   tradier_access_token = tradier_paper_access_token,
--   tradier_base_url = 'https://sandbox.tradier.com'
-- WHERE email = '<client-email>';

-- Verification query.
-- SELECT
--   email,
--   tradier_account_mode,
--   tradier_connected,
--   tradier_connected_at,
--   tradier_live_account_id,
--   tradier_paper_account_id,
--   tradier_account_id,
--   tradier_base_url,
--   tradier_live_access_token IS NOT NULL AS has_live_token,
--   tradier_paper_access_token IS NOT NULL AS has_paper_token,
--   tradier_access_token IS NOT NULL AS has_active_token
-- FROM members
-- WHERE email = '<client-email>';

NOTIFY pgrst, 'reload schema';
