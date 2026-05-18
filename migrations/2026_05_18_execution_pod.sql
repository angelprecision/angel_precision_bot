-- ============================================================================
-- Execution pod assignment
-- ============================================================================
-- Adds execution_pod to members. Each live Render service runs as a "pod"
-- serving up to MAX_POD_CLIENTS (default 5) clients. A bot service with
-- POD_ID=live-pod-1 only loads members where execution_pod='live-pod-1'.
--
-- Nullable — existing members are unaffected (NULL = not assigned to any
-- pod = only picked up by shared/single-client services, never a pod).
-- Run once in Supabase SQL editor.
-- ============================================================================

ALTER TABLE members
  ADD COLUMN IF NOT EXISTS execution_pod text;

-- ── Assign first pod (example — set the real emails) ───────────────────────
-- UPDATE members SET execution_pod = 'live-pod-1'
--   WHERE email IN ('client1@email.com', 'client2@email.com');

-- ── Pod roster check ───────────────────────────────────────────────────────
-- SELECT execution_pod, count(*) AS clients,
--        string_agg(email, ', ' ORDER BY email) AS roster
-- FROM members
-- WHERE approved AND subscription_active AND execution_pod IS NOT NULL
-- GROUP BY execution_pod ORDER BY execution_pod;
