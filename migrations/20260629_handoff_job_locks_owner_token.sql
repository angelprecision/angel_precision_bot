BEGIN;

ALTER TABLE public.handoff_job_locks
ADD COLUMN IF NOT EXISTS owner_token TEXT;

ALTER TABLE public.handoff_job_locks
ADD COLUMN IF NOT EXISTS failed_reason TEXT;

CREATE INDEX IF NOT EXISTS idx_handoff_job_locks_owner
ON public.handoff_job_locks (run_key, owner_token);

COMMIT;
