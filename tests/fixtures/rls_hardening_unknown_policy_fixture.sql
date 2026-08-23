-- Isolated negative case: a non-literal public policy with an unreviewed
-- identity must abort the migration before any lockdown DDL is committed.

\set ON_ERROR_STOP on

CREATE POLICY unreviewed_members_policy ON public.members
    FOR SELECT TO PUBLIC USING (user_id IS NOT NULL);
