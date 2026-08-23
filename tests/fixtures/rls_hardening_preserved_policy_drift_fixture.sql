-- Isolated negative case: changing a preserved owner predicate must abort the
-- migration before any lockdown DDL is committed.

\set ON_ERROR_STOP on

ALTER POLICY members_read_own ON public.members
    USING (true);
