# Supabase RLS rollout plan — review draft

Status: **DRAFT / HOLD — not applied to Supabase, not a release authorization.**

This review branch is based on committed GitHub `main` at
`cb4a687eaf59438542064b891595aee4a6cc1271`.

## Phase 1 in this branch

`migrations/20260823_rls_public_surface_lockdown.sql` is a fail-closed
baseline for the 22 public tables found with RLS disabled in the live project.
It:

- verifies the exact table scope, `postgres` ownership, and existing
  `service_role` CRUD access before making changes;
- enables RLS on all 22 tables;
- revokes `PUBLIC`, `anon`, and `authenticated` table privileges on those
  tables;
- removes those roles from future table, sequence, and function defaults
  created by `postgres` in `public`;
- does not create guessed tenant policies, change `FORCE ROW LEVEL SECURITY`,
  rewrite existing policies, or alter public views.

The bot’s direct `DATABASE_URL` path and `SUPABASE_SERVICE_KEY` path are
privileged paths and are intentionally preserved. No application or execution
code is changed in this branch.

## Required staging gates before merge or apply

Run `sql/validation/07_rls_hardening.sql` against an isolated staging
database after applying the migration there. The result must prove:

1. All 22 tables have RLS enabled.
2. `anon` and `authenticated` have no table privileges on the 22-table scope.
3. `service_role` retains SELECT/INSERT/UPDATE/DELETE on every target table.
4. The bot’s direct PostgreSQL lifecycle paths still read and write orders,
   client state, fills, signals, and proof records.
5. Anonymous and authenticated Data API requests return denial/zero rows for
   the fail-closed phase-1 scope.
6. Every dashboard operation that must remain available has an explicit,
   reviewed owner predicate before an authenticated policy is added.
7. Public views are either made `security_invoker = true` with tested base
   policies or have access revoked/moved to a non-exposed schema. The current
   branch reports these views but intentionally does not guess their consumer
   contract.
8. Existing policies using `USING (true)` or `WITH CHECK (true)` receive an
   owner decision; they are not silently widened or reused as tenant policy.
9. The remaining `supabase_admin` default privileges are handled by an
   owner-authorized follow-up, because the application `postgres` role is not a
   member of `supabase_admin`.

## Rollback boundary

Do not use a blanket rollback that disables RLS and restores public grants.
If staging exposes a consumer regression, stop the rollout, capture the exact
role/table/view failure, and amend the policy contract. Any production rollback
must be an explicitly reviewed, time-bounded operator action with the affected
tables and grants named; this branch contains no automatic rollback SQL.

## Review posture

This is a security migration draft for further review. It is not safe to merge
or deploy until the staging gates above pass and the dashboard owner mapping is
documented for any required authenticated access.
