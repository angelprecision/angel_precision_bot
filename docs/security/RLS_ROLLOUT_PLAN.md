# Supabase RLS rollout plan — review draft

Status: **DRAFT / HOLD — not applied to Supabase, not a release authorization.**

This review branch is based on committed GitHub `main` at
`cb4a687eaf59438542064b891595aee4a6cc1271`.

## Phase 1 in this branch

`migrations/20260823_rls_public_surface_lockdown.sql` is a fail-closed
baseline for the live public security surface. It:

- enables RLS on the exact 22 tables found with RLS disabled;
- revokes `PUBLIC`, `anon`, and `authenticated` table privileges on those 22
  tables and makes service-role CRUD explicit;
- closes the 17 additional tables with live-verified permissive public/anon
  policies by revoking public table access, while preserving service-role CRUD;
- removes exactly 18 live-verified `USING (true)` / `WITH CHECK (true)` policies
  targeting `public` or `anon`; the service-role-only client-health policy is
  preserved;
- revokes `PUBLIC`, `anon`, and `authenticated` access to all seven currently
  exposed public views and grants explicit `service_role` read access;
- removes those roles from future table, sequence, and function defaults
  created by `postgres` in `public`; the preflight refuses to proceed while
  Supabase-managed `supabase_admin` defaults remain broad;
- fails before DDL if the table, view, policy, owner, privilege, or public-RLS
  scope has drifted.

No guessed tenant policies are created. The dashboard auth model and the
`client_email`/`client_id` ownership bridge remain unresolved. The bot’s direct
PostgreSQL path and `SUPABASE_SERVICE_KEY` path are intentionally preserved.

## Required staging gates before merge or apply

Run `sql/validation/07_rls_hardening.sql` against an isolated staging
database after applying the migration. It must pass all catalog assertions,
and staging must additionally prove:

1. All 22 target tables have RLS enabled and no other public table has RLS
   disabled.
2. `anon` and `authenticated` have no SELECT/INSERT/UPDATE/DELETE privileges
   on either lockdown table scope.
3. `service_role` retains SELECT/INSERT/UPDATE/DELETE on every lockdown table
   and SELECT on every lockdown view.
4. Anonymous and authenticated Data API requests are denied for the
   fail-closed phase-1 scope, including the seven views.
5. The bot’s direct PostgreSQL lifecycle paths still read and write orders,
   client state, fills, signals, proof records, and migration history.
6. Every dashboard operation that must remain available has an explicit,
   reviewed owner predicate before an authenticated policy is added.
7. No public/anon/authenticated permissive policy remains. Service-role-only
   policies require a separate owner review but do not expose the Data API.
8. An owner-authorized follow-up removes the remaining `supabase_admin`
   default privileges before this migration is applied, because the
   application `postgres` role is not a member of `supabase_admin`.

## Rollback boundary

Do not use a blanket rollback that disables RLS, restores public grants, or
restores anonymous policies. If staging exposes a consumer regression, stop
the rollout, capture the exact role/table/view failure, and add a reviewed
owner-scoped policy or privileged service path. Any production rollback must
be an explicitly reviewed, time-bounded operator action with the affected
objects named; this branch contains no automatic rollback SQL.

## Review posture

This is a security migration draft for further review. It is not safe to merge
or deploy until the staging assertions, real role-based Data API tests, bot
lifecycle proof, and dashboard owner mapping pass. The migration test is part
of the P0 workflow, but unrelated existing P0 failures still keep the PR on
HOLD.
