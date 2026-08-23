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
- preserves the two reviewed owner-scoped public policies as exact contracts
  and fails closed on any new or renamed policy visible to
  `public`/`anon`/`authenticated`, including non-literal predicates;
- revokes `PUBLIC`, `anon`, and `authenticated` access to all seven currently
  exposed public views and grants explicit `service_role` read access;
- revokes existing `PUBLIC`, `anon`, and `authenticated` access to the 24
  currently exposed public sequences and grants explicit `service_role`
  `USAGE`, `SELECT`, and `UPDATE` access;
- removes those roles from future table, sequence, and function defaults
  created by `postgres` in `public`; the preflight refuses to proceed while
  Supabase-managed `supabase_admin` defaults remain broad;
- fails before DDL if the table, view, sequence, policy, owner, privilege, or
  public-RLS scope has drifted, including unexpected exposed sequences or
  permissive public/anon policies; it also fails unless `service_role` retains
  `rolbypassrls = true` and every expected policy remains `PERMISSIVE`.

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
3. `anon` and `authenticated` have no USAGE/SELECT/UPDATE privileges on any
   reviewed public sequence.
4. `service_role` retains `rolbypassrls = true`, SELECT/INSERT/UPDATE/DELETE
   on every lockdown table, and SELECT on every lockdown view.
5. `service_role` retains USAGE/SELECT/UPDATE on every reviewed public
   sequence.
6. Anonymous and authenticated Data API requests are denied for the
   fail-closed phase-1 scope, including the seven views.
7. The bot’s direct PostgreSQL lifecycle paths still read and write orders,
   client state, fills, signals, proof records, and migration history.
8. Every dashboard operation that must remain available has an explicit,
   reviewed owner predicate before an authenticated policy is added.
9. No public/anon/authenticated permissive policy remains, the exact retained
   owner predicates match the reviewed baseline, and no unreviewed public-role
   policy identity exists. Service-role-only policies require a separate owner
   review but do not expose the Data API.
10. An owner-authorized follow-up removes the remaining `supabase_admin`
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
or deploy until the staging assertions, the disposable anon/authenticated/
service-role access fixture, real role-based Data API tests, bot lifecycle
proof, dashboard owner mapping, and the negative policy-drift fixtures pass.
The migration, role-access, and failure-injection tests are part of the P0
workflow, but unrelated existing P0 failures still keep the PR on HOLD.
