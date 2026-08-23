# Supabase RLS — TODO

Status: **REVIEW DRAFT — PHASE 1 NOT APPLIED.** PR #499 contains a
fail-closed public-surface lockdown for the exact live findings. It is not a
production authorization or a replacement for the tenant-policy follow-up.

## Phase 1 scope in PR #499

- 22 public tables with RLS disabled: RLS enabled, public/anon/authenticated
  table privileges revoked, and service-role CRUD made explicit.
- 17 additional RLS-enabled tables with live-verified permissive public/anon
  policies: public/anon/authenticated table access revoked and the exact
  unsafe policies removed.
- 7 public security-definer views: public/anon/authenticated access revoked;
  service-role read access retained explicitly.
- 24 existing public sequences with anon/authenticated privileges: access
  revoked and service-role sequence privileges retained explicitly.
- Future `postgres`-owned public table, sequence, and function defaults no
  longer grant PUBLIC/anon/authenticated access.

## Intentionally unresolved

No authenticated tenant policies are guessed in this phase. Before any
dashboard access is restored, verify the live ownership bridge for each
operation (`auth.uid()`/application identity to `client_id` or
`client_email`) and add explicit `TO authenticated` policies with both
`USING` and `WITH CHECK` where writes are required. Service-role and direct
PostgreSQL paths remain privileged paths and do not need a public RLS policy.

The migration preflight refuses to proceed while the `supabase_admin` default
privileges remain broad. They require owner-authorized follow-up because the
application `postgres` role is not a member of `supabase_admin`.

## Acceptance criteria for the follow-up policy PR

- [ ] Owner columns and identity mapping are verified against the live schema.
- [ ] Dashboard reads are limited to the authenticated owner, not merely the
      authenticated role.
- [ ] Dashboard writes have both owner `USING` and `WITH CHECK` predicates.
- [ ] Anonymous reads and writes remain denied.
- [ ] Anonymous and authenticated roles have no access to public sequences;
      the bot service role retains the required sequence privileges and
      `rolbypassrls = true`.
- [ ] The reviewed policy rows match the expected roles, command, predicates,
      and `PERMISSIVE` mode before any policy is dropped; retained public-role
      owner policies are exact-contract checked and unknown policy identities
      fail closed.
- [ ] Public views are either kept privileged-only or converted to tested
      `security_invoker` views with owner-scoped base policies.
- [ ] Staging proves dashboard own-row access, cross-tenant denial, bot
      service-role CRUD, and anonymous Data API denial.
- [ ] The `supabase_admin` default privilege owner authorizes the follow-up.

No production SQL should be applied until the rollout plan and validation
assertions pass on isolated staging.
