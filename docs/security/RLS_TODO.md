# Supabase RLS — TODO

Status: **REVIEW DRAFT — PHASE 1 NOT APPLIED.** The exact live 22-table
baseline is drafted in `migrations/20260823_rls_public_surface_lockdown.sql`.
See `docs/security/RLS_ROLLOUT_PLAN.md`. The dashboard auth model and the
authenticated tenant policies remain intentionally unresolved.

## Tables that need Row-Level Security

The post-audit recommendation calls for RLS on these four tables (all
Supabase-managed):

| Table | Why | Owner column |
|---|---|---|
| `proof_trades` | Per-client proof artifacts — must not leak across tenants. | `client_email` or `email` |
| `ap_signals` | Per-client signal log — visible only to the owning client. | `client_email` or `client_id` |
| `members` | Authenticated user list — read-self only. | `email` |
| `client_risk_profiles` | Risk caps, position size policy, kill-switch state. | `client_id` |

## Why we are not pasting RLS SQL in this PR

1. **Owner-column mapping is not finalized.** Several of these tables use
   `client_email` while others use `client_id`. The mapping between
   Supabase Auth `auth.uid()` / `auth.email()` and these columns must be
   confirmed against the live schema before policies are written.
2. **Service-role writes must remain unaffected.** The bot writes to
   these tables via `SUPABASE_SERVICE_KEY`. Policies must explicitly
   allow `auth.role() = 'service_role'` or all bot writes break.
3. **Anon reads need explicit scoping.** The current default in some
   Supabase projects allows public anon reads on tables not yet locked
   down. RLS must be enabled AND a deny-by-default policy added — RLS
   without policies leaves the table effectively closed only if RLS is
   in `enforce` mode.
4. **Untested RLS SQL breaks the dashboard.** Policies that look
   superficially correct can lock out the dashboard backend (which signs
   in as a particular role) if the role/claim mapping is off by one.

## Acceptance criteria for the follow-up PR

When the follow-up RLS PR is written, it must satisfy all of:

- [ ] Owner-column verified against the live schema (`\d <table>` output
      attached to the PR).
- [ ] `auth.role() = 'service_role'` explicitly allowed on all four
      tables (bypass policy or wide-open service-role policy).
- [ ] `auth.email()` (or `auth.uid()`) compared against the owner column
      in the SELECT/UPDATE/DELETE policies.
- [ ] Anonymous (`anon`) reads explicitly denied.
- [ ] Tested against a staging Supabase project (NOT prod) with at least:
      - The dashboard backend signed in as a real authenticated user can
        SELECT its own rows and CANNOT select another user's rows.
      - The bot using `SUPABASE_SERVICE_KEY` can still INSERT / UPDATE /
        DELETE freely.
      - Anonymous Supabase client returns 0 rows / 401.
- [ ] Reversible: a `DROP POLICY` companion is provided in the PR body
      so policies can be backed out fast if the dashboard misbehaves.

## Suggested policy shapes (illustrative — DO NOT apply blindly)

These are **starting points**, not production SQL. Verify against the live
schema and the dashboard auth model first.

```sql
-- proof_trades: per-client read access by authenticated email
ALTER TABLE proof_trades ENABLE ROW LEVEL SECURITY;

CREATE POLICY proof_trades_select_own
  ON proof_trades
  FOR SELECT
  USING ( auth.email() = client_email );

CREATE POLICY proof_trades_service_role_all
  ON proof_trades
  FOR ALL
  USING ( auth.role() = 'service_role' )
  WITH CHECK ( auth.role() = 'service_role' );
```

The same shape with the right owner column applies to `ap_signals`,
`members`, and `client_risk_profiles`. The exact column names must be
confirmed before SQL is run.

## Out of scope for the credential/auth/client-scope hardening PR

This file is the only RLS-related change in this PR. The actual policy
SQL will live in a follow-up PR once the dashboard auth model is locked.
No destructive migrations are added here.
