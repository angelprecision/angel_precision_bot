# Post-deploy validation queries (Phase 8)

After deploying Phases 2–7 to production, run the queries in this directory
to confirm the live system is behaving as the audit specified. Every query
is **read-only** (SELECT only — no INSERT / UPDATE / DELETE / ALTER / DROP).

No new migrations are required for Phase 8. The Phase 2 migrations
(`20260519_phase2_orders_meta.sql`, `20260519_phase2_lift_caps.sql`) created
the `orders.meta` JSONB column the Phase 3–6 telemetry uses. All Phase 2–7
work is code-only.

## How to run

Connect with your existing read-only DB role (Supabase → SQL Editor, or
`psql $DATABASE_URL_READONLY`):

```bash
psql $DATABASE_URL_READONLY -f sql/validation/<filename>.sql
```

Each `.sql` file is self-contained and ends with a one-line `\\echo` showing
what it just printed.

## What each query proves

| File | Validates |
|---|---|
| `01_phase2_adaptive_autocancel.sql` | Adaptive autocancel is in production: `ENTRY_REEVAL` events appear, ceilings at 90s (normal) / 120s (A+) fire. |
| `02_phase3_submit_time_refresh.sql` | Submit-time ask refresh telemetry (`selector_ask` / `submit_ask` / `quote_age_ms`) is populating, and `RUNAWAY_QUOTE_AT_SUBMIT` blocks are working. |
| `03_phase4_account_equity_sizing.sql` | Position sizing uses `account_equity * POSITION_RISK_PCT`; `final_qty` matches expectation; no `MAX_TRADE_USD_CAP` hits unless intentional. |
| `04_phase5_post_cancel_retry.sql` | Post-cancel retry events show ARMED → SUBMITTED → (FILLED or ABORTED) progression; no infinite retry loops; max 2 attempts honored. |
| `05_phase6_dashboard_telemetry.sql` | The 23-field telemetry projection is exposable for every entry; no gaps in core fields. |
| `06_phase7_exit_pricing.sql` | Exit prices come from option contract quotes, fill prices are sane, and partial exits do not leave the position unfunded. |
| `07_rls_hardening.sql` | Read-only catalog proof for the 22-table RLS/grant lockdown, privileged-path preservation, public views, permissive policies, and remaining default privileges. |
| `99_health_summary.sql` | One-pager: entry funnel by reason_bucket, average fill latency, sizing distribution, retry success rate. |

## Safety guarantees

All queries:
- Use only `SELECT` (verified by grep below)
- Set explicit `LIMIT` clauses where row counts could be large
- The Phase 2–7 queries read from `orders`, `positions`, `audit_log`, and
  `decision_events` only
- `07_rls_hardening.sql` reads PostgreSQL catalog metadata and privileges only;
  it does not select application rows from `client_state`, `clients`, or any
  secret-bearing table

To audit the queries before running:

```bash
# Match only WRITE/DDL keywords used as SQL statements (word-boundary + start of
# line or after whitespace), to avoid false positives on string literals.
grep -rEni '(^|[^A-Za-z_])(INSERT|UPDATE|DELETE|ALTER|DROP|TRUNCATE|GRANT|REVOKE)[ \t]+(INTO|FROM|TABLE|INDEX|VIEW|ROLE|SCHEMA|DATABASE)' \
  sql/validation/ | grep -v '\.md:'
# should print nothing
```

Also confirm `CREATE TABLE` and `CREATE INDEX` are absent:

```bash
grep -rEni 'CREATE[ \t]+(TABLE|INDEX|VIEW|ROLE|SCHEMA|DATABASE|FUNCTION)' \
  sql/validation/ | grep -v '\.md:'
# should print nothing
```
