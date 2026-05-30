# Supabase SQL editor — paste-ready queries

Every file in this folder is **paste-and-run** in the Supabase SQL editor.
No `%s` placeholders, no parameter substitution, no clever CTEs that fight
with Supabase's auto-LIMIT wrapping. If you change a window, change the
literal value inline.

The Flask endpoints (`/admin/operator/*`) use a separate parameterised
version of the same SQL — never paste those into the Supabase editor,
the `%s` is a psycopg placeholder and PostgreSQL will reject it.

| File | What it shows | When to use |
| --- | --- | --- |
| `01_ghost_orders.sql` | ENTRY orders stuck in PENDING_TRIGGER with no broker id | Find stranded orders to investigate |
| `02_ghost_orders_by_client.sql` | Same, one client only | Compare main vs jason vs jose |
| `03_ghost_orders_summary.sql` | Per-client ghost counts + reserved capital | Quick triage of who has the most stuck orders |
| `04_signal_ledger_recent.sql` | Per-client-per-signal ledger, last N hours | What happened on each account for each signal |
| `05_signal_ledger_mismatches.sql` | Only signals where clients diverged | Find fairness/fan-out issues |
| `06_signal_ledger_one_signal.sql` | Full drill-down on one canonical_signal_id | Investigate a specific suspect signal |
| `07_schema_check.sql` | `information_schema.columns` for `orders` + `ap_signals` | **Run BEFORE applying the ledger view** |
| `08_signal_ledger_bucket_counts.sql` | Per-bucket counts over a window | Dashboard parity check |

## Quick start

1. Run `07_schema_check.sql` and confirm the columns the ledger view assumes.
2. Apply `sql/views/ap_multi_account_signal_ledger.sql` (it creates the view used by 04–06 + 08).
3. Run `01_ghost_orders.sql` to see what's stuck.
4. Run `05_signal_ledger_mismatches.sql` to see where clients diverged.
5. Pick a signal from #5 and feed it into `06_signal_ledger_one_signal.sql`.

## Why two SQL versions exist

The Flask endpoint executes via psycopg, which substitutes `%s` placeholders
with real values at execution time. Pasting that SQL directly into the
Supabase editor fails because PostgreSQL sees the literal `%` character.

The files here use inline literal values (`INTERVAL '24 hours'`,
`'tradefluencehq@gmail.com'`, etc.) so they run as-is.
