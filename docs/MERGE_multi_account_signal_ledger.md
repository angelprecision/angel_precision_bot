# MERGE FILE — Multi-Account Signal Ledger (Item 4)

**Status:** PR for review. READ-ONLY visibility only.
**Branch:** `feat/multi-account-signal-ledger`
**Files:** `sql/views/ap_multi_account_signal_ledger.sql`, `app.py` (one new GET
endpoint), new `tests/test_signal_ledger_reeval.py`.

**Did NOT touch:** exits, sizing, contract selection, retry, broker execution,
watcher rearm, proof logging. No order mutation, no cancel, no cleanup.

## What it does
One row per (canonical signal, client/account, ENTRY order) so you can answer,
for a single signal: what happened on main vs Jason vs Jose — never evaluated,
PENDING_TRIGGER with no broker id, watcher-invalidated, submitted, filled,
canceled, or different qty/contract per client.

## The REEVAL join (the critical detail)
`orders.signal_id` may be wrapped `REEVAL:<uuid>:<hex>` while `ap_signals.signal_id`
is the bare UUID. The view normalizes with
`split_part(o.signal_id, ':', 2)` before the LEFT JOIN, so re-evaluation orders
attach to their original signal. This matches the codebase's existing
`canonical_signal_id()` helper exactly (proven in the test).

## SQL view
`sql/views/ap_multi_account_signal_ledger.sql` — `CREATE OR REPLACE VIEW
ap_multi_account_signal_ledger`. Exposes order + signal fields, watcher_audit
reason/quote fields, selector_candidate_audit (item 3), the new quote-domain
fields (mode / paper_fill_mode / quote_domain_mismatch_possible), and a
`ledger_bucket` classifier:
NO_ORDER_FOR_CLIENT / PENDING_TRIGGER_NO_BROKER / BROKER_SUBMITTED /
FILLED_OR_PARTIAL / TERMINAL_NO_FILL / OTHER.

### Schema verification (do not guess — run first)
The file's STEP 0 has the `information_schema.columns` queries. Columns were
verified from code (orders INSERT + transition UPDATE; ap_signals upsert in
ap_signal_store.py). The one assumption is `ap_signals.created_at` (Supabase
default timestamp) — verify and adjust if your table uses another name.

## Backend endpoint
`GET /admin/operator/signal-ledger` (bot app.py, `@require_hmac`, read-only).
Params: client_id, symbol, canonical_signal_id, status, bucket,
since_hours (default 24, max 720), limit (default 500, max 2000). All filters
parameterized — no string interpolation of user input.

Returns `{ok, rows, summary{total_rows, clients_seen, signals_seen,
pending_trigger_no_broker, broker_submitted, filled_or_partial,
terminal_no_fill, no_order_for_client}}`.

## Tests
`tests/test_signal_ledger_reeval.py` — 7 tests: REEVAL strip, bare passthrough,
agreement with `canonical_signal_id()`, REEVAL==bare grouping, split_part index,
view-file read-only + join-shape assertions.

## Acceptance criteria
1. REEVAL order joins to original signal — proven by test + view expression. ✅
2. Endpoint shows all clients for a canonical signal (group by canonical_signal_id). ✅
3. PENDING_TRIGGER + null broker → PENDING_TRIGGER_NO_BROKER bucket. ✅
4. watcher_audit reason_code / raw_reason exposed. ✅
5. selector_candidate_audit exposed. ✅
6. Read-only (SELECT only; @require_hmac). ✅
7. SQL view file committed. ✅
8. REEVAL normalization test included. ✅

## Deploy
1. Run the view SQL in Supabase (after the STEP 0 column check).
2. Deploy the bot (endpoint ships with it).
3. Query `GET /admin/operator/signal-ledger?since_hours=24`.
No env changes. View can be dropped with `DROP VIEW ap_multi_account_signal_ledger;`
with zero data impact.
