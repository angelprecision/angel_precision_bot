# P0 — Repair EXIT_FILLED reconciler query parameterization and external-row fence

## Status

**SPEC-FIRST DRAFT. IMPLEMENTATION REQUIRED. DO NOT MERGE OR DEPLOY YET.**

Created from current `main` after merged PR #524.

Reference main SHA at discovery: `ba1e86a01ed9c81423cc6b5baaa766e43310bec7`.

This is a surgical reconciler reliability repair. It is independent from the August 27 entry-flow failure fixed by #524.

---

## Production problem

Render repeatedly emits:

```text
[ap.reconciler] ERROR: [client] _heal_exit_filled_positions_from_orders failed: tuple index out of range
```

The failure occurs inside `APReconciler._heal_exit_filled_positions_from_orders()`.

Current SQL includes a parameterized query plus this literal pattern:

```sql
AND COALESCE(o.local_order_id, '') NOT LIKE 'external-exit:%'
```

while the same statement also binds `%s` parameters through psycopg/psycopg2.

The bare `%` inside the SQL literal is interpreted by the DB-API formatting layer as formatting syntax. That produces the observed `tuple index out of range` before the intended query can complete.

This is not theoretical. It is continuously present in production logs.

### Historical comparison

The same faulty SQL exists in the Monday recovery baseline and current main. Therefore:

- this bug was **not introduced by #514 or #524**;
- it did **not cause the August 27 zero-entry morning**;
- it is still an active production reliability defect;
- it becomes more dangerous as tradeflow increases because it disables a backup path whose job is to heal filled EXIT truth into positions after restart/fill-monitor gaps.

---

## Secondary correctness issue in the same exact seam

The query currently selects:

```text
local_order_id
position_id
fill_price
filled_qty
filled_ts
broker_order_id
```

but the application-level defensive fence immediately below also reads:

```python
meta = row.get("meta")
external_marker = (
    isinstance(meta, dict)
    and str(meta.get("external_broker_order") or "").lower() == "true"
)
```

The SQL query does **not** select `o.meta`, so the application-level metadata fence cannot inspect the metadata for real query results.

The SQL-level predicate already excludes `external_broker_order=true`, so this omission is not the source of the current exception. But the code comment explicitly claims the application-level fence protects legacy rows or test doubles that do not apply the SQL predicate. Without `o.meta` in the selected shape, that second fence is incomplete.

Because both defects are in the same function, same query, and same external-row safety contract, they belong in one surgical PR.

---

## Required invariant

> The backup EXIT_FILLED healing query must execute safely through the real database driver and must never adopt an external/manual exit row.

For bot-owned EXIT rows:

```text
orders truth first
-> reconciler reads confirmed EXIT_FILLED row
-> linked bot position is incomplete
-> existing position finalization logic may heal it
```

For external/manual exit rows:

```text
external-exit:* local_order_id
OR meta.external_broker_order == true
-> this healing path must not adopt/finalize the row
-> manual-close reconciler retains authority
```

The fix must restore the existing backup-healing behavior. It must not create a new exit authority.

---

## Exact implementation seam

Primary production file:

- `ap_reconciler.py`

Primary function:

- `APReconciler._heal_exit_filled_positions_from_orders()`

Do not redesign the reconciler.

### Required SQL repair

**Preferred fix:** parameterize the LIKE pattern rather than relying on escaping a percent sign in SQL text.

Conceptually:

```sql
AND COALESCE(o.local_order_id, '') NOT LIKE %s
```

with parameters including:

```python
(self.client_id, "external-exit:%")
```

Use the exact parameter order required by the final SQL.

Parameterization is preferred over embedding `'external-exit:%%'` because it keeps SQL literal semantics independent from DB-API formatting rules and makes the pattern an explicit bound value.

If implementation chooses a different safe DB-driver-compatible form, tests must prove it against the real project database wrapper / PostgreSQL fixture, not only a MagicMock.

### Required selected-row repair

Add `o.meta` to the SELECT list/row shape so the existing second application-level fence can actually evaluate `external_broker_order` on returned rows.

Do not delete the SQL-level external-row filters merely because the application-level fence exists. Keep both layers:

1. SQL excludes known external rows before adoption;
2. application code defensively rechecks returned rows.

---

## Live behavior impact

This change affects a live reconciler worker.

It does **not** intentionally change which trading opportunities are eligible and it does **not** submit or cancel orders.

Expected before/after:

### Before

```text
reconciler loop
-> _heal_exit_filled_positions_from_orders()
-> SQL formatting failure
-> tuple index out of range
-> backup EXIT_FILLED healing skipped
-> worker continues and repeats failure later
```

### After

```text
reconciler loop
-> parameterized query executes
-> only bot-owned eligible EXIT_FILLED rows returned
-> application-level external-row fence rechecks local_order_id + metadata
-> existing position-finalization code runs only for eligible rows
```

---

## Hard scope exclusions

This PR must **not** change:

- entry scanners;
- overnight or intraday signal generation;
- watcher logic;
- confirmed breach logic;
- #512 direction ownership;
- contract selector behavior;
- moneyness / DTE / delta / spread / OI / volume thresholds;
- sizing;
- Master Control;
- max positions;
- exposure limits;
- broker submit;
- broker cancel;
- order placement or ladder pricing;
- order monitor semantics;
- fill monitor ownership;
- manual-close ownership policy;
- exit-decision policy;
- exit thresholds;
- `proof_trades` semantics;
- LIVE/PAPER classification;
- queue eligibility.

Do not add a new broker call.

Do not broaden the SQL to adopt rows currently excluded by bot/external ownership rules.

Do not change position P&L formulas in this PR.

Do not combine unrelated reconciler bugs.

---

## Mutation authority checklist

This function already participates in position healing after confirmed EXIT order truth.

The PR may restore that **existing** path but must not add new mutation authority.

Required proof:

- no new `submit_order` call;
- no new `cancel_order` call;
- no new order-state transition;
- no new `proof_trades` write;
- no new queue write;
- no change to `client_id` scoping;
- no cross-client read/adoption;
- no LIVE/PAPER taxonomy mutation.

The query must remain scoped to `o.client_id = self.client_id` and position join `p.client_id = o.client_id`.

---

## Required regression tests

Add a dedicated file, suggested:

`tests/test_p0_reconciler_exit_filled_query_parameterization.py`

Register it in `.github/workflows/p0_regression.yml` if the reconciler regression suite is part of P0 workflow registration conventions.

At minimum prove all of the following.

### A. Real PostgreSQL/DB-wrapper execution does not raise formatting exception

Execute the actual query through the project's test database path or a driver-faithful PostgreSQL fixture.

Assert:

- no `IndexError` / `tuple index out of range`;
- pattern binding succeeds;
- query returns normally.

A MagicMock-only test is insufficient for this bug because the defect is specifically in DB-API parameter interpolation.

### B. Bot-owned EXIT_FILLED row is eligible

Insert/build a row with:

```text
kind = EXIT
status = EXIT_FILLED
local_order_id = normal bot id
meta.external_broker_order absent/false
fill_price non-null
filled_qty > 0
linked position avg_fill > 0
linked position missing one or more finalization fields
same client_id
```

Assert the row survives query + application fence and reaches existing finalization logic.

### C. `external-exit:%` prefix is excluded

Row:

```text
local_order_id = external-exit:<anything>
```

Assert:

- not adopted by backup healing;
- position finalization is not invoked for that row.

### D. metadata external broker row is excluded

Row:

```json
{"external_broker_order": true}
```

with a non-prefix local order id.

Assert SQL/application fence excludes it.

### E. application-level metadata fence actually receives `meta`

Test returned row shape includes `meta` and that `external_broker_order` is evaluated.

This locks the SELECT/fence contract and prevents a future refactor from silently dropping the column again.

### F. wrong client cannot be adopted

Create same-looking EXIT_FILLED row for another client.

Assert zero adoption/finalization.

### G. non-EXIT row excluded

ENTRY FILLED row must never be consumed by this method.

### H. EXIT but wrong status excluded

Examples:

```text
SUBMITTED
PARTIAL
CANCELED
EXPIRED
```

must not be healed as EXIT_FILLED.

### I. zero/blank fill truth excluded

Null fill price or `filled_qty <= 0` must remain excluded.

### J. already-complete position excluded

If `exit_price`, `realized_pnl`, `realized_pnl_pct`, and `quantity_remaining` are all already populated, backup healing should not re-finalize it.

### K. client scoping is preserved in both orders and positions join

Prove a matching order cannot attach to a position belonging to another client.

### L. repeated run is idempotent

Once the existing finalizer completes the position, a subsequent reconciler pass must not repeat the same finalization through this query.

---

## Required production-shape test

Include a fixture shaped like the real `orders`/`positions` rows used by this query, not a reduced toy dictionary that omits:

- `local_order_id`;
- `position_id`;
- `fill_price`;
- `filled_qty`;
- `filled_ts`;
- `broker_order_id`;
- `meta`;
- `client_id` association;
- linked position finalization fields.

The test should fail on current main with the actual formatting error or an equivalent driver-faithful reproduction, then pass with the fix.

---

## Required diagnostics

Preserve the existing error logging contract for truly unexpected exceptions.

After the fix, normal healthy cycles must stop emitting:

```text
_heal_exit_filled_positions_from_orders failed: tuple index out of range
```

Do not suppress the exception with a broader `except` as the "fix."

Do not downgrade the error while leaving the query broken.

The SQL must actually execute.

---

## Required verification before review-ready

Codex must run and report:

1. the new targeted regression file;
2. existing reconciler tests;
3. existing external/manual close reconciliation tests;
4. existing position finalization tests touched by this path;
5. existing fill-monitor/reconciler ownership regressions relevant to EXIT_FILLED;
6. full P0 Regression Suite on the **exact final head SHA**.

If the full suite is too broad for local execution, exact-head GitHub CI is mandatory before review-ready.

---

## Required Codex completion report

Before marking review-ready, post:

1. exact final head SHA;
2. exact changed production files;
3. exact changed tests/workflow files;
4. exact old SQL and new SQL;
5. exact final bound parameter tuple/list order;
6. proof `o.meta` is now selected and application fence uses it;
7. real-driver or PostgreSQL regression output proving the tuple-index failure is gone;
8. bot-owned positive-control result;
9. external-prefix negative-control result;
10. external-metadata negative-control result;
11. cross-client negative-control result;
12. idempotency result;
13. confirmation no broker submit/cancel path changed;
14. confirmation no entry eligibility changed;
15. confirmation no `proof_trades` / queue semantics changed;
16. `git diff main...HEAD --stat`;
17. concise adversarial diff audit.

---

## Merge gate

**HARD HOLD** until implementation and exact-head tests prove:

- the actual database path no longer throws `tuple index out of range`;
- bot-owned eligible EXIT_FILLED rows can again reach existing healing;
- external/manual exit rows remain excluded by both SQL and application fences;
- `client_id` ownership remains exact;
- no broker submit/cancel or entry behavior changes;
- no new position mutation authority is created beyond the existing healing path.

Expected final verdict after successful implementation and review: **MERGE**.
