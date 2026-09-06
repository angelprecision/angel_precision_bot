# P0 FOLLOW-UP TO PR #585 — PRESERVE CANONICAL ENTRY QUANTITY DURING BROKER RECOVERY

## STATUS

**DRAFT / DO NOT MERGE YET.**

This work is intentionally sequenced **after PR #546 is complete and committed to `main`**.

Before implementation begins:

1. Finish and merge/reconcile #546.
2. Rebase this branch onto the resulting committed `main`.
3. Re-audit the rebased diff before adding production changes.
4. Keep this PR limited to the quantity-authority defect described below.

This is a narrow follow-up to merged PR #585. It is **not** authorization to redesign the exit engine, entry policy, scanner, risk, sizing, Master Control, proof taxonomy, queue, broker adapter, or strategy behavior.

---

# WHY THIS PR EXISTS

PR #585 materially improved broker-open exit ownership recovery, canonical/degraded-owner convergence, pending EXIT identity hydration, exact account/mode/OCC fencing, and fail-closed broker-truth handling.

The merged recovery path still has one quantity-authority defect:

- a canonical ENTRY order may prove that the original position filled `N` contracts;
- fresh exact broker truth may prove that only `R` contracts remain open now, where `0 < R < N`;
- the current recovery code can persist/reconstruct both full quantity and remaining quantity as `R`.

Example:

```text
Canonical ENTRY filled_qty = 2
Current exact broker open qty = 1
Canonical positions row = missing
```

Current incorrect reconstruction can become:

```text
qty = 1
quantity_remaining = 1
status = OPEN
```

That silently erases the already-exited contract and changes the meaning of canonical position state.

The required state is:

```text
qty = 2
quantity_remaining = 1
status = PARTIAL
```

The repository already uses these semantics:

- `qty` = original/canonical position quantity;
- `quantity_remaining` = currently open quantity after any partial reductions.

Broker truth is authoritative for **current open exposure**. It is not automatically authoritative for **original entry size**.

---

# PRIMARY INVARIANT

For a broker-recovered position with proven canonical ENTRY identity:

```text
full_entry_qty      = exact durable ENTRY filled quantity
broker_remaining_qty = fresh exact broker open quantity
```

The only admissible positive canonical relationship is:

```text
0 < broker_remaining_qty <= full_entry_qty
```

Then:

```text
position.qty                = full_entry_qty
position.quantity_remaining = broker_remaining_qty
```

Never collapse `position.qty` to `broker_remaining_qty` merely because broker truth reports the current remainder.

If:

```text
broker_remaining_qty > full_entry_qty
```

that is contradictory authority. Fail closed. Do not manufacture a larger canonical position.

---

# REQUIRED PRODUCTION CHANGES

## CHANGE 1 — `_upsert_broker_position_to_db()` MUST SEPARATE FULL ENTRY QUANTITY FROM CURRENT BROKER REMAINDER

**File:** `ap_exit_engine.py`

**Function:** `_upsert_broker_position_to_db(...)`

### Current failure

The function currently derives a positive `qty` from the broker position and uses that value for both:

```text
qty
quantity_remaining
```

The function already resolves an exact filled ENTRY order before canonical insertion. That ENTRY order is the correct authority for original position quantity.

### Required behavior

After exact filled ENTRY resolution, derive two independent values:

```python
full_entry_qty = positive_int(exact_entry_order["filled_qty"])
broker_remaining_qty = positive_int(broker_position["quantity"])
```

Do not reuse one variable for both meanings.

### Required validation

Before any canonical `positions` INSERT:

1. `full_entry_qty` must be a strictly positive integral quantity.
2. `broker_remaining_qty` must be a strictly positive integral quantity.
3. `broker_remaining_qty <= full_entry_qty` must hold.
4. client identity must remain exact.
5. execution mode must remain exact (`live` or `paper`, preserving existing mode fences).
6. OCC contract identity must remain exact.
7. the exact ENTRY candidate must remain the same candidate already proven by #585 logic.

If any quantity validation fails:

```text
zero positions INSERT
zero broker submit
zero broker cancel
zero proof_trades mutation
zero queue mutation
```

Return/propagate a visible recovery failure classification. Do not silently coerce.

### Required INSERT shape

If:

```text
full_entry_qty == broker_remaining_qty
```

insert/reconstruct:

```text
qty = full_entry_qty
quantity_remaining = broker_remaining_qty
status = OPEN
```

If:

```text
0 < broker_remaining_qty < full_entry_qty
```

insert/reconstruct:

```text
qty = full_entry_qty
quantity_remaining = broker_remaining_qty
status = PARTIAL
```

Do not write `OPEN` for a newly reconstructed row whose exact current remainder is already smaller than the proven original fill quantity.

### Extended-schema and fallback INSERT parity

`_upsert_broker_position_to_db()` currently has an extended-schema INSERT and a minimal/fallback INSERT used after `UndefinedColumn` handling.

Both INSERT variants must use the **same quantity semantics**:

```text
qty                -> full_entry_qty
quantity_remaining -> broker_remaining_qty
status              -> derived OPEN/PARTIAL status
```

Do not fix only the extended INSERT and leave the fallback path writing `broker_qty, broker_qty`.

The savepoint / rollback-to-savepoint / release-savepoint behavior added for real PostgreSQL aborted-transaction semantics must remain intact.

### Existing-row conflict path

If `ON CONFLICT DO NOTHING` resolves to an already-existing canonical row:

- do not overwrite original `qty` with broker remainder;
- preserve exact client/mode/OCC selection;
- return the canonical row identity only when the existing-row lookup is unambiguous;
- if multiple active rows remain possible, preserve the current fail-closed ambiguity behavior.

---

## CHANGE 2 — EXISTING CANONICAL DB ROW REPAIR MUST NOT COLLAPSE `qty`

**File:** `ap_exit_engine.py`

**Path:** `_broker_position_precheck()` existing-row reconciliation

### Required behavior

When an existing canonical DB row is found and fresh exact broker truth reports the current open quantity:

- broker truth may repair `quantity_remaining`;
- broker truth must **not** rewrite/collapse canonical original `qty` merely because fewer contracts remain at the broker.

Example:

```text
DB qty = 2
DB quantity_remaining = 0   # stale
Broker exact qty = 1
```

Correct repair:

```text
DB qty = 2
DB quantity_remaining = 1
```

not:

```text
DB qty = 1
DB quantity_remaining = 1
```

If the durable row itself has contradictory quantity authority such as:

```text
qty <= 0
quantity_remaining > 0
```

or:

```text
quantity_remaining > qty
```

fail closed unless a separate already-existing canonical authority proves the correct original quantity. Do not guess.

---

## CHANGE 3 — `_managed_position_from_row(... prefer_qty_override=True)` MUST PRESERVE FULL QUANTITY

**File:** `ap_exit_engine.py`

**Function:** `_managed_position_from_row(...)`

### Current failure

In broker-truth mode, the current function can force:

```python
mp.quantity = qty_override
mp.quantity_remaining = qty_override
```

where `qty_override` is current broker open quantity.

That makes runtime state lose original position size whenever the position has already been partially reduced.

### Required behavior

Split runtime quantity authority exactly as durable state does.

For a canonical DB row:

```text
runtime quantity           = canonical full/original qty from durable row
runtime quantity_remaining = fresh exact broker remainder
```

Example:

```text
row.qty = 2
row.quantity_remaining = 1
broker qty override = 1
```

must produce:

```text
ManagedPosition.quantity = 2
ManagedPosition.quantity_remaining = 1
```

### Required invariant

After construction:

```text
0 <= quantity_remaining <= quantity
```

For broker-open recovery specifically:

```text
0 < quantity_remaining <= quantity
```

If the broker remainder exceeds proven full quantity, quarantine/fail closed rather than expanding `quantity` to make the invariant pass.

### Degraded owners

Do **not** broaden this PR into a redesign of degraded-owner quantity semantics.

A degraded owner that has no proven canonical full ENTRY identity may continue representing only broker-open exposure as allowed by #585. The change in this PR is specifically:

> once canonical full ENTRY quantity is proven, convergence/reconstruction must preserve that full quantity and use broker truth only for the remaining quantity.

---

## CHANGE 4 — CANONICAL ADOPTION / DEGRADED-TO-CANONICAL CONVERGENCE MUST PRESERVE THE SAME QUANTITY SEMANTICS

**File:** `ap_exit_engine.py`

**Paths:** canonical adoption and degraded-owner convergence introduced/hardened by #585

When a degraded broker owner converges to a canonical owner:

- canonical `position_id` remains authoritative;
- canonical ENTRY geometry remains authoritative;
- canonical full quantity comes from proven durable ENTRY/position state;
- current remaining quantity comes from fresh exact broker truth;
- runtime exit evidence may transfer under the existing #585 allowlist;
- pending EXIT identity/watermarks must remain preserved under existing rules;
- do not copy degraded broker remainder into canonical full quantity.

Required post-convergence example:

```text
ENTRY filled_qty = 2
broker remaining = 1
degraded runtime owner represented broker qty 1
canonical owner appears
```

must converge to:

```text
canonical.quantity = 2
canonical.quantity_remaining = 1
single active owner
```

not:

```text
canonical.quantity = 1
canonical.quantity_remaining = 1
```

---

# REQUIRED STATUS SEMANTICS

For a newly reconstructed canonical row:

| Proven full ENTRY qty | Exact broker remaining | Required result |
|---:|---:|---|
| 2 | 2 | `OPEN`, `qty=2`, `remaining=2` |
| 2 | 1 | `PARTIAL`, `qty=2`, `remaining=1` |
| 1 | 1 | `OPEN`, `qty=1`, `remaining=1` |
| 1 | 2 | **FAIL CLOSED** |
| 2 | 0 | existing broker-flat/unresolved safety path; **do not insert as open recovery** |
| 2 | malformed | **FAIL CLOSED** |
| missing | 1 | no canonical INSERT from guessed entry quantity; preserve #585 degraded behavior |
| ambiguous ENTRY | 1 | no canonical INSERT; preserve ambiguity HOLD/degraded behavior |

Do not infer a completed partial fill record, exit price, realized P&L, or proof trade merely from `full_entry_qty > broker_remaining_qty`.

The quantity difference proves current exposure is smaller than original entry exposure. It does **not** prove the economics or identity of the missing exit fill.

Therefore this PR must not fabricate:

- `exit_price`;
- realized P&L;
- realized P&L percentage;
- proof-trade economics;
- missing EXIT order ids;
- fill timestamps;
- scale-out reason;
- strategy reason.

---

# MONEY-PATH SAFETY REQUIREMENTS

This PR is quantity-authority repair. It must not create new broker authority.

For every fail-closed path introduced here:

```text
broker submit count = 0
broker cancel count = 0
broker replace count = 0
position terminalization = 0
proof_trades writes = 0
queue writes = 0
```

For a valid recovered position that later independently reaches the existing exit policy:

- the existing #585 submit-safety seam remains authoritative;
- exact fresh broker quantity must still be checked at submit time;
- existing duplicate-submit fences remain intact;
- this PR must not add a second canonical EXIT submission path.

---

# EXECUTION MODE / CLIENT / ACCOUNT / OCC REQUIREMENTS

Preserve all #585 authority fences.

The quantity fix must never weaken:

- `client_id` equality;
- execution-mode equality;
- LIVE identity preservation;
- PAPER identity preservation;
- configured Tradier account identity (`broker.cfg.account_id` with existing supported fallbacks);
- exact OCC contract identity;
- ambiguous active-row HOLD behavior;
- malformed broker payload fail-closed behavior;
- pending EXIT hydration FOUND/NONE/UNAVAILABLE/AMBIGUOUS behavior.

A PAPER row must not become LIVE because the broker transport is production-shaped.
A LIVE row must not lose LIVE identity during recovery.

---

# FAILURE-TIMING MATRIX TO TEST

The implementation must be exercised across these boundaries where applicable:

1. exact ENTRY proven, before broker truth read;
2. broker truth read succeeds, before canonical INSERT;
3. extended INSERT fails with `UndefinedColumn`;
4. rollback to savepoint occurs;
5. fallback INSERT executes;
6. process dies after canonical INSERT but before engine owner installation;
7. restart reads the inserted PARTIAL row;
8. broker precheck reconciles it again;
9. degraded owner exists before canonical row appears;
10. canonical adoption occurs after partial broker reduction;
11. active EXIT identity already exists during restart;
12. broker truth becomes unavailable during a later cycle.

At every restart boundary, the same authority relationship must survive:

```text
full quantity = proven original entry quantity
remaining quantity = exact current broker exposure
```

---

# REQUIRED ADVERSARIAL BEHAVIORAL TESTS

Source-string assertions are secondary. Add executed behavioral tests.

## Test 1 — full position recovery

```text
ENTRY filled_qty = 2
broker quantity = 2
positions row missing
```

Assert durable row:

```text
qty = 2
quantity_remaining = 2
status = OPEN
```

Assert runtime owner:

```text
quantity = 2
quantity_remaining = 2
```

---

## Test 2 — partial position recovery: PRIMARY REGRESSION TEST

```text
ENTRY filled_qty = 2
broker quantity = 1
positions row missing
```

Assert durable row:

```text
qty = 2
quantity_remaining = 1
status = PARTIAL
```

Assert runtime owner:

```text
quantity = 2
quantity_remaining = 1
```

Assert:

```text
zero fabricated realized P&L
zero fabricated exit_price
zero proof_trades write
```

---

## Test 3 — restart parity after partial recovery

Start from the durable result of Test 2.

Construct a new engine/process.

Assert after restart:

```text
quantity = 2
quantity_remaining = 1
single canonical owner
```

No collapse to `1/1` is allowed.

---

## Test 4 — existing stale row repaired by broker remainder only

```text
DB qty = 2
DB quantity_remaining = 0
broker exact quantity = 1
```

Assert:

```text
DB qty remains 2
DB quantity_remaining becomes 1
runtime quantity = 2
runtime remaining = 1
```

---

## Test 5 — broker quantity greater than proven ENTRY quantity

```text
ENTRY filled_qty = 1
broker quantity = 2
```

Assert:

```text
zero canonical positions INSERT
zero broker submit
zero broker cancel
zero proof_trades write
visible contradiction diagnostic
```

Do not "repair" full quantity upward to 2.

---

## Test 6 — active EXIT already exists on partial recovered position

```text
ENTRY full qty = 2
broker remaining = 1
active EXIT already persisted for remaining exposure
process restarts
```

Assert:

```text
quantity = 2
remaining = 1
pending EXIT identity rehydrated
zero duplicate submit
zero cancel
```

Preserve #585 pending EXIT restart semantics.

---

## Test 7 — degraded-to-canonical convergence after partial reduction

Process A:

```text
degraded owner installed from broker remaining = 1
canonical full entry not yet available
```

Later canonical authority appears:

```text
ENTRY filled_qty = 2
broker remaining = 1
```

Assert convergence:

```text
one owner
canonical id
quantity = 2
remaining = 1
broker_repair_degraded = false
runtime exit evidence preserved
pending EXIT watermarks preserved
```

---

## Test 8 — malformed quantity inputs

Exercise at least:

```text
None
""
"   "
0
-1
1.5
"1.5"
NaN
Infinity
boolean
```

for both ENTRY quantity and broker remaining quantity where structurally possible.

Assert fail closed with zero money-path mutation.

---

## Test 9 — execution-mode contradiction

```text
ENTRY = LIVE
recovery row / candidate = PAPER
```

and inverse.

Assert no canonical adoption/insert.

---

## Test 10 — client/OCC contradiction

Wrong client or wrong OCC exact contract must contribute zero authority.

Assert no canonical mutation.

---

## Test 11 — PostgreSQL-backed INSERT semantics

Use the existing PostgreSQL-backed fixture used by broker-repair tests.

Exercise the real `_upsert_broker_position_to_db()` path with:

```text
ENTRY filled_qty = 2
broker qty = 1
```

Read the actual persisted row and assert:

```text
qty == 2
quantity_remaining == 1
status == PARTIAL
```

This test is mandatory. A mock-only assertion is insufficient for the production INSERT defect this PR fixes.

---

## Test 12 — fallback INSERT parity under missing extended columns

Cause the extended INSERT to raise the same `UndefinedColumn` class used by the existing PostgreSQL-aborted-transaction regression.

Assert fallback row is still:

```text
qty = full_entry_qty
quantity_remaining = broker_remaining_qty
status = derived OPEN/PARTIAL
```

Do not allow fallback to regress to `broker_remaining/broker_remaining`.

---

# TEST REVIEW REQUIREMENTS

Before approval, inspect tests for false confidence:

- no source-string-only proof for the primary quantity behavior;
- no mocks that bypass the actual INSERT parameter mapping;
- no callback that is never reached;
- no monkeypatch that disables broker safety guards globally;
- restart test must instantiate a fresh engine rather than reuse the same in-memory object;
- PostgreSQL test must read back the real row;
- partial case (`ENTRY 2 -> broker 1`) must be represented explicitly;
- contradictory case (`ENTRY 1 -> broker 2`) must be represented explicitly.

---

# NON-GOALS — DO NOT BROADEN THIS PR

Do not change:

- scanner admission;
- signal scoring;
- contract selector thresholds;
- position sizing;
- max positions;
- daily loss policy;
- entry watcher behavior;
- entry retry policy;
- take-profit thresholds;
- hard/soft stop thresholds;
- runner/trailing-stop policy;
- EOD policy;
- broker order pricing policy;
- order cancellation policy;
- proof-trade taxonomy;
- queue behavior;
- Master Control policy;
- unrelated #586/#587 cleanup unless a direct rebase conflict requires mechanical adaptation.

No attempt to increase trade flow by weakening a safety gate belongs in this PR.

---

# FILE SCOPE

Expected production scope should remain primarily:

```text
ap_exit_engine.py
```

Expected test scope should remain primarily:

```text
tests/test_p0_exit_engine_broker_truth.py
```

Additional files require explicit justification in the PR body after implementation.

Do not introduce a migration unless implementation proves one is actually required. The defect is in reconstruction authority, not an established need for a schema change.

---

# REQUIRED PRE-MERGE AUDIT

After implementation and after rebasing onto post-#546 `main`, audit the **entire current PR head**, not only the latest amendment.

Review in this order:

1. PR description/spec.
2. Complete actual diff.
3. Review comments.
4. Changed files.
5. Exact production caller path.
6. Runtime/restart/convergence parity.
7. Production metadata shape.
8. Broker submit/cancel paths.
9. `orders` / `positions` / `proof_trades` / queue mutations.
10. Exact-head P0 evidence.

Explicitly answer:

- Does this change LIVE behavior?
- Is it active or flag-off?
- Does it touch broker submit/cancel?
- Does it mutate orders?
- Does it mutate positions?
- Does it mutate proof_trades?
- Does it mutate queue state?
- Does it preserve client_id?
- Does it preserve execution_mode?
- Does it preserve exact OCC identity?
- Does it preserve pending EXIT identity?
- Can it create a duplicate owner?
- Can it create a duplicate EXIT submit?
- Can it silently suppress a legitimate exit?
- Can it rewrite a partial position as a fresh full position?
- Can it contaminate PAPER/LIVE taxonomy?

---

# MERGE BAR

Do not merge until all of the following are true:

- #546 is complete and this PR is rebased onto resulting `main`;
- production diff is narrow;
- canonical `qty` remains original proven entry quantity;
- `quantity_remaining` follows exact broker current exposure;
- `ENTRY 2 -> broker 1` persists `2/1`, not `1/1`;
- restart preserves `2/1`;
- degraded-to-canonical convergence preserves `2/1`;
- `ENTRY 1 -> broker 2` fails closed;
- extended and fallback PostgreSQL INSERTs behave identically;
- zero new broker cancel authority;
- zero second EXIT submission path;
- zero fabricated fill economics;
- exact-head P0 passes;
- PostgreSQL-backed partial recovery test passes;
- independent post-implementation audit returns **MERGE**.

---

# INTENDED RESULT

This PR should make broker recovery more truthful without making Angel Precision more complicated:

```text
original entry quantity stays original
current broker exposure stays current
partial positions stay partial
restarts preserve the same state
recovery does not invent fills or economics
no new order authority is introduced
```

That is the entire job of this follow-up.