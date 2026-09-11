# P0 PR #605 surgical recut: exact EXIT fill timestamp pre-mutation fence

## Status

**DRAFT / HARD HOLD / CODEX IMPLEMENTATION REQUIRED**

This branch is a clean recut from committed `main` at:

`80dbd631d78c489a36aa39b422c86829cb444b09`

Do **not** merge or deploy this specification-only head.

This PR replaces the current PR #605 as the merge candidate for the HOOD EXIT-fill timestamp incident. PR #605 may remain open temporarily as archaeological/reference material, but its expanded timestamp-quality architecture is intentionally **not** part of this recut.

PR #579 is intentionally preserved. **Do not revert #579.** Its rollback proof established that the pre-#579 code can leave a durable `EXIT_FILLED` order while the canonical position remains `OPEN`. This recut closes the remaining producer/OSM seam without removing #579's canonical EXIT-fill convergence path.

---

## 1. Incident being fixed

Observed LIVE incident shape:

- broker reports an EXIT as executed/filled,
- durable EXIT order reaches `EXIT_FILLED` with positive `filled_qty` and positive `fill_price`,
- durable `filled_ts` remains NULL,
- #579 canonical position convergence correctly refuses to consume the fill because exact fill chronology is unproven,
- durable order is terminal while canonical position remains open.

That is a half-committed money-path state.

The remaining defect on current main is straightforward:

```text
broker/reconciler producer
    -> OSM.transition(EXIT_FILLED, filled_qty>0, fill_price>0, filled_ts=None)
    -> OSM persists EXIT_FILLED / economics
    -> #579 position convergence runs after durable order mutation
    -> convergence HOLDs because exact fill timestamp is missing
    -> order = EXIT_FILLED, position = OPEN
```

The fix is **not** a new chronology architecture. The fix is to prevent the invalid durable EXIT fill mutation from occurring in the first place, and to make the affected reconciler producers pass the exact broker execution timestamp when it exists.

---

## 2. Why PR #579 stays

PR #579 owns the canonical durable EXIT-fill -> position convergence contract.

Its binding invariant remains valid:

```text
exact durable broker-confirmed EXIT fill
+ exact identity/economics
+ exact timezone-aware broker execution timestamp
MUST converge the canonical position exactly once.

BROKER FLAT WITHOUT EXACT FILL AUTHORITY -> HOLD.
```

The verified rollback proof for #579 showed the pre-#579 defect: a durable `EXIT_FILLED` row could exist while the canonical position remained `OPEN`.

Therefore:

- do not revert #579,
- do not bypass `APPositionManager.converge_position_from_durable_exit_order()`,
- do not reintroduce direct position-close SQL,
- do not weaken #579 identity/economics/watermark checks,
- do not fabricate EXIT execution chronology.

This PR only prevents a timestamp-less positive EXIT fill from becoming durable terminal/partial fill truth and fixes the reconciler producers that currently omit the timestamp.

---

## 3. Why current PR #605 is not the merge candidate

Current PR #605 expanded from an incident patch into a broad timestamp-quality model spanning many production files, a schema migration, proof-trade semantics, manual-close semantics, autonomous recovery, position projections, and a second accepted chronology class based on broker order-update time.

That architecture may contain useful research, but it is **not required** to fix the HOOD incident and is not permitted in this recut.

In particular, this recut does **not** introduce or ship:

- `order_update_not_exact_execution`,
- `exit_observed_at`,
- `exit_timestamp_quality`,
- proof-trade timestamp-quality columns,
- migrations for chronology quality,
- `transaction_date` as execution-time authority,
- alternate close semantics where `positions.status=CLOSED` while `exit_ts` is NULL,
- new manual-close chronology behavior,
- new autonomous-recovery chronology behavior,
- broad recovery/proof/schema rewrites.

If production evidence later proves Tradier can report executed quantity/price while never exposing an exact execution timestamp, that is a **separate design problem** and must receive its own fail-first evidence, policy decision, and PR. Do not smuggle that decision into this incident fix.

---

## 4. Existing fail-first evidence already on current main

Current main already contains:

`tests/test_p0_reconciler_exit_fill_timestamp_contract.py`

That file is intentionally treated as the initial fail-first contract for this recut. The production implementation should make those tests pass without broadening the architecture.

Existing cases include:

1. exact Tradier fill timestamp parsing,
2. normal EXIT fill propagates timestamp and broker identity,
3. normal EXIT fill with missing timestamp HOLDs before OSM,
4. missing-broker-id recent fill without timestamp never terminalizes,
5. missing-broker-id recent fill propagates exact timestamp + broker id,
6. OSM rejects missing EXIT fill timestamp before any DB write,
7. stale ACK EXIT fill propagates exact timestamp,
8. stale ACK EXIT fill without timestamp HOLDs before OSM,
9. stale ACK terminal accounting increments `orders_corrected` only after OSM accepts,
10. EXIT-specific fill status enforces timestamp even when `kind` is missing.

Do not delete, weaken, xfail, or rewrite these tests merely to fit implementation.

Additional adversarial cases required by this spec are listed below.

---

## 5. Binding invariant

For any durable transition to `EXIT_PARTIAL_FILL` or `EXIT_FILLED` with positive cumulative executed quantity:

```text
BEFORE any orders-table mutation:

exact timezone-aware broker execution timestamp MUST be present and valid.
```

If it is missing, empty, malformed, timezone-naive, or not supplied by the producer:

```text
OSM returns False
order status is unchanged
filled_qty/fill_price are unchanged by that attempted transition
filled_ts remains unchanged
position is not mutated
proof is not mutated
callback/convergence hooks are not invoked as if the transition succeeded
caller retains/polls/alerts according to its existing lifecycle behavior
```

No local `datetime.now()`, `NOW()`, `updated_ts`, submit time, queue time, recovery time, or broker order-update timestamp may be promoted to EXIT execution time.

ENTRY semantics are unchanged.

---

## 6. Allowed production scope

The implementation is expected to fit in exactly these production files:

1. `ap/order_state_machine.py`
2. `ap_reconciler.py`

Supporting scope:

3. `tests/test_p0_reconciler_exit_fill_timestamp_contract.py`
4. `.github/workflows/p0_regression.yml` only if needed to keep this existing test in the canonical P0 inventory
5. this spec file

### Scope escalation rule

If Codex believes another production file is required, **stop** and prove the need with one fail-first test against clean current main before touching that file.

Do not modify a third production file merely because it would make implementation more convenient.

---

## 7. Forbidden scope

Unless a new clean-main fail-first proof demonstrates direct necessity, do not modify:

- `ap/position_manager.py`
- `ap/exit_fill_truth_guard.py`
- `ap/fill_monitor.py`
- `ap/manual_close_reconciliation.py`
- `ap/exit_autonomous_recovery.py`
- `ap_recovery.py`
- `ap/order_monitor.py`
- `ap/reconcile.py`
- `ap_proof_logger.py`
- `ap/operator/live_execution_journal.py`
- `ap/schema_attestation.py`
- any migration
- `client_runner.py`
- selector/risk/entry logic
- broker submit/cancel/replace logic
- queue eligibility
- proof eligibility policy
- position schema or close semantics

Do not create a second EXIT convergence implementation. #579 remains canonical.

---

## 8. Exact broker execution timestamp authority for this PR

For this recut, the only broker-response fields that may be treated as exact EXIT execution chronology are:

```text
last_fill_date
filled_at
filled_ts
fill_ts
```

A value is usable only if it parses to a timezone-aware datetime (`tzinfo` present and `utcoffset()` non-NULL).

Explicitly **not exact execution authority** in this PR:

```text
transaction_date
update_date
updated_at
updated_ts
created_at
submitted_ts
local processing time
recovery time
NOW()
datetime.now()
```

Important current-main trap:

`ap.manual_close_reconciliation.BROKER_FILL_TIMESTAMP_KEYS` currently includes `transaction_date`.

Therefore the surgical reconciler helper must **not blindly reuse that collection/helper as exact execution authority**. Either define the narrow exact-key tuple locally in `ap_reconciler.py`, or call an existing helper only if the implementation separately proves it cannot accept `transaction_date` for this path.

This PR must not modify `ap/manual_close_reconciliation.py` just to change that global semantic. That would broaden the incident fix into manual/external-close behavior.

---

## 9. `ap/order_state_machine.py` implementation contract

### 9.1 Add one narrow timestamp normalizer/validator

Add a private helper near the existing broker timestamp normalization helpers, conceptually:

```python
def _normalize_exit_broker_filled_ts(value) -> str | None:
    if value is None or blank:
        return None
    parse ISO datetime
    require tzinfo and utcoffset
    return canonical UTC ISO string
```

Requirements:

- `datetime` input may be accepted if timezone-aware,
- ISO string input may be accepted,
- `Z` may normalize to `+00:00`,
- naive datetime/string -> `None`,
- malformed value -> `None`,
- no fallback to local current time.

Do not introduce timestamp-quality enums or metadata.

### 9.2 Fence positive EXIT fill before DB mutation

Inside `APOrderStateMachine.transition()`, after current row read / cumulative quantity validation but **before** building/executing the SQL UPDATE, calculate the effective cumulative fill:

```text
incoming filled_qty if supplied
otherwise durable prev_filled
```

For `new_status in {EXIT_PARTIAL_FILL, EXIT_FILLED}` and effective cumulative fill > 0:

- normalize/validate `filled_ts`,
- if invalid/missing: emit HOLD diagnostic and return `False` immediately,
- do not call `run_with_retry()` / DB UPDATE,
- do not invoke exit convergence hooks,
- do not mutate position/proof,
- do not mark the transition successful.

The fence is status-authoritative. It must apply even if `kind` is missing or malformed but the requested status is `EXIT_PARTIAL_FILL` / `EXIT_FILLED`.

Reason code:

`EXIT_FILL_TIMESTAMP_MISSING_OR_INVALID`

Required event semantics should retain:

```text
decision=HOLD
action=HOLD
position_mutated=false
order_terminalized=false
```

### 9.3 Durable write after successful validation

For a valid positive EXIT fill:

- persist the normalized exact `filled_ts` together with the same guarded transition that persists status/economics,
- do not fabricate timestamp,
- preserve existing broker identity CAS behavior,
- preserve existing status CAS behavior,
- preserve #579 convergence hook behavior.

### 9.4 Same-state partial updates

`EXIT_PARTIAL_FILL -> EXIT_PARTIAL_FILL` with positive cumulative fill remains subject to the exact timestamp requirement.

If a caller submits a later cumulative fill update without exact fill timestamp, HOLD before DB mutation.

### 9.5 ENTRY non-regression

Do not change the existing ENTRY `FILLED` fallback semantics in this PR.

Whatever historical ENTRY behavior currently exists remains unchanged. This recut is EXIT-only.

---

## 10. `ap_reconciler.py` implementation contract

### 10.1 Add strict local exact-timestamp extraction

Add a small helper near existing numeric/broker normalization helpers:

```python
_EXACT_BROKER_EXIT_FILL_TIMESTAMP_KEYS = (
    "last_fill_date",
    "filled_at",
    "filled_ts",
    "fill_ts",
)

def _extract_broker_fill_timestamp(raw: dict) -> Optional[str]:
    ...
```

Behavior:

- `raw` must be a dict,
- inspect only the four exact keys above,
- if no exact field exists -> `None`,
- if an exact field is present but malformed/naive -> `None`,
- do not fall through to `transaction_date`, update time, local time, submit time, or current time,
- return timezone-aware normalized UTC ISO string.

If multiple exact fields are simultaneously present and parse to different instants, fail closed (`None`) rather than picking one by precedence.

If multiple present fields represent the same instant in different ISO spellings/offsets, they may normalize to the same UTC instant and be accepted.

### 10.2 `_apply_osm_fill_update()`

Extend only as needed to propagate:

- `broker_order_id`
- `filled_ts`

through both:

- `osm.apply_fill_update(...)` for already-partial lifecycle paths,
- fallback `osm.transition(...)` path.

The current fallback must not drop `broker_order_id` or `filled_ts`.

Do not add timestamp-quality parameters.

### 10.3 `_advance_order_to_broker_fill()`

For EXIT family only:

1. resolve exact broker order id using existing identity helpers,
2. extract exact broker fill timestamp using the narrow helper,
3. if positive broker executed quantity/price is present but exact timestamp is missing/invalid:
   - alert/HOLD,
   - do not call OSM fill transition,
   - do not increment `orders_corrected`,
   - do not mutate position/proof,
4. when exact timestamp exists, pass `broker_order_id` and `filled_ts` into the existing OSM path.

ENTRY behavior must remain unchanged.

### 10.4 `_handle_stale_acknowledged_exits()`

When broker truth says FILLED with positive quantity/price for an EXIT:

- extract exact fill timestamp,
- missing/invalid -> HOLD before OSM,
- exact -> call OSM with broker id, quantity, price, timestamp,
- increment `orders_corrected` only if OSM returns truthy,
- if OSM returns false, alert/error and do not claim correction.

For broker terminal status without a fill:

- preserve existing terminal mapping behavior,
- but also correct the accounting bug: `orders_corrected` increments only when OSM transition returns truthy.

Do not redesign canceled-with-execution handling here. #579/fill-monitor/recovery already owns that broader lifecycle family. If a new defect is found there, prove it separately rather than expanding this PR.

### 10.5 `_resolve_missing_id_exit_truth()` recent-fill endpoint

If recent EXIT fill evidence is used to resolve a missing broker id:

- positive fill qty/price is insufficient by itself,
- exact `broker_order_id` is required,
- exact fill timestamp is required,
- missing either -> keep quarantine/HOLD and do not terminalize,
- when both exist, pass both into OSM.

### 10.6 Recent fill query shape

If `_get_recent_exit_fill()` currently does not return both `filled_ts` and `broker_order_id`, extend its SELECT narrowly so this endpoint can prove both.

No schema change is permitted.

---

## 11. HOLD diagnostics

Use one stable incident reason for missing/malformed exact fill chronology:

`EXIT_FILL_TIMESTAMP_MISSING_OR_INVALID`

At minimum include when available:

```text
client_id
local_order_id
broker_order_id
position_id
contract
broker_status
durable_status
filled_qty
fill_price
source
action=HOLD
position_mutated=false
order_terminalized=false
```

Do not log or label a correction as successful when OSM returns false.

---

## 12. Required tests

### 12.1 Existing tests must pass unchanged

`tests/test_p0_reconciler_exit_fill_timestamp_contract.py`

Do not weaken existing assertions.

### 12.2 Add exact-key / authority negatives to the same file

Add cases proving all of the following:

1. `last_fill_date` exact aware timestamp -> accepted.
2. `filled_at` exact aware timestamp -> accepted.
3. `filled_ts` exact aware timestamp -> accepted.
4. `fill_ts` exact aware timestamp -> accepted.
5. `transaction_date` only -> rejected as exact execution timestamp.
6. `updated_at` only -> rejected.
7. naive exact-key timestamp -> rejected.
8. malformed exact-key timestamp -> rejected.
9. two exact fields resolving to different instants -> rejected.
10. two exact fields resolving to same instant with different offsets -> accepted.

### 12.3 OSM pre-mutation tests

For `EXIT_FILLED` and `EXIT_PARTIAL_FILL` independently:

- positive quantity + no timestamp -> return false before DB call,
- positive quantity + naive timestamp -> return false before DB call,
- positive quantity + malformed timestamp -> return false before DB call,
- positive quantity + exact aware timestamp -> DB path is allowed,
- kind missing + EXIT status -> same fence applies,
- ENTRY `FILLED` missing timestamp -> existing historical behavior unchanged.

At least one test must monkeypatch `run_with_retry` to fail immediately if the missing/invalid timestamp path reaches a DB write.

### 12.4 HOOD incident-shaped replay

Add/retain a focused production-shaped replay:

```text
local EXIT_ACKNOWLEDGED
broker id present
broker says filled
exec qty = 1
avg fill price > 0
exact last_fill_date present
```

Expected:

```text
OSM receives EXIT_FILLED with exact broker id + exact filled_ts
canonical #579 convergence is allowed to proceed
```

Negative twin:

```text
same broker response, exact execution timestamp absent
```

Expected:

```text
HOLD before OSM terminal mutation
order does not become EXIT_FILLED
position unchanged
```

### 12.5 PostgreSQL production-shape proof

Add one real PostgreSQL test if the existing test infrastructure can do so without introducing new production scope:

A. Seed `EXIT_ACKNOWLEDGED`, positive requested quantity, broker identity.

B. Call OSM `transition(EXIT_FILLED, filled_qty=1, fill_price=..., filled_ts=None)`.

C. Assert:

- returns false,
- durable status remains `EXIT_ACKNOWLEDGED`,
- durable `filled_qty` / `fill_price` are not advanced by this rejected transition,
- `filled_ts` remains NULL.

D. Repeat with exact timezone-aware `filled_ts` and assert the guarded durable transition can proceed into #579 convergence semantics.

Do not create a new database schema contract or migration for this test.

---

## 13. Tests specifically not required here

Do not add broad test matrices for:

- proof-trade timestamp quality,
- manual external close order-update timestamps,
- non-exact position close timestamps,
- schema migration/attestation,
- autonomous recovery timestamp-quality propagation,
- journal timestamp-quality propagation.

Those belong to the retired broad #605 design, not this incident recut.

---

## 14. CI / merge gates

After implementation, on one unchanged head SHA:

1. `python -m py_compile ap_reconciler.py ap/order_state_machine.py tests/test_p0_reconciler_exit_fill_timestamp_contract.py`
2. `git diff --check`
3. focused timestamp-contract tests green
4. #579 EXIT convergence / partial-then-cancel regression tests green
5. exact-head canonical P0 green
6. separate pull-request merge-ref canonical P0 green
7. exact-head and merge-ref must run the same canonical test inventory
8. no production reroll between the two P0 results

If `main` advances before merge, rebase onto committed main and rerun all gates. Do not manually cherry-pick unrelated open-PR behavior into this branch.

---

## 15. Diff-size / scope gate

Before merge clearance, verify the final net diff.

Expected production files:

```text
ap/order_state_machine.py
ap_reconciler.py
```

Expected supporting files:

```text
tests/test_p0_reconciler_exit_fill_timestamp_contract.py
.github/workflows/p0_regression.yml   # only if necessary
this spec
```

Any additional production file automatically returns the PR to HARD HOLD until justified by a clean-main fail-first test.

No migration is allowed.

---

## 16. Money-path audit checklist

Before merge clearance verify:

- new broker ENTRY POST call sites: 0
- new broker EXIT POST call sites: 0
- new broker cancel call sites: 0
- new broker replace call sites: 0
- new direct position mutation path: 0
- new proof mutation path: 0
- new EXIT convergence authority: 0
- fabricated EXIT execution timestamps: 0
- `transaction_date` promoted to exact fill time: 0

The only behavioral change should be:

```text
invalid timestamp-less positive EXIT fill
previously: could durable-mutate order, then convergence HOLD
now: HOLD before durable EXIT fill mutation
```

and:

```text
reconciler has exact broker execution timestamp
previously: some producers dropped it
now: propagate it to OSM
```

---

## 17. Codex implementation order

Implement in this order so failures stay attributable:

### Step 1 — prove baseline

Run the existing focused test file against clean branch head and record which cases fail before production edits.

Do not edit tests to manufacture a red state.

### Step 2 — OSM pre-mutation fence

Implement only the EXIT timestamp validator + early HOLD in `ap/order_state_machine.py`.

Run OSM-focused tests.

### Step 3 — reconciler exact timestamp helper

Implement strict exact-key extraction in `ap_reconciler.py`, explicitly excluding `transaction_date`.

Run parser authority tests.

### Step 4 — producer propagation

Patch only:

- `_apply_osm_fill_update`
- `_advance_order_to_broker_fill`
- `_handle_stale_acknowledged_exits`
- `_resolve_missing_id_exit_truth`
- recent-fill SELECT if necessary

Run focused tests after each producer family.

### Step 5 — add missing adversarial tests

Add transaction-date-only, naive/malformed, conflicting exact-field, ENTRY non-regression, and real PostgreSQL proof.

### Step 6 — full P0

Run exact-head and merge-ref on unchanged head.

### Step 7 — stop

Do not continue into timestamp-quality architecture, migrations, proof semantics, manual-close redesign, or autonomous recovery after the focused tests are green.

The PR is complete when the incident invariant is closed within the allowed scope.

---

## 18. Final merge-clearance report format

Before asking for merge, update the PR body with:

```text
Base SHA:
Head SHA:
Changed production files:
Changed supporting files:
Focused tests:
#579 regression tests:
PostgreSQL proof:
Exact-head P0 run/job:
Merge-ref P0 run/job:
git diff --check:
Money-path audit:
Open review threads:
Final verdict requested: MERGE
```

Do not mark ready merely because unit tests are green. Final clearance requires net-diff audit and unchanged-head CI evidence.
