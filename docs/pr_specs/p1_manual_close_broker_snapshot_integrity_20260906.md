# P1 SPEC: reject incomplete manual-close broker snapshots

## STATUS

**IMPLEMENTATION PRESENT / MERGE REVIEW PENDING / DO NOT MERGE OR DEPLOY.**

Base authority:

- repository: `angelprecision/angel_precision_bot`
- current committed main: `eb1fdefd8fb35effd1752a8a4de50147c06b066b`
- audit date: 2026-09-06
- defect is active on committed main

This PR hardens the manual-close external-fill discovery path so a partial or malformed broker-position snapshot cannot be silently reduced into a clean-looking set and then used as proof that a local position is missing at the broker.

This is intentionally separate from:

- #590: normal autonomous exit-submit fail-closed broker truth;
- #591: reconciler quantity authority + closed-position repair unavailable-vs-empty handling.

---

## VERIFIED CURRENT-MAIN DEFECT

Current `ap/manual_close_reconciliation.py` already has several strict primitives:

- `positive_int()` rejects booleans, non-finite values, fractional values, and non-positive values;
- `is_valid_occ_contract()` exists;
- broker-order pagination has explicit AVAILABLE/EMPTY/UNAVAILABLE/MALFORMED states.

The defect is not that every quantity is accepted. The defect is that **broker position snapshot normalization silently drops malformed rows and then treats the reduced result as authoritative**.

### A. `normalize_positions_payload()` silently drops bad rows

Current behavior:

```text
for row in rows:
    if not isinstance(row, dict):
        continue

    contract = normalize_contract(...)
    quantity = positive_int(...)

    if contract and quantity > 0:
        normalized.append(...)
```

So these can disappear without invalidating the snapshot:

- non-dict row;
- row with malformed/missing contract;
- row with quantity 0/negative/fractional/boolean/non-finite/malformed;
- row whose symbol is non-empty but not a valid OCC option identity.

A snapshot such as:

```text
[
  valid position A,
  malformed position B
]
```

can become:

```text
[valid position A]
```

and downstream code has no signal that the original broker snapshot was incomplete.

### B. `fetch_authoritative_broker_positions()` can also silently drop malformed rows from an authoritative adapter

Current behavior for `list_positions_authoritative()`:

```text
rows = authoritative()
return [dict(row) for row in rows if isinstance(row, dict)]
```

Again, non-dict rows are discarded rather than making the snapshot invalid/incomplete.

### C. PASS 2 reasons from absence after the reduced snapshot

Current manual-close flow:

```text
fetch_authoritative_broker_positions()
-> broker_contract_qty map
-> missing_positions = local positions whose exact contract is absent from map
-> fetch broker orders
-> external-fill discovery/adoption
```

The map builder again does:

```text
normalize_contract(...)
positive_int(...)
if bpos_contract and quantity > 0:
    broker_contract_qty[bpos_contract] += quantity
```

Malformed broker rows can therefore disappear at more than one boundary.

This creates the unsafe implication:

```text
partial/reduced broker snapshot
-> exact local OCC absent from reduced map
-> position classified missing at broker
-> broker order evidence may then authorize external-close adoption
```

The existing order/fill identity checks reduce risk, but absence must not be inferred from a snapshot whose completeness was never proven.

---

## REQUIRED INVARIANT

Before PASS 2 may use broker-position absence as authority:

```text
broker position snapshot successfully retrieved
AND entire relevant snapshot structurally valid
AND every relevant row identity-valid
AND every relevant quantity valid
AND duplicate/conflicting exact-OCC authority resolved or rejected
AND snapshot classified COMPLETE
```

Then and only then:

```text
exact local OCC absent
-> position may be considered broker-missing
-> existing strict broker-order external-fill discovery may run
```

Otherwise:

```text
UNAVAILABLE / MALFORMED / INCOMPLETE / AMBIGUOUS
-> HOLD PASS 2
-> no missing-position inference
-> no external-fill adoption from that scan
-> no position finalization
-> no proof mutation
```

A partial snapshot is not negative proof.

---

## REQUIRED SNAPSHOT STATE TAXONOMY

Preserve at least these states, or an equivalent explicit contract:

```text
AVAILABLE_COMPLETE_NONEMPTY
AVAILABLE_COMPLETE_EMPTY
UNAVAILABLE
MALFORMED
INCOMPLETE
AMBIGUOUS
```

Do not represent all successful-return lists as equally authoritative if rows were discarded during normalization.

Do not silently return a list that has forgotten whether input rows were malformed.

The adapter compatibility contract treats `positions: null`, `positions: "null"`,
and `positions: {"position": null}` / `{"position": "null"}` as explicit
complete-empty responses only when the surrounding envelopes contain no error
or failure status. A bare empty `positions` object remains malformed.

---

## EXACT POSITION ROW VALIDATION

Every row used to establish snapshot completeness must be validated before absence can become authority.

### Structure

Reject snapshot completeness if a relevant row is not a mapping/dict in the expected broker shape.

Do not `continue` and forget it.

### OCC identity

For option rows, require an exact valid normalized OCC contract before the row contributes to a complete option-position snapshot.

Use the existing `is_valid_occ_contract()` authority where appropriate.

Do not treat arbitrary non-empty symbol text as sufficient option identity.

### Quantity

Reuse the strict `positive_int()` behavior where appropriate, but preserve the distinction between:

```text
valid non-zero integral signed quantity for an identified broker row
explicit/valid zero under a documented broker flat-row contract, if such rows can exist
invalid/malformed quantity
```

Tradier uses positive quantities for long positions and negative quantities for
short positions. Preserve either sign as presence evidence when the row has an
explicit identity; the sign must not make an unrelated valid short row poison
the complete account snapshot. Zero, fractional, boolean, non-finite, and
malformed quantities remain invalid.

Current list semantics generally omit flat positions; do not invent zero-row semantics without evidence.

The critical requirement is: an invalid quantity row must make the relevant snapshot incomplete/malformed, not disappear.

### Duplicate exact OCC rows

If the broker returns more than one row for the same normalized OCC contract:

- combine only if the broker contract explicitly permits split lots and the semantics are proven;
- otherwise classify duplicate exact-OCC authority as ambiguous;
- contradictory quantities/identity must fail closed.

Do not let last-write-wins map behavior or silent summation erase ambiguity without documented authority.

### Underlying/non-option rows

Audit actual production Tradier position shapes.

If stock/equity rows can coexist with option rows, they may be ignored only if they are independently well-formed and their non-option identity is explicit. A malformed unknown row must not be ignored merely because the manual-close path hopes it is unrelated.

An `underlying` field alone is not an explicit non-option identity. The row
must provide a broker symbol/instrument identity, or an exact OCC option
identity plus a matching underlying.

The implementation must document how it distinguishes:

```text
well-formed unrelated non-option row
vs malformed row whose target identity cannot be established
```

---

## REQUIRED CALLER TRACE

Trace the full PASS 2 authority chain:

```text
scan local candidate positions
-> needs_broker_scan
-> fetch_authoritative_broker_positions
-> normalize broker position payload
-> validate snapshot completeness
-> build exact OCC quantity map
-> classify local position present/missing
-> fetch current-session broker orders
-> exact external EXIT fill matching
-> durable adoption
-> canonical position finalizer
-> proof downstream
```

For every changed function report:

```text
caller
-> validation
-> state read
-> authority resolution
-> mutation
-> return classification
-> downstream consumer
```

Do not add a strict normalizer if PASS 2 later rebuilds a permissive map from raw rows.

---

## REQUIRED IMPLEMENTATION BEHAVIOR

### 1. `normalize_positions_payload()` must fail closed on malformed rows

The implementation must not silently discard a malformed relevant row while returning an authoritative list.

Acceptable shapes include:

- return an explicit state/result object;
- raise a specific malformed/incomplete error;
- return `(state, rows)` equivalent.

What is not acceptable:

```text
bad row -> continue -> return reduced authoritative list
```

### 2. Authoritative adapter list must validate every row

If `list_positions_authoritative()` returns a list containing malformed/non-dict rows, the snapshot must not become complete after list comprehension filtering.

### 3. PASS 2 must require complete broker-position truth before absence inference

`missing_positions` may only be constructed from a snapshot explicitly classified as complete authoritative truth.

If snapshot validity is uncertain, return/HOLD before current-session order discovery for those candidates.

### 4. Exact OCC identity must be enforced at snapshot boundary

Do not wait until broker-order matching to discover that broker-position identity was weak.

If an option-position row cannot establish exact OCC identity, the relevant snapshot is incomplete/invalid for absence reasoning.

### 5. Preserve existing strict broker-order evidence

Do not weaken current order/fill requirements.

This PR must not broaden which external EXIT order qualifies for adoption.

The intended sequence remains:

```text
complete authoritative position snapshot
-> exact candidate absent
-> complete authoritative current-session order snapshot
-> exact sell_to_close/OCC/client/mode/qty/timestamp evidence
-> existing adoption/finalization path
```

### 6. Do not let order evidence rescue an invalid position snapshot

Even a perfectly valid external sell-to-close order must not be adopted by PASS 2 if the broker-position snapshot used to establish current absence is incomplete or malformed.

The next scan can retry when authoritative snapshot truth becomes available.

---

## FAILURE-TIMING MATRIX

Execute these boundaries:

```text
broker positions transport failure
malformed top-level positions payload
malformed positions node
list containing non-dict row
row missing OCC identity
row invalid OCC symbol
row malformed quantity
row duplicate/conflicting exact OCC
complete snapshot established
crash after snapshot before orders fetch
orders fetch unavailable
orders complete but no exact fill
exact fill found before adoption
adoption write failure
crash after durable EXIT adoption before finalization
restart with durable adopted EXIT evidence
```

Snapshot failure before complete authority must never create durable negative proof that survives into later adoption.

---

## REQUIRED BEHAVIORAL TESTS

Structural source assertions are secondary only.

### Fail-first production-shape cases

At least one test must prove current main does this today:

```text
positions payload contains valid row + malformed/non-dict row
-> normalize_positions_payload returns only valid row
-> no error/state indicates incompleteness
```

And one must prove:

```text
list_positions_authoritative returns [valid_dict, malformed_non_dict]
-> fetch_authoritative_broker_positions returns only valid_dict
```

These are the concrete fail-first defects.

### Mandatory snapshot negatives

1. top-level payload not dict -> HOLD/error.
2. `positions` node malformed -> HOLD/error.
3. `position` rows scalar -> HOLD/error.
4. one non-dict row among otherwise valid rows -> snapshot incomplete/HOLD.
5. missing contract/OCC identity -> HOLD.
6. non-empty but invalid OCC symbol -> HOLD for option snapshot completeness.
7. valid negative integral quantity -> position presence (not HOLD).
8. quantity fractional -> HOLD.
9. quantity boolean -> HOLD.
10. quantity NaN/inf -> HOLD.
11. malformed quantity string -> HOLD.
12. duplicate exact OCC conflicting quantity -> HOLD.
13. duplicate exact OCC ambiguous row identity -> HOLD.
14. malformed unknown row alongside target-absent snapshot -> no missing-position inference.

### Positive snapshot controls

15. valid complete one-option snapshot -> exact map built.
16. valid complete multi-option snapshot -> exact map built for all contracts.
17. documented complete empty snapshot -> exact empty authority.
18. well-formed unrelated stock row + valid option rows -> behavior documented and tested without invalidating completeness unnecessarily.

### Missing-position/adoption negatives

19. incomplete snapshot + local target absent from reduced rows -> `missing_positions` must not be formed / no adoption.
20. unavailable snapshot + exact broker sell order exists -> no adoption.
21. malformed target row + exact broker sell order exists -> no adoption.
22. wrong OCC order -> no adoption.
23. wrong side -> no adoption.
24. wrong client/mode -> no adoption.
25. ambiguous EXIT orders -> no adoption.
26. missing/invalid broker fill timestamp -> no adoption.
27. partial/insufficient quantity evidence -> existing policy remains fail closed.

### Positive end-to-end control

28. complete authoritative broker-position snapshot proves exact local OCC absent + complete current-session orders contain one exact external `sell_to_close` fill satisfying existing identity/economic rules -> existing adoption/finalization path succeeds once.

29. restart after durable adopted EXIT evidence -> idempotent finalization, no duplicate proof.

### Mutation assertions

For every incomplete/unavailable/malformed snapshot case:

- broker POST = 0;
- broker cancel = 0;
- no external EXIT adoption write;
- no position finalization;
- no quantity_remaining close mutation from PASS 2;
- no proof_trades write;
- no queue/result mutation;
- diagnostics identify snapshot reason.

---

## LIVE/PAPER / IDENTITY CONTROLS

The final implementation must preserve exact:

- `client_id`;
- normalized `execution_mode`;
- `position_id`;
- exact OCC contract;
- broker order identity;
- local external order identity prefix behavior;
- broker fill timestamp source;
- proof taxonomy downstream.

PAPER evidence must never finalize LIVE and vice versa.

Do not allow a valid position snapshot from one client/account to establish absence for another.

---

## RUNTIME / RESTART PARITY

Manual-close scanning after process restart must resolve the same snapshot authority from the same broker facts.

| Scenario | uninterrupted PASS 2 | restart PASS 2 |
|---|---|---|
| valid complete target absent | eligible for order discovery | same |
| non-dict row present | HOLD | HOLD |
| invalid OCC row present | HOLD | HOLD |
| malformed quantity row present | HOLD | HOLD |
| transport unavailable | HOLD | HOLD |
| complete empty snapshot | authoritative empty | authoritative empty |

Do not cache or persist an incomplete snapshot as negative broker evidence.

---

## TEST QUALITY REQUIREMENTS

Do not accept:

- parser-only tests with no PASS 2 consumer proof;
- tests that assert malformed rows are dropped and call that success;
- mocks that bypass `fetch_authoritative_broker_positions()`;
- negative tests without a valid complete-snapshot + exact external-fill positive control;
- tests that never wire the finalizer/adoption seam;
- structural `inspect.getsource()` as primary evidence.

At least one behavioral test must execute:

```text
needs_broker_scan
-> fetch_authoritative_broker_positions
-> snapshot classification
-> missing-position decision
-> broker-order discovery gate
```

And one positive control must continue through the existing exact external-fill adoption path.

---

## SCOPE

Preferred production scope:

- `ap/manual_close_reconciliation.py`

Tests:

- focused manual-close broker-position snapshot integrity tests
- production-shaped PASS 2 tests
- workflow registration if a new P0/P1 blocking file is added

Do not modify:

- normal autonomous exit submission (#590);
- reconciler closed repair (#591);
- broker submit/cancel policy;
- entry execution;
- scanner/selector/risk/sizing;
- proof taxonomy rules;
- canonical finalizer economics;
- external fill matching rules except as necessary to preserve the stricter snapshot precondition.

If a shared broker-position result type from #591 lands first and is correct for this path, rebase and reuse it rather than creating competing snapshot taxonomies.

---

## RELEASE GATE

**MERGE REVIEW PENDING** until all are true:

1. fail-first proves malformed/non-dict broker-position rows are silently dropped on current main;
2. malformed/incomplete snapshots cannot become authoritative reduced lists;
3. exact OCC identity is required before a position row contributes to snapshot completeness;
4. invalid quantity rows invalidate/HOLD rather than disappear;
5. duplicate/conflicting exact-OCC authority fails closed;
6. PASS 2 cannot infer local target absence unless broker-position snapshot is explicitly complete;
7. valid external fill evidence cannot rescue an invalid snapshot;
8. complete valid snapshot + exact valid external fill still finalizes through the existing canonical path exactly once;
9. runtime/restart behavior agrees;
10. client/mode/account/OCC identity remains exact;
11. zero broker submit/cancel authority is added;
12. exact-head P0/P1 CI is green;
13. complete final diff and review threads receive independent MERGE audit.

Do not merge or deploy this spec PR.
