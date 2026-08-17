# P0 SPEC: Preserve broker-order UNKNOWN in reconciler missing-ID EXIT recovery

**Date:** 2026-08-17  
**Branch:** `spec/p0-reconciler-missing-id-exit-order-truth-20260817`  
**Classification:** P0 money-path safety  
**Primary production owner:** `ap_reconciler.py`  
**Status:** SPEC-ONLY / DRAFT / DO NOT MERGE OR DEPLOY UNTIL IMPLEMENTED + RE-AUDITED

---

## 1. Why this is a separate PR from #481

This defect was discovered while forensically auditing PR #481, but it is **not a regression introduced by #481**.

The exact pre-#481 base (`main@d0d37e79ae698e604eb8080065d2314b161de351`) already contains both unsafe reconciler behaviors described below:

1. `_safe_get_broker_open_orders()` collapses unavailable/malformed broker-order truth into `[]`.
2. `_broker_order_qty_from_raw()` uses `abs(int(float(...)))` and can manufacture a believable positive integer from signed, boolean, or fractional input.

Because those defects already exist on #481's base, they must **not** be appended indefinitely to #481. #481 owns exact-contract broker-position quantity / false-flat truth and autonomous FILLED-quantity truth. This PR owns **reconciler missing-broker-ID EXIT order recovery truth**.

### Relationship to #481

- #481 may merge independently once its own exact-head gate is satisfied.
- This PR does **not** require #481 to remain open.
- Before production implementation begins here, rebase/sync this branch onto the then-current `main`, especially if #481 has merged.
- Do **not** copy, re-implement, or modify #481's autonomous-recovery quantity work in this PR.
- Do **not** touch `ap/brokers/tradier.py` to solve this defect. #481 already establishes the adapter-level `list_orders()` contract needed by downstream callers: unavailable/malformed truth raises, authoritative empty returns `[]`.

---

## 2. Does this defect stop the bot from trading?

**No. This is not a system-wide trade-flow halt.**

The defect sits in a specific reconciler recovery path for an EXIT order that exists locally but has **no trusted broker order ID**.

Normal signal generation, eligibility, contract selection, ENTRY submission, ordinary broker-ID-backed order monitoring, and ordinary exit management can continue while this PR is open.

That is why #481 can merge and the bot can continue to take trades while this P0 is queued.

However, this remains P0 because when the narrow recovery condition occurs, the bot can convert **unknown broker order truth into permission to replace an exit**. That can create duplicate `SELL_TO_CLOSE` exposure if a real broker EXIT still exists but the reconciler failed to prove it.

This is a money-path safety issue, not a general liveness issue.

---

## 3. Exact production defect A: broker-order UNKNOWN becomes authoritative empty

Current reconciler behavior is effectively:

```python
def _safe_get_broker_open_orders(self) -> list[dict]:
    for method_name in (...):
        method = getattr(self.broker, method_name, None)
        if not callable(method):
            continue
        try:
            result = method(...)
            ...
            if valid_success:
                return rows
        except Exception:
            ...
    return []
```

The final `return []` destroys the distinction between:

- **AVAILABLE_EMPTY**: broker query succeeded and authoritatively proved there are zero open orders.
- **UNKNOWN / UNAVAILABLE**: broker query failed, raised, returned `None`, returned a malformed shape, or no usable method existed.

That distinction is binding for a money-path caller.

### Unsafe lifecycle

A local protective EXIT exists with no `broker_order_id`:

```text
local EXIT has no broker_order_id
        ↓
reconciler tries broker open-order recovery
        ↓
broker query raises / malformed / unavailable
        ↓
_safe_get_broker_open_orders() returns []
        ↓
_recover_missing_broker_id_exit() returns False
        ↓
reconciler records a negative recovery pass
        ↓
second pass repeats
        ↓
_resolve_missing_id_exit_truth()
        ↓
mark_exit_replacement_safe() or clear_exit_in_flight()
        ↓
local EXIT transitions to CANCELED
        ↓
future replacement EXIT may be allowed
```

If the real broker EXIT was still open, a duplicate `SELL_TO_CLOSE` can become possible.

### Core invariant

> **UNKNOWN broker-order truth is never negative broker-order proof.**

Time does not turn UNKNOWN into EMPTY.  
Retry count does not turn UNKNOWN into EMPTY.  
A logging warning does not turn UNKNOWN into EMPTY.

---

## 4. Exact production defect B: reconciler broker-order quantity laundering

Current reconciler behavior is effectively:

```python
def _broker_order_qty_from_raw(self, raw: dict) -> int:
    for key in ("quantity", "qty", "order_qty", "remaining_quantity", "remaining_qty"):
        try:
            val = raw.get(key)
            if val is not None and val != "":
                return abs(int(float(val)))
        except Exception:
            pass
    return 0
```

Unsafe examples:

```text
-4     -> +4
True   -> 1
0.5    -> 0
-0.5   -> 0
```

That parsed quantity participates in missing-ID broker EXIT candidate filtering and scoring.

A malformed or signed broker quantity must not gain positive matching authority simply because `abs(int(float(...)))` can turn it into an integer.

### Core invariant

> Missing-ID broker order recovery may only use quantity as positive identity evidence when the broker quantity is explicitly proven to be a positive, finite, mathematically integral, non-boolean value.

---

# 5. Tight production scope

## Allowed production file

**ONLY:**

```text
ap_reconciler.py
```

## Allowed non-production files

```text
tests/test_p0_reconciler_missing_id_exit_order_truth.py
.github/workflows/p0_regression.yml
docs/pr_specs/p0_reconciler_missing_id_exit_order_truth_20260817.md
```

If implementation requires another production file, STOP and document why before changing it. Do not silently broaden scope.

### Explicitly out of scope

Do not modify:

- `ap/brokers/tradier.py`
- `ap/exit_autonomous_recovery.py`
- `ap/exit_safety.py`
- `ap_exit_engine.py`
- `order_state_machine.py`
- ENTRY submit logic
- selector logic
- watcher logic
- FVG / VI / intelligence logic
- stop / target logic
- position sizing
- queue ownership
- proof-trade economics
- schema / migrations
- broker cancel architecture
- short-option management

This PR must not become a generic reconciler rewrite.

---

# 6. Required fix A: tri-state broker open-order truth

Change `_safe_get_broker_open_orders()` so callers can distinguish authoritative empty from unknown.

A minimal acceptable contract is:

```python
Optional[list[dict]]
```

with these semantics:

```text
list with one or more rows  -> AVAILABLE_NONEMPTY
[]                          -> AVAILABLE_EMPTY
None                        -> UNKNOWN / UNAVAILABLE
```

Equivalent tiny enum/dataclass is acceptable if it reduces ambiguity, but do not build a new framework.

## AVAILABLE_EMPTY may only mean

The broker query successfully returned a supported response shape and authoritatively contained zero open orders.

## UNKNOWN must include

- broker method raises `ConnectionError`
- timeout
- auth / HTTP exception propagated by adapter
- deterministic malformed-payload exception
- result is `None`
- result shape is unrecognized
- malformed individual order row makes the snapshot incomplete
- no usable broker order-query method exists

Do not silently filter a malformed row out and still claim the remaining list is authoritative.

---

# 7. Required fix B: propagate tri-state through missing-ID EXIT recovery

Audit every direct caller of `_safe_get_broker_open_orders()`.

At minimum this includes:

- `_recover_missing_broker_id_exit()`
- `_handle_order_without_broker_id()` through the recovery result
- `_resolve_missing_id_exit_truth()` through the recovery result
- `_broker_open_exit_exists_for_contract()`

The current boolean recovery result is insufficient if `False` means both:

1. authoritative scan succeeded and found no valid match;
2. broker-order truth could not be established.

Use a minimal tri-state result.

An acceptable pattern is:

```python
Optional[bool]
```

where:

```text
True  -> exact broker EXIT recovered
False -> authoritative broker scan completed; no acceptable match
None  -> broker order truth UNKNOWN
```

A small named enum is also acceptable.

## Binding behavior on UNKNOWN

When missing-ID EXIT recovery returns UNKNOWN:

MUST NOT:

- increment `_missing_id_exit_tracker` as though a successful negative scan occurred;
- call `mark_exit_replacement_safe()`;
- call `clear_exit_in_flight()`;
- transition the local EXIT to `CANCELED` based on negative proof;
- authorize replacement;
- conclude that no broker EXIT exists.

Required behavior:

```text
HOLD / quarantine
```

Suggested deterministic reason:

```text
RECONCILER_BROKER_ORDER_TRUTH_UNKNOWN_HOLD
```

The row should remain visible and protected for the next broker-truth attempt.

---

# 8. Required fix C: Endpoint 3 must require authoritative negative proof

`_resolve_missing_id_exit_truth()` contains the path that can ultimately release replacement safety and terminalize the local missing-ID EXIT after repeated negative checks.

That endpoint is valid **only** after authoritative broker-order proof.

Required precondition:

```text
broker order query AVAILABLE
AND no acceptable matching live broker EXIT
AND no recent fill evidence
AND existing repeated-negative policy satisfied
```

It is forbidden when broker-order truth is UNKNOWN.

Do not rely on the caller's retry count alone. Make the authority requirement obvious in code and tests.

---

# 9. Required fix D: strict broker-order quantity truth

Replace `abs(int(float(...)))` in the missing-ID order-recovery quantity path.

Do not use:

- `abs()`
- `round()`
- `int()` truncation as validation
- sign correction
- boolean numeric coercion

A small local parser should distinguish at least:

```text
VALID
ABSENT
MALFORMED
```

Recognized fields may remain the existing reconciler set unless production evidence requires otherwise:

```text
quantity
qty
order_qty
remaining_quantity
remaining_qty
```

## VALID

A quantity is VALID only if it is:

- non-boolean
- finite
- strictly positive
- mathematically integral

Examples:

```text
1     -> VALID 1
4     -> VALID 4
4.0   -> VALID 4
"4"   -> VALID 4
```

## MALFORMED

Examples:

```text
True
False
0
-1
-4
0.5
-0.5
NaN
Infinity
-Infinity
"garbage"
[]
{}
```

## ABSENT

No recognized quantity key exists / no quantity evidence is supplied.

Absent quantity is not automatically a conflict because a broker order may still be recoverable from strong exact identity metadata.

Malformed-present quantity **is** conflicting evidence and must not be converted into positive match authority.

---

# 10. Candidate matching/scoring contract

For `_score_missing_id_exit_candidate()` and `_recover_missing_broker_id_exit()`:

### VALID quantity

If requested quantity is known and broker quantity is valid:

- exact match may keep the current positive `qty_exact` confidence;
- genuine mismatch may keep the current mismatch rejection/penalty policy.

### ABSENT quantity

- no `qty_exact` confidence;
- do not manufacture a mismatch;
- candidate may still survive if stronger exact identity / side / time evidence proves it under existing policy.

### MALFORMED quantity

- no `qty_exact` confidence;
- do not coerce it to zero or positive magnitude;
- do not adopt a broker order on the strength of malformed quantity;
- reject/hold that candidate unless a separately documented stronger identity rule explicitly establishes that malformed quantity is irrelevant.

If malformed quantity is necessary to distinguish competing candidates, the result is ambiguity/HOLD, not adoption.

Do not weaken exact OCC contract matching.

---

# 11. `_broker_open_exit_exists_for_contract()` caller rule

Changing `_safe_get_broker_open_orders()` to tri-state requires this caller to stop treating UNKNOWN as `False`.

A minimal acceptable contract is:

```text
True  -> matching broker open EXIT confirmed
False -> authoritative broker order snapshot confirms no matching open EXIT
None  -> broker-order truth UNKNOWN
```

When `_handle_db_position_missing_at_broker()` receives UNKNOWN from this check, it must conservatively HOLD the broker-missing observation and must not use UNKNOWN as proof that no protective broker EXIT exists.

This adaptation is allowed because it is a direct caller required by the tri-state function signature. Do not expand beyond this direct propagation.

---

# 12. Fail-first test requirements

Write the regression tests **before** production edits.

Because this branch was created from pre-#481 `main`, implementation must first sync/rebase onto the current main after #481 is merged (if #481 has merged by then), then record the exact implementation base SHA.

The fail-first tests must demonstrate the unsafe current behavior on that exact implementation base before production edits.

## Group A: broker open-order truth

### A1

`broker.list_orders()` raises:

```python
ValueError("TRADIER_ORDERS_PAYLOAD_MALFORMED")
```

Fail-first current result:

```text
_safe_get_broker_open_orders() == []
```

Post-fix:

```text
None / UNKNOWN
```

### A2

`ConnectionError` -> UNKNOWN.

### A3

`TimeoutError` -> UNKNOWN.

### A4

broker method returns `None` -> UNKNOWN.

### A5

unrecognized/malformed response -> UNKNOWN.

### A6

authoritative successful empty snapshot -> `[]`.

### A7

valid list with one open order -> AVAILABLE_NONEMPTY.

### A8

mixed valid + malformed broker rows -> UNKNOWN, not partial authoritative snapshot.

---

# 13. End-to-end missing-ID EXIT tests

### B1: repeated UNKNOWN never becomes negative proof

Setup:

- local EXIT has no broker order ID;
- position ID and exact OCC contract known;
- broker order query raises on every pass;
- no recent fill.

Run enough reconciler passes to exceed the existing negative-pass threshold.

Assert:

- no `mark_exit_replacement_safe()`;
- no `clear_exit_in_flight()`;
- local EXIT not transitioned to `CANCELED` from negative proof;
- no replacement authorization;
- UNKNOWN does not increment authoritative negative proof;
- deterministic HOLD diagnostic emitted.

### B2

Same as B1 with malformed broker payload.

### B3: authoritative empty still preserves normal recovery

Broker order query succeeds with authoritative `[]` on the required passes, no recent fill, all other existing proof conditions satisfied.

Assert the existing eventual replacement-safe path remains available.

This proves:

```text
UNKNOWN != EMPTY
```

### B4: live broker EXIT still recovered

A valid same-OCC sell-to-close broker order is present.

Assert:

- broker order ID is recovered/adopted;
- no replacement-safe release;
- local order transitions through the existing acknowledgement path.

### B5: different OCC does not recover

Same underlying but different OCC contract.

Assert no adoption.

---

# 14. Strict broker-order quantity tests

### C1

`quantity=-4`

Fail-first must prove current helper returns `+4`.

Post-fix:

```text
MALFORMED
```

Never `+4`.

### C2

`quantity=True` -> MALFORMED, never 1.

### C3

`quantity=0.5` -> MALFORMED, never 0.

### C4

`quantity=-0.5` -> MALFORMED.

### C5

`quantity=NaN` -> MALFORMED.

### C6

`quantity=Infinity` -> MALFORMED.

### C7

`quantity="garbage"` -> MALFORMED.

### C8

`quantity=4` -> VALID 4.

### C9

requested qty=4 + broker qty=-4:

- no `qty_exact` score;
- no false adoption from sign flip.

### C10

requested qty=4 + broker qty=0.5:

- no `qty_exact` score;
- no false adoption from truncation.

### C11

requested qty=4 + broker qty=4:

- existing valid `qty_exact` behavior preserved.

### C12

quantity field absent but exact identity metadata strongly matches:

- no quantity score;
- preserve existing identity-based recovery policy if all other requirements pass.

### C13

quantity malformed-present + otherwise weak candidate:

- candidate cannot be rescued by fabricated quantity;
- HOLD/reject.

---

# 15. Identity / isolation requirements

All tests and implementation must preserve:

- exact `client_id` ownership;
- exact `execution_mode` ownership;
- PAPER cannot mutate LIVE;
- LIVE cannot mutate PAPER;
- exact OCC contract matching;
- one client's broker order cannot be adopted by another client's missing-ID EXIT;
- unrelated same-underlying OCC order cannot satisfy exact contract recovery.

No wildcard contract recovery.

---

# 16. Money-path restrictions

This PR must introduce:

```text
NO new ENTRY submit authority
NO new EXIT submit authority
NO new broker cancel authority
NO BUY_TO_CLOSE authority
NO short-option management
NO new position creation authority
NO new proof-trade finalization authority
NO strategy changes
NO selector changes
NO watcher changes
NO queue ownership changes
NO stop/target changes
NO FVG/intelligence changes
NO schema changes
```

The only behavioral change is **removing replacement/cancellation authority when broker-order truth is UNKNOWN or candidate quantity evidence is malformed**.

Existing legitimate replacement behavior after authoritative negative proof must remain.

---

# 17. CI requirements

Add:

```text
tests/test_p0_reconciler_missing_id_exit_order_truth.py
```

to `.github/workflows/p0_regression.yml`.

Run at minimum:

1. the new focused test file;
2. #481 broker-order unknown/Tradier order-shape tests after #481 is on main;
3. existing missing-ID EXIT recovery tests;
4. existing broker-owned exit recovery tests;
5. stale working EXIT / replacement-liveness tests relevant to reconciler ownership;
6. canonical exit fill/proof truth tests;
7. PAPER/LIVE isolation tests;
8. exact-head full `P0 Regression Suite` with normal CI Postgres service.

Do not declare ready from focused unit tests alone.

---

# 18. Required implementation delivery from Claude/Codex

Return all of the following:

1. exact implementation base SHA after syncing latest `main`;
2. confirmation whether #481 was already merged into that base;
3. new exact PR head SHA;
4. exact production files changed;
5. exact test files changed/added;
6. exact workflow change;
7. fail-first proof broker-order exception became `[]`;
8. post-fix proof broker-order exception is UNKNOWN;
9. repeated UNKNOWN end-to-end proof showing no replacement-safe release;
10. authoritative `[]` proof showing existing replacement-safe behavior remains;
11. valid live same-OCC EXIT recovery proof;
12. fail-first `-4 -> +4` quantity proof;
13. post-fix signed/fractional/bool/nonfinite quantity proof;
14. valid qty=4 scoring proof;
15. proof malformed quantity does not gain `qty_exact` confidence;
16. exact client/mode isolation proof;
17. exact focused test counts;
18. exact-head P0 workflow run ID;
19. exact-head P0 workflow conclusion;
20. broker read/submit/cancel audit;
21. orders/positions/proof/queue mutation audit;
22. confirmation no new broker submit/cancel authority;
23. confirmation no unrelated strategy change;
24. confirmation PR remains Draft until independent re-audit;
25. confirmation not merged and not deployed.

---

# 19. Final implementation instruction

**Implement only this bounded reconciler P0.**

Before touching production code:

1. refresh current `main`;
2. if #481 has merged, rebase/sync this branch onto that merge;
3. do not duplicate #481's fixes;
4. write fail-first tests;
5. prove the defects on the exact implementation base;
6. make the smallest production change in `ap_reconciler.py`;
7. run exact-head CI;
8. return the new head for independent forensic audit.

Do not merge.  
Do not deploy.  
Do not mark ready without explicit authorization.
