# P0 SPEC: enforce strict broker position truth in reconciliation and closed repair

## STATUS

**SPEC ONLY / HARD HOLD / IMPLEMENTATION REQUIRED. DO NOT MERGE OR DEPLOY.**

Base authority:

- repository: `angelprecision/angel_precision_bot`
- current committed main: `eb1fdefd8fb35effd1752a8a4de50147c06b066b`
- audit date: 2026-09-06
- defect is active on committed main

This PR owns one authority contract shared by two current-main defects:

1. reconciler broker-position quantity parsing destroys sign/precision and converts malformed truth into apparently valid quantity;
2. the non-strict Tradier positions adapter converts transport/parsing failure into `[]`, and closed-position repair can interpret that empty list as successful broker-flat truth and write durable zero remaining quantity.

These belong together because both determine whether broker position state is authoritative enough to mutate durable position state.

---

## VERIFIED CURRENT-MAIN DEFECT A: LOSSY QUANTITY RESOLUTION

Current `ap_reconciler.py::_broker_position_qty()` resolves quantity as:

```python
raw = (
    bp.get("quantity")
    or bp.get("qty")
    or bp.get("long_quantity")
    or bp.get("short_quantity")
    or 0
)
return abs(int(float(raw)))
```

This is unsafe as an authority resolver.

It can transform:

```text
-1      -> 1
1.5     -> 1
True    -> 1
"1.9"  -> 1
short_quantity=1 -> positive quantity 1
malformed value -> 0
```

It also uses `or` precedence, so explicit zero is treated as absence and later aliases can override it. Contradictory duplicate quantity authority is not rejected.

The resolver is consumed by multiple production callers including:

- broker-position import / missing-position recovery;
- DB-vs-broker position reconciliation;
- closed-position remaining-quantity repair;
- broker-derived entry-price/cost-basis calculations;
- broker-open maps used as durable-state authority.

A strict helper is useless if these callers continue to use the old lossy chain.

---

## VERIFIED CURRENT-MAIN DEFECT B: BROKER FAILURE CAN BECOME AUTHORITATIVE EMPTY

Current `ap/brokers/tradier.py` behavior:

```text
list_positions()
-> _list_positions(strict=False)
-> transport / payload / conversion exception
-> log TRADIER_LIST_POSITIONS_FAILED
-> return []
```

The docstring currently states that `list_positions()` returns `[]` for both no positions and error.

Current closed-position repair in `ap_reconciler.py` does:

```text
bp_list = self.broker.list_positions() or []
-> iterate rows
-> broker_truth_available = True
```

Therefore a non-strict Tradier failure returning `[]` is indistinguishable from a successful complete empty broker snapshot.

The same repair then does:

```text
broker_qty = broker_open_by_contract.get(contract, 0)
if broker_qty <= 0:
    UPDATE positions
    SET quantity_remaining = 0,
        close_source = 'CLOSED_REPAIR'
```

So:

```text
Tradier transport/payload failure
-> []
-> broker_truth_available=True
-> exact contract absent from empty map
-> interpreted as broker flat
-> durable quantity_remaining=0 / CLOSED_REPAIR mutation
```

That is a P0 authority collapse: UNKNOWN becomes FLAT.

---

## REQUIRED INVARIANT

Durable position state may consume broker-position truth only when the snapshot is proven:

- successfully retrieved;
- complete for the relevant account;
- structurally valid;
- exact-account scoped;
- exact-OCC scoped;
- quantity-valid;
- direction-valid;
- internally non-contradictory.

Required state taxonomy:

```text
AVAILABLE_OPEN
AVAILABLE_EMPTY / AVAILABLE_FLAT
UNAVAILABLE
MALFORMED
AMBIGUOUS / CONTRADICTORY
```

Equivalent representations are acceptable, but these states must remain distinguishable to every mutation caller.

Never:

```text
UNAVAILABLE -> [] -> FLAT
MALFORMED -> 0 -> FLAT
negative short -> abs() -> positive long
fractional -> truncation -> valid integral quantity
boolean -> integer quantity
```

---

## AUTHORITY CONTRACT FOR QUANTITY

### Accepted long quantity

For long option-position authority, quantity must be:

```text
finite
integral
strictly positive when representing OPEN long exposure
explicit zero only when the authoritative field contract permits flat zero
not boolean
not negative
not fractional
not NaN/inf
```

### Explicit zero

Explicit zero must not be treated as missing due to Python `or` chaining.

If one alias says `0` and another says `1`, that is contradictory authority unless the broker contract explicitly defines those fields as independent dimensions.

### Negative quantity

Negative quantity must preserve direction. It must not be converted to positive using `abs()`.

If the current business path only manages long options, negative target quantity must fail closed for that target rather than being normalized into a long.

### Fractional quantity

Options contracts are integral. `1.5` must not become `1`.

### Boolean quantity

`True` and `False` are invalid broker quantity authority even though Python treats booleans as numeric subclasses.

### Non-finite values

NaN, +inf, -inf must fail closed.

### Quantity aliases

Audit at least these current/legacy aliases:

```text
quantity
qty
long_quantity
short_quantity
```

Do not silently select the first truthy alias when multiple explicit aliases disagree.

`short_quantity` must never become positive long authority.

---

## REQUIRED BROKER SNAPSHOT CONTRACT

### Adapter boundary

The path used for durable mutation must preserve the distinction between:

```text
successful complete empty result
successful complete non-empty result
transport unavailable
HTTP/auth unavailable
malformed payload
malformed row
conversion failure
```

Implementation options include:

- use the already-existing strict Tradier seam where appropriate;
- introduce an explicit result object/state for authoritative position reads;
- wrap the adapter at the reconciler boundary with strict classification.

Do not globally redesign Tradier unless required. The smallest safe contract is preferred.

### Important existing seam

Current Tradier already exposes `list_positions_strict()`, which raises on failure. Audit whether the reconciler can consume that seam safely rather than inventing another transport path.

However, strict transport alone is insufficient. Row quantity and identity still require strict validation.

---

## REQUIRED CALLER TRACE

Trace every production use of broker-position quantity that can influence state:

```text
broker adapter
-> snapshot availability classification
-> row identity validation
-> quantity authority resolution
-> broker-open map / exact target lookup
-> reconciliation decision
-> DB mutation / exit-engine seed / diagnostic
```

At minimum audit all callers of `_broker_position_qty()`.

For each changed function report:

```text
caller
-> validation
-> state read
-> authority resolution
-> mutation
-> return classification
-> downstream consumer
```

No caller may continue using the lossy resolver for money-path or durable-state authority after a new strict helper is added.

---

## CLOSED-POSITION REPAIR REQUIREMENTS

The current `CLOSED + quantity_remaining>0` repair path must satisfy:

```text
successful complete authoritative broker snapshot
+ exact contract identity
+ exact quantity classification
-> repair may proceed according to existing policy
```

Otherwise:

```text
UNAVAILABLE / MALFORMED / AMBIGUOUS
-> HOLD / manual-review diagnostic
-> quantity_remaining unchanged
-> close_source unchanged by flatten branch
-> zero position lifecycle mutation
```

### Broker still OPEN

If exact broker truth proves the contract is still held, existing restore semantics may continue only with a valid positive integral long quantity.

### Broker exactly FLAT

Only a successful complete authoritative snapshot proving no exact matching broker position may reach the current flatten repair.

The absence proof must not come from a swallowed adapter exception.

---

## FAILURE-TIMING MATRIX

Test these boundaries:

```text
before Tradier request
transport exception
HTTP/auth error
malformed positions envelope
malformed position row
quantity conversion failure
successful complete empty snapshot
successful complete target-open snapshot
snapshot succeeds but exact target row malformed
snapshot succeeds with duplicate/conflicting target rows
DB update failure after valid authority
restart after valid authority before mutation
restart after durable mutation
```

A failure before authoritative snapshot completion must never be remembered as flat proof across retry/restart.

---

## REQUIRED BEHAVIORAL TESTS

Structural source tests are secondary.

### Fail-first A: actual non-strict adapter collapse

Reproduce the production adapter behavior:

```text
Tradier `_get()` raises
-> public `list_positions()` returns []
-> feed current reconciler closed-position repair
```

Unpatched main should demonstrate that this can set `broker_truth_available=True` and reach flatten logic.

After the fix:

```text
adapter failure
-> UNAVAILABLE
-> quantity_remaining unchanged
-> no CLOSED_REPAIR write
```

This exact adapter-to-caller behavior is mandatory. A synthetic reconciler exception alone is not enough.

### Fail-first B: lossy quantity

Prove current main transforms at least:

```text
-1 -> 1
1.5 -> 1
```

through the actual production quantity resolver/caller.

### Mandatory quantity cases

1. `quantity=-1` -> reject as long authority.
2. `quantity=1.5` -> reject.
3. `quantity=True` -> reject.
4. `quantity=False` -> reject as quantity authority.
5. `quantity=NaN` -> reject.
6. `quantity=inf` -> reject.
7. `quantity=-inf` -> reject.
8. `quantity=" "` -> reject/missing according to exact contract, never flat proof.
9. `quantity="garbage"` -> reject.
10. `quantity=0` explicit -> preserve as explicit zero, do not fall through to a later contradictory alias.
11. `quantity=0, long_quantity=1` -> contradiction HOLD unless exact broker contract proves precedence.
12. `long_quantity=1, short_quantity=1` -> contradiction/direction HOLD.
13. `short_quantity=1` only -> never positive-long authority.
14. valid `quantity=1` -> OPEN qty 1 positive control.
15. valid numeric string `"1"` -> accept only if current broker transport legitimately emits numeric strings.

### Snapshot cases

16. strict broker transport exception -> no durable mutation.
17. malformed payload -> no durable mutation.
18. malformed unrelated row -> determine completeness contract explicitly; do not silently drop a relevant malformed row and call snapshot complete.
19. duplicate exact OCC rows with conflicting quantities -> HOLD.
20. successful authoritative empty snapshot -> existing exact-flat repair behavior positive control.
21. successful exact OCC qty=1 -> restore/open behavior positive control.
22. wrong OCC only -> does not count as target open, but flat/absence authority requires the snapshot itself to be complete and valid.
23. wrong account -> cannot donate position truth.
24. LIVE/PAPER contradiction -> zero mutation.

### DB mutation failure

25. valid broker truth + DB update failure -> diagnostics/retry, no fabricated success.
26. restart after failed mutation -> reruns from broker truth; does not assume previous repair succeeded.

---

## RUNTIME / RESTART PARITY

The following must resolve identically:

| Scenario | uninterrupted reconciler | restart/recovery |
|---|---|---|
| transport unavailable | HOLD | HOLD |
| malformed quantity | HOLD | HOLD |
| exact OPEN qty=1 | OPEN authority | OPEN authority |
| complete empty snapshot | FLAT authority | FLAT authority |
| conflicting aliases | HOLD | HOLD |
| negative/short target | not long authority | not long authority |

Do not add one strict runtime resolver while restart or repair code still reconstructs quantity with `abs(int(float(...)))`.

---

## MONEY-PATH / DURABLE-STATE ASSERTIONS

For every unavailable/malformed/contradictory broker truth case:

- broker ENTRY POST = 0;
- broker EXIT POST = 0;
- new broker cancel = 0;
- `positions.quantity_remaining` unchanged;
- `positions.status` unchanged by this repair;
- `close_source` not changed to `CLOSED_REPAIR`;
- no proof_trades mutation;
- no queue/result mutation;
- no fabricated fill/economics;
- diagnostic retains client/mode/OCC/reason.

For valid exact-flat positive control, existing intended durable repair may occur once.

For valid exact-open positive control, only the exact target position may be restored/seeded under existing policy.

---

## TEST QUALITY REQUIREMENTS

Do not accept:

- source-string assertions as primary proof;
- a new helper tested in isolation while callers still use the old resolver;
- mocks that bypass `TradierBroker.list_positions()` / strict authoritative seam;
- negative tests without exact OPEN and exact EMPTY positive controls;
- tests where mutation callback is never wired;
- tests that use a friendly fake payload shape absent from production.

At least one PostgreSQL-backed test must execute:

```text
closed durable position with remaining qty
-> production-shaped Tradier adapter failure
-> reconciler repair pass
-> durable row reread
-> unchanged quantity_remaining / close_source
```

And one PostgreSQL-backed positive control must show valid authoritative flat truth can still perform the intended repair.

---

## SCOPE

Preferred production scope:

- `ap_reconciler.py`
- `ap/brokers/tradier.py`

Tests:

- focused strict broker-position truth tests
- closed-position repair regressions
- adapter unavailable-vs-empty regression
- P0 workflow registration if needed

Do not modify:

- exit submit policy (#590 owns the exit-engine callback seam);
- manual-close snapshot completeness (# separate P1);
- scanner/selector/risk/sizing;
- entry submit logic;
- proof taxonomy;
- queue semantics;
- unrelated order quantity parsing unless proven to share the same authority path and separately audited.

---

## RELEASE GATE

**HARD HOLD** until all are true:

1. actual Tradier error->`[]` fail-first is reproduced through the repair caller;
2. actual lossy `-1`/fractional quantity fail-first is reproduced;
3. broker adapter failure cannot become flat authority;
4. malformed/negative/fractional/boolean/non-finite/conflicting quantity cannot become valid long authority;
5. successful complete empty snapshot remains distinguishable and still supports intended flat repair;
6. valid exact OPEN remains supported;
7. every relevant `_broker_position_qty()` caller is migrated/audited;
8. runtime/restart/repair resolve the same authority from the same facts;
9. exact client/mode/account/OCC identity is preserved;
10. exact-head PostgreSQL-backed P0 CI is green;
11. complete final diff and review threads receive independent MERGE audit.

Do not merge or deploy this spec PR.