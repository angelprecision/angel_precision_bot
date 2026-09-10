# P0 — Converge canonical position after exact durable EXIT_FILLED

## STATUS

**SPEC ONLY / HARD HOLD. DO NOT MERGE OR DEPLOY.**

Base: `main@d3c61850df709fe4c399196b9c509f28c9af2a8a`

This PR owns one invariant only:

> When Angel Precision already has exact, durable, broker-confirmed EXIT fill truth for a canonical LIVE position, the canonical `positions` row must converge to that fill exactly once and must stop contributing open exposure.

It must not infer a close from broker-flat truth alone.

---

## September 4 production defect

Jason LIVE QQQ exposed the bug in a money-path-relevant way.

Observed durable order truth:

```text
client_id: jasoncosby1@gmail.com
execution_mode: live
underlying: QQQ
contract: QQQ260904C00719000

ENTRY
local_order_id: 07a16fb9-1317-45ff-934c-d7635e7b67d7
broker_order_id: 144692716
status: FILLED
fill_price: 1.48
qty: 1

EXIT
local_order_id: 5eb5a294-4914-489d-98d5-67659f17ddc6
broker_order_id: 144698172
status: EXIT_FILLED
fill_price: 1.17
qty: 1
filled_ts: 2026-09-04 14:42:34.680787+00
```

Yet the canonical/local position remained:

```text
status: OPEN
qty: 1
avg_fill: 1.48
execution_mode: live
```

That stale OPEN row continued contributing `$148` of exposure after the broker exit was already exactly filled.

Downstream consequence later that same session:

```text
stale QQQ $148
+ BMY selected cost $168
= $316 projected exposure

stale QQQ $148
+ NEE selected cost $127
= $275 projected exposure
```

Combined with the independent sector-identity bug, those stale dollars helped falsely suppress valid LIVE BMY and NEE entries.

The exit itself was already broker-confirmed. The failure is **durable fill -> canonical position projection/convergence**.

---

## Relationship to pending PRs

### #566

#566 owns broker-flat/manual-exit recovery and exact Tradier fill adoption. Its amended path correctly refuses to manufacture `CLOSED` from broker-flat truth and can adopt exact tagged broker `FILLED` evidence into OSM.

This PR is downstream and narrower:

```text
#566 / normal monitor / reconciler
    -> exact durable EXIT_FILLED exists
    -> THIS PR guarantees canonical position convergence
```

Do not duplicate #566's broker lookup, broker tag adoption, cancel, replacement, or submit authority.

### #488 / #428

Older PRs describe adjacent historical versions of this invariant, but they are stale, broad, and not acceptable as current-main implementation vehicles. Rebuild only the still-reproducing September 4 seam on this clean base.

If fail-first replay on the eventual implementation base proves current main plus #566 already converges the exact QQQ shape, STOP and close this PR as obsolete/no-code. Do not invent a patch to justify the PR.

---

## Required exact code trace before implementation

For every candidate implementation location, trace the actual production caller through downstream mutations:

```text
broker/order monitor/recovery/reconciler
-> exact EXIT fill classification
-> OSM EXIT_FILLED durable transition
-> fill economics authority
-> canonical position identity resolution
-> position quantity/status projection
-> exit_ts / exit economics projection
-> proof_trades consumer
-> exposure snapshot consumer
-> restart reread
```

The implementation must identify **one canonical convergence seam**. Do not add multiple independent writers that can disagree.

A correct helper in isolation is not enough.

---

## Binding identity authority

A position mutation is permitted only when all required identity dimensions are proven and non-conflicting:

- `position_id` / canonical position owner;
- `client_id`;
- `execution_mode`;
- exact OCC contract;
- EXIT local order ID;
- EXIT broker order ID;
- EXIT kind;
- exact durable `EXIT_FILLED` state;
- exact filled quantity/cumulative fill authority;
- exact positive fill price;
- exact broker execution timestamp;
- lifecycle/materialization generation if present in the production row.

Missing, blank, malformed, conflicting, stale, or ambiguous identity must HOLD with **zero position mutation**.

Do not wildcard-match by OCC alone.

Do not cross PAPER/LIVE.

Do not allow a synthetic/broker-repair owner to overwrite a different exact canonical position unless the existing canonical convergence/adoption invariant proves that identity.

---

## Required state behavior

### Full EXIT fill

For a one-contract QQQ production shape:

```text
position OPEN qty/remaining 1
+ exact EXIT_FILLED cumulative qty 1
-> canonical position quantity_remaining = 0
-> canonical position terminal status through the existing canonical close vocabulary
-> exact exit fill economics persisted
-> exact exit timestamp persisted
-> position disappears from open exposure
-> proof flow receives no duplicate event
```

Use the repository's existing canonical terminal vocabulary and existing close/proof helpers where valid. Do not invent a second CLOSED taxonomy.

### Partial EXIT fill

```text
position remaining 3
+ exact cumulative EXIT fill advances 1 -> 2
-> consume delta 1 only
-> remaining becomes 2
-> position stays open
-> duplicate replay of cumulative 2 consumes zero
```

Cumulative broker fills must never be compared directly to an already-reduced local remainder without a watermark.

### Duplicate replay

Repeated `EXIT_FILLED` or restart replay of the same cumulative fill must be idempotent:

- no second quantity decrement;
- no second proof trade;
- no second realized P&L application;
- no second queue/result terminalization;
- no broker call.

### Broker flat without exact fill

```text
broker exact flat
+ no exact external fill economics
-> NO fabricated CLOSED
-> NO fabricated fill price
-> NO fabricated timestamp
-> reconciliation pending / HOLD through existing semantics
```

This invariant from #566 is non-negotiable.

---

## Failure timing matrix

Execute behavioral tests for process death or durable write failure at every meaningful boundary:

1. after broker reports FILLED but before OSM `EXIT_FILLED` persists;
2. after OSM `EXIT_FILLED` persists but before canonical position mutation;
3. after position quantity mutation but before terminal status/economics projection if those are separate operations;
4. after position terminalization but before proof write;
5. after proof write but before queue/result cleanup;
6. after cleanup but before in-memory owner hydration;
7. restart after each boundary.

Required result: restart converges to one canonical final state without replaying money mutations.

Where possible, make the position projection atomic with its exact authority check. If existing architecture requires multiple writes, prove durable idempotency between them.

---

## Data-corruption matrix

Behavioral tests must cover:

- missing `client_id`;
- wrong `client_id`;
- missing `execution_mode`;
- PAPER row with LIVE fill evidence;
- LIVE row with PAPER fill evidence;
- missing OCC;
- malformed OCC;
- same underlying but different OCC;
- same OCC on ambiguous duplicate local positions;
- missing EXIT local order ID;
- wrong EXIT local order ID;
- missing broker order ID;
- wrong broker order ID;
- `status=EXIT_FILLED` with missing fill price;
- zero fill price;
- negative fill price;
- missing filled timestamp;
- naive/ambiguous timestamp;
- missing filled quantity;
- zero quantity;
- negative quantity;
- fractional quantity;
- conflicting quantity aliases;
- cumulative fill regression;
- `quantity_remaining=NULL` production shape;
- legacy row with `qty` authority only;
- column/meta disagreement;
- stale generation;
- synthetic position ID plus exact canonical row present;
- duplicate EXIT rows for the same position.

Every malformed/ambiguous case must fail closed before economic mutation.

---

## Runtime / restart / recovery parity matrix

The same exact fill truth must resolve identically whether observed by:

| Scenario | Normal order monitor | Restart recovery | Autonomous/manual fill adoption |
|---|---|---|---|
| full exact EXIT fill | same canonical close | same canonical close | same canonical close |
| partial exact fill | same delta | same delta | same delta |
| duplicate cumulative fill | zero delta | zero delta | zero delta |
| client mismatch | HOLD | HOLD | HOLD |
| mode mismatch | HOLD | HOLD | HOLD |
| OCC mismatch | HOLD | HOLD | HOLD |
| missing economics | HOLD | HOLD | HOLD |
| exact broker flat, no fill | no close | no close | no close |

Do not accept prose claiming they use the same concept. Tests must assert resolved values and mutations.

---

## Money-path safety

This PR must add **zero new broker authority**.

Required assertions:

- zero broker POST calls on every test path;
- zero broker cancel calls;
- zero replacement authority;
- no position mutation before exact fill authority;
- exactly one position mutation for a new valid fill delta;
- no proof_trades write before exact fill;
- no duplicate proof on restart;
- no queue/result corruption;
- no change to scanner, Master Control, selector, score, sizing, trigger, stop, target, or entry policy.

Could this make Jason trade junk? **No.** It changes no entry eligibility. It only prevents already-closed exposure from remaining phantom-open and suppressing future valid trades.

---

## Required fail-first production replay

Before changing production code, reproduce the September 4 QQQ shape with a production-shaped PostgreSQL-backed test if the existing P0 fixture permits it:

```text
canonical LIVE position QQQ260904C00719000 OPEN qty=1
exact ENTRY FILLED @ 1.48
exact EXIT EXIT_FILLED @ 1.17 qty=1 with broker timestamp
-> current code leaves position OPEN   [fail-first]
```

Then the same replay must prove after the fix:

```text
-> canonical position terminal/remaining=0
-> exact economics retained
-> open exposure excludes the $148 entry capital
-> a subsequent unrelated BMY/NEE exposure calculation cannot see QQQ as open
```

The exposure assertion is downstream evidence, not permission to modify Master Control in this PR.

---

## Test quality requirements

Do not use `inspect.getsource()` as primary evidence.

Mocks may isolate broker calls, but at least one test must execute the actual SQL/update seam against the project's PostgreSQL-backed/driver-faithful fixture.

Inspect assertions for encoded bugs:

- tests must not merely assert a callback was called;
- tests must reread the durable position;
- negative tests need a positive exact-fill control;
- restart tests must reconstruct runtime state rather than reusing the same object;
- tests must prove zero broker calls, not just omit a broker mock;
- no broad monkeypatch may bypass the convergence guard under test.

---

## Expected production scope

Keep implementation surgical. Preferred scope is the existing canonical EXIT-fill projection/close helper and its direct caller only.

Potential files must be justified by fail-first trace. Do not assume these all need edits.

Allowed only if proven necessary:

- canonical OSM fill transition consumer;
- reconciler exact-fill convergence helper;
- one canonical position-close/projection helper;
- focused P0 tests;
- P0 workflow registration.

Explicitly out of scope:

- `ap_master_control.py`;
- sector mapping;
- entry watcher;
- contract selector;
- broker submit/cancel implementation;
- exit thresholds;
- scanner;
- sizing;
- intelligence;
- unrelated migrations.

---

## Merge gate

Final implementation verdict must remain **HARD HOLD** until all of the following are true:

1. exact September 4 QQQ fail-first replay demonstrated;
2. current-main implementation is surgical;
3. full changed-file audit completed;
4. callers/downstream mutations traced;
5. normal/restart/adoption parity demonstrated;
6. PostgreSQL-backed behavioral replay passes;
7. partial-fill and duplicate replay pass;
8. malformed/ambiguous authority tests fail closed;
9. exact broker-flat-without-fill negative control proves no fabricated close;
10. zero broker submit/cancel regression;
11. exact-head blocking P0 CI green;
12. independent final re-audit gives MERGE.
