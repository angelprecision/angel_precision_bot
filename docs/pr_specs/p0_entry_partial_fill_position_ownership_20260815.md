# P0 SPEC — ENTRY partial fills must become managed positions immediately

## Status

**SPEC ONLY / HARD HOLD. DO NOT MERGE OR DEPLOY AS AN IMPLEMENTATION.**

This PR is intentionally stacked on PR #473 (`spec/p0-filled-entry-restart-handoff-recovery-20260814`) because #473 already owns adjacent FILLED-entry handoff logic in `ap/fill_monitor.py`. Do not implement this from stale `main` and then manually splice the two versions together.

After #473 merges, rebase/retarget this PR to the resulting `main` and rerun the complete focused + adjacent suites.

## Proven production defect

Current fill-monitor behavior is asymmetric:

```text
ENTRY FILLED
  -> OSM FILLED
  -> _open_position_safe(...)
  -> durable position
  -> bind filled-entry identity
  -> seed exit engine

ENTRY PARTIAL_FILL
  -> OSM PARTIAL_FILL / apply_fill_update
  -> audit ORDER_PARTIAL
  -> return
```

The partial branch contains canonical accounting only for `kind == EXIT`. There is no corresponding ENTRY position creation/update path.

Therefore a broker-confirmed partial ENTRY fill creates real long option exposure before AP has a durable managed position for that exposure.

Example:

```text
requested qty = 10
broker cumulative fill = 4
orders.status = PARTIAL_FILL
orders.filled_qty = 4
broker holds 4 contracts
positions row = absent
exit engine owner = absent
```

That gap exists immediately; it does not require the remainder to be canceled to be dangerous.

## Binding invariant

**Every broker-confirmed positive ENTRY fill quantity must have exactly one durable managed-position owner before fill processing returns.**

The position must represent cumulative entry economics without double-counting replays and without resurrecting contracts already exited while the entry order is still partially working.

Required durable relationship:

```text
position.qty                 = cumulative ENTRY contracts filled
position.avg_fill            = broker-confirmed cumulative average ENTRY fill price
position.quantity_remaining  = previous remaining + newly-added ENTRY fill delta
```

The third line is deliberate. Do NOT simply set `quantity_remaining = cumulative_filled_qty` on later entry fills, because the exit engine may have already closed some of the earlier partial quantity.

## Canonical cumulative-fill semantics

Treat broker `filled_qty` from fill monitor as cumulative.

For one logical entry order:

```text
previous durable entry qty = position.qty
new broker cumulative qty  = new_filled
entry_delta                 = new_filled - previous durable entry qty
```

Rules:

- `entry_delta < 0` -> HOLD/anomaly; never reduce position from an ENTRY callback.
- `entry_delta == 0` -> idempotent replay; no economic mutation.
- `entry_delta > 0` -> add delta to total entered quantity and remaining open quantity.
- cumulative average fill must be finite, positive, and broker-confirmed.
- contract/client/mode/local-order identity must match exactly before an existing position is expanded.

## Required architecture

Do not create a second partial-position implementation inside fill monitor with raw SQL.

Add/reuse one PositionManager-owned cumulative-entry-fill primitive and call it from both ENTRY `PARTIAL_FILL` and ENTRY `FILLED` handoff as needed.

Preferred semantic API (name may differ if an equivalent primitive already exists after #473):

```python
APPositionManager.apply_entry_fill_cumulative(
    *,
    local_order_id,
    broker_order_id,
    plan_id,
    signal_id,
    contract,
    side,
    execution_mode,
    cumulative_filled_qty,
    cumulative_avg_fill,
    filled_ts,
    stop_underlying,
    target_underlying,
    ...
) -> result
```

The method must serialize on the strongest execution identity (`local_order_id` / broker order id) using the same advisory-lock / row-lock philosophy already used by `open_position()`.

### First positive fill

If no position exists for the exact entry execution:

- create one OPEN managed position with qty = cumulative fill;
- quantity_remaining = cumulative fill;
- preserve exact client/mode/contract/side/order identities;
- return the position id.

### Later cumulative fill

If the exact position exists:

- lock/read current row;
- prove same client, mode, contract, side, local order and broker identity where present;
- compute `entry_delta = cumulative_filled_qty - current position.qty`;
- reject regression;
- on positive delta:
  - `qty = cumulative_filled_qty`;
  - `quantity_remaining = existing quantity_remaining + entry_delta`;
  - `avg_fill = broker cumulative avg fill`;
  - preserve status as active (`OPEN`/current legitimate active state), never overwrite CLOSING/CLOSED blindly;
- on zero delta: idempotent no-op.

If the position is terminal but the same entry broker order later reports a larger cumulative entry fill, do not silently reopen it. HOLD/quarantine with a dedicated anomaly because broker/order chronology must be reconciled explicitly.

## Fill-monitor integration

### ENTRY `PARTIAL_FILL`

After OSM successfully applies the cumulative partial fill:

1. call the canonical cumulative-entry-fill position primitive;
2. durably bind the order->position identity using the same #473 owner-handoff contract;
3. ensure the exact position is seeded/registered with the exit engine before returning;
4. do not release full-entry guards merely because a partial fill occurred if those guards are needed to protect remaining broker ownership; preserve current reservation policy unless its exact arithmetic must be adjusted for real filled exposure;
5. do not pair-cancel/recreate broker mutations unless #473's reviewed contract explicitly requires them for partial fills. Partial entry ownership is the job; new broker side effects are not.

### ENTRY `FILLED` after earlier partial

The terminal FILLED path must update the existing partial position to the final cumulative quantity rather than relying on `open_position()` idempotency returning the existing id unchanged.

Example:

```text
partial 4 @ cumulative avg 1.00 -> position qty 4 / remaining 4
exit engine closes 1           -> position qty 4 / remaining 3
entry later reaches 10 @ 1.08  -> position qty 10 / remaining 9
```

Anything that sets remaining to 10 in that example is wrong because it resurrects the already-closed contract.

## Interaction with PR #473

#473 owns restart-safe FILLED-entry identity handoff, pair-resolution provenance, and exit-engine seeding.

This PR must reuse those canonical bind/seed primitives rather than fork them.

Required integration cases after implementation:

- first partial fill -> crash -> restart -> exactly one managed owner for filled quantity;
- partial -> terminal fill -> crash at each owner-handoff boundary -> one owner, correct cumulative qty;
- replay duplicate partial callback -> no duplicate position;
- replay old lower cumulative qty -> anomaly/HOLD;
- #473 recovery logic must not treat an already-managed partial position as a missing fresh FILLED position and duplicate it.

## Expected production files

Because this is stacked on #473, expected scope is:

- `ap/fill_monitor.py`
- `ap/position_manager.py`

If a small shared #473 handoff helper file must be touched to support an existing primitive, document it before editing. Do not expand into broker adapter, queue, scanner, selector, entry watcher, exit-decision policy, or proof taxonomy.

## Required tests

Create `tests/test_p0_entry_partial_fill_position_ownership.py`.

Minimum production-shaped cases:

1. 10 requested, cumulative partial 4 -> one position qty=4 remaining=4.
2. duplicate callback cumulative 4 -> same position, no quantity change.
3. cumulative 4 -> 7 -> same position qty=7 remaining=7 if no exits occurred.
4. cumulative 4, one exit closes -> remaining 3, then cumulative entry reaches 7 -> qty=7 remaining=6.
5. cumulative partial 4 -> terminal cumulative 10 -> same position qty=10, no duplicate.
6. partial 4 -> exit 1 -> terminal 10 -> remaining 9.
7. broker cumulative regression 7 -> 4 -> zero position mutation, anomaly visible.
8. cumulative over order qty -> existing fill-monitor overfill policy preserved; no extra position exposure manufactured.
9. missing/zero/nonfinite avg fill -> no economic position mutation.
10. invalid side -> fail closed.
11. client mismatch -> fail closed.
12. execution_mode mismatch -> fail closed.
13. contract mismatch -> fail closed.
14. local_order_id mismatch -> fail closed.
15. conflicting broker_order_id -> fail closed/HOLD.
16. first partial creates order->position durable linkage.
17. first partial seeds exactly one exit-engine owner.
18. duplicate partial cannot seed a second owner.
19. restart after first partial rehydrates exactly one owner with exact remaining qty.
20. terminal FILLED after partial reuses same owner.
21. terminal position receiving later higher cumulative entry fill -> HOLD; no silent reopen.
22. PARTIAL_FILL then broker CANCELED leaves filled position managed and open with exact filled qty.
23. PARTIAL_FILL then broker EXPIRED leaves filled position managed.
24. no pre-fill proof trade finalization.
25. no new broker submit/cancel/standing-stop/pair-cancel authority is introduced by the partial branch unless separately proven as required by #473 semantics.

Run all #473 focused tests plus existing fill monitor / position manager / partial-close / exact owner-handoff suites.

## Money-path audit

- Live behavior: **YES.** Real partial fills become managed immediately.
- Broker submit/cancel: **NO new authority.**
- Orders: PARTIAL_FILL already mutates; add only durable position linkage required for ownership.
- Positions: **YES**, this is the intended correction.
- proof_trades: no terminal proof mutation from entry partial fill.
- queue: none.
- client_id/execution_mode: exact equality required.
- diagnostics: preserve cumulative broker qty, previous durable qty, delta, avg fill, owner/bind/seed result.
- PAPER/LIVE taxonomy: exact and isolated.
- Could this make Jason trade junk? It does not select or enter anything new. It ensures any contracts the broker already filled are visible to AP's protection machinery.

## Claude implementation instruction

Work on this stacked branch only after reading #473's current diff and review state. Do not start from the old f26d31c version of fill monitor and overwrite #473 changes.

First add a failing test proving `PARTIAL_FILL` with qty 4 leaves no position. Then design the cumulative PositionManager primitive so partial and later terminal fills share one economic owner.

Before requesting review report:

1. exact #473 head/base used;
2. changed functions and why each is necessary;
3. cumulative quantity arithmetic including the partial-exit interleaving case;
4. idempotency/concurrency proof;
5. crash/restart ownership proof;
6. exact client/mode/contract/order identity fences;
7. broker mutation count proof;
8. focused + full adjacent test counts;
9. exact-head CI SHA;
10. fresh MERGE / HOLD / HARD HOLD recommendation.

Do not merge, deploy, change environment variables, or mutate production data.