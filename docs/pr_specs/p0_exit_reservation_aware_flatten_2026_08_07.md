# P0 — Reservation-aware exit quantity, manual flatten, and broker/DB reconciliation

**Status: IMPLEMENTATION CONTRACT ONLY / HARD HOLD.**

This is the second half of the 2026-08-07 AVGO P0. It must remain separate from the stale-order retry-liveness repair so each money-path change can be audited independently.

## Incident requiring this change — 2026-08-07 AVGO PAPER

AVGO held 7 contracts. A prior Angel Precision scale-out SELL_TO_CLOSE 2 @ approximately $2.49 remained working at the broker. Because those two contracts were reserved by the open exit, only 5 contracts were actually available for another sell-to-close.

An operator later attempted to flatten all 7 contracts. Angel Precision’s current pre-submit broker quantity guard accepted the request because broker `list_positions()` still reported total long quantity = 7. The guard does not account for live working SELL_TO_CLOSE reservations. Tradier then rejected SELL 7 because only 5 were available.

The result was a trapped position during a major profit retracement. This exact incident must remain in the implementation and regression rationale.

## Confirmed current-code gap

`ap/exit_safety.py::resolve_exit_broker_truth()` reads `broker.list_positions()` and returns `broker_truth_open_qty`. It does not inspect broker open orders and therefore does not compute working exit reservation quantity or currently available-to-close quantity.

`APExitEngine._submit_exit_decision()` blocks only when requested quantity exceeds total `broker_truth_open_qty`. For AVGO:

```text
broker open position qty = 7
working SELL_TO_CLOSE qty = 2
broker available to close = 5
requested EXIT ALL qty = 7
current guard: 7 <= 7 -> allowed
broker: rejects because available is 5
```

`emergency_flatten()` also constructs `CLOSE_ALL` using `quantity_remaining`, so an operator/emergency flatten can bypass duplicate-resubmit protection and still request total position quantity while a prior working exit reserves part of it.

This must be fixed at the shared broker-truth/submit seam, not by teaching the dashboard to guess quantities.

## Canonical quantity model

Every exit attempt must distinguish four quantities:

```text
broker_open_qty
working_exit_remaining_qty
available_to_close_qty = max(0, broker_open_qty - working_exit_remaining_qty)
db_confirmed_remaining_qty
```

Definitions:

- `broker_open_qty`: exact current long position from broker position snapshot.
- `working_exit_remaining_qty`: sum of still-live SELL_TO_CLOSE quantities for this exact account + contract after subtracting broker-confirmed cumulative fills on those orders.
- `available_to_close_qty`: quantity the broker can accept on a new SELL_TO_CLOSE right now.
- `db_confirmed_remaining_qty`: local quantity reduced only by confirmed fill/reconciliation truth, never merely by submission or reservation.

A working order reserves quantity. It does **not** count as a fill.

## Required behavior

### Normal new exit

Before any broker SELL_TO_CLOSE submit:

1. resolve exact broker open position quantity;
2. resolve exact matching live SELL_TO_CLOSE orders for the account/contract;
3. calculate remaining reserved quantity using broker order qty minus cumulative filled qty;
4. calculate available-to-close;
5. compare requested quantity to available quantity;
6. if an existing exact Angel Precision exit already owns the requested tranche, do not duplicate it;
7. if a different exit action needs additional contracts, cap/route only the unreserved quantity where strategy semantics permit;
8. otherwise require cancel/replace of the existing order first.

### Operator / emergency `EXIT ALL`

`EXIT ALL` means **flatten the broker position**, not literally “submit current DB qty regardless of existing exits.”

Preferred sequence:

```text
resolve broker position + matching live exits
-> cancel matching Angel Precision exits by exact identity
-> prove each cancel terminal
-> re-read broker position and open exits
-> apply any fills that occurred during cancellation
-> submit SELL_TO_CLOSE for exact broker remaining open qty
-> continue reconciliation until broker position is flat
```

If cancellation cannot be proven:

- do not submit overlapping quantity;
- optionally sell only exact unreserved `available_to_close_qty` if the existing safety policy allows concurrent disjoint tranches;
- otherwise report a precise partial-flatten blocked state and keep protective recovery active;
- never repeatedly blast SELL 7 into a broker that only has 5 available.

For the AVGO shape, either safe result is acceptable depending on implementation ownership:

A. cancel old SELL 2, prove cancel, re-read broker qty=7, SELL 7; or
B. leave exact valid SELL 2 working and SELL only 5, then continue managing the remaining 2.

The implementation must choose one deterministic authority. For operator/emergency EXIT ALL, prefer A because it produces one flatten owner and eliminates competing sell orders.

## Exact production changes Claude should implement

### 1. `ap/exit_safety.py` — add reservation-aware broker exit truth

Extend broker truth without breaking existing callers. Do not silently redefine `broker_truth_open_qty`.

Add fields conceptually equivalent to:

```python
{
    "broker_truth_open_qty": 7,
    "working_exit_reserved_qty": 2,
    "available_to_close_qty": 5,
    "matching_working_exit_orders": [...],
    "is_fresh_exact": True,
    "reservation_truth_exact": True,
    "audit": {...},
}
```

Requirements for matching open exit orders:

- exact broker account identity when supplied by broker payload;
- exact normalized OCC contract;
- exit-side instruction must prove SELL_TO_CLOSE / equivalent broker shape;
- only live statuses count: open/pending/accepted/submitted/queued/working/acknowledged/partially_filled and broker-specific equivalents already normalized elsewhere;
- reserved remaining quantity = order quantity - cumulative filled quantity, bounded at >=0;
- unknown/malformed quantity on a matching live exit makes reservation truth **unknown**, not zero;
- broker open-order API failure makes `reservation_truth_exact=False`; do not manufacture availability.

Reuse broker-shape extraction helpers from `ap/exit_autonomous_recovery.py` only if doing so does not create circular imports. Prefer extracting a small shared pure helper only if necessary; do not build a new lifecycle subsystem.

### 2. `ap_exit_engine.py` — enforce `available_to_close_qty` at the final submit seam

Immediately before the external `on_exit` / `on_scale` callback:

- require exact reservation truth for any quantity-sensitive exit where broker open orders may reserve contracts;
- attach reservation audit to diagnostics;
- do not treat total position qty as synonymous with submit-available qty;
- if `decision.quantity > available_to_close_qty`, do not send the impossible quantity.

For normal scale/profit exits:

- if an exact active exit order already represents the tranche, retain ownership and wait/retry through PR #423’s lifecycle instead of creating a duplicate;
- if disjoint quantity remains and strategy semantics explicitly allow another tranche, submit only that unreserved quantity with a diagnostic explaining the cap;
- default to no duplicate/overlap.

For forced-risk / operator flatten, route to the dedicated cancel-existing-then-flatten behavior below.

### 3. `ap_exit_engine.py` — make `emergency_flatten()` reservation-aware

Do not let `allow_inflight_override=True` mean “ignore broker reservations.”

Implement an exact flatten preparation step before the replacement SELL:

```python
prepare_exit_all(pos)
```

or equivalent local helper. It should:

- resolve exact broker position + live exit reservations;
- identify matching Angel Precision exits using pending local/broker identity plus broker order identity;
- cancel exact known working exits;
- prove terminal cancellation;
- apply any newly discovered fills before calculating final qty;
- re-read broker position/open orders;
- submit the exact remaining broker-open qty;
- preserve client_id/execution_mode throughout.

No contract-only cancellation guess in LIVE. If the pending broker id is missing and multiple matching exits exist, fail closed to existing fuzzy-reconciliation owner rather than canceling arbitrary orders.

### 4. `ap_execution_core.py` — preserve submit metadata and do not re-expand quantity

Audit `_on_position_close` and `_on_position_scale`, the callbacks wired into `APExitEngine`, to ensure they do not recompute quantity from original `pos.quantity` or stale DB state after the exit engine has computed a reservation-safe quantity.

The callback must consume the exact `ExitDecision.quantity` produced by the final broker-truth gate and preserve:

- client_id
- execution_mode
- position_id
- contract
- exit reason/action
- retry/replacement generation

If execution core already does this, add regression coverage rather than changing production code.

### 5. DB / OSM / fill reconciliation — retain submitted != filled

No local position quantity may be reduced because an exit was submitted, acknowledged, canceled, or reserved.

Only these may reduce `quantity_remaining`:

- broker-confirmed cumulative exit fill through existing `note_partial_exit_fill` / OSM/fill-monitor path;
- exact broker-position reconciliation that proves a lower open quantity.

When a working SELL 2 exists against a 7-contract broker position:

```text
positions.quantity_remaining = 7  # until fills prove otherwise
working_exit_reserved_qty = 2
available_to_close_qty = 5
```

If 1 of the 2 fills:

```text
broker_open_qty = 6
old order cumulative_filled = 1
old order remaining_reserved = 1
available_to_close_qty = 5
DB confirmed remaining = 6
```

The system must not infer `DB remaining=5` merely because another 1 is still reserved.

### 6. Rejection circuit breaker interaction

A broker rejection caused by an impossible overlapping quantity should become impossible after this fix. Add a specific diagnostic if broker nevertheless returns an insufficient-available-quantity rejection despite exact reservation preflight.

Do not allow repeated self-generated quantity rejections to trip the generic exit circuit breaker and permanently halt protective exits without preserving the reservation mismatch evidence.

## Production metadata shape requirements

Do not assume Tradier open-order payloads use one field spelling. Current repository code already sees variants such as:

- order identity: `broker_order_id`, `order_id`, `id`, `orderId`
- contract: `contract`, `symbol`, `option_symbol`, `instrument`
- quantity: `qty`, `quantity`, `order_qty`, `remaining_qty`, `remaining_quantity`
- fill quantity: `filled_qty`, `filled_quantity`, `cumulative_filled_qty`, `exec_quantity`
- side/instruction: `side`, `action`, `instruction`, `order_action`, `transaction_type`, `trade_action`
- status: `status`, `Status`, `state`, `order_status`

Use the real adapter shape discovered in tests/fixtures and retain defensive variants. Never assume a missing field means zero reservation.

## Scope lock

Expected production files:

1. `ap/exit_safety.py`
2. `ap_exit_engine.py`
3. `ap_execution_core.py` only if the callback currently re-expands/corrupts the safe quantity

Existing OSM/fill/reconciler files should change only if a focused test proves they currently mutate quantity on submission/reservation or cannot apply the necessary exact broker truth. If that happens, stop and document the dependency before broadening.

Do NOT change:

- scanner/intelligence/admission;
- contract selection;
- entry path;
- queue;
- sizing policy;
- profit target or stop thresholds;
- proof-trade taxonomy;
- PAPER/LIVE decision pricing authority.

## Required AVGO regression replay

Suggested test file:

`tests/test_p0_exit_reservation_aware_flatten_avgo.py`

Minimum cases:

1. broker position 7 + working STC 2 -> truth reports open=7, reserved=2, available=5.
2. requested normal SELL 7 with reserved=2 -> zero impossible SELL 7 calls.
3. operator EXIT ALL -> cancel exact old SELL 2; after confirmed cancel and broker re-read, submit SELL 7.
4. cancel not confirmed -> zero overlapping SELL 7.
5. safe alternative path, if implemented: old SELL 2 retained -> submit at most SELL 5.
6. partial old order: order qty 2, cumulative filled 1, broker position 6 -> reserved=1, available=5.
7. malformed matching order qty -> reservation truth unknown and new overlapping submit blocked.
8. open-order API failure -> reservation truth unknown, no quantity guess.
9. unrelated BUY_TO_OPEN/SELL_TO_OPEN order does not reserve exit quantity.
10. same OCC contract in a different broker account/client is excluded.
11. PAPER working order cannot reserve LIVE account quantity and vice versa.
12. canceled/rejected/filled/expired exit orders reserve zero.
13. multiple working exits sum their remaining reservations exactly.
14. broker position absent from a successful fresh positions snapshot -> exact flat; no sell submitted.
15. DB says 7 but broker says 6 -> broker qty wins for submit quantity; DB is reconciled only through exact truth path.
16. DB says 5 but broker says 7 after stale local state -> emergency flatten can still close broker truth without fabricating local fills.
17. submitted/acknowledged working exit does not decrement DB `quantity_remaining`.
18. partial fill decrements DB/in-memory exactly once.
19. late duplicate fill callback does not double decrement.
20. full operator flatten reaches broker-flat state and clears local position only after broker confirmation.
21. `client_id` preserved through every resolver/cancel/submit call.
22. `execution_mode` preserved; PAPER/LIVE taxonomy never coalesces.
23. rejection diagnostics carry total/reserved/available/requested quantities.
24. proof_trades remain fill-confirmed only; no proof written from submission/cancel.
25. queue and entry orders are untouched.

## Diagnostics required

Every quantity decision should expose:

- client_id
- execution_mode
- position_id
- contract
- broker account id
- broker_open_qty
- working_exit_reserved_qty
- available_to_close_qty
- requested_exit_qty
- final_submit_qty
- matching working exit broker ids/statuses/remaining qty
- reservation_truth_exact
- snapshot/check timestamp
- cancel intent/result/confirmed state for EXIT ALL
- broker qty after cancel/re-read
- final flatten outcome

Do not overwrite existing metadata blobs. Merge a nested `exit_quantity_truth` / `exit_flatten_audit` object if persistence is required.

## Review gate

This change directly affects active broker SELL_TO_CLOSE submit quantity and emergency broker CANCEL behavior.

### HARD HOLD conditions

- total broker position quantity is still used as available quantity when live exit reservations exist;
- `allow_inflight_override` can still submit overlapping quantity;
- EXIT ALL can repeatedly submit an impossible quantity;
- cancel response is trusted without terminal broker confirmation;
- a reservation can decrement DB position quantity before fill;
- partial fill can be double-counted;
- missing/malformed open-order quantity becomes zero availability reservation silently;
- client_id/execution_mode/account identity is not exact;
- PAPER/LIVE orders can contaminate each other;
- incident replay is absent;
- exact-head P0 CI is not green.

### Required final verdict

`MERGE`, `HOLD`, or `HARD HOLD` with explicit answers to:

- Does this change live behavior?
- Is it flag-off or active?
- Does it touch broker submit/cancel?
- Does it mutate orders, positions, proof_trades, queue?
- Does it preserve client_id / execution_mode?
- Does it use real production metadata shape?
- Does it preserve diagnostics downstream?
- Could it pollute PAPER/LIVE taxonomy?
- Could it make Jason trade junk?
