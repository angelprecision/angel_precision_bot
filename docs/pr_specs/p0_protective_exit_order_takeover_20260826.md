# P0 — Protective broker sell takeover before canonical EXIT submit

Date: 2026-08-26
Base: `main@7a0a2f435be33531d03db67653db91426c3415c6`
Status: **SPEC FIRST / HARD HOLD / IMPLEMENTATION REQUIRED / DO NOT MERGE OR DEPLOY**

## Incident

A LIVE position in `NOW260828P00122000` opened with quantity 1 and entry fill about $1.54.

Immediately after the confirmed ENTRY fill, `ap/fill_monitor.py` placed a broker-side GTC protective stop:

```text
2026-08-26 13:51:19Z
Standing stop placed @ $1.08
broker_stop=143387714
status=ok
```

Later the exit engine correctly generated repeated `CLOSE_ALL` decisions. Every ordinary `sell_to_close` submit was rejected by Tradier with:

```text
Sell order is for more shares than your current long position,
please review current position quantity along with open orders for security.
```

The broker still showed one long contract, but the existing GTC stop already reserved that contract. The canonical EXIT path knew the position was open but did not know that broker order `143387714` already owned sell authority.

This produced a live deadlock:

```text
broker long qty = 1
protective GTC sell qty = 1
canonical CLOSE_ALL wants qty = 1
Tradier rejects second sell
exit engine clears in-flight after rejection
circuit breaker sees broker qty still 1 and overrides itself
next CLOSE_ALL retries
same rejection repeats
```

Production reached at least 28 rejected exits while the position remained open.

## Root cause

Current `ap/fill_monitor.py` creates the standing stop directly at the broker, then records its broker order id only in diagnostics/audit output.

The current clean #470-derived implementation does **not** durably bind the protective broker order into canonical order/position lifecycle state that later EXIT code can own, inspect, cancel, or adopt.

Current `ap/order_monitor.py` still contains bounded stale EXIT handling:

```text
TIMEOUT_EXIT_PENDING = 45s
EXIT_CHECK_INTERVAL = 15s
```

But that monitor can act only on canonical EXIT rows it can see. The blocking GTC stop is outside that lifecycle, so the existing stale-exit machinery cannot cancel it.

The repository recovery sequence explains the regression risk:

```text
#507 restored the tree exactly to #460
#513 restored the clean #470 filled-entry identity handoff only
```

Later standing-stop durability work from historical #429 was not restored by #513. Historical #429 is evidence only; do not cherry-pick it wholesale.

## Critical distinction

Do **not** implement a blanket rule that cancels every protective stop after 60–120 seconds merely because it is still open.

A broker protective stop is intentionally long-lived protection. Canceling it solely by age can remove the only broker-side backstop while the position remains open.

The required timeout behavior is:

- ordinary canonical EXIT orders remain subject to the existing bounded stale-exit timeout;
- protective broker orders remain active while no replacement EXIT owns the close;
- when a canonical `CLOSE_ALL` or quantity-reducing EXIT takes ownership, any conflicting protective sell must be resolved first.

The P0 is therefore **exit takeover**, not generic stop aging.

## Required invariant

Before a new LIVE `sell_to_close` broker POST for quantity `Q` may occur on an exact OCC contract:

1. broker long quantity for the exact account/contract is proven;
2. active broker sell orders for the exact account/contract are inspected;
3. any existing sell order that already reserves quantity overlapping `Q` is classified;
4. a canonical already-owned EXIT is adopted/held, not duplicated;
5. an exact protective standing stop that conflicts with the requested EXIT is canceled;
6. cancellation is confirmed terminal by broker truth before replacement submit;
7. broker long quantity is re-read after cancellation;
8. only then may exactly one replacement EXIT submit for no more than current broker long quantity.

If the protective order fills during the cancel race, that broker fill wins. Adopt/reconcile the fill and submit **zero** replacement quantity.

If cancel outcome or broker status is unavailable, malformed, contradictory, or ambiguous, fail closed. Do not submit a second sell.

## Canonical path to modify

Trace the actual current production call chain before editing:

```text
APExitEngine decision
-> canonical exit callback / submit seam
-> APOrderStateMachine EXIT_REQUESTED row
-> exit safety / broker-truth preflight
-> broker.place_order(... side="sell_to_close")
```

The protective-order takeover must live immediately before the irreversible broker EXIT POST, after exact client/mode/position/contract/qty identity is known.

Do not add a second async exit worker.

## Existing broker capabilities to reuse

`TradierBroker` already exposes:

```text
list_orders()
get_order(order_id)
cancel_order(broker_order_id)
list_positions()
```

Reuse these methods. Do not add raw Tradier HTTP calls in exit-engine business logic if the adapter already owns the transport.

`cancel_order()` is a cancel request plus broker re-query. Treat its result as evidence, not magic. Exact terminal truth must still be classified.

## Broker open-order classification

Implementation must normalize real Tradier order shapes and match only the exact account + exact OCC contract.

At minimum inspect fields/aliases for:

```text
id / order_id
status
class
type
side
option_symbol / contract / symbol
quantity / qty
exec_quantity
remaining_quantity if present
duration
tag if present
```

Do not rely on ticker-only matching.

### Active sell statuses

Codex must inventory real statuses already handled by the broker adapter/reconciler and define one canonical active set. Unknown statuses are ambiguous and must not authorize a replacement POST.

### Protective standing stop candidate

A protective candidate must be proven from broker truth, preferably:

```text
exact OCC contract
side == sell_to_close
order type == stop or stop-limit equivalent actually produced by fill_monitor
active broker status
positive remaining quantity
```

If durable metadata from the originating ENTRY contains the exact protective broker id, require that identity when available.

For legacy/current orphan stops where durable id is missing, a broker-discovered exact-contract active stop may be classified as an orphan protective order only when identity is otherwise unambiguous within that broker account.

Multiple conflicting active sells, contradictory quantities, or ambiguous order types => HOLD / reconciliation required, zero new submit.

## Required takeover algorithm

Preferred flow:

```text
EXIT_REQUESTED exact identity
-> fresh broker position snapshot
-> fresh broker order snapshot
-> classify active sell_to_close orders for exact OCC

CASE 1: no active conflicting sell
    -> continue existing submit path

CASE 2: one exact canonical EXIT already active
    -> adopt/hold existing broker ownership
    -> zero duplicate POST

CASE 3: one exact protective stop conflicts
    -> durable takeover intent marker
    -> broker.cancel_order(protective_id)
    -> broker.get_order(protective_id)

       if terminal canceled/rejected/expired with zero fill:
           -> re-read broker position qty
           -> submit replacement EXIT up to proven remaining qty

       if filled/partially filled:
           -> consume fill truth
           -> re-read broker position qty
           -> submit only residual qty if positive and canonical policy still requires it

       if open/pending/unknown/error/malformed:
           -> HOLD
           -> zero replacement POST

CASE 4: multiple or ambiguous active sells
    -> HOLD / reconcile
    -> zero new POST
```

The durable takeover marker must prevent two workers from canceling/submitting concurrently for the same position generation.

## Current incident positive control

Replay exact production shape:

```text
execution_mode = live
contract = NOW260828P00122000
position qty = 1
broker long qty = 1
protective broker order = 143387714
protective side = sell_to_close
protective type = stop
protective qty = 1
protective duration = gtc
canonical CLOSE_ALL qty = 1
```

Before fix:

```text
new sell_to_close submitted
-> Tradier rejects: more shares than current long position / open orders
```

After fix:

```text
list broker orders
-> find exact protective stop 143387714
-> cancel 143387714
-> prove terminal canceled
-> re-read long qty = 1
-> submit exactly one canonical EXIT qty = 1
```

Assertions:

```text
protective cancel count = 1
replacement EXIT submit count = 1
no second concurrent sell
client_id unchanged
execution_mode == live unchanged
position_id unchanged
contract exact OCC unchanged
qty never exceeds proven broker long qty
```

## Mandatory race controls

1. **Stop fills before cancel request**
   - detect terminal fill;
   - re-read broker qty;
   - zero duplicate full-close POST.

2. **Stop fills between cancel request and status re-read**
   - fill wins;
   - consume fill truth;
   - submit residual only if proven > 0.

3. **Cancel request returns success but broker still reports open**
   - HOLD;
   - zero replacement POST.

4. **Cancel transport exception**
   - outcome unproven;
   - re-query exact order;
   - zero submit unless terminal truth becomes proven.

5. **Broker list_orders unavailable/error**
   - fail closed for LIVE replacement takeover;
   - zero submit.

6. **Unknown/malformed order status**
   - HOLD;
   - zero submit.

7. **Two active sell orders on same OCC**
   - do not guess;
   - HOLD / reconciliation required.

8. **Existing canonical active EXIT**
   - do not cancel it as an orphan protective order;
   - adopt/monitor existing ownership.

9. **Partial fill on active sell**
   - quantity math uses proven remaining broker position;
   - never submit original full qty blindly.

10. **Broker position already flat**
    - zero submit;
    - drive existing external-close/flat reconciliation path.

11. **Wrong client or PAPER mode**
    - no cross-account or cross-mode lifecycle mutation.

12. **Same ticker, different OCC**
    - never cancel or adopt by ticker alone.

## Durable protective-order ownership

The hot takeover fixes the live deadlock even for legacy orphan stops, but future stops must not remain audit-only.

Restore the narrow useful part of historical #429 conceptually, without importing unrelated branch history:

After a fresh confirmed ENTRY fill places a standing stop, persist exact protective identity on the originating ENTRY or canonical position metadata:

```text
protective_order_state
protective_broker_order_id
protective_contract
protective_qty
protective_created_at
protective_status
protective_source = standing_stop
execution_mode
client_id
```

Rules:

- persist a durable `PLACEMENT_PENDING` marker before any standing-stop broker POST;
  if that write misses or errors, do not place the stop;
- if the post-identity update fails, retain the pending marker as
  `OUTCOME_UNPROVEN` so restart/recovery holds rather than inferring that no
  stop exists;
- concrete broker order id required before state may be `SUBMITTED/ACTIVE`;
- missing id or response ambiguity = `OUTCOME_UNPROVEN`;
- cumulative terminal execution is a coherence fence, including fills already
  present in the first order snapshot; final position truth remains the only
  replacement-size authority;
- never automatically resubmit a stop from an ambiguous prior attempt;
- recovery must not replay historical broker mutation merely because an old ENTRY row is FILLED;
- terminal close must clear/terminalize protective ownership diagnostics without fabricating broker chronology.

## Existing stale EXIT timeout must remain

Do not remove or weaken current order-monitor policy:

```text
TIMEOUT_EXIT_PENDING = 45 seconds
TIMEOUT_EXIT_ACK = 90 seconds
EXIT_CHECK_INTERVAL = 15 seconds
```

That handles stale canonical EXIT orders.

This PR adds the missing protective-order takeover path. It does not replace stale EXIT monitoring.

## Rejection-loop correction

Today the rejection circuit breaker becomes self-defeating:

```text
rejection_count >= threshold
broker_truth_open_qty > 0
-> PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH
-> submit another sell
-> same reservation rejection
```

After this P0, `broker_truth_open_qty > 0` is not sufficient permission to POST another sell.

The override must first prove there is no active conflicting broker sell, or successfully resolve the protective-order takeover.

Do not simply disable the circuit breaker. Make the broker open-order state part of the safety truth.

## Production file budget

Preferred production files:

```text
ap/order_state_machine.py
ap/exit_safety.py
ap/fill_monitor.py
```

`ap/brokers/tradier.py` may change only if real production response normalization cannot be safely handled through existing methods.

`ap/order_monitor.py` should remain unchanged unless implementation proves a real missing hook. Its existing canonical EXIT timeout is not the defect.

Tests may add one focused P0 file and register it in `.github/workflows/p0_regression.yml`.

If implementation requires exit threshold, selector, watcher, sizing, queue, proof, or scanner edits, STOP and report scope expansion.

## Explicitly out of scope

Do not change:

```text
entry eligibility
scanner output
CALL/PUT direction arbitration
trigger confirmation
contract selector
moneyness
DTE
delta
spread/OI/volume
position sizing
capital limits
profit targets
hard-stop thresholds
touched-profit thresholds
runner logic
proof_trades taxonomy
trade_queue status policy
intelligence
```

## Mutation audit expectations

### Broker

Yes, this P0 intentionally changes broker cancel behavior at one exact seam:

```text
active conflicting protective sell
-> cancel before replacement EXIT
```

No new ENTRY submit authority.
No broad account-wide cancel.
No ticker-only cancel.
No cancel of unrelated OCC contracts.

### Orders

May persist protective ownership/takeover diagnostics and existing EXIT lifecycle state.

### Positions

No quantity/P&L mutation except through existing proven fill/close lifecycle.

### proof_trades

No direct mutation.

### queue

No mutation.

## Required diagnostics

At minimum emit structured events/reason codes for:

```text
EXIT_PROTECTIVE_PREFLIGHT_START
EXIT_PROTECTIVE_ORDER_FOUND
EXIT_PROTECTIVE_CANCEL_REQUESTED
EXIT_PROTECTIVE_CANCEL_CONFIRMED
EXIT_PROTECTIVE_CANCEL_OUTCOME_UNPROVEN
EXIT_PROTECTIVE_FILLED_DURING_TAKEOVER
EXIT_PROTECTIVE_REPLACEMENT_ALLOWED
EXIT_PROTECTIVE_REPLACEMENT_BLOCKED
EXIT_ACTIVE_BROKER_SELL_AMBIGUOUS
```

Diagnostics must preserve:

```text
client_id
execution_mode
position_id
local_order_id
contract
requested_qty
broker_long_qty
protective_broker_order_id
protective_qty
protective_status
replacement_qty
```

Never log credentials or access tokens.

## Required tests

At minimum:

1. exact 2026-08-26 NOW incident replay;
2. protective cancel confirmed -> one replacement submit;
3. protective fills before cancel -> zero duplicate submit;
4. protective fills during cancel race -> residual-only or zero submit;
5. cancel ACK but still open -> zero replacement;
6. cancel exception + re-query terminal canceled -> safe replacement;
7. cancel exception + re-query unknown -> zero replacement;
8. list_orders unavailable -> zero replacement in LIVE;
9. multiple same-contract active sells -> HOLD;
10. same ticker different OCC untouched;
11. existing canonical EXIT broker order is adopted/held, not canceled as orphan;
12. partial fill quantity math;
13. broker already flat -> zero submit;
14. client isolation;
15. LIVE/PAPER isolation;
16. stale canonical EXIT timeout behavior remains 45s/90s;
17. rejection-circuit-breaker override cannot bypass unresolved active broker sell;
18. durable stop id is persisted only with concrete broker id;
19. ambiguous standing-stop placement is never automatically replayed;
20. restart with durable protective id can resolve takeover without ticker inference;
21. no proof_trades/queue mutation;
22. no ENTRY broker POST added;
23. at most one cancel and one replacement POST per proven takeover generation.

Use real production-shaped Tradier order dictionaries in tests. Do not use MagicMock attributes that invent fields real responses do not carry.

## Fail-first requirement

Before production changes, reproduce the exact incident on the unmodified rebased base:

```text
broker long qty 1
active GTC stop sell_to_close qty 1
canonical CLOSE_ALL qty 1
-> code submits second sell without canceling the stop
-> broker rejection shape is accepted into existing rejection path
```

Record the failing assertion and broker call counts.

The same fixture must pass after implementation.

## Required validation

At minimum run:

```bash
python -m pytest -q \
  tests/test_p0_protective_exit_order_takeover.py \
  tests/test_p0_exit_closed_guard_and_circuit_breaker.py \
  tests/test_p0_exit_circuit_breaker_repair.py \
  tests/test_p0_broker_owned_exit_requested_recovery.py \
  tests/test_p0_broker_open_protective_monitoring.py \
  tests/test_p0_terminal_close_proof_binding.py \
  tests/test_p0_manual_client_close_proof.py \
  tests/test_fill_monitor_mvp_hardening.py

python -m py_compile \
  ap/order_state_machine.py \
  ap/exit_safety.py \
  ap/fill_monitor.py

git diff --check
```

Then exact-head authoritative P0 workflow on the final SHA.

## Required implementation completion report

Before review-ready, report:

```text
BASE SHA
HEAD SHA
exact changed files
actual diff stat
PR description vs actual diff
review comments/threads
exact live submit code path
exact protective stop creation code path
real Tradier list_orders shape used
real cancel_order/get_order status shape used
fail-first NOW result
post-fix NOW result
cancel call counts
replacement submit call counts
race-case results
client_id preservation
execution_mode preservation
position/order/proof/queue mutation audit
rejection-circuit-breaker interaction
order-monitor timeout preservation
exact-head CI
final MERGE / HOLD / HARD HOLD
```

## Merge gate

**HARD HOLD until implementation, production-shaped race tests, exact-head CI, and independent money-path diff audit are complete.**

Correct lifecycle:

```text
ENTRY filled
-> broker protective stop may exist
-> canonical exit decision fires
-> exact broker sell preflight
-> protective stop relinquishes ownership with broker-terminal proof
-> re-read position quantity
-> exactly one canonical replacement exit
-> normal fill/reconcile/proof lifecycle
```

Never again:

```text
protective stop silently reserves qty
-> canonical exit blindly submits second sell
-> Tradier rejects forever
-> circuit breaker keeps retrying the impossible order
```
