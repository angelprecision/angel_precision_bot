# P0 — Restore stale working-exit cancel/retry liveness

**Status: IMPLEMENTATION CONTRACT ONLY / HARD HOLD.**

Do not merge this specification as though production is fixed. Claude must implement the runtime correction, focused regression tests, and exact-head proof on this branch before review.

## Incident requiring this change — 2026-08-07 AVGO PAPER

During active management of an AVGO options position, Angel Precision submitted a scale-out SELL_TO_CLOSE for 2 contracts at approximately $2.49. The order remained working/unfilled while the position subsequently retraced from roughly +200% peak profit to roughly +80% without the expected exit completing.

The broker continued to reserve the 2 contracts against that working sell order. The system did not autonomously cancel/reprice/retry the stale exit. A later operator attempt to flatten the full 7-contract position was rejected because the broker considered only 5 contracts available to sell while 2 remained committed to the old working sell order.

This incident is preserved here so future reviewers understand why the retry lifecycle exists. It is not a generic optimization. It is a P0 money-path liveness requirement.

## Confirmed regression in current production path

The active exit path is:

`ClientRunner -> APExecutionCore -> APExitEngine -> APOrderStateMachine / broker -> fill_monitor / reconciler`

`ap/exit_manager.py` is legacy/dead and must NOT be used as the fix.

`ap/order_monitor.py` still contains the intended stale-exit recovery mechanism:

1. detect EXIT_REQUESTED / EXIT_SUBMITTED / EXIT_ACKNOWLEDGED timeout;
2. query broker status first;
3. if broker proves filled, advance the lifecycle instead of canceling;
4. otherwise cancel the broker order;
5. require broker-confirmed terminal cancellation;
6. transition the old EXIT order to CANCELED;
7. release exit-engine in-flight ownership;
8. allow APExitEngine to re-evaluate and reprice.

The file also documents the original money-risk rationale: a green option can round-trip while a mispriced limit exit sits unfilled. `TIMEOUT_EXIT_PENDING` is currently 45s and `TIMEOUT_EXIT_ACK` 90s.

However, `ORDER_MONITOR_MODE` defaults to `watchdog`, and `_handle_stale_exit()` currently returns before all broker cancel / clear / retry behavior whenever `ORDER_MONITOR_CAN_ACT` is false. Thus the old recovery exists in source but is alert-only under the default production mode.

This is the primary regression to repair.

A second continuity defect exists in `APExitEngine`: adaptive exit pricing uses `_exit_stuck_count` to step replacement limits toward executable BID, but `_mark_exit_submitted()` resets `_exit_stuck_count=0`, and `clear_exit_in_flight()` does not preserve/increment a replacement attempt. Claude must make retry generation/attempt state explicit so cancel -> replacement actually advances the pricing ladder instead of repeatedly behaving like attempt 0.

## Required behavior

For a broker-backed EXIT order that remains unfilled beyond the configured stale threshold:

`active exit -> exact broker status check -> fill truth if any -> cancel stale remainder -> prove cancel terminal -> release old generation -> increment retry generation/attempt -> refresh quote -> submit replacement for broker-confirmed remaining position`

The retry must be bounded, identity-fenced, broker-confirmed, and restart-safe.

### Non-negotiable invariants

1. **Never submit a replacement while an old matching broker exit may still be live.**
2. **Never clear `exit_in_flight` from elapsed time alone.** Cancellation or terminal broker truth is required.
3. **A broker fill discovered during the stale check wins over cancellation/replacement.** Apply cumulative fill truth first.
4. **Partial fill reduces only broker-confirmed filled quantity.** The unfilled remainder may be canceled/replaced after broker proof.
5. **Preserve `client_id`, `execution_mode`, `position_id`, contract, local order id and broker order id through every cancel/retry diagnostic.**
6. **PAPER and LIVE remain separate identities.** Do not use the PAPER 15-minute market-data behavior to change LIVE pricing taxonomy.
7. **Do not reopen/mutate position economic quantity merely because an exit order was canceled.** Position quantity changes only from broker-confirmed fills or exact broker-position reconciliation.
8. **No second asynchronous exit owner.** Reuse APOrderMonitor for stale broker-order detection and APExitEngine for replacement decision/pricing.
9. **No broker cancel without a known broker order identity unless an existing exact/fenced recovery helper first proves the match.**
10. **No silent exhaustion.** If bounded retries exhaust while broker still has an open position, preserve protective monitoring and emit an actionable P0 diagnostic.

## Exact production changes Claude should implement

### 1. `ap/order_monitor.py` — restore narrow stale EXIT authority in watchdog mode

Do NOT flip `ORDER_MONITOR_MODE` globally to active. That would silently enable unrelated position-mutating watchdog behavior.

Instead, change `_handle_stale_exit()` so watchdog mode is allowed to perform only this narrow risk-reducing lifecycle:

- exact broker order status query;
- broker fill/partial-fill advancement through the existing OSM/fill hooks;
- broker cancel of the exact stale EXIT order;
- confirmation that cancellation is terminal;
- transition of that exact EXIT order to `CANCELED`;
- notify APExitEngine that replacement is safe using the existing identity-fenced `mark_exit_replacement_safe(...)` / `clear_exit_in_flight(...)` APIs;
- DO NOT call generic position reopen fallback in watchdog mode.

Keep generic `ALLOW_ORDER_MONITOR_POSITION_REOPEN` semantics unchanged for unrelated paths.

The current early block:

```python
if not ORDER_MONITOR_CAN_ACT:
    ...
    return
```

must no longer suppress broker-proven stale EXIT recovery. Replace it with an explicit narrow authorization decision, e.g. conceptually:

```python
_stale_exit_recovery_allowed = True  # exact broker-backed risk reduction only
if not ORDER_MONITOR_CAN_ACT and not _stale_exit_recovery_allowed:
    return
```

Do not implement this as an unconditional broad actor-mode switch.

### 2. `ap/order_monitor.py` — broker confirmation must include post-cancel re-query

Do not trust `cancel_order()` response alone if the adapter only acknowledges receipt. Reuse the repository’s existing broker status query helper and require one of the repository’s terminal cancel statuses before unlocking replacement.

If cancel is requested but status remains open/pending/working/acknowledged:

- keep old exit ownership;
- do not clear in-flight;
- do not submit replacement;
- retry broker status on subsequent monitor cycles;
- emit `EXIT_CANCEL_NOT_CONFIRMED_REPLACEMENT_BLOCKED` (or equivalent precise canonical diagnostic).

### 3. `ap_exit_engine.py` — add durable/explicit replacement attempt continuity

The pricing ladder currently reads `_exit_stuck_count`, but new submissions reset it. Fix this without inventing another retry engine.

Use one explicit replacement-attempt field on `ManagedPosition`, e.g. `exit_replace_attempt`, initialized to 0 and incremented only after broker-confirmed cancel/terminal proof for the previous generation. If an existing durable field already provides the exact semantics after full code inspection, reuse it instead of adding another.

Requirements:

- attempt 0 = original exit;
- broker-confirmed stale cancel -> attempt 1;
- next broker-confirmed stale cancel -> attempt 2;
- next -> attempt 3;
- pricing consumes this replacement attempt so the existing 0/33%/66%/BID ladder actually advances;
- a successful completed exit resets the attempt;
- restart/seed must not silently return a known retrying order to attempt 0 if durable metadata proves prior attempts;
- bounded maximum must be explicit; after exhaustion for an open broker position, switch to the safest existing forced-risk/bid behavior rather than HOLD forever, subject to existing hard safety guards.

Do not use stale quote retry counters for this. `exit_retry_*` in current APExitEngine is specifically stale-option-quote protective ownership, a different failure class.

### 4. `ap_exit_engine.py` — replacement-safe should be one-generation, identity-fenced authority

Use the existing `pending_exit_replace_allowed`, `pending_exit_replace_reason`, and `pending_exit_replace_allowed_ts` shape instead of adding a competing boolean.

After exact old-order cancellation proof:

- stamp replacement safe for the exact `local_order_id` / `broker_order_id`;
- clear old pending identity only when that identity matches;
- allow exactly one replacement submit;
- consume/reset replacement-safe permission when the new generation is submitted;
- reject late callbacks from the old generation through the existing per-order cumulative-fill watermark and identity guards.

### 5. `ap/exit_autonomous_recovery.py` — align recovery with stale-single-order semantics

Current recovery correctly refuses replacement while a matching live broker exit exists and can cancel multiple ambiguous live matches, but an exact single working exit simply returns `CONFIRMED_OPEN` forever.

Do not make autonomous recovery race APOrderMonitor. Instead:

- if APOrderMonitor is healthy, keep this helper as reconciliation/fallback;
- if the order is demonstrably stale and the monitor/recovery ownership contract says this helper owns recovery, permit the same exact cancel-with-proof -> replacement-safe transition;
- otherwise return `CONFIRMED_OPEN` with an explicit owner diagnostic.

There must be one cancel owner at a time.

## Scope lock

Expected production files:

1. `ap/order_monitor.py`
2. `ap_exit_engine.py`
3. `ap/exit_autonomous_recovery.py` only if required to prevent ownership contradiction

Focused tests may add new files under `tests/` and add them to the authoritative P0 workflow.

Do NOT change:

- scanner/admission/intelligence;
- contract selection;
- entry retry/reprice;
- queue fanout;
- sizing;
- proof-trade taxonomy;
- LIVE/PAPER quote price authority;
- stop/target strategy thresholds;
- unrelated self-healing position mutations.

If more than three production files are required, stop and explain the exact dependency before expanding scope.

## Required regression tests

Add an incident replay suite, suggested path:

`tests/test_p0_exit_retry_liveness_avgo.py`

Minimum cases:

1. EXIT_ACKNOWLEDGED beyond timeout, broker still working -> exact cancel attempted even when `ORDER_MONITOR_MODE=watchdog`.
2. cancel confirmed -> old OSM EXIT becomes CANCELED and replacement is permitted.
3. cancel not confirmed -> no in-flight clear, zero replacement submits.
4. broker says FILLED during stale check -> apply fill, zero cancel, zero replacement.
5. broker says PARTIAL_FILL -> apply cumulative delta once; cancel/retry only remaining order quantity.
6. duplicate partial callbacks -> no double decrement.
7. late fill from canceled old generation after replacement submit -> identity/watermark prevents double mutation.
8. replacement pricing attempt progression is 0 -> 1 -> 2 -> 3+, with attempt 3+ at executable BID under existing pricing policy.
9. successful fill resets retry attempt.
10. restart with durable prior attempt does not revert to attempt 0.
11. client A’s exit can never cancel/retry client B’s order.
12. PAPER order cannot touch LIVE order and vice versa.
13. unknown/missing broker order id -> fail closed, no cancel guess.
14. broker API failure -> retain protective ownership and retry later, never unlock replacement by age.
15. bounded retry exhaustion while broker remains open -> critical diagnostic + protective forced-risk handling; never silent HOLD forever.

## Diagnostics required

Each retry generation must preserve at least:

- `client_id`
- `execution_mode`
- `position_id`
- `contract`
- old local/broker order ids
- new local/broker order ids
- original exit reason/action
- broker status before cancel
- cancel result and confirmed status
- cumulative filled quantity before replacement
- broker open position quantity
- replacement attempt number
- fresh bid/ask/quote timestamp used to price replacement
- submitted replacement limit
- terminal outcome

Do not overwrite existing order/position metadata blobs. Merge under a dedicated nested key such as `exit_retry_liveness` if persistence is needed.

## Review gate

This PR changes live behavior and directly touches broker CANCEL plus subsequent broker SELL_TO_CLOSE submission. It is active money-path behavior, not observe-only.

Before merge the reviewer must read PR body, full diff, review threads, changed files, exact runtime path and exact-head CI.

### HARD HOLD conditions

- watchdog mode can still alert-and-return on a stale broker-backed EXIT;
- replacement can occur before broker-confirmed old-order terminal state;
- retry attempt does not actually advance pricing;
- partial fills can be double-applied;
- client_id/execution_mode can be lost or defaulted;
- any stale order can be canceled by contract-only fuzzy guessing without fenced ownership;
- a retry can submit against stale/missing quote truth outside existing forced-risk rules;
- exact AVGO incident replay is absent;
- exact-head P0 CI is not green.

### Required final verdict

`MERGE`, `HOLD`, or `HARD HOLD` with explicit answers to:

- Does this change live behavior?
- Is it flag-off or active?
- Does it touch broker submit/cancel?
- Does it mutate orders, positions, proof_trades, queue?
- Does it preserve client_id / execution_mode?
- Does it use real production broker/order metadata shape?
- Does it preserve diagnostics downstream?
- Could it pollute PAPER/LIVE taxonomy?
- Could it make Jason trade junk?
