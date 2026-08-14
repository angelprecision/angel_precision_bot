# P0 amendment — Restore unfilled EXIT retry / reprice liveness

**Status:** DRAFT / HARD HOLD. This amendment is binding on PR #469. Do not merge or deploy until implementation and exact-head evidence prove the retry lifecycle.

**Audited base:** `main@ef4a4d232c0fadfbbca7592711477705b706bb4d` on 2026-08-14.

## Why this amendment exists

The exit retry behavior Angel Precision previously relied on is still described in current code, but current-main defaults can suppress the behavior in two separate places.

The intended current-main design in `ap/order_monitor.py` is:

```text
unfilled EXIT
-> check broker status first
-> after bounded stale age, cancel the still-unfilled broker EXIT
-> broker-confirm cancel
-> clear exit_in_flight
-> make the canonical position eligible for exit evaluation again
-> exit engine re-evaluates/re-prices
-> submit one replacement EXIT through the existing OSM owner
```

Current constants/documentation say:

```text
TIMEOUT_EXIT_PENDING = 45s
TIMEOUT_EXIT_ACK     = 90s
EXIT_CHECK_INTERVAL  = 15s
```

and the code comment explicitly describes:

```text
broker-fill-check -> cancel -> clear_exit_in_flight
-> exit engine re-evaluates & re-prices within its ~8s loop
```

That is the behavior the product is supposed to have. It is not reliably live under current defaults.

## Confirmed blocker 1 — watchdog mode suppresses stale EXIT action

Current main defines:

```python
ORDER_MONITOR_MODE = os.getenv("ORDER_MONITOR_MODE", "watchdog").strip().lower()
ORDER_MONITOR_CAN_ACT = ORDER_MONITOR_MODE in {"active", "actor", "enforce", "enforced"}
```

`_handle_stale_exit(...)` then does:

```python
if not ORDER_MONITOR_CAN_ACT:
    ...
    log.warning("WATCHDOG MODE — stale exit action suppressed ...")
    return
```

Therefore the default code path can detect a stale unfilled EXIT and then intentionally do nothing beyond alerting.

This is incompatible with the required EXIT liveness contract.

## Confirmed blocker 2 — position reopen/re-arm is default-off

Current main also defines:

```python
ALLOW_ORDER_MONITOR_POSITION_REOPEN = os.getenv(
    "ALLOW_ORDER_MONITOR_POSITION_REOPEN", "0"
).strip() == "1"
```

The guarded post-cancel repair returns `False` when this flag is off.

Current code itself documents why this is fatal to retry liveness:

```text
Without this, the position stays CLOSING and the exit engine
never re-submits even after clearing in-flight.
```

So even if `ORDER_MONITOR_MODE` is externally set active, the second default-off gate can still leave the economic position without a replacement EXIT.

## P0 invariant

A broker-submitted EXIT that is still unfilled must never become permanently inert merely because a watchdog/reopen environment flag is at its default.

For every non-terminal EXIT attempt, exactly one of these outcomes must become durable and observable within a bounded interval:

1. broker-confirmed full fill;
2. broker-confirmed partial fill with exact remaining quantity still canonically owned;
3. broker-confirmed cancel/reject/expire followed by one canonical retry/reprice owner;
4. explicit terminal HOLD/quarantine because broker outcome or identity is unproven;
5. emergency escalation under the existing explicit emergency authority.

The forbidden state is:

```text
position still economically open at broker
AND prior EXIT is unfilled/canceled/rejected/expired
AND no active canonical replacement EXIT
AND exit engine cannot re-evaluate because local state is stuck CLOSING/in-flight
```

No supported production configuration may silently create that state.

## Required retry behavior

### Broker truth first

Before canceling/repricing an old EXIT:

- fetch broker status using the existing canonical broker-status path;
- if FILLED, finalize from broker fill truth and do not cancel/retry;
- if PARTIAL, account for exact broker-confirmed filled quantity and preserve one owner for the remainder;
- if broker status is unknown/unreachable, HOLD rather than blindly submit another sell;
- only cancel when the broker proves the old EXIT is still open/cancelable.

### Cancel confirmation before replacement

Never submit a replacement while the prior broker EXIT may still be active.

Required sequence:

```text
old EXIT active
-> broker status confirms open/unfilled
-> cancel through existing canonical cancel owner
-> broker confirms CANCELED (or terminal reject/expire)
-> durable local transition agrees
-> clear/re-arm canonical exit ownership
-> re-evaluate price using fresh quote
-> exactly one replacement submit
```

No cancel acknowledgement means no replacement submit.

### Retry pricing must integrate with #469 price-improvement policy

The retry must not merely restore the old behavior of repeatedly slamming the low BID.

For a normal non-emergency exit:

```text
initial fresh quote: bid 1.54 / ask 1.58 / current 1.55
attempt 1: 1.55
if still unfilled after bounded attempt window:
  confirm broker state
  cancel old order
  refresh quote
  submit one lower/equal replacement under the canonical ladder
```

The ladder is monotonic toward executable BID unless a newer fresh quote proves the market itself moved upward.

Example narrow-spread lifecycle:

```text
t=0      sell limit 1.55
t=bounded stale age, still open
         broker-check -> cancel -> confirmed canceled
         fresh quote/re-evaluation
t=retry  sell limit 1.54 (or fresh same-cycle equivalent)
```

For wider spreads, use bounded intermediate price improvement when supported by the existing single replacement owner. Do not create a second retry scheduler.

### Timing / liveness

The current code's intended defaults are approximately 45 seconds for pending EXITs, 90 seconds for acknowledged-but-unfilled EXITs, checked on a 15-second exit cadence. Implementation may adjust exact constants only if current production evidence proves a safer bounded schedule.

What is not acceptable is "wait indefinitely" or "retry only if a default-off env flag happens to be enabled."

For each EXIT order, diagnostics must make the following measurable:

```text
exit_retry_owner
exit_retry_attempt
exit_retry_old_local_order_id
exit_retry_old_broker_order_id
exit_retry_old_status
exit_retry_cancel_requested_at
exit_retry_cancel_confirmed_at
exit_retry_rearmed_at
exit_retry_replacement_local_order_id
exit_retry_replacement_broker_order_id
exit_retry_reason
exit_retry_quote_ts
exit_retry_limit
```

Use existing order metadata/diagnostic surfaces. No new table.

## Architecture requirement

Do not solve this by adding another independent EXIT thread.

Codex must identify the one canonical owner of:

- stale EXIT detection;
- broker status proof;
- cancel;
- cancel confirmation;
- clearing/re-arming exit state;
- replacement submission.

If current ownership is split between order monitor + exit engine + OSM, preserve that architecture but make the handoff an enforced postcondition.

It is acceptable to remove the default-off suppression specifically for canonical EXIT liveness if that is the smallest safe current-main repair. It is also acceptable to move re-arm authority into an already-canonical exit/OSM seam if that eliminates the need to mutate a position back to `OPEN`.

It is **not** acceptable to globally set an environment variable and call the incident fixed. The code and tests must make the invariant structurally true under the supported default production configuration.

## Position-state caution

Do not casually reopen a genuinely closing/partially-filled/externally-flat position.

Before any `CLOSING -> OPEN` or equivalent re-arm mutation, prove:

- exact `client_id`;
- exact `execution_mode`;
- exact `position_id`;
- exact OCC contract;
- broker position still has positive remaining quantity;
- old EXIT is broker-confirmed terminal and no newer active EXIT exists;
- quantity matches the remaining economic exposure;
- no external/manual close evidence has already flattened the broker position.

Prefer an explicit retryable-exit ownership state over a semantically false `OPEN` mutation if current architecture supports it safely.

## Required regressions

Add these to the focused P0 suite for #469.

1. Default supported production configuration: stale unfilled EXIT must progress to broker status check/cancel/retry; it must not stop at `WATCHDOG MODE — stale exit action suppressed`.
2. `ORDER_MONITOR_MODE=watchdog`: supported default must not silently strand an EXIT. Either canonical EXIT action remains enabled or another canonical owner proves retry.
3. `ALLOW_ORDER_MONITOR_POSITION_REOPEN=0`: supported default must not strand a CLOSING position after confirmed cancel.
4. EXIT_PENDING age >= configured threshold, broker reports open -> one cancel request, never duplicate.
5. EXIT_ACKNOWLEDGED age >= configured threshold, broker reports open -> one cancel request, never duplicate.
6. Broker reports FILLED at stale check -> no cancel, no replacement; finalize exact fill.
7. Broker status unknown -> HOLD; no cancel and no duplicate replacement sell.
8. Cancel requested but not yet confirmed -> no replacement submit.
9. Cancel confirmed -> canonical ownership re-armed exactly once and one replacement submitted.
10. Existing newer active EXIT discovered during re-arm -> do not reopen/retry the older order.
11. Partial fill -> remaining quantity only; never resell already-filled quantity.
12. Restart after cancel confirmation but before replacement -> recovery produces at most one replacement.
13. Restart while old EXIT still broker-active -> adopt/monitor old EXIT, no duplicate replacement.
14. Rejected/expired old EXIT -> bounded canonical replacement path if broker exposure remains open.
15. Broker position already flat -> no replacement; reconcile broker fill/external close truth.
16. Wrong client/mode/contract/position identity -> fail closed, zero cancel and zero replacement.
17. Jason LIVE-shaped fixture: exact LIVE identity preserved from old EXIT through replacement.
18. PAPER equivalent remains isolated and cannot donate/reuse LIVE ownership.
19. Price-improvement integration: 1.54/1.58/current 1.55 -> first 1.55; after confirmed unfilled cancel, replacement can step to 1.54.
20. Wide-spread +2%-display/-12%-bid incident fixture -> first attempt price-improves; retry ladder progresses without ever fabricating realized P&L.
21. Submitted replacement limit differs from eventual broker fill -> final position/proof use broker fill only.
22. Trap broker submit/cancel functions and prove: one old order, at most one cancel, at most one replacement submit per retry generation.
23. Repeated monitor ticks before threshold -> no cancel/retry storm.
24. Repeated monitor ticks after cancel confirmation -> idempotent, no duplicate replacement.
25. Explicit emergency exit still has bounded immediate flatten authority and does not wait through the normal price-improvement ladder.

## Required incident replay trace

The implementation evidence must show a complete trace like:

```text
EXIT attempt A submitted @ 1.55
-> broker OPEN / unfilled after stale threshold
-> cancel A requested
-> broker A=CANCELED
-> durable A=CANCELED
-> exact position remains broker-open for N contracts
-> canonical retry ownership re-armed
-> fresh quote acquired
-> EXIT attempt B submitted @ <=1.55 and >= fresh bid
-> broker B FILLED @ actual fill
-> position finalized from actual fill
-> proof finalized from same actual fill
```

And the negative trace:

```text
EXIT attempt A submitted
-> stale threshold reached
-> broker outcome UNKNOWN
-> HOLD
-> zero cancel
-> zero replacement
-> zero fake close/proof
```

## Release gate addition for PR #469

PR #469 remains **HARD HOLD** until all of the following are proven on the exact implementation head:

- current-main watchdog suppression is removed/bypassed for canonical EXIT liveness or structurally superseded by one canonical retry owner;
- default-off position reopen cannot strand an exit;
- unfilled exits retry/reprice under bounded timing;
- broker status and cancel confirmation always precede replacement;
- no duplicate sell exposure is possible;
- partial-fill quantity is exact;
- restart is idempotent;
- retry prices integrate with the #469 price-improvement ladder;
- actual broker fill remains final economic/proof truth;
- exact client/mode/position/contract identity is preserved;
- focused + adjacent P0 tests and exact-head CI are green;
- independent money-path audit returns MERGE rather than HOLD/HARD HOLD.
