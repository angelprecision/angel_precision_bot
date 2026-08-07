# PR #423 Exact Patch Amendment — Claude implementation map

This amendment is binding. It narrows the implementation so Claude should not need to rediscover the architecture or invent a new lifecycle.

## Historical regression provenance

The failure is not a newly requested feature.

Commit `329381c5a7bdf54dd76a0e41f48c47c59bf2d062` (2026-05-17) changed exit stale thresholds from 300/600 seconds to 45/90 seconds and explicitly documented the intended active mechanism:

`broker-fill-check -> cancel -> clear_exit_in_flight -> exit engine re-evaluates & re-prices within its 8s loop`

That commit left `ORDER_MONITOR_MODE` defaulting to `watchdog` and described the monitor as passive.

Commit `e438a61992eb489a26e9bd874606fefb67698353` (2026-05-18) then confirmed the practical consequence of that passive gate for ENTRY orders: `_handle_stale_entry` logged and returned without canceling in watchdog mode. It added `ALLOW_ENTRY_CANCEL_IN_WATCHDOG=1`, but made no equivalent EXIT exception.

Therefore the H4 exit reliability mechanism remained source-present but behavior-suppressed. The 2026-08-07 AVGO incident is consistent with that latent defect.

## Patch 1 — ap/order_monitor.py

### A. Keep global monitor mode passive

Do not change:

```python
ORDER_MONITOR_MODE = os.getenv("ORDER_MONITOR_MODE", "watchdog").strip().lower()
ORDER_MONITOR_CAN_ACT = ORDER_MONITOR_MODE in {"active", "actor", "enforce", "enforced"}
```

Add a narrow control directly beside the existing ENTRY exception:

```python
ALLOW_STALE_EXIT_RECOVERY_IN_WATCHDOG = os.getenv(
    "ALLOW_STALE_EXIT_RECOVERY_IN_WATCHDOG", "1"
).strip().lower() in {"1", "true", "yes", "on"}
```

Default MUST be enabled. This is the restoration of the previously intended H4 safety behavior, not an experimental flag-off feature.

Include this value in the monitor startup/config diagnostic payload.

### B. Replace the early watchdog return inside `_handle_stale_exit`

Current shape:

```python
if not ORDER_MONITOR_CAN_ACT:
    self._alert(...)
    log.warning(...)
    return

broker_oid = self._get_broker_order_id(local_order_id)
broker_status = self._query_broker_order(broker_oid)
...
```

Required shape:

```python
_stale_exit_recovery_allowed = (
    ORDER_MONITOR_CAN_ACT or ALLOW_STALE_EXIT_RECOVERY_IN_WATCHDOG
)

if not _stale_exit_recovery_allowed:
    self._alert(...)
    log.warning(...)
    return

if not ORDER_MONITOR_CAN_ACT:
    log.warning(
        "[%s] WATCHDOG MODE — exact stale EXIT recovery permitted | "
        "order=%s status=%s pos=%s",
        self.client_id, local_order_id, status, position_id,
    )
```

This exception authorizes only the remaining exact stale-EXIT recovery code in `_handle_stale_exit`. It does NOT authorize generic position reopening or unrelated watchdog mutations.

### C. Exact broker identity is mandatory

Immediately after:

```python
broker_oid = self._get_broker_order_id(local_order_id)
```

require a nonblank broker id before any cancel:

```python
if not broker_oid:
    self._emit_order_event(
        local_order_id=local_order_id,
        stage="order_monitor",
        decision="BLOCK",
        reason_code="STALE_EXIT_BROKER_ID_UNPROVEN",
        explanation="Stale EXIT recovery blocked: exact broker_order_id unavailable",
        contract=contract,
        position_id=position_id,
    )
    return
```

Do not fuzzy-cancel by contract in this method.

### D. Broker fill truth wins before cancel

Keep the current pre-cancel broker query:

```python
broker_status = self._query_broker_order(broker_oid)
```

If executed/filled, keep routing through:

```python
self._advance_from_broker_status(local_order_id, broker_status, contract)
return
```

If the current helper can surface `PARTIAL_FILL`, apply that broker cumulative fill through existing OSM/fill hooks before attempting to cancel the unfilled remainder. Do not duplicate fill accounting inside order_monitor.

### E. Cancel response is not sufficient proof

Current code treats the status extracted from `cancel_result` as the confirmation authority.

Change to:

```python
cancel_result = self._cancel_broker_order(broker_oid)
cancel_response_status = self._extract_broker_status(cancel_result)
post_cancel_status = self._query_broker_order(broker_oid)
```

`post_cancel_status` is the terminal proof authority when available.

Canonical decision:

```python
confirmed_status = post_cancel_status or cancel_response_status
is_confirmed_canceled = self._is_terminal_cancel_status(confirmed_status)
```

If the broker status is still open/pending/working/accepted/acknowledged/queued/partial, DO NOT unlock replacement. Emit:

`EXIT_CANCEL_NOT_CONFIRMED_REPLACEMENT_BLOCKED`

and return with the current `exit_in_flight` ownership intact.

A broker API/read failure after cancel request is UNKNOWN, not confirmation.

### F. Broker-confirmed cancellation must hand ownership to the exit engine without reopening economic quantity

After exact cancellation proof:

```python
self.osm.transition(local_order_id, "CANCELED", last_error=reason)
```

Do NOT call `_guarded_revert_position_open_after_exit_cancel()` from watchdog-mode stale EXIT recovery. Cancellation of a working SELL_TO_CLOSE changes order state, not confirmed position quantity.

Preferred handoff:

```python
if position_id and self.exit_engine:
    self.exit_engine.mark_exit_replacement_safe(
        position_id,
        reason="STALE_EXIT_BROKER_CANCEL_CONFIRMED",
        local_order_id=local_order_id,
        broker_order_id=broker_oid,
        reconciled=True,
    )
```

Then clear old in-flight ownership only through the exact identity-safe engine method expected by `_can_submit_exit`. If `mark_exit_replacement_safe()` already performs/permits this one-generation transition, reuse it. If `_can_submit_exit` requires `exit_in_flight=False`, call `clear_exit_in_flight(...)` only AFTER marking/recording the retry attempt and only with the exact old order identities.

Claude must inspect `_can_submit_exit` and implement the minimal compatible sequence. Do not bypass it with `allow_inflight_override=True` for normal stale replacements.

## Patch 2 — ap_exit_engine.py

### A. Add one declared field to ManagedPosition

Do not reuse `_exit_stuck_count` as durable retry generation. It is also used by sentinel stuck diagnostics and is reset by submission.

Add:

```python
exit_replace_attempt: int = 0
```

If ManagedPosition persistence/hydration already has a generic metadata reconstruction block, hydrate this from `meta.exit_retry_liveness.replace_attempt` (or the chosen dedicated nested key). Missing key -> 0.

Do not infer it from elapsed time.

### B. Pricing ladder consumes `exit_replace_attempt`

Current adaptive pricing reads:

```python
_attempt = int(getattr(pos, "_exit_stuck_count", 0))
```

Replace with:

```python
_attempt = max(0, int(getattr(pos, "exit_replace_attempt", 0) or 0))
```

Keep the existing pricing math unchanged:

- 0 -> tier start
- 1 -> 33% toward BID
- 2 -> 66% toward BID
- >=3 -> BID

This PR is liveness repair, not pricing-policy redesign.

### C. `_mark_exit_submitted()` must NOT reset replacement attempt

Keep `_exit_stuck_count = 0` if sentinel diagnostics need it.

Do not set `exit_replace_attempt = 0` here. A replacement submission is the consumer of the current retry generation, not proof of success.

Continue resetting one-generation `pending_exit_replace_allowed` state when the new order is successfully marked submitted.

### D. Increment replacement attempt only on broker-confirmed prior generation termination

Inside `mark_exit_replacement_safe(...)`, after exact old-order identity validation succeeds and before replacement permission is exposed:

```python
pos.exit_replace_attempt = min(
    int(getattr(pos, "exit_replace_attempt", 0) or 0) + 1,
    EXIT_REPLACE_MAX_ATTEMPTS,
)
```

Add:

```python
EXIT_REPLACE_MAX_ATTEMPTS = int(os.getenv("EXIT_REPLACE_MAX_ATTEMPTS", "4"))
```

The fourth generation and later already price at BID under the standing ladder. The max is a liveness/diagnostic bound, not permission to leave an open broker position unmanaged.

Persist the attempt under a nested metadata key if the position persistence layer supports non-destructive metadata merge. Preserve all existing metadata.

### E. Successful completion resets attempt

Reset `exit_replace_attempt = 0` only when one of these proves the current exit lifecycle completed safely:

- pending requested tranche fully broker-filled;
- position broker-confirmed flat/closed;
- exact reconciliation proves no open position remains.

Do not reset on submit, acknowledge, cancel request, cancel response without proof, quote refresh, restart, or an OSM lookup.

### F. Retry exhaustion behavior

If `exit_replace_attempt >= EXIT_REPLACE_MAX_ATTEMPTS` and broker truth still proves an open long position:

- emit `EXIT_REPLACE_RETRY_EXHAUSTED_BROKER_OPEN` with all identities and quote truth;
- keep protective ownership active;
- for forced-risk exits, price at existing executable BID behavior;
- do not turn this into a silent HOLD;
- do not invent a market order if the current exit transport/policy does not already authorize it.

## Patch 3 — ap/exit_autonomous_recovery.py

Do not create a second timer or cancel loop.

For an exact pending broker id with OPEN_BROKER_STATUS:

- when APOrderMonitor is healthy/registered, return `CONFIRMED_OPEN` plus an owner detail such as `recovery_owner=order_monitor_stale_exit`;
- do not cancel from both components.

Only allow autonomous recovery to run `_cancel_order_with_proof()` for the single exact working exit when the order monitor is unavailable/dead AND the order age exceeds the same configured stale threshold.

Then call existing `_mark_replacement_safe(...)` with exact identities.

## Tests — implementation must use real methods

Do not create tests that copy formulas.

At minimum instantiate real `APOrderMonitor._handle_stale_exit`, real `APExitEngine.mark_exit_replacement_safe`, real `_submit_exit_decision` pricing path, and real partial-fill methods with broker/DB boundaries mocked.

The AVGO replay must prove:

```text
position qty 7
scale-out SELL_TO_CLOSE 2 @ 2.49
broker status working beyond timeout
ORDER_MONITOR_MODE=watchdog
=> exact cancel attempted
=> cancel terminal proof required
=> attempt becomes 1
=> replacement uses fresh quote and attempt-1 pricing
=> no duplicate old/new broker exit overlap
```

## No-scope-creep assertions

The final diff should contain zero changes to:

- `ap_master_control.py`
- contract selector
- entry watcher
- entry order submit/retry
- queue
- proof logger taxonomy
- intelligence/scoring
- stop/target thresholds

If implementation requires one of those files, HARD HOLD and explain why.