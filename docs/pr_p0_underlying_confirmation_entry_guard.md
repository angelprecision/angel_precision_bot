# P0: Fail closed when underlying confirmation data is unavailable before ENTRY

This file is a review handoff for Codex/manual implementation. It intentionally does not change live behavior.

## Verdict

Current requested production change is **HOLD until implemented in code**.

## Verified production path

- `ap/queue.py::_dispatch()` approves and sizes signals, then calls `order_state_machine.create_entry_order()` before watcher/immediate routing.
- `ap_execution_core.py::_on_entry_trigger()` is the watcher breach callback and calls `order_state_machine.submit_existing_entry()`.
- `ap/order_state_machine.py::submit_existing_entry()` builds the Tradier order payload and calls `_submit_order_with_retry()`.
- `ap/order_state_machine.py::submit_entry()` is the direct submit path and currently creates an order before broker submit.

## Required code change

Add a shared helper in `ap/underlying_confirmation.py` and wire it before any daily/1d ENTRY order creation or broker submit.

Required constants:

```python
UNDERLYING_CONFIRMATION_UNAVAILABLE_LAST_ERROR = "entry_blocked:underlying_confirmation_unavailable"
UNDERLYING_CONFIRMATION_UNAVAILABLE_REASON_CODE = "UNDERLYING_CONFIRMATION_UNAVAILABLE"
UNDERLYING_CONFIRMATION_STAGE = "underlying_confirmation"
```

Required functions:

```python
class UnderlyingConfirmationUnavailable(Exception): ...

def is_daily_timeframe(value) -> bool: ...

def requires_underlying_confirmation(plan=None, payload=None) -> bool: ...

def require_underlying_confirmation_available(
    *, plan=None, broker=None, payload=None,
    client_id="", execution_mode="", now=None, max_age_sec=None,
) -> dict: ...
```

Rules:

1. Daily/1d/d/overnight signals always require underlying confirmation.
2. Non-daily behavior is unchanged unless metadata explicitly requires confirmation.
3. Valid data requires ticker/symbol, positive price, timestamp, and provider/source.
4. Stale data blocks. Default max age: 30 seconds.
5. Broker quote fetch may be used only through read-only `broker.get_quote(ticker)`.
6. Never synthesize/fallback a fake price.
7. Never touch broker submit/cancel internals.
8. Preserve `client_id` and `execution_mode` in all emitted diagnostics.

## Required wiring

### `ap/queue.py::_dispatch()`

Before `order_state_machine.create_entry_order(...)`, call `require_underlying_confirmation_available(...)`.

On failure:

- Do not create an order.
- Do not arm watcher.
- Do not submit broker order.
- `_mark_job(job_id, "REJECTED", error="entry_blocked:underlying_confirmation_unavailable", result={...})`.
- Result JSON must include `stage`, `decision`, `reason`, `reason_code`, `client_id`, `execution_mode`, `ticker`, `signal_id`.
- Emit decision event with stage `underlying_confirmation` or `entry_validation`, decision `REJECT`, reason code `UNDERLYING_CONFIRMATION_UNAVAILABLE`.
- Log structured rejection to `ap_signals`.

### `ap/order_state_machine.py::submit_entry()`

At the top, before `create_entry_order(...)`, call the helper.

On failure:

- Do not create an order row.
- Do not call `_submit_order_with_retry()`.
- Emit decision event.
- Return a rejected result with `local_order_id=None`.

### `ap/order_state_machine.py::submit_existing_entry()`

After resolving ticker/contract/qty/limit and before `_submit_order_with_retry(...)`, call the helper.

On failure:

- Do not call `_submit_order_with_retry()`.
- Terminalize existing entry order through the existing OSM transition path with `last_error="entry_blocked:underlying_confirmation_unavailable"`.
- Emit decision event.
- Return not-ok with reason code `UNDERLYING_CONFIRMATION_UNAVAILABLE`.

## Required tests

Create `tests/test_p0_underlying_confirmation_entry_guard.py` covering:

- 1d CALL missing underlying data blocks before submit.
- 1d PUT missing underlying data blocks before submit.
- stale underlying data blocks.
- fresh underlying data allows path to continue.
- non-daily unchanged unless metadata requires confirmation.
- queue `_dispatch()` writes `trade_queue.last_error = entry_blocked:underlying_confirmation_unavailable`.
- queue `_dispatch()` result preserves `client_id` and `execution_mode`.
- queue `_dispatch()` does not call `create_entry_order` when blocked.
- `submit_entry()` does not call `create_entry_order` when blocked.
- `submit_entry()` does not call `_submit_order_with_retry` when blocked.
- `submit_existing_entry()` does not call `_submit_order_with_retry` when blocked.
- decision events contain stage, decision, reason code, client_id, execution_mode.

## Merge posture

MERGE only when the implementation is guard-only and fail-closed.

HARD HOLD if it:

- adds fallback fake prices,
- permits stale data,
- hides missing data,
- modifies broker submit/cancel internals,
- creates order rows before blocking in queue/direct submit,
- drops `client_id` or `execution_mode`,
- pollutes paper/live taxonomy.
