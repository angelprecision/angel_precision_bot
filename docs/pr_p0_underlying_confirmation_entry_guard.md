# P0 underlying confirmation entry guard

Documentation-only implementation handoff. This PR does not change runtime behavior.

## Verified entry path

- `ap/queue.py::_dispatch()` reaches `order_state_machine.create_entry_order()` before watcher routing.
- `ap_execution_core.py::_on_entry_trigger()` handles watcher breach and uses `submit_existing_entry()`.
- `ap/order_state_machine.py::submit_existing_entry()` is the existing-entry submit path.
- `ap/order_state_machine.py::submit_entry()` is the direct submit path.

## Required behavior

For daily / 1d / overnight ENTRY paths, unavailable, stale, or malformed underlying confirmation data must block before an entry row is created or submitted.

Canonical last_error:

`entry_blocked:underlying_confirmation_unavailable`

Canonical reason_code:

`UNDERLYING_CONFIRMATION_UNAVAILABLE`

Canonical stage:

`underlying_confirmation`

## Required helper

Add `ap/underlying_confirmation.py` with:

- `UnderlyingConfirmationUnavailable`
- `is_daily_timeframe(value)`
- `requires_underlying_confirmation(plan=None, payload=None)`
- `require_underlying_confirmation_available(plan=None, broker=None, payload=None, client_id="", execution_mode="", now=None, max_age_sec=None)`

Validation must require:

- expected underlying ticker/symbol
- positive current underlying price
- timestamp freshness, default max age 30 seconds
- provider/source metadata
- preserved `client_id`
- preserved `execution_mode`

Do not synthesize prices. Do not allow stale data.

## Required wiring

1. `ap/queue.py::_dispatch()` before `create_entry_order()`.
2. `ap/order_state_machine.py::submit_entry()` before `create_entry_order()`.
3. `ap/order_state_machine.py::submit_existing_entry()` before the existing submit call.

On failure:

- No new entry row in queue/direct path.
- Existing entry path terminalizes the existing entry row with the canonical last_error.
- Decision event uses reason_code `UNDERLYING_CONFIRMATION_UNAVAILABLE`.
- Queue result keeps `client_id`, `execution_mode`, `ticker`, and `signal_id`.

## Required tests

- 1d CALL missing underlying blocks before submit.
- 1d PUT missing underlying blocks before submit.
- stale underlying blocks.
- fresh underlying allows continuation.
- non-daily unchanged unless explicitly requiring confirmation.
- queue last_error is preserved.
- client_id and execution_mode are preserved.
- no entry row is created on queue/direct block.
- submit function is not called when blocked.

## Review posture

MERGE only for a guard-only, fail-closed implementation.

HARD HOLD if fallback prices are added, stale data is allowed, diagnostics lose client/mode, or order rows are created before the guard blocks.
