# P0: Complete PR #398 queue/signal CAS safety

**STATUS: DRAFT IMPLEMENTATION CONTRACT ONLY. DO NOT MERGE.**

Base commit: `3315a633a6061563ecd92a9c15b1615051b90176` (merged PR #398)

## Why this follow-up exists

PR #398 materially improves queue deferral truth by introducing a checked `PROCESSING -> WATCHING` queue CAS and explicit outcomes. Post-merge audit found three remaining gaps. This follow-up must complete the same contract without reverting #398 or expanding into any other subsystem.

## P0 blocker 1: ap_signals compensation is not state-fenced

Current behavior writes `ap_signals.decision_status=WATCHING` through an upsert, then on queue CAS failure performs another unconditional upsert with `decision_status=ERROR`.

A concurrent owner may advance the same `(signal_id, client_email)` row after the WATCHING write. The compensation upsert can then overwrite that newer state with ERROR.

### Required implementation

Add one helper in `ap/queue.py` for compensation only, for example:

```python
def _compensate_watching_signal_if_unchanged(...)-> str:
    # Expected-state update only.
    # UPDATE ap_signals to ERROR WHERE signal_id + client_email + decision_status=WATCHING.
    # Return explicit outcome: TRANSITIONED / ALREADY_ADVANCED / MISSING / DB_ERROR.
```

The helper must:

1. Scope by exact canonical `signal_id` and `client_email`.
2. Update only where `decision_status == "WATCHING"`.
3. Never use a blind upsert for compensation.
4. Verify whether one row changed.
5. On zero rows, reread the exact row and preserve every non-WATCHING state.
6. Log observed state and return an explicit outcome.
7. Never change queue, OSM, watcher, broker, orders, positions, exits, or proof trades.

Use this helper for TERMINAL, MISSING_OR_UNEXPECTED, and DB_ERROR compensation paths in `_persist_watching_deferral()`.

## P0 blocker 2: early queue ERROR writes are unconditional

Current invalid-client, invalid-mode, invalid-signal, and signal-persistence-failure branches call `_mark_job(job_id, "ERROR")`. `_mark_job()` updates by ID only and can overwrite a row that concurrently advanced from PROCESSING.

### Required implementation

Add one guarded queue error CAS helper, for example:

```python
def _checked_processing_error_cas(job_id, *, error, result=None) -> str:
    # UPDATE trade_queue SET status=ERROR ...
    # WHERE id=? AND status=PROCESSING
    # classify zero-row state without mutating it.
```

It must return explicit outcomes equivalent to:

- `ERROR_CAS_TRANSITIONED`
- `ERROR_CAS_ALREADY_ERROR`
- `ERROR_CAS_TERMINAL`
- `ERROR_CAS_WATCHING_OR_ADVANCED`
- `ERROR_CAS_MISSING`
- `ERROR_CAS_DB_ERROR`

Replace only the early `_mark_job(ERROR)` calls inside `_persist_watching_deferral()` with this guarded helper.

Never overwrite `WATCHING`, `SUBMITTED`, `FILLED`, `CANCELED`, `CANCELLED`, `EXPIRED`, `DONE`, `REJECTED`, or an already-`ERROR` row.

## P1 completeness defect: one direct WATCHING writer remains

The after-hours contract-selection branch still directly performs:

```python
_log_signal_to_db(... decision_status="WATCHING")
_mark_job(job_id, "WATCHING", ...)
```

It bypasses `_persist_watching_deferral()`, the checked queue CAS, explicit outcome handling, and malformed-score sanitization.

### Required implementation

Replace that direct sequence with one `_persist_watching_deferral(...)` call using the existing:

- `stage="contract_selection"`
- `reason_code="market_closed_deferred"`
- human reason
- `watching_error="after_hours_deferred:awaiting_overnight_reeval"`
- truthful `watching_result`
- `failure_error="ap_signals_write_failed:after_hours_deferred"`

Keep `track_counterfactual_signal()` best-effort behavior, but call it only after `_persist_watching_deferral()` returns true.

Remove the direct `float(payload.get("score") or 0)` from this branch. The canonical helper owns score sanitization.

## Hard scope

Production files: exactly one preferred, `ap/queue.py`.

Test files: at most two:

- `tests/test_p0_queue_deferral_truth.py`
- one existing after-hours queue truth suite, preferably `tests/test_queue_truth_hardening.py`

Do not touch:

- scanners or signal scoring policy
- master-control admission thresholds
- selector policy or limits
- watcher ownership or trigger logic
- retry timing or counts
- broker submit/cancel
- order-state-machine behavior
- positions, exits, proof_trades, reconciliation
- database schema or migrations

Stop if implementation requires more than one production file or roughly 220 production lines.

## Required tests

### Signal compensation fencing

1. Current state WATCHING -> compensation transitions to ERROR.
2. Current state queued/triggered/submitted/filled/rejected/error -> zero-row compensation and state preserved.
3. Wrong client -> no mutation.
4. Wrong signal -> no mutation.
5. Missing row -> explicit missing outcome.
6. DB failure -> explicit DB_ERROR and CRITICAL log.

### Early queue failure fencing

1. PROCESSING + invalid client -> guarded ERROR transition.
2. PROCESSING + invalid mode -> guarded ERROR transition.
3. PROCESSING + invalid signal -> guarded ERROR transition.
4. PROCESSING + failed ap_signals write -> guarded ERROR transition.
5. Concurrent WATCHING or terminal row -> never overwritten.
6. Missing row and DB error -> no blind mutation; explicit log/outcome.

### Single WATCHING authority

1. After-hours contract-selection branch calls `_persist_watching_deferral()` exactly once.
2. It no longer directly calls `_mark_job(..., "WATCHING")`.
3. Malformed score such as `"A+"` reaches the helper without raising.
4. Counterfactual tracking runs only after durable deferral success.
5. Deferral failure produces no watcher, OSM, broker, order, position, exit, or proof mutation.

## Merge gate

- Focused tests green.
- All `ap.queue` importing tests green.
- Exact-head P0 green.
- Full diff confirms exactly one production file and no money-path changes.
- Independent final audit required.
- Do not merge without explicit user instruction.
