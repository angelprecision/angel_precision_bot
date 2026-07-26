# P0 Queue Deferral Truth

## Status

DRAFT IMPLEMENTATION CONTRACT ONLY. Production logic is not implemented by this branch. Do not merge until code and tests are added.

## Problem

`ap/queue.py` has more than one after-hours / deferral writer. One path correctly writes `ap_signals` first with `decision_status="WATCHING"` and only then marks the `trade_queue` row `WATCHING`. Another master-control deferrable-block path directly writes `decision_status="watching"` lowercase after already marking the queue row `REJECTED`.

That creates split truth:

- `trade_queue.status = REJECTED`
- `ap_signals.decision_status = watching`
- casing may not match overnight reeval queries expecting `WATCHING`
- operators see a rejected queue row while overnight logic may treat the same signal as pending watch material

This is exactly the kind of invisible state mismatch that makes a working bot look like it has no trade flow. Humanity survives another day if we stop writing contradictory statuses.

## Scope

Touch only queue deferral and signal-decision persistence.

Allowed files:

- `ap/queue.py`
- focused tests under `tests/`

Do not touch scanner selection, score thresholds, contract-selection quality rules, broker submit/cancel, exits, proof trades, or reconciler.

## Required behavior

### 1. Define canonical decision-status constants

Add constants near queue status constants:

```python
DECISION_REJECTED = "rejected"
DECISION_WATCHING = "WATCHING"
DECISION_QUEUED = "queued"
```

Use `DECISION_WATCHING` everywhere deferral rows need overnight discovery.

No lowercase `"watching"` may be written by queue code.

### 2. One helper must own deferral persistence

Create a helper like:

```python
def _defer_signal_to_watching(...):
    ...
```

It must:

1. write/upsert the `ap_signals` row first;
2. use `decision_status=DECISION_WATCHING`;
3. use `canonical_client_email(client_id)`;
4. include stage/reason/queued_at/raw payload;
5. return `True` only if the signal row write succeeded;
6. only then allow caller to mark `trade_queue.status = WATCHING`.

If `ap_signals` write fails, queue must become `ERROR`, not phantom `WATCHING`.

Required error:

```text
ap_signals_write_failed:after_hours_deferred
```

### 3. Remove split truth in MC deferrable block path

The master-control blocked path currently marks the queue job `REJECTED` and then, outside market hours for deferrable reasons, directly upserts `ap_signals` as lowercase `watching`.

Replace that path with one of two explicit policies:

#### Preferred policy

If the reason is deferrable and market is closed, call the shared deferral helper and mark queue `WATCHING` if the signal write succeeds.

#### Acceptable conservative policy

If the master-control decision is a true rejection, do not write `ap_signals WATCHING` at all. Leave both queue and ap_signals rejected.

Do not allow queue `REJECTED` + ap_signals `WATCHING` for the same lifecycle.

### 4. Paper overnight rescue route must also prove ap_signals discoverability

For the `_paper_overnight_reeval_only_enabled(...)` early route, do not mark queue `WATCHING` unless the corresponding `ap_signals` row exists or is successfully upserted with `DECISION_WATCHING`.

If the signal write fails, mark queue `ERROR` with:

```text
ap_signals_write_failed:paper_overnight_reeval_only
```

### 5. Queue/ap_signals client identity must be canonical

All signal persistence must use:

```python
canonical_client_email(client_id)
```

Direct updates must filter by both `signal_id` and canonical client email.

## Acceptance tests

Add focused tests proving:

1. After-hours daily signal writes `ap_signals.decision_status == "WATCHING"` and queue `WATCHING`.
2. `ap_signals` write failure produces queue `ERROR`, not queue `WATCHING`.
3. MC deferrable block outside market hours does not produce queue `REJECTED` + ap_signals `WATCHING` split truth.
4. Lowercase `"watching"` is not emitted by queue signal persistence.
5. Paper overnight reeval-only route upserts/ensures `ap_signals WATCHING` before queue `WATCHING`.
6. Same `signal_id` across two clients does not cross-update client rows.

## Merge gate

Merge only when exact-head focused tests pass and a grep/diff review proves no remaining direct lowercase `decision_status="watching"` write exists in queue code.
