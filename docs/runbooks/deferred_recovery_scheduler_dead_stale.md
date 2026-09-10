# Deferred Recovery Scheduler Dead/Stale Runbook

## Scope

This runbook covers the ClientRunner deferred-materialization scheduler. It applies independently to each exact `client_id` and `execution_mode` (LIVE or PAPER).

The scheduler is a timer and caller boundary into the canonical recovery path. It is not a second broker-submit or cancel authority.

## Safety behavior

The scheduler is considered unsafe when:

- its thread is absent or no longer alive;
- its startup-ready handshake is not set;
- its completed-tick heartbeat is older than the configured heartbeat budget;
- the scheduler exits unexpectedly.

When unsafe, the runner must:

1. clear `entries_allowed`;
2. enter degraded mode with the scheduler-specific reason;
3. block new ENTRY work;
4. never launch an overlapping replacement scheduler;
5. preserve existing durable order state for forensic recovery.

This is an entry halt, not proof that an incident row has healed.

## Detection

Inspect runner logs and health state for:

- `DEFERRED_RECOVERY_SCHEDULER_DEAD`
- `DEFERRED_RECOVERY_SCHEDULER_NOT_READY`
- `DEFERRED_RECOVERY_SCHEDULER_STALE`
- `entries_allowed BLOCKED: deferred recovery scheduler unhealthy`

Record:

- client ID;
- execution mode;
- commit SHA;
- pod/instance ID;
- scheduler thread state;
- scheduler start time;
- last completed tick;
- heartbeat budget;
- deferred row local order ID, generation, lifecycle, retry attempt, and next retry time.

Never combine LIVE and PAPER evidence in one incident record.

## Recovery procedure

1. Confirm the affected client and mode from the runner manifest and logs.
2. Confirm `entries_allowed` is cleared. If it is not, stop the runner and treat the event as a fail-closed violation.
3. Confirm whether the scheduler thread is dead or merely stale.
4. Do not manually invoke a second recovery executor in the same runner.
5. Stop and restart the affected runner through the normal deployment/process supervisor.
6. Confirm startup recovery completes before scheduler readiness is published.
7. Confirm exactly one scheduler thread is alive and ready.
8. Confirm the first health check reports a fresh heartbeat.
9. Confirm entry permission is restored only after the scheduler gate passes.
10. Re-read the affected durable row using exact client, mode, local order, signal, canonical signal, generation, lifecycle, and broker-evidence predicates.
11. For a CCEP/deferred row, verify whether it is still `PENDING_TRIGGER`/`RETRY_WAIT`, `BROKER_READY`, terminal, or has broker submit evidence.
12. Do not declare the incident resolved merely because the scheduler is healthy. The row requires its own state/provenance/ownership verification.

## Incident classification

### Scheduler-only incident

The scheduler was dead/stale, but no deferred row was due or mutated.

Required evidence:

- zero unintended broker calls;
- zero position mutation;
- zero proof-trade mutation;
- zero queue/result corruption;
- scheduler restart and heartbeat recovery.

### Deferred-row incident

A due row was stranded, claimed, advanced, or left ambiguous while the scheduler was unhealthy.

Required evidence:

- exact row identity and mode;
- generation and owner history;
- retry-attempt history;
- selector/copyback state;
- submit-intent and broker-order evidence;
- proof that no duplicate submit occurred;
- proof that position/proof-trade state changed only after confirmed fill.

### CCEP incident

A confirmed-breach row lacks complete trigger provenance, has contradictory identity, or has ambiguous durable authority.

Required response:

- keep the row fail-closed;
- do not infer authority from `trigger_crossed_at` alone;
- do not manually clear metadata;
- preserve the row for exact PostgreSQL readback and replay;
- verify the confirmation writer and deferred claim path both write the four-field provenance.

## Do not do

- Do not start a second scheduler against the same runner.
- Do not manually mark a row `BROKER_READY`, `SUBMITTED`, or `FILLED`.
- Do not add broker IDs from memory or logs without exact broker/client/mode/order proof.
- Do not clear scheduler degraded reasons while the thread is dead or stale.
- Do not treat a successful process restart as proof that the original CCEP incident healed.
