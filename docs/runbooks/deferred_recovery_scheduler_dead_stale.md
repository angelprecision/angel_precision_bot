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

1. publish a critical scheduler-health diagnostic;
2. before the first startup unlock, keep `entries_allowed` cleared and enter
   degraded mode with the scheduler-specific reason;
3. after startup, a dead child may request one controlled replacement of the
   owning `ClientRunner` through the existing supervisor;
4. after startup, a stale child that is still alive is a process/pod restart
   condition: clear entry permission, retain the runner tombstone, and do not
   request or create an in-process replacement;
5. never launch an overlapping replacement scheduler or runner while the old
   scheduler child is still alive;
6. preserve existing durable order state for forensic recovery.

The startup hold is an entry halt.  A post-startup scheduler fault is a
deferred-recovery subsystem incident, not proof that an incident row has
healed. A dead child can be replaced only after it has exited. A stale but
still-alive child cannot be killed safely from Python; the runner is stopped,
the process/pod restart requirement is exposed, and the external process
supervisor must restart the process before any new runner is created.

## Detection

Inspect runner logs and health state for:

- `DEFERRED_RECOVERY_SCHEDULER_DEAD`
- `DEFERRED_RECOVERY_SCHEDULER_NOT_READY`
- `DEFERRED_RECOVERY_SCHEDULER_STALE`
- `DEFERRED_RECOVERY_PROCESS_RESTART_REQUIRED`
- `entries_allowed BLOCKED: deferred recovery scheduler unhealthy`
- `deferred_recovery_process_restart_required=true` in runner status;
- a separate scheduler health state containing the fault reason;
- `last_deferred_recovery_completed_ts` for liveness;
- `last_deferred_recovery_success_ts` and `deferred_recovery_last_error_ts`
  for recovery outcome health.

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
2. Confirm `entries_allowed` is cleared while the affected runner is stopped
   or awaiting restart. Treat the event as a deferred-recovery subsystem
   incident, not as evidence that any durable row healed.
3. Confirm whether the scheduler thread is dead or merely stale.
4. Do not manually invoke a second recovery executor in the same runner.
5. If the child is dead, confirm the runner emitted
   `DEFERRED_RECOVERY_SUPERVISOR_RESTART_REQUESTED`; wait for the child to
   exit before the replacement runner starts.
6. If the child is stale and still alive, confirm the runner emitted
   `DEFERRED_RECOVERY_PROCESS_RESTART_REQUIRED`. Do not wait for an in-process
   replacement: the old child cannot be safely killed, the registry tombstone
   is intentional, and the normal process/pod supervisor must restart the
   process. Confirm no replacement runner or second recovery consumer starts
   before that restart.
7. Confirm startup recovery completes before scheduler readiness is published.
8. Confirm exactly one scheduler thread is alive and ready after the external
   restart.
9. Confirm startup entry permission is restored only after the scheduler
   readiness gate passes.
10. Re-read the affected durable row using exact client, mode, local order, signal, canonical signal, generation, lifecycle, and broker-evidence predicates.
11. For a CCEP/deferred row, verify whether it is still `PENDING_TRIGGER`/`RETRY_WAIT`, `BROKER_READY`, terminal, or has broker submit evidence.
12. Compare liveness (`last_deferred_recovery_completed_ts`) separately from
   outcome health (`last_deferred_recovery_success_ts` and the error
   timestamp).
13. Do not declare the incident resolved merely because the scheduler is healthy. The row requires its own state/provenance/ownership verification.

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
- Do not clear scheduler health reasons while the thread is dead or stale.
- Do not start an in-runner scheduler replacement or bypass the supervisor's
  old-child exit check.
- Do not treat a stale, still-alive child as self-healable. Escalate
  `DEFERRED_RECOVERY_PROCESS_RESTART_REQUIRED` to the normal process/pod
  restart authority and leave durable rows untouched until restart recovery
  rereads them.
- Do not treat a successful process restart as proof that the original CCEP incident healed.
