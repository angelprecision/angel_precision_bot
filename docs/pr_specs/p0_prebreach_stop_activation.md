# P0: Activate scanner stop only after first entry-direction breach

## Status

**DRAFT IMPLEMENTATION CONTRACT ONLY. DO NOT MERGE OR DEPLOY.**

This branch preserves the July 29, 2026 BAC PUT production incident and the exact surgical repair contract. Production code is not changed by this specification commit.

## Production incident

Canonical signal:

- signal ID: `777b5c45-8cb9-4ac9-920b-5e08687c1450`
- ticker/side: `BAC PUT`
- entry trigger: `61.90`
- scanner stop: `62.49`
- target: `61.31`
- score/tier: `70 / B`

Tradefluence PAPER later entered `BAC260731P00062000` and recorded approximately `+158.57%` MFE.

Jason LIVE never reached contract selection or broker submission. Its local order `f49f954c-5f0a-4ba5-8f39-f0730701fb6e` was created at approximately 09:31:39 ET and canceled about 0.32 seconds later as `stop_already_broken_terminal` while still using `DEFERRED:BAC` and having no broker order.

The arm-time quote was:

- bid `62.45`
- ask `62.49`
- midpoint `62.47`

For a PUT with trigger `61.90`, the canonical entry-side quote was still above the trigger. No PUT entry breach had occurred. The watcher nevertheless treated `ask >= stop` as a terminal stop violation before the first entry-direction breach.

## Root cause

The regular-session late-attachment path runs the pending-trigger classifier at watcher arm time. The classifier evaluates the scanner stop before proving that the trigger has ever been breached.

Current behavior effectively permits this sequence:

1. Signal has never crossed its entry trigger.
2. Current quote is still on the normal pre-trigger side.
3. Opposite-side stop geometry is evaluated anyway.
4. Order is terminally canceled before the setup can become active.

The detail string is also inconsistent for PUTs: control uses the ask for stop evaluation, while the diagnostic may report the bid as `stop_broken_at_bid`. That makes post-incident traces materially misleading.

## Required production invariant

The scanner stop is dormant until the first confirmed entry-direction breach for that signal/client/mode lifecycle.

Before first breach:

- CALL remains eligible while the canonical CALL trigger quote is below the trigger, even when it is at or below the scanner stop.
- PUT remains eligible while the canonical PUT trigger quote is above the trigger, even when the ask is at or above the scanner stop.
- The classifier returns a nonterminal pre-trigger disposition.
- No terminal order mutation occurs.
- No broker submit or cancel occurs.

After first breach:

- The stop becomes active for the remainder of the same durable lifecycle.
- Restart/reattachment must preserve that activation state.
- CALL stop truth remains side-aware.
- PUT stop truth remains side-aware.
- The diagnostic must name the same quote side/value that authorized the decision.

## Intended implementation scope

Expected production files only:

1. `ap/pending_trigger_classifier.py`
2. `ap_entry_watcher.py`
3. A focused test file, preferably `tests/test_p0_prebreach_stop_activation.py`
4. Amend only the directly contradictory assertions in `tests/test_p0_late_attachment_continuation.py`

Do not add a schema migration unless existing durable trigger evidence cannot be reused.

### Classifier contract

Add an explicit input representing durable stop activation, such as `trigger_previously_breached` or `stop_activated`. Do not infer activation merely from the current stop geometry.

Required ordering:

1. Validate side, trigger, stop, target, and quote inputs.
2. Resolve whether durable first-breach evidence exists.
3. When no durable breach exists and the current canonical trigger quote is still pre-trigger, return `PRE_TRIGGER`/keep-watching before evaluating the stop.
4. When the current quote confirms a first breach, record or propagate durable breach evidence before later lifecycle decisions rely on it.
5. Evaluate scanner-stop terminalization only when durable stop activation is true.
6. Emit diagnostics using the actual controlling side:
   - CALL: the exact CALL stop-authority quote and value.
   - PUT: the exact PUT stop-authority quote and value.

### Durable evidence

Prefer existing order/signal metadata already used for trigger lifecycle evidence, including fields such as `trigger_crossed_at` or `trigger_confirmed_at`, rather than creating a second competing truth system.

The watcher must bind the evidence to:

- canonical signal ID
- client ID
- normalized execution mode
- local order/generation where applicable

Cross-client, cross-mode, or stale-generation evidence must not activate the stop.

### Runtime behavior

The change is active behavior, but it is limited to pending-entry classification before broker submission.

It must not:

- bypass trigger confirmation
- directly choose a contract
- change selector thresholds or budgets
- change account sizing
- add broker-submit authority
- add broker-cancel authority
- mutate positions or proof trades
- change exit behavior
- change queue CAS policy

## Required regression coverage

### Exact BAC replay

Input:

- PUT trigger `61.90`
- stop `62.49`
- bid `62.45`
- ask `62.49`
- no durable prior breach

Expected:

- nonterminal pre-trigger result
- watcher remains eligible/armed
- no `STOP_ALREADY_BROKEN_TERMINAL`
- no order terminalization
- no broker callback

### Activated BAC mirror

Same quote geometry, but with valid durable prior trigger-breach evidence.

Expected:

- stop may terminalize under the existing side-aware rule
- reason identifies the controlling PUT ask, not the bid
- one terminal mutation only

### CALL mirror

Prove equivalent pre-breach and post-breach behavior for CALL geometry.

### Restart durability

1. Confirm first trigger breach.
2. Persist existing durable breach evidence.
3. Recreate/recover watcher.
4. Prove the stop remains activated for the same client/mode/signal lifecycle.
5. Prove evidence from another client, mode, or generation is rejected.

### Idempotency

Repeated classifier/watcher evaluations must not:

- duplicate terminal writes
- create duplicate local orders
- submit twice
- cancel a nonexistent broker order

## Acceptance criteria

- BAC pre-breach replay remains watchable.
- Post-breach stop protection is unchanged or stronger.
- Restart does not forget stop activation.
- PUT diagnostics report the same ask used for control.
- PAPER and LIVE share the same geometry classification; later differences may only come from explicit mode/account policies.
- Focused tests pass.
- Adjacent watcher/recovery tests pass.
- Exact-head P0 CI passes.
- Production-safety audit confirms no selector, sizing, submit/cancel, position, proof, exit, or queue regression.

## Required final report

Return:

- previous base SHA
- exact final head SHA
- exact files changed
- exact production lines/functions changed
- focused test command and result
- adjacent test command and result
- exact-head CI run/job/result
- BAC replay output
- CALL mirror output
- restart durability proof
- explicit statement that no merge or deployment occurred
