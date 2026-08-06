# P0: Reserve one post-open overnight reevaluation attempt

## Status

Draft implementation contract. Do not merge or deploy until the accompanying patch is applied to production source, focused tests pass, and exact-head CI is green.

## Incident

On 2026-08-06 the overnight reevaluation scheduler used all six configured attempts before the regular session opened. Each attempt ran approximately 90 seconds after the previous one and classified the current rows as `RETRYABLE_ALL_DEFERRED` because the option market was closed. The sixth premarket result set `OVERNIGHT_REEVAL_RETRY_EXHAUSTED` before 09:30 ET.

Consequences observed after the bell:

- Jason LIVE remained `BLOCKED` with `overnight_reeval_missing`.
- Jose and Tradefluence PAPER remained `DEGRADED`.
- Current-day scanner rows existed, but no post-open reevaluation was available to process them.
- Restarting the runner reset process-local exhaustion, demonstrating that the defect was scheduler liveness rather than missing scanner data.

## Root cause

The retry budget is counted by attempt, but the scheduler does not distinguish an attempt that can observe an open option market from one that can only return market-closed deferral. Six short-interval premarket passes can therefore consume the complete daily budget before any attempt is capable of producing a valid post-open decision.

## Required invariant

If every consumed premarket attempt for the current Eastern trading date returns exactly `RETRYABLE_ALL_DEFERRED`, the scheduler must reserve one and only one attempt for `09:30:05 ET`.

The reservation must not apply when any premarket attempt reports row errors, terminal errors, exceptions, mixed classifications, or another non-all-deferred result.

After the reserved attempt runs:

- a completed result follows the existing success, handoff, and readiness path;
- a retryable result at the exhausted budget becomes honest `RETRY_EXHAUSTED`;
- no second reserved attempt is permitted;
- restart and force-run behavior remain governed by their existing contracts.

## Proposed implementation

The review patch in `tools/_apply_p0_post_open_reeval.py` makes these changes in `client_runner.py`:

1. Add daily state for:
   - whether all premarket attempts have remained all-deferred;
   - whether the post-open retry is reserved;
   - whether a post-open attempt has already executed.
2. Reset all three fields when the Eastern trading date changes.
3. Resolve the reserved timestamp from the existing `OVERNIGHT_REEVAL_POST_OPEN_RETRY_DELAY_SEC`, producing 09:30:05 ET under the current default.
4. Preserve the existing 90-second premarket cadence while the ordinary budget remains.
5. At the sixth attempt, reserve 09:30:05 only under the narrow all-deferred predicate.
6. Permit the max-attempt guard to admit that single reserved run.
7. Persist reservation, execution, and all-deferred diagnostics into the result and handoff lock metadata.
8. Clear the reservation on completion or when the post-open run begins.

## Regression tests

The patch extends `tests/test_p0_overnight_reeval_retry_liveness.py` with four required cases:

1. Six all-deferred premarket attempts reserve 09:30:05 and do not set the exhausted date.
2. The reserved seventh attempt runs after open, completes, and triggers post-reevaluation handoff exactly once.
3. A reserved post-open attempt that is still retryable may exhaust, and a subsequent call cannot execute an eighth engine run.
4. A mixed premarket sequence containing row errors does not earn the reservation and exhausts under the existing policy.

Focused validation command:

```bash
python -m pytest \
  tests/test_p0_overnight_reeval_retry_liveness.py \
  tests/test_p0_readiness_deadline_enforcement.py \
  -q --tb=short
python -m py_compile client_runner.py tests/test_p0_overnight_reeval_retry_liveness.py
git diff --check
```

## Non-goals

This PR must not change:

- scanner schedules or fanout;
- contract selector gates, spreads, liquidity, DTE, or affordability;
- broker submit or cancel behavior;
- order, position, proof-trade, or queue mutation logic;
- client identity or execution-mode routing;
- retry count defaults or the ordinary premarket retry interval;
- readiness severity differences between LIVE and PAPER.

## Safety review

- No broker call is added.
- No database row is promoted manually.
- No existing exhausted date is cleared outside the daily reset and existing force-run contracts.
- The additional attempt is bounded to one per date and only after six homogeneous market-closed deferrals.
- Mixed or malformed outcomes remain fail-closed.

## Observability

The result and lock payload must expose:

- `post_open_retry_reserved`
- `post_open_attempt_performed`
- `premarket_all_deferred`
- `next_retry_at`
- `attempt_count`
- `result_class`

An operator must be able to prove from durable lock metadata whether 09:30:05 was reserved, executed, completed, or exhausted.

## Rollout and rollback

1. Review the exact base-to-head diff and all test changes.
2. Require focused tests and the full P0 workflow on the exact head SHA.
3. Deploy only through the normal Render deployment path.
4. Verify next-session lock metadata contains the reserved-attempt fields.
5. Roll back by reverting this isolated commit. No schema rollback is required.

## Reviewer checklist

- Confirm the reservation predicate cannot be satisfied by mixed row errors.
- Confirm no more than one engine call can occur beyond `OVERNIGHT_REEVAL_MAX_ATTEMPTS`.
- Confirm force-run semantics are unchanged.
- Confirm completion still invokes morning handoff and readiness exactly once.
- Confirm no broker, queue, order, position, or scanner code changes are present.
- Confirm the branch is not mergeable while it contains only the review patcher rather than the applied production diff.
