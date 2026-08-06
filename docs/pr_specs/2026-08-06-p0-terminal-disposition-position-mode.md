# P0: Preserve terminal callback disposition and mode-scoped position truth

## Status

Draft implementation contract. Do not merge or deploy until the review patch is applied, the resulting production diff is independently audited, focused tests pass, and exact-head CI is green.

## Incident evidence

During the August 6, 2026 LIVE FAST breach:

- the setup reached breach-time capital validation and contract selection;
- capital was available;
- no valid FAST option contract survived spread, quote, liquidity, and affordability gates;
- no broker order was submitted;
- the terminal order had no broker order ID, submitted timestamp, fill timestamp, or open position;
- the position gate logged `snapshot_execution_mode_required:None` and fell back to the process-local count.

The selector rejection itself was correct and is not changed here.

A deeper source audit also found one concrete callback-contract defect adjacent to the terminal selector path: when the selector returns a non-null but structurally invalid result, the branch invokes `_terminalize_deferred_breach_failure(...)` without returning the resulting durable disposition. The watcher callback can therefore receive `None` even though terminalization was attempted. Other deferred selector terminal branches already return the disposition.

## Confirmed defects

### 1. Position snapshot identity is omitted

`APPositionManager.snapshot()` requires a keyword-only `mode` so Postgres position and pending-entry counts remain scoped to LIVE or PAPER. Both execution-core capacity helpers currently call `snapshot()` without a mode.

The resulting exception is caught, but LIVE capacity truth then degrades to a local process counter. That fallback did not block FAST, yet it weakens the authoritative capital gate and can hide cross-process positions after restart.

### 2. One terminal selector branch drops the disposition

`_terminalize_deferred_breach_failure()` returns a dictionary such as:

```python
{
    "disposition": "TERMINAL_DURABLE",
    "reason_code": "...",
    "terminal_status": "EXPIRED",
}
```

The invalid-selector-result branch calls the helper without `return`. The nested `_terminalize_breach_failure` function is also annotated as returning `None` even though all meaningful paths return a disposition dictionary.

The watcher contract requires the callback result to prove durable terminal, retry, or reconciliation ownership before removing or retaining the watcher. A dropped result creates ambiguity and can cause duplicate callback attempts or quarantine behavior after the order has already moved.

## Proposed implementation

The review patch in `tools/_apply_p0_terminal_disposition_position_mode.py` changes `ap_execution_core.py` only:

1. Add `_position_snapshot_mode()` that resolves canonical lowercase `live` or `paper` from the runner identity.
2. Fail closed on missing or invalid execution mode instead of inventing a default.
3. Pass `mode=self._position_snapshot_mode()` to both authoritative position snapshot calls.
4. Preserve the existing local-count fallback behavior when the snapshot itself raises; this PR does not redesign capacity policy.
5. Return `_terminalize_deferred_breach_failure(...)` from the invalid-selector-result branch.
6. Correct `_terminalize_breach_failure`'s annotation to its actual dictionary contract.

## Regression tests

The patch creates `tests/test_p0_terminal_disposition_position_mode.py` with these cases:

1. LIVE open-position and pending-entry counts call `snapshot(mode="live")`.
2. PAPER counts call `snapshot(mode="paper")`.
3. Invalid/missing mode never invokes an unscoped snapshot and follows the existing fallback behavior.
4. The invalid-selector-result branch explicitly returns the terminal disposition.
5. The terminal helper declares the dictionary contract it already implements.

Focused validation command:

```bash
python -m pytest \
  tests/test_p0_terminal_disposition_position_mode.py \
  tests/test_p0_deferred_breach_lifecycle_completion.py \
  tests/test_p0_pending_trigger_lifecycle_integrity.py \
  tests/test_p0_fenced_retry_terminalization.py \
  -q --tb=short
python -m py_compile ap_execution_core.py tests/test_p0_terminal_disposition_position_mode.py
git diff --check
```

## What this PR deliberately does not change

- No selector spread, OI, volume, delta, DTE, quote, affordability, or acceptance-cap gate.
- No broker submit or cancel call.
- No OSM transition graph change. The current graph already permits `PENDING_TRIGGER -> ERROR`; making that transition legal is not needed.
- No direct `trade_queue` status mutation. The observed `ARMED` queue row is retained as evidence, but the queue's terminal ownership authority must be proven before changing it.
- No order, position, proof-trade, or signal taxonomy rewrite.
- No client ID or execution-mode normalization beyond passing the already authoritative mode into the position snapshot.
- No change to LIVE versus PAPER capital thresholds.

## Safety analysis

- Passing mode narrows reads; it does not create or mutate trading state.
- Invalid mode does not query both modes or default to PAPER/LIVE.
- Returning the existing terminal dictionary does not add a terminal write; it communicates the write's outcome to the watcher that already owns the callback.
- No broker request can be reached through either change.
- The selector's correct FAST no-contract outcome remains identical.

## Observability expectations

After deployment:

- the warning `snapshot_execution_mode_required:None` must disappear from valid LIVE and PAPER runners;
- position snapshots must be attributable to `mode=live` or `mode=paper` in test and debug instrumentation;
- invalid selector-result terminals must produce a callback disposition of `TERMINAL_DURABLE` or `KEEP_WATCHER`, never an implicit `None`;
- no additional broker submission should occur for a terminal selector result.

## Rollout and rollback

1. Apply the patcher on this branch.
2. Inspect the complete base-to-head diff. Expected production scope is `ap_execution_core.py` only, plus one new focused test.
3. Run focused tests and the full P0 workflow against the exact head SHA.
4. Deploy through the normal Render path.
5. Verify LIVE and PAPER logs no longer show missing-mode snapshot errors.
6. Verify a forced invalid-selector-result test produces one durable terminal disposition and zero broker calls.
7. Roll back by reverting the isolated commit. No schema or data rollback is required.

## Reviewer checklist

- Confirm `_position_snapshot_mode()` cannot silently map unknown identity to either mode.
- Confirm both snapshot call sites pass the keyword-only mode.
- Confirm no position mutation is introduced.
- Confirm the invalid selector branch exits immediately with the terminal disposition.
- Confirm no existing successful or retryable selector path changes.
- Confirm the OSM transition graph is untouched.
- Confirm queue status is not mutated without a proven ownership contract.
- Confirm exact-head focused tests and full P0 CI are green before removing Draft/HOLD.
