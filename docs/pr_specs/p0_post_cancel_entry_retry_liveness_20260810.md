# P0 SPEC — Make post-cancel ENTRY retries durable, counted, and bounded

**Status: DRAFT / HARD HOLD. Current-main implementation contract only. Do not merge the old stale retry implementation into this branch.**

Base at branch creation: `main` / `570a56933615cbac82e3256c0e350119eca80c0d`.

This is a narrow current-main repair for the 2026-08-10 post-cancel retry lifecycle. It supersedes only this concrete slice of the older/stale #392 retry work. Do not resurrect #392's broad historical diff.

## Incident that created this PR

2026-08-10 Tradefluence PAPER submitted four entries visible at Tradier and all were canceled unfilled after `entry_max_age_normal_reached`:

- MO Aug 21 $69 PUT — 7 contracts, limit around `$2.01`
- META Aug 10 $597.50 CALL — 3 contracts, limit around `$3.27`
- INTC Aug 10 $98 PUT — 12 contracts, limit around `$0.63`
- AVGO Aug 14 $450 CALL — 6 contracts, limit around `$2.75`

The exact cancel reason is explicitly classified as retryable by the current post-cancel policy. Durable metadata showed the retry lifecycle was ARMED, but the opportunities did not produce the intended bounded replacement flow. Several ended `FAILED`; AVGO ended `ABORTED`; observed retry accounting did not show a clean consumed-attempt lifecycle.

Whether those trades later won is not authority to loosen risk gates. The defect is that a retry the system itself authorized did not execute through a coherent durable lifecycle.

## Verification already completed

Current `main` was read directly before opening this spec.

### Confirmed counter-key contract bug

`ap/post_cancel_retry.py:376-377` reads:

```python
prior_retries = int(meta.get("retry_attempts") or 0)
next_attempt = prior_retries + 1
```

The evaluator's contract and docstring use **plural** `retry_attempts`.

`ap/order_monitor.py:3200+`, `_maybe_arm_post_cancel_retry()`, writes:

```python
_meta["retry_status"]  = "ARMED"
_meta["retry_attempt"] = int(decision.attempt_number)
```

That is **singular** `retry_attempt`.

Therefore the consumer and producer do not share one counter key. A later `evaluate_retry()` can observe zero prior retries even after an attempt was armed/consumed.

This is a concrete current-main defect, not historical speculation.

### Confirmed non-atomic ARMED consumption

`ap/order_monitor.py:3237+`, `_check_armed_retries()` selects:

```sql
WHERE client_id = %s
  AND kind = 'ENTRY'
  AND status = 'CANCELED'
  AND meta ->> 'retry_status' = 'ARMED'
  AND COALESCE((meta ->> 'retry_ready_at')::float, 0) <= %s
```

It then directly calls `_submit_armed_retry(...)` for each selected row.

There is no durable `ARMED -> IN_FLIGHT` claim/CAS before `process_signal()`. Duplicate monitor invocations, overlapping recovery, or restart timing can therefore consume the same ARMED intent without a durable single-owner handoff.

### Confirmed first submit-time rejection becomes terminal FAILED

`ap/order_monitor.py:3289+`, `_submit_armed_retry()` correctly routes the retry through `ap.execution.process_signal()`. That is good and must remain: current dynamic risk, capital, symbol lock, quote refresh, chase, time and other admission gates remain authoritative.

But around current lines `3339+`, any `process_signal()` result with `ok=False` is stamped:

```python
status="FAILED"
```

No bounded second attempt is scheduled, even though `ap/post_cancel_retry.py` defines `ENTRY_RETRY_MAX_ATTEMPTS` default `2`.

### Confirmed retry policy already fail-closes unknown reasons

`ap/post_cancel_retry.py` has explicit retryable/non-retryable cancel-reason sets and treats unknown reasons as non-retryable. Keep that discipline. This PR must not turn every failure into an infinite chase loop.

## Root cause / failure class

The code has a good policy evaluator and a good fresh-admission submit seam, but the durable orchestration between them is inconsistent:

```text
CANCELED
-> ARMED (singular counter)
-> selected without durable claim
-> process_signal
-> first rejection => FAILED
```

The intended contract is bounded continuation:

```text
CANCELED
-> ARMED attempt N
-> exact durable claim
-> fresh process_signal gates
-> either SUBMITTED
   or terminal policy/risk ABORT
   or narrowly transient re-arm for N+1 <= max
```

## Required production changes

### File 1 — `ap/post_cancel_retry.py`

#### Change A — one canonical attempt counter

Add a small parser:

```python
def retry_attempt_count(meta: dict) -> int:
    """Return non-negative consumed/armed retry attempt count.

    Canonical key: retry_attempts.
    Legacy read compatibility: retry_attempt.
    Never accept negative/malformed values as authority.
    """
```

Recommended behavior:

```python
vals = []
for key in ("retry_attempts", "retry_attempt"):
    try:
        value = int(meta.get(key))
    except Exception:
        continue
    if value >= 0:
        vals.append(value)
return max(vals) if vals else 0
```

Then replace current line 376:

```python
prior_retries = int(meta.get("retry_attempts") or 0)
```

with:

```python
prior_retries = retry_attempt_count(meta)
```

Canonical new writes are plural `retry_attempts`. Singular remains read-only compatibility for one release; do not let two counters independently advance.

Do not change `ENTRY_RETRY_MAX_ATTEMPTS`, delay defaults, alignment math, or reason taxonomy in this file unless a focused replay proves a separate bug.

### File 2 — `ap/order_monitor.py`

#### Change B — write the canonical counter at ARM, current lines 3200+

Replace:

```python
_meta["retry_attempt"] = int(decision.attempt_number)
```

with canonical:

```python
_meta["retry_attempts"] = int(decision.attempt_number)
```

For compatibility, a mirrored singular field may be written temporarily only if an existing dashboard depends on it:

```python
_meta["retry_attempt"] = int(decision.attempt_number)  # legacy mirror only
```

If mirrored, all production authority still reads `retry_attempts` through the shared parser.

Also stamp:

```python
retry_last_transition = "ARMED"
retry_last_reason = decision.reason_code
```

Keep existing payload, cancel reason, ready time and diagnostics.

#### Change C — durable claim before `process_signal`, current `_check_armed_retries()` lines 3237+

Do not call `_submit_armed_retry()` directly from a stale SELECT result.

Add a helper:

```python
def _claim_armed_retry(
    self,
    *,
    local_order_id: str,
    expected_attempt: int,
) -> tuple[bool, dict, str]:
```

Use one PostgreSQL UPDATE/CAS:

```sql
UPDATE orders
SET meta = COALESCE(meta,'{}'::jsonb) || %s::jsonb,
    updated_ts = NOW()
WHERE client_id = %s
  AND local_order_id = %s
  AND kind = 'ENTRY'
  AND status = 'CANCELED'
  AND meta->>'retry_status' = 'ARMED'
  AND COALESCE((meta->>'retry_attempts')::int, 0) = %s
RETURNING local_order_id, contract, symbol, direction, execution_mode, meta
```

Patch:

```json
{
  "retry_status": "IN_FLIGHT",
  "retry_claimed_at": "<utc>",
  "retry_claim_owner": "order_monitor:<client_id>",
  "retry_last_transition": "IN_FLIGHT"
}
```

Only the worker that gets the row may call `process_signal()`.

Zero-row CAS:

- reread exact row;
- if already `SUBMITTED`/terminal -> no-op;
- if another valid `IN_FLIGHT` owner exists -> HOLD/no duplicate submit;
- never fall back to an unconditional submit.

The claim must preserve exact `client_id` and exact durable `execution_mode`; add execution-mode predicate to the SQL once normalized from the monitor's configured mode. Unknown/mismatched mode -> no claim.

#### Change D — classify submit-time failure, do not blindly retry policy failures

Keep `process_signal()` as the only submit/admission authority.

Create a **small explicit** submit-result classifier in `ap/order_monitor.py` or `ap/post_cancel_retry.py`:

```python
TRANSIENT_RETRY_SUBMIT_ERRORS = frozenset({...})
TERMINAL_RETRY_SUBMIT_ERRORS = frozenset({...})
```

Before coding the set, grep current `ap.execution.process_signal` result vocabulary and tests. Do not invent reason strings.

Policy:

- dynamic risk/policy/account blocks are terminal for this retry generation and must not be re-armed;
- price running away from allowed chase band is terminal (`runaway_quote_at_submit` stays terminal);
- kill switch/read-only/client inactive/positions full/daily cap/time gate/trend or thesis invalidation remain terminal according to current authority;
- only proven transient infrastructure/quote-refresh/temporary-lock outcomes may consume one attempt and schedule the next bounded attempt.

Candidate transient errors must be proven from current `process_signal()` implementation. Examples to investigate, not blindly adopt: `quote_refresh_failed`, `submit_quote_unavailable`, a stale self-owned symbol lock, broker transport/transient admission error.

#### Change E — bounded re-arm after a proven transient reject

If submit result is transient **and**:

```python
current_attempt < ENTRY_RETRY_MAX_ATTEMPTS
```

then write the same canceled parent row back to `ARMED` with:

- `retry_attempts = current_attempt` (already consumed; evaluator will create `current+1` if re-evaluated) OR directly schedule `next_attempt=current+1` consistently — choose one model and test it;
- fresh `retry_ready_at` using the existing retry delay helper;
- `retry_status_detail = transient_submit_reject_rearmed:<error>`;
- complete submit quote diagnostics;
- no new broker id/local id because no replacement order was accepted.

Preferred simpler model: the claim itself represents attempt N. On transient reject, increment to N+1 when re-arming and ensure max is checked before the write. Do not call `evaluate_retry()` again if that would re-run cancel-reason semantics unnecessarily; reuse its delay helper/policy without duplicating direction math.

At max attempts:

```text
retry_status = EXHAUSTED
retry_status_detail = retry_max_attempts_exhausted:<last_error>
```

Do not leave `FAILED + attempts=0` for a consumed retry.

#### Change F — successful submit proves replacement identity

Existing success path writes `retry_new_local_order_id` and `retry_new_broker_order_id`. Strengthen acceptance:

- `result.ok=True` is not enough if `local_order_id` is blank;
- require a nonblank new local order id;
- if broker id is blank because the new order is legitimately pre-submit/deferred, preserve its exact lifecycle status and owner rather than claiming broker submission;
- use a status name that matches truth (`SUBMITTED` only if the returned contract means accepted continuation under current process_signal contract; otherwise `HANDED_OFF` if necessary).

Do not broaden this unless current process_signal return shape proves the current `SUBMITTED` name is inaccurate.

#### Change G — restart-safe stale retry recovery

Add bounded stale-claim recovery for the new state.

Before re-arming a stale `IN_FLIGHT` row, prove no replacement order exists using the durable identity stored in retry metadata / canonical signal lineage. If replacement existence is ambiguous, HOLD. Never resubmit because a timer expired alone.

The stale timer may scan both `IN_FLIGHT` and `SUBMITTING` rows so they can be
classified under the same exact client/mode fence. Their recovery outcomes are
different: an abandoned `IN_FLIGHT` row has not crossed the final submit fence
and may receive one bounded re-arm after no replacement is proven; an abandoned
`SUBMITTING` row may still belong to a worker whose symbol-lock lease expired
while it was in the broker/admission path, so it is quarantined to `HOLD` when
no exact replacement proof exists. A stale `SUBMITTING` timer must never
re-arm the parent.

No new scheduler. Use order monitor/recovery's existing tick.

#### Change H — fenced parent ARM/ABORT metadata writes

The cancel hook must update the canceled ENTRY parent through one exact
snapshot CAS. ARM and ABORT writes require the monitor's explicitly wired
`live|paper` mode and must match `client_id`, `kind='ENTRY'`,
`status='CANCELED'`, the durable mode predicate, and the complete prior JSONB
snapshot. A zero-row CAS is a failed/contended write and must not emit a
durable retry lifecycle transition or schedule a submit. The legacy unscoped
`ap.db.update_order(..., meta=...)` path is not an authority for these writes.

### Tests — `tests/test_p0_post_cancel_entry_retry_liveness.py`

Required load-bearing cases:

1. **Counter mismatch regression**: legacy/singular `retry_attempt=1` is read as one; next attempt is 2.
2. Canonical `retry_attempts=1` -> next attempt 2.
3. Both keys disagree -> parser uses safe monotonic max; no counter regression.
4. ARM writes canonical plural counter.
5. Two workers observe same ARMED row -> exactly one CAS claims `IN_FLIGHT`; exactly one `process_signal()` call.
6. Claim requires exact client id.
7. Claim requires exact execution mode; PAPER cannot consume LIVE row and vice versa.
8. Attempt 1 transient submit rejection -> bounded re-arm for attempt 2.
9. Attempt 2 transient rejection -> `EXHAUSTED`; no attempt 3.
10. `runaway_quote_at_submit` -> terminal, no re-arm.
11. `risk_gate_blocked` -> terminal, no re-arm.
12. `positions_full` -> terminal, no re-arm.
13. `daily_trade_cap` -> terminal, no re-arm.
14. kill switch/read-only/client-inactive result -> terminal, no re-arm.
15. success -> parent carries exact new local id and broker id/status truth.
16. malformed retry payload -> terminal with useful detail and consumed attempt visible.
17. `process_signal` exception -> classified detail; no silent zero-attempt FAILED.
18. stale IN_FLIGHT + proven existing replacement -> no resubmit.
19. stale IN_FLIGHT + broker/order identity ambiguous -> HOLD, no resubmit.
20. restart stale IN_FLIGHT + proven no replacement -> at most one safe bounded recovery.
21. stale SUBMITTING + no replacement proof -> HOLD, never timer-rearmed.
22. ARM/ABORT metadata writes require exact client/status/kind/mode/snapshot CAS.
23. two real PostgreSQL sessions racing the ARMED CAS yield one claimant.
24. no direct broker submit call is introduced in order monitor; submission remains through `ap.execution.process_signal`.
25. no broker cancel added.
26. original cancel reason `entry_max_age_normal_reached` remains retryable.
27. unknown cancel reason remains fail-closed.

Use the MO/META/INTC/AVGO shapes as fixture labels where useful, but assertions must test lifecycle truth, not whether the market later went green.

## Explicit non-goals

Do not change:

- selector thresholds;
- DTE/delta/spread/OI/volume policy;
- sizing;
- daily trade limits;
- risk admission;
- score policy;
- chase/runaway threshold;
- underlying thesis rules;
- exit engine;
- reconciler/proof trades;
- queue fanout;
- max retry count (`2`) without a separate reviewed policy change.

Do not use this PR to force-fill trades that current market/risk truth rejects.

## Production file budget and scope reconciliation

Expected maximum:

1. `ap/post_cancel_retry.py`
2. `ap/order_monitor.py`
3. `ap/execution.py` — the narrowly justified current-main primitive already
   carried by this branch: its explicit metadata allow-list preserves retry
   lineage/mode before the durable replacement insert, and its exception result
   returns the exact `local_order_id` for reconciliation.
4. `tests/test_p0_post_cancel_entry_retry_liveness.py`
5. `tests/test_phase9_retry_wire_in.py` — existing wire-in fixtures updated
   only to pass explicit mode and observe the fenced parent-write seam.
6. `.github/workflows/p0_regression.yml` only for CI wiring

The `ap/execution.py` change is the required durable primitive proven missing on
current `main`; no further execution-core expansion is part of this PR. The
corrections in this amendment stay within the three production-file budget.

## Money-path audit answers

- Changes live behavior: **YES**, post-cancel ENTRY continuation.
- Flag-off or active: `ENTRY_RETRY_ENABLED` defaults active; respect existing flag.
- Broker submit: no new direct POST; existing `process_signal` path only.
- Broker cancel: **NO new cancel behavior**.
- Orders mutation: **YES**, retry metadata CAS only.
- Positions mutation: no new writer.
- proof_trades mutation: **NO**.
- Queue mutation: **NO**.
- `client_id`: exact.
- `execution_mode`: exact `live|paper`, no inference/default.
- Production metadata shape: current `orders.meta` JSONB; no migration required.
- Diagnostics: make attempts/claim/failure/exhaustion explicit and monotonic.
- PAPER/LIVE pollution: claim fence must prevent it.
- Could make Jason trade junk: only if dynamic gates are bypassed; this PR explicitly forbids that. Every accepted retry still goes through current `process_signal()` authority.

## Required validation before merge consideration

```bash
python -m pytest -q \
  tests/test_p0_post_cancel_entry_retry_liveness.py \
  tests/test_phase9_retry_wire_in.py \
  tests/test_phase5_post_cancel_retry.py
python -m py_compile ap/post_cancel_retry.py ap/order_monitor.py

git diff --check
```

Also run the current authoritative entry/recovery tests that touch order monitor and exact-head P0 CI.

Required source checks:

```bash
grep -R "retry_attempt" -n ap/post_cancel_retry.py ap/order_monitor.py
grep -R "retry_status.*ARMED\|retry_status.*IN_FLIGHT\|retry_status.*EXHAUSTED" -n ap/order_monitor.py
grep -R "process_signal" -n ap/order_monitor.py
```

Acceptance sequence to prove directly:

```text
CANCELED retryable reason
-> ARMED attempt 1
-> one durable claimant
-> fresh process_signal gates
-> transient reject only: ARMED attempt 2
-> one durable claimant
-> SUBMITTED/HANDED_OFF or EXHAUSTED
```

At no point may two workers submit the same retry intent, a PAPER worker consume a LIVE retry, or a risk/policy reject be converted into a forced trade.

Final review must inspect description, cumulative diff, comments, changed files, exact submit path, real production metadata shape, broker exposure, order/position/proof/queue mutations, client/mode identity, diagnostics, PAPER/LIVE taxonomy, Jason safety, and issue **MERGE / HOLD / HARD HOLD**.
