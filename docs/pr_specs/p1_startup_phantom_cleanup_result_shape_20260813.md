# P1 SPEC: Make LIVE startup phantom cleanup result-shape safe

## STATUS

**P1 / DRAFT / HOLD / IMPLEMENTATION WORK ORDER**

Base: `main@3ac102dab320007409107ad36cc9494897d2799e`.

This PR owns one defect only: the 2026-08-13 Jason LIVE startup exception from `_clear_old_phantom_orders()`. It must not become a cleanup rewrite, PENDING_TRIGGER rewrite, WATCHING recovery change, broker recovery change, or exit/proof change.

## PRODUCTION INCIDENT

Jason LIVE startup emitted:

`[jasoncosby1@gmail.com] Startup phantom clear: tuple index out of range`

Immediately afterward the runner continued initialization, loaded the position manager, risk profile, execution core, recovery stack, and other organs. The exception therefore did not directly cause the later WATCHING readiness freeze, but it proves the startup cleanup path is not production-shape safe.

Current-main source confirms the likely failure seam:

```python
_row = c.fetchone() or (0, 0)
if isinstance(_row, dict):
    tier_a_cancel = int(_row.get("canceled") or 0)
    tier_a_skip = int(_row.get("retry_eligible") or 0)
else:
    tier_a_cancel = int(_row[0] or 0)
    tier_a_skip = int(_row[1] or 0)
```

The SQL explicitly returns two aggregate aliases (`canceled`, `retry_eligible`), but production produced a non-dict result shape that reached the positional branch and could not satisfy `_row[1]`.

Do **not** assume the database returned a normal 2-tuple and paper over the exception. Reproduce the exact driver/wrapper result shape first.

## WHY EXISTING TESTS MISSED IT

Current tests are mostly source-structural. `tests/test_startup_cleanup_safe_skip.py` checks that the caller unpacks three counters and that the SQL contains the required safety predicates, but it explicitly notes the runner is not isolated enough for an end-to-end DB test. `tests/test_p0_live_startup_cleanup_reeval_canonical_guard.py` is also structural/source-based.

The missing contract is an executable production-shaped cursor/result test for the aggregate-return boundary.

## REQUIRED ROOT-CAUSE TRACE

Before editing production code, Codex must trace:

1. `ClientRunner._clear_old_phantom_orders()`
2. `ap.db.conn()` cursor/row factory used in deployed production
3. `run_with_retry()` transaction/retry semantics
4. the exact CTE/aggregate query returning `canceled` and `retry_eligible`
5. the actual object type/length/mapping behavior returned by `fetchone()`
6. what happens to Tier A DB mutations if result decoding raises before the context manager exits
7. whether Tier B executes after any malformed Tier A result
8. whether a retry can re-run Tier A after an ambiguous commit/ACK boundary

Record the exact root cause in the PR. If the observed production shape cannot be reproduced, add diagnostics that expose type/description/field cardinality without logging sensitive row data, then HOLD rather than guessing.

## REQUIRED IMPLEMENTATION

Preferred fix is a small, explicit result-decoding helper at the cleanup boundary, not a general DB-wrapper rewrite.

The helper must:

- accept the production mapping-row shape;
- accept a normal two-column tuple/sequence only when cardinality is exactly sufficient;
- use cursor `description` / column names where necessary instead of assuming a positional contract that the production wrapper does not guarantee;
- normalize `NULL` counts to zero;
- reject booleans, negative values, non-integral values, malformed strings, missing columns, duplicate/ambiguous column names, and unexpected one-column results;
- never convert malformed evidence into a favorable cleanup count;
- return a typed/structured result or raise a dedicated local cleanup-result error;
- preserve transaction rollback behavior when result proof is malformed;
- never continue into Tier B after ambiguous Tier A execution truth unless the transaction semantics prove Tier A did not mutate or were rolled back;
- never infer that zero counts means the query succeeded when the row shape is missing/unreadable.

If the production cursor can be made to return a stable mapping row locally for this function without changing global DB semantics, that is preferable to adding broad compatibility magic across the repository.

## TRANSACTION / RETRY SAFETY

This path mutates `orders`, so result decoding is not merely logging.

Prove all of the following:

- malformed result before transaction commit -> Tier A mutations rollback;
- retry after rollback does not double-transition rows;
- commit-ACK ambiguity cannot cause a second cleanup decision to overwrite a row that advanced concurrently;
- existing UPDATE predicates/CAS-like state fences remain authoritative;
- `run_with_retry()` is not changed globally merely to accommodate this one call;
- rows advanced to SUBMITTED/FILLED/position-owned between attempts remain protected by the existing SQL predicates.

If exact commit ambiguity cannot be proven safe, HOLD and scope a separate DB-retry contract rather than hiding it with `try/except`.

## EXISTING CLEANUP SEMANTICS TO PRESERVE

Do not weaken the established cleanup policy:

### Tier A

Pre-submission ENTRY rows only, age-gated, no broker identity, no submitted/fill/position truth, protected by startup grace and active canonical-signal peer proof.

The explicit outcomes remain:

- `STARTUP_CLEANUP_CANCELED_STALE_ORPHAN`
- `STARTUP_CLEANUP_SKIPPED_ACTIVE_SIGNAL` / `RETRY_ELIGIBLE`
- recent-row safe skip behavior

### Tier B

Submitted/acknowledged rows with missing broker identity remain a separate, much more sensitive class. Preserve existing LIVE logging/degraded semantics and split-brain protections. Do not widen Tier B eligibility.

## FILE BUDGET

Expected production scope:

- `client_runner.py`
- one dedicated regression test file, preferably `tests/test_p1_startup_phantom_cleanup_result_shape.py`
- `tests/test_startup_cleanup_safe_skip.py` only for adjacent additions if necessary
- `ap/db.py` only if reproduction proves the defect is a documented cursor contract violation that cannot be fixed locally; any DB change requires separate full hot-path audit

No changes to:

- `ap_recovery.py`
- `ap/preopen_readiness.py` (#449 owns WATCHING readiness)
- order state machine
- pending-trigger restart recovery
- execution core
- broker adapters
- scanner/selector/sizing
- exit engine / proof truth
- queue lifecycle

## REQUIRED REGRESSIONS

Minimum executable cases:

1. mapping row `{canceled: 0, retry_eligible: 0}` -> `(0,0)`.
2. mapping row positive counts -> exact counts.
3. normal 2-tuple -> exact counts if the production contract permits tuple rows.
4. named tuple / row object matching production -> exact counts.
5. `None` fetch result -> explicit classification; do not silently claim successful query unless SQL contract proves this can represent zero rows.
6. one-element sequence -> dedicated malformed-result failure, no index error.
7. empty sequence -> malformed-result failure.
8. three-plus element unexpected sequence -> fail/diagnose unless column-name mapping proves the two target fields exactly.
9. missing `canceled` mapping field -> fail closed.
10. missing `retry_eligible` field -> fail closed.
11. duplicate/ambiguous columns -> fail closed.
12. NULL values -> zero only for present known columns.
13. negative count -> reject.
14. fractional count -> reject.
15. boolean count -> reject.
16. unparsable string -> reject.
17. DB execute failure -> current safe error path, no Tier B.
18. fetch failure -> rollback/no Tier B.
19. malformed result after Tier A CTE -> prove rollback before retry.
20. retry after rollback -> one eventual mutation at most.
21. concurrent order advancement before retry -> existing WHERE fences preserve advanced row.
22. Tier A zero/zero valid result -> Tier B executes exactly once under current policy.
23. LIVE Tier B positive count -> existing critical/degraded behavior unchanged.
24. PAPER Tier B behavior unchanged.
25. 2026-06-03 parity source guards remain green.
26. `test_startup_cleanup_safe_skip.py` remains green.
27. `test_p0_live_startup_cleanup_reeval_canonical_guard.py` remains green.
28. DB hot-path contract remains green if any DB helper is touched.
29. exact-head P0 regression workflow remains green.
30. startup integration fixture no longer logs `tuple index out of range`.

## OBSERVABILITY

On malformed result evidence, log one bounded structured diagnostic containing only:

- client id
- execution mode
- stage (`tier_a_result_decode`)
- row Python type
- sequence cardinality if available
- cursor description column names if available
- commit SHA / pod context if already available

Do not log tokens, broker credentials, full SQL parameter payloads, or arbitrary row content.

The generic `Startup phantom clear: <exc>` warning may remain as a last-resort catch, but the known result-shape failure must have an explicit reason code so production can distinguish DB/result-contract debt from SQL execution failure.

## MONEY-PATH / LIFECYCLE AUDIT

- runtime behavior changed: yes, startup cleanup no longer aborts on valid production row shape;
- broker submit/cancel: none;
- orders mutation: existing cleanup UPDATEs only; no new eligibility;
- positions/proof/queue: none;
- client/mode: exact existing runner scope;
- PAPER/LIVE taxonomy: unchanged;
- exits: unchanged;
- #445 PENDING_TRIGGER behavior: unchanged;
- #449 WATCHING readiness behavior: not implemented here.

## NON-OVERLAP

- #449 owns the 133 WATCHING readiness deadlock.
- #448 owns PAPER NULL-mode fill debt and MCD intelligence exception.
- #428 owns exact EXIT/proof reconciliation. Do not touch its ownership taxonomy from this PR.
- #445 owns PENDING_TRIGGER pre-broker recovery.

## FINAL CODEX HANDOFF

Codex must:

1. reproduce the production result shape first;
2. document exact root cause;
3. implement the smallest local decode/contract correction;
4. prove transaction/retry semantics;
5. run focused + adjacent startup cleanup suites;
6. run DB hot-path tests if DB code changes;
7. run exact-head P0 CI;
8. update PR with final SHA, files, tests, CI links, DB mutation audit, and `MERGE / HOLD / HARD HOLD` verdict;
9. not merge or deploy.

## CURRENT VERDICT

**HOLD.** The failure did not directly block Jason's runner in the captured startup, but a LIVE startup mutation path throwing `tuple index out of range` is not acceptable production behavior and the current structural tests do not exercise the result boundary that failed.