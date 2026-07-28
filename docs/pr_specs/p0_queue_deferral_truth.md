# P0 Queue Deferral Truth

## Status

**IMPLEMENTED.** Production logic, tests, and docstrings are complete on this branch.
Do not merge without explicit `merge #398` from Angel.

## Problem

`ap/queue.py` had more than one after-hours / deferral writer. One path correctly
wrote `ap_signals` first with `decision_status="WATCHING"` and only then marked
the `trade_queue` row `WATCHING`. Another master-control deferrable-block path
directly wrote `decision_status="watching"` lowercase after already marking the
queue row `REJECTED`.

That created split truth:

- `trade_queue.status = REJECTED`
- `ap_signals.decision_status = watching` (lowercase — invisible to overnight reeval)
- Operators saw a rejected queue row while overnight logic could never find the signal
- BAC PUT incident: signal exited at 0.95, hit 1.17 — phantom WATCHING rows were
  part of the fragile truth contract that enabled premature exits

## Scope

Allowed files:

- `ap/queue.py`
- `tests/test_p0_queue_deferral_truth.py`
- `tests/test_p0_manual_rescue_restart_guard_bypass.py` (pre-existing test isolation fix)

Do not touch scanner selection, score thresholds, contract-selection quality rules,
broker submit/cancel, exits, proof trades, or reconciler.

## Implemented behavior

### 1. Canonical decision-status constants

```python
DECISION_REJECTED = "rejected"
DECISION_WATCHING = "WATCHING"
DECISION_QUEUED   = "queued"
```

No lowercase `"watching"` may be written by queue code. Enforced by a tokenizer-level
grep guard in `test_9b_no_direct_lowercase_write_in_queue_source`.

### 2. Single deferral authority: `_persist_watching_deferral()`

All intentional deferrals route through one function. It:

1. Validates canonical client identity — refuses ownerless rows.
2. Validates execution mode (exactly `"paper"` or `"live"`).
3. Validates canonical signal identity — refuses missing/invented signal IDs.
4. Writes/confirms the `ap_signals` row **first** with `decision_status="WATCHING"`.
5. Only then calls `_checked_watching_cas()` to transition the queue row.
6. On signal-write failure: marks queue `ERROR` — never phantom `WATCHING`.

### 3. Explicit CAS outcome taxonomy

`_checked_watching_cas()` returns one of five string constants rather than a bare
bool, so `_persist_watching_deferral()` can respond correctly to each distinct
zero-row outcome without blind compensation:

| Constant | When returned |
|---|---|
| `WATCHING_CAS_TRANSITIONED` | UPDATE affected exactly 1 row — queue is now `WATCHING` |
| `WATCHING_CAS_ALREADY_WATCHING` | UPDATE hit 0 rows; `SELECT` confirms row is already `WATCHING` |
| `WATCHING_CAS_TERMINAL` | UPDATE hit 0 rows; `SELECT` confirms row is in a terminal status |
| `WATCHING_CAS_MISSING_OR_UNEXPECTED` | UPDATE hit 0 rows; row is absent or in an unexpected nonterminal state |
| `WATCHING_CAS_DB_ERROR` | The UPDATE or classification `SELECT` raised an exception |

### 4. Outcome-specific handling in `_persist_watching_deferral()`

#### Success outcomes (return `True`)
- `WATCHING_CAS_TRANSITIONED` — queue row is now canonical `WATCHING`.
- `WATCHING_CAS_ALREADY_WATCHING` — repeated deferral against an already-canonical
  `WATCHING` row is **idempotent success**. Signal is not reverted, queue is not
  marked `ERROR`.

#### Terminal outcome (return `False`, no `_mark_job`)
- `WATCHING_CAS_TERMINAL` — the queue row is in a terminal state and is the
  authoritative source. `_mark_job` is **never called**. Best-effort signal revert
  attempted; the Boolean return of `_log_signal_to_db()` is checked explicitly.
  If revert fails, `DEFERRAL_SIGNAL_REVERT_FAILED` is logged at CRITICAL.

#### Genuine failure outcomes (return `False`)
- `WATCHING_CAS_MISSING_OR_UNEXPECTED` or `WATCHING_CAS_DB_ERROR` — best-effort
  signal compensation attempted. The Boolean return of `_log_signal_to_db()` is
  checked explicitly (not via `try/except` alone — the helper absorbs exceptions
  and returns `False`). `_mark_job` is **not blindly called**; the queue row's
  current state was not confirmed to be `PROCESSING`.

### 5. Signal-first persistence ordering

`ap_signals` write is confirmed before the queue CAS. Two separate databases
(Supabase Postgres for `ap_signals`, primary Postgres for `trade_queue`) means
perfect atomicity is impossible — what this guarantees is a checked transition
plus loud, best-effort compensation on failure.

### 6. Client isolation

`ap_signals` upsert key is `(signal_id, client_email)`. Queue CAS is scoped
to `job_id` only. One client's deferral can never claim or update another client's row.

### 7. Compensation Boolean failures logged critically

If the compensation write to `_log_signal_to_db()` returns `False` (not an
exception — the helper absorbs those), `DEFERRAL_SIGNAL_REVERT_FAILED` is
emitted at CRITICAL level so operators can detect split truth without relying
on exception monitoring alone.

## Acceptance tests (34 focused)

| # | Test | What it proves |
|---|---|---|
| 1 | `test_1_canonical_ordinary_deferral` | Uppercase WATCHING, canonical client, mode stamped |
| 2 | `test_2_master_control_deferrable_routes_through_helper` | No REJECTED before WATCHING |
| 2b | `test_2b_terminal_rejection_in_session_stays_rejected` | In-session block stays REJECTED |
| 2c | `test_2c_malformed_score_in_dispatch_does_not_raise` | score="A+" sanitized |
| 3 | `test_3_paper_overnight_only_persists_signal_before_watching` | Signal-first ordering |
| 4 | `test_4_signal_persistence_failure_marks_error` | No phantom WATCHING on write failure |
| 4b | `test_4b_no_supabase_client_marks_error` | No client → ERROR |
| 4c | `test_4c_missing_signal_identity_fails_closed` | No random UUID minted |
| 4d | `test_4d_malformed_score_does_not_raise` | Coerced to 0.0 |
| 4e | `test_4e_queue_cas_raises_returns_false` | DB_ERROR → False |
| 4f | `test_4f_queue_cas_zero_rows_returns_false` | MISSING_OR_UNEXPECTED → False; no blind ERROR mark |
| 5 | `test_5_idempotent` | Two calls → one row |
| 6 | `test_6_client_isolation` | Same signal two clients → two distinct rows |
| 7 | `test_7_execution_mode_validation_and_stamping` | Mode required; stamped for provenance |
| 7b | `test_7b_invalid_client_refused` | Blank client → ERROR |
| 8 | `test_8_overnight_loader_discovers_uppercase_watching` | Real predicate; lowercase invisible |
| 9 | `test_9_no_split_truth_on_deferral` | Never REJECTED + WATCHING |
| 9b | `test_9b_no_direct_lowercase_write_in_queue_source` | Tokenizer grep guard |
| 10 | `test_10_no_money_path_effects` | Zero selector/OSM/watcher/broker calls |
| A | `test_cas_A_processing_transitioned` | UPDATE 1 row → TRANSITIONED |
| B | `test_cas_B_already_watching_classification` | SELECT=WATCHING → ALREADY_WATCHING |
| C | `test_cas_C_terminal_classification` | REJECTED/ERROR/DONE → TERMINAL (parametrized) |
| D | `test_cas_D_missing_row` | SELECT=None → MISSING_OR_UNEXPECTED |
| E | `test_cas_E_unexpected_nonterminal` | NEW/PENDING_TRIGGER → MISSING_OR_UNEXPECTED |
| F | `test_cas_F_db_exception` | Exception → DB_ERROR; never re-raises |
| B-i | `test_persist_B_already_watching_idempotent_success` | ALREADY_WATCHING → True; no revert |
| C-i | `test_persist_C_terminal_queue_never_overwritten` | TERMINAL → False; no _mark_job |
| D-i | `test_persist_D_missing_row_no_blind_error_mark` | MISSING → False; no blind ERROR |
| E-i | `test_persist_E_unexpected_nonterminal_queue_unchanged` | Unexpected → False; queue untouched |
| F-i | `test_persist_F_db_error_no_false_success` | DB_ERROR → False; compensation attempted |
| G | `test_persist_G_compensation_bool_failure_emits_critical` | revert returns False → CRITICAL log |

Adjacent suites: **165 passed** (all `ap.queue`-importing test files).

## Merge gate

Merge only when:
- exact-head focused tests pass (34/34); and
- adjacent tests pass (165/165); and
- Angel issues explicit `merge #398`.
