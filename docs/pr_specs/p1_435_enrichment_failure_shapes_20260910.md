# PR #435 — Enrichment / handoff failure-shape matrix

Local evidence under `/workspace/pr435/` only. **HARD HOLD** — no push / merge / deploy.

## Production seam

`APExecutionCore._on_entry_trigger` (patch: `harness/ap_execution_core_breach_handoff.patch`):

1. `enqueue_breach_context_best_effort(...)` inside `try/except` — observe-only, **non-gating**
2. (optional) enrichment / materializer work — must not own money-path authority
3. `order_state_machine.submit_existing_entry` — existing submit path continues after handoff failure

Handoff module: `ap/intelligence_context_handoff.py`  
Rejection type: `ap.intelligence_context_materializer.BreachMaterializationRejected`

## Artifact

| File | Role |
|------|------|
| `tests/test_p0_435_enrichment_failure_shapes.py` | Table-driven / parametrized failure-shape matrix |
| Reuses | `MutationLedger`, `_run_breach_then_submit` from `tests/test_p0_435_exactly_one_submit_and_zero_broker.py` |

## Matrix

### A) Enrichment exception shapes → **pre-submit diagnostic abort** (`submit == 0`)

Documented: zero submit is correct when enrichment raises *between* handoff and `submit_existing_entry` (harness models a pre-submit abort). Async materializer enrichment is off the money path; either way intelligence failure must not cancel / close / resize.

| Shape | Submit | Broker cancel / position / size | Eligibility authority |
|-------|--------|----------------------------------|------------------------|
| `TypeError` | **0** | **0** | unchanged / non-authority |
| `ValueError` | **0** | **0** | unchanged / non-authority |
| `RuntimeError` | **0** | **0** | unchanged / non-authority |
| `KeyError` | **0** | **0** | unchanged / non-authority |
| `TimeoutError` | **0** | **0** | unchanged / non-authority |
| `concurrent.futures.TimeoutError` (alias) | **0** | **0** | unchanged / non-authority |
| `BreachMaterializationRejected` | **0** | **0** | unchanged / non-authority |

Assertions: `disposition == ENRICHMENT_DIAGNOSTIC_ONLY`, `mutations` all-zero for cancel/broker_cancel/positions/order_meta/proof/queue, handoff result never claims `submit`/`cancel`/`affected_eligibility`.

### B) Handoff shapes → **non-gating** (`submit_existing_entry.call_count == 1`)

| Shape | Submit | Broker cancel | Notes |
|-------|--------|---------------|-------|
| Worker disabled | **1** | **0** | `{ok:True, accepted:False, disabled:True}` |
| Capacity exhausted | **1** | **0** | `error=handoff_capacity_exhausted` |
| Executor `submit` raises (`RuntimeError` / `TypeError` / `ValueError` / `KeyError` / `TimeoutError` / `BreachMaterializationRejected`) | **1** | **0** | `submit_intelligence_enqueue` swallows → `{ok:False, accepted:False}` |
| Handoff fn raises into runner `try/except` | **1** | **0** | mirrors production warning-and-continue |
| Control: no enrichment | **1** | **0** | `disposition=SUBMITTED` |

## Verification

```bash
cd /workspace/pr435
PYTHONPATH=/workspace/pr435 INTELLIGENCE_CONTEXT_STORE_BACKEND=memory \
  .venv/bin/python -m py_compile \
    tests/test_p0_435_enrichment_failure_shapes.py \
    tests/test_p0_435_exactly_one_submit_and_zero_broker.py \
    tests/test_p0_435_money_path_non_authority.py

PYTHONPATH=/workspace/pr435 INTELLIGENCE_CONTEXT_STORE_BACKEND=memory \
  .venv/bin/pytest -v --tb=short \
    tests/test_p0_435_enrichment_failure_shapes.py \
    tests/test_p0_435_exactly_one_submit_and_zero_broker.py \
    tests/test_p0_435_money_path_non_authority.py
# → 43 passed (30 new matrix rows/cases + 13 prior)
```

## Production code touches

**None.** Observe-only tests pin current safe behavior.

## Counts

| Suite | Passed |
|-------|--------|
| `test_p0_435_enrichment_failure_shapes.py` | **30** |
| `test_p0_435_exactly_one_submit_and_zero_broker.py` | 7 |
| `test_p0_435_money_path_non_authority.py` | 6 |
| **Total this run** | **43** |
