# PR #435 Runtime/Restart Parity + Money-Path Evidence Summary

Local evidence only under `/workspace/pr435/` (no clone/push/merge).
Repo tip ref: `refs/heads/spec/p1-breach-setup-intelligence-20260811`.
Pattern sources fetched via GitHub API `get_file_contents`:
- `tests/test_p0_watcher_recovery_execution_ownership.py`
- `tests/test_p0_intelligence_context_snapshots.py`
(also mirrored under `/workspace/pr435/harness/` for reference).

## Artifacts added

| File | Purpose |
|------|---------|
| `tests/test_p0_435_runtime_restart_materializer_parity.py` | Concrete resolved-value parity matrix |
| `tests/test_p0_435_exactly_one_submit_and_zero_broker.py` | Exactly-one submit / zero-broker mutation proofs |
| `PARITY_MONEYPATH_SUMMARY.md` | This summary |

## Wired `ap/` modules (PYTHONPATH=/workspace/pr435)

| Module | Role |
|--------|------|
| `ap/intelligence_context_materializer.py` | enqueue / recover / build payload (tip copy) |
| `ap/intelligence_context_handoff.py` | best-effort non-gating handoff |
| `ap/intelligence_snapshot_store.py` | **memory backend** (was stub; now supports matrix) |
| `ap/intelligence_market_data.py` | geometry / PIT helpers |
| `ap/intelligence_breach_market_structure.py` | freeze helpers |
| `ap/fair_value_gap.py` / `fvg_telemetry.py` | FVG observe-only |
| stubs: `market_context_builder`, `the_strat_confluence`, `sector_context`, `volume_confirmation`, `vwap_context`, `db` | allow materializer imports without full monorepo |

## Parity matrix — concrete resolved values

Paths covered: **runtime** `enqueue_breach_context`, **restart** `recover_missing_intelligence_jobs`, **materializer** `build_intelligence_context_payload` (accept).

| Scenario | Runtime resolved | Restart shape | Materializer |
|----------|------------------|---------------|--------------|
| LIVE first breach | `ok=True inserted=True` `execution_mode=LIVE` `breach_lineage=INITIAL_BREACH` `client_id=client@example.com` `canonical_signal_id=canon-parity-1` `local_order_id=loid-parity-1` `observe_only=True` `affected_eligibility=False` | `breach=1 breach_errors=0` same lineage/mode | identity fields match job payload; `underlying_observation.price=501.25` |
| PAPER first breach | same with `execution_mode=PAPER` | same | same |
| PAPER / LIVE rebreach | `breach_lineage=REBREACH_AFTER_RESET` | `breach=1` with reset meta | N/A (lineage on freeze) |
| generation mismatch | `error_code=BREACH_LIFECYCLE_GENERATION_CONFLICT` `inserted=False` no job | `breach=0 breach_errors=1` | N/A (reject before enrich) |
| retry contradiction | `error_code=BREACH_LIFECYCLE_ATTEMPT_CONFLICT` | `breach_errors=1` | N/A |
| malformed timestamp | `error_code=BREACH_EVIDENCE_INVALID_TRIGGER_CROSSED_AT` | `breach_errors=1` | N/A |
| client conflict | `error_code=BREACH_IDENTITY_CONFLICT_CLIENT_ID` | `breach_errors=1` | N/A |
| execution-mode conflict | `error_code=BREACH_IDENTITY_CONFLICT_EXECUTION_MODE` | `breach_errors=1` | N/A |
| ownership loss | worker `error_code=JOB_CLAIM_OWNERSHIP_LOST` `completed=False`; job stays `RUNNING` under original owner | diagnostic-only | execution continues |
| missing intelligence store | `error_code=INTELLIGENCE_STORE_UNAVAILABLE` `inserted=False` no job | DB down → `ok=False` counts zero | execution continues / no terminalize |

All reject / diagnostic rows assert **no execution mutation flags** (`affected_eligibility` false/absent; no submit/cancel/broker_* authority).

## Money-path proofs (exactly one submit / zero broker)

Harness mirrors production sequencing from `APExecutionCore._on_entry_trigger`:
1. `enqueue_breach_context_best_effort` (observe-only, non-gating)
2. optional enrichment
3. `order_state_machine.submit_existing_entry` (existing submit path)

| Case | Submit count | Broker cancel | Notes |
|------|--------------|---------------|-------|
| valid handoff + submit | **1** | **0** | handoff accepted once |
| handoff disabled | **1** | **0** | `{ok:True, accepted:False, disabled:True}` |
| capacity exhausted | **1** | **0** | `error=handoff_capacity_exhausted` |
| intelligence persistence failure | **1** | **0** | `INTELLIGENCE_STORE_UNAVAILABLE` does not gate |
| enrichment exception | **0** | **0** | zero order/position/proof/queue mutation |

## What still needs live Postgres / broker

Honest gaps (stubs only in this pack):

1. **Postgres-backed** `recover_missing_intelligence_jobs` SQL against real `trade_queue` / `orders` / `ap_intelligence_jobs` — cursor is mocked; migration fencing covered elsewhere in tip tests.
2. **Live broker** path through full `APExecutionCore._on_entry_trigger` (selector, live submit gates, OSM) — this pack uses a contract harness + MagicMock OSM/broker, not Tradier.
3. **Worker process** `process_due_intelligence_jobs_once` end-to-end with real claim leases — memory claim/complete ownership loss is covered; DB lease expiry is not.
4. Full tip `ap/intelligence_snapshot_store.py` Postgres insert/advisory-lock paths — memory backend only here (`INTELLIGENCE_CONTEXT_STORE_BACKEND=memory`).
5. Enrichment modules (`market_context_builder`, strat/FVG/sector/volume/vwap) are **stubs** sufficient for identity/observe-only flags; scoring fidelity needs the tip modules.

## How to run

```bash
cd /workspace/pr435
PYTHONPATH=/workspace/pr435 INTELLIGENCE_CONTEXT_STORE_BACKEND=memory \
  .venv/bin/python -m py_compile \
    tests/test_p0_435_runtime_restart_materializer_parity.py \
    tests/test_p0_435_exactly_one_submit_and_zero_broker.py \
    ap/intelligence_snapshot_store.py

PYTHONPATH=/workspace/pr435 INTELLIGENCE_CONTEXT_STORE_BACKEND=memory \
  .venv/bin/pytest -q \
    tests/test_p0_435_runtime_restart_materializer_parity.py \
    tests/test_p0_435_exactly_one_submit_and_zero_broker.py
```
