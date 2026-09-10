# PR #435 Three-Path Frozen-Envelope Parity

**Scope:** local evidence under `/workspace/pr435/` only (HARD HOLD — no push/merge/deploy).  
**Module:** `tests/test_p0_435_runtime_restart_materializer_parity.py`  
**Store:** `INTELLIGENCE_CONTEXT_STORE_BACKEND=memory` (no production DB).

## What was built

One table-driven / fixture-driven suite that feeds **ONE frozen BREACH envelope** through three paths and asserts identical critical hashes/fields:

| Path | Symbol | How exercised |
|------|--------|---------------|
| 1. Runtime enqueue | `enqueue_breach_context` | Direct freeze + memory job insert |
| 1b. Runtime handoff | `enqueue_breach_context_best_effort` | Worker enabled; `submit_intelligence_enqueue` monkeypatched sync |
| 2. Restart rebuild | `recover_missing_intelligence_jobs` | Cursor monkeypatch returns durable order meta + queue payload |
| 3. Materializer | `build_snapshot_kwargs` | Runs on jobs from paths 1 and 2 (and a fresh path-1 job) |

Envelope fixture: `_frozen_breach_envelope` (PAPER/LIVE × first-breach/rebreach) with mirrored `metadata` == durable order-meta keys so recovery merge cannot invent divergent keys.

## Fields proved IDENTICAL across all three paths

| Field | Status | Notes |
|-------|--------|-------|
| `input_hash` | **IDENTICAL** | Runtime job == restart job == materializer `snapshot_kwargs.input_hash` (byte-identical SHA-256) |
| `breach_lineage` | **IDENTICAL** | `INITIAL_BREACH` / `REBREACH_AFTER_RESET` on frozen signal + breach_evidence |
| `canonical_strategy_pattern` | **IDENTICAL** | Resolved `2-3-2` from daily `pattern=2-3` |
| `breach_lifecycle` (public) | **IDENTICAL** | `generation`, `attempt`, `recovered`, `preclaimed`, `execution_mode`, `client_id`, `owner`, `source` |
| `entry_readiness_observe_only.classification` | **IDENTICAL** | Via materializer on runtime-job vs restart-job vs fresh materializer path |
| `entry_timing_candidate` | **IDENTICAL** | Dual-emitted observe-only candidate enum |
| `market_structure` zone_ids | **IDENTICAL** | Ordered zone_id list from freeze |
| `market_structure` structure hash | **IDENTICAL** | `_stable_hash({zone_ids, relationship_zone_id, class, schema_version, data_as_of})` |
| Identity columns | **IDENTICAL** | `client_id`, `execution_mode`, `canonical_signal_id`, `local_order_id`, `signal_id`, `phase=BREACH` |
| Observe-only fence | **IDENTICAL** | `observe_only=True`, `affected_eligibility=False` on job payload + materializer payload |

Parametrized rows: `paper-first-breach`, `live-first-breach`, `paper-rebreach` × (direct three-path + best-effort vs direct).

## Remaining PARTIAL / UNKNOWN gaps (honest)

| Gap | Status | Why |
|-----|--------|-----|
| Real Postgres `recover_missing_intelligence_jobs` SQL against `orders`/`trade_queue`/`ap_intelligence_jobs` | **PARTIAL** | Cursor is mocked; migration/claim fencing covered elsewhere (`test_p0_intelligence_postgres.py`) but not this three-path envelope |
| Async best-effort handoff race / thread-pool latency | **PARTIAL** | Sync monkeypatch proves freeze equivalence; does **not** prove async executor ordering or capacity-exhaust shapes inside this suite |
| `market_structure` / readiness on the **enqueue job payload itself** | **N/A / PARTIAL** | Freeze at enqueue stamps lineage/pattern/lifecycle only; structure + readiness are materializer-time. Parity is proven after `build_snapshot_kwargs`, not on raw job.payload |
| Tip enrichment scoring fidelity (`market_context_builder`, strat/FVG/sector/volume/vwap) | **PARTIAL** | Local stubs suffice for identity + zone freeze; full tip scoring not claimed identical to production |
| Worker claim lease / `complete_job_with_snapshot` end-to-end with same envelope | **PARTIAL** | Ownership-loss diagnostic covered separately; not chained into three-path hash assert |
| Production September LIVE durable IDs / PIT candles | **UNKNOWN** | Envelope is synthetic SPY geometry; cohort fixtures remain scaffold (`UNKNOWN` IDs) |
| `computed_at` / `git_commit` / wall-clock fields | **NOT ASSERTED** | Intentionally excluded — process-local / time-varying |
| Env-unset vs enabled job-count matrix rows | **PARTIAL** | Covered in money-path / handoff non-authority packs; not duplicated as three-path hash rows |

## How to run

```bash
cd /workspace/pr435
PYTHONPATH=/workspace/pr435 INTELLIGENCE_CONTEXT_STORE_BACKEND=memory \
  .venv/bin/python -m py_compile tests/test_p0_435_runtime_restart_materializer_parity.py
PYTHONPATH=/workspace/pr435 INTELLIGENCE_CONTEXT_STORE_BACKEND=memory \
  .venv/bin/python -m pytest tests/test_p0_435_runtime_restart_materializer_parity.py -v --tb=short
```

## Test counts (local)

- File total: **25 passed** (19 prior matrix/diagnostic + **6** new three-path rows)
- New: `test_three_path_frozen_envelope_parity` ×3 + `test_three_path_best_effort_handoff_matches_direct_enqueue` ×3

## Observe-only / money-path authority

This suite does **not** change money-path authority. All asserted paths stamp `observe_only=True` / `affected_eligibility=False`. No broker submit/cancel/eligibility mutation flags are set by enqueue, recovery, or materializer outputs under test.
