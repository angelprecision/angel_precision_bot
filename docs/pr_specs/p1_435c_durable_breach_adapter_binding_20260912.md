# PR #621 — 435-C Durable BREACH Snapshot Adapter Binding Spec

## Status

**DRAFT / HARD HOLD / AMENDED FOR INDEPENDENT FINAL AUDIT. DO NOT MERGE OR DEPLOY.**

`#615` and `#625` are both MERGED. Rebased onto
`main@71f25dabf739bce3360593ac44a7c7bf71ef9513`.

## Existing authority reused (do not rebuild)

Merged `#327` owns the intelligence substrate:
- `ap_intelligence_snapshots`;
- `ap_intelligence_jobs`;
- durable phase-aware identity (`identity_key`);
- PRETRIGGER/PREOPEN parent relationships;
- generic snapshot worker/store;
- idempotent enqueue by `input_hash`;
- phase-local `context_revision` allocation.

Merged `#625` owns the BREACH assembly owner:
- canonical BREACH identity normalization;
- parent validation (candidate vs authoritative);
- deterministic identity_hash and input_hash;
- assembly envelope with observe-only / affected_eligibility=false stamps;
- deterministic `assembly_proof_hash` sealing final status, candidate and
  authoritative parent maps, parent validation/lineage, missing phases, and
  safety flags.

This amendment extends only that existing #625 authority seam. It does not add
the #622 runtime caller or any execution/trading policy.

Merged `#615` owns FVG geometry / lifecycle / opposing / strong-break / VI /
regime / setup / structure schema / model version.

Merged `#614` owns exact trigger-time PIT market evidence, `as_of`, and
5m/15m/1h/4h coverage/provenance.

`#330` remains archaeology only. `#621` builds no second identity, no second
worker, no second table, no second retry, no second revision allocator.

`#621` owns exactly one seam: the durable adapter between the merged `#625`
envelope and the merged `#327` job/snapshot store, plus a small guarded
dispatch inside `build_snapshot_kwargs`.

## #621 production files

Exactly two new/adapter files, per spec §21:

1. `ap/intelligence_breach_snapshot_adapter.py` (new).
2. `ap/intelligence_context_materializer.py` (one import, one dispatch guard;
   otherwise unchanged byte-for-byte).

The amendment also extends the existing #625 assembly owner with its proof
helper/output; it does not add a #621 runtime caller or execution policy.

## Canonical caller trace

```
build_breach_snapshot_envelope(...)          # #625 (already merged)
    -> enqueue_breach_snapshot_job(envelope) # #621 adapter (new)
        -> enqueue_intelligence_job(...)     # #327 (already merged)
    -> intelligence worker claims job        # #327 (already merged)
    -> build_snapshot_kwargs(job)            # #327 materializer, GUARDED DISPATCH added by #621
        -> build_breach_snapshot_kwargs(job) # #621 adapter (new) if is_frozen_breach_job(job)
    -> complete_job_with_snapshot(...)       # #327 (already merged)
```

The guard is:

```python
if is_frozen_breach_job(job):
    return _build_breach_snapshot_kwargs(job)
```

`is_frozen_breach_job(job)` is True iff `job.phase == "BREACH"` AND
`job.payload["payload_kind"] == "FROZEN_BREACH_V1"`. Every other job (PRETRIGGER,
PREOPEN, legacy generic BREACH) is untouched.

## Immutable frozen payload

Enqueue serializes the following into `job.payload` (spec §7):

- `payload_kind = "FROZEN_BREACH_V1"`, `adapter_version`, `assembly_version`;
- complete `#625` canonical identity;
- `#625` identity_hash and input_hash;
- `#625` assembly_proof_hash;
- exact `trigger_crossed_at` and `evidence_as_of`;
- exact `candidate_parent_snapshot_ids` and `authoritative_parent_snapshot_ids`;
- exact `parent_lineage`, `parent_validation`, `missing_parent_phases`;
- exact `#614` canonical evidence projection;
- exact frozen `#615` structure (deep-copied so caller mutation cannot rewrite);
- deterministic `structure_hash` (sha256 of canonicalized structure);
- structure_schema_version, structure_model_version, source_versions;
- observe_only=true, affected_eligibility=false;
- envelope_status (COMPLETE or PARTIAL only);
- assembly proof is reverified from this frozen payload before snapshot write.

No mutable latest-market aliases, no worker-now price, no fresh market context,
no later candle, no current FVG reconstruction.

## Snapshot semantics

- Same `#625` input_hash -> same `#327` BREACH job/revision (idempotent).
- Genuinely different `#625` input_hash at same store identity -> new
  `#327` BREACH context_revision. New generation, changed evidence, or changed
  structure all produce a different input_hash and therefore a new revision.
- Snapshot `data_as_of` = envelope `evidence_as_of` = `#614` PIT `as_of`.
  Never `collected_at`, worker start/completion, or `_now_iso()`.
- Enqueue requires normalized aware equality across envelope trigger, canonical
  `#625` identity trigger, and explicit `evidence_as_of`; missing, malformed,
  naive, later, or conflicting values are rejected before `#327` mutation.
- Enqueue recomputes `#625` `hash_breach_identity` and
  `hash_breach_snapshot_input` over the exact canonical inputs and rejects
  stale hashes fail-soft.
- Enqueue requires and recomputes the exported #625 assembly proof before any
  #327 mutation. A status, parent, lineage, missing-phase, candidate, or flag
  mismatch returns `BREACH_ENVELOPE_ASSEMBLY_PROOF_MISMATCH` with no job or
  context-revision mutation.
- Status comes only from the sealed #625 output: no PARTIAL-to-COMPLETE
  promotion or structure inference exists in #621. Only the exact sealed
  authoritative PREOPEN parent is eligible for the top-level parent.
- #327 store identity comes only from nested canonical #625 identity. Top-level
  client/mode/signal/canonical/local-order/profile aliases must normalize equal
  or are rejected before allocation.
- `hash_frozen_breach_structure` is strict JSON-only: custom objects, sets,
  bytes, non-string keys, and non-finite numbers are rejected.
- Snapshot status: `#625` COMPLETE -> `COMPLETE`; `#625` PARTIAL -> `PARTIAL`;
  `#625` REJECTED -> no authoritative snapshot job (fail-soft telemetry only).
- Top-level `parent_snapshot_id` is authoritative PREOPEN only, else `None`.
  Candidate PREOPEN is never promoted. Full candidate/authoritative maps are
  preserved inside the payload.

## Worker cross-check (fail closed for intelligence)

`build_breach_snapshot_kwargs(job)` cross-checks the durable job row against
its frozen payload identity: client_id, execution_mode, signal_id,
canonical_signal_id, local_order_id, phase, profile_version, input_hash. It
also recomputes the frozen identity/input/assembly/structure hashes before
mapping the sealed status. Any mismatch raises `RuntimeError` so `#327`
retry/terminal machinery marks the intelligence job (not the trade) failed.
Trading is unaffected.

## Persistence failure behavior

Intelligence telemetry only:

- observable via `#327` retry/terminal states;
- may retry through existing snapshot/job machinery (`#327` owns retry);
- MUST NOT alter watcher ownership;
- MUST NOT delay or deny selector or broker execution;
- MUST NOT terminalize the trade or the setup;
- MUST NOT create fallback current-market evidence;
- MUST NOT change LIVE/PAPER execution mode.

No global readiness gate from `#621`.

## No market data or broker calls in worker path

Adapter path performs zero calls to:
- `collect_point_in_time_context`;
- `#615` structure/freezer functions;
- broker quote/history;
- current quote helpers;
- selector;
- watcher;
- execution core;
- order submission.

Verified by tests that monkeypatch these to raise on invocation.

## Test evidence

`tests/test_p1_435c_durable_breach_adapter.py` (81 focused tests):

1. structure hash: deterministic, order-stable, evidence-sensitive;
2. enqueue contract: rejects non-mapping, REJECTED envelope, wrong phase,
   observe_only=false, affected_eligibility=true, missing evidence_as_of,
   COMPLETE without structure;
3. idempotency: same envelope dedupes same-input, new generation and new
   semantic evidence both produce new `context_revision`;
4. isolation: client, LIVE/PAPER, CALL/PUT;
5. dispatch predicate; worker fails closed on identity or input_hash mismatch;
6. candidate vs authoritative parent invariants;
7. generic PRETRIGGER/PREOPEN/legacy BREACH path unchanged (dispatch does not
   fire and generic builder is invoked);
8. frozen-BREACH worker path does not call `collect_point_in_time_context` or
   the generic context builder;
9. immutable payload: caller mutation after enqueue does not affect persisted
   structure; replay preserves exact `evidence_as_of`;
10. canonical `#614` PIT -> real `#615` freezer -> real `#625`
    `build_breach_snapshot_envelope` -> #621 enqueue -> worker claim ->
    `complete_job_with_snapshot` -> `get_latest_snapshot` against the real
    `#327` memory backend, with exact versions, FVG/coverage/provenance,
    parent/hash retention, and no-refetch assertions;
11. exact-time, hash-mutation, and strict-JSON fail-first coverage;
12. assembly-proof status/parent/lineage/candidate/flag mutation and replay
    stability;
13. nested canonical identity authority and all six compatibility-alias
    conflict cases;
14. worker-side frozen assembly-proof revalidation;
15. money-path isolation via source-string audit.

Adjacent intelligence suites (`#614` PIT, `#615` structure, `#625` identity,
`#327` snapshot/job) all continue to pass without change.

## Money-path audit

Adapter module imports:
- `ap.intelligence_breach_snapshot_assembly` (`#625`);
- `ap.intelligence_snapshot_store` (`#327`);
- Python stdlib only otherwise.

Adapter module does NOT import (verified in test):
`ap_execution_core`, `ap_entry_watcher`, `ap_exit_engine`, `ap.broker`,
`ap.order_state_machine`, `ap.position_manager`, `ap.exit_manager`,
`ap.selector`, `ap.trade_queue`, `ap.risk`, `ap.contract_selector`,
`ap.intelligence_market_data`.

## Merge gate (unchanged)

No merge until independent audit clears the unchanged final SHA on:
- focused `#621` tests;
- existing `#327` intelligence snapshot suites;
- `#614` / `#615` / `#625` integration suites;
- restart/idempotency proof;
- exact-head P0 + cohesion;
- genuine merge-ref parity;
- `git diff --check`;
- whole-PR money-path audit.
