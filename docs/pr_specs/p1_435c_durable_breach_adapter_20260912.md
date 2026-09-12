# P1 435-C — Durable BREACH Snapshot Adapter

## Status
DRAFT / HARD HOLD / AMENDED FOR INDEPENDENT FINAL AUDIT. Do not merge or deploy.
Kept open for independent whole-PR audit.

## Merged reality at implementation
- `#614` PIT market data: **MERGED**
- `#615` frozen BREACH market structure: **MERGED**
- `#625` BREACH identity / parent validation / assembly owner: **MERGED**
- `main` at rebase: `71f25dabf739bce3360593ac44a7c7bf71ef9513`

`#330` remains archaeology only. The generic `#327` intelligence job / snapshot
store is the sole durable substrate. This PR does not resurrect `#330` and does
not build a second BREACH identity, worker, or table.

## Canonical dependency chain
```
#614 PIT MERGED
  -> #615 frozen BREACH structure MERGED
    -> #625 BREACH identity / assembly owner MERGED
      -> #621 durable BREACH adapter (this PR)
        -> #622 nonblocking runtime bridge
          -> #623 replay / evidence closure
            -> surgical #436 LIVE timing policy
```

## Purpose
Reuse the durable `#327` intelligence substrate.  This PR is
**persistence infrastructure only**.

Convert:

`#614 exact PIT evidence` + `#615 already-frozen structure`
+ `#625 exact BREACH identity / parent validation / envelope`

into:

`#327 durable BREACH job + snapshot`

without refetching, recomputing, interpreting, delaying, rejecting, or changing
a trade.

## Production surface (exactly two files)
- `ap/intelligence_breach_snapshot_adapter.py` — new.
- `ap/intelligence_context_materializer.py` — 1 dispatch guard, 1 import block,
  otherwise unchanged byte-for-byte.

No new table, migration, worker, queue, retry subsystem, revision allocator, or
BREACH identity model. `ap_execution_core`, entry watcher, master control,
selector, contract selector, broker, order state machine, order monitor, exit
engine, position manager, proof trades, risk / sizing, trade queues, LIVE
admission, and FVG timing policy remain unchanged. This PR MUST NOT change a
single LIVE trade.

## Frozen-BREACH job marker
The adapter stamps every enqueued job with the explicit payload marker:

`payload_kind = "FROZEN_BREACH_V1"`

The dispatch seam in `build_snapshot_kwargs()` delegates to the adapter ONLY
when both:

- `phase == "BREACH"`, and
- `job["payload"]["payload_kind"] == FROZEN_BREACH_V1`

Every other job (PRETRIGGER, PREOPEN, legacy generic BREACH) preserves the
existing generic path unchanged, byte-for-byte.

## Required identity (persisted / cross-checked)
- client_id
- execution_mode (LIVE/PAPER isolated)
- signal_id
- canonical_signal_id
- local_order_id
- materialization_generation from `#625` identity
- trigger_crossed_at (== canonical BREACH as-of)
- profile_version, model_version, structure_schema_version, structure_model_version
- `#625` identity_hash and input_hash (both persisted)
- deterministic structure_hash (payload integrity, NOT a second identity)

## Invariants
- Existing `#327` snapshot/job tables and writers remain canonical.
- No second snapshot table, worker, queue, or BREACH identity model.
- Restart/replay of the same durable opportunity and same frozen evidence
  resolves the same snapshot identity/content hash (same input_hash ->
  duplicate same-input `#327` job).
- A newer lifecycle/materialization generation cannot silently reuse an older
  frozen structure snapshot: it produces a different `#625` input_hash and a
  new `#327` BREACH context_revision.
- Missing/malformed identity or structure remains telemetry failure only and
  cannot change eligibility, watcher ownership, selector behavior, broker
  reachability, position, proof, or LIVE/PAPER execution mode.
- REJECTED `#625` envelope never enqueues an authoritative snapshot: adapter
  returns `ok=False, error_code=BREACH_ENVELOPE_REJECTED` (fail-soft, no
  exception).
- Snapshot `data_as_of` == exact `#625` `evidence_as_of` == exact `#614` PIT
  `as_of`. Never `collected_at`, never worker time, never `_now_iso()`.
- Enqueue normalizes aware trigger timestamps to UTC and rejects any missing,
  malformed, naive, or conflicting BREACH boundary.
- Enqueue recomputes the merged `#625` `identity_hash` and `input_hash` from
  the exact canonical identity, evidence, candidate parent map, and frozen
  structure before touching `#327`; mismatch is fail-soft with no job mutation.
- `structure_hash` accepts only strict JSON-shaped values and rejects custom
  objects, unordered collections, non-string keys, and non-finite numbers.
- Top-level `parent_snapshot_id` is authoritative PREOPEN only (or `None`);
  candidate parents never promoted. Full candidate/authoritative maps preserved
  inside the payload.
- PAPER and LIVE remain exact-mode isolated.

## Fail-soft for trading
Enqueue failure, snapshot write failure, worker retry, worker crash, DB outage,
retry exhaustion: **intelligence subsystem only**. No terminalize trade,
watcher, or setup. No global readiness gate from `#621`.

## Test evidence
Focused suite `tests/test_p1_435c_durable_breach_adapter.py`:

- strict deterministic structure hash (key-order stable, list-order sensitive,
  semantic FVG/strong-break/VI/reclaim changes change hash, unsupported values
  reject);
- exact trigger/evidence-as-of equality, equivalent-offset normalization, and
  malformed/naive/later-time rejection;
- #625 hash-authority verification with post-assembly evidence, structure,
  candidate-parent, identity, and as-of mutation rejection;
- enqueue contract (rejects non-mapping, REJECTED envelope, wrong phase,
  observe_only=false, affected_eligibility=true, missing evidence_as_of,
  COMPLETE without structure);
- idempotency and generation fencing (same envelope dedupes same-input,
  new generation and new evidence both produce new context_revision);
- isolation (client, LIVE/PAPER, CALL/PUT);
- dispatch predicate and worker cross-check (fails closed on identity /
  input-hash mismatch);
- candidate vs authoritative parent invariant (top-level = authoritative
  PREOPEN only; None when no authoritative PREOPEN, candidate PREOPEN
  present in payload but NOT promoted);
- generic PRETRIGGER / PREOPEN / legacy BREACH path unchanged;
- frozen-BREACH worker path never invokes `collect_point_in_time_context`,
  `build_intelligence_context_payload`, broker, selector, or watcher;
- immutable frozen payload (caller mutation after enqueue cannot alter
  persisted structure; replay preserves exact evidence_as_of);
- end-to-end canonical `#614` PIT -> real `#615` freezer -> real `#625`
  identity/assembly -> enqueue -> claim -> complete_job_with_snapshot ->
  `get_latest_snapshot` round-trip through the real `#327` memory backend,
  including version, FVG, coverage, provenance, parent, hash, and no-refetch
  assertions;
- money-path isolation (adapter module imports nothing under `ap_execution_core`,
  entry watcher, exit engine, broker, order state machine, position manager,
  exit manager, selector, trade queue, risk, contract selector, or intelligence
  market-data collector).
