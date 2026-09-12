# P1 — restore current-main BREACH identity / assembly owner

## Status

**SPEC ONLY / DRAFT / HARD HOLD / DO NOT MERGE OR DEPLOY.**

This PR exists because the current intelligence stack incorrectly assumed merged PR #330 still owned BREACH assembly/runtime identity. PR #330 was reverted by merged PR #350. Current main retains #327's generic intelligence snapshot/job store and phase enum, but not #330's BREACH-specific assembly, parent validation, or handoff.

Current main at spec creation:

`f92d2337633c6a9980bcdf729085cb44f7ae6685`

Canonical upstream:

`#614 PIT market data -> #615 frozen market structure`

Canonical downstream after this owner exists:

`this PR -> #621 durable #615 adapter -> #622 runtime BREACH handoff -> #623 replay/evidence -> #436 timing policy`

## Exact responsibility

Restore only the missing current-main **BREACH identity + parent-validation + payload-assembly authority** required by #621.

Do not restore old #330 wholesale.

This PR must define a pure/deterministic owner that can answer:

> Given exact durable PRETRIGGER/PREOPEN parent snapshots plus exact immutable BREACH identity/evidence, what is the canonical observe-only BREACH snapshot identity and assembly envelope?

It does **not** wire that owner into `_on_entry_trigger()`.

It does **not** collect market data.

It does **not** run #615 policy.

It does **not** submit to broker or affect entry eligibility.

## Existing authority that must be reused

Current main already has:

- `ap_intelligence_snapshots`;
- `ap_intelligence_jobs`;
- `phase='BREACH'` support;
- generic `identity_key(...)`;
- `write_snapshot(...)`;
- `complete_job_with_snapshot(...)`;
- `get_latest_snapshot(...)`;
- composite readers;
- PRETRIGGER/PREOPEN materialization/worker lifecycle.

The existing generic snapshot uniqueness is already scoped by:

`client_id + execution_mode + canonical_signal_id + local_order_id + phase + context_revision + profile_version`.

Do not create a second table/store/worker unless a fail-first test proves the existing durable schema cannot represent the required identity.

## Missing authority to restore

### 1. Canonical BREACH identity object

Define one immutable normalized identity object containing, at minimum:

- client_id;
- normalized execution_mode;
- signal_id;
- canonical_signal_id;
- local_order_id;
- ticker;
- side;
- exact trigger_crossed_at;
- canonical lifecycle/materialization generation when present;
- profile/model version;
- BREACH phase.

Ticker/side/generation must be validated as payload/assembly identity even if they are not additional DB uniqueness columns.

Do not join by ticker/time proximity.

Do not synthesize generation from current time.

### 2. BREACH parent validation

Given candidate PRETRIGGER/PREOPEN parents, validate exact compatible identity before use.

At minimum reject parent mismatch on:

- client_id;
- execution_mode;
- canonical_signal_id;
- ticker;
- side;
- profile version;
- incompatible local_order/generation when current phase contract requires them.

A missing parent may produce explicit PARTIAL/MISSING telemetry if existing behavior permits it.

A mismatched parent must never be silently consumed.

### 3. Deterministic BREACH assembly envelope

Build one observe-only envelope that preserves:

- canonical BREACH identity;
- exact parent snapshot IDs;
- exact `trigger_crossed_at` / evidence `as_of`;
- explicit identity warnings/errors;
- source/profile versions;
- structure slot reserved for #621 to attach #615 output;
- deterministic input hash;
- deterministic assembly version;
- `observe_only=true`;
- `affected_eligibility=false`.

The assembly owner must accept already-frozen evidence only.

No mutable watcher/plan/broker object should be required by this owner.

### 4. Deterministic hashing

Use canonical JSON serialization + cryptographic hash (SHA-256 or equivalent).

Hash inputs must not include:

- worker wall clock;
- random UUID;
- Python object identity;
- unordered repr;
- later/current market quote.

Identical durable identity/evidence after restart must produce identical hash.

## Explicit non-scope

Do not implement:

- `submit_breach_intelligence_handoff`;
- `_on_entry_trigger()` wiring;
- `ap_execution_core.py` edits;
- synchronous/asynchronous runtime dispatch;
- Tradier/network/history reads;
- #615 structure calculation;
- #621 persistence adapter;
- #622 runtime bridge;
- #623 replay system;
- #436 WAIT/REARM/READY policy;
- selector/broker/OSM/order/position/proof/queue mutation;
- intraday #617/#618/#619.

If runtime wiring becomes necessary to make a test pass, the implementation has crossed into #622 and must STOP.

## Historical #330 usage rule

PR #330 is archaeology only.

It may be read to recover useful concepts such as:

- parent identity validation;
- immutable BREACH input;
- deterministic parent linkage;
- fail-soft observe-only posture.

Do not cherry-pick or resurrect its large `ap/intelligence_evaluation.py` implementation or `ap_execution_core.py` handoff.

Current main and the #614/#615 contracts are authoritative.

## Preferred implementation shape

Prefer one new narrow module, for example:

`ap/intelligence_breach_snapshot_assembly.py`

with pure helpers such as:

- `normalize_breach_identity(...)`;
- `validate_breach_parent_snapshot(...)`;
- `build_breach_snapshot_envelope(...)`;
- `hash_breach_snapshot_input(...)`.

If an existing current-main intelligence module is clearly the canonical owner, use it instead. Keep production scope to 1–2 files.

Do not modify `ap/intelligence_snapshot_store.py` unless a concrete fail-first proves a generic-store defect.

Do not add a migration unless the current JSONB payload plus existing uniqueness cannot represent the contract. If a migration appears necessary, STOP and document why before writing it.

## Required fail-first / tests

At minimum prove:

1. exact LIVE identity produces canonical BREACH identity;
2. exact PAPER identity produces canonical BREACH identity;
3. same ticker across LIVE/PAPER remains isolated;
4. same ticker across clients remains isolated;
5. CALL vs PUT mismatch rejects parent;
6. canonical_signal mismatch rejects parent;
7. ticker mismatch rejects parent;
8. mode mismatch rejects parent;
9. profile mismatch rejects parent;
10. stale/incompatible generation rejects when generation authority exists;
11. exact PRETRIGGER parent accepted;
12. exact PREOPEN parent accepted;
13. exact parents produce deterministic parent IDs;
14. missing optional parent yields explicit missing/partial state, not invented identity;
15. same input after restart -> identical identity hash;
16. dictionary insertion order does not change hash;
17. `collected_at` / worker wall clock change does not change canonical BREACH identity/hash;
18. trigger_crossed_at change DOES change canonical evidence hash;
19. side/ticker/generation change DOES change canonical identity/hash;
20. malformed/naive breach timestamp fails closed;
21. malformed execution mode fails closed or explicit UNKNOWN according to current canonical store contract, never cross-routes;
22. output always `observe_only=true`, `affected_eligibility=false`;
23. zero DB mutation in pure assembly tests;
24. zero broker/selector/watcher/OSM/order/position/proof/queue calls.

Add production-shaped integration tests against existing #327 snapshot rows/fixtures to prove parent validation consumes the real generic snapshot shape rather than a hand-designed substitute.

## Merge gate

On one unchanged final implementation SHA require:

- focused identity/assembly tests;
- existing #327 snapshot/job tests;
- #614 PIT tests;
- #615 structure tests for adjacency only;
- exact-head P0 + cohesion;
- genuine pull_request merge-ref P0 + cohesion;
- current rollback jobs;
- `git diff --check`;
- whole-PR money-path audit;
- final production file count <=2 unless independently justified.

## Stopping rule

This PR is complete when current main has exactly one deterministic, observe-only BREACH identity/parent-validation/assembly owner that can be called by #621 without runtime wiring and without rebuilding #330.

Leave OPEN / DRAFT / HARD HOLD for independent audit.
