# PR #622 — 435-D Observe-Only BREACH Runtime Bridge Binding Spec

## Status

**DRAFT / HARD HOLD. IMPLEMENTATION PRESENT FOR REVIEW ONLY. DO NOT MERGE OR DEPLOY.**

Depends on:
- final #615;
- #625 canonical BREACH identity/assembly;
- #621 durable snapshot adapter;
- #327 durable job/snapshot substrate.

## Existing runtime seam to reuse

Current main calls the bridge after BREACH risk revalidation and approved-plan
recovery inside `_on_entry_trigger()`. The historical #330 implementation is
not imported or resurrected.

Do not create a second trigger callback, watcher, selector path, or execution
engine. The bridge does not call `contract_selector.select()`.

#622 owns only the tiny bridge that freezes the already-confirmed identity and
attached #614 evidence, then hands it to the bounded sideband worker. The
worker calls #615, #625, and #621; #327 remains the durable persistence owner.

## Current-main source and ordering audit

The confirmed production PIT source is **NOT PRESENT at this runtime seam**.
The exact current-main evidence path is:

`ap.queue._dispatch()` -> `enqueue_pretrigger_context_best_effort()` ->
`submit_intelligence_enqueue()` -> later
`process_due_intelligence_jobs_once()` -> `build_snapshot_kwargs()` ->
`build_intelligence_context_payload()` ->
`collect_point_in_time_context()`.

The collector is therefore a background materializer source, not a producer
that writes an exact #614 envelope onto `WatchedSignal`, `approved_plan`, or
`sig` before `_on_entry_trigger()`. In the separate runtime path,
`entry_watcher.watch()` builds the watcher signal with identity, trigger, plan
metadata, and durable `trigger_crossed_at`. Although caller-provided mappings
can travel inside plan metadata, this path has no code that calls #614 or
creates/attaches an authoritative PIT envelope. `WatchedSignal` stores that
payload and parses the timestamp, and `APExecutionCore._on_entry_trigger()`
receives it unchanged for the #622 projection. #622 consequently preserves
missing evidence as explicit UNKNOWN/MISSING and performs no lookup to
manufacture it.

The seam is intentionally before hydration and before the existing selector
continuation. `_refresh_hydrated_prebreach_plan()` currently mutates only
contract/price/quantity/reserved-cost fields and contract-selection metadata;
the focused test proves it leaves #622/#625 identity and evidence fields
unchanged. If that owner later mutates a bridge identity/evidence field, the
seam must move after hydration.

## Critical latency invariant

The callback path must remain:

`confirmed breach -> immutable local artifact -> nonblocking handoff -> existing selector/submission path`

The background sideband is:

`frozen artifact -> #615 -> #625 -> #621 -> #327`

It must never become:

`confirmed breach -> provider/history fetch -> wait -> rebuild all timeframes -> #615 -> selector`

No synchronous market-history transport, #615 freeze, database enqueue, or
snapshot write may be introduced before selector execution.

No `Future.result()`, join, sleep, polling wait, or blocking worker handoff may be added.

## Evidence availability behavior

When exact #614 evidence is already attached to the confirmed watcher/plan:
- freeze a deep-copied, bounded runtime artifact;
- hand it off without waiting;
- run #615 in the worker;
- assemble through #625 and enqueue through #621 into #327.

When evidence is missing, stale, unavailable, cache-missed, malformed, or late:
- record honest UNKNOWN/MISSING/non-authoritative state;
- emit diagnostics;
- preserve the existing trade path.

Missing intelligence is not adverse truth.

Late intelligence may enrich later analytics but cannot retroactively mutate a selector/broker decision already made for that generation.

## Normalized structure-view API

The implementation owns one provider-neutral read-side helper:
`resolve_breach_structure_view(frozen_context, expected_identity=...)`.
It consumes only an already-produced/frozen mapping and returns the complete
identity, exact `as_of`/`decision_boundary`, source/profile/model versions,
structure and point-in-time provenance, normalized zones, frozen lower-TF
acceptance/rejection/reclaim/penetration facts, and diagnostics. It performs no
provider, history, network, database, selector, or eligibility work.

Its only normalized statuses are `AUTHORITATIVE`, `STALE`, `UNKNOWN`, and
`INVALID`. An authoritative empty zone set is `AUTHORITATIVE` with
`zones=[]`; unavailable evidence is `UNKNOWN`; stale evidence or an older
generation is `STALE`; malformed or contradictory input is `INVALID`. Missing
zones are never fabricated and no volume imbalance is inferred. The same
frozen input must yield identical output through uninterrupted, restart, and
materialized/deferred paths.

## Runtime authority

#622 adds zero admission authority.

Forbidden outcomes from this bridge:
- WAIT;
- REARM;
- TERMINAL;
- REJECT;
- CANCEL;
- watcher removal;
- watcher ownership transfer;
- selector suppression;
- broker suppression;
- broker submit/cancel/replace;
- order mutation;
- position mutation;
- proof mutation;
- queue mutation.

Those remain outside #622.

## Identity requirements

The runtime bridge must preserve exact:
- client_id;
- execution_mode;
- signal_id;
- canonical_signal_id;
- local_order_id;
- current lifecycle/materialization generation;
- profile_version;
- model_version;
- phase;
- trigger_crossed_at;
- BREACH snapshot identity.

The process-local duplicate key is exactly the full #625 semantic tuple above;
generation is removed only when deriving the generation-fence scope. A profile
or model change is therefore a new identity, while an older generation remains
stale within the same semantic scope.

Stale generation or identity mismatch:
- may prevent intelligence enrichment;
- must not mutate the current trade lifecycle;
- must not attach evidence to another opportunity.

An absent or invalid current materialization generation is treated as an
unproven sideband identity and is not handed off.

## Performance budget

Instrument bridge-owned compute separately from provider/network time.

Because provider/network work is forbidden on the synchronous bridge, expected bridge work should be bounded local/cached compute.

Record timing diagnostics sufficient to answer:
- evidence lookup duration;
- freezer duration;
- snapshot enqueue/write handoff duration;
- total bridge duration;
- fallback reason when evidence unavailable.

Do not turn telemetry timing into trading authority.

## Required tests

At minimum:
1. exact cached evidence -> #615 snapshot enrichment -> normal selector path continues;
2. cache miss -> UNKNOWN/MISSING + selector path continues;
3. stale evidence -> non-authoritative + selector path continues;
4. malformed evidence -> fail-soft + selector path continues;
5. #615 exception -> fail-soft + selector path continues;
6. snapshot persistence/enqueue exception -> fail-soft + selector path continues;
7. an accepted worker failure releases the exact identity for retry, including
   #615 and #625 exceptions; durable #621 success/duplicate retains it;
8. late result after broker-ready/submit cannot cancel or rewrite decision;
9. stale generation cannot attach evidence;
10. wrong client cannot attach evidence;
11. wrong mode cannot attach evidence;
12. same ticker concurrent clients stay isolated;
13. same ticker opposite sides stay isolated;
14. exactly one existing selector invocation remains reachable for the valid path;
15. no additional network/history call occurs in synchronous bridge;
16. no watcher/OSM/broker/order/position/proof/queue mutation from intelligence path;
17. restart/recovered breach uses the same durable identity/frozen evidence contract where existing runtime permits replay;
18. PAPER/LIVE share intelligence taxonomy but never execution identity;
19. the real deferred `_on_entry_trigger()` invokes the existing
    `self.contract_selector.select(...)` exactly once under each bridge outcome:
    ACCEPTED, bridge exception, SATURATED/REJECTED, and missing/invalid
    intelligence; no synthetic selector marker stands in for that call;
20. the real continuation submits unchanged when the bridge throws,
    rejects/saturates, or reports missing/invalid evidence.

## Explicit non-scope

No:
- new market-data transport;
- full-history rebuild;
- entry policy;
- #436 timing policy;
- scanner work;
- setup discovery;
- selector threshold change;
- sizing/risk change;
- broker adapter change;
- exit logic.

## Merge gate

Before implementation, trace current main exactly:
`_on_entry_trigger -> existing BREACH dispatch -> snapshot assembly -> selector continuation`

After implementation require:
- focused #622 latency/fail-soft tests;
- #330 BREACH dispatch regressions;
- #614/#615/#621 integration tests;
- exact-head P0 + cohesion;
- genuine merge-ref parity;
- `git diff --check`;
- final proof that no synchronous network/history read was introduced.

No merge until independent audit clears exact final SHA.

## Implementation surface

- `ap/intelligence_breach_runtime_bridge.py` — immutable artifact, bounded
  duplicate guard, nonblocking handoff, and background #615/#625/#621 chain;
- `ap_execution_core.py` — one diagnostic-only call at the existing confirmed
  BREACH seam;
- `tests/test_p1_435d_breach_runtime_bridge.py` — identity, timing,
  evidence-fallback, failure, isolation, and authority tests;
- `.github/workflows/p0_regression.yml` — focused test enrolled in both
  exact-head and merge-ref inventories.
