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
- trigger_crossed_at;
- BREACH snapshot identity.

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
7. late result after broker-ready/submit cannot cancel or rewrite decision;
8. stale generation cannot attach evidence;
9. wrong client cannot attach evidence;
10. wrong mode cannot attach evidence;
11. same ticker concurrent clients stay isolated;
12. same ticker opposite sides stay isolated;
13. exactly one existing selector invocation remains reachable for the valid path;
14. no additional network/history call occurs in synchronous bridge;
15. no watcher/OSM/broker/order/position/proof/queue mutation from intelligence path;
16. restart/recovered breach uses the same durable identity/frozen evidence contract where existing runtime permits replay;
17. PAPER/LIVE share intelligence taxonomy but never execution identity.

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
