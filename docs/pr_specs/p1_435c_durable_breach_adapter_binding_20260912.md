# PR #621 — 435-C Durable BREACH Snapshot Adapter Binding Spec

## Status

**DRAFT / HARD HOLD / SPEC ONLY. DO NOT MERGE OR DEPLOY.**

Depends on final merged #615.

## Existing authority to reuse

Do not build a new snapshot system.

Merged #327/#330 already own:
- `ap_intelligence_snapshots`;
- `ap_intelligence_jobs`;
- durable phase-aware identity;
- PRETRIGGER/PREOPEN parent relationships;
- snapshot worker/store;
- idempotent persistence;
- BREACH snapshot assembly;
- BREACH dispatch identity and compact pointers.

#621 owns only the adapter/audit required to carry sharper #614/#615 evidence through that existing substrate.

## Canonical input

Input is the exact frozen result of:
`#614 PIT collector -> #615 breach market-structure freezer`

The adapter must not refetch or reinterpret market data.

## Required durable identity

A persisted BREACH structure snapshot must bind exact:
- `client_id`;
- normalized `execution_mode`;
- `signal_id`;
- `canonical_signal_id`;
- `local_order_id`;
- lifecycle/materialization generation when current architecture exposes it;
- exact `trigger_crossed_at`;
- exact #614 PIT `as_of`;
- #615 `schema_version`;
- #615 `model_version`;
- deterministic structure input hash;
- deterministic frozen-structure output hash;
- parent PRETRIGGER/PREOPEN snapshot identity where applicable.

Never join by ticker/time proximity.

## Snapshot semantics

Same exact economic opportunity + same generation + same frozen evidence must be idempotent.

Restart/replay must not generate a semantically new interpretation merely because:
- worker process changed;
- collection occurred later;
- current quote changed;
- current market moved;
- another client traded same ticker;
- PAPER and LIVE both saw same setup.

A later/newer generation may create a distinct BREACH snapshot only when canonical lifecycle identity says it is genuinely new.

## Data payload requirements

Persist enough #615 output to support later replay/attribution without recomputation from current market state:
- data_as_of / trigger as-of;
- frozen underlying observation + provenance;
- 5m/15m coverage/source authority;
- relevant 4h/1h FVG identities and geometry;
- relevant opposing FVG;
- penetration measurements;
- strong-break observations;
- exact-or-MISSING VI;
- pullback/reclaim/re-breach evidence available at snapshot time;
- explicit regime/UNKNOWN;
- setup archetype / observe-only posture;
- reason codes;
- model/schema version.

Do not persist fabricated current/future evidence.

## Persistence failure behavior

This remains intelligence telemetry only.

Snapshot persistence failure:
- must be observable;
- may retry through existing snapshot/job machinery if that machinery already owns retry;
- must not alter watcher ownership;
- must not delay/deny selector or broker execution;
- must not terminalize the trade;
- must not create fallback current-market evidence.

## Required tests

At minimum:
1. exact LIVE snapshot identity persists correctly;
2. exact PAPER identity remains PAPER;
3. same ticker LIVE/PAPER cannot cross-bind;
4. wrong client cannot attach to parent snapshot;
5. wrong canonical signal cannot attach;
6. wrong local order cannot attach;
7. wrong/new generation cannot overwrite prior generation;
8. same exact evidence repeated is idempotent;
9. restart with same durable facts resolves same snapshot identity;
10. worker-time later current price cannot rewrite frozen breach price;
11. structure version/hash is durable;
12. parent PRETRIGGER/PREOPEN link remains exact;
13. snapshot persistence error is fail-soft for trading;
14. no broker/selector/watcher/order/position/proof/queue mutation;
15. no new table/migration unless an independently proven existing schema cannot represent a mandatory field; if schema work appears necessary, STOP and re-audit instead of broadening silently.

## Expected scope

Prefer adaptation inside existing intelligence snapshot/materializer modules plus focused tests only after tracing actual current-main caller flow.

Before editing production, post the exact trace:
`caller -> input identity -> existing BREACH assembly -> adapter -> snapshot write -> downstream reader`

If more than a very small number of production files appears necessary, STOP and keep HARD HOLD.

## Merge gate

After implementation on then-current main:
- focused #621 tests;
- existing #327/#330 intelligence snapshot suites;
- #614/#615 integration suites;
- restart/idempotency proof;
- exact-head P0 + cohesion;
- genuine merge-ref parity;
- `git diff --check`;
- whole-PR money-path audit.

No merge until independent audit clears the unchanged final SHA.
