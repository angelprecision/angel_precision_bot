# P1 435-C — Durable BREACH Snapshot Adapter

## Status
DRAFT / HARD HOLD / SPEC ONLY. Do not merge or deploy.

## Purpose
Reuse the durable intelligence substrate already merged in #327/#330. Do not create a second snapshot system.

Canonical path:

`#614 point-in-time BREACH evidence + #615 frozen structure -> existing ap_intelligence_snapshots BREACH record`

## Required identity
Persist/verify exact:
- client_id
- execution_mode
- signal_id
- canonical_signal_id
- local_order_id
- durable lifecycle/materialization generation when present
- trigger_crossed_at / canonical BREACH as-of
- structure schema/model version
- deterministic input/structure hash

## Invariants
- Existing #327/#330 snapshot/job tables and writers remain canonical.
- No new snapshot table, worker, queue, or BREACH identity model.
- Restart/replay of the same durable opportunity and same frozen evidence resolves the same snapshot identity/content hash.
- A newer lifecycle/materialization generation cannot silently reuse an older frozen structure snapshot.
- Missing/malformed identity or structure remains telemetry failure only and cannot change eligibility, watcher ownership, selector behavior, or broker reachability.
- PAPER and LIVE remain exact-mode isolated.

## Required tests
- exact #614/#615 production-shaped payload adapts into the existing BREACH writer;
- same opportunity/restart is idempotent;
- generation mismatch fails closed for telemetry;
- client/mode/signal/local-order mismatch fails closed;
- structure/schema version is durable and queryable;
- no future worker-time data can rewrite trigger-time structure;
- zero broker/order/position/proof/queue mutation.

## Dependency
Implement only after #615 is rebased and independently cleared against merged #614.
