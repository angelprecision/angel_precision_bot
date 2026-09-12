# PR #623 — 435-E BREACH Replay / Evidence Closure Binding Spec

## Status

**DRAFT / HARD HOLD / SPEC ONLY. DO NOT MERGE OR DEPLOY.**

Depends on final:
- #615;
- #621;
- #622.

## Purpose

Prove the replacement #435 stack describes real production-shaped incidents deterministically before any timing policy is given authority.

This PR is replay/evidence only.

No admission policy. No WAIT/REARM/TERMINAL authority. No broker or watcher mutation.

## Required incident set

Mandatory fixtures:
- NOW;
- QQQ;
- HOOD;
- LULU.

Add positive/negative controls sufficient to prove the model is not merely overfitting those examples.

Where exact durable production identities are available, bind replay to those identities rather than ticker/time guesses.

## Evidence chain per opportunity

For each replay, record and assert:
1. exact client/mode/signal/order/generation identity;
2. exact `trigger_crossed_at`;
3. #614 frozen PIT as-of;
4. selected 5m/15m source + authority state;
5. completed 1h/4h structure available at that time;
6. relevant FVG identity/geometry/alignment;
7. breach price position relative to FVG;
8. wick/body penetration;
9. >=50% body-through state;
10. body/range strength;
11. strong-break result;
12. pullback/reclaim/re-breach state as evidence later becomes available;
13. actual post-breach underlying path;
14. existing selector/submit outcome when available;
15. no fabricated option fill/economics.

## NOW fixture intent

Must reproduce the incident class:
- PUT candidate at/bottom of bullish FVG support;
- penetration/tap/wick occurs;
- no completed strong 5m/15m body acceptance through support at the decision-time evidence point;
- structure snapshot honestly reports opposing FVG and weak/unconfirmed break;
- observe-only stack does not itself block/alter the historical execution path.

## QQQ / HOOD / LULU fixture intent

Replay the pullback/recovery hypothesis without hindsight fabrication.

Record:
- first breach;
- immediate extension or pullback;
- return/reclaim to pretrigger side when it happened;
- exact completed-close re-breach when it happened;
- time-to-green/MFE/MAE only from exact available path evidence if already present in fixture sources;
- no claim that a hypothetical delayed entry filled unless a prospective policy + contemporaneous executable evidence exists.

## Restart parity

For the same exact durable opportunity and generation:
- uninterrupted execution and restart/recovery must produce the same frozen BREACH structure from the same evidence;
- restart may not substitute current quote/current candle for missing historical decision-time truth;
- snapshot identity/hash should remain consistent under #621 contract;
- stale/newer generation must not attach to the old evidence.

## Mandatory negative controls

At minimum:
- wrong client;
- wrong execution_mode;
- same ticker different signal;
- same ticker opposite side;
- stale generation;
- later current price contradiction;
- missing provider evidence;
- stale provider evidence;
- malformed timestamps;
- future candle after breach;
- same OCC/economic trade reused elsewhere where relevant.

These controls must fail closed for attribution/enrichment with zero trading mutation.

## Comparison output

Produce a machine-readable replay summary per fixture with:
- expected structural facts;
- observed structural facts;
- match/mismatch;
- source authority;
- missing fields;
- deterministic hash/version;
- restart parity result;
- money-path mutation count (must remain zero from intelligence stack).

## Closure criterion for historical #435

Historical broad PR #435 may be declared replaced only when:
- #614 is merged;
- #615 is merged;
- #621/#622 are merged;
- mandatory #623 fixtures pass;
- restart parity passes;
- exact identity isolation passes;
- zero intelligence money-path mutation is proven;
- no required 435 capability remains dependent on the historical monolith.

Then historical #435 remains permanently **DO NOT MERGE WHOLESALE**.

## Explicit non-scope

No:
- #436 policy implementation;
- selected-contract authority;
- broker submit/cancel;
- watcher lifecycle change;
- selector threshold/ranking change;
- risk/sizing change;
- position/proof/queue mutation;
- LIVE promotion.

## Merge gate

Require:
- all mandatory fixture replays;
- uninterrupted/restart parity;
- exact identity negatives;
- #614/#615/#621/#622 focused suites;
- exact-head P0 + cohesion;
- genuine merge-ref parity;
- `git diff --check`;
- independent final audit of the unchanged SHA.
