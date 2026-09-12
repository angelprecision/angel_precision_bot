# Angel Precision Intelligence Work Progress — Canonical Repository Map

Date: 2026-09-12

## Why this document exists

This file is the durable repository record of the current intelligence build so future implementation/audit does not depend on chat history.

The guiding rule is surgical ownership: one narrow responsibility per PR, no duplicate subsystems, no broad historical branch resurrection.

Current baseline at creation:
`main@846525a2ee798071bb0079078f0aec871c415677`

Historical monolith PR #435 remains **DO NOT MERGE WHOLESALE**.

---

# Canonical 435 replacement stack

## 435-A — PR #614 — MERGED

Purpose: point-in-time market-data foundation.

Merged responsibilities:
- exact BREACH as-of from `trigger_crossed_at`;
- exact completed 5m/15m evidence;
- completed 1h/4h aggregation;
- bounded history;
- provider-vs-frozen source authority;
- authoritative/stale/missing/not-due coverage semantics;
- no worker/current quote contamination of BREACH evidence.

Does not own structural interpretation or timing policy.

## 435-B — PR #615 — ACTIVE / HARD HOLD

Purpose: observe-only market-structure brain.

Owns:
- 4H/1H FVG geometry/lifecycle;
- stable zone IDs;
- aligned vs opposing FVG;
- inside/above/below/boundary state;
- exact VI or explicit MISSING;
- 5m/15m wick-vs-body penetration;
- >=50% body-through;
- body/range strength;
- strong-break observation;
- pullback/reclaim/re-breach evidence;
- explicit regime/UNKNOWN;
- setup archetype + observe-only posture.

Binding amendment is stored on the #615 branch:
`docs/pr_specs/p1_615_post_614_amendment_20260912.md`

Before merge #615 must:
- rebase onto post-#614 main;
- consume #614 `data_sources["candles"]` directly;
- consume exact #614 `as_of`, not worker `collected_at`;
- preserve #614 coverage/source authority;
- use candle completion boundary for breach-straddling sequencing;
- prove exact #614 -> #615 production-shape integration;
- remain observe-only / zero eligibility authority.

## 435-C — PR #621 — HARD HOLD

Purpose: adapt `#614 PIT + #615 frozen structure` into the already-merged durable BREACH snapshot system from #327/#330.

Binding contract:
`docs/pr_specs/p1_435c_durable_breach_adapter_binding_20260912.md`

Must reuse existing:
- snapshot/job tables;
- snapshot store/worker;
- phase-aware identity;
- PRETRIGGER/PREOPEN parent linkage;
- BREACH snapshot assembly.

Must bind exact client/mode/signal/canonical/local-order/generation/trigger/as-of/version/hash identity.

No second snapshot subsystem.

## 435-D — PR #622 — HARD HOLD

Purpose: observe-only runtime bridge into #330's existing BREACH dispatch seam.

Binding contract:
`docs/pr_specs/p1_435d_breach_runtime_bridge_binding_20260912.md`

Critical rule:
**no synchronous network/history fetch or wait before selector execution.**

Only already-available/cached/frozen evidence may be consumed synchronously. Cache miss/stale/unavailable intelligence becomes UNKNOWN/MISSING and existing trading proceeds unchanged.

No WAIT/REARM/TERMINAL authority in #622.

## 435-E — PR #623 — HARD HOLD

Purpose: replay/evidence closure before policy authority.

Binding contract:
`docs/pr_specs/p1_435e_breach_replay_evidence_binding_20260912.md`

Mandatory production-shaped incidents:
- NOW;
- QQQ;
- HOOD;
- LULU;
- positive/negative identity and structure controls.

Must prove uninterrupted/restart parity and zero intelligence money-path mutation.

Closure criterion: when #614/#615/#621/#622 + #327/#330 reproduce the required incidents deterministically, historical #435 is considered replaced and remains permanently DO NOT MERGE WHOLESALE.

---

# Post-435 intelligence stack

## PR #436 — SURGICAL RECUT REQUIRED

Historical branch is too broad/stale to merge as-is.

Canonical responsibility:
`frozen structure -> prospective READY / WAIT / REARM / TERMINAL candidate`

No global `second touch = buy` rule.

Examples of policy questions:
- first touch into opposing FVG -> WAIT candidate;
- wick-only / no completed acceptance -> not proven break;
- completed 5m/15m body acceptance -> stronger READY evidence;
- pullback/reclaim/re-breach -> reevaluate with fresh current truth.

#620 was closed as redundant because this ownership belongs in #436.

## PR #437 — SELECTED CONTRACT EVIDENCE

After selector chooses durable OCC and before broker POST, freeze exact selected-contract evidence:
- OCC/expiration/DTE/strike;
- bid/ask/mid, with ASK/intended BUY as executable entry authority;
- spread;
- Greeks/IV;
- OI/volume;
- underlying/expected move;
- qty/debit;
- premium chase;
- exact client/mode/order/generation identity.

First implementation observe-only.

## PR #577 — PATH / OUTCOME CAPTURE

Existing implementation must be rebased/audited after canonical upstream evidence exists.

Capture:
- MFE/MAE;
- time-to-green;
- submit/fill timing;
- reclaim/re-breach path;
- realized result;
- prospective counterfactual evidence only when contemporaneous shadow policy existed.

## PR #443 — EXACT ATTRIBUTION

Join exactly:
`BREACH snapshot + CONTRACT_SELECTED + actual path + canonical proof`

No fuzzy ticker/time joins.

## PR #438 — PROMOTION GATE

Promotion sequence:
`observe-only -> PAPER authoritative -> LIVE shadow -> explicitly approved limited LIVE`

Evaluate:
- expectancy;
- win rate;
- average win/loss;
- fill rate;
- MFE/MAE;
- time-to-green;
- skipped winners;
- avoided losers;
- concentration;
- attribution coverage;
- sample size/uncertainty.

No automatic LIVE promotion.

---

# Separate intraday discovery stack

Goal: continuously monitor a small highly liquid universe such as SPY, QQQ, IWM, AAPL, GOOGL, NVDA, MSFT and discover setups developing during RTH.

5–10 setups/day is an opportunity-discovery objective, not a quota.

Architecture:
`market updates -> #617 forming bars -> #618 pure classifier -> #619 observe-only candidates -> future promotion seam`

## PR #617 — FORMING BAR STATE

Binding contract:
`docs/pr_specs/p1_intraday_forming_bar_state_binding_20260912.md`

Owns deterministic incremental 5m/15m/30m/60m RTH bar state only.

No setup classification or trading authority.

## PR #618 — PURE SETUP CLASSIFIER

Binding contract:
`docs/pr_specs/p1_intraday_setup_classifier_binding_20260912.md`

Initial primitives:
- 1;
- 2U;
- 2D;
- 3.

Initial setup:
- developing/completed 2-3-2.

No market-data I/O, DB, scanner, watcher or broker behavior.

## PR #619 — CANDIDATE SCANNER

Binding contract:
`docs/pr_specs/p1_intraday_candidate_scanner_binding_20260912.md`

Owns only bounded orchestration + deterministic candidate identity/deduplication.

No signal promotion, watcher install, selector, broker, order, position, proof or queue mutation.

A separate future PR must own promotion from observe-only candidate into the existing Angel Precision signal/watcher pipeline.

---

# Global engineering invariants

1. One authority owner per responsibility.
2. Keep PRs narrow and behaviorally proven.
3. Never resurrect broad historical branches wholesale when current-main authority already exists.
4. Unknown/missing intelligence is not adverse truth.
5. Observe-only evidence cannot change eligibility.
6. No synchronous full-history rebuild in breach/entry hot paths.
7. Exact client/mode/signal/order/generation identity is mandatory.
8. Decision-time evidence cannot be rewritten by later current-market state.
9. No position/proof before exact fill authority.
10. No duplicate broker submit path.
11. Runtime/restart must resolve the same authority from the same durable facts.
12. If implementation needs broader production scope than its PR contract, STOP and re-audit rather than silently broadening.
