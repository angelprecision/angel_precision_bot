# P0 SPEC — PAPER canonical submit quote-domain parity

**STATUS: SPEC ONLY / HARD HOLD / DO NOT MERGE OR DEPLOY UNTIL IMPLEMENTED, REPLAYED, AND REVIEWED.**

Base: `main@462106c8839769ef6b3137867839a44aec39ad09`

## One job

Repair the canonical `ExecutionCore -> OSM -> Tradier sandbox` PAPER entry path so the final persisted broker limit is derived from a **fresh exact-OCC current-market quote authority**, not from the delayed/sandbox quote domain that currently downgrades otherwise-valid selector pricing immediately before submit.

This is a forward fix. Do not replace the selector, do not change LIVE pricing, do not add another broker submitter, and do not solve this by weakening stale-entry cancellation.

## Proven production defect

On 2026-08-19 PAPER orders reached Tradier sandbox but were submitted at stale/non-marketable prices after the canonical final quote refresh.

Representative TSLA shape:

```text
selector price / market-side evidence: about $1.86-$1.88
canonical PAPER pre-submit refresh:
    bid = $1.17
    ask = $1.21
current canonical PAPER ask-cross:
    $1.21 + $0.02 = $1.23
broker receives:
    limit = $1.23
shortly afterward order monitor sees:
    $1.52-$1.69
result:
    STALE_ENTRY_CANCEL / MISSED_MOVE
```

The arithmetic is not the root cause. The quote source is.

The repo already documents the same architectural mismatch in `ap/execution.py`: selector/current market data can come from Tradier LIVE while PAPER broker fills are judged in sandbox/delayed space. That legacy module contains `PAPER_ENTRY_FILL_MODE=marketable_limit` and `_compute_paper_marketable_limit()`, but the canonical production entry path is `APExecutionCore -> APOrderStateMachine.submit_existing_entry()`, not legacy `ap.execution.process_signal()`.

Therefore, do **not** "fix" this by routing canonical entries through `process_signal()`. PR #475 is explicitly retiring that second submit authority for retries.

## Relationship to existing PRs

- #482 owns PAPER **post-submit sandbox fill rescue / repricing priority** for an already broker-open ENTRY.
- #475 owns **post-cancel retry convergence** back to one canonical ENTRY path.
- This PR owns **initial canonical PAPER final-submit price authority before the first broker POST**.

Do not absorb #482 or #475 work. A correctly priced first submit and a later bounded rescue are complementary, not substitutes.

## Binding invariant

For PAPER only:

```text
selector chooses exact OCC
-> breach-time/final quality validation
-> fetch fresh exact-OCC quote from current market-data authority
-> prove quote source is not the Tradier sandbox/delayed execution domain
-> apply existing bounded PAPER marketable-limit policy to fresh current-market ask
-> preserve all existing chase/spread/affordability/final-cost gates
-> persist exact final submit price and quote evidence durably
-> broker_ready CAS / handoff proof
-> OSM rereads durable row
-> exactly one Tradier sandbox broker POST with that durable price
```

Never:

```text
fresh/current selector evidence
-> sandbox/delayed quote refresh
-> lower the price to delayed ask + pennies
-> persist stale limit
-> broker POST
```

For LIVE:

**behavior must remain unchanged.** Same quote source, same price calculation, same chase gates, same durable authority, same broker payload. This PR is not permission to change LIVE execution semantics.

## Current-main seams to inspect before editing

1. `ap_execution_core.py::_on_entry_trigger()`
2. the exact helper/block that performs the final exact-OCC quote refresh and computes the PAPER ask-cross/final limit
3. deferred and ordinary canonical entry branches separately; prove whether they converge before broker-ready persistence
4. `ap/order_state_machine.py::submit_existing_entry()` only as an authority audit; expected production edit count is zero
5. `ap/execution.py` quote-domain/PAPER helpers as historical implementation evidence, **not** as a submit engine
6. `ap/contract_selector.py` / selector instance fields that identify the market-data broker used for selection/direct quote recovery
7. existing tests:
   - `tests/test_quote_domain_paper_fill.py`
   - `tests/test_p0_entry_limit_pricing.py`
   - deferred lifecycle / final-cost revalidation tests around `ap_execution_core.py`

## Exact implementation contract

### 1. Identify one current-market quote authority already owned by the canonical stack

Do not construct an ad hoc new Tradier client inside `_on_entry_trigger()`.

Trace the active selector/data-broker object and reuse the same current-market data authority already used for contract selection/direct quote recovery. Preferred source is the selector's explicitly configured data broker if present.

The implementation must record and validate the quote source/base URL. For PAPER final-submit pricing:

- quote source must be explicit;
- a base URL containing `sandbox` is **not** acceptable as current-market pricing authority;
- missing/unknown quote source must not silently fall back to the sandbox broker;
- exact OCC identity must match the materialized order contract;
- quote must have finite positive bid/ask and non-inverted book;
- existing final spread/drift/chase rules remain authoritative.

If the current canonical stack does not expose a reusable market-data broker, STOP and document the smallest dependency-injection seam required. Do not instantiate credentials from environment in a random helper.

### 2. Add a small pure PAPER pricing helper, not another submit engine

The repo already has PAPER marketable-limit math in `ap/execution.py::_compute_paper_marketable_limit()`:

```python
cushion = min(
    submit_ask * PAPER_ENTRY_SLIPPAGE_CUSHION_PCT,
    PAPER_ENTRY_MAX_CUSHION_DOLLARS,
)
paper_limit = round(submit_ask + max(cushion, 0.01), 2)
```

Do not copy-paste a third divergent algorithm.

Preferred implementation:

- extract the pure PAPER pricing constants/helper into a neutral module, for example `ap/paper_entry_pricing.py` or another narrowly named module with **zero broker/DB side effects**;
- have legacy `ap/execution.py` import that helper so historical behavior remains semantically equivalent;
- have `ap_execution_core.py` import the same helper for the canonical PAPER path.

If a safe extraction causes circular imports, keep the helper canonical in one neutral location and prove both callers use it. Do not import all of `ap.execution` into ExecutionCore merely to reach one private helper.

No market-order authority is added by this PR. If `PAPER_ENTRY_FILL_MODE=market` exists in legacy code, leave that legacy behavior alone unless the canonical path already explicitly supports it. The required fix is `marketable_limit` parity.

### 3. Replace PAPER final-submit quote authority, not selector pricing

Do not change `contract_selector.py`'s PAPER `MID_SIMULATION` scoring/selection basis. That value is selection/simulation evidence, not final broker-execution authority.

At the canonical final-submit seam:

- LIVE: keep existing refresh logic untouched.
- PAPER: fetch the fresh exact-OCC quote from current-market data authority.
- apply the existing bounded PAPER marketable-limit helper to the fresh current-market ask.
- run every existing chase/runaway, spread, quantity, affordability, real-cost revalidation, Master Control, and broker-ready ordering guard against the correct final economics.

If current code compares the final quote to selector-era price for runaway/chase protection, preserve that check using the **current-market quote**, not the sandbox quote. A genuine live-market runaway must still block PAPER. This PR is not permission to chase anything.

### 4. Durable price authority remains OSM-owned

Do not weaken `APOrderStateMachine.submit_existing_entry()`.

The exact final PAPER price must be persisted to the durable `orders.limit_price` (and any canonical broker-ready metadata) **before** OSM broker submission. OSM must reread/use the durable price exactly as today.

Forbidden shortcut:

```text
caller computes better paper price
-> passes it only in memory
-> OSM durable row still contains old price
```

Required:

```text
compute final price
-> exact client/mode/local-order CAS persist
-> durable reread/postcondition
-> broker_ready/handoff proof
-> OSM submit_existing_entry
-> broker payload price == durable orders.limit_price
```

### 5. Persist quote-domain evidence

For PAPER orders, durable metadata must retain enough evidence to answer "which market priced this order?" after the fact.

At minimum persist, without removing existing fields:

- `selector_price`
- `selector_pricing_basis`
- `paper_pricing_policy = marketable_limit`
- `submit_market_quote_bid`
- `submit_market_quote_ask`
- `submit_market_quote_source`
- `submit_market_quote_base_url`
- `submit_market_quote_ts` or quote age
- `paper_cushion_applied`
- `final_submit_limit`
- `broker_execution_quote_domain` / sandbox identity as diagnostics if available
- exact `client_id`
- exact canonical `execution_mode=paper`
- exact OCC contract
- local order id / signal id already used by the path

Do not overwrite original selector evidence. We need both "what selector saw" and "what final execution pricing used."

### 6. Failure behavior

If PAPER current-market final quote authority is unavailable, malformed, stale beyond existing freshness tolerance, sandbox-only, or identity-conflicting:

- zero broker POST;
- do not fall back to delayed sandbox ask as if it were current truth;
- use the existing retry/terminal taxonomy appropriate to quote/data unavailability;
- preserve durable diagnostics explaining the quote-domain failure.

Do not invent a new retry engine. Reuse the canonical existing result path.

## Mandatory fail-first tests

Create `tests/test_p0_paper_canonical_submit_quote_domain_20260819.py` and register it in the P0 workflow after fail-first proof.

Minimum required cases:

1. production TSLA shape: selector `1.88`, current-market quote `bid/ask ~1.65/1.69`, sandbox quote `1.17/1.21` -> final PAPER limit is derived from `1.69`, never `1.21`
2. assert resulting marketable-limit math uses the shared bounded helper and would be executable relative to the current-market ask
3. broker payload price equals durable final `orders.limit_price`
4. persisted `submit_market_quote_base_url` is non-sandbox and broker execution domain is sandbox
5. exact OCC mismatch in returned market quote -> fail closed, zero broker POST
6. market-data quote source unknown -> fail closed, zero broker POST
7. market-data source accidentally points to sandbox -> fail closed, zero broker POST
8. zero bid/ask -> existing retryable quote-data path, zero broker POST
9. inverted quote -> fail closed
10. non-finite quote -> fail closed
11. existing final spread-too-wide rule remains unchanged
12. existing runaway/chase-band rejection remains unchanged when current market genuinely runs away
13. selector price may be above current ask after a pullback; final PAPER price follows fresh market authority and does not blindly preserve a stale higher selector price unless an existing guard explicitly requires it
14. deferred PAPER path uses correct final price
15. ordinary canonical PAPER path uses correct final price if that path shares the same submit seam
16. exact client identity preserved for José-shaped PAPER account
17. exact client identity preserved for Tradefluence-shaped PAPER account
18. `execution_mode=paper` preserved; no LIVE/PAPER row crossover
19. LIVE comparison test: identical inputs before/after produce identical LIVE final limit and broker payload
20. LIVE must never call PAPER marketable-limit helper
21. no new broker submit call site; exactly the existing OSM submit path
22. no new broker cancel authority
23. no position/proof/queue mutation before fill
24. duplicate/restart callback cannot POST twice because existing OSM/durable ownership remains authoritative
25. final-cost/Master-Control test proves they see the final PAPER broker-bound limit, not selector MID or sandbox delayed ask

## Required production replay

Replay at least these recent PAPER shapes from production metadata:

- 2026-08-19 TSLA Jose
- 2026-08-19 TSLA Tradefluence
- one Aug-18/17 example such as C, MSFT, SMCI, QCOM, UNH, or NOW

For each, show:

```text
selector price
old sandbox refresh bid/ask
old final limit
current-market replay bid/ask at equivalent seam if captured/available
new computed final limit
existing chase/spread/final-cost decision
broker POST count
```

If historical current-market quote at the exact millisecond is unavailable, use the recorded selector/direct-quote evidence and a deterministic production-shaped replay. Do not fabricate historical quotes.

## Non-goals / forbidden shortcuts

- Do not revert the bot to July 29.
- Do not change LIVE pricing.
- Do not change selector MID_SIMULATION scoring.
- Do not lower spread/OI/volume/delta/DTE/premium gates.
- Do not disable `STALE_ENTRY_CANCEL` or `MISSED_MOVE` to hide a bad initial price.
- Do not implement #482 rescue behavior here.
- Do not implement #475 post-cancel retry here.
- Do not route canonical submission through `ap.execution.process_signal()`.
- Do not let caller memory override OSM durable price authority.
- Do not add a second broker POST owner.
- Do not default missing `client_id` or execution mode.
- Do not mutate positions or proof trades before broker-confirmed fill.

## Expected size

This should be **small-to-medium**, not large:

- likely 1 main production behavior file: `ap_execution_core.py`
- possibly 1 tiny pure helper module plus a narrow import change in `ap/execution.py` to share the existing PAPER marketable-limit math
- expected zero production changes in OSM
- 1 focused test file plus CI/spec

A reasonable implementation is roughly low hundreds of production LOC at most, much of it diagnostics/tests. If this expands into selector rewrites, OSM rewrites, order-monitor rescue, retry engines, or broker adapter redesign, STOP. Scope has escaped the proven defect.

## Required delivery back to operator

Return:

1. exact base SHA and final head SHA;
2. exact changed filenames;
3. exact canonical quote source chosen and why it is current-market authority;
4. fail-first count with the TSLA $1.88 -> sandbox $1.21 -> old $1.23 reproduction;
5. post-fix expected price from the same replay;
6. proof LIVE behavior is unchanged;
7. proof broker payload == durable `orders.limit_price`;
8. proof no second broker submit/cancel owner exists;
9. focused/adjacent test counts;
10. exact-head P0 CI result;
11. interaction audit against #482 and #475;
12. fresh recommendation: MERGE / HOLD / HARD HOLD.

**Do not merge or deploy from this spec alone.**