# P0 Gate G Production-Shape Correction

## Status

**DRAFT / HARD HOLD. IMPLEMENTATION CONTRACT ONLY. DO NOT MERGE OR DEPLOY THIS DOCS-ONLY PR AS A FIX.**

Base: `main@5284edbdc7af845a634314dc3348cb50f3f846e0`.

This is PR 2 in the profitability-intelligence repair stack. It is intentionally separate from the outcome-binding repair and from the new breach-time intelligence architecture.

## Objective

Make the existing live-admission intelligence stop pretending it knows facts it does not know.

The active Gate G path today calls `intelligence_bridge.py`, which creates/uses the legacy `APSignalPipeline`. That pipeline can influence admission, but several of its inputs are not production-truthful at the moment they are evaluated.

This PR does **not** attempt to make Gate G the final profitability engine. It removes false authority and shape errors so Gate G is safe and honest while a better breach-time intelligence stack is built observe-only.

## Confirmed defects / risks

### 1. Real 0DTE can become 1DTE

The legacy pipeline uses truthy fallback patterns equivalent to:

`dte = int(signal.get("dte") or 1)`

A legitimate numeric `0` is falsy and therefore becomes `1`.

For an options system where 0DTE and 1DTE have materially different premium behavior, gamma/theta, risk, and liquidity, this is not cosmetic metadata corruption. Gate G can evaluate the wrong product class.

Required invariant:

> Explicit `dte=0` must survive as `0` end-to-end. Only missing/unparseable DTE may use a documented fallback, and a fallback must never become hard authority.

### 2. Contract quality is evaluated before a real contract may exist

Gate G can run before selector has produced a real OCC contract and fresh executable option quote.

Legacy `run_quick()` paths can substitute estimated/default values for fields such as premium, spread, delta, open interest, and volume when signal metadata does not contain actual selected-contract evidence.

That creates a dangerous semantic collision:

- a hard contract-quality gate sounds like it evaluated a real contract;
- in reality it may have evaluated defaults or estimates before any actual contract existed.

Required invariant:

> Pre-selector intelligence must never claim authoritative contract-quality approval or veto unless a production-proven real selected contract and fresh quote are present.

Before selector, contract evidence must be classified `NOT_AVAILABLE_YET`, not silently synthesized.

### 3. Gate G risk manager uses non-canonical account state

`intelligence_bridge` initializes a per-client legacy pipeline with account equity sourced from environment, defaulting to `$25,000` if missing.

Its internal risk manager maintains process-local `open_positions` and `daily_pnl`, but repository-wide current-main tracing does not show production lifecycle calls reliably updating those fields from canonical account/order/position truth.

That means this layer can reason about capital, sector exposure, correlations, or daily loss using state that is stale, empty, or generic.

Master Control already has separate account-risk authority based on real runtime state.

Required invariant:

> A secondary intelligence module may not become a second account-risk authority using synthetic equity or process-local portfolio state.

If exact current account state is unavailable to Gate G, portfolio/account-risk dimensions become `UNAVAILABLE/ADVISORY`, while canonical Master Control risk remains authoritative.

### 4. Scanner score and intelligence score semantics are mixed

Current bridge behavior includes scanner-approved observe-only/fail-open paths. That was intentionally introduced after weak intelligence blocked legitimate opportunities.

Do not undo that safety correction by merely increasing `INTEL_APPROVE_THRESHOLD` or making low-confidence outcomes hard blocks.

Required invariant:

- `RISK_VETO` only when backed by a proven hard-risk input with real production truth.
- missing/incomplete profitability evidence does not become a safety veto.
- no raw free-text reasoning creates authority.
- scanner score and intelligence score remain separately identified in diagnostics.

### 5. Technical intelligence is not the intended intraday entry engine

The legacy technical agent is a generic multi-indicator ensemble and the historical data source can use daily bars. That is not equivalent to 5m/15m entry efficiency or 4h/1h structural context.

This PR must **not** pretend to solve that by adding random indicators to Gate G.

The richer setup-quality architecture belongs in the subsequent BREACH intelligence PR.

## Required implementation strategy

### A. Add strict input provenance classification

For every Gate G input used in an authoritative decision, preserve:

- field name
- value
- source
- source timestamp if market-derived
- whether it is exact, derived, estimated, defaulted, or unavailable
- whether it may influence authority

Suggested categories:

- `PRODUCTION_EXACT`
- `PRODUCTION_DERIVED`
- `ESTIMATED_ADVISORY`
- `DEFAULT_ADVISORY`
- `UNAVAILABLE`

No `ESTIMATED_ADVISORY`, `DEFAULT_ADVISORY`, or `UNAVAILABLE` field may independently create a hard veto.

### B. Fix numeric falsey parsing

Audit the Gate G pipeline for `x or fallback` patterns where zero is valid.

At minimum:

- DTE
- bid/ask/spread values
- option volume
- open interest
- delta where 0 is semantically possible even if normally invalid for selection
- score components
- P&L/account values

Use explicit `is None`/parse helpers and preserve malformed diagnostics.

### C. Split setup safety from selected-contract quality

Before selector:

- do not hard-veto on fake contract spread;
- do not hard-veto on fake OI/volume;
- do not hard-veto on estimated premium;
- do not size based on estimated contract cost;
- do not emit `CONTRACT_QUALITY_FAILED` unless real contract evidence exists.

After selector, real contract intelligence is handled by the later selected-contract PR.

Gate G may still perform scanner/setup-level admission and proven non-contract safety checks.

### D. Remove legacy account state from hard authority

Implementation must trace every current `APRiskManager` hard-veto category and classify whether its input comes from canonical truth.

Expected categories:

1. **Account risk/capital** -> Master Control canonical authority, not legacy process-local authority.
2. **Sector/correlation exposure** -> advisory unless exact current positions are injected from canonical truth with identity/freshness proof.
3. **Daily loss kill switch** -> must use canonical account/day P&L authority or remain non-authoritative.
4. **VIX/environment market safety** -> may remain authoritative only if fresh source/provenance and policy explicitly require it.
5. **Contract quality** -> unavailable pre-selector.
6. **Regime disagreement** -> advisory, preserving #382 semantics.

Do not create a second DB query subsystem inside intelligence if Master Control already has the data. Prefer passing a read-only canonical snapshot into intelligence, or demoting the duplicated check.

### E. Preserve #379 / #382 safety semantics

Do not regress:

- low confidence -> fail open
- timeout/error/unavailable -> fail open
- malformed/unknown -> fail open
- observe-only -> never blocks
- SPY broad-trend mismatch -> advisory, not hard veto
- true structured hard-risk veto -> may block

The purpose is to improve the truth behind hard authority, not restore broad over-veto behavior.

### F. Diagnostics must identify what actually decided

For every Gate G result, persist structured diagnostics containing:

- `scanner_score`
- `legacy_intel_score`
- `intel_status`
- canonical admission reason code
- hard-authority inputs used
- advisory/defaulted inputs ignored for authority
- `dte_raw` and `dte_resolved`
- whether selected contract evidence existed
- account-state source
- execution mode
- client id
- canonical signal id when available

No operator should need to reverse-engineer free text to know why a setup passed or failed.

## Required tests

Minimum regression matrix:

1. explicit `dte=0` remains 0.
2. `dte="0"` parses to 0.
3. missing DTE is classified fallback/advisory, not silently exact.
4. malformed DTE cannot become 1 and authoritative without diagnostics.
5. pre-selector signal with no OCC contract cannot emit authoritative `CONTRACT_QUALITY_FAILED` from defaults.
6. pre-selector missing spread cannot be replaced by default spread and used as hard authority.
7. pre-selector missing OI/volume cannot become fake passing liquidity.
8. real contract evidence is not accidentally consumed pre-selector from stale metadata belonging to another order.
9. client mismatch on contract evidence -> ignore/quarantine evidence, no authority.
10. PAPER contract metadata cannot authorize LIVE.
11. environment `ACCOUNT_EQUITY=25000` cannot override a different canonical client equity in a hard gate.
12. no canonical account snapshot -> legacy account-risk checks advisory/unavailable.
13. exact canonical hard daily-loss authority still blocks through the canonical risk path, not a fake intelligence copy.
14. regime mismatch remains advisory.
15. `LOW_CONFIDENCE` remains fail-open.
16. timeout/error/unavailable remain fail-open.
17. true structured hard-risk veto with exact input still blocks.
18. malformed structured veto fails according to the existing canonical admission contract, not a free-text parser.
19. scanner-approved observe-only path preserves scanner vs intel scores separately.
20. no test may mock the adjudicator while claiming end-to-end Gate G proof; at least one real-seam test must call `_run_final_quality_gates` / production Gate G path.

## Hidden production-shape tests

Replay signal dictionaries shaped like actual queue/master-control payloads rather than idealized unit dictionaries.

Include:

- numeric zero DTE
- missing nested metadata
- legacy casing for execution mode
- stale selected-contract-like fields from retries
- missing `risk_detail`
- duplicated fields at signal vs metadata level with conflicting values
- null/blank client id
- canonical identity present vs absent

Conflict resolution must be explicit and fail closed where money identity is involved.

## Scope budget

Expected production files:

1. `intelligence_bridge.py`
2. `ap_intelligence/ap_signal_pipeline.py`
3. `ap_intelligence/agents/ap_risk_manager.py` only as needed to remove false hard authority
4. `ap/intelligence_admission_policy.py` only if a stable reason code is required; do not broaden policy semantics casually
5. focused tests + P0 workflow

Conditional:

- `ap_master_control.py` only if the canonical account snapshot must be passed through an existing seam and cannot be sourced otherwise.

No more than five production files without a written dependency explanation.

## Explicit non-goals

Do not change:

- scanner minimum score
- A/B tier thresholds
- scanner generation
- trigger geometry rules
- watcher ownership
- selector DTE ladder
- selector spread/OI/volume thresholds
- broker BUY submit/cancel
- entry retry policy
- position sizing policy
- exit logic
- profit targets/stops
- proof taxonomy
- queue mutation semantics

Do not add 5m/15m/FVG/VWAP admission here. That belongs to the next PR.

## Live-behavior classification

This implementation **can change LIVE admission behavior** because false hard vetoes/approvals may be removed or reclassified.

Therefore acceptance requires explicit before/after decision replay, not only unit tests.

For each replay, report:

- old Gate G decision
- new Gate G decision
- exact field whose authority changed
- whether broker submit would become newly reachable
- why that change is correct using production truth

## Jason safety question

Final review must answer:

> Could this PR cause Jason LIVE to enter a setup solely because missing/estimated contract or portfolio fields were treated as favorable?

Required answer: **NO**, proven by tests.

It must also answer:

> Could this PR incorrectly block Jason because a generic `$25k` equity or empty process-local portfolio disagrees with reality?

Required answer: **NO**, proven by tests.

## Money-path safety

The PR itself must introduce:

- new broker submit functions: **0**
- new broker cancel functions: **0**
- order mutations: **0** beyond existing downstream consequences of an admission verdict
- position mutations: **0**
- proof mutations: **0**
- queue writer changes: **0** unless only structured diagnostics are persisted through an existing path

## Definition of done

Gate G is not considered “smart” after this PR. It is considered **honest**.

It must know what it knows, know what it does not know, preserve real zero values, and never use invented contract/account state as execution authority.

## Release verdict

Current state: **HARD HOLD — docs only.**

Implementation may move to MERGE consideration only after focused production-shape replay, exact-head P0 CI, and independent review.