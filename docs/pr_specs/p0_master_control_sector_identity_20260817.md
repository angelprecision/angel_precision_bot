# P0 — Remove Master Control's fake `other` super-sector

**Status:** SPEC ONLY / HARD HOLD / DO NOT MERGE OR DEPLOY AS CREATED

**Base:** `main@d0d37e79ae698e604eb8080065d2314b161de351`

## Production finding

Jason's LIVE trade-flow audit from 2026-08-06 through 2026-08-17 found 78 post-trigger `breach_risk_check_false` failures. After separating the deferred placeholder-cost defect owned by PR #474, **22 failures were `revalidate_sector_cap_other`**.

Current `ap_master_control.py` owns a small class-local `SECTOR_MAP`. Both sector aggregation and final `revalidate_exposure()` use the same fallback:

```python
sector = self.SECTOR_MAP.get(ticker.upper(), "other")
```

That makes every unmapped ticker economically identical for sector exposure. A QCOM position, an industrial, a healthcare name, a semiconductor, and any other missing symbol can all consume one shared `other` bucket and block one another.

In the recent Jason opportunity population, roughly 150 distinct symbols appeared while only roughly 33 were covered by the Master Control map. The majority therefore fell into one synthetic sector.

This is not conservative sector risk. It is false correlation.

## Existing canonical seam already in the repository

`ap/exposure_gate.py` already provides a broader sector taxonomy and a `get_sector(symbol)` resolver. Its explicit unknown-sector contract is the correct one:

- known ticker -> canonical sector string;
- unknown ticker -> `None`;
- unknown sector does **not** become a shared sector bucket;
- same-symbol and total-exposure protections remain available independently.

That module also includes index classification for SPY/QQQ/IWM/DIA.

This PR must reuse one canonical resolver. Do **not** add a third sector map, external API lookup, network dependency, AI classification, or database lookup inside the money path.

## Scope ownership

This PR owns only **sector identity resolution and sector-cap application inside Master Control**.

It does not own:

- PR #474 deferred actual-cost ordering;
- max positions;
- total capital cap;
- per-position cap;
- ticker duplicate/correlation policy outside sector resolution;
- selector quality;
- scanner ranking;
- entry timing;
- position sizing percentages;
- broker submit/cancel;
- exits.

## Required invariant

```text
known sector -> enforce the existing sector cap against known same-sector exposure
unknown sector -> do not aggregate with unrelated unknown symbols
unknown sector -> preserve total account / same-symbol / position / broker / selector protections
```

An unknown sector may reduce one layer of correlation protection for that specific candidate, but it may never create fake information by grouping unrelated securities together.

## Exact current-main call seams to rewrite

Claude/Codex must inspect all current-main references to `SECTOR_MAP` before editing and return the list. At minimum, the audit has proven these authoritative uses in `ap_master_control.py`:

1. `get_sector_exposure(positions)` / equivalent sector aggregation helper:

```python
sector = self.SECTOR_MAP.get(ticker.upper(), "other")
```

2. `revalidate_exposure(...)` final capital check:

```python
sector = self.SECTOR_MAP.get(ticker.upper(), "other")
sector_capital = self._sector_capital_deployed(..., sector)
...
return block(... f"revalidate_sector_cap_{sector}")
```

3. Any helper used by `evaluate()` or sizing context that reads the class-local map for active-money authority.

Search the whole repository for:

- `SECTOR_MAP`
- `.get(ticker.upper(), "other")`
- `sector_cap_`
- `_sector_capital_deployed`
- `get_sector_exposure`

Do not assume the two known call sites are the whole surface.

## Implementation contract

### 1. One canonical resolver

Preferred current-main shape:

```python
from ap.exposure_gate import get_sector as resolve_sector
```

or a module import if avoiding symbol shadowing is clearer.

Master Control must not maintain an independent authoritative class-local map after this PR. If the old constant must remain temporarily for non-authoritative diagnostics/backward compatibility, it cannot control an allow/block decision and a follow-up removal note must be explicit.

### 2. Normalize the candidate once

At the Master Control risk boundary, normalize symbol using existing ticker normalization semantics. Resolve sector exactly once per candidate evaluation/revalidation when practical.

Return values:

- recognized nonblank sector -> authoritative sector identity;
- `None`/blank -> `sector_unknown` state.

Do not silently coerce malformed values to `other`, `technology`, `index`, or any environment default.

### 3. Known sector behavior remains numerically identical

For a known candidate sector, preserve the current sector-cap math exactly:

```text
known-sector deployed capital
+ known-sector pending/approved exposure included by current policy
+ candidate actual/requested cost under the current stage
<= equity * max_sector_pct
```

This PR may change identity input, not percentages, thresholds, rounding, or ordering of unrelated gates.

If current Master Control's sector calculation includes/excludes statuses differently from `ap/exposure_gate.check_exposure()`, do not replace the whole risk engine merely to reuse `get_sector`. Reuse only the resolver unless a failing production-shaped test proves the current accounting itself is wrong.

### 4. Unknown sector is not a shared bucket

When `resolve_sector(candidate_ticker)` is unknown:

- do not call `_sector_capital_deployed(..., "other")`;
- do not calculate `projected_sector_exposure` against a synthetic `other` bucket;
- do not return `revalidate_sector_cap_other`;
- continue all independent account-level, same-symbol, affordability, slot, kill-switch, selector and final submit gates;
- attach diagnostics:
  - `sector=None` or explicit null;
  - `sector_resolution="unknown"`;
  - `sector_cap_applied=false`;
  - `sector_cap_skip_reason="unknown_sector_identity"`.

Unknown must stay unknown. Missing information is not permission to invent correlation.

### 5. Existing positions also require canonical resolution

When calculating exposure for a known candidate sector, resolve each relevant existing position ticker through the same canonical resolver.

- position resolves to candidate's known sector -> count it;
- position resolves to another known sector -> do not count it;
- position sector unknown -> do not count it toward the candidate's known sector;
- preserve it in diagnostics as unknown exposure rather than silently reclassifying it.

Do not mutate historical `positions` rows to backfill sectors in this PR.

### 6. Indices

The canonical resolver currently knows SPY, QQQ, IWM and DIA as index exposure. Preserve that behavior.

This PR does not decide whether the configured index sector cap percentage is strategically correct. It only prevents index instruments from falling into `other` and gives them stable identity.

### 7. Diagnostics must identify the true reason

Replace false reasons like:

```text
revalidate_sector_cap_other
```

with either:

- `revalidate_sector_cap_<known-sector>` when a real mapped sector exceeded cap; or
- no sector block plus `sector_unknown` diagnostics when sector identity is unavailable.

Keep actual capital figures in diagnostics so future audit can reconstruct:

- equity;
- max_sector_pct;
- candidate sector;
- current known-sector exposure;
- candidate cost;
- projected sector exposure;
- whether sector gate applied;
- resolver source/version if a current version field exists.

### 8. No fail-open on the other risk gates

The following are explicitly frozen:

- `max_position_pct` / actual-cost cap;
- total capital exposure cap;
- `max_positions`;
- daily loss/kill switch;
- account equity authority;
- exact client and execution mode;
- contract affordability;
- selector quality;
- broker submit preflight.

A test must prove an unknown-sector candidate is still blocked when total-cap or per-position authority says no.

## Expected production file budget

Preferred production scope:

1. `ap_master_control.py`

`ap/exposure_gate.py` should be **read/reused, not edited**, unless a current-main reproduction proves its `get_sector` resolver itself lacks a required symbol/normalization invariant. If a second production file appears necessary, STOP and report before editing it.

Tests:

- new `tests/test_p0_master_control_sector_identity.py`
- register in `.github/workflows/p0_regression.yml`

No migration.

## Fail-first production-shaped cases

Before touching production code, create tests that fail on exact current main:

1. candidate QCOM plus unrelated unknown-sector exposure currently collapses to `other` and can block as `revalidate_sector_cap_other`.
2. candidate DHR plus unrelated PM/PCAR/DDOG style rows can contribute to the same fake bucket.
3. two genuinely known technology tickers still share a sector and can correctly exceed cap.
4. SPY and QQQ resolve to `index` under the canonical resolver.
5. totally unknown ticker returns no canonical sector instead of `other`.

The first two are the regression; the others prevent "fixing" it by disabling sector risk entirely.

## Minimum acceptance suite

1. known tech + known tech over cap -> block with known sector reason.
2. known financial + known financial under cap -> pass sector gate.
3. QCOM uses canonical technology mapping from `ap.exposure_gate` rather than Master Control's old missing entry.
4. ARM/SMCI/NOW and other symbols covered by canonical resolver get their canonical sector.
5. unknown candidate + unrelated unknown position -> no shared-sector block.
6. unknown candidate + total account cap exceeded -> still blocked by total-cap authority.
7. unknown candidate + per-position actual cost exceeded -> still blocked.
8. unknown existing position cannot pollute a known candidate's sector exposure.
9. SPY + QQQ aggregate under index if the existing sector cap applies to indices.
10. IWM + unrelated tech do not share a sector.
11. symbol case/whitespace normalization does not create duplicate sectors.
12. blank/malformed ticker -> fail closed under existing input-validity rules; never invent sector.
13. exact client identity preserved.
14. exact `execution_mode` preserved.
15. PAPER and LIVE use identical sector taxonomy, with no cross-account exposure query pollution.
16. no broker submit/cancel method added or moved.
17. no order mutation introduced by sector resolver.
18. no position mutation.
19. no proof-trade mutation.
20. no queue mutation.
21. no threshold/config changes.
22. AST/source assertion: Master Control may not authoritatively call `.get(..., "other")` for sector risk.
23. AST/source assertion: no new `SECTOR_MAP` definition introduced in Master Control.
24. adjacent `ap/exposure_gate.py` tests remain green.
25. #474 deferred-risk tests remain green after rebase because both affect final risk evaluation ordering.

## Import/cycle audit required

Before committing, prove importing `ap_master_control` -> `ap.exposure_gate` does not introduce a circular import. Read `ap/exposure_gate.py` imports and every module-level import it executes.

If a cycle exists, do **not** copy the map back into Master Control. Extract the resolver into the smallest dependency-neutral module and update both consumers in a separately documented file-budget expansion. That expansion must be justified with the exact cycle chain.

## Money-path declaration

- **Changes LIVE behavior:** YES. Valid LIVE entries previously falsely blocked by fake `other` correlation may now proceed to later gates.
- **Flag-off:** NO. This is risk-truth correction, not an optional feature.
- **Broker submit/cancel:** no new broker methods; downstream broker submit may become reachable for candidates no longer falsely blocked.
- **Orders:** no new mutation at this seam.
- **Positions:** read only for exposure.
- **proof_trades:** no mutation.
- **queue:** no mutation.
- **client_id / execution_mode:** exact preservation mandatory.
- **Production metadata shape:** use real position/order/master-control shapes; no toy `sector` field assumed to exist unless current schema proves it.
- **Diagnostics:** preserve and improve.
- **PAPER/LIVE taxonomy:** one resolver, separate account/mode exposure scopes.
- **Could make Jason trade junk:** only if implemented by disabling sector risk. That is forbidden. Known-sector limits and every other gate remain unchanged.

## Claude/Codex implementation order

1. Rebase to exact then-current `main` and record SHA.
2. Read this full spec.
3. Search every Master Control sector reference.
4. Read `ap/exposure_gate.py` and prove import-cycle safety.
5. Write fail-first tests reproducing `revalidate_sector_cap_other` on unrelated symbols.
6. Run them on unpatched main and record failures.
7. Change only the canonical resolver seam and unknown-sector branch.
8. Run focused tests.
9. Run Master Control/exposure/position-cap adjacent suites.
10. Run #474 adjacent tests.
11. Inspect complete base-to-head diff.
12. Prove no thresholds changed.
13. Run exact-head P0 GitHub CI.
14. Return exact head SHA, files, test counts, CI run IDs, broker/mutation audit and final MERGE/HOLD/HARD HOLD.

No merge, deploy, config change, or production data mutation is authorized.

**Current verdict: HARD HOLD until implemented and independently money-path reviewed.**
