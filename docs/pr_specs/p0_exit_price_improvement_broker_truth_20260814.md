# P0 — Exit price improvement before bid surrender + manual-force-exit broker truth

**Status:** DRAFT / HARD HOLD / implementation contract only. Do not merge or deploy this spec by itself.

**Base proven:** `main@ef4a4d232c0fadfbbca7592711477705b706bb4d` on 2026-08-14.

## User-visible failure we are fixing

Angel Precision is too willing to sell at the low end of an option spread even when the position is still showing a materially better current market value.

Example:

```text
option bid      1.54
option ask      1.58
current/mark    1.55
```

For a non-emergency close, the first sell attempt should be approximately **1.55**, not an immediate surrender to **1.54**, and never **1.58** merely because that is the ask.

The more damaging production shape is a wide spread where a position appears slightly green or near flat but immediate bid-side liquidation manufactures a large realized loss.

Example class:

```text
entry / economic value near 1.00
current market reference near 1.02
fresh bid materially lower
fresh ask materially higher

bad behavior:
  exit decision says roughly +2%
  sell immediately at low bid
  realized fill becomes roughly -12%
```

This PR must make a **bounded attempt to realize the current executable market value before stepping down toward the bid**. It must not pretend midpoint/mark is guaranteed executable, but it also must not donate the whole spread on the first non-emergency sell.

## Confirmed current-main code path

### `app.py` dashboard force exit

`POST /admin/position/force_exit/<position_id>` creates:

```python
ExitDecision(
    action="CLOSE_ALL",
    quantity=qty,
    reason=f"ADMIN FORCE EXIT — {reason}",
    urgency="IMMEDIATE",
    reason_code="ADMIN_FORCE_EXIT",
)
```

It supplies no `suggested_limit`.

When the position is present in `APExitEngine`, the callback chain reaches `APExecutionCore._on_position_close()`.

When the position is **not** present in the exit engine, current main contains an unsafe fallback that calls `APPositionManager.close_position_from_exit_fill(...)` at the **entry price** without submitting or confirming a broker sell. That can mark local state closed while LIVE broker exposure remains open. This fallback must be removed from authoritative close behavior.

The same dashboard route also contains direct proof logging after submit acceptance using `current_bid/current_option_price`; submit acceptance is not broker fill truth. Normal production exit proof already waits for broker-confirmed fill and this endpoint must converge on that authority.

### `ap_execution_core.py` exit pricing

`APExecutionCore._on_position_close()` currently treats `decision.suggested_limit > 0` as first pricing authority. Otherwise, the first fallback limit is the current BID.

Current logic therefore makes an `ADMIN_FORCE_EXIT` with no suggested price start at BID.

There is also a misleading comment stating that an option market order fills at ASK. For a `sell_to_close`, marketable sell flow consumes **bid-side** liquidity. Correct the comment while touching this seam so a future change does not "fix" correct sell-side behavior to match incorrect prose.

### `ap_exit_engine.py`

Current exit decision truth correctly distinguishes executable BID from display/midpoint truth. Preserve that distinction. This PR changes **order pricing strategy after an exit has already been authorized**; it must not use midpoint to fabricate whether the exit should occur.

Decision authority and execution-price improvement are separate concerns:

```text
Should we exit?              executable/risk truth
At what limit do we try?     bounded price-improvement policy
What did we actually earn?   broker-confirmed fill truth
```

Do not collapse those three layers.

## Core invariant

For a **non-emergency SELL TO CLOSE**, Angel Precision should try to realize the current quoted market value before surrendering to the bid.

The first limit must be derived from fresh same-cycle BID/ASK/current-market truth and must satisfy:

```text
bid <= first_sell_limit <= ask
```

The first limit must never be derived from ASK alone and must never be below BID.

If a trustworthy current/mark value is inside the fresh spread, prefer that value. Otherwise use a fresh quote-derived midpoint/current-market reference.

Example:

```text
bid=1.54 ask=1.58 current=1.55
=> first SELL limit = 1.55
```

If the spread is already one valid price increment wide, there is no meaningful improvement room; BID is acceptable.

## Required pricing policy

Implement one canonical sell-price-improvement helper. Do not duplicate arithmetic in dashboard, exit engine, execution core, and OSM.

Suggested API shape is conceptual, not binding:

```python
resolve_sell_exit_price(
    *,
    bid,
    ask,
    current_market_price,
    quote_fresh,
    reason_code,
    urgency,
    attempt,
    age_seconds,
    tick_size,
) -> ExitPriceDecision
```

The result must expose enough diagnostics to prove:

- bid
- ask
- current/mark candidate
- chosen limit
- quote timestamp/freshness
- spread dollars
- spread percent
- pricing stage
- reason for fallback
- emergency vs non-emergency classification

### Stage 1 — price improvement

For manual/operator exits, profit exits, scale-outs, runner/trailing exits, soft exits, and other non-emergency closes:

1. Require a fresh positive BID.
2. Use fresh ASK when available to establish the spread.
3. Choose the best trustworthy current market reference inside `[bid, ask]`.
4. If the current market value is absent, malformed, stale, or outside the spread, use a midpoint calculated from the same fresh BID/ASK snapshot.
5. Normalize to a broker-valid option price increment using the repo's existing tick/price normalization authority if one exists. Do not invent a second incompatible MPV table.
6. If there is at least one valid increment of improvement above BID, first submit above BID, as close as practical to current market value.

Acceptance example:

```text
bid=1.54 ask=1.58 current=1.55
first limit=1.55
```

A first non-emergency submit at 1.54 for that shape is a regression.

### Stage 2 — bounded step-down

Price improvement must not become "sit forever at midpoint".

Reuse the **existing single owner** of exit cancel/replace/reprice behavior. Do not create another scheduler, retry loop, or direct broker submitter.

A non-emergency exit must get at least one real price-improvement attempt before reaching BID when the spread provides room.

Then step down monotonically toward BID based on the existing exit-order age/attempt lifecycle. The exact timing must be compatible with the current production monitor cadence and must be tested against the actual owner of replacement orders.

Required monotonic invariant:

```text
ask >= attempt_1 >= attempt_2 >= ... >= bid
```

No retry may reprice upward after a lower sell limit was already submitted unless a **newer fresh quote** proves the market itself moved upward and the existing order can be safely replaced under one broker owner.

For a narrow example:

```text
1.54 / 1.58, current 1.55
attempt 1 -> 1.55
later bounded fallback -> 1.54
```

For a wider spread, do not jump from midpoint/current directly to a deeply lower BID without a bounded intermediate attempt if the existing replace cadence can safely support it.

### True emergencies remain separate

The following risk exits may retain aggressive existing behavior because fill certainty outranks spread capture:

- hard disaster/hard stop where immediate flattening is required;
- EOD forced close near the hard session cutoff;
- sentinel / kill-switch / explicit emergency flatten.

Do not silently classify `ADMIN_FORCE_EXIT` as a true emergency merely because its urgency is `IMMEDIATE`.

Manual/operator intent means "close this now," not "voluntarily cross the entire spread on the first packet regardless of price".

If the operator explicitly requests an emergency/market flatten through an existing dedicated emergency path, preserve that separate behavior and audit it distinctly.

## Dashboard manual force-exit correction

`/admin/position/force_exit/<position_id>` must converge on the canonical exit lifecycle.

### When exact engine ownership exists

- preserve exact `client_id`;
- preserve exact `execution_mode`;
- preserve exact `position_id`;
- preserve exact OCC contract;
- preserve exact remaining quantity;
- create/submit one canonical EXIT lifecycle through the existing exit engine -> execution core -> OSM path;
- use the new price-improvement policy for the initial non-emergency sell limit;
- final position/P&L/proof truth comes only from broker-confirmed fill.

### When the position is not owned by the exit engine

**Delete the current authoritative fallback that closes the DB position at entry price.**

Do not replace it with another direct DB close or direct Tradier POST.

Fail closed with an operator-visible stable result such as:

```text
MANUAL_FORCE_EXIT_OWNER_UNPROVEN
```

and route recovery through already-existing canonical ownership/manual-close machinery.

If broker exposure is still open, canonical position ownership must be restored before submit.

If broker exposure is already flat because the user closed externally, reuse the existing manual-close reconciliation / exact broker EXIT-fill machinery. Do not duplicate PR #386 or PR #460.

No local `CLOSED` mutation is allowed merely because the in-memory owner is missing.

## Proof truth

The dashboard force-exit path must not write final `proof_trades` economics from:

- submitted limit;
- current BID;
- midpoint;
- mark;
- entry price fallback.

Submitting an exit is not filling an exit.

Final proof must consume the same broker-confirmed fill truth used to finalize the canonical position, through the existing normal fill-finalization path.

Do not create a second proof writer.

## Existing PR boundaries / no duplicate architecture

This work is adjacent to, but must not duplicate:

- **#386** manual client/external closes from broker fill truth;
- **#460** reconciler broker-truth requirement / removal of fabricated close prices;
- **#428** exact broker EXIT fill/proof truth work;
- current OSM EXIT ownership/adoption/recovery machinery.

This PR owns **sell-price improvement on bot-submitted exits** and removal of the unsafe dashboard force-exit local-close shortcut.

If implementation discovers one of the adjacent PRs already fixes the dashboard fallback on then-current main, preserve that implementation and scope this PR strictly to price improvement plus missing regression coverage.

## Required production path audit before editing

Codex must fresh-fetch current main and trace, in order:

1. `app.py::admin_force_exit_position`
2. `APExitEngine._submit_exit_decision`
3. `APExecutionCore._on_position_close`
4. `APOrderStateMachine.create_exit_order / submit_exit`
5. current exit order monitor / cancel-replace / retry owner
6. Tradier payload (`sell_to_close`, limit/market type, price)
7. fill monitor / OSM EXIT_FILLED transition
8. `APPositionManager.close_position_from_exit_fill`
9. proof finalization
10. restart/reconciliation ownership for an in-flight EXIT

Do not implement a pricing ladder until the exact current owner of cancel/replace is proven.

## Required focused regressions

Create a focused P0 module, e.g.:

`tests/test_p0_exit_price_improvement_and_manual_force_truth.py`

Minimum cases:

1. **Canonical example**: BID 1.54 / ASK 1.58 / current 1.55 -> first non-emergency SELL limit exactly 1.55 (subject only to existing valid-tick normalization), never 1.54 and never 1.58.
2. Current/mark outside spread -> ignore it; use same-cycle quote-derived reference.
3. Stale current/mark -> ignore it.
4. Stale/missing ASK with fresh BID -> fail to a documented conservative policy; never invent an above-bid midpoint from stale data.
5. One-tick spread -> BID is allowed because no improvement increment exists.
6. Wide spread -> first attempt is above BID and bounded inside spread.
7. Step-down is monotonic and reaches BID only after the price-improvement stage for non-emergency exits.
8. Newer quote moves upward -> repricing upward is allowed only with proven newer quote and one existing broker order owner; no duplicate submit.
9. ADMIN_FORCE_EXIT is non-emergency price-improvement behavior by default.
10. Explicit hard-stop/EOD/sentinel emergency preserves current aggressive behavior.
11. Manual force exit with engine owner uses exact client/mode/position/contract/qty and one OSM EXIT.
12. Manual force exit with no engine owner -> `MANUAL_FORCE_EXIT_OWNER_UNPROVEN`, zero position-close mutation, zero proof mutation, zero broker submit from the fallback.
13. Manual force exit submit accepted but not filled -> position remains open/closing and proof remains unfinalized.
14. Manual force exit broker fill at price different from submitted limit -> canonical position and proof use actual fill, not submitted limit/current mark.
15. Partial fill -> only actual filled quantity is accounted; remaining exposure stays owned.
16. Rejected/unfilled price-improvement order -> no fake close/proof.
17. Exact LIVE/PAPER isolation.
18. Wrong client id, wrong mode, wrong contract, wrong position id -> block with zero broker call.
19. Restart with an in-flight improved-price EXIT -> adopt existing broker owner, never issue a duplicate sell.
20. Trap every broker submit/cancel entry point and prove exactly one submit on successful first handoff and no duplicate submit during retries/recovery.

## Incident-shape regression

Add an explicit economics regression for the class Angel described:

```text
entry_price = 1.00
current displayed/reference = 1.02
fresh spread is wide enough that immediate bid liquidation would realize about -12%
```

The test must prove the bot does **not** make the low BID the first non-emergency sell limit merely because BID is executable.

This does not guarantee a +2% fill. It guarantees the bot attempts price improvement first and records only the broker's actual eventual fill.

## Diagnostics

Every exit submit should preserve downstream diagnostics for forensic review:

```text
exit_price_policy_version
exit_price_stage
exit_price_reference_source
exit_price_quote_ts
exit_price_bid
exit_price_ask
exit_price_current_market
exit_price_initial_limit
exit_price_submit_limit
exit_price_attempt
exit_price_order_age_sec
exit_price_spread_abs
exit_price_spread_pct
exit_price_emergency_class
```

Store these in the existing EXIT order metadata / decision diagnostics authority. Do not create a new table for this PR.

Diagnostics must survive submit -> ack -> fill -> proof/journal surfaces where those surfaces already carry EXIT metadata. Do not overwrite existing diagnostics wholesale.

## Money-path and state-mutation gate

This PR changes LIVE behavior: **YES**, EXIT order pricing.

Expected broker impact:

- no new ENTRY submits;
- no new independent EXIT submitter;
- no new independent cancel owner;
- existing EXIT submits receive a different first limit for non-emergency sells;
- existing replacement owner may submit cancel/replace at bounded lower limits according to one canonical policy.

Expected database impact:

- `orders`: existing EXIT lifecycle + pricing diagnostics only;
- `positions`: only existing canonical fill finalizer may mutate realized close economics;
- `proof_trades`: only existing broker-fill finalizer may write final economics;
- `trade_queue`: **NO change**;
- ENTRY orders: **NO change**;
- client identity: preserved exactly;
- execution mode: preserved exactly;
- PAPER/LIVE taxonomy: no cross-mode fallback.

## Strict non-scope

Do not change:

- scanners;
- entry eligibility;
- master-control score gates;
- contract selection;
- position sizing;
- entry trigger timing;
- FVG/VI strategy;
- profit thresholds;
- stop thresholds;
- runner activation thresholds;
- trade queue lifecycle;
- intelligence admission;
- account allocation.

Do not use this PR to make losing exits artificially look better in telemetry. The actual broker fill remains economic truth even when price improvement fails.

## Suggested production file budget

Expected maximum production scope after current-head audit:

1. one canonical exit-pricing helper module (new or existing authority);
2. `ap_execution_core.py` only as necessary to consume the helper / preserve one OSM submit path;
3. `ap_exit_engine.py` only if it currently owns the reprice attempt lifecycle or needs to emit the pricing intent;
4. `app.py` only for dashboard force-exit convergence and removal of the fake local-close fallback;
5. `ap/order_state_machine.py` only if the existing canonical replacement owner needs a surgical pricing-metadata or exact-price handoff change.

If this can be done with fewer production files, use fewer.

Do not create a package-shadowing architecture or another exit engine.

## Validation gate

Before MERGE consideration:

1. read PR description;
2. read actual cumulative diff;
3. read all review comments;
4. list changed files;
5. trace exact current production submit/cancel/reprice path;
6. verify no duplicate broker owner exists;
7. verify exact client/mode/position/contract/qty identity;
8. replay 1.54/1.58/current-1.55;
9. replay wide-spread +2%-display / negative-BID incident class;
10. replay manual force exit with and without engine ownership;
11. prove broker-confirmed fill, not submitted limit, finalizes position/proof;
12. run focused tests;
13. run all adjacent EXIT/OSM/fill/proof P0 suites;
14. run exact-head GitHub P0 CI;
15. inspect changed-file money-path reachability;
16. issue independent MERGE / HOLD / HARD HOLD.

## Release verdict

**HARD HOLD until implemented, current-main audited, regression-tested, and exact-head CI passes.**

No merge or deployment is authorized by this spec.