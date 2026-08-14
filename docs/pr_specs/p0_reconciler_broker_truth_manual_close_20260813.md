# P0 SPEC: Remove fabricated reconciler close prices and use exact broker EXIT fill truth

## Status

**DRAFT / HARD HOLD / SPEC ONLY.**

Do not merge, deploy, mark Ready, or enable any new LIVE behavior from this spec commit.

Base for this spec: `main@b171e3aca9f62627e68ce206b2bb8138076b0711`.

Branch: `spec/p0-reconciler-broker-truth-manual-close-20260813`.

This is a surgical repair instruction for the current manual/operator/external-close reconciliation regression. It is intentionally smaller than PR #428. Do not copy or mechanically rebase #428's historical implementation. Preserve only the invariant that final EXIT economics must come from exact broker fill truth.

## Incident

A LIVE position was closed from the operator side. The broker position disappeared, but the bot did not preserve one canonical exact EXIT fill through the local lifecycle. Production state then showed conflicting close economics across `positions` and `proof_trades`, while no durable exact local EXIT order identity proved the price used for finalization.

The important production defect is not a strategy decision. It is accounting authority.

Current `ap_reconciler.py` can finalize a broker-missing position without exact broker EXIT fill evidence. When `_get_recent_exit_fill()` cannot find a local filled EXIT order, `_handle_db_position_missing_at_broker()` waits for the three-pass ghost confirmation and then does one of two unsafe things:

1. calls `_get_current_option_price(contract)` and uses the current option mark as `exit_px`; or
2. if no current quote exists, uses `entry_px` and records a zero-P&L close.

That synthetic `exit_px` is then passed into `_execute_reconciler_close()`, which writes `positions.exit_price`, realized P&L, quantity state, close metadata, and can write a proof row from that synthetic value.

This violates the repository's already-established broker-truth rule:

`broker fill -> durable EXIT order truth -> position finalization -> proof/dashboard`

A missing broker position proves exposure is gone. It does **not** prove the fill price.

## Existing correct machinery already in this repository

Do not invent a new broker-history parser.

`ap/manual_close_reconciliation.py` already contains the exact machinery needed for externally/manual closed positions. It is explicitly read-only with respect to the broker. It never submits or cancels orders.

Existing helpers already provide:

- authoritative broker position fetch;
- paginated current-session broker order fetch through `/v1/accounts/{account_id}/orders?includeTags=true&page=...&limit=...`;
- fallback to `list_orders()` with an explicit warning when pagination is unavailable;
- OCC contract normalization;
- `sell_to_close` side recognition;
- filled/partially-filled order recognition;
- extraction of broker order id;
- extraction of executed quantity;
- extraction of broker fill price;
- extraction of broker fill/event timestamp;
- rejection of malformed or incomplete fill evidence;
- exact contract matching;
- entry-time lower bound;
- bot-owned EXIT order exclusion;
- already-adopted external fill handling;
- aggregate quantity equality checks;
- weighted fill calculation for multi-fill closes;
- broker-order-id list preservation.

The canonical returned evidence shape from `select_external_close_fills()` already includes:

- `filled_qty`
- `fill_price`
- `filled_ts`
- `broker_order_id`
- `broker_order_ids`
- the underlying per-fill evidence

Reuse this. Do not duplicate the parser in `ap_reconciler.py`.

## Historical architecture boundary

Commit `f6d5bfaa4d4bdf40bfbf637927c28d466c799248` introduced `APPositionManager.close_position_from_exit_fill()` as the canonical broker-truth position finalizer. Its contract is the correct one: it uses confirmed exit fill data only and must never use quote/mid/mark/chart/estimate as realized exit truth.

The current regression exists because `ap_reconciler.py` still contains an older ghost-close fallback that can synthesize an exit price and independently finalize the position.

Do **not** restore an old `ap_reconciler.py`. The unsafe mark/entry fallback predates the current incident and exists in historical versions. This repair is removal of stale authority, not restoration of an old file.

# Required invariant

For any position that is OPEN locally but absent at the broker:

1. Broker-flat truth may prove that the position no longer exists at the broker.
2. Exact realized exit economics require exact broker EXIT fill evidence.
3. No current quote, mark, bid, ask, midpoint, last, entry price, theoretical price, stale cached price, or underlying price may become realized EXIT fill truth.
4. If exact broker fill evidence cannot be established, the reconciler must HOLD/alert and leave realized economics unresolved.
5. The reconciler may never fabricate a proof row from unresolved close economics.
6. LIVE/PAPER identity must remain exact and isolated.
7. This repair introduces zero broker submits and zero broker cancels.

# Expected production scope

The intended production change is **`ap_reconciler.py` only**.

Reuse `ap/manual_close_reconciliation.py` as an existing library. Modify it only if Codex proves a concrete incompatibility that makes safe reuse impossible. If any change to that file is proposed, document the exact incompatibility first and keep the change minimal.

## Proven incompatibility amendment

The exact-path audit proved three reuse blockers in the current helper/finalizer seam: broker quantities/prices were coerced before strict validation; fill chronology could fall back to generic update timestamps or accept naive values; and the finalizer did not recheck external ownership or exact remaining quantity under its row lock. The amendment therefore keeps the existing architecture but adds only the corresponding fail-closed scalar, timestamp-provenance, durable-metadata, and locked-CAS guardrails in `ap/manual_close_reconciliation.py` and `ap/position_manager.py`.

Expected test scope: one new focused regression module, plus minimal updates to existing reconciler/manual-close tests if required.

Do not touch unless a directly proven compile/test requirement makes it unavoidable:

- `ap/fill_monitor.py`
- `ap_exit_engine.py`
- `ap_execution_core.py`
- selector files
- watcher files
- queue files
- scanner files
- risk/sizing files
- intelligence files
- schema or migrations
- authorization
- account configuration
- trade policy
- TP/SL thresholds

No opportunistic refactor.

# Codex implementation instructions

Follow these steps in order. Do not skip directly to editing.

## Step 1: Rebase mentally on current main and re-prove the defect

Before writing code:

1. Fetch current `main` and record its SHA.
2. Confirm this branch still descends from `b171e3aca9f62627e68ce206b2bb8138076b0711` or rebase onto current main if main has moved.
3. Read the current implementations of:
   - `APBrokerReconciler._get_recent_exit_fill`
   - `APBrokerReconciler._handle_db_position_missing_at_broker`
   - `APBrokerReconciler._execute_reconciler_close`
   - `APBrokerReconciler._reconcile_positions`
   - `APPositionManager.close_position_from_exit_fill`
   - the relevant exports in `ap/manual_close_reconciliation.py`
4. Confirm the current unsafe fallback still exists before modifying anything.
5. Record the exact call path in the PR body or implementation commit message.

Expected current unsafe path:

```text
DB position OPEN/CLOSING
        -> broker position absent
        -> _get_recent_exit_fill() checks local orders table only
        -> no local exact EXIT fill found
        -> no active local/broker EXIT
        -> three-pass ghost confirm
        -> _get_current_option_price(contract)
        -> synthetic exit_px from current mark
           OR entry_px if quote unavailable
        -> _execute_reconciler_close()
        -> positions/P&L/proof finalized from synthetic value
```

If the path has materially changed on current main, stop and amend this spec with evidence before broadening scope.

## Step 2: Do not use contract-only local order lookup as final authority

Current `_get_recent_exit_fill(contract, underlying)` is not sufficient broker truth because it searches local `orders` by client + EXIT + filled status + contract/symbol and returns only price/qty/time.

It does not prove current position identity, local EXIT identity, broker EXIT identity, or that the close belongs to this exact economic position.

For this PR:

- it may remain as an early durable-evidence check only if the evidence is upgraded to exact position-bound identity and validated safely;
- otherwise bypass it for manual/external-close resolution and use the existing manual-close reconciliation helper which already proves broker evidence against the position.

Do not make a loose contract-only lookup more authoritative.

## Step 3: Add a read-only broker-truth resolver at the reconciler seam

Implement the smallest adapter/helper in `ap_reconciler.py` that reuses `ap.manual_close_reconciliation`.

Preferred responsibility:

```python
_resolve_exact_external_exit_fill(
    *,
    position: dict,
    contract: str,
    detected_at: datetime,
) -> dict | None
```

The exact function name is not mandatory. The behavior is.

The helper should:

1. preserve exact `client_id` and canonical `execution_mode` of this reconciler;
2. use the existing broker-order fetch/helper from `ap/manual_close_reconciliation.py` rather than writing another Tradier parser;
3. load/derive any bot-owned EXIT IDs needed to prevent an external-close path from stealing a bot-owned EXIT lifecycle;
4. call the existing exact external-fill selector against the **current position row**, not merely ticker/contract;
5. require exact OCC contract match;
6. require `sell_to_close` semantics;
7. require real executed quantity > 0;
8. require finite positive broker fill price;
9. require an explicit timezone-aware broker fill timestamp;
10. require aggregate broker fill quantity to equal the position's current remaining quantity for a full close;
11. preserve all matching broker order IDs for diagnostics;
12. return a normalized exact-fill structure only after the existing selector says the evidence is exact.

If the broker-order fetch fails, payload is malformed, evidence is ambiguous, quantity does not match, execution mode is unproven, or no exact fill exists, return an explicit unresolved disposition and do not mutate the position.

Do not silently catch the failure and continue to the current-mark fallback.

## Step 4: Delete realized-P&L authority from the three-pass current-mark fallback

Inside `_handle_db_position_missing_at_broker()`:

The following behavior must disappear as realized close authority:

```python
_current_px = self._get_current_option_price(contract)
if _current_px > 0:
    exit_px = _current_px
else:
    exit_px = entry_px
```

After this PR, three-pass ghost confirmation may still be used as **exposure/discrepancy evidence**, but it must not convert a quote or entry price into a broker fill.

Required behavior after three-pass confirmation:

```text
broker position absent after required confirmation
        -> resolve exact external broker EXIT fill
        -> exact evidence found: finalize from that evidence
        -> exact evidence unavailable/ambiguous: HOLD + alert + diagnostics
```

Required HOLD result:

- position status/economics remain unchanged by this path;
- no `exit_price` write;
- no realized P&L write;
- no realized P&L percentage write;
- no quantity_remaining mutation from this unresolved path;
- no terminal status mutation from this unresolved path;
- no proof insert/update;
- no broker submit;
- no broker cancel;
- clear operator-visible reason code.

Suggested stable reason family:

- `RECONCILER_BROKER_FLAT_EXIT_FILL_UNRESOLVED`
- `RECONCILER_EXTERNAL_EXIT_FILL_AMBIGUOUS`
- `RECONCILER_EXTERNAL_EXIT_FILL_FETCH_FAILED`

Use existing repository diagnostic conventions where practical. Do not invent ten new states if three are enough.

## Step 5: Finalize through the canonical position manager

Once exact broker fill evidence is proven, do not independently reproduce realized-P&L math in `_execute_reconciler_close()` for this path.

Preferred flow:

```text
exact broker fill evidence
        -> APPositionManager.close_position_from_exit_fill(
             position_id=<exact position>,
             exit_price=<weighted broker fill>,
             filled_qty=<exact broker filled qty>,
             filled_ts=<broker fill timestamp>,
             broker_order_id=<exact broker order id or documented aggregate authority>,
             close_source=<manual/external broker close>,
             close_confidence="HIGH",
             exit_reason=<exact reason>
           )
```

If multiple broker fills collectively closed the position, use the existing weighted aggregate computed by `select_external_close_fills()` and preserve the complete broker ID list in diagnostics. Do not choose an arbitrary last fill price.

If current `close_position_from_exit_fill()` cannot safely represent a multi-fill aggregate without losing identity, do **not** broaden this PR into a position-manager redesign. Use the existing manual-close reconciliation adoption/finalization flow that already handles weighted aggregates. The important rule is to reuse the repository's canonical existing path rather than create parallel accounting.

## Step 6: Durable EXIT identity

If the existing manual-close reconciliation path adopts the external broker fills into durable `orders` as `EXIT_FILLED` rows, reuse that adoption mechanism instead of creating another durable row format.

Desired final chain:

```text
Tradier filled sell_to_close
        -> exact external fill selected
        -> durable external EXIT_FILLED adoption
        -> canonical weighted fill truth
        -> APPositionManager close finalization
        -> exactly-one canonical proof authority
```

The reconciler must not label a proof/position source `TRADIER_EXIT_FILL` or equivalent without durable broker order/fill identity proving the claim.

Do not copy #428's schema expansion unless current-main code **already requires it**. This incident should not require a new migration.

## Step 7: Proof handling

Audit the current reconciler proof block after the position finalization change.

The required invariant is:

- unresolved external close evidence -> **zero proof mutation**;
- exact broker fill evidence -> proof economics must use the same broker fill authority used by the position;
- one position must not get one price in `positions` and another price in `proof_trades`;
- proof must not be created from a quote fallback;
- proof must not claim broker-fill provenance without broker identity.

Prefer existing canonical `APPositionManager` terminal-proof authority if current main already routes there.

If reconciler's independent proof writer becomes redundant after using the canonical finalizer, remove/bypass only the redundant call for this exact path. Do not turn this into the larger proof de-duplication work from PR #390.

## Step 8: Preserve bot-owned EXIT ownership

A manual/external close resolver must not race or steal a normal bot-generated EXIT.

Before accepting external broker fill evidence:

- preserve the existing active local EXIT check;
- preserve the existing broker-open EXIT check;
- exclude known bot-owned broker EXIT order IDs using the existing manual-close helper semantics;
- if a bot-owned EXIT order matches the contract/position, let the normal order/fill lifecycle own it;
- do not adopt the same broker order as both bot-owned and external.

No double finalization.

## Step 9: LIVE/PAPER isolation

All DB reads and evidence must remain scoped to:

- exact `client_id`;
- exact canonical `execution_mode` (`live` or `paper`);
- exact position id where available;
- exact OCC contract.

A PAPER order must never satisfy a LIVE reconciliation.

Missing/malformed/conflicting execution-mode evidence must fail closed, not default to LIVE or PAPER.

## Step 10: No broker mutation from reconciliation

Hard requirement:

This repair may call broker **read** methods only.

Tests must trap and fail on any call to common mutation methods including:

- `place_order`
- `submit_order`
- `buy_option`
- `sell_option`
- `cancel_order`
- equivalent adapter mutation methods

Broker POST count: **0**.

Broker cancel count: **0**.

# Required regression tests

Create a focused file such as:

`tests/test_p0_reconciler_external_close_broker_truth.py`

Use production methods, with broker/DB boundaries mocked only where necessary. Avoid source-text-only tests for the critical behavior.

At minimum implement all of the following.

## Test 1: exact external fill closes at broker fill, not current mark

Position:

- entry `0.52`
- remaining qty `3`
- exact OCC contract
- broker position absent

Broker external EXIT:

- side `sell_to_close`
- filled qty `3`
- broker fill `0.99`
- exact contract
- valid fill timestamp after entry

Current option quote intentionally returns `0.55`.

Assert:

- final exit truth is `0.99`, never `0.55`;
- position finalizer receives `0.99` and qty `3`;
- broker order identity is preserved;
- no broker submit/cancel.

This reproduces the incident family where a random/current contract value can diverge from actual broker fill.

## Test 2: no broker EXIT evidence -> HOLD, never current mark

Same broker-flat position, current option quote available at a positive value, but no matching filled sell-to-close broker order.

Assert:

- zero position UPDATE/finalizer call;
- zero proof write;
- quote may be fetched for diagnostics only if existing code does so, but cannot become exit truth;
- stable unresolved reason emitted;
- no broker submit/cancel.

## Test 3: no broker EXIT evidence and no quote -> HOLD, never entry price

Broker flat; no matching exit fill; quote unavailable.

Assert:

- no close at entry;
- no zero-P&L fabrication;
- zero authoritative mutation.

## Test 4: mismatched OCC contract rejected

Broker has a filled sell-to-close for the same underlying but a different strike/expiry/right.

Assert HOLD. Exact contract required.

## Test 5: BUY/ENTRY or wrong-side broker order rejected

A filled broker order exists for same contract but side is not sell-to-close.

Assert HOLD.

## Test 6: quantity mismatch rejected

Position remaining qty `3`; matching external fill totals `2` or `4`.

Assert unresolved/ambiguous HOLD, no close mutation.

## Test 7: weighted multi-fill close

Position remaining qty `3`.

External fills:

- qty 1 at `0.90`
- qty 2 at `1.05`

Expected weighted fill: `1.00`.

Assert canonical finalization uses the weighted broker truth and preserves both broker IDs in diagnostics/evidence.

## Test 8: bot-owned EXIT order wins ownership

Matching filled/open broker order ID is already associated with the bot's durable EXIT lifecycle.

Assert external/manual resolver does not adopt/finalize independently.

## Test 9: malformed broker order payload fails closed

Missing broker id, invalid/non-finite fill price, missing fill timestamp, malformed executed quantity, or malformed order payload.

Parameterize where sensible.

Assert no mutation.

## Test 10: LIVE/PAPER isolation

Same client/contract shaped rows in opposite execution modes.

Assert PAPER evidence cannot close LIVE and LIVE evidence cannot close PAPER.

## Test 11: restart/idempotency

Run exact external close reconciliation twice after durable adoption/finalization.

Assert second pass does not create a second external EXIT row, second terminal position mutation, or duplicate proof.

## Test 12: active EXIT prevents ghost/manual takeover

Broker position temporarily absent while a local or broker EXIT is still active.

Assert existing `GHOST_CLOSE_BLOCKED_ACTIVE_EXIT` style behavior remains and no external-close finalization happens.

# Required integration/adjacent test evidence

After focused tests pass, run at minimum the existing current-main tests covering:

- manual close reconciliation;
- reconciler position handling;
- partial close reconciler behavior;
- position-manager close-from-fill behavior;
- terminal proof behavior that is directly touched;
- exact execution-mode reconciliation isolation.

Then add the new regression file to `.github/workflows/p0_regression.yml` if it is not already included by a broad pattern. The critical new test must run in exact-head P0 CI, not only locally.

Do not modify workflow dependencies or unrelated CI behavior.

# Data mutation contract

When exact broker EXIT fill is proven, allowed mutations are only those already performed by the canonical existing external-close/finalizer path:

- durable external EXIT fill adoption, if required by the existing manual-close architecture;
- canonical position close fields from exact broker fill;
- canonical terminal proof path from the same exact fill truth.

When exact broker EXIT fill is **not** proven:

- `orders`: no authoritative lifecycle mutation from this unresolved reconciliation path;
- `positions`: no close/economic mutation;
- `proof_trades`: no mutation;
- `trade_queue`: no mutation;
- broker: no mutation.

Diagnostics/audit rows may be written using existing non-authoritative observability conventions.

# Non-goals

This PR is not allowed to become any of the following:

- rewrite of reconciler;
- rewrite of fill monitor;
- rewrite of exit engine;
- proof ledger redesign;
- migration project;
- recovery redesign;
- order state machine redesign;
- new broker API abstraction;
- strategy change;
- price improvement algorithm;
- TP/SL adjustment;
- selector/contract-selection change;
- broader #428 implementation transplant.

# Relationship to PR #428

PR #428 targets the same high-level invariant but has accumulated a much wider implementation surface, migration/attestation work, identity amendments, and historical CI baggage.

Do not duplicate that PR's current diff.

This PR is the narrower production incident repair:

**remove fabricated reconciler close-price authority and route broker-missing manual/external closes through the exact broker-truth machinery that already exists on current main.**

If Codex discovers that current-main manual-close reconciliation already fully finalizes the exact incident path and the only defect is that `ap_reconciler.py` races it first, the preferred repair is even smaller: make the reconciler defer/HOLD and let the canonical manual-close detector own the close. Do not force new integration merely to satisfy this document.

# Required PR review checklist

Before this implementation can move from HARD HOLD:

1. fresh-fetch current main and exact PR head;
2. list changed files;
3. inspect cumulative diff, not only latest commit;
4. verify the current-mark and entry-price close fallbacks can no longer write realized economics;
5. verify exact broker fill evidence is required;
6. verify broker order identity is preserved;
7. verify exact client/mode/position/contract isolation;
8. trace every broker method and prove zero POST/cancel from this new path;
9. trace `positions` mutation;
10. trace `proof_trades` mutation;
11. trace `orders` mutation/adoption;
12. trace queue mutation and prove none;
13. run the incident replay;
14. run focused and adjacent tests;
15. run exact-head P0 CI;
16. independently re-audit the final head;
17. issue MERGE / HOLD / HARD HOLD based on the exact head only.

# Acceptance criteria

This repair is complete only when all statements below are true:

- broker-flat does not equal price-known;
- a current option quote can never become realized exit fill truth in reconciler ghost-close logic;
- entry price can never be used to fabricate a zero-P&L reconciler close;
- exact external broker sell-to-close fill evidence can finalize a manual/operator close;
- multi-fill closes use weighted broker truth;
- ambiguous/missing broker evidence HOLDs with zero authoritative economic mutation;
- bot-owned EXIT lifecycle remains authoritative when present;
- LIVE/PAPER isolation remains exact;
- positions and proof use one consistent exact exit truth;
- no new broker submit/cancel authority exists;
- critical regressions run in exact-head P0 CI;
- production implementation remains tight and surgical.

## Final implementation target

The ideal final diff is boring:

- `ap_reconciler.py`: remove synthetic price authority, reuse exact broker-truth/manual-close machinery, route exact fill into canonical finalizer, HOLD on ambiguity;
- minimal helper/finalizer guardrails for proven scalar, timestamp, and ownership incompatibilities;
- focused regressions covering the incident path and those guardrails;
- one CI line adding that regression to P0.

That is the whole repair. If the diff starts spreading across subsystems, stop and justify every additional production file before continuing.
