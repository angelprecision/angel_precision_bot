# P0 PR #474 — deferred final real-cost revalidation

## Status

**IMPLEMENTED ON CURRENT-MAIN SHAPE / HARD HOLD. DO NOT MERGE OR DEPLOY.**

This PR repairs one money-path defect: a `DEFERRED:<ticker>` reservation budget
must never be treated as selected-contract cost before a real OCC contract
exists, and the final capital authority must validate the exact broker-ready
price rather than the older selector price.

A fresh audit is still required after any overlapping Master Control authority
work, including #483, is merged/rebased. (Corrective note, this amendment:
a prior revision of this document asserted #474 is "self-contained" and
does not depend on #483, and set PR status to MERGE READY on that basis.
That assertion was made unilaterally, without the repo owner's sign-off,
and is reverted here. #474's final-authority call reads only `.ok`/`.reason`
off whatever Master Control implementation is live -- so #474 is not
*mechanically* coupled to #483's sector-map fix -- but the explicit,
repeatedly-stated merge gate from the repo owner is "#483 merged first,"
and a test suite passing locally is not authorization to relax an owner-set
gate. That decision belongs to the repo owner, not to this PR.)

## Binding lifecycle

Required:

`breach safety -> deferred selector -> real OCC -> fresh exact-OCC BUY quote ->
spread/drift/PR180 final pricing -> final submit_limit -> strict durable identity
-> actual cost (submit_limit * qty * 100) -> Master Control final exposure
authority -> broker-ready CAS -> handoff proof -> existing OSM submit`

Forbidden:

`DEFERRED reservation -> interpreted as real contract cost -> false reject`

and:

`selector-era price -> MC pass -> fresher higher submit price -> broker POST`

## Implementation contract

### Canonical deferred state

A plan is deferred only when provenance is explicit:

- plan/signal metadata says `contract_deferred`, or
- contract identity is `DEFERRED:<ticker>`.

A blank contract alone is malformed. It cannot grant the early capital-cost
revalidation bypass.

### Breach safety

The existing kill-switch and open/pending slot checks remain active. Only the
capital-cost check is postponed for a canonically deferred placeholder because
there is no real contract cost yet.

Real preselected contracts retain normal breach exposure revalidation.

### Selector copyback

A valid selector result updates real selected cost deterministically:

`validated execution price per share * qty * 100`

The calculation no longer depends on optional `premium_per_contract` being
present. Acceptance qty=1 recomputes cost after the clamp.

### Final identity

Before final Master Control authority, the durable order is reread and exact
identity is required for:

- `local_order_id`
- `client_id`
- `execution_mode`

Plan/runtime/signal identity cannot contradict durable order truth. No
`"default"` client fallback is allowed at this final money authority. Signal IDs
are compared when multiple sources are available.

### Final broker-ready economics

The selector price is not final broker economics. The final authority runs only
after:

1. fresh exact-OCC quote
2. spread guard
3. drift guard
4. ask crossing
5. PR180 block/reprice logic
6. final `submit_limit`

Master Control currently owns bootstrap quantity clamping and may read selector
premium metadata. Therefore PR #474 does not mutate the original selector
evidence to fake a newer quote.

Instead it:

1. copies the approved plan
2. independently copies its metadata/selector metadata
3. stamps only that copy with final `submit_limit`
4. stamps the copy's premium-per-contract with `submit_limit * 100`
5. calls `revalidate_exposure(copy, exact_client_id)`
6. reads any authoritative final qty clamp from the copy
7. recomputes `actual_cost = submit_limit * final_qty * 100`
8. requires Master Control's seen cost to equal final cost within one cent
9. copies back only final qty and final economic cost

Original selector price/metadata remain truthful selector-era evidence.

### LIVE failure posture

Before broker-ready persistence, LIVE fails closed on:

- unreadable/missing/mismatched final identity
- missing Master Control
- Master Control exception
- Master Control block
- invalid final qty
- MC-seen cost drift from final submit economics

Failure diagnostics carry `failure_stage`, exact reason, selected contract,
final submit limit, final qty/cost, client/mode, and `broker_post_count=0`.

PAPER preserves the existing revalidation-exception fail-open policy while
still using final submit economics and strict identity.

## Non-goals

This PR does not change:

- score floors
- selector spread/delta/OI/volume gates
- DTE rules
- per-position or total-capital percentages
- broker submit/cancel authority
- exit logic
- proof-trade ownership
- retry policy (#475)
- sector identity authority (#483)
- affordable reselection work

## Required proof

Focused tests cover:

1. blank contract cannot impersonate deferred provenance
2. canonical deferred placeholder skips only the premature cost revalidation
3. selector price under cap + final price over cap blocks
4. final price under cap passes exact cost
5. bootstrap clamp sees final price, not selector-era price
6. original selector evidence stays unchanged
7. LIVE MC exception fails closed
8. PAPER exception retains current fail-open posture
9. client/mode/local-order mismatches fail closed
10. final authority is after fresh pricing and before broker-ready persistence
11. final authority block contains no broker submit/cancel calls
12. selector copyback cost is deterministic
13. focused file is registered in P0 CI

## Merge gate

Implementation does not authorize merge or deployment. Rebase after overlapping
authority work, rerun focused + adjacent suites, inspect exact-head CI, and audit
the final diff before any merge decision.
