# P0: Preserve selector terminal truth and spend quote budget in ranked order

## Status

**DRAFT IMPLEMENTATION CONTRACT ONLY. DO NOT MERGE OR DEPLOY.**

This branch preserves the July 29, 2026 IBM CALL production incident and the exact surgical repair contract. Production code is not changed by this specification commit.

## Production incident

Canonical signal:

- signal ID: `66b1607b-5364-4cd3-9987-2b03a4199517`
- ticker/side: `IBM CALL`
- entry trigger: `228.98`
- scanner stop: `221.45`
- target: `236.51`
- score/tier: `70 / B`
- trigger crossed: approximately `10:05:18 ET`
- trigger confirmed: approximately `10:05:33 ET`

Tradefluence PAPER and Jose PAPER selected and filled:

- contract: `IBM260731C00230000`
- strike: `230`
- limit: approximately `$2.07`
- fill: approximately `$2.05`

Jason LIVE did not submit.

Jason order evidence:

- database order ID: `26483`
- local order ID: `f5a13b0b-5954-4d40-8fbc-ee84d58d2b1a`
- terminal status: `EXPIRED`
- placeholder contract: `DEFERRED:IBM`
- no broker order ID
- no fill
- no position
- five selector attempts
- final `last_error`: `RETRY_MAX_ATTEMPTS_EXCEEDED`
- last surface reason before exhaustion: `SELECTOR_REQUEST_BUDGET_EXHAUSTED`

## Account and contract truth

Jason LIVE account equity was approximately `$1,747.09`.

With the existing 10% maximum position policy:

- maximum position dollars: approximately `$174.71`
- maximum affordable premium per share: approximately `$1.7471`

The valid quality contract later observed for the 230 CALL had approximately:

- bid `$2.95`
- ask `$3.15`
- midpoint `$3.05`
- delta `0.4082`
- volume `1,249`
- open interest `2,045`
- spread approximately `6.56%`

One contract at the LIVE ask required approximately `$315`, which exceeded Jason's current percentage-based position budget.

Therefore the selector had valid structural evidence for `UNTRADEABLE_FOR_ACCOUNT_SIZE`. This incident must not be described as if request capacity alone prevented an otherwise affordable order.

## Confirmed defects

### 1. Terminal affordability truth was overwritten

The selector produced a valid account-size terminal reason in an earlier expiration bucket, then continued through later work and surfaced `SELECTOR_REQUEST_BUDGET_EXHAUSTED` instead.

The retry layer treated the later transient reason as authoritative, retried five times, and ultimately replaced the true cause with `RETRY_MAX_ATTEMPTS_EXCEEDED`.

This is taxonomy corruption. A later transient failure must not erase already-proven structural terminal truth.

### 2. Direct-quote budget was not spent in ranked order

Diagnostics ranked the IBM 230 CALL as the top candidate, but the 40 direct-quote attempts began around much farther OTM strikes and continued outward. The request budget was consumed on low-value candidates while the recorded rank-1 candidate was not first in the attempted-symbol sequence.

The candidate list used for diagnostics and the candidate list consumed by direct-quote revalidation must be the same ordered list.

## Required production invariants

### Canonical reason precedence

Across expiration/DTE attempts, preserve every bucket outcome and reduce them through one explicit precedence function.

A proven structural terminal result must dominate later transient capacity failures.

At minimum:

1. identity/mode/source safety failures remain fail-closed under their existing precedence;
2. proven `UNTRADEABLE_FOR_ACCOUNT_SIZE` remains terminal when supported by valid quality and quote evidence;
3. terminal contract-quality rejection remains terminal under existing policy;
4. `SELECTOR_REQUEST_BUDGET_EXHAUSTED` is retryable only when request capacity is genuinely the unresolved blocker and no stronger terminal evidence exists;
5. `RETRY_MAX_ATTEMPTS_EXCEEDED` must not replace the canonical root cause in durable diagnostics;
6. the order may record retry exhaustion as lifecycle context, but the root selector reason must remain queryable and authoritative.

### Ranked quote spending

When candidates are ranked for recovery/revalidation:

- assign and consume the returned ranked list;
- attempt rank 1 first, then rank 2, and so on;
- preserve deterministic tie-breaking;
- preserve the existing bounded request limits;
- do not increase direct-quote, chain, expiration, or elapsed-time budgets in this PR;
- stamp attempted symbol, rank, request number, and remaining budget consistently.

Normalized duplicate OCC rows remain available to the quality loop so a stale or
zero-quoted row cannot suppress a valid representation of the same contract.
Duplicate groups are ordered by quote usability, execution ask, spread, and
liquidity; unrelated provider metadata is not financial authority. The direct
quote revalidator still fences calls by normalized OCC identity, so duplicates
consume at most one direct-quote slot per request.

### Retry eligibility

The retry owner must consume the selector's canonical reduced reason, not merely the last loop iteration's reason.

The selector publishes `canonical_selector_reason`,
`last_observed_selector_reason`, and (when a request cap stopped the attempt)
`operational_reason` in its attached `selector_failure`. Execution core copies
the canonical field unchanged and only adds lifecycle taxonomy around it; it
does not reconstruct canonical truth from reject buckets or raw last-failure
events.

When canonical result is terminal affordability:

- do not schedule another selector retry;
- terminalize once under existing ownership/CAS rules;
- do not call broker submit or cancel;
- retain account budget and best valid rejected contract evidence.

## Intended implementation scope

Expected production files only:

1. `ap/contract_selector.py`
2. `ap_execution_core.py` only at the existing selector-result/retry-classification seam
3. focused tests, preferably:
   - `tests/test_p0_selector_terminal_reason_precedence.py`
   - amendments to `tests/test_p0_selector_direct_quote_budget_authority.py`
   - only directly relevant retry-taxonomy assertions

Do not change `.github/workflows/p0_regression.yml` unless the focused file is otherwise absent from the authoritative P0 suite.

## Interaction with open PR #401

PR #401 already changes selector recovery and retry files. This specification must not be implemented concurrently as a blind overlapping branch.

Before production implementation:

1. audit #401 exact head against this contract;
2. identify whether #401 already fixes either invariant;
3. if #401 is still intended to merge, amend #401 surgically or rebase this implementation after #401;
4. if #401 is abandoned, implement fresh from then-current main;
5. never copy the entire #401 subsystem into this PR.

The purpose of this draft is to preserve the incident and acceptance contract without creating another giant overlapping implementation.

## Required implementation behavior

### Failure collection

Each expiration/DTE attempt must return a structured outcome containing at least:

- reason code
- retryable/terminal disposition
- expiration/DTE identity
- quality-survivor count
- affordable-survivor count
- best quality candidate
- best rejected candidate
- quote source and freshness authority
- account position-dollar budget
- candidate ask/cost
- request-budget use and remaining capacity

### Final reducer

Add or isolate one pure reducer/helper that chooses the final selector result from all attempted outcomes.

The reducer must be deterministic and directly unit-tested. It must not depend on loop order for precedence.

For the IBM replay:

- at least one bucket proves valid quality but no affordable contract;
- a later bucket exhausts direct-quote capacity;
- final canonical reason must be `UNTRADEABLE_FOR_ACCOUNT_SIZE`;
- lifecycle may note that later request capacity was also exhausted, but it cannot replace the root reason.

### Durable diagnostics

Preserve both:

- `canonical_selector_reason`
- `last_observed_selector_reason`

or equivalent fields with unambiguous names.

The root reason must remain visible after terminalization and after any retry owner cleanup.

Do not stamp an order as though a broker order was attempted when none existed.

## Required regression coverage

### Exact IBM affordability replay

Input:

- equity `$1,747.09`
- max position pct `10%`
- max position dollars approximately `$174.71`
- rank-1 contract `IBM260731C00230000`
- valid LIVE ask `$3.15`
- one-contract cost `$315`
- later DTE/direct-quote work reaches request-budget exhaustion

Expected:

- final canonical reason `UNTRADEABLE_FOR_ACCOUNT_SIZE`
- terminal/non-retryable selector disposition
- zero additional selector retries
- zero broker submit/cancel calls
- durable diagnostics retain `$174.71` budget and `$315` required cost
- lifecycle exhaustion does not replace the root cause

The replay must pass through the actual deferred execution owner, prove one
selector invocation with no retry-owner reschedule, and retain the canonical
root plus the `$174.71` / `$315` affordability evidence after terminal cleanup.

### Duplicate OCC quality resolution

Use the same normalized OCC twice with one stale/zero chain quote and one valid
chain quote, reverse provider order, and vary liquidity plus irrelevant provider
metadata. Expected:

- the valid representation remains selectable;
- the normalized selected contract is identical in both orders;
- direct-quote call count and canonical selection truth are identical;
- the duplicate does not consume a second quote-budget slot.

### Ranked direct-quote order

Create a deliberately unsorted chain where:

- rank 1 is near trigger and valid;
- lower-ranked candidates appear earlier in raw chain order;
- request budget is smaller than candidate count.

Expected:

- attempted symbols follow ranked order exactly;
- rank 1 is attempted first;
- deterministic tie-breaking is preserved;
- no budget increase.

### Transient-only control

When no terminal structural evidence exists and all unresolved candidates are blocked solely by bounded request capacity:

- final reason remains `SELECTOR_REQUEST_BUDGET_EXHAUSTED`;
- existing bounded retry behavior remains available;
- retry owner/generation fencing remains unchanged.

### Mixed reason precedence

Test permutations of the same bucket outcomes to prove loop order cannot change the final canonical reason.

### PAPER/LIVE explanation parity

Use the same chain and quote quality for PAPER and LIVE, with different account budgets.

Expected:

- contract-quality classification matches;
- PAPER may select when its explicit budget permits;
- LIVE returns `UNTRADEABLE_FOR_ACCOUNT_SIZE` when its explicit budget does not;
- mode alone is not mislabeled as a quote/request failure.

## Explicit non-goals

This PR must not:

- increase Jason's 10% max position percentage
- add a one-contract small-account override
- loosen spread, delta, volume, open-interest, DTE, premium, or score rules
- increase selector request budgets
- modify trigger geometry
- modify watcher ownership
- add broker submit/cancel authority
- mutate positions or proof trades
- alter exits
- rewrite selector recovery

## Separate policy decision

Fixing these defects makes the reason truthful and the selector efficient. It does **not** make the IBM contract affordable under the current 10% position policy.

Allowing Jason to take this exact contract requires a separate explicit risk-policy decision, such as a bounded one-contract small-account fallback with an absolute ask/cost cap and final pre-submit enforcement. That policy must remain separate, default-off, and independently reviewed. It must not be smuggled into this taxonomy PR.

## Acceptance criteria

- IBM final root reason remains `UNTRADEABLE_FOR_ACCOUNT_SIZE`.
- No pointless retry occurs after proven terminal affordability.
- Ranked candidates are consumed in ranked order.
- Existing selector budgets are unchanged.
- Existing quality and risk thresholds are unchanged.
- No broker path runs for the rejected IBM order.
- Focused and adjacent tests pass.
- Exact-head P0 CI passes.
- #401 overlap is explicitly resolved before implementation.
- Production-safety audit confirms no sizing, submit/cancel, position, proof, exit, queue, or watcher regression.

## Required final report

Return:

- previous base SHA
- exact final head SHA
- exact files changed
- #401 overlap decision
- exact functions/lines changed
- focused test commands/results
- adjacent test commands/results
- exact-head CI run/job/result
- IBM replay output
- ranked-attempt output
- canonical-versus-last-reason diagnostics
- explicit statement that risk limits were not changed
- explicit statement that no merge or deployment occurred
