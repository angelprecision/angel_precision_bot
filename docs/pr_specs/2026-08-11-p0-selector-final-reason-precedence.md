# P0 Implementation Note — Deferred selector final-reason precedence

This branch implements the work order below. The production correction is
limited to deferred selector reason reduction and its candidate-accounting
evidence; it does not change broker submit/cancel behavior, thresholds, or
position/proof/reconciliation paths.

## Incident

On 2026-08-11, LIVE and PAPER accounts both produced many deferred selector failures where the observed chain blocker was retryable quote/data failure, especially `CHAIN_ROW_ZERO_BID_ASK`, but the durable canonical reason was rewritten to terminal `MONEYNESS_OUT_OF_RANGE`. Affected rows commonly terminalized after `materialization_attempts=1`.

Representative production shape:

- no quality survivor
- near/usable candidates with zero or unusable quote data
- unrelated far-OTM contracts structurally skipped as `STRUCTURAL_MONEYNESS_OUT_OF_RANGE`
- `resolve_selector_recovery_final_reason()` returns `MONEYNESS_OUT_OF_RANGE` before considering retryable attempted-data or retryable quality evidence
- downstream materialization persists `FAILED_TERMINAL / TERMINAL_NO_TRADEABLE_CONTRACT`
- no retry is scheduled

The canonical retry policy treats chain/direct zero-bid-ask data failures as retryable. A structurally irrelevant far-OTM contract must not terminalize the entire request when viable candidates remain unresolved for retryable data reasons.

## Required implementation

1. Audit `ap/selector_retry_policy.py::resolve_selector_recovery_final_reason()` and its call sites in deferred contract selection/materialization.
2. Change final-reason resolution so structural DTE/moneyness/delta skips terminalize only when the relevant candidate universe is fully accounted for and no viable/retryable candidate evidence remains.
3. Preserve fail-closed behavior for genuinely terminal geometry and policy.
4. Preserve terminal account-affordability outcomes such as `UNTRADEABLE_FOR_ACCOUNT_SIZE`, `NO_AFFORDABLE_CONTRACT`, and final premium-cap truth when full-set accounting proves them.
5. Preserve actual request-budget exhaustion behavior when eligible candidates remain unattempted.
6. Preserve both canonical final reason and observed diagnostics. Do not erase `last_observed_selector_reason`, reject buckets, structural skips, direct-quote attempts, request budget diagnostics, quote source, execution mode, sandbox mode, or best rejected candidate.
7. Do not loosen delta, moneyness, spread, OI, volume, premium, DTE, or account-size gates.
8. Do not modify broker submit/cancel behavior.
9. Do not mutate positions, proof_trades, reconciliation behavior, or exits.
10. Preserve `client_id`, `execution_mode`, `signal_id`, local order identity, materialization generation, watcher ownership, and selector recovery cursor identity/CAS rules.

## Required precedence contract

Deferred recovery should resolve approximately in this authority order, subject to existing market-truth invalidation semantics:

1. terminal setup/market invalidation
2. terminal policy applying to the viable candidate set
3. terminal quality applying to the viable candidate set
4. actual request-budget exhaustion with eligible candidates remaining
5. retryable attempted provider/data failures
6. retryable quality/data failures
7. structural terminal geometry only when full-set accounting proves no viable/retryable candidate remains
8. affordability only when full-set accounting proves the candidate universe is unaffordable
9. unknown -> fail closed

The key invariant is: presence of one `STRUCTURAL_MONEYNESS_OUT_OF_RANGE` record is not sufficient authority to declare the whole selector request terminal.

## Required tests

Add executable tests through the real resolver and the real deferred owner/materialization seam, not hard-coded result tables.

### Mixed retryable-data + structural moneyness replay

Construct a chain with near-ATM candidates that fail with `CHAIN_ROW_ZERO_BID_ASK` or `DIRECT_QUOTE_ZERO_BID_ASK` and far-OTM candidates that are structurally skipped as `STRUCTURAL_MONEYNESS_OUT_OF_RANGE`.

Expected:

- final reason remains retryable data
- classification is `RETRYABLE_DATA`
- deferred retry is scheduled
- row is not terminalized after attempt 1
- no broker submit/cancel occurs until a true quality survivor exists

### Genuine terminal moneyness control

Every relevant candidate is structurally outside moneyness, with no retryable provider/data evidence.

Expected: `MONEYNESS_OUT_OF_RANGE` remains terminal and no retry is scheduled.

### Affordability controls

Replay small-account shapes equivalent to 2026-08-11 AVGO/TSLA production evidence where quality contracts exceed the account budget.

Expected: affordability/account-size result remains terminal; no selector retry, broker submit, or cancel.

### PAPER/LIVE parity

Run the same mixed-evidence selector shape in LIVE and PAPER while preserving mode-specific transport identity. Expected final classification must be semantically identical, except for legitimate execution/data-domain diagnostics.

### Cursor/ownership

Prove retry scheduling preserves exact client, execution mode, signal, local order, materialization generation, and selector recovery cursor authority. Ownership/CAS loss must fail closed.

## Production safety gate

Before merge, audit exact head for:

- live-active code path
- no threshold relaxation
- no broker submit/cancel expansion
- no position/proof_trades mutation
- exact production metadata shape
- PAPER/LIVE taxonomy parity
- restart ownership/cursor correctness
- exact-head CI and incident-shaped executable replay

## Amendment corrections

The implementation now carries candidate-scoped outcomes from the selector to
the deferred resolver. Aggregate reject buckets remain diagnostic only, so a
terminal OI/spread result on one candidate cannot veto retryable quote/data
evidence on another candidate. The resolver also requires an explicit complete
candidate-accounting marker before terminal structural geometry or
affordability can be selected. Incomplete or ambiguous accounting fails closed
to the unknown recovery reason, while actual unattempted candidates retain
the request-budget outcome.

The fleet replay accepts the canonical zero-quote reasons emitted by the
selector (`CHAIN_ROW_ZERO_BID_ASK` and `DIRECT_QUOTE_ZERO_BID_ASK`) while
continuing to assert independent budgets, preserved diagnostics, and zero
broker submit/cancel activity.

## Merge policy

Draft only until exact-head production audit is complete. Do not merge or deploy without explicit approval.
