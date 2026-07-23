# P0: Canonical PAPER pre-submit market truth

## Status

Implementation contract only. No production behavior is implemented by this commit. Keep the pull request in draft until production code and exact-head tests are added.

Base SHA: `ffd22ca37ab0dcf874c2b463e61bee16337772fa`

## Production incidents

### Tradefluence BAC PUT

- contract: `BAC260724P00062000`
- selected quote: bid `0.88`, ask `0.96`, midpoint `0.92`
- original selector ask / planned limit: approximately `0.94`
- submit-time quote: bid `0.61`, ask `0.64`, midpoint `0.625`
- actual submitted PAPER limit: `0.66`
- underlying PUT trigger: `61.17`
- submit-time underlying bid / ask: `61.20 / 61.36`
- classifier reason: `PUT_NO_LONGER_BELOW_TRIGGER`
- classifier nevertheless persisted `passed=true` because PAPER converts market-validity failures into warnings
- order canceled about 23 seconds later when the option quote returned to approximately `0.94`

The `0.66` value was not an entry fill. It was the real limit sent to the PAPER broker from the submit-time quote. The defect is that the bot submitted after the PUT breach had reversed and accepted unknown/sandbox quote provenance as sufficient PAPER market truth.

### Tradefluence and Jose PEP PUT

- trigger: `133.95`
- submit-time underlying bid / ask: `134.65 / 134.72`
- classifier reason: `PUT_NO_LONGER_BELOW_TRIGGER`
- classifier nevertheless persisted `passed=true`
- both accounts submitted and filled seven contracts near `2.60`
- the trade became an immediate loser

Both incidents reached the same exact code path. This is not a generic PUT ban. BAC later produced a strong move. The repair is to enter a PUT only while the ticker-specific PUT breach is currently valid, and to re-arm the setup when it is temporarily invalid instead of terminally killing it.

## Root cause in current main

`ap/live_submit_gates.py:check_market_validity_gate()` computes the correct directional failure. Its `_fail()` helper then sets `blocked = execution_mode == "live"`, which means PAPER receives `passed=true` and `reason_code=PASS` even when the audit payload says `PUT_NO_LONGER_BELOW_TRIGGER`, `CALL_NO_LONGER_ABOVE_TRIGGER`, stop broken, or target complete.

The current design deliberately lets PAPER exercise stale sandbox flow. That permissive behavior has leaked past harmless quote-age warnings and now authorizes broker submissions whose ticker-specific thesis is false.

## Required behavior

### 1. Separate transport warnings from thesis invalidation

PAPER may remain warning-only for provider quote age when a synchronous quote has valid receipt-time evidence. The following setup-geometry failures must block broker submission in both PAPER and LIVE:

- `CALL_NO_LONGER_ABOVE_TRIGGER`
- `PUT_NO_LONGER_BELOW_TRIGGER`
- `CALL_STOP_ALREADY_BROKEN`
- `PUT_STOP_ALREADY_BROKEN`
- `TARGET_ALREADY_INVALID`
- malformed, zero, crossed, or missing current underlying quote

A `GateResult` must never contain `passed=true` with one of those adverse reason codes in its audit payload.

### 2. Re-arm instead of terminalize

When final directional validity fails before broker POST:

- do not submit or cancel a broker order;
- do not mutate positions, proof trades, or realized performance;
- preserve exact `client_id`, `execution_mode`, `signal_id`, setup owner, and trigger generation;
- return the durable ENTRY row to the existing retryable `PENDING_TRIGGER` / watcher-owned lifecycle;
- clear stale materialized contract price authority so the next valid breach performs a fresh contract selection or direct quote revalidation;
- set an explicit bounded retry time and retain the original signal expiration/session validity;
- do not consume a daily trade slot.

Required reason codes:

- `PAPER_SUBMIT_DIRECTION_REVERSED_REARMED`
- `LIVE_SUBMIT_DIRECTION_REVERSED_REARMED`
- `SUBMIT_STOP_ALREADY_BROKEN_REARMED`
- `SUBMIT_TARGET_ALREADY_COMPLETE_TERMINAL`
- `SUBMIT_UNDERLYING_TRUTH_UNAVAILABLE_RETRY`

Target-complete may terminalize because the opportunity is finished. Direction reversal and temporary quote unavailability must remain retryable.

### 3. Require real PAPER market-data provenance

PAPER order execution may remain on Tradier sandbox, but contract and underlying market truth must come from the configured data broker.

Before PAPER broker submit, persist and validate:

- selector data broker base URL/domain;
- submit-time data broker base URL/domain;
- execution broker base URL/domain;
- quote source and provenance;
- selected bid/ask/mid and timestamps;
- submit bid/ask/mid and timestamps;
- current underlying bid/ask/mid and timestamps.

`quote_source=unknown` or selector/submit market data coming only from sandbox cannot certify directional validity. The row must remain retryable with `PAPER_LIVE_DATA_TRUTH_UNAVAILABLE`; it must not submit a synthetic or sandbox-canned price.

Do not confuse quote-domain separation with an error: PAPER execution broker should be sandbox while market-data broker should be live. That intentional separation must be recorded as `paper_data_order_domain_separated=true`, not mislabeled as pollution.

### 4. Preserve diagnostics

Every decision event and order meta patch must preserve:

- exact client ID;
- exact execution mode;
- original trigger, stop, and target;
- current underlying quote fields;
- selected and submit option quote fields;
- provider timestamp, receipt timestamp, age, source, and domain;
- whether the action was BLOCK, REARM, or TERMINAL;
- whether any broker call occurred.

## Expected production files

Keep implementation narrow. Expected files:

- `ap/live_submit_gates.py`
- `ap_execution_core.py`
- `ap_entry_watcher.py` only for existing-owner re-arm wiring
- `ap/order_state_machine.py` only if a canonical nonterminal transition is missing
- `.github/workflows/p0_regression.yml`
- `tests/test_p0_paper_submit_market_truth.py`

Do not edit exit logic, proof-trade taxonomy, scanner thresholds, contract-quality thresholds, position sizing, or broker fill simulation.

## Required production-shaped tests

1. Tradefluence BAC replay: planned `0.94`, submit quote `0.61/0.64`, underlying PUT trigger `61.17`, current bid `61.20`. Expected: no broker submit, durable re-arm, exact PAPER identity retained.
2. BAC rebreach: current underlying returns below trigger with fresh live-domain data. Expected: one fresh materialization and at most one broker submit.
3. PEP replay: trigger `133.95`, current bid `134.65`. Expected: no broker submit and no position/proof row.
4. Valid PAPER PUT: current bid is below trigger, target remains open, stop intact, live-domain data fresh. Expected: normal submit unchanged.
5. Valid PAPER CALL mirror.
6. Missing/unknown quote source: retryable hold, no submit.
7. Sandbox-only market quote: retryable hold, no submit.
8. PAPER sandbox execution plus live data broker: allowed and correctly classified as intentional domain separation.
9. Repeated poll/restart: no duplicate broker order, no duplicate watcher owner, no generation regression.
10. LIVE behavior from merged PR #375 remains intact.

## Activation and merge gates

- Rebase after any preceding entry-watcher or selector PRs that merge first.
- Exact-head focused tests green.
- Exact-head P0 workflow green.
- Attach a trace proving `direction reversed -> no POST -> re-arm -> valid rebreach -> one POST`.
- No merge without explicit approval from Angel.
