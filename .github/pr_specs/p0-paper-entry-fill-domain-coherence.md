# P0 Implementation Brief: PAPER Entry Fill Recovery Before Missed-Move Cancellation

> Status: implementation brief only. This branch is intentionally opened as a draft PR before production code changes so the implementation can be done and reviewed against a fixed contract. Remove this brief before merge if repository policy prefers no retained PR-spec files.

## Objective

Repair PAPER entry fill-management control flow without loosening LIVE entry behavior or any selector/risk safeguards.

The production-proven failure is that broker-owned PAPER entries can be canceled by generic `MISSED_MOVE` handling before the existing PAPER retry/reprice/fallback path gets authority.

Production examples observed Aug 17-19:

- NOW: submitted limit 1.53, submit ask 1.51, monitor current 2.20, age 11s -> `STALE_ENTRY_CANCEL MISSED_MOVE`
- UNH: submitted limit 3.52, submit ask 3.50, monitor current 5.85, age 11s -> `STALE_ENTRY_CANCEL MISSED_MOVE`
- MSFT: submitted limit 2.92, submit ask 2.90, monitor current 3.60, age 8s -> `STALE_ENTRY_CANCEL MISSED_MOVE`

Affected rows had NULL/no evidence for PAPER recovery diagnostics such as retry/reprice/fallback attempts. That means generic stale handling is preempting the dedicated PAPER recovery lifecycle.

## Primary file

`ap/order_monitor.py`

Avoid touching other production modules unless a test fixture/helper absolutely requires a microscopic change.

## Existing intended PAPER policy

`APOrderMonitor` already contains PAPER-only recovery behavior such as:

- `PAPER_ENTRY_REPEG_SECONDS` (historically 10,25,40)
- `PAPER_ENTRY_MARKET_FALLBACK_AFTER_SECONDS` (historically 45)
- `PAPER_ENTRY_MARKET_FALLBACK_ENABLED`
- `PAPER_ENTRY_MARKET_FALLBACK_MAX_SPREAD_PCT`
- `_paper_entry_retry_and_fallback(...)`

The bug is not absence of PAPER recovery. The bug is control-flow precedence and quote authority.

## Required behavior

For a broker-owned ENTRY whose client/execution mode is PAPER and state is `SUBMITTED` or `ACKNOWLEDGED`, PAPER-specific retry/fill management must own the pre-hard-age lifecycle before generic LIVE-style missed-move cancellation can terminalize it.

Conceptual intent:

```python
if paper and broker_order_id:
    run_paper_entry_retry_and_fallback()
    # Do not subsequently let generic MISSED_MOVE logic kill the same stale
    # in-memory broker order in this monitor cycle.
    evaluate_paper_hard_age_separately()
else:
    preserve_existing_generic_missed_move_behavior()
```

Do not blindly paste this shape if current structure requires a smaller/safer seam. Preserve existing ownership/CAS/idempotency semantics.

## Critical invariant

The generic `MISSED_MOVE_MIN_SECS` / `MISSED_MOVE_PRICE_MULT` / `MISSED_MOVE_ENTRY_CANCEL` path must not cancel a PAPER sandbox entry before the dedicated PAPER retry policy gets authority.

In particular, this must no longer happen merely because a separate market-data quote is far above the sandbox order limit:

```text
PAPER order
age 8-11s
external/data quote > submitted limit * 1.07
=> immediate generic MISSED_MOVE cancel
```

## PAPER quote authority

Inside PAPER fill-management/reprice decisions, do not rely on generic `_quote_broker()` if that helper prioritizes `self.data_broker` over `self.broker`.

The strategy/selector data broker may intentionally represent live market data while the PAPER execution broker is Tradier sandbox. Those are different questions:

1. What is the option doing in the real market?
2. What pricing domain is the PAPER execution broker using to evaluate/match this sandbox order?

For PAPER order fill/reprice/cancel authority, use the PAPER execution broker (`self.broker`) or an equally explicit execution-domain source. Do not globally change `_quote_broker()` because other monitor functionality may intentionally consume dedicated market data.

Do not silently fall back to a foreign/live quote and use it as sandbox fill authority if the execution-broker quote is missing.

## Same-cycle idempotency rule

If PAPER recovery cancels/replaces/reprices or submits a market fallback, do not continue through generic stale/missed-move logic against the old in-memory `broker_order_id` / limit in that same monitor cycle.

After a recovery action, return from this order's fill-management decision and let the next poll re-read durable/broker state.

Must prevent sequences like:

```text
cancel old sandbox order
replace it
continue using stale old broker id
cancel again
```

## Hard-age behavior

Do not make PAPER entries immortal.

Preserve the existing PAPER absolute/max-age safety ceiling (`PAPER_ENTRY_MAX_AGE_NORMAL`, A+ age policy, or their current equivalents). A truly unresolved PAPER order may still terminalize after its recovery policy/absolute ceiling.

The desired change is **PAPER recovery before generic stale terminalization**, not removal of stale protection.

## Diagnostics

No schema migration. Use existing metadata/audit surfaces.

Persist truthful evidence where current metadata shape supports it, including as many of these as are practical without broad refactoring:

- `paper_entry_retry_enabled`
- `paper_fill_management_quote_source`
- `paper_fill_management_quote_base_url`
- `paper_execution_broker_base_url`
- `paper_fill_management_bid`
- `paper_fill_management_ask`
- `paper_fill_management_mid`
- `reprice_attempt_count`
- `last_reprice_age_seconds`
- `last_reprice_outcome`
- `old_limit_price`
- `new_limit_price`
- `market_fallback_used`
- `market_fallback_outcome`

Resolve domain labels from actual broker/base URL rather than inferring from `execution_mode` alone. Example mapping:

- `https://sandbox.tradier.com` -> `tradier_sandbox`
- `https://api.tradier.com` -> `tradier_live`
- otherwise -> `unknown`

Diagnostic-write failure must not crash entry management.

## LIVE fence: mandatory

For LIVE clients, preserve current behavior. This PR must not loosen Jason's path.

Do not change LIVE:

- `MISSED_MOVE_MIN_SECS`
- `MISSED_MOVE_PRICE_MULT`
- `_try_repeg` behavior
- existing `MISSED_MOVE_ENTRY_CANCEL`
- hard-age behavior
- broker submit/cancel semantics
- pricing rule
- watcher confirmation
- selector/risk quality gates

Never add PAPER market fallback behavior to LIVE.

Do not modify delta, spread, OI, volume, moneyness, DTE, risk, trigger confirmation, or account sizing to increase trade count.

## Explicit non-goals

Do not modify, except for unavoidable test-only wiring:

- `ap_execution_core.py`
- `ap_entry_watcher.py`
- `ap_entry_watcher/__init__.py`
- `ap/contract_selector.py`
- `ap/contract_quote_revalidator.py`
- `ap/brokers/tradier.py`
- `ap/order_state_machine.py`
- `ap/fill_monitor.py`
- `ap/position_manager.py`
- `ap/queue.py`
- exit engine
- proof-trade lifecycle

No DB migration. No new table. No queue behavior changes. No position mutation. No proof-trades mutation. No selector loosening. Preserve `client_id`, `execution_mode`, OCC contract identity, qty, and broker identity persistence.

## Required tests

### 1. Exact NOW regression

PAPER order:

- state `SUBMITTED`
- broker order id present
- contract `NOW260821P00120000` or production-shaped equivalent
- limit 1.53
- age 11s
- data broker reports 2.20
- execution/sandbox broker reports its own PAPER-domain quote

Expected:

- no `MISSED_MOVE_ENTRY_CANCEL` solely because data broker = 2.20
- PAPER recovery is evaluated
- order is not prematurely terminalized

### 2. MSFT 8-second regression

PAPER:

- limit 2.92
- data/external quote 3.60
- age 8s

Expected: no generic 7% missed-move terminalization before PAPER recovery eligibility/authority.

### 3. First PAPER reprice rung

At first configured PAPER retry rung with broker id and valid sandbox/execution quote:

- PAPER retry executes
- attempt counter increments
- replacement/reprice uses execution-domain quote
- identity remains durable

### 4. PAPER market fallback

At/after configured fallback age, with valid execution quote and spread within configured max:

- existing PAPER fallback remains reachable
- fallback audit fields are persisted truthfully

### 5. No double action in one cycle

If PAPER recovery cancel/replaces, generic stale cancellation must not run afterward against the old broker id.

### 6. LIVE control

Run the same NOW-style inputs with `client_mode/execution_mode = LIVE`.

Expected: existing LIVE missed-move behavior is unchanged.

### 7. Identity preservation

PAPER recovery preserves:

- `client_id`
- `execution_mode`
- `local_order_id`
- OCC contract
- qty
- `buy_to_open` side/action

Replacement broker identity is persisted through the existing mechanism.

### 8. Missing PAPER execution quote

If execution-broker quote is unavailable:

- fabricate nothing
- do not silently use live/data-broker quote as fill authority
- no crash
- deterministic retry/hard-age behavior
- diagnostic says quote unavailable

### 9. Hard ceiling survives

A genuinely stale PAPER order beyond the absolute configured max age must still be eligible for existing terminal cancellation semantics.

## Acceptance criteria

This PR is acceptable only if all are true:

1. PAPER generic missed-move cannot preempt PAPER recovery.
2. PAPER retry/fill management uses explicit PAPER execution-domain quote authority.
3. PAPER recovery action ends the current decision cycle for that stale broker object.
4. PAPER absolute stale ceiling remains.
5. LIVE behavior remains unchanged.
6. No selector/risk/watcher safeguards are loosened.
7. No schema migration.
8. No new queue/position/proof-trades mutation.
9. `client_id`, execution mode, OCC contract, qty, and broker identity remain correct.
10. Regression tests reproduce the production NOW/MSFT failure shapes.

## Review checklist before merge

Review must explicitly answer:

- Does this change LIVE behavior?
- Is any behavior behind a flag, and what is the default?
- Does it touch broker submit/cancel paths? If yes, exactly how and only for PAPER?
- Does it mutate orders/positions/proof_trades/queue beyond existing PAPER behavior?
- Are `client_id` and `execution_mode` preserved?
- Does it use actual production metadata shape?
- Are diagnostics preserved downstream?
- Does it pollute PAPER/LIVE taxonomy?
- Could it make Jason trade junk? Required answer: no.

## Size constraint

Keep production code small. Expected shape: one PAPER control-flow precedence repair, one scoped quote-authority repair, minimal diagnostics, and focused tests. Do not build a new execution subsystem.