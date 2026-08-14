# P1 SPEC — Restore one selector direct-quote budget authority

## Status

DRAFT / SPEC ONLY. DO NOT MERGE OR DEPLOY FROM THIS SPEC.

Historical reference: PR #441. Do **not** cherry-pick #441. Reimplement only the invariant below on current main.

## One job

Make selector direct-quote budget configuration deterministic and self-attesting so LIVE and PAPER cannot silently run contradictory recovery-budget configuration.

This PR does **not** change selector quality, trade criteria, entry logic, sizing, broker submission, exits, or intelligence.

## Fresh production evidence after the Aug-14 rollback

Same PEP deferred-breach selector path:

LIVE Jason:

```text
SELECTOR_MAX_DIRECT_QUOTE_CALLS = 40
DIRECT_QUOTE_RECOVERY_TOP_N     = 40
CONTRACT_REVALIDATE_TOP_N       = 40
conflict                        = false
```

PAPER Jose:

```text
SELECTOR_MAX_DIRECT_QUOTE_CALLS = 20
DIRECT_QUOTE_RECOVERY_TOP_N     = 8
CONTRACT_REVALIDATE_TOP_N       = 20
conflict                        = true
```

Important: PAPER's behavioral deferred cap is 20 because its canonical value is 20. The `8` alias is contradictory diagnostic state; it must not become behavioral authority.

## Frozen authority contract

`SELECTOR_MAX_DIRECT_QUOTE_CALLS` remains the **sole behavioral authority**.

Preserve current request-kind behavior:

```text
canonical absent:
  ordinary = 5
  deferred breach recovery = 40

canonical = 20:
  ordinary = 20
  deferred breach recovery = 20

canonical = 40:
  ordinary = 20
  deferred breach recovery = 40
```

`DIRECT_QUOTE_RECOVERY_TOP_N` and `CONTRACT_REVALIDATE_TOP_N` are compatibility/diagnostic aliases only.

They may report disagreement or malformed configuration. They may never lower, raise, or replace the canonical behavioral budget.

## HARD FILE BUDGET

Production: **3 files maximum**

1. `ap/contract_selector.py`
2. `ap/contract_quote_revalidator.py`
3. `ap/selector_recovery_deploy_preflight.py`

Tests: only

4. `tests/test_p0_selector_direct_quote_budget_authority.py`
5. `tests/test_p0_selector_recovery_deploy_preflight.py`

No workflow edit unless exact current-main P0 does not already run these tests.

If another production file is required, STOP and report the blocker. Do not expand scope.

## Required implementation

### 1. `ap/contract_selector.py`

Use the existing direct-quote budget resolver. Do not create a second resolver.

The resolver must explicitly expose:

- parsed canonical value
- parsed direct-recovery alias value
- parsed contract-revalidate alias value
- behavioral effective limit
- source
- `conflict`
- `conflict_detail`
- explicitly present malformed/non-positive keys

Rules:

- valid canonical value remains behavioral authority
- valid disagreeing alias => diagnostic `conflict=true`; canonical still wins
- malformed/non-positive explicit key => surfaced as invalid configuration
- alias-only configuration => alias remains diagnostic-only; existing defaults remain behavioral authority
- no env => preserve current defaults

Do **not** throw a new runtime exception in ordinary selector execution merely because an alias disagrees. Deployment/preflight owns the fail-closed fence.

Preserve all provider-call counting and selector request-kind behavior.

### 2. `ap/contract_quote_revalidator.py`

Remove the obsolete independent import-time read of `CONTRACT_REVALIDATE_TOP_N` if current-main search confirms it has no behavioral consumer.

A compatibility export may remain fixed if imports/tests require it, but it must not read environment or become authority.

Do not change:

- quote validity
- spread/liquidity rules
- premium/affordability checks
- direct-quote provider-call counting
- market-data routing

### 3. `ap/selector_recovery_deploy_preflight.py`

Consume the production resolver from `ap.contract_selector`.

Report a sanitized object containing:

```text
canonical_value
direct_recovery_alias
contract_revalidate_alias
source
ordinary_effective_limit
deferred_effective_limit
conflict
conflict_detail
invalid_explicit_keys
```

Preflight must HOLD / exit nonzero when:

- `conflict=true`, or
- any explicit budget key is malformed/non-positive.

Canonical-only clean config must pass.
Matching aliases must pass.
Existing lifecycle/max-attempt preflight gates remain untouched.

## Required regressions

Use the real resolver/context path, not a fake lookup table.

Must prove:

1. empty env => ordinary 5 / deferred 40
2. canonical 20 => ordinary 20 / deferred 20
3. canonical 40 => ordinary 20 / deferred 40
4. `40/40/40` => conflict false
5. production PAPER shape `20/8/20` => behavior 20 + conflict true
6. aliases alone never become behavioral authority
7. malformed canonical is surfaced
8. malformed direct-recovery alias is surfaced
9. malformed contract-revalidate alias is surfaced
10. identical environment yields identical selector budgets for LIVE and PAPER while execution modes remain distinct
11. preflight rejects `20/8/20`
12. preflight accepts canonical-only clean config
13. zero broker submit/cancel calls
14. zero order/position/proof/queue mutations

## Explicit non-goals

Do not change:

- delta
- DTE
- moneyness
- spread cap
- OI / volume
- premium limits
- affordability or account sizing
- scanner admission
- watcher trigger logic
- trigger confirmation
- retry counts
- contract ranking
- broker submit/cancel
- positions
- proof trades
- exits/reconciliation
- intelligence
- PAPER/LIVE execution identity
- PAPER market-data routing
- database schema

## Deployment policy boundary

This PR must **not hard-code a fleet value such as 40** just to make PAPER match LIVE.

After code is green, each active trading service must be inspected operationally and configured with one approved canonical value. Prefer removing legacy aliases; if compatibility requires them, they must equal the canonical value.

No environment mutation or deployment is authorized by this PR.

## Acceptance

```text
one canonical budget authority
-> ordinary/deferred request-kind behavior unchanged
-> aliases diagnostic only
-> conflicting/malformed deployment detected before trading
-> LIVE/PAPER same config => same selector recovery capacity
-> no quality-gate change
-> no money-path change
```

## Codex instruction

Implement this spec on the PR branch only. Use historical #441 as evidence, not code to cherry-pick. Keep the production diff to the three named files maximum and tests to the two named files. If current main makes that impossible, STOP and report the exact blocker. Run focused tests plus exact-head P0. Push implementation to this PR. Do not merge, deploy, change environment variables, or broaden into #439/#456/#434/intelligence work.