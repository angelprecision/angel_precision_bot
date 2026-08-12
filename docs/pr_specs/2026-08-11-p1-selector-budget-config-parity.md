# P1 Work Order — Selector direct-quote budget configuration parity

## Status after authority retrace — 2026-08-11

The runtime/config retrace is complete. Codex should **not** spend time rediscovering selector budget ownership before implementation.

This branch still contains specification-only work. No production behavior has been changed yet.

## Confirmed production incident

2026-08-11 production metadata shows selector recovery budget drift between LIVE and PAPER pods.

Representative LIVE diagnostics:

- `SELECTOR_MAX_DIRECT_QUOTE_CALLS=40`
- `DIRECT_QUOTE_RECOVERY_TOP_N=40`
- `CONTRACT_REVALIDATE_TOP_N=40`
- deferred effective limit = 40
- `conflict=false`

Representative PAPER diagnostics:

- `SELECTOR_MAX_DIRECT_QUOTE_CALLS=20`
- `DIRECT_QUOTE_RECOVERY_TOP_N=8`
- `CONTRACT_REVALIDATE_TOP_N=20`
- deferred effective limit = 20
- `conflict=true`

The important conclusion is that PAPER's behavioral deferred limit is **20 because its canonical env itself is 20**. The `8` alias is not silently reducing the behavioral cap.

---

# CONFIRMED AUTHORITY TRACE

## 1. Sole behavioral authority is already `SELECTOR_MAX_DIRECT_QUOTE_CALLS`

Current main owns direct-quote budget resolution in:

`ap/contract_selector.py`

Relevant objects/functions:

- `DirectQuoteBudgetConfig`
- `_parse_positive_int_config()`
- `_resolve_direct_quote_budget_config()`
- `_new_selector_request_context()`
- `SelectorRequestContext`

Current behavior is:

### Deferred breach materialization

`DEFERRED_BREACH_MATERIALIZATION` uses the full resolved canonical budget.

- canonical env absent -> 40
- canonical env = 20 -> 20
- canonical env = 40 -> 40

### Ordinary selector request

Ordinary requests preserve the smaller pre-#401 envelope.

- canonical env absent -> 5
- canonical env = 1 -> 1
- canonical env = 20 -> 20
- canonical env = 40 -> capped at 20

**Do not change this request-kind separation in PR #441.**

## 2. Legacy aliases are diagnostic-only in the actual resolver

`_resolve_direct_quote_budget_config()` reads:

- `SELECTOR_MAX_DIRECT_QUOTE_CALLS`
- `DIRECT_QUOTE_RECOVERY_TOP_N`
- `CONTRACT_REVALIDATE_TOP_N`

If the canonical value is valid, it is the behavioral authority.

A valid alias that disagrees with canonical sets:

- `conflict=true`
- `conflict_detail=...`
- critical log `SELECTOR_DIRECT_QUOTE_BUDGET_CONFLICT`

but does **not** change the effective limit.

If canonical is absent or malformed, aliases are deliberately forbidden from becoming behavioral fallback authority; the deferred resolver uses default 40.

Therefore:

- PAPER `20/8/20` resolves behaviorally to 20 with conflict=true.
- LIVE `40/40/40` resolves behaviorally to 40 with conflict=false.

## 3. `CONTRACT_REVALIDATE_TOP_N` has one leftover import-time read that is no longer behavioral

`ap/contract_quote_revalidator.py` still contains:

- `_positive_int_env()`
- `DEFAULT_REVALIDATE_TOP_N = _positive_int_env("CONTRACT_REVALIDATE_TOP_N", 5)`

Current main comments already call this a compatibility diagnostic only.

`ap/contract_selector.py` imports `DEFAULT_REVALIDATE_TOP_N`, but current code search shows no behavioral use of that imported constant after the import.

This is unnecessary secondary configuration reading and should be removed in this PR unless Codex finds a real current-main runtime consumer during exact-head implementation.

Do **not** replace it with another alias reader.

## 4. Direct quote call enforcement is context-owned, not alias-owned

`ap/contract_quote_revalidator.py` enforces provider spending using:

- `request_context.effective_direct_quote_limit`
- `request_context.max_direct_quote_calls`
- `provider_call_counts["direct_quote_calls"]`

It does not use `DIRECT_QUOTE_RECOVERY_TOP_N` or `CONTRACT_REVALIDATE_TOP_N` as an independent behavioral cap for an active production selector context.

Do not change this provider-call enforcement logic except to carry improved config diagnostics if needed.

## 5. Current deployment preflight does NOT validate direct-quote budget configuration

`ap/selector_recovery_deploy_preflight.py` currently validates deferred lifecycle/restart rows and the deferred-materialization max-attempt configuration.

It currently reports:

- `resolved_max_attempts`
- `max_attempts_config_conflict`
- candidate/unsafe row counts

It does **not** call `_resolve_direct_quote_budget_config()` and does not fail when:

- `20/8/20` is present
- canonical is malformed
- an alias is malformed
- explicit valid aliases disagree with canonical

This is the main missing code-level deployment fence for #441.

## 6. Repository deployment manifests do not define the bot-pod selector budget values

`render.yaml` is present, but its committed services are morning/cron jobs and do not declare these selector budget variables for the active trading pods.

Therefore the observed LIVE/PAPER values are being injected from Render service environment configuration outside the repository manifest.

**Do not invent a code explanation for the 20 vs 40 divergence. The deployment environments themselves must be inspected/normalized.**

The intended target deployment after this PR is:

- one canonical value per trading pod
- same deferred selector budget on LIVE and PAPER unless a reviewed mode-specific exception exists
- no contradictory legacy aliases
- preflight evidence reports `conflict=false`

For the current production policy, normalize the active trading pods to canonical deferred budget **40** only after #439 is independently proven and only after provider-call safety review. Prefer removing the two aliases from deployment entirely; if operational compatibility requires retaining them temporarily, set them equal to canonical so diagnostics report no conflict.

---

# EXACT IMPLEMENTATION SCOPE

Expected production code scope: **3 files maximum**.

Expected tests: **2 existing files**.

No database migration. No broker code. No scanner/trigger/exit code. No workflow edit should be required because both test files are already included in `.github/workflows/p0_regression.yml`.

## Production file 1 — `ap/contract_selector.py`

### Keep unchanged

Preserve:

- `SELECTOR_MAX_DIRECT_QUOTE_CALLS` as sole behavioral authority
- ordinary request cap behavior
- deferred request full canonical behavior
- default ordinary=5
- default deferred=40
- aliases never become behavioral fallback authority
- all provider-call/quality/selector semantics

### Change

Extend `DirectQuoteBudgetConfig` so the config resolution result can explicitly distinguish:

- env absent
- env valid
- env explicitly present but malformed/non-positive
- valid aliases matching canonical
- valid aliases conflicting with canonical

Recommended fields, naming may follow existing conventions:

- parsed canonical value
- parsed direct-recovery alias value
- parsed contract-revalidate alias value
- `invalid_explicit_keys` or equivalent
- current `conflict` and `conflict_detail`

Do not force consumers to infer malformed config from string parsing a second time.

Update `_resolve_direct_quote_budget_config()` so:

1. canonical valid -> canonical remains behavioral authority
2. aliases valid but different -> `conflict=true`
3. any explicitly present malformed/non-positive canonical or alias -> surfaced as invalid configuration
4. aliases-only -> remain diagnostic-only and do not become behavioral authority
5. no env -> current defaults remain unchanged

Update `_new_selector_request_context()` diagnostics to consume parsed values from `DirectQuoteBudgetConfig` rather than reparsing raw strings via `.isdigit()`.

Carry invalid/config state into selector diagnostics so production evidence can show it without inspecting process memory.

**Do not make #441 alter selector thresholds or select a contract differently under valid config.**

### Important runtime behavior decision

Do not introduce a new exception into the live selector execution path merely because an alias disagrees with canonical. Current behavior is deterministic: canonical wins. The deployment/preflight layer should reject the inconsistent deployment.

For malformed explicit canonical configuration, preserve current selector behavior unless exact-head reproduction proves a safe startup fence exists before market work. The required fail-closed behavior for this PR is at deployment/startup validation, not an unreviewed new exception inside the money path.

## Production file 2 — `ap/contract_quote_revalidator.py`

Remove the obsolete secondary alias read if exact-head search confirms it remains unused:

- `_positive_int_env()` if no other current consumer exists in this module
- `DEFAULT_REVALIDATE_TOP_N = _positive_int_env("CONTRACT_REVALIDATE_TOP_N", 5)`

Then remove the unused `DEFAULT_REVALIDATE_TOP_N` import from `ap/contract_selector.py`.

Do **not** change:

- `_direct_quote_budget_failure()`
- provider call counting
- quote validity
- spread/liquidity/premium/affordability rechecks
- market-data routing

The active request context remains the budget authority.

## Production file 3 — `ap/selector_recovery_deploy_preflight.py`

Add direct-quote budget attestation using the production resolver from `ap.contract_selector`.

Do not duplicate parsing logic in preflight.

`run_preflight()` must return a sanitized object such as:

```json
{
  "selector_direct_quote_budget": {
    "canonical_value": 40,
    "direct_recovery_alias": 40,
    "contract_revalidate_alias": 40,
    "source": "SELECTOR_MAX_DIRECT_QUOTE_CALLS",
    "ordinary_effective_limit": 20,
    "deferred_effective_limit": 40,
    "conflict": false,
    "conflict_detail": null,
    "invalid_explicit_keys": []
  }
}
```

Do not print secrets or unrelated environment values.

`main()` must return deployment HOLD / exit 2 when either:

- direct-quote budget `conflict=true`, or
- any explicit budget env is malformed/non-positive.

Existing candidate-row and max-attempt conflict gates remain unchanged.

A clean `40/40/40` configuration must not be blocked.

A clean canonical-only `40` configuration must not be blocked.

The preflight should report both request envelopes because canonical 40 intentionally means:

- ordinary effective = 20
- deferred effective = 40

That difference is by request kind, not PAPER/LIVE mode.

---

# TEST CHANGES

## Test file 1 — `tests/test_p0_selector_direct_quote_budget_authority.py`

Preserve existing provider-budget tests.

Update/add exact config tests:

1. `{}` -> deferred default 40, ordinary default 5, no config invalidity
2. canonical `40` only -> deferred 40, ordinary 20, conflict=false
3. canonical `20` only -> deferred 20, ordinary 20, conflict=false
4. `40/40/40` -> deferred 40, conflict=false
5. `20/8/20` -> behavioral limit 20 **and** conflict=true
6. alias only (`DIRECT_QUOTE_RECOVERY_TOP_N=8`) -> deferred default 40; alias never becomes authority
7. alias only (`CONTRACT_REVALIDATE_TOP_N=20`) -> deferred default 40; alias never becomes authority
8. malformed canonical `abc`, `0`, `-2` -> invalid explicit config is surfaced
9. malformed direct-recovery alias -> invalid explicit config surfaced
10. malformed contract-revalidate alias -> invalid explicit config surfaced
11. identical env for LIVE/PAPER -> equal deferred budget, while execution_mode remains distinct
12. ordinary request remains capped at 20 even when canonical deferred value is 40
13. no direct quote call behavior begins obeying a legacy alias

Do not rewrite the tests as a hard-coded lookup table; instantiate the real resolver/context path.

## Test file 2 — `tests/test_p0_selector_recovery_deploy_preflight.py`

Add command-level tests against the actual `main()` exit contract:

1. clean canonical-only 40 -> no selector-budget config hold
2. clean 40/40/40 -> no selector-budget config hold
3. current PAPER-shaped 20/8/20 -> preflight reports conflict and exits 2
4. malformed canonical -> exits 2
5. malformed direct-recovery alias -> exits 2
6. malformed contract-revalidate alias -> exits 2
7. output contains canonical source, ordinary envelope, deferred envelope, conflict state, invalid keys
8. existing candidate-row HOLD remains independent
9. existing max-attempt config HOLD remains independent
10. preflight performs zero broker calls and zero DB mutations beyond its existing read-only query

Both test files are already in the P0 GitHub Actions workflow. Do not add another workflow entry unless current main has changed.

---

# DEPLOYMENT CHANGES — OUTSIDE REPOSITORY CODE

After code/tests are green and #439 is independently resolved, inspect each active Render trading service.

For each LIVE/PAPER trading pod:

1. Read `SELECTOR_MAX_DIRECT_QUOTE_CALLS`.
2. Read `DIRECT_QUOTE_RECOVERY_TOP_N`.
3. Read `CONTRACT_REVALIDATE_TOP_N`.
4. Confirm execution identity/client identity separately.
5. Normalize the canonical value according to approved deployment policy.
6. Prefer deleting legacy aliases if Render/service compatibility does not require them.
7. If retained, aliases must equal canonical.
8. Restart/redeploy the exact service.
9. Run preflight inside that service environment.
10. Capture sanitized output proving `conflict=false` and the intended ordinary/deferred envelopes.

Current intended parity target based on LIVE production and #401 capacity policy:

- LIVE canonical = 40
- José PAPER canonical = 40
- Tradefluence PAPER canonical = 40

Aliases should be removed or equal 40.

This deployment normalization is **not** a substitute for #439. Do not use increased capacity to hide incorrect final-reason disposition.

---

# EXPLICIT NON-GOALS

Do not change:

- selector moneyness
- delta bands
- DTE policy
- spread threshold
- OI/volume thresholds
- premium limits
- affordability/account sizing
- scanner admission
- watcher trigger behavior
- deferred retry count
- broker submit/cancel
- positions
- proof_trades
- exit/reconciliation
- PAPER live-market-data routing

PAPER should continue using the intended live market-data domain for selector truth while executing through PAPER/sandbox identity.

---

# FILE BUDGET / REVIEW TRIPWIRE

Expected implementation:

### Production

1. `ap/contract_selector.py`
2. `ap/contract_quote_revalidator.py`
3. `ap/selector_recovery_deploy_preflight.py`

### Tests

4. `tests/test_p0_selector_direct_quote_budget_authority.py`
5. `tests/test_p0_selector_recovery_deploy_preflight.py`

If Codex touches broker, scanner, watcher, exit, sizing, queue, position, proof, or database-schema files, STOP and justify before continuing.

If more than approximately five files are needed, treat that as a scope-expansion warning and audit before accepting it.

---

# CODEX EXECUTION INSTRUCTION

Work PR #441 only.

The authority retrace above is complete. Do not redo broad architectural discovery unless current main materially differs.

Implement the smallest exact-head change that:

1. preserves `SELECTOR_MAX_DIRECT_QUOTE_CALLS` as sole behavioral authority;
2. retires the obsolete import-time alias read;
3. surfaces malformed/contradictory explicit config deterministically;
4. makes selector-recovery deploy preflight HOLD on conflicting/malformed budget config;
5. proves ordinary/deferred request-kind separation;
6. proves PAPER/LIVE config parity when given identical env;
7. changes no quality gate or money-path behavior under valid configuration.

Run the focused tests and full exact-head P0 workflow, update the PR with exact head SHA/test evidence, and do not merge or deploy.

## Merge policy

P1 draft only until exact-head tests, deployment-shape audit, and production environment attestation pass. No merge/deploy without explicit approval.