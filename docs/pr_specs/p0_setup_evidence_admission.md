# P0: Canonical regime advisory and setup-specific evidence admission

## Status

Implementation contract only. No production behavior is implemented by this commit. Keep the pull request in draft until production code and exact-head tests are added.

Base SHA: `ffd22ca37ab0dcf874c2b463e61bee16337772fa`

## Production evidence

Current main is the merge commit for PR #382, whose contract says broad SPY regime disagreement is advisory and structured hard-risk findings remain authoritative.

Production rows created on that exact commit nevertheless carried legacy risk payloads for both Tradefluence BAC and PEP:

```text
approved=false
reason="PUT blocked — SPY in BULL trend"
hard_veto=<missing>
reason_code=<missing>
intel_status=RISK_VETO_OVERRIDE
contracts=1
```

Execution then ignored the metadata's one-contract research shape and submitted normal account-sized PAPER quantities. More importantly, the broad SPY label hid a useful setup-specific difference:

### BAC PUT

- scanner score: `72`
- timeframe/pattern: `1d / 3-2-2`
- EV score: `44.5`
- quality evidence: `spread_score=3`, `liquidity_score=3`
- later option move observed near `+20%`

### PEP PUT

- scanner score: `72`
- timeframe/pattern: `1d / 3-2-2`
- EV score: `0.0`
- quality evidence: empty
- final ticker-specific PUT trigger was already reversed at submit
- trade became an immediate loser

A blanket rule that rejects every PUT while SPY is bullish would reject BAC along with PEP. That is the wrong abstraction. Broad regime is context. The admission decision must use ticker-specific market truth and actual setup evidence.

## Root-cause questions implementation must answer

Before editing behavior, trace and document why an exact-PR-#382 runtime still produced the legacy payload:

1. Which concrete risk-manager class produced the row?
2. Did `ap_intelligence/agents/ap_risk_manager.py` return the structured `hard_veto/reason_code` fields?
3. Did `ap_signal_pipeline.py` drop them?
4. Did `intelligence_bridge.py` receive a legacy object from another producer/import path?
5. Did master control or durable metadata overwrite the structured payload with stale legacy evidence?
6. Was the process running a different imported module than the repository path implied by `git_commit`?

Persist producer module, class, source file, policy version, and structured/legacy classification in diagnostics so this cannot become another invisible import-path séance.

## Required behavior

### 1. Broad SPY mismatch can never become a hard veto by legacy accident

At the final admission boundary, normalize a payload whose only negative reason is a broad SPY direction mismatch into:

```text
reason_code=APPROVED_WITH_REGIME_MISMATCH
hard_veto=false
intel_status=REGIME_MISMATCH_ADVISORY
authoritative=false
```

This compatibility normalization is allowed only for the closed set of known broad-regime mismatch reasons. It must not reinterpret VIX extreme, buying-power, contract-quality, exposure, kill-switch, or malformed structured hard vetoes.

A genuine hard risk rejection must carry:

```text
approved=false
hard_veto=true
reason_code=<closed-set hard-veto code>
```

Unknown or contradictory structured risk payloads fail closed with explicit diagnostics.

### 2. Eliminate the misleading regime data-collection override

A broad regime mismatch must not become `RISK_VETO_OVERRIDE` or claim `contracts=1`. It is advisory context and should proceed through normal setup-specific admission and normal sizing if every actual quality gate passes.

A genuine hard risk veto must block. Do not convert a real hard veto into a larger PAPER trade merely because the account is PAPER.

This removes the current dishonest state where metadata says one-contract research but execution submits seven contracts.

### 3. Require setup-specific evidence when the legacy compatibility path is used

For daily single-name setups entering through a legacy regime-mismatch payload, require at least one positive, attributable setup-specific evidence source before broker authority is granted.

Minimum compatibility contract:

- finite `ev_score > 0`; and
- at least one named positive quality component or completed setup-quality packet; and
- ticker-specific final market validity passes under PR #391.

When all are absent or zero:

```text
reason_code=INTEL_SETUP_EVIDENCE_INSUFFICIENT
allowed=false
authoritative=false
action=COUNTERFACTUAL_ONLY
```

Do not create a broker order, position, or proof trade. Preserve the row for counterfactual outcome tracking.

This narrow compatibility rule blocks the observed PEP shape (`ev_score=0`, empty quality evidence) while allowing BAC's positive setup evidence to continue to normal ticker-specific validation. It is not a universal EV threshold and must not be applied to structured modern approvals that carry a complete quality packet under a newer policy version.

### 4. Preserve truth downstream

Every order and decision event must retain:

- exact client ID and execution mode;
- raw risk payload;
- normalized structured payload;
- producer module/class/policy version;
- scanner score and EV score separately;
- named quality components;
- regime context as advisory evidence;
- ticker-specific market-validity result;
- final authority and reason code.

Do not rewrite historical proof trades or classify blocked counterfactual rows as PAPER losses/wins.

## Expected production files

- `ap_intelligence/agents/ap_risk_manager.py` only if producer output is still wrong
- `ap_intelligence/ap_signal_pipeline.py` only if transport drops fields
- `intelligence_bridge.py`
- `ap/intelligence_admission_policy.py`
- `ap_master_control.py` only for final compatibility/evidence authority
- `.github/workflows/p0_regression.yml`
- `tests/test_p0_setup_evidence_admission.py`

Do not change broker submit/cancel mechanics, exit logic, contract selection, scanner score thresholds, position sizing percentage, proof-trade writes, or queue fanout.

## Required tests

1. Exact structured PR #382 advisory payload: BAC PUT in BULL SPY, `hard_veto=false`, positive sizing. Expected normal advisory allow.
2. Legacy BAC replay: reason is only SPY regime mismatch, EV `44.5`, positive spread/liquidity evidence. Expected normalized advisory, then normal ticker-specific admission.
3. Legacy PEP replay: reason is only SPY regime mismatch, EV `0`, empty evidence. Expected counterfactual-only, no broker order.
4. PEP with positive setup evidence but final PUT trigger reversed. Expected blocked/re-armed by PR #391, proving this PR does not duplicate market-validity logic.
5. Genuine hard veto with `hard_veto=true`. Expected block in both PAPER and LIVE.
6. Contradictory `approved=true, hard_veto=true`. Expected fail closed.
7. Legacy VIX extreme / buying-power / exposure failure. Expected no regime-advisory normalization.
8. Raw and normalized payloads preserved with exact client/mode.
9. Blocked counterfactual row does not create position, proof trade, or filled-trade count.
10. No broad SPY mismatch produces `RISK_VETO_OVERRIDE` or `contracts=1` metadata after repair.

## Merge gates

- Root-cause trace identifies the exact legacy producer/transport seam.
- Production implementation added.
- Focused exact-head tests green.
- Exact-head P0 workflow green.
- Controlled BAC/PEP replay attached.
- No merge without explicit approval from Angel.
