# P0: Canonical regime advisory without adding a new trade-flow gate

## Status

Implementation contract only. No production behavior is implemented by this commit. Keep the pull request in draft until production code and exact-head tests are added.

Base SHA: `ffd22ca37ab0dcf874c2b463e61bee16337772fa`

## Amendment rationale

The initial specification proposed blocking legacy regime-mismatch setups when `ev_score=0` and named setup-quality evidence was absent. That blocking proposal is removed.

July 23 produced only two economic entries despite a large candidate pool. Adding another admission gate before repairing watcher readiness and selector exhaustion would repeat the exact pattern that collapsed trade flow. PEP can already be prevented at the correct seam by PR #391 because its ticker-specific PUT trigger was no longer valid immediately before broker submission.

This PR now has one job: repair the structured regime-advisory contract from PR #382 and preserve evidence. It does not decide whether PEP, BAC, or any other setup is otherwise tradeable.

## Production evidence

Current main is the merge commit for PR #382, whose contract says broad SPY regime disagreement is advisory and structured hard-risk findings remain authoritative.

Production rows created on that exact commit nevertheless carried legacy risk payloads for both BAC and PEP:

```text
approved=false
reason="PUT blocked — SPY in BULL trend"
hard_veto=<missing>
reason_code=<missing>
intel_status=RISK_VETO_OVERRIDE
contracts=1
```

The broad label failed to distinguish market context from genuine account safety. BAC later produced a strong move, while PEP entered only because PAPER ignored its ticker-specific final market-validity failure. A blanket PUT ban would lose BAC; a zero-EV admission gate could suppress still more independent setups. Neither belongs here.

## Root-cause questions implementation must answer

Before editing behavior, trace and document why an exact-PR-#382 runtime still produced the legacy payload:

1. Which concrete risk-manager class produced the row?
2. Did `ap_intelligence/agents/ap_risk_manager.py` return the structured `hard_veto/reason_code` fields?
3. Did `ap_signal_pipeline.py` drop them?
4. Did `intelligence_bridge.py` receive a legacy object from another producer/import path?
5. Did master control or durable metadata overwrite the structured payload with stale legacy evidence?
6. Was the process running a different imported module than the repository path implied by `git_commit`?

Persist producer module, class, source file, policy version, and structured/legacy classification in diagnostics.

## Required behavior

### 1. Broad SPY mismatch can never become a hard veto by legacy accident

At the final intelligence-admission boundary, normalize a payload whose only negative reason is a broad SPY direction mismatch into:

```text
reason_code=APPROVED_WITH_REGIME_MISMATCH
hard_veto=false
intel_status=REGIME_MISMATCH_ADVISORY
authoritative=false
```

This compatibility normalization is allowed only for the closed set of known broad-regime mismatch reasons. It must not reinterpret VIX extreme, buying power, contract quality, account exposure, kill switch, daily loss, malformed identity, or any structured hard veto.

A genuine hard risk rejection must carry:

```text
approved=false
hard_veto=true
reason_code=<closed-set hard-veto code>
```

Unknown or contradictory structured risk payloads fail closed with explicit diagnostics.

### 2. Remove the misleading regime data-collection override

A broad regime mismatch must not become `RISK_VETO_OVERRIDE`, must not claim `contracts=1`, and must not route through a special research-size execution lane.

It is advisory context. The setup proceeds through the ordinary downstream contracts:

- existing scanner/master-control policy;
- ranked candidate selection;
- contract selection;
- PR #391 ticker-specific final market validity;
- ordinary account sizing and hard risk limits.

A genuine hard risk veto blocks in both PAPER and LIVE. PAPER mode is not permission to override actual account safety.

### 3. Evidence is diagnostic, not a new P0 admission gate

Persist scanner score, EV score, quality components, regime context, producer identity, and final outcome for analysis.

For this PR:

- `ev_score=0` does not independently block;
- missing quality components do not independently block;
- no new tier, score, pattern, liquidity, or daily-count threshold is introduced;
- no counterfactual-only execution lane is created;
- no existing valid setup is removed from the ranked candidate pool solely because the legacy payload lacks evidence fields.

The evidence may support a later data-backed calibration PR after sufficient sessions. It cannot be promoted to live authority from one losing PEP trade and one winning BAC trade. Humanity has tried inventing universal laws from samples of two before. Results remain mixed.

### 4. Preserve truth downstream

Every intelligence decision event and relevant order metadata must retain:

- exact client ID and execution mode;
- raw risk payload;
- normalized structured payload;
- producer module/class/source file/policy version;
- scanner score and EV score separately;
- named quality components when available;
- regime context as advisory evidence;
- final authority and reason code;
- downstream PR #391 market-validity outcome by reference, not duplicated logic.

Do not rewrite historical proof trades or classify advisory regime context as a win/loss label.

## Expected production files

- `ap_intelligence/agents/ap_risk_manager.py` only if producer output is still wrong
- `ap_intelligence/ap_signal_pipeline.py` only if transport drops fields
- `intelligence_bridge.py`
- `ap/intelligence_admission_policy.py`
- `ap_master_control.py` only for final compatibility normalization
- `.github/workflows/p0_regression.yml`
- `tests/test_p0_regime_advisory_compatibility.py`

Do not change broker submit/cancel mechanics, exit logic, contract selection, scanner thresholds, EV thresholds, position sizing percentages, proof writes, queue fanout, watcher caps, or final market-validity logic.

## Required tests

1. Exact structured PR #382 advisory payload: BAC PUT in BULL SPY, `hard_veto=false`. Expected advisory allow.
2. Legacy BAC replay: reason is only SPY regime mismatch. Expected normalized advisory with no size override.
3. Legacy PEP replay: reason is only SPY regime mismatch, EV `0`, empty evidence. Expected normalized advisory at this layer; PR #391 later blocks/re-arms it because ticker-specific PUT direction is reversed.
4. Genuine hard veto with `hard_veto=true`. Expected block in both PAPER and LIVE.
5. Contradictory `approved=true, hard_veto=true`. Expected fail closed.
6. Legacy VIX extreme, buying-power, exposure, kill-switch, or daily-loss rejection. Expected no regime-advisory normalization.
7. Raw and normalized payloads preserve exact client and mode.
8. No broad SPY mismatch produces `RISK_VETO_OVERRIDE`, `contracts=1`, or a special PAPER override after repair.
9. EV and quality-evidence values are persisted but do not alter admission.
10. The compatibility layer makes no broker, order, position, proof, or queue mutation.

## Merge gates

- Root-cause trace identifies the exact legacy producer/transport seam.
- Production implementation added.
- Focused exact-head tests green.
- Exact-head P0 workflow green.
- Controlled BAC/PEP replay proves the regime layer is advisory and PR #391 owns final ticker truth.
- No merge without explicit approval from Angel.
