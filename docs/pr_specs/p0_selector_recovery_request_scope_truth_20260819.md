# P0 SPEC — Selector recovery request-scope terminal truth

**STATUS: SPEC ONLY / HARD HOLD / DO NOT MERGE OR DEPLOY UNTIL IMPLEMENTED, REPLAYED, AND REVIEWED.**

Base: `main@cb4a687eaf59438542064b891595aee4a6cc1271`

## One job

Repair the post-PR-#401 deferred selector-recovery final-reason reduction so a **candidate-level structural skip cannot become request-level terminal truth unless the entire relevant candidate set proves that terminal condition**, and so a known truthful selector reason is not replaced by `UNKNOWN_SELECTOR_RECOVERY_FAILURE` merely because the recovery evidence reducer cannot classify every field.

This is a forward fix. **Do not revert #401.** Preserve its durable cursor/generation identity, provider-call budget, direct-quote recovery, candidate ranking, request limits, execution-mode fencing, and fail-closed behavior.

## Production evidence / why this PR exists

Historical DB comparison:

- Jul 27–29: `MONEYNESS_OUT_OF_RANGE = 0`
- Aug 6–18: `MONEYNESS_OUT_OF_RANGE = 58`
- Jul 27–29: `UNKNOWN_SELECTOR_RECOVERY_FAILURE = 0`
- Aug 6–18: `UNKNOWN_SELECTOR_RECOVERY_FAILURE = 34`
- watcher expiry also rose sharply, but this PR does **not** change watcher lifetime. First repair the upstream selector survivor semantics, then remeasure downstream expiry.

The current production reducer in `ap/selector_retry_policy.py::resolve_selector_recovery_final_reason()` contains request-level logic equivalent to:

```python
structural_values = set(skipped.values())
for structural, canonical in (
    ("STRUCTURAL_DTE_OUT_OF_RANGE", "DTE_OUT_OF_RANGE"),
    ("STRUCTURAL_MONEYNESS_OUT_OF_RANGE", "MONEYNESS_OUT_OF_RANGE"),
    ("STRUCTURAL_DELTA_OUT_OF_RANGE", "DELTA_OUT_OF_RANGE"),
    ("STRUCTURAL_TERMINAL_POLICY_REJECT", "TERMINAL_POLICY_REJECT"),
):
    if structural in structural_values:
        return canonical
```

That is safe for a single candidate, but it is not sufficient proof for an entire selector request. One far-OTM candidate can coexist with another valid-geometry candidate whose direct quote is transiently unavailable or still unattempted. Presence of one structural reject must not erase the survivor.

The same resolver currently ends with:

```python
return "UNKNOWN_SELECTOR_RECOVERY_FAILURE"
```

That fallback can overwrite a selector reason that was already known and classified, turning a retryable data condition into a fail-closed invariant terminal.

## Current-main seams to inspect before editing

1. `ap/selector_retry_policy.py::resolve_selector_recovery_final_reason()`
2. The current deferred-breach call site in `ap/contract_selector.py` that builds evidence and invokes `resolve_selector_recovery_final_reason()`
3. Existing production-shaped tests:
   - `tests/test_p0_selector_recovery_capacity_and_validity.py`
   - `tests/test_p0_selector_terminal_truth_ranked_quotes_current_main.py`
   - `tests/test_p0_selector_recovery_july27_replay.py`
   - `tests/test_p0_selector_retry_taxonomy.py`
4. `ap_execution_core.py` only as a consumer audit. **Expected production edit count here is zero.** If implementation requires changing retry ownership, broker submission, watcher lifetime, or ExecutionCore scheduling, STOP and report why.

## Binding invariant

There are two different scopes of truth:

### Candidate truth

A specific OCC row may be structurally invalid because of DTE, moneyness, delta, invalid OCC identity, side mismatch, etc. That candidate must remain skipped exactly as today, and its direct-quote provider budget must remain unspent when the structural prefilter says no quote is warranted.

### Request truth

The entire deferred selector request may be terminalized with a specific structural reason **only when the complete relevant candidate set proves that no candidate survives that structural condition**.

Therefore:

- one `STRUCTURAL_MONEYNESS_OUT_OF_RANGE` + one retryable candidate != request-level `MONEYNESS_OUT_OF_RANGE`
- all relevant candidates `STRUCTURAL_MONEYNESS_OUT_OF_RANGE` = request-level `MONEYNESS_OUT_OF_RANGE`
- one structural DTE reject + one unattempted valid-geometry OCC != request-level `DTE_OUT_OF_RANGE`
- mixed structural reasons must never be falsely collapsed to the first reason encountered merely because that reason appears in a set
- a known original selector reason must remain the fallback truth when the evidence reducer has no stronger exhaustive proof
- genuinely unknown/unmapped failure remains fail closed

## Exact implementation contract

### 1. `ap/selector_retry_policy.py` — make structural terminality exhaustive

Keep the public function signature unless a backward-compatible optional field is needed. Add a small pure helper near the resolver, for example:

```python
def _resolve_exhaustive_structural_terminal_reason(data: dict) -> str | None:
    ...
```

The helper must consume **candidate identity**, not only `set(skipped.values())`.

Use these evidence collections already present in current recovery flow:

- `structural_skip_results: {normalized_occ: structural_reason}`
- `attempted_results: {normalized_occ: result_dict}`
- `eligible_unattempted_symbols: [normalized_occ, ...]`

The live deferred handoff must preserve the complete candidate records before
validation:

- `structural_skip_records: [{symbol, skip_reason}, ...]` is the authoritative
  structural stream. It must not be truncated or collapsed into an OCC-keyed
  dict before validation; duplicate normalized OCC records with conflicting
  reasons are malformed evidence and must fail closed.
- `quality_rejection_records: [{symbol, reason}, ...]` is the per-candidate
  ordinary-quality stream. Aggregate `quality_rejections` counts are
  diagnostic only and cannot represent candidate-universe membership.

Normalize OCC keys with the same canonical whitespace/case rules already used by the selector. Do not invent ticker-only identity.

Build the known candidate universe as the normalized union of identities from
the structural, attempted, eligible-unattempted, and per-candidate quality
sources above. Then apply this rule:

> **AMENDMENT (2026-08-20, post-implementation review + real production evidence):** the three evidence collections above are **not sufficient on their own** to prove request completeness. Real production evidence proved this directly: a Supabase order row (CRM, `local_order_id=fa787602-2871-42fa-b579-df25feafb237`, `jasoncosby1@gmail.com`, LIVE, 2026-08-12) shows `direct_quote_eligible_candidates=48`, `47` candidates represented in `structural_skip_results`, and one genuinely direct-quote-attempted candidate (`CRM260814P00172500`, real outcome `DIRECT_QUOTE_ZERO_BID_ASK`) that was **not** represented in the reducer evidence used for that historical lifecycle. Historical persisted diagnostics prove the candidate was attempted but omitted from the evidence actually available to the reducer at that point in the historical flow; regardless of the specific historical cause, the fix is independent in-pass candidate accounting that prevents such an omission from ever falsely strengthening exhaustive structural proof.
>
> The implementation therefore adds a **fourth, independent accounting source**: `direct_quote_known_eligible_symbols` — every candidate symbol that reached direct-quote eligibility this pass, regardless of its eventual fate (structurally skipped, genuinely attempted, or otherwise). At the real call site this is `request_context.direct_quote_eligible_symbols`, which is populated unconditionally, in-pass, before either the structural-skip or the direct-quote-attempt branch runs — by construction it always equals `structural_skip_results.keys() ∪ {genuinely-attempted candidates}` (exactly matching CRM's real `48 = 47 + 1`).
>
> **Updated rule:** when `direct_quote_known_eligible_symbols` is supplied, every symbol in that set must be represented by the structural, attempted, eligible-unattempted, or per-candidate quality evidence before exhaustive structural proof can succeed. If any named symbol is unaccounted for by those sources, the helper must return `None` (no exhaustive proof) regardless of what the union alone would otherwise conclude. When the field is absent (`None`), this check is skipped entirely — this is an additive, opt-in strengthening of the original model, not a replacement for it; existing callers/evidence shapes that do not supply it are unaffected.

A specific canonical structural reason (`DTE_OUT_OF_RANGE`, `MONEYNESS_OUT_OF_RANGE`, `DELTA_OUT_OF_RANGE`, `TERMINAL_POLICY_REJECT`) may be returned at request level only when:

1. the known candidate universe is non-empty;
2. there are **zero** eligible-unattempted candidates;
3. there are **zero** attempted candidates that are retryable/transient/success/otherwise non-structural;
4. every candidate in the known candidate universe is represented by a structural skip; and
5. every structural skip in that exhaustive candidate set maps to the **same** canonical request-level reason; and
6. *(added 2026-08-20)* when an independent `direct_quote_known_eligible_symbols` accounting is supplied, every symbol it names is represented by the structural, attempted, eligible-unattempted, or per-candidate quality evidence — otherwise the candidate universe is not actually known to be complete, and exhaustive proof must be refused regardless of conditions 1–5.

An ordinary quality-rejected candidate (for example `OI_TOO_LOW` or
`SPREAD_TOO_WIDE`) is therefore part of the known universe and is not a
structural skip. Its presence blocks exhaustive structural terminality even
when the aggregate quality count is otherwise only one entry. The complete
structural record stream is the only input eligible for the homogeneous
structural-reason check; conflicting duplicate OCC records must never be
silently overwritten.

If any candidate survives those conditions, return `None` from the helper and continue the existing reducer.

If the set is fully structural but contains mixed structural reasons, do **not** pick the first matching reason. Prefer an already-truthful selector fallback (Step 2 below). If no known fallback exists, retain fail-closed unknown behavior rather than manufacturing a specific false reason.

Do not alter `_structural_direct_quote_skip()` thresholds. Do not make an out-of-band candidate eligible for quote recovery. This PR changes only **reduction scope**, not candidate admission.

### 2. Preserve known original selector truth before `UNKNOWN_SELECTOR_RECOVERY_FAILURE`

Add an optional evidence field named exactly one of:

- `original_selector_reason`, or
- `fallback_selector_reason`

Choose one name and use it consistently at the `contract_selector.py` call site and tests.

Immediately before the final unknown fallback:

1. normalize the supplied original selector reason using the existing taxonomy contract;
2. prove it is a known reason by using existing policy lookup/classification, not a new hand-written allowlist;
3. if it is known, return it unchanged/canonicalized;
4. if it is blank, malformed, or classified `UNKNOWN_FAIL_CLOSED`, return `UNKNOWN_SELECTOR_RECOVERY_FAILURE` exactly as today.

There is one binding exception to that known-reason fallback: the canonical
structural request-level reasons `DTE_OUT_OF_RANGE`, `MONEYNESS_OUT_OF_RANGE`,
`DELTA_OUT_OF_RANGE`, and `TERMINAL_POLICY_REJECT` are governed exclusively by
the exhaustive structural-proof helper. Even if taxonomy lookup classifies one
of these values as known, `fallback_selector_reason` MUST NOT resurrect it at
Step 8.5 after exhaustive structural proof has failed. Such a structural
reason may be returned only when the exhaustive helper already proved the
complete, homogeneous candidate set; otherwise the resolver remains fail-closed
as `UNKNOWN_SELECTOR_RECOVERY_FAILURE`. Known non-structural fallback reasons
continue to follow the preservation rule above.

Do not convert a known terminal quality reason into retryable data. Do not convert known retryable data into terminal quality. The reducer may choose a **stronger proven reason** earlier in its existing precedence; this fallback applies only when the reducer otherwise reaches unknown.

### 3. `ap/contract_selector.py` — thread the truthful pre-reducer reason into the deferred resolver call

The current production tree has one deferred-breach branch that imports and calls `resolve_selector_recovery_final_reason()`.

At that call site:

- capture the selector's already-established canonical reason **before** the recovery reducer can overwrite it;
- pass it as the new fallback evidence field;
- do not use `operational_reason` (for example `SELECTOR_REQUEST_BUDGET_EXHAUSTED`) as a substitute for canonical selector truth when those fields intentionally differ;
- preserve `canonical_selector_reason`, `last_observed_selector_reason`, `selector_terminal_reason`, and operational diagnostics as separate fields.

If evidence-building logic is ever duplicated across deferred branches, a tiny local helper may be extracted only if behavior remains identical. Do not refactor the selector broadly.

### 4. ExecutionCore consumer behavior

No new retry owner. No new thread. No retry-count increase. No watcher timeout change. No OSM status vocabulary change.

Expected behavior after this PR:

- retryable data reason -> existing durable retry schedule
- proven exhaustive structural terminal -> existing terminalization path
- known terminal quality/policy reason -> existing terminalization path
- genuinely unknown -> existing fail-closed path

## Mandatory fail-first tests

Create `tests/test_p0_selector_recovery_request_scope_truth_20260819.py` and register it in the P0 workflow only after tests are meaningful.

Minimum required cases:

1. one moneyness structural skip + one eligible unattempted OCC -> NOT `MONEYNESS_OUT_OF_RANGE`
2. one moneyness skip + one attempted `DIRECT_QUOTE_ZERO_BID_ASK` transient -> NOT `MONEYNESS_OUT_OF_RANGE`; preserve retryable data truth
3. all candidate OCCs moneyness structural skips -> `MONEYNESS_OUT_OF_RANGE`
4. all candidate OCCs DTE structural skips -> `DTE_OUT_OF_RANGE`
5. all candidate OCCs delta structural skips -> `DELTA_OUT_OF_RANGE`
6. mixed moneyness + DTE structural skips with known fallback `NO_CONTRACT_AFTER_FILTERS` -> preserve known fallback, do not pick whichever set member is encountered first
7. mixed structural skips with no known fallback -> fail closed as unknown, not fabricated moneyness/DTE/delta
8. known original `DIRECT_QUOTE_ZERO_BID_ASK` reaches reducer fallback -> preserved
9. known original `SELECTOR_REQUEST_BUDGET_EXHAUSTED` -> preserved
10. known original `OI_TOO_LOW` -> preserved terminal quality
11. blank original -> `UNKNOWN_SELECTOR_RECOVERY_FAILURE`
12. unmapped original -> `UNKNOWN_SELECTOR_RECOVERY_FAILURE`
13. evidence insertion order permutations produce identical final reason
14. duplicate OCC formatting/case cannot create fake extra candidates
15. ordinary selector request remains behaviorally unchanged; reducer remains deferred-only
16. LIVE/PAPER use same selector-quality truth for the same candidate evidence while retaining distinct execution modes
17. exact Jason LIVE-shaped deferred plan, `client_id=jasoncosby1@gmail.com`, one structural outlier plus transient viable OCC -> durable retry path, zero broker submit/cancel
18. exact Jason LIVE-shaped all-structural set -> terminal path, zero broker submit/cancel
19. retry attempt limits/cutoff are unchanged
20. direct-quote budget counts are unchanged by final-reason reduction

Add explicit static/behavioral assertions that this PR introduces:

- zero new `submit_order` / `place_order` / `cancel_order` authority
- zero position mutations
- zero proof-trade mutations
- zero queue writer beyond existing result propagation
- no `client_id` or `execution_mode` defaulting

## Replay requirement

Before requesting review, replay at least three real post-Aug-6 failure shapes from production metadata, preferably one each from the observed DDOG/PANW/WDAY/CRM/PLTR class. Strip secrets but preserve:

- client/mode
- ticker/direction
- trigger/underlying anchor
- ranked candidate symbols
- structural skip map
- attempted result map
- eligible unattempted symbols
- final reason before/after

The replay must prove the PR restores survivors only where another candidate was genuinely viable/retryable. It must **not** turn an all-invalid candidate set into a trade.

## Non-goals / forbidden shortcuts

- Do not revert #401 wholesale.
- Do not remove moneyness, DTE, delta, spread, OI, volume, premium, earnings, or affordability gates.
- Do not increase direct-quote budgets.
- Do not increase retry counts or watcher lifetime.
- Do not make unknown reasons retryable by default.
- Do not edit Master Control limits.
- Do not touch broker submit/cancel.
- Do not change sizing.
- Do not repair Jason `client_state.day_key` here.

## Expected size

Production scope should be **small**: normally 2 production files (`ap/selector_retry_policy.py`, `ap/contract_selector.py`) and 1 focused test file plus CI registration/spec. Expected implementation is roughly tens to low hundreds of production LOC, not a subsystem rewrite.

If Claude needs `ap_execution_core.py`, watcher, OSM, queue, or DB schema changes to make these tests pass, **STOP**. That means the proposed scope has drifted away from the proven defect.

## Required delivery back to operator

Return:

1. exact base SHA and final head SHA;
2. exact changed filenames;
3. fail-first test count and names;
4. post-fix focused/adjacent counts;
5. exact-head P0 CI result;
6. three production replays with before/after terminal reason;
7. diff-level money-path audit;
8. proof that all structural quality thresholds are byte-for-byte/constant-for-constant unchanged;
9. proof no new broker submit/cancel authority exists;
10. fresh recommendation: MERGE / HOLD / HARD HOLD.

**Do not merge or deploy from this spec alone.**
