# MERGE FILE — Selector Candidate Audit (Item 3)

**Status:** PR for review. EVIDENCE ONLY — no contract-selection behavior change.
**Branch:** `feat/selector-candidate-audit`
**Files:** `ap/contract_selector.py`, `ap_execution_core.py`, new
`tests/test_selector_candidate_audit.py`.

**Did NOT touch:** selection ranking logic, exits, sizing, scoring, client_runner,
proof logger, dashboard, rearm, retry.

## What it does
When a contract choice looks wrong (AVGO/NFLX/MSFT), we can now prove whether the
selector picked the best available contract or skipped a better one — because the
top-3 candidates are persisted into `orders.meta.selector_candidate_audit`.

### Audit structure (per ENTRY order)
```
selector_candidate_audit:
  selected_contract, selected_reason, underlying_price, candidates_considered
  top_candidates: [ {rank, contract, strike, expiration, dte, bid, ask, mid,
                     last, spread_pct, delta, delta_reason, moneyness_pct,
                     distance_from_underlying, volume, open_interest, rank_score} x3 ]
  rejected_candidate_reasons: { reason_code: count }  (sorted desc)
```

### How it's wired (evidence only)
1. `SelectedContract` gains a `candidate_audit: Optional[dict] = None` field
   (in `to_dict()` too). Default None — nothing else changes.
2. `_build_candidate_audit()` builds the top-N payload from the already-sorted
   `scored` list + the existing `_rejections` counter. Never raises.
3. `select()` attaches it to the returned `SelectedContract` after the final
   contract is locked (after the cheap-contract upgrade pass), so it reflects the
   ACTUAL winner.
4. `execution_core` persists it into `orders.meta` via the OSM's existing
   `update_order_meta()` (non-destructive JSONB merge) after a successful submit.

Top-N is configurable via `SELECTOR_CANDIDATE_AUDIT_TOP_N` (default 3).

## Why persist at submit, not at creation
`submit_existing_entry` does not write meta (meta is set at order creation, before
breach-time contract selection runs). The audit only exists after breach-time
selection, so we persist it right after the successful submit using the same
non-destructive `update_order_meta` merge the watcher audit uses.

## Tests
`tests/test_selector_candidate_audit.py` — 7 tests: top-N limit, field extraction
(spread/moneyness/distance), missing-delta handling, rejected-reason ordering,
empty-scored safety, zero-underlying safety, and the SelectedContract field/to_dict.
P0 regression (29) still passes.

## Verification SQL (after a session)
```sql
SELECT symbol, contract,
       meta->'selector_candidate_audit'->>'selected_contract'  AS selected,
       meta->'selector_candidate_audit'->>'candidates_considered' AS n_considered,
       jsonb_array_length(meta->'selector_candidate_audit'->'top_candidates') AS n_top,
       meta->'selector_candidate_audit'->'rejected_candidate_reasons' AS rejected
FROM orders
WHERE kind='ENTRY' AND meta->'selector_candidate_audit' IS NOT NULL
  AND created_ts > NOW() - INTERVAL '2 days'
ORDER BY created_ts DESC;
```

## Rollout
Merge anytime — it's additive evidence. No env change needed (default top-3).
