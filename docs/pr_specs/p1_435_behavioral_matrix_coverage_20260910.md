# PR #435 Behavioral Matrix Coverage Summary

Amendment source: `docs/pr_specs/p1_435_regime_pullback_architecture_amendment_20260909.md`
(required behavioral tests § — 25 scenarios).

Local artifact path: `/workspace/pr435/` (parent pushes; this workspace does not push).

Fixture modules:

- `tests/test_p0_435_behavioral_matrix.py`
- `tests/test_p0_435_aapl_opening_replay_shape.py`
- `tests/test_p0_435_money_path_non_authority.py`

Related prior coverage retained under `/workspace/pr435/`:

- `tests/test_p0_435_market_structure_freeze.py`
- `test_p0_435_canonical_identity.py`
- `test_p0_435_fvg_as_of_cache.py` / `test_fvg_as_of_cache.py`

## Coverage of the amendment's 25 scenarios

| # | Scenario | Status | Where |
|---|----------|--------|-------|
| 1 | Bullish continuation with no pullback | **COVERED** | `test_p0_435_behavioral_matrix` — CALL clean continuation → `READY_NOW` |
| 2 | Bullish setup with active pullback | **PARTIAL** | `pullback_candidate_features` research freeze at breach (observe_only); full `regime_pullback_v1.pullback_state` still deferred |
| 3 | Bearish symmetric pullback | **PARTIAL** | PUT symmetric clean continuation covered; explicit pullback-state enum deferred with #2 |
| 4 | First FVG touch | **PARTIAL** | Market-structure freeze relationship enums exercised in `test_p0_435_market_structure_freeze` / matrix zone-hash; dedicated touch_count=1 lifecycle deferred |
| 5 | Second touch after exact persisted first touch | **DEFERRED** | Requires persisted FVG touch lineage store + exact first_touch_ts authority |
| 6 | Deep penetration then reclaim | **DEFERRED** | Needs `penetration_pct` / `reclaim_state` producer on tip |
| 7 | Gap broken/invalidated | **PARTIAL** | `broken_reclaimed` lifecycle normalization exists in freeze; dedicated broken→invalidated behavioral case deferred |
| 8 | Wick-only trigger breach | **COVERED** | `classify_wick_vs_body_breach` + `test_p0_435_deferred_gaps_closed`; readiness waits on wick-only first breach |
| 9 | Completed 5m reclaim | **PARTIAL** | 5m confirmation MISSING honesty + follow_through path covered; explicit reclaim-of-trigger posture deferred |
| 10 | Completed 15m follow-through | **COVERED** | Matrix READY_NOW requires `fifteen.follow_through`; AAPL weak path proves opposing follow-through denial |
| 11 | Late extension / little remaining target distance | **COVERED** | Heavy extension → `REBREACH_PREFERRED`; target reached / `remaining_r <= 0` → `INVALID` |
| 12 | Opposing 4H wall directly in path | **COVERED** (prior) | `test_p0_435_market_structure_freeze.test_opposing_wall_ahead_for_call` |
| 13 | Missing 5m → explicit `MISSING` | **COVERED** | `_directional_confirmation([])` + matrix missing-5m case; never fabricates follow_through |
| 14 | Stale 4H/1H evidence → explicit `STALE` | **COVERED** | `assess_htf_candle_freshness` + breach_evidence/component wiring; fixture in `test_p0_435_deferred_gaps_closed` |
| 15 | Future candle excluded | **COVERED** (prior) | PIT `completed_bars_as_of` in market-structure freeze tests / freeze helper |
| 16 | Malformed/non-finite candle cannot improve classification | **PARTIAL** | `_finite_number` / confirmation skips bad rows; dedicated improve-classification regression deferred |
| 17 | First-touch history missing → touch count `UNKNOWN` | **DEFERRED** | Touch-count UNKNOWN contract awaits FVG lifecycle persistence fields |
| 18 | Runtime/restart materializer identical frozen classification | **PARTIAL** | Identical frozen market_structure → identical zone ids/hash covered; full restart/materializer parity deferred |
| 19 | Duplicate enqueue remains idempotent | **DEFERRED** | Store idempotency lives in `intelligence_snapshot_store` (memory/DB); not re-proven in this pack |
| 20 | Classifier exception leaves execution path untouched | **PARTIAL** | Money-path handoff never raises on pool/submit failure; full classifier-exception fence at call site deferred |
| 21 | Observe-only `WAIT_*` cannot delay selector/broker | **COVERED** | All readiness outputs assert `observe_only=True`, `affected_eligibility=False` |
| 22 | Observe-only `REJECT_CANDIDATE` cannot terminalize | **PARTIAL** | Dual-emits `SETUP_INVALID` candidate beside `INVALID`; non-authority flags proven; full REJECT_CANDIDATE posture name still advisory |
| 23 | Observe-only `ENTER_NOW_CANDIDATE` cannot unblock submit | **PARTIAL** | Dual-emits `READY_NOW_CANDIDATE` beside `READY_NOW`; non-authority flags proven |
| 24 | Exact `client_id` / `execution_mode` / signal / local order unchanged | **COVERED** (prior) | `test_p0_435_canonical_identity` + lifecycle freeze strip |
| 25 | Zero broker submit/cancel, zero position/proof mutation | **COVERED** | `test_p0_435_money_path_non_authority` — disabled / capacity exhausted / enqueue error shapes; no submit/cancel/eligibility mutation implied |

## Extra proofs in this pack (beyond the numbered 25)

- First-breach vs rebreach lineage comparison under identical confirmation evidence.
- AAPL LIVE PUT opening-window first-breach shape: pattern `2-3`/`1d` → canonical `2-3-2`, `FIRST_30_MINUTES` timing, opposing continuation → `WAIT_CONFIRMATION` or `REBREACH_PREFERRED`, no fabricated option quotes/outcomes.
- Regime disagreement, when exposed on evidence as advisory metadata, does not hard-veto readiness (`READY_NOW` retained; `affected_eligibility=False`).

## Counts

- **COVERED**: 1, 8, 10, 11, 12, 13, 14, 15, 21, 24, 25 (+ extras)
- **PARTIAL**: 2, 3, 4, 7, 9, 16, 18, 20, 22, 23
- **DEFERRED**: 5, 6, 17, 19

Deferred items primarily wait on FVG touch-lineage persistence, full `regime_pullback_v1` state table, and store/restart integration harnesses — not on money-path authority (which remains observe-only). See `DEFERRED_GAPS_CLOSED.md`.
