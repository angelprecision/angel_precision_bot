# PR #435 — Deferred behavioral gaps closed (local `/workspace/pr435/`)

Branch tip reference: `eed1da09` / `refs/heads/spec/p1-breach-setup-intelligence-20260811`.
This workspace writes local artifacts only — **no clone/push/merge**.

All additions remain **`observe_only=true` / `affected_eligibility=false`**. No entry authority.

## Closed in this amendment pack

| # | Gap | Status | Where |
|---|-----|--------|-------|
| 8 | Wick-only vs body-confirmed breach | **CLOSED** | `classify_wick_vs_body_breach` in `ap/intelligence_breach_market_structure.py`; wired into `breach_evidence.wick_vs_body_breach` + readiness diagnostics; wick-only on first breach → `WAIT_CONFIRMATION` |
| 14 | Stale 4H/1H evidence → explicit `STALE` | **CLOSED** | `assess_htf_candle_freshness` / `assess_breach_htf_freshness`; mirrored on `breach_evidence.htf_freshness`, `market_structure.htf_freshness`, and payload `component_statuses.four_hour` / `four_hour_fvg` / `one_hour_fvg` |
| 2 (partial) | Pullback-candidate features at breach | **CLOSED (research freeze)** | `freeze_pullback_candidate_features` → `pullback_candidate_features` with `observe_only=true`, `records_future_pullback_outcomes=false`. Does **not** invent future pullback completion/outcomes. Full `regime_pullback_v1.pullback_state` producer still deferred. |
| 22/23 (partial) | Entry-timing candidate enum alignment | **CLOSED (dual-emit)** | `map_entry_timing_candidate` dual-emits amendment enums beside legacy `READY_NOW` / `WAIT_CONFIRMATION` / `REBREACH_PREFERRED` / `INVALID` without affecting eligibility |

### Amendment entry-timing candidate enums (dual-emitted)

- `READY_NOW_CANDIDATE`
- `WAIT_PULLBACK_CANDIDATE`
- `WAIT_FVG_RETEST_CANDIDATE`
- `WAIT_OPPOSING_FVG_ACCEPTANCE_CANDIDATE`
- `REBREACH_PREFERRED`
- `SETUP_INVALID`

Legacy classifications remain on `classification` for back-compat. Candidate lives on
`entry_timing_candidate` / `entry_readiness_observe_only.entry_timing_candidate` /
payload `entry_timing_candidate_observe_only`.

### Pullback-candidate feature fields (at breach only)

`extension`, `percent_move_consumed`, `wick_vs_body`, `fifteen_minute_state`,
`five_minute_state`, `nearest_aligned_fvg_behind`, `opposing_ahead`,
`runway_to_opposing_wall`, `vwap_status` / `volume_status` (**MISSING honesty**),
`first_vs_rebreach`, `htf_freshness` summary. No outcome / MFE / PnL fields.

## Still deferred (not in this pack)

| # | Gap | Why still deferred |
|---|-----|--------------------|
| 5 | Second FVG touch after persisted first | Needs durable FVG touch-lineage store |
| 6 | Deep penetration then reclaim | Needs persisted `penetration_pct` / `reclaim_state` producer |
| 17 | Touch count `UNKNOWN` when history missing | Same persistence contract |
| 19 | Duplicate enqueue idempotency (DB) | Lives in `intelligence_snapshot_store` / Postgres harness |
| 2 (full) | Versioned `regime_pullback_v1` classifier | Features freeze lands first; full regime/pullback_state table still open |

## Files touched (local)

- `ap/intelligence_breach_market_structure.py` — wick/body, HTF freshness, pullback freeze helpers
- `ap/intelligence_context_materializer.py` (+ root copy) — wiring, dual-emit enums, remaining-opportunity aliases, STALE components
- `tests/test_p0_435_deferred_gaps_closed.py` — fixture coverage
- `DEFERRED_GAPS_CLOSED.md` — this note

## Verification

```text
py_compile clean on materializer + market_structure + new tests
PYTHONPATH=/workspace/pr435 pytest tests/test_p0_435_deferred_gaps_closed.py → 16 passed
Prior fixture pack (behavioral / market_structure / AAPL / money-path / identity) still green
```

**HARD HOLD / observe-only / no entry authority.**
