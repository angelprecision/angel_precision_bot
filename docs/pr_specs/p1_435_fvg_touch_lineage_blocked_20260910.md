# FVG touch lineage — BLOCKED (no durable authority)

**Status:** observe-only implementation **not** landed.  
**Gaps:** amendment behavioral matrix **#5** (second touch after persisted first), **#6** (deep penetration then reclaim), **#17** (missing history → touch count `UNKNOWN`).  
**Workspace:** `/workspace/pr435/` only. HARD HOLD — no invent, no push, no merge, no eligibility mutation.

## Decision

Searched the local PR #435 tree for durable FVG touch / retest / opposing-zone acceptance / `zone_id` **touch lineage** on signals, evidence, watcher/runtime fields, snapshot store, and tip docs.

**Durable touch-lineage authority does not exist.** Therefore `resolve_fvg_touch_lineage` was **not** implemented. Fabricating `touch_count=1`, synthetic `first_touch_ts`, or fake prior-touch history from a single freeze pass would violate the amendment contract (“Do not infer `touch_count=1` from missing history. Missing history means unknown.”).

Locking test: `tests/test_p0_435_fvg_touch_lineage.py`.

## What already exists (geometry / relationship — NOT touch lineage)

| Asset | Present fields | Why insufficient for #5/#6/#17 |
|-------|----------------|--------------------------------|
| `ap.fair_value_gap.FairValueGap` | `direction`, `low`/`high`/`midpoint`, `fill_pct`, `status` (`unfilled`/`partial_fill`/`midpoint_touched`/`filled`), `mitigated` | Single-pass fill from future candles in the **same** detect call — not cross-event touch history |
| `freeze_fvg_zone` / `deterministic_zone_id` | Stable `zone_id`, geometry, `lifecycle_status`, `alignment`, PIT `data_as_of` | Zone **identity** at freeze time only; no `touch_count` / timestamps |
| Market-structure relationship freeze | Enums incl. `ALIGNED_RETEST_ZONE_BEHIND`, `INSIDE_OPPOSING_*`, `OPPOSING_WALL_ACCEPTED`/`REJECTED`, `relationship_zone_id` | Instantaneous price↔zone relationship; does not record prior touches of that `zone_id` |
| `resolve_breach_lineage` | `INITIAL_BREACH` / rebreach enums | **Breach** lineage (trigger crossings), not **FVG zone** touch lineage |
| `classify_wick_vs_body_breach` | Wick vs body on the breach bar | Breach confirmation style; not FVG touch kind history |
| `fvg_telemetry` / score_breakdown.`fvg` | Compact opposing/aligned diagnostics | Observe-only score path; no persisted per-zone touch ledger |
| Snapshot store / `ap.db` | Context snapshot persistence | No FVG touch-lineage table or columns |

Also checked: `harness/`, `tests/`, `DEFERRED_GAPS_CLOSED.md`, `BEHAVIORAL_MATRIX_SUMMARY.md`, `p1_435_regime_pullback_architecture_amendment_20260909.md`. No `docs/pr_specs/` tree under this workspace.

## Missing fields (authority required before implement)

From the amendment FVG / structural-state contract — **none of these are produced or persisted** on signals, `breach_evidence`, frozen zones, or watcher/runtime exports today:

```text
touch_count
first_touch_ts
last_touch_ts
penetration_pct
reclaim_state
rejection_state
touch_kind          # wick | body | retest | opposing_acceptance (classifiable only with authority)
```

Related production gaps for #6:

```text
persisted penetration_pct / reclaim_state producer on tip
```

Honesty statuses when inputs absent (once authority lands): `UNKNOWN` / `MISSING` — never fabricate counts or timestamps.

## What a production export would unlock

To unblock observe-only `resolve_fvg_touch_lineage` (with `observe_only=true`, `affected_eligibility=false`):

1. **Durable per-`zone_id` touch ledger** (or equivalent watcher/runtime export) carrying at least:
   - `zone_id` (compatible with `deterministic_zone_id` or an attested mapping)
   - `touch_count`, `first_touch_ts`, `last_touch_ts`
   - optional `touch_kind` / OHLC evidence for wick vs body vs retest vs opposing acceptance
   - `penetration_pct`, `reclaim_state`, `rejection_state` when available
   - `source` / `as_of` / signal or symbol key for PIT join
2. **Join path** into BREACH materialization so lineage freezes beside `breach_evidence` / `market_structure` without mutating eligibility.
3. **Explicit UNKNOWN/MISSING** when ledger rows or candles are absent — second-touch cases (#5) only when a **persisted** first touch exists for the same `zone_id`.

Until that export (or in-repo store with the same fields) is present in tip, this gap stays **BLOCKED**.

## Explicit non-actions

- Did **not** invent production touch history from candle replay alone.
- Did **not** wire a fake `fvg_touch_lineage` blob that claims known `touch_count`.
- Did **not** change eligibility, money path, client_id, or execution mode.
- Did **not** push / merge / deploy.

## Cross-refs

- `DEFERRED_GAPS_CLOSED.md` — still deferred rows #5 / #6 / #17
- `BEHAVIORAL_MATRIX_SUMMARY.md` — matrix rows 5, 6, 17
- `p1_435_regime_pullback_architecture_amendment_20260909.md` — FVG structural-state contract
