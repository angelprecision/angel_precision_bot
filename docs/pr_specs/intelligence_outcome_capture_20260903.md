# SPEC: Intelligence outcome-capture pipeline — populate the edge dataset

**STATUS: HARD HOLD — SPEC ONLY. IMPLEMENTATION REQUIRED. DO NOT MERGE OR DEPLOY.**

Base: `main@d3c6185`

---

## PRODUCTION PROBLEM

The intelligence infrastructure exists but is not populated, so no edge model,
tuning, or expectancy analysis is possible. Verified against production Supabase
(project `jhawzqnhcihevkhehogm`) on 2026-09-03:

| Table | Rows | Feature cols | Outcome cols |
|---|---|---|---|
| `signal_outcomes` | 329 | `score`/`grade` NULL on ~all rows; `pattern` blank on 324/329 | `win`/`hit_target`/`hit_stop` present |
| `ap_signal_option_outcomes` | 2 | present on the 2 rows | `max_option_move_pct`, `hit_30/50/100pct` = 0 populated |

Realized win rate on the 328 labeled `signal_outcomes` rows is **41%**. There is
no way to ask "do higher-score / specific-pattern / specific-regime signals win
more?" because the discriminating features are not persisted next to outcomes.

## ROOT CAUSES (code-level, traced on `main@d3c6185`)

1. **Feature-at-signal is never written by the live path.**
   `ap_signal_store.insert_option_outcome()` (writes `ap_signal_option_outcomes`)
   is only called from `ap_options_intelligence.evaluate_contract()`. That
   function is **dead code** — `ap/contract_selector.py` header (lines 11–18)
   states the live gate is `APContractSelectionEngine.select()` and that
   `evaluate_contract()` "is now dead code: it is not imported or called anywhere
   in the live system." The live selector computes `spread_pct`, `delta`,
   `open_interest`, `volume`, `mid`, `dte` (all fields on `SelectedContract`,
   `ap/contract_selector.py:2345`) but never persists them against a `signal_id`.

2. **Outcome fields on `ap_signal_option_outcomes` have no writer at all.**
   `peak_mark`, `trough_mark`, `max_option_move_pct`, `minutes_to_option_peak`,
   `hit_30pct/50pct/100pct` require a post-entry option-price tracker. Grep for
   any such job returns nothing. No process fills these columns.

3. **`signal_outcomes` loses pattern/score at exit time.**
   The single writer is `ap_feedback_loop.record_outcome()`, called only from
   `ap_execution_core._finalize_proof` (~line 11613) on broker-confirmed fills.
   It reads `pattern`/`score` from the `signal` dict handed in at *exit* time.
   Production shows these arriving NULL/blank, i.e. the staged `signal` object
   at finalize does not carry the original scoring metadata. Because
   `record_outcome` fires only on confirmed fills (~2–3% of signals) AND the
   stale-close bug (addressed by #566) was zeroing some fills without economics,
   the labeled set is both tiny and under-featured.

## NON-GOALS / EXPLICIT EXCLUSIONS

- **No changes to selection, sizing, scoring, risk, or triggering logic.** This
  PR is capture-only. `SelectedContract` and `select()` decision behavior are
  untouched; the only addition is a persistence side-effect that cannot alter
  the returned contract.
- **No collision with #519, #566, #568, #569.** This PR does not modify
  `ap_exit_engine.py`, `ap/order_state_machine.py`, `ap/exit_*`,
  `ap_execution_core` deferred/selector-recovery seams, or
  `ap_overnight_reeval.py`. Touched files are disjoint from those PRs (see
  Changed Files).
- **No revival of `evaluate_contract()`.** It stays dead. We wire capture into
  the live selector, not the dead gate.
- **No broker submit / cancel path added.** Capture is read-only w.r.t. the
  broker.

## DESIGN

Three capture points, each fail-soft (a capture exception must never propagate
into the trading path — wrap in try/except, log, continue).

### A. Feature-at-signal write (fixes root cause 1)
At the point `APContractSelectionEngine.select()` finalizes a winning
`SelectedContract`, persist a features row keyed by `signal_id` into
`ap_signal_option_outcomes` via the existing `insert_option_outcome()` upsert.
Map: `spread_pct→spread_pct_at_signal`, `delta→delta_at_signal`,
`open_interest→oi_at_signal`, `volume→volume_at_signal`, `mid→mark_at_signal`,
plus `contract_symbol`, `strike`, `expiration`, `option_type`, `chain_grade`
(from selection metadata). `iv_at_signal` is captured **only if** the chain row
already carries it (`greeks.mid_iv`/`greeks.smv_vol`); do not fetch it — if
absent, write NULL. This write is an UPSERT on `signal_id`, so it is idempotent
and safe on selector retries.

### B. Carry scoring metadata through to `signal_outcomes` (fixes root cause 3)
Ensure the `signal` dict staged for execution retains `pattern`, `side`,
`score`/`signal_score`, `timeframe`, `regime`, `confluence` from signal creation
through to `_finalize_proof`. Preferred implementation: at stage time, snapshot
the scoring fields into `staged["signal_meta"]` and have `record_outcome` prefer
those. Do not recompute or infer — carry the values already produced upstream.
If a field was never produced upstream, write NULL, never a guess.

### C. Post-entry option-path tracker (fixes root cause 2)
Add a bounded, idempotent tracker that, for each filled position with a
`signal_id`, samples the option mark on the existing monitoring cadence and
maintains running `peak_mark`, `trough_mark`, `minutes_to_option_peak/trough`,
`max_option_move_pct`, `max_option_drawdown_pct`, and the `hit_30/50/100pct`
booleans, upserting to `ap_signal_option_outcomes` on `signal_id`. It must reuse
the price feed already polled by the exit monitor (no new quote budget), run only
during market hours for open positions, and finalize `close_mark` when the
position closes. If the tracker cannot sample (feed unavailable), it records
nothing and leaves prior values intact — never writes zeros.

## FAIL-FIRST TESTS (must fail on `main`, pass after implementation)

1. `test_selector_persists_feature_row_on_win` — drive `select()` to a winning
   contract with a `signal_id`; assert a row appears in `ap_signal_option_outcomes`
   with non-NULL `spread_pct_at_signal`, `delta_at_signal`, `oi_at_signal`,
   `mark_at_signal`. **Fails on main** (no such write).
2. `test_selector_feature_write_is_idempotent` — call select twice for the same
   `signal_id`; assert exactly one row (upsert), latest features win.
3. `test_selector_capture_exception_never_breaks_selection` — monkeypatch
   `insert_option_outcome` to raise; assert `select()` still returns the same
   `SelectedContract`. Guards the non-goal.
4. `test_outcome_carries_pattern_and_score` — stage a signal with
   `pattern='2-1-2', score=86, regime='trend'`; run the finalize path; assert the
   `signal_outcomes` row has those exact values, not NULL. **Fails on main.**
5. `test_option_path_tracker_records_peak_trough_and_thresholds` — feed a
   synthetic mark series (e.g. +12% → +34% → −5%); assert `peak_mark`,
   `max_option_move_pct≈34`, `hit_30pct=true`, `hit_50pct=false`,
   `minutes_to_option_peak` set. **Fails on main** (no tracker).
6. `test_tracker_writes_nothing_when_feed_unavailable` — feed returns None;
   assert no zero rows written, prior values intact.
7. `test_no_write_when_signal_id_missing` — selector/tracker with no signal_id;
   assert no orphan rows.

## ACCEPTANCE / VERIFICATION QUERIES (post-deploy, 3–5 trading days)

- `SELECT COUNT(*) FILTER (WHERE spread_pct_at_signal IS NOT NULL)` on
  `ap_signal_option_outcomes` climbs with approved signals (not stuck at 2).
- `SELECT COUNT(*) FILTER (WHERE max_option_move_pct IS NOT NULL)` climbs with
  filled positions (tracker working).
- `signal_outcomes.score`/`pattern` non-NULL on new rows.
- Then the first real edge query becomes answerable:
  `win% and avg option_pnl_pct GROUP BY pattern, score_band, regime`.

## CHANGED FILES (planned; disjoint from #519/#566/#568/#569)

| File | Purpose |
|---|---|
| `ap/contract_selector.py` | Add fail-soft feature-capture side-effect at winner finalize (capture only; no decision change) |
| `ap_feedback_loop.py` | Prefer carried `signal_meta` scoring fields in `record_outcome` |
| `ap_execution_core.py` | Snapshot scoring metadata into `staged["signal_meta"]` at stage time (no logic change) |
| `ap_signal_store.py` | Extend `insert_option_outcome` for tracker outcome-field upserts if needed |
| `ap/option_path_tracker.py` | NEW — bounded, feed-reusing post-entry option tracker |
| `tests/test_intelligence_outcome_capture.py` | NEW — fail-first suite (7 tests above) |

## GOVERNANCE NOTE

This is the data foundation for edge work. It does not itself change win rate or
fill rate. The stated 5–7 fills/day and win-rate goals depend on ANALYSIS this
pipeline makes possible; nothing here should be read as promising those numbers.
Merge requires exact-head P0 green + independent audit + operator (Angel)
approval, per standing governance.
