# MERGE FILE — Score-65 Structured Eligibility Refinement

**Status:** AWAITING REVIEW — not committed to main, no behavior change until opt-in.
**Branch:** `refine/score65-structured-eligibility`
**Scope:** `ap_master_control.py` only (+ new test file). No exits, dashboard, sizing,
client_runner, proof logging, broker execution, retry engine, or watcher rearm touched.

---

## What this does

Replaces the blunt `effective_score < 70 -> REJECTED_LOW_SCORE` gate with a structured
`_score_allows_entry()` function that admits clean score-65..69 setups while keeping the
dangerous ones blocked.

**Default behavior is UNCHANGED.** The feature is gated behind `SCORE65_ALLOW=false`.
With the flag off, a score-66 clean setup still returns `REJECTED_LOW_SCORE` exactly as
today. Nothing reaches the broker differently until you set `SCORE65_ALLOW=true` on Render.

## Decision table (when SCORE65_ALLOW=true)

| score | 0DTE | spread | delta | result |
|-------|------|--------|-------|--------|
| >= 70 | any | any | any | ALLOWED (normal path, "" reason) |
| < 65 | any | any | any | REJECTED_LOW_SCORE_UNDER_65 |
| 65-69 | yes | any | any | REJECTED_SCORE65_0DTE |
| 65-69 | no | > 8% | any | REJECTED_SCORE65_WIDE_SPREAD |
| 65-69 | no | ok | < 0.35 | REJECTED_SCORE65_WEAK_CONTRACT |
| 65-69 | no | ok/absent | ok/absent | ALLOWED_SCORE65_NON_0DTE_DAILY or _CLEAN |

## Mapping to your spec

| Your requirement | How it's handled |
|---|---|
| Always block score < 65 | `REJECTED_LOW_SCORE_UNDER_65` |
| 65-69 only if not 0DTE | `REJECTED_SCORE65_0DTE` blocks all 0DTE |
| not late-day index 0DTE | covered by not-0DTE rule + existing PR-C gate downstream |
| quote refresh succeeds / submit_ask exists | **NOT checked here** — see Architecture Note |
| spread_pct under threshold | `SCORE65_MAX_SPREAD_PCT` (default 8%) |
| not far OTM / weak delta if delta present | `SCORE65_MIN_DELTA` (default 0.35), only when delta present |
| daily/overnight OR clean non-0DTE intraday | timeframe-based ALLOWED label |
| 0DTE score 65 always rejected | yes, hard |
| index 0DTE caution >= 75 | unchanged — existing PR-C gate fires downstream |
| score >= 70 keeps current behavior | yes, returns early with "" reason |

## Architecture Note — why quote_ok / submit_ask are NOT checked here

Your spec asked the eligibility function to verify `quote refresh succeeds` and
`submit_ask exists`. **Those values do not exist at this point in the pipeline.**

The flow is: `master_control.evaluate()` (this gate) -> `contract_selector.select()`
(resolves the actual contract + quote) -> submit (quote refresh). The score gate runs
BEFORE contract selection. We only have what the scanner put on the signal: score,
timeframe, expiration/dte, delta, spread_pct.

This is already handled correctly downstream:
- `contract_selector` rejects `spread_too_wide` at selection time.
- `QUOTE_REFRESH_FAILED_AT_SUBMIT` (PR-H, already on main) fails closed if the quote
  can't be refreshed at submit.

So a score-65 setup that passes this gate STILL cannot submit on a stale/failed quote —
the existing gates catch it. Duplicating those checks here against data we don't have
would be dishonest (we'd be checking `None`). I flagged this rather than fake it.

## Reason codes emitted

Rejections (written to ap_signals.decision_status = 'rejected_low_score'):
- REJECTED_LOW_SCORE_UNDER_65
- REJECTED_SCORE65_0DTE
- REJECTED_SCORE65_WIDE_SPREAD
- REJECTED_SCORE65_WEAK_CONTRACT
- REJECTED_LOW_SCORE (feature-off, unchanged)

Admissions (written to ap_signals.decision_status = 'score65_admitted'):
- ALLOWED_SCORE65_NON_0DTE_DAILY
- ALLOWED_SCORE65_NON_0DTE_CLEAN

The two reason codes your spec listed but I did NOT implement, with why:
- REJECTED_SCORE65_LATE_INDEX — redundant. Late-day index 0DTE is already blocked by
  the not-0DTE rule here, and by PR-C downstream. A non-0DTE late-day index setup is
  not inherently dangerous, so blocking it would be over-broad.
- REJECTED_SCORE65_STALE_QUOTE — cannot check here (quote not resolved yet, see above).
  PR-H's QUOTE_REFRESH_FAILED_AT_SUBMIT covers it downstream.

## New env vars (all optional, safe defaults)

| Var | Default | Meaning |
|-----|---------|---------|
| SCORE65_ALLOW | false | Master switch. OFF = identical to current behavior. |
| SCORE65_FLOOR | 65 | Bottom of the exception band. |
| SCORE65_MAX_SPREAD_PCT | 0.08 | Max spread for a 65 to pass (8%). |
| SCORE65_MIN_DELTA | 0.35 | Min |delta| for a 65 to pass (when delta present). |

## Before you flip SCORE65_ALLOW=true — run the count query

Run this in Supabase to see what you'd actually be admitting:

```sql
SELECT
  CASE WHEN ticker IN ('SPY','QQQ','IWM','SPX','NDX','DIA') THEN 'index' ELSE 'single_name' END AS name_class,
  CASE WHEN dte = 0 OR (expiration::date = CURRENT_DATE) THEN '0DTE' ELSE 'non_0DTE' END AS dte_class,
  COALESCE(timeframe, 'unknown') AS timeframe,
  COUNT(*) AS rejected_count,
  ROUND(AVG(score)::numeric, 1) AS avg_score,
  COUNT(*) FILTER (WHERE context_notes ILIKE '%spread%') AS wide_spread_n,
  COUNT(*) FILTER (WHERE context_notes ILIKE '%quote%')  AS quote_fail_n
FROM ap_signals
WHERE decision_status = 'rejected_low_score'
  AND score >= 65 AND score < 70
  AND created_at::date = CURRENT_DATE
GROUP BY 1, 2, 3
ORDER BY rejected_count DESC;
```

The non_0DTE rows are the ones this refinement would newly admit. If that's a sane
number of clean setups, flip the flag. If it's mostly 0DTE or wide-spread, the
refinement correctly leaves them blocked and you've lost nothing.

## Tests

`tests/test_score65_eligibility.py` — 13 tests, all passing. Covers every row of the
decision table plus the feature-off path that preserves current behavior.

## Rollout

1. Review + merge this branch.
2. Deploy (no behavior change — flag is off).
3. Run the count query above.
4. If the numbers look right, set `SCORE65_ALLOW=true` on the bot's Render env.
5. Watch `ap_signals` for `score65_admitted` rows + their downstream fill outcomes.
6. If a score-65 admit underperforms, set `SCORE65_ALLOW=false` — instant revert,
   no redeploy.

---

## ITEM-2 UPDATE — REJECTED_SCORE65_UNKNOWN_DTE (unblocks enabling)

The score-65 exception is only safe when we can PROVE the setup is non-0DTE.
Previously, an unparseable/missing DTE silently defaulted to non-0DTE (False),
which could let an unknown-expiry 65 slip through once SCORE65_ALLOW=true.

Fix:
- `_score_allows_entry` gains `dte_known: bool = True`. When False and score is
  in [65, hard_floor), it returns `REJECTED_SCORE65_UNKNOWN_DTE` BEFORE the
  0DTE / spread / delta checks.
- The caller now tracks `_dte_known` three-state: True only when the `dte`
  field parses cleanly OR a YYYY-MM-DD expiration is present. A present-but-
  unparseable `dte`, or no DTE signal at all, yields `_dte_known=False`.
- Default `dte_known=True` keeps every existing call site / test unaffected.

New reason code: REJECTED_SCORE65_UNKNOWN_DTE.

Tests added (6): unknown DTE rejected; rejected even with clean spread/delta/
timeframe; known non-0DTE still admitted; default-True back-comat; doesn't
affect score>=70; under-65 still reports UNDER_65 (cheaper check first).
Suite now 19/19.

This satisfies the gate: "do not enable SCORE65_ALLOW until unknown-DTE
rejection exists." The flag remains OFF by default — enabling still also
requires the deployed quote-domain PR, the count query review, and one
monitored session.
