# September LIVE cohort fixtures — evidence vs UNKNOWN

PR tip researched: `eed1da092af1de0fa18c30678332a2c66971516c` (`spec/p1-breach-setup-intelligence-20260811`)  
Artifacts written only under `/workspace/pr435/` (no clone / push / merge).

## Research method

GitHub API read-only via `user-Github`:

- `pull_request_read` on #435 (body + issue comments)
- `get_file_contents` for amendment specs on #435 / #436 / #567 tip
- `search_code` / `search_issues` / `search_pull_requests` / `search_commits` for QQQ, HOOD, LULU, NKE, GOOGL, C, IWM, `2026-09-08`, `2026-09-09`, `september_live`

## Required cohort (amendment + parent task)

| Case | Session | What was found | Still UNKNOWN |
|------|---------|----------------|---------------|
| **QQQ PUT** | 2026-09-09 | Named in `p1_435_regime_pullback_architecture_amendment_20260909.md` and mirrored by #436/#438/#443/#577 as a mandatory replay (“immediate entry became materially red before later recovery”). | `signal_id`, canonical/local order ids, client, exact first-breach timestamp, trigger/stop/target, underlying PIT price, option BID/ASK, OCC, fill, outcome |
| **HOOD PUT** | 2026-09-09 | Named in same amendment family (“pullback before later recovery/continuation”). | Same identity/quote/outcome gap as QQQ |
| **LULU CALL** | 2026-09-08 | Named in same amendment family (“pullback before later recovery/continuation”). Parent task supplies CALL + 2026-09-08. | Same identity/quote/outcome gap |
| **NKE CALL** | 2026-09-08 | Named in parent task / PR body outstanding list (`QQQ/HOOD/LULU/NKE`). **No** amendment narrative body, signal id, or quotes in GitHub search hits. | Entire durable production envelope; FVG zones |

**Honest consequence:** the four required cases use **synthetic classifier envelopes** (`structure_provenance: synthetic_classifier_envelope`) so tip resolvers/readiness/market_structure can be exercised. Placeholder geometry is **not** production truth. Option BID/ASK and outcomes are **not** invented into BREACH scoring inputs.

## Positive / related controls

| Case | Found? | Notes |
|------|--------|-------|
| **AAPL opening-window** | **Yes (partial durable)** | PR #435 comments: LIVE PUT, client `jasoncosby1@gmail.com`, signal `83503891-b746-4c79-a39c-19e8cd8d4cd8`, pattern `2-3`/`1d` → daily `2-3-2`, broker ENTRY ~`2026-08-12T13:51:51Z`, trigger ~302.79–302.80, OCC `AAPL260814P00300000`, fill 1.32 / exit 0.98 / later underlying ~300.62. Fill/outcome kept in `research_only_outcome_sidecar` only. Exact stop/target and PIT 5m/15m bars still incomplete → candles are production-**shaped**, not exact PIT dumps. |
| **IWM** | **Partial** | PR #567 / `p0_preopen_watcher_ownership_20260901.md`: LIVE PUT, `1D`, STRAT `3-2-2`, score 72, trigger **292.70**, submit ~09:32:58 ET, fill 1.04, “IWM 293 PUT”. Useful A-shaped control. **Not** proven as a September entry-timing “winner” cohort row; `signal_id` UNKNOWN. |
| **GOOGL winner** | **No** | No durable signal/timestamp/quote evidence in API search → deferred case, no signal envelope. |
| **C winner** | **No** | Same as GOOGL → deferred. |

## Files created

1. `tests/fixtures/september_live_cohort_202609.json` — structured cohort + deferred stubs  
2. `tests/test_p0_435_september_live_cohort.py` — first-breach ABCD via tip helpers; asserts `observe_only=true`, `affected_eligibility=false`; generic mapper (no ticker hardcoding)  
3. `SEPTEMBER_COHORT_NOTES.md` — this file  

Helpers imported from existing `/workspace/pr435/ap/` tip copies (no additional module fetch required for py_compile).

## ABCD mapping (generic)

| Class | Meaning | Tip signals used |
|-------|---------|------------------|
| A | valid thesis + good immediate entry | `READY_NOW` and no opposing-FVG acceptance relationship |
| B | valid thesis + inefficient immediate / pullback preferred | `WAIT_CONFIRMATION` or `REBREACH_PREFERRED` |
| C | valid thesis + opposing FVG acceptance required | market_structure relationship in opposing-wall / inside-opposing / approaching set |
| D | invalid setup | readiness `INVALID` |

## Gaps blocking merge-grade replay

- No PostgreSQL / production DB dump of September Jason LIVE rows was available through GitHub API.
- Amendment text repeatedly requires “exact production identities loaded from durable data” but does **not** embed those identities.
- Until durable rows are exported (signal_id, trigger_crossed_at, geometry, PIT candles, optional contemporaneous quotes), cohort fixtures must remain structure/UNKNOWN-honest and must not claim production option quotes or outcomes as scoring inputs.
