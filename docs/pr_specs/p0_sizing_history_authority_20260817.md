# P0 — Make position-sizing history authoritative, mode-safe, and observable

**Status:** SPEC ONLY / HARD HOLD / DO NOT MERGE OR DEPLOY AS CREATED

**Base:** `main@d0d37e79ae698e604eb8080065d2314b161de351`

## Production finding

Jason's recent LIVE sizing diagnostics reported a fallback such as `tier_fallback (4<20 trades)` while an independent production query found many more historical `positions` rows that appeared to satisfy the broad status/P&L shape used by the sizer. The discrepancy is proven. What is not yet proven is that every broadly matching row is eligible LIVE sizing history. Some may be PAPER, legacy/null-mode, duplicate/repair, or otherwise ineligible.

This PR fixes **history authority and provenance**, not the count by fiat.

## Exact current-main seam

`ap/position_sizer.py::_fetch_history(self, client_id, limit=100)` currently has two sources:

1. injected `self.history_provider`, if present;
2. direct PostgreSQL through `from ap.db import conn` otherwise.

The DB path broadly selects closed/stopped/taken-profit/expired `positions` rows for one `client_id` with non-null realized P&L, orders them by close/update/create time, and limits to 100. A broad exception logs and returns `[]`.

So current behavior can collapse:

```text
history DB/provider failure -> [] -> looks identical to authoritative zero history -> bootstrap/tier fallback
```

Unavailable history is not proof of zero trades.

## Scope

Own only historical sizing source/provenance, client/mode eligibility, unavailable-vs-empty truth, and sizing diagnostics. Do not change base position percentage, max contracts, per-trade budget, Master Control, selector, broker execution, retries, exit thresholds, or proof taxonomy.

## Mandatory caller/import trace before edits

Search every production occurrence of `APPositionSizer(`, `PositionSizer(`, `history_provider=`, sizer `.size(` calls, `tier_fallback`, and `_fetch_history(`. For each active constructor record file/function, exact client id supplied later, execution mode available, whether `history_provider` is injected, the provider implementation/import if so, provider DB/schema/client/mode semantics, and whether the sizer instance is shared across clients/modes.

**Do not change `_fetch_history()` until the active production caller is proven.**

## Required history policy

Determine from existing code/tests/docs whether sizing history is intended to be exact client + exact execution mode, or exact client economic history across modes. For LIVE money, if policy is undefined, default to the safer rule:

```text
LIVE sizing uses only exact LIVE history with proven mode identity.
PAPER sizing uses exact PAPER history.
NULL/unknown/conflicting mode rows are excluded from authoritative tier math and surfaced in diagnostics.
```

Do not merge PAPER into LIVE merely to make the sample exceed 20. Do not infer historical mode from current membership, broker URL, account name, or environment.

## Implementation contract

### 1. Structured history truth

Replace the ambiguous list-only internal contract with a typed result conceptually carrying:

```text
status: OK | UNAVAILABLE | IDENTITY_CONFLICT
rows: list
eligible_count: int
source: injected_provider | postgres
execution_mode: live | paper | unknown
excluded_unknown_mode_count: int
excluded_wrong_mode_count: int
error_code: optional stable code
```

Names may differ; semantics may not.

Authoritative zero is `status=OK, rows=[], eligible_count=0`. Provider/DB failure is `status=UNAVAILABLE`. These may never collapse.

### 2. No exception-to-zero for LIVE authority

The current broad `except Exception: return []` is forbidden as authoritative LIVE history. Preserve safe exception diagnostics and return UNAVAILABLE to the caller.

### 3. Define unavailable behavior at the sizing call site

For LIVE, unavailable history may not promote to a more aggressive tier. Preferred behavior is an explicit `SIZING_HISTORY_UNAVAILABLE` HOLD before broker submit unless current reviewed policy proves a conservative no-history fallback that can never exceed the minimum bootstrap allocation. If that fallback is retained, diagnostics must say unavailable, not zero trades.

For PAPER, a conservative fallback may remain for liveness/data collection if existing behavior requires it, but it must preserve `history_status=UNAVAILABLE`.

Inspect the actual `size()` return contract and callers before choosing HOLD vs conservative fallback so no placeholder quantity or exception loop is created.

### 4. Exact mode identity

If mode-specific history is selected, allowed history mode values are only canonical `live` and `paper`. Null/blank/malformed/conflicting rows are excluded and counted. No historical mutation/backfill in this PR.

### 5. Provider and DB paths must agree

If `history_provider` is active, provider and direct DB paths need the same statuses, realized-P&L requirement, mode policy, limit/order semantics, client normalization, and unknown-row treatment. Add equivalent-fixture tests proving eligible IDs/counts match.

### 6. Do not silently substitute proof_trades

Do not switch to proof-trade history just because its count is attractive. Proof taxonomy has separate unresolved history work unless the current design already proves it is canonical for sizing.

### 7. Freeze risk ceilings

Even if valid history count rises, preserve `max_position_pct`, per-trade dollar budget, `MAX_CONTRACTS`, min/max contracts, equity authority, Master Control final cost revalidation, and selector affordability. A history repair is not permission to force two contracts when two do not fit the current account budget.

### 8. Diagnostics

Every sizing decision must expose history source, availability, requested mode, eligible count, unknown/wrong-mode exclusions, threshold applied, and whether fallback means genuine low sample or unavailable history.

Distinguish reasons such as:

```text
tier_fallback_low_sample:<n><threshold>
sizing_history_unavailable:<reason>
```

Never report `0 trades` because PostgreSQL timed out.

## Expected production file budget

1. `ap/position_sizer.py`
2. active production constructor/caller only if exact mode must be threaded and is not already available.

If caller already carries mode, do not touch it unnecessarily. No migration expected.

Tests: new `tests/test_p0_sizing_history_authority.py`, existing sizing threshold tests, and P0 workflow registration if this is in the P0 path.

## Fail-first requirements

Before production edits prove on exact current main:

1. direct DB exception returns `[]` indistinguishable from authoritative empty;
2. provider exception/malformed result cannot be represented distinctly from low sample safely;
3. active production constructor/provider path is reproduced;
4. broad unscoped history can contain rows ineligible under the selected exact-mode policy.

## Minimum acceptance cases

1. authoritative DB zero -> OK/count0.
2. DB connection exception -> UNAVAILABLE.
3. SQL exception -> UNAVAILABLE.
4. malformed row -> explicit malformed/unavailable, no fake zero.
5. provider authoritative zero -> OK/count0.
6. provider exception -> UNAVAILABLE.
7. provider/direct DB equivalent fixture -> same eligible IDs.
8. exact client isolation.
9. LIVE history excludes PAPER under mode-specific policy.
10. PAPER excludes LIVE.
11. null mode excluded/count surfaced.
12. malformed mode excluded/count surfaced.
13. current member mode cannot backfill historical identity.
14. valid CLOSED LIVE row included.
15. STOPPED inclusion pinned to current policy.
16. TAKEN_PROFIT inclusion pinned to current policy.
17. EXPIRED inclusion pinned to current policy.
18. realized_pnl NULL excluded.
19. deterministic ordering/limit.
20. LIVE UNAVAILABLE cannot yield more aggressive sizing than minimum-history path.
21. PAPER unavailable fallback, if retained, labeled unavailable.
22. high valid sample reaches existing tier without threshold changes.
23. no position percentage constants changed.
24. no max-contract constants changed.
25. no broker submit/cancel code added.
26. no order mutation added.
27. no position mutation.
28. no proof mutation.
29. no queue mutation.
30. exact client/mode/history diagnostics survive in the sizing result/decision context.

## Production reconciliation evidence required

After implementation run read-only production comparison for Jason: broad old-query count, exact new eligible LIVE count, excluded PAPER count, excluded NULL/unknown count, source used by deployed runner, and any provider/DB disagreement. Do not update rows to make counts align.

If truthful eligible LIVE count is 4, then 4<20 is correct and the broad-count hypothesis is rejected. If materially larger, identify exactly why runtime saw 4.

## Money-path declaration

- LIVE behavior: potentially YES because sizing tier or HOLD can change.
- No new feature flag should hide history truth.
- Broker submit/cancel: no direct changes.
- Orders/positions/proof/queue: no new mutations.
- client_id/execution_mode: more exact, never less.
- PAPER/LIVE taxonomy: strict if mode-specific policy selected.
- Main risk: accidentally over-sizing Jason by treating PAPER/legacy history as LIVE. Explicitly forbidden.

## Claude/Codex delivery contract

Return exact base/head SHA; every production constructor/caller; whether a provider is active and its exact implementation; selected history policy and supporting evidence; fail-first results; exact changed files; focused/adjacent test counts; broad vs authoritative Jason history counts with mode exclusions; proof risk constants unchanged; proof no broker/mutation authority; exact-head CI; and fresh MERGE/HOLD/HARD HOLD.

Do not merge, deploy, alter client risk configuration, or mutate historical production data.

**Current verdict: HARD HOLD until source/caller truth is proven and implementation is independently reviewed.**
