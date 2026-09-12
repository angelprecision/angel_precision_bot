# P1 Intraday Setup Classifier

## Status
SPEC ONLY / DRAFT / HARD HOLD. No implementation, merge, or deployment authority.

Base: `main@846525a2ee798071bb0079078f0aec871c415677`.

## Purpose
Provide pure deterministic setup classification over completed bars plus one explicit forming bar. Initial target is The Strat-style bar state and 2-3-2 detection, with a design that can later add other setup families without changing market-data acquisition or execution code.

This PR owns classification only. It consumes bar state. It does not fetch market data and it does not create orders/watchers.

## Canonical bar-state classification
Given previous completed-bar range and current forming/completed bar:
- inside prior high/low -> `1`
- breaks prior high only -> `2U`
- breaks prior low only -> `2D`
- breaks both sides -> `3`

Once both sides have been broken, the bar remains `3`; it must not downgrade back to `2U` or `2D` later in the same bucket.

## 2-3-2 contract
Classify sequences across explicit timeframe identity, including a developing third bar:
- completed bar N-2 = `2U` or `2D`
- completed bar N-1 = `3`
- current bar N = developing/completed `2U` or `2D`
- emit a research/setup classification with direction and exact source bar identities.

A developing 2-3-2 may exist before the third bar closes. If the third bar later breaks the opposite side and becomes `3`, the candidate is invalidated by classification state, not hidden or rewritten.

## Output contract
Pure return value only, including:
- ticker
- timeframe
- session date
- setup_type
- state: `FORMING_CANDIDATE | COMPLETED_SETUP | INVALIDATED | NO_SETUP | UNKNOWN`
- direction when known
- exact three bar IDs/start timestamps
- prior/high-low trigger geometry used for classification
- classifier version
- reason codes
- `observe_only=true`
- `affected_eligibility=false`

## Explicit non-scope
No market-data network calls, scanner scheduling, ticker universe selection, FVG/VI interpretation, watcher, selector, sizing/risk, broker submit/cancel/replace, order/position/proof/queue mutation, or entry policy.

## Mandatory tests
1. `1`, `2U`, `2D`, `3` classification matrix.
2. A bar that first becomes `2U` then breaks the low becomes and stays `3`.
3. Symmetric `2D` then high break becomes `3`.
4. Developing 2-3-2U detection.
5. Developing 2-3-2D detection.
6. Completed 2-3-2 detection.
7. Third-bar outside break invalidates former 2-3-2 candidate.
8. Exact timeframe/session/bar identity retained.
9. Missing/malformed OHLC -> `UNKNOWN`, never setup.
10. Input immutability.
11. Same bars on different ticker/timeframe cannot share setup identity.
12. Zero external I/O and zero money-path mutation.

## Merge gate
The implementation must remain a pure classifier with no I/O. Exact-head and merge-ref P0/cohesion must pass on an unchanged SHA. Any scanner or execution authority entering this PR is a HARD HOLD.
