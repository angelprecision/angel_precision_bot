# P0 — Restore QQQ/IWM scanner-to-client flow without weakening admission

**Status:** SPEC ONLY / HARD HOLD / EVIDENCE-FIRST IMPLEMENTATION WORK ORDER

**Base:** `main@d0d37e79ae698e604eb8080065d2314b161de351`

## Proven production symptom

Production order history from 2026-08-01 through 2026-08-17 contains SPY ENTRY rows but **zero QQQ, zero IWM, and zero DIA ENTRY rows** across the active client order layer.

This is upstream of broker execution. A QQQ/IWM order cannot be rejected by Tradier, contract selection, or Jason's final broker submit if no durable client ENTRY ever exists.

The repository's canonical universe helper currently says:

```python
INDICES = ["SPY", "QQQ", "IWM", "^GSPC"]
TICKERS = dedupe_keep_order(INDICES + SP100 + HEAVYWEIGHTS + REST_NASDAQ + EXTRA_TICKERS)
_ZERO_DTE = {"SPY", "QQQ", "IWM", "SPX", "NDX"}
```

in `ap/scanner_utils.py`.

Therefore either:

1. the actual production scanner does not consume this universe;
2. QQQ/IWM are removed/translated before iteration;
3. market-data acquisition fails or skips them;
4. pattern/scoring output is never emitted for them;
5. signal persistence/fanout drops them;
6. index normalization diverges between scanner and downstream consumers;
7. the production scanner deployment is running code/config different from the assumed repository seam.

**Do not guess which one.** This PR must prove the first divergence using the real current-main import/call path, then repair only that seam.

## Objective

Restore QQQ/IWM as genuine first-class opportunity sources through the same scanner -> signal -> fanout pipeline used by SPY, while preserving every existing quality/risk/trigger/contract/broker gate.

This PR is **not** authorization to force 0DTE index trades, manufacture signals, lower scores, bypass scanner patterns, or guarantee a fixed number of index trades.

Correct result:

```text
QQQ/IWM are scanned and can produce normal durable candidates when their real market setup qualifies.
```

Incorrect result:

```text
QQQ/IWM always create orders because we need more trades.
```

## Exact investigation order — mandatory before production edits

Claude/Codex must execute this trace in order and record exact files/functions/imports. Do not jump directly to `ap/scanner_utils.py` because configuration that nobody imports is decorative literature.

### Stage 1 — Runtime entry point and deployment

Identify every production scanner entry point used by current Render/GitHub scheduled jobs. Search and inspect:

- `render.yaml`
- `.github/workflows/*scanner*`
- scheduled scripts/commands
- scanner service modules
- `client_runner.py` only if it invokes scanners directly
- any external scanner ingestion endpoint configured in repository deployment files

Return:

- exact command;
- exact Python module/file;
- exact function called;
- exact current-main SHA expected by deployment;
- whether the scanner runs in this repository or consumes externally produced signals.

If the actual signal producer is outside this repository, STOP before pretending this PR can fix it. Document the external boundary and create/route the repair to the correct repository/service.

### Stage 2 — Universe import seam

From the proven entry point, trace imports until the runtime symbol universe is constructed.

Explicitly determine whether it consumes:

- `ap.scanner_utils.INDICES`
- `ap.scanner_utils.TICKERS`
- a legacy `ap_scanner_utils`
- a hardcoded list
- an env/config universe
- database-driven tickers
- another scanner package entirely.

Record the final runtime universe for one test invocation and assert membership/count for SPY, QQQ and IWM.

If SPY is present and QQQ/IWM absent here, this is the first divergence. Fix only universe construction/import normalization.

### Stage 3 — Symbol normalization / provider translation

If QQQ/IWM are in the runtime universe, trace the exact provider request symbol. Check for special handling involving:

- `^GSPC`
- SPX/NDX aliases
- index/equity classification
- ETF vs index semantics
- symbol stripping/prefixing/suffixing
- provider-specific ticker maps
- 0DTE lists
- market-hours filters.

SPY, QQQ and IWM are exchange-traded ETFs for the relevant quote/bar path; do not apply an unsupported cash-index symbol transformation to QQQ/IWM merely because they are grouped as "indices" operationally.

### Stage 4 — Market-data acquisition

For SPY, QQQ and IWM in the same deterministic scanner run, capture:

- request attempted yes/no;
- provider/source;
- response status/class;
- bar count;
- last timestamp/timezone;
- session date;
- OHLC validity;
- retry/error classification.

A provider failure is not a pattern failure. Preserve the distinction.

If QQQ/IWM provider calls fail while SPY succeeds, repair transport/symbol handling only. Do not loosen pattern rules.

### Stage 5 — Pattern generation and scoring

If valid market data exists, prove whether the scanner evaluates QQQ/IWM through the same pattern families as SPY.

Capture per symbol:

- pattern evaluation attempted;
- candidate count before scoring;
- direction;
- trigger/stop/target geometry;
- raw scanner score;
- exact rejection reason if no candidate survives.

"No qualifying QQQ setup today" is a legitimate outcome if evaluation was actually performed.

### Stage 6 — Durable signal persistence

For a deterministic qualifying fixture, trace candidate -> durable signal write. Identify exact table/writer and fields:

- signal id;
- ticker;
- side;
- timeframe;
- pattern;
- score;
- scanner source/version;
- execution/admission metadata expected downstream.

Prove SPY and QQQ/IWM use the same canonical ticker value at persistence.

### Stage 7 — Fanout / client eligibility

Trace the exact durable signal consumer into per-client queue/order creation:

```text
scanner signal
-> ap_signals or equivalent canonical store
-> eligibility/fanout
-> trade_queue if used
-> Master Control
-> ENTRY row / watcher
```

At every boundary, log/assert QQQ/IWM are not dropped solely because of symbol family.

If a normal existing risk/quality gate rejects the signal, preserve the rejection and reason. This PR does not bypass it.

## Implementation rules by first divergence

Only one repair class should be implemented in this PR, selected by the proven first divergence.

### Case A — runtime universe omission

If current production scanner genuinely imports a universe missing QQQ/IWM:

- make one canonical runtime universe include SPY/QQQ/IWM;
- delete/retire duplicate stale universe only if safe and explicitly audited;
- preserve ordering/dedup semantics;
- add a startup/scan diagnostic with universe source and index membership.

Do not broaden the ticker universe beyond symbols already intended by current configuration.

### Case B — provider symbol/transport bug

If runtime iterates QQQ/IWM but data fetch fails:

- fix provider translation/transport at the narrow adapter/helper;
- no fallback to stale/last-only bars;
- preserve timezone/session freshness;
- distinguish RETRYABLE_DATA from terminal input invalidity;
- no silent substitution of SPY data for QQQ/IWM.

### Case C — index-family pattern bypass

If data is valid but code conditionally bypasses pattern evaluation for QQQ/IWM:

- remove only unintended special-case exclusion;
- route them through the same applicable scanner logic as SPY;
- preserve score/pattern thresholds unchanged.

### Case D — persistence/fanout normalization mismatch

If candidates exist but vanish when persisted/consumed:

- establish one canonical symbol normalization at that boundary;
- preserve signal identity and scanner provenance;
- do not duplicate signals under aliases;
- exact client eligibility remains authoritative.

### Case E — no code defect

If the trace proves QQQ/IWM were scanned with valid data every session, normal pattern logic simply produced no qualifying candidates, and no downstream symbol drop exists, **make zero production behavior change**. Convert the PR to observability/replay only and report that trade absence is strategy-output, not liveness.

This case is mandatory. The existence of this PR is not permission to fabricate a bug.

## Required production diagnostics

Whichever case applies, we need an auditable proof spine for the index family. Use existing observability infrastructure rather than a second logging subsystem.

For each scanner run, enough evidence must exist to answer:

```text
Was SPY scanned?
Was QQQ scanned?
Was IWM scanned?
Did each have valid current-session data?
Did pattern evaluation run?
Did any candidate survive?
If not, what exact stage/reason stopped it?
If yes, was the durable signal persisted/fanned out?
```

Do not log every candle or flood production. One structured per-symbol/stage summary is sufficient.

## Expected file budget

**Do not predetermine production files before Stage 1 proves the runtime entry point.**

After trace, Claude/Codex must state the smallest expected production file set before editing. More than two production files requires an explicit justification in the PR update.

Tests should include a new focused module such as:

`tests/test_p0_index_scanner_flow_liveness.py`

and the test must be registered in the real scanner/P0 workflow that covers the proven runtime seam.

## Required fail-first evidence

At least one of the following must fail against exact pre-fix current main before behavior changes:

- runtime universe lacks QQQ/IWM;
- provider request path mishandles QQQ/IWM;
- pattern loop skips QQQ/IWM;
- persistence normalizer drops/changes QQQ/IWM;
- fanout rejects them solely due unintended symbol family handling.

If none fail, use Case E. Do not write a behavioral patch without a reproduced defect.

## Minimum acceptance cases

1. proven production scanner entry point documented.
2. exact import chain to runtime universe documented.
3. SPY membership proven.
4. QQQ membership proven after repair or Case E.
5. IWM membership proven after repair or Case E.
6. one deterministic current-session SPY data fetch succeeds.
7. same provider contract works for QQQ.
8. same provider contract works for IWM.
9. stale QQQ bars fail freshness rather than entering pattern logic.
10. missing IWM data is classified data-unavailable, not "no setup".
11. QQQ pattern evaluation executes on valid fixture.
12. IWM pattern evaluation executes on valid fixture.
13. a nonqualifying QQQ fixture creates zero client order and preserves exact rejection reason.
14. a qualifying QQQ fixture persists one canonical signal.
15. that signal fans out at most once per eligible client under existing idempotency.
16. IWM same deterministic persistence/fanout proof.
17. no score threshold changed.
18. no trigger/stop/target geometry relaxation.
19. no Master Control risk relaxation.
20. no contract selector threshold relaxation.
21. no broker submit/cancel method added.
22. no forced 0DTE behavior introduced.
23. no SPY regression.
24. no stock-universe regression.
25. exact `client_id` and execution mode remain downstream-authoritative.
26. PAPER and LIVE receive the same scanner truth; client-specific gates remain separate.
27. duplicate aliases cannot create two signals for one economic setup.
28. observability can distinguish scan absence, data absence, pattern rejection and downstream rejection.
29. startup/deployment configuration references the same code path tested.
30. exact-head CI green on the final branch.

## Money-path declaration

- **Changes LIVE behavior:** potentially YES only if a proven upstream liveness defect is repaired; normal downstream gates remain unchanged.
- **Flag-off:** no new trading flag required; observability may be always on and bounded.
- **Broker submit/cancel:** NO direct changes.
- **Orders:** no direct mutation changes; valid restored signals may reach existing order creation later.
- **Positions/proof_trades:** no changes.
- **Queue:** only existing fanout may become reachable; no new queue writer.
- **client_id / execution_mode:** not generated by scanner guesses; existing per-client authority preserved.
- **Production metadata:** use actual scanner/durable signal shape.
- **Could make Jason trade junk:** not if implemented correctly because no quality/risk threshold is loosened. If the patch contains a symbol-specific forced approval, HARD HOLD it.

## Claude/Codex delivery checklist

Return:

1. exact base/head SHAs;
2. production scanner command and entry point;
3. full import chain to universe;
4. pre-fix SPY/QQQ/IWM runtime-universe evidence;
5. first divergence identified;
6. fail-first regression evidence;
7. exact changed files;
8. why each production file is necessary;
9. pre/post per-stage SPY/QQQ/IWM trace;
10. proof no thresholds changed;
11. proof no broker methods changed;
12. focused + adjacent test counts;
13. workflow/Render command parity proof;
14. exact-head CI run IDs;
15. final MERGE/HOLD/HARD HOLD recommendation.

Do not merge, deploy, alter scanner schedules, or change production configuration from this task.

**Current verdict: HARD HOLD until first divergence is reproduced and a bounded implementation exists.**
