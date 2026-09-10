# P0 — Recover missing terminal proof after exact durable fill lifecycle

## STATUS

**SPEC ONLY / HARD HOLD. DO NOT MERGE OR DEPLOY.**

Base: `main@d3c61850df709fe4c399196b9c509f28c9af2a8a`

Incident date: 2026-09-04

This work order owns one reliability defect only:

> a trade can complete at the broker, persist exact ENTRY and EXIT fills, and converge its canonical position to terminal/CLOSED state, yet never materialize the corresponding `proof_trades` record after a database outage/restart.

Do not use this PR to redesign proof taxonomy, exit logic, broker reconciliation, position convergence, scanner policy, or trade selection.

---

## Production incident

During the September 4 Supabase outage/restart, both PAPER accounts completed the same GOOGL trade at Tradier, and durable order/position truth later converged, but neither account received a `proof_trades` row until manual backfill.

### Jose

```text
client_id: jose.vasquez4011...
contract: GOOGL260904P00340000
ENTRY broker_order_id: 38245630
ENTRY qty/fill: 13 @ 0.85
EXIT broker_order_id: 38246008
EXIT qty/fill: 13 @ 1.01
option gain: +18.8235%
execution_mode: paper
canonical position: CLOSED / quantity_remaining=0
proof_trades before repair: 0
```

### Tradefluence

```text
client_id: tradefluencehq...
contract: GOOGL260904P00340000
ENTRY broker_order_id: 38245707
ENTRY qty/fill: 13 @ 0.84
EXIT broker_order_id: 38246016
EXIT qty/fill: 13 @ 1.01
option gain: +20.2381%
execution_mode: paper
canonical position: CLOSED / quantity_remaining=0
proof_trades before repair: 0
```

The failure was therefore downstream of broker execution and downstream of canonical position closure:

```text
ENTRY exact broker fill
-> position exists
-> EXIT exact broker fill
-> position becomes CLOSED with exact economics
-> database/restart boundary interrupts terminal proof materialization
-> proof_trades remains absent indefinitely
```

This is not the same failure as an external/manual exit whose exact broker fill is unknown.

Jason's September 4 manual GOOGL close is the negative control: exact ENTRY fill was durable, broker-flat was later proven, but the exact external EXIT order/fill was not captured. A contemporaneous quote/reference price must **not** be promoted into canonical official proof by this recovery path.

---

## Existing code authority to reuse

Current main already contains terminal proof authority in `ap/position_manager.py`, including `_ensure_terminal_close_proof(...)` and related missing-terminal-proof helpers/tests.

Implementation must first trace the current callers and prove whether this existing authority can materialize a missing proof when invoked from a restart/reconciliation sweep.

**Do not create a second proof writer if the existing canonical seam can perform the write.**

Required caller trace before editing:

```text
terminal position / exact EXIT_FILLED discovery
-> candidate validation
-> exact ENTRY identity read
-> exact EXIT identity read
-> execution-mode resolution
-> quantity/economics/timestamp validation
-> existing canonical proof lookup
-> canonical terminal-proof authority
-> proof taxonomy guard
-> idempotent commit
-> downstream reporting/training eligibility
```

For every production function changed, document:

```text
caller
-> validation
-> state read
-> authority resolution
-> mutation
-> return classification
-> downstream consumer
```

A correct helper in isolation is not sufficient.

---

## Binding invariant

Only the following shape may recover a missing terminal proof:

```text
exact canonical terminal position
+ exact client_id
+ exact normalized execution_mode
+ exact OCC contract
+ exact originating ENTRY local/broker identity
+ exact durable ENTRY fill qty/price/timestamp
+ exact EXIT local/broker identity
+ exact durable EXIT fill qty/price/timestamp
+ quantities reconcile to terminal quantity_remaining=0
+ terminal position economics agree with exact fills
+ no canonical proof already exists
-> invoke one canonical proof authority
-> exactly one proof exists
```

If any required authority is missing, malformed, contradictory, ambiguous, stale, or cross-mode:

```text
HOLD / diagnostic
-> zero proof fabrication
-> zero broker calls
-> zero order mutation
-> zero position-economic mutation
-> zero queue/result mutation
```

Recovery must be monotonic and idempotent. It may fill a missing terminal observation; it may not rewrite economic truth to make records agree.

---

## Exact identity rules

### Client

`client_id` must match exactly across:

- canonical position;
- originating ENTRY order;
- EXIT order;
- any existing proof candidate.

No client-name inference, email-prefix inference, environment fallback, or cross-account OCC matching.

### Execution mode

Normalize only explicit `live` / `paper` authority.

Two durable nonblank sources that conflict must HOLD.

Never infer historical mode from:

- current client configuration;
- broker URL;
- environment;
- account name;
- PAPER/LIVE service identity.

PAPER can never become `LIVE_OFFICIAL` through fallback recovery.

### Contract

Require exact normalized OCC identity. Ticker alone is insufficient.

Same ticker + same contract may be traded more than once. Bind by originating order/position lifecycle identity, not OCC + timestamp proximity alone.

### Orders

ENTRY and EXIT must each resolve to one unambiguous durable economic order.

Require appropriate filled terminal statuses and positive durable fill quantity.

Two possible ENTRY or EXIT candidates without a stronger exact lifecycle identity is ambiguity -> HOLD.

### Quantity

Full-terminal repair requires exact reconciliation:

```text
entry cumulative filled quantity
- terminal cumulative exit consumption
= 0
```

If the position is partial/open or quantity authority conflicts, no terminal proof.

Do not use `max(qty, 1)`, rounding, absolute-value repair, or synthetic default quantity.

### Economics

Prices must come from durable broker fill truth already persisted in the lifecycle.

Do not use:

- current bid/ask;
- last quote;
- mark;
- current underlying;
- inferred P&L as a substitute for missing exit fill;
- manually supplied dashboard reference price.

If canonical position realized economics and exact order fills materially contradict, HOLD and preserve diagnostics.

### Timestamps

Require parseable timezone-aware ordering:

```text
ENTRY filled_ts <= EXIT filled_ts
```

Naive/malformed/missing timestamps fail closed unless one existing canonical normalizer can prove the durable value without inventing it.

---

## Recovery trigger

The missing-proof repair must be reachable after process/database recovery without requiring another broker event.

Preferred architecture:

```text
normal reconciler/recovery cadence
-> bounded/paginated read of terminal positions missing canonical proof
-> exact durable fill validation
-> existing canonical proof authority
-> idempotent result
```

Do not scan the entire historical ledger every hot-loop tick. Use a bounded candidate window/cursor consistent with existing recovery infrastructure.

The trigger must work in:

1. uninterrupted runtime after a transient proof-write failure;
2. process restart after position closure;
3. database restart/outage where orders and position are durable but proof is absent;
4. replay after proof commit but before caller acknowledgement.

All four must converge to the same result.

---

## Relationship to existing PRs

### #566 — broker-flat / exact Tradier fill adoption

#566 owns finding/adopting an exact external Tradier EXIT fill when local ownership diverges.

This PR begins **after exact fill truth already exists durably**. Do not copy #566 broker lookup/adoption machinery here.

If #566 later adopts an exact fill and the normal terminal finalizer loses its proof write, this recovery path may repair the missing proof from the resulting exact durable lifecycle.

### #579 — exact `EXIT_FILLED` -> position convergence

#579 owns the case where exact durable EXIT fill exists but the canonical position remains OPEN/stale.

This PR must not close positions. Until the canonical position is terminal and quantities reconcile, missing-proof recovery does nothing.

### #498 — exactly-once proof identity / execution-mode integrity

#498 owns canonical proof identity, duplicate prevention, and execution-mode preservation. Its current open branch is broad/stale and is not a safe vehicle for this September 4 liveness defect.

Implementation here must reuse whatever canonical exactly-once authority exists on the then-current main. If #498 lands first, rebase and consume it rather than duplicating its identity framework.

### #390

#390 is explicitly stale and marked for rebuild. Do not resurrect its historical production hunks.

---

## Failure-class audit

### Authority

Must prove one authority for:

- client identity;
- execution mode;
- position identity;
- originating ENTRY identity;
- EXIT identity;
- cumulative quantity;
- entry and exit fill economics;
- canonical proof identity;
- proof taxonomy.

Conflicting duplicate authorities HOLD.

### State transitions

Required scenarios:

- terminal close and proof write succeeds normally;
- terminal close succeeds but proof write fails;
- process dies before proof invocation;
- process dies during/after proof DB commit;
- restart sees missing proof;
- restart sees already-created proof;
- partial EXIT remains nonterminal;
- #579-style exact EXIT fill but position still OPEN;
- #566-style broker flat but exact fill absent.

### Failure timing

Execute tests for failure:

- after ENTRY fill persistence;
- after EXIT fill persistence;
- after position terminal update but before proof call;
- during proof INSERT/UPSERT;
- after proof commit but before caller receives success;
- during restart candidate scan;
- after candidate selection but before identity validation.

### Data corruption

HOLD with zero mutation for:

- missing client;
- malformed/unknown mode;
- explicit LIVE/PAPER conflict;
- blank OCC;
- wrong OCC;
- missing local order identity where canonical binding requires it;
- missing broker order identity where broker-confirmed classification requires it;
- zero/negative/non-finite price;
- zero/negative/fractional or contradictory quantity;
- missing/invalid fill timestamp;
- EXIT before ENTRY timestamp;
- stale position generation;
- duplicate ENTRY candidates;
- duplicate EXIT candidates;
- same OCC reused by another economic trade;
- legacy metadata conflicting with canonical columns.

### External authority

This PR adds no new broker read requirement and no broker write authority.

Durable broker-backed fill rows are inputs. Broker transport availability must not determine whether a missing durable proof is safe to write.

### Money-path safety

Every recovery test must prove:

- zero broker ENTRY POST;
- zero broker EXIT POST;
- zero broker cancel;
- zero order state mutation;
- zero position quantity/economic mutation;
- zero `trade_queue` / queue-result mutation;
- exactly one proof on the valid path;
- no proof on fail-closed paths;
- no PAPER/LIVE taxonomy crossing;
- no official LIVE promotion from quote-derived/manual reference economics.

---

## Mandatory production-shaped behavioral tests

Structural/source tests may remain secondary. They are not the proof.

1. **Jose September 4 replay**: exact PAPER GOOGL ENTRY `38245630` 13 @ 0.85 + EXIT `38246008` 13 @ 1.01 + terminal position + no proof -> exactly one PAPER proof.
2. **Tradefluence September 4 replay**: exact PAPER GOOGL ENTRY `38245707` 13 @ 0.84 + EXIT `38246016` 13 @ 1.01 + terminal position + no proof -> exactly one PAPER proof.
3. Run each replay twice -> still one proof.
4. Crash after terminal position commit but before proof -> restart creates one proof.
5. Crash after proof commit but before acknowledgement -> restart creates zero duplicates.
6. Wrong client on EXIT -> HOLD, zero proof.
7. Wrong execution mode -> HOLD, zero proof.
8. Column/meta mode conflict -> HOLD, zero proof.
9. Wrong OCC -> HOLD, zero proof.
10. Same OCC reused by later separate trade -> exact lifecycle binds separately; neither cross-binds.
11. Two ambiguous ENTRY candidates -> HOLD.
12. Two ambiguous EXIT candidates -> HOLD.
13. Exact ENTRY but missing exact EXIT fill -> HOLD.
14. Exact EXIT but canonical position still OPEN -> HOLD; #579 owns convergence.
15. Partial exit / quantity remaining > 0 -> no terminal proof.
16. Full EXIT quantities conflict with position economics -> HOLD.
17. Zero/negative/non-finite fill price -> HOLD.
18. Malformed fill timestamp -> HOLD.
19. EXIT timestamp before ENTRY -> HOLD.
20. Existing canonical proof -> NOOP, preserve stronger metadata.
21. Existing weaker/repair proof candidate -> use current canonical binding authority; never create an independent second economic proof.
22. PAPER result remains non-LIVE taxonomy under every fallback shape.
23. Exact LIVE lifecycle can become official only if the existing canonical LIVE taxonomy guard independently proves eligibility.
24. **Jason manual GOOGL negative control**: exact external EXIT fill absent and only broker-flat/reference price available -> no `LIVE_OFFICIAL` proof and no invented broker EXIT identity.
25. DB exception during proof write -> position/orders unchanged, retry remains possible.
26. PostgreSQL concurrency: two recovery workers race on one missing proof -> exactly one canonical row.
27. Candidate pagination: missing proof beyond first page is eventually recovered without reprocessing creating duplicates.
28. Zero broker `submit_order`, zero broker `cancel_order`, zero replacement calls across the whole suite.

At least the incident, crash/restart, idempotency, ambiguity, and concurrency cases must execute against production-shaped PostgreSQL.

---

## Tests must not encode the bug

Inspect assertions for:

- mocks that bypass the actual SQL uniqueness/binding path;
- a fake positions schema friendlier than production;
- proof rows inserted directly by the test instead of through the real canonical authority;
- test success because the recovery callback was never reached;
- same-contract matching without exact economic identity;
- `COALESCE(..., 'paper'/'live')` mode defaults;
- tests that call a helper but never exercise restart/reconciler routing;
- negative cases without a positive control.

A green suite that never crosses the production caller boundary is not acceptance evidence.

---

## Preferred implementation scope

First attempt to keep production changes within the existing recovery/reconciliation and canonical proof boundaries, likely:

```text
ap_reconciler.py or the current restart/recovery owner
ap/position_manager.py only if a small public/idempotent wrapper around existing canonical proof authority is required
```

A tiny dedicated candidate-scanner helper is acceptable only if it avoids duplicating proof authority.

Do **not** modify unless fail-first evidence proves unavoidable:

- broker adapter;
- `ap_exit_engine.py`;
- order submit/cancel machinery;
- entry watcher;
- selector;
- scanner;
- Master Control;
- sizing/risk;
- exit policy/thresholds;
- queue/result handling;
- intelligence;
- proof taxonomy rules.

Do not add a migration merely to make implementation convenient. If current canonical exactly-once schema is insufficient, document the exact missing invariant and coordinate with #498 rather than smuggling a second proof identity into this PR.

---

## Runtime / restart / recovery matrix

Final review must publish actual resolved behavior, not “same concept” prose:

| Scenario | Normal finalizer | Same-process repair | Restart repair |
|---|---|---|---|
| exact PAPER terminal lifecycle, proof absent | one PAPER proof | one PAPER proof | one PAPER proof |
| exact LIVE terminal lifecycle, taxonomy valid | canonical LIVE result | same | same |
| proof already exists | NOOP/enrich only | NOOP/enrich only | NOOP/enrich only |
| position OPEN | no proof | no proof | no proof |
| exact EXIT missing | no proof | no proof | no proof |
| mode conflict | HOLD | HOLD | HOLD |
| identity ambiguity | HOLD | HOLD | HOLD |
| manual broker-flat reference only | no official proof | no official proof | no official proof |

---

## Acceptance gate

Before MERGE consideration:

1. Rebase implementation on the then-current production main.
2. Read this binding spec in full.
3. Read the complete actual diff, not only the latest amendment.
4. Read and resolve all review threads.
5. List every changed production file.
6. Trace every changed production line to caller and downstream mutation.
7. Execute the September 4 Jose and Tradefluence PostgreSQL replays.
8. Execute crash-at-boundary and restart replays.
9. Execute mode/client/OCC/quantity/economics corruption matrix.
10. Execute real concurrency exactly-once proof.
11. Prove zero broker submit/cancel calls.
12. Prove zero order/position/queue mutation by the recovery path.
13. Run adjacent #566/#579/proof-taxonomy/exactly-once suites.
14. Run exact-head blocking P0 CI.
15. Independently audit the final diff and return MERGE / HOLD / HARD HOLD.

**HARD HOLD** if the implementation can manufacture a missing EXIT fill, infer mode, cross clients, close an OPEN position, create a second proof writer, duplicate a proof on restart, or touch broker submit/cancel authority.
