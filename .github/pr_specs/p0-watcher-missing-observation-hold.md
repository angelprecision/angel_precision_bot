# P0 Implementation Brief: Preserve Watcher Breach Continuity Across Missing Observations

> Status: implementation brief only. This branch is intentionally opened as a draft PR before production code changes so implementation can be done and audited against a fixed contract. Remove this brief before merge if repository policy prefers no retained PR-spec files.

## Objective

Repair watcher liveness so an unusable/missing canonical quote **holds** existing breach continuity instead of resetting it, while preserving all current safety hardening.

This is a tightly scoped semantic correction. Do not revert the watcher hardening wholesale. Do not restore LAST-price trigger authority. Do not weaken stop handling. Do not change selector, broker submission, OSM, queue, positions, or proof trades.

## Proven regression

Known-good July 29 behavior effectively skipped a ticker when no usable quote was present:

```python
quote = quotes.get(w.ticker)
if not quote:
    continue
```

That meant missing market-data truth did not mutate breach continuity.

Example known-good semantics:

```text
valid breach observation
breach_count = 1

next poll has no usable canonical quote
HOLD
breach_count remains 1

next valid breach observation
breach_count = 2
trigger confirms
```

Current watcher hardening correctly refuses to use unavailable required-side evidence, but current base/shim semantics allow that absence to reset pre-confirmation continuity.

Current failure shape:

```text
valid breach -> count 1
missing required side -> count 0
valid breach -> count 1
```

Intermittent quote gaps can therefore prevent a legitimate multi-poll confirmation indefinitely.

## Primary production file

`ap_entry_watcher.py`

Potentially adjust tests and, only if necessary for the cross-file seam, `ap_entry_watcher/__init__.py`.

Do not redesign the shim/package architecture in this PR.

## Required invariant

Implement exactly:

```text
VALID BREACH OBSERVATION
=> increment breach continuity

VALID CONTRADICTORY/NON-BREACH OBSERVATION
=> reset breach continuity

NO USABLE REQUIRED-SIDE OBSERVATION
=> HOLD breach continuity unchanged
```

**HOLD is neither increment nor reset.**

## Canonical trigger-side authority must remain unchanged

Preserve current trigger evidence sides:

- CALL trigger evidence -> ASK
- PUT trigger evidence -> BID

Do not use LAST, MARK, MID, or opposite side as a substitute for missing canonical trigger evidence.

If the required canonical side is missing or unusable, that is still **not valid evidence**. The only change is that absence must not erase already accumulated valid evidence.

## Required pre-confirmation behavior

Where current logic effectively does something like:

```python
if required_quote_unavailable:
    suppress_entry_breach_evidence = True
    if not already_confirmed:
        breach_count = 0
        pending_first_breach_at = None
        breach_price = 0
```

change the pre-confirmation semantics so unusable required-side truth:

- does not increment breach count
- does not reset breach count
- does not clear first-breach evidence solely because truth is unavailable
- does not manufacture a trigger price
- does not call `on_trigger`
- does not advance a valid-observation timestamp as though a real canonical observation occurred

Preserve the existing state until a usable canonical observation arrives.

## Valid contradiction must still reset

Example CALL with trigger 100:

```text
poll 1 ASK 100.10 -> breach_count 1
poll 2 no ASK -> HOLD at 1
poll 3 ASK 99.80 -> valid contradiction -> reset to 0
poll 4 ASK 100.12 -> breach_count 1
```

Do not make breach state sticky across actual contradictory market truth. Only absence of usable truth gets HOLD semantics.

## Preserve LAST-only hardening

If bid and ask are unavailable but LAST is present and crosses the trigger, LAST must not become entry authority by default.

Example:

```text
bid = None
ask = None
last = 101.00
```

For a CALL requiring ASK, this means:

```text
no usable canonical observation
=> HOLD existing continuity
=> no increment
=> no callback
```

Do not recreate legacy `bid = ask = last` trigger authority.

## Preserve post-confirmation safety

This is mandatory.

Once a trigger lifecycle has already been durably confirmed, missing entry-side truth must not disable independently provable stop/lifecycle safety.

Preserve behavior where:

```text
trigger already confirmed
entry-side quote unavailable
stop-side canonical quote proves stop violated
=> stop safety may terminalize
```

Likewise:

```text
confirmed lifecycle
stop-side truth unavailable
=> HOLD/retry rather than fabricate safety truth
```

Do not blindly re-fire the entry callback because trigger state was already confirmed.

## Preserve retry-backoff safety

If `on_trigger` failed and the watcher is in callback retry backoff:

- do not call `on_trigger` before retry is due
- continue observing independently provable stop safety
- preserve existing ownership/retry semantics

The current safety-active retry behavior is valuable and must remain.

## Trigger timestamp persistence

Do not broaden this PR into a redesign of trigger timestamp persistence. The separate observed behavior where LIVE callback can be delayed if trigger timestamp persistence fails should remain visible and testable, but this PR's core fix is missing-observation continuity.

If implementation touches adjacent code, preserve fail-closed durability behavior and existing logs such as `WATCHER_TRIGGER_TIMESTAMP_PERSIST_FAILED` / retry diagnostics. Do not make timestamp write failures silently succeed.

## No durable-state inventions

Do not add a database column, table, or queue field.

Do not mutate orders, positions, proof_trades, trade_queue, or broker orders except through existing watcher callback paths after a genuine confirmed trigger.

This PR is about watcher observation semantics, not lifecycle architecture.

## Required tests

### 1. CALL breach -> outage -> breach

CALL trigger 100:

```text
poll 1 ask 100.05 -> count 1
poll 2 ask unavailable -> count remains 1
poll 3 ask 100.06 -> count 2 -> trigger confirms exactly once
```

### 2. PUT breach -> outage -> breach

Equivalent test using canonical BID.

Expected: same HOLD semantics.

### 3. Valid contradiction resets

CALL:

```text
breach -> missing -> valid ask below trigger
```

Expected: reset to 0 on the valid contradictory observation.

### 4. Multiple missing polls

```text
breach_count 1
missing
missing
missing
```

Expected:

- count remains 1
- callback count remains 0
- no fabricated observation state

### 5. LAST-only quote

```text
bid = None
ask = None
last crosses trigger
```

Expected:

- no increment
- no reset solely due to absence
- no callback
- existing continuity held

### 6. Unusable canonical values

Test required-side values that are unavailable/non-authoritative under current validity policy, including appropriate production-shaped cases for zero, NaN, infinity, malformed/non-numeric values.

Expected:

- no increment
- no reset
- no trigger
- no exception

### 7. Confirmed trigger + broken stop

Construct already-confirmed watcher. Remove entry-side quote but provide canonical stop-side evidence proving stop violation.

Expected:

- stop safety still wins
- no entry callback replay

### 8. Confirmed trigger + stop-side unavailable

Expected:

- HOLD/retry semantics
- no blind callback
- no false terminal state

### 9. Retry backoff

Make trigger callback fail and enter retry state. During backoff:

- callback does not fire early
- stop observation remains active
- ownership remains consistent with current hardening

### 10. Missing observation with no prior breach

If `breach_count = 0` and canonical quote is missing:

- count remains 0
- no fake first-breach timestamp/state is created

### 11. Shim/base integration regression

Add at least one test through the **real exported watcher package path**, not only an isolated helper.

This regression arose at the semantic seam between:

- `ap_entry_watcher/__init__.py`
- `ap_entry_watcher.py`

Integration test should prove:

```text
shim receives last-only/unusable observation
=> passes non-authoritative/no-usable truth downstream
=> base watcher preserves existing breach continuity
=> no LAST-based trigger
```

This is required to prevent the same cross-file contract drift from returning later.

## Explicit non-goals

Do not modify:

- `ap_execution_core.py`
- `ap/order_state_machine.py`
- `ap/order_monitor.py`
- `ap/contract_selector.py`
- `ap/contract_quote_revalidator.py`
- `ap/brokers/tradier.py`
- `ap/queue.py`
- `client_runner.py`
- fill monitor
- exit engine
- position lifecycle
- proof trades

No threshold changes. No trade-count tuning. No broker behavior changes. No selector changes. No risk loosening. No stop relaxation.

## Acceptance criteria

This PR is acceptable only if all are true:

1. Missing canonical quote means HOLD.
2. Missing quote never increments breach count.
3. Missing quote never resets existing breach count.
4. A valid non-breach/contradictory quote still resets.
5. LAST-only still cannot trigger entry.
6. CALL remains ASK-authoritative.
7. PUT remains BID-authoritative.
8. Post-confirmation stop safety remains active.
9. Retry-backoff safety remains active.
10. `on_trigger` fires at most once per legitimate confirmation lifecycle.
11. No LIVE/PAPER quality thresholds change.
12. No broker/position/queue/proof mutation is added.

## Review checklist before merge

Review must explicitly answer:

- Does this change LIVE behavior? Yes only in watcher liveness under missing/unusable canonical observations; no quality/risk threshold is loosened.
- Is it flag-off or active? State the exact runtime path/default.
- Does it touch broker submit/cancel? Required answer: no.
- Does it mutate orders/positions/proof_trades/queue? Required answer: no new mutation.
- Does it preserve `client_id` / `execution_mode` through existing callback paths? Required answer: yes.
- Does it use the real production quote shape?
- Are diagnostics preserved downstream?
- Does it pollute PAPER/LIVE taxonomy? Required answer: no.
- Could it make Jason trade junk? Required answer: no; only valid canonical observations can increment/confirm.

## Size constraint

Keep production diff small. The essential behavior change is conceptually:

```text
required canonical quote unavailable:
RESET
```

becoming:

```text
required canonical quote unavailable:
HOLD
```

while preserving all safety branches around it. Do not rewrite the watcher.