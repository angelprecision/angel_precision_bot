# P0 SPEC — Broker position unavailability must never become broker-flat truth

## Status

**SPEC ONLY / HARD HOLD. DO NOT MERGE OR DEPLOY AS AN IMPLEMENTATION.**

Base: `main@f26d31cef3d3bf5d3c5d5ff16fb260f245602f89`.

## Proven production defect

Current production path:

```text
ap/brokers/tradier.py::TradierBroker.list_positions()
  -> ap/exit_safety.py::resolve_exit_broker_truth()
  -> ap/order_state_machine.py::APOrderStateMachine.submit_exit()
```

`TradierBroker.list_positions()` catches every exception from `_get()` and returns `[]`.

`_get()` raises on HTTP 401/403/429/5xx and transport failures. Therefore the adapter currently collapses two materially different facts into one return value:

```text
successful broker query, no open positions -> []
broker unavailable / auth failed / rate limited / timeout / server error -> []
```

`resolve_exit_broker_truth()` is intentionally written to distinguish an exception from a successful empty snapshot. It cannot do so because the Tradier adapter swallows the exception. A swallowed failure therefore becomes:

```text
broker_truth_open_qty = 0
is_fresh_exact = True
snapshot_status = contract_absent_open_qty_zero
```

`APOrderStateMachine.submit_exit()` may then mark the local position `CLOSED` under `SYNTHETIC_POSITION_STALE_BROKER_FLAT` while the broker still holds the contracts.

This is a LIVE false-exit / unmanaged-position failure.

## Binding invariant

**Broker uncertainty must remain uncertainty.**

The system may declare an option contract flat only after a successful, parseable, authoritative broker positions response for the exact account.

These states must remain distinct:

```text
SUCCESS_EMPTY    = broker answered successfully and no positions exist
SUCCESS_ABSENT   = broker answered successfully; positions exist but exact OCC absent
SUCCESS_MATCH    = exact OCC present with authoritative quantity
UNAVAILABLE      = timeout/network/auth/rate-limit/5xx/transport exception
MALFORMED        = successful response cannot be interpreted as the supported Tradier positions shape
```

Only the first three may produce fresh exact broker quantity. `UNAVAILABLE` and `MALFORMED` must produce no flat claim and no position-terminalizing side effect.

## Required production changes

### 1. `ap/brokers/tradier.py`

Change `TradierBroker.list_positions()` so transport/HTTP failures propagate to callers, matching the already-hardened contract of `list_orders()`.

Required behavior:

- HTTP 401/403 -> raise; never return `[]`.
- HTTP 429 -> raise; never return `[]`.
- HTTP 5xx -> raise; never return `[]`.
- connect timeout/read timeout/connection error -> raise; never return `[]`.
- JSON/payload shape that cannot be interpreted as the supported positions schema -> raise a deterministic error; never silently coerce malformed truth into flatness.
- successful broker response representing no positions -> return `[]`.
- successful response with one position object -> return normalized one-element list.
- successful response with a position list -> return normalized list.

Do not add retries in this PR. This PR changes truth semantics, not transport policy.

### 2. `ap/exit_safety.py`

Preserve the existing exception-to-unknown behavior in `resolve_exit_broker_truth()` and make the success boundary explicit enough that tests prove:

```text
list_positions raises -> broker_truth_open_qty=None, is_fresh_exact=False
malformed return shape -> broker_truth_open_qty=None, is_fresh_exact=False
successful [] -> broker_truth_open_qty=0, is_fresh_exact=True
successful valid list with exact OCC absent -> 0 / True
successful exact OCC -> exact long qty / True
```

Do not infer success from the type `list` alone if the adapter reports/returns a structured unavailable sentinel. Prefer exceptions for Tradier current-main compatibility.

## Required integration proof

Drive the real `resolve_exit_broker_truth()` -> `submit_exit()` guard path with production-shaped broker stubs.

For every unavailable/malformed case:

- zero `positions.status='CLOSED'` mutation;
- zero `SYNTHETIC_POSITION_STALE_BROKER_FLAT` terminalization;
- zero local exit-order cancellation caused by a flat claim;
- zero proof-trade finalization;
- broker exposure remains represented as unknown/open-risk, not flat;
- exact diagnostic reason is preserved.

For a successful authoritative empty snapshot:

- the existing stale-synthetic-position cleanup behavior may remain, because broker-flat is actually proven.

## Production file budget

Expected production files:

- `ap/brokers/tradier.py`
- `ap/exit_safety.py`

`ap/order_state_machine.py` should not require behavior changes. Tests may exercise it. If a production OSM edit appears necessary, STOP and explain why the existing `is_fresh_exact` contract is insufficient before expanding scope.

Do not touch scanners, selector, entry watcher, sizing, exit strategy thresholds, proof taxonomy, queue, or broker submit/cancel behavior.

## Required tests

Create `tests/test_p0_broker_position_unavailable_not_flat.py`.

Minimum cases:

1. successful Tradier empty positions payload -> `[]` and exact flat.
2. successful `positions=null` production shape -> authoritative empty.
3. successful one-position object -> normalized list.
4. successful list -> normalized list.
5. exact OCC absent from successful nonempty snapshot -> exact zero.
6. exact OCC present qty 4 -> exact 4.
7. 401 -> exception propagates; resolver unknown.
8. 403 -> exception propagates; resolver unknown.
9. 429 -> exception propagates; resolver unknown.
10. 500 -> exception propagates; resolver unknown.
11. connect timeout -> resolver unknown.
12. read timeout -> resolver unknown.
13. connection error -> resolver unknown.
14. malformed positions payload -> unknown, not zero.
15. unavailable broker during `submit_exit()` -> position stays nonterminal; no synthetic-flat close.
16. successful authoritative flat during `submit_exit()` -> existing synthetic-flat cleanup remains reachable.
17. LIVE and PAPER clients remain account/mode isolated; no cross-account snapshot may authorize flatness.
18. diagnostic audit differentiates `broker_positions_error`, `broker_positions_malformed`, and authoritative flat.

Run adjacent suites for exit broker truth and reconciler position truth. Do not rewrite unrelated expectations merely to make the new semantics pass.

## Money-path audit

- Live behavior: **YES, protective fail-closed correction.**
- Broker submit: **NO new submit path.**
- Broker cancel: **NO new cancel path.**
- Orders mutation: only existing downstream behavior; this PR must prevent false terminalization on unavailable truth.
- Positions mutation: prevents false `CLOSED` writes.
- proof_trades: no direct mutation.
- queue: none.
- client_id/execution_mode: unchanged and must remain exact.
- PAPER/LIVE taxonomy: unchanged.
- Could this make Jason trade junk? It cannot create an entry. It prevents a LIVE holding from becoming invisible when Tradier is temporarily unavailable.

## Claude implementation instruction

Start from exact current `main`, not an older branch. Read the full implementations of `TradierBroker._get`, `list_orders`, `list_positions`, `resolve_exit_broker_truth`, and the `SYNTHETIC_POSITION_STALE_BROKER_FLAT` branch of `APOrderStateMachine.submit_exit` before editing.

Reproduce the false-flat path first with a failing production-path test. Then make the smallest change that restores the distinction between unavailable and authoritative empty.

Return in the PR description:

1. exact before/after adapter contract;
2. exact changed files/functions;
3. all broker error classes tested;
4. DB mutation proof for unavailable vs authoritative-flat cases;
5. focused and adjacent test counts;
6. exact-head CI SHA;
7. fresh MERGE / HOLD / HARD HOLD recommendation.

Do not merge, deploy, change environment variables, or mutate production data.