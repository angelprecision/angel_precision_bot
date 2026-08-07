# PR #424 Exact Patch Amendment — Claude implementation map

This amendment is binding. It translates the AVGO reservation defect into exact function-level edits so implementation does not wander into entry logic, strategy, or a second exit lifecycle.

## Patch 1 — ap/exit_safety.py

### A. Preserve the existing public field

Do NOT rename or redefine:

```python
broker_truth_open_qty
```

It remains total broker-confirmed long position quantity.

Add reservation truth alongside it.

### B. Add canonical open-exit status sets near module constants

Use lowercase normalized values:

```python
_OPEN_EXIT_ORDER_STATUSES = {
    "open",
    "pending",
    "accepted",
    "submitted",
    "queued",
    "working",
    "acknowledged",
    "partially_filled",
    "partial_fill",
}

_TERMINAL_EXIT_ORDER_STATUSES = {
    "filled",
    "canceled",
    "cancelled",
    "rejected",
    "expired",
}
```

Do not count terminal orders as reservations.

### C. Add pure broker-order extraction helpers in this module

Do not import `ap.exit_autonomous_recovery` from `ap.exit_safety`; avoid lifecycle-layer circular dependency.

Add small pure helpers equivalent to:

```python
def _extract_order_status(raw: dict[str, Any]) -> str:
    return _normalize_text(
        raw.get("status")
        or raw.get("Status")
        or raw.get("state")
        or raw.get("order_status")
    )


def _extract_order_id(raw: dict[str, Any]) -> str:
    return str(
        raw.get("broker_order_id")
        or raw.get("order_id")
        or raw.get("id")
        or raw.get("orderId")
        or ""
    ).strip()


def _extract_order_contract(raw: dict[str, Any]) -> str:
    return _normalize_contract(
        raw.get("contract")
        or raw.get("symbol")
        or raw.get("option_symbol")
        or raw.get("instrument")
    )


def _extract_order_account_id(raw: dict[str, Any]) -> str:
    return _normalize_text(
        raw.get("account_id")
        or raw.get("account")
        or raw.get("account_number")
    )
```

Support nested `raw` payloads the same way `resolve_exit_broker_truth()` already supports nested broker position shapes when the real adapter fixtures prove nested order fields exist.

### D. Add exact SELL_TO_CLOSE classifier

Use the same defensive vocabulary already present elsewhere in the repo:

```python
def _is_sell_to_close_order(raw: dict[str, Any]) -> bool:
    text = " ".join(
        str(raw.get(k) or "")
        for k in (
            "side", "action", "instruction", "order_action",
            "transaction_type", "trade_action", "type",
            "description", "memo", "notes",
        )
    ).lower()
    compact = text.replace("_", "").replace("-", "").replace(" ", "")
    return (
        "selltoclose" in compact
        or compact == "stc"
        or "sell to close" in text
    )
```

Do not classify generic SELL as SELL_TO_CLOSE unless the actual broker adapter guarantees that semantic for option-closing orders and focused fixtures prove it.

### E. Add exact quantity parser that distinguishes UNKNOWN from zero

Do not use `_safe_int(... ) or 0` for live matching orders.

Conceptual helper:

```python
def _extract_order_qty(raw: dict[str, Any]) -> Optional[int]:
    for key in ("qty", "quantity", "order_qty"):
        value = _safe_int(raw.get(key))
        if value is not None:
            return abs(value)
    return None


def _extract_order_cumulative_filled_qty(raw: dict[str, Any]) -> Optional[int]:
    for key in (
        "filled_qty", "filled_quantity", "cumulative_filled_qty",
        "exec_quantity",
    ):
        value = _safe_int(raw.get(key))
        if value is not None:
            return max(0, abs(value))
    # Some broker payloads expose remaining quantity instead of filled quantity.
    # Do not manufacture filled=0 unless order_qty itself is exact and the real
    # broker shape proves missing filled means zero for an unfilled live order.
    return None
```

Where the broker fixture provides `remaining_qty` / `remaining_quantity` as authoritative, prefer direct remaining quantity rather than deriving it.

### F. Add `resolve_exit_reservation_truth(...)`

Signature:

```python
def resolve_exit_reservation_truth(
    *,
    broker: Any,
    client_id: str,
    contract: str,
    broker_open_qty: Optional[int],
) -> dict[str, Any]:
```

Return shape:

```python
{
    "working_exit_reserved_qty": Optional[int],
    "available_to_close_qty": Optional[int],
    "reservation_truth_exact": bool,
    "matching_working_exit_orders": list[dict],
    "audit": dict,
}
```

Broker order-list acquisition should try the broker’s real available methods in the same bounded defensive style already used by recovery:

```python
for method_name in ("list_open_orders", "get_open_orders", "list_orders", "orders"):
    ...
```

Prefer an API call scoped to `status="open"` when supported. If the available method returns all orders, filter by normalized live status.

Fail-closed rules:

- no callable order-list method -> `reservation_truth_exact=False`;
- API exception -> false;
- malformed payload -> false;
- matching live SELL_TO_CLOSE with unknown order quantity -> false;
- matching live SELL_TO_CLOSE with order qty exact but fill/remaining shape ambiguous -> false unless real adapter semantics prove exact remaining qty;
- exact empty set of matching live exits -> reserved=0;
- terminal matching exits -> ignored, not unknown.

For each exact matching working exit persist diagnostic fields only:

```python
{
    "broker_order_id": ...,
    "status": ...,
    "order_qty": ...,
    "cumulative_filled_qty": ...,
    "remaining_reserved_qty": ...,
    "account": ...,
    "contract": ...,
}
```

Then:

```python
reserved = sum(order["remaining_reserved_qty"] for order in matches)
available = max(0, int(broker_open_qty) - reserved)
```

If total reservation exceeds broker open qty, do NOT clamp and call truth healthy. Return `reservation_truth_exact=False` with `reservation_exceeds_open_qty` in audit. That shape means at least one snapshot is stale or malformed.

### G. Extend `resolve_exit_broker_truth()`

After exact broker position quantity is resolved, call `resolve_exit_reservation_truth(...)` and merge the result into the top-level return.

Expected healthy shape:

```python
{
    "broker_truth_open_qty": 7,
    "working_exit_reserved_qty": 2,
    "available_to_close_qty": 5,
    "reservation_truth_exact": True,
    "matching_working_exit_orders": [...],
    "is_fresh_exact": True,
    "audit": {...},
}
```

Important: `is_fresh_exact` continues to describe broker POSITION truth. Reservation truth has its own boolean. Do not silently fold an open-order API outage into `is_fresh_exact=False` and break existing broker-flat consumers that only require position truth.

## Patch 2 — ap_exit_engine.py final submit gate

Current `_submit_exit_decision()` already calls:

```python
_broker_truth = resolve_exit_broker_truth(...)
```

Immediately after the existing broker-open quantity classification, extract:

```python
_reservation_exact = _broker_truth.get("reservation_truth_exact") is True
_reserved_qty = _broker_truth.get("working_exit_reserved_qty")
_available_qty = _broker_truth.get("available_to_close_qty")
```

Add these to `_broker_truth_audit` and every quantity block diagnostic:

```python
_broker_truth_audit.update({
    "working_exit_reserved_qty": _reserved_qty,
    "available_to_close_qty": _available_qty,
    "reservation_truth_exact": _reservation_exact,
    "matching_working_exit_orders": _broker_truth.get(
        "matching_working_exit_orders", []
    ),
})
```

### A. Normal SCALE_OUT / soft exit

Before callback submit:

```python
_requested_qty = int(decision.quantity or 0)
```

If position truth is exact OPEN but reservation truth is not exact:

- normal SCALE_OUT / profit exit -> BLOCK submit with `EXIT_BLOCKED_RESERVATION_TRUTH_UNAVAILABLE`;
- keep protective monitoring;
- do not guess available quantity.

If reservation truth is exact and `_requested_qty > _available_qty`:

- if an existing exact Angel Precision working exit already owns the same position/tranche, return HOLD and let PR #423 stale-exit lifecycle manage it;
- do not submit overlapping quantity;
- emit `EXIT_BLOCKED_WORKING_EXIT_RESERVATION`.

Do not silently cap a strategy SCALE_OUT from 2 to 1 unless the strategy action explicitly allows partial tranche reduction and tests prove downstream scale-count semantics remain correct. Default: block duplicate/overlap and let existing exit owner finish or retry.

### B. Forced-risk exit / emergency flatten

Do not use the normal “reservation unavailable -> HOLD” behavior for an emergency flatten forever. Route forced full flatten to a dedicated exact cancellation preparation method described below.

## Patch 3 — ap_exit_engine.py emergency flatten preparation

Add a private method on APExitEngine:

```python
def _prepare_emergency_flatten_quantity(
    self,
    pos: ManagedPosition,
    *,
    reason: str,
) -> dict:
    ...
```

Return one of these dispositions:

```python
{
    "ok": True,
    "submit_qty": 7,
    "broker_truth": {...},
    "canceled_exit_ids": [...],
    "reason_code": "FLATTEN_READY",
}
```

or:

```python
{
    "ok": False,
    "submit_qty": 0,
    "broker_truth": {...},
    "reason_code": "FLATTEN_EXISTING_EXIT_CANCEL_UNPROVEN",
}
```

Required sequence:

1. `resolve_exit_broker_truth()`.
2. If position truth exact FLAT -> no submit; route through existing broker-flat local close path.
3. If position truth unknown -> fail closed; no guessed sell.
4. If reservation truth exact and reserved=0 -> submit broker open qty.
5. If matching working exits exist:
   - identify the pending exact Angel Precision broker order by `pos.pending_exit_broker_order_id` first;
   - only cancel by exact broker id;
   - if multiple working exits exist and exact ownership cannot be proven, route to existing recovery/reconciler ambiguity handling; do not cancel arbitrary orders by contract alone;
   - cancel exact known order(s) using existing broker cancel transport;
   - re-query exact broker order status until current call’s bounded proof mechanism is exhausted; do not sleep for long periods inside exit engine;
   - only terminal cancel/reject/expire counts as cancellation proof;
   - FILLED/PARTIAL discovered during cancel must be applied through existing fill reconciliation before final quantity calculation.
6. Re-run `resolve_exit_broker_truth()` after confirmed cancellation/fill reconciliation.
7. Require `reservation_truth_exact=True` and reserved=0 for the canonical one-order EXIT ALL path.
8. `submit_qty = exact broker_truth_open_qty` from the re-read, NOT stale `pos.quantity_remaining`.

If the implementation intentionally chooses the disjoint-tranche alternative (leave old exact SELL 2 working + submit only available 5), that must be a named disposition and separately tested. Default contract for EXIT ALL is cancel-existing-then-single-flatten-owner.

## Patch 4 — emergency_flatten() call site

Current emergency flatten constructs a CLOSE_ALL decision from local quantity.

Replace the quantity authority with `_prepare_emergency_flatten_quantity()`.

Conceptual shape:

```python
prep = self._prepare_emergency_flatten_quantity(pos, reason=reason)
if not prep.get("ok"):
    self._emit_exit_event(... prep reason/audit ...)
    continue

_submit_qty = int(prep.get("submit_qty") or 0)
if _submit_qty <= 0:
    continue

decision = ExitDecision(
    action="CLOSE_ALL",
    quantity=_submit_qty,
    ... existing reason / pnl / urgency ...,
)
```

Passing `allow_inflight_override=True` afterward must only override in-memory duplicate gating after the preparation step has proven there is no overlapping broker reservation. It must never mean “ignore existing broker exits.”

## Patch 5 — ap_execution_core.py callback audit

Inspect `_on_position_close` and `_on_position_scale`.

Hard invariant:

```python
broker submit quantity == int(decision.quantity)
```

No callback may replace that with:

- `pos.quantity`
- original entry qty
- stale DB qty
- `pos.quantity_remaining` after the final safety resolver already chose a smaller/exact broker quantity.

If current code already consumes `decision.quantity` exactly, production file stays unchanged and add a regression test.

Do not change execution-core pricing in this PR.

## Patch 6 — DB quantity invariant tests

No new production DB mutation is required unless a real-method test proves current code violates this.

Lock the invariant:

```text
submission/acknowledgement/reservation/cancel request/cancel confirmation
DO NOT decrement quantity_remaining

broker-confirmed cumulative fill or exact broker-position reconciliation
MAY decrement quantity_remaining
```

AVGO exact replay:

```text
start DB remaining=7
working STC qty=2 filled=0 -> DB remains 7, reserved 2, available 5
partial fill 1 -> DB becomes 6, remaining reservation 1, available 5
cancel remaining 1 -> DB remains 6, reserved 0, available 6
EXIT ALL -> SELL 6
broker fill 6 -> DB becomes 0 / CLOSED only after confirmation
```

## Patch 7 — broker rejection diagnostic

At the callback/broker-rejection seam, if a broker nevertheless rejects for insufficient available quantity after `reservation_truth_exact=True`, emit a distinct critical diagnostic:

`EXIT_BROKER_REJECTED_DESPITE_EXACT_RESERVATION_PREFLIGHT`

Include:

- requested qty
- broker open qty
- reserved qty
- available qty
- matching exit ids/statuses
- snapshot timestamps
- client_id
- execution_mode
- position_id
- contract

Do not discard this evidence into a generic rejection counter only.

## Required real-method tests

Suggested test file remains:

`tests/test_p0_exit_reservation_aware_flatten_avgo.py`

Tests must drive real:

- `resolve_exit_broker_truth`
- new reservation resolver
- `APExitEngine._submit_exit_decision`
- `APExitEngine.emergency_flatten`
- existing `note_partial_exit_fill`
- execution-core callbacks if quantity preservation needs proof

Mock only external broker/DB/network boundaries.

Do not copy the available-quantity formula into a test helper and claim the production method works.

## No-scope-creep assertions

Final production diff should not touch:

- scanners
- intelligence
- master-control admission
- contract selector
- entry watcher
- entry submit/retry
- queue
- sizing rules
- TP/SL thresholds
- proof-trade taxonomy

Expected runtime files remain `ap/exit_safety.py`, `ap_exit_engine.py`, and only if proven necessary `ap_execution_core.py`.