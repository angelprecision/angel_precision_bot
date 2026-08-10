# P0 SPEC — Exact broker EXIT fill truth before realized P&L finalization

**Status: DRAFT / HARD HOLD. Implementation contract only. Do not merge or deploy until production code + exact-head tests are on this branch and independently audited.**

Base at branch creation: `main` / `570a56933615cbac82e3256c0e350119eca80c0d`.

## Incident that created this PR

2026-08-10 manual close of the C Aug 14 2026 $135 CALL:

- economic position: `C260814C00135000`
- quantity: 9
- broker ENTRY fill: `$2.33`
- operator manual SELL_TO_CLOSE: 9
- Tradier actual manual EXIT fill: `$1.95`
- canonical proof row instead recorded exit approximately `$0.71`, approximately `-69.5%`
- proof row carried no authoritative broker exit order id/timestamp and used `RECONCILER_AUTO_CLOSE | HIGH | broker_position_missing`

The `$0.71` value was traced to an older, different C option trade: `C260807P00134000`. The stale row was broker order `36637227`, a PAPER PUT EXIT for 21 contracts with 21 filled at `$0.71`.

This is not a stale-quote/display bug. Realized economic truth was attributed from the wrong OCC contract.

## Verification already completed

Current `main` source was read directly before opening this spec.

### Confirmed unsafe lookup

`ap_reconciler.py:2930-2956`, `_get_recent_exit_fill()` currently queries:

```sql
SELECT fill_price, filled_qty, updated_ts
FROM orders
WHERE client_id = %s
  AND kind = 'EXIT'
  AND status IN ('FILLED', 'EXIT_FILLED')
  AND (contract = %s OR symbol = %s OR symbol = %s)
ORDER BY updated_ts DESC
LIMIT 1
```

Bindings are:

```python
(self.client_id, contract, contract, underlying)
```

The final `symbol = underlying` branch is the defect. For ticker `C`, it allows a filled EXIT from any C option contract to become HIGH-confidence evidence for `C260814C00135000`.

### Confirmed destructive consumer

`ap_reconciler.py:3082+`, `_handle_db_position_missing_at_broker()` calls:

```python
exit_fill = self._get_recent_exit_fill(contract, underlying)
```

If any positive row is returned, it immediately sets:

```python
exit_px = float(exit_fill["fill_price"])
close_confidence = "HIGH"
```

and later calls `_execute_reconciler_close()`.

### Confirmed second unsafe economics fallback

When no filled EXIT row is found, the same function waits for three ghost observations and can still finalize the position using either:

```python
exit_px = self._get_current_option_price(contract)
close_confidence = "MEDIUM_THREE_PASS_CURRENT_MARK"
```

or:

```python
exit_px = entry_px
close_confidence = "MEDIUM_THREE_PASS_NO_EXIT_EVIDENCE"
```

`_execute_reconciler_close()` then writes `positions.exit_price`, realized P&L, terminal status, and a `proof_trades` row for a full close. Therefore removing the ticker fallback alone is insufficient. The reconciler can still manufacture realized economics from a mark or entry price before the exact manual-close path sees the external broker fill.

### Confirmed correct existing authority already exists

`ap/manual_close_reconciliation.py` already contains the strict path we should preserve:

- `select_external_close_fills()` begins around line 552.
- requires exact OCC contract match;
- requires SELL_TO_CLOSE side;
- requires filled/partially-filled broker status;
- requires fill timestamp at/after position entry;
- requires exact aggregate quantity;
- durable adopted fills are revalidated for exact contract, CALL/PUT direction, filled status, timestamp, and economics;
- `detect_manual_closes()` begins around line 1191 and finalizes through `APPositionManager.close_position_from_exit_fill()` using weighted broker fill truth.

That subsystem never submits or cancels broker orders. The required change is to stop the generic reconciler from outracing it with invented economics.

## Root cause

Two independent truth authorities exist for a broker-missing position:

1. strict manual-close reconciliation: exact external broker EXIT fill truth;
2. generic reconciler: ticker-level EXIT lookup or three-pass mark/entry fallback.

The generic path can finalize first. Once the position/proof is terminalized with fabricated economics, the strict external-fill path no longer owns a clean active position to finalize.

## Required production change

### File 1 — `ap_reconciler.py`

Keep this surgical. Do not redesign reconciliation.

#### Change A — exact fill identity at `_get_recent_exit_fill`, current lines 2930-2956

Change the method signature to accept the position identity and exact mode:

```python
def _get_recent_exit_fill(
    self,
    contract: str,
    *,
    position_id: str,
    execution_mode: str,
) -> Optional[dict]:
```

Required validation before SQL:

```python
contract = self._norm_contract(contract)
position_id = str(position_id or "").strip()
mode = _normalize_execution_mode(execution_mode)
if not contract or not position_id or mode is None:
    return None
```

Required SQL shape:

```sql
SELECT broker_order_id,
       local_order_id,
       position_id,
       contract,
       execution_mode,
       fill_price,
       filled_qty,
       filled_ts,
       updated_ts
FROM orders
WHERE client_id = %s
  AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s
  AND kind = 'EXIT'
  AND status IN ('FILLED', 'EXIT_FILLED', 'EXIT_PARTIAL_FILL')
  AND UPPER(TRIM(COALESCE(contract, ''))) = %s
  AND position_id::text = %s
ORDER BY COALESCE(filled_ts, updated_ts) DESC
LIMIT 2
```

Required behavior:

- 0 matches -> `None`.
- exactly 1 valid match -> return it.
- 2+ matches -> do **not** pick newest arbitrarily; emit `RECONCILER_EXIT_FILL_IDENTITY_AMBIGUOUS` and return `None` unless a separately proven aggregate helper is added. For this PR, prefer HOLD over inventing weighted semantics inside the generic reconciler.
- fill price must be finite/positive.
- filled quantity must be positive.
- broker order id must be nonblank.
- never match by underlying ticker.
- never infer execution mode from environment, client id, or broker URL.

If a legitimate existing bot EXIT row does not yet carry `position_id`, do not weaken this query to ticker-level matching. Let the existing fill-monitor/manual-close/recovery path bind identity first.

#### Change B — broker-missing position handler, current lines 3082+

Replace:

```python
exit_fill = self._get_recent_exit_fill(contract, underlying)
```

with exact identity:

```python
position_mode = _normalize_execution_mode(pos.get("execution_mode"))
exit_fill = self._get_recent_exit_fill(
    contract,
    position_id=str(pos_id or ""),
    execution_mode=position_mode or "",
)
```

If exact fill exists, existing close/finalization may continue.

Before exact fill evidence can authorize finalization, the single fill must cover
the position's current unresolved quantity:

- resolve current `quantity_remaining` (falling back to stored `qty` only when
  `quantity_remaining` is NULL, matching the existing close path);
- require `filled_qty == quantity_remaining` exactly;
- an `EXIT_PARTIAL_FILL` or any smaller positive fill is not sufficient;
- on mismatch or unreadable quantity, emit
  `RECONCILER_EXIT_FILL_QTY_COVERAGE_UNPROVEN` and HOLD without a position,
  realized-P&L, or `proof_trades` mutation;
- do not aggregate multiple fills in this PR. Multiple exact candidates remain
  ambiguity HOLD.

If exact fill does **not** exist:

- preserve the existing active local/broker EXIT guard;
- preserve ghost counting as diagnostic evidence;
- **do not call `_execute_reconciler_close()` from a current quote or entry price**;
- do not set terminal `positions.status`;
- do not write `exit_price` / realized P&L;
- do not write `proof_trades`;
- keep the position visible to the strict manual-close detector/recovery path;
- emit a stable diagnostic, e.g.:

```text
BROKER_POSITION_MISSING_EXIT_FILL_UNPROVEN
```

Include:

- client_id
- execution_mode
- position_id
- exact contract
- underlying
- ghost pass count
- local active-exit result
- broker working-exit result
- reason `exact_broker_exit_fill_missing`

Three broker-flat observations prove the position is absent at the broker. They do **not** prove a realized fill price.

#### Change C — do not alter `_execute_reconciler_close()` semantics broadly

Keep `_execute_reconciler_close()` for callers that provide exact proven economics. Do not turn this PR into a position-manager rewrite.

Add a defensive assertion at its entrance if useful:

```python
if close_confidence == "HIGH" and not exact_exit_fill_proven:
    ... block ...
```

Only add the parameter if necessary to prevent future accidental use. Prefer the smallest call-site correction if all other callers are already exact.

### File 2 — focused regression tests

Create:

`tests/test_p0_reconciler_exact_exit_fill_truth.py`

Use production-shaped dict rows / PostgreSQL where the existing test harness makes that cheap. Do not copy production formulas into tests.

Required cases:

1. **Exact C incident**: target position `C260814C00135000`, older PAPER filled EXIT `C260807P00134000` / broker order `36637227` / 21 contracts at `0.71`, same client/ticker. `_get_recent_exit_fill()` must return `None` for target position.
2. Exact target EXIT for `C260814C00135000` at `1.95`, matching `position_id`, mode and client -> returned.
3. Exact contract but wrong `position_id` -> rejected.
4. Exact contract/position but PAPER row queried by LIVE reconciler -> rejected.
5. Exact contract/position but LIVE row queried by PAPER reconciler -> rejected.
6. Multiple exact candidate rows -> ambiguity HOLD, never newest-wins.
7. Broker-flat pass 1 -> position unchanged.
8. Broker-flat pass 2 -> position unchanged.
9. Broker-flat pass 3 with live mark available -> position STILL unchanged; no realized P&L/proof write.
10. Broker-flat pass 3 without live mark -> position STILL unchanged; no zero-P&L fabricated close.
11. Active local/broker EXIT continues to block ghost close as today.
12. Strict manual-close selector still accepts exact external STC `9 @ 1.95` for the C position.
13. Existing exact bot `EXIT_FILLED` row with exact position identity still allows generic reconcile finalization.
14. No broker submit call.
15. No broker cancel call.
16. Exact EXIT fill qty 2 vs current remaining 4 -> coverage HOLD, no close/proof mutation.
17. `EXIT_PARTIAL_FILL` qty 2 vs current remaining 4 -> coverage HOLD.
18. Exact EXIT fill qty 2 vs current remaining 2 -> existing close path remains functional.

## Explicit non-goals

Do not change:

- exit thresholds;
- touched-profit/runner behavior;
- scanner or selector;
- entry retry;
- queue;
- sizing/risk;
- broker submit/cancel behavior;
- manual-close broker discovery rules except tests if needed;
- proof taxonomy architecture;
- migrations/schema.

Do not repair today's historical C row automatically inside runtime code. Historical correction/audit is a separate controlled data operation after this faucet is closed.

## Production file budget

Expected:

1. `ap_reconciler.py`
2. `tests/test_p0_reconciler_exact_exit_fill_truth.py`
3. `.github/workflows/p0_regression.yml` only if the focused test is not already picked up by an existing pattern

If implementation needs more production files, stop and explain why before widening scope.

## Money-path audit answers

- Changes live behavior: **YES**, only broker-missing position reconciliation.
- Flag-off or active: **ACTIVE path**.
- Broker submit/cancel touched: **NO**.
- Orders mutation: lookup only in the changed path; no new order writer.
- Positions mutation: prevents unproven terminal mutation; exact-fill close remains existing behavior.
- proof_trades mutation: prevents unproven proof writes; no new proof writer.
- Queue mutation: **NO**.
- `client_id` preserved: **REQUIRED exact**.
- `execution_mode` preserved: **REQUIRED exact; no default**.
- Production metadata shape: uses current `orders`/`positions` columns already consumed by these modules.
- Diagnostics downstream: add explicit unproven/ambiguous reasons; do not erase existing fields.
- PAPER/LIVE pollution risk: reduced by exact mode fence.
- Could make Jason trade junk: **NO entry path is touched**.

## Required validation before merge consideration

```bash
python -m pytest -q \
  tests/test_p0_reconciler_exact_exit_fill_truth.py \
  tests/test_p0_manual_client_close_proof.py \
  tests/test_p0_manual_close_terminal_recovery.py \
  tests/test_p0_partial_close_reconciler.py
python -m py_compile ap_reconciler.py ap/manual_close_reconciliation.py

git diff --check
```

Then run the authoritative P0 workflow on the exact head.

Required grep/audit:

```bash
grep -R "symbol = %s OR symbol = %s" -n ap_reconciler.py
grep -R "MEDIUM_THREE_PASS_CURRENT_MARK\|MEDIUM_THREE_PASS_NO_EXIT_EVIDENCE" -n ap_reconciler.py
```

Acceptance is not "tests green." Acceptance is:

- wrong C contract `$0.71` cannot satisfy C Aug14 $135 CALL exit truth;
- broker-flat with no exact EXIT fill cannot create realized P&L or proof;
- exact `$1.95` STC remains recoverable by the strict manual-close path;
- zero new broker POST/cancel call sites;
- exact client/mode/position/contract identity survives end-to-end.

Final review must read PR description, actual diff, comments, changed files, exact runtime path and issue **MERGE / HOLD / HARD HOLD**.
