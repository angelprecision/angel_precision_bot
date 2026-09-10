# ap/reconcile.py
# =============================================================================
# Reconciles local order state against Tradier.
# Called periodically to catch any orders the fill_monitor missed.
#
# FIXES applied vs original:
#   1. _create_position_from_entry now sets client_id on the position row
#   2. _close_position_from_exit sets exit_price + updates realized_pnl_today
#      in client_state (matches fill_monitor behaviour exactly)
#   3. audit() includes client_id on every row
#   4. _get_open_orders filters by client_id — multi-client safe
#   5. Small sleep between broker API calls to respect rate limits
#   6. Idempotency guard: skip exit processing if position already CLOSED
#   7. Expiry guard: never create a position for an expired contract —
#      auto-cancels the stale order so it stops appearing in reconcile loops
# =============================================================================

import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ap.logger import get_logger
from ap.utils import now_utc_iso, json_dumps
from ap.db import conn, run_with_retry, update_order
from ap.config import Config

log = get_logger("ap.reconcile")
cfg = Config()

OPT_MULTIPLIER      = 100    # standard US equity options
BROKER_CALL_SLEEP   = 0.15   # seconds between broker API calls — avoids rate-limit cascade


# =============================================================================
# HELPERS
# =============================================================================

def audit(client_id: str, level: str, event: str, payload: dict):
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "INSERT INTO audit_log (ts, level, event, payload, client_id) VALUES (%s,%s,%s,%s,%s)",
            (now_utc_iso(), level, event, json_dumps(payload), client_id),
        ))


def _tradier_status_to_local(remote: dict) -> str:
    """Map Tradier order status strings to our local status enum."""
    s = str(remote.get("status", "")).lower().strip()
    if s == "filled":
        return "FILLED"
    if s in ("partially_filled", "partial_filled", "partially-filled"):
        return "PARTIAL"
    if s == "rejected":
        return "REJECTED"
    if s in ("canceled", "cancelled"):
        return "CANCELED"
    if s in ("open", "pending", "submitted"):
        return "ACK"
    return "ACK"


def _extract_error(remote: dict) -> Optional[str]:
    for k in ("reason", "message", "error"):
        v = remote.get(k)
        if v:
            return str(v)
    return None


def _to_int(x: Any) -> Optional[int]:
    try:
        return int(float(x))
    except Exception:
        return None


def _to_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def _extract_filled_qty(remote: Dict[str, Any]) -> Optional[int]:
    for k in ("exec_quantity", "filled_quantity", "filled",
              "quantity_executed", "last_fill_quantity"):
        if k in remote and remote.get(k) is not None:
            v = _to_int(remote[k])
            if v is not None:
                return v
    return None


def _extract_fill_price(remote: Dict[str, Any]) -> Optional[float]:
    """Prefer avg_fill_price; fall back to last_fill_price."""
    for k in ("avg_fill_price", "last_fill_price"):
        fv = _to_float(remote.get(k))
        if fv is not None and fv > 0:
            return fv
    return None


def _infer_direction_from_contract(contract: str) -> str:
    """AAPL260130C00200000 -> CALL, AAPL260130P00200000 -> PUT"""
    m = re.search(r"\d{6}([CP])", contract)
    if m:
        return "CALL" if m.group(1) == "C" else "PUT"
    return "CALL"


def _is_expired(contract: str) -> bool:
    """
    FIX 7: Parse the 6-digit expiry from any OCC contract symbol and return
    True if that date is in the past.

    Example: AAPL260130C00240000 -> expiry 2026-01-30 -> expired as of today.

    Prevents the reconciler from creating live position records for contracts
    that already expired — which causes the exit manager to loop forever
    trying to price a dead contract.
    """
    m = re.search(r"(\d{6})[CP]", contract)
    if not m:
        return False
    try:
        exp = datetime.strptime(m.group(1), "%y%m%d").replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) > exp
    except Exception:
        return False


# =============================================================================
# DB READS
# =============================================================================

def _get_open_orders(client_id: str, limit: int = 50) -> list[dict]:
    """
    FIX 4: filter by client_id — safe in multi-client deployments.
    Only returns orders that have a real broker_order_id (not N/A).
    """
    with conn() as c:
        rows = run_with_retry(lambda: c.execute(
            """
            SELECT id, local_order_id, broker_order_id, status, kind,
                   symbol, contract, qty, position_id, filled_qty, client_id
            FROM orders
            WHERE client_id = %s
              AND broker_order_id IS NOT NULL
              AND broker_order_id != 'N/A'
              AND status IN ('NEW', 'ACK', 'PARTIAL')
            ORDER BY updated_ts ASC
            LIMIT ?
            """,
            (client_id, limit),
        ).fetchall())
        return [dict(r) for r in rows]


# =============================================================================
# POSITION CREATION (ENTRY fill)
# =============================================================================

def _create_position_from_entry(order_row: dict, fill_price: float, filled_qty: int) -> str:
    """
    FIX 1: sets client_id on the new position row.
    FIX 7: expiry guard — refuses to create a position for an expired contract
           and auto-cancels the stale order so it never appears again.
    Called only when: order is ENTRY + FILLED + position_id is None.
    """
    contract  = order_row["contract"]
    client_id = order_row.get("client_id", "default")

    # ── EXPIRY GUARD (FIX 7) ─────────────────────────────────────────────────
    if _is_expired(contract):
        log.warning(
            "RECONCILE_SKIP_EXPIRED | client=%s contract=%s local_order_id=%s | "
            "Refusing to create position for expired contract.",
            client_id, contract, order_row.get("local_order_id"),
        )
        # Auto-cancel the order so it stops appearing in _get_open_orders
        with conn() as c:
            run_with_retry(lambda: c.execute(
                "UPDATE orders SET status='CANCELED', last_error=%s, updated_ts=%s "
                "WHERE local_order_id=%s",
                (
                    "auto-canceled: contract expired at reconcile time",
                    now_utc_iso(),
                    order_row["local_order_id"],
                ),
            ))
        audit(client_id, "WARN", "RECONCILE_SKIP_EXPIRED", {
            "local_order_id": order_row.get("local_order_id"),
            "contract":       contract,
            "reason":         "contract expired — position creation blocked",
        })
        return ""
    # ─────────────────────────────────────────────────────────────────────────

    pos_id    = str(uuid.uuid4())
    direction = _infer_direction_from_contract(contract)
    tp_pct    = float(cfg.TAKE_PROFIT_PCT)
    sl_pct    = float(cfg.STOP_LOSS_PCT)

    with conn() as c:
        run_with_retry(lambda: c.execute(
            """
            INSERT INTO positions (
                id, client_id, underlying, contract, direction,
                qty, avg_fill, entry_ts, tp_pct, sl_pct, status
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                pos_id,
                client_id,
                order_row["symbol"],
                contract,
                direction,
                int(filled_qty),
                float(fill_price),
                now_utc_iso(),
                tp_pct,
                sl_pct,
                "OPEN",
            ),
        ))

        # Link order -> position
        run_with_retry(lambda: c.execute(
            "UPDATE orders SET position_id=%s, updated_ts=%s WHERE local_order_id=%s",
            (pos_id, now_utc_iso(), order_row["local_order_id"]),
        ))

    return pos_id


# =============================================================================
# POSITION CLOSE (EXIT fill)
# =============================================================================

def _close_position_from_exit(order_row: dict, fill_price: float):
    """
    PR #579 (Step 8): Route bot-owned EXIT fills through the ONE canonical
    authority — APPositionManager.converge_position_from_durable_exit_order().

    The legacy direct position UPDATE is removed because it violated the
    one-canonical-authority requirement: no exact broker fill timestamp, no
    durable EXIT watermark, no full identity fence.

    External/manual EXIT rows are excluded — they belong to the manual-close
    authority and must not be re-routed here.
    """
    client_id      = str(order_row.get("client_id") or "").strip()
    local_order_id = str(order_row.get("local_order_id") or "").strip()
    execution_mode = str(order_row.get("execution_mode") or "").strip().lower()

    if not local_order_id:
        log.error(
            "[%s] _close_position_from_exit: no local_order_id on EXIT order",
            client_id,
        )
        return

    # External/manual EXIT rows belong to the manual-close authority.
    meta = order_row.get("meta") or {}
    if isinstance(meta, str):
        try:
            import json as _json
            meta = _json.loads(meta)
        except Exception:
            meta = {}
    if (
        local_order_id.startswith("external-exit:")
        or str(meta.get("external_broker_order") or "").lower() == "true"
    ):
        log.debug(
            "[%s] _close_position_from_exit: skipping external/manual EXIT %s",
            client_id, local_order_id,
        )
        return

    if execution_mode not in ("live", "paper"):
        log.error(
            "[%s] _close_position_from_exit: unresolved execution_mode=%r "
            "for EXIT %s — HOLD",
            client_id, execution_mode, local_order_id,
        )
        return

    try:
        from ap.position_manager import APPositionManager
        pm = APPositionManager(client_id)
        result = pm.converge_position_from_durable_exit_order(
            exit_local_order_id=local_order_id,
            expected_execution_mode=execution_mode,
        )
        disposition = str(getattr(result, "disposition", "") or "")
        if disposition in ("APPLIED_FULL", "APPLIED_PARTIAL", "ALREADY_APPLIED"):
            log.info(
                "[%s] reconcile _close_position_from_exit converged | "
                "order=%s disposition=%s",
                client_id, local_order_id, disposition,
            )
        else:
            log.warning(
                "[%s] reconcile _close_position_from_exit HOLD | "
                "order=%s disposition=%s reason=%s",
                client_id, local_order_id, disposition,
                getattr(result, "reason", ""),
            )
    except Exception as exc:
        log.error(
            "[%s] _close_position_from_exit canonical convergence failed "
            "order=%s: %s",
            client_id, local_order_id, exc,
        )


# =============================================================================
# MAIN RECONCILE LOOP
# =============================================================================

def reconcile_once(broker, client_id: str = "default", limit: int = 50) -> int:
    """
    Poll Tradier for each open/pending order and sync local state.

    FIX 4: takes client_id — only reconciles that client's orders.
    FIX 5: sleeps between broker API calls to avoid rate-limit cascade.

    Returns: count of orders processed (state changed or checked).
    """
    orders = _get_open_orders(client_id, limit=limit)
    if not orders:
        return 0

    processed = 0

    for o in orders:
        broker_id = o.get("broker_order_id")
        if not broker_id or broker_id == "N/A":
            continue

        try:
            remote = broker.get_order(broker_id)

            new_status     = _tradier_status_to_local(remote)
            err            = _extract_error(remote) if new_status == "REJECTED" else None
            new_filled_qty = _extract_filled_qty(remote)
            if new_filled_qty is None:
                new_filled_qty = int(o.get("filled_qty") or 0)

            # Only write + audit when something actually changed
            prev_status    = o.get("status")
            prev_filled    = int(o.get("filled_qty") or 0)
            status_changed = prev_status != new_status
            qty_changed    = prev_filled != int(new_filled_qty or 0)

            if status_changed or qty_changed:
                update_order(
                    o["local_order_id"],
                    status          = new_status,
                    broker_order_id = broker_id,
                    last_error      = err,
                    filled_qty      = new_filled_qty,
                )

                # FIX 3: client_id on every audit row
                audit(client_id, "INFO", "RECONCILE_ORDER_UPDATE", {
                    "local_order_id":  o["local_order_id"],
                    "broker_order_id": broker_id,
                    "prev_status":     prev_status,
                    "new_status":      new_status,
                    "filled_qty":      int(new_filled_qty or 0),
                })

            # ENTRY FILLED -> create position if not already created
            if (new_status == "FILLED"
                    and o.get("kind") == "ENTRY"
                    and not o.get("position_id")):
                fill_price = _extract_fill_price(remote)
                if fill_price and int(new_filled_qty or 0) > 0:
                    pos_id = _create_position_from_entry(o, fill_price, int(new_filled_qty))
                    if pos_id:  # empty string means expiry guard blocked it
                        audit(client_id, "INFO", "POSITION_CREATED_FROM_FILL", {
                            "local_order_id": o["local_order_id"],
                            "position_id":    pos_id,
                            "fill_price":     fill_price,
                            "filled_qty":     int(new_filled_qty),
                        })

            # EXIT FILLED -> close linked position
            if (new_status == "FILLED"
                    and o.get("kind") == "EXIT"
                    and o.get("position_id")):
                fill_price = _extract_fill_price(remote)
                if fill_price:
                    _close_position_from_exit(o, fill_price)
                    audit(client_id, "INFO", "POSITION_CLOSED_FROM_EXIT_FILL", {
                        "local_order_id": o["local_order_id"],
                        "position_id":    o.get("position_id"),
                        "fill_price":     fill_price,
                    })

            processed += 1

        except Exception as e:
            log.exception("Reconcile failed for broker_order_id=%s: %s", broker_id, e)
            audit(client_id, "ERROR", "RECONCILE_ERROR", {
                "broker_order_id": broker_id,
                "local_order_id":  o.get("local_order_id"),
                "error":           str(e),
            })

        finally:
            # FIX 5: pace broker API calls — avoids hammering Tradier under load
            time.sleep(BROKER_CALL_SLEEP)

    return processed

def run_reconciliation(client_id: str, broker=None, limit: int = 50) -> dict:
    """
    Compatibility wrapper called by admin_api.py.
    Resolves broker from the active ClientRunner if not provided.
    """
    if broker is None:
        try:
            from client_runner import _active_runners
            runner = _active_runners.get(client_id)
            if runner is None:
                # Try finding any runner for this client
                for email, r in _active_runners.items():
                    if email == client_id:
                        runner = r
                        break
            broker = getattr(getattr(runner, "core", None), "broker", None) if runner else None
        except Exception:
            broker = None

    if broker is None:
        return {"ok": False, "client_id": client_id, "error": "no_broker_available",
                "processed": 0}

    processed = reconcile_once(broker=broker, client_id=client_id, limit=limit)

    # PR #30 LIVE-SAFETY: heartbeat after every successful reconciler cycle.
    # A watchdog elsewhere reads check_staleness() and emits CRITICAL
    # decision_event RECONCILER_STALE when age >= RECONCILER_SLA_SECONDS.
    # Failed cycles (no_broker_available, exception above) intentionally
    # do NOT beat — staleness is the signal.
    try:
        from ap import reconciler_heartbeat as _hb
        _hb.record_heartbeat(client_id, status="ok")
    except Exception:
        pass

    return {"ok": True, "client_id": client_id, "processed": processed}

