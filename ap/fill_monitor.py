# ap/fill_monitor.py — Fill Monitor (OSM-routed)
"""
Fill Monitor — polls broker to confirm order fills and routes all lifecycle
transitions through APOrderStateMachine.

DESIGN:
  - This module is a PURE BROKER POLLER. It has NO lifecycle authority.
  - It queries broker status, then calls osm.transition() with the mapped status.
  - APOrderStateMachine owns all DB writes, position creation, and exit-engine hooks.
  - No direct writes to orders/positions/client_state from here.

CRITICAL RULE:
  - Fill monitor MUST NEVER pause on kill switch. It reconciles reality.
  - Equity/symbol-lock release is still done here for ENTRY orders (OSM does
    not own the in-memory equity/lock state — that lives in ap.state).
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from ap.db import conn, run_with_retry
from ap.utils import now_utc_iso, json_dumps
from ap.logger import get_logger
from ap.config import Config
from ap.state import release_equity, release_symbol_lock
from ap.broker import BrokerAdapter

log = get_logger("ap.fill_monitor")
cfg = Config()

OPT_MULTIPLIER = 100


# =============================================================================
# AUDIT LOG
# =============================================================================

def audit(client_id: str, level: str, event: str, payload: dict):
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "INSERT INTO audit_log (ts, level, event, payload, client_id) "
            "VALUES (%s,%s,%s,%s,%s)",
            (now_utc_iso(), level, event, json_dumps(payload), client_id),
        ))


# =============================================================================
# DB QUERY — pending orders
# =============================================================================

def get_pending_orders():
    with conn() as c:
        rows = run_with_retry(lambda: c.execute("""
            SELECT
                client_id,
                local_order_id,
                broker_order_id,
                position_id,
                kind,
                symbol,
                contract,
                direction,
                qty,
                limit_price,
                reserved_cost,
                status,
                created_ts
            FROM orders
            WHERE kind IN ('ENTRY','EXIT')
              AND status IN (
                  'CREATED',
                  'SUBMITTED',
                  'ACKNOWLEDGED',
                  'PARTIAL_FILL',
                  'EXIT_SUBMITTED',
                  'EXIT_ACKNOWLEDGED',
                  'EXIT_PARTIAL_FILL'
              )
              AND broker_order_id IS NOT NULL
              AND broker_order_id != 'N/A'
            ORDER BY created_ts ASC
        """).fetchall())
        return [dict(r) for r in rows]


# =============================================================================
# BROKER CHECK
# =============================================================================

def check_order_with_broker(broker: BrokerAdapter, order: dict) -> dict:
    """
    Query broker for actual order status.
    Returns: {"status": str, "filled_qty": int, "avg_fill": float,
               "reason": str, "raw": dict}
    Maps broker statuses to canonical OSM statuses, with EXIT-kind awareness.
    """
    broker_order_id = order.get("broker_order_id")
    if not broker_order_id or broker_order_id == "N/A":
        return {"status": "UNKNOWN", "reason": "NO_BROKER_ID"}

    kind = (order.get("kind") or "ENTRY").upper()

    try:
        raw    = broker.get_order(broker_order_id)
        status = (raw.get("status") or "").upper()

        if kind == "EXIT":
            status_map = {
                "FILLED":           "EXIT_FILLED",
                "OPEN":             "EXIT_ACKNOWLEDGED",
                "PENDING":          "EXIT_ACKNOWLEDGED",
                "PARTIALLY_FILLED": "EXIT_PARTIAL_FILL",
                "CANCELED":         "CANCELED",
                "REJECTED":         "REJECTED",
                "EXPIRED":          "EXPIRED",
            }
        else:
            status_map = {
                "FILLED":           "FILLED",
                "OPEN":             "ACKNOWLEDGED",
                "PENDING":          "ACKNOWLEDGED",
                "PARTIALLY_FILLED": "PARTIAL_FILL",
                "CANCELED":         "CANCELED",
                "REJECTED":         "REJECTED",
                "EXPIRED":          "EXPIRED",
            }
        our = status_map.get(status, "UNKNOWN")

        filled_qty = int(
            raw.get("exec_quantity")
            or raw.get("filled_quantity")
            or raw.get("quantity")
            or 0
        )
        avg_fill = float(raw.get("avg_fill_price") or raw.get("price") or 0.0)

        return {
            "status":     our,
            "filled_qty": filled_qty,
            "avg_fill":   avg_fill,
            "reason":     raw.get("reason") or status,
            "raw":        raw,
        }

    except Exception as e:
        audit(order["client_id"], "ERROR", "FILL_CHECK_FAILED", {
            "error":           str(e),
            "broker_order_id": broker_order_id,
            "local_order_id":  order.get("local_order_id"),
        })
        return {"status": "ERROR", "reason": str(e)}


# =============================================================================
# EQUITY / LOCK RELEASE (entry orders only — in-memory, not in OSM)
# =============================================================================

def _release_entry_guards(order: dict):
    """
    Release reserved equity and symbol lock for ENTRY orders.
    Uses orders.reserved_cost if present, else limit_price * qty * 100.
    """
    client_id = order["client_id"]
    symbol    = order["symbol"]

    cost = None
    if order.get("reserved_cost") is not None:
        try:
            cost = float(order["reserved_cost"])
        except Exception:
            cost = None

    if cost is None:
        cost = (
            float(order.get("limit_price") or 0.0)
            * int(order.get("qty") or 0)
            * OPT_MULTIPLIER
        )

    if cost and cost > 0:
        release_equity(client_id, cost)

    release_symbol_lock(client_id, symbol)


# =============================================================================
# CORE — process one pending order via OSM
# =============================================================================

def process_pending_order(broker: BrokerAdapter, order: dict, osm=None, pm=None):
    """
    Poll broker and route ALL lifecycle transitions through APOrderStateMachine.

    osm — APOrderStateMachine instance (required for full routing).
          If None, falls back to legacy direct-DB path for backward compat
          during the transition period. Pass osm whenever possible.
    """
    client_id = order["client_id"]
    local_id  = order["local_order_id"]
    broker_id = order.get("broker_order_id")
    kind      = (order.get("kind") or "ENTRY").upper()

    result = check_order_with_broker(broker, order)

    # ── FILLED / EXIT_FILLED ──────────────────────────────────────────────────
    if result["status"] in ("FILLED", "EXIT_FILLED"):
        mapped = result["status"]  # already kind-aware from check_order_with_broker

        if osm:
            try:
                osm.transition(
                    local_id, mapped,
                    filled_qty=result["filled_qty"],
                    fill_price=result["avg_fill"],
                )
            except Exception as e:
                log.error("[%s] OSM transition %s failed for %s: %s",
                          client_id, mapped, local_id, e)
            # Open position in DB on confirmed ENTRY fill — idempotent via plan_id/signal_id guards
            if kind == "ENTRY" and pm:
                try:
                    plan_id   = order.get("plan_id")   or order.get("signal_id") or local_id
                    signal_id = order.get("signal_id") or local_id
                    pm.open_position(
                        plan_id    = plan_id,
                        signal_id  = signal_id,
                        ticker     = (order.get("symbol") or "").upper(),
                        contract   = order.get("contract") or order.get("symbol") or "",
                        side       = (order.get("direction") or "CALL").upper(),
                        qty        = result["filled_qty"] or int(order.get("qty") or 0),
                        entry_price= result["avg_fill"],
                    )
                except Exception as _pm_err:
                    log.error("[%s] pm.open_position failed for %s: %s",
                              client_id, local_id, _pm_err)
        else:
            # Legacy fallback — only used if caller didn't pass osm
            _legacy_update_order_status(local_id, "FILLED", filled_qty=result["filled_qty"])
            if kind == "ENTRY":
                _legacy_create_position_from_fill(order, result["avg_fill"], result["filled_qty"])
            elif kind == "EXIT":
                _legacy_close_position_from_exit_fill(order, result["avg_fill"])

        if kind == "ENTRY":
            _release_entry_guards(order)

        audit(client_id, "INFO", "ORDER_FILLED", {
            "local_order_id":  local_id,
            "broker_order_id": broker_id,
            "kind":            kind,
            "osm_status":      mapped,
            "filled_qty":      result["filled_qty"],
            "avg_fill":        result["avg_fill"],
        })
        return

    # ── PARTIAL FILL / EXIT_PARTIAL_FILL ─────────────────────────────────────
    if result["status"] in ("PARTIAL_FILL", "EXIT_PARTIAL_FILL"):
        mapped = result["status"]

        if osm:
            try:
                osm.transition(
                    local_id, mapped,
                    filled_qty=result["filled_qty"],
                )
            except Exception as e:
                log.error("[%s] OSM transition %s failed for %s: %s",
                          client_id, mapped, local_id, e)
        else:
            _legacy_update_order_status(local_id, "PARTIAL_FILL",
                                        filled_qty=result["filled_qty"])

        audit(client_id, "INFO", "ORDER_PARTIAL", {
            "local_order_id":  local_id,
            "broker_order_id": broker_id,
            "kind":            kind,
            "osm_status":      mapped,
            "filled_qty":      result["filled_qty"],
            "total_qty":       int(order["qty"]),
        })
        return

    # ── TERMINAL FAILURES ─────────────────────────────────────────────────────
    if result["status"] in ("REJECTED", "CANCELED", "EXPIRED"):
        terminal = result["status"]

        if osm:
            try:
                osm.transition(
                    local_id, terminal,
                    last_error=result.get("reason"),
                )
            except Exception as e:
                log.error("[%s] OSM transition %s failed for %s: %s",
                          client_id, terminal, local_id, e)
        else:
            _legacy_update_order_status(local_id, terminal,
                                        error=result.get("reason"))
            if kind == "EXIT" and order.get("position_id"):
                with conn() as c:
                    run_with_retry(lambda: c.execute(
                        "UPDATE positions SET status='OPEN', exit_reason=NULL "
                        "WHERE id=%s AND client_id=%s",
                        (order["position_id"], client_id),
                    ))

        if kind == "ENTRY":
            _release_entry_guards(order)

        audit(client_id, "WARNING", f"ORDER_{terminal}", {
            "local_order_id":  local_id,
            "broker_order_id": broker_id,
            "kind":            kind,
            "reason":          result.get("reason"),
        })
        return

    # ── STILL PENDING / ACKNOWLEDGED ─────────────────────────────────────────
    if result["status"] in ("ACKNOWLEDGED", "EXIT_ACKNOWLEDGED", "UNKNOWN"):
        try:
            created = datetime.fromisoformat(order["created_ts"])
            age = (datetime.now(timezone.utc) - created).total_seconds()
            if age > 300:
                audit(client_id, "WARNING", "ORDER_PENDING_LONG", {
                    "local_order_id":  local_id,
                    "broker_order_id": broker_id,
                    "kind":            kind,
                    "age_seconds":     age,
                })
        except Exception:
            pass
        return

    # ── BROKER ERROR ──────────────────────────────────────────────────────────
    if result["status"] == "ERROR":
        audit(client_id, "ERROR", "ORDER_CHECK_ERROR", {
            "local_order_id":  local_id,
            "broker_order_id": broker_id,
            "reason":          result.get("reason"),
        })


# =============================================================================
# MAIN LOOP
# =============================================================================

def fill_monitor_loop(broker: BrokerAdapter, poll_seconds: float = 10.0, osm=None, pm=None):
    """
    Fill monitor must NEVER pause on kill switch — it's the reconciliation layer.

    osm — APOrderStateMachine instance. Pass it from client_runner so all
          transitions route through the canonical state machine.
    """
    log.info("Fill monitor started (osm=%s pm=%s)", "wired" if osm else "legacy-fallback", "wired" if pm else "none")

    while True:
        try:
            pending = get_pending_orders()
            for order in pending:
                try:
                    process_pending_order(broker, order, osm=osm, pm=pm)
                except Exception as e:
                    log.exception(
                        "Failed to process order %s: %s",
                        order.get("local_order_id"), e,
                    )

            time.sleep(poll_seconds)

        except Exception as e:
            log.exception("Fill monitor loop error: %s", e)
            time.sleep(poll_seconds * 2)


# =============================================================================
# LEGACY FALLBACK HELPERS (deprecated — used only when osm=None)
# These will be removed once all callers pass osm.
# =============================================================================

def _legacy_update_order_status(
    local_order_id: str,
    status: str,
    filled_qty: int | None = None,
    error: str | None = None,
):
    """DEPRECATED: Direct DB write. Use osm.transition() instead."""
    with conn() as c:
        updates = ["status=%s", "updated_ts=%s"]
        params  = [status, now_utc_iso()]
        if filled_qty is not None:
            updates.append("filled_qty=%s")
            params.append(int(filled_qty))
        if error is not None:
            updates.append("last_error=%s")
            params.append(error)
        params.append(local_order_id)
        sql = f"UPDATE orders SET {', '.join(updates)} WHERE local_order_id=%s"
        run_with_retry(lambda: c.execute(sql, params))


def _legacy_create_position_from_fill(
    order: dict, avg_fill_price: float, filled_qty: int
):
    """DEPRECATED: Direct DB write. OSM.transition(FILLED) handles this via position_manager."""
    import uuid
    pos_id    = str(uuid.uuid4())
    client_id = order["client_id"]
    direction = (order.get("direction") or "CALL").upper()

    with conn() as c:
        run_with_retry(lambda: c.execute(
            """
            INSERT INTO positions (
                id, client_id, underlying, contract, direction, qty, avg_fill,
                entry_ts, tp_pct, sl_pct, status
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                pos_id, client_id,
                order["symbol"], order["contract"], direction,
                int(filled_qty), float(avg_fill_price), now_utc_iso(),
                float(cfg.TAKE_PROFIT_PCT), float(cfg.STOP_LOSS_PCT),
                "OPEN",
            ),
        ))
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "UPDATE orders SET position_id=%s WHERE local_order_id=%s",
            (pos_id, order["local_order_id"]),
        ))

    audit(client_id, "INFO", "POSITION_CREATED_FROM_FILL_LEGACY", {
        "position_id":    pos_id,
        "local_order_id": order["local_order_id"],
        "contract":       order["contract"],
        "qty":            int(filled_qty),
        "avg_fill":       float(avg_fill_price),
    })
    return pos_id


def _legacy_close_position_from_exit_fill(order: dict, avg_fill_price: float):
    """DEPRECATED: Direct DB write. OSM.transition(EXIT_FILLED) via exit engine handles this."""
    client_id   = order["client_id"]
    position_id = order.get("position_id")
    if not position_id:
        log.error("Exit order has no position_id: %s", order.get("local_order_id"))
        return

    with conn() as c:
        pos_row = run_with_retry(lambda: c.execute(
            "SELECT * FROM positions WHERE id=%s AND client_id=%s",
            (position_id, client_id),
        ).fetchone())

    if not pos_row:
        log.error("Position not found: %s", position_id)
        return

    pos          = dict(pos_row)
    entry_price  = float(pos["avg_fill"])
    qty          = int(pos["qty"])
    realized_pnl = (float(avg_fill_price) - entry_price) * qty * OPT_MULTIPLIER

    with conn() as c:
        run_with_retry(lambda: c.execute(
            "UPDATE positions SET status='CLOSED', exit_ts=%s, realized_pnl=%s "
            "WHERE id=%s AND client_id=%s",
            (now_utc_iso(), float(realized_pnl), position_id, client_id),
        ))
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "UPDATE client_state "
            "SET realized_pnl_today = COALESCE(realized_pnl_today, 0.0) + %s "
            "WHERE client_id=%s",
            (float(realized_pnl), client_id),
        ))

    audit(client_id, "INFO", "POSITION_CLOSED_FROM_EXIT_LEGACY", {
        "position_id": position_id,
        "contract":    pos["contract"],
        "entry_price": entry_price,
        "exit_price":  float(avg_fill_price),
        "qty":         qty,
        "realized_pnl": float(realized_pnl),
    })
