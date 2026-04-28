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

Upgrades in this version:
- 1-1 pair manager: on ENTRY fill, cancel opposite side of inside-bar pair
- exit_price_sync: on EXIT fill, push real avg_fill to dashboard proof_trades
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from ap.trace import trace_gate
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
        run_with_retry(
            lambda: c.execute(
                "INSERT INTO audit_log (ts, level, event, payload, client_id) "
                "VALUES (%s,%s,%s,%s,%s)",
                (now_utc_iso(), level, event, json_dumps(payload), client_id),
            )
        )


# =============================================================================
# DB QUERY — pending orders
# =============================================================================


def get_pending_orders() -> list[dict]:
    with conn() as c:
        rows = run_with_retry(
            lambda: c.execute(
                """
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
                    created_ts,
                    plan_id,
                    signal_id,
                    tier,
                    score,
                    pattern,
                    stop_underlying,
                    target_underlying,
                    trigger_price,
                    timeframe
                FROM orders
                WHERE kind IN ('ENTRY','EXIT')
                  AND status IN (
                    'CREATED',
                    'SUBMITTED',
                    'ACKNOWLEDGED',
                    'PARTIAL_FILL',
                    'PENDING_TRIGGER',
                    'EXIT_SUBMITTED',
                    'EXIT_ACKNOWLEDGED',
                    'EXIT_PARTIAL_FILL'
                  )
                  AND broker_order_id IS NOT NULL
                  AND broker_order_id != 'N/A'
                ORDER BY created_ts ASC
                """
            ).fetchall()
        )
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
        raw = broker.get_order(broker_order_id)
        status = (raw.get("status") or "").upper()

        if kind == "EXIT":
            status_map = {
                "FILLED": "EXIT_FILLED",
                "OPEN": "EXIT_ACKNOWLEDGED",
                "PENDING": "EXIT_ACKNOWLEDGED",
                "PARTIALLY_FILLED": "EXIT_PARTIAL_FILL",
                "CANCELED": "CANCELED",
                "REJECTED": "REJECTED",
                "EXPIRED": "EXPIRED",
            }
        else:
            status_map = {
                "FILLED": "FILLED",
                "OPEN": "ACKNOWLEDGED",
                "PENDING": "ACKNOWLEDGED",
                "PARTIALLY_FILLED": "PARTIAL_FILL",
                "CANCELED": "CANCELED",
                "REJECTED": "REJECTED",
                "EXPIRED": "EXPIRED",
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
            "status": our,
            "filled_qty": filled_qty,
            "avg_fill": avg_fill,
            "reason": raw.get("reason") or status,
            "raw": raw,
        }

    except Exception as e:
        audit(
            order["client_id"],
            "ERROR",
            "FILL_CHECK_FAILED",
            {
                "error": str(e),
                "broker_order_id": broker_order_id,
                "local_order_id": order.get("local_order_id"),
            },
        )
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
    symbol = order["symbol"]

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
# PAIR MANAGER HELPER
# =============================================================================


def _cancel_pair_opposite(order: dict, osm) -> None:
    """
    On ENTRY fill: cancel the opposite side of a 1-1 (inside bar) pair.
    Prevents the bot from holding both a CALL and PUT on the same ticker.
    Non-blocking — failure here never stops fill processing.
    """
    if not osm:
        return
    try:
        from ap.signal_pair_manager import get_pair_manager

        _pm = get_pair_manager()
        _ticker = (order.get("symbol") or "").upper()
        _side = (order.get("direction") or "CALL").upper()
        _local_id = order.get("local_order_id", "")

        cancel_id = _pm.on_fill(
            ticker=_ticker,
            side=_side,
            local_order_id=_local_id,
        )

        if cancel_id:
            log.warning(
                "[%s] 1-1 PAIR FILL — canceling opposite side order=%s",
                _ticker,
                cancel_id,
            )
            try:
                osm.transition(
                    cancel_id,
                    "CANCELED",
                    last_error="pair_fill_cancel",
                )
                log.info(
                    "[%s] Opposite side CANCELED | order=%s",
                    _ticker,
                    cancel_id,
                )
            except Exception as _te:
                log.error(
                    "[%s] Failed to cancel pair opposite %s: %s",
                    _ticker,
                    cancel_id,
                    _te,
                )
    except ImportError:
        # pair manager not yet deployed — skip silently
        pass
    except Exception as _e:
        log.debug("Pair manager cancel failed (non-critical): %s", _e)


# =============================================================================
# CORE — process one pending order via OSM
# =============================================================================


def process_pending_order(
    broker: BrokerAdapter,
    order: dict,
    osm=None,
    pm=None,
    exit_engine=None,
):
    """
    Poll broker and route ALL lifecycle transitions through APOrderStateMachine.

    osm — APOrderStateMachine instance (required for full routing).
    If None, falls back to legacy direct-DB path for backward compat
    during the transition period. Pass osm whenever possible.
    """
    client_id = order["client_id"]
    local_id = order["local_order_id"]
    broker_id = order.get("broker_order_id")
    kind = (order.get("kind") or "ENTRY").upper()

    result = check_order_with_broker(broker, order)

    # ── FILLED / EXIT_FILLED ────────────────────────────────────────────────
    if result["status"] in ("FILLED", "EXIT_FILLED"):
        mapped = result["status"]

        if osm:
            ok = False
            try:
                ok = osm.transition(
                    local_id,
                    mapped,
                    filled_qty=result["filled_qty"],
                    fill_price=result["avg_fill"],
                )
            except Exception as e:
                log.error(
                    "[%s] OSM transition %s failed for %s: %s",
                    client_id,
                    mapped,
                    local_id,
                    e,
                )

            # ── ENTRY FILL: open position + cancel pair + seed exit engine ─
            if ok and kind == "ENTRY" and pm:
                _pos_id = None
                try:
                    plan_id = order.get("plan_id") or order.get("signal_id") or local_id
                    signal_id = order.get("signal_id") or local_id
                    _ticker = (order.get("symbol") or "").upper()
                    _qty = int(
                        result.get("filled_qty")
                        or result.get("qty")
                        or order.get("filled_qty")
                        or order.get("contracts")
                        or 0
                    )
                    _price = float(
                        result.get("avg_fill")
                        or result.get("avg_fill_price")
                        or order.get("fill_price")
                        or 0
                    )

                    # Event for reporting/trace
                    trace_gate(
                        str(signal_id),
                        _ticker,
                        "ORDER_FILLED",
                        "PASS",
                        reason="entry_filled",
                        trigger_price=_price,
                        contracts=_qty,
                        side=(order.get("direction") or "CALL").upper(),
                        kind="ENTRY",
                        tier=str(order.get("tier") or "B"),
                        plan_id=str(plan_id or ""),
                    )

                    # 1-1 PAIR CANCEL — immediate on fill
                    _cancel_pair_opposite(order, osm)

                    # Standing broker stop (best-effort)
                    try:
                        _stop_pct = 0.30
                        _stop_px = round(_price * (1 - _stop_pct), 2)
                        _stop_resp = (
                            osm._broker.session.post(
                                f"{osm._broker._base_url}/v1/accounts/{osm._broker._account_id}/orders",
                                data={
                                    "class": "option",
                                    "option_symbol": order.get("contract", ""),
                                    "side": "sell_to_close",
                                    "quantity": _qty,
                                    "type": "stop",
                                    "stop": _stop_px,
                                    "duration": "gtc",
                                },
                                headers={"Accept": "application/json"},
                                timeout=10,
                            )
                            if hasattr(osm, "_broker")
                            else None
                        )

                        import time as _t

                        _t.sleep(2)

                        if _stop_resp and _stop_resp.status_code < 300:
                            _stop_data = _stop_resp.json().get("order", {}) or {}
                            _stop_id = _stop_data.get("id", "?")
                            _stop_stat = _stop_data.get("status", "unknown")
                            log.info(
                                "[%s] Standing stop placed @ $%.2f | broker_stop=%s status=%s",
                                _ticker,
                                _stop_px,
                                _stop_id,
                                _stop_stat,
                            )
                            if _stop_stat not in (
                                "ok",
                                "open",
                                "pending",
                                "filled",
                                "accepted",
                                "queued",
                                "pending_review",
                                "partially_filled",
                            ):
                                log.warning(
                                    "[%s] Stop order status unexpected: %s",
                                    _ticker,
                                    _stop_stat,
                                )
                        else:
                            _err_body = (
                                _stop_resp.text[:200] if _stop_resp else "no_response"
                            )
                            log.warning(
                                "[%s] Standing stop FAILED — exit engine sole protection | %s",
                                _ticker,
                                _err_body,
                            )
                            audit(
                                client_id,
                                "WARNING",
                                "STOP_ORDER_FAILED",
                                {
                                    "local_order_id": local_id,
                                    "ticker": _ticker,
                                    "stop_px": _stop_px,
                                    "body": _err_body,
                                },
                            )
                    except Exception as _se:
                        log.warning(
                            "[%s] Standing stop placement error: %s", _ticker, _se
                        )

                    _pos_id = pm.open_position(
                        plan_id=plan_id,
                        signal_id=signal_id,
                        ticker=_ticker,
                        contract=order.get("contract")
                        or order.get("symbol")
                        or "",
                        side=(order.get("direction") or "CALL").upper(),
                        qty=result["filled_qty"]
                        or int(order.get("qty") or 0),
                        entry_price=result["avg_fill"],
                        tier=str(order.get("tier") or "B"),
                        score=float(order.get("score") or 0),
                        pattern=str(order.get("pattern") or ""),
                        stop_underlying=float(order.get("stop_underlying"))
                        if order.get("stop_underlying")
                        else None,
                        target_underlying=float(order.get("target_underlying"))
                        if order.get("target_underlying")
                        else None,
                    )
                except Exception as _pm_err:
                    log.error(
                        "[%s] pm.open_position failed for %s: %s",
                        client_id,
                        local_id,
                        _pm_err,
                    )

                # Wire into exit engine
                if _pos_id and exit_engine:
                    try:
                        from ap_exit_engine import ManagedPosition as _MP

                        _mp = _MP(
                            ticker=(order.get("symbol") or "").upper(),
                            option_symbol=order.get("contract")
                            or order.get("symbol")
                            or "",
                            side=(order.get("direction") or "CALL").upper(),
                            quantity=result["filled_qty"]
                            or int(order.get("qty") or 0),
                            entry_price=result["avg_fill"],
                            underlying_entry=float(
                                order.get("trigger_price")
                                or order.get("underlying_entry")
                                or 0
                            ),
                            underlying_target=float(
                                order.get("target_underlying")
                                or order.get("underlying_target")
                                or 0
                            ),
                            underlying_stop=float(
                                order.get("stop_underlying")
                                or order.get("underlying_stop")
                                or 0
                            ),
                        )
                        _mp.position_id = _pos_id
                        _mp.signal = {
                            "signal_id": signal_id,
                            "pattern": str(order.get("pattern") or ""),
                            "tier": str(order.get("tier") or "B"),
                            "score": float(order.get("score") or 0),
                            "timeframe": str(order.get("timeframe") or "1d"),
                            "side": (order.get("direction") or "CALL").upper(),
                        }
                        exit_engine.add_position(_mp)
                        log.info(
                            "[%s] Exit engine seeded for %s pos=%s",
                            client_id,
                            order.get("symbol", "?"),
                            _pos_id,
                        )
                    except Exception as _ee_err:
                        log.error(
                            "[%s] exit_engine.add_position failed for %s: %s",
                            client_id,
                            local_id,
                            _ee_err,
                        )

            # ── EXIT FILL: sync real price to dashboard ─────────────
            if ok and kind == "EXIT":
                try:
                    from ap.exit_price_sync import sync_exit_price_to_dashboard

                    _pos_id_exit = order.get("position_id")
                    _avg_fill_exit = result.get("avg_fill")
                    _entry_price = (
                        order.get("entry_price") or order.get("fill_price")
                    )
                    _ticker_exit = (order.get("symbol") or "").upper()

                    if _pos_id_exit and _avg_fill_exit is not None:
                        sync_exit_price_to_dashboard(
                            position_id=str(_pos_id_exit),
                            exit_avg_fill=float(_avg_fill_exit),
                            entry_price=float(_entry_price)
                            if _entry_price
                            else None,
                            ticker=_ticker_exit,
                        )
                except ImportError:
                    # exit_price_sync not yet deployed
                    pass
                except Exception as _eps:
                    log.debug(
                        "[%s] exit_price_sync failed (non-critical): %s",
                        client_id,
                        _eps,
                    )
        else:
            # Legacy fallback
            _legacy_update_order_status(
                local_id, "FILLED", filled_qty=result["filled_qty"]
            )
            if kind == "ENTRY":
                _legacy_create_position_from_fill(
                    order, result["avg_fill"], result["filled_qty"]
                )
            elif kind == "EXIT":
                _legacy_close_position_from_exit_fill(order, result["avg_fill"])

        if kind == "ENTRY":
            _release_entry_guards(order)

        audit(
            client_id,
            "INFO",
            "ORDER_FILLED",
            {
                "local_order_id": local_id,
                "broker_order_id": broker_id,
                "kind": kind,
                "osm_status": mapped,
                "filled_qty": result["filled_qty"],
                "avg_fill": result["avg_fill"],
            },
        )
        return

    # ── PARTIAL FILL / EXIT_PARTIAL_FILL ─────────────────────────────────
    if result["status"] in ("PARTIAL_FILL", "EXIT_PARTIAL_FILL"):
        mapped = result["status"]
        if osm:
            try:
                osm.transition(local_id, mapped, filled_qty=result["filled_qty"])
            except Exception as e:
                log.error(
                    "[%s] OSM transition %s failed for %s: %s",
                    client_id,
                    mapped,
                    local_id,
                    e,
                )
        else:
            _legacy_update_order_status(
                local_id, "PARTIAL_FILL", filled_qty=result["filled_qty"]
            )

        audit(
            client_id,
            "INFO",
            "ORDER_PARTIAL",
            {
                "local_order_id": local_id,
                "broker_order_id": broker_id,
                "kind": kind,
                "osm_status": mapped,
                "filled_qty": result["filled_qty"],
                "total_qty": int(order["qty"]),
            },
        )
        return

    # ── TERMINAL FAILURES ────────────────────────────────────────────────
    if result["status"] in ("REJECTED", "CANCELED", "EXPIRED"):
        terminal = result["status"]
        if osm:
            try:
                osm.transition(
                    local_id,
                    terminal,
                    last_error=result.get("reason"),
                )
            except Exception as e:
                log.error(
                    "[%s] OSM transition %s failed for %s: %s",
                    client_id,
                    terminal,
                    local_id,
                    e,
                )
        else:
            _legacy_update_order_status(
                local_id, terminal, error=result.get("reason")
            )

        # EXIT failure: revert position status to OPEN
        if kind == "EXIT" and order.get("position_id"):
            with conn() as c:
                run_with_retry(
                    lambda: c.execute(
                        "UPDATE positions "
                        "SET status='OPEN', exit_reason=NULL "
                        "WHERE id=%s AND client_id=%s",
                        (order["position_id"], client_id),
                    )
                )

        if kind == "ENTRY":
            _release_entry_guards(order)

        audit(
            client_id,
            "WARNING",
            f"ORDER_{terminal}",
            {
                "local_order_id": local_id,
                "broker_order_id": broker_id,
                "kind": kind,
                "reason": result.get("reason"),
            },
        )
        return

    # ── STILL PENDING / ACKNOWLEDGED ─────────────────────────────────────
    if result["status"] in ("ACKNOWLEDGED", "EXIT_ACKNOWLEDGED", "UNKNOWN"):
        try:
            created_raw = order.get("created_ts")
            if isinstance(created_raw, str):
                created = datetime.fromisoformat(created_raw)
            else:
                created = created_raw  # assume datetime
            age = (datetime.now(timezone.utc) - created).total_seconds()
            if age > 300:
                audit(
                    client_id,
                    "WARNING",
                    "ORDER_PENDING_LONG",
                    {
                        "local_order_id": local_id,
                        "broker_order_id": broker_id,
                        "kind": kind,
                        "age_seconds": age,
                    },
                )
        except Exception:
            pass
        return

    # ── BROKER ERROR ─────────────────────────────────────────────────-----
    if result["status"] == "ERROR":
        audit(
            client_id,
            "ERROR",
            "ORDER_CHECK_ERROR",
            {
                "local_order_id": local_id,
                "broker_order_id": broker_id,
                "reason": result.get("reason"),
            },
        )


# =============================================================================
# MAIN LOOP
# =============================================================================


def fill_monitor_loop(
    broker: BrokerAdapter,
    poll_seconds: float = 10.0,
    osm=None,
    pm=None,
    exit_engine=None,
):
    """
    Fill monitor must NEVER pause on kill switch — it's the reconciliation layer.

    osm — APOrderStateMachine instance. Pass it from client_runner so all
    transitions route through the canonical state machine.
    """
    log.info(
        "Fill monitor started (osm=%s pm=%s ee=%s)",
        "wired" if osm else "legacy-fallback",
        "wired" if pm else "none",
        "wired" if exit_engine else "none",
    )

    while True:
        try:
            pending = get_pending_orders()
            for order in pending:
                try:
                    process_pending_order(
                        broker,
                        order,
                        osm=osm,
                        pm=pm,
                        exit_engine=exit_engine,
                    )
                except Exception as e:
                    log.exception(
                        "Failed to process order %s: %s",
                        order.get("local_order_id"),
                        e,
                    )
            time.sleep(poll_seconds)
        except Exception as e:
            log.exception("Fill monitor loop error: %s", e)
            time.sleep(poll_seconds * 2)


# =============================================================================
# LEGACY FALLBACK HELPERS (deprecated — used only when osm=None)
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
        params = [status, now_utc_iso()]
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

    pos_id = str(uuid.uuid4())
    client_id = order["client_id"]
    direction = (order.get("direction") or "CALL").upper()

    with conn() as c:
        run_with_retry(
            lambda: c.execute(
                """
                INSERT INTO positions (
                    id, client_id, underlying, contract, direction, qty, avg_fill,
                    entry_ts, tp_pct, sl_pct, status
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    pos_id,
                    client_id,
                    order["symbol"],
                    order["contract"],
                    direction,
                    int(filled_qty),
                    float(avg_fill_price),
                    now_utc_iso(),
                    float(cfg.TAKE_PROFIT_PCT),
                    float(cfg.STOP_LOSS_PCT),
                    "OPEN",
                ),
            )
        )

    with conn() as c:
        run_with_retry(
            lambda: c.execute(
                "UPDATE orders SET position_id=%s WHERE local_order_id=%s",
                (pos_id, order["local_order_id"]),
            )
        )

    audit(
        client_id,
        "INFO",
        "POSITION_CREATED_FROM_FILL_LEGACY",
        {
            "position_id": pos_id,
            "local_order_id": order["local_order_id"],
            "contract": order["contract"],
            "qty": int(filled_qty),
            "avg_fill": float(avg_fill_price),
        },
    )
    return pos_id


def _legacy_close_position_from_exit_fill(order: dict, avg_fill_price: float):
    """DEPRECATED: Direct DB write. OSM.transition(EXIT_FILLED) via exit engine handles this."""
    client_id = order["client_id"]
    position_id = order.get("position_id")

    if not position_id:
        log.error("Exit order has no position_id: %s", order.get("local_order_id"))
        return

    with conn() as c:
        pos_row = run_with_retry(
            lambda: c.execute(
                "SELECT * FROM positions WHERE id=%s AND client_id=%s",
                (position_id, client_id),
            ).fetchone()
        )

    if not pos_row:
        log.error("Position not found: %s", position_id)
        return

    pos = dict(pos_row)
    entry_price = float(pos["avg_fill"])
    qty = int(pos["qty"])
    exit_px = float(avg_fill_price)
    realized_pnl = (exit_px - entry_price) * qty * OPT_MULTIPLIER
    realized_pnl_pct = (
        round(((exit_px - entry_price) / entry_price) * 100, 2)
        if entry_price > 0
        else 0.0
    )

    with conn() as c:
        run_with_retry(
            lambda: c.execute(
                """
                UPDATE positions
                SET status='CLOSED', exit_ts=%s, exit_price=%s,
                    realized_pnl=%s, realized_pnl_pct=%s,
                    close_source=%s, close_confidence=%s
                WHERE id=%s AND client_id=%s
                """,
                (
                    now_utc_iso(),
                    exit_px,
                    float(realized_pnl),
                    realized_pnl_pct,
                    "FILL_MONITOR",
                    "HIGH",
                    position_id,
                    client_id,
                ),
            )
        )

    with conn() as c:
        run_with_retry(
            lambda: c.execute(
                "UPDATE client_state "
                "SET realized_pnl_today = COALESCE(realized_pnl_today, 0.0) + %s "
                "WHERE client_id=%s",
                (float(realized_pnl), client_id),
            )
        )

    audit(
        client_id,
        "INFO",
        "FINALIZED_TRADE_FROM_FILL_MONITOR",
        {
            "position_id": position_id,
            "contract": pos["contract"],
            "entry_price": entry_price,
            "exit_price": exit_px,
            "qty": qty,
            "realized_pnl": float(realized_pnl),
            "realized_pnl_pct": realized_pnl_pct,
            "close_source": "FILL_MONITOR",
            "close_confidence": "HIGH",
        },
    )

    # Update proof_trades with actual broker fill price (non-critical)
    try:
        _epx = exit_px
        _pct = realized_pnl_pct
        _win = realized_pnl_pct > 0
        _cid = client_id

        def _update_proof():
            with conn() as c2:
                c2.execute(
                    """
                    UPDATE proof_trades
                    SET exit_option_price = %s,
                        option_pnl_pct    = %s,
                        win               = %s
                    WHERE client_email = %s
                      AND closed_at >= NOW() - INTERVAL '4 hours'
                      AND ABS(COALESCE(exit_option_price,0) - %s) > 0.05
                    """,
                    (_epx, _pct, _win, _cid, _epx),
                )
                return c2.rowcount

        updated = run_with_retry(_update_proof) or 0
        if updated:
            log.info(
                "[%s] proof_trades corrected with actual fill $%.4f pnl=%.1f%%",
                client_id,
                exit_px,
                realized_pnl_pct,
            )
    except Exception as _pe:
        log.debug(
            "[%s] proof_trades correction (non-critical): %s",
            client_id,
            _pe,
        )
