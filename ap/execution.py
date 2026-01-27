# ap/execution.py
# ✅ Client-scoped execution
# ✅ Growth/target auto-stop check FIRST
# ✅ Hourly throttle
# ✅ Risk gates
# ✅ Audit trail
# ✅ Position sizing + contract resolution

import uuid

from ap.config import Config
from ap.logger import get_logger
from ap.models import Signal, OrderPlan
from ap.db import conn, run_with_retry, get_client_state, update_client_state
from ap.utils import now_utc_iso, json_dumps
from ap.risk import run_gates, effective_limits
from ap.broker import BrokerAdapter

from ap.contract_selection import pick_expiration, resolve_contract_symbol

cfg = Config()
log = get_logger("ap.execution")

DEFAULT_CLIENT_ID = "default"

MAX_TRADES_PER_HOUR_DEFAULT = int(cfg.__dict__.get("MAX_TRADES_PER_HOUR", 12)) if hasattr(cfg, "__dict__") else 12


def count_open_positions(client_id: str) -> int:
    with conn() as c:
        row = run_with_retry(lambda: c.execute(
            "SELECT COUNT(*) n FROM positions WHERE client_id=? AND status IN ('OPEN','CLOSING')",
            (client_id,)
        ).fetchone())
        return int(row["n"]) if row else 0


def count_trades_last_hour(client_id: str) -> int:
    with conn() as c:
        row = run_with_retry(lambda: c.execute(
            """
            SELECT COUNT(*) n
            FROM positions
            WHERE client_id=?
              AND entry_ts >= datetime('now', '-1 hour')
            """,
            (client_id,)
        ).fetchone())
        return int(row["n"]) if row else 0


def audit(client_id: str, level: str, event: str, payload: dict):
    with conn() as c:
        # Prefer client_id column (migration)
        try:
            run_with_retry(lambda: c.execute(
                "INSERT INTO audit_log (client_id, ts, level, event, payload) VALUES (?,?,?,?,?)",
                (client_id, now_utc_iso(), level, event, json_dumps(payload)),
            ))
        except Exception:
            # Backward compatibility if audit_log has no client_id column
            run_with_retry(lambda: c.execute(
                "INSERT INTO audit_log (ts, level, event, payload) VALUES (?,?,?,?)",
                (now_utc_iso(), level, event, json_dumps(payload)),
            ))


def calc_position_size(account_equity: float, client_state: dict) -> float:
    """
    Use effective limits (tier-based) for position sizing.
    Defaults to 15% of equity if not configured elsewhere.
    """
    limits = effective_limits(client_state)
    pct = float(limits.get("position_pct", 0.15))
    size = max(0.0, float(account_equity) * pct)
    return float(size)


def get_contract_price(broker: BrokerAdapter, contract: str, side: str = "BUY") -> float:
    """
    Uses your pricing layer. BrokerAdapter should support contract pricing.
    """
    from ap.contract_pricing import get_contract_price as _get
    px = float(_get(broker, contract, side=side))
    return px


def validate_contract_price(contract_price: float, min_price: float = 0.05, max_price: float = 10_000.0) -> bool:
    return (contract_price is not None) and (min_price <= float(contract_price) <= max_price)


def calculate_qty(position_size_dollars: float, contract_price: float) -> int:
    # contract_price is per contract, multiplier handled by broker/pricing layer;
    # your repo appears to treat contract_price as premium * 100 already in some places.
    # If your contract_price is premium dollars (e.g. 1.25), change divisor accordingly.
    if contract_price <= 0:
        return 0
    qty = int(position_size_dollars // contract_price)
    return max(0, qty)


def create_order_plan(signal: Signal, state: dict, broker: BrokerAdapter, account_equity: float) -> OrderPlan:
    """
    Resolve option contract and compute qty.
    """
    trig = signal.trigger or {}
    strike = trig.get("strike")
    expiry_hint = trig.get("expiry_hint", "Weekly")

    if strike is None:
        raise ValueError("Missing trigger.strike")

    # Pick expiration + resolve contract symbol
    expiration = pick_expiration(expiry_hint=expiry_hint)
    contract = resolve_contract_symbol(
        underlying=signal.symbol,
        expiration=expiration,
        strike=float(strike),
        right=signal.direction  # CALL/PUT
    )

    # Position sizing
    position_size_dollars = calc_position_size(account_equity, state)

    # Price the contract
    contract_price = get_contract_price(broker, contract, side="BUY")

    if not validate_contract_price(contract_price, min_price=0.01, max_price=10_000.0):
        raise RuntimeError(f"Bad contract price: {contract_price}")

    qty = calculate_qty(position_size_dollars, contract_price)
    if qty <= 0:
        raise RuntimeError(f"Position size too small for contract price (${contract_price})")

    # Defaults (override if your Signal trigger contains tp/sl)
    tp_pct = float(state.get("tp_pct_default", 0.23))
    sl_pct = float(state.get("sl_pct_default", 0.50))

    return OrderPlan(
        client_id=signal.client_id if hasattr(signal, "client_id") else None,
        underlying=signal.symbol,
        contract=contract,
        direction=signal.direction,
        qty=qty,
        limit_price=None,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        meta={
            "signal_id": signal.signal_id,
            "pattern_id": getattr(signal, "pattern_id", None),
            "confidence_tag": getattr(signal, "confidence_tag", None),
            "expiry": expiration,
            "strike": float(strike),
            "expiry_hint": expiry_hint,
        },
    )


def persist_order(client_id: str, local_order_id: str, position_id: str, plan: OrderPlan):
    with conn() as c:
        ts = now_utc_iso()
        run_with_retry(lambda: c.execute(
            """
            INSERT INTO orders (
                client_id,
                local_order_id, broker_order_id, position_id, kind, status,
                symbol, contract, qty, limit_price, filled_qty, retries, last_error,
                created_ts, updated_ts
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                client_id,
                local_order_id,
                None,
                position_id,
                "ENTRY",
                "NEW",
                plan.underlying,
                plan.contract,
                int(plan.qty),
                plan.limit_price,
                0,
                0,
                None,
                ts,
                ts,
            ),
        ))


def update_order_row(local_order_id: str, status: str, broker_order_id: str | None = None, last_error: str | None = None):
    with conn() as c:
        run_with_retry(lambda: c.execute(
            """
            UPDATE orders
            SET status=?, broker_order_id=COALESCE(?, broker_order_id), last_error=?, updated_ts=?
            WHERE local_order_id=?
            """,
            (status, broker_order_id, last_error, now_utc_iso(), local_order_id),
        ))


def open_position_from_fill(client_id: str, position_id: str, plan: OrderPlan, avg_fill: float):
    with conn() as c:
        run_with_retry(lambda: c.execute(
            """
            INSERT INTO positions (
                id, client_id, underlying, contract, direction,
                qty, avg_fill, tp_pct, sl_pct, status, entry_ts
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                position_id,
                client_id,
                plan.underlying,
                plan.contract,
                plan.direction,
                int(plan.qty),
                float(avg_fill),
                float(plan.tp_pct),
                float(plan.sl_pct),
                "OPEN",
                now_utc_iso(),
            ),
        ))


def process_signal(signal: Signal, broker: BrokerAdapter, client_id: str = DEFAULT_CLIENT_ID) -> dict:
    """
    Client-scoped execution.
    Uses client_state for mode/kill/equity counters.
    """

    # --- Growth / subscription stop (FIRST) ---
    try:
        from ap.account_growth import check_growth_status
        growth = check_growth_status(client_id)
        if growth and growth.get("should_stop"):
            audit(client_id, "INFO", "GROWTH_STOP", growth)
            update_client_state(client_id, {"kill_switch": 1, "mode": "READ_ONLY"})
            return {
                "ok": False,
                "reason": "PROFIT_TARGET_HIT",
                "message": growth.get("message"),
                "client_id": client_id,
            }
    except Exception as e:
        # Fail-safe: if growth module is broken, stop trading (don’t free-run)
        audit(client_id, "ERROR", "GROWTH_CHECK_ERROR", {"err": str(e)})
        update_client_state(client_id, {"kill_switch": 1, "mode": "READ_ONLY"})
        return {"ok": False, "reason": "GROWTH_CHECK_ERROR", "client_id": client_id}

    # Load per-client state
    state = get_client_state(client_id)

    if int(state.get("kill_switch", 0) or 0) == 1 or (state.get("mode") or "").upper() == "READ_ONLY":
        return {"ok": False, "reason": "CLIENT_READ_ONLY", "client_id": client_id}

    # Hourly throttle
    max_per_hour = int(state.get("max_trades_per_hour", MAX_TRADES_PER_HOUR_DEFAULT) or MAX_TRADES_PER_HOUR_DEFAULT)
    recent = count_trades_last_hour(client_id)
    if recent >= max_per_hour:
        audit(client_id, "INFO", "THROTTLE_HOURLY", {"recent_trades_1h": recent, "max_per_hour": max_per_hour})
        return {"ok": False, "reason": "THROTTLED", "details": {"recent_trades_1h": recent}, "client_id": client_id}

    # Refresh equity from broker
    try:
        current_equity = float(broker.get_account_equity())
    except Exception as e:
        audit(client_id, "ERROR", "EQUITY_FETCH_FAIL", {"err": str(e)})
        return {"ok": False, "reason": "EQUITY_FETCH_FAIL", "client_id": client_id}

    # Update equity
    update_client_state(client_id, {"current_equity": current_equity})

    risk_state = {
        "current_equity_last": current_equity,
        "trades_taken_today": int(state.get("trades_taken_today", 0) or 0),
        "daily_stop_hit": bool(int(state.get("daily_stop_hit", 0) or 0)),
        "kill_switch": bool(int(state.get("kill_switch", 0) or 0)),
        "mode": state.get("mode", "PAPER"),
        # include tier limits if your risk module uses them
        "max_trades_per_day": state.get("max_trades_per_day"),
        "max_concurrent_positions": state.get("max_concurrent_positions"),
    }

    open_positions = count_open_positions(client_id)
    gates = run_gates(risk_state, open_positions)

    audit(client_id, "INFO", "GATES_RESULT", {
        "signal_id": signal.signal_id,
        "ok": gates.ok,
        "reason": gates.reason,
        "details": gates.details,
        "open_positions": open_positions,
    })

    if not gates.ok:
        if gates.reason == "DRAWDOWN_KILL":
            update_client_state(client_id, {"kill_switch": 1, "mode": "READ_ONLY"})
        return {"ok": False, "reason": gates.reason, "details": gates.details, "client_id": client_id}

    # Create order plan
    try:
        plan = create_order_plan(signal, risk_state, broker, account_equity=current_equity)
        audit(client_id, "INFO", "ORDER_PLAN_CREATED", {"signal_id": signal.signal_id, "plan": plan.model_dump()})
    except Exception as e:
        audit(client_id, "ERROR", "ORDER_PLAN_FAIL", {"signal_id": signal.signal_id, "err": str(e)})
        return {"ok": False, "reason": "ORDER_PLAN_FAIL", "error": str(e), "client_id": client_id}

    # Persist an entry order row + position id
    position_id = str(uuid.uuid4())
    local_order_id = str(uuid.uuid4())

    try:
        persist_order(client_id, local_order_id, position_id, plan)
    except Exception as e:
        audit(client_id, "ERROR", "ORDER_PERSIST_FAIL", {"err": str(e)})
        return {"ok": False, "reason": "ORDER_PERSIST_FAIL", "client_id": client_id}

    # Place order
    try:
        resp = broker.place_order(
            symbol=plan.underlying,
            contract=plan.contract,
            qty=int(plan.qty),
            limit_price=plan.limit_price,
            side="buy_to_open" if plan.direction == "CALL" else "buy_to_open",
        )
        broker_order_id = getattr(resp, "broker_order_id", None)
        status = getattr(resp, "status", "SUBMITTED")
        update_order_row(local_order_id, status=status, broker_order_id=broker_order_id)

        audit(client_id, "INFO", "ENTRY_ORDER_SUBMITTED", {
            "signal_id": signal.signal_id,
            "local_order_id": local_order_id,
            "broker_order_id": broker_order_id,
            "status": status,
            "contract": plan.contract,
            "qty": int(plan.qty),
        })

        # If broker returns an immediate fill price
        avg_fill = getattr(resp, "avg_fill", None) or getattr(resp, "fill_price", None)
        if avg_fill is not None:
            open_position_from_fill(client_id, position_id, plan, avg_fill=float(avg_fill))
            audit(client_id, "INFO", "POSITION_OPENED", {"position_id": position_id, "avg_fill": float(avg_fill)})

            # increment trades today
            update_client_state(client_id, {"trades_taken_today": int(state.get("trades_taken_today", 0) or 0) + 1})

        return {
            "ok": True,
            "client_id": client_id,
            "local_order_id": local_order_id,
            "broker_order_id": broker_order_id,
            "status": status,
            "position_id": position_id,
        }

    except Exception as e:
        update_order_row(local_order_id, status="ERROR", last_error=str(e))
        audit(client_id, "ERROR", "ENTRY_ORDER_FAIL", {"local_order_id": local_order_id, "err": str(e)})
        return {"ok": False, "reason": "ENTRY_ORDER_FAIL", "error": str(e), "client_id": client_id}
