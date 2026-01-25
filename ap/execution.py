# ap/execution.py
import uuid

from ap.config import Config
from ap.logger import get_logger
from ap.models import Signal, OrderPlan
from ap.db import conn, run_with_retry, get_client_state, update_client_state
from ap.utils import now_utc_iso, json_dumps
from ap.risk import run_gates, effective_limits
from ap.broker import BrokerAdapter

# contract selection helpers
from ap.contract_selection import pick_expiration, resolve_contract_symbol

cfg = Config()
log = get_logger("ap.execution")

DEFAULT_CLIENT_ID = "default"


def count_open_positions(client_id: str) -> int:
    with conn() as c:
        row = run_with_retry(lambda: c.execute(
            "SELECT COUNT(*) as n FROM positions WHERE status='OPEN' AND client_id=?",
            (client_id,)
        ).fetchone())
        return int(row["n"]) if row else 0


def audit(client_id: str, level: str, event: str, payload: dict):
    """
    Writes audit log. Migration adds client_id column; if not present it will still work
    if your table is older (but you already migrated).
    """
    with conn() as c:
        # Prefer including client_id
        try:
            run_with_retry(lambda: c.execute(
                "INSERT INTO audit_log (client_id, ts, level, event, payload) VALUES (?,?,?,?,?)",
                (client_id, now_utc_iso(), level, event, json_dumps(payload)),
            ))
        except Exception:
            # Fallback for older audit_log schema
            run_with_retry(lambda: c.execute(
                "INSERT INTO audit_log (ts, level, event, payload) VALUES (?,?,?,?)",
                (now_utc_iso(), level, event, json_dumps(payload)),
            ))


def create_order_plan(signal: Signal, state: dict, broker: BrokerAdapter) -> OrderPlan:
    """
    Builds a real OrderPlan by resolving the Tradier option_symbol from:
      signal.trigger['strike']
      signal.trigger['expiry_hint']  (Weekly / 0DTE)
      signal.direction (CALL / PUT)
    """
    limits = effective_limits(state)
    pos_pct = limits["pos_pct"]

    strike_val = signal.trigger.get("strike")
    if strike_val is None:
        raise RuntimeError("Missing strike in signal.trigger (parser must supply this)")

    strike = float(strike_val)
    hint = signal.trigger.get("expiry_hint")
    raw_strike = signal.trigger.get("raw_strike")

    expirations = broker.get_option_expirations(signal.symbol)
    exp = pick_expiration(expirations, hint)

    chain = broker.get_option_chain(signal.symbol, exp)
    contract = resolve_contract_symbol(chain, strike, signal.direction)

    return OrderPlan(
        plan_id=str(uuid.uuid4()),
        symbol=signal.symbol,
        contract=contract,
        direction=signal.direction,
        qty=1,  # MVP fixed qty
        position_pct=pos_pct,
        tp_pct=cfg.TAKE_PROFIT_PCT,
        sl_pct=cfg.STOP_LOSS_PCT,
        limit_price=None,
        metadata={
            "pattern_id": signal.pattern_id,
            "confidence": signal.confidence_tag,
            "expiration": exp,
            "strike": strike,
            "expiry_hint": hint,
            "raw_strike": raw_strike,
        },
    )


def persist_order(
    client_id: str,
    local_order_id: str,
    kind: str,
    status: str,
    plan: OrderPlan,
    broker_order_id=None,
    filled_qty=0,
    last_error=None,
):
    with conn() as c:
        ts = now_utc_iso()
        # Include client_id in insert
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
                broker_order_id,
                None,
                kind,
                status,
                plan.symbol,
                plan.contract,
                plan.qty,
                plan.limit_price,
                filled_qty,
                0,
                last_error,
                ts,
                ts,
            ),
        ))


def update_order_row(local_order_id: str, **fields):
    """
    Update orders row by local_order_id.
    fields can include: broker_order_id, status, filled_qty, last_error
    """
    if not fields:
        return

    allowed = {"broker_order_id", "status", "filled_qty", "last_error", "position_id"}
    sets = []
    vals = []
    for k, v in fields.items():
        if k in allowed:
            sets.append(f"{k}=?")
            vals.append(v)

    sets.append("updated_ts=?")
    vals.append(now_utc_iso())

    vals.append(local_order_id)

    with conn() as c:
        run_with_retry(lambda: c.execute(
            f"UPDATE orders SET {', '.join(sets)} WHERE local_order_id=?",
            tuple(vals),
        ))


def open_position_from_fill(client_id: str, plan: OrderPlan, avg_fill: float) -> str:
    pos_id = str(uuid.uuid4())
    with conn() as c:
        run_with_retry(lambda: c.execute(
            """
            INSERT INTO positions (
                id, client_id,
                underlying, contract, direction, qty, avg_fill,
                entry_ts, tp_pct, sl_pct, status
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                pos_id,
                client_id,
                plan.symbol,
                plan.contract,
                plan.direction,
                plan.qty,
                avg_fill,
                now_utc_iso(),
                plan.tp_pct,
                plan.sl_pct,
                "OPEN",
            ),
        ))
    return pos_id


def process_signal(signal: Signal, broker: BrokerAdapter, client_id: str = DEFAULT_CLIENT_ID) -> dict:
    """
    Client-scoped execution.
    Uses client_state for mode/kill/equity counters.
    """
    # Load per-client state
    state = get_client_state(client_id)

    # Safety: respect client kill switch / read-only mode
    if int(state.get("kill_switch", 0)) == 1 or (state.get("mode") or "").upper() == "READ_ONLY":
        return {"ok": False, "reason": "CLIENT_READ_ONLY", "client_id": client_id}

    # Refresh equity from broker
    try:
        current_equity = float(broker.get_account_equity())
    except Exception as e:
        audit(client_id, "ERROR", "BROKER_EQUITY_FAIL", {"err": str(e)})
        # Put this client into READ_ONLY
        update_client_state(client_id, {"mode": "READ_ONLY"})
        return {"ok": False, "reason": "BROKER_EQUITY_FAIL", "error": str(e), "client_id": client_id}

    # Persist equity snapshot to client_state
    update_client_state(client_id, {"current_equity": current_equity})

    # Build a state dict compatible with your risk module
    # (risk.py expects keys like current_equity_last / trades_taken_today / daily_stop_hit / kill_switch / mode, etc.)
    risk_state = {
        "current_equity_last": current_equity,
        "trades_taken_today": int(state.get("trades_taken_today", 0)),
        "daily_stop_hit": bool(int(state.get("daily_stop_hit", 0))),
        "kill_switch": bool(int(state.get("kill_switch", 0))),
        "mode": state.get("mode", "PAPER"),
    }

    open_positions = count_open_positions(client_id)

    # Run gates
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

    # Create order plan (resolve contract)
    try:
        plan = create_order_plan(signal, risk_state, broker)
        audit(client_id, "INFO", "ORDER_PLAN_CREATED", {"signal_id": signal.signal_id, "plan": plan.model_dump()})
    except Exception as e:
        audit(client_id, "ERROR", "ORDER_PLAN_FAIL", {"signal_id": signal.signal_id, "err": str(e), "trigger": signal.trigger})
        return {"ok": False, "reason": "ORDER_PLAN_FAIL", "error": str(e), "client_id": client_id}

    local_order_id = str(uuid.uuid4())
    persist_order(client_id, local_order_id, "ENTRY", "NEW", plan)

    # Execute
    try:
        resp = broker.place_order(
            plan.symbol,
            plan.contract,
            plan.qty,
            plan.limit_price,
            side="buy_to_open"
        )

        audit(client_id, "INFO", "ORDER_RESPONSE", {"local_order_id": local_order_id, "resp": getattr(resp, "__dict__", str(resp))})

        update_order_row(
            local_order_id,
            broker_order_id=getattr(resp, "broker_order_id", None),
            status=getattr(resp, "status", None),
            filled_qty=getattr(resp, "filled_qty", 0) or 0,
        )

        if resp.status == "REJECTED":
            update_order_row(local_order_id, last_error=str(getattr(resp, "error", "")))
            audit(client_id, "ERROR", "ORDER_REJECTED", {"local_order_id": local_order_id, "error": getattr(resp, "error", "")})
            return {"ok": False, "reason": "ORDER_REJECTED", "error": getattr(resp, "error", ""), "client_id": client_id}

        # Tradier: ACK is successful submission
        if resp.status == "ACK":
            update_client_state(client_id, {"trades_taken_today": int(state.get("trades_taken_today", 0)) + 1})
            audit(client_id, "INFO", "ORDER_ACKNOWLEDGED", {"local_order_id": local_order_id, "broker_order_id": getattr(resp, "broker_order_id", None)})
            return {
                "ok": True,
                "reason": "ORDER_ACK",
                "local_order_id": local_order_id,
                "broker_order_id": getattr(resp, "broker_order_id", None),
                "plan": plan.model_dump(),
                "client_id": client_id,
            }

        # SIM broker can return FILLED/PARTIAL
        if resp.status in ("FILLED", "PARTIAL"):
            pos_id = open_position_from_fill(client_id, plan, resp.avg_fill_price)
            update_order_row(local_order_id, position_id=pos_id)
            update_client_state(client_id, {"trades_taken_today": int(state.get("trades_taken_today", 0)) + 1})
            audit(client_id, "INFO", "POSITION_OPENED", {"position_id": pos_id, "plan": plan.model_dump()})
            return {
                "ok": True,
                "position_id": pos_id,
                "local_order_id": local_order_id,
                "broker_order_id": getattr(resp, "broker_order_id", None),
                "plan": plan.model_dump(),
                "client_id": client_id,
            }

        # Unknown status - fail safe
        update_client_state(client_id, {"mode": "READ_ONLY"})
        update_order_row(local_order_id, last_error=f"UNKNOWN_STATUS:{getattr(resp, 'status', None)}")
        return {"ok": False, "reason": "UNKNOWN_ORDER_STATUS", "status": getattr(resp, "status", None), "client_id": client_id}

    except Exception as e:
        update_order_row(local_order_id, status="REJECTED", last_error=str(e))
        audit(client_id, "ERROR", "EXECUTION_EXCEPTION", {"err": str(e)})
        update_client_state(client_id, {"mode": "READ_ONLY"})
        return {"ok": False, "reason": "EXECUTION_EXCEPTION", "error": str(e), "client_id": client_id}
