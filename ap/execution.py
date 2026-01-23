# ap/execution.py
import uuid

from ap.config import Config
from ap.logger import get_logger
from ap.models import Signal, OrderPlan
from ap.db import conn, run_with_retry
from ap.utils import now_utc_iso, json_dumps
from ap.state import load_state, update_state, reserve_equity, release_equity
from ap.risk import run_gates, effective_limits
from ap.broker import BrokerAdapter

# contract selection helpers
from ap.contract_selection import pick_expiration, resolve_contract_symbol

cfg = Config()
log = get_logger("ap.execution")


def count_open_positions() -> int:
    with conn() as c:
        row = run_with_retry(lambda: c.execute(
            "SELECT COUNT(*) as n FROM positions WHERE status='OPEN'"
        ).fetchone())
        return int(row["n"]) if row else 0


def audit(level: str, event: str, payload: dict):
    with conn() as c:
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
        run_with_retry(lambda: c.execute(
            """
            INSERT INTO orders (
                local_order_id, broker_order_id, position_id, kind, status,
                symbol, contract, qty, limit_price, filled_qty, retries, last_error,
                created_ts, updated_ts
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
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


def open_position_from_fill(plan: OrderPlan, avg_fill: float) -> str:
    pos_id = str(uuid.uuid4())
    with conn() as c:
        run_with_retry(lambda: c.execute(
            """
            INSERT INTO positions (
                id, underlying, contract, direction, qty, avg_fill,
                entry_ts, tp_pct, sl_pct, status
            )
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                pos_id,
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


def process_signal(signal: Signal, broker: BrokerAdapter) -> dict:
    # Load once
    state = load_state()

    # Refresh equity from broker
    current_equity = float(state.get("current_equity_last", 10000.0))
    try:
        current_equity = float(broker.get_account_equity())
    except Exception as e:
        audit("ERROR", "BROKER_EQUITY_FAIL", {"err": str(e)})
        update_state({"mode": "READ_ONLY"})
        return {"ok": False, "reason": "BROKER_EQUITY_FAIL", "error": str(e)}

    # Update snapshot + persist once
    state["current_equity_last"] = current_equity
    update_state({"current_equity_last": current_equity})

    open_positions = count_open_positions()

    # Run gates off the in-memory snapshot (no extra DB load)
    gates = run_gates(state, open_positions)

    audit("INFO", "GATES_RESULT", {
        "signal_id": signal.signal_id,
        "ok": gates.ok,
        "reason": gates.reason,
        "details": gates.details,
    })

    if not gates.ok:
        if gates.reason == "DRAWDOWN_KILL":
            update_state({"kill_switch": True, "mode": "READ_ONLY"})
        return {"ok": False, "reason": gates.reason, "details": gates.details}

    # Create order plan (resolve contract)
    try:
        plan = create_order_plan(signal, state, broker)
        audit("INFO", "ORDER_PLAN_CREATED", {"signal_id": signal.signal_id, "plan": plan.model_dump()})
    except Exception as e:
        audit("ERROR", "ORDER_PLAN_FAIL", {"signal_id": signal.signal_id, "err": str(e), "trigger": signal.trigger})
        return {"ok": False, "reason": "ORDER_PLAN_FAIL", "error": str(e)}

    # Reserve equity (position_pct of equity)
    reserve_amt = plan.position_pct * float(state["current_equity_last"])
    reserve_equity(reserve_amt)
    audit("INFO", "EQUITY_RESERVED", {"signal_id": signal.signal_id, "amount": reserve_amt})

    local_order_id = str(uuid.uuid4())
    persist_order(local_order_id, "ENTRY", "NEW", plan)

    # Execute
    try:
        resp = broker.place_order(plan.symbol, plan.contract, plan.qty, plan.limit_price)
        audit("INFO", "ORDER_RESPONSE", {"local_order_id": local_order_id, "resp": getattr(resp, "__dict__", str(resp))})

        if resp.status == "REJECTED":
            release_equity(reserve_amt)
            audit("ERROR", "ORDER_REJECTED", {"local_order_id": local_order_id, "error": resp.error})
            return {"ok": False, "reason": "ORDER_REJECTED", "error": resp.error}

        # Tradier returns ACK (not FILLED immediately). Treat ACK as successful submission.
        if resp.status == "ACK":
            release_equity(reserve_amt)
            audit("INFO", "ORDER_ACKNOWLEDGED", {"local_order_id": local_order_id, "broker_order_id": resp.broker_order_id})
            return {"ok": True, "reason": "ORDER_ACK", "broker_order_id": resp.broker_order_id, "plan": plan.model_dump()}

        # SIM broker can return FILLED/PARTIAL
        if resp.status in ("FILLED", "PARTIAL"):
            pos_id = open_position_from_fill(plan, resp.avg_fill_price)
            update_state({"trades_taken_today": int(state.get("trades_taken_today", 0)) + 1})
            release_equity(reserve_amt)
            audit("INFO", "POSITION_OPENED", {"position_id": pos_id, "plan": plan.model_dump()})
            return {"ok": True, "position_id": pos_id, "plan": plan.model_dump()}

        # Unknown status - fail safe
        update_state({"mode": "READ_ONLY"})
        release_equity(reserve_amt)
        return {"ok": False, "reason": "UNKNOWN_ORDER_STATUS", "status": resp.status}

    except Exception as e:
        release_equity(reserve_amt)
        audit("ERROR", "EXECUTION_EXCEPTION", {"err": str(e)})
        update_state({"mode": "READ_ONLY"})
        return {"ok": False, "reason": "EXECUTION_EXCEPTION", "error": str(e)}


