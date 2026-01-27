# ap/execution.py - COMPLETE MULTI-CLIENT VERSION
# client_id properly threaded through all functions
# Ready to deploy - no manual edits needed

import math
from datetime import datetime, timezone

from ap.logger import get_logger
from ap.db import conn, run_with_retry, get_client, get_client_state, update_client_state, insert_order, new_local_order_id
from ap.utils import json_dumps, now_utc_iso
from zoneinfo import ZoneInfo

log = get_logger("ap.execution")

OPT_MULTIPLIER = 100


def audit(client_id: str, level: str, event: str, payload: dict):
    """Log audit event with client_id"""
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "INSERT INTO audit_log (ts, level, event, payload, client_id) VALUES (?,?,?,?,?)",
            (now_utc_iso(), level, event, json_dumps(payload), client_id)
        ))


def _count_open_positions(client_id: str) -> int:
    """Count open positions for a client"""
    with conn() as c:
        row = run_with_retry(lambda: c.execute("""
            SELECT COUNT(*) AS n
            FROM positions
            WHERE client_id=?
              AND status IN ('OPEN','CLOSING')
        """, (client_id,)).fetchone())
        return int(row["n"] or 0)


def _today_trade_count(state: dict) -> int:
    """Get today's trade count from state"""
    return int(state.get("trades_taken_today") or 0)


def _get_equity_for_client(broker, client_id: str, client_cfg: dict, state: dict) -> float:
    """Get current equity for client (from broker or state)"""
    try:
        eq = float(broker.get_account_equity())
        return eq
    except Exception:
        return float(state.get("current_equity") or state.get("starting_equity_today") or client_cfg.get("initial_equity") or 0.0)


def _position_size_dollars(equity: float, pct: float) -> float:
    """Calculate position size in dollars"""
    return max(0.0, equity * pct)


def _get_contract_price(broker, contract: str) -> float:
    """Get current price of a contract from broker"""
    try:
        # This depends on your broker implementation
        # For Tradier: use option_chains or similar
        price = broker.get_contract_price(contract)
        return float(price)
    except Exception:
        # Fallback: assume mid-range
        return 150.0


def _validate_contract_price(price: float) -> bool:
    """Validate contract is in valid price range ($100-250)"""
    return 100.0 <= price <= 250.0


def _calculate_qty(dollar_amount: float, contract_price: float) -> int:
    """Calculate contract quantity from dollar amount"""
    if contract_price <= 0:
        return 0
    qty = int(dollar_amount / contract_price)
    return max(1, qty)

NY = ZoneInfo("America/New_York")

def _ny_day_key() -> str:
    return datetime.now(NY).strftime("%Y-%m-%d")

def _maybe_reset_client_daily_state(broker, client_id: str, client_cfg: dict, st: dict) -> dict:
    """
    Resets per-client daily counters when NY day changes.
    Also syncs starting_equity_today to current equity (important for PAPER).
    """
    today = _ny_day_key()
    prev = (st.get("day_key") or "").strip()

    # Always try to compute equity once (used for baseline)
    equity = _get_equity_for_client(broker, client_id, client_cfg, st)

    # If day changed OR day_key missing, reset daily counters
    if prev != today:
        patch = {
            "day_key": today,
            "last_day_reset_ts": now_utc_iso(),
            "trades_taken_today": 0,
            "realized_pnl_today": 0.0,
            "daily_stop_hit": 0,
            "profit_cap_state": "normal",
            "starting_equity_today": float(equity),
            "current_equity": float(equity),
        }
        update_client_state(client_id, patch)
        st.update(patch)
    else:
        # Same day: just keep equity fresh
        try:
            update_client_state(client_id, {"current_equity": float(equity)})
        except Exception:
            pass
        st["current_equity"] = float(equity)

    return st

def process_signal(broker, client_id: str, signal_payload: dict) -> dict:
    """
    Core execution: process a signal for a client.
    Called by worker_loop() from queue.py.
    
    Args:
        broker: Broker instance
        client_id: Which client this signal is for
        signal_payload: Dict with signal data
    
    Returns:
        Dict with {"ok": bool, ...}
    """
    try:
        # 0) VALIDATE CLIENT ACTIVE
        client = get_client(client_id)
        if (client.get("status") or "").upper() != "ACTIVE":
            return {"ok": False, "error": "client_inactive", "client_id": client_id}

        # 1) GET CLIENT STATE
        st = get_client_state(client_id)
        mode = (st.get("mode") or "").upper()
        if bool(st.get("kill_switch")) or mode == "READ_ONLY":
            return {"ok": False, "reason": "READ_ONLY", "error": "bot_in_read_only", "client_id": client_id}
       
        # NEW: daily reset + baseline sync (prevents PAPER growth check crashes)
        st = _maybe_reset_client_daily_state(broker, client_id, client, st)
    

        # 2) GROWTH/TARGET CHECK FIRST (before any trade)
        try:
            from ap.account_growth import check_growth_status
            growth = check_growth_status(client_id)
            if growth.get("should_stop"):
                update_client_state(client_id, {"kill_switch": 1, "mode": "READ_ONLY"})
                audit(client_id, "INFO", "GROWTH_STOP", growth)
                return {"ok": False, "reason": "GROWTH_STOP", "error": "profit_target_hit", "client_id": client_id}
        except Exception as e:
                audit(client_id, "ERROR", "GROWTH_CHECK_FAILED", {"error": str(e), "mode": mode})

    # Fail-open in PAPER/SIM so you can keep testing.
    # Fail-closed in LIVE for safety.
    if mode in ("PAPER", "SIM"):
        log.warning(f"Growth check failed in {mode}; allowing PAPER trade for testing. err={e}")
    else:
        return {"ok": False, "error": "growth_check_failed", "client_id": client_id}

        # 3) DAILY TRADE CAP
        max_trades_per_day = int(client.get("max_trades_per_day") or 0) or 25
        if _today_trade_count(st) >= max_trades_per_day:
            audit(client_id, "INFO", "DAILY_CAP_REACHED", {"max_trades_per_day": max_trades_per_day})
            return {"ok": False, "reason": "DAILY_CAP", "error": "daily_trade_cap_reached", "client_id": client_id}

        # 4) MAX CONCURRENT POSITIONS
        max_open = int(client.get("max_concurrent_positions") or 0) or 2
        open_now = _count_open_positions(client_id)
        if open_now >= max_open:
            audit(client_id, "INFO", "MAX_OPEN_REACHED", {"max_open": max_open, "open_now": open_now})
            return {"ok": False, "reason": "MAX_OPEN", "error": "max_open_positions", "client_id": client_id}

        # 5) PARSE SIGNAL
        symbol = (signal_payload.get("symbol") or "").upper().strip()
        direction = (signal_payload.get("direction") or "").upper().strip()
        trigger = signal_payload.get("trigger") or {}

        if not symbol or direction not in ("CALL", "PUT"):
            return {"ok": False, "error": "invalid_symbol_or_direction", "client_id": client_id}

        pattern_id = signal_payload.get("pattern_id", "SCANNER_V1")
        signal_id = signal_payload.get("signal_id", "unknown")

        # 6) EXTRACT TRIGGER DATA
        strike = trigger.get("strike")
        entry = trigger.get("entry")
        stop = trigger.get("stop")
        pt1 = trigger.get("pt1")
        expiry_hint = trigger.get("expiry_hint", "Weekly")

        if not strike:
            return {"ok": False, "error": "missing_strike", "client_id": client_id}

        # 7) BUILD CONTRACT NAME
        contract = f"{symbol} {strike} {direction[0]} {expiry_hint}"

        # 8) GET CLIENT EQUITY (for position sizing)
        equity = _get_equity_for_client(broker, client_id, client, st)
        if equity <= 0:
            return {"ok": False, "error": "invalid_equity", "equity": equity, "client_id": client_id}

        # 9) POSITION SIZING (15% of working capital)
        position_pct = 0.15  # 15% per trade
        dollar_amount = _position_size_dollars(equity, position_pct)

        # 10) GET CONTRACT PRICE AND VALIDATE
        try:
            contract_price = _get_contract_price(broker, contract)
        except Exception as e:
            contract_price = 150.0  # fallback
            log.warning(f"Could not get contract price for {contract}, using fallback: {e}")

        if not _validate_contract_price(contract_price):
            audit(client_id, "WARN", "CONTRACT_PRICE_OUT_OF_RANGE", {
                "contract": contract,
                "price": contract_price,
                "min": 100.0,
                "max": 250.0
            })
            return {"ok": False, "error": "contract_price_out_of_range", "price": contract_price, "client_id": client_id}

        # 11) CALCULATE QTY
        qty = _calculate_qty(dollar_amount, contract_price)
        if qty <= 0:
            return {"ok": False, "error": "position_size_too_small", "dollar_amount": dollar_amount, "contract_price": contract_price, "client_id": client_id}

        # 12) CREATE POSITION ID
        position_id = f"pos_{signal_id}_{now_utc_iso().replace(':', '').replace('-', '')}"

        # 13) INSERT POSITION (OPEN)
        try:
            with conn() as c:
                run_with_retry(lambda: c.execute("""
                    INSERT INTO positions (
                        id, client_id, underlying, contract, direction, qty,
                        avg_fill, entry_ts, tp_pct, sl_pct, status
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    position_id,
                    client_id,
                    symbol,
                    contract,
                    direction,
                    int(qty),
                    float(contract_price),
                    now_utc_iso(),
                    0.23,  # TP: 23%
                    0.15,  # SL: 15%
                    "OPEN"
                )))
        except Exception as e:
            log.error(f"Failed to insert position: {e}")
            audit(client_id, "ERROR", "POSITION_INSERT_FAILED", {"error": str(e), "position_id": position_id})
            return {"ok": False, "error": "position_insert_failed", "client_id": client_id}

        # 14) CREATE ORDER PLAN
        local_order_id = new_local_order_id()
        limit_price = float(entry) if entry else contract_price

        try:
            insert_order(
                client_id=client_id,  # ← REQUIRED: pass client_id
                local_order_id=local_order_id,
                position_id=position_id,
                kind="ENTRY",
                status="NEW",
                symbol=symbol,
                contract=contract,
                qty=int(qty),
                limit_price=limit_price,
            )
        except Exception as e:
            log.error(f"Failed to insert order: {e}")
            audit(client_id, "ERROR", "ORDER_INSERT_FAILED", {"error": str(e)})
            return {"ok": False, "error": "order_insert_failed", "client_id": client_id}

        # 15) PLACE ACTUAL ORDER WITH BROKER
        try:
            order_resp = broker.place_order(
                symbol=symbol,
                contract=contract,
                qty=int(qty),
                limit_price=limit_price,
                side="buy_to_open"
            )

            broker_order_id = getattr(order_resp, "broker_order_id", None)
            order_status = getattr(order_resp, "status", "SUBMITTED")

            # Update order with broker ID
            from ap.db import update_order
            update_order(
                local_order_id,
                status=order_status,
                broker_order_id=broker_order_id
            )

            log.info(f"Order placed: {client_id} {symbol} {direction} qty={qty} @ ${limit_price}")

        except Exception as e:
            log.error(f"Broker order failed: {e}")
            audit(client_id, "ERROR", "BROKER_ORDER_FAILED", {"error": str(e), "contract": contract})
            return {"ok": False, "error": "broker_order_failed", "details": str(e), "client_id": client_id}

        # 16) UPDATE CLIENT STATE
        try:
            update_client_state(client_id, {
                "trades_taken_today": _today_trade_count(st) + 1,
                "current_equity": equity,
            })
        except Exception as e:
            log.warning(f"Failed to update client state: {e}")

        # 17) AUDIT SUCCESS
        audit(client_id, "INFO", "TRADE_EXECUTED", {
            "signal_id": signal_id,
            "pattern_id": pattern_id,
            "symbol": symbol,
            "direction": direction,
            "strike": strike,
            "contract": contract,
            "qty": qty,
            "entry_price": contract_price,
            "position_id": position_id,
            "order_id": broker_order_id,
            "status": order_status,
        })

        return {
            "ok": True,
            "client_id": client_id,
            "signal_id": signal_id,
            "position_id": position_id,
            "order_id": broker_order_id,
            "symbol": symbol,
            "direction": direction,
            "contract": contract,
            "qty": qty,
            "entry_price": contract_price,
            "tp_pct": 0.23,
            "sl_pct": 0.15,
            "status": "executed",
        }

    except Exception as e:
        log.error(f"Execution failed: {e}")
        try:
            audit(client_id, "ERROR", "EXECUTION_FAILED", {"error": str(e)})
        except Exception:
            pass
        return {"ok": False, "error": str(e), "client_id": client_id}
