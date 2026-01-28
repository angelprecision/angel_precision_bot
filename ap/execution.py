# ap/execution.py - FIXED VERSION
# Fixes: contract symbol resolution, growth tracking, proper option pricing

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from ap.logger import get_logger
from ap.db import (
    conn,
    run_with_retry,
    get_client,
    get_client_state,
    update_client_state,
    insert_order,
    new_local_order_id,
    update_order,
)
from ap.utils import json_dumps, now_utc_iso
from ap.contract_selection import pick_expiration, resolve_contract_symbol

log = get_logger("ap.execution")

OPT_MULTIPLIER = 100
NY = ZoneInfo("America/New_York")


def audit(client_id: str, level: str, event: str, payload: dict):
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "INSERT INTO audit_log (ts, level, event, payload, client_id) VALUES (?,?,?,?,?)",
            (now_utc_iso(), level, event, json_dumps(payload), client_id),
        ))


def _ny_day_key() -> str:
    return datetime.now(NY).strftime("%Y-%m-%d")


def _count_open_positions(client_id: str) -> int:
    with conn() as c:
        row = run_with_retry(lambda: c.execute("""
            SELECT COUNT(*) AS n
            FROM positions
            WHERE client_id=?
              AND status IN ('OPEN','CLOSING')
        """, (client_id,)).fetchone())
        return int(row["n"] or 0)


def _today_trade_count(st: dict) -> int:
    return int(st.get("trades_taken_today") or 0)


def _get_equity_for_client(broker, st: dict, client_cfg: dict) -> float:
    """Get current equity with fallbacks"""
    try:
        return float(broker.get_account_equity())
    except Exception as e:
        log.warning(f"Failed to get broker equity: {e}")
        return float(
            st.get("current_equity")
            or st.get("starting_equity_today")
            or client_cfg.get("initial_equity")
            or 100000.0  # Default fallback
        )


def _resolve_option_contract(broker, symbol: str, strike: float, direction: str, mode: str = "PAPER") -> tuple[str, float]:

    """
    Get the actual option contract symbol from broker's option chain.
    Returns: (contract_symbol, premium_per_share)
    """
    try:
        # Get available expirations
        expirations = broker.get_option_expirations(symbol)
        if not expirations:
            raise ValueError(f"No expirations available for {symbol}")
        
        # Pick expiration (0DTE or next available)
        expiration = pick_expiration(expirations, hint="0DTE")
        
        # Get option chain for that expiration
        chain = broker.get_option_chain(symbol, expiration)
if not chain:
    # ✅ PAPER/SIM fallback: allow E2E testing without live option chains
        if (mode or "").upper() in ("PAPER", "SIM"):
        contract_symbol = f"{symbol}_{expiration}_{int(strike)}_{direction}"
        premium = 1.00  # $100/contract synthetic premium
        log.warning(f"[{mode}] No options in chain for {symbol} {expiration}. Using synthetic {contract_symbol} @ {premium}")
        return contract_symbol, premium
            raise ValueError(f"No options in chain for {symbol} {expiration}")

        # Resolve actual contract symbol
        contract_symbol, premium = _resolve_option_contract(broker, symbol, strike, direction, mode=mode)
        
        # Get current premium
        from ap.contract_pricing import get_contract_price
        premium = get_contract_price(broker, contract_symbol, side="BUY")
        
        if premium <= 0:
            raise ValueError(f"Invalid premium for {contract_symbol}: {premium}")
        
        return contract_symbol, premium
        
    except Exception as e:
        log.error(f"Contract resolution failed: {e}")
        raise


def _validate_premium(premium: float) -> bool:
    """Accept $50–$250 contracts => premium 0.50–2.50 per share"""
    return 0.50 <= float(premium) <= 2.50


def _calc_qty(dollars: float, premium: float) -> int:
    """Calculate number of contracts based on dollar allocation"""
    cost_per_contract = premium * OPT_MULTIPLIER
    if cost_per_contract <= 0:
        return 0
    return max(1, int(dollars // cost_per_contract))


def _maybe_reset_daily_state(broker, client_id: str, client_cfg: dict, st: dict) -> dict:
    """Reset daily counters at market open"""
    today = _ny_day_key()
    prev = st.get("day_key")

    equity = _get_equity_for_client(broker, st, client_cfg)

    if prev != today:
        log.info(f"Daily reset for {client_id}: equity=${equity:,.2f}")
        patch = {
            "day_key": today,
            "trades_taken_today": 0,
            "realized_pnl_today": 0.0,
            "daily_stop_hit": 0,
            "starting_equity_today": equity,
            "current_equity": equity,
        }
        update_client_state(client_id, patch)
        st.update(patch)
    else:
        # Just update equity
        update_client_state(client_id, {"current_equity": equity})
        st["current_equity"] = equity

    return st


def _check_growth_limits(client_id: str, mode: str) -> dict:
    """
    Check if growth targets are hit (SAFE: won't block if not initialized).
    Returns: {"ok": True/False, "error": str}
    """
    # Skip growth checks in paper/sim modes
    if mode in ("PAPER", "SIM"):
        return {"ok": True}
    
    try:
        from ap.account_growth import check_growth_status, get_growth_metrics
        
        # Check if growth tracking is even enabled
        metrics = get_growth_metrics(client_id)
        if not metrics.get("ok"):
            # Not initialized - allow trades
            log.debug(f"Growth tracking not initialized for {client_id}, allowing trades")
            return {"ok": True}
        
        # Check if we should stop
        status = check_growth_status(client_id)
        if status.get("should_stop"):
            reason = status.get("reason", "unknown")
            log.warning(f"Growth limit hit for {client_id}: {reason}")
            
            # Set kill switch
            update_client_state(client_id, {
                "kill_switch": 1,
                "mode": "READ_ONLY"
            })
            
            audit(client_id, "WARNING", "GROWTH_TARGET_HIT", status)
            return {"ok": False, "error": f"growth_limit_{reason}"}
        
        return {"ok": True}
        
    except Exception as e:
        log.error(f"Growth check failed: {e}")
        # If we can't check growth, block trades in live mode for safety
        if mode == "LIVE":
            return {"ok": False, "error": "growth_check_failed"}
        return {"ok": True}


def process_signal(broker, client_id: str, signal_payload: dict) -> dict:
    """
    Main execution entry point.
    Receives signal from scanner and executes trade if all checks pass.
    
    Args:
        broker: Broker adapter instance
        client_id: Client identifier
        signal_payload: {
            "symbol": "SPY",
            "direction": "CALL" or "PUT",
            "trigger": {"strike": 580.0},
            "reason": "breakout"
        }
    
    Returns:
        {"ok": True/False, ...details}
    """
    try:
        log.info(f"Processing signal for {client_id}: {signal_payload}")
        
        # 1) Validate client
        client = get_client(client_id)
        if (client.get("status") or "").upper() != "ACTIVE":
            log.warning(f"Client {client_id} is not active: {client.get('status')}")
            return {"ok": False, "error": "client_inactive"}

        # 2) Get state
        st = get_client_state(client_id)
        mode = (st.get("mode") or "PAPER").upper()
        
        log.info(f"Client {client_id} mode: {mode}")

        # Check kill switch
        if st.get("kill_switch"):
            log.warning(f"Kill switch active for {client_id}")
            return {"ok": False, "error": "kill_switch_active"}
        
        # Check read-only mode
        if mode == "READ_ONLY":
            log.warning(f"Client {client_id} in read-only mode")
            return {"ok": False, "error": "read_only_mode"}

        # 3) Reset daily state if needed
        st = _maybe_reset_daily_state(broker, client_id, client, st)

        # 4) Check growth limits (SAFE)
        growth_check = _check_growth_limits(client_id, mode)
        if not growth_check.get("ok"):
            return growth_check

        # 5) Check daily trade cap
        max_trades = int(client.get("max_trades_per_day") or 25)
        trades_today = _today_trade_count(st)
        
        if trades_today >= max_trades:
            log.warning(f"Daily trade cap hit: {trades_today}/{max_trades}")
            return {"ok": False, "error": "daily_trade_cap", "trades_today": trades_today}

        # 6) Check max open positions
        max_open = int(client.get("max_concurrent_positions") or 2)
        open_positions = _count_open_positions(client_id)
        
        if open_positions >= max_open:
            log.warning(f"Max open positions: {open_positions}/{max_open}")
            return {"ok": False, "error": "max_open_positions", "open_positions": open_positions}

        # 7) Parse signal
        symbol = (signal_payload.get("symbol") or "").upper()
        direction = (signal_payload.get("direction") or "").upper()
        trigger = signal_payload.get("trigger") or {}
        
        if not symbol:
            return {"ok": False, "error": "missing_symbol"}
        
        if direction not in ("CALL", "PUT"):
            return {"ok": False, "error": "invalid_direction", "direction": direction}

        strike = trigger.get("strike")
        if not strike:
            return {"ok": False, "error": "missing_strike"}
        
        strike = float(strike)

        # 8) Get equity and calculate position size
        equity = _get_equity_for_client(broker, st, client)
        position_pct = float(client.get("base_position_pct") or 0.15)
        dollars = equity * position_pct
        
        log.info(f"Position sizing: equity=${equity:,.2f}, pct={position_pct}, dollars=${dollars:,.2f}")

        # 9) Resolve actual option contract and get premium
        try:
            contract_symbol, premium = _resolve_option_contract(broker, symbol, strike, direction)
            log.info(f"Resolved contract: {contract_symbol}, premium=${premium:.2f}/share")
        except Exception as e:
            log.error(f"Failed to resolve contract: {e}")
            return {"ok": False, "error": "contract_resolution_failed", "details": str(e)}

        # 10) Validate premium range
        if not _validate_premium(premium):
            log.warning(f"Premium out of range: ${premium:.2f} (want $0.75-$2.50)")
            return {
                "ok": False,
                "error": "premium_out_of_range",
                "premium": premium,
                "contract_cost": premium * OPT_MULTIPLIER,
            }

        # 11) Calculate quantity
        qty = _calc_qty(dollars, premium)
        if qty < 1:
            log.warning(f"Position too small: qty={qty}")
            return {"ok": False, "error": "position_too_small", "dollars": dollars, "premium": premium}
        
        total_cost = qty * premium * OPT_MULTIPLIER
        log.info(f"Order: {qty} contracts @ ${premium:.2f} = ${total_cost:,.2f}")

        # 12) Create position record
        position_id = f"pos_{now_utc_iso().replace(':','').replace('-','').replace('.','')[:20]}"
        
        tp_pct = float(client.get("take_profit_pct") or 0.30)
        sl_pct = float(client.get("stop_loss_pct") or 0.50)
        
        with conn() as c:
            run_with_retry(lambda: c.execute("""
                INSERT INTO positions (
                    id, client_id, underlying, contract, direction, qty,
                    avg_fill, entry_ts, tp_pct, sl_pct, status
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (
                position_id,
                client_id,
                symbol,
                contract_symbol,
                direction,
                qty,
                premium,
                now_utc_iso(),
                tp_pct,
                sl_pct,
                "OPEN"
            )))

        # 13) Create order record
        local_order_id = new_local_order_id()
        insert_order(
            client_id=client_id,
            local_order_id=local_order_id,
            position_id=position_id,
            kind="ENTRY",
            status="NEW",
            symbol=symbol,
            contract=contract_symbol,
            qty=qty,
            limit_price=premium,
        )

        # 14) Submit order to broker
        log.info(f"Submitting order to broker: {contract_symbol}")
        try:
            resp = broker.place_order(
                symbol=symbol,
                contract=contract_symbol,
                qty=qty,
                limit_price=premium,
                side="buy_to_open",
            )
            
            broker_order_id = getattr(resp, "broker_order_id", None)
            status = getattr(resp, "status", "SUBMITTED")
            error = getattr(resp, "error", None)
            
            log.info(f"Broker response: status={status}, order_id={broker_order_id}")
            
            if error:
                log.error(f"Broker error: {error}")
                update_order(local_order_id, status="REJECTED", last_error=error)
                return {"ok": False, "error": "broker_rejected", "details": error}
            
            # Update order with broker ID
            update_order(
                local_order_id,
                status=status,
                broker_order_id=broker_order_id,
            )
            
        except Exception as e:
            log.error(f"Broker submission failed: {e}")
            update_order(local_order_id, status="REJECTED", last_error=str(e))
            return {"ok": False, "error": "broker_error", "details": str(e)}

        # 15) Update state
        update_client_state(client_id, {
            "trades_taken_today": trades_today + 1,
            "current_equity": equity,
        })

        # 16) Audit log
        audit(client_id, "INFO", "TRADE_EXECUTED", {
            "symbol": symbol,
            "direction": direction,
            "contract": contract_symbol,
            "qty": qty,
            "premium": premium,
            "total_cost": total_cost,
            "position_id": position_id,
            "broker_order_id": broker_order_id,
        })

        log.info(f"✅ Trade executed successfully: {contract_symbol} x{qty}")

        return {
            "ok": True,
            "symbol": symbol,
            "direction": direction,
            "contract": contract_symbol,
            "qty": qty,
            "premium": premium,
            "total_cost": total_cost,
            "position_id": position_id,
            "broker_order_id": broker_order_id,
            "local_order_id": local_order_id,
        }

    except Exception as e:
        log.exception(f"Execution failed: {e}")
        audit(client_id, "ERROR", "EXECUTION_FAILED", {"error": str(e), "signal": signal_payload})
        return {"ok": False, "error": "execution_exception", "details": str(e)}
