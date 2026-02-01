# ap/execution.py - ULTIMATE PRODUCTION VERSION (FIXED)
# =====================================================================
# ANGEL PRECISION BOT - TRADE EXECUTION ENGINE
#
# CRITICAL GUARANTEE: Positions are NEVER created until broker confirms fill.
# This file creates ORDERS. fill_monitor.py creates POSITIONS.
#
# Safety Features (Enterprise Grade):
# - Duplicate symbol prevention (atomic locks)
# - Equity reservation (prevents over-allocation)
# - Position size cap ($5K max per trade)
# - Daily loss stop (MAX_DAILY_LOSS_PCT kill switch)
# - Pre-trade validation (equity, risk limits, growth targets)
# - Contract resolution with PAPER/SIM fallback
# - Premium validation (prevents bad fills)
# - Order submission with retry logic
# - Comprehensive error handling
# - Full audit trail
# - Daily reset automation
# - Growth tracking integration
#
# IMPORTANT:
# - We reserve EXACT final total_cost (qty * premium * 100), not the "budget"
# - We keep reservation + symbol lock until fill_monitor releases them
# =====================================================================

from __future__ import annotations

import time
from datetime import datetime
from zoneinfo import ZoneInfo

from ap.logger import get_logger
from ap.config import Config
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
from ap.state import (
    reserve_equity_if_available,
    release_equity,
    acquire_symbol_lock,
    release_symbol_lock,
)

log = get_logger("ap.execution")
cfg = Config()

# =====================================================================
# CONSTANTS
# =====================================================================

OPT_MULTIPLIER = 100
NY = ZoneInfo("America/New_York")

# Premium validation bounds
MIN_PREMIUM_PER_SHARE = 0.50
MAX_PREMIUM_PER_SHARE = 2.50

# Retry configuration
MAX_BROKER_RETRIES = 3
BROKER_RETRY_DELAY = 1.0


# =====================================================================
# AUDIT LOGGING
# =====================================================================

def audit(client_id: str, level: str, event: str, payload: dict):
    """Log all critical events to audit table"""
    try:
        with conn() as c:
            run_with_retry(
                lambda: c.execute(
                    "INSERT INTO audit_log (ts, level, event, payload, client_id) VALUES (?,?,?,?,?)",
                    (now_utc_iso(), level, event, json_dumps(payload), client_id),
                )
            )
    except Exception as e:
        log.error(f"Audit logging failed: {e}")


# =====================================================================
# DAILY RESET
# =====================================================================

def _ny_day_key() -> str:
    """Get current date in NY timezone"""
    return datetime.now(NY).strftime("%Y-%m-%d")


def _maybe_reset_daily_state(broker, client_id: str, client_cfg: dict, st: dict) -> dict:
    """Reset daily counters at market open"""
    today = _ny_day_key()
    prev = st.get("day_key")

    equity = _get_equity_for_client(broker, st, client_cfg)

    if prev != today:
        log.info(f"📅 Daily reset for {client_id}: equity=${equity:,.2f}")

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

        audit(client_id, "INFO", "DAILY_RESET", {
            "date": today,
            "starting_equity": equity
        })
    else:
        update_client_state(client_id, {"current_equity": equity})
        st["current_equity"] = equity

    return st


# =====================================================================
# EQUITY LOOKUP
# =====================================================================

def _get_equity_for_client(broker, st: dict, client_cfg: dict) -> float:
    """Get current account equity with fallbacks"""
    try:
        equity = float(broker.get_account_equity())
        log.debug(f"💰 Equity from broker: ${equity:,.2f}")
        return equity
    except Exception as e:
        log.warning(f"⚠️  Failed to get broker equity: {e}")

        fallbacks = [
            ("current_equity", st.get("current_equity")),
            ("starting_equity_today", st.get("starting_equity_today")),
            ("initial_equity", client_cfg.get("initial_equity")),
            ("hard_default", 100000.0),
        ]

        for source, value in fallbacks:
            if value and float(value) > 0:
                equity = float(value)
                log.warning(f"Using equity from {source}: ${equity:,.2f}")
                return equity

        log.error("All equity fallbacks failed, using 100k default")
        return 100000.0


# =====================================================================
# POSITION COUNTING
# =====================================================================

def _count_open_positions(client_id: str) -> int:
    """Count currently open/closing positions"""
    with conn() as c:
        row = run_with_retry(
            lambda: c.execute(
                """
                SELECT COUNT(*) AS n
                FROM positions
                WHERE client_id=?
                  AND status IN ('OPEN','CLOSING')
                """,
                (client_id,),
            ).fetchone()
        )
        return int(row["n"] or 0)


def _today_trade_count(st: dict) -> int:
    """Get trades taken today"""
    return int(st.get("trades_taken_today") or 0)


# =====================================================================
# DAILY LOSS STOP CHECK (FIXED)
# =====================================================================

def _check_daily_loss_stop(client_id: str, st: dict) -> dict:
    """
    Check if daily LOSS exceeds threshold.

    FIX: only losses count (no abs()).
    """
    starting_equity = float(st.get("starting_equity_today") or 100000.0)
    realized_pnl = float(st.get("realized_pnl_today") or 0.0)

    loss = -min(0.0, realized_pnl)  # only negative pnl counts as loss
    loss_pct = loss / starting_equity if starting_equity > 0 else 0.0

    if loss_pct >= float(cfg.MAX_DAILY_LOSS_PCT):
        log.error(
            f"🛑 DAILY LOSS STOP HIT: {loss_pct*100:.1f}% "
            f"(loss=${loss:,.2f} | realized={realized_pnl:,.2f} | start=${starting_equity:,.2f})"
        )

        update_client_state(client_id, {
            "kill_switch": 1,
            "mode": "READ_ONLY",
            "daily_stop_hit": 1
        })

        audit(client_id, "CRITICAL", "DAILY_LOSS_STOP_HIT", {
            "loss_pct": loss_pct,
            "loss": loss,
            "realized_pnl_today": realized_pnl,
            "starting_equity_today": starting_equity,
            "threshold": float(cfg.MAX_DAILY_LOSS_PCT)
        })

        return {
            "ok": False,
            "error": "daily_loss_stop",
            "loss_pct": loss_pct,
            "loss": loss,
            "realized_pnl_today": realized_pnl,
        }

    return {"ok": True}


# =====================================================================
# GROWTH TRACKING
# =====================================================================

def _check_growth_limits(client_id: str, mode: str) -> dict:
    """Check growth targets"""
    if mode in ("PAPER", "SIM"):
        return {"ok": True}

    try:
        from ap.account_growth import check_growth_status, get_growth_metrics

        metrics = get_growth_metrics(client_id)
        if not metrics.get("ok"):
            log.debug(f"Growth tracking not initialized for {client_id}")
            return {"ok": True}

        status = check_growth_status(client_id)

        if status.get("should_stop"):
            reason = status.get("reason", "unknown")
            log.warning(f"🛑 Growth limit hit for {client_id}: {reason}")

            update_client_state(client_id, {"kill_switch": 1, "mode": "READ_ONLY"})
            audit(client_id, "WARNING", "GROWTH_TARGET_HIT", status)

            return {"ok": False, "error": f"growth_limit_{reason}", "details": status}

        return {"ok": True}

    except Exception as e:
        log.error(f"❌ Growth check failed: {e}")

        if mode == "LIVE":
            return {"ok": False, "error": "growth_check_failed", "details": str(e)}

        return {"ok": True}


# =====================================================================
# CONTRACT RESOLUTION
# =====================================================================

def _resolve_option_contract(
    broker,
    symbol: str,
    strike: float,
    direction: str,
    mode: str = "PAPER",
    exp_hint: str = "0DTE",
) -> tuple[str, float]:
    """Resolve contract symbol and premium"""
    direction = (direction or "").upper()
    if direction not in ("CALL", "PUT"):
        raise ValueError(f"Invalid direction: {direction}")

    expirations = broker.get_option_expirations(symbol)
    if not expirations:
        raise ValueError(f"No expirations available for {symbol}")

    expiration = pick_expiration(expirations, hint=exp_hint)
    log.debug(f"Selected expiration: {expiration}")

    chain = broker.get_option_chain(symbol, expiration)

    if not chain:
        if (mode or "").upper() in ("PAPER", "SIM"):
            contract_symbol = f"{symbol}_{expiration}_{int(strike)}_{direction}"
            premium = 1.00
            audit(client_id="default", level="WARNING", event="SYNTHETIC_CONTRACT", payload={
                "symbol": symbol, "expiration": expiration, "strike": strike,
                "direction": direction, "contract": contract_symbol, "premium": premium
            })
            return contract_symbol, premium
        raise ValueError(f"No options in chain for {symbol} {expiration}")

    contract_symbol = resolve_contract_symbol(chain, strike, direction)
    log.debug(f"Resolved contract: {contract_symbol}")

    from ap.contract_pricing import get_contract_price
    premium = float(get_contract_price(broker, contract_symbol, side="BUY"))

    if premium <= 0:
        if (mode or "").upper() in ("PAPER", "SIM"):
            log.warning(f"⚠️  Invalid premium {premium} for {contract_symbol}, using $1.00")
            premium = 1.00
        else:
            raise ValueError(f"Invalid premium for {contract_symbol}: {premium}")

    log.info(f"✅ Contract resolved: {contract_symbol} @ ${premium:.2f}/share")
    return contract_symbol, premium


# =====================================================================
# PREMIUM VALIDATION
# =====================================================================

def _validate_premium(premium: float, mode: str = "PAPER") -> tuple[bool, str]:
    """Validate premium is within acceptable range"""
    premium = float(premium)

    if (mode or "").upper() in ("PAPER", "SIM") and premium == 1.00:
        return True, ""

    if premium < MIN_PREMIUM_PER_SHARE:
        return False, f"Premium too low: ${premium:.2f} < ${MIN_PREMIUM_PER_SHARE:.2f}"

    if premium > MAX_PREMIUM_PER_SHARE:
        return False, f"Premium too high: ${premium:.2f} > ${MAX_PREMIUM_PER_SHARE:.2f}"

    return True, ""


# =====================================================================
# POSITION SIZING (FIXED: allow qty=0)
# =====================================================================

def _calc_qty(dollars: float, premium: float) -> int:
    """Calculate contract quantity (0 allowed so tiny budgets block)"""
    cost_per_contract = float(premium) * OPT_MULTIPLIER
    if cost_per_contract <= 0:
        return 0
    qty = int(float(dollars) // cost_per_contract)
    return max(0, qty)


# =====================================================================
# BROKER ORDER SUBMISSION (FIXED: dict or object response)
# =====================================================================

def _submit_order_with_retry(
    broker,
    symbol: str,
    contract: str,
    qty: int,
    premium: float,
) -> tuple[bool, str | None, str | None]:
    """Submit order with retry logic"""
    last_error = None

    for attempt in range(1, MAX_BROKER_RETRIES + 1):
        try:
            log.info(f"📤 Submitting order (attempt {attempt}/{MAX_BROKER_RETRIES}): {contract} x{qty}")

            resp = broker.place_order(
                symbol=symbol,
                contract=contract,
                qty=qty,
                limit_price=premium,
                side="buy_to_open",
            )

            if isinstance(resp, dict):
                broker_order_id = resp.get("broker_order_id") or resp.get("order_id") or resp.get("id")
                status = (resp.get("status") or "UNKNOWN")
                error = resp.get("error")
            else:
                broker_order_id = getattr(resp, "broker_order_id", None) or getattr(resp, "order_id", None)
                status = getattr(resp, "status", "UNKNOWN")
                error = getattr(resp, "error", None)

            log.info(f"📥 Broker response: status={status}, order_id={broker_order_id}")

            if error:
                log.error(f"❌ Broker rejected order: {error}")
                return False, broker_order_id, str(error)

            if str(status).upper() in ("ACK", "ACKED", "FILLED", "SUBMITTED", "OK", "ACCEPTED", "PENDING", "OPEN"):
                log.info(f"✅ Order submitted successfully: {broker_order_id}")
                return True, broker_order_id, None

            last_error = f"Unknown status: {status}"
            log.warning(f"⚠️  Unexpected broker status: {status}")

        except Exception as e:
            last_error = str(e)
            log.warning(f"⚠️  Broker submission attempt {attempt} failed: {e}")

            if attempt < MAX_BROKER_RETRIES:
                delay = BROKER_RETRY_DELAY * (2 ** (attempt - 1))
                log.info(f"⏳ Retrying in {delay}s...")
                time.sleep(delay)
                continue

    log.error(f"❌ Order submission failed after {MAX_BROKER_RETRIES} attempts")
    return False, None, last_error


# =====================================================================
# MAIN EXECUTION FUNCTION
# =====================================================================

def process_signal(broker, client_id: str, signal_payload: dict) -> dict:
    """
    Process trading signal and execute trade.

    FIXED order of operations:
    - acquire symbol lock
    - resolve contract & premium
    - validate premium
    - calc qty + cap => final total_cost
    - reserve EXACT total_cost
    - create order (store direction, reserved_cost)
    - re-check kill switch before submit
    - submit
    - keep lock+reserve until fill_monitor releases
    """
    execution_start = time.time()

    symbol = None
    symbol_locked = False
    equity_reserved = False
    reserved_cost = 0.0

    try:
        log.info(f"{'='*70}")
        log.info(f"🎯 EXECUTION START: {client_id}")
        log.info(f"Signal: {signal_payload.get('symbol')} {signal_payload.get('direction')}")
        log.info(f"{'='*70}")

        # STEP 1: VALIDATE CLIENT
        log.info("Step 1: Validating client...")
        try:
            client = get_client(client_id)
        except ValueError:
            log.error(f"❌ Client not found: {client_id}")
            return {"ok": False, "error": "client_not_found"}

        if (client.get("status") or "").upper() != "ACTIVE":
            return {"ok": False, "error": "client_inactive", "status": client.get("status")}

        # STEP 2: STATE & MODE
        log.info("Step 2: Checking state and mode...")
        st = get_client_state(client_id)
        mode = (st.get("mode") or "PAPER").upper()

        if st.get("kill_switch"):
            return {"ok": False, "error": "kill_switch_active"}
        if mode == "READ_ONLY":
            return {"ok": False, "error": "read_only_mode"}

        # STEP 3: DAILY RESET
        log.info("Step 3: Checking daily reset...")
        st = _maybe_reset_daily_state(broker, client_id, client, st)

        # STEP 4: DAILY LOSS STOP
        log.info("Step 4: Checking daily loss stop...")
        loss_check = _check_daily_loss_stop(client_id, st)
        if not loss_check.get("ok"):
            return loss_check

        # STEP 5: GROWTH LIMITS
        log.info("Step 5: Checking growth limits...")
        growth_check = _check_growth_limits(client_id, mode)
        if not growth_check.get("ok"):
            return growth_check

        # STEP 6: DAILY TRADE CAP
        log.info("Step 6: Checking daily trade cap...")
        max_trades = int(client.get("max_trades_per_day") or cfg.MAX_TRADES_PER_DAY)
        trades_today = _today_trade_count(st)
        if trades_today >= max_trades:
            return {"ok": False, "error": "daily_trade_cap", "trades_today": trades_today, "max_trades": max_trades}

        # STEP 7: MAX OPEN POSITIONS
        log.info("Step 7: Checking max open positions...")
        max_open = int(client.get("max_concurrent_positions") or cfg.MAX_CONCURRENT_POSITIONS)
        open_positions = _count_open_positions(client_id)
        if open_positions >= max_open:
            return {"ok": False, "error": "max_open_positions", "open_positions": open_positions, "max_open": max_open}

        # STEP 8: PARSE SIGNAL
        log.info("Step 8: Parsing signal...")
        symbol = (signal_payload.get("symbol") or "").strip().upper()
        direction = (signal_payload.get("direction") or "").strip().upper()
        trigger = signal_payload.get("trigger") or {}

        if not symbol:
            return {"ok": False, "error": "missing_symbol"}
        if direction not in ("CALL", "PUT"):
            return {"ok": False, "error": "invalid_direction", "direction": direction}

        strike = trigger.get("strike")
        if strike is None:
            return {"ok": False, "error": "missing_strike"}
        strike = float(strike)

        log.info(f"✅ Signal parsed: {symbol} {direction} ${strike}")

        # STEP 9: ACQUIRE SYMBOL LOCK
        log.info("Step 9: Acquiring symbol lock...")
        if not acquire_symbol_lock(client_id, symbol, ttl_seconds=90):
            audit(client_id, "WARNING", "SYMBOL_LOCKED", {"symbol": symbol, "direction": direction})
            return {"ok": False, "error": "symbol_locked", "symbol": symbol}
        symbol_locked = True

        # STEP 10: POSITION SIZING BUDGET
        log.info("Step 10: Calculating budget...")
        equity = _get_equity_for_client(broker, st, client)
        position_pct = float(client.get("base_position_pct") or cfg.BASE_POSITION_PCT)
        budget = min(equity * position_pct, float(cfg.MAX_POSITION_COST))

        # STEP 11: RESOLVE CONTRACT (BEFORE RESERVATION)
        log.info("Step 11: Resolving contract...")
        exp_hint = (signal_payload.get("exp_hint") or signal_payload.get("dte") or "0DTE")
        contract_symbol, premium = _resolve_option_contract(
            broker, symbol, strike, direction, mode=mode, exp_hint=str(exp_hint).upper()
        )

        # STEP 12: VALIDATE PREMIUM
        log.info("Step 12: Validating premium...")
        premium_valid, premium_error = _validate_premium(premium, mode)
        if not premium_valid:
            release_symbol_lock(client_id, symbol)
            symbol_locked = False
            return {"ok": False, "error": "premium_out_of_range", "details": premium_error, "premium": premium}

        # STEP 13: QUANTITY + FINAL CAP
        log.info("Step 13: Calculating qty...")
        qty = _calc_qty(budget, premium)
        if qty < 1:
            release_symbol_lock(client_id, symbol)
            symbol_locked = False
            return {"ok": False, "error": "position_too_small", "budget": budget, "premium": premium}

        total_cost = float(qty) * float(premium) * OPT_MULTIPLIER
        if total_cost > float(cfg.MAX_POSITION_COST):
            qty = int(float(cfg.MAX_POSITION_COST) // (float(premium) * OPT_MULTIPLIER))
            qty = max(1, qty)
            total_cost = float(qty) * float(premium) * OPT_MULTIPLIER

        log.info(f"📦 Qty: {qty} | Premium: ${premium:.2f} | Total: ${total_cost:,.2f}")

        # STEP 14: RESERVE EXACT COST (FIXED)
        log.info("Step 14: Reserving equity (exact)...")
        if not reserve_equity_if_available(client_id, total_cost, equity):
            release_symbol_lock(client_id, symbol)
            symbol_locked = False
            audit(client_id, "WARNING", "INSUFFICIENT_AVAILABLE_EQUITY", {
                "symbol": symbol, "requested_cost": total_cost, "equity": equity
            })
            return {"ok": False, "error": "insufficient_available_equity", "requested_cost": total_cost, "equity": equity}

        equity_reserved = True
        reserved_cost = float(total_cost)

        # STEP 15: CREATE ORDER RECORD (STORE direction + reserved_cost)
        log.info("Step 15: Creating order record...")
        local_order_id = new_local_order_id()

        insert_order(
            client_id=client_id,
            local_order_id=local_order_id,
            position_id=None,
            kind="ENTRY",
            status="NEW",
            symbol=symbol,
            contract=contract_symbol,
            qty=qty,
            limit_price=float(premium),
            direction=direction,          # requires ap/db.py insert_order update
            reserved_cost=reserved_cost,  # requires ap/db.py insert_order update
        )

        # STEP 16: PRE-SUBMIT KILL SWITCH RE-CHECK (FIXED)
        log.info("Step 16: Pre-submit kill-switch re-check...")
        st2 = get_client_state(client_id)
        if st2.get("kill_switch") or (st2.get("mode") or "").upper() == "READ_ONLY":
            update_order(local_order_id, status="CANCELED", last_error="killed_before_submit")
            if equity_reserved:
                release_equity(client_id, reserved_cost)
                equity_reserved = False
            if symbol_locked:
                release_symbol_lock(client_id, symbol)
                symbol_locked = False
            return {"ok": False, "error": "killed_before_submit"}

        # STEP 17: SUBMIT TO BROKER
        log.info("Step 17: Submitting to broker...")
        success, broker_order_id, error = _submit_order_with_retry(
            broker, symbol, contract_symbol, qty, float(premium)
        )

        if not success:
            update_order(local_order_id, status="REJECTED", broker_order_id=broker_order_id, last_error=error)
            if equity_reserved:
                release_equity(client_id, reserved_cost)
                equity_reserved = False
            if symbol_locked:
                release_symbol_lock(client_id, symbol)
                symbol_locked = False
            audit(client_id, "ERROR", "ORDER_REJECTED", {
                "symbol": symbol, "contract": contract_symbol, "qty": qty,
                "local_order_id": local_order_id, "broker_order_id": broker_order_id, "error": error
            })
            return {"ok": False, "error": "broker_rejected", "details": error, "local_order_id": local_order_id}

        # STEP 18: ACK ORDER
        update_order(local_order_id, status="ACK", broker_order_id=broker_order_id)

        # STEP 19: UPDATE COUNTERS
        update_client_state(client_id, {"trades_taken_today": trades_today + 1, "current_equity": equity})

        # STEP 20: AUDIT TRAIL
        audit(client_id, "INFO", "TRADE_EXECUTED", {
            "symbol": symbol,
            "direction": direction,
            "contract": contract_symbol,
            "qty": qty,
            "premium": float(premium),
            "total_cost": reserved_cost,
            "reserved_cost": reserved_cost,
            "local_order_id": local_order_id,
            "broker_order_id": broker_order_id,
            "execution_time_ms": int((time.time() - execution_start) * 1000)
        })

        return {
            "ok": True,
            "symbol": symbol,
            "direction": direction,
            "contract": contract_symbol,
            "qty": qty,
            "premium": float(premium),
            "total_cost": reserved_cost,
            "reserved_cost": reserved_cost,
            "local_order_id": local_order_id,
            "broker_order_id": broker_order_id,
            "status": "PENDING_FILL",
        }

    except Exception as e:
        log.exception(f"💥 EXECUTION EXCEPTION: {e}")

        if equity_reserved:
            try:
                release_equity(client_id, reserved_cost)
            except Exception:
                pass

        if symbol_locked and symbol:
            try:
                release_symbol_lock(client_id, symbol)
            except Exception:
                pass

        audit(client_id, "ERROR", "EXECUTION_FAILED", {
            "error": str(e),
            "signal": signal_payload,
        })

        return {"ok": False, "error": "execution_exception", "details": str(e)}

