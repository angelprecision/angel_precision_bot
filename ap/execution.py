# ap/execution.py - PRODUCTION (SIMPLE + SAFE)
# =====================================================================
# Creates ORDERS only. fill_monitor creates POSITIONS after true fills.
#
# Core Safety:
# - symbol lock (prevents duplicate symbol trades)
# - reserve exact cost (prevents over-allocation bursts)
# - daily loss stop (only stops on losses)
# - hard per-trade $ cap
# - pre-submit kill-switch recheck
#
# Signal Quality Gates (added):
# - Trend alignment: CALL blocked in BEAR, PUT blocked in BULL
# - Time gate: individual stocks blocked before 10:00 AM ET
#   (indices SPY/QQQ/IWM execute immediately — quick TP handled in exit)
#
# Debuggability:
# - minimal branching
# - consistent audit events
# - order row stores: direction + reserved_cost
#
# FIX: MAX_DAILY_LOSS_PCT → DAILY_MAX_LOSS_PCT (matches config.py)
# =====================================================================

from __future__ import annotations

import time
from datetime import datetime
from zoneinfo import ZoneInfo

import yfinance as yf

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

OPT_MULTIPLIER = 100
NY = ZoneInfo("America/New_York")

MIN_PREMIUM_PER_SHARE = 0.50
MAX_PREMIUM_PER_SHARE = 2.50

MAX_BROKER_RETRIES = 3
BROKER_RETRY_DELAY = 1.0

# Indices that execute immediately with no time gate
_INDICES = {"SPY", "QQQ", "IWM"}


def audit(client_id: str, level: str, event: str, payload: dict):
    try:
        with conn() as c:
            run_with_retry(lambda: c.execute(
                "INSERT INTO audit_log (ts, level, event, payload, client_id) VALUES (?,?,?,?,?)",
                (now_utc_iso(), level, event, json_dumps(payload), client_id),
            ))
    except Exception as e:
        log.error(f"Audit logging failed: {e}")


def _ny_day_key() -> str:
    return datetime.now(NY).strftime("%Y-%m-%d")


# ── SIGNAL QUALITY GATES ──────────────────────────────────────────────────────

def _get_spy_trend_live() -> str:
    try:
        df = yf.download("SPY", period="30d", interval="1d", progress=False, auto_adjust=True)
        if df is None or df.empty or len(df) < 20:
            return "UNKNOWN"
        if hasattr(df.columns, "get_level_values"):
            try:
                df.columns = df.columns.get_level_values(0)
            except Exception:
                pass
        closes = df["Close"].dropna().values.astype(float)
        if len(closes) < 20 or closes[-1] == 0:
            return "UNKNOWN"
        ma20 = float(closes[-20:].mean())
        return "BULL" if closes[-1] > ma20 else "BEAR"
    except Exception as e:
        log.warning(f"SPY trend fetch failed at execution: {e}")
        return "UNKNOWN"


def _trend_allows_execution(symbol: str, direction: str, spy_trend: str) -> tuple[bool, str]:
    if symbol.upper() in _INDICES:
        return True, ""
    if spy_trend == "UNKNOWN":
        return True, ""
    if spy_trend == "BEAR" and direction == "CALL":
        return False, f"trend_blocked:BEAR_market_no_CALL:{symbol}"
    if spy_trend == "BULL" and direction == "PUT":
        return False, f"trend_blocked:BULL_market_no_PUT:{symbol}"
    return True, ""


def _time_gate_allows_execution(symbol: str) -> tuple[bool, str]:
    if symbol.upper() in _INDICES:
        return True, ""

    now_et = datetime.now(NY)
    if now_et.hour > 10 or (now_et.hour == 10 and now_et.minute >= 0):
        return True, ""

    if now_et.hour == 9 and now_et.minute >= 30:
        return False, f"time_gate_blocked:first_30min:{symbol}:{now_et.strftime('%H:%M')}ET"

    return True, ""


# ── EXISTING HELPERS ──────────────────────────────────────────────────────────

def _get_equity_for_client(broker, st: dict, client_cfg: dict) -> float:
    try:
        return float(broker.get_account_equity())
    except Exception as e:
        log.warning(f"broker equity failed: {e}")
        for _, v in [
            ("current_equity", st.get("current_equity")),
            ("starting_equity_today", st.get("starting_equity_today")),
            ("initial_equity", client_cfg.get("initial_equity")),
            ("hard_default", 100000.0),
        ]:
            if v and float(v) > 0:
                return float(v)
        return 100000.0


def _maybe_reset_daily_state(broker, client_id: str, client_cfg: dict, st: dict) -> dict:
    today = _ny_day_key()
    prev = st.get("day_key")
    equity = _get_equity_for_client(broker, st, client_cfg)

    if prev != today:
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
        audit(client_id, "INFO", "DAILY_RESET", {"day_key": today, "starting_equity": equity})
    else:
        update_client_state(client_id, {"current_equity": equity})
        st["current_equity"] = equity

    return st


def _count_open_positions(client_id: str) -> int:
    with conn() as c:
        row = run_with_retry(lambda: c.execute(
            """
            SELECT COUNT(*) AS n
            FROM positions
            WHERE client_id=?
              AND status IN ('OPEN','CLOSING')
            """,
            (client_id,),
        ).fetchone())
        return int(row["n"] or 0)


def _check_daily_loss_stop(client_id: str, st: dict) -> dict:
    starting = float(st.get("starting_equity_today") or 0.0) or 100000.0
    realized = float(st.get("realized_pnl_today") or 0.0)

    loss = -min(0.0, realized)
    loss_pct = loss / starting if starting > 0 else 0.0

    # FIX: was cfg.MAX_DAILY_LOSS_PCT — renamed to DAILY_MAX_LOSS_PCT in config.py
    if loss_pct >= float(cfg.DAILY_MAX_LOSS_PCT):
        update_client_state(client_id, {"kill_switch": 1, "mode": "READ_ONLY", "daily_stop_hit": 1})
        audit(client_id, "CRITICAL", "DAILY_LOSS_STOP_HIT", {
            "loss_pct": loss_pct,
            "loss": loss,
            "realized_pnl_today": realized,
            "starting_equity_today": starting,
            "threshold": float(cfg.DAILY_MAX_LOSS_PCT),  # FIX: was MAX_DAILY_LOSS_PCT
        })
        return {"ok": False, "error": "daily_loss_stop", "loss_pct": loss_pct, "loss": loss}

    return {"ok": True}


def _validate_premium(premium: float, mode: str) -> tuple[bool, str]:
    premium = float(premium)
    if mode in ("PAPER", "SIM") and premium == 1.00:
        return True, ""
    if premium < MIN_PREMIUM_PER_SHARE:
        return False, f"premium_too_low:{premium:.2f}"
    if premium > MAX_PREMIUM_PER_SHARE:
        return False, f"premium_too_high:{premium:.2f}"
    return True, ""


def _calc_qty(max_cost: float, premium: float) -> int:
    cost_per_contract = float(premium) * OPT_MULTIPLIER
    if cost_per_contract <= 0:
        return 0
    return int(float(max_cost) // cost_per_contract)


def _resolve_option_contract(broker, client_id: str, symbol: str, strike: float, direction: str, mode: str, exp_hint: str) -> tuple[str, float]:
    direction = (direction or "").upper()
    if direction not in ("CALL", "PUT"):
        raise ValueError(f"invalid_direction:{direction}")

    expirations = broker.get_option_expirations(symbol)
    if not expirations:
        raise ValueError(f"no_expirations:{symbol}")

    expiration = pick_expiration(expirations, hint=exp_hint)
    chain = broker.get_option_chain(symbol, expiration)

    if not chain:
        if mode in ("PAPER", "SIM"):
            contract = f"{symbol}_{expiration}_{int(strike)}_{direction}"
            premium = 1.00
            audit(client_id, "WARNING", "SYNTHETIC_CONTRACT", {
                "symbol": symbol,
                "expiration": expiration,
                "strike": strike,
                "direction": direction,
                "contract": contract,
                "premium": premium,
                "mode": mode,
                "reason": "no_chain_data",
            })
            return contract, premium
        raise ValueError(f"no_chain:{symbol}:{expiration}")

    contract = resolve_contract_symbol(chain, strike, direction)

    from ap.contract_pricing import get_contract_price
    premium = float(get_contract_price(broker, contract, side="BUY"))

    if premium <= 0:
        if mode in ("PAPER", "SIM"):
            premium = 1.00
        else:
            raise ValueError(f"invalid_premium:{contract}:{premium}")

    return contract, premium


def _submit_order_with_retry(broker, symbol: str, contract: str, qty: int, premium: float) -> tuple[bool, str | None, str | None]:
    last_error = None

    for attempt in range(1, MAX_BROKER_RETRIES + 1):
        try:
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

            if error:
                return False, broker_order_id, str(error)

            if str(status).upper() in ("ACK", "ACKED", "FILLED", "SUBMITTED", "OK", "ACCEPTED", "PENDING", "OPEN"):
                return True, broker_order_id, None

            last_error = f"unexpected_status:{status}"

        except Exception as e:
            last_error = str(e)
            if attempt < MAX_BROKER_RETRIES:
                time.sleep(BROKER_RETRY_DELAY * (2 ** (attempt - 1)))
                continue

    return False, None, last_error


# ── MAIN ENTRY POINT ──────────────────────────────────────────────────────────

def process_signal(broker, client_id: str, signal_payload: dict) -> dict:
    t0 = time.time()

    symbol = None
    locked = False
    reserved = False
    reserved_cost = 0.0

    try:
        client = get_client(client_id)
        if (client.get("status") or "").upper() != "ACTIVE":
            return {"ok": False, "error": "client_inactive"}

        st = get_client_state(client_id)
        mode = (st.get("mode") or "PAPER").upper()
        if st.get("kill_switch"):
            return {"ok": False, "error": "kill_switch_active"}
        if mode == "READ_ONLY":
            return {"ok": False, "error": "read_only_mode"}

        st = _maybe_reset_daily_state(broker, client_id, client, st)

        loss_check = _check_daily_loss_stop(client_id, st)
        if not loss_check.get("ok"):
            return loss_check

        max_trades = int(client.get("max_trades_per_day") or cfg.MAX_TRADES_PER_DAY)
        trades_today = int(st.get("trades_taken_today") or 0)
        if trades_today >= max_trades:
            return {"ok": False, "error": "daily_trade_cap", "trades_today": trades_today, "max_trades": max_trades}

        max_open = int(client.get("max_concurrent_positions") or cfg.MAX_CONCURRENT_POSITIONS)
        open_positions = _count_open_positions(client_id)
        if open_positions >= max_open:
            return {"ok": False, "error": "max_open_positions", "open_positions": open_positions, "max_open": max_open}

        symbol = (signal_payload.get("symbol") or "").strip().upper()
        direction = (signal_payload.get("direction") or "").strip().upper()
        trigger = signal_payload.get("trigger") or {}
        strike = trigger.get("strike")

        if not symbol:
            return {"ok": False, "error": "missing_symbol"}
        if direction not in ("CALL", "PUT"):
            return {"ok": False, "error": "invalid_direction", "direction": direction}
        if strike is None:
            return {"ok": False, "error": "missing_strike"}

        strike = float(strike)

        time_ok, time_reason = _time_gate_allows_execution(symbol)
        if not time_ok:
            audit(client_id, "INFO", "TIME_GATE_BLOCKED", {
                "symbol": symbol, "direction": direction, "reason": time_reason,
            })
            log.info(f"⏳ Time gate blocked {symbol} {direction}: {time_reason}")
            return {"ok": False, "error": "time_gate", "details": time_reason}

        spy_trend = _get_spy_trend_live()
        trend_ok, trend_reason = _trend_allows_execution(symbol, direction, spy_trend)
        if not trend_ok:
            audit(client_id, "INFO", "TREND_GATE_BLOCKED", {
                "symbol": symbol, "direction": direction,
                "spy_trend": spy_trend, "reason": trend_reason,
            })
            log.info(f"⛔ Trend gate blocked {symbol} {direction}: {trend_reason} (SPY={spy_trend})")
            return {"ok": False, "error": "trend_gate", "details": trend_reason, "spy_trend": spy_trend}

        if not acquire_symbol_lock(client_id, symbol, ttl_seconds=90):
            audit(client_id, "WARNING", "SYMBOL_LOCKED", {"symbol": symbol})
            return {"ok": False, "error": "symbol_locked", "symbol": symbol}
        locked = True

        equity = _get_equity_for_client(broker, st, client)
        position_pct = float(client.get("base_position_pct") or cfg.BASE_POSITION_PCT)
        budget = min(equity * position_pct, float(cfg.MAX_POSITION_COST))

        exp_hint = (
            trigger.get("expiry_hint")
            or signal_payload.get("exp_hint")
            or signal_payload.get("dte")
            or "DAILY"
        ).strip().upper()
        contract, premium = _resolve_option_contract(
            broker, client_id, symbol, strike, direction, mode=mode, exp_hint=exp_hint
        )

        ok_p, prem_err = _validate_premium(premium, mode)
        if not ok_p:
            release_symbol_lock(client_id, symbol)
            locked = False
            return {"ok": False, "error": "premium_out_of_range", "details": prem_err, "premium": float(premium)}

        qty = _calc_qty(min(budget, float(cfg.MAX_POSITION_COST)), premium)
        if qty < 1:
            release_symbol_lock(client_id, symbol)
            locked = False
            return {"ok": False, "error": "position_too_small", "budget": float(budget), "premium": float(premium)}

        total_cost = float(qty) * float(premium) * OPT_MULTIPLIER
        if total_cost > float(cfg.MAX_POSITION_COST):
            qty = int(float(cfg.MAX_POSITION_COST) // (float(premium) * OPT_MULTIPLIER))
            qty = max(1, qty)
            total_cost = float(qty) * float(premium) * OPT_MULTIPLIER

        if not reserve_equity_if_available(client_id, total_cost, equity):
            release_symbol_lock(client_id, symbol)
            locked = False
            audit(client_id, "WARNING", "RESERVE_EQUITY_FAILED", {
                "symbol": symbol, "cost": float(total_cost), "equity": float(equity)
            })
            return {"ok": False, "error": "insufficient_available_equity",
                    "cost": float(total_cost), "equity": float(equity)}

        reserved = True
        reserved_cost = float(total_cost)

        local_order_id = new_local_order_id()
        insert_order(
            client_id=client_id,
            local_order_id=local_order_id,
            position_id=None,
            kind="ENTRY",
            status="NEW",
            symbol=symbol,
            contract=contract,
            direction=direction,
            qty=qty,
            limit_price=float(premium),
            reserved_cost=float(reserved_cost),
        )

        st2 = get_client_state(client_id)
        if st2.get("kill_switch") or (st2.get("mode") or "").upper() == "READ_ONLY":
            update_order(local_order_id, status="CANCELED", last_error="killed_before_submit")
            release_equity(client_id, reserved_cost)
            release_symbol_lock(client_id, symbol)
            return {"ok": False, "error": "killed_before_submit"}

        ok, broker_order_id, err = _submit_order_with_retry(broker, symbol, contract, qty, float(premium))
        if not ok:
            update_order(local_order_id, status="REJECTED", broker_order_id=broker_order_id, last_error=err)
            release_equity(client_id, reserved_cost)
            release_symbol_lock(client_id, symbol)
            audit(client_id, "ERROR", "ORDER_REJECTED", {
                "symbol": symbol, "contract": contract,
                "error": err, "local_order_id": local_order_id
            })
            return {"ok": False, "error": "broker_rejected", "details": err,
                    "local_order_id": local_order_id}

        update_order(local_order_id, status="ACK", broker_order_id=broker_order_id)
        update_client_state(client_id, {
            "trades_taken_today": trades_today + 1,
            "current_equity": float(equity)
        })

        audit(client_id, "INFO", "TRADE_EXECUTED", {
            "symbol": symbol, "direction": direction, "contract": contract,
            "qty": int(qty), "premium": float(premium),
            "reserved_cost": float(reserved_cost),
            "local_order_id": local_order_id,
            "broker_order_id": broker_order_id,
            "spy_trend": spy_trend,
            "ms": int((time.time() - t0) * 1000),
        })

        return {
            "ok": True,
            "symbol": symbol,
            "direction": direction,
            "contract": contract,
            "qty": int(qty),
            "premium": float(premium),
            "reserved_cost": float(reserved_cost),
            "local_order_id": local_order_id,
            "broker_order_id": broker_order_id,
            "status": "PENDING_FILL",
            "spy_trend": spy_trend,
        }

    except Exception as e:
        log.exception(f"EXECUTION_EXCEPTION: {e}")
        try:
            if reserved:
                release_equity(client_id, reserved_cost)
        except Exception:
            pass
        try:
            if locked and symbol:
                release_symbol_lock(client_id, symbol)
        except Exception:
            pass
        audit(client_id, "ERROR", "EXECUTION_FAILED", {
            "error": str(e), "symbol": symbol, "signal": signal_payload
        })
        return {"ok": False, "error": "execution_exception", "details": str(e)}
