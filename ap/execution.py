# ap/execution.py
# CLEAN + SAFE OPTION EXECUTION ENGINE
# Premium-based pricing (0.75–2.50 = $75–$250 per contract)

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

log = get_logger("ap.execution")

OPT_MULTIPLIER = 100
NY = ZoneInfo("America/New_York")


# =========================
# AUDIT
# =========================
def audit(client_id: str, level: str, event: str, payload: dict):
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "INSERT INTO audit_log (ts, level, event, payload, client_id) VALUES (?,?,?,?,?)",
            (now_utc_iso(), level, event, json_dumps(payload), client_id),
        ))


# =========================
# HELPERS
# =========================
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
    try:
        return float(broker.get_account_equity())
    except Exception:
        return float(
            st.get("current_equity")
            or st.get("starting_equity_today")
            or client_cfg.get("initial_equity")
            or 0.0
        )


# =========================
# OPTION PRICING (CORRECT)
# =========================
def _get_contract_premium(broker, contract: str) -> float:
    """
    Returns OPTION PREMIUM PER SHARE.
    Example: 1.25 == $125/contract
    """
    try:
        p = float(broker.get_contract_price(contract))

        # If broker returns contract dollars (150), convert → 1.50
        if p > 20:
            p = p / OPT_MULTIPLIER

        return p
    except Exception:
        return 1.25  # safe fallback


def _validate_premium(premium: float) -> bool:
    """
    Accept $75–$250 contracts
    => premium 0.75–2.50
    """
    return 0.75 <= float(premium) <= 2.50


def _calc_qty(dollars: float, premium: float) -> int:
    cost_per_contract = premium * OPT_MULTIPLIER
    if cost_per_contract <= 0:
        return 0
    return int(dollars // cost_per_contract)


# =========================
# DAILY RESET (SAFE)
# =========================
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
            "profit_cap_state": "normal",
            "starting_equity_today": equity,
            "current_equity": equity,
        }
        update_client_state(client_id, patch)
        st.update(patch)
    else:
        update_client_state(client_id, {"current_equity": equity})
        st["current_equity"] = equity

    return st


# =========================
# CORE EXECUTION
# =========================
def process_signal(broker, client_id: str, signal_payload: dict) -> dict:
    try:
        # 0) CLIENT
        client = get_client(client_id)
        if (client.get("status") or "").upper() != "ACTIVE":
            return {"ok": False, "error": "client_inactive"}

        # 1) STATE
        st = get_client_state(client_id)
        mode = (st.get("mode") or "PAPER").upper()

        if st.get("kill_switch") or mode == "READ_ONLY":
            return {"ok": False, "error": "bot_in_read_only"}

        st = _maybe_reset_daily_state(broker, client_id, client, st)

        # 2) GROWTH (SAFE)
        try:
            from ap.account_growth import check_growth_status

            if mode not in ("PAPER", "SIM"):
                g = check_growth_status(client_id)
                if g.get("should_stop"):
                    update_client_state(client_id, {"kill_switch": 1, "mode": "READ_ONLY"})
                    audit(client_id, "INFO", "GROWTH_STOP", g)
                    return {"ok": False, "error": "profit_target_hit"}

        except Exception as e:
            audit(client_id, "ERROR", "GROWTH_CHECK_FAILED", {"err": str(e)})
            if mode not in ("PAPER", "SIM"):
                return {"ok": False, "error": "growth_check_failed"}

        # 3) DAILY CAP
        if _today_trade_count(st) >= int(client.get("max_trades_per_day") or 25):
            return {"ok": False, "error": "daily_trade_cap"}

        # 4) MAX OPEN
        if _count_open_positions(client_id) >= int(client.get("max_concurrent_positions") or 2):
            return {"ok": False, "error": "max_open_positions"}

        # 5) SIGNAL
        symbol = (signal_payload.get("symbol") or "").upper()
        direction = (signal_payload.get("direction") or "").upper()
        trigger = signal_payload.get("trigger") or {}

        if direction not in ("CALL", "PUT"):
            return {"ok": False, "error": "invalid_direction"}

        strike = trigger.get("strike")
        if not strike:
            return {"ok": False, "error": "missing_strike"}

        contract = f"{symbol} {strike} {direction[0]}"

        # 6) EQUITY & SIZING
        equity = _get_equity_for_client(broker, st, client)
        dollars = equity * 0.15  # 15% risk

        # 7) OPTION PRICE
        premium = _get_contract_premium(broker, contract)

        if not _validate_premium(premium):
            return {
                "ok": False,
                "error": "contract_price_out_of_range",
                "premium": premium,
                "contract_cost": premium * OPT_MULTIPLIER,
            }

        qty = _calc_qty(dollars, premium)
        if qty < 1:
            return {"ok": False, "error": "position_too_small"}

        # 8) POSITION
        position_id = f"pos_{now_utc_iso().replace(':','').replace('-','')}"
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
                contract,
                direction,
                qty,
                premium,
                now_utc_iso(),
                0.23,
                0.15,
                "OPEN"
            )))

        # 9) ORDER
        local_order_id = new_local_order_id()
        insert_order(
            client_id=client_id,
            local_order_id=local_order_id,
            position_id=position_id,
            kind="ENTRY",
            status="NEW",
            symbol=symbol,
            contract=contract,
            qty=qty,
            limit_price=premium,
        )

        resp = broker.place_order(
            symbol=symbol,
            contract=contract,
            qty=qty,
            limit_price=premium,
            side="buy_to_open",
        )

        update_order(
            local_order_id,
            status=getattr(resp, "status", "SUBMITTED"),
            broker_order_id=getattr(resp, "broker_order_id", None),
        )

        update_client_state(client_id, {
            "trades_taken_today": _today_trade_count(st) + 1,
            "current_equity": equity,
        })

        audit(client_id, "INFO", "TRADE_EXECUTED", {
            "symbol": symbol,
            "direction": direction,
            "qty": qty,
            "premium": premium,
            "contract_cost": premium * OPT_MULTIPLIER,
        })

        return {
            "ok": True,
            "symbol": symbol,
            "direction": direction,
            "qty": qty,
            "premium": premium,
            "contract_cost": premium * OPT_MULTIPLIER,
            "position_id": position_id,
        }

    except Exception as e:
        log.error(f"EXECUTION_FAILED: {e}")
        audit(client_id, "ERROR", "EXECUTION_FAILED", {"err": str(e)})
        return {"ok": False, "error": "execution_failed"}
