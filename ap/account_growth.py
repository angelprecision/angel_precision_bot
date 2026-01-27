# ap/account_growth.py (SAFE VERSION - won't block unless initialized)
"""
Account growth tracker for fee-based bot rental.

SAFE BEHAVIOR (important for live/paper testing):
- If growth tracking is NOT initialized, we DO NOT block trades.
- If growth_tracking_enabled is falsey, we DO NOT block trades.
- No defaulting to $10k/$10k that can create bogus working_capital.

How to enable:
- Call init_fee_rental_account(...) for a client_id, which sets:
  growth_tracking_enabled=1, client_capital, rental_fee, targets, etc.

Your execution engine can call:
    from ap.account_growth import check_growth_status
    growth = check_growth_status(client_id)
    if growth["should_stop"]: stop trading
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Dict, Any

from ap.logger import get_logger
from ap.db import conn, run_with_retry, update_client_state

log = get_logger("ap.account_growth")


def _row_client_state(client_id: str) -> Optional[dict]:
    with conn() as c:
        row = run_with_retry(lambda: c.execute(
            "SELECT * FROM client_state WHERE client_id=?",
            (client_id,)
        ).fetchone())
    return dict(row) if row else None


def _parse_iso_dt_maybe(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        # Allow "Z"
        if s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        # If it already has time, parse directly
        if "T" in s:
            dt = datetime.fromisoformat(s)
            # Ensure tz-aware
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        # Date-only -> assume end of day UTC
        dt = datetime.fromisoformat(s + "T23:59:59")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def get_growth_metrics(client_id: str) -> dict:
    """
    Returns a metrics dict.

    SAFE RULE:
    - If not initialized, returns {"ok": False, "error": "growth_not_initialized", ...}
      (no exceptions)
    """
    state = _row_client_state(client_id)
    if not state:
        return {"ok": False, "error": "client_not_found", "client_id": client_id}

    enabled = int(state.get("growth_tracking_enabled") or 0)
    if not enabled:
        return {
            "ok": False,
            "error": "growth_tracking_disabled",
            "client_id": client_id,
        }

    client_capital = float(state.get("client_capital") or 0.0)
    rental_fee = float(state.get("rental_fee") or 0.0)
    working_capital = float(state.get("working_capital") or 0.0)

    # working_capital can be derived if missing
    if working_capital <= 0:
        working_capital = client_capital + rental_fee

    # Must be explicitly set
    if client_capital <= 0 or rental_fee <= 0 or working_capital <= 0:
        return {
            "ok": False,
            "error": "growth_not_initialized",
            "client_id": client_id,
            "hint": "Run init_fee_rental_account() to set client_capital, rental_fee, working_capital, and targets.",
        }

    current_balance = float(state.get("current_equity") or 0.0)
    if current_balance <= 0:
        # If equity not yet set, assume at least working_capital (neutral)
        current_balance = working_capital

    profit_target_min = float(state.get("profit_target_min") or 0.0)
    profit_target_max = float(state.get("profit_target_max") or 0.0)

    # Targets should be explicitly set; if missing, set conservative defaults ONCE (non-breaking)
    if profit_target_min <= 0:
        profit_target_min = rental_fee
    if profit_target_max <= 0:
        profit_target_max = rental_fee + 15000.0

    profit = current_balance - working_capital
    profit_pct = (profit / working_capital * 100.0) if working_capital > 0 else 0.0

    remaining_to_min = max(0.0, profit_target_min - profit)
    remaining_to_max = max(0.0, profit_target_max - profit)

    progress_to_max_pct = 0.0
    if profit_target_max > 0:
        progress_to_max_pct = min((profit / profit_target_max) * 100.0, 100.0)

    return {
        "ok": True,
        "client_id": client_id,
        "client_capital": client_capital,
        "rental_fee": rental_fee,
        "working_capital": working_capital,
        "current_balance": current_balance,
        "profit": profit,
        "profit_pct": profit_pct,
        "profit_target_min": profit_target_min,
        "profit_target_max": profit_target_max,
        "min_hit": profit >= profit_target_min,
        "max_hit": profit >= profit_target_max,
        "remaining_to_min": remaining_to_min,
        "remaining_to_max": remaining_to_max,
        "end_balance_if_min": working_capital + profit_target_min,
        "end_balance_if_max": working_capital + profit_target_max,
        "progress_to_max_pct": progress_to_max_pct,
        "subscription_end_date": state.get("subscription_end_date"),
        "account_type": state.get("account_type"),
    }


def check_growth_status(
    client_id: str,
    subscription_end_date: Optional[str] = None,
    stop_at_min: bool = False,
) -> dict:
    """
    SAFE RULE:
    - If not initialized or disabled -> should_stop=False (do not block trades)
    """
    metrics = get_growth_metrics(client_id)

    # Not blocking unless ok=True
    if not metrics.get("ok"):
        return {
            "should_stop": False,
            "reason": metrics.get("error") or "growth_not_ready",
            "message": "Growth tracking not initialized/disabled — allowing trades.",
            "metrics": metrics,
        }

    # Check min profit
    if stop_at_min and metrics["min_hit"]:
        return {
            "should_stop": True,
            "reason": "min_profit_hit",
            "message": f"Minimum profit hit: ${metrics['profit']:,.2f} / ${metrics['profit_target_min']:,.2f}",
            "metrics": metrics,
        }

    # Check max profit
    if metrics["max_hit"]:
        return {
            "should_stop": True,
            "reason": "max_profit_hit",
            "message": f"Maximum profit hit: ${metrics['profit']:,.2f} / ${metrics['profit_target_max']:,.2f}",
            "metrics": metrics,
        }

    # Subscription end date: prefer explicit arg, else state value
    end_str = subscription_end_date or (metrics.get("subscription_end_date") or "")
    if end_str:
        end_dt = _parse_iso_dt_maybe(end_str)
        if end_dt:
            now = datetime.now(timezone.utc)
            if now >= end_dt:
                return {
                    "should_stop": True,
                    "reason": "subscription_ended",
                    "message": f"Subscription ended on {end_str}",
                    "metrics": metrics,
                }
        else:
            log.warning(f"Could not parse subscription_end_date: {end_str}")

    # Still growing
    return {
        "should_stop": False,
        "reason": "still_growing",
        "message": f"Growing: ${metrics['current_balance']:,.2f} → Target: ${metrics['end_balance_if_max']:,.2f} "
                   f"(${metrics['remaining_to_max']:,.2f} remaining)",
        "metrics": metrics,
    }


def init_fee_rental_account(
    client_id: str,
    client_capital: float = 10000.0,
    rental_fee: float = 10000.0,
    profit_target_min: Optional[float] = None,
    profit_target_max: Optional[float] = None,
    subscription_end_date: Optional[str] = None,
) -> bool:
    """
    Enables growth tracking for a client by writing required fields to client_state.
    """
    try:
        client_capital = float(client_capital)
        rental_fee = float(rental_fee)

        if profit_target_min is None:
            profit_target_min = rental_fee
        if profit_target_max is None:
            profit_target_max = rental_fee + 15000.0

        working_capital = float(client_capital + rental_fee)

        patch = {
            "client_capital": float(client_capital),
            "rental_fee": float(rental_fee),
            "working_capital": float(working_capital),
            "profit_target_min": float(profit_target_min),
            "profit_target_max": float(profit_target_max),
            "subscription_end_date": subscription_end_date,
            "growth_tracking_enabled": 1,
            "account_type": "fee_rental",
            # initialize equity baseline if missing
            "current_equity": float(working_capital),
        }

        update_client_state(client_id, patch)
        log.info(
            f"Growth initialized: {client_id} "
            f"capital=${client_capital:,.2f} fee=${rental_fee:,.2f} working=${working_capital:,.2f} "
            f"target_max=${float(profit_target_max):,.2f}"
        )
        return True
    except Exception as e:
        log.error(f"init_fee_rental_account failed: {e}")
        return False


def disable_growth_tracking(client_id: str) -> bool:
    """
    Hard disable growth tracking (safe for paper/testing).
    """
    try:
        update_client_state(client_id, {"growth_tracking_enabled": 0})
        return True
    except Exception as e:
        log.error(f"disable_growth_tracking failed: {e}")
        return False


def get_progress_bar(client_id: str, width: int = 20) -> str:
    metrics = get_growth_metrics(client_id)
    if not metrics.get("ok"):
        return "Growth not initialized"

    progress = float(metrics.get("progress_to_max_pct") or 0.0) / 100.0
    filled = int(progress * width)
    bar = "█" * filled + "░" * (width - filled)
    return f"[{bar}] ${metrics['profit']:,.2f} / ${metrics['profit_target_max']:,.2f} ({metrics['progress_to_max_pct']:.1f}%)"


def format_growth_summary(client_id: str) -> str:
    metrics = get_growth_metrics(client_id)
    if not metrics.get("ok"):
        return "Growth tracking not initialized."

    status = check_growth_status(client_id)

    summary = f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
💰 FEE-RENTAL ACCOUNT PROGRESS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Your Capital:       ${metrics['client_capital']:>12,.2f}
Rental Fee (paid):  ${metrics['rental_fee']:>12,.2f}
Working Capital:    ${metrics['working_capital']:>12,.2f}
Current Balance:    ${metrics['current_balance']:>12,.2f}

📈 Profit:          ${metrics['profit']:>12,.2f} ({metrics['profit_pct']:>5.1f}%)

🎯 Profit Targets:
   Min (cover fee): ${metrics['profit_target_min']:>12,.2f} {'✓' if metrics['min_hit'] else ''}
   Max (+ bonus):   ${metrics['profit_target_max']:>12,.2f} {'✓' if metrics['max_hit'] else ''}

📊 Progress:        {get_progress_bar(client_id)}

Status:             {status['message']}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""".strip()
    return summary


def get_rental_summary(client_id: str) -> dict:
    metrics = get_growth_metrics(client_id)
    status = check_growth_status(client_id)

    if not metrics.get("ok"):
        return {"ok": False, "error": metrics.get("error"), "metrics": metrics}

    return {
        "ok": True,
        "client_id": client_id,
        "client_capital": metrics["client_capital"],
        "rental_fee": metrics["rental_fee"],
        "working_capital": metrics["working_capital"],
        "current_balance": metrics["current_balance"],
        "profit": metrics["profit"],
        "profit_pct": metrics["profit_pct"],
        "min_profit_target": metrics["profit_target_min"],
        "max_profit_target": metrics["profit_target_max"],
        "min_profit_hit": metrics["min_hit"],
        "max_profit_hit": metrics["max_hit"],
        "end_balance_if_min": metrics["end_balance_if_min"],
        "end_balance_if_max": metrics["end_balance_if_max"],
        "progress_pct": metrics["progress_to_max_pct"],
        "should_stop": status.get("should_stop"),
        "stop_reason": status.get("reason"),
        "message": status.get("message"),
    }

