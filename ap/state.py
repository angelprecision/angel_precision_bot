# ap/state.py
"""
Angel Precision Bot - state management (SQLite kv store)

Goals:
- Stable defaults + bootstrap
- Daily reset of counters (NY day)
- Safe, retryable SQLite writes
- Atomic reserve/release of reserved_equity (BEGIN IMMEDIATE)
"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional

from ap.db import conn, run_with_retry
from ap.utils import now_utc_iso, json_dumps, json_loads

NY = ZoneInfo("America/New_York")

# ---------------------------
# State schema (kv table)
# ---------------------------
DEFAULT_STATE: Dict[str, Any] = {
    # Mode should match the rest of your bot: "PAPER" | "LIVE"
    # (If you still use "SIM" elsewhere, treat it as PAPER in your checks.)
    "mode": "PAPER",

    # Global safety controls
    "kill_switch": False,

    # Equity snapshots / daily baselines
    "initial_equity_run": 10000.0,     # initial equity for this run (can be overridden at runtime)
    "starting_equity_today": 10000.0,  # equity baseline used for "growth" checks
    "current_equity_last": 10000.0,    # last seen equity

    # Daily counters / gates
    "realized_pnl_today": 0.0,
    "trades_taken_today": 0,
    "daily_stop_hit": False,
    "profit_cap_state": "normal",  # normal | throttled | hard_stop

    # Reserved funds (approved-but-not-filled entries)
    "reserved_equity": 0.0,

    # Heartbeats
    "last_heartbeat_ts": None,
    "last_exit_ts_iso": None,

    # Daily reset tracking (NY day)
    "day_key": None,              # e.g. "2026-01-27" (New York day)
    "last_day_reset_ts": None,    # ISO timestamp when reset occurred
}


# ---------------------------
# Internal helpers
# ---------------------------
def _day_key_now_ny() -> str:
    return datetime.now(NY).strftime("%Y-%m-%d")


def _read_all_kv() -> Dict[str, Any]:
    with conn() as c:
        rows = run_with_retry(lambda: c.execute("SELECT k, v FROM kv").fetchall())
        data: Dict[str, Any] = {}
        for r in rows:
            data[r["k"]] = json_loads(r["v"])
        return data


def _exists_any_kv() -> bool:
    with conn() as c:
        row = run_with_retry(lambda: c.execute("SELECT 1 AS one FROM kv LIMIT 1").fetchone())
        return bool(row)


# ---------------------------
# Public API
# ---------------------------
def bootstrap_state(state: Dict[str, Any]) -> None:
    """
    Writes all keys in 'state' into kv.
    Safe to call multiple times; overwrites existing keys.
    """
    ts = now_utc_iso()
    with conn() as c:
        for k, v in state.items():
            run_with_retry(lambda k=k, v=v: c.execute(
                "INSERT OR REPLACE INTO kv (k,v,updated_at) VALUES (?,?,?)",
                (k, json_dumps(v), ts),
            ))


def load_state() -> Dict[str, Any]:
    """
    Loads kv into a full state dict merged onto DEFAULT_STATE.
    Bootstraps DEFAULT_STATE if kv is empty.
    """
    if not _exists_any_kv():
        bootstrap_state(DEFAULT_STATE)
        return dict(DEFAULT_STATE)

    st = dict(DEFAULT_STATE)
    kv = _read_all_kv()
    st.update(kv)
    return st


def get_kv(key: str, default: Any = None) -> Any:
    with conn() as c:
        row = run_with_retry(lambda: c.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone())
        if not row:
            return default
        return json_loads(row["v"])


def set_kv(key: str, value: Any) -> None:
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "INSERT OR REPLACE INTO kv (k,v,updated_at) VALUES (?,?,?)",
            (key, json_dumps(value), now_utc_iso()),
        ))


def update_state(patch: Dict[str, Any]) -> None:
    """
    Bulk update multiple keys with the same updated_at.
    """
    ts = now_utc_iso()
    with conn() as c:
        for k, v in patch.items():
            run_with_retry(lambda k=k, v=v: c.execute(
                "INSERT OR REPLACE INTO kv (k,v,updated_at) VALUES (?,?,?)",
                (k, json_dumps(v), ts),
            ))


# ---------------------------
# Daily reset (NY)
# ---------------------------
def reset_daily_state(current_equity: Optional[float] = None) -> Dict[str, Any]:
    """
    Hard reset daily counters and gates.
    If current_equity is provided, we also reset daily baseline to it.
    Returns the updated state dict.
    """
    patch: Dict[str, Any] = {
        "day_key": _day_key_now_ny(),
        "last_day_reset_ts": now_utc_iso(),
        "realized_pnl_today": 0.0,
        "trades_taken_today": 0,
        "daily_stop_hit": False,
        "profit_cap_state": "normal",
        "reserved_equity": 0.0,  # clear stale reservations at day reset
    }
    if current_equity is not None:
        eq = float(current_equity)
        patch["starting_equity_today"] = eq
        patch["current_equity_last"] = eq

    update_state(patch)
    st = load_state()
    st.update(patch)
    return st


def maybe_reset_daily_state(current_equity: Optional[float] = None) -> Dict[str, Any]:
    """
    If NY day changed since last reset, resets daily counters.
    Call this before growth/risk checks (ideally right after you fetch equity).
    Returns the current (possibly updated) state dict.
    """
    st = load_state()
    today = _day_key_now_ny()
    prev = st.get("day_key")

    # If missing day_key, treat it as needing initialization
    if prev != today:
        return reset_daily_state(current_equity=current_equity)

    # If same day, just optionally refresh current_equity_last snapshot
    if current_equity is not None:
        update_state({"current_equity_last": float(current_equity)})
        st["current_equity_last"] = float(current_equity)

    return st


# ---------------------------
# Equity reservation (atomic)
# ---------------------------
def reserve_equity(amount: float) -> bool:
    """
    Atomically reserve equity by incrementing kv.reserved_equity.

    Uses BEGIN IMMEDIATE so two concurrent requests can't clobber the value.
    """
    amount = float(amount)
    if amount <= 0:
        return True

    def _txn():
        with conn() as c:
            c.execute("BEGIN IMMEDIATE")
            row = c.execute("SELECT v FROM kv WHERE k='reserved_equity'").fetchone()
            reserved = json_loads(row["v"]) if row else 0.0
            reserved_new = float(reserved) + amount
            c.execute(
                "INSERT OR REPLACE INTO kv (k,v,updated_at) VALUES (?,?,?)",
                ("reserved_equity", json_dumps(reserved_new), now_utc_iso()),
            )
            c.execute("COMMIT")

    run_with_retry(_txn)
    return True


def release_equity(amount: float) -> None:
    """
    Atomically release reserved equity by decrementing kv.reserved_equity.
    Floors at 0.
    """
    amount = float(amount)
    if amount <= 0:
        return

    def _txn():
        with conn() as c:
            c.execute("BEGIN IMMEDIATE")
            row = c.execute("SELECT v FROM kv WHERE k='reserved_equity'").fetchone()
            reserved = json_loads(row["v"]) if row else 0.0
            reserved_new = max(0.0, float(reserved) - amount)
            c.execute(
                "INSERT OR REPLACE INTO kv (k,v,updated_at) VALUES (?,?,?)",
                ("reserved_equity", json_dumps(reserved_new), now_utc_iso()),
            )
            c.execute("COMMIT")

    run_with_retry(_txn)


# ---------------------------
# Convenience helpers
# ---------------------------
def set_mode(mode: str) -> None:
    """
    mode: "PAPER" or "LIVE" (or "SIM" if you still use it)
    """
    set_kv("mode", str(mode).upper().strip())


def bump_heartbeat() -> None:
    update_state({"last_heartbeat_ts": now_utc_iso()})


def set_last_exit_ts(ts_iso: Optional[str]) -> None:
    update_state({"last_exit_ts_iso": ts_iso})


def add_realized_pnl(pnl: float) -> None:
    """
    Adds to realized PnL for today.
    """
    st = load_state()
    cur = float(st.get("realized_pnl_today") or 0.0)
    update_state({"realized_pnl_today": cur + float(pnl)})


def inc_trades_taken(n: int = 1) -> None:
    st = load_state()
    cur = int(st.get("trades_taken_today") or 0)
    update_state({"trades_taken_today": cur + int(n)})


def set_profit_cap_state(state: str) -> None:
    update_state({"profit_cap_state": str(state)})


def set_daily_stop(hit: bool = True) -> None:
    update_state({"daily_stop_hit": bool(hit)})


def snapshot_equity(equity: float) -> None:
    """
    Updates current_equity_last (does not change starting_equity_today).
    """
    update_state({"current_equity_last": float(equity)})
