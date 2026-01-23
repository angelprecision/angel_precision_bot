# ap/state.py
from ap.db import conn, run_with_retry
from ap.utils import now_utc_iso, json_dumps, json_loads

DEFAULT_STATE = {
    "mode": "SIM",
    "kill_switch": False,
    "initial_equity_run": 10000.0,
    "starting_equity_today": 10000.0,
    "current_equity_last": 10000.0,
    "realized_pnl_today": 0.0,
    "trades_taken_today": 0,
    "daily_stop_hit": False,
    "profit_cap_state": "normal",  # normal | throttled | hard_stop
    "reserved_equity": 0.0,        # reserved for approved-but-not-filled entries
    "last_heartbeat_ts": None,
    "last_exit_ts_iso": None,
}

def load_state() -> dict:
    with conn() as c:
        rows = run_with_retry(lambda: c.execute("SELECT k, v FROM kv").fetchall())
        if not rows:
            bootstrap_state(DEFAULT_STATE)
            return dict(DEFAULT_STATE)

        st = dict(DEFAULT_STATE)
        for r in rows:
            st[r["k"]] = json_loads(r["v"])
        return st

def bootstrap_state(state: dict):
    ts = now_utc_iso()
    with conn() as c:
        for k, v in state.items():
            run_with_retry(lambda k=k, v=v: c.execute(
                "INSERT OR REPLACE INTO kv (k,v,updated_at) VALUES (?,?,?)",
                (k, json_dumps(v), ts),
            ))

def set_kv(key: str, value):
    with conn() as c:
        run_with_retry(lambda: c.execute(
            "INSERT OR REPLACE INTO kv (k,v,updated_at) VALUES (?,?,?)",
            (key, json_dumps(value), now_utc_iso()),
        ))

def update_state(patch: dict):
    ts = now_utc_iso()
    with conn() as c:
        for k, v in patch.items():
            run_with_retry(lambda k=k, v=v: c.execute(
                "INSERT OR REPLACE INTO kv (k,v,updated_at) VALUES (?,?,?)",
                (k, json_dumps(v), ts),
            ))

def reserve_equity(amount: float) -> bool:
    """
    Reserve equity by incrementing kv.reserved_equity.
    For MVP: always allow reservation.
    Implemented with retry and minimal lock time.
    """
    amount = float(amount)
    with conn() as c:
        # Read current value
        row = run_with_retry(lambda: c.execute(
            "SELECT v FROM kv WHERE k='reserved_equity'"
        ).fetchone())
        reserved = json_loads(row["v"]) if row else 0.0

        reserved_new = float(reserved) + amount

        run_with_retry(lambda: c.execute(
            "INSERT OR REPLACE INTO kv (k,v,updated_at) VALUES (?,?,?)",
            ("reserved_equity", json_dumps(reserved_new), now_utc_iso()),
        ))
    return True

def release_equity(amount: float):
    amount = float(amount)
    with conn() as c:
        row = run_with_retry(lambda: c.execute(
            "SELECT v FROM kv WHERE k='reserved_equity'"
        ).fetchone())
        reserved = json_loads(row["v"]) if row else 0.0

        reserved_new = max(0.0, float(reserved) - amount)

        run_with_retry(lambda: c.execute(
            "INSERT OR REPLACE INTO kv (k,v,updated_at) VALUES (?,?,?)",
            ("reserved_equity", json_dumps(reserved_new), now_utc_iso()),
        ))
