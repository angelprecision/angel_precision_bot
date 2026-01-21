from dataclasses import dataclass
from ap.db import conn
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
    "reserved_equity": 0.0,         # reserved for approved-but-not-filled entries
    "last_heartbeat_ts": None,
    "last_exit_ts_iso": None,
}

def load_state() -> dict:
    with conn() as c:
        rows = c.execute("SELECT k, v FROM kv").fetchall()
        if not rows:
            bootstrap_state(DEFAULT_STATE)
            return dict(DEFAULT_STATE)
        st = dict(DEFAULT_STATE)
        for r in rows:
            st[r["k"]] = json_loads(r["v"])
        return st

def bootstrap_state(state: dict):
    with conn() as c:
        ts = now_utc_iso()
        for k, v in state.items():
            c.execute(
                "INSERT OR REPLACE INTO kv (k,v,updated_at) VALUES (?,?,?)",
                (k, json_dumps(v), ts),
            )

def set_kv(key: str, value):
    with conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO kv (k,v,updated_at) VALUES (?,?,?)",
            (key, json_dumps(value), now_utc_iso()),
        )

def update_state(patch: dict):
    with conn() as c:
        ts = now_utc_iso()
        for k, v in patch.items():
            c.execute(
                "INSERT OR REPLACE INTO kv (k,v,updated_at) VALUES (?,?,?)",
                (k, json_dumps(v), ts),
            )

def reserve_equity(amount: float) -> bool:
    """
    Atomic reservation: reserved_equity += amount if possible.
    For MVP: allow reservation always; you can later cap by buying power.
    """
    with conn() as c:
        c.execute("BEGIN IMMEDIATE;")
        row = c.execute("SELECT v FROM kv WHERE k='reserved_equity'").fetchone()
        reserved = json_loads(row["v"]) if row else 0.0
        reserved_new = float(reserved) + float(amount)
        c.execute(
            "INSERT OR REPLACE INTO kv (k,v,updated_at) VALUES (?,?,?)",
            ("reserved_equity", json_dumps(reserved_new), now_utc_iso()),
        )
        c.execute("COMMIT;")
        return True

def release_equity(amount: float):
    with conn() as c:
        c.execute("BEGIN IMMEDIATE;")
        row = c.execute("SELECT v FROM kv WHERE k='reserved_equity'").fetchone()
        reserved = json_loads(row["v"]) if row else 0.0
        reserved_new = max(0.0, float(reserved) - float(amount))
        c.execute(
            "INSERT OR REPLACE INTO kv (k,v,updated_at) VALUES (?,?,?)",
            ("reserved_equity", json_dumps(reserved_new), now_utc_iso()),
        )
        c.execute("COMMIT;")

