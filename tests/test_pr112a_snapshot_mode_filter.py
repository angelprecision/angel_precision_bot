"""
tests/test_pr112a_snapshot_mode_filter.py
PR#112 amendment: snapshot() filters filled-unreconciled by execution mode.
Wrong-mode and null-mode orphan fills do not consume active slots.
"""
import re, sqlite3, pytest
from pathlib import Path

_REPO  = Path(__file__).resolve().parents[1]
PM_SRC = (_REPO / "ap" / "position_manager.py").read_text()


# ── Source checks ─────────────────────────────────────────────────────────────

def test_snapshot_accepts_mode_param():
    assert "def snapshot(self, *, mode" in PM_SRC

def test_slot_query_uses_mode_predicate():
    # _mode_predicate is a Python variable holding the SQL fragment
    idx = PM_SRC.find("calls_unreconciled")
    region = PM_SRC[idx - 600 : idx + 800]
    assert "_mode_predicate" in region, "slot block must use _mode_predicate"
    # The predicate value uses LOWER(COALESCE(execution_mode
    assert "LOWER(COALESCE(execution_mode" in PM_SRC, (
        "_mode_predicate must be defined as LOWER(COALESCE(execution_mode...) somewhere in pm"
    )

def test_capital_query_uses_mode_predicate():
    idx = PM_SRC.find("real_deployed_capital")
    region = PM_SRC[idx : idx + 1500]
    assert "_mode_predicate" in region

def test_orphan_wrong_mode_counter():
    assert "orphan_wrong_mode" in PM_SRC

def test_log_includes_null_and_wrong_mode():
    assert "SNAPSHOT_ORPHAN_FILLED_IGNORED" in PM_SRC
    assert "null_mode_count" in PM_SRC
    assert "wrong_mode_count" in PM_SRC
    assert "expected_mode" in PM_SRC


# ── SQL helpers ───────────────────────────────────────────────────────────────

def _sqls():
    return [m.group(1) for m in
            re.compile(r'c\.execute\(\s*[f]?\"{3}(.*?)\"{3}', re.DOTALL).finditer(PM_SRC)]

def _translate(sql, mode=None):
    # Replace {_mode_predicate} with a parameterized SQL fragment (%s → ?)
    if mode is not None:
        # Keep %s so the mode stays as a bind parameter
        sql = sql.replace("{_mode_predicate}",
                          "LOWER(COALESCE(execution_mode, '')) = LOWER(%s)")
    else:
        sql = sql.replace("{_mode_predicate}", "execution_mode IS NOT NULL")
    sql = re.sub(r'%s', '?', sql)
    sql = sql.replace('%%', '%')
    sql = re.sub(r'::[a-z_]+', '', sql)
    sql = re.sub(r'\\bNOW\\(\\)', 'CURRENT_TIMESTAMP', sql, flags=re.I)
    return sql

def _db(rows):
    db = sqlite3.connect(":memory:")
    db.execute("""
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY, client_id TEXT, kind TEXT,
            direction TEXT, status TEXT, execution_mode TEXT,
            filled_qty INTEGER DEFAULT 0, fill_price REAL,
            reserved_cost REAL, limit_price REAL, qty INTEGER DEFAULT 1,
            broker_order_id TEXT, submitted_ts TEXT, contract TEXT,
            position_id TEXT, created_ts TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    for row in rows:
        cols = ", ".join(row); ph = ", ".join(["?"] * len(row))
        db.execute(f"INSERT INTO orders ({cols}) VALUES ({ph})", list(row.values()))
    db.commit()
    return db

def _run_slot(db, client_id, mode=None):
    sql = next(s for s in _sqls() if "calls_unreconciled" in s and "FROM orders" in s)
    cur = db.cursor()
    # Build params to match production code:
    # (*_mode_params, _wrong_mode_val, *_mode_params, *_mode_params, client_id)
    # When mode is set: _mode_params=(mode,), appears 3 times + 1 wrong_mode_val + client_id = 4+1
    # When mode is None: _mode_params=(), just (wrong_mode_val="", client_id) = 2
    mode_val   = mode or ""
    translated = _translate(sql, mode)
    total_q    = translated.count("?")
    # params: 3 occurrences of _mode_predicate (n, calls, puts) + 1 wrong_mode_val + 1 client_id
    if mode:
        params = [mode, mode_val, mode, mode, client_id]  # n + wrong + calls + puts + client
    else:
        params = [mode_val, client_id]  # wrong_mode="" + client (no mode %s when IS NOT NULL)
    assert len(params) == total_q, f"params={len(params)} != q={total_q}: sql={translated[:200]}"
    cur.execute(translated, params)
    row = cur.fetchone()
    return dict(zip([d[0] for d in cur.description], row)) if row else {}

def _run_cap(db, client_id, mode=None):
    sql = next(
        s for s in _sqls()
        if ") AS cap" in s and "fill_price * filled_qty * 100" in s and "FROM orders" in s
    )
    cur = db.cursor()
    mode_val   = mode or ""
    translated = _translate(sql, mode)
    total_q    = translated.count("?")
    # capital query has 1 _mode_predicate occurrence + 1 client_id
    if mode:
        params = [client_id, mode]  # production: (self.client_id, *_mode_params)
    else:
        params = [client_id]
    assert len(params) == total_q, f"cap params={len(params)} != q={total_q}"
    cur.execute(translated, params)
    row = cur.fetchone()
    return dict(zip([d[0] for d in cur.description], row)) if row else {}


# ── AC1: 40 null-mode orphan fills → 0 slots (live client) ───────────────────

def test_ac1_null_mode_orphans_not_counted():
    CLIENT = "jasoncosby1@gmail.com"
    rows = [
        {"client_id": CLIENT, "kind": "ENTRY", "direction": "CALL",
         "status": "FILLED", "execution_mode": None,
         "filled_qty": 1, "fill_price": 2.50, "position_id": None}
        for _ in range(21)
    ] + [
        {"client_id": CLIENT, "kind": "ENTRY", "direction": "PUT",
         "status": "FILLED", "execution_mode": None,
         "filled_qty": 1, "fill_price": 0.61, "position_id": None}
        for _ in range(19)
    ]
    db = _db(rows)
    res = _run_slot(db, CLIENT, mode="live")
    assert res.get("n", 0) == 0, f"Expected 0 real slots, got {res.get('n')}"
    assert res.get("orphan_null_mode", 0) == 40
    assert res.get("orphan_wrong_mode", 0) == 0
    db.close()


# ── AC2: 1 live fill → 1 slot for live client ─────────────────────────────────

def test_ac2_live_fill_counts_for_live_client():
    CLIENT = "jasoncosby1@gmail.com"
    rows = [{"client_id": CLIENT, "kind": "ENTRY", "direction": "CALL",
             "status": "FILLED", "execution_mode": "live",
             "filled_qty": 1, "fill_price": 2.50, "position_id": None}]
    db  = _db(rows)
    res = _run_slot(db, CLIENT, mode="live")
    assert res.get("n", 0) == 1, f"Live fill must count for live client. got {res.get('n')}"
    assert res.get("orphan_null_mode",  0) == 0
    assert res.get("orphan_wrong_mode", 0) == 0
    db.close()


# ── AC3: 1 paper fill → 0 slots for live client (wrong-mode ignored) ──────────

def test_ac3_paper_fill_not_counted_for_live_client():
    CLIENT = "jasoncosby1@gmail.com"
    rows = [{"client_id": CLIENT, "kind": "ENTRY", "direction": "CALL",
             "status": "FILLED", "execution_mode": "paper",
             "filled_qty": 1, "fill_price": 2.50, "position_id": None}]
    db  = _db(rows)
    res = _run_slot(db, CLIENT, mode="live")
    assert res.get("n", 0) == 0, "Paper fill must not count for live client"
    assert res.get("orphan_wrong_mode", 0) == 1, "Paper fill must be counted as wrong-mode"
    db.close()


# ── AC4: 1 paper fill → 1 slot for paper client ───────────────────────────────

def test_ac4_paper_fill_counts_for_paper_client():
    CLIENT = "jose.vasquez4011@gmail.com"
    rows = [{"client_id": CLIENT, "kind": "ENTRY", "direction": "PUT",
             "status": "FILLED", "execution_mode": "paper",
             "filled_qty": 1, "fill_price": 0.61, "position_id": None}]
    db  = _db(rows)
    res = _run_slot(db, CLIENT, mode="paper")
    assert res.get("n", 0) == 1, "Paper fill must count for paper client"
    assert res.get("orphan_null_mode",  0) == 0
    assert res.get("orphan_wrong_mode", 0) == 0
    db.close()


# ── AC5: Capital query excludes null/wrong-mode rows ─────────────────────────

def test_ac5_capital_excludes_null_mode():
    CLIENT = "jasoncosby1@gmail.com"
    rows = [
        {"client_id": CLIENT, "kind": "ENTRY", "direction": "CALL",
         "status": "FILLED", "execution_mode": None,
         "filled_qty": 1, "fill_price": 2.50, "position_id": None},
        {"client_id": CLIENT, "kind": "ENTRY", "direction": "CALL",
         "status": "FILLED", "execution_mode": "live",
         "filled_qty": 1, "fill_price": 3.00, "position_id": None},
    ]
    db  = _db(rows)
    res = _run_cap(db, CLIENT, mode="live")
    # Only the live fill: 3.00 * 1 * 100 = 300
    assert abs(float(res.get("cap", 0)) - 300.0) < 0.01, (
        f"Capital must only count live fill ($300), not null-mode. got {res.get('cap')}"
    )
    db.close()

def test_ac5_capital_excludes_wrong_mode():
    CLIENT = "jasoncosby1@gmail.com"
    rows = [
        {"client_id": CLIENT, "kind": "ENTRY", "direction": "CALL",
         "status": "FILLED", "execution_mode": "paper",
         "filled_qty": 1, "fill_price": 2.50, "position_id": None},
    ]
    db  = _db(rows)
    res = _run_cap(db, CLIENT, mode="live")
    assert abs(float(res.get("cap", 0))) < 0.01, (
        "Paper fill must not contribute to live client capital"
    )
    db.close()
