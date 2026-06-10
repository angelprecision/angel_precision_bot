"""
tests/test_pr112_snapshot_execution_mode_filter.py
PR#112 regression: historical null-mode orphan fills must not consume active
position slots. execution_mode IS NOT NULL excludes legacy debt rows.
"""
import re, sqlite3, pytest
from pathlib import Path

_REPO   = Path(__file__).resolve().parents[1]
PM_SRC  = (_REPO / "ap" / "position_manager.py").read_text()

# ── Source checks ─────────────────────────────────────────────────────────────

def test_slot_query_filters_execution_mode_not_null():
    idx = PM_SRC.find("calls_unreconciled")
    region = PM_SRC[idx - 200 : idx + 600]
    assert "execution_mode IS NOT NULL" in region, (
        "Slot query must filter execution_mode IS NOT NULL"
    )

def test_capital_query_filters_execution_mode_not_null():
    # The real_deployed_capital comment precedes the c.execute block.
    # The SQL itself starts a few lines after the comment.
    idx = PM_SRC.find("real_deployed_capital")
    region = PM_SRC[idx : idx + 1500]
    assert "execution_mode IS NOT NULL" in region, (
        "Capital query must filter execution_mode IS NOT NULL"
    )

def test_orphan_ignored_log_present():
    assert "SNAPSHOT_ORPHAN_FILLED_IGNORED" in PM_SRC
    assert "execution_mode_null_not_current_mode" in PM_SRC
    assert "orphan_null_mode" in PM_SRC

# ── SQL translation helpers ───────────────────────────────────────────────────

def _get_execute_sqls(src):
    return [m.group(1)
            for m in re.compile(r'c\.execute\(\s*\"{3}(.*?)\"{3}', re.DOTALL).finditer(src)]

def _translate(sql):
    sql = re.sub(r'%s', '?', sql)
    sql = sql.replace('%%', '%')
    sql = re.sub(r'::[a-z_]+', '', sql)
    sql = re.sub(r'\\bNOW\\(\\)', 'CURRENT_TIMESTAMP', sql, flags=re.I)
    return sql

def _orders_db(rows):
    """Build a SQLite :memory: orders fixture from a list of row dicts."""
    db = sqlite3.connect(":memory:")
    db.execute("""
        CREATE TABLE orders (
            id              INTEGER PRIMARY KEY,
            client_id       TEXT,
            kind            TEXT,
            direction       TEXT,
            status          TEXT,
            execution_mode  TEXT,
            filled_qty      INTEGER DEFAULT 0,
            fill_price      REAL,
            reserved_cost   REAL,
            limit_price     REAL,
            qty             INTEGER DEFAULT 1,
            broker_order_id TEXT,
            submitted_ts    TEXT,
            contract        TEXT,
            position_id     TEXT,
            created_ts      TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    for row in rows:
        cols = ", ".join(row.keys())
        ph   = ", ".join(["?"] * len(row))
        db.execute(f"INSERT INTO orders ({cols}) VALUES ({ph})", list(row.values()))
    db.commit()
    return db

def _run_slot_query(db, client_id):
    sqls = _get_execute_sqls(PM_SRC)
    sql  = next(s for s in sqls if "calls_unreconciled" in s and "FROM orders" in s)
    translated = _translate(sql)
    cur  = db.cursor()
    cur.execute(translated, (client_id,))
    row = cur.fetchone()
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row)) if row else {}

def _run_cap_query(db, client_id):
    sqls = _get_execute_sqls(PM_SRC)
    # Capital query contains 'cap' alias and fill-truth cost expression
    sql = next(
        s for s in sqls
        if ") AS cap" in s and "fill_price * filled_qty * 100" in s and "FROM orders" in s
    )
    translated = _translate(sql)
    cur  = db.cursor()
    cur.execute(translated, (client_id,))
    row = cur.fetchone()
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row)) if row else {}

# ── AC1: 40 null-mode orphans → 0 real slots ─────────────────────────────────

def test_ac1_null_mode_orphans_excluded_from_slots():
    """40 historical null-mode FILLED orders must not consume any real slots."""
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
    db  = _orders_db(rows)
    res = _run_slot_query(db, CLIENT)
    assert res.get("n", 0) == 0, (
        f"Null-mode orphans must not consume slots. got n={res.get('n')}"
    )
    assert res.get("calls_unreconciled", 0) == 0
    assert res.get("puts_unreconciled",  0) == 0
    assert res.get("orphan_null_mode",   0) == 40, (
        f"Orphan count must be 40. got {res.get('orphan_null_mode')}"
    )
    db.close()

def test_ac1_null_mode_orphans_excluded_from_capital():
    """40 null-mode orphans must contribute $0 to pending_entry_capital."""
    CLIENT = "jasoncosby1@gmail.com"
    rows = [
        {"client_id": CLIENT, "kind": "ENTRY", "direction": "CALL",
         "status": "FILLED", "execution_mode": None,
         "filled_qty": 1, "fill_price": 2.50, "position_id": None}
        for _ in range(40)
    ]
    db  = _orders_db(rows)
    res = _run_cap_query(db, CLIENT)
    assert abs(float(res.get("cap", 0))) < 0.01, (
        f"Null-mode orphans must contribute $0 to capital. got {res.get('cap')}"
    )
    db.close()


# ── AC2: Current live unreconciled fill → 1 slot ─────────────────────────────

def test_ac2_live_unreconciled_fill_counts_as_slot():
    """A FILLED order with execution_mode='live' counts as 1 real slot."""
    CLIENT = "jasoncosby1@gmail.com"
    rows = [
        {"client_id": CLIENT, "kind": "ENTRY", "direction": "CALL",
         "status": "FILLED", "execution_mode": "live",
         "filled_qty": 1, "fill_price": 2.50, "position_id": None}
    ]
    db  = _orders_db(rows)
    res = _run_slot_query(db, CLIENT)
    assert res.get("n", 0) == 1, f"Live fill must count as 1 slot. got {res.get('n')}"
    assert res.get("calls_unreconciled", 0) == 1
    assert res.get("orphan_null_mode",   0) == 0
    db.close()


# ── AC3: Paper client fill → counted; live client doesn't see it ─────────────

def test_ac3_paper_fill_counted_for_paper_client():
    """execution_mode='paper' unreconciled fill must count for paper client."""
    CLIENT = "jose.vasquez4011@gmail.com"
    rows = [
        {"client_id": CLIENT, "kind": "ENTRY", "direction": "PUT",
         "status": "FILLED", "execution_mode": "paper",
         "filled_qty": 1, "fill_price": 0.61, "position_id": None}
    ]
    db  = _orders_db(rows)
    res = _run_slot_query(db, CLIENT)
    assert res.get("n", 0) == 1, "Paper fill must count for paper client"
    assert res.get("puts_unreconciled", 0) == 1
    db.close()

def test_ac3_null_mode_not_counted_regardless_of_client():
    """execution_mode IS NULL is excluded regardless of which client it is."""
    for client in ["jasoncosby1@gmail.com", "jose.vasquez4011@gmail.com"]:
        rows = [
            {"client_id": client, "kind": "ENTRY", "direction": "CALL",
             "status": "FILLED", "execution_mode": None,
             "filled_qty": 1, "fill_price": 2.50, "position_id": None}
        ]
        db  = _orders_db(rows)
        res = _run_slot_query(db, client)
        assert res.get("n", 0) == 0, (
            f"Null-mode fill must not count for {client}. got {res.get('n')}"
        )
        db.close()


# ── AC4: Mixed — orphans + one live fill → only live fill counted ─────────────

def test_ac4_mixed_orphans_and_live_fill():
    """40 null-mode + 1 live fill → pending_entries=1, orphan_null_mode=40."""
    CLIENT = "jasoncosby1@gmail.com"
    rows = [
        {"client_id": CLIENT, "kind": "ENTRY", "direction": "CALL",
         "status": "FILLED", "execution_mode": None,
         "filled_qty": 1, "fill_price": 2.50, "position_id": None}
        for _ in range(40)
    ] + [
        {"client_id": CLIENT, "kind": "ENTRY", "direction": "CALL",
         "status": "FILLED", "execution_mode": "live",
         "filled_qty": 1, "fill_price": 3.10, "position_id": None}
    ]
    db  = _orders_db(rows)
    res = _run_slot_query(db, CLIENT)
    assert res.get("n", 0) == 1,    f"Only 1 live fill should count. got {res.get('n')}"
    assert res.get("orphan_null_mode", 0) == 40
    db.close()


# ── AC5: Submitted unfilled rows → entry_attempt_lock only, no slot ───────────

def test_ac5_submitted_unfilled_is_lock_not_slot():
    """SUBMITTED + broker-confirmed + unfilled → lock only, not slot."""
    src = PM_SRC
    # entry_attempt_lock query does not filter execution_mode (it's for
    # duplicate-submit protection; unfilled rows don't block slot limits)
    assert "entry_attempt_lock_count" in src
    # The slot query (calls_unreconciled block) must not include SUBMITTED
    # without fill in the main count (it already requires fill-truth predicate)
    idx = src.find("calls_unreconciled")
    region = src[idx - 200 : idx + 600]
    # fill-truth predicate must still be present
    assert "COALESCE(filled_qty, 0) > 0" in region
    assert "fill_price IS NOT NULL" in region
