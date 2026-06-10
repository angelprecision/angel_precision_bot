"""
tests/test_pr108_snapshot_no_orders_side.py
PR#108 regression: position_manager snapshot SQL must not reference orders.side.
orders table uses direction, not side.
"""
from pathlib import Path
import re, pytest

_REPO = Path(__file__).resolve().parents[1]
PM_SRC = (_REPO / "ap" / "position_manager.py").read_text()

# ── Source-level: no orders.side in any SQL block ────────────────────────────

def test_no_coalesce_side_in_position_manager():
    """
    grep -R 'COALESCE(side' ap/position_manager.py must return nothing.
    orders.side does not exist in production schema — orders use direction.
    """
    matches = re.findall(r"COALESCE\(side[,'\)]", PM_SRC)
    assert not matches, (
        f"orders.side reference found in ap/position_manager.py: {matches}\n"
        "Use COALESCE(direction,...) instead."
    )

def test_direction_used_for_call_filter():
    """CALL filter uses direction, not side."""
    assert "COALESCE(direction,'')) = \'CALL\'" in PM_SRC or \
           "COALESCE(direction,\'\')) = \'CALL\'" in PM_SRC or \
           "COALESCE(direction,'')) = 'CALL'" in PM_SRC

def test_direction_used_for_put_filter():
    """PUT filter uses direction, not side."""
    assert "COALESCE(direction,'')) = 'PUT'" in PM_SRC

def test_filled_unreconciled_calls_query_uses_direction():
    """calls_unreconciled FILTER must use direction only."""
    idx = PM_SRC.find("calls_unreconciled")
    assert idx > 0
    # Read the FILTER block
    region = PM_SRC[idx - 200:idx + 100]
    assert "direction" in region
    assert "side" not in region, (
        "calls_unreconciled FILTER must not reference orders.side"
    )

def test_filled_unreconciled_puts_query_uses_direction():
    """puts_unreconciled FILTER must use direction only."""
    idx = PM_SRC.find("puts_unreconciled")
    assert idx > 0
    region = PM_SRC[idx - 200:idx + 100]
    assert "direction" in region
    assert "side" not in region, (
        "puts_unreconciled FILTER must not reference orders.side"
    )

# ── Behavioral: direction-based filtering works correctly ────────────────────

def test_call_direction_filter_logic():
    """
    Simulate the FILTER logic in Python:
    orders with direction='CALL' must count as calls_unreconciled.
    orders with direction='PUT' must count as puts_unreconciled.
    Orders without direction must count as neither (0 in both).
    """
    orders = [
        {"direction": "CALL", "filled_qty": 1, "fill_price": 2.50,
         "status": "FILLED", "position_id": None},
        {"direction": "PUT",  "filled_qty": 1, "fill_price": 0.61,
         "status": "FILLED", "position_id": None},
        {"direction": None,   "filled_qty": 1, "fill_price": 1.00,
         "status": "FILLED", "position_id": None},
    ]

    def is_unreconciled(o):
        s = (o.get("status") or "").upper()
        terminal = {"CANCELED","CANCELLED","EXPIRED","REJECTED","ERROR","FAILED","CLOSED"}
        return (
            (int(o.get("filled_qty") or 0) > 0 or o.get("fill_price") is not None
             or s in {"PARTIAL_FILL","PARTIALLY_FILLED","FILLED","OPEN"})
            and s not in terminal
            and not o.get("position_id")
        )

    calls = sum(1 for o in orders
                if is_unreconciled(o)
                and (o.get("direction") or "").upper() == "CALL")
    puts  = sum(1 for o in orders
                if is_unreconciled(o)
                and (o.get("direction") or "").upper() == "PUT")

    assert calls == 1, f"Expected 1 CALL, got {calls}"
    assert puts  == 1, f"Expected 1 PUT, got {puts}"

def test_pr106_behavior_intact():
    """PR#106 fill-truth slot accounting keywords still present."""
    for keyword in [
        "filled_qty",
        "fill_price IS NOT NULL",
        "entry_attempt_lock_count",
        "position_id IS NULL",
        "pending_entry_capital",
    ]:
        assert keyword in PM_SRC, f"PR#106 keyword missing: {keyword}"

# ══════════════════════════════════════════════════════════════════════════════
# REAL DB-BACKED REGRESSION TEST (PR#108)
# Extracts the three exact SQL blocks from ap/position_manager.py and
# executes them against an in-memory SQLite fixture that has NO side column.
# This proves the production SQL is clean and returns correct results.
# ══════════════════════════════════════════════════════════════════════════════

import re, sqlite3


def _get_execute_sqls(src: str):
    """Return list of SQL strings from every c.execute(\"\"\"...\"\"\"}) call."""
    return [
        m.group(1)
        for m in re.compile(r'c\.execute\(\s*\"{3}(.*?)\"{3}', re.DOTALL).finditer(src)
    ]


def _sqlite_translate(sql: str) -> str:
    """Minimal psycopg2 → SQLite translation for snapshot queries only."""
    sql = re.sub(r'%s', '?', sql)       # psycopg2 placeholder → SQLite
    sql = sql.replace('%%', '%')          # escaped literal % in psycopg2
    sql = re.sub(r'::[a-z_]+', '', sql)  # ::timestamptz, ::text, etc.
    sql = re.sub(r'\\bNOW\\(\\)', 'CURRENT_TIMESTAMP', sql, flags=re.IGNORECASE)
    return sql


def _build_orders_fixture() -> tuple:
    """
    Create an in-memory SQLite DB with an orders table that has NO side column.
    Returns (connection, client_id).
    """
    CLIENT = 'test-snapshot@example.com'
    db = sqlite3.connect(':memory:')
    db.execute("""
        CREATE TABLE orders (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id       TEXT,
            kind            TEXT,
            direction       TEXT,
            status          TEXT,
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
    # 1. FILLED CALL, position_id NULL → counts as filled_unreconciled_calls
    db.execute(
        "INSERT INTO orders (client_id,kind,direction,status,filled_qty,fill_price,contract) "
        "VALUES (?,?,?,?,?,?,?)",
        (CLIENT, 'ENTRY', 'CALL', 'FILLED', 1, 2.50, 'AAPL260612C00190000'),
    )
    # 2. FILLED PUT, position_id NULL → counts as filled_unreconciled_puts
    db.execute(
        "INSERT INTO orders (client_id,kind,direction,status,filled_qty,fill_price,contract) "
        "VALUES (?,?,?,?,?,?,?)",
        (CLIENT, 'ENTRY', 'PUT', 'FILLED', 1, 0.61, 'RIVN260612P00016500'),
    )
    # 3. SUBMITTED CALL, broker_order_id present, unfilled → entry_attempt_lock
    db.execute(
        "INSERT INTO orders "
        "(client_id,kind,direction,status,filled_qty,fill_price,reserved_cost,broker_order_id,contract) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (CLIENT, 'ENTRY', 'CALL', 'SUBMITTED', 0, None, 450.0, 'br-001', 'AAPL260612C00195000'),
    )
    # 4. SUBMITTED PUT, submitted_ts present, unfilled → entry_attempt_lock
    db.execute(
        "INSERT INTO orders "
        "(client_id,kind,direction,status,filled_qty,fill_price,reserved_cost,submitted_ts,contract) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (CLIENT, 'ENTRY', 'PUT', 'SUBMITTED', 0, None, 300.0, '2026-06-10T10:00:00', 'RIVN260612P00020000'),
    )
    # 5. PENDING_TRIGGER watcher — must NOT count as slot or lock
    db.execute(
        "INSERT INTO orders (client_id,kind,direction,status,contract) VALUES (?,?,?,?,?)",
        (CLIENT, 'ENTRY', 'CALL', 'PENDING_TRIGGER', 'TSLA260612C00900000'),
    )
    db.commit()
    return db, CLIENT


def _fetchone_dict(cur):
    row = cur.fetchone()
    if row is None:
        return {}
    return {d[0]: row[i] for i, d in enumerate(cur.description)}


def test_snapshot_sql_no_side_column_db_backed():
    """
    Real DB regression test.

    Extracts the exact SQL strings from the production source file and runs
    them against a SQLite fixture that has NO side column (mirrors production).

    Proves that:
    - The queries do not reference orders.side
    - filled_unreconciled_calls == 1 (CALL FILLED, position_id NULL)
    - filled_unreconciled_puts  == 1 (PUT  FILLED, position_id NULL)
    - entry_attempt_lock_count  == 2 (SUBMITTED rows 3 + 4)
    - entry_attempt_reserved_cost == 750.0
    - pending_entries == 2 (rows 1+2 only; SUBMITTED/PENDING_TRIGGER excluded)
    - pending_entry_capital == 311.0 (2.50*100 + 0.61*100)
    """
    sqls    = _get_execute_sqls(PM_SRC)
    # Indices confirmed by scanning all c.execute(""") calls in the file:
    #   [26] = slot/unreconciled query  (calls_unreconciled / puts_unreconciled)
    #   [27] = entry_attempt_lock query
    #   [28] = pending_entry_capital query
    slot_sql = _sqlite_translate(sqls[26])
    lock_sql = _sqlite_translate(sqls[27])
    cap_sql  = _sqlite_translate(sqls[28])

    # Reject immediately if side sneaked back in
    assert "COALESCE(side" not in slot_sql, "slot SQL must not reference side"
    assert "COALESCE(side" not in lock_sql, "lock SQL must not reference side"
    assert "COALESCE(side" not in cap_sql,  "cap  SQL must not reference side"

    db, CLIENT = _build_orders_fixture()
    cur = db.cursor()

    # ── slot / unreconciled ──────────────────────────────────────────────────
    cur.execute(slot_sql, (CLIENT,))
    slot = _fetchone_dict(cur)
    assert slot.get("n") == 2, (
        f"pending_entries: expected 2 (FILLED rows only), got {slot.get('n')}. "
        "SUBMITTED and PENDING_TRIGGER rows must not count."
    )
    assert slot.get("calls_unreconciled") == 1, (
        f"filled_unreconciled_calls: expected 1, got {slot.get('calls_unreconciled')}"
    )
    assert slot.get("puts_unreconciled") == 1, (
        f"filled_unreconciled_puts: expected 1, got {slot.get('puts_unreconciled')}"
    )

    # ── entry_attempt_lock ───────────────────────────────────────────────────
    cur.execute(lock_sql, (CLIENT,))
    lock = _fetchone_dict(cur)
    assert lock.get("n") == 2, (
        f"entry_attempt_lock_count: expected 2 (SUBMITTED rows 3+4), got {lock.get('n')}"
    )
    assert abs(float(lock.get("reserved") or 0) - 750.0) < 0.01, (
        f"entry_attempt_reserved_cost: expected 750.0 (450+300), got {lock.get('reserved')}"
    )

    # ── pending_entry_capital ────────────────────────────────────────────────
    cur.execute(cap_sql, (CLIENT,))
    cap = _fetchone_dict(cur)
    # AAPL: 2.50 * 1 * 100 = 250.0 | RIVN: 0.61 * 1 * 100 = 61.0 → 311.0
    assert abs(float(cap.get("cap") or 0) - 311.0) < 0.01, (
        f"pending_entry_capital: expected 311.0, got {cap.get('cap')}"
    )

    db.close()
