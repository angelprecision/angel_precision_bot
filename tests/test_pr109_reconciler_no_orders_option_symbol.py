"""
tests/test_pr109_reconciler_no_orders_option_symbol.py
PR#109 regression: _backfill_missing_position_links must not SELECT
option_symbol directly from orders (column does not exist on orders table).
"""
import re
import sqlite3
import pytest
from pathlib import Path

_REPO   = Path(__file__).resolve().parents[1]
REC_SRC = (_REPO / "ap_reconciler.py").read_text()


# ── Source-level: alias used, not bare column ─────────────────────────────────

def test_fetch_select_uses_alias_not_bare_column():
    """
    The _fetch() SELECT must use  contract AS option_symbol,
    not  option_symbol  as a bare column name.
    """
    idx_fetch = REC_SRC.find("def _fetch():")
    idx_end   = REC_SRC.find("rows = c.fetchall()", idx_fetch)
    block     = REC_SRC[idx_fetch:idx_end]
    assert "contract AS option_symbol" in block, (
        "_fetch SELECT must use 'contract AS option_symbol', not bare option_symbol"
    )

def test_no_bare_option_symbol_in_fetch_select():
    """The alias target (option_symbol) must come from 'contract AS', not directly."""
    idx_fetch = REC_SRC.find("def _fetch():")
    idx_end   = REC_SRC.find("rows = c.fetchall()", idx_fetch)
    block     = REC_SRC[idx_fetch:idx_end]
    # Find position of the alias declaration
    alias_pos = block.find("contract AS option_symbol")
    assert alias_pos >= 0
    # Everything before the alias line must not contain a bare "option_symbol," column ref
    # (a standalone column name on its own line/position before the alias)
    before = block[:alias_pos]
    assert "option_symbol," not in before, (
        "Bare 'option_symbol,' column reference found before the AS alias in _fetch"
    )


def test_positions_option_symbol_references_untouched():
    """positions.option_symbol INSERT/SELECT references must still exist."""
    idx_ins   = REC_SRC.find("INSERT INTO positions")
    assert idx_ins > 0
    ins_block = REC_SRC[idx_ins:idx_ins + 600]
    assert "option_symbol" in ins_block, (
        "INSERT INTO positions must still include option_symbol column"
    )


# ── DB-backed: _fetch query runs without UndefinedColumn ─────────────────────

def _get_execute_sqls(src: str):
    return [
        m.group(1)
        for m in re.compile(r'c\.execute\(\s*\"{3}(.*?)\"{3}', re.DOTALL).finditer(src)
    ]


def _sqlite_translate(sql: str) -> str:
    sql = re.sub(r'%s', '?', sql)
    sql = sql.replace('%%', '%')
    sql = re.sub(r'::[a-z_]+', '', sql)
    sql = re.sub(r'\\bNOW\\(\\)', 'CURRENT_TIMESTAMP', sql, flags=re.IGNORECASE)
    return sql


def test_fetch_query_runs_against_orders_without_option_symbol_column():
    """
    DB-backed regression.
    Creates an orders fixture WITHOUT option_symbol.
    Extracts the _fetch SELECT from the source and runs it against SQLite.
    Asserts:
      - query does not raise (OperationalError: no such column: option_symbol)
      - returned row has option_symbol key (from alias)
      - returned option_symbol == contract value
    """
    sqls = _get_execute_sqls(REC_SRC)

    fetch_sql = None
    for sql in sqls:
        if "execution_mode" in sql and "status = 'FILLED'" in sql and "FROM orders" in sql:
            fetch_sql = sql
            break
    assert fetch_sql is not None, "Could not locate _fetch SELECT in reconciler source"
    assert "contract AS option_symbol" in fetch_sql, (
        f"_fetch SELECT must use contract AS option_symbol:\n{fetch_sql}"
    )

    translated = _sqlite_translate(fetch_sql)

    CLIENT = "test-reconciler@example.com"
    db     = sqlite3.connect(":memory:")
    db.execute("""
        CREATE TABLE orders (
            id             INTEGER PRIMARY KEY,
            client_id      TEXT,
            kind           TEXT,
            status         TEXT,
            contract       TEXT,
            symbol         TEXT,
            direction      TEXT,
            filled_qty     INTEGER DEFAULT 0,
            fill_price     REAL,
            filled_ts      TEXT,
            execution_mode TEXT,
            broker_order_id TEXT,
            local_order_id  TEXT,
            position_id    TEXT
        )
    """)
    db.execute(
        "INSERT INTO orders "
        "(client_id,kind,status,contract,symbol,direction,"
        "filled_qty,fill_price,execution_mode,local_order_id) "
        "VALUES (?,?,?,?,?,?,?,?,?,?)",
        (CLIENT, "ENTRY", "FILLED", "RIVN260612P00016500",
         "RIVN260612P00016500", "PUT", 1, 0.61, "live", "ord-001"),
    )
    db.commit()

    cur = db.cursor()
    try:
        cur.execute(translated, (CLIENT,))
    except Exception as e:
        pytest.fail(
            f"_fetch SELECT raised on orders fixture without option_symbol column: "
            f"{type(e).__name__}: {e}"
        )

    rows = cur.fetchall()
    assert len(rows) == 1, f"Expected 1 row, got {len(rows)}"

    cols = [d[0] for d in cur.description]
    row  = dict(zip(cols, rows[0]))

    assert "option_symbol" in row, (
        f"Returned row must have option_symbol key (from alias). Got: {list(row.keys())}"
    )
    assert row["option_symbol"] == "RIVN260612P00016500", (
        f"option_symbol alias must equal contract, got {row['option_symbol']!r}"
    )
    db.close()
