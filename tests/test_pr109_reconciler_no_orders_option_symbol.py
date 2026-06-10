"""
tests/test_pr109_reconciler_no_orders_option_symbol.py
PR#109 regression: _backfill_missing_position_links must not SELECT
option_symbol directly from orders (column does not exist on orders table).
"""
import re, sqlite3
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
REC_SRC = (_REPO / "ap_reconciler.py").read_text()


# ── Source-level: no bare option_symbol selected from orders ─────────────────

def test_fetch_select_uses_alias_not_bare_column():
    """
    The _fetch() SELECT inside _backfill_missing_position_links must use
      contract AS option_symbol
    NOT
      option_symbol   (bare column reference)
    """
    idx_fetch = REC_SRC.find("def _fetch():")
    idx_end   = REC_SRC.find("rows = c.fetchall()", idx_fetch)
    block     = REC_SRC[idx_fetch:idx_end]
    assert "contract AS option_symbol" in block, (
        "_fetch SELECT must use 'contract AS option_symbol', not bare option_symbol"
    )
    # Bare option_symbol must not appear before the alias keyword
    alias_pos = block.find("contract AS option_symbol")
    # Any option_symbol before the alias line is a bare column ref — not allowed
    bare_before = re.search(r"(?<!AS )\boption_symbol\b", block[:alias_pos])
    assert not bare_before, (
        f"Bare option_symbol found before alias in _fetch: {bare_before.group()!r}"
    )

def test_no_direct_option_symbol_from_orders_in_fetch():
    """No SQL query against orders selects option_symbol as a plain column."""
    # Find every FROM orders block
    for m in re.finditer(r"FROM\s+orders\b", REC_SRC):
        # Look backward for the SELECT clause
        region_start = max(0, m.start() - 400)
        region       = REC_SRC[region_start:m.start()]
        # A bare option_symbol in the column list is only OK if it's aliased
        # (i.e., "contract AS option_symbol" not just "option_symbol,")
        bare = re.findall(r"(?<!AS )\boption_symbol\b(?!\s*,|\s*\n|\s*from|\s*AS)",
                          region, re.IGNORECASE)
        for hit in bare:
            assert False, (
                f"Bare option_symbol found in SELECT...FROM orders region: {hit!r}"
            )

def test_positions_option_symbol_references_untouched():
    """positions.option_symbol INSERT/SELECT references must still exist."""
    # Positions table legitimately has option_symbol
    assert "option_symbol" in REC_SRC, (
        "positions.option_symbol references should still be present in reconciler"
    )
    # Specifically the INSERT INTO positions block
    idx_ins = REC_SRC.find("INSERT INTO positions")
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
      - returned row has option_symbol key
      - returned option_symbol == contract value
    """
    sqls = _get_execute_sqls(REC_SRC)

    # Find the backfill _fetch SELECT: it's the one that selects execution_mode
    # and has WHERE status = 'FILLED'
    fetch_sql = None
    for sql in sqls:
        if "execution_mode" in sql and "status = 'FILLED'" in sql and "FROM orders" in sql:
            fetch_sql = sql
            break
    assert fetch_sql is not None, "Could not locate _fetch SELECT in reconciler source"

    # Must contain alias, not bare column
    assert "contract AS option_symbol" in fetch_sql, (
        f"_fetch SELECT must use contract AS option_symbol, got:\n{fetch_sql}"
    )

    translated = _sqlite_translate(fetch_sql)

    CLIENT = "test-reconciler@example.com"
    db = sqlite3.connect(":memory:")
    # orders table WITHOUT option_symbol (mirrors production schema)
    db.execute("""
        CREATE TABLE orders (
            id            INTEGER PRIMARY KEY,
            client_id     TEXT,
            kind          TEXT,
            status        TEXT,
            contract      TEXT,
            symbol        TEXT,
            direction     TEXT,
            filled_qty    INTEGER DEFAULT 0,
            fill_price    REAL,
            filled_ts     TEXT,
            execution_mode TEXT,
            broker_order_id TEXT,
            local_order_id  TEXT,
            position_id   TEXT
        )
    """)
    db.execute(
        "INSERT INTO orders (client_id,kind,status,contract,symbol,direction,"
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
        f"option_symbol alias must equal contract value, got {row['option_symbol']!r}"
    )
    db.close()
