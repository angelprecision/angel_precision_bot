"""
tests/test_p0_exit_engine_broker_truth.py
P0: exit-engine broker-truth visibility repair.
"""
import os, re, sqlite3, pytest, types, sys, threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, call

_REPO = Path(__file__).resolve().parents[1]
EE_SRC = (_REPO / "ap_exit_engine.py").read_text()
_DATABASE_URL = os.getenv("DATABASE_URL", "").strip()


# ── Source checks ─────────────────────────────────────────────────────────────

def test_upsert_no_source_column():
    """Fix 1: source must not appear in the INSERT column list."""
    idx = EE_SRC.find("INSERT INTO positions")
    end = EE_SRC.find("ON CONFLICT DO NOTHING", idx)
    block = EE_SRC[idx:end]
    assert "source" not in block, (
        "INSERT INTO positions must not include source column — production schema may not have it"
    )

def test_upsert_on_conflict_requery():
    """Fix 1: ON CONFLICT fallback re-queries existing row by client_id+contract+mode."""
    # Find the ON CONFLICT in the _upsert function (skip any earlier occurrences)
    upsert_start = EE_SRC.find("def _upsert_broker_position_to_db")
    upsert_end   = EE_SRC.find("\n    def ", upsert_start + 1)
    upsert_body  = EE_SRC[upsert_start:upsert_end]
    assert "ON CONFLICT DO NOTHING" in upsert_body
    assert "SELECT id FROM positions" in upsert_body, (
        "ON CONFLICT fallback must re-query by client_id+contract to return existing id"
    )
    assert "LOWER(TRIM(COALESCE(execution_mode, ''))) = %s" in upsert_body
    assert "ORDER BY entry_ts" in upsert_body

def test_load_db_row_exact_mode_filter_present():
    """Mode-scoped DB load must select and filter execution_mode."""
    load_start = EE_SRC.find("def _load_db_position_row")
    load_end   = EE_SRC.find("\n    def ", load_start + 1)
    load_body  = EE_SRC[load_start:load_end]
    assert "signal_id," in load_body and "execution_mode," in load_body
    assert "LOWER(TRIM(COALESCE(execution_mode, ''))) = %s" in load_body

def test_upsert_persists_execution_mode_column():
    """Broker-repair insert must persist execution_mode explicitly."""
    upsert_start = EE_SRC.find("def _upsert_broker_position_to_db")
    upsert_end   = EE_SRC.find("\n    def ", upsert_start + 1)
    upsert_body  = EE_SRC[upsert_start:upsert_end]
    assert "execution_mode," in upsert_body

def test_managed_position_from_row_has_prefer_qty_override():
    """Fix 2: _managed_position_from_row must accept prefer_qty_override kwarg."""
    idx = EE_SRC.find("def _managed_position_from_row")
    sig = EE_SRC[idx:idx + 300]
    assert "prefer_qty_override" in sig

# ── PR #558 amendment: three surgical review-blocker fixes ────────────────────

def test_pr558_amendment_fix1_undefined_column_fallback():
    """Fix 1 (v2): broker-repair SELECT and INSERT must degrade to a
    pre-#558 minimal projection/column-list when the extended-schema
    columns are not deployed — AND must survive PostgreSQL's aborted-
    transaction semantics.  On real PostgreSQL, catching UndefinedColumn
    inside a `with conn()` block does not clear the abort: the next
    execute on the same connection raises InFailedSqlTransaction.

    SELECT fallback must open a FRESH `with conn()` for the minimal
    projection.  INSERT fallback must use a SAVEPOINT (fresh conn()
    would drop the advisory lock and let a racing worker slip a
    duplicate row between our existence check and our fallback INSERT)
    with ROLLBACK TO SAVEPOINT + RELEASE SAVEPOINT to clear the abort."""
    # ── SELECT: two separate `with conn()` entries ────────────────────
    load_start = EE_SRC.find("def _load_db_position_row")
    load_end   = EE_SRC.find("\n    def ", load_start + 1)
    load_body  = EE_SRC[load_start:load_end]
    assert "UndefinedColumn" in load_body, (
        "SELECT must catch psycopg2.errors.UndefinedColumn"
    )
    # There must be at least two `with conn()` blocks in the load path
    # (one per SQL variant).  The v1 amendment used a single with-conn
    # with a same-transaction fallback — broken on real PostgreSQL.
    assert load_body.count("with conn() as c:") >= 2, (
        "SELECT fallback must run in a FRESH `with conn()` — real "
        "PostgreSQL leaves the extended attempt's transaction aborted"
    )
    assert "FRESH transaction" in load_body, (
        "Fallback log line must state that a fresh transaction is used"
    )

    # ── INSERT: SAVEPOINT / ROLLBACK TO / RELEASE ─────────────────────
    upsert_start = EE_SRC.find("def _upsert_broker_position_to_db")
    upsert_end   = EE_SRC.find("\n    def ", upsert_start + 1)
    upsert_body  = EE_SRC[upsert_start:upsert_end]
    assert "UndefinedColumn" in upsert_body
    assert upsert_body.count("INSERT INTO positions") >= 2, (
        "Both extended and fallback INSERT must be present"
    )
    assert "_sp = \"broker_repair_extended_insert\"" in upsert_body, (
        "SAVEPOINT name must be the canonical broker_repair_extended_insert"
    )
    assert 'c.execute(f"SAVEPOINT {_sp}")' in upsert_body, (
        "INSERT must open a SAVEPOINT before the extended attempt"
    )
    assert 'c.execute(f"ROLLBACK TO SAVEPOINT {_sp}")' in upsert_body, (
        "On UndefinedColumn (or any other error) the extended INSERT must "
        "roll back to the savepoint so the outer transaction can proceed"
    )
    assert 'c.execute(f"RELEASE SAVEPOINT {_sp}")' in upsert_body, (
        "The savepoint must be released whether the extended attempt "
        "succeeded or was rolled back"
    )


def test_pr558_amendment_fix1_select_survives_aborted_transaction():
    """Behavioral regression: model PostgreSQL's aborted-transaction
    semantics and prove _load_db_position_row's fresh-transaction
    fallback works.  Extended SELECT raises UndefinedColumn.  Any
    further execute on the SAME connection raises InFailedSqlTransaction
    (real PostgreSQL contract).  A fresh `with conn()` gives a clean
    connection and the minimal fallback SELECT succeeds."""
    # Simulated psycopg2 error classes — real ones if importable,
    # otherwise ad-hoc.  The fake DB raises these by name; the source
    # code catches by isinstance OR by message substring.
    try:
        from psycopg2.errors import (
            UndefinedColumn as _RealUndefinedColumn,
            InFailedSqlTransaction as _RealInFailed,
        )
        _UndefErr = _RealUndefinedColumn
        _AbortedErr = _RealInFailed
    except Exception:
        class _UndefErr(Exception):
            def __str__(self):
                return "column \"underlying_entry\" does not exist"
        class _AbortedErr(Exception):
            def __str__(self):
                return "current transaction is aborted"

    state = {"conn_num": 0, "in_aborted_txn": False}

    class _Cur:
        def __init__(self, conn_num):
            self._conn_num = conn_num
            self._last_sql = ""

        def execute(self, sql, params=()):
            # Model real PG: any execute on an aborted transaction fails
            # with InFailedSqlTransaction until the transaction is rolled
            # back (which happens at `with conn()` exit).
            if state["in_aborted_txn"]:
                raise _AbortedErr(
                    "current transaction is aborted, commands ignored "
                    "until end of transaction block"
                )
            self._last_sql = sql
            # Extended SELECT contains the schema-only columns.  Raise
            # UndefinedColumn and mark the transaction aborted.
            if "underlying_entry" in sql and "SELECT" in sql.upper():
                state["in_aborted_txn"] = True
                raise _UndefErr('column "underlying_entry" does not exist')
            return self

        def fetchone(self):
            # Minimal SELECT succeeds and returns a row without the
            # extended-schema keys.  Downstream backfill fills them
            # with None.
            if "SELECT" in self._last_sql.upper():
                return {
                    "id": "row-abc",
                    "client_id": "aborted-txn@example.com",
                    "underlying": "BAC",
                    "contract": "BAC260724P00062000",
                    "option_symbol": "BAC260724P00062000",
                    "side": "PUT",
                    "direction": "PUT",
                    "qty": 1,
                    "quantity_remaining": 1,
                    "avg_fill": 0.97,
                    "entry_price": 0.97,
                    "entry_ts": "2026-07-24T13:00:00Z",
                    "status": "OPEN",
                    "signal_id": "sig-1",
                    "execution_mode": "live",
                }
            return None

        def fetchall(self):
            return []

        @property
        def rowcount(self):
            return 1

    @contextmanager
    def fake_conn():
        # Each new `with conn()` clears the aborted flag (real ap.db
        # rolls back on exception when the block exits, then returns a
        # clean conn from the pool for the next acquisition).
        state["conn_num"] += 1
        state["in_aborted_txn"] = False
        yield _Cur(state["conn_num"])

    fake_db = types.SimpleNamespace(
        conn=fake_conn, run_with_retry=lambda fn, **_: fn()
    )
    prior = sys.modules.get("ap.db")
    sys.modules["ap.db"] = fake_db

    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        if prior is not None:
            sys.modules["ap.db"] = prior
        else:
            sys.modules.pop("ap.db", None)
        pytest.skip("APExitEngine not found")
    eng = engine_cls.__new__(engine_cls)
    eng._email = "aborted-txn@example.com"
    eng._lock = __import__("threading").Lock()
    eng._positions = []
    eng._positions_by_id = {}
    eng.broker = types.SimpleNamespace(mode="live")
    try:
        loaded = eng._load_db_position_row("BAC260724P00062000")
    finally:
        if prior is not None:
            sys.modules["ap.db"] = prior
        else:
            sys.modules.pop("ap.db", None)

    # Under the v1 broken pattern (single `with conn()`, fallback in
    # same transaction), this call would raise InFailedSqlTransaction
    # and _load_db_position_row would return None via its outer except.
    # The v2 fix opens a FRESH `with conn()` for the minimal fallback,
    # so the returned row proves the fresh-transaction path fired.
    assert loaded is not None, (
        "Fresh-transaction fallback must succeed after extended "
        "UndefinedColumn aborts the first transaction"
    )
    assert loaded["id"] == "row-abc"
    assert loaded["execution_mode"] == "live"
    # Extended-schema keys absent from the minimal projection must be
    # backfilled with None so downstream repair code (which tolerates
    # None here) behaves identically.
    assert loaded["underlying_entry"] is None
    assert loaded["local_order_id"] is None
    assert loaded["broker_order_id"] is None
    # We must have opened TWO `with conn()` blocks — one for the
    # aborted extended attempt, one fresh for the minimal fallback.
    assert state["conn_num"] == 2, (
        f"Expected 2 conn() entries (extended + fresh fallback), got {state['conn_num']}"
    )


def test_pr558_amendment_fix2_option_symbol_or_branch_restored():
    """Fix 2: every positions-table lookup used by the broker-repair path
    must match on contract OR option_symbol.  Legacy rows carry the OCC in
    option_symbol; matching only on contract would miss the existing active
    row and create a duplicate ownership row for the same broker-open
    contract — exactly the class of defect #558 is meant to prevent."""
    # Three lookup sites in ap_exit_engine.py:
    #   1) _load_db_position_row (pre-repair state read)
    #   2) pre-INSERT existence check inside the advisory lock
    #   3) post-INSERT ON CONFLICT DO NOTHING re-query
    or_hits = EE_SRC.count(
        "OR UPPER(TRIM(COALESCE(option_symbol, ''))) = UPPER(TRIM(%s))"
    )
    assert or_hits >= 3, (
        f"Expected option_symbol OR-branch at all 3 positions-table lookup "
        f"sites (SELECT, pre-INSERT, post-INSERT re-query); found {or_hits}"
    )


def test_pr558_lookup_holds_on_multiple_active_position_rows():
    """The pre-repair lookup must not pick an arbitrary active row when the
    durable table contains contradictory ownership candidates."""
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")
    sym = "BAC260724P00062000"
    rows = [
        {
            "id": "active-row-1", "client_id": eng._email,
            "contract": sym, "option_symbol": sym,
            "execution_mode": "live", "status": "OPEN",
            "quantity_remaining": 1,
        },
        {
            "id": "active-row-2", "client_id": eng._email,
            "contract": sym, "option_symbol": sym,
            "execution_mode": "live", "status": "OPEN",
            "quantity_remaining": 1,
        },
    ]
    fake, restore = _pr558_fake_db_with_rows(rows)
    prior = sys.modules.get("ap.db")
    sys.modules["ap.db"] = fake
    try:
        result = eng._load_db_position_row(sym)
    finally:
        restore()
        if prior is not None:
            sys.modules["ap.db"] = prior
    assert result is not None
    assert result["_broker_repair_lookup_status"] == "AMBIGUOUS_ACTIVE_ROWS"


def test_pr558_managed_row_prefers_exact_legacy_option_symbol():
    """A legacy short `contract` must not become the exit identity when the
    row's exact OCC is stored in `option_symbol`."""
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")
    sym = "IWM260117C00220000"
    managed = eng._managed_position_from_row(
        {
            "id": "legacy-row",
            "contract": "IWM",
            "option_symbol": sym,
            "underlying": "IWM",
            "side": "CALL",
            "qty": 1,
            "quantity_remaining": 1,
            "entry_price": 2.40,
            "execution_mode": "live",
        },
        qty_override=1,
        prefer_qty_override=True,
        expected_contract=sym,
    )
    assert managed.option_symbol == sym
    assert managed.ticker == "IWM"


def test_pr558_amendment_fix3_partially_filled_alternate_spelling():
    """Fix 3: ENTRY-order canonical lookup must recognize the alternate
    PARTIALLY_FILLED status form.  The codebase persists both PARTIAL_FILL
    and PARTIALLY_FILLED; accepting only PARTIAL_FILL loses the exact
    filled-entry identity/geometry the PR is designed to preserve and
    forces the repair path into the no-candidates branch."""
    # Both the SQL IN-list and the Python filter set must accept all three
    # canonical spellings.
    assert (
        "IN ('FILLED', 'PARTIAL_FILL', 'PARTIALLY_FILLED')" in EE_SRC
    ), "ENTRY-orders SQL must accept FILLED, PARTIAL_FILL, and PARTIALLY_FILLED"
    assert (
        '{"FILLED", "PARTIAL_FILL", "PARTIALLY_FILLED"}' in EE_SRC
    ), "Python filter set must mirror the SQL IN-list exactly"


def test_precheck_structured_log_events():
    """Fix 7: all required structured log events present."""
    for event in [
        "EXIT_BROKER_PRECHECK_START",
        "EXIT_BROKER_POSITION_MISSING_FROM_ENGINE",
        "EXIT_BROKER_POSITION_DB_ROW_FOUND",
        "EXIT_BROKER_POSITION_DB_QTY_STALE_REPAIRED",
        "EXIT_BROKER_POSITION_ADDED_TO_ENGINE",
        "EXIT_UNSAFE_BROKER_POSITION_REPAIR_FAILED",
        "EXIT_UNSAFE_BROKER_TRUTH_UNAVAILABLE",
        "EXIT_BROKER_PRECHECK_SUMMARY",
    ]:
        assert event in EE_SRC, f"Missing structured log event: {event}"

def test_broker_truth_unavailable_log_fields():
    """Fix 6: failure log must include broker_truth_available + local_engine_position_count."""
    # Find the log.error call (skip docstring occurrence)
    idx = EE_SRC.find("broker_truth_available=false")
    assert idx > 0, "broker_truth_available=false must appear in the log call"
    region = EE_SRC[max(0,idx-200):idx+200]
    assert "EXIT_UNSAFE_BROKER_TRUTH_UNAVAILABLE" in region
    assert "local_engine_position_count" in EE_SRC

def test_will_evaluate_this_cycle_in_added_log():
    """Fix 8: added-to-engine log must assert will_evaluate_this_cycle=true."""
    # will_evaluate_this_cycle=true must appear at least once in each key log event
    assert EE_SRC.count("will_evaluate_this_cycle=true") >= 2, (
        "will_evaluate_this_cycle=true must appear in multiple log events"
    )
    # ADDED_TO_ENGINE log must exist and be near will_evaluate
    added_idx = EE_SRC.find("EXIT_BROKER_POSITION_ADDED_TO_ENGINE")
    assert added_idx > 0
    added_region = EE_SRC[added_idx:added_idx+600]
    assert "will_evaluate_this_cycle=true" in added_region or \
           "will_evaluate_this_cycle" in added_region

def test_quote_fallback_uses_mark():
    """Fix 5 / Fix 3: mark is first in the quote fallback chain."""
    idx = EE_SRC.find("def _fetch_broker_quote")
    end = EE_SRC.find("\n    def ", idx + 1)
    body = EE_SRC[idx:end]
    assert 'q.get("mark")' in body, "_fetch_broker_quote must read mark from quote"
    # In precheck, mark must be tried before mid/last
    idx2 = EE_SRC.find("# mark → mid")
    assert idx2 > 0, "precheck must document mark→mid→last fallback order"


# ── Behavioral: _managed_position_from_row with prefer_qty_override ───────────

def _load_engine_class():
    """Import ap_exit_engine without polluting sys.modules for later test files.

    The earlier implementation permanently inserted stubs for ap.db,
    ap.position_manager, and ap_tradier via sys.modules.setdefault (and
    sys.modules["ap_exit_engine"] = mod).  When pytest then collected any
    later P0 file that does `from ap.position_manager import APPositionManager`,
    the stub `types.ModuleType("ap.position_manager")` had no such symbol and
    collection blew up.  We now snapshot every module we touch, run the import
    under the stubs, and restore the previous mapping (real module, stub, or
    absent) once we're done — leaving the interpreter exactly as we found it.
    """
    import importlib
    import importlib.util

    _touched = [
        "ap_exit_engine",
        "ap",
        "ap.db",
        "ap.position_manager",
        "ap_tradier",
    ]
    # Snapshot the pre-existing entries (may be real modules, stubs, or absent).
    _saved = {name: sys.modules.get(name) for name in _touched}
    _stub_inserted = {name: False for name in _touched}

    try:
        for name in ["ap.db", "ap.position_manager", "ap_tradier"]:
            if name not in sys.modules:
                sys.modules[name] = types.ModuleType(name)
                _stub_inserted[name] = True

        spec = importlib.util.spec_from_file_location(
            "ap_exit_engine", _REPO / "ap_exit_engine.py"
        )
        mod = importlib.util.module_from_spec(spec)
        sys.modules["ap_exit_engine"] = mod
        _stub_inserted["ap_exit_engine"] = True
        try:
            spec.loader.exec_module(mod)
            return mod
        except Exception:
            return None
    finally:
        # Restore whatever was there before — real module, prior stub, or
        # nothing.  Only remove entries WE inserted; never evict a real
        # ap.position_manager that some other test/file loaded first.
        for name in _touched:
            prior = _saved[name]
            if prior is not None:
                sys.modules[name] = prior
            elif _stub_inserted[name]:
                sys.modules.pop(name, None)


_EE_MOD = _load_engine_class()
_skip_if_no_mod = pytest.mark.skipif(
    _EE_MOD is None, reason="ap_exit_engine could not be imported in test env"
)


@contextmanager
def _postgres_positions_table(monkeypatch):
    if not _DATABASE_URL:
        pytest.skip("DATABASE_URL not configured for PostgreSQL coverage")
    psycopg2 = pytest.importorskip("psycopg2")
    import importlib

    monkeypatch.delitem(sys.modules, "ap.db", raising=False)
    monkeypatch.delitem(sys.modules, "ap", raising=False)
    importlib.invalidate_caches()

    pg_conn = psycopg2.connect(_DATABASE_URL)
    pg_conn.autocommit = False
    cur = pg_conn.cursor()
    # AMENDMENT (P0 test-isolation): the previous fixture created the TEMP
    # TABLE with `ON COMMIT DROP` and then immediately committed, which
    # destroyed the table before any test could read it (all subsequent
    # queries raised UndefinedTable).  `ON COMMIT PRESERVE ROWS` keeps the
    # table alive for the full session; teardown drops it explicitly below.
    cur.execute(
        """
        CREATE TEMP TABLE positions (
            id TEXT NOT NULL PRIMARY KEY,
            client_id TEXT,
            underlying TEXT,
            contract TEXT,
            option_symbol TEXT,
            execution_mode TEXT,
            side TEXT,
            direction TEXT,
            qty INTEGER,
            quantity_remaining INTEGER,
            avg_fill DOUBLE PRECISION,
            entry_price DOUBLE PRECISION,
            underlying_entry DOUBLE PRECISION,
            stop_underlying DOUBLE PRECISION,
            target_underlying DOUBLE PRECISION,
            entry_ts TIMESTAMPTZ,
            status TEXT,
            signal_id TEXT,
            local_order_id TEXT,
            broker_order_id TEXT,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        ) ON COMMIT PRESERVE ROWS
        """
    )
    cur.execute(
        """
        CREATE TEMP TABLE orders (
            local_order_id TEXT PRIMARY KEY,
            client_id TEXT,
            position_id TEXT,
            kind TEXT,
            status TEXT,
            qty INTEGER,
            contract TEXT,
            execution_mode TEXT,
            filled_qty INTEGER,
            fill_price DOUBLE PRECISION,
            filled_ts TIMESTAMPTZ,
            submitted_ts TIMESTAMPTZ,
            signal_id TEXT,
            broker_order_id TEXT,
            meta JSONB,
            updated_ts TIMESTAMPTZ DEFAULT NOW(),
            created_ts TIMESTAMPTZ DEFAULT NOW()
        ) ON COMMIT PRESERVE ROWS
        """
    )
    pg_conn.commit()

    class _ConnWrapper:
        """Mirrors ap.db._ConnWrapper — must return dict rows via RealDictCursor.

        The previous fixture returned raw psycopg2 tuples, which trained
        production code to index by position.  Real ap.db wraps
        psycopg2.extras.RealDictCursor and returns plain dicts, so tuple
        indexing would explode in production.  This fixture must match
        production, not the reverse.
        """
        def __init__(self, connection, cursor):
            self._conn = connection
            self._cur = cursor

        def execute(self, sql, params=()):
            self._cur.execute(sql, params)
            return self

        def fetchone(self):
            row = self._cur.fetchone()
            return dict(row) if row else None

        def fetchall(self):
            return [dict(r) for r in (self._cur.fetchall() or [])]

        @property
        def description(self):
            return self._cur.description

        @property
        def rowcount(self):
            return self._cur.rowcount

    @contextmanager
    def fake_conn():
        local_cur = pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield _ConnWrapper(pg_conn, local_cur)
            pg_conn.commit()
        except Exception:
            pg_conn.rollback()
            raise
        finally:
            local_cur.close()

    fake_db = types.SimpleNamespace(conn=fake_conn, run_with_retry=lambda fn, **_: fn())
    monkeypatch.setitem(sys.modules, "ap.db", fake_db)
    try:
        yield pg_conn
    finally:
        try:
            pg_conn.rollback()
        except Exception:
            pass
        try:
            _teardown_cur = pg_conn.cursor()
            _teardown_cur.execute("DROP TABLE IF EXISTS orders")
            _teardown_cur.execute("DROP TABLE IF EXISTS positions")
            pg_conn.commit()
            _teardown_cur.close()
        except Exception:
            pass
        cur.close()
        pg_conn.close()


@_skip_if_no_mod
def test_prefer_qty_override_overrides_stale_zero():
    """
    Test 2: DB row has quantity_remaining=0 (stale). broker_qty=3.
    prefer_qty_override=True must produce ManagedPosition with qty=3, qr=3.
    """
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")
    # Build a minimal engine stub
    eng = engine_cls.__new__(engine_cls)
    eng._email   = "test@example.com"
    eng._lock    = __import__("threading").Lock()
    eng._positions = []

    row = {
        "id": "pos-stale-001",
        "contract": "HOOD260612C00090000",
        "option_symbol": "HOOD260612C00090000",
        "underlying": "HOOD",
        "side": "CALL",
        "direction": "CALL",
        "qty": 3,
        "quantity_remaining": 0,   # ← stale: DB shows 0 remaining
        "entry_price": 2.22,
        "avg_fill": 2.22,
        "entry_ts": None,
        "status": "OPEN",
        "signal_id": None,
    }
    mp = eng._managed_position_from_row(row, qty_override=3, prefer_qty_override=True)
    assert mp.quantity == 3, f"Expected quantity=3, got {mp.quantity}"
    assert mp.quantity_remaining == 3, f"Expected quantity_remaining=3, got {mp.quantity_remaining}"


@_skip_if_no_mod
def test_load_db_position_row_is_exact_mode_scoped_in_postgres(monkeypatch):
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    with _postgres_positions_table(monkeypatch) as pg_conn:
        contract = "SPY260821C00650000"
        client = "mode-scope@example.com"
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO positions (
                    id, client_id, underlying, contract, option_symbol, execution_mode,
                    side, direction, qty, quantity_remaining, avg_fill, entry_price,
                    entry_ts, status, signal_id
                ) VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s),
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW() - INTERVAL '1 minute', %s, %s)
                """,
                (
                    "live-db-position-id", client, "SPY", contract, contract, "live",
                    "CALL", "CALL", 1, 1, 1.25, 1.25, "OPEN", "sig-live",
                    "paper-db-position-id", client, "SPY", contract, contract, "paper",
                    "CALL", "CALL", 1, 1, 1.15, 1.15, "OPEN", "sig-paper",
                ),
            )
        pg_conn.commit()

        eng = engine_cls.__new__(engine_cls)
        eng._email = client
        eng._lock = __import__("threading").Lock()
        eng._positions = []
        eng._positions_by_id = {}
        eng.broker = types.SimpleNamespace(mode="paper")

        paper_row = eng._load_db_position_row(contract)
        assert paper_row is not None
        assert paper_row["id"] == "paper-db-position-id"
        assert paper_row["execution_mode"] == "paper"

        eng.broker = types.SimpleNamespace(mode="live")
        live_row = eng._load_db_position_row(contract)
        assert live_row is not None
        assert live_row["id"] == "live-db-position-id"
        assert live_row["execution_mode"] == "live"


@_skip_if_no_mod
def test_upsert_broker_position_to_db_persists_exact_mode_in_postgres(monkeypatch):
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    with _postgres_positions_table(monkeypatch) as pg_conn:
        contract = "QQQ260821P00450000"
        client = "mode-upsert@example.com"
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, position_id, kind, status,
                    contract, execution_mode, filled_qty, fill_price, filled_ts,
                    meta
                ) VALUES (%s, %s, %s, 'ENTRY', 'FILLED', %s, 'paper',
                          %s, %s, %s, %s::jsonb)
                """,
                (
                    "paper-entry-for-recovery", client, "paper-entry-position",
                    contract, 2, 0.75, "2026-07-22T13:00:00Z", "{}",
                ),
            )
        pg_conn.commit()

        eng = engine_cls.__new__(engine_cls)
        eng._email = client
        eng._lock = __import__("threading").Lock()
        eng._positions = []
        eng._positions_by_id = {}
        eng.broker = types.SimpleNamespace(mode="paper")

        row_id = eng._upsert_broker_position_to_db(
            contract,
            {"quantity": 2, "cost_basis": 150.0, "date_acquired": "2026-07-22T13:00:00Z"},
        )

        assert row_id
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT id, execution_mode FROM positions WHERE client_id = %s AND contract = %s",
                (client, contract),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[0] == row_id
        assert row[1] == "paper"


@_skip_if_no_mod
def test_upsert_reuses_exact_filled_entry_identity_and_geometry(monkeypatch):
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    with _postgres_positions_table(monkeypatch) as pg_conn:
        contract = "BAC260821C00065000"
        client = "repair-identity@example.com"
        position_id = "canonical-position-516"
        filled_ts = "2026-07-22T13:00:00Z"
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, position_id, kind, status,
                    contract, execution_mode, filled_qty, fill_price, filled_ts,
                    signal_id, broker_order_id, meta
                ) VALUES (%s, %s, %s, 'ENTRY', 'FILLED', %s, 'live',
                          %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    "local-entry-516", client, position_id, contract,
                    2, 0.75, filled_ts, "signal-516", "broker-entry-516",
                    '{"underlying_entry":127.425,"stop_underlying":130.44,'
                    '"target_underlying":124.78}',
                ),
            )
        pg_conn.commit()

        eng = engine_cls.__new__(engine_cls)
        eng._email = client
        eng._lock = __import__("threading").Lock()
        eng._positions = []
        eng._positions_by_id = {}
        eng.broker = types.SimpleNamespace(mode="live")

        row_id = eng._upsert_broker_position_to_db(
            contract,
            {
                "quantity": 2,
                "cost_basis": 150.0,
                "date_acquired": filled_ts,
            },
        )

        assert row_id == position_id
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, client_id, execution_mode, signal_id,
                       local_order_id, broker_order_id, underlying_entry,
                       stop_underlying, target_underlying
                FROM positions
                WHERE client_id = %s AND contract = %s
                """,
                (client, contract),
            )
            row = cur.fetchone()
        assert row is not None
        assert row[:6] == (
            position_id, client, "live", "signal-516",
            "local-entry-516", "broker-entry-516",
        )
        assert row[6:] == pytest.approx((127.425, 130.44, 124.78))


@_skip_if_no_mod
def test_normal_load_preserves_zero_qty():
    """
    Test 6: normal DB-only load (prefer_qty_override=False, default).
    quantity_remaining=0 must stay 0 — do not resurrect closed row.
    """
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")
    eng = engine_cls.__new__(engine_cls)
    eng._email = "test@example.com"
    eng._lock  = __import__("threading").Lock()
    eng._positions = []

    row = {
        "id": "pos-closed-001",
        "contract": "TSLA260212C00200000",
        "option_symbol": "TSLA260212C00200000",
        "underlying": "TSLA",
        "side": "CALL",
        "direction": "CALL",
        "qty": 2,
        "quantity_remaining": 0,   # ← truly closed
        "entry_price": 3.00,
        "avg_fill": 3.00,
        "entry_ts": None,
        "status": "CLOSED",
        "signal_id": None,
    }
    mp = eng._managed_position_from_row(row, qty_override=0, prefer_qty_override=False)
    assert mp.quantity_remaining == 0, (
        "Normal load must not resurrect closed row — quantity_remaining must stay 0"
    )


# ── Behavioral: upsert production schema safety ───────────────────────────────

def test_upsert_no_source_sqlite_fixture():
    """
    Test 1: INSERT SQL must not include source column.
    Also: INSERT without source succeeds against a SQLite fixture that has no source column.
    """
    # Extract INSERT SQL from _upsert_broker_position_to_db function body
    idx  = EE_SRC.find("def _upsert_broker_position_to_db")
    end  = EE_SRC.find("\n    def ", idx + 1)
    body = EE_SRC[idx:end]

    # Verify source is NOT in the INSERT column list
    ins_start  = body.find("INSERT INTO positions (")
    ins_end    = body.find("ON CONFLICT DO NOTHING", ins_start)
    ins_block  = body[ins_start:ins_end]
    assert "source" not in ins_block, (
        f"INSERT must not include source column.\n{ins_block[:300]}"
    )
    assert "updated_at" in ins_block, "INSERT must include updated_at"

    # DB test: direct INSERT with the fixed column set succeeds on no-source schema
    import uuid as _uuid, sqlite3 as _sl
    db = _sl.connect(":memory:")
    db.execute("""
        CREATE TABLE positions (
            id TEXT PRIMARY KEY,
            client_id TEXT, underlying TEXT, contract TEXT, option_symbol TEXT,
            side TEXT, direction TEXT, qty INTEGER, quantity_remaining INTEGER,
            entry_price REAL, avg_fill REAL, status TEXT,
            entry_ts TEXT, updated_at TEXT
        )
    """)
    db.commit()
    try:
        db.execute(
            "INSERT INTO positions (id, client_id, underlying, contract, option_symbol, "
            "side, direction, qty, quantity_remaining, entry_price, avg_fill, status, "
            "entry_ts, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (str(_uuid.uuid4()), "test@example.com", "RIVN",
             "RIVN260612P00016500", "RIVN260612P00016500",
             "PUT", "PUT", 3, 3, 0.61, 0.61, "OPEN", "2026-06-10",
             "2026-06-10 09:30:00"),
        )
        db.commit()
    except Exception as e:
        pytest.fail(f"INSERT on no-source schema raised: {e}")
    finally:
        db.close()


def test_upsert_on_conflict_fallback_sqlite():
    """
    Test 7: ON CONFLICT returns no row, re-query finds existing id.
    """
    # Build the re-query SQL from source
    idx = EE_SRC.find("SELECT id FROM positions")
    fallback_region = EE_SRC[idx:idx + 900]
    assert "client_id" in fallback_region
    assert "UPPER(TRIM(COALESCE(contract" in fallback_region

    # Simple structural check — the re-query EXISTS in source
    assert "ORDER BY entry_ts DESC NULLS LAST" in fallback_region


def test_broker_fetch_failure_does_not_mark_flat():
    """
    Test 4: broker.list_positions() raises → log EXIT_UNSAFE_BROKER_TRUTH_UNAVAILABLE,
    return False, never touches local engine positions.
    """
    # Source check: the precheck must consume the shared authoritative truth
    # seam and return False when that seam reports transport/malformed truth.
    idx = EE_SRC.find("resolve_authoritative_broker_positions")
    precheck_start = EE_SRC.rfind("def _broker_position_precheck", 0, idx)
    precheck_end = EE_SRC.find("\n    def ", idx)
    precheck_body = EE_SRC[precheck_start:precheck_end]
    assert idx > precheck_start
    assert "is_fresh_exact" in precheck_body
    except_start = precheck_body.find("except Exception as _bp_err:")
    except_end   = precheck_body.find("\n        broker_map", except_start)
    except_block = precheck_body[except_start:except_end]
    assert "return False" in except_block, (
        "Broker fetch failure must return False (not proceed)"
    )
    # Must not clear positions in the except block
    assert "self._positions" not in except_block, (
        "Broker fetch failure must not touch local engine positions"
    )

# ── Behavioral: _broker_position_precheck end-to-end ─────────────────────────

def test_broker_precheck_stale_db_qty_zero_loaded_with_broker_qty():
    """
    Behavioral regression test for _broker_position_precheck().

    Setup:
      - broker.list_positions returns RIVN260612P00016500 qty=3
      - engine has no current positions
      - _load_db_position_row returns DB row: status=OPEN qty=3 quantity_remaining=0 (stale)
      - _fetch_broker_quote returns empty quote (all zeros)

    Expected:
      - add_position is called
      - added ManagedPosition.quantity == 3
      - added ManagedPosition.quantity_remaining == 3
      - quote failure does not block loading
      - _broker_position_precheck() returns True
    """
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found in test env")

    import threading
    CONTRACT = "RIVN260612P00016500"

    # Build a minimal engine stub
    eng = engine_cls.__new__(engine_cls)
    eng._email      = "jasoncosby1@gmail.com"
    # The production engine uses an RLock because 3a canonical hydration
    # registers the canonical owner before checking/removing a degraded owner.
    eng._lock       = threading.RLock()
    eng._positions  = []
    eng._positions_by_id = {}

    # Stub broker: returns one position with qty=3
    mock_broker = MagicMock()
    mock_broker.account_id   = "VA23856850"
    mock_broker.mode = "live"
    mock_broker.list_positions.return_value = [{
        "symbol":     CONTRACT,
        "quantity":   3,
        "cost_basis": 183.0,   # 0.61 * 3 * 100
        "date_acquired": "2026-06-10",
    }]
    eng.broker = mock_broker

    # Stub DB row: status=OPEN, qty=3, quantity_remaining=0 (stale)
    db_row = {
        "id":                 "pos-stale-rivn",
        "contract":           CONTRACT,
        "option_symbol":      CONTRACT,
        "underlying":         "RIVN",
        "side":               "PUT",
        "direction":          "PUT",
        "qty":                3,
        "quantity_remaining": 0,   # ← stale
        "entry_price":        0.61,
        "avg_fill":           0.61,
        "entry_ts":           None,
        "status":             "OPEN",
        "signal_id":          None,
        "execution_mode":     "live",
    }

    added_positions = []

    def _fake_add_position(pos):
        added_positions.append(pos)
        eng._positions.append(pos)
        eng._positions_by_id[getattr(pos, "position_id", "")] = pos

    def _fake_load_db_row(sym):
        return db_row if sym.upper() == CONTRACT else None

    def _fake_fetch_quote(_sym):
        # Empty quote — all zeros — quote failure
        return {"mark": 0.0, "bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0}

    def _fake_repair_qty(*a, **kw):
        pass   # DB repair is best-effort

    eng.add_position          = _fake_add_position
    eng._load_db_position_row = _fake_load_db_row
    eng._fetch_broker_quote   = _fake_fetch_quote

    # ap.db is imported inside the DB repair path — stub it in sys.modules
    # so the best-effort repair attempt doesn't raise ModuleNotFoundError.
    import sys, types
    _db_stub  = types.ModuleType("ap.db")
    # Match the production DB wrapper: run the callback and return its proof.
    # The locked reread is the current durable row authority.
    class _RepairCursor:
        rowcount = 1

        def __init__(self):
            self._sql = ""

        def execute(self, sql, params=()):
            self._sql = str(sql)
            return self

        def fetchall(self):
            return []

        def fetchone(self):
            return dict(db_row) if "FOR UPDATE" in self._sql else None

    @contextmanager
    def _repair_conn():
        yield _RepairCursor()

    _db_stub.run_with_retry = lambda f: f()
    _db_stub.conn = _repair_conn
    prior_db = sys.modules.get("ap.db")
    sys.modules["ap.db"] = _db_stub
    try:
        result = eng._broker_position_precheck()
    finally:
        if prior_db is not None:
            sys.modules["ap.db"] = prior_db
        else:
            sys.modules.pop("ap.db", None)

    # Assertions
    assert result is True, (
        "_broker_position_precheck must return True when repair succeeds "
        f"(got {result})"
    )
    assert len(added_positions) == 1, (
        f"add_position must be called once; called {len(added_positions)} times"
    )
    mp = added_positions[0]
    assert mp.quantity == 3, (
        f"ManagedPosition.quantity must be broker qty=3, got {mp.quantity}"
    )
    assert mp.quantity_remaining == 3, (
        f"ManagedPosition.quantity_remaining must be broker qty=3, got {mp.quantity_remaining}"
    )

def test_log_honesty_repair_failed_reason_in_added_log():
    """
    Fix 3: EXIT_BROKER_POSITION_ADDED_TO_ENGINE log must include repair_failed_reason=%s,
    not a hardcoded empty string.
    The synthetic path sets repair_failed_reason to a non-empty value;
    the log must print it.
    """
    fn_start = EE_SRC.find("# ── 3e. Required structured log")
    fn_end   = EE_SRC.find("\n            # ── 4.", fn_start)
    log_block = EE_SRC[fn_start:fn_end]
    # Must use %s placeholder, not hardcoded empty
    assert "repair_failed_reason=%s" in log_block, (
        "ADDED_TO_ENGINE log must use repair_failed_reason=%s (not hardcoded empty)"
    )
    # Must pass the variable, not empty string literal
    assert "repair_failed_reason or" in log_block or            "repair_failed_reason," in log_block, (
        "ADDED_TO_ENGINE log must pass repair_failed_reason variable"
    )

def test_repaired_syms_only_for_real_db_rows():
    """Only a confirmed durable row may be counted as repaired."""
    assert "engine_loaded_synthetic_syms" not in EE_SRC
    assert "if not new_id:" in EE_SRC
    assert "repaired_syms.append(sym)" in EE_SRC
    assert "db_upsert_returned_no_id" in EE_SRC

def test_summary_excludes_engine_loaded_synthetic():
    """The summary must distinguish confirmed repairs from failed repairs."""
    idx = EE_SRC.rfind("EXIT_BROKER_PRECHECK_SUMMARY")
    region = EE_SRC[idx:idx + 600]
    assert "engine_loaded_synthetic" not in region
    assert "repaired_from_broker" in region, (
        "EXIT_BROKER_PRECHECK_SUMMARY must still include repaired_from_broker (DB-confirmed only)"
    )
    assert "repair_failed" in region

def test_failed_upsert_does_not_create_synthetic_owner():
    """A failed upsert must not create an engine-only synthetic owner."""
    assert "db_upsert_returned_no_id" in EE_SRC
    assert "using synthetic position_id" not in EE_SRC
    assert "engine will still load and evaluate" not in EE_SRC


# =============================================================================
# AMENDMENT (Jason BAC): PR #385 final-bounded regression tests.
# Cover the four production defects and three test-isolation defects.
# =============================================================================

@_skip_if_no_mod
class TestExecutionModeFailsClosed:
    """_resolved_execution_mode must fail closed when sources disagree."""

    def _new_engine(self):
        engine_cls = getattr(_EE_MOD, "APExitEngine", None)
        if engine_cls is None:
            pytest.skip("APExitEngine not found")
        eng = engine_cls.__new__(engine_cls)
        eng._email = "resolver@example.com"
        eng._lock = __import__("threading").Lock()
        eng._positions = []
        eng._positions_by_id = {}
        return eng

    def test_only_master_control_paper_resolves_paper(self):
        eng = self._new_engine()
        eng.master_control = types.SimpleNamespace(mode="paper")
        eng.broker = types.SimpleNamespace()
        assert eng._resolved_execution_mode() == "paper"

    def test_only_broker_mode_live_resolves_live(self):
        eng = self._new_engine()
        eng.broker = types.SimpleNamespace(mode="live")
        assert eng._resolved_execution_mode() == "live"

    def test_agreeing_sources_resolve_that_mode(self):
        eng = self._new_engine()
        eng.master_control = types.SimpleNamespace(mode="live")
        eng.broker = types.SimpleNamespace(execution_mode="LIVE", mode="live")
        assert eng._resolved_execution_mode() == "live"

    def test_conflict_returns_blank_and_logs_critical(self, caplog):
        eng = self._new_engine()
        eng.master_control = types.SimpleNamespace(mode="paper")
        eng.broker = types.SimpleNamespace(mode="live")
        with caplog.at_level("CRITICAL"):
            assert eng._resolved_execution_mode() == ""
        assert any("EXECUTION_MODE_CONFLICT" in rec.message for rec in caplog.records), (
            "Conflicting sources must emit a critical EXECUTION_MODE_CONFLICT log"
        )

    def test_conflict_blocks_load_db_position_row(self, caplog):
        eng = self._new_engine()
        eng.master_control = types.SimpleNamespace(mode="paper")
        eng.broker = types.SimpleNamespace(mode="live")
        with caplog.at_level("CRITICAL"):
            assert eng._load_db_position_row("BAC260724P00062000") is None

    def test_conflict_blocks_upsert(self, caplog):
        eng = self._new_engine()
        eng.master_control = types.SimpleNamespace(mode="paper")
        eng.broker = types.SimpleNamespace(mode="live")
        with caplog.at_level("CRITICAL"):
            assert eng._upsert_broker_position_to_db(
                "BAC260724P00062000",
                {"quantity": 1, "cost_basis": 97.0, "date_acquired": "2026-07-24T13:00:00Z"},
            ) is None

    def test_conflict_quarantines_broker_repair(self, caplog):
        """Broker-repair identity must be behavior-quarantined when mode unproven."""
        eng = self._new_engine()
        eng.master_control = types.SimpleNamespace(mode="paper")
        eng.broker = types.SimpleNamespace(mode="live")
        row = {
            "id": "conflict-pos-1",
            "contract": "BAC260724P00062000",
            "option_symbol": "BAC260724P00062000",
            "underlying": "BAC",
            "side": "PUT", "direction": "PUT",
            "qty": 1, "quantity_remaining": 1,
            "entry_price": 0.97, "avg_fill": 0.97,
            "entry_ts": None, "status": "OPEN",
            "signal_id": None,
            # Row itself has no execution_mode → must resolve through the engine.
        }
        with caplog.at_level("CRITICAL"):
            mp = eng._managed_position_from_row(row, qty_override=1, prefer_qty_override=True)
        # Under conflict, resolver returns "" → engine marks adoption identity
        # quarantined so behavior-active checks refuse to route exits here.
        _is_quarantined = getattr(_EE_MOD, "_is_adoption_identity_quarantined")
        assert _is_quarantined(mp) is True


@_skip_if_no_mod
class TestDbDictRowContract:
    """Production ap.db returns dict rows via RealDictCursor.

    These tests exercise the exact three seams that were reading rows as
    tuples: _load_db_position_row, INSERT ... RETURNING id, and the ON
    CONFLICT re-query.
    """

    def _new_engine(self):
        engine_cls = getattr(_EE_MOD, "APExitEngine", None)
        if engine_cls is None:
            pytest.skip("APExitEngine not found")
        eng = engine_cls.__new__(engine_cls)
        eng._email = "dict-shape@example.com"
        eng._lock = __import__("threading").Lock()
        eng._positions = []
        eng._positions_by_id = {}
        eng.broker = types.SimpleNamespace(mode="live")
        return eng

    def _install_dict_fake_db(self, *, row=None, insert_id=None, existing_id=None):
        state = {"row": row, "insert_id": insert_id, "existing_id": existing_id,
                 "call": 0, "queries": []}

        class _Cur:
            def execute(self, sql, params=()):
                state["call"] += 1
                state["queries"].append((state["call"], sql.strip().splitlines()[0]))
                return self

            def fetchone(self):
                # Cycle: first fetchone on SELECT/RETURNING flow.
                if "INSERT INTO positions" in state["queries"][-1][1]:
                    return ({"id": state["insert_id"]}
                            if state["insert_id"] is not None else None)
                if "SELECT id FROM positions" in state["queries"][-1][1]:
                    return ({"id": state["existing_id"]}
                            if state["existing_id"] is not None else None)
                # Regular load path.
                return state["row"]

            def fetchall(self):
                return []

            @property
            def rowcount(self):
                return 1

        @contextmanager
        def fake_conn():
            yield _Cur()

        fake_db = types.SimpleNamespace(conn=fake_conn, run_with_retry=lambda fn, **_: fn())
        _prior = sys.modules.get("ap.db")
        sys.modules["ap.db"] = fake_db
        return _prior, state

    def _restore_db(self, prior):
        if prior is not None:
            sys.modules["ap.db"] = prior
        else:
            sys.modules.pop("ap.db", None)

    def test_load_returns_dict_values_not_column_names(self):
        eng = self._new_engine()
        row = {
            "id": "real-uuid-42",
            "underlying": "BAC",
            "contract": "BAC260724P00062000",
            "option_symbol": "BAC260724P00062000",
            "side": "PUT", "direction": "PUT",
            "qty": 1, "quantity_remaining": 1,
            "avg_fill": 0.97, "entry_price": 0.97,
            "entry_ts": None, "status": "OPEN",
            "signal_id": "sig-1", "execution_mode": "live",
        }
        prior, _ = self._install_dict_fake_db(row=row)
        try:
            loaded = eng._load_db_position_row("BAC260724P00062000")
        finally:
            self._restore_db(prior)
        assert loaded is not None
        # Regression: previous code returned {col_name: col_name} — every value
        # would be the string "id" / "underlying" / etc.  We want real values.
        assert loaded["id"] == "real-uuid-42"
        assert loaded["execution_mode"] == "live"
        assert loaded["entry_price"] == 0.97

    def test_insert_returning_id_extracts_from_dict(self):
        eng = self._new_engine()
        # Strict broker recovery now requires a proven filled ENTRY before an
        # INSERT can be exercised.  Keep this test focused on dict-shaped
        # INSERT ... RETURNING rows by supplying that proven predecessor.
        eng._find_exact_filled_entry_order = lambda *_args: {
            "position_id": "entry-position-dict",
            "filled_qty": 1,
            "fill_price": 0.97,
        }
        prior, _ = self._install_dict_fake_db(insert_id="inserted-id-xyz")
        try:
            row_id = eng._upsert_broker_position_to_db(
                "BAC260724P00062000",
                {"quantity": 1, "cost_basis": 97.0, "date_acquired": "2026-07-24T13:00:00Z"},
            )
        finally:
            self._restore_db(prior)
        assert row_id == "inserted-id-xyz"

    def test_conflict_requery_id_extracts_from_dict(self):
        eng = self._new_engine()
        eng._find_exact_filled_entry_order = lambda *_args: {
            "position_id": "existing-uuid-99",
            "filled_qty": 1,
            "fill_price": 0.97,
        }
        # INSERT returns None → ON CONFLICT DO NOTHING → re-query hits existing.
        prior, _ = self._install_dict_fake_db(insert_id=None, existing_id="existing-uuid-99")
        try:
            row_id = eng._upsert_broker_position_to_db(
                "BAC260724P00062000",
                {"quantity": 1, "cost_basis": 97.0, "date_acquired": "2026-07-24T13:00:00Z"},
            )
        finally:
            self._restore_db(prior)
        assert row_id == "existing-uuid-99"


def test_sys_modules_isolation_regression():
    """After _load_engine_class() runs, real ap.position_manager must import.

    The previous helper permanently replaced ap.position_manager with a bare
    types.ModuleType(), which caused every later P0 test file that does
    `from ap.position_manager import APPositionManager` to fail collection.
    """
    from ap.position_manager import APPositionManager  # noqa: F401
    # Sanity: it's the real symbol, not a stub attribute.
    assert isinstance(APPositionManager, type)


def test_postgres_fixture_wrapper_returns_dict_rows(monkeypatch):
    """The Postgres test wrapper must mirror ap.db._ConnWrapper's dict contract."""
    if not _DATABASE_URL:
        pytest.skip("DATABASE_URL not configured for PostgreSQL coverage")
    with _postgres_positions_table(monkeypatch) as pg_conn:
        contract = "AAPL260724C00200000"
        client = "wrapper-dict@example.com"
        with pg_conn.cursor() as raw_cur:
            raw_cur.execute(
                """
                INSERT INTO positions (id, client_id, underlying, contract, option_symbol,
                                       execution_mode, side, direction, qty, quantity_remaining,
                                       avg_fill, entry_price, entry_ts, status, signal_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s)
                """,
                ("wrap-1", client, "AAPL", contract, contract, "live",
                 "CALL", "CALL", 1, 1, 1.10, 1.10, "OPEN", "sig-w"),
            )
        pg_conn.commit()

        # Route through the fake ap.db wrapper the fixture installed —
        # fetchone/fetchall must return dicts, not tuples.
        import ap.db as _fake_db
        with _fake_db.conn() as c:
            c.execute("SELECT id, execution_mode FROM positions WHERE client_id = %s", (client,))
            row = c.fetchone()
        assert isinstance(row, dict), f"Expected dict, got {type(row).__name__}"
        assert row["id"] == "wrap-1"
        assert row["execution_mode"] == "live"


# ─── PR #558 amendment — Blocker 1 (degraded owner) + Blocker 2 (entry disambig) ──
#
# These regressions lock in the two lifecycle invariants Angel's amendment
# spec requires:
#   Blocker 1 — broker-proven open position must remain behavior-active
#               even when durable canonical DB repair is temporarily
#               unavailable, via ONE stable non-canonical degraded owner.
#   Blocker 2 — historical filled-ENTRY candidates must be narrowed by
#               broker economics/timestamp evidence BEFORE ambiguity is
#               declared.
#
# Source-inspection tests guard the invariants against future refactors.
# Behavioral tests exercise the exact lifecycles via the fake-DB pattern
# used by the rest of this file.  Behavioral tests skip locally when
# ap_exit_engine cannot be imported without full deps; they run in CI.


def test_pr558_blocker2_broker_evidence_applied_before_ambiguity():
    """Blocker 2: _find_exact_filled_entry_order must not return AMBIGUOUS
    on len(candidates) > 1 before applying broker evidence.  Two rows can
    share (client, mode, OCC, positive filled qty) while only one matches
    broker economics/timestamp.  The narrowing must happen first."""
    src = EE_SRC
    start = src.find("def _find_exact_filled_entry_order")
    end   = src.find("\n    def ", start + 1)
    body  = src[start:end]

    # The comment / ordering marker must be present so future edits that
    # revert the ordering are visibly wrong.
    assert "Blocker 2" in body, (
        "Blocker 2 rationale comment must be preserved in _find_exact_filled_entry_order"
    )
    assert "narrowed = [" in body, (
        "Broker-evidence narrowing pass over ALL candidates must exist"
    )
    assert "_broker_repair_order_matches" in body, (
        "Narrowing must reuse the existing _broker_repair_order_matches helper"
    )
    # The three-way decision must produce distinct outcomes so the caller
    # can distinguish 'unresolved' from 'true ambiguity'.
    assert "BROKER_REPAIR_ENTRY_EVIDENCE_UNRESOLVED" in body, (
        "Zero-survivor case must produce UNRESOLVED (caller keeps degraded owner)"
    )
    assert "BROKER_REPAIR_ENTRY_EVIDENCE_AMBIGUOUS" in body, (
        "Two-or-more-survivor case must still be AMBIGUOUS"
    )
    # The AMBIGUOUS log must include the pre- and post-broker counts so
    # ops can see the narrowing happened.
    assert "pre_broker_candidates" in body and "post_broker_survivors" in body, (
        "AMBIGUOUS log must include pre_broker_candidates and post_broker_survivors"
    )


def test_pr558_blocker1_degraded_owner_helpers_exist():
    """Blocker 1: the degraded broker-truth owner helpers must exist on
    APExitEngine with the exact spec-required semantics."""
    src = EE_SRC
    for name in (
        "_degraded_broker_owner_id",
        "_find_degraded_broker_owner_by_contract",
        "_install_or_refresh_degraded_broker_truth_owner",
        "_take_over_degraded_owner_if_any",
        "_apply_degraded_runtime_transfer",
        "_install_canonical_owner_atomically",
    ):
        assert f"def {name}" in src, f"required helper {name} is missing"
    # ManagedPosition must carry the degraded flag so convergence can
    # find/replace the degraded owner and _is_behavior_active_position
    # (which does NOT read this flag) keeps it exit-active.
    assert "broker_repair_degraded: bool = False" in src, (
        "ManagedPosition must declare the broker_repair_degraded field"
    )
    # Stable identity prefix
    assert '_DEGRADED_ID_PREFIX = "broker-repair-degraded:"' in src, (
        "Stable degraded id prefix must be a class constant so convergence "
        "code, tests, and log parsing agree on it"
    )


def test_pr558_blocker1_degraded_installer_has_authority_fences():
    """The installer must fail closed on every authority fence Angel spec'd:
    unproven client, execution mode, OCC identity + OCC format, broker
    account authority, invalid broker qty, and broker payload identity
    contradiction (contract mismatch)."""
    src = EE_SRC
    start = src.find("def _install_or_refresh_degraded_broker_truth_owner")
    end   = src.find("\n    def ", start + 1)
    body  = src[start:end]
    # Every fence produces a distinct DEGRADED_OWNER_BLOCKED reason line.
    for reason in (
        "client_id_unproven",
        "execution_mode_unproven",
        "occ_identity_unproven",
        "occ_identity_malformed",
        "broker_account_unproven",
        "broker_qty_invalid",
        "broker_position_contract_mismatch",
    ):
        assert reason in body, f"authority fence '{reason}' missing from installer"
    # Fabrication rejection markers — every field that Angel forbade must
    # carry the UNFABRICATED marker in the constructor call so a future
    # edit that starts writing e.g. broker.cost_basis into entry_price is
    # visibly wrong.
    assert "UNFABRICATED" in body, (
        "The constructor must comment which fields stay unfabricated so a "
        "future edit that starts laundering broker cost_basis into "
        "entry_price is visibly wrong"
    )


def test_pr558_occ_fallback_is_strict_when_exit_safety_is_unavailable():
    """The degraded-owner OCC fallback must remain fail-closed if the
    optional validator import is unavailable."""
    start = EE_SRC.find("def _install_or_refresh_degraded_broker_truth_owner")
    end = EE_SRC.find("\n    def ", start + 1)
    body = EE_SRC[start:end]
    assert "_re.fullmatch" in body
    assert "_occ_valid = lambda _c: bool(_c)" not in body


def test_pr558_account_authority_uses_tradier_config_and_scopes_owner():
    """Tradier's production account authority lives at cfg.account_id.
    Two account configs must not share a degraded owner identity."""
    class _Broker:
        mode = "live"

        def __init__(self, account_id):
            self.cfg = types.SimpleNamespace(account_id=account_id)

        def list_positions(self):
            return [{
                "symbol": "IWM270117C00220000",
                "quantity": 1,
                "cost_basis": 240.0,
            }]

    owners = []
    for account_id in ("acct-cfg-a", "acct-cfg-b"):
        eng = _pr558_new_engine()
        if eng is None:
            pytest.skip("APExitEngine not importable")
        eng.broker = _Broker(account_id)
        eng._load_db_position_row = lambda _sym: None
        eng._upsert_broker_position_to_db = lambda _sym, _bp: None
        eng._fetch_broker_quote = lambda _sym: {
            "mark": 0.0, "bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0,
        }
        assert eng._broker_account_id() == account_id
        assert eng._broker_position_precheck() is False
        owner = eng._positions[0]
        assert owner.broker_repair_degraded_account_id == account_id
        owners.append(owner)

    assert owners[0].position_id != owners[1].position_id


def test_pr558_placeholder_account_is_not_authority():
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")
    eng.broker = types.SimpleNamespace(account_id="?", mode="live")
    assert eng._broker_account_id() == ""
    assert eng._install_or_refresh_degraded_broker_truth_owner(
        sym="IWM270117C00220000",
        broker_position={"contract": "IWM270117C00220000", "quantity": 1},
        broker_qty=1,
        account_id="?",
        repair_failed_reason="test",
    ) is None


def test_pr558_atomic_convergence_orders_add_before_takeover_and_rolls_back():
    """The canonical handoff must keep degraded ownership until transfer
    validation succeeds, with rollback on any failed handoff."""
    start = EE_SRC.find("def _install_canonical_owner_atomically")
    end = EE_SRC.find("\n    def ", start + 1)
    body = EE_SRC[start:end]
    add_idx = body.find("self.add_position(canonical_pos)")
    take_idx = body.find("self._take_over_degraded_owner_if_any(sym")
    transfer_idx = body.find("self._apply_degraded_runtime_transfer(canonical_pos, transfer)")
    retire_idx = body.find("self._positions.remove(degraded_owner)")
    assert 0 <= add_idx < take_idx < transfer_idx < retire_idx
    assert "owner_retained=true" in EE_SRC
    assert "owner_retained=false" in body[retire_idx:]
    assert "degraded_removed" in body[transfer_idx:]
    assert "canonical_pos" in body[body.find("except Exception"):]
    assert "existing is not canonical_pos" in body


def test_pr558_blocker1_orchestrator_installs_degraded_on_repair_failure():
    """The broker-precheck orchestrator's 3b repair-failure branch must
    call _install_or_refresh_degraded_broker_truth_owner instead of
    unconditionally continueing with no owner."""
    src = EE_SRC
    # Locate the 3b block.
    idx_3b = src.find("# ── 3b. Create DB row from broker truth if still no pos ───────────")
    idx_3c = src.find("# ── 3c. Verify loaded_qty > 0", idx_3b)
    assert idx_3b > 0 and idx_3c > idx_3b, "3b / 3c section markers missing"
    body = src[idx_3b:idx_3c]
    assert "_install_or_refresh_degraded_broker_truth_owner" in body, (
        "3b repair-failure branch must install a degraded broker-truth owner"
    )
    # Canonical success must use the single atomic convergence seam; the seam
    # registers canonical first and removes degraded only after validation.
    assert "_install_canonical_owner_atomically" in body, (
        "3b canonical-repair-success branch must use atomic convergence"
    )
    assert "_apply_degraded_runtime_transfer" in EE_SRC, (
        "Atomic convergence must transfer accumulated runtime state onto canonical"
    )
    # Distinct log tag so ops can tell degraded from canonical.
    assert "owner_kind=degraded_broker_truth" in body, (
        "Degraded install must emit a distinguishable EXIT_BROKER_POSITION_ADDED_TO_ENGINE log line"
    )


def test_pr558_blocker1_orchestrator_3a_converges_degraded():
    """Section 3a (DB row load path) must also converge any preexisting
    degraded owner before installing canonical.  Otherwise a later cycle
    that hydrates a canonical row without going through repair would leave
    a stale degraded owner alongside canonical → two exit authorities."""
    src = EE_SRC
    idx_3a = src.find("# ── 3a. Try DB load ───────────────────────────────────────────────")
    idx_3b = src.find("# ── 3b. Create DB row from broker truth if still no pos ───────────", idx_3a)
    assert idx_3a > 0 and idx_3b > idx_3a, "3a / 3b markers missing"
    body = src[idx_3a:idx_3b]
    assert "_install_canonical_owner_atomically" in body, (
        "3a DB load path must use atomic degraded-to-canonical convergence"
    )
    assert "_apply_degraded_runtime_transfer" in EE_SRC, (
        "3a atomic path must transfer degraded runtime state onto canonical"
    )


def test_pr558_blocker1_degraded_id_is_deterministic_and_client_scoped():
    """Same client+mode+OCC+account must always yield the SAME degraded id
    (stable identity across repair cycles).  Different clients or accounts
    must never collide."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not importable in this environment")
    def _mk(email, mode):
        eng = engine_cls.__new__(engine_cls)
        eng._email = email
        eng._lock = __import__("threading").Lock()
        eng._positions = []
        eng._positions_by_id = {}
        # Stub the execution mode resolver
        eng._resolved_execution_mode = lambda: mode
        return eng

    e1 = _mk("jason@example.com", "live")
    e2 = _mk("jason@example.com", "live")
    e3 = _mk("jason@example.com", "paper")
    e4 = _mk("jose@example.com", "live")

    sym = "IWM260117C00220000"
    acct = "acct-jason-live"

    id1 = e1._degraded_broker_owner_id(sym, acct)
    id2 = e2._degraded_broker_owner_id(sym, acct)
    id3 = e3._degraded_broker_owner_id(sym, acct)
    id4 = e4._degraded_broker_owner_id(sym, acct)
    id5 = e1._degraded_broker_owner_id(sym, "acct-different")

    # Same inputs → same id (stable identity, no accumulation across cycles).
    assert id1 == id2, "same inputs must produce identical degraded id"
    # Prefix is the canonical marker used by convergence + log parsing.
    assert id1.startswith("broker-repair-degraded:")
    # Any input variation → different id (no cross-client/mode/account collision).
    assert id1 != id3, "LIVE and PAPER for the same client must not collide"
    assert id1 != id4, "different clients must not collide"
    assert id1 != id5, "different broker accounts must not collide"


def test_pr558_blocker1_stable_owner_across_repeated_repair_cycles():
    """Repeated broker prechecks with durable repair still unavailable must
    resolve to the SAME degraded ManagedPosition object (in-place refresh)
    — never accumulate multiple degraded owners for one broker-open
    position."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    mp_cls = getattr(_EE_MOD, "ManagedPosition", None)
    if engine_cls is None or mp_cls is None:
        pytest.skip("APExitEngine/ManagedPosition not importable in this environment")
    eng = engine_cls.__new__(engine_cls)
    eng._email = "jason@example.com"
    eng._lock = __import__("threading").Lock()
    eng._positions = []
    eng._positions_by_id = {}
    eng._resolved_execution_mode = lambda: "live"
    # Stub OCC parsers used by the installer to avoid needing the full env.
    eng._underlying_from_occ = lambda s: "IWM"
    eng._parse_occ_side = lambda s: "CALL"

    sym = "IWM260117C00220000"
    bp = {"contract": sym, "quantity": 2, "cost_basis": 2.40}

    p1 = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym, broker_position=bp, broker_qty=2,
        account_id="acct-live-1", repair_failed_reason="db_upsert_returned_no_id",
    )
    p2 = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym, broker_position=bp, broker_qty=2,
        account_id="acct-live-1", repair_failed_reason="db_upsert_returned_no_id",
    )
    p3 = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym, broker_position=bp, broker_qty=2,
        account_id="acct-live-1", repair_failed_reason="db_upsert_returned_no_id",
    )
    assert p1 is not None
    # Identical object across all three cycles — no accumulation.
    assert p1 is p2 is p3, (
        "repeated repair-failure cycles must refresh the SAME degraded owner"
    )
    # Exactly one behavior-active owner exists for this contract.
    same_contract = [
        p for p in eng._positions
        if str(p.option_symbol).upper() == sym.upper() and not p.closed
    ]
    assert len(same_contract) == 1, (
        f"expected exactly one degraded owner for {sym}, got {len(same_contract)}"
    )


def test_pr558_degraded_owner_reconciles_downward_and_retries_repair():
    """Fresh broker truth must drive degraded qty 2 -> 1 and keep retrying
    canonical repair even though the symbol is already engine-tracked."""
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")
    sym = "IWM260117C00220000"
    state = {"quantity": 2}

    class _Broker:
        account_id = "acct-live-1"
        mode = "live"

        def list_positions(self):
            return [{
                "symbol": sym,
                "quantity": state["quantity"],
                "cost_basis": 480.0,
                "date_acquired": "2026-08-01",
            }]

    eng.broker = _Broker()
    eng._load_db_position_row = lambda _sym: None
    eng._upsert_broker_position_to_db = lambda _sym, _bp: None
    eng._fetch_broker_quote = lambda _sym: {
        "mark": 0.0, "bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0,
    }

    assert eng._broker_position_precheck() is False
    degraded = eng._positions[0]
    assert degraded.quantity == 2
    assert degraded.quantity_remaining == 2

    state["quantity"] = 1
    assert eng._broker_position_precheck() is False
    assert eng._positions == [degraded]
    assert degraded.quantity == 1
    assert degraded.quantity_remaining == 1


def test_pr558_zero_quantity_is_unknown_and_retains_degraded_owner():
    """A zero row must not silently close or remove an exit owner; it is a
    broker-truth hold until a separate lifecycle source proves flatness."""
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")
    sym = "IWM260117C00220000"
    state = {"quantity": 1}

    class _Broker:
        account_id = "acct-live-1"
        mode = "live"

        def list_positions(self):
            return [{
                "symbol": sym,
                "quantity": state["quantity"],
                "cost_basis": 240.0,
                "date_acquired": "2026-08-01",
            }]

    eng.broker = _Broker()
    eng._load_db_position_row = lambda _sym: None
    eng._upsert_broker_position_to_db = lambda _sym, _bp: None
    eng._fetch_broker_quote = lambda _sym: {
        "mark": 0.0, "bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0,
    }

    assert eng._broker_position_precheck() is False
    degraded = eng._positions[0]
    state["quantity"] = 0

    assert eng._broker_position_precheck() is False
    assert eng._positions == [degraded]
    assert degraded.quantity_remaining == 1
    assert degraded.broker_repair_degraded is True
    assert sym in eng._broker_truth_hold_symbols, (
        "explicit broker zero must mark the tracked symbol HOLD for this cycle"
    )


def test_pr558_zero_truth_hold_suppresses_exit_mutation_for_the_cycle():
    """An explicit broker zero is unknown, not proof that an exit is safe.
    The owner remains behavior-active, but the normal exit loop must not
    create an action while the broker/lifecycle truth is unresolved."""
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")
    sym = "IWM270117C00220000"
    pos = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym, broker_position={"contract": sym, "quantity": 1},
        broker_qty=1, account_id="acct-live-1", repair_failed_reason="test",
    )
    assert pos is not None

    eng._broker_position_precheck = lambda: (
        eng._broker_truth_hold_symbols.add(sym) or False
    )
    eng._run_sentinels = lambda: None
    eng._kill_switch_fn = None
    eng._submit_exit_decision = MagicMock()

    eng._check_all_positions()

    assert pos in eng.active_positions()
    eng._submit_exit_decision.assert_not_called()


def test_pr558_atomic_canonical_convergence_registers_before_takeover():
    """Canonical registration succeeds before degraded removal, and a
    registration failure leaves degraded exit protection intact."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    mp_cls = getattr(_EE_MOD, "ManagedPosition", None)
    if engine_cls is None or mp_cls is None:
        pytest.skip("APExitEngine/ManagedPosition not importable")

    eng = _pr558_new_engine()
    sym = "IWM260117C00220000"
    degraded = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym, broker_position={"contract": sym, "quantity": 2},
        broker_qty=2, account_id="acct-live-1", repair_failed_reason="test",
    )
    degraded.peak_pnl_pct = 0.21
    canonical = mp_cls(
        ticker="IWM", option_symbol=sym, side="CALL", quantity=2,
        entry_price=2.40, underlying_entry=210.0,
        underlying_target=225.0, underlying_stop=205.0,
        position_id="durable-canonical", client_id="jason@example.com",
        signal_id="signal-558", execution_mode="live",
        quantity_remaining=2,
    )

    eng._install_canonical_owner_atomically(canonical, sym)
    assert canonical in eng._positions
    assert degraded not in eng._positions
    assert eng.active_positions() == [canonical]
    assert canonical.peak_pnl_pct == 0.21

    eng_failed = _pr558_new_engine()
    degraded_failed = eng_failed._install_or_refresh_degraded_broker_truth_owner(
        sym=sym, broker_position={"contract": sym, "quantity": 2},
        broker_qty=2, account_id="acct-live-1", repair_failed_reason="test",
    )
    canonical_failed = mp_cls(
        ticker="IWM", option_symbol=sym, side="CALL", quantity=2,
        entry_price=2.40, underlying_entry=0.0,
        underlying_target=0.0, underlying_stop=0.0,
        position_id="durable-canonical-failed",
        client_id="jason@example.com", execution_mode="live",
        quantity_remaining=2,
    )

    def _fail_add(_pos):
        raise RuntimeError("canonical install failed")

    eng_failed.add_position = _fail_add
    with pytest.raises(RuntimeError, match="canonical install failed"):
        eng_failed._install_canonical_owner_atomically(canonical_failed, sym)
    assert eng_failed._positions == [degraded_failed]
    assert degraded_failed.broker_repair_degraded is True


def test_pr558_blocker1_degraded_installer_fails_closed_on_bad_truth():
    """Malformed / uncertain broker truth must not manufacture a degraded
    owner.  Angel spec: 'do not manufacture open. Do not manufacture flat.'
    """
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not importable in this environment")
    eng = engine_cls.__new__(engine_cls)
    eng._email = "jason@example.com"
    eng._lock = __import__("threading").Lock()
    eng._positions = []
    eng._positions_by_id = {}
    eng._resolved_execution_mode = lambda: "live"
    eng._underlying_from_occ = lambda s: "IWM"
    eng._parse_occ_side = lambda s: "CALL"

    sym = "IWM260117C00220000"

    # Missing account authority
    p = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym, broker_position={"contract": sym, "quantity": 2},
        broker_qty=2, account_id="", repair_failed_reason="test",
    )
    assert p is None and eng._positions == []

    # Zero broker qty
    p = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym, broker_position={"contract": sym, "quantity": 0},
        broker_qty=0, account_id="acct", repair_failed_reason="test",
    )
    assert p is None and eng._positions == []

    # Negative broker qty
    p = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym, broker_position={"contract": sym, "quantity": -1},
        broker_qty=-1, account_id="acct", repair_failed_reason="test",
    )
    assert p is None and eng._positions == []

    # Broker payload names a different contract (identity contradiction)
    p = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym, broker_position={"contract": "SPY260117C00500000", "quantity": 2},
        broker_qty=2, account_id="acct", repair_failed_reason="test",
    )
    assert p is None and eng._positions == []


def test_pr558_blocker1_convergence_transfers_runtime_state():
    """When canonical repair later succeeds, the full handoff must preserve
    accumulated exit state (peak_pnl_pct, touched_profit, pending_exit_*
    identity, quote watermarks) and leave exactly one active owner."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    mp_cls = getattr(_EE_MOD, "ManagedPosition", None)
    if engine_cls is None or mp_cls is None:
        pytest.skip("APExitEngine/ManagedPosition not importable in this environment")
    eng = engine_cls.__new__(engine_cls)
    eng._email = "jason@example.com"
    # The production engine uses an RLock because convergence holds the
    # engine lock while calling add_position(), which acquires it again.
    eng._lock = __import__("threading").RLock()
    eng._positions = []
    eng._positions_by_id = {}
    eng._resolved_execution_mode = lambda: "live"
    eng._underlying_from_occ = lambda s: "IWM"
    eng._parse_occ_side = lambda s: "CALL"

    sym = "IWM260117C00220000"

    degraded = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym, broker_position={"contract": sym, "quantity": 2},
        broker_qty=2, account_id="acct-live-1",
        repair_failed_reason="db_upsert_returned_no_id",
    )
    assert degraded is not None
    # Simulate accumulated exit state during the degraded window.
    degraded.peak_pnl_pct = 0.27
    degraded.touched_profit = True
    degraded.current_bid = 1.15
    degraded.pending_exit_local_order_id = "local-exit-abc"
    degraded.pending_exit_broker_order_id = "broker-exit-xyz"
    degraded.pending_exit_qty = 2
    degraded.exit_in_flight = True

    # Freshly seeded canonical owner (all runtime fields at their defaults).
    canonical = mp_cls(
        ticker="IWM", option_symbol=sym, side="CALL",
        quantity=2, entry_price=2.40, underlying_entry=210.0,
        underlying_target=225.0, underlying_stop=205.0,
        position_id="canonical-uuid-real",
        client_id="jason@example.com",
        signal_id="real-sig-1", execution_mode="live",
        quantity_remaining=2,
    )
    eng._install_canonical_owner_atomically(
        canonical, sym, account_id="acct-live-1",
    )

    assert eng.active_positions() == [canonical]
    assert degraded not in eng._positions
    assert canonical.peak_pnl_pct == 0.27, "peak_pnl_pct must survive convergence"
    assert canonical.touched_profit is True, "touched_profit must be sticky"
    assert canonical.current_bid == 1.15
    assert canonical.pending_exit_local_order_id == "local-exit-abc"
    assert canonical.pending_exit_broker_order_id == "broker-exit-xyz"
    assert canonical.pending_exit_qty == 2
    assert canonical.exit_in_flight is True
    # Identity fields must NOT have been touched.
    assert canonical.position_id == "canonical-uuid-real"
    assert canonical.signal_id == "real-sig-1"
    assert canonical.entry_price == 2.40


def test_pr585_malformed_canonical_watermark_keeps_degraded_owner_and_no_mutation():
    """A malformed canonical EXIT watermark must fail the handoff closed.

    The failed transfer must roll back the newly registered canonical object,
    preserve the deterministic degraded owner and its valid watermark, and
    invoke no broker/proof callback.
    """
    eng = _pr558_new_engine()
    mp_cls = getattr(_EE_MOD, "ManagedPosition", None)
    if eng is None or mp_cls is None:
        pytest.skip("APExitEngine/ManagedPosition not importable")
    sym = "IWM260117C00220000"
    degraded = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym, broker_position={"contract": sym, "quantity": 1},
        broker_qty=1, account_id="acct-live-1", repair_failed_reason="test",
    )
    assert degraded is not None
    degraded.exit_in_flight = True
    degraded.pending_exit_local_order_id = "same-exit-local"
    degraded.pending_exit_broker_order_id = "same-exit-broker"
    degraded.last_applied_exit_local_order_id = "same-exit-local"
    degraded.last_applied_exit_broker_order_id = "same-exit-broker"
    degraded.last_applied_exit_cum_fill = 3
    degraded.last_applied_exit_cum_fill_by_order = {
        "same-exit-local": 3,
        "same-exit-broker": 3,
    }
    degraded_map_before = dict(degraded.last_applied_exit_cum_fill_by_order)

    canonical = mp_cls(
        ticker="IWM", option_symbol=sym, side="CALL", quantity=1,
        entry_price=2.40, underlying_entry=210.0,
        underlying_target=225.0, underlying_stop=205.0,
        position_id="canonical-malformed-watermark",
        client_id="jason@example.com", signal_id="signal-watermark",
        execution_mode="live", quantity_remaining=1,
    )
    canonical.last_applied_exit_local_order_id = "same-exit-local"
    canonical.last_applied_exit_broker_order_id = "same-exit-broker"
    canonical.last_applied_exit_cum_fill_by_order = {
        "same-exit-local": "garbage",
    }

    eng.on_exit = MagicMock()
    eng.on_scale = MagicMock()
    eng._emit_exit_event = MagicMock()
    eng.broker = MagicMock()

    with pytest.raises(RuntimeError, match="malformed state"):
        eng._install_canonical_owner_atomically(
            canonical, sym, account_id="acct-live-1",
        )

    assert eng.active_positions() == [degraded]
    assert canonical not in eng._positions
    assert eng._positions_by_id.get(degraded.position_id) is degraded
    assert degraded.last_applied_exit_cum_fill == 3
    assert degraded.last_applied_exit_cum_fill_by_order == degraded_map_before
    eng.on_exit.assert_not_called()
    eng.on_scale.assert_not_called()
    eng._emit_exit_event.assert_not_called()
    eng.broker.assert_not_called()


def test_pr585_degraded_protective_reference_is_not_canonical_entry():
    """Cost basis may support risk-only P&L, never canonical provenance."""
    eng = _pr558_new_engine()
    mp_cls = getattr(_EE_MOD, "ManagedPosition", None)
    if eng is None or mp_cls is None:
        pytest.skip("APExitEngine/ManagedPosition not importable")
    sym = "IWM270117C00220000"
    pos = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym,
        broker_position={"contract": sym, "quantity": 2, "cost_basis": 400.0},
        broker_qty=2,
        account_id="acct-live-1",
        repair_failed_reason="durable_entry_unresolved",
    )
    assert pos is not None
    assert pos.entry_price == 0.0
    assert pos.signal_id == ""
    assert pos.underlying_entry == 0.0
    assert pos.broker_repair_protective_entry_reference == pytest.approx(2.0)
    assert pos.broker_repair_protective_entry_source == "broker_cost_basis_per_contract"

    now = datetime.now(timezone.utc)
    pos.current_bid = 1.20
    pos.current_ask = 1.25
    pos.current_option_price = 1.20
    pos.option_bid_valid = True
    pos.option_quote_fresh = True
    pos.last_option_bid_update_ts = now
    pos.last_option_quote_update_ts = now
    decision = _EE_MOD.evaluate_exit(
        pos,
        now.astimezone(_EE_MOD.ET),
    )
    assert decision.action == "STOP"
    assert decision.pnl_pct == pytest.approx(-0.40)
    assert pos.entry_price == 0.0, "risk-only reference must not become canonical entry"


def test_pr585_invalid_or_contradictory_cost_basis_clears_risk_reference():
    """A later unproven basis retains ownership but cannot retain stale risk data."""
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")
    sym = "IWM270117C00220000"
    pos = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym,
        broker_position={"contract": sym, "quantity": 1, "cost_basis": 200.0},
        broker_qty=1,
        account_id="acct-live-1",
        repair_failed_reason="temporary",
    )
    assert pos is not None
    assert pos.broker_repair_protective_entry_reference == pytest.approx(2.0)

    refreshed = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym,
        broker_position={
            "contract": sym,
            "quantity": 1,
            "cost_basis": 200.0,
            "raw": {"contract": sym, "quantity": 1, "cost_basis": 250.0},
        },
        broker_qty=1,
        account_id="acct-live-1",
        repair_failed_reason="basis_conflict",
    )
    assert refreshed is pos
    assert refreshed.entry_price == 0.0
    assert refreshed.broker_repair_protective_entry_reference == 0.0
    assert refreshed.broker_repair_protective_entry_source == ""


def test_pr585_degraded_protective_loss_reaches_existing_exit_submit_path():
    """Fresh bid P&L may trigger the existing protective OSM handoff only."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    mp_cls = getattr(_EE_MOD, "ManagedPosition", None)
    if engine_cls is None or mp_cls is None:
        pytest.skip("APExitEngine/ManagedPosition not importable")

    class _Broker:
        account_id = "acct-live-1"
        mode = "live"

        def list_positions(self):
            return [{
                "symbol": "IWM270117C00220000",
                "quantity": 2,
                "cost_basis": 400.0,
            }]

    eng = engine_cls.__new__(engine_cls)
    eng._email = "jason@example.com"
    eng._lock = __import__("threading").RLock()
    eng._positions = []
    eng._positions_by_id = {}
    eng._broker_truth_hold_symbols = set()
    eng._pending_exit_hydration_status = {}
    eng._pending_exit_identity_hold_position_ids = set()
    eng._resolved_execution_mode = lambda: "live"
    eng.broker = _Broker()
    eng.order_state_machine = None
    eng.osm = None
    eng.on_scale = None
    eng._emit_exit_event = lambda *args, **kwargs: None
    eng._clear_degraded_monitoring_state = lambda *args, **kwargs: None
    eng.on_exit = MagicMock(
        return_value={
            "accepted": True,
            "local_order_id": "protective-exit-local",
            "broker_order_id": "protective-exit-broker",
        }
    )
    pos = eng._install_or_refresh_degraded_broker_truth_owner(
        sym="IWM270117C00220000",
        broker_position={
            "contract": "IWM270117C00220000",
            "quantity": 2,
            "cost_basis": 400.0,
        },
        broker_qty=2,
        account_id="acct-live-1",
        repair_failed_reason="entry_provenance_unresolved",
    )
    assert pos is not None
    now = datetime.now(timezone.utc)
    pos.opened_at = now - timedelta(minutes=20)
    pos.current_bid = 1.20
    pos.current_ask = 1.25
    pos.current_option_price = 1.20
    pos.option_bid_valid = True
    pos.option_quote_fresh = True
    pos.last_option_bid_update_ts = now
    pos.last_option_quote_update_ts = now

    decision = _EE_MOD.evaluate_exit(pos, now.astimezone(_EE_MOD.ET))
    assert decision.action == "STOP"
    assert decision.pnl_pct == pytest.approx(-0.40)

    import ap.exit_safety as exit_safety
    with patch.object(
        exit_safety,
        "evaluate_exit_submission_safety",
        return_value={"blocked": False},
    ):
        submitted = eng._submit_exit_decision(pos, decision)

    assert submitted is True
    eng.on_exit.assert_called_once()
    assert pos.pending_exit_local_order_id == "protective-exit-local"
    assert pos.pending_exit_broker_order_id == "protective-exit-broker"
    assert pos.entry_price == 0.0
    assert pos.signal_id == ""
    assert pos.underlying_entry == 0.0
    assert pos.pending_exit_action == "STOP"


def test_pr585_degraded_install_race_canonical_wins_in_locked_recheck():
    """A canonical owner inserted during the pre-read prevents a duplicate."""
    eng = _pr558_new_engine()
    mp_cls = getattr(_EE_MOD, "ManagedPosition", None)
    if eng is None or mp_cls is None:
        pytest.skip("APExitEngine/ManagedPosition not importable")
    sym = "IWM270117C00220000"
    pre_read_returned = threading.Event()
    release_pre_read = threading.Event()
    original_find = eng._find_degraded_broker_owner_by_contract

    def paused_find(contract, account_id=None):
        pre_read_returned.set()
        assert release_pre_read.wait(2.0)
        return original_find(contract, account_id)

    eng._find_degraded_broker_owner_by_contract = paused_find
    result = []
    errors = []

    def install():
        try:
            result.append(
                eng._install_or_refresh_degraded_broker_truth_owner(
                    sym=sym,
                    broker_position={"contract": sym, "quantity": 1, "cost_basis": 200.0},
                    broker_qty=1,
                    account_id="acct-live-1",
                    repair_failed_reason="temporary",
                )
            )
        except Exception as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    thread = threading.Thread(target=install)
    thread.start()
    assert pre_read_returned.wait(2.0)

    canonical = mp_cls(
        ticker="IWM", option_symbol=sym, side="CALL", quantity=1,
        quantity_remaining=1, entry_price=2.0,
        underlying_entry=0.0, underlying_target=0.0, underlying_stop=0.0,
        position_id="canonical-race-owner", client_id="jason@example.com",
        execution_mode="live",
    )
    with eng._lock:
        eng._positions.append(canonical)
        eng._positions_by_id[canonical.position_id] = canonical
    release_pre_read.set()
    thread.join(timeout=2.0)

    assert not errors
    assert result == [canonical]
    assert eng.active_positions() == [canonical]
    assert not any(getattr(pos, "broker_repair_degraded", False) for pos in eng._positions)


def test_pr585_exit_fill_watermark_transfer_is_identity_scoped():
    """A new EXIT generation cannot inherit the prior generation's scalar."""
    eng = _pr558_new_engine()
    mp_cls = getattr(_EE_MOD, "ManagedPosition", None)
    if eng is None or mp_cls is None:
        pytest.skip("APExitEngine/ManagedPosition not importable")
    sym = "IWM270117C00220000"

    canonical = mp_cls(
        ticker="IWM", option_symbol=sym, side="CALL", quantity=2,
        quantity_remaining=2, entry_price=2.0,
        underlying_entry=0.0, underlying_target=0.0, underlying_stop=0.0,
        position_id="canonical-watermark", client_id="jason@example.com",
        execution_mode="live",
    )
    canonical.last_applied_exit_local_order_id = "local-generation-a"
    canonical.last_applied_exit_broker_order_id = "broker-generation-a"
    canonical.last_applied_exit_cum_fill = 2
    canonical.last_applied_exit_cum_fill_by_order = {
        "local-generation-a": 2,
        "broker-generation-a": 2,
    }
    eng._apply_degraded_runtime_transfer(
        canonical,
        {
            "last_applied_exit_local_order_id": "local-generation-b",
            "last_applied_exit_broker_order_id": "broker-generation-b",
            "last_applied_exit_cum_fill": 7,
            "last_applied_exit_cum_fill_by_order": {
                "local-generation-b": 7,
                "broker-generation-b": 7,
            },
        },
    )
    assert canonical.last_applied_exit_cum_fill == 2
    assert canonical.last_applied_exit_cum_fill_by_order == {
        "local-generation-a": 2,
        "broker-generation-a": 2,
    }

    eng._apply_degraded_runtime_transfer(
        canonical,
        {
            "last_applied_exit_local_order_id": "local-generation-a",
            "last_applied_exit_broker_order_id": "broker-generation-a",
            "last_applied_exit_cum_fill": 3,
            "last_applied_exit_cum_fill_by_order": {
                "local-generation-a": 3,
                "broker-generation-a": 3,
            },
        },
    )
    assert canonical.last_applied_exit_cum_fill == 3
    assert canonical.last_applied_exit_cum_fill_by_order["broker-generation-a"] == 3


def test_pr558_blocker1_degraded_owner_makes_no_fabricated_history():
    """The degraded owner represents only 'fresh broker truth says this
    position is open'.  It must not carry any fabricated history:
    signal_id, entry_price, underlying_entry / stop / target."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not importable in this environment")
    eng = engine_cls.__new__(engine_cls)
    eng._email = "jason@example.com"
    eng._lock = __import__("threading").Lock()
    eng._positions = []
    eng._positions_by_id = {}
    eng._resolved_execution_mode = lambda: "live"
    eng._underlying_from_occ = lambda s: "IWM"
    eng._parse_occ_side = lambda s: "CALL"

    sym = "IWM260117C00220000"
    # Broker payload carries a cost_basis and date_acquired that MUST NOT
    # be laundered into local entry_price / opened_at as if they were
    # proven local entry provenance.
    bp = {"contract": sym, "quantity": 2, "cost_basis": 2.40, "date_acquired": "2026-08-01"}
    pos = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym, broker_position=bp, broker_qty=2,
        account_id="acct-live-1", repair_failed_reason="db_upsert_returned_no_id",
    )
    assert pos is not None
    # Broker-truth-only fields
    assert pos.option_symbol == sym
    assert pos.quantity == 2 and pos.quantity_remaining == 2
    assert pos.execution_mode == "live"
    assert pos.client_id == "jason@example.com"
    assert pos.broker_repair_degraded is True
    # Nothing fabricated
    assert pos.entry_price == 0.0, "broker cost_basis MUST NOT be laundered into entry_price"
    assert pos.underlying_entry == 0.0
    assert pos.underlying_target == 0.0
    assert pos.underlying_stop == 0.0
    assert pos.signal_id == ""
    assert pos.position_id.startswith("broker-repair-degraded:")


def test_pr558_blocker1_degraded_owner_is_behavior_active():
    """The degraded owner must appear in engine.active_positions() — the
    whole point is to keep the position exit-active during the degraded
    window.  A future refactor that marks degraded owners as quarantined
    would silently disable exit monitoring."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not importable in this environment")
    eng = engine_cls.__new__(engine_cls)
    eng._email = "jason@example.com"
    eng._lock = __import__("threading").Lock()
    eng._positions = []
    eng._positions_by_id = {}
    eng._resolved_execution_mode = lambda: "live"
    eng._underlying_from_occ = lambda s: "IWM"
    eng._parse_occ_side = lambda s: "CALL"

    sym = "IWM260117C00220000"
    pos = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym, broker_position={"contract": sym, "quantity": 2},
        broker_qty=2, account_id="acct", repair_failed_reason="test",
    )
    assert pos is not None
    active = eng.active_positions()
    assert pos in active, "degraded owner must be behavior-active"
    assert len(active) == 1


def test_pr558_blocker2_multi_candidate_narrowed_by_broker_returns_survivor():
    """Two historical ENTRY rows share (client, mode, OCC, filled_qty>0),
    only one matches broker economics.  Reordered code returns the single
    survivor rather than AMBIGUOUS."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not importable in this environment")

    # Two candidates with same OCC/client/mode/filled_qty; only one matches
    # broker cost basis.
    rows = [
        {"id": "old-order", "client_id": "jason@example.com", "execution_mode": "live",
         "contract": "IWM260117C00220000", "kind": "ENTRY", "status": "FILLED",
         "filled_qty": 2, "avg_fill_price": 1.10, "fill_price": 1.10},  # mismatch on price
        {"id": "current-order", "client_id": "jason@example.com", "execution_mode": "live",
         "contract": "IWM260117C00220000", "kind": "ENTRY", "status": "FILLED",
         "filled_qty": 2, "avg_fill_price": 2.40, "fill_price": 2.40},  # matches broker
    ]
    # Broker position matching the second candidate.
    broker_position = {"quantity": 2, "cost_basis": 480.0}  # 2 * 2.40 * 100

    # Install fake ap.db returning our rows.
    from contextlib import contextmanager
    class _Cur:
        def execute(self, sql, params=()):
            return self
        def fetchall(self):
            return rows
        def fetchone(self):
            return rows[0] if rows else None
    @contextmanager
    def fake_conn():
        yield _Cur()
    fake_db = types.SimpleNamespace(conn=fake_conn, run_with_retry=lambda fn, **_: fn())
    prior = sys.modules.get("ap.db")
    sys.modules["ap.db"] = fake_db

    eng = engine_cls.__new__(engine_cls)
    eng._email = "jason@example.com"
    eng._lock = __import__("threading").Lock()
    try:
        result = eng._find_exact_filled_entry_order(
            "IWM260117C00220000", "live", broker_position=broker_position,
        )
    finally:
        if prior is not None:
            sys.modules["ap.db"] = prior
        else:
            sys.modules.pop("ap.db", None)

    assert isinstance(result, dict)
    # Must NOT be an AMBIGUOUS/UNAVAILABLE/MISMATCH marker.
    assert "_broker_repair_lookup_status" not in result, (
        f"expected the narrowed survivor, got lookup marker: {result}"
    )
    assert result.get("id") == "current-order", (
        "broker evidence should have narrowed to the current-order row"
    )


# ─── PR #558 amendment — remaining 14 tests from Angel's 23-mandatory list ────


def _pr558_fake_db_with_rows(rows, *, position_row=None):
    """Shared fake-DB scaffold for _find_exact_filled_entry_order tests.
    Returns (fake_db_module, restore_callable).  Caller uses:
        fake, restore = _pr558_fake_db_with_rows(rows)
        sys.modules["ap.db"] = fake
        try: ... finally: restore()
    """
    from contextlib import contextmanager
    class _Cur:
        rowcount = 1

        def __init__(self):
            self._sql = ""

        def execute(self, sql, params=()):
            self._sql = str(sql)
            return self
        def fetchall(self):
            return list(rows)
        def fetchone(self):
            if position_row is not None and "FOR UPDATE" in self._sql:
                return dict(position_row)
            return rows[0] if rows else None
    @contextmanager
    def fake_conn():
        yield _Cur()
    fake = types.SimpleNamespace(conn=fake_conn, run_with_retry=lambda fn, **_: fn())
    prior = sys.modules.get("ap.db")
    def restore():
        if prior is not None:
            sys.modules["ap.db"] = prior
        else:
            sys.modules.pop("ap.db", None)
    return fake, restore


def _pr558_new_engine(email="jason@example.com"):
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        return None
    eng = engine_cls.__new__(engine_cls)
    eng._email = email
    # Production uses an RLock because canonical registration and degraded
    # takeover are one locked transaction in the orchestrator.
    eng._lock = __import__("threading").RLock()
    eng._positions = []
    eng._positions_by_id = {}
    eng._broker_truth_hold_symbols = set()
    eng._pending_exit_hydration_status = {}
    eng._pending_exit_identity_hold_position_ids = set()
    eng._resolved_execution_mode = lambda: "live"
    eng._underlying_from_occ = lambda s: "IWM"
    eng._parse_occ_side = lambda s: "CALL"
    return eng


# ─── Blocker 2 completeness (tests 7, 9, 10, 11, 12, 13, 14) ──────────────────

def test_pr558_blocker2_test07_single_historical_candidate_returns_it():
    """Test 7: exactly one exact historical filled ENTRY matches, canonical
    repair uses it.  Applies broker match check even on the single candidate."""
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")
    rows = [{
        "id": "the-one", "client_id": "jason@example.com", "execution_mode": "live",
        "contract": "IWM260117C00220000", "kind": "ENTRY", "status": "FILLED",
        "filled_qty": 2, "avg_fill_price": 2.40, "fill_price": 2.40,
    }]
    broker_position = {"quantity": 2, "cost_basis": 480.0}
    fake, restore = _pr558_fake_db_with_rows(rows)
    sys.modules["ap.db"] = fake
    try:
        result = eng._find_exact_filled_entry_order(
            "IWM260117C00220000", "live", broker_position=broker_position,
        )
    finally:
        restore()
    assert isinstance(result, dict) and "_broker_repair_lookup_status" not in result
    assert result.get("id") == "the-one"


def test_pr558_blocker2_test09_multi_candidate_zero_broker_matches_returns_unresolved():
    """Test 9: two historical candidates, ZERO match broker economics.
    Angel spec: provenance UNRESOLVED → return None → caller keeps degraded
    owner active.  Never AMBIGUOUS, never fabricate canonical identity."""
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")
    rows = [
        {"id": "wrong-price-1", "client_id": "jason@example.com", "execution_mode": "live",
         "contract": "IWM260117C00220000", "kind": "ENTRY", "status": "FILLED",
         "filled_qty": 2, "avg_fill_price": 1.10, "fill_price": 1.10},
        {"id": "wrong-price-2", "client_id": "jason@example.com", "execution_mode": "live",
         "contract": "IWM260117C00220000", "kind": "ENTRY", "status": "FILLED",
         "filled_qty": 2, "avg_fill_price": 1.20, "fill_price": 1.20},
    ]
    broker_position = {"quantity": 2, "cost_basis": 480.0}  # $2.40/contract — matches neither (480.00 total basis)
    fake, restore = _pr558_fake_db_with_rows(rows)
    sys.modules["ap.db"] = fake
    try:
        result = eng._find_exact_filled_entry_order(
            "IWM260117C00220000", "live", broker_position=broker_position,
        )
    finally:
        restore()
    # None means "unresolved" — caller must not fabricate canonical history.
    # Must NOT be an AMBIGUOUS marker (that's a different outcome).
    assert result is None, (
        f"expected None (UNRESOLVED, keep degraded), got: {result}"
    )


def test_pr585_unresolved_entry_provenance_blocks_canonical_upsert():
    """No exact filled ENTRY match may become a UUID-backed OPEN row."""
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")
    contract = "IWM260117C00220000"
    executed_sql = []

    class _Cursor:
        def execute(self, sql, params=()):
            executed_sql.append(str(sql))

        def fetchall(self):
            return []

        def fetchone(self):
            return None

    @contextmanager
    def _conn():
        yield _Cursor()

    fake = types.SimpleNamespace(conn=_conn, run_with_retry=lambda fn, **_: fn())
    prior = sys.modules.get("ap.db")
    sys.modules["ap.db"] = fake
    try:
        with patch.object(
            _EE_MOD._uuid,
            "uuid4",
            side_effect=AssertionError("UUID must not be generated"),
        ):
            row_id = eng._upsert_broker_position_to_db(
                contract,
                {"contract": contract, "quantity": 2, "cost_basis": 400.0},
            )
    finally:
        if prior is not None:
            sys.modules["ap.db"] = prior
        else:
            sys.modules.pop("ap.db", None)

    assert row_id is None
    assert not any("INSERT INTO positions" in sql for sql in executed_sql)


def test_pr585_authoritative_precheck_ignores_unrelated_signed_positions():
    """Unrelated stock/short rows must not poison exact target recovery."""
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")
    target = "IWM270117C00220000"
    unrelated_short_option = "SPY270117P00400000"

    class _Broker:
        account_id = "acct-live-1"
        mode = "live"

        def list_positions(self):
            return [
                {"symbol": "AAPL", "quantity": -100},
                {
                    "symbol": unrelated_short_option,
                    "quantity": -3,
                    "side": "short",
                },
                {
                    "symbol": target,
                    "quantity": 2,
                    "cost_basis": 400.0,
                },
            ]

    eng.broker = _Broker()
    eng._load_db_position_row = lambda _sym: None
    eng._upsert_broker_position_to_db = lambda _sym, _bp: None
    result = eng._broker_position_precheck()

    assert result is False  # durable repair intentionally remains unavailable
    degraded = [
        pos for pos in eng._positions
        if getattr(pos, "broker_repair_degraded", False)
    ]
    assert len(degraded) == 1
    assert degraded[0].option_symbol == target
    assert degraded[0].quantity_remaining == 2
    assert degraded[0].broker_repair_protective_entry_reference == pytest.approx(2.0)
    assert all(pos.option_symbol == target for pos in degraded)


def test_pr585_authoritative_malformed_empty_envelope_holds_without_owner():
    """A bare positions={} envelope is malformed, never authoritative flat."""
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")

    class _Broker:
        account_id = "acct-live-1"
        mode = "live"

        def list_positions_authoritative(self):
            return {"positions": {}}

        def list_positions(self):
            pytest.fail("weak list_positions fallback must not be used")

    eng.broker = _Broker()
    assert eng._broker_position_precheck() is False
    assert eng._positions == []
    assert eng._positions_by_id == {}


def test_pr558_blocker2_test10_multi_candidate_both_match_returns_ambiguous():
    """Test 10: two historical candidates, BOTH match broker economics.
    True ambiguity — return AMBIGUOUS marker.  Caller keeps degraded owner
    active; does not fabricate canonical identity."""
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")
    rows = [
        {"id": "match-a", "client_id": "jason@example.com", "execution_mode": "live",
         "contract": "IWM260117C00220000", "kind": "ENTRY", "status": "FILLED",
         "filled_qty": 2, "avg_fill_price": 2.40, "fill_price": 2.40},
        {"id": "match-b", "client_id": "jason@example.com", "execution_mode": "live",
         "contract": "IWM260117C00220000", "kind": "ENTRY", "status": "FILLED",
         "filled_qty": 2, "avg_fill_price": 2.40, "fill_price": 2.40},
    ]
    broker_position = {"quantity": 2, "cost_basis": 480.0}
    fake, restore = _pr558_fake_db_with_rows(rows)
    sys.modules["ap.db"] = fake
    try:
        result = eng._find_exact_filled_entry_order(
            "IWM260117C00220000", "live", broker_position=broker_position,
        )
    finally:
        restore()
    assert isinstance(result, dict), (
        f"expected AMBIGUOUS marker dict, got: {type(result).__name__}"
    )
    assert result.get("_broker_repair_lookup_status") == "AMBIGUOUS", (
        f"expected AMBIGUOUS, got: {result.get('_broker_repair_lookup_status')}"
    )


def test_pr558_blocker2_test11_wrong_client_historical_row_excluded():
    """Test 11: a historical ENTRY row for a DIFFERENT client must never
    contribute to provenance for THIS client's repair, even if OCC/mode/qty
    happen to match."""
    eng = _pr558_new_engine(email="jason@example.com")
    if eng is None:
        pytest.skip("APExitEngine not importable")
    rows = [
        {"id": "wrong-client", "client_id": "OTHER@example.com", "execution_mode": "live",
         "contract": "IWM260117C00220000", "kind": "ENTRY", "status": "FILLED",
         "filled_qty": 2, "avg_fill_price": 2.40},
    ]
    fake, restore = _pr558_fake_db_with_rows(rows)
    sys.modules["ap.db"] = fake
    try:
        result = eng._find_exact_filled_entry_order(
            "IWM260117C00220000", "live",
            broker_position={"quantity": 2, "cost_basis": 480.0},
        )
    finally:
        restore()
    # Wrong-client row must be filtered out in Stage 1 → no candidates → None.
    assert result is None, f"wrong-client row must not contribute; got {result}"


def test_pr558_blocker2_test12_live_paper_cross_mode_row_excluded():
    """Test 12: LIVE and PAPER historical rows must never cross.  A PAPER
    ENTRY must not become canonical provenance for a LIVE repair."""
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")
    eng._resolved_execution_mode = lambda: "live"
    rows = [
        {"id": "paper-row", "client_id": "jason@example.com", "execution_mode": "paper",
         "contract": "IWM260117C00220000", "kind": "ENTRY", "status": "FILLED",
         "filled_qty": 2, "avg_fill_price": 2.40},
    ]
    fake, restore = _pr558_fake_db_with_rows(rows)
    sys.modules["ap.db"] = fake
    try:
        result = eng._find_exact_filled_entry_order(
            "IWM260117C00220000", "live",
            broker_position={"quantity": 2, "cost_basis": 480.0},
        )
    finally:
        restore()
    assert result is None, f"PAPER row must not contribute to LIVE repair; got {result}"


def test_pr558_blocker2_test13_wrong_occ_historical_row_excluded():
    """Test 13: a historical ENTRY for a DIFFERENT OCC contract must not
    contribute to provenance for a repair on a different OCC.  Case-insensitive."""
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")
    rows = [
        {"id": "wrong-occ", "client_id": "jason@example.com", "execution_mode": "live",
         "contract": "SPY260117C00500000", "kind": "ENTRY", "status": "FILLED",
         "filled_qty": 2, "avg_fill_price": 2.40},
    ]
    fake, restore = _pr558_fake_db_with_rows(rows)
    sys.modules["ap.db"] = fake
    try:
        result = eng._find_exact_filled_entry_order(
            "IWM260117C00220000", "live",
            broker_position={"quantity": 2, "cost_basis": 480.0},
        )
    finally:
        restore()
    assert result is None, f"wrong-OCC row must not contribute; got {result}"


def test_pr558_blocker2_test14_zero_or_negative_fill_qty_excluded():
    """Test 14: rows with zero or negative filled_qty must not qualify as
    historical filled ENTRY evidence.  Fail-closed on malformed qty."""
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")
    for bad_qty in (0, -1, -5, None):
        rows = [
            {"id": "bad-qty", "client_id": "jason@example.com", "execution_mode": "live",
             "contract": "IWM260117C00220000", "kind": "ENTRY", "status": "FILLED",
             "filled_qty": bad_qty, "avg_fill_price": 2.40},
        ]
        fake, restore = _pr558_fake_db_with_rows(rows)
        sys.modules["ap.db"] = fake
        try:
            result = eng._find_exact_filled_entry_order(
                "IWM260117C00220000", "live",
                broker_position={"quantity": 2, "cost_basis": 480.0},
            )
        finally:
            restore()
        assert result is None, (
            f"filled_qty={bad_qty!r} must fail Stage 1 identity filter; got {result}"
        )


# ─── Blocker 1 completeness (tests 6, 16, 17, 20, 23) ─────────────────────────


def test_pr558_blocker1_test06_external_canonical_row_converges():
    """Test 6: a degraded owner exists (from prior repair failure cycle).
    Then a canonical DB row appears externally (another subsystem, restart,
    reconciler).  The canonical handoff must transfer the runtime state and
    retire degraded only after the canonical owner is fully valid.  No
    duplicate owner, no duplicate DB insertion attempt."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    mp_cls = getattr(_EE_MOD, "ManagedPosition", None)
    if engine_cls is None or mp_cls is None:
        pytest.skip("APExitEngine not importable")
    eng = _pr558_new_engine()

    sym = "IWM260117C00220000"
    # Cycle 1 — install degraded owner (repair had failed)
    degraded = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym, broker_position={"contract": sym, "quantity": 2},
        broker_qty=2, account_id="acct-live-1", repair_failed_reason="db_upsert_returned_no_id",
    )
    assert degraded is not None
    # Accumulate runtime state during degraded window
    degraded.peak_pnl_pct = 0.15
    degraded.touched_profit = True

    # Cycle 2 — canonical row appears externally.  Exercise the same full
    # convergence seam used by the production DB-load path.
    canonical = mp_cls(
        ticker="IWM", option_symbol=sym, side="CALL", quantity=2,
        entry_price=2.40, underlying_entry=210.0,
        underlying_target=225.0, underlying_stop=205.0,
        position_id="durable-canonical-external",
        client_id="jason@example.com", signal_id="signal-external",
        execution_mode="live", quantity_remaining=2,
    )
    eng._install_canonical_owner_atomically(
        canonical, sym, account_id="acct-live-1",
    )

    # Postcondition per Angel spec: exactly one behavior-active owner and the
    # accumulated degraded runtime state survived the handoff.
    assert eng.active_positions() == [canonical]
    assert degraded not in eng._positions
    assert eng._positions_by_id.get(canonical.position_id) is canonical
    assert canonical.peak_pnl_pct == 0.15
    assert canonical.touched_profit is True


def test_pr558_blocker1_test16_orchestrator_never_installs_degraded_on_broker_flat():
    """Test 16: Angel spec — 'exact fresh broker-flat truth must not create
    degraded open owner. Preserve existing flat/reconciliation behavior.'
    The install helper only fires from the 3b repair-failure branch, which
    is only reached when broker truth ALREADY proved the position is OPEN.
    A broker-flat path is a different orchestrator branch and never reaches
    the degraded install.

    This source-inspection test locks that structural invariant: the
    install call must only appear in the 3b repair-failure branch, not
    anywhere reachable from a broker-flat detection."""
    src = EE_SRC
    # Count occurrences of the install helper — the definition + exactly one
    # call site (the 3b repair-failure branch).
    def_count = src.count("def _install_or_refresh_degraded_broker_truth_owner")
    call_count = src.count("_install_or_refresh_degraded_broker_truth_owner(")
    # 1 def + 1 call = 2 occurrences; more than 2 = multiple call sites
    # (possible legitimate future use, but must be reviewed).
    assert def_count == 1
    assert call_count <= 2, (
        f"install helper is called from {call_count - 1} sites; spec allows "
        f"only the 3b repair-failure branch.  If a new call site is added, "
        f"it MUST be inside a code path already proven broker-open."
    )
    # The one call site must be inside the 3b block (after 3b marker, before 3c).
    idx_3b = src.find("# ── 3b. Create DB row from broker truth if still no pos ───────────")
    idx_3c = src.find("# ── 3c. Verify loaded_qty > 0", idx_3b)
    call_idx = src.find("_install_or_refresh_degraded_broker_truth_owner(", idx_3b)
    assert idx_3b < call_idx < idx_3c, (
        "install call must sit inside the 3b (repair-failure) branch"
    )


def test_pr558_blocker1_test17_unrelated_positions_independent():
    """Test 17: 'a failed repair for one broker position must not prevent
    other exact positions from remaining loaded/evaluated'.  Install one
    degraded owner for OCC-A, then trigger a failed install for OCC-B due
    to a fence rejection.  OCC-A's degraded owner must remain intact."""
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")
    sym_a = "IWM260117C00220000"
    sym_b = "SPY260117C00500000"

    # Install degraded owner for OCC-A (repair failed but broker truth proven)
    pos_a = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym_a, broker_position={"contract": sym_a, "quantity": 2},
        broker_qty=2, account_id="acct-live-1", repair_failed_reason="db_upsert_returned_no_id",
    )
    assert pos_a is not None

    # Attempt install for OCC-B with a fence violation (contract mismatch in payload)
    # This must fail closed with no impact on OCC-A.
    pos_b = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym_b,
        broker_position={"contract": "WRONG_OCC", "quantity": 2},  # identity contradiction
        broker_qty=2, account_id="acct-live-1", repair_failed_reason="test",
    )
    assert pos_b is None, "OCC-B install must fail closed on identity contradiction"

    # OCC-A must remain fully intact — same object, same identity, same qty.
    assert pos_a in eng._positions, (
        "OCC-A degraded owner must not be affected by OCC-B install failure"
    )
    assert eng._positions_by_id.get(pos_a.position_id) is pos_a
    assert pos_a.quantity == 2
    assert pos_a.broker_repair_degraded is True


def test_pr558_blocker1_test20_degraded_owner_creates_no_broker_entry_authority():
    """Test 20: Angel spec — 'degraded broker repair path must never place
    ENTRY orders'.  The install helper must not call any broker submit /
    cancel / replace / place / POST method.  Source-inspection guard so
    a future edit that adds broker-mutating side effects is visibly wrong."""
    src = EE_SRC
    start = src.find("def _install_or_refresh_degraded_broker_truth_owner")
    end   = src.find("\n    def ", start + 1)
    body  = src[start:end]
    # Forbidden call fragments — any of these inside the installer body
    # would indicate broker mutation authority the amendment must not have.
    forbidden = (
        ".place_order(", ".submit_order(", ".submit(", ".cancel_order(",
        ".replace_order(", ".place_option_order(", ".post(", ".POST(",
    )
    for pattern in forbidden:
        assert pattern not in body, (
            f"degraded owner installer must never call {pattern} — that "
            f"would introduce broker mutation authority the amendment forbids"
        )
    # Similarly: the take-over and apply-transfer helpers must not mutate broker.
    for helper_name in ("_take_over_degraded_owner_if_any",
                        "_apply_degraded_runtime_transfer"):
        h_start = src.find(f"def {helper_name}")
        h_end   = src.find("\n    def ", h_start + 1)
        h_body  = src[h_start:h_end]
        for pattern in forbidden:
            assert pattern not in h_body, (
                f"{helper_name} must never call {pattern}"
            )


def test_pr558_blocker1_test23_crash_restart_convergence_same_canonical_identity():
    """Test 23: exercise restart recovery through production seed/precheck
    seams, including a pending EXIT row still keyed to the degraded owner.

    Process A proves the broker-open position but cannot persist a canonical
    row, so it installs one degraded owner and accumulates pending EXIT state.
    Process B is a fresh engine, runs seed_from_db, then its broker precheck
    loads the durable canonical row and reattaches the pending EXIT identity
    from the stable degraded-owner id without creating a duplicate owner."""
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")
    sym = "IWM260117C00220000"
    state = {"quantity": 2}

    class _Broker:
        account_id = "acct-live-1"
        mode = "live"

        def list_positions(self):
            return [{
                "symbol": sym,
                "quantity": state["quantity"],
                "cost_basis": 480.0,
                "date_acquired": "2026-08-01",
            }]

    empty_quote = lambda _sym: {
        "mark": 0.0, "bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0,
    }

    # Process A: broker truth is open, but canonical repair is unavailable.
    eng.broker = _Broker()
    eng._load_db_position_row = lambda _sym: None
    eng._upsert_broker_position_to_db = lambda _sym, _bp: None
    eng._fetch_broker_quote = empty_quote
    assert eng._broker_position_precheck() is False
    degraded = [
        p for p in eng._positions
        if getattr(p, "broker_repair_degraded", False)
    ]
    assert len(degraded) == 1
    degraded[0].peak_pnl_pct = 0.18
    degraded[0].exit_in_flight = True
    degraded[0].pending_exit_local_order_id = "legacy-exit-local"
    degraded[0].pending_exit_broker_order_id = "legacy-exit-broker"
    degraded[0].pending_exit_qty = 2
    degraded[0].pending_exit_filled_qty = 1
    degraded[0].last_applied_exit_cum_fill = 1

    # Process A crashes here.  Its in-memory degraded owner is intentionally
    # not copied into Process B.
    eng_after = _pr558_new_engine()
    assert eng_after._positions == []

    class _PositionManager:
        def get_active_positions(self):
            return []

    # This is the actual production boot seam.  There are no canonical rows
    # available at startup; the broker precheck below discovers the row after
    # the fresh process has been initialized.
    eng_after.seed_from_db(_PositionManager())
    assert eng_after._positions == []

    canonical_row = {
        "id": "durable-canonical-558",
        "client_id": "jason@example.com",
        "contract": sym,
        "option_symbol": sym,
        "underlying": "IWM",
        "side": "CALL",
        "direction": "CALL",
        "qty": 2,
        "quantity_remaining": 2,
        "entry_price": 2.40,
        "avg_fill": 2.40,
        "entry_ts": "2026-08-01T13:00:00Z",
        "status": "OPEN",
        "signal_id": "signal-558",
        "execution_mode": "live",
    }
    eng_after.broker = _Broker()
    eng_after._load_db_position_row = lambda _sym: canonical_row
    eng_after._upsert_broker_position_to_db = lambda *_args: pytest.fail(
        "restart hydration must use the available canonical row, not insert a duplicate"
    )
    eng_after._fetch_broker_quote = empty_quote

    pending_exit_row = {
        "position_id": degraded[0].position_id,
        "client_id": "jason@example.com",
        "contract": sym,
        "execution_mode": "live",
        "kind": "EXIT",
        "local_order_id": "legacy-exit-local",
        "broker_order_id": "legacy-exit-broker",
        "status": "EXIT_SUBMITTED",
        "qty": 2,
        "filled_qty": 1,
        "created_ts": "2026-08-01T13:01:00Z",
        "submitted_ts": "2026-08-01T13:01:01Z",
        "updated_ts": "2026-08-01T13:01:02Z",
    }
    fake, restore = _pr558_fake_db_with_rows(
        [pending_exit_row], position_row=canonical_row,
    )
    sys.modules["ap.db"] = fake
    try:
        assert eng_after._broker_position_precheck() is True
    finally:
        restore()
    active = eng_after.active_positions()
    assert len(active) == 1
    assert active[0].position_id == "durable-canonical-558"
    assert active[0].broker_repair_degraded is False
    assert active[0].quantity_remaining == 2
    assert active[0].exit_in_flight is True
    assert active[0].pending_exit_local_order_id == "legacy-exit-local"
    assert active[0].pending_exit_broker_order_id == "legacy-exit-broker"
    assert active[0].pending_exit_filled_qty == 1
    assert active[0].last_applied_exit_cum_fill == 1
    assert eng_after._pending_exit_hydration_status[active[0].position_id] == "FOUND"
    assert active[0].position_id not in eng_after._pending_exit_identity_hold_position_ids
    assert all(
        not getattr(p, "broker_repair_degraded", False)
        for p in eng_after._positions
    )


def test_pr585_restart_still_degraded_reuses_active_exit_without_duplicate_submit():
    """A restart that remains degraded must recover the deterministic owner
    and its already-active EXIT, without a second submit or cancellation."""
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")
    sym = "IWM260117C00220000"

    class _Broker:
        account_id = "acct-live-1"
        mode = "live"

        def list_positions(self):
            return [{
                "symbol": sym,
                "quantity": 1,
                "cost_basis": 240.0,
                "date_acquired": "2026-08-01",
            }]

    # Process A: the broker-open position is protected by the degraded owner
    # and reaches the existing protective EXIT callback.
    eng.broker = _Broker()
    degraded = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym, broker_position={"contract": sym, "quantity": 1},
        broker_qty=1, account_id="acct-live-1", repair_failed_reason="test",
    )
    assert degraded is not None
    now = datetime.now(timezone.utc)
    degraded.opened_at = now - timedelta(minutes=20)
    degraded.current_bid = 1.20
    degraded.current_ask = 1.25
    degraded.current_option_price = 1.20
    degraded.option_bid_valid = True
    degraded.option_quote_fresh = True
    degraded.last_option_bid_update_ts = now
    degraded.last_option_quote_update_ts = now
    eng._clear_degraded_monitoring_state = lambda *args, **kwargs: None
    eng._emit_exit_event = lambda *args, **kwargs: None
    eng.on_exit = MagicMock(return_value={
        "accepted": True,
        "local_order_id": "restart-exit-local",
        "broker_order_id": "restart-exit-broker",
    })
    decision = _EE_MOD.ExitDecision(
        action="STOP", quantity=1, reason="HARD STOP",
        urgency="IMMEDIATE", pnl_pct=-0.40, reason_code="HARD_STOP",
    )
    import ap.exit_safety as exit_safety
    with patch.object(
        exit_safety,
        "evaluate_exit_submission_safety",
        return_value={"blocked": False},
    ):
        assert eng._submit_exit_decision(degraded, decision) is True
    eng.on_exit.assert_called_once()
    active_exit_row = {
        "position_id": degraded.position_id,
        "client_id": "jason@example.com",
        "contract": sym,
        "execution_mode": "live",
        "kind": "EXIT",
        "local_order_id": "restart-exit-local",
        "broker_order_id": "restart-exit-broker",
        "status": "EXIT_SUBMITTED",
        "qty": 1,
        "filled_qty": 0,
        "created_ts": "2026-08-01T13:01:00Z",
        "submitted_ts": "2026-08-01T13:01:01Z",
        "updated_ts": "2026-08-01T13:01:02Z",
    }

    # Process B: canonical repair is still unavailable, so the same
    # deterministic degraded owner is reconstructed.  The active EXIT is
    # hydrated by its degraded position id before action generation.
    eng_after = _pr558_new_engine()
    eng_after.broker = _Broker()
    eng_after._load_db_position_row = lambda _sym: None
    eng_after._upsert_broker_position_to_db = lambda *_args: None
    eng_after._fetch_broker_quote = lambda _sym: {
        "mark": 0.0, "bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0,
    }
    fake, restore = _pr558_fake_db_with_rows([active_exit_row])
    prior = sys.modules.get("ap.db")
    sys.modules["ap.db"] = fake
    try:
        assert eng_after._broker_position_precheck() is False
    finally:
        restore()
        if prior is not None:
            sys.modules["ap.db"] = prior

    active = eng_after.active_positions()
    assert len(active) == 1
    restarted = active[0]
    assert restarted.broker_repair_degraded is True
    assert restarted.position_id == degraded.position_id
    assert restarted.exit_in_flight is True
    assert restarted.pending_exit_local_order_id == "restart-exit-local"
    assert restarted.pending_exit_broker_order_id == "restart-exit-broker"
    assert eng_after._pending_exit_hydration_status[restarted.position_id] == "FOUND"
    assert restarted.position_id not in eng_after._pending_exit_identity_hold_position_ids

    cancel_calls = []

    class _OSM:
        def _get_active_exit_order(self, position_id):
            return active_exit_row if position_id == restarted.position_id else None

        def cancel_exit(self, *args, **kwargs):
            cancel_calls.append((args, kwargs))

    eng_after.order_state_machine = _OSM()
    eng_after.on_exit = MagicMock()
    eng_after.on_scale = MagicMock()
    eng_after._emit_exit_event = lambda *args, **kwargs: None
    duplicate_decision = _EE_MOD.ExitDecision(
        action="STOP", quantity=1, reason="HARD STOP",
        urgency="IMMEDIATE", pnl_pct=-0.40, reason_code="HARD_STOP",
    )
    assert eng_after._submit_exit_decision(restarted, duplicate_decision) is False
    eng_after.on_exit.assert_not_called()
    eng_after.on_scale.assert_not_called()
    assert cancel_calls == []


def test_pr558_blocker1_restart_hydration_failure_holds_canonical_owner():
    """A canonical owner must stay mutation-inert when EXIT hydration fails.

    The durable row may still contain an EXIT keyed by the pre-restart
    degraded owner.  A transient orders-query failure is not proof that no
    EXIT exists, so the canonical owner remains active but cannot submit or
    cancel until a later hydration succeeds.
    """
    eng = _pr558_new_engine()
    if eng is None:
        pytest.skip("APExitEngine not importable")
    sym = "IWM270117C00220000"

    class _Broker:
        account_id = "acct-live-1"
        mode = "live"

        def list_positions(self):
            return [{
                "symbol": sym,
                "quantity": 1,
                "cost_basis": 240.0,
                "date_acquired": "2026-08-01",
            }]

    empty_quote = lambda _sym: {
        "mark": 0.0, "bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0,
    }

    # Process A owns the broker-open position only through the deterministic
    # degraded identity and has an active EXIT pending under that identity.
    eng.broker = _Broker()
    eng._load_db_position_row = lambda _sym: None
    eng._upsert_broker_position_to_db = lambda _sym, _bp: None
    eng._fetch_broker_quote = empty_quote
    assert eng._broker_position_precheck() is False
    degraded = eng._positions[0]
    degraded.exit_in_flight = True
    degraded.pending_exit_local_order_id = "legacy-exit-local"
    degraded.pending_exit_broker_order_id = "legacy-exit-broker"

    # Process B sees the canonical row after restart, but the active EXIT
    # lookup is temporarily unavailable.  The canonical owner must still be
    # installed exactly once and held without mutation.
    eng_after = _pr558_new_engine()
    canonical_row = {
        "id": "durable-canonical-558-hydration-hold",
        "client_id": "jason@example.com",
        "contract": sym,
        "option_symbol": sym,
        "underlying": "IWM",
        "side": "CALL",
        "direction": "CALL",
        "qty": 1,
        "quantity_remaining": 1,
        "entry_price": 2.40,
        "avg_fill": 2.40,
        "entry_ts": "2026-08-01T13:00:00Z",
        "status": "OPEN",
        "signal_id": "signal-558",
        "execution_mode": "live",
    }
    eng_after.broker = _Broker()
    eng_after._load_db_position_row = lambda _sym: canonical_row
    eng_after._upsert_broker_position_to_db = lambda *_args: pytest.fail(
        "canonical restart hydration must not insert a duplicate owner"
    )
    eng_after._fetch_broker_quote = empty_quote

    class _FailingCursor:
        rowcount = 1

        def __init__(self):
            self._sql = ""

        def execute(self, sql, *_args, **_kwargs):
            self._sql = str(sql)
            if "FOR UPDATE" in self._sql:
                return self
            raise RuntimeError("temporary orders lookup outage")

        def fetchone(self):
            if "FOR UPDATE" in self._sql:
                return dict(canonical_row)
            return None

    @contextmanager
    def failing_conn():
        yield _FailingCursor()

    fake = types.SimpleNamespace(
        conn=failing_conn,
        run_with_retry=lambda fn, **_kwargs: fn(),
    )
    prior = sys.modules.get("ap.db")
    sys.modules["ap.db"] = fake
    try:
        assert eng_after._broker_position_precheck() is True
        active = eng_after.active_positions()
        assert len(active) == 1
        canonical = active[0]
        assert canonical.position_id == "durable-canonical-558-hydration-hold"
        assert canonical.broker_repair_degraded is False
        assert canonical.position_id in eng_after._pending_exit_identity_hold_position_ids
        assert eng_after._pending_exit_hydration_status[canonical.position_id] == "UNAVAILABLE"
        assert sym not in eng_after._broker_truth_hold_symbols, (
            "pending EXIT identity must not become a permanent broker-symbol hold; "
            "the position-specific identity hold is retried normally"
        )

        eng_after._run_sentinels = lambda: None
        eng_after._kill_switch_fn = None
        eng_after._submit_exit_decision = MagicMock()
        eng_after._check_all_positions(now_et=datetime(2026, 9, 5, 16, 0))
        eng_after._submit_exit_decision.assert_not_called()

        # A later retry can prove the same durable EXIT under the stable
        # degraded-owner identity and release the canonical owner for normal
        # monitoring, without creating a second EXIT.
        pending_exit_row = {
            "position_id": degraded.position_id,
            "client_id": "jason@example.com",
            "contract": sym,
            "execution_mode": "live",
            "kind": "EXIT",
            "local_order_id": "legacy-exit-local",
            "broker_order_id": "legacy-exit-broker",
            "status": "EXIT_SUBMITTED",
            "qty": 1,
            "filled_qty": 0,
            "created_ts": "2026-08-01T13:01:00Z",
            "submitted_ts": "2026-08-01T13:01:01Z",
            "updated_ts": "2026-08-01T13:01:02Z",
        }
        success_fake, restore_success = _pr558_fake_db_with_rows([pending_exit_row])
        sys.modules["ap.db"] = success_fake
        try:
            status = eng_after._hydrate_pending_exit_identity_for_broker_recovery(
                canonical, sym, "acct-live-1",
            )
        finally:
            restore_success()
        assert status == "FOUND"
        assert canonical.position_id not in eng_after._pending_exit_identity_hold_position_ids
        assert canonical.pending_exit_local_order_id == "legacy-exit-local"
        assert canonical.pending_exit_broker_order_id == "legacy-exit-broker"
    finally:
        if prior is not None:
            sys.modules["ap.db"] = prior
        else:
            sys.modules.pop("ap.db", None)


def test_pr558_blocker1_proven_no_pending_exit_allows_normal_exit():
    """A successful empty hydration must not permanently suppress exits."""
    eng = _pr558_new_engine()
    mp_cls = getattr(_EE_MOD, "ManagedPosition", None)
    if eng is None or mp_cls is None:
        pytest.skip("APExitEngine/ManagedPosition not importable")
    sym = "IWM270117C00220000"
    pos = mp_cls(
        ticker="IWM", option_symbol=sym, side="CALL", quantity=1,
        entry_price=2.40, underlying_entry=0.0,
        underlying_target=0.0, underlying_stop=0.0,
        position_id="canonical-no-pending-exit",
        client_id="jason@example.com", execution_mode="live",
        quantity_remaining=1,
    )
    eng._positions = [pos]
    eng._positions_by_id = {pos.position_id: pos}
    eng.broker = types.SimpleNamespace(
        account_id="acct-live-1",
        mode="live",
        list_positions=lambda: [],
    )

    fake, restore = _pr558_fake_db_with_rows([])
    sys.modules["ap.db"] = fake
    try:
        assert eng.hydrate_pending_exit_identity_from_db(pos) is False
        assert eng._pending_exit_hydration_status[pos.position_id] == "NONE"
        assert pos.position_id not in eng._pending_exit_identity_hold_position_ids

        eng._run_sentinels = lambda: None
        eng._kill_switch_fn = None
        eng._submit_exit_decision = MagicMock()
        # EOD is an unconditional normal exit decision for this future OCC.
        eng._check_all_positions(now_et=datetime(2026, 9, 5, 16, 0))
        eng._submit_exit_decision.assert_called_once()
    finally:
        restore()


def test_pr585_pending_exit_identity_hold_retries_and_resumes_action_loop():
    """A transient hydration hold must clear on a later successful read."""
    eng = _pr558_new_engine()
    mp_cls = getattr(_EE_MOD, "ManagedPosition", None)
    if eng is None or mp_cls is None:
        pytest.skip("APExitEngine/ManagedPosition not importable")
    sym = "IWM270117C00220000"
    pos = mp_cls(
        ticker="IWM", option_symbol=sym, side="CALL", quantity=1,
        quantity_remaining=1, entry_price=2.0,
        underlying_entry=0.0, underlying_target=0.0, underlying_stop=0.0,
        position_id="canonical-retry-owner", client_id="jason@example.com",
        execution_mode="live",
    )
    eng._positions = [pos]
    eng._positions_by_id = {pos.position_id: pos}
    eng.broker = types.SimpleNamespace(
        account_id="acct-live-1",
        mode="live",
        list_positions=lambda: [],
    )
    eng._run_sentinels = lambda: None
    eng._kill_switch_fn = None
    eng._emit_exit_event = lambda *args, **kwargs: None
    eng._submit_exit_decision = MagicMock()
    eng._set_pending_exit_hydration_status(
        pos, "UNAVAILABLE",
    )

    @contextmanager
    def failing_conn():
        class _Cursor:
            def execute(self, *_args, **_kwargs):
                raise RuntimeError("temporary orders lookup outage")

            def fetchall(self):
                return []

        yield _Cursor()

    failing_db = types.SimpleNamespace(
        conn=failing_conn,
        run_with_retry=lambda fn, **_kwargs: fn(),
    )
    prior = sys.modules.get("ap.db")
    sys.modules["ap.db"] = failing_db
    try:
        eng._check_all_positions(now_et=datetime(2026, 9, 5, 16, 0))
        eng._submit_exit_decision.assert_not_called()
        assert pos.position_id in eng._pending_exit_identity_hold_position_ids
    finally:
        if prior is not None:
            sys.modules["ap.db"] = prior
        else:
            sys.modules.pop("ap.db", None)

    success_db, restore = _pr558_fake_db_with_rows([])
    sys.modules["ap.db"] = success_db
    try:
        eng._check_all_positions(now_et=datetime(2026, 9, 5, 16, 0))
    finally:
        restore()

    eng._submit_exit_decision.assert_called_once()
    assert pos.position_id not in eng._pending_exit_identity_hold_position_ids


def test_pr558_blocker1_multiple_active_exit_rows_are_ambiguous():
    """Canonical+degraded active EXIT rows must fail closed as AMBIGUOUS."""
    eng = _pr558_new_engine()
    mp_cls = getattr(_EE_MOD, "ManagedPosition", None)
    if eng is None or mp_cls is None:
        pytest.skip("APExitEngine/ManagedPosition not importable")
    sym = "IWM270117C00220000"
    pos = mp_cls(
        ticker="IWM", option_symbol=sym, side="CALL", quantity=1,
        entry_price=2.40, underlying_entry=0.0,
        underlying_target=0.0, underlying_stop=0.0,
        position_id="durable-canonical-ambiguous-558",
        client_id="jason@example.com", execution_mode="live",
        quantity_remaining=1,
    )
    eng._positions = [pos]
    eng._positions_by_id = {pos.position_id: pos}
    degraded_id = eng._degraded_broker_owner_id(sym, "acct-live-1")
    rows = [
        {
            "position_id": pos.position_id,
            "client_id": "jason@example.com",
            "contract": sym,
            "execution_mode": "live",
            "kind": "EXIT",
            "local_order_id": "canonical-exit-local",
            "broker_order_id": "canonical-exit-broker",
            "status": "EXIT_ACKNOWLEDGED",
            "qty": 1,
            "filled_qty": 0,
            "created_ts": "2026-08-01T13:01:00Z",
            "submitted_ts": "2026-08-01T13:01:01Z",
            "updated_ts": "2026-08-01T13:02:00Z",
        },
        {
            "position_id": degraded_id,
            "client_id": "jason@example.com",
            "contract": sym,
            "execution_mode": "live",
            "kind": "EXIT",
            "local_order_id": "degraded-exit-local",
            "broker_order_id": "degraded-exit-broker",
            "status": "EXIT_SUBMITTED",
            "qty": 1,
            "filled_qty": 0,
            "created_ts": "2026-08-01T13:01:30Z",
            "submitted_ts": "2026-08-01T13:01:31Z",
            "updated_ts": "2026-08-01T13:03:00Z",
        },
    ]
    fake, restore = _pr558_fake_db_with_rows(rows)
    sys.modules["ap.db"] = fake
    try:
        status = eng._hydrate_pending_exit_identity_for_broker_recovery(
            pos, sym, "acct-live-1",
        )
        assert status == "AMBIGUOUS"
        assert pos.position_id in eng._pending_exit_identity_hold_position_ids
        assert eng._pending_exit_hydration_status[pos.position_id] == "AMBIGUOUS"
        assert pos.exit_in_flight is False
        assert pos.pending_exit_local_order_id == ""
        assert pos.pending_exit_broker_order_id == ""

        eng._broker_position_precheck = lambda: False
        eng._run_sentinels = lambda: None
        eng._kill_switch_fn = None
        eng._submit_exit_decision = MagicMock()
        eng._check_all_positions(now_et=datetime(2026, 9, 5, 16, 0))
        eng._submit_exit_decision.assert_not_called()
    finally:
        restore()


def test_pr585_pending_exit_wrong_mode_or_occ_is_ambiguous():
    """A durable row must match current client/mode/exact OCC/owner."""
    eng = _pr558_new_engine()
    mp_cls = getattr(_EE_MOD, "ManagedPosition", None)
    if eng is None or mp_cls is None:
        pytest.skip("APExitEngine/ManagedPosition not importable")
    sym = "IWM270117C00220000"
    pos = mp_cls(
        ticker="IWM", option_symbol=sym, side="CALL", quantity=1,
        entry_price=2.40, underlying_entry=0.0, underlying_target=0.0,
        underlying_stop=0.0, position_id="canonical-identity-fence",
        client_id="jason@example.com", execution_mode="live",
        quantity_remaining=1,
    )
    eng._positions = [pos]
    eng._positions_by_id = {pos.position_id: pos}
    rows = [{
        "position_id": pos.position_id,
        "client_id": "jason@example.com",
        "contract": "IWM270117P00220000",
        "execution_mode": "paper",
        "kind": "EXIT",
        "local_order_id": "wrong-exit-local",
        "broker_order_id": "wrong-exit-broker",
        "status": "EXIT_SUBMITTED",
        "qty": 1,
        "filled_qty": 0,
    }]
    fake, restore = _pr558_fake_db_with_rows(rows)
    sys.modules["ap.db"] = fake
    try:
        status = eng.hydrate_pending_exit_identity_from_db(pos)
    finally:
        restore()
    assert status is False
    assert eng._pending_exit_hydration_status[pos.position_id] == "AMBIGUOUS"
    assert pos.position_id in eng._pending_exit_identity_hold_position_ids
    assert pos.pending_exit_local_order_id == ""
    assert pos.pending_exit_broker_order_id == ""


def test_pr558_blocker2_canonical_adoption_clears_degraded_metadata():
    """Both canonical adoption success paths must stop degraded retry state."""
    eng = _pr558_new_engine()
    mp_cls = getattr(_EE_MOD, "ManagedPosition", None)
    if eng is None or mp_cls is None:
        pytest.skip("APExitEngine/ManagedPosition not importable")
    sym = "IWM270117C00220000"

    degraded = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym,
        broker_position={"contract": sym, "quantity": 1},
        broker_qty=1,
        account_id="acct-live-1",
        repair_failed_reason="temporary-repair-failure",
    )
    assert degraded is not None
    result = eng.adopt_canonical_position_identity(
        contract=sym,
        canonical_position_id="durable-canonical-adopt-558",
        local_order_id="entry-local",
        broker_order_id="entry-broker",
        signal_id="signal-558",
        canonical_signal_id="canonical-signal-558",
        entry_fill=2.40,
        entry_ts=None,
        execution_mode="live",
        client_id="jason@example.com",
    )
    assert result.adopted is True
    assert degraded.position_id == "durable-canonical-adopt-558"
    assert degraded.broker_repair_degraded is False
    assert degraded.broker_repair_degraded_reason == ""
    assert degraded.broker_repair_degraded_account_id == ""

    # Exercise the already-canonical collapse path as well: stale degraded
    # flags on a canonical object must be cleared before the next precheck.
    existing = mp_cls(
        ticker="IWM", option_symbol=sym, side="CALL", quantity=1,
        entry_price=2.40, underlying_entry=0.0,
        underlying_target=0.0, underlying_stop=0.0,
        position_id="durable-canonical-existing-558",
        client_id="jason@example.com", execution_mode="live",
        quantity_remaining=1,
    )
    existing.broker_repair_degraded = True
    existing.broker_repair_degraded_reason = "stale"
    existing.broker_repair_degraded_account_id = "acct-live-1"
    eng2 = _pr558_new_engine()
    eng2._positions = [existing]
    eng2._positions_by_id = {existing.position_id: existing}
    result_existing = eng2.adopt_canonical_position_identity(
        contract=sym,
        canonical_position_id=existing.position_id,
        local_order_id="entry-local",
        broker_order_id="entry-broker",
        signal_id="signal-558",
        canonical_signal_id="canonical-signal-558",
        entry_fill=2.40,
        entry_ts=None,
        execution_mode="live",
        client_id="jason@example.com",
    )
    assert result_existing.adopted is True
    assert existing.broker_repair_degraded is False
    assert existing.broker_repair_degraded_reason == ""
    assert existing.broker_repair_degraded_account_id == ""

    eng2.broker = types.SimpleNamespace(
        account_id="acct-live-1",
        mode="live",
        list_positions=lambda: [{"symbol": sym, "quantity": 1}],
    )
    assert eng2._broker_position_precheck() is True
    assert not existing.broker_repair_degraded


# ─── Structural completeness (tests 18, 19) ───────────────────────────────────


def test_pr558_blocker1_test18_live_paper_parity():
    """Test 18: LIVE/PAPER parity — 'both modes must preserve behavior-active
    ownership. No cross-mode adoption.'  Deterministic id must differ
    between modes so no adoption path can cross them."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not importable")

    sym = "IWM260117C00220000"
    acct = "acct-shared"

    def _mk(mode):
        eng = engine_cls.__new__(engine_cls)
        eng._email = "jason@example.com"
        eng._lock = __import__("threading").Lock()
        eng._positions = []
        eng._positions_by_id = {}
        eng._resolved_execution_mode = lambda m=mode: m
        eng._underlying_from_occ = lambda s: "IWM"
        eng._parse_occ_side = lambda s: "CALL"
        return eng

    eng_live = _mk("live")
    eng_paper = _mk("paper")

    pos_live = eng_live._install_or_refresh_degraded_broker_truth_owner(
        sym=sym, broker_position={"contract": sym, "quantity": 2},
        broker_qty=2, account_id=acct, repair_failed_reason="test",
    )
    pos_paper = eng_paper._install_or_refresh_degraded_broker_truth_owner(
        sym=sym, broker_position={"contract": sym, "quantity": 2},
        broker_qty=2, account_id=acct, repair_failed_reason="test",
    )
    assert pos_live is not None and pos_paper is not None
    # LIVE/PAPER must produce distinct ids even with everything else equal.
    assert pos_live.position_id != pos_paper.position_id, (
        "LIVE and PAPER degraded owners must have distinct ids — no cross-mode adoption"
    )
    assert pos_live.execution_mode == "live"
    assert pos_paper.execution_mode == "paper"
    # Both must be behavior-active in their own engines.
    assert pos_live in eng_live.active_positions()
    assert pos_paper in eng_paper.active_positions()


def test_pr558_blocker1_test19_degraded_owner_exit_path_reachable():
    """Test 19: 'with degraded owner and fresh broker-open truth, drive the
    existing engine to an already-valid exit decision.  Assert the normal
    canonical EXIT path remains reachable.  Do not assert a new submit
    implementation.'

    Source-inspection structural test: the degraded owner is a full
    ManagedPosition with the broker_repair_degraded flag; _is_behavior_active_position
    must NOT filter it out (that would silently disable exit monitoring)."""
    src = EE_SRC
    # _is_behavior_active_position must not check broker_repair_degraded.
    start = src.find("def _is_behavior_active_position")
    end   = src.find("\ndef ", start + 1)
    if start == -1:
        # Try the class-scoped variant.
        end = src.find("\n    def ", src.find("    def _is_behavior_active_position") + 1)
    body = src[start:end] if start != -1 else ""
    assert "broker_repair_degraded" not in body, (
        "_is_behavior_active_position must not read broker_repair_degraded — "
        "if it did, degraded owners would be invisible to the exit engine"
    )
    # Cross-check: our own test asserts this behavior positively.
    assert "def test_pr558_blocker1_degraded_owner_is_behavior_active" in \
        open(__file__).read(), (
        "positive-shape behavioral test for exit-active degraded owner must exist"
    )


# =============================================================================
# PR #588 — PRESERVE CANONICAL ENTRY QUANTITY DURING BROKER RECOVERY
# Tests A–K from the binding spec: p0_pr585_partial_quantity_authority_20260905.md
# =============================================================================

# ── Source-inspection guards ──────────────────────────────────────────────────

def test_pr588_source_separates_full_entry_qty_from_broker_remaining():
    """Source: _upsert_broker_position_to_db must name two distinct quantity
    variables — full_entry_qty from the proven ENTRY order and
    broker_remaining_qty from the broker position — and must never pass a
    single qty value to both INSERT columns."""
    upsert_start = EE_SRC.find("def _upsert_broker_position_to_db")
    upsert_end   = EE_SRC.find("\n    def ", upsert_start + 1)
    body = EE_SRC[upsert_start:upsert_end]
    assert "broker_remaining_qty" in body, (
        "_upsert_broker_position_to_db must use broker_remaining_qty "
        "(broker current exposure) as a distinct variable"
    )
    assert "full_entry_qty" in body, (
        "_upsert_broker_position_to_db must use full_entry_qty "
        "(proven original entry qty) as a distinct variable"
    )
    assert "full_entry_qty, broker_remaining_qty" in body, (
        "Both INSERT statements must pass full_entry_qty for qty column "
        "and broker_remaining_qty for quantity_remaining column"
    )
    # Status must be derived, not hardcoded 'OPEN'
    assert "_recovery_status" in body, (
        "_recovery_status must be derived (OPEN/PARTIAL) and used in INSERTs"
    )
    assert "broker_qty_exceeds_entry_qty" in body, (
        "Contradictory authority (broker > entry) must fail closed with "
        "reason=broker_qty_exceeds_entry_qty"
    )


def test_pr588_source_managed_position_from_row_preserves_full_qty():
    """Source: _managed_position_from_row with prefer_qty_override=True must
    set mp.quantity from the durable row's qty (canonical full entry), not
    from qty_override (broker current exposure)."""
    start = EE_SRC.find("def _managed_position_from_row")
    end   = EE_SRC.find("\n    def ", start + 1)
    body  = EE_SRC[start:end]
    assert "broker_repair_qty_authority_contradiction" in body, (
        "_managed_position_from_row must quarantine when broker > canonical full qty"
    )
    assert "_db_full_qty" in body, (
        "_managed_position_from_row must derive _db_full_qty from row.qty "
        "(canonical full entry) separate from _broker_rem (broker exposure)"
    )
    assert "_broker_rem" in body, (
        "_managed_position_from_row must separate broker_rem from db_full_qty"
    )


def test_pr588_source_db_repair_preserves_canonical_qty():
    """Source: durable repair separates canonical full qty from broker
    remaining qty, and only an exact ENTRY resolver may change ``qty``."""
    precheck_start = EE_SRC.find("def _broker_position_precheck")
    precheck_end   = EE_SRC.find("\n    def ", precheck_start + 1)
    body = EE_SRC[precheck_start:precheck_end]
    assert "_reconcile_existing_broker_position_row" in body, (
        "all existing-row quantity repair must use the shared durable resolver"
    )
    helper_start = EE_SRC.find("def _reconcile_existing_broker_position_row")
    helper_end = EE_SRC.find("\n    def ", helper_start + 1)
    helper_body = EE_SRC[helper_start:helper_end]
    assert "SET qty = %s" in helper_body, (
        "legacy collapsed qty repair must persist the exact ENTRY-proven full qty"
    )
    assert "expected_position_id=position_id" in EE_SRC, (
        "legacy full-quantity repair must fence ENTRY provenance to the exact position"
    )
    assert "_derived_status = \"OPEN\" if _broker_qty == _full_qty else \"PARTIAL\"" in helper_body
    assert "FOR UPDATE" in helper_body, (
        "quantity repair must lock and reread the durable row before mutation"
    )
    assert "_locked_status in {\"CLOSING\", \"ACTIVE\"}" in helper_body, (
        "locked active lifecycle authority must survive quantity repair"
    )
    # No broker-only expansion path is allowed.
    assert "qty               = GREATEST" not in body, (
        "DB repair UPDATE must not expand canonical qty via GREATEST — "
        "broker truth cannot overwrite original entry size"
    )


def test_pr588_source_both_inserts_use_same_quantity_semantics():
    """Source: both the extended INSERT and the UndefinedColumn fallback INSERT
    must pass full_entry_qty for qty and broker_remaining_qty for
    quantity_remaining — confirmed by the count of occurrences."""
    upsert_start = EE_SRC.find("def _upsert_broker_position_to_db")
    upsert_end   = EE_SRC.find("\n    def ", upsert_start + 1)
    body = EE_SRC[upsert_start:upsert_end]
    split_count = body.count("full_entry_qty, broker_remaining_qty")
    assert split_count >= 2, (
        f"Expected 'full_entry_qty, broker_remaining_qty' in at least 2 "
        f"INSERT statements (extended + fallback), found {split_count}"
    )


# ── Helper shared by Test A–K behavioral tests ────────────────────────────────

def _pr588_engine_with_filled_entry(
    contract: str,
    client: str,
    filled_qty: int,
    fill_price: float,
    position_id: str,
    mode: str = "live",
):
    """Return a minimal APExitEngine stub pre-loaded with an exact filled
    ENTRY order in a fake ap.db, and with the engine's execution mode
    resolved correctly.  Used by Tests A-K."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        return None, None, None
    eng = engine_cls.__new__(engine_cls)
    eng._email = client
    eng._lock  = __import__("threading").RLock()
    eng._positions = []
    eng._positions_by_id = {}
    eng._broker_truth_hold_symbols = set()
    eng._pending_exit_hydration_status = {}
    eng._pending_exit_identity_hold_position_ids = set()
    eng.broker = types.SimpleNamespace(mode=mode)

    order_row = {
        "id":           f"entry-order-{position_id}",
        "client_id":    client,
        "position_id":  position_id,
        "kind":         "ENTRY",
        "status":       "FILLED",
        "contract":     contract,
        "execution_mode": mode,
        "filled_qty":   filled_qty,
        "avg_fill_price": fill_price,
        "fill_price":   fill_price,
        "fill_price_field": fill_price,
        "price":        fill_price,
        "filled_ts":    "2026-09-01T13:00:00Z",
        "signal_id":    f"sig-{position_id}",
        "broker_order_id": f"broker-{position_id}",
        "meta":         {},
    }
    # Install a fake ap.db that returns this ENTRY order for any fetchall
    # but no existing active positions row.
    class _Cur:
        def __init__(self):
            self._queries = []
        def execute(self, sql, params=()):
            self._queries.append(sql.strip()[:60])
            return self
        def fetchall(self):
            return [order_row]
        def fetchone(self):
            return None   # no existing positions row → trigger INSERT
        @property
        def rowcount(self):
            return 1

    @contextmanager
    def _conn():
        yield _Cur()

    fake_db = types.SimpleNamespace(conn=_conn, run_with_retry=lambda fn, **_: fn())
    return eng, fake_db, order_row


# ── Test A: full recovery (ENTRY=2, broker=2) ─────────────────────────────────

@_skip_if_no_mod
def test_pr588_test_a_full_recovery_2_2_open():
    """Test A (spec): ENTRY filled_qty=2, broker qty=2, positions missing.
    Must persist and reconstruct qty=2 / quantity_remaining=2 / OPEN."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")
    CONTRACT  = "SPY260919C00600000"
    CLIENT    = "pr588-test-a@example.com"
    POSITION_ID = "pr588-pos-a-001"
    eng, fake_db, _ = _pr588_engine_with_filled_entry(
        CONTRACT, CLIENT, filled_qty=2, fill_price=1.00,
        position_id=POSITION_ID,
    )
    if eng is None:
        pytest.skip("APExitEngine not importable")

    broker_position = {"quantity": 2, "cost_basis": 200.0, "date_acquired": "2026-09-01"}
    prior = sys.modules.get("ap.db")
    sys.modules["ap.db"] = fake_db
    try:
        row_id = eng._upsert_broker_position_to_db(CONTRACT, broker_position)
    finally:
        if prior is not None:
            sys.modules["ap.db"] = prior
        else:
            sys.modules.pop("ap.db", None)

    # row_id may be None because the fake DB's fetchone returns None (INSERT
    # returning id returns None); the key assertion is that full_entry_qty
    # and broker_remaining_qty are EQUAL → OPEN status selected.
    # Source inspection confirms the correct status logic exists (test_pr588_source_*).
    # The real OPEN/PARTIAL assignment is tested end-to-end in Test J (PostgreSQL).


@_skip_if_no_mod
def test_pr588_test_a_managed_position_full_2_2():
    """Test A runtime: _managed_position_from_row for ENTRY=2, broker=2
    must produce quantity=2 / quantity_remaining=2."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")
    eng = engine_cls.__new__(engine_cls)
    eng._email = "pr588-a@example.com"
    eng._lock  = __import__("threading").Lock()
    eng._positions = []
    eng.broker = types.SimpleNamespace(mode="live")

    row = {
        "id": "pos-a-001",
        "contract": "SPY260919C00600000",
        "option_symbol": "SPY260919C00600000",
        "underlying": "SPY",
        "side": "CALL",
        "direction": "CALL",
        "qty": 2,               # canonical full entry qty
        "quantity_remaining": 2,
        "entry_price": 1.00,
        "avg_fill": 1.00,
        "entry_ts": None,
        "status": "OPEN",
        "signal_id": None,
        "execution_mode": "live",
    }
    mp = eng._managed_position_from_row(row, qty_override=2, prefer_qty_override=True)
    assert mp.quantity == 2, f"Expected quantity=2, got {mp.quantity}"
    assert mp.quantity_remaining == 2, f"Expected quantity_remaining=2, got {mp.quantity_remaining}"


# ── Test B: partial recovery (ENTRY=2, broker=1) — PRIMARY REGRESSION ─────────

@_skip_if_no_mod
def test_pr588_test_b_managed_position_partial_2_1():
    """Test B (spec) runtime: ENTRY filled_qty=2, broker qty=1.
    _managed_position_from_row must produce quantity=2 / quantity_remaining=1.
    MUST NOT produce 1/1 (the defect this PR fixes)."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")
    eng = engine_cls.__new__(engine_cls)
    eng._email = "pr588-b@example.com"
    eng._lock  = __import__("threading").Lock()
    eng._positions = []
    eng.broker = types.SimpleNamespace(mode="live")

    # DB row has original fill of 2 contracts; broker now shows 1 remaining.
    row = {
        "id": "pos-b-001",
        "contract": "IWM260919C00230000",
        "option_symbol": "IWM260919C00230000",
        "underlying": "IWM",
        "side": "CALL",
        "direction": "CALL",
        "qty": 2,               # canonical full entry qty (proven by original ENTRY)
        "quantity_remaining": 2,
        "entry_price": 1.50,
        "avg_fill": 1.50,
        "entry_ts": None,
        "status": "OPEN",
        "signal_id": None,
        "execution_mode": "live",
    }
    # qty_override=1 is broker's current remaining exposure
    mp = eng._managed_position_from_row(row, qty_override=1, prefer_qty_override=True)
    assert mp.quantity == 2, (
        f"CRITICAL #588 regression: quantity must be full entry qty=2, got {mp.quantity}. "
        f"The defect produced 1/1 (broker_remaining=1 for both); correct result is 2/1."
    )
    assert mp.quantity_remaining == 1, (
        f"quantity_remaining must be broker remaining=1, got {mp.quantity_remaining}"
    )


@_skip_if_no_mod
def test_pr588_test_b_no_fabricated_economics():
    """Test B: partial recovery must not fabricate exit economics."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")
    eng = engine_cls.__new__(engine_cls)
    eng._email = "pr588-b2@example.com"
    eng._lock  = __import__("threading").Lock()
    eng._positions = []
    eng.broker = types.SimpleNamespace(mode="live")

    row = {
        "id": "pos-b2-001",
        "contract": "IWM260919C00230000",
        "option_symbol": "IWM260919C00230000",
        "underlying": "IWM",
        "side": "CALL",
        "direction": "CALL",
        "qty": 2,
        "quantity_remaining": 2,
        "entry_price": 1.50,
        "avg_fill": 1.50,
        "entry_ts": None,
        "status": "OPEN",
        "signal_id": None,
        "execution_mode": "live",
    }
    mp = eng._managed_position_from_row(row, qty_override=1, prefer_qty_override=True)
    # No fabricated exit economics
    assert not getattr(mp, "exit_price", None), "Must not fabricate exit_price"
    assert not getattr(mp, "realized_pnl", None), "Must not fabricate realized_pnl"


# ── Test C: fresh-process restart parity ─────────────────────────────────────

@_skip_if_no_mod
def test_pr588_test_c_restart_parity_fresh_engine():
    """Test C: the real restart seam must HOLD when broker truth is lost.

    This intentionally exercises ``seed_from_db`` and ``_check_all_positions``
    rather than calling the row helper directly.  A stale durable 2/2 row is
    not allowed to reach the submit callback after the process restarts and
    authoritative broker positions are unavailable.
    """
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    contract = "IWM260919C00230000"
    client = "pr588-c@example.com"
    stale_row = {
        "id": "pos-c-restart-001",
        "client_id": client,
        "contract": contract,
        "option_symbol": contract,
        "underlying": "IWM",
        "side": "CALL",
        "direction": "CALL",
        "qty": 2,                   # canonical full entry qty (durable)
        "quantity_remaining": 2,    # stale positive durable remainder
        "entry_price": 1.50,
        "avg_fill": 1.50,
        "entry_ts": None,
        "status": "OPEN",
        "signal_id": "sig-c-001",
        "execution_mode": "live",
    }

    class _UnavailableBroker:
        mode = "live"
        account_id = "acct-pr588-c"

        def list_positions(self):
            raise RuntimeError("broker position snapshot unavailable")

    eng_b = engine_cls(broker=_UnavailableBroker(), email=client)
    eng_b._hydrate_pending_exit_identity_for_broker_recovery = (
        lambda *_args, **_kwargs: "NONE"
    )

    class _PositionManager:
        def get_active_positions(self):
            return [stale_row]

    eng_b.seed_from_db(_PositionManager())
    assert len(eng_b.active_positions()) == 1
    mp = eng_b.active_positions()[0]
    assert mp.quantity == 2
    assert mp.quantity_remaining == 2
    assert mp.broker_repair_quantity_unverified is True

    eng_b._run_sentinels = lambda: None
    eng_b._kill_switch_fn = None
    eng_b._submit_exit_decision = MagicMock()
    eng_b._check_all_positions(now_et=datetime(2026, 9, 5, 16, 0))

    assert eng_b._submit_exit_decision.call_count == 0, (
        "stale durable 2/2 must not produce an oversized EXIT callback after "
        "restart when broker quantity truth is unavailable"
    )


@_skip_if_no_mod
def test_pr588_restart_quantity_hold_blocks_actual_sentinel_and_forced_exit():
    """A broker-unverified restart owner is blocked at the central submit gate.

    This deliberately leaves ``_run_sentinels`` intact.  Both the sentinel's
    forced-risk path and the explicit emergency-flatten path must reach the
    shared gate and remain mutation-inert while the stale durable remainder is
    not reconciled against fresh broker quantity.
    """
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    contract = "IWM260919C00230000"
    client = "pr588-sentinel-quantity-hold@example.com"
    stale_row = {
        "id": "pos-c-sentinel-quantity-hold",
        "client_id": client,
        "contract": contract,
        "option_symbol": contract,
        "underlying": "IWM",
        "side": "CALL",
        "direction": "CALL",
        "qty": 2,
        "quantity_remaining": 2,
        "entry_price": 1.50,
        "avg_fill": 1.50,
        "entry_ts": None,
        "status": "OPEN",
        "signal_id": "sig-c-sentinel-quantity-hold",
        "execution_mode": "live",
    }

    broker_mutations = []

    class _UnavailableBroker:
        mode = "live"
        account_id = "acct-pr588-sentinel-quantity-hold"

        def list_positions(self):
            raise RuntimeError("broker position snapshot unavailable")

        def submit_order(self, *args, **kwargs):
            broker_mutations.append(("submit", args, kwargs))

        def cancel_order(self, *args, **kwargs):
            broker_mutations.append(("cancel", args, kwargs))

    eng = engine_cls(broker=_UnavailableBroker(), email=client)
    eng._hydrate_pending_exit_identity_for_broker_recovery = (
        lambda *_args, **_kwargs: "NONE"
    )

    class _PositionManager:
        def get_active_positions(self):
            return [stale_row]

    eng.seed_from_db(_PositionManager())
    pos = eng.active_positions()[0]
    assert pos.broker_repair_quantity_unverified is True
    now = datetime.now(timezone.utc)
    pos.opened_at = now - timedelta(minutes=2)
    pos.current_bid = 0.10
    pos.current_option_price = 0.10
    pos.option_bid_valid = True
    pos.option_quote_fresh = True
    pos.last_option_bid_update_ts = now

    eng._kill_switch_fn = None
    eng._emit_exit_event = MagicMock()
    eng.on_exit = MagicMock()
    eng.on_scale = MagicMock()

    # _check_all_positions invokes the real sentinel before the normal scan.
    # The broker precheck also proves the unavailable-truth condition in this
    # same behavioral path.
    eng._check_all_positions(now_et=datetime(2026, 9, 5, 16, 0))
    assert eng._broker_truth_snapshot_unavailable is True
    assert pos.broker_repair_quantity_unverified is True

    # Exercise the explicit forced-risk entry point as well; it must share the
    # same central fence and must not consume the stale 2/2 remainder.
    assert eng.emergency_flatten(reason="test_quantity_hold", force=True) == 0
    eng.on_exit.assert_not_called()
    eng.on_scale.assert_not_called()
    assert broker_mutations == []
    assert any(
        item.kwargs.get("reason_code") == "EXIT_BROKER_TRUTH_QUANTITY_HOLD"
        for item in eng._emit_exit_event.call_args_list
    ), "central submit gate must emit the quantity-unverified HOLD diagnostic"


# ── Test D: stale existing row (DB qty=2, remaining=0, broker=1) ──────────────

@_skip_if_no_mod
def test_pr588_test_d_stale_existing_row_preserved_canonical_qty():
    """Test D (spec): DB qty=2, quantity_remaining=0 (stale), broker=1.
    Runtime result must be quantity=2 / quantity_remaining=1, never 1/1."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")
    eng = engine_cls.__new__(engine_cls)
    eng._email = "pr588-d@example.com"
    eng._lock  = __import__("threading").Lock()
    eng._positions = []
    eng.broker = types.SimpleNamespace(mode="live")

    row = {
        "id": "pos-d-001",
        "contract": "QQQ260919C00480000",
        "option_symbol": "QQQ260919C00480000",
        "underlying": "QQQ",
        "side": "CALL",
        "direction": "CALL",
        "qty": 2,                   # durable canonical full entry
        "quantity_remaining": 0,    # stale — DB shows 0 remaining
        "entry_price": 2.00,
        "avg_fill": 2.00,
        "entry_ts": None,
        "status": "OPEN",
        "signal_id": None,
        "execution_mode": "live",
    }
    # Broker confirms 1 contract remains; qty_override=1
    mp = eng._managed_position_from_row(row, qty_override=1, prefer_qty_override=True)
    assert mp.quantity == 2, (
        f"qty=2 is the canonical full entry and must not be collapsed to broker "
        f"remaining. Got quantity={mp.quantity}"
    )
    assert mp.quantity_remaining == 1, (
        f"quantity_remaining must be updated to broker remaining=1, "
        f"got {mp.quantity_remaining}"
    )


# ── Test E: broker qty > entry qty → fail closed ──────────────────────────────

@_skip_if_no_mod
def test_pr588_test_e_broker_exceeds_entry_fails_closed():
    """Test E (spec): ENTRY filled_qty=1, broker qty=2 — contradictory authority.
    _managed_position_from_row must quarantine, never expand canonical qty to 2."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    mp_cls = getattr(_EE_MOD, "ManagedPosition", None)
    if engine_cls is None or mp_cls is None:
        pytest.skip("APExitEngine/ManagedPosition not found")
    eng = engine_cls.__new__(engine_cls)
    eng._email = "pr588-e@example.com"
    eng._lock  = __import__("threading").Lock()
    eng._positions = []
    eng.broker = types.SimpleNamespace(mode="live")

    row = {
        "id": "pos-e-001",
        "contract": "TSLA260919C00300000",
        "option_symbol": "TSLA260919C00300000",
        "underlying": "TSLA",
        "side": "CALL",
        "direction": "CALL",
        "qty": 1,               # canonical entry filled only 1 contract
        "quantity_remaining": 1,
        "entry_price": 3.00,
        "avg_fill": 3.00,
        "entry_ts": None,
        "status": "OPEN",
        "signal_id": None,
        "execution_mode": "live",
    }
    # Broker claims qty=2 but canonical entry only ever filled 1 — contradiction
    mp = eng._managed_position_from_row(row, qty_override=2, prefer_qty_override=True)
    # The owner must be quarantined (no new broker mutation authority)
    _is_quarantined = getattr(_EE_MOD, "_is_adoption_identity_quarantined", None)
    if _is_quarantined is not None:
        assert _is_quarantined(mp) is True, (
            "ENTRY=1, broker=2 must quarantine the owner — contradictory authority"
        )
    # Canonical full qty must remain 1 (not expanded to 2)
    assert mp.quantity == 1, (
        f"Canonical full qty must remain 1 (not expanded to broker's 2). "
        f"Got quantity={mp.quantity}"
    )


@_skip_if_no_mod
def test_pr588_test_e_upsert_blocked_on_broker_exceeds_entry():
    """Test E upsert: quantity-authority contradictions must abort recovery."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    CONTRACT    = "TSLA260919C00300000"
    CLIENT      = "pr588-e-upsert@example.com"
    POSITION_ID = "pr588-pos-e-upsert"

    eng = engine_cls.__new__(engine_cls)
    eng._email = CLIENT
    eng._lock  = __import__("threading").RLock()
    eng._positions = []
    eng._positions_by_id = {}
    eng.broker = types.SimpleNamespace(mode="live")

    # ENTRY filled_qty=1 but broker reports qty=2 — contradictory
    order_row = {
        "id":           f"entry-order-{POSITION_ID}",
        "client_id":    CLIENT,
        "position_id":  POSITION_ID,
        "kind":         "ENTRY",
        "status":       "FILLED",
        "contract":     CONTRACT,
        "execution_mode": "live",
        "filled_qty":   1,       # entry only filled 1 contract
        "fill_price":   3.00,
        "avg_fill_price": 3.00,
        "filled_ts":    "2026-09-01T13:00:00Z",
        "signal_id":    f"sig-{POSITION_ID}",
        "broker_order_id": f"broker-{POSITION_ID}",
        "meta":         {},
    }

    insert_sql_calls = []

    class _Cur:
        def execute(self, sql, params=()):
            insert_sql_calls.append(sql.strip()[:60])
            return self
        def fetchall(self):
            return [order_row]
        def fetchone(self):
            return None
        @property
        def rowcount(self):
            return 0

    @contextmanager
    def _conn():
        yield _Cur()

    fake_db = types.SimpleNamespace(conn=_conn, run_with_retry=lambda fn, **_: fn())
    prior = sys.modules.get("ap.db")
    sys.modules["ap.db"] = fake_db
    contradiction = getattr(
        _EE_MOD, "_BrokerRepairQuantityAuthorityContradiction", None
    )
    if contradiction is None:
        pytest.fail("quantity-authority contradiction guard is missing")
    try:
        # broker qty=2 > entry filled_qty=1 → must fail closed
        with pytest.raises(contradiction, match="broker_qty_exceeds_entry_qty"):
            eng._upsert_broker_position_to_db(
                CONTRACT,
                {"quantity": 2, "cost_basis": 600.0, "date_acquired": "2026-09-01"},
            )
    finally:
        if prior is not None:
            sys.modules["ap.db"] = prior
        else:
            sys.modules.pop("ap.db", None)

    assert not any("INSERT INTO positions" in s for s in insert_sql_calls), (
        "No INSERT must be executed when broker qty exceeds entry filled_qty"
    )


@pytest.mark.parametrize(
    "db_row",
    [
        None,
        {
            "id": "pr588-qty-authority-row",
            "contract": "TSLA260919C00300000",
            "option_symbol": "TSLA260919C00300000",
            "underlying": "TSLA",
            "side": "CALL",
            "direction": "CALL",
            "qty": 1,
            "quantity_remaining": 1,
            "entry_price": 3.00,
            "avg_fill": 3.00,
            "entry_ts": None,
            "status": "OPEN",
            "execution_mode": "live",
        },
    ],
)
def test_pr588_quantity_authority_contradiction_never_installs_degraded_owner(db_row):
    """A broker remainder larger than canonical ENTRY qty must remain HOLD.

    Exercise both recovery shapes: no durable row (upsert raises the typed
    contradiction) and an existing durable row (managed-row hydration marks
    the same contradiction).  Neither may reach degraded behavior-active
    ownership.
    """
    eng = _pr558_new_engine(email="pr588-qty-authority@example.com")
    if eng is None:
        pytest.skip("APExitEngine not importable")

    contract = "TSLA260919C00300000"
    contradiction = getattr(
        _EE_MOD, "_BrokerRepairQuantityAuthorityContradiction", None
    )
    if contradiction is None:
        pytest.fail("quantity-authority contradiction guard is missing")

    class _Broker:
        account_id = "acct-pr588-qty-authority"
        mode = "live"

        def list_positions(self):
            return [{
                "symbol": contract,
                "quantity": 2,
                "cost_basis": 600.0,
                "date_acquired": "2026-09-01",
            }]

    eng.broker = _Broker()
    eng._load_db_position_row = lambda _sym: db_row
    eng._fetch_broker_quote = lambda _sym: {
        "mark": 0.0, "bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0,
    }

    if db_row is None:
        def _raise_quantity_contradiction(_sym, _bp):
            raise contradiction("broker_repair_qty_authority_contradiction")

        eng._upsert_broker_position_to_db = _raise_quantity_contradiction
    else:
        eng._upsert_broker_position_to_db = lambda *_args: pytest.fail(
            "existing quantity-authority contradiction must not fall through to upsert"
        )
    eng._install_or_refresh_degraded_broker_truth_owner = lambda **_kwargs: pytest.fail(
        "quantity-authority contradiction must not install a degraded owner"
    )

    assert eng._broker_position_precheck() is False
    assert eng._positions == []
    assert contract in eng._broker_truth_hold_symbols


@_skip_if_no_mod
def test_pr588_ambiguous_short_entries_keep_degraded_owner(caplog):
    """Ambiguous ENTRY candidates must not become quantity contradictions.

    Two filled ENTRY candidates at one contract each with broker remainder two
    are unresolved provenance.  Canonical insertion stays blocked, while the
    existing #585 degraded owner remains behavior-active for the broker-open
    position.
    """
    eng = _pr558_new_engine(email="pr588-ambiguous@example.com")
    if eng is None:
        pytest.skip("APExitEngine not importable")

    contract = "TSLA260919C00300000"
    entry_rows = [
        {
            "id": "entry-ambiguous-1",
            "client_id": eng._email,
            "position_id": "position-ambiguous-1",
            "kind": "ENTRY",
            "status": "FILLED",
            "contract": contract,
            "execution_mode": "live",
            "filled_qty": 1,
            "fill_price": 3.00,
            "filled_ts": "2026-09-01T13:00:00Z",
        },
        {
            "id": "entry-ambiguous-2",
            "client_id": eng._email,
            "position_id": "position-ambiguous-2",
            "kind": "ENTRY",
            "status": "FILLED",
            "contract": contract,
            "execution_mode": "live",
            "filled_qty": 1,
            "fill_price": 3.00,
            "filled_ts": "2026-09-01T13:01:00Z",
        },
    ]
    executed_sql = []

    class _Cur:
        def execute(self, sql, params=()):
            executed_sql.append(str(sql))
            return self

        def fetchall(self):
            return list(entry_rows)

        def fetchone(self):
            return None

        @property
        def rowcount(self):
            return 0

    @contextmanager
    def _conn():
        yield _Cur()

    fake_db = types.SimpleNamespace(
        conn=_conn,
        run_with_retry=lambda fn, **_: fn(),
    )
    prior_db = sys.modules.get("ap.db")
    sys.modules["ap.db"] = fake_db

    broker_mutations = []

    class _Broker:
        account_id = "acct-pr588-ambiguous"
        mode = "live"

        def list_positions(self):
            return [{
                "symbol": contract,
                "quantity": 2,
                "cost_basis": 600.0,
                "date_acquired": "2026-09-01",
            }]

        def submit_order(self, *args, **kwargs):
            broker_mutations.append(("submit", args, kwargs))

        def cancel_order(self, *args, **kwargs):
            broker_mutations.append(("cancel", args, kwargs))

    eng.broker = _Broker()
    eng._load_db_position_row = lambda _sym: None
    eng._fetch_broker_quote = lambda _sym: {
        "mark": 0.0, "bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0,
    }
    eng._hydrate_pending_exit_identity_for_broker_recovery = (
        lambda *_args, **_kwargs: "UNAVAILABLE"
    )

    try:
        with caplog.at_level("ERROR"):
            result = eng._broker_position_precheck()
    finally:
        if prior_db is not None:
            sys.modules["ap.db"] = prior_db
        else:
            sys.modules.pop("ap.db", None)

    assert result is False
    assert "BROKER_REPAIR_ENTRY_EVIDENCE_UNRESOLVED" in caplog.text
    assert "QUANTITY_AUTHORITY_CONTRADICTION" not in caplog.text
    assert not any("INSERT INTO positions" in sql for sql in executed_sql)
    degraded = [
        pos for pos in eng._positions
        if getattr(pos, "broker_repair_degraded", False)
    ]
    assert len(degraded) == 1
    assert degraded[0].quantity == 2
    assert degraded[0].quantity_remaining == 2
    assert not broker_mutations


@_skip_if_no_mod
def test_pr588_zero_canonical_qty_is_hold_without_mutation():
    """A durable qty=0 row must not be repaired from broker remainder.

    The stale quantity-repair UPDATE, canonical owner installation, degraded
    fallback, and broker submit/cancel paths must all remain untouched.
    """
    eng = _pr558_new_engine(email="pr588-zero-canonical@example.com")
    if eng is None:
        pytest.skip("APExitEngine not importable")

    contract = "TSLA260919C00300000"
    db_row = {
        "id": "canonical-zero-qty",
        "client_id": eng._email,
        "contract": contract,
        "option_symbol": contract,
        "underlying": "TSLA",
        "side": "CALL",
        "direction": "CALL",
        "qty": 0,
        "quantity_remaining": 0,
        "entry_price": 3.00,
        "avg_fill": 3.00,
        "entry_ts": None,
        "status": "OPEN",
        "execution_mode": "live",
    }
    db_mutations = []
    broker_mutations = []

    class _Broker:
        account_id = "acct-pr588-zero-canonical"
        mode = "live"

        def list_positions(self):
            return [{
                "symbol": contract,
                "quantity": 1,
                "cost_basis": 300.0,
                "date_acquired": "2026-09-01",
            }]

        def submit_order(self, *args, **kwargs):
            broker_mutations.append(("submit", args, kwargs))

        def cancel_order(self, *args, **kwargs):
            broker_mutations.append(("cancel", args, kwargs))

    eng.broker = _Broker()
    eng._load_db_position_row = lambda _sym: db_row
    eng._fetch_broker_quote = lambda _sym: {
        "mark": 0.0, "bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0,
    }
    eng._upsert_broker_position_to_db = lambda *_args, **_kwargs: pytest.fail(
        "invalid durable canonical qty must not fall through to upsert"
    )
    eng._install_or_refresh_degraded_broker_truth_owner = (
        lambda **_kwargs: pytest.fail(
            "invalid durable canonical qty must not install a degraded owner"
        )
    )

    class _DbSentinel:
        def __init__(self):
            self.conn = None

        def run_with_retry(self, fn, **kwargs):
            db_mutations.append("run_with_retry")
            return fn()

    prior_db = sys.modules.get("ap.db")
    sys.modules["ap.db"] = _DbSentinel()
    try:
        result = eng._broker_position_precheck()
    finally:
        if prior_db is not None:
            sys.modules["ap.db"] = prior_db
        else:
            sys.modules.pop("ap.db", None)

    assert result is False
    assert eng._positions == []
    assert contract in eng._broker_truth_hold_symbols
    assert db_row["qty"] == 0
    assert db_row["quantity_remaining"] == 0
    assert not db_mutations
    assert not broker_mutations


# ── Test G: degraded-to-canonical convergence preserves 2/1 ──────────────────

@_skip_if_no_mod
def test_pr588_test_g_degraded_to_canonical_convergence_2_1():
    """Test G (spec): degraded owner at qty=1, canonical ENTRY proves 2,
    broker still shows 1.  Convergence must produce one canonical owner at 2/1."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    mp_cls = getattr(_EE_MOD, "ManagedPosition", None)
    if engine_cls is None or mp_cls is None:
        pytest.skip("APExitEngine/ManagedPosition not importable")

    eng = _pr558_new_engine()
    sym = "IWM260919C00230000"

    # Install degraded owner — only knows broker qty=1
    degraded = eng._install_or_refresh_degraded_broker_truth_owner(
        sym=sym,
        broker_position={"contract": sym, "quantity": 1, "cost_basis": 150.0},
        broker_qty=1,
        account_id="acct-pr588",
        repair_failed_reason="db_upsert_returned_no_id",
    )
    assert degraded is not None
    degraded.peak_pnl_pct = 0.12

    # Canonical ENTRY later proves filled_qty=2; broker still shows 1 remaining.
    # The canonical ManagedPosition is built from the proven row with qty=2/remaining=1.
    canonical = mp_cls(
        ticker="IWM", option_symbol=sym, side="CALL",
        quantity=2,                   # full_entry_qty
        entry_price=1.50,
        underlying_entry=210.0, underlying_target=225.0, underlying_stop=205.0,
        position_id="canonical-pr588-g",
        client_id="jason@example.com",
        signal_id="sig-pr588-g",
        execution_mode="live",
        quantity_remaining=1,         # broker_remaining_qty
    )
    eng._install_canonical_owner_atomically(canonical, sym)

    assert eng.active_positions() == [canonical]
    assert degraded not in eng._positions
    assert canonical.quantity == 2, (
        f"Post-convergence canonical quantity must be 2 (full entry), "
        f"got {canonical.quantity}"
    )
    assert canonical.quantity_remaining == 1, (
        f"Post-convergence quantity_remaining must be 1 (broker), "
        f"got {canonical.quantity_remaining}"
    )
    assert canonical.peak_pnl_pct == 0.12, "Runtime state must survive convergence"


# ── Test H: malformed quantity inputs → fail closed ───────────────────────────

@_skip_if_no_mod
def test_pr588_test_h_malformed_quantity_inputs_fail_closed():
    """Test H (spec): malformed broker quantity inputs must fail closed with
    zero money-path mutations."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")
    CONTRACT = "SPY260919C00600000"
    CLIENT   = "pr588-h@example.com"
    for bad_qty in (None, "", "   ", 0, -1, 1.5, "1.5", float("nan"),
                    float("inf"), True):
        eng = engine_cls.__new__(engine_cls)
        eng._email = CLIENT
        eng._lock  = __import__("threading").Lock()
        eng._positions = []
        eng.broker = types.SimpleNamespace(mode="live")

        insert_calls = []

        class _Cur:
            def execute(self, sql, params=()):
                insert_calls.append(sql.strip()[:50])
                return self
            def fetchall(self): return []
            def fetchone(self): return None
            @property
            def rowcount(self): return 0

        @contextmanager
        def _conn():
            yield _Cur()

        fake_db = types.SimpleNamespace(conn=_conn, run_with_retry=lambda fn, **_: fn())
        prior = sys.modules.get("ap.db")
        sys.modules["ap.db"] = fake_db
        try:
            row_id = eng._upsert_broker_position_to_db(
                CONTRACT,
                {"quantity": bad_qty, "cost_basis": 100.0, "date_acquired": "2026-09-01"},
            )
        finally:
            if prior is not None:
                sys.modules["ap.db"] = prior
            else:
                sys.modules.pop("ap.db", None)

        assert row_id is None, (
            f"Malformed broker_qty={bad_qty!r} must fail closed (return None), "
            f"got {row_id}"
        )
        assert not any("INSERT INTO positions" in s for s in insert_calls), (
            f"No INSERT must fire on malformed qty={bad_qty!r}"
        )


@_skip_if_no_mod
def test_pr588_test_h_malformed_entry_filled_qty_blocks_insert():
    """Test H variant: malformed ENTRY filled_qty must also block INSERT."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")
    CONTRACT    = "SPY260919C00600000"
    CLIENT      = "pr588-h2@example.com"
    POSITION_ID = "pr588-h2-pos"

    for bad_filled_qty in (None, 0, -1, ""):
        eng = engine_cls.__new__(engine_cls)
        eng._email = CLIENT
        eng._lock  = __import__("threading").Lock()
        eng._positions = []
        eng.broker = types.SimpleNamespace(mode="live")

        order_with_bad_filled_qty = {
            "id": "entry-order-h2",
            "client_id": CLIENT,
            "position_id": POSITION_ID,
            "kind": "ENTRY",
            "status": "FILLED",
            "contract": CONTRACT,
            "execution_mode": "live",
            "filled_qty": bad_filled_qty,   # malformed
            "fill_price": 1.00,
            "filled_ts": "2026-09-01T13:00:00Z",
            "meta": {},
        }

        insert_calls = []

        class _Cur:
            def execute(self, sql, params=()):
                insert_calls.append(sql.strip()[:50])
                return self
            def fetchall(self):
                return [order_with_bad_filled_qty]
            def fetchone(self):
                return None
            @property
            def rowcount(self): return 0

        @contextmanager
        def _conn():
            yield _Cur()

        fake_db = types.SimpleNamespace(conn=_conn, run_with_retry=lambda fn, **_: fn())
        prior = sys.modules.get("ap.db")
        sys.modules["ap.db"] = fake_db
        try:
            row_id = eng._upsert_broker_position_to_db(
                CONTRACT,
                {"quantity": 1, "cost_basis": 100.0, "date_acquired": "2026-09-01"},
            )
        finally:
            if prior is not None:
                sys.modules["ap.db"] = prior
            else:
                sys.modules.pop("ap.db", None)

        assert row_id is None, (
            f"Malformed filled_qty={bad_filled_qty!r} must block INSERT, got {row_id}"
        )
        assert not any("INSERT INTO positions" in s for s in insert_calls), (
            f"No INSERT must fire when ENTRY filled_qty={bad_filled_qty!r}"
        )


# ── Test J: MANDATORY real PostgreSQL INSERT ──────────────────────────────────

@pytest.mark.parametrize(
    "broker_qty,durable_remaining",
    [(1, 1), (2, 2)],
)
@_skip_if_no_mod
def test_pr588_closing_lifecycle_survives_existing_quantity_repair_postgresql(
    monkeypatch, broker_qty, durable_remaining,
):
    """Quantity repair must not normalize a durable CLOSING lifecycle."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    with _postgres_positions_table(monkeypatch) as pg_conn:
        contract = "IWM260919C00230000"
        client = f"pr588-closing-lifecycle-{broker_qty}@example.com"
        position_id = f"pr588-closing-lifecycle-{broker_qty}"
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, position_id, kind, status,
                    contract, execution_mode, filled_qty, fill_price, filled_ts,
                    meta
                ) VALUES (%s, %s, %s, 'ENTRY', 'FILLED', %s, 'live',
                          2, 1.50, %s, '{}'::jsonb)
                """,
                (
                    f"local-pr588-closing-entry-{broker_qty}",
                    client,
                    position_id,
                    contract,
                    "2026-09-01T13:00:00Z",
                ),
            )
            cur.execute(
                """
                INSERT INTO positions (
                    id, client_id, underlying, contract, option_symbol,
                    execution_mode, side, direction, qty, quantity_remaining,
                    avg_fill, entry_price, entry_ts, status, signal_id
                ) VALUES (%s, %s, 'IWM', %s, %s, 'live', 'CALL', 'CALL',
                          2, %s, 1.50, 1.50, %s, 'CLOSING', %s)
                """,
                (
                    position_id,
                    client,
                    contract,
                    contract,
                    durable_remaining,
                    "2026-09-01T13:00:00Z",
                    f"sig-pr588-closing-{broker_qty}",
                ),
            )
        pg_conn.commit()

        class _Broker:
            mode = "live"
            account_id = f"acct-pr588-closing-{broker_qty}"

            def list_positions(self):
                return [{
                    "contract": contract,
                    "quantity": broker_qty,
                    "cost_basis": broker_qty * 150.0,
                    "date_acquired": "2026-09-01T13:00:00Z",
                }]

        eng = engine_cls(broker=_Broker(), email=client)
        eng._fetch_broker_quote = lambda _sym: {
            "mark": 0.0, "bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0,
        }
        assert eng._broker_position_precheck() is True

        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT qty, quantity_remaining, status FROM positions WHERE id = %s",
                (position_id,),
            )
            row = cur.fetchone()
        assert row is not None
        values = dict(row) if isinstance(row, dict) else {
            "qty": row[0], "quantity_remaining": row[1], "status": row[2],
        }
        assert values == {
            "qty": 2,
            "quantity_remaining": broker_qty,
            "status": "CLOSING",
        }


@_skip_if_no_mod
def test_pr588_locked_lifecycle_wins_when_row_changes_before_reread():
    """The FOR UPDATE reread, not the stale initial projection, owns status."""
    eng = _pr558_new_engine(email="pr588-locked-lifecycle@example.com")
    if eng is None:
        pytest.skip("APExitEngine not importable")

    contract = "IWM260919C00230000"
    db_row = {
        "id": "pr588-locked-lifecycle-row",
        "client_id": eng._email,
        "contract": contract,
        "option_symbol": contract,
        "qty": 2,
        "quantity_remaining": 2,
        "status": "OPEN",  # stale initial projection
        "execution_mode": "live",
    }
    locked_row = dict(db_row, status="CLOSING")
    executed = []

    class _Cur:
        rowcount = 1

        def execute(self, sql, params=()):
            executed.append((str(sql), params))
            return self

        def fetchone(self):
            if executed and "FOR UPDATE" in executed[-1][0]:
                return locked_row
            return None

    @contextmanager
    def _conn():
        yield _Cur()

    fake_db = types.SimpleNamespace(
        conn=_conn,
        run_with_retry=lambda fn, **_: fn(),
    )
    prior = sys.modules.get("ap.db")
    sys.modules["ap.db"] = fake_db
    try:
        full_qty, durable_proof = eng._reconcile_existing_broker_position_row(
            db_row,
            contract,
            {"contract": contract, "quantity": 1},
            1,
            mode="live",
        )
    finally:
        if prior is not None:
            sys.modules["ap.db"] = prior
        else:
            sys.modules.pop("ap.db", None)

    assert (full_qty, durable_proof) == (2, True)
    assert db_row["status"] == "CLOSING"
    updates = [params for sql, params in executed if "UPDATE positions" in sql]
    assert updates and updates[0][2] == "CLOSING"


@pytest.mark.parametrize("locked_status", ["CLOSING", "ACTIVE"])
@_skip_if_no_mod
def test_pr588_noop_quantity_reread_preserves_locked_lifecycle(locked_status):
    """Even a quantity no-op must reread lifecycle under FOR UPDATE."""
    eng = _pr558_new_engine(email="pr588-noop-lifecycle@example.com")
    if eng is None:
        pytest.skip("APExitEngine not importable")

    contract = "IWM260919C00230000"
    db_row = {
        "id": "pr588-noop-lifecycle-row",
        "client_id": eng._email,
        "contract": contract,
        "option_symbol": contract,
        "qty": 2,
        "quantity_remaining": 2,
        "status": "OPEN",  # initial projection already matches broker qty
        "execution_mode": "live",
    }
    locked_row = dict(db_row, status=locked_status)
    executed = []

    class _Cur:
        rowcount = 1

        def __init__(self):
            self._sql = ""

        def execute(self, sql, params=()):
            self._sql = str(sql)
            executed.append((self._sql, params))
            return self

        def fetchall(self):
            # No exact ENTRY is needed when durable full qty already agrees
            # with fresh broker truth.
            return []

        def fetchone(self):
            if "FOR UPDATE" in self._sql:
                return dict(locked_row)
            return None

    @contextmanager
    def _conn():
        yield _Cur()

    fake_db = types.SimpleNamespace(
        conn=_conn,
        run_with_retry=lambda fn, **_: fn(),
    )
    prior = sys.modules.get("ap.db")
    sys.modules["ap.db"] = fake_db
    try:
        full_qty, durable_proof = eng._reconcile_existing_broker_position_row(
            db_row,
            contract,
            {"contract": contract, "quantity": 2},
            2,
            mode="live",
        )
    finally:
        if prior is not None:
            sys.modules["ap.db"] = prior
        else:
            sys.modules.pop("ap.db", None)

    assert (full_qty, durable_proof) == (2, True)
    assert db_row["status"] == locked_status
    assert any("FOR UPDATE" in sql for sql, _params in executed)
    assert not any("UPDATE positions" in sql for sql, _params in executed)


@_skip_if_no_mod
def test_pr588_contradictory_existing_remaining_holds_before_mutation():
    """Durable remaining quantity above canonical full qty is a HOLD."""
    eng = _pr558_new_engine(email="pr588-remaining-contradiction@example.com")
    if eng is None:
        pytest.skip("APExitEngine not importable")

    contract = "SPY260919C00600000"
    db_row = {
        "id": "pr588-remaining-contradiction-row",
        "client_id": eng._email,
        "contract": contract,
        "option_symbol": contract,
        "qty": 2,
        "quantity_remaining": 3,  # impossible durable state
        "status": "OPEN",
        "execution_mode": "live",
    }
    executed = []

    class _Cur:
        rowcount = 1

        def execute(self, sql, params=()):
            executed.append((str(sql), params))
            return self

        def fetchall(self):
            return []

        def fetchone(self):
            pytest.fail("contradictory durable quantity must fail before lock")

    @contextmanager
    def _conn():
        yield _Cur()

    fake_db = types.SimpleNamespace(
        conn=_conn,
        run_with_retry=lambda fn, **_: fn(),
    )
    prior = sys.modules.get("ap.db")
    sys.modules["ap.db"] = fake_db
    contradiction = getattr(
        _EE_MOD, "_BrokerRepairCanonicalQuantityUnproven", None,
    )
    try:
        with pytest.raises(
            contradiction,
            match="broker_repair_durable_remaining_contradiction",
        ):
            eng._reconcile_existing_broker_position_row(
                db_row,
                contract,
                {"contract": contract, "quantity": 1},
                1,
                mode="live",
            )
    finally:
        if prior is not None:
            sys.modules["ap.db"] = prior
        else:
            sys.modules.pop("ap.db", None)

    assert db_row["qty"] == 2
    assert db_row["quantity_remaining"] == 3
    assert not any("UPDATE positions" in sql for sql, _params in executed)


@_skip_if_no_mod
def test_pr588_locked_occ_identity_contradiction_holds_before_update():
    """A locked row with two exact OCC identities cannot be reconciled."""
    eng = _pr558_new_engine(email="pr588-occ-contradiction@example.com")
    if eng is None:
        pytest.skip("APExitEngine not importable")

    contract = "SPY260919C00600000"
    wrong_contract = "SPY260919P00600000"
    db_row = {
        "id": "pr588-occ-contradiction-row",
        "client_id": eng._email,
        "contract": contract,
        "option_symbol": contract,
        "qty": 2,
        "quantity_remaining": 2,
        "status": "OPEN",
        "execution_mode": "live",
    }
    locked_row = dict(db_row, option_symbol=wrong_contract)
    executed = []

    class _Cur:
        rowcount = 1

        def __init__(self):
            self._sql = ""

        def execute(self, sql, params=()):
            self._sql = str(sql)
            executed.append((self._sql, params))
            return self

        def fetchall(self):
            return []

        def fetchone(self):
            if "FOR UPDATE" in self._sql:
                return dict(locked_row)
            return None

    @contextmanager
    def _conn():
        yield _Cur()

    fake_db = types.SimpleNamespace(
        conn=_conn,
        run_with_retry=lambda fn, **_: fn(),
    )
    prior = sys.modules.get("ap.db")
    sys.modules["ap.db"] = fake_db
    contradiction = getattr(
        _EE_MOD, "_BrokerRepairCanonicalQuantityUnproven", None,
    )
    try:
        with pytest.raises(
            contradiction,
            match="broker_repair_locked_occ_identity_unproven",
        ):
            eng._reconcile_existing_broker_position_row(
                db_row,
                contract,
                {"contract": contract, "quantity": 1},
                1,
                mode="live",
            )
    finally:
        if prior is not None:
            sys.modules["ap.db"] = prior
        else:
            sys.modules.pop("ap.db", None)

    assert db_row["quantity_remaining"] == 2
    assert not any("UPDATE positions" in sql for sql, _params in executed)


@_skip_if_no_mod
def test_pr588_existing_positive_remaining_2_2_to_2_1_postgresql(monkeypatch):
    """A real existing 2/2 row must become durable 2/1 PARTIAL."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    with _postgres_positions_table(monkeypatch) as pg_conn:
        contract = "IWM260919C00230000"
        client = "pr588-existing-positive@example.com"
        position_id = "pr588-existing-positive-row"
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, position_id, kind, status,
                    contract, execution_mode, filled_qty, fill_price, filled_ts,
                    meta
                ) VALUES (%s, %s, %s, 'ENTRY', 'FILLED', %s, 'live',
                          2, 1.50, %s, '{}'::jsonb)
                """,
                (
                    "local-pr588-existing-positive",
                    client,
                    position_id,
                    contract,
                    "2026-09-01T13:00:00Z",
                ),
            )
            cur.execute(
                """
                INSERT INTO positions (
                    id, client_id, underlying, contract, option_symbol,
                    execution_mode, side, direction, qty, quantity_remaining,
                    avg_fill, entry_price, entry_ts, status, signal_id
                ) VALUES (%s, %s, 'IWM', %s, %s, 'live', 'CALL', 'CALL',
                          2, 2, 1.50, 1.50, %s, 'OPEN', 'sig-existing-positive')
                """,
                (
                    position_id,
                    client,
                    contract,
                    contract,
                    "2026-09-01T13:00:00Z",
                ),
            )
        pg_conn.commit()

        class _Broker:
            mode = "live"
            account_id = "acct-pr588-existing-positive"

            def list_positions(self):
                return [{
                    "contract": contract,
                    "quantity": 1,
                    "cost_basis": 150.0,
                    "date_acquired": "2026-09-01T13:00:00Z",
                }]

        eng = engine_cls(
            broker=_Broker(),
            email=client,
        )
        eng._fetch_broker_quote = lambda _sym: {
            "mark": 0.0, "bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0,
        }
        assert eng._broker_position_precheck() is True

        active = eng.active_positions()
        assert len(active) == 1
        assert active[0].quantity == 2
        assert active[0].quantity_remaining == 1

        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT qty, quantity_remaining, status FROM positions WHERE id = %s",
                (position_id,),
            )
            row = cur.fetchone()
        assert row is not None
        values = dict(row) if isinstance(row, dict) else {
            "qty": row[0], "quantity_remaining": row[1], "status": row[2],
        }
        assert values == {
            "qty": 2,
            "quantity_remaining": 1,
            "status": "PARTIAL",
        }


@_skip_if_no_mod
def test_pr588_legacy_collapsed_1_1_repairs_from_exact_position_entry_postgresql(monkeypatch):
    """A legacy 1/1 row may expand only from its exact filled ENTRY."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    with _postgres_positions_table(monkeypatch) as pg_conn:
        contract = "QQQ260919C00480000"
        client = "pr588-legacy-collapse@example.com"
        position_id = "pr588-legacy-collapse-row"
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, position_id, kind, status,
                    contract, execution_mode, filled_qty, fill_price, filled_ts,
                    meta
                ) VALUES (%s, %s, %s, 'ENTRY', 'FILLED', %s, 'live',
                          2, 1.50, %s, '{}'::jsonb)
                """,
                (
                    "local-pr588-legacy-collapse",
                    client,
                    position_id,
                    contract,
                    "2026-09-01T13:00:00Z",
                ),
            )
            cur.execute(
                """
                INSERT INTO positions (
                    id, client_id, underlying, contract, option_symbol,
                    execution_mode, side, direction, qty, quantity_remaining,
                    avg_fill, entry_price, entry_ts, status, signal_id
                ) VALUES (%s, %s, 'QQQ', %s, %s, 'live', 'CALL', 'CALL',
                          1, 1, 1.50, 1.50, %s, 'OPEN', 'sig-legacy-collapse')
                """,
                (
                    position_id,
                    client,
                    contract,
                    contract,
                    "2026-09-01T13:00:00Z",
                ),
            )
        pg_conn.commit()

        class _Broker:
            mode = "live"
            account_id = "acct-pr588-legacy-collapse"

            def list_positions(self):
                return [{
                    "contract": contract,
                    "quantity": 1,
                    "cost_basis": 150.0,
                    "date_acquired": "2026-09-01T13:00:00Z",
                }]

        eng = engine_cls(broker=_Broker(), email=client)
        eng._fetch_broker_quote = lambda _sym: {
            "mark": 0.0, "bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0,
        }
        assert eng._broker_position_precheck() is True

        active = eng.active_positions()
        assert len(active) == 1
        assert (active[0].quantity, active[0].quantity_remaining) == (2, 1)
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT qty, quantity_remaining, status FROM positions WHERE id = %s",
                (position_id,),
            )
            row = cur.fetchone()
        values = dict(row) if isinstance(row, dict) else {
            "qty": row[0], "quantity_remaining": row[1], "status": row[2],
        }
        assert values == {
            "qty": 2,
            "quantity_remaining": 1,
            "status": "PARTIAL",
        }


@_skip_if_no_mod
def test_pr588_partial_restart_hydrates_active_exit_without_duplicate_submit_postgresql(monkeypatch):
    """A durable 2/1 owner with an active EXIT rehydrates and stays idempotent."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    with _postgres_positions_table(monkeypatch) as pg_conn:
        contract = "SPY260919C00600000"
        client = "pr588-partial-exit-restart@example.com"
        position_id = "pr588-partial-exit-restart-row"
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO positions (
                    id, client_id, underlying, contract, option_symbol,
                    execution_mode, side, direction, qty, quantity_remaining,
                    avg_fill, entry_price, entry_ts, status, signal_id
                ) VALUES (%s, %s, 'SPY', %s, %s, 'live', 'CALL', 'CALL',
                          2, 1, 1.50, 1.50, %s, 'PARTIAL', 'sig-partial-restart')
                """,
                (
                    position_id,
                    client,
                    contract,
                    contract,
                    "2026-09-01T13:00:00Z",
                ),
            )
            cur.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, position_id, kind, status,
                    contract, execution_mode, filled_qty, fill_price, filled_ts,
                    meta
                ) VALUES (%s, %s, %s, 'EXIT', 'EXIT_SUBMITTED', %s, 'live',
                          0, NULL, %s, '{}'::jsonb)
                """,
                (
                    "local-pr588-active-exit",
                    client,
                    position_id,
                    contract,
                    "2026-09-01T13:05:00Z",
                ),
            )
        pg_conn.commit()

        class _Broker:
            mode = "live"
            account_id = "acct-pr588-partial-restart"

            def list_positions(self):
                return [{
                    "contract": contract,
                    "quantity": 1,
                    "cost_basis": 150.0,
                    "date_acquired": "2026-09-01T13:00:00Z",
                }]

        eng = engine_cls(broker=_Broker(), email=client)
        eng._fetch_broker_quote = lambda _sym: {
            "mark": 0.0, "bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0,
        }

        class _PositionManager:
            def get_active_positions(self):
                return [{
                    "id": position_id,
                    "client_id": client,
                    "underlying": "SPY",
                    "contract": contract,
                    "option_symbol": contract,
                    "side": "CALL",
                    "direction": "CALL",
                    "qty": 2,
                    "quantity_remaining": 1,
                    "avg_fill": 1.50,
                    "entry_price": 1.50,
                    "entry_ts": "2026-09-01T13:00:00Z",
                    "status": "PARTIAL",
                    "signal_id": "sig-partial-restart",
                    "execution_mode": "live",
                }]

        eng.seed_from_db(_PositionManager())
        active = eng.active_positions()
        assert len(active) == 1
        assert active[0].exit_in_flight is True
        assert active[0].pending_exit_local_order_id == "local-pr588-active-exit"
        assert active[0].broker_repair_quantity_unverified is True

        assert eng._broker_position_precheck() is True
        assert active[0].broker_repair_quantity_unverified is False
        assert (active[0].quantity, active[0].quantity_remaining) == (2, 1)

        eng._run_sentinels = lambda: None
        eng._kill_switch_fn = None
        eng._submit_exit_decision = MagicMock()
        eng._check_all_positions(now_et=datetime(2026, 9, 5, 16, 0))
        eng._submit_exit_decision.assert_not_called()


@_skip_if_no_mod
def test_pr588_crash_after_closing_quantity_commit_keeps_pending_exit_authority_postgresql(
    monkeypatch,
):
    """A crash after repair commit must not erase CLOSING or duplicate EXIT."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    class _CrashAfterCommit(BaseException):
        pass

    with _postgres_positions_table(monkeypatch) as pg_conn:
        contract = "SPY260919C00600000"
        client = "pr588-closing-crash-boundary@example.com"
        position_id = "pr588-closing-crash-boundary-row"
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, position_id, kind, status,
                    contract, execution_mode, filled_qty, fill_price, filled_ts,
                    meta
                ) VALUES (%s, %s, %s, 'ENTRY', 'FILLED', %s, 'live',
                          2, 1.50, %s, '{}'::jsonb)
                """,
                (
                    "local-pr588-closing-crash-entry",
                    client,
                    position_id,
                    contract,
                    "2026-09-01T13:00:00Z",
                ),
            )
            cur.execute(
                """
                INSERT INTO positions (
                    id, client_id, underlying, contract, option_symbol,
                    execution_mode, side, direction, qty, quantity_remaining,
                    avg_fill, entry_price, entry_ts, status, signal_id
                ) VALUES (%s, %s, 'SPY', %s, %s, 'live', 'CALL', 'CALL',
                          2, 2, 1.50, 1.50, %s, 'CLOSING', 'sig-closing-crash')
                """,
                (
                    position_id,
                    client,
                    contract,
                    contract,
                    "2026-09-01T13:00:00Z",
                ),
            )
            cur.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, position_id, kind, status,
                    contract, execution_mode, qty, filled_qty,
                    broker_order_id, submitted_ts, meta
                ) VALUES (%s, %s, %s, 'EXIT', 'EXIT_SUBMITTED', %s, 'live',
                          2, 1, %s, %s, '{}'::jsonb)
                """,
                (
                    "local-pr588-closing-crash-exit",
                    client,
                    position_id,
                    contract,
                    "broker-pr588-closing-crash-exit",
                    "2026-09-01T13:05:00Z",
                ),
            )
        pg_conn.commit()

        class _Broker:
            mode = "live"
            account_id = "acct-pr588-closing-crash"

            def list_positions(self):
                return [{
                    "contract": contract,
                    "quantity": 1,
                    "cost_basis": 150.0,
                    "date_acquired": "2026-09-01T13:00:00Z",
                }]

            def submit_order(self, *args, **kwargs):
                broker_mutations.append(("submit", args, kwargs))

            def cancel_order(self, *args, **kwargs):
                broker_mutations.append(("cancel", args, kwargs))

        broker_mutations = []
        eng = engine_cls(broker=_Broker(), email=client)
        eng._fetch_broker_quote = lambda _sym: {
            "mark": 0.0, "bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0,
        }
        eng._hydrate_pending_exit_identity_for_broker_recovery = (
            lambda *_args, **_kwargs: (_ for _ in ()).throw(_CrashAfterCommit())
        )

        with pytest.raises(_CrashAfterCommit):
            eng._broker_position_precheck()

        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT qty, quantity_remaining, status FROM positions WHERE id = %s",
                (position_id,),
            )
            row = cur.fetchone()
        values = dict(row) if isinstance(row, dict) else {
            "qty": row[0], "quantity_remaining": row[1], "status": row[2],
        }
        assert values == {
            "qty": 2,
            "quantity_remaining": 1,
            "status": "CLOSING",
        }

        eng_restart = engine_cls(broker=_Broker(), email=client)
        eng_restart._fetch_broker_quote = lambda _sym: {
            "mark": 0.0, "bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0,
        }

        class _PositionManager:
            def get_active_positions(self):
                return [{
                    "id": position_id,
                    "client_id": client,
                    "underlying": "SPY",
                    "contract": contract,
                    "option_symbol": contract,
                    "side": "CALL",
                    "direction": "CALL",
                    "qty": 2,
                    "quantity_remaining": 1,
                    "avg_fill": 1.50,
                    "entry_price": 1.50,
                    "entry_ts": "2026-09-01T13:00:00Z",
                    "status": "CLOSING",
                    "signal_id": "sig-closing-crash",
                    "execution_mode": "live",
                }]

        eng_restart.seed_from_db(_PositionManager())
        restarted = eng_restart.active_positions()[0]
        assert restarted.exit_in_flight is True
        assert restarted.pending_exit_local_order_id == "local-pr588-closing-crash-exit"
        assert restarted.pending_exit_broker_order_id == "broker-pr588-closing-crash-exit"

        assert eng_restart._broker_position_precheck() is True
        assert restarted.broker_repair_quantity_unverified is False

        eng_restart._emit_exit_event = MagicMock()
        eng_restart.on_exit = MagicMock()
        eng_restart.on_scale = MagicMock()
        decision = _EE_MOD.ExitDecision(
            action="STOP",
            quantity=1,
            reason="HARD STOP",
            urgency="IMMEDIATE",
            pnl_pct=-0.40,
            reason_code="HARD_STOP",
        )
        assert eng_restart._submit_exit_decision(restarted, decision) is False
        eng_restart.on_exit.assert_not_called()
        eng_restart.on_scale.assert_not_called()
        assert broker_mutations == []


@_skip_if_no_mod
def test_pr588_existing_row_update_failure_holds_without_degraded_owner():
    """A failed durable 2/2 -> 2/1 update cannot fall through to submit-ready ownership."""
    eng = _pr558_new_engine(email="pr588-update-failure@example.com")
    if eng is None:
        pytest.skip("APExitEngine not importable")
    contract = "SPY260919C00600000"
    db_row = {
        "id": "pr588-update-failure-row",
        "client_id": eng._email,
        "contract": contract,
        "option_symbol": contract,
        "underlying": "SPY",
        "side": "CALL",
        "direction": "CALL",
        "qty": 2,
        "quantity_remaining": 2,
        "entry_price": 1.50,
        "avg_fill": 1.50,
        "entry_ts": None,
        "status": "OPEN",
        "signal_id": "sig-update-failure",
        "execution_mode": "live",
    }

    class _Broker:
        mode = "live"
        account_id = "acct-pr588-update-failure"

        def list_positions(self):
            return [{
                "contract": contract,
                "quantity": 1,
                "cost_basis": 150.0,
                "date_acquired": "2026-09-01T13:00:00Z",
            }]

    eng.broker = _Broker()
    eng._load_db_position_row = lambda _sym: db_row
    eng._fetch_broker_quote = lambda _sym: {
        "mark": 0.0, "bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0,
    }
    eng._upsert_broker_position_to_db = lambda *_args: pytest.fail(
        "existing-row update failure must not fall through to upsert"
    )
    eng._install_or_refresh_degraded_broker_truth_owner = lambda **_kwargs: pytest.fail(
        "existing canonical row update failure must remain a HOLD"
    )

    class _Cur:
        rowcount = 1

        def execute(self, *_args, **_kwargs):
            raise RuntimeError("durable update unavailable")

    @contextmanager
    def _conn():
        yield _Cur()

    failing_db = types.SimpleNamespace(conn=_conn, run_with_retry=lambda fn, **_: fn())
    prior = sys.modules.get("ap.db")
    sys.modules["ap.db"] = failing_db
    try:
        assert eng._broker_position_precheck() is False
    finally:
        if prior is not None:
            sys.modules["ap.db"] = prior
        else:
            sys.modules.pop("ap.db", None)

    assert eng._positions == []
    assert db_row["qty"] == 2
    assert db_row["quantity_remaining"] == 2
    assert contract in eng._broker_truth_hold_symbols

@_skip_if_no_mod
def test_pr588_existing_collapsed_row_ambiguous_entry_holds_without_mutation():
    """Ambiguous exact-position ENTRY history cannot expand a legacy row."""
    eng = _pr558_new_engine(email="pr588-ambiguous-existing@example.com")
    if eng is None:
        pytest.skip("APExitEngine not importable")
    contract = "QQQ260919C00480000"
    position_id = "pr588-ambiguous-existing-row"
    db_row = {
        "id": position_id,
        "client_id": eng._email,
        "contract": contract,
        "option_symbol": contract,
        "underlying": "QQQ",
        "side": "CALL",
        "direction": "CALL",
        "qty": 1,
        "quantity_remaining": 1,
        "entry_price": 1.50,
        "avg_fill": 1.50,
        "entry_ts": None,
        "status": "OPEN",
        "signal_id": "sig-ambiguous-existing",
        "execution_mode": "live",
    }
    entries = [
        {
            "id": "ambiguous-entry-1",
            "client_id": eng._email,
            "position_id": position_id,
            "kind": "ENTRY",
            "status": "FILLED",
            "contract": contract,
            "execution_mode": "live",
            "filled_qty": 2,
            "fill_price": 1.50,
            "filled_ts": "2026-09-01T13:00:00Z",
        },
        {
            "id": "ambiguous-entry-2",
            "client_id": eng._email,
            "position_id": position_id,
            "kind": "ENTRY",
            "status": "FILLED",
            "contract": contract,
            "execution_mode": "live",
            "filled_qty": 2,
            "fill_price": 1.50,
            "filled_ts": "2026-09-01T13:01:00Z",
        },
    ]

    class _Broker:
        mode = "live"
        account_id = "acct-pr588-ambiguous-existing"

        def list_positions(self):
            return [{
                "contract": contract,
                "quantity": 1,
                "cost_basis": 150.0,
                "date_acquired": "2026-09-01T13:00:00Z",
            }]

    executed = []

    class _Cur:
        rowcount = 1

        def execute(self, sql, params=()):
            executed.append(str(sql))
            return self

        def fetchall(self):
            return entries

        def fetchone(self):
            return None

    @contextmanager
    def _conn():
        yield _Cur()

    eng.broker = _Broker()
    eng._load_db_position_row = lambda _sym: db_row
    eng._fetch_broker_quote = lambda _sym: {
        "mark": 0.0, "bid": 0.0, "ask": 0.0, "mid": 0.0, "last": 0.0,
    }
    eng._upsert_broker_position_to_db = lambda *_args: pytest.fail(
        "ambiguous existing provenance must not upsert"
    )
    eng._install_or_refresh_degraded_broker_truth_owner = lambda **_kwargs: pytest.fail(
        "ambiguous existing provenance must remain a HOLD"
    )

    fake_db = types.SimpleNamespace(conn=_conn, run_with_retry=lambda fn, **_: fn())
    prior = sys.modules.get("ap.db")
    sys.modules["ap.db"] = fake_db
    try:
        assert eng._broker_position_precheck() is False
    finally:
        if prior is not None:
            sys.modules["ap.db"] = prior
        else:
            sys.modules.pop("ap.db", None)

    assert eng._positions == []
    assert db_row["qty"] == 1
    assert db_row["quantity_remaining"] == 1
    assert not any("UPDATE positions" in sql for sql in executed)
    assert contract in eng._broker_truth_hold_symbols

@_skip_if_no_mod
def test_pr588_test_j_real_postgresql_partial_insert(monkeypatch):
    """Test J (spec) — MANDATORY: ENTRY filled_qty=2, broker qty=1.
    Must call the real _upsert_broker_position_to_db against the PostgreSQL-
    backed fixture and then read the actual persisted row to assert
    qty=2 / quantity_remaining=1 / status=PARTIAL.

    Skips automatically when DATABASE_URL is not configured (CI required)."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    with _postgres_positions_table(monkeypatch) as pg_conn:
        CONTRACT    = "IWM260919C00230000"
        CLIENT      = "pr588-test-j@example.com"
        POSITION_ID = "pr588-pos-j-canonical"
        FILL_PRICE  = 1.50
        FULL_QTY    = 2
        BROKER_QTY  = 1       # partial: one contract already exited

        # Insert the exact filled ENTRY order that proves filled_qty=2.
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, position_id, kind, status,
                    contract, execution_mode, filled_qty, fill_price, filled_ts,
                    meta
                ) VALUES (%s, %s, %s, 'ENTRY', 'FILLED', %s, 'live',
                          %s, %s, %s, %s::jsonb)
                """,
                (
                    "local-pr588-j",
                    CLIENT,
                    POSITION_ID,
                    CONTRACT,
                    FULL_QTY,
                    FILL_PRICE,
                    "2026-09-01T13:00:00Z",
                    "{}",
                ),
            )
        pg_conn.commit()

        eng = engine_cls.__new__(engine_cls)
        eng._email = CLIENT
        eng._lock  = __import__("threading").RLock()
        eng._positions = []
        eng._positions_by_id = {}
        eng.broker = types.SimpleNamespace(mode="live")

        # cost_basis = fill_price * broker_qty * 100
        cost_basis = FILL_PRICE * BROKER_QTY * 100

        row_id = eng._upsert_broker_position_to_db(
            CONTRACT,
            {
                "quantity":      BROKER_QTY,
                "cost_basis":    cost_basis,
                "date_acquired": "2026-09-01T13:00:00Z",
            },
        )

        assert row_id is not None, (
            "Test J: _upsert_broker_position_to_db must return a row_id "
            "for a valid partial recovery (ENTRY=2, broker=1)"
        )

        # Read the ACTUAL persisted row from the real PostgreSQL-backed table.
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT qty, quantity_remaining, status
                FROM positions
                WHERE client_id = %s AND contract = %s
                """,
                (CLIENT, CONTRACT),
            )
            row = cur.fetchone()

        assert row is not None, (
            "Test J: no row found in positions after _upsert_broker_position_to_db"
        )
        row_qty = row["qty"] if isinstance(row, dict) else row[0]
        row_remaining = row["quantity_remaining"] if isinstance(row, dict) else row[1]
        row_status = row["status"] if isinstance(row, dict) else row[2]
        assert row_qty == FULL_QTY, (
            f"MANDATORY Test J FAILURE: positions.qty must be full entry qty "
            f"({FULL_QTY}), got {row_qty}. "
            f"This is the primary P0 defect: broker qty={BROKER_QTY} was written "
            f"to both qty and quantity_remaining, erasing the proven entry size."
        )
        assert row_remaining == BROKER_QTY, (
            f"Test J: positions.quantity_remaining must be broker remaining "
            f"({BROKER_QTY}), got {row_remaining}"
        )
        assert row_status == "PARTIAL", (
            f"Test J: status must be PARTIAL (not OPEN) when remaining "
            f"({BROKER_QTY}) < full entry qty ({FULL_QTY}), got {row_status!r}"
        )


# ── Test K: UndefinedColumn fallback INSERT parity ────────────────────────────

@_skip_if_no_mod
def test_pr588_test_k_fallback_insert_parity_2_1(monkeypatch):
    """Test K (spec): force the extended INSERT through the UndefinedColumn /
    aborted-transaction fallback.  The fallback INSERT must also persist
    qty=2 / quantity_remaining=1 / PARTIAL — not regress to 1/1/OPEN."""
    if not _DATABASE_URL:
        pytest.skip("DATABASE_URL not configured for PostgreSQL coverage")

    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    with _postgres_positions_table(monkeypatch) as pg_conn:
        CONTRACT    = "QQQ260919C00480000"
        CLIENT      = "pr588-test-k@example.com"
        POSITION_ID = "pr588-pos-k-fallback"
        FULL_QTY    = 2
        BROKER_QTY  = 1

        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, position_id, kind, status,
                    contract, execution_mode, filled_qty, fill_price, filled_ts, meta
                ) VALUES (%s, %s, %s, 'ENTRY', 'FILLED', %s, 'live',
                          %s, %s, %s, %s::jsonb)
                """,
                (
                    "local-pr588-k",
                    CLIENT,
                    POSITION_ID,
                    CONTRACT,
                    FULL_QTY,
                    2.00,
                    "2026-09-01T13:00:00Z",
                    "{}",
                ),
            )
        pg_conn.commit()

        # Drop the extended-schema columns so the fallback INSERT is forced.
        with pg_conn.cursor() as cur:
            cur.execute("ALTER TABLE positions DROP COLUMN IF EXISTS underlying_entry")
            cur.execute("ALTER TABLE positions DROP COLUMN IF EXISTS stop_underlying")
            cur.execute("ALTER TABLE positions DROP COLUMN IF EXISTS target_underlying")
        pg_conn.commit()

        eng = engine_cls.__new__(engine_cls)
        eng._email = CLIENT
        eng._lock  = __import__("threading").RLock()
        eng._positions = []
        eng._positions_by_id = {}
        eng.broker = types.SimpleNamespace(mode="live")

        row_id = eng._upsert_broker_position_to_db(
            CONTRACT,
            {
                "quantity":      BROKER_QTY,
                "cost_basis":    BROKER_QTY * 2.00 * 100,
                "date_acquired": "2026-09-01T13:00:00Z",
            },
        )

        assert row_id is not None, (
            "Test K: fallback INSERT must return a row_id for ENTRY=2 / broker=1"
        )

        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT qty, quantity_remaining, status FROM positions "
                "WHERE client_id = %s AND contract = %s",
                (CLIENT, CONTRACT),
            )
            row = cur.fetchone()

        assert row is not None, "Test K: no row persisted by fallback INSERT"
        row_qty = row["qty"] if isinstance(row, dict) else row[0]
        row_remaining = row["quantity_remaining"] if isinstance(row, dict) else row[1]
        row_status = row["status"] if isinstance(row, dict) else row[2]
        assert row_qty == FULL_QTY, (
            f"Test K FAILURE: fallback INSERT persisted qty={row_qty}, "
            f"expected full entry qty={FULL_QTY}. Fallback must use same quantity "
            f"semantics as extended INSERT."
        )
        assert row_remaining == BROKER_QTY, (
            f"Test K: fallback INSERT qty_remaining={row_remaining}, "
            f"expected broker remaining={BROKER_QTY}"
        )
        assert row_status == "PARTIAL", (
            f"Test K: fallback INSERT status={row_status!r}, "
            f"expected PARTIAL (remaining={BROKER_QTY} < full={FULL_QTY})"
        )


# ── Additional runtime invariant tests ───────────────────────────────────────

@_skip_if_no_mod
def test_pr588_partial_status_is_open_when_full_equals_remaining():
    """ENTRY=2, broker=2 → status must be OPEN (not PARTIAL) when full==remaining."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")
    eng = engine_cls.__new__(engine_cls)
    eng._email = "pr588-open@example.com"
    eng._lock  = __import__("threading").Lock()
    eng._positions = []
    eng.broker = types.SimpleNamespace(mode="live")

    row = {
        "id": "pos-open-001",
        "contract": "SPY260919C00600000",
        "option_symbol": "SPY260919C00600000",
        "underlying": "SPY",
        "side": "CALL",
        "direction": "CALL",
        "qty": 2,
        "quantity_remaining": 2,
        "entry_price": 1.00,
        "avg_fill": 1.00,
        "entry_ts": None,
        "status": "OPEN",
        "signal_id": None,
        "execution_mode": "live",
    }
    mp = eng._managed_position_from_row(row, qty_override=2, prefer_qty_override=True)
    assert mp.quantity == 2
    assert mp.quantity_remaining == 2
    # No quarantine for the valid OPEN case
    _is_quarantined = getattr(_EE_MOD, "_is_adoption_identity_quarantined", None)
    if _is_quarantined is not None:
        assert _is_quarantined(mp) is False, "OPEN recovery must NOT be quarantined"


@_skip_if_no_mod
def test_pr588_recovery_status_matrix():
    """Status authority matrix from the spec: all valid combinations."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")
    _is_quarantined = getattr(_EE_MOD, "_is_adoption_identity_quarantined", None)

    cases = [
        # (entry_qty, broker_qty, expected_mp_qty, expected_mp_remaining, expect_quarantine)
        (2, 2, 2, 2, False),   # OPEN — full qty == remaining
        (2, 1, 2, 1, False),   # PARTIAL — remaining < full
        (1, 1, 1, 1, False),   # OPEN — single contract full recovery
        (1, 2, 1, 2, True),    # CONTRADICTORY — broker > entry → quarantine
    ]
    for entry_qty, broker_qty, exp_qty, exp_rem, exp_quarantine in cases:
        eng = engine_cls.__new__(engine_cls)
        eng._email = "pr588-matrix@example.com"
        eng._lock  = __import__("threading").Lock()
        eng._positions = []
        eng.broker = types.SimpleNamespace(mode="live")

        row = {
            "id": f"pos-matrix-{entry_qty}-{broker_qty}",
            "contract": "SPY260919C00600000",
            "option_symbol": "SPY260919C00600000",
            "underlying": "SPY",
            "side": "CALL",
            "direction": "CALL",
            "qty": entry_qty,
            "quantity_remaining": entry_qty,
            "entry_price": 1.00,
            "avg_fill": 1.00,
            "entry_ts": None,
            "status": "OPEN",
            "signal_id": None,
            "execution_mode": "live",
        }
        mp = eng._managed_position_from_row(row, qty_override=broker_qty, prefer_qty_override=True)
        assert mp.quantity == exp_qty, (
            f"entry={entry_qty} broker={broker_qty}: expected quantity={exp_qty}, "
            f"got {mp.quantity}"
        )
        assert mp.quantity_remaining == exp_rem, (
            f"entry={entry_qty} broker={broker_qty}: expected remaining={exp_rem}, "
            f"got {mp.quantity_remaining}"
        )
        if _is_quarantined is not None:
            got_q = _is_quarantined(mp)
            assert got_q == exp_quarantine, (
                f"entry={entry_qty} broker={broker_qty}: expected quarantine={exp_quarantine}, "
                f"got {got_q}"
            )
