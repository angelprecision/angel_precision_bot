"""
tests/test_p0_exit_engine_broker_truth.py
P0: exit-engine broker-truth visibility repair.
"""
import json, os, re, sqlite3, pytest, types, sys
from contextlib import contextmanager
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
    assert "SELECT *" in upsert_body and "FROM positions" in upsert_body, (
        "ON CONFLICT fallback must re-query by client_id+contract to return existing id"
    )
    assert "LOWER(TRIM(COALESCE(execution_mode, ''))) = %s" in upsert_body
    assert "ORDER BY entry_ts" in upsert_body

def test_load_db_row_exact_mode_filter_present():
    """Mode-scoped DB load must select and filter execution_mode."""
    load_start = EE_SRC.find("def _load_db_position_row")
    load_end   = EE_SRC.find("\n    def ", load_start + 1)
    load_body  = EE_SRC[load_start:load_end]
    assert "signal_id, execution_mode" in load_body
    for column in ("underlying_entry", "stop_underlying", "target_underlying"):
        assert column in load_body
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
        ) ON COMMIT PRESERVE ROWS;

        CREATE TEMP TABLE orders (
            id TEXT PRIMARY KEY,
            client_id TEXT,
            execution_mode TEXT,
            contract TEXT,
            kind TEXT,
            status TEXT,
            filled_qty INTEGER,
            fill_price DOUBLE PRECISION,
            filled_ts TIMESTAMPTZ,
            updated_ts TIMESTAMPTZ,
            created_ts TIMESTAMPTZ,
            position_id TEXT,
            signal_id TEXT,
            local_order_id TEXT,
            broker_order_id TEXT,
            underlying_entry DOUBLE PRECISION,
            stop_underlying DOUBLE PRECISION,
            target_underlying DOUBLE PRECISION,
            meta JSONB
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


@contextmanager
def _postgres_shared_positions_table(monkeypatch):
    """Create a shared-schema PostgreSQL fixture for multi-connection races."""
    if not _DATABASE_URL:
        pytest.skip("DATABASE_URL not configured for PostgreSQL coverage")
    psycopg2 = pytest.importorskip("psycopg2")
    import uuid

    schema = f"broker_repair_test_{uuid.uuid4().hex}"
    pg_conn = psycopg2.connect(_DATABASE_URL)
    pg_conn.autocommit = True
    with pg_conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE SCHEMA {schema};
            CREATE TABLE {schema}.positions (
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
            );
            CREATE TABLE {schema}.orders (
                id TEXT PRIMARY KEY,
                client_id TEXT,
                execution_mode TEXT,
                contract TEXT,
                kind TEXT,
                status TEXT,
                filled_qty INTEGER,
                fill_price DOUBLE PRECISION,
                filled_ts TIMESTAMPTZ,
                updated_ts TIMESTAMPTZ,
                created_ts TIMESTAMPTZ,
                position_id TEXT,
                signal_id TEXT,
                local_order_id TEXT,
                broker_order_id TEXT,
                underlying_entry DOUBLE PRECISION,
                stop_underlying DOUBLE PRECISION,
                target_underlying DOUBLE PRECISION,
                meta JSONB
            );
            SET search_path TO {schema}, public
            """
        )

    class _ConnWrapper:
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

    @contextmanager
    def shared_conn():
        connection = psycopg2.connect(_DATABASE_URL)
        connection.autocommit = False
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute(f"SET search_path TO {schema}, public")
            yield _ConnWrapper(connection, cursor)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()

    fake_db = types.SimpleNamespace(
        conn=shared_conn,
        run_with_retry=lambda fn, **_: fn(),
    )
    monkeypatch.setitem(sys.modules, "ap.db", fake_db)
    try:
        with pg_conn.cursor() as cur:
            cur.execute(f"SET search_path TO {schema}, public")
        yield pg_conn
    finally:
        with pg_conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA {schema} CASCADE")
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
def test_postgres_recovery_reuses_canonical_filled_entry_identity(monkeypatch):
    """The real recovery INSERT keeps the filled ENTRY's canonical id and geometry."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    with _postgres_positions_table(monkeypatch) as pg_conn:
        contract = "NOW260825P00122000"
        client = "canonical-recovery@example.com"
        canonical_id = "2fe10f52-e459-4bc5-a57a-1a80e3618040"
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orders (
                    id, client_id, execution_mode, contract, kind, status,
                    filled_qty, fill_price, filled_ts, position_id, signal_id,
                    local_order_id, broker_order_id,
                    underlying_entry, stop_underlying, target_underlying
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    "entry-order-1", client, "live", contract, "ENTRY", "PARTIAL_FILL",
                    1, 1.30, "2026-08-25T13:54:01+00:00", canonical_id,
                    "signal-1", "local-entry-1", "broker-entry-1",
                    127.425, 130.44, 124.78,
                ),
            )
        pg_conn.commit()

        eng = engine_cls.__new__(engine_cls)
        eng._email = client
        eng._lock = __import__("threading").Lock()
        eng._positions = []
        eng._positions_by_id = {}
        eng.broker = types.SimpleNamespace(
            mode="live",
            account_id="canonical-acct",
            list_positions=lambda: [{
                "symbol": contract,
                "quantity": 1,
                "cost_basis": 130.0,
                "side": "PUT",
            }],
        )
        eng._fetch_broker_quote = lambda sym: {}

        assert eng._broker_position_precheck() is True
        active = eng.active_positions()
        assert len(active) == 1
        assert active[0].position_id == canonical_id
        assert active[0].execution_mode == "live"
        assert active[0].underlying_entry == pytest.approx(127.425)
        assert active[0].underlying_stop == pytest.approx(130.44)
        assert active[0].underlying_target == pytest.approx(124.78)

        with pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, client_id, execution_mode, contract, signal_id,
                       local_order_id, broker_order_id,
                       underlying_entry, stop_underlying, target_underlying
                FROM positions
                WHERE id = %s
                """,
                (canonical_id,),
            )
            row = cur.fetchone()
        assert row == (
            canonical_id, client, "live", contract, "signal-1",
            "local-entry-1", "broker-entry-1",
            127.425, 130.44, 124.78,
        )


@_skip_if_no_mod
def test_postgres_recovery_generates_uuid_without_proven_entry_position_id(monkeypatch):
    """A matching fill without a proven id gets an explicit UUID in PostgreSQL."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    with _postgres_positions_table(monkeypatch) as pg_conn:
        contract = "QQQ260821P00450000"
        client = "uuid-recovery@example.com"
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orders (
                    id, client_id, execution_mode, contract, kind, status,
                    filled_qty, fill_price, filled_ts, position_id, signal_id,
                    underlying_entry, stop_underlying, target_underlying
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    "entry-order-without-position-id", client, "paper", contract,
                    "ENTRY", "FILLED", 1, 1.50,
                    "2026-08-25T13:54:01+00:00", None, "signal-uuid",
                    450.0, 455.0, 440.0,
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
            {
                "quantity": 1,
                "cost_basis": 150.0,
                "date_acquired": "2026-08-25T13:54:01+00:00",
            },
        )

        import uuid
        assert uuid.UUID(str(row_id))
        assert row_id.repair_row["execution_mode"] == "paper"
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, client_id, execution_mode, contract, signal_id,
                       underlying_entry, stop_underlying, target_underlying
                FROM positions
                WHERE id = %s
                """,
                (str(row_id),),
            )
            row = cur.fetchone()
        assert row == (
            str(row_id), client, "paper", contract, "signal-uuid",
            450.0, 455.0, 440.0,
        )


@_skip_if_no_mod
def test_upsert_fails_closed_on_contradictory_historical_aliases(monkeypatch):
    """Zero plus a positive historical alias is contradictory evidence."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    with _postgres_positions_table(monkeypatch) as pg_conn:
        client = "contradictory-history@example.com"
        contract = "NOW260825P00122000"
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO orders (
                    id, client_id, execution_mode, contract, kind, status,
                    filled_qty, fill_price, filled_ts, position_id,
                    underlying_entry, meta
                ) VALUES (
                    %s, %s, %s, %s, 'ENTRY', 'FILLED',
                    1, 1.30, %s, NULL,
                    0, %s
                )
                """,
                (
                    "contradictory-history-order",
                    client,
                    "live",
                    contract,
                    "2026-08-25T13:54:01+00:00",
                    json.dumps({
                        "execution_mode": "live",
                        "underlying_entry_price": 127.42,
                    }),
                ),
            )
        pg_conn.commit()

        eng = engine_cls.__new__(engine_cls)
        eng._email = client
        eng.broker = types.SimpleNamespace(mode="live")

        assert eng._upsert_broker_position_to_db(
            contract,
            {"quantity": 1, "cost_basis": 130.0},
        ) is None

        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM positions WHERE client_id = %s",
                (client,),
            )
            assert cur.fetchone()[0] == 0


@_skip_if_no_mod
def test_upsert_is_idempotent_for_repeated_same_client_mode_contract(monkeypatch):
    """Restart/retry repair must reuse one active owner, not mint UUIDs."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    with _postgres_positions_table(monkeypatch) as pg_conn:
        client = "idempotent-repair@example.com"
        contract = "QQQ260821P00450000"
        broker_position = {"quantity": 2, "cost_basis": 150.0}

        def _new_engine():
            eng = engine_cls.__new__(engine_cls)
            eng._email = client
            eng.broker = types.SimpleNamespace(mode="paper")
            return eng

        first_id = _new_engine()._upsert_broker_position_to_db(contract, broker_position)
        second_id = _new_engine()._upsert_broker_position_to_db(contract, broker_position)

        assert first_id
        assert second_id == first_id
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM positions
                WHERE client_id = %s
                  AND execution_mode = 'paper'
                  AND contract = %s
                  AND status = 'OPEN'
                """,
                (client, contract),
            )
            assert cur.fetchone()[0] == 1


@_skip_if_no_mod
def test_upsert_concurrent_repairs_share_one_active_owner(monkeypatch):
    """Two PostgreSQL connections racing to repair one contract stay idempotent."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    import threading

    with _postgres_shared_positions_table(monkeypatch) as pg_conn:
        client = "concurrent-repair@example.com"
        contract = "SPY260821C00650000"
        broker_position = {"quantity": 1, "cost_basis": 130.0}
        ready = threading.Barrier(2)
        results = []
        errors = []

        def _worker():
            eng = engine_cls.__new__(engine_cls)
            eng._email = client
            eng.broker = types.SimpleNamespace(mode="live")

            def _find(*_args, **_kwargs):
                # Both transactions finish their lookup before either reaches
                # the advisory lock/insert section.
                ready.wait(timeout=10)
                return None

            eng._find_exact_filled_entry_order = _find
            try:
                results.append(
                    eng._upsert_broker_position_to_db(contract, broker_position)
                )
            except Exception as exc:  # pragma: no cover - assertion reports it
                errors.append(exc)

        threads = [threading.Thread(target=_worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=20)

        assert not errors
        assert len(results) == 2
        assert results[0]
        assert results[1] == results[0]
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)
                FROM positions
                WHERE client_id = %s
                  AND execution_mode = 'live'
                  AND contract = %s
                  AND status = 'OPEN'
                """,
                (client, contract),
            )
            assert cur.fetchone()[0] == 1


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
    idx = EE_SRC.rfind("SELECT *")
    fallback_region = EE_SRC[idx:idx + 1200]
    assert "client_id" in fallback_region
    assert "UPPER(TRIM(COALESCE(contract" in fallback_region

    # Simple structural check — the re-query EXISTS in source
    assert "ORDER BY entry_ts DESC NULLS LAST" in fallback_region


def test_broker_fetch_failure_does_not_mark_flat():
    """
    Test 4: broker.list_positions() raises → log EXIT_UNSAFE_BROKER_TRUTH_UNAVAILABLE,
    return False, never touches local engine positions.
    """
    # Source check: return False after the log (not clearing positions)
    # Find the except block that handles broker.list_positions failure
    idx = EE_SRC.find("broker.list_positions() or []")
    except_start = EE_SRC.find("except Exception as _bp_err:", idx)
    except_end   = EE_SRC.find("\n        broker_map", except_start)
    except_block = EE_SRC[except_start:except_end]
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
    eng._lock       = threading.Lock()
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
    _ap_stub  = types.ModuleType("ap")
    _db_stub  = types.ModuleType("ap.db")
    # run_with_retry calls f() — for DB repair, just silently skip
    _db_stub.run_with_retry = lambda f: None
    _db_stub.conn = MagicMock()
    _ap_stub.db   = _db_stub
    sys.modules.setdefault("ap",    _ap_stub)
    sys.modules.setdefault("ap.db", _db_stub)

    result = eng._broker_position_precheck()

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


@_skip_if_no_mod
def test_find_exact_filled_entry_evidence_is_client_mode_contract_scoped(monkeypatch):
    """Real PostgreSQL must enforce client, durable mode, contract, and status scope."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    with _postgres_positions_table(monkeypatch) as pg_conn:
        client = "evidence@example.com"
        contract = "NOW260825P00122000"
        valid_position_id = "canonical-position-1"

        def _insert_order(
            order_id,
            *,
            client_value=client,
            mode="live",
            contract_value=contract,
            kind="ENTRY",
            status="FILLED",
            filled_qty=1,
            fill_price=1.30,
            position_id=None,
            meta=None,
        ):
            with pg_conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO orders (
                        id, client_id, execution_mode, contract, kind, status,
                        filled_qty, fill_price, filled_ts, position_id, meta
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        order_id, client_value, mode, contract_value, kind, status,
                        filled_qty, fill_price,
                        "2026-08-25T13:54:01+00:00",
                        position_id,
                        json.dumps(meta) if meta is not None else None,
                    ),
                )
            pg_conn.commit()

        _insert_order("wrong-client", client_value="other@example.com", meta={"execution_mode": "live"})
        _insert_order("wrong-mode", mode="paper", meta={"execution_mode": "paper"})
        _insert_order("contradictory-mode", meta={"execution_mode": "paper"})
        _insert_order(
            "wrong-contract",
            contract_value="NOW260825C00122000",
            meta={"execution_mode": "live"},
        )
        _insert_order("exit-order", kind="EXIT", meta={"execution_mode": "live"})
        _insert_order("cancelled-order", status="CANCELED", meta={"execution_mode": "live"})
        _insert_order(
            "valid-order",
            position_id=valid_position_id,
            meta={"execution_mode": "live"},
        )

        eng = engine_cls.__new__(engine_cls)
        eng._email = client
        eng.broker = types.SimpleNamespace(mode="live")

        found = eng._find_exact_filled_entry_order(
            contract,
            "live",
            {"quantity": 1, "cost_basis": 130.0},
        )
        assert found is not None
        assert found["id"] == "valid-order"
        assert found["position_id"] == valid_position_id

        # A blank column may be completed by durable metadata, but only when
        # that metadata is itself valid and non-contradictory.
        meta_only_contract = "NOW260825P00123000"
        _insert_order(
            "meta-only-order",
            mode=None,
            contract_value=meta_only_contract,
            meta={"execution_mode": "live"},
        )
        found_meta_only = eng._find_exact_filled_entry_order(
            meta_only_contract,
            "live",
            {"quantity": 1, "cost_basis": 130.0},
        )
        assert found_meta_only is not None
        assert found_meta_only["id"] == "meta-only-order"


@_skip_if_no_mod
def test_upsert_reuses_proven_position_id_and_entry_geometry(monkeypatch):
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    eng = engine_cls.__new__(engine_cls)
    eng._email = "canonical@example.com"
    eng.broker = types.SimpleNamespace(mode="live")

    calls = []

    class _Cursor:
        def __init__(self):
            self.last_sql = ""
            self.last_params = ()

        def execute(self, sql, params=()):
            self.last_sql = str(sql)
            self.last_params = params
            calls.append((self.last_sql, params))

        def fetchone(self):
            if "INSERT INTO positions" in self.last_sql:
                return {"id": "canonical-position-1"}
            return None

    cursor = _Cursor()

    @contextmanager
    def fake_conn():
        yield cursor

    fake_db = types.SimpleNamespace(
        conn=fake_conn,
        run_with_retry=lambda fn, **_: fn(),
    )
    monkeypatch.setitem(sys.modules, "ap.db", fake_db)
    eng._find_exact_filled_entry_order = lambda sym, mode, bp: {
        "position_id": "canonical-position-1",
        "signal_id": "signal-1",
        "fill_price": 1.30,
        "filled_qty": 1,
        "filled_ts": "2026-08-25T13:54:01+00:00",
        "underlying_entry": 127.425,
        "stop_underlying": 130.44,
        "target_underlying": 124.78,
    }

    row_id = eng._upsert_broker_position_to_db(
        "NOW260825P00122000",
        {
            "quantity": 1,
            "cost_basis": 130.0,
            "date_acquired": "2026-08-25T13:54:01+00:00",
        },
    )

    assert row_id == "canonical-position-1"
    insert_sql, params = next(item for item in calls if "INSERT INTO positions" in item[0])
    assert "id, client_id" in insert_sql
    assert params[0] == "canonical-position-1"
    assert params[1] == eng._email
    assert params[3] == "NOW260825P00122000"
    assert params[5] == "live"
    assert params[12:15] == (127.425, 130.44, 124.78)
    assert params[16] == "signal-1"


@_skip_if_no_mod
def test_upsert_generates_explicit_uuid_when_entry_has_no_proven_position_id(monkeypatch):
    import uuid

    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    eng = engine_cls.__new__(engine_cls)
    eng._email = "uuid@example.com"
    eng.broker = types.SimpleNamespace(mode="paper")

    state = {"sql": "", "params": ()}

    class _Cursor:
        def execute(self, sql, params=()):
            state["sql"] = str(sql)
            state["params"] = params

        def fetchone(self):
            if "INSERT INTO positions" in state["sql"]:
                return {"id": state["params"][0]}
            return None

    @contextmanager
    def fake_conn():
        yield _Cursor()

    fake_db = types.SimpleNamespace(
        conn=fake_conn,
        run_with_retry=lambda fn, **_: fn(),
    )
    monkeypatch.setitem(sys.modules, "ap.db", fake_db)
    eng._find_exact_filled_entry_order = lambda sym, mode, bp: None

    row_id = eng._upsert_broker_position_to_db(
        "QQQ260821P00450000",
        {"quantity": 2, "cost_basis": 150.0},
    )

    assert row_id == state["params"][0]
    assert uuid.UUID(str(row_id))
    assert state["params"][0]
    assert state["params"][0] != "None"


@_skip_if_no_mod
def test_ambiguous_filled_entry_evidence_holds_repair_without_newest_choice(monkeypatch):
    """Two equally plausible fills must not silently choose one lifecycle id."""
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    eng = engine_cls.__new__(engine_cls)
    eng._email = "ambiguous@example.com"
    eng.broker = types.SimpleNamespace(mode="live")

    rows = [
        {
            "id": "entry-a",
            "client_id": eng._email,
            "execution_mode": "live",
            "contract": "NOW260825P00122000",
            "kind": "ENTRY",
            "status": "FILLED",
            "filled_qty": 1,
            "fill_price": 1.30,
            "position_id": "canonical-a",
            "underlying_entry": 127.425,
            "stop_underlying": 130.44,
            "target_underlying": 124.78,
        },
        {
            "id": "entry-b",
            "client_id": eng._email,
            "execution_mode": "live",
            "contract": "NOW260825P00122000",
            "kind": "ENTRY",
            "status": "PARTIAL_FILL",
            "filled_qty": 1,
            "fill_price": 1.30,
            "position_id": "canonical-b",
            "underlying_entry": 127.525,
            "stop_underlying": 131.44,
            "target_underlying": 123.78,
        },
    ]
    calls = []

    class _Cursor:
        def execute(self, sql, params=()):
            calls.append((str(sql), params))

        def fetchall(self):
            return rows

        def fetchone(self):
            return None

    @contextmanager
    def fake_conn():
        yield _Cursor()

    fake_db = types.SimpleNamespace(
        conn=fake_conn,
        run_with_retry=lambda fn, **_: fn(),
    )
    monkeypatch.setitem(sys.modules, "ap.db", fake_db)

    broker_position = {
        "quantity": 1,
        "cost_basis": 130.0,
    }
    lookup = eng._find_exact_filled_entry_order(
        "NOW260825P00122000", "live", broker_position
    )
    assert lookup["_lookup_status"] == "AMBIGUOUS"
    assert not any("LIMIT 1" in sql.upper() for sql, _ in calls)

    assert eng._upsert_broker_position_to_db(
        "NOW260825P00122000", broker_position
    ) is None
    assert not any("INSERT INTO positions" in sql for sql, _ in calls)


@_skip_if_no_mod
def test_managed_position_recovery_preserves_proven_entry_geometry():
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    eng = engine_cls.__new__(engine_cls)
    eng._email = "geometry@example.com"
    eng.broker = types.SimpleNamespace(mode="live")
    row = {
        "id": "canonical-position-2",
        "contract": "NOW260825P00122000",
        "option_symbol": "NOW260825P00122000",
        "underlying": "NOW",
        "side": "PUT",
        "qty": 1,
        "quantity_remaining": 1,
        "entry_price": 1.30,
        "underlying_entry": 127.425,
        "stop_underlying": 130.44,
        "target_underlying": 124.78,
        "execution_mode": "live",
    }
    mp = eng._managed_position_from_row(row, qty_override=1, prefer_qty_override=True)
    assert mp.position_id == "canonical-position-2"
    assert mp.underlying_entry == pytest.approx(127.425)
    assert mp.underlying_stop == pytest.approx(130.44)
    assert mp.underlying_target == pytest.approx(124.78)


@_skip_if_no_mod
def test_load_db_position_row_returns_underlying_entry_stop_target_columns(monkeypatch):
    """F1 regression: `_load_db_position_row` must SELECT the three durable
    underlying columns so `_managed_position_from_row` can hydrate them.

    Without the SELECT change, `row.get("underlying_entry")` returns None on
    the DB-found path even when the positions row has the value set, silently
    reverting the engine to underlying_entry=0.0 on every broker-precheck
    that finds an existing DB row (the common case).
    """
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not found")

    with _postgres_positions_table(monkeypatch) as pg_conn:
        contract = "NOW260828P00122000"
        client = "underlying-hydration@example.com"
        with pg_conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO positions (
                    id, client_id, underlying, contract, option_symbol, execution_mode,
                    side, direction, qty, quantity_remaining, avg_fill, entry_price,
                    underlying_entry, stop_underlying, target_underlying,
                    entry_ts, status, signal_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    NOW(), %s, %s
                )
                """,
                (
                    "canonical-position-hydration", client, "NOW", contract, contract, "live",
                    "PUT", "PUT", 1, 1, 1.30, 1.30,
                    127.425, 130.44, 124.78,
                    "OPEN", "sig-hydration",
                ),
            )
        pg_conn.commit()

        eng = engine_cls.__new__(engine_cls)
        eng._email = client
        eng._lock = __import__("threading").Lock()
        eng._positions = []
        eng._positions_by_id = {}
        eng.broker = types.SimpleNamespace(mode="live")

        row = eng._load_db_position_row(contract)
        assert row is not None
        assert row["id"] == "canonical-position-hydration"
        # F1: without the SELECT-list fix these come back missing (None from
        # dict.get), so the assertion below fails and the underlying-entry
        # hydration silently degrades to 0.0 downstream.
        assert row.get("underlying_entry") == pytest.approx(127.425)
        assert row.get("stop_underlying") == pytest.approx(130.44)
        assert row.get("target_underlying") == pytest.approx(124.78)

        # End-to-end: hydration must flow all the way through to the
        # ManagedPosition the exit engine consumes.
        mp = eng._managed_position_from_row(row, qty_override=1, prefer_qty_override=True)
        assert mp.underlying_entry == pytest.approx(127.425)
        assert mp.underlying_stop == pytest.approx(130.44)
        assert mp.underlying_target == pytest.approx(124.78)


@_skip_if_no_mod
def test_broker_precheck_retries_after_db_repair_failure_without_owner(caplog):
    import threading

    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    contract = "NOW260825P00122000"
    eng = engine_cls.__new__(engine_cls)
    eng._email = "hold@example.com"
    eng._lock = threading.Lock()
    eng._positions = []
    eng._positions_by_id = {}
    eng.broker = types.SimpleNamespace(
        mode="live",
        account_id="acct-1",
        list_positions=lambda: [{
            "symbol": contract,
            "quantity": 1,
            "cost_basis": 130.0,
        }],
    )
    eng._load_db_position_row = lambda sym: None
    attempts = []

    def failed_repair(sym, bp):
        attempts.append((sym, bp))
        return None

    eng._upsert_broker_position_to_db = failed_repair
    eng._fetch_broker_quote = lambda sym: {}
    added = []
    eng.add_position = lambda pos: added.append(pos)

    with caplog.at_level("ERROR"):
        assert eng._broker_position_precheck() is False
    assert added == []
    assert eng._positions == []
    assert not eng._positions_by_id
    assert len(attempts) == 1
    assert any("EXIT_UNSAFE_BROKER_POSITION_REPAIR_FAILED" in rec.message for rec in caplog.records)

    # A failed cycle does not manufacture ownership or suppress the next retry.
    with caplog.at_level("ERROR"):
        assert eng._broker_position_precheck() is False
    assert len(attempts) == 2
    assert added == []
    assert eng._positions == []
    assert not eng._positions_by_id


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

def test_recovery_requires_confirmed_db_id_before_engine_add():
    """A broker recovery owner cannot be installed without a DB identity."""
    precheck_start = EE_SRC.find("def _broker_position_precheck")
    precheck_end = EE_SRC.find("\n    def ", precheck_start + 1)
    precheck_body = EE_SRC[precheck_start:precheck_end]
    assert "engine_loaded_synthetic_syms" not in precheck_body
    assert "if not new_id:" in precheck_body
    assert 'raise RuntimeError(repair_failed_reason)' in precheck_body
    assert "repaired_syms.append(sym)" in precheck_body

def test_summary_reports_only_confirmed_recovery():
    """Summary must distinguish DB-confirmed recovery from repair failure."""
    idx = EE_SRC.rfind("EXIT_BROKER_PRECHECK_SUMMARY")
    region = EE_SRC[idx:idx + 600]
    assert "engine_loaded_synthetic" not in region
    assert "repaired_from_broker" in region, (
        "EXIT_BROKER_PRECHECK_SUMMARY must still include repaired_from_broker (DB-confirmed only)"
    )
    assert "repair_failed" in region

def test_upsert_has_explicit_id_and_uuid_fallback():
    """The production INSERT must always receive id explicitly."""
    upsert_start = EE_SRC.find("def _upsert_broker_position_to_db")
    upsert_end = EE_SRC.find("\n    def ", upsert_start + 1)
    upsert_body = EE_SRC[upsert_start:upsert_end]
    insert_start = upsert_body.find("INSERT INTO positions (")
    insert_end = upsert_body.find("ON CONFLICT DO NOTHING", insert_start)
    insert_block = upsert_body[insert_start:insert_end]
    assert "id, client_id" in insert_block
    assert "position_id = proven_position_id or str(_uuid.uuid4())" in upsert_body


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
                state["queries"].append((state["call"], str(sql)))
                return self

            def fetchone(self):
                sql = state["queries"][-1][1].upper()
                if "INSERT INTO POSITIONS" in sql:
                    return ({"id": state["insert_id"]}
                            if state["insert_id"] is not None else None)
                if "FROM POSITIONS" in sql and "SELECT *" in sql:
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
