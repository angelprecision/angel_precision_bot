from __future__ import annotations

import importlib
import os
import re
import sys
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

REPO_ROOT = Path(__file__).resolve().parents[1]


class _ArchiveCursor:
    def __init__(self):
        self.keys: set[tuple[str, str, str]] = set()
        self._next_row = None
        self.rowcount = 0
        self.inserts = []

    def execute(self, sql, params=None):
        if "INSERT INTO option_chain_snapshots" in sql:
            snapshot_date, ticker, expiration = params[0], params[1], params[2]
            key = (str(snapshot_date), str(ticker), str(expiration))
            self.inserts.append((sql, params))
            if key in self.keys:
                self._next_row = None
                self.rowcount = 0
            else:
                self.keys.add(key)
                self._next_row = {"id": len(self.keys)}
                self.rowcount = 1
        elif "SELECT to_regclass" in sql:
            self._next_row = {"table_name": None}
        else:
            self._next_row = None
        return self

    def fetchone(self):
        return self._next_row

    def fetchall(self):
        return []


@contextmanager
def _fake_conn(cursor):
    yield cursor


def _conn_factory(cursor):
    @contextmanager
    def _ctx():
        yield cursor
    return _ctx


def _chain():
    return [
        {"strike": 99, "option_type": "call", "bid": 2.1, "ask": 2.3, "greeks": {"mid_iv": 0.30, "delta": 0.58}, "open_interest": 100, "volume": 12},
        {"strike": 99, "option_type": "put", "bid": 1.9, "ask": 2.1, "greeks": {"mid_iv": 0.32, "delta": -0.42}, "open_interest": 88, "volume": 7},
        {"strike": 105, "option_type": "call", "bid": 0.8, "ask": 1.0, "greeks": {"mid_iv": 0.40, "delta": 0.25}, "open_interest": 30, "volume": 4},
    ]


class _Broker:
    def __init__(self, *, fail_tickers=None):
        self.fail_tickers = set(fail_tickers or [])

    def get_quote(self, ticker):
        return {"last": 100.0}

    def get_option_expirations(self, ticker):
        if ticker in self.fail_tickers:
            raise RuntimeError("expirations unavailable")
        return ["2026-07-10", "2026-07-17", "2026-07-24"]

    def get_option_chain(self, ticker, expiration):
        return _chain()

    def place_order(self, *args, **kwargs):
        raise AssertionError("archiver must not submit broker orders")

    def cancel_order(self, *args, **kwargs):
        raise AssertionError("archiver must not cancel broker orders")


def test_chain_compaction_fixture():
    from ap.chain_archiver import compact_chain

    compacted = compact_chain(_chain(), underlying_price=100.0)

    assert compacted[0] == {
        "strike": 99.0,
        "type": "call",
        "bid": 2.1,
        "ask": 2.3,
        "mid": 2.2,
        "iv": 0.3,
        "delta": 0.58,
        "oi": 100.0,
        "vol": 12.0,
    }
    assert len(str(compacted).encode("utf-8")) < 150_000


def test_atm_iv_calculation():
    from ap.chain_archiver import atm_iv, compact_chain

    compacted = compact_chain(_chain(), underlying_price=100.0)

    assert atm_iv(compacted, 100.0) == 0.31


def test_expected_move_calculation():
    from ap.chain_archiver import expected_move

    assert expected_move(100.0, 0.31, 1) == 1.6226
    assert expected_move(100.0, 0.31, 9) == 4.8678


def test_idempotency_same_ticker_date_expiration_creates_no_duplicate():
    from ap.chain_archiver import archive_daily_chains

    cursor = _ArchiveCursor()
    conn_factory = _conn_factory(cursor)
    broker = _Broker()

    first = archive_daily_chains(
        broker=broker,
        conn_factory=conn_factory,
        tickers=["AAPL"],
        snapshot_date=date(2026, 7, 4),
        enabled=True,
    )
    second = archive_daily_chains(
        broker=broker,
        conn_factory=conn_factory,
        tickers=["AAPL"],
        snapshot_date=date(2026, 7, 4),
        enabled=True,
    )

    assert first.rows_inserted == 3
    assert second.rows_inserted == 0
    assert second.rows_existing == 3
    assert len(cursor.keys) == 3
    assert all("%s" in sql for sql, _ in cursor.inserts)
    assert all("?" not in sql for sql, _ in cursor.inserts)


def test_failure_isolation_one_ticker_does_not_abort_batch():
    from ap.chain_archiver import archive_daily_chains

    result = archive_daily_chains(
        broker=_Broker(fail_tickers={"BAD"}),
        conn_factory=_conn_factory(_ArchiveCursor()),
        tickers=["BAD", "AAPL"],
        snapshot_date=date(2026, 7, 4),
        enabled=True,
    )

    assert result.ok is True
    assert result.rows_inserted == 3
    assert result.errors and result.errors[0]["ticker"] == "BAD"


def test_signed_endpoint_rejects_unsigned_request(monkeypatch):
    # Importing app.py constructs the Flask app, so stub external DB/crypto deps
    # the same way existing endpoint tests do.
    old_app = sys.modules.pop("app", None)
    old_env = {k: os.environ.get(k) for k in ("APP_ENV", "SIGNING_SECRET", "DATABASE_URL", "ALLOW_LEGACY_FILL_MONITOR")}
    os.environ["APP_ENV"] = "prod"
    os.environ["SIGNING_SECRET"] = "test-secret"
    os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")
    os.environ["ALLOW_LEGACY_FILL_MONITOR"] = "1"

    psycopg2_mod = MagicMock()
    psycopg2_mod.errors = SimpleNamespace()
    psycopg2_extras_mod = MagicMock()
    psycopg2_pool_mod = MagicMock()
    supabase_mod = MagicMock()
    cryptography_mod = MagicMock()
    fernet_mod = MagicMock()
    fernet_mod.Fernet = MagicMock()
    supabase_mod.create_client = MagicMock()
    supabase_mod.Client = MagicMock()

    class _AppCursor:
        def execute(self, *_args, **_kwargs):
            return self

        def fetchone(self):
            return {"exists": 1}

        def fetchall(self):
            return []

    @contextmanager
    def fake_conn():
        yield _AppCursor()

    import ap.db as db_mod
    import ap.state as state_mod

    monkeypatch.setattr(db_mod, "init_db", lambda: None)
    monkeypatch.setattr(db_mod, "conn", fake_conn)
    monkeypatch.setattr(db_mod, "get_client", lambda client_id: {"status": "ACTIVE"})
    monkeypatch.setattr(db_mod, "create_client", lambda **kwargs: None)
    monkeypatch.setattr(state_mod, "update_state", lambda *args, **kwargs: None)

    with patch.dict(sys.modules, {
        "psycopg2": psycopg2_mod,
        "psycopg2.extras": psycopg2_extras_mod,
        "psycopg2.pool": psycopg2_pool_mod,
        "supabase": supabase_mod,
        "cryptography": cryptography_mod,
        "cryptography.fernet": fernet_mod,
    }):
        try:
            app_mod = importlib.import_module("app")
            app_mod.app.testing = True
            response = app_mod.app.test_client().post("/cron/chain-archive", json={})
            assert response.status_code == 401
            assert response.get_json()["error"] == "unauthorized"
        finally:
            sys.modules.pop("app", None)
            if old_app is not None:
                sys.modules["app"] = old_app
            for key, value in old_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


def test_endpoint_source_is_hmac_guarded():
    src = (REPO_ROOT / "app.py").read_text()
    match = re.search(r'@app\.post\("/cron/chain-archive"\)\s+@require_hmac\s+def cron_chain_archive', src)
    assert match, "chain archive cron endpoint must be protected by @require_hmac"
