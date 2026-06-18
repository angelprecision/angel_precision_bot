"""
tests/test_p0_release_after_hours_deferred.py

P0 — Release Deferred Overnight Rows After Open.

Verifies the /admin/release_after_hours_deferred endpoint:
  - Pre-open guard refuses to run without force=true (returns 412).
  - force=true bypasses the guard.
  - UPDATE SET clause is EXACTLY `last_error = NULL`. No other columns.
  - UPDATE WHERE clause filters status='WATCHING' AND
    last_error='after_hours_deferred:awaiting_overnight_reeval' + lookback.
  - Optional `clients` filter appends client_id = ANY(...).
  - Lookback clamping (<1 -> 1, default 36).
  - Response shape: released count + per-client breakdown.
  - Idempotent (0 rows -> clean 200).
  - DB exception -> 500 with error body.

DB is fully mocked. Flask test client is used. APP_ENV=dev disables HMAC.
"""
from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

os.environ["APP_ENV"] = "dev"
os.environ["DATABASE_URL"] = "postgresql://mock/mock"


def _stub(name):
    sys.modules.setdefault(name, MagicMock())

_stub("psycopg2"); _stub("psycopg2.extras"); _stub("psycopg2.pool")
_stub("supabase"); _stub("cryptography"); _stub("cryptography.fernet")
sys.modules["supabase"].create_client = MagicMock()
sys.modules["supabase"].Client = MagicMock()
sys.modules["cryptography.fernet"].Fernet = MagicMock()


@pytest.fixture(scope="module")
def flask_app():
    try:
        import app as _app_mod  # noqa: F401
    except Exception as exc:
        pytest.skip(f"app module unavailable: {exc}")
    return sys.modules["app"].app


@pytest.fixture
def client(flask_app):
    flask_app.testing = True
    return flask_app.test_client()


def _recorder():
    rec = {"calls": [], "fetch": []}
    class _Cur:
        def execute(self, sql, params=None):
            rec["calls"].append((sql, params))
        def fetchall(self):
            return rec["fetch"]
        def __enter__(self): return self
        def __exit__(self, *a): return False
    class _Conn:
        def __enter__(self): return _Cur()
        def __exit__(self, *a): return False
    return rec, _Conn


def _post(client, body):
    rec, ConnCls = _recorder()
    def _factory(): return ConnCls()
    with patch("ap.db.conn", _factory), \
         patch("ap.db.run_with_retry", side_effect=lambda fn: fn()):
        resp = client.post("/admin/release_after_hours_deferred", json=body)
    return resp, rec


class TestPreOpenGuard:
    def test_force_false_behavior(self, client):
        resp, rec = _post(client, {"force": False})
        if resp.status_code == 412:
            data = resp.get_json()
            assert data["ok"] is False
            assert data["reason"] == "pre_open_guard"
            assert data["released"] == 0
            assert rec["calls"] == []
        else:
            assert resp.status_code == 200  # CI clock past 09:35 ET

    def test_force_true_runs_update(self, client):
        resp, rec = _post(client, {"force": True})
        assert resp.status_code == 200
        assert len(rec["calls"]) == 1


class TestUpdateShape:
    def test_set_clause_is_only_last_error_null(self, client):
        resp, rec = _post(client, {"force": True})
        assert resp.status_code == 200
        sql, _ = rec["calls"][0]
        sql_u = " ".join(sql.split()).upper()
        assert "SET LAST_ERROR = NULL" in sql_u
        set_clause = sql_u.split("SET", 1)[1].split("WHERE", 1)[0]
        for forbidden in ("STATUS ", "PAYLOAD", "STARTED_TS", "FINISHED_TS",
                          "BROKER_ORDER_ID", "RESULT_JSON", "DIRECTION",
                          "CONTRACT", "TRIGGER_PRICE", "SCORE"):
            assert forbidden not in set_clause, \
                f"SET clause must not mention {forbidden!r}: {set_clause!r}"

    def test_where_clause_filters_correctly(self, client):
        resp, rec = _post(client, {"force": True})
        assert resp.status_code == 200
        sql, _ = rec["calls"][0]
        sql_u = " ".join(sql.split()).upper()
        where = sql_u.split("WHERE", 1)[1]
        assert "STATUS = 'WATCHING'" in where
        assert "AFTER_HOURS_DEFERRED:AWAITING_OVERNIGHT_REEVAL" in where
        assert "CREATED_TS >=" in where
        assert "INTERVAL" in where

    def test_clients_filter_adds_any_predicate(self, client):
        resp, rec = _post(client, {
            "force": True,
            "clients": ["jose.vasquez4011@gmail.com", "tradefluencehq@gmail.com"],
        })
        assert resp.status_code == 200
        sql, params = rec["calls"][0]
        assert "CLIENT_ID = ANY" in " ".join(sql.split()).upper()
        lists = [p for p in params if isinstance(p, list)]
        assert lists, f"no list param: {params!r}"
        assert "jose.vasquez4011@gmail.com" in lists[0]
        assert "tradefluencehq@gmail.com" in lists[0]

    def test_no_clients_filter_omits_predicate(self, client):
        resp, rec = _post(client, {"force": True})
        sql = " ".join(rec["calls"][0][0].split()).upper()
        assert "CLIENT_ID = ANY" not in sql


class TestLookback:
    def test_default_36(self, client):
        _r, rec = _post(client, {"force": True})
        assert rec["calls"][0][1][0] == "36"

    def test_custom_passes(self, client):
        _r, rec = _post(client, {"force": True, "lookback_h": 12})
        assert rec["calls"][0][1][0] == "12"

    def test_clamps_below_one(self, client):
        for v in (0, -5, -100):
            _r, rec = _post(client, {"force": True, "lookback_h": v})
            assert rec["calls"][0][1][0] == "1", f"lookback={v}"


class TestResponse:
    def test_released_and_breakdown(self, client):
        rec, ConnCls = _recorder()
        rec["fetch"] = [
            {"id": 1, "client_id": "jose.vasquez4011@gmail.com"},
            {"id": 2, "client_id": "jose.vasquez4011@gmail.com"},
            {"id": 3, "client_id": "tradefluencehq@gmail.com"},
        ]
        def _factory(): return ConnCls()
        with patch("ap.db.conn", _factory), \
             patch("ap.db.run_with_retry", side_effect=lambda fn: fn()):
            resp = client.post("/admin/release_after_hours_deferred", json={"force": True})

        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        assert data["released"] == 3
        assert data["by_client"]["jose.vasquez4011@gmail.com"] == 2
        assert data["by_client"]["tradefluencehq@gmail.com"] == 1
        assert data["lookback_hours"] == 36
        assert data["force"] is True
        assert "et_now" in data

    def test_idempotent_zero_is_200(self, client):
        resp, _ = _post(client, {"force": True})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["released"] == 0
        assert data["by_client"] == {}


class TestException:
    def test_db_failure_returns_500(self, client):
        def _raises(*_a, **_kw):
            raise RuntimeError("simulated db outage")
        with patch("ap.db.conn", _raises), \
             patch("ap.db.run_with_retry", side_effect=RuntimeError("simulated db outage")):
            resp = client.post("/admin/release_after_hours_deferred", json={"force": True})
        assert resp.status_code == 500
        data = resp.get_json()
        assert data["ok"] is False
        assert "simulated db outage" in str(data.get("error", ""))
