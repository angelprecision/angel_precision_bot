"""
Tests for ap.auth.validate_api_key Postgres-safe shape
(fix/credential-auth-client-scope-hardening).

Proves the Patch-2 fix:
  - missing/invalid keys return None (no DB call needed)
  - the SQL uses %s placeholder (NOT ?)
  - ACTIVE client returns dict; status check is case-insensitive
  - inactive client returns None
  - DB exception returns None (fail closed)

Run:
    pytest tests/test_auth_api_key.py -xvs
"""
from __future__ import annotations

import os
import re
from contextlib import contextmanager
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

# ap.db requires DATABASE_URL at import; set a dummy.
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_auth_api_key",
)


# ============================================================
# 1) Source-shape proof: SQL uses %s, NOT ?
# ============================================================

class TestSourceShape:
    AUTH_SRC = (REPO_ROOT / "ap" / "auth.py").read_text()

    def test_select_uses_percent_s_placeholder(self):
        """The query must use the Postgres %s placeholder, not SQLite ?."""
        # Find the SELECT against clients table.
        m = re.search(
            r"SELECT[^\"]+FROM\s+clients\s+WHERE\s+api_key=(\?|%s)",
            self.AUTH_SRC,
            re.IGNORECASE,
        )
        assert m, "validate_api_key must run a SELECT against clients"
        assert m.group(1) == "%s", (
            f"validate_api_key must use Postgres %s placeholder, found {m.group(1)!r}"
        )

    def test_no_chained_execute_fetchone(self):
        """The Postgres wrapper does not support .execute(...).fetchone() in
        one chain. Source must call execute and fetch separately."""
        # If the buggy pattern is present anywhere in validate_api_key, it's a regression.
        m = re.search(
            r"def validate_api_key.*?def check_rate_limit",
            self.AUTH_SRC,
            re.DOTALL,
        )
        assert m, "validate_api_key + check_rate_limit must exist"
        body = m.group(0)
        assert not re.search(r"\.execute\([^)]*\)\.fetchone\(\)", body), (
            "validate_api_key must not chain .execute(...).fetchone() \u2014 "
            "use c.execute(...) then c.fetchone() separately"
        )

    def test_require_client_auth_still_exposed(self):
        """We must NOT remove require_client_auth \u2014 dashboard/API needs it."""
        assert "def require_client_auth" in self.AUTH_SRC


# ============================================================
# 2) Behavioral tests (DB-mocked)
# ============================================================
#
# We patch ap.db.conn so the test never needs a real Postgres. The replacement
# context manager yields a fake cursor that records every execute() call and
# returns a configurable row from fetchone().

class FakeCursor:
    def __init__(self, row=None, raise_on_execute=None):
        self._row = row
        self._raise = raise_on_execute
        self.executed_sql = None
        self.executed_params = None

    def execute(self, sql, params=None):
        self.executed_sql = sql
        self.executed_params = params
        if self._raise:
            raise self._raise

    def fetchone(self):
        return self._row


@contextmanager
def _fake_conn_factory(cursor: FakeCursor):
    @contextmanager
    def _ctx():
        yield cursor
    yield _ctx


def _install_fake_conn(monkeypatch, cursor: FakeCursor):
    """Patch ap.auth.conn so it returns our fake cursor."""
    import ap.auth

    @contextmanager
    def fake_conn():
        yield cursor

    monkeypatch.setattr(ap.auth, "conn", fake_conn)
    # run_with_retry just runs the lambda; keep the real implementation
    # but make sure it sees our patched conn.
    monkeypatch.setattr(ap.auth, "run_with_retry", lambda fn: fn())


# ---- 2a) missing/invalid keys ------------------------------------------

class TestInvalidKeys:
    def test_none_returns_none(self):
        from ap.auth import validate_api_key
        assert validate_api_key(None) is None  # type: ignore[arg-type]

    def test_empty_returns_none(self):
        from ap.auth import validate_api_key
        assert validate_api_key("") is None

    def test_wrong_prefix_returns_none(self):
        from ap.auth import validate_api_key
        # 'sk_' is the OpenAI prefix shape; our keys must start with 'ak_'.
        assert validate_api_key("sk_live_abcdef") is None
        assert validate_api_key("ak-live-no-underscore") is None
        assert validate_api_key("AK_uppercase") is None


# ---- 2b) ACTIVE client returns dict -----------------------------------

class TestActiveClient:
    def test_active_client_returns_dict(self, monkeypatch):
        from ap.auth import validate_api_key
        cur = FakeCursor(row={
            "client_id": "client-A",
            "name":      "Acme Trading",
            "status":    "ACTIVE",
        })
        _install_fake_conn(monkeypatch, cur)

        out = validate_api_key("ak_live_xyz")
        assert out == {
            "client_id": "client-A",
            "name":      "Acme Trading",
            "status":    "ACTIVE",
        }
        # SQL shape
        assert "WHERE api_key=%s" in cur.executed_sql
        assert "?" not in cur.executed_sql.split("FROM clients", 1)[1]
        # Params passed correctly
        assert cur.executed_params == ("ak_live_xyz",)

    def test_active_status_case_insensitive(self, monkeypatch):
        from ap.auth import validate_api_key
        for variant in ("active", "Active", "ACTIVE", "  active  "):
            cur = FakeCursor(row={"client_id": "c", "name": "n", "status": variant})
            _install_fake_conn(monkeypatch, cur)
            assert validate_api_key("ak_live_xyz") is not None, \
                f"variant {variant!r} should pass active check"

    def test_no_row_returns_none(self, monkeypatch):
        from ap.auth import validate_api_key
        cur = FakeCursor(row=None)
        _install_fake_conn(monkeypatch, cur)
        assert validate_api_key("ak_live_unknown") is None


# ---- 2c) inactive returns None ----------------------------------------

class TestInactiveClient:
    @pytest.mark.parametrize("status", [
        "INACTIVE", "SUSPENDED", "PENDING", "CANCELLED", "TRIAL", "",
    ])
    def test_non_active_status_blocks(self, monkeypatch, status):
        from ap.auth import validate_api_key
        cur = FakeCursor(row={"client_id": "c", "name": "n", "status": status})
        _install_fake_conn(monkeypatch, cur)
        assert validate_api_key("ak_live_xyz") is None, \
            f"status {status!r} must NOT validate"


# ---- 2d) DB exception fails closed -------------------------------------

class TestDbExceptionFailsClosed:
    def test_execute_raises_returns_none(self, monkeypatch):
        from ap.auth import validate_api_key
        cur = FakeCursor(raise_on_execute=RuntimeError("connection refused"))
        _install_fake_conn(monkeypatch, cur)
        assert validate_api_key("ak_live_xyz") is None

    def test_run_with_retry_raises_returns_none(self, monkeypatch):
        from ap.auth import validate_api_key
        # Make run_with_retry itself raise.
        import ap.auth
        monkeypatch.setattr(
            ap.auth, "run_with_retry",
            lambda fn: (_ for _ in ()).throw(RuntimeError("backoff exhausted")),
        )
        # conn() isn't reached, but provide one anyway.
        @contextmanager
        def fake_conn():
            yield FakeCursor()
        monkeypatch.setattr(ap.auth, "conn", fake_conn)
        assert validate_api_key("ak_live_xyz") is None
