"""
Tests for ap.db.get_position_by_id and ap.db.get_order_by_id
optional client_id ownership filter
(fix/credential-auth-client-scope-hardening).

Proves Patch-3:
  - both signatures accept an optional client_id kwarg with default None
  - when client_id is provided, the SQL includes 'AND client_id=%s'
  - when client_id is None (or omitted), the SQL is the original global form
  - existing callers (admin/internal) keep working with the legacy signature
  - no other db helpers regressed by signature change

Run:
    pytest tests/test_client_scoped_db_lookups.py -xvs
"""
from __future__ import annotations

import inspect
import os
import re
from contextlib import contextmanager
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_scoped_lookups",
)


# ============================================================
# 1) Source-shape proofs
# ============================================================

class TestSignatures:
    def test_get_position_by_id_signature(self):
        from ap.db import get_position_by_id
        sig = inspect.signature(get_position_by_id)
        params = list(sig.parameters.values())
        # First param is position_id, second is optional client_id.
        assert len(params) >= 2, "get_position_by_id must accept (position_id, client_id=None)"
        assert params[0].name == "position_id"
        assert params[1].name == "client_id"
        assert params[1].default is None, \
            "client_id parameter must default to None for back-compat"

    def test_get_order_by_id_signature(self):
        from ap.db import get_order_by_id
        sig = inspect.signature(get_order_by_id)
        params = list(sig.parameters.values())
        assert len(params) >= 2
        assert params[0].name == "local_order_id"
        assert params[1].name == "client_id"
        assert params[1].default is None


class TestSqlShape:
    DB_SRC = (REPO_ROOT / "ap" / "db.py").read_text()

    def test_position_query_has_both_branches(self):
        """The function must have both:
          1. 'WHERE id=%s AND client_id=%s' for scoped lookups
          2. 'WHERE id=%s' for legacy global lookups
        """
        m = re.search(
            r"def get_position_by_id.*?def list_positions",
            self.DB_SRC, re.DOTALL,
        )
        assert m, "get_position_by_id must exist"
        body = m.group(0)
        assert "WHERE id=%s AND client_id=%s" in body, \
            "scoped branch must filter by client_id"
        # Legacy branch still present
        assert re.search(r"WHERE id=%s(?![\s\w]+AND\s+client_id)", body), \
            "legacy global branch (no client_id filter) must still exist"

    def test_order_query_has_both_branches(self):
        m = re.search(
            r"def get_order_by_id.*?def list_orders",
            self.DB_SRC, re.DOTALL,
        )
        assert m, "get_order_by_id must exist"
        body = m.group(0)
        assert "WHERE local_order_id=%s AND client_id=%s" in body
        assert re.search(
            r"WHERE local_order_id=%s(?![\s\w]+AND\s+client_id)",
            body,
        ), "legacy global branch must still exist"


# ============================================================
# 2) Behavioral tests (DB-mocked)
# ============================================================

class FakeCursor:
    def __init__(self, row=None):
        self._row = row
        self.executed_sql = None
        self.executed_params = None

    def execute(self, sql, params=None):
        self.executed_sql = sql
        self.executed_params = params

    def fetchone(self):
        return self._row


def _install_fake_conn(monkeypatch, cursor: FakeCursor):
    import ap.db

    @contextmanager
    def fake_conn():
        yield cursor

    monkeypatch.setattr(ap.db, "conn", fake_conn)
    # run_with_retry just runs the lambda
    monkeypatch.setattr(ap.db, "run_with_retry", lambda fn: fn())


# ---- 2a) get_position_by_id --------------------------------------------

class TestGetPositionByIdBehavior:
    def test_global_lookup_when_client_id_omitted(self, monkeypatch):
        from ap.db import get_position_by_id
        cur = FakeCursor(row={"id": "pos-1", "client_id": "client-A"})
        _install_fake_conn(monkeypatch, cur)

        out = get_position_by_id("pos-1")
        assert out == {"id": "pos-1", "client_id": "client-A"}
        # Legacy SQL shape: no client_id filter.
        assert "WHERE id=%s" in cur.executed_sql
        assert "client_id" not in cur.executed_sql
        assert cur.executed_params == ("pos-1",)

    def test_scoped_lookup_when_client_id_provided(self, monkeypatch):
        from ap.db import get_position_by_id
        cur = FakeCursor(row={"id": "pos-1", "client_id": "client-A"})
        _install_fake_conn(monkeypatch, cur)

        out = get_position_by_id("pos-1", client_id="client-A")
        assert out == {"id": "pos-1", "client_id": "client-A"}
        assert "WHERE id=%s AND client_id=%s" in cur.executed_sql
        assert cur.executed_params == ("pos-1", "client-A")

    def test_cross_tenant_returns_none(self, monkeypatch):
        """The SQL filter means the DB just returns no row; the helper
        returns None. This is the cross-tenant leak guard."""
        from ap.db import get_position_by_id
        cur = FakeCursor(row=None)  # simulating "row exists but not for this client"
        _install_fake_conn(monkeypatch, cur)

        out = get_position_by_id("pos-1", client_id="client-B")
        assert out is None
        assert "AND client_id=%s" in cur.executed_sql
        assert cur.executed_params == ("pos-1", "client-B")


# ---- 2b) get_order_by_id ----------------------------------------------

class TestGetOrderByIdBehavior:
    def test_global_lookup_when_client_id_omitted(self, monkeypatch):
        from ap.db import get_order_by_id
        cur = FakeCursor(row={"local_order_id": "loc-1", "client_id": "client-A"})
        _install_fake_conn(monkeypatch, cur)

        out = get_order_by_id("loc-1")
        assert out == {"local_order_id": "loc-1", "client_id": "client-A"}
        assert "WHERE local_order_id=%s" in cur.executed_sql
        assert "client_id" not in cur.executed_sql
        assert cur.executed_params == ("loc-1",)

    def test_scoped_lookup_when_client_id_provided(self, monkeypatch):
        from ap.db import get_order_by_id
        cur = FakeCursor(row={"local_order_id": "loc-1", "client_id": "client-A"})
        _install_fake_conn(monkeypatch, cur)

        out = get_order_by_id("loc-1", client_id="client-A")
        assert out == {"local_order_id": "loc-1", "client_id": "client-A"}
        assert "WHERE local_order_id=%s AND client_id=%s" in cur.executed_sql
        assert cur.executed_params == ("loc-1", "client-A")

    def test_cross_tenant_returns_none(self, monkeypatch):
        from ap.db import get_order_by_id
        cur = FakeCursor(row=None)
        _install_fake_conn(monkeypatch, cur)
        out = get_order_by_id("loc-1", client_id="client-B")
        assert out is None
        assert "AND client_id=%s" in cur.executed_sql


# ============================================================
# 3) No existing callers break
# ============================================================
#
# The two helpers have zero non-test, non-definition callers right now
# (verified at PR-prep time). The test below re-checks at test time so a
# future caller doesn't get silently broken by the signature widening.

class TestNoBrokenCallers:
    def test_no_unkeyworded_two_arg_callers(self):
        """If anyone calls get_position_by_id(pos_id, something) without
        keyword, our new optional param could misroute. Confirm no callers
        do that today.
        """
        ap_dir = REPO_ROOT / "ap"
        callers: list[tuple[str, str]] = []
        for path in ap_dir.rglob("*.py"):
            if path.name == "db.py":
                continue
            src = path.read_text()
            for func in ("get_position_by_id", "get_order_by_id"):
                # Look for two-positional-arg calls (no '=' inside the parens).
                for m in re.finditer(
                    rf"\b{func}\(([^)]*)\)", src,
                ):
                    args = m.group(1).strip()
                    if not args:
                        continue
                    # If there's a comma at the TOP level AND no '=' before
                    # that second arg, it's a positional second arg.
                    # (Cheap parser — tolerates nested calls only because we
                    # don't have any in practice.)
                    if "," in args and "=" not in args.split(",", 1)[1]:
                        callers.append((str(path.relative_to(REPO_ROOT)), args))

        assert not callers, (
            "Found callers passing a positional second arg to get_position_by_id "
            "/ get_order_by_id; they must use the keyword form: "
            f"{callers}"
        )
