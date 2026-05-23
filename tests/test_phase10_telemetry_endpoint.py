"""
Phase 10 tests: telemetry HTTP endpoint (PR #27).

Verifies that the dashboard backend can actually consume the Phase 6
projection via HTTP. This closes the gap from PR #23 review: the helper
existed but had zero callers.

Run:
    pytest tests/test_phase10_telemetry_endpoint.py -xvs
"""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_phase10",
)
# Provide an admin key so auth passes during tests; real production uses
# the same env var with a real secret.
os.environ.setdefault("ADMIN_KEY", "test-admin-key-phase10")


# ============================================================
# 1. Module surface
# ============================================================

class TestModuleSurface:
    def test_blueprint_importable(self):
        from ap.telemetry_api import telemetry_bp
        assert telemetry_bp is not None
        assert telemetry_bp.name == "telemetry"
        assert telemetry_bp.url_prefix == "/telemetry"

    def test_schema_constant_has_all_keys(self):
        from ap.telemetry_api import SCHEMA
        keys = SCHEMA["keys"]
        # The full Phase 6 schema must be exposed verbatim.
        required = {
            "client_id", "local_order_id", "broker_order_id",
            "symbol", "contract", "direction", "status", "score",
            "entry_attempt", "repeg_attempt", "retry_attempt",
            "reason_bucket", "cancel_reason_detail",
            "selector_ask", "submit_ask", "submit_limit", "fill_price",
            "quote_age_ms", "seconds_to_fill",
            "account_equity", "position_budget", "final_qty",
            "sizing_reason_code",
        }
        assert set(keys) == required, \
            f"schema keys must match Phase 6 contract: extra={set(keys)-required} missing={required-set(keys)}"

    def test_result_buckets_exposed(self):
        from ap.telemetry_api import SCHEMA
        assert "filled" in SCHEMA["reason_buckets"]
        assert "canceled_signal_alive" in SCHEMA["reason_buckets"]


# ============================================================
# 2. app.py registers the blueprint
# ============================================================

class TestAppRegistration:
    def test_app_py_imports_telemetry_bp(self):
        src = (REPO_ROOT / "app.py").read_text()
        assert "from ap.telemetry_api import telemetry_bp" in src

    def test_app_py_registers_telemetry_bp(self):
        src = (REPO_ROOT / "app.py").read_text()
        assert "app.register_blueprint(_telemetry_bp)" in src

    def test_registration_in_create_app(self):
        src = (REPO_ROOT / "app.py").read_text()
        # The blueprint registration must live inside create_app().
        # Find the create_app function body and assert it.
        idx_def = src.find("def create_app(")
        assert idx_def > 0
        # End of create_app is the next top-level def or end of file.
        idx_next_def = src.find("\ndef ", idx_def + 1)
        body = src[idx_def: idx_next_def if idx_next_def > 0 else len(src)]
        assert "from ap.telemetry_api import telemetry_bp" in body
        assert "register_blueprint(_telemetry_bp)" in body


# ============================================================
# 3. Flask test client + DB mocking
# ============================================================

@pytest.fixture
def client():
    """Build the blueprint in isolation so we can hit it with the Flask
    test client without booting the whole app (which would require a real
    DB connection, Redis, etc.).
    """
    from flask import Flask
    from ap.telemetry_api import telemetry_bp
    app = Flask(__name__)
    app.register_blueprint(telemetry_bp)
    return app.test_client()


def _headers(*, admin=True):
    h = {}
    if admin:
        h["X-Admin-Key"] = "test-admin-key-phase10"
    return h


# ============================================================
# 4. Auth
# ============================================================

class TestAuth:
    def test_no_header_returns_401(self, client):
        resp = client.get("/telemetry/entries?client_id=client-A")
        assert resp.status_code == 401
        assert resp.get_json() == {"ok": False, "error": "unauthorized"}

    def test_wrong_header_returns_401(self, client):
        resp = client.get("/telemetry/entries?client_id=client-A",
                          headers={"X-Admin-Key": "wrong"})
        assert resp.status_code == 401

    def test_admin_key_accepted(self, client):
        # We mock the DB select so this exercises only the auth layer.
        with patch("ap.telemetry_api._select_entries", return_value=[]):
            resp = client.get("/telemetry/entries?client_id=client-A",
                              headers=_headers())
            assert resp.status_code == 200

    def test_telemetry_key_accepted(self, client, monkeypatch):
        monkeypatch.setenv("TELEMETRY_API_KEY", "tele-secret-key")
        # Re-import to make module-level _telemetry_key() see new env? No \u2014
        # _telemetry_key() reads os.getenv each call, so monkeypatch is fine.
        with patch("ap.telemetry_api._select_entries", return_value=[]):
            resp = client.get("/telemetry/entries?client_id=client-A",
                              headers={"X-Telemetry-Key": "tele-secret-key"})
            assert resp.status_code == 200


# ============================================================
# 5. Schema endpoint
# ============================================================

class TestSchemaRoute:
    def test_schema_endpoint(self, client):
        resp = client.get("/telemetry/schema", headers=_headers())
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True
        assert "keys" in body
        assert "reason_buckets" in body
        assert "selector_ask" in body["keys"]
        assert "submit_ask" in body["keys"]
        assert "filled" in body["reason_buckets"]


# ============================================================
# 6. /entries listing
# ============================================================

class TestListEntries:
    SAMPLE_ROW = {
        "client_id": "client-A",
        "local_order_id": "loc-42",
        "broker_order_id": "br-42",
        "position_id": None,
        "kind": "ENTRY",
        "status": "FILLED",
        "symbol": "QCOM",
        "contract": "QCOM260523C00185000",
        "direction": "CALL",
        "qty": 9,
        "limit_price": 3.08,
        "last_error": None,
        "created_ts": "2026-05-23T14:00:00Z",
        "updated_ts": "2026-05-23T14:00:12Z",
        "meta": {
            "score": 92.0,
            "entry_attempt": 0,
            "repeg_attempts": 0,
            "retry_attempts": 0,
            "selector_ask": 3.05,
            "submit_ask": 3.08,
            "submit_limit": 3.08,
            "quote_age_ms": 14,
            "account_equity": 30000.0,
            "position_budget": 3000.0,
            "final_qty": 9,
            "sizing_reason_code": "ACCOUNT_EQUITY_PCT",
            "ticker": "QCOM",
        },
        "p_avg_fill": 3.08,
        "p_opened_ts": "2026-05-23T14:00:12Z",
    }

    def test_requires_client_id(self, client):
        resp = client.get("/telemetry/entries", headers=_headers())
        assert resp.status_code == 400
        body = resp.get_json()
        assert body["ok"] is False
        assert "client_id" in body["error"]

    def test_returns_projected_entries(self, client):
        with patch("ap.telemetry_api._select_entries",
                   return_value=[self.SAMPLE_ROW]):
            resp = client.get("/telemetry/entries?client_id=client-A",
                              headers=_headers())
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True
        assert body["client_id"] == "client-A"
        assert body["count"] == 1
        e = body["entries"][0]

        # All required keys present
        from ap.entry_telemetry import compute_entry_telemetry
        # Compute the expected projection and assert key set matches
        expected_keys = set(compute_entry_telemetry({}).keys())
        assert set(e.keys()) == expected_keys

        # Golden values from Phase 4 acceptance
        assert e["symbol"] == "QCOM"
        assert e["direction"] == "CALL"
        assert e["reason_bucket"] == "filled"
        assert e["selector_ask"] == 3.05
        assert e["submit_ask"] == 3.08
        assert e["fill_price"] == 3.08
        assert e["final_qty"] == 9
        assert e["sizing_reason_code"] == "ACCOUNT_EQUITY_PCT"
        assert e["account_equity"] == 30000.0

    def test_limit_clamped(self, client):
        captured = {}
        def fake_select(**kwargs):
            captured.update(kwargs)
            return []
        with patch("ap.telemetry_api._select_entries", side_effect=fake_select):
            # Way over max
            client.get("/telemetry/entries?client_id=c&limit=99999",
                       headers=_headers())
            assert captured["limit"] == 500
            # Negative
            client.get("/telemetry/entries?client_id=c&limit=-1",
                       headers=_headers())
            assert captured["limit"] == 1

    def test_since_hours_clamped(self, client):
        captured = {}
        def fake_select(**kwargs):
            captured.update(kwargs)
            return []
        with patch("ap.telemetry_api._select_entries", side_effect=fake_select):
            client.get("/telemetry/entries?client_id=c&since_hours=9999",
                       headers=_headers())
            assert captured["since_hours"] == 168
            client.get("/telemetry/entries?client_id=c&since_hours=0",
                       headers=_headers())
            assert captured["since_hours"] == 1

    def test_status_filter_parsed(self, client):
        captured = {}
        def fake_select(**kwargs):
            captured.update(kwargs)
            return []
        with patch("ap.telemetry_api._select_entries", side_effect=fake_select):
            client.get("/telemetry/entries?client_id=c&status=FILLED,CANCELED",
                       headers=_headers())
            assert captured["statuses"] == ["FILLED", "CANCELED"]

    def test_internal_error_returns_500(self, client):
        with patch("ap.telemetry_api._select_entries",
                   side_effect=RuntimeError("DB down")):
            resp = client.get("/telemetry/entries?client_id=c",
                              headers=_headers())
            assert resp.status_code == 500
            assert resp.get_json() == {"ok": False, "error": "internal"}


# ============================================================
# 7. /entries/<id> single-row
# ============================================================

class TestGetOneEntry:
    def test_not_found_returns_404(self, client):
        with patch("ap.telemetry_api._select_one_entry", return_value=None):
            resp = client.get("/telemetry/entries/missing", headers=_headers())
            assert resp.status_code == 404

    def test_returns_one_projected_entry(self, client):
        sample = dict(TestListEntries.SAMPLE_ROW)
        with patch("ap.telemetry_api._select_one_entry", return_value=sample):
            resp = client.get("/telemetry/entries/loc-42", headers=_headers())
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["ok"] is True
        e = body["entry"]
        assert e["local_order_id"] == "loc-42"
        assert e["reason_bucket"] == "filled"
        assert e["fill_price"] == 3.08


# ============================================================
# 8. Read-only invariant: no INSERT/UPDATE/DELETE in the module
# ============================================================

class TestReadOnlyInvariant:
    def test_no_write_sql_in_module(self):
        src = (REPO_ROOT / "ap" / "telemetry_api.py").read_text()
        # Strip comments + docstrings so we don't match the safety comment.
        import re
        cleaned = re.sub(r'""".*?"""', "", src, flags=re.DOTALL)
        cleaned = re.sub(r"#[^\n]*", "", cleaned)
        # Look for write-SQL keywords as actual statements.
        for kw in ("INSERT INTO", "UPDATE ", "DELETE FROM", "ALTER ", "DROP ",
                   "TRUNCATE", "GRANT ", "REVOKE "):
            assert kw not in cleaned, \
                f"telemetry_api.py contains forbidden write keyword: {kw!r}"

    def test_uses_select_with_proper_params(self):
        src = (REPO_ROOT / "ap" / "telemetry_api.py").read_text()
        # Both SELECT helpers exist
        assert "def _select_entries(" in src
        assert "def _select_one_entry(" in src
        # Both use parameterized queries (no f-string into SQL beyond the
        # explicit status_clause built from safe placeholders)
        assert "(%s)" in src or "%s" in src, "must use parameterized queries"
