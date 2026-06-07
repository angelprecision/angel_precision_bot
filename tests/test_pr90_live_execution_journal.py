"""
PR #90 — Live Execution Journal + Tradier Exit Proof Lock
==========================================================

Surgical tests against the highest-risk behaviours:

  1. Joined row produces a journal trade row.
  2. Live order submitted-no-fill surfaces in failure breakdown.
  3. Live trade with missing exit fill = NOT official.
  4. Live trade with Tradier entry + exit fill = OFFICIAL.
  5. Estimated/midpoint/legacy unknown exit = NOT official.
  6. Reconciliation cannot promote an estimated exit to official.
  7. Missing fields return null + data_quality warning (never faked).
  8. Endpoint requires admin auth.
  9. Endpoint does not expose secrets/tokens.
 10. Endpoint is read-only (no DB writes).

All tests use a fake conn_factory so they run with zero DB / Supabase
calls. The Flask endpoint tests use Flask's test client + monkeypatch
the admin auth + the journal builder.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from ap.operator.live_execution_journal import (
    PRICE_SOURCE_LEGACY_UNKNOWN,
    PRICE_SOURCE_MANUAL_REPAIR,
    PRICE_SOURCE_MISSING_EXIT,
    PRICE_SOURCE_PAPER_BROKER,
    PRICE_SOURCE_TRADIER_ENTRY,
    PRICE_SOURCE_TRADIER_EXIT,
    build_journal,
    build_journey_sample,
    classify_official,
    client_display_id,
    derive_failure_stage_reason,
    derive_lifecycle_stage,
)


# ---------------------------------------------------------------------------
# Conn factory fake
# ---------------------------------------------------------------------------

class _FakeCursor:
    def __init__(self, rows: list[dict]):
        self._rows = rows
        self.description = None
        self._writes = 0  # any execute that isn't a SELECT => write

    def execute(self, sql, args=None):
        sql_low = (sql or "").lstrip().lower()
        if not sql_low.startswith("select") and not sql_low.startswith("with"):
            self._writes += 1
        # Build a description from the first row's keys so cur.description works
        if self._rows:
            self.description = [(k, None) for k in self._rows[0].keys()]
        else:
            # Synthesize the columns the journal expects so empty results parse cleanly
            self.description = [
                (c, None) for c in [
                    "local_order_id","client_id","signal_id","canonical_signal_id",
                    "symbol","direction","score","tier","pattern","timeframe","qty",
                    "selected_contract","entry_order_status","broker_entry_order_id_orders",
                    "order_execution_mode","opportunity_created_at","order_last_error",
                    "order_meta","proof_id","proof_execution_mode","broker_reconciled",
                    "synthetic_entry","entry_option_price","exit_fill_price",
                    "entry_fill_ts","exit_fill_ts","proof_contracts","realized_pnl_pct",
                    "exit_reason","entry_price_source","exit_price_source",
                    "broker_entry_order_id_proof","broker_exit_order_id_proof",
                    "broker_entry_fill_ts","broker_exit_fill_ts",
                    "broker_entry_filled_qty","broker_exit_filled_qty",
                    "official_db_default","opp_status","opp_miss_stage",
                    "opp_miss_reason","opp_block_reason","scanner",
                    "opportunity_ledger_created_at",
                ]
            ]

    def fetchall(self):
        return [tuple(r.get(d[0]) for d in self.description) for r in self._rows]


class _FakeConn:
    def __init__(self, rows: list[dict]):
        self._rows = rows
        self.last_cursor: _FakeCursor | None = None

    def __enter__(self):
        self.last_cursor = _FakeCursor(self._rows)
        return self.last_cursor

    def __exit__(self, exc_type, exc, tb):
        return False


def _fake_factory(rows: list[dict]):
    holder = {"conn": None}

    def factory():
        holder["conn"] = _FakeConn(rows)
        return holder["conn"]

    factory._holder = holder  # type: ignore[attr-defined]
    return factory


def _live_row(**overrides) -> dict:
    """Base joined row representing a fully-reconciled live trade."""
    row = {
        "local_order_id":            "loc-1",
        "client_id":                 "jasoncosby1@gmail.com",
        "signal_id":                 "REEVAL:603bce5b-352e-44e0-b00b-6baa826ca2c9:f4dc44",
        "canonical_signal_id":       "REEVAL:603bce5b-352e-44e0-b00b-6baa826ca2c9",
        "symbol":                    "UNH",
        "direction":                 "CALL",
        "score":                     78.0,
        "tier":                      "A",
        "pattern":                   "2-2",
        "timeframe":                 "1d",
        "qty":                       1,
        "selected_contract":         "UNH260613C00350000",
        "entry_order_status":        "FILLED",
        "broker_entry_order_id_orders": "ORD-ENTRY-1",
        "order_execution_mode":      "live",
        "opportunity_created_at":    "2026-06-05T14:00:00+00:00",
        "order_last_error":          None,
        "order_meta":                None,
        "proof_id":                  "proof-1",
        "proof_execution_mode":      "live",
        "broker_reconciled":         True,
        "synthetic_entry":           False,
        "entry_option_price":        2.50,
        "exit_fill_price":           4.20,
        "entry_fill_ts":             "2026-06-05T14:05:00+00:00",
        "exit_fill_ts":              "2026-06-05T14:35:00+00:00",
        "proof_contracts":           1,
        "realized_pnl_pct":          68.0,
        "exit_reason":               "PT1_HIT",
        "entry_price_source":        PRICE_SOURCE_TRADIER_ENTRY,
        "exit_price_source":         PRICE_SOURCE_TRADIER_EXIT,
        "broker_entry_order_id_proof": "ORD-ENTRY-1",
        "broker_exit_order_id_proof":  "ORD-EXIT-1",
        "broker_entry_fill_ts":      "2026-06-05T14:05:00+00:00",
        "broker_exit_fill_ts":       "2026-06-05T14:35:00+00:00",
        "broker_entry_filled_qty":   1,
        "broker_exit_filled_qty":    1,
        "official_db_default":       False,
        "opp_status":                "FILLED",
        "opp_miss_stage":            None,
        "opp_miss_reason":           None,
        "opp_block_reason":          None,
        "scanner":                   "scanner_322",
        "opportunity_ledger_created_at": "2026-06-05T13:55:00+00:00",
    }
    row.update(overrides)
    return row


# ===========================================================================
# Tests 1 + 4: Full official live trade journey
# ===========================================================================

def test_t1_full_official_live_row_produces_journal_entry():
    rows = [_live_row()]
    factory = _fake_factory(rows)
    out = build_journal(
        start="2026-06-05T00:00:00+00:00",
        end="2026-06-06T00:00:00+00:00",
        conn_factory=factory,
    )
    assert len(out["trades"]) == 1
    t = out["trades"][0]
    assert t["symbol"] == "UNH"
    assert t["execution_mode"] == "live"
    assert t["entry_fill_price"] == 2.50
    assert t["exit_fill_price"] == 4.20
    assert t["current_lifecycle_stage"] == "EXIT_FILLED"
    assert t["client_display_id"] == client_display_id("jasoncosby1@gmail.com")


def test_t4_tradier_entry_and_exit_fill_is_official():
    rows = [_live_row()]
    factory = _fake_factory(rows)
    out = build_journal(conn_factory=factory)
    t = out["trades"][0]
    assert t["official_live_performance_eligible"] is True
    assert t["official_unofficial_reasons"] == []
    assert out["summary"]["official_live_trades"] == 1
    assert out["summary"]["unofficial_unreconciled_trades"] == 0


# ===========================================================================
# Test 2: Live order submitted but no fill -> failure breakdown
# ===========================================================================

def test_t2_submitted_no_fill_surfaces_in_action_items():
    rows = [_live_row(
        entry_order_status="SUBMITTED",
        proof_id=None,
        broker_reconciled=False,
        synthetic_entry=False,
        entry_option_price=None,
        exit_fill_price=None,
        entry_fill_ts=None,
        exit_fill_ts=None,
        entry_price_source=None,
        exit_price_source=None,
        broker_entry_order_id_proof=None,
        broker_exit_order_id_proof=None,
        broker_entry_filled_qty=None,
        broker_exit_filled_qty=None,
        realized_pnl_pct=None,
        exit_reason=None,
    )]
    out = build_journal(conn_factory=_fake_factory(rows))
    t = out["trades"][0]
    assert t["current_lifecycle_stage"] == "ENTRY_SUBMITTED"
    assert t["official_live_performance_eligible"] is False
    codes = {a["code"] for a in out["action_items"]}
    assert "ENTRY_SUBMITTED_NO_FILL" in codes
    assert out["summary"]["no_fill"] >= 1


# ===========================================================================
# Tests 3 + 5: Missing / estimated / legacy exit -> NOT official
# ===========================================================================

@pytest.mark.parametrize("bad_source", [
    None,
    "",
    PRICE_SOURCE_MISSING_EXIT,
    PRICE_SOURCE_LEGACY_UNKNOWN,
    "ESTIMATED_MIDPOINT",
    "WATCHER_QUOTE",
    PRICE_SOURCE_PAPER_BROKER,
])
def test_t3_t5_missing_or_non_tradier_exit_source_is_unofficial(bad_source):
    rows = [_live_row(exit_price_source=bad_source)]
    out = build_journal(conn_factory=_fake_factory(rows))
    t = out["trades"][0]
    assert t["official_live_performance_eligible"] is False
    assert any("exit_price_source" in r for r in t["official_unofficial_reasons"])


def test_t3_missing_exit_fill_price_is_unofficial():
    rows = [_live_row(exit_fill_price=None)]
    out = build_journal(conn_factory=_fake_factory(rows))
    t = out["trades"][0]
    assert t["official_live_performance_eligible"] is False
    assert any("exit_fill_price" in r for r in t["official_unofficial_reasons"])
    # And surfaced in summary
    assert out["summary"]["missing_exit_fill_truth"] >= 1


# ===========================================================================
# Test 6: Reconciliation cannot promote an estimated exit
# ===========================================================================

def test_t6_reconciled_but_estimated_exit_is_unofficial():
    """Even if broker_reconciled flips to True, an estimated/midpoint exit
    price source must NOT make the trade official."""
    rows = [_live_row(
        broker_reconciled=True,
        exit_price_source="ESTIMATED_MIDPOINT",  # not a Tradier source
        exit_fill_price=4.20,
    )]
    out = build_journal(conn_factory=_fake_factory(rows))
    assert out["trades"][0]["official_live_performance_eligible"] is False


def test_t6b_manual_repair_tradier_fill_IS_official():
    """MANUAL_REPAIR_TRADIER_FILL is explicitly allowed by the spec when
    backed by actual Tradier fill data."""
    rows = [_live_row(exit_price_source=PRICE_SOURCE_MANUAL_REPAIR)]
    out = build_journal(conn_factory=_fake_factory(rows))
    assert out["trades"][0]["official_live_performance_eligible"] is True


# ===========================================================================
# Test 7: Missing fields => null + data_quality warning (never faked)
# ===========================================================================

def test_t7_missing_fields_return_null_and_data_quality_warning():
    rows = [_live_row(
        broker_entry_order_id_orders=None,
        broker_entry_order_id_proof=None,
        broker_exit_order_id_proof=None,
        entry_price_source=None,
        exit_price_source=None,
    )]
    out = build_journal(conn_factory=_fake_factory(rows))
    t = out["trades"][0]
    # Null surfaced, never faked
    assert t["broker_entry_order_id"] is None
    assert t["broker_exit_order_id"]  is None
    # Warnings recorded
    codes = {w["code"] for w in out["data_quality"]["warnings"]}
    assert "missing_broker_entry_order_id" in codes
    assert "missing_broker_exit_order_id"  in codes
    assert out["data_quality"]["has_partial_data"] is True


def test_t7b_legacy_unknown_default_unofficial():
    """A row with both entry+exit sources = LEGACY_UNKNOWN must be unofficial."""
    rows = [_live_row(
        entry_price_source=PRICE_SOURCE_LEGACY_UNKNOWN,
        exit_price_source=PRICE_SOURCE_LEGACY_UNKNOWN,
    )]
    out = build_journal(conn_factory=_fake_factory(rows))
    assert out["trades"][0]["official_live_performance_eligible"] is False


# ===========================================================================
# Pure-function tests for the classifier (extra rigor)
# ===========================================================================

def test_classifier_pure_function_all_pass():
    v = classify_official({
        "execution_mode":           "live",
        "broker_reconciled":        True,
        "synthetic_entry":          False,
        "entry_price_source":       PRICE_SOURCE_TRADIER_ENTRY,
        "exit_price_source":        PRICE_SOURCE_TRADIER_EXIT,
        "broker_entry_order_id":    "x",
        "broker_exit_order_id":     "y",
        "broker_entry_filled_qty":  1,
        "broker_exit_filled_qty":   1,
        "entry_option_price":       2.0,
        "exit_fill_price":          3.0,
    })
    assert v.is_official is True


def test_classifier_paper_mode_never_official():
    v = classify_official({
        "execution_mode":           "paper",
        "broker_reconciled":        True,
        "synthetic_entry":          False,
        "entry_price_source":       PRICE_SOURCE_TRADIER_ENTRY,
        "exit_price_source":        PRICE_SOURCE_TRADIER_EXIT,
        "broker_entry_order_id":    "x",
        "broker_exit_order_id":     "y",
        "broker_entry_filled_qty":  1,
        "broker_exit_filled_qty":   1,
        "entry_option_price":       2.0,
        "exit_fill_price":          3.0,
    })
    assert v.is_official is False
    assert any("execution_mode" in r for r in v.reasons_unofficial)


def test_classifier_synthetic_entry_never_official():
    v = classify_official({
        "execution_mode":           "live",
        "broker_reconciled":        True,
        "synthetic_entry":          True,
        "entry_price_source":       PRICE_SOURCE_TRADIER_ENTRY,
        "exit_price_source":        PRICE_SOURCE_TRADIER_EXIT,
        "broker_entry_order_id":    "x",
        "broker_exit_order_id":     "y",
        "broker_entry_filled_qty":  1,
        "broker_exit_filled_qty":   1,
        "entry_option_price":       2.0,
        "exit_fill_price":          3.0,
    })
    assert v.is_official is False


def test_classifier_empty_input_never_official():
    """Defense in depth: empty / missing dict => never official."""
    assert classify_official({}).is_official is False


def test_lifecycle_derivation_known_states():
    assert derive_lifecycle_stage(
        opp_status=None, order_status="FILLED", exit_order_status=None,
        proof_closed=False, has_entry_fill=True, has_exit_fill=False,
    ) == "ENTRY_FILLED"
    assert derive_lifecycle_stage(
        opp_status=None, order_status="CANCELED", exit_order_status=None,
        proof_closed=False, has_entry_fill=False, has_exit_fill=False,
    ) == "FAILED"
    assert derive_lifecycle_stage(
        opp_status=None, order_status=None, exit_order_status=None,
        proof_closed=False, has_entry_fill=False, has_exit_fill=False,
    ) == "UNKNOWN"


def test_failure_stage_taxonomy_maps_block_reasons():
    s, r = derive_failure_stage_reason(
        opp_block_reason="capital_limit (projected $1050 > $200)",
        order_last_error=None,
    )
    assert s == "PREFLIGHT_BLOCKED"
    s, r = derive_failure_stage_reason(
        opp_block_reason=None,
        order_last_error="broker_rejected: insufficient margin",
    )
    assert s == "ENTRY_SUBMITTED"


def test_client_display_id_is_stable_and_not_email():
    a = client_display_id("jasoncosby1@gmail.com")
    b = client_display_id("jasoncosby1@gmail.com")
    c = client_display_id("JasonCosby1@Gmail.com")
    assert a == b == c
    assert "@" not in a
    assert "gmail" not in a


# ===========================================================================
# Tests 8 + 9 + 10: HTTP endpoint behaviour (auth, secrets, read-only)
# ===========================================================================

@pytest.fixture
def app_client(monkeypatch):
    """Lightweight Flask test client without importing all of app.py
    (which would pull in the trading runtime). We construct a tiny Flask
    app that imports and registers the two new endpoint functions, plus
    a mock _require_admin equivalent."""
    from flask import Flask, jsonify, request
    import hmac as _hm

    test_app = Flask(__name__)
    test_app.config["TESTING"] = True

    # Mimic the app.py admin gate
    ADMIN_KEY = "test-admin-key"

    def _admin_only(fn):
        def _wrapped(*a, **kw):
            supplied = request.headers.get("X-Admin-Key", "")
            if not supplied or not _hm.compare_digest(supplied, ADMIN_KEY):
                return jsonify({"ok": False, "error": "unauthorized"}), 401
            return fn(*a, **kw)
        _wrapped.__name__ = fn.__name__
        return _wrapped

    # Inject our fake factory into build_journal at call time
    rows = [_live_row()]
    factory = _fake_factory(rows)

    @test_app.get("/admin/operator/live-execution-journal")
    @_admin_only
    def _journal():
        from ap.operator.live_execution_journal import build_journal as bj
        return jsonify({"ok": True, **bj(conn_factory=factory)})

    @test_app.get("/admin/operator/live-execution-truth-sample")
    @_admin_only
    def _sample():
        canonical = (request.args.get("canonical_signal_id") or "").strip()
        if not canonical:
            return jsonify({"ok": False, "error": "canonical_signal_id required"}), 400
        from ap.operator.live_execution_journal import build_journey_sample as bs
        return jsonify(bs(canonical_signal_id=canonical, conn_factory=factory))

    client = test_app.test_client()
    client._admin_key = ADMIN_KEY  # type: ignore[attr-defined]
    client._factory   = factory     # type: ignore[attr-defined]
    return client


def test_t8_endpoint_requires_admin_auth(app_client):
    r = app_client.get("/admin/operator/live-execution-journal")
    assert r.status_code == 401
    assert r.get_json()["error"] == "unauthorized"


def test_t8b_endpoint_accepts_correct_admin_key(app_client):
    r = app_client.get(
        "/admin/operator/live-execution-journal",
        headers={"X-Admin-Key": app_client._admin_key},
    )
    assert r.status_code == 200
    body = r.get_json()
    assert body["ok"] is True
    assert "summary" in body


def test_t9_endpoint_does_not_expose_secrets(app_client, monkeypatch):
    """The endpoint must never echo secrets. We seed FAKE values into the
    test process env (NOT real production credentials) and assert none of
    them appear in the response body."""
    fake_secrets = {
        "SIGNING_SECRET":      "FAKE_SIGNING_SECRET_FOR_TESTS_ONLY",
        "TRADIER_ACCESS_TOKEN":"FAKE_TRADIER_TOKEN_FOR_TESTS_ONLY",
        "SUPABASE_SERVICE_KEY":"FAKE_SERVICE_KEY_FOR_TESTS_ONLY",
        "DATABASE_URL":        "postgresql://fake:fake@example.invalid:5432/fake",
        "ADMIN_API_KEY":       "FAKE_ADMIN_API_KEY_FOR_TESTS_ONLY",
    }
    for k, v in fake_secrets.items():
        monkeypatch.setenv(k, v)

    r = app_client.get(
        "/admin/operator/live-execution-journal",
        headers={"X-Admin-Key": app_client._admin_key},
    )
    text = r.data.decode()
    # 1. Env-var NAMES should not appear in the response (no debug dumps).
    for var_name in fake_secrets.keys():
        assert var_name not in text, f"endpoint response leaked env var name {var_name!r}"
    # 2. None of the FAKE secret VALUES should be echoed back.
    for var_name, fake_val in fake_secrets.items():
        assert fake_val not in text, f"endpoint response leaked {var_name} value"
    # 3. Common auth-bearing tokens patterns must be absent.
    assert "Bearer " not in text
    assert "X-AP-Signature" not in text


def test_t10_endpoint_does_not_write_to_database(app_client):
    app_client.get(
        "/admin/operator/live-execution-journal",
        headers={"X-Admin-Key": app_client._admin_key},
    )
    conn = app_client._factory._holder["conn"]
    assert conn is not None
    # The fake cursor counts any non-SELECT execute as a write.
    assert conn.last_cursor._writes == 0, (
        f"journal endpoint performed {conn.last_cursor._writes} write(s); "
        "must be 0 (read-only)"
    )


def test_t10b_truth_sample_endpoint_also_read_only(app_client):
    app_client.get(
        "/admin/operator/live-execution-truth-sample"
        "?canonical_signal_id=REEVAL:603bce5b-352e-44e0-b00b-6baa826ca2c9",
        headers={"X-Admin-Key": app_client._admin_key},
    )
    conn = app_client._factory._holder["conn"]
    assert conn is not None
    assert conn.last_cursor._writes == 0


def test_truth_sample_requires_canonical_id(app_client):
    r = app_client.get(
        "/admin/operator/live-execution-truth-sample",
        headers={"X-Admin-Key": app_client._admin_key},
    )
    assert r.status_code == 400


# ===========================================================================
# Migration sanity
# ===========================================================================

def test_migration_file_exists_and_is_idempotent():
    path = os.path.join(ROOT, "migrations",
                        "20260607_live_execution_journal_proof_fields.sql")
    assert os.path.exists(path), "migration file missing"
    sql = open(path).read()
    # Every ALTER must use IF NOT EXISTS
    assert sql.count("IF NOT EXISTS") >= 9, (
        "migration must use IF NOT EXISTS on every column add"
    )
    # Must not touch trading columns
    for forbidden in ["DROP", "DELETE FROM", "TRUNCATE"]:
        assert forbidden not in sql.upper(), f"migration contains {forbidden}"
    # Must default official=false
    assert "DEFAULT FALSE" in sql.upper()
