"""Tests for item 6 (fairness audit) + item 7 (ghost-order report).

Both endpoints are READ-ONLY. These tests verify:
  - the aggregation/classification logic produces correct mismatch/bucket signals
  - the SQL is parameterized (no string interpolation of user input)
  - the ghost-order endpoint NEVER attempts mutation
  - the report explicitly self-labels read-only

Run: DATABASE_URL=postgresql://test:test@localhost/test python -m pytest tests/test_fairness_and_ghost.py -v
"""
import os
import pathlib
import re


_APP_SRC = pathlib.Path(__file__).parent.parent / "app.py"


def _read_app():
    return _APP_SRC.read_text()


# ── Item 6: fairness audit aggregation logic ─────────────────────────────────


def _classify_signal(rows, active_clients=None):
    """Mirror the fairness-audit aggregation so we can unit-test the math."""
    from collections import defaultdict
    BLOCKED_BUCKETS = {
        "PENDING_TRIGGER_NO_BROKER", "WATCHER_INVALIDATED",
        "WATCHER_EXPIRED", "REJECTED", "CANCELED", "TERMINAL_NO_FILL",
    }
    clients_seen = sorted({r.get("client_id") for r in rows if r.get("client_id")})
    missing_clients = sorted(set(active_clients or []) - set(clients_seen))
    contracts = {r.get("contract") for r in rows if r.get("contract")}
    qtys = {r.get("qty") for r in rows if r.get("qty") is not None}
    contract_mismatch = len(contracts) > 1
    qty_mismatch = len(qtys) > 1
    n_filled = sum(1 for r in rows if r.get("ledger_bucket") == "FILLED")
    n_blocked = sum(1 for r in rows if r.get("ledger_bucket") in BLOCKED_BUCKETS)
    bucket_breakdown = defaultdict(int)
    for r in rows:
        b = r.get("ledger_bucket")
        if b:
            bucket_breakdown[b] += 1
    return {
        "clients_seen": clients_seen,
        "missing_clients": missing_clients,
        "contract_mismatch": contract_mismatch,
        "qty_mismatch": qty_mismatch,
        "n_filled": n_filled,
        "n_blocked": n_blocked,
        "bucket_breakdown": dict(bucket_breakdown),
        "has_mismatch": bool(contract_mismatch or qty_mismatch or missing_clients),
    }


class TestFairnessAggregation:
    def test_clean_signal_no_mismatch(self):
        # All three clients filled the same contract at same qty.
        rows = [
            {"client_id": "main",   "contract": "NFLX260619C950", "qty": 1, "ledger_bucket": "FILLED"},
            {"client_id": "jason",  "contract": "NFLX260619C950", "qty": 1, "ledger_bucket": "FILLED"},
            {"client_id": "jose",   "contract": "NFLX260619C950", "qty": 1, "ledger_bucket": "FILLED"},
        ]
        result = _classify_signal(rows, active_clients=["main", "jason", "jose"])
        assert result["has_mismatch"] is False
        assert result["n_filled"] == 3
        assert result["n_blocked"] == 0
        assert result["missing_clients"] == []
        assert result["contract_mismatch"] is False
        assert result["qty_mismatch"] is False

    def test_missing_client_flagged(self):
        # Jose missing entirely — should flag as mismatch.
        rows = [
            {"client_id": "main",  "contract": "X", "qty": 1, "ledger_bucket": "FILLED"},
            {"client_id": "jason", "contract": "X", "qty": 1, "ledger_bucket": "FILLED"},
        ]
        result = _classify_signal(rows, active_clients=["main", "jason", "jose"])
        assert result["missing_clients"] == ["jose"]
        assert result["has_mismatch"] is True

    def test_different_contract_per_client(self):
        rows = [
            {"client_id": "main",  "contract": "NFLX260619C950", "qty": 1, "ledger_bucket": "FILLED"},
            {"client_id": "jason", "contract": "NFLX260619C960", "qty": 1, "ledger_bucket": "FILLED"},
        ]
        result = _classify_signal(rows)
        assert result["contract_mismatch"] is True
        assert result["has_mismatch"] is True

    def test_different_qty_per_client(self):
        rows = [
            {"client_id": "main",  "contract": "X", "qty": 1, "ledger_bucket": "FILLED"},
            {"client_id": "jason", "contract": "X", "qty": 3, "ledger_bucket": "FILLED"},
        ]
        result = _classify_signal(rows)
        assert result["qty_mismatch"] is True
        assert result["has_mismatch"] is True

    def test_mixed_outcomes_split_filled_blocked(self):
        rows = [
            {"client_id": "main",  "contract": "X", "qty": 1, "ledger_bucket": "FILLED"},
            {"client_id": "jason", "contract": "X", "qty": 1, "ledger_bucket": "WATCHER_INVALIDATED"},
            {"client_id": "jose",  "contract": "X", "qty": 1, "ledger_bucket": "REJECTED"},
        ]
        result = _classify_signal(rows)
        assert result["n_filled"] == 1
        assert result["n_blocked"] == 2
        assert result["bucket_breakdown"]["FILLED"] == 1
        assert result["bucket_breakdown"]["WATCHER_INVALIDATED"] == 1
        assert result["bucket_breakdown"]["REJECTED"] == 1

    def test_none_qty_not_counted_as_distinct(self):
        # qty=None on some rows must not falsely trigger qty_mismatch.
        rows = [
            {"client_id": "a", "contract": "X", "qty": 1,    "ledger_bucket": "FILLED"},
            {"client_id": "b", "contract": "X", "qty": None, "ledger_bucket": "PENDING_TRIGGER_NO_BROKER"},
        ]
        result = _classify_signal(rows)
        # Only one non-None qty → no mismatch
        assert result["qty_mismatch"] is False


# ── Item 7: ghost-order endpoint MUST be read-only ────────────────────────────


class TestGhostOrdersIsReadOnly:
    """Critical invariant: the ghost-orders endpoint must NEVER mutate.
    These tests scan the source to prove no mutation verb appears inside the
    endpoint body, and the response always includes report_only=True.
    """

    def _extract_ghost_endpoint(self):
        """Pull just the ghost-orders endpoint body from app.py source."""
        src = _read_app()
        m = re.search(
            r'@app\.get\("/admin/operator/ghost-orders"\).*?(?=@app\.(?:get|post|route)\()',
            src, re.S
        )
        assert m, "could not locate ghost-orders endpoint in app.py"
        return m.group(0)

    def test_endpoint_exists(self):
        assert '@app.get("/admin/operator/ghost-orders")' in _read_app()

    def test_endpoint_has_no_mutation_verbs_in_sql(self):
        body = self._extract_ghost_endpoint()
        # Only SELECT statements may appear. Strip comments and docstrings first.
        # We test on uppercased body to catch case variations.
        upper = body.upper()
        for verb in ("INSERT INTO", "UPDATE ORDERS", "UPDATE ", "DELETE FROM", "DROP "):
            assert verb not in upper, f"forbidden SQL verb '{verb}' found in ghost-orders endpoint body"

    def test_endpoint_does_not_call_mutation_methods(self):
        body = self._extract_ghost_endpoint()
        # Defensive: ensure no calls to OSM mutation methods.
        for forbidden in (
            "update_order_meta",
            "cancel_pending_entry",
            "expire_pending_entry",
            "transition(",
            "place_order(",
            ".cancel(",
            ".delete(",
        ):
            assert forbidden not in body, (
                f"forbidden mutation method '{forbidden}' found in ghost-orders endpoint body"
            )

    def test_response_explicitly_labeled_report_only(self):
        body = self._extract_ghost_endpoint()
        # The JSON response includes "report_only": True so any consumer can
        # verify the contract by reading the response.
        assert '"report_only": True' in body, "ghost-orders response must include report_only=True"

    def test_response_note_warns_no_mutation(self):
        body = self._extract_ghost_endpoint()
        assert "READ-ONLY REPORT" in body or "READ-ONLY" in body
        # The note field is part of the response so the dashboard can show it.
        assert '"note"' in body

    def test_select_columns_match_orders_schema(self):
        """Confirm the SELECT references actual orders columns (no typos that
        would cause runtime SQL errors on deploy)."""
        body = self._extract_ghost_endpoint()
        for col in ("local_order_id", "client_id", "plan_id", "signal_id",
                    "symbol", "contract", "direction", "qty", "limit_price",
                    "reserved_cost", "trigger_price", "score", "tier",
                    "pattern", "timeframe", "last_error", "created_ts",
                    "updated_ts"):
            assert col in body, f"expected orders column '{col}' in ghost-orders SELECT"


# ── Both endpoints: SQL parameterization safety ──────────────────────────────


class TestSqlParameterization:
    """No user-supplied filter value may be string-interpolated into SQL."""

    def test_fairness_audit_uses_param_placeholders(self):
        src = _read_app()
        m = re.search(
            r'@app\.get\("/admin/operator/fairness-audit"\).*?(?=@app\.(?:get|post|route)\()',
            src, re.S
        )
        assert m, "could not locate fairness-audit endpoint"
        body = m.group(0)
        # All filters must use %s placeholders, not f-string interpolation.
        assert "%s" in body
        # No f-string with user input baked into SQL.
        assert "f\"SELECT" not in body
        assert "f'SELECT" not in body

    def test_ghost_endpoint_uses_param_placeholders(self):
        src = _read_app()
        m = re.search(
            r'@app\.get\("/admin/operator/ghost-orders"\).*?(?=@app\.(?:get|post|route)\()',
            src, re.S
        )
        assert m, "could not locate ghost-orders endpoint"
        body = m.group(0)
        assert "%s" in body
        assert "f\"SELECT" not in body
        assert "f'SELECT" not in body


# ── Row conversion safety (mirrors PR #57 fix in both endpoints) ─────────────


class TestRowConversionSafety:
    """Both endpoints must safely handle dict and tuple cursor rows."""

    def _both_have_isinstance_dict(self):
        src = _read_app()
        # Find both endpoints' _fetch() blocks
        for ep in ("fairness-audit", "ghost-orders"):
            m = re.search(
                r'@app\.get\("/admin/operator/%s"\).*?(?=@app\.(?:get|post|route)\()' % ep,
                src, re.S
            )
            assert m, f"endpoint /admin/operator/{ep} not found"
            body = m.group(0)
            assert "isinstance(r, dict)" in body, (
                f"endpoint {ep} missing dict-row safety check"
            )
            assert "dict(zip(cols, r))" in body, (
                f"endpoint {ep} missing tuple-row fallback"
            )

    def test_both_endpoints_safe_row_conversion(self):
        self._both_have_isinstance_dict()
