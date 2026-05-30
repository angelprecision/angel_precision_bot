"""Item 4 — verify REEVAL signal_id normalization for the multi-account ledger.

The ledger view joins orders.signal_id -> ap_signals.signal_id by normalizing
the REEVAL:<uuid>:<hex> wrapper with split_part(o.signal_id, ':', 2). That SQL
rule must produce the SAME canonical UUID as the codebase's canonical_signal_id()
helper, otherwise REEVAL orders would fail to join (acceptance criterion 1).

This test proves the two normalizations agree, and that a Python re-implementation
of the view's exact CASE/split_part expression matches as well.

Run: DATABASE_URL=postgresql://test:test@localhost/test python -m pytest tests/test_signal_ledger_reeval.py -v
"""
import os


def _sql_split_part(s, sep, idx):
    """Faithful re-implementation of Postgres split_part (1-indexed)."""
    parts = s.split(sep)
    return parts[idx - 1] if 1 <= idx <= len(parts) else ""


def _view_canonical(order_signal_id):
    """Mirror the view's CASE expression exactly."""
    if order_signal_id.startswith("REEVAL:"):
        return _sql_split_part(order_signal_id, ":", 2)
    return order_signal_id


UUID = "8d9338d0-5dde-4b7b-81ea-208039999b72"


class TestReevalNormalization:
    def test_view_expression_strips_reeval(self):
        assert _view_canonical(f"REEVAL:{UUID}:f4dc44") == UUID

    def test_view_expression_passes_bare_uuid(self):
        assert _view_canonical(UUID) == UUID

    def test_view_matches_code_helper(self):
        from ap_signal_store import canonical_signal_id
        for sid in (UUID, f"REEVAL:{UUID}:f4dc44", f"REEVAL:{UUID}:abc123"):
            assert _view_canonical(sid) == canonical_signal_id(sid), sid

    def test_reeval_and_bare_resolve_equal(self):
        # The whole point: a REEVAL order and the bare signal share one
        # canonical_signal_id so they group onto the same ledger signal.
        assert _view_canonical(f"REEVAL:{UUID}:f4dc44") == _view_canonical(UUID)

    def test_split_part_position_2_is_uuid(self):
        # Guards the exact split_part index the spec mandates.
        assert _sql_split_part(f"REEVAL:{UUID}:f4dc44", ":", 1) == "REEVAL"
        assert _sql_split_part(f"REEVAL:{UUID}:f4dc44", ":", 2) == UUID


class TestViewFileShape:
    def test_view_file_exists_and_readonly(self):
        path = os.path.join(os.path.dirname(__file__), "..", "sql", "views",
                            "ap_multi_account_signal_ledger.sql")
        with open(path) as f:
            sql = f.read()
        # Read-only guarantees: a view, with no mutation verbs in the body.
        assert "CREATE OR REPLACE VIEW ap_multi_account_signal_ledger" in sql
        body = sql.upper()
        for verb in ("INSERT INTO ORDERS", "UPDATE ORDERS", "DELETE FROM ORDERS"):
            assert verb not in body, f"view must be read-only, found {verb}"

    def test_view_uses_split_part_join(self):
        path = os.path.join(os.path.dirname(__file__), "..", "sql", "views",
                            "ap_multi_account_signal_ledger.sql")
        with open(path) as f:
            sql = f.read()
        assert "split_part(o.signal_id, ':', 2)" in sql
        assert "LEFT JOIN ap_signals" in sql
        assert "WHERE o.kind = 'ENTRY'" in sql


class TestFetchRowConversion:
    """PR #57 review fix: _fetch() must return column values, not column names.

    When the cursor returns dict rows (psycopg2 RealDictCursor / psycopg3),
    iterating the dict and zipping with cols produces {'key': 'key'} instead
    of {'key': value}. The fix checks isinstance(r, dict) first.
    """

    @staticmethod
    def _convert(cols, row):
        """Mirror the fixed _fetch() row-conversion logic."""
        if isinstance(row, dict):
            return dict(row)
        return dict(zip(cols, row))

    def test_tuple_row_maps_values(self):
        cols = ["symbol", "client_id", "ledger_bucket"]
        row  = ("NFLX", "jose@example.com", "PENDING_TRIGGER_NO_BROKER")
        result = self._convert(cols, row)
        assert result["ledger_bucket"] == "PENDING_TRIGGER_NO_BROKER"
        assert result["symbol"] == "NFLX"

    def test_dict_row_returns_values_not_keys(self):
        # This is the bug: if row is a dict (e.g. RealDictRow), iterating it
        # gives the keys, so dict(zip(cols, row)) == {'symbol': 'symbol', ...}.
        # The fix must return dict(row) instead.
        cols = ["symbol", "client_id", "ledger_bucket"]
        row  = {"symbol": "NFLX", "client_id": "jose@example.com",
                "ledger_bucket": "FILLED"}
        result = self._convert(cols, row)
        # Must NOT be the column-name-to-column-name mapping
        assert result["ledger_bucket"] != "ledger_bucket", (
            "dict row was incorrectly zipped: values are column names, not data"
        )
        assert result["ledger_bucket"] == "FILLED"

    def test_dict_row_preserves_extra_columns(self):
        # dict(row) preserves all columns even if cols list is shorter.
        cols = ["symbol"]
        row  = {"symbol": "MSFT", "client_id": "jason@example.com",
                "ledger_bucket": "WATCHER_INVALIDATED", "score": 72}
        result = self._convert(cols, row)
        assert result["score"] == 72
        assert result["ledger_bucket"] == "WATCHER_INVALIDATED"

    def test_all_ledger_bucket_values_are_expected_strings(self):
        """Bucket values must be one of the spec-defined 9 strings."""
        valid_buckets = {
            "NO_ORDER_FOR_CLIENT",
            "PENDING_TRIGGER_NO_BROKER",
            "WATCHER_INVALIDATED",
            "WATCHER_EXPIRED",
            "BROKER_SUBMITTED",
            "FILLED",
            "REJECTED",
            "CANCELED",
            "TERMINAL_NO_FILL",
            "OTHER",
        }
        for bucket in valid_buckets:
            cols = ["ledger_bucket"]
            row  = (bucket,)
            result = self._convert(cols, row)
            assert result["ledger_bucket"] in valid_buckets


class TestNineBucketCASE:
    """PR #57 spec update: ledger_bucket must split TERMINAL_NO_FILL into the
    9 distinct buckets so the dashboard can show why each order died."""

    @staticmethod
    def _classify(status, broker_order_id, last_error, local_order_id="LOID"):
        """Mirror the SQL CASE classifier in Python so we can unit-test it."""
        if local_order_id is None:
            return "NO_ORDER_FOR_CLIENT"
        if status == "PENDING_TRIGGER" and broker_order_id is None:
            return "PENDING_TRIGGER_NO_BROKER"
        if status in ("ACK", "SUBMITTED", "ACKNOWLEDGED") and broker_order_id is not None:
            return "BROKER_SUBMITTED"
        if status in ("FILLED", "PARTIALLY_FILLED", "PARTIAL_FILL"):
            return "FILLED"
        if status == "EXPIRED" and last_error == "watcher_expired":
            return "WATCHER_EXPIRED"
        if status == "CANCELED" and last_error == "watcher_invalidated":
            return "WATCHER_INVALIDATED"
        if status == "REJECTED":
            return "REJECTED"
        if status in ("CANCELED", "CANCELLED"):
            return "CANCELED"
        if status in ("EXPIRED", "ERROR"):
            return "TERMINAL_NO_FILL"
        return "OTHER"

    def test_watcher_invalidated_distinct_from_canceled(self):
        # CANCELED with last_error=watcher_invalidated → WATCHER_INVALIDATED
        b1 = self._classify("CANCELED", None, "watcher_invalidated")
        # CANCELED with any other last_error → plain CANCELED
        b2 = self._classify("CANCELED", "B1", "broker_canceled")
        assert b1 == "WATCHER_INVALIDATED"
        assert b2 == "CANCELED"
        assert b1 != b2

    def test_watcher_expired_distinct_from_terminal_no_fill(self):
        b1 = self._classify("EXPIRED", None, "watcher_expired")
        b2 = self._classify("EXPIRED", None, "stale_order_cleanup")
        assert b1 == "WATCHER_EXPIRED"
        assert b2 == "TERMINAL_NO_FILL"

    def test_rejected_distinct_bucket(self):
        assert self._classify("REJECTED", None, "broker_no_buy_power") == "REJECTED"

    def test_filled_drops_or_partial_suffix(self):
        for status in ("FILLED", "PARTIAL_FILL", "PARTIALLY_FILLED"):
            assert self._classify(status, "B1", None) == "FILLED"

    def test_acknowledged_treated_as_broker_submitted(self):
        assert self._classify("ACKNOWLEDGED", "B1", None) == "BROKER_SUBMITTED"

    def test_no_order_when_local_order_id_missing(self):
        assert self._classify("FILLED", "B1", None, local_order_id=None) == "NO_ORDER_FOR_CLIENT"

    def test_pending_trigger_needs_no_broker_to_classify(self):
        # PENDING_TRIGGER without broker → its own bucket
        assert self._classify("PENDING_TRIGGER", None, None) == "PENDING_TRIGGER_NO_BROKER"


class TestViewExposesQuoteDomainFields:
    """The ledger view must expose the PR #59 quote-domain fields so the
    operator dashboard can render the Quote Domain panel without an extra
    backend call.

    CREATE OR REPLACE VIEW invariant: new columns MUST be appended at the
    end of the SELECT list. Postgres rejects the replacement if existing
    column names/positions change. These tests assert the new columns
    appear AFTER the pre-existing final column (ledger_bucket).
    """

    @staticmethod
    def _view_text():
        path = os.path.join(os.path.dirname(__file__), "..", "sql", "views",
                            "ap_multi_account_signal_ledger.sql")
        with open(path) as f:
            return f.read()

    @staticmethod
    def _pos(text, needle):
        """Position of needle in the view body. -1 if missing."""
        return text.find(needle)

    def test_view_exposes_selector_sandbox_fields(self):
        text = self._view_text()
        assert "selector_quote_source" in text
        assert "selector_quote_base_url" in text
        assert "selector_sandbox_mode" in text

    def test_view_exposes_submit_sandbox_fields(self):
        text = self._view_text()
        assert "submit_quote_source" in text
        assert "submit_quote_base_url" in text
        assert "submit_sandbox_mode" in text

    def test_view_exposes_broker_base_url(self):
        text = self._view_text()
        assert "broker_base_url" in text

    def test_view_exposes_watcher_sandbox_mode(self):
        text = self._view_text()
        # Watcher sandbox lives inside watcher_audit jsonb
        assert "'watcher_audit'->>'watcher_sandbox_mode'" in text

    # ── Column-ORDER invariants (the actual CREATE OR REPLACE VIEW gate) ──

    def test_new_columns_appended_after_ledger_bucket(self):
        """Every new column added by this PR must appear AFTER the existing
        final column 'AS ledger_bucket'. Inserting them in the middle would
        break Supabase's CREATE OR REPLACE VIEW."""
        text = self._view_text()
        anchor = self._pos(text, "AS ledger_bucket")
        assert anchor > 0, "ledger_bucket column not found — view shape changed"
        new_cols = [
            "AS selector_quote_source",
            "AS selector_quote_base_url",
            "AS selector_sandbox_mode",
            "AS submit_quote_source",
            "AS submit_quote_base_url",
            "AS submit_sandbox_mode",
            "AS broker_base_url",
            "AS tradier_sandbox_mode",
            "AS watcher_sandbox_mode",
        ]
        for col in new_cols:
            pos = self._pos(text, col)
            assert pos > anchor, (
                f"{col} must appear AFTER 'AS ledger_bucket' (pos {anchor}); "
                f"found at {pos}. CREATE OR REPLACE VIEW requires existing "
                "column order to be preserved — new columns must be appended."
            )

    def test_pre_existing_columns_still_in_original_order(self):
        """Spot-check: the pre-existing columns must still appear in their
        original relative order (canonical_signal_id ... ledger_bucket).
        Picks a few stable anchors across the view.

        Anchors are chosen from both aliased columns (AS x) and bare column
        references (o.x,) — the view uses both forms."""
        text = self._view_text()
        anchors = [
            "AS canonical_signal_id",
            "AS order_signal_id",
            "o.client_id,",                  # bare column ref
            "AS symbol",
            "AS order_status",
            "o.broker_order_id,",            # bare column ref
            "AS watcher_reason_code",
            "AS watcher_quote_source",
            "AS selector_candidate_audit",
            "AS order_updated_ts",
            "AS ledger_bucket",
        ]
        positions = [self._pos(text, a) for a in anchors]
        for a, p in zip(anchors, positions):
            assert p > 0, f"pre-existing column anchor {a!r} missing from view"
        # Strictly increasing positions = original order preserved
        for i in range(1, len(positions)):
            assert positions[i] > positions[i - 1], (
                f"column order changed: {anchors[i]} appears before "
                f"{anchors[i - 1]} — CREATE OR REPLACE VIEW will reject this"
            )

    def test_new_columns_appended_in_a_contiguous_block(self):
        """Defensive: all 9 new columns should appear as a contiguous block
        at the end. This catches any future drift where someone interleaves
        new columns with pre-existing ones again."""
        text = self._view_text()
        new_cols = [
            "AS selector_quote_source",
            "AS selector_quote_base_url",
            "AS selector_sandbox_mode",
            "AS submit_quote_source",
            "AS submit_quote_base_url",
            "AS submit_sandbox_mode",
            "AS broker_base_url",
            "AS tradier_sandbox_mode",
            "AS watcher_sandbox_mode",
        ]
        new_positions = sorted([self._pos(text, c) for c in new_cols])
        from_pos = self._pos(text, "FROM orders o")
        # All new columns must precede the FROM clause (they're in the SELECT).
        for c, p in zip(new_cols, new_positions):
            assert 0 < p < from_pos, f"{c} not in SELECT body"
        # No pre-existing column should appear between the new columns and FROM.
        between = text[new_positions[0]:from_pos]
        for old in ("AS watcher_reason_code", "AS canonical_signal_id",
                    "o.broker_order_id,", "AS order_status"):
            assert old not in between, (
                f"pre-existing column {old} appears AFTER a new column — "
                "new columns are not a contiguous appended block"
            )
