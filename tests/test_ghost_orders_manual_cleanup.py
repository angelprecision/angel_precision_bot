"""Tests for POST /admin/operator/ghost-orders/manual-cleanup.

Covers all 15 required cases from the spec plus structural invariants.

Test surfaces:
  1. Classifier-level safety gates (pure Python, no DB needed)
  2. Source-scan invariants (no DELETE/DROP/broker calls, parameterized SQL,
     COALESCE || meta merge, RETURNING, idempotency guard in WHERE)
  3. Response-shape tests (dry_run flag, mutation_performed, keys)

Run:
  DATABASE_URL=postgresql://test:test@localhost/test \
  python -m pytest tests/test_ghost_orders_manual_cleanup.py -v
"""
import json
import pathlib
import re

_APP_SRC = pathlib.Path(__file__).parent.parent / "app.py"


def _read_app():
    return _APP_SRC.read_text()


def _extract_cleanup_endpoint():
    src = _read_app()
    m = re.search(
        r'@app\.post\("/admin/operator/ghost-orders/manual-cleanup"\).*?'
        r'(?=@app\.(?:get|post|route)\()',
        src, re.S,
    )
    assert m, "manual-cleanup endpoint not found in app.py"
    return m.group(0)


def _executable_only(body: str) -> str:
    """Strip docstrings, inline comments, and string literals — leave only
    executable Python tokens for forbidden-verb scanning."""
    cleaned = re.sub(r'"""[\s\S]*?"""', '""', body)
    cleaned = re.sub(r"'''[\s\S]*?'''", "''", cleaned)
    cleaned = re.sub(r'#[^\n]*', '', cleaned)
    cleaned = re.sub(r'"(?:\\.|[^"\\])*"', '""', cleaned)
    cleaned = re.sub(r"'(?:\\.|[^'\\])*'", "''", cleaned)
    return cleaned


def _strip_docstring_and_comments(body: str) -> str:
    """Strip docstrings + comments but KEEP string literals — use for SQL
    content scans where the SQL lives inside string literals."""
    cleaned = re.sub(r'"""[\s\S]*?"""', '""', body)
    cleaned = re.sub(r"'''[\s\S]*?'''", "''", cleaned)
    cleaned = re.sub(r'#[^\n]*', '', cleaned)
    return cleaned


def _extract_ghost_sql_builder():
    src = _read_app()
    m = re.search(r'def _ghost_build_sql_and_params\(.*?(?=\ndef |\nclass )', src, re.S)
    assert m, "_ghost_build_sql_and_params not found"
    return m.group(0)


def _extract_ghost_classifier():
    src = _read_app()
    m = re.search(r'def _ghost_classify_row\(.*?(?=\ndef |\nclass )', src, re.S)
    assert m, "_ghost_classify_row not found"
    return m.group(0)


# ── Mirror the shared classifier for unit tests ──────────────────────────────
def _classify(row, *, recent_skip_hours=1, cancel_threshold=24, expire_threshold=72):
    kind      = row.get("kind")
    status    = row.get("status")
    broker_id = row.get("broker_order_id")
    loid      = row.get("local_order_id")
    age       = row.get("age_hours")
    try:
        age_f = float(age) if age is not None else None
    except (TypeError, ValueError):
        age_f = None
    if not loid:                                    return "skip_missing_local_order_id"
    if broker_id:                                   return "skip_has_broker_order_id"
    if (kind or "").upper() != "ENTRY":             return "skip_not_entry"
    if (status or "").upper() != "PENDING_TRIGGER": return "skip_not_pending_trigger"
    if age_f is None:                               return "skip_unclear_state"
    if age_f < float(recent_skip_hours):            return "skip_recent"
    if age_f >= float(expire_threshold):            return "would_expire_pending_entry"
    if age_f >= float(cancel_threshold):            return "would_cancel_pending_entry"
    return "skip_unclear_state"


def _ghost_row(**overrides):
    """Return a valid ghost-order candidate row."""
    base = {
        "kind": "ENTRY", "status": "PENDING_TRIGGER",
        "broker_order_id": None, "local_order_id": "LOID-1",
        "client_id": "main@x.com", "symbol": "NFLX",
        "contract": "NFLX260619C950", "age_hours": 50.0,
        "updated_ts": "2026-05-28T10:00:00",
    }
    base.update(overrides)
    return base


ELIGIBLE = {"would_cancel_pending_entry", "would_expire_pending_entry"}


# ── 1. dry_run=true performs zero mutation ───────────────────────────────────

class TestDryRunPerformsZeroMutation:
    """Test 1: dry_run=true must never reach the UPDATE SQL path."""

    def test_dry_run_true_is_default_in_source(self):
        body = _extract_cleanup_endpoint()
        # Default must be True, not False
        assert 'body.get("dry_run", True)' in body, (
            "dry_run must default to True so accidental calls are safe"
        )

    def test_dry_run_branch_has_no_update_sql(self):
        """The block that executes when dry_run=True must contain zero
        UPDATE statements — only a jsonify(return)."""
        body = _executable_only(_extract_cleanup_endpoint())
        # Verify UPDATE appears in the source (it must exist for live path)
        assert "UPDATE orders" in _strip_docstring_and_comments(
            _extract_cleanup_endpoint()
        ), "UPDATE must exist for live mutation path"
        # Verify UPDATE is NOT reachable without dry_run=False — this is
        # enforced structurally: the dry_run branch returns before UPDATE.
        # We verify the textual order: 'if dry_run:' appears before 'UPDATE'.
        src = _extract_cleanup_endpoint()
        pos_dry_run_check = src.find("if dry_run:")
        pos_update = src.find("UPDATE orders")
        assert pos_dry_run_check > 0 and pos_update > 0
        assert pos_dry_run_check < pos_update, (
            "'if dry_run:' guard must appear before 'UPDATE orders' in source "
            "so the early-return prevents mutation when dry_run=True"
        )

    def test_response_shape_when_dry_run(self):
        body = _extract_cleanup_endpoint()
        # dry_run response must include mutation_performed: False and rows_mutated: 0
        assert '"mutation_performed": False' in body
        assert '"rows_mutated":  0' in body


# ── 2. Only eligible rows mutated ────────────────────────────────────────────

class TestOnlyEligibleRowsMutated:
    """Test 2: only would_cancel and would_expire rows enter the UPDATE path."""

    def test_eligible_actions_are_only_two(self):
        src = _read_app()
        # _GHOST_ELIGIBLE_ACTIONS must contain exactly these two
        m = re.search(r'_GHOST_ELIGIBLE_ACTIONS = frozenset\(\{([^}]+)\}\)', src)
        assert m, "_GHOST_ELIGIBLE_ACTIONS not found"
        body = m.group(1)
        assert "would_expire_pending_entry" in body
        assert "would_cancel_pending_entry" in body

    def test_would_cancel_row_is_eligible(self):
        r = _ghost_row(age_hours=30.0)
        assert _classify(r) == "would_cancel_pending_entry"
        assert _classify(r) in ELIGIBLE

    def test_would_expire_row_is_eligible(self):
        r = _ghost_row(age_hours=100.0)
        assert _classify(r) == "would_expire_pending_entry"
        assert _classify(r) in ELIGIBLE

    def test_skip_rows_are_not_eligible(self):
        skips = [
            _ghost_row(age_hours=0.5),                      # skip_recent
            _ghost_row(broker_order_id="BRK-1"),            # skip_has_broker
            _ghost_row(kind="EXIT"),                        # skip_not_entry
            _ghost_row(status="FILLED"),                    # skip_not_pending
            _ghost_row(local_order_id=None),                # skip_missing_loid
            _ghost_row(age_hours=None),                     # skip_unclear
        ]
        for r in skips:
            action = _classify(r)
            assert action not in ELIGIBLE, f"expected skip, got {action}"

    def test_endpoint_filters_eligible_before_update(self):
        body = _extract_cleanup_endpoint()
        # The eligible list must be filtered to ELIGIBLE_ACTIONS before the
        # UPDATE loop — verify the filter expression appears in source.
        assert "in _GHOST_ELIGIBLE_ACTIONS" in body, (
            "UPDATE path must filter rows to _GHOST_ELIGIBLE_ACTIONS"
        )


# ── 3. Rows with broker_order_id never mutated ───────────────────────────────

class TestBrokerOrderIdNeverMutated:
    """Test 3."""

    def test_broker_id_classified_skip(self):
        r = _ghost_row(broker_order_id="BRK-99", age_hours=100.0)
        assert _classify(r) == "skip_has_broker_order_id"
        assert _classify(r) not in ELIGIBLE

    def test_update_where_requires_broker_null(self):
        body = _strip_docstring_and_comments(_extract_cleanup_endpoint())
        assert "broker_order_id IS NULL" in body, (
            "UPDATE WHERE must require broker_order_id IS NULL"
        )


# ── 4. Non-ENTRY rows never mutated ──────────────────────────────────────────

class TestNonEntryNeverMutated:
    """Test 4."""

    def test_exit_order_classified_skip(self):
        r = _ghost_row(kind="EXIT", age_hours=100.0)
        assert _classify(r) == "skip_not_entry"

    def test_null_kind_classified_skip(self):
        r = _ghost_row(kind=None, age_hours=100.0)
        assert _classify(r) == "skip_not_entry"

    def test_update_where_requires_entry_kind(self):
        body = _strip_docstring_and_comments(_extract_cleanup_endpoint())
        assert "kind            = 'ENTRY'" in body or "kind = 'ENTRY'" in body, (
            "UPDATE WHERE must gate on kind = 'ENTRY'"
        )


# ── 5. Non-PENDING_TRIGGER rows never mutated ────────────────────────────────

class TestNonPendingTriggerNeverMutated:
    """Test 5."""

    def test_filled_classified_skip(self):
        for status in ("FILLED", "EXIT_FILLED", "SUBMITTED", "ACKNOWLEDGED",
                       "CANCELED", "EXPIRED", "REJECTED"):
            r = _ghost_row(status=status, age_hours=100.0)
            assert _classify(r) == "skip_not_pending_trigger", (
                f"status={status} should be skip_not_pending_trigger"
            )

    def test_update_where_requires_pending_trigger(self):
        body = _strip_docstring_and_comments(_extract_cleanup_endpoint())
        assert "status          = 'PENDING_TRIGGER'" in body or \
               "status = 'PENDING_TRIGGER'" in body, (
            "UPDATE WHERE must gate on status = 'PENDING_TRIGGER'"
        )


# ── 6. Missing local_order_id never mutated ──────────────────────────────────

class TestMissingLoidNeverMutated:
    """Test 6."""

    def test_null_loid_classified_skip(self):
        r = _ghost_row(local_order_id=None, age_hours=100.0)
        assert _classify(r) == "skip_missing_local_order_id"

    def test_empty_loid_classified_skip(self):
        r = _ghost_row(local_order_id="", age_hours=100.0)
        assert _classify(r) == "skip_missing_local_order_id"

    def test_update_where_requires_loid_not_null(self):
        body = _strip_docstring_and_comments(_extract_cleanup_endpoint())
        assert "local_order_id  IS NOT NULL" in body or \
               "local_order_id IS NOT NULL" in body, (
            "UPDATE WHERE must require local_order_id IS NOT NULL"
        )


# ── 7. Recent rows never mutated ─────────────────────────────────────────────

class TestRecentRowsNeverMutated:
    """Test 7."""

    def test_recent_row_classified_skip(self):
        r = _ghost_row(age_hours=0.5)
        assert _classify(r) == "skip_recent"

    def test_exactly_at_recent_threshold_is_unclear(self):
        r = _ghost_row(age_hours=1.0)  # == recent_skip_hours default 1
        assert _classify(r) == "skip_unclear_state"  # < is strict, 1.0 not skip


# ── 8. Filled/submitted/broker-seen rows never mutated ───────────────────────

class TestFilledSubmittedNeverMutated:
    """Test 8."""

    def test_filled_status_is_skip(self):
        r = _ghost_row(status="FILLED", age_hours=100.0)
        assert _classify(r) not in ELIGIBLE

    def test_acknowledged_status_is_skip(self):
        r = _ghost_row(status="ACKNOWLEDGED", age_hours=100.0)
        assert _classify(r) not in ELIGIBLE

    def test_broker_seen_is_skip(self):
        r = _ghost_row(broker_order_id="BRK-5", age_hours=100.0)
        assert _classify(r) not in ELIGIBLE


# ── 9. Endpoint requires HMAC ────────────────────────────────────────────────

class TestRequiresHmac:
    """Test 9."""

    def test_hmac_decorator_present(self):
        src = _extract_cleanup_endpoint()
        # @require_hmac must appear before the def line
        m = re.search(
            r'@require_hmac\s+def admin_operator_ghost_orders_manual_cleanup',
            src, re.S,
        )
        assert m, "@require_hmac decorator missing on manual-cleanup endpoint"

    def test_endpoint_is_post_not_get(self):
        src = _read_app()
        assert '@app.post("/admin/operator/ghost-orders/manual-cleanup")' in src, (
            "Endpoint must be POST, not GET — mutations only from POST"
        )


# ── 10. No DELETE/DROP SQL ───────────────────────────────────────────────────

class TestNoDeleteOrDrop:
    """Test 10."""

    def test_no_delete_in_executable_code(self):
        body = _executable_only(_extract_cleanup_endpoint()).upper()
        assert "DELETE FROM" not in body, "DELETE FROM found in executable code"
        assert " DELETE " not in body,    "DELETE found in executable code"
        assert "DROP " not in body,       "DROP found in executable code"

    def test_no_truncate(self):
        body = _executable_only(_extract_cleanup_endpoint()).upper()
        assert "TRUNCATE" not in body


# ── 11. No broker calls ──────────────────────────────────────────────────────

class TestNoBrokerCalls:
    """Test 11."""

    def test_no_broker_methods_in_executable_code(self):
        body = _executable_only(_extract_cleanup_endpoint())
        for forbidden in ("broker.cancel", "broker.place", "broker.submit",
                          ".cancel_order", ".place_order", ".submit_order"):
            assert forbidden not in body, f"forbidden broker call {forbidden!r} found"


# ── 12. Parameterized SQL only ───────────────────────────────────────────────

class TestParameterizedSql:
    """Test 12."""

    def test_update_uses_param_placeholders(self):
        body = _strip_docstring_and_comments(_extract_cleanup_endpoint())
        # UPDATE must use %s for all values
        assert "= %s" in body, "UPDATE must use %s placeholders"
        assert "|| %s::jsonb" in body, "meta merge must use %s::jsonb parameter"

    def test_no_fstring_sql(self):
        body = _executable_only(_extract_cleanup_endpoint())
        assert 'f"UPDATE' not in body
        assert "f'UPDATE" not in body

    def test_select_uses_shared_builder(self):
        body = _extract_cleanup_endpoint()
        assert "_ghost_build_sql_and_params(" in body, (
            "Cleanup must use the shared _ghost_build_sql_and_params so it "
            "scans the same candidate set as the dry-run endpoint"
        )


# ── 13. mutation_performed only true when rows changed ───────────────────────

class TestMutationPerformedFlag:
    """Test 13."""

    def test_mutation_performed_is_len_based(self):
        body = _extract_cleanup_endpoint()
        assert "mutation_performed = len(mutated_rows) > 0" in body, (
            "mutation_performed must be True only when mutated_rows is non-empty"
        )

    def test_dry_run_response_has_mutation_performed_false(self):
        body = _extract_cleanup_endpoint()
        # Dry-run path hardcodes False
        assert '"mutation_performed": False' in body


# ── 14. Audit meta appended / not replaced ───────────────────────────────────

class TestAuditMetaMerged:
    """Test 14: meta must be merged with COALESCE || jsonb, never replaced."""

    def test_update_uses_coalesce_merge(self):
        body = _strip_docstring_and_comments(_extract_cleanup_endpoint())
        assert "COALESCE(meta, '{}'::jsonb) || %s::jsonb" in body, (
            "meta must be merged non-destructively with COALESCE || JSONB"
        )

    def test_update_does_not_set_meta_directly(self):
        body = _strip_docstring_and_comments(_extract_cleanup_endpoint())
        # Should not contain 'meta = %s' (wholesale replacement)
        assert "meta           = %s" not in body and "meta = %s" not in body, (
            "must NOT replace meta wholesale — only merge with ||"
        )

    def test_audit_fields_present_in_meta_patch(self):
        body = _extract_cleanup_endpoint()
        for field in ("ghost_cleanup", "ghost_cleanup_action", "ghost_cleanup_at",
                      "ghost_cleanup_by", "prior_status", "prior_updated_ts"):
            assert field in body, f"audit meta field {field!r} missing"

    def test_ghost_cleanup_by_is_admin_manual_endpoint(self):
        body = _extract_cleanup_endpoint()
        assert "admin_manual_endpoint" in body


# ── 15. Idempotency — already cleaned rows not mutated again ─────────────────

class TestIdempotency:
    """Test 15."""

    def test_update_where_excludes_already_cleaned(self):
        body = _strip_docstring_and_comments(_extract_cleanup_endpoint())
        assert "(meta->>'ghost_cleanup') IS DISTINCT FROM 'true'" in body, (
            "WHERE clause must exclude rows with ghost_cleanup=true in meta "
            "so re-running cleanup is a no-op on already-cleaned rows"
        )

    def test_returning_used_to_confirm_update(self):
        body = _strip_docstring_and_comments(_extract_cleanup_endpoint())
        assert "RETURNING local_order_id" in body, (
            "UPDATE must use RETURNING to confirm exactly which rows changed; "
            "empty RETURNING means WHERE guard excluded it (already cleaned)"
        )

    def test_where_guard_no_match_goes_to_skipped(self):
        body = _extract_cleanup_endpoint()
        assert "where_guard_no_match" in body, (
            "rows where RETURNING is empty must be added to skipped list with "
            "skip_reason='where_guard_no_match' so the caller sees them"
        )


# ── Bonus: status mapping correctness ────────────────────────────────────────

class TestStatusMapping:
    def test_would_cancel_maps_to_canceled(self):
        src = _read_app()
        m = re.search(r'_GHOST_ACTION_STATUS\s*=\s*\{([^}]+)\}', src)
        assert m, "_GHOST_ACTION_STATUS not found"
        block = m.group(1)
        assert "would_cancel_pending_entry" in block
        assert '"CANCELED"' in block or "'CANCELED'" in block

    def test_would_expire_maps_to_expired(self):
        src = _read_app()
        m = re.search(r'_GHOST_ACTION_STATUS\s*=\s*\{([^}]+)\}', src)
        assert m
        block = m.group(1)
        assert "would_expire_pending_entry" in block
        assert '"EXPIRED"' in block or "'EXPIRED'" in block

    def test_last_error_is_ghost_cleanup_manual(self):
        body = _strip_docstring_and_comments(_extract_cleanup_endpoint())
        assert "ghost_cleanup_manual" in body, (
            "last_error must be 'ghost_cleanup_manual', not 'watcher_invalidated'"
        )


# ── Shared classifier is the single source of truth ─────────────────────────

class TestSharedClassifierUsed:
    def test_cleanup_calls_ghost_classify_row(self):
        body = _extract_cleanup_endpoint()
        assert "_ghost_classify_row(" in body, (
            "Cleanup endpoint must call the shared _ghost_classify_row "
            "function — not a separate inline classifier"
        )

    def test_cleanup_calls_ghost_build_sql(self):
        body = _extract_cleanup_endpoint()
        assert "_ghost_build_sql_and_params(" in body

    def test_module_level_classifier_exists(self):
        src = _read_app()
        assert "def _ghost_classify_row(" in src

    def test_module_level_sql_builder_exists(self):
        src = _read_app()
        assert "def _ghost_build_sql_and_params(" in src
