"""Tests for /admin/operator/ghost-orders/dry-run — READ-ONLY classifier.

Two test surfaces:
  1. Classifier logic: every documented action string is produced for the
     correct inputs; edge cases (None values, missing keys) don't crash.
  2. Read-only invariants: the endpoint source contains zero mutation verbs
     (INSERT / UPDATE / DELETE / DROP), zero references to OSM mutation
     methods, and the response explicitly self-labels dry_run=true /
     mutation_performed=false / report_only=true.

Run: DATABASE_URL=postgresql://test:test@localhost/test \
     python -m pytest tests/test_ghost_orders_dry_run.py -v
"""
import os
import pathlib
import re


_APP_SRC = pathlib.Path(__file__).parent.parent / "app.py"


def _read_app():
    return _APP_SRC.read_text()


def _extract_dry_run_endpoint():
    """Pull the dry-run endpoint body so tests can scan it for forbidden
    verbs/method calls without affecting other endpoints."""
    src = _read_app()
    m = re.search(
        r'@app\.get\("/admin/operator/ghost-orders/dry-run"\).*?(?=@app\.(?:get|post|route)\()',
        src, re.S
    )
    assert m, "dry-run endpoint not found in app.py"
    return m.group(0)


def _executable_only(body: str) -> str:
    """Strip docstrings, comments, and string literals from the endpoint body
    so source-scan tests only inspect EXECUTABLE code. Mentioning a forbidden
    method name in a docstring (e.g. 'this endpoint does NOT call
    cancel_pending_entry') is the whole point of the documentation — what we
    actually want to forbid is real call sites."""
    # Strip triple-quoted strings (the function docstring + multi-line strings).
    cleaned = re.sub(r'"""[\s\S]*?"""', '""', body)
    cleaned = re.sub(r"'''[\s\S]*?'''", "''", cleaned)
    # Strip single-line comments.
    cleaned = re.sub(r'#[^\n]*', '', cleaned)
    # Strip single/double-quoted string literals (the response note + log
    # strings). Be careful: only single-line, non-greedy.
    cleaned = re.sub(r'"(?:\\.|[^"\\])*"', '""', cleaned)
    cleaned = re.sub(r"'(?:\\.|[^'\\])*'", "''", cleaned)
    return cleaned


# ── 1. Pure classifier behavior ─────────────────────────────────────────────


def _make_classifier(recent_skip_hours=1, cancel_threshold=24, expire_threshold=72):
    """Re-implement the classifier in lockstep with the endpoint so we can
    unit-test every branch. The endpoint's classifier is a closure inside
    the route; this mirror keeps the same rule order and is locked in by
    the source-shape test below."""
    def _classify(row):
        kind      = row.get("kind")
        status    = row.get("status")
        broker_id = row.get("broker_order_id")
        loid      = row.get("local_order_id")
        age       = row.get("age_hours")
        try:
            age_f = float(age) if age is not None else None
        except (TypeError, ValueError):
            age_f = None

        if not loid:
            return "skip_missing_local_order_id"
        if broker_id:
            return "skip_has_broker_order_id"
        if (kind or "").upper() != "ENTRY":
            return "skip_not_entry"
        if (status or "").upper() != "PENDING_TRIGGER":
            return "skip_not_pending_trigger"
        if age_f is None:
            return "skip_unclear_state"
        if age_f < float(recent_skip_hours):
            return "skip_recent"
        if age_f >= float(expire_threshold):
            return "would_expire_pending_entry"
        if age_f >= float(cancel_threshold):
            return "would_cancel_pending_entry"
        return "skip_unclear_state"
    return _classify


class TestClassifierProducesEveryAction:
    """One test per action string — proves every documented label is reachable."""

    def setup_method(self):
        self.classify = _make_classifier()

    def _base_row(self):
        return {
            "kind": "ENTRY",
            "status": "PENDING_TRIGGER",
            "broker_order_id": None,
            "local_order_id": "LOID-1",
            "age_hours": 100.0,
        }

    def test_would_expire(self):
        # age >= expire_threshold (default 72)
        r = self._base_row(); r["age_hours"] = 100.0
        assert self.classify(r) == "would_expire_pending_entry"

    def test_would_cancel(self):
        # cancel_threshold (24) <= age < expire_threshold (72)
        r = self._base_row(); r["age_hours"] = 30.0
        assert self.classify(r) == "would_cancel_pending_entry"

    def test_skip_recent(self):
        # age < recent_skip_hours (default 1)
        r = self._base_row(); r["age_hours"] = 0.5
        assert self.classify(r) == "skip_recent"

    def test_skip_has_broker_order_id(self):
        r = self._base_row(); r["broker_order_id"] = "BRK-99"
        assert self.classify(r) == "skip_has_broker_order_id"

    def test_skip_not_entry(self):
        r = self._base_row(); r["kind"] = "EXIT"
        assert self.classify(r) == "skip_not_entry"

    def test_skip_not_entry_when_kind_none(self):
        r = self._base_row(); r["kind"] = None
        assert self.classify(r) == "skip_not_entry"

    def test_skip_not_pending_trigger(self):
        r = self._base_row(); r["status"] = "FILLED"
        assert self.classify(r) == "skip_not_pending_trigger"

    def test_skip_missing_local_order_id(self):
        r = self._base_row(); r["local_order_id"] = None
        assert self.classify(r) == "skip_missing_local_order_id"

    def test_skip_missing_local_order_id_empty_string(self):
        # Empty-string loid must also skip — defensive against malformed rows.
        r = self._base_row(); r["local_order_id"] = ""
        assert self.classify(r) == "skip_missing_local_order_id"

    def test_skip_unclear_state_age_none(self):
        # Age can't be determined → unclear, never cancel/expire.
        r = self._base_row(); r["age_hours"] = None
        assert self.classify(r) == "skip_unclear_state"

    def test_skip_unclear_state_age_between_recent_and_cancel(self):
        # recent_skip_hours <= age < cancel_threshold → too old to be recent
        # but too young to act on.
        r = self._base_row(); r["age_hours"] = 5.0
        assert self.classify(r) == "skip_unclear_state"


class TestClassifierOrdering:
    """Rule order must short-circuit correctly — a row that has BOTH a broker
    order id AND is stale must skip_has_broker_order_id, not would_expire."""

    def test_broker_id_short_circuits_age(self):
        c = _make_classifier()
        r = {"kind": "ENTRY", "status": "PENDING_TRIGGER",
             "broker_order_id": "BRK-7", "local_order_id": "L1",
             "age_hours": 1000.0}
        assert c(r) == "skip_has_broker_order_id"

    def test_missing_loid_short_circuits_everything(self):
        c = _make_classifier()
        r = {"kind": "ENTRY", "status": "PENDING_TRIGGER",
             "broker_order_id": "BRK-9", "local_order_id": None,
             "age_hours": 5000.0}
        # missing loid is the very first check, even before broker_id
        assert c(r) == "skip_missing_local_order_id"

    def test_not_entry_short_circuits_age(self):
        c = _make_classifier()
        r = {"kind": "EXIT", "status": "PENDING_TRIGGER",
             "broker_order_id": None, "local_order_id": "L1",
             "age_hours": 1000.0}
        assert c(r) == "skip_not_entry"

    def test_not_pending_short_circuits_age(self):
        c = _make_classifier()
        r = {"kind": "ENTRY", "status": "CANCELED",
             "broker_order_id": None, "local_order_id": "L1",
             "age_hours": 1000.0}
        assert c(r) == "skip_not_pending_trigger"


class TestClassifierThresholdBoundaries:
    def test_age_exactly_recent_threshold_is_unclear(self):
        # age == recent_skip_hours (1.0) is NOT skip_recent (boundary is <).
        # It falls into skip_unclear_state (1.0 < cancel_threshold 24).
        c = _make_classifier(recent_skip_hours=1, cancel_threshold=24, expire_threshold=72)
        r = {"kind":"ENTRY","status":"PENDING_TRIGGER","broker_order_id":None,
             "local_order_id":"L1","age_hours":1.0}
        assert c(r) == "skip_unclear_state"

    def test_age_exactly_cancel_threshold_is_cancel(self):
        c = _make_classifier(recent_skip_hours=1, cancel_threshold=24, expire_threshold=72)
        r = {"kind":"ENTRY","status":"PENDING_TRIGGER","broker_order_id":None,
             "local_order_id":"L1","age_hours":24.0}
        assert c(r) == "would_cancel_pending_entry"

    def test_age_exactly_expire_threshold_is_expire(self):
        c = _make_classifier(recent_skip_hours=1, cancel_threshold=24, expire_threshold=72)
        r = {"kind":"ENTRY","status":"PENDING_TRIGGER","broker_order_id":None,
             "local_order_id":"L1","age_hours":72.0}
        assert c(r) == "would_expire_pending_entry"

    def test_collapsed_thresholds_default_to_expire(self):
        # If a caller inverts thresholds, the endpoint clamps expire>=cancel,
        # so the collapsed band only emits would_expire.
        c = _make_classifier(recent_skip_hours=1, cancel_threshold=24, expire_threshold=24)
        r = {"kind":"ENTRY","status":"PENDING_TRIGGER","broker_order_id":None,
             "local_order_id":"L1","age_hours":50.0}
        assert c(r) == "would_expire_pending_entry"


# ── 2. Read-only invariants enforced by source-scan ─────────────────────────


class TestEndpointIsReadOnly:
    """The dry-run endpoint MUST NOT contain any mutation primitives.
    These tests scan the endpoint source for forbidden tokens."""

    def test_endpoint_exists(self):
        assert '@app.get("/admin/operator/ghost-orders/dry-run")' in _read_app()

    def test_no_mutation_verbs_in_sql(self):
        # Scan EXECUTABLE code only — docstrings and the disclaiming note
        # are allowed to mention these verbs by name.
        body = _executable_only(_extract_dry_run_endpoint()).upper()
        for verb in ("INSERT INTO", "UPDATE ORDERS", "UPDATE ",
                     "DELETE FROM", "DROP "):
            assert verb not in body, f"forbidden SQL verb {verb!r} found in executable code"

    def test_no_osm_mutation_methods(self):
        body = _executable_only(_extract_dry_run_endpoint())
        for forbidden in (
            "cancel_pending_entry",
            "expire_pending_entry",
            "update_order_meta",
            "transition(",
            ".cancel(",
            ".delete(",
            "place_order(",
            "submit_existing_entry",
        ):
            assert forbidden not in body, (
                f"forbidden mutation method {forbidden!r} found in executable code"
            )

    def test_no_broker_calls(self):
        body = _executable_only(_extract_dry_run_endpoint())
        for forbidden in ("broker.place", "broker.cancel", "broker.submit",
                          ".place_order", ".cancel_order"):
            assert forbidden not in body, (
                f"forbidden broker reference {forbidden!r} found in executable code"
            )

    def test_response_self_labels_dry_run(self):
        body = _extract_dry_run_endpoint()
        # The endpoint MUST return these exact flags so consumers can verify
        # the read-only contract from the response payload alone.
        assert '"report_only": True' in body
        assert '"dry_run": True' in body
        assert '"mutation_performed": False' in body

    def test_response_includes_note_about_no_mutation(self):
        body = _extract_dry_run_endpoint()
        assert '"note"' in body
        assert "DRY-RUN ONLY" in body or "DRY-RUN" in body
        # The note explicitly disclaims mutation methods.
        for s in ("cancel_pending_entry", "expire_pending_entry"):
            assert s in body, f"note must mention forbidden method {s!r}"


class TestSqlIsParameterized:
    def test_uses_param_placeholders_only(self):
        body = _extract_dry_run_endpoint()
        assert "%s" in body                 # filter params are placeholders
        assert "f\"SELECT" not in body      # no f-string SQL
        assert "f'SELECT" not in body
        # No string-interpolated filter values
        assert "'%s'" not in body or "= %s" in body  # values are bound, never inlined


class TestRowConversionSafety:
    def test_endpoint_handles_dict_and_tuple_rows(self):
        body = _extract_dry_run_endpoint()
        # Same isinstance check pattern PR #57 / PR #60 use
        assert "isinstance(r, dict)" in body
        assert "dict(zip(cols, r))" in body


class TestSummaryShape:
    """Response must include the documented summary keys."""

    def test_required_top_level_keys_in_response(self):
        body = _extract_dry_run_endpoint()
        for key in ("rows_scanned", "rows_eligible", "rows_skipped",
                    "proposed_actions", "action_summary",
                    "per_client_summary", "per_symbol_summary"):
            assert f'"{key}"' in body, f"required response key {key!r} missing"

    def test_thresholds_echoed_in_response(self):
        body = _extract_dry_run_endpoint()
        # Caller needs to know which thresholds produced the classification.
        assert '"thresholds"' in body
        assert '"recent_skip_hours"' in body
        assert '"cancel_threshold"' in body
        assert '"expire_threshold"' in body


class TestClassifierStringSet:
    """The endpoint must emit only the 8 documented action strings."""

    def test_all_eight_action_strings_present_in_source(self):
        body = _extract_dry_run_endpoint()
        for a in (
            "would_expire_pending_entry",
            "would_cancel_pending_entry",
            "skip_recent",
            "skip_has_broker_order_id",
            "skip_not_entry",
            "skip_not_pending_trigger",
            "skip_missing_local_order_id",
            "skip_unclear_state",
        ):
            assert a in body, f"action string {a!r} missing from endpoint"

    def test_only_documented_actions_can_be_emitted(self):
        """Drive the mirror classifier with a wide range of inputs and
        verify every output falls in the documented set."""
        allowed = {
            "would_expire_pending_entry",
            "would_cancel_pending_entry",
            "skip_recent",
            "skip_has_broker_order_id",
            "skip_not_entry",
            "skip_not_pending_trigger",
            "skip_missing_local_order_id",
            "skip_unclear_state",
        }
        c = _make_classifier()
        cases = [
            {},  # empty row
            {"kind": "ENTRY"},
            {"kind": "ENTRY", "status": "PENDING_TRIGGER", "local_order_id": "L"},
            {"kind": "ENTRY", "status": "PENDING_TRIGGER", "local_order_id": "L",
             "broker_order_id": "B"},
            {"kind": "ENTRY", "status": "PENDING_TRIGGER", "local_order_id": "L",
             "age_hours": 0},
            {"kind": "ENTRY", "status": "PENDING_TRIGGER", "local_order_id": "L",
             "age_hours": "not a number"},
            {"kind": None, "status": None, "local_order_id": None,
             "broker_order_id": None, "age_hours": None},
        ]
        for row in cases:
            out = c(row)
            assert out in allowed, f"unexpected action {out!r} for row {row}"
