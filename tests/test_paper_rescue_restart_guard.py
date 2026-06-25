"""
tests/test_paper_rescue_restart_guard.py

POST /admin/paper_rescue_restart_guard

Converts REJECTED/restart_guard:overnight_skip trade_queue rows to WATCHING
for paper clients only, marks them as overnight-reeval-only rescue rows,
then runs overnight_reeval followed by morning_handoff_audit.

Hard invariants tested throughout:
  PAPER ONLY — live client aborts the entire operation
  NEVER broker.submit_order
  NEVER create_entry_order
  NEVER touches live client rows
  DB write limited to trade_queue.status + trade_queue.last_error
  Idempotent — converting same row twice is a no-op on second call
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

_REPO      = Path(__file__).resolve().parents[1]
_MODULE_SRC = (_REPO / "ap_paper_rescue_restart_guard.py").read_text()
_APP_SRC    = (_REPO / "app.py").read_text()


# ---------------------------------------------------------------------------
# Import the module under test
# ---------------------------------------------------------------------------

def _load_module():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "ap_paper_rescue_restart_guard_shim",
        _REPO / "ap_paper_rescue_restart_guard.py",
    )
    with patch.dict(sys.modules, {
        "ap.db":                    MagicMock(conn=MagicMock(), run_with_retry=lambda fn: fn()),
        "ap_morning_handoff_audit": MagicMock(
            run_morning_handoff_audit=MagicMock(return_value={
                "ok": True, "scanned": 0, "auto_rearmed": 0,
                "rows": [], "errors": [],
            })
        ),
    }):
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    return mod

_mod = _load_module()


# ---------------------------------------------------------------------------
# Helper: build minimal runner stubs
# ---------------------------------------------------------------------------

def _paper_runner(email: str = "jose.vasquez4011@gmail.com") -> object:
    r = MagicMock()
    r.mode = "PAPER"
    r.email = email
    r.order_state_machine = MagicMock(name="osm")
    r.core.entry_watcher  = MagicMock(name="watcher")
    return r


def _live_runner(email: str = "jasoncosby1@gmail.com") -> object:
    r = MagicMock()
    r.mode = "LIVE"
    r.email = email
    r.order_state_machine = MagicMock(name="osm")
    r.core.entry_watcher  = MagicMock(name="watcher")
    return r


def _rescued_rows(client_id: str, count: int = 2) -> list[dict]:
    return [
        {
            "id":         1000 + i,
            "client_id":  client_id,
            "signal_id":  f"sig-{i:04d}",
            "status":     "REJECTED",
            "last_error": "restart_guard:overnight_skip",
            "created_ts": "2026-06-17T08:00:00+00:00",
        }
        for i in range(count)
    ]


# ---------------------------------------------------------------------------
# Source guards
# ---------------------------------------------------------------------------

class TestSourceGuards:
    def test_module_has_run_paper_rescue_restart_guard(self):
        assert "run_paper_rescue_restart_guard" in _MODULE_SRC

    def test_source_last_error_is_correct(self):
        assert "_SOURCE_LAST_ERROR = \"restart_guard:overnight_skip\"" in _MODULE_SRC

    def test_bypass_last_error_is_correct(self):
        assert "_BYPASS_LAST_ERROR = \"after_hours_deferred:awaiting_overnight_reeval\"" in _MODULE_SRC

    def test_from_status_is_rejected(self):
        assert '_FROM_STATUS = "REJECTED"' in _MODULE_SRC

    def test_to_status_is_watching(self):
        assert '_TO_STATUS   = "WATCHING"' in _MODULE_SRC

    def test_no_broker_submit_in_module(self):
        bad = [ln for ln in _MODULE_SRC.splitlines()
               if not ln.strip().startswith("#")
               and not ln.strip().startswith("-")
               and not ln.strip().startswith("*")
               and not ln.strip().startswith("NEVER")
               and "submit_order" in ln]
        assert len(bad) == 0

    def test_no_create_entry_order_in_module(self):
        bad = [ln for ln in _MODULE_SRC.splitlines()
               if not ln.strip().startswith("#")
               and not ln.strip().startswith("-")
               and not ln.strip().startswith("*")
               and not ln.strip().startswith("NEVER")
               and "create_entry_order" in ln]
        assert len(bad) == 0

    def test_live_guard_raises_valueerror(self):
        assert "ValueError" in _MODULE_SRC
        assert "LIVE" in _MODULE_SRC

    def test_idempotency_guard_in_convert_row(self):
        """_convert_row must only update rows that are STILL REJECTED/overnight_skip."""
        fn_start = _MODULE_SRC.find("def _convert_row(")
        fn_body  = _MODULE_SRC[fn_start: fn_start + 1800]
        assert "_FROM_STATUS" in fn_body, "convert_row must guard status=REJECTED"
        assert "_SOURCE_LAST_ERROR" in fn_body, "convert_row must guard last_error"

    def test_endpoint_in_app(self):
        assert "/admin/paper_rescue_restart_guard" in _APP_SRC

    def test_endpoint_paper_only_guard_in_app(self):
        idx    = _APP_SRC.find("/admin/paper_rescue_restart_guard")
        region = _APP_SRC[idx: idx + 3000]
        assert "LIVE" in region, "Endpoint must check runner.mode == LIVE"
        assert "400" in region, "Endpoint must return 400 for live clients"

    def test_endpoint_lookback_h_default_36(self):
        idx    = _APP_SRC.find("admin_paper_rescue_restart_guard")
        region = _APP_SRC[idx: idx + 2000]
        assert "lookback_h" in region
        assert "36" in region

    def test_endpoint_calls_run_paper_rescue_restart_guard(self):
        idx    = _APP_SRC.find("admin_paper_rescue_restart_guard")
        region = _APP_SRC[idx: idx + 2000]
        assert "run_paper_rescue_restart_guard" in region

    def test_morning_handoff_audit_called_in_module(self):
        assert "run_morning_handoff_audit" in _MODULE_SRC

    def test_overnight_reeval_called_in_module(self):
        assert "run_overnight_reeval" in _MODULE_SRC

    def test_execution_mode_always_paper_in_audit_call(self):
        fn_start = _MODULE_SRC.find("run_morning_handoff_audit(")
        region   = _MODULE_SRC[fn_start: fn_start + 400]
        assert 'execution_mode="paper"' in region, (
            "Audit must always be called with execution_mode='paper'"
        )

    def test_osm_fallback_chain_uses_order_state_machine_first(self):
        """Audit OSM resolution must try order_state_machine before osm."""
        osm_idx    = _MODULE_SRC.find("order_state_machine")
        legacy_idx = _MODULE_SRC.find('"osm"')
        assert osm_idx != -1
        assert legacy_idx != -1
        assert osm_idx < legacy_idx

    def test_summary_sentinel_in_module(self):
        assert "PAPER_RESCUE_RESTART_GUARD_SUMMARY" in _MODULE_SRC


# ---------------------------------------------------------------------------
# Test 1: live client aborts entire operation
# ---------------------------------------------------------------------------

class TestLiveClientAbort:

    def test_live_client_raises_value_error(self):
        live = _live_runner("jasoncosby1@gmail.com")
        with pytest.raises(ValueError, match="LIVE"):
            _mod.run_paper_rescue_restart_guard(
                paper_client_ids=["jasoncosby1@gmail.com"],
                runners={"jasoncosby1@gmail.com": live},
                lookback_hours=36,
                dry_run=False,
            )

    def test_live_client_zero_rows_touched(self):
        """Even finding rows should not happen when a live client is in scope."""
        live = _live_runner("jasoncosby1@gmail.com")
        with patch.object(_mod, "_load_rescue_rows") as mock_load, \
             pytest.raises(ValueError):
            _mod.run_paper_rescue_restart_guard(
                paper_client_ids=["jasoncosby1@gmail.com"],
                runners={"jasoncosby1@gmail.com": live},
                lookback_hours=36,
                dry_run=False,
            )
        mock_load.assert_not_called()

    def test_mixed_live_and_paper_aborts_on_live(self):
        """If one client is live, the whole operation aborts — no paper rows touched."""
        paper = _paper_runner("jose.vasquez4011@gmail.com")
        live  = _live_runner("jasoncosby1@gmail.com")
        with patch.object(_mod, "_load_rescue_rows") as mock_load, \
             pytest.raises(ValueError, match="LIVE"):
            _mod.run_paper_rescue_restart_guard(
                paper_client_ids=[
                    "jose.vasquez4011@gmail.com",
                    "jasoncosby1@gmail.com",
                ],
                runners={
                    "jose.vasquez4011@gmail.com": paper,
                    "jasoncosby1@gmail.com":      live,
                },
                lookback_hours=36,
                dry_run=False,
            )
        mock_load.assert_not_called()

    def test_endpoint_returns_400_for_live_client(self):
        """App endpoint must return 400 JSON when a live runner is in scope."""
        idx    = _APP_SRC.find("def admin_paper_rescue_restart_guard")
        region = _APP_SRC[idx: idx + 2500]
        assert "400" in region
        assert "paper-only" in region or "PAPER" in region or "live_clients" in region

    def test_is_live_runner_helper(self):
        live  = _live_runner()
        paper = _paper_runner()
        assert _mod._is_live_runner(live)  is True
        assert _mod._is_live_runner(paper) is False

    def test_is_live_runner_lowercase_mode(self):
        r = MagicMock()
        r.mode = "live"   # lowercase — must still be detected
        assert _mod._is_live_runner(r) is True


# ---------------------------------------------------------------------------
# Test 2: converts REJECTED/overnight_skip → WATCHING with correct last_error
# ---------------------------------------------------------------------------

class TestConversion:

    def _run(self, *, dry_run=False, row_count=2):
        email  = "jose.vasquez4011@gmail.com"
        paper  = _paper_runner(email)
        rows   = _rescued_rows(email, row_count)

        convert_mock = MagicMock(return_value=True)
        audit_mock   = MagicMock(return_value={
            "ok": True, "scanned": row_count, "auto_rearmed": row_count,
            "rows": [], "errors": [],
        })

        with patch.object(_mod, "_load_rescue_rows", return_value=rows), \
             patch.object(_mod, "_convert_row", convert_mock), \
             patch("ap_morning_handoff_audit.run_morning_handoff_audit", audit_mock), \
             patch.dict(sys.modules, {
                 "ap_morning_handoff_audit": MagicMock(
                     run_morning_handoff_audit=audit_mock
                 )
             }):
            result = _mod.run_paper_rescue_restart_guard(
                paper_client_ids=[email],
                runners={email: paper},
                lookback_hours=36,
                dry_run=dry_run,
            )
        return result, convert_mock, audit_mock

    def test_rows_found_count(self):
        result, _, _ = self._run(row_count=2)
        assert result["rows_found"] == 2

    def test_rows_converted_count(self):
        result, _, _ = self._run(row_count=2)
        assert result["rows_converted"] == 2

    def test_convert_row_called_for_each_row(self):
        result, convert_mock, _ = self._run(row_count=2)
        assert convert_mock.call_count == 2

    def test_convert_row_called_with_correct_job_ids(self):
        result, convert_mock, _ = self._run(row_count=2)
        called_ids = {c.args[0] for c in convert_mock.call_args_list}
        assert called_ids == {1000, 1001}

    def test_result_ok_true_on_success(self):
        result, _, _ = self._run()
        assert result["ok"] is True

    def test_lookback_hours_in_result(self):
        result, _, _ = self._run()
        assert result["lookback_hours"] == 36

    def test_per_client_breakdown_present(self):
        result, _, _ = self._run()
        assert "jose.vasquez4011@gmail.com" in result["per_client"]
        assert result["per_client"]["jose.vasquez4011@gmail.com"]["rows_found"] == 2
        assert result["per_client"]["jose.vasquez4011@gmail.com"]["rows_converted"] == 2


# ---------------------------------------------------------------------------
# Test 3: DB write uses correct status and last_error values
# ---------------------------------------------------------------------------

class TestDbWriteValues:

    def test_bypass_last_error_value(self):
        assert _mod._BYPASS_LAST_ERROR == "after_hours_deferred:awaiting_overnight_reeval"

    def test_from_status_is_rejected(self):
        assert _mod._FROM_STATUS == "REJECTED"

    def test_to_status_is_watching(self):
        assert _mod._TO_STATUS == "WATCHING"

    def test_source_last_error_is_overnight_skip(self):
        assert _mod._SOURCE_LAST_ERROR == "restart_guard:overnight_skip"

    def test_convert_row_where_clause_has_idempotency_guards(self):
        """_convert_row must only update rows still in REJECTED/overnight_skip state."""
        fn_start = _MODULE_SRC.find("def _convert_row(")
        fn_body  = _MODULE_SRC[fn_start: fn_start + 1800]
        assert "_FROM_STATUS" in fn_body
        assert "_SOURCE_LAST_ERROR" in fn_body
        assert "rowcount" in fn_body

    def test_convert_row_sets_overnight_flags(self):
        fn_start = _MODULE_SRC.find("def _convert_row(")
        fn_body  = _MODULE_SRC[fn_start: fn_start + 2200]
        assert "'force_overnight_reeval_only', true" in fn_body
        assert "'do_not_queue_directly', true" in fn_body
        assert "manual_rescue_route', 'overnight_reeval_only" in fn_body

    def test_no_rows_outside_lookback_window(self):
        """_load_rescue_rows must include a created_ts >= cutoff filter."""
        fn_start = _MODULE_SRC.find("def _load_rescue_rows(")
        fn_body  = _MODULE_SRC[fn_start: fn_start + 800]
        assert "created_ts" in fn_body
        assert "lookback" in fn_body.lower() or "cutoff" in fn_body


# ---------------------------------------------------------------------------
# Test 4: morning handoff audit called after conversion
# ---------------------------------------------------------------------------

class TestAuditCalled:

    def _run_with_capture(self, dry_run=False):
        email  = "jose.vasquez4011@gmail.com"
        paper  = _paper_runner(email)
        rows   = _rescued_rows(email, 1)
        captured_kwargs = {}

        def _capture_audit(**kwargs):
            captured_kwargs.update(kwargs)
            return {"ok": True, "scanned": 1, "auto_rearmed": 1, "rows": [], "errors": []}

        with patch.object(_mod, "_load_rescue_rows", return_value=rows), \
             patch.object(_mod, "_convert_row", return_value=True), \
             patch.dict(sys.modules, {
                 "ap_morning_handoff_audit": MagicMock(
                     run_morning_handoff_audit=_capture_audit
                 )
             }):
            result = _mod.run_paper_rescue_restart_guard(
                paper_client_ids=[email],
                runners={email: paper},
                lookback_hours=36,
                dry_run=dry_run,
            )
        return result, captured_kwargs

    def test_audit_called_after_conversion(self):
        _, kwargs = self._run_with_capture()
        assert "client_id" in kwargs, "Morning handoff audit must be called"

    def test_audit_execution_mode_is_paper(self):
        _, kwargs = self._run_with_capture()
        assert kwargs.get("execution_mode") == "paper"

    def test_audit_client_id_matches_email(self):
        _, kwargs = self._run_with_capture()
        assert kwargs.get("client_id") == "jose.vasquez4011@gmail.com"

    def test_audit_dry_run_passed_through(self):
        _, kwargs = self._run_with_capture(dry_run=True)
        assert kwargs.get("dry_run") is True

    def test_audit_result_in_per_client(self):
        result, _ = self._run_with_capture()
        per = result["per_client"].get("jose.vasquez4011@gmail.com", {})
        assert per.get("audit_result") is not None

    def test_audit_result_in_audit_results(self):
        result, _ = self._run_with_capture()
        assert "jose.vasquez4011@gmail.com" in result["audit_results"]


# ---------------------------------------------------------------------------
# Test 5: dry run classifies but does not convert rows
# ---------------------------------------------------------------------------

class TestDryRun:

    def _run_dry(self, row_count=2):
        email  = "jose.vasquez4011@gmail.com"
        paper  = _paper_runner(email)
        rows   = _rescued_rows(email, row_count)
        convert_mock = MagicMock(return_value=True)

        with patch.object(_mod, "_load_rescue_rows", return_value=rows), \
             patch.object(_mod, "_convert_row", convert_mock), \
             patch.dict(sys.modules, {
                 "ap_morning_handoff_audit": MagicMock(
                     run_morning_handoff_audit=MagicMock(return_value={
                         "ok": True, "scanned": 0, "auto_rearmed": 0,
                         "rows": [], "errors": [],
                     })
                 )
             }):
            result = _mod.run_paper_rescue_restart_guard(
                paper_client_ids=[email],
                runners={email: paper},
                lookback_hours=36,
                dry_run=True,
            )
        return result, convert_mock

    def test_dry_run_does_not_call_convert_row(self):
        _, convert_mock = self._run_dry()
        convert_mock.assert_not_called()

    def test_dry_run_rows_found_still_counted(self):
        result, _ = self._run_dry(row_count=3)
        assert result["rows_found"] == 3

    def test_dry_run_result_has_dry_run_true(self):
        result, _ = self._run_dry()
        assert result["dry_run"] is True


# ---------------------------------------------------------------------------
# Test 6: idempotency — already-converted row is a no-op
# ---------------------------------------------------------------------------

class TestIdempotency:

    def test_already_converted_row_skipped(self):
        """_convert_row returns False (rowcount=0) when row is already WATCHING."""
        email = "jose.vasquez4011@gmail.com"
        paper = _paper_runner(email)
        rows  = _rescued_rows(email, 1)

        # First call succeeds; second call returns 0 rowcount (already converted)
        convert_mock = MagicMock(side_effect=[True, False])

        with patch.object(_mod, "_load_rescue_rows", return_value=rows), \
             patch.object(_mod, "_convert_row", convert_mock), \
             patch.dict(sys.modules, {
                 "ap_morning_handoff_audit": MagicMock(
                     run_morning_handoff_audit=MagicMock(return_value={
                         "ok": True, "scanned": 0, "auto_rearmed": 0,
                         "rows": [], "errors": [],
                     })
                 )
             }):
            # First audit
            r1 = _mod.run_paper_rescue_restart_guard(
                paper_client_ids=[email], runners={email: paper},
                lookback_hours=36, dry_run=False,
            )
            # Second audit — same row, convert_mock returns False (already done)
            r2 = _mod.run_paper_rescue_restart_guard(
                paper_client_ids=[email], runners={email: paper},
                lookback_hours=36, dry_run=False,
            )

        assert r1["rows_converted"] == 1
        assert r2["rows_skipped_already_converted"] == 1
        assert r2["rows_converted"] == 0

    def test_calling_twice_is_safe(self):
        """Calling the rescue twice must never raise."""
        email = "jose.vasquez4011@gmail.com"
        paper = _paper_runner(email)
        rows  = _rescued_rows(email, 1)

        with patch.object(_mod, "_load_rescue_rows", return_value=rows), \
             patch.object(_mod, "_convert_row", MagicMock(return_value=False)), \
             patch.dict(sys.modules, {
                 "ap_morning_handoff_audit": MagicMock(
                     run_morning_handoff_audit=MagicMock(return_value={
                         "ok": True, "scanned": 0, "auto_rearmed": 0,
                         "rows": [], "errors": [],
                     })
                 )
             }):
            try:
                _mod.run_paper_rescue_restart_guard(
                    paper_client_ids=[email], runners={email: paper},
                    lookback_hours=36, dry_run=False,
                )
                _mod.run_paper_rescue_restart_guard(
                    paper_client_ids=[email], runners={email: paper},
                    lookback_hours=36, dry_run=False,
                )
            except Exception as exc:
                pytest.fail(f"Second call raised: {exc}")


# ---------------------------------------------------------------------------
# Test 7: clients=[] or no matching rows → returns empty counts
# ---------------------------------------------------------------------------

class TestEmptyClients:

    def test_empty_client_list_returns_zero_counts(self):
        result = _mod.run_paper_rescue_restart_guard(
            paper_client_ids=[],
            runners={},
            lookback_hours=36,
            dry_run=False,
        )
        assert result["ok"] is True
        assert result["rows_found"] == 0
        assert result["rows_converted"] == 0
        assert result["clients_processed"] == 0

    def test_no_rows_in_window_returns_zero(self):
        email = "jose.vasquez4011@gmail.com"
        paper = _paper_runner(email)

        with patch.object(_mod, "_load_rescue_rows", return_value=[]), \
             patch.dict(sys.modules, {
                 "ap_morning_handoff_audit": MagicMock(
                     run_morning_handoff_audit=MagicMock(return_value={
                         "ok": True, "scanned": 0, "auto_rearmed": 0,
                         "rows": [], "errors": [],
                     })
                 )
             }):
            result = _mod.run_paper_rescue_restart_guard(
                paper_client_ids=[email],
                runners={email: paper},
                lookback_hours=36,
                dry_run=False,
            )
        assert result["rows_found"] == 0
        assert result["rows_converted"] == 0
        assert result["ok"] is True


# ---------------------------------------------------------------------------
# Test 8: multi-client — each gets separate counts
# ---------------------------------------------------------------------------

class TestMultiClient:

    def test_two_paper_clients_each_get_rows(self):
        e1 = "jose.vasquez4011@gmail.com"
        e2 = "tradefluencehq@gmail.com"
        rows = _rescued_rows(e1, 2) + _rescued_rows(e2, 3)

        with patch.object(_mod, "_load_rescue_rows", return_value=rows), \
             patch.object(_mod, "_convert_row", return_value=True), \
             patch.dict(sys.modules, {
                 "ap_morning_handoff_audit": MagicMock(
                     run_morning_handoff_audit=MagicMock(return_value={
                         "ok": True, "scanned": 1, "auto_rearmed": 1,
                         "rows": [], "errors": [],
                     })
                 )
             }):
            result = _mod.run_paper_rescue_restart_guard(
                paper_client_ids=[e1, e2],
                runners={e1: _paper_runner(e1), e2: _paper_runner(e2)},
                lookback_hours=36,
                dry_run=False,
            )

        assert result["rows_found"] == 5
        assert result["rows_converted"] == 5
        assert result["per_client"][e1]["rows_found"] == 2
        assert result["per_client"][e2]["rows_found"] == 3
