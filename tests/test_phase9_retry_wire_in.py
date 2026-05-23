"""
Phase 9 tests: post-cancel retry wired into order_monitor (PR #26).

Verifies that the live cancel path actually:
  1. Calls evaluate_retry after every confirmed cancel.
  2. Persists ARM/ABORT state into orders.meta.
  3. Polls armed retries on the fast loop cadence.
  4. Submits a fresh entry via process_signal when ready.
  5. Emits ENTRY_RETRY_ARMED / SUBMITTED / ABORTED tokens from the live
     path, not just from the unit-test docstring.

Run:
    pytest tests/test_phase9_retry_wire_in.py -xvs
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
OM_SRC = (REPO_ROOT / "ap" / "order_monitor.py").read_text()

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_phase9",
)


# ============================================================
# 1. The wire-in is present at every cancel-confirmation site
# ============================================================

class TestCancelHookSites:
    def test_hook_after_created_no_broker_cancel(self):
        m = re.search(
            r"status == \"CREATED\" and not broker_oid:.*?return",
            OM_SRC, re.DOTALL,
        )
        assert m, "CREATED/no-broker cancel block must exist"
        assert "_maybe_arm_post_cancel_retry" in m.group(0), \
            "CREATED/no-broker cancel must call _maybe_arm_post_cancel_retry"

    def test_hook_after_broker_confirmed_cancel(self):
        m = re.search(
            r"if self\._is_terminal_cancel_status\(confirmed_status\):.*?else:",
            OM_SRC, re.DOTALL,
        )
        assert m, "broker-confirmed cancel block must exist"
        assert "_maybe_arm_post_cancel_retry" in m.group(0), \
            "broker-confirmed cancel must call _maybe_arm_post_cancel_retry"

    def test_handler_has_two_hook_sites(self):
        handler_match = re.search(
            r"def _handle_stale_entry.*?def _handle_stale_exit",
            OM_SRC, re.DOTALL,
        )
        assert handler_match, "_handle_stale_entry must exist"
        body = handler_match.group(0)
        hook_calls = body.count("_maybe_arm_post_cancel_retry(")
        assert hook_calls >= 2, \
            f"_handle_stale_entry must have >=2 hook calls, found {hook_calls}"


# ============================================================
# 2. The poll loop is wired and on fast cadence
# ============================================================

class TestPollLoopWired:
    def test_run_loop_calls_check_armed_retries(self):
        m = re.search(r"def _run\(self\):.*?def _check_entry_orders",
                      OM_SRC, re.DOTALL)
        assert m, "_run must exist"
        body = m.group(0)
        assert "self._check_armed_retries()" in body, \
            "_run main loop must call self._check_armed_retries()"

    def test_check_armed_retries_runs_on_fast_cadence(self):
        """_check_armed_retries() must run on EVERY tick of the fast loop
        (every EXIT_CHECK_INTERVAL = 15s), not gated inside the
        POLL_INTERVAL=60s entry-check block. Otherwise worst-case retry
        latency would be 90s, outside the 15-30s spec.
        """
        loop_match = re.search(
            r"while not self\._stop_event\.wait\(EXIT_CHECK_INTERVAL\):(.*?)def ",
            OM_SRC, re.DOTALL,
        )
        assert loop_match, "fast loop must exist"
        body = loop_match.group(1)
        idx_call  = body.find("self._check_armed_retries()")
        idx_gate  = body.find("if _now - _last_entry_check >= POLL_INTERVAL")
        assert idx_call > 0, "_check_armed_retries() must be called in the fast loop"
        assert idx_gate > 0, "POLL_INTERVAL gate must exist"

        # Find the end of the POLL_INTERVAL block: the next line at the same
        # indent level as the gate's `if` (12 spaces) marks the end. The call
        # to _check_armed_retries() must come AFTER that boundary so it runs
        # on every fast-loop iteration, not only on slow-cadence iterations.
        gate_line_start = body.rfind("\n", 0, idx_gate) + 1
        gate_indent     = len(body[gate_line_start:idx_gate])
        # Scan forward from idx_gate to find the first line whose indent
        # returns to gate_indent (the end of the gated block).
        cursor = idx_gate
        gate_end = None
        for line_start in range(idx_gate, len(body)):
            if body[line_start] != "\n":
                continue
            ls = line_start + 1
            if ls >= len(body):
                break
            # Skip blank lines.
            if body[ls] == "\n":
                continue
            # Measure the indent of this line.
            li = ls
            while li < len(body) and body[li] == " ":
                li += 1
            indent = li - ls
            if indent <= gate_indent and body[ls:li] == " " * gate_indent:
                # New statement at the gate's indent level (or shallower).
                gate_end = ls
                break
        assert gate_end is not None, "could not find end of POLL_INTERVAL gate"
        assert idx_call > gate_end, (
            "_check_armed_retries() is INSIDE the POLL_INTERVAL gate; it must "
            "be outside so it runs on the fast cadence."
        )


# ============================================================
# 3. New methods exist with correct shape
# ============================================================

class TestNewMethodsExist:
    def test_maybe_arm_post_cancel_retry_signature(self):
        assert re.search(
            r"def _maybe_arm_post_cancel_retry\(\s*self,\s*"
            r"local_order_id: str,\s*contract: str,\s*cancel_reason: str,\s*\)",
            OM_SRC,
        ), "_maybe_arm_post_cancel_retry signature must match"

    def test_check_armed_retries_signature(self):
        assert re.search(
            r"def _check_armed_retries\(self\)",
            OM_SRC,
        ), "_check_armed_retries() must exist"

    def test_submit_armed_retry_calls_process_signal(self):
        m = re.search(
            r"def _submit_armed_retry.*?def _stamp_retry_status",
            OM_SRC, re.DOTALL,
        )
        assert m, "_submit_armed_retry must exist"
        body = m.group(0)
        assert "from ap.execution import process_signal" in body
        assert "process_signal(self.broker, self.client_id, retry_payload)" in body

    def test_stamp_retry_status_exists(self):
        assert "def _stamp_retry_status(" in OM_SRC


# ============================================================
# 4. Log tokens emitted from live monitor (not just docstrings)
# ============================================================

class TestLogTokensLive:
    """The three canonical retry tokens must appear inside log calls in the
    live monitor, not only in comments/docstrings. A code-only string in a
    docstring would mean ops never sees the event in Render logs.
    """

    def _emits_token_in_log_call(self, token: str) -> bool:
        # Look for token used as a string literal that appears within ~200
        # chars after a `log.{info,warning,error}(` token — enough to span
        # multi-line log calls without matching unrelated nearby code.
        for m in re.finditer(re.escape(token), OM_SRC):
            window_start = max(0, m.start() - 300)
            window = OM_SRC[window_start:m.start()]
            if re.search(r'log\.(info|warning|error)\(\s*\n?\s*["\']?$',
                          window + '"'):
                # The token immediately follows a log.* call (with optional
                # newline/whitespace and opening quote).
                return True
            # Cheaper substring check: any log.* call within the preceding 300
            # chars AND no `def ` between them (meaning we're still inside the
            # same function).
            if ("log.info(" in window or "log.warning(" in window
                    or "log.error(" in window):
                tail = window[max(window.rfind("log.info("),
                                  window.rfind("log.warning("),
                                  window.rfind("log.error(")):]
                if "def " not in tail and "class " not in tail:
                    return True
        return False

    def test_armed_token_emitted_from_monitor(self):
        assert self._emits_token_in_log_call("ENTRY_RETRY_ARMED"), \
            "ENTRY_RETRY_ARMED must be inside a log.* call in order_monitor"

    def test_submitted_token_emitted_from_monitor(self):
        assert self._emits_token_in_log_call("ENTRY_RETRY_SUBMITTED"), \
            "ENTRY_RETRY_SUBMITTED must be inside a log.* call in order_monitor"

    def test_aborted_token_emitted_from_monitor(self):
        assert self._emits_token_in_log_call("ENTRY_RETRY_ABORTED"), \
            "ENTRY_RETRY_ABORTED must be inside a log.* call in order_monitor"


# ============================================================
# 5. decision_event emissions include post_cancel_retry stage
# ============================================================

class TestDecisionEventsEmitted:
    def test_stage_post_cancel_retry_present(self):
        assert 'stage="post_cancel_retry"' in OM_SRC, \
            "stage='post_cancel_retry' must be used"

    def test_arm_decision_emitted(self):
        assert re.search(
            r'_emit_order_event\([^)]*decision="ARM"',
            OM_SRC, re.DOTALL,
        ), "ARM decision must be emitted"

    def test_abort_decision_emitted(self):
        assert re.search(
            r'_emit_order_event\([^)]*decision="ABORT"',
            OM_SRC, re.DOTALL,
        ), "ABORT decision must be emitted"

    def test_submit_decision_emitted(self):
        assert re.search(
            r'_emit_order_event\([^)]*decision="SUBMIT"',
            OM_SRC, re.DOTALL,
        ), "SUBMIT decision must be emitted"


# ============================================================
# 6. ENTRY_RETRY_ENABLED gate
# ============================================================

class TestEnabledGate:
    def test_constant_declared(self):
        assert re.search(
            r'ENTRY_RETRY_ENABLED\s*=\s*os\.getenv\(\s*"ENTRY_RETRY_ENABLED"',
            OM_SRC,
        )

    def test_arm_path_gated(self):
        m = re.search(
            r"def _maybe_arm_post_cancel_retry.*?def _check_armed_retries",
            OM_SRC, re.DOTALL,
        )
        assert m
        assert "if not ENTRY_RETRY_ENABLED:" in m.group(0), \
            "arm path must short-circuit when ENTRY_RETRY_ENABLED is off"

    def test_poll_path_gated(self):
        m = re.search(
            r"def _check_armed_retries.*?def _submit_armed_retry",
            OM_SRC, re.DOTALL,
        )
        assert m
        assert "if not ENTRY_RETRY_ENABLED:" in m.group(0), \
            "poll path must short-circuit when ENTRY_RETRY_ENABLED is off"


# ============================================================
# 7. Behavioral: ARM path persists ARMED meta + emits event
# ============================================================

@pytest.fixture
def monitor_with_mocks():
    from ap.order_monitor import APOrderMonitor

    osm = MagicMock()
    broker = MagicMock()
    pm = MagicMock()

    m = APOrderMonitor(
        client_id="client-A",
        broker=broker,
        order_state_machine=osm,
        position_manager=pm,
    )

    m._emitted = []
    def _record_emit(**kwargs):
        m._emitted.append(kwargs)
    m._emit_order_event = _record_emit
    return m, osm, broker


class TestArmPathBehavior:
    def test_arm_emits_armed_event_and_persists_meta(self, monitor_with_mocks, monkeypatch):
        m, osm, broker = monitor_with_mocks

        osm.get_order.return_value = {
            "local_order_id": "loc-1",
            "symbol": "QCOM",
            "direction": "CALL",
            "meta": {
                "signal_entry_price": 185.0,
                "retry_attempts": 0,
                "score": 90.0,
                "ticker": "QCOM",
                "signal_id": "sig-1",
                "source": "scanner",
            },
        }
        broker.get_quote.return_value = {"last": 185.05}

        captured = {}
        def fake_update_order(local_oid, **kwargs):
            captured["local_oid"] = local_oid
            captured.update(kwargs)
        monkeypatch.setattr("ap.db.update_order", fake_update_order)

        m._maybe_arm_post_cancel_retry(
            local_order_id="loc-1",
            contract="QCOM260523C00185000",
            cancel_reason="entry_max_age_normal_reached",
        )

        assert captured.get("local_oid") == "loc-1"
        meta = captured.get("meta") or {}
        assert meta.get("retry_status") == "ARMED"
        assert meta.get("retry_attempt") == 1
        assert meta.get("retry_ready_at") is not None
        assert 15.0 <= float(meta.get("retry_wait_secs", 0)) <= 30.0
        payload = meta.get("retry_payload") or {}
        assert payload.get("ticker") == "QCOM"
        assert payload.get("direction") == "CALL"
        assert payload.get("retry_attempt") == 1

        arm_events = [e for e in m._emitted if e.get("decision") == "ARM"]
        assert len(arm_events) == 1
        assert arm_events[0].get("stage") == "post_cancel_retry"
        assert arm_events[0].get("reason_code") == "RETRY_ARMED"

    def test_abort_path_emits_aborted_event(self, monitor_with_mocks, monkeypatch):
        m, osm, broker = monitor_with_mocks
        osm.get_order.return_value = {
            "local_order_id": "loc-2",
            "symbol": "QCOM",
            "direction": "CALL",
            "meta": {"signal_entry_price": 185.0},
        }
        broker.get_quote.return_value = {"last": 185.0}

        captured = {}
        def fake_update_order(local_oid, **kwargs):
            captured["local_oid"] = local_oid
            captured.update(kwargs)
        monkeypatch.setattr("ap.db.update_order", fake_update_order)

        m._maybe_arm_post_cancel_retry(
            local_order_id="loc-2",
            contract="QCOM260523C00185000",
            cancel_reason="thesis_invalid",
        )

        abort_events = [e for e in m._emitted if e.get("decision") == "ABORT"]
        assert len(abort_events) == 1
        assert abort_events[0].get("reason_code") == "NON_RETRYABLE_REASON"
        assert abort_events[0].get("stage") == "post_cancel_retry"
        meta = captured.get("meta") or {}
        assert meta.get("retry_status") == "ABORTED"


# ============================================================
# 8. Runtime gate disables both paths
# ============================================================

class TestRuntimeGate:
    def test_disabled_short_circuits_arm(self, monitor_with_mocks, monkeypatch):
        m, osm, broker = monitor_with_mocks
        osm.get_order.return_value = {"meta": {}}
        broker.get_quote.return_value = {"last": 185.0}
        monkeypatch.setattr("ap.order_monitor.ENTRY_RETRY_ENABLED", False)

        m._maybe_arm_post_cancel_retry(
            local_order_id="loc-x",
            contract="QCOM260523C00185000",
            cancel_reason="entry_max_age_normal_reached",
        )
        assert m._emitted == [], \
            "disabled gate must short-circuit before any emission"


# ============================================================
# 9. Submit step
# ============================================================

class TestSubmitArmedRetry:
    def test_submit_calls_process_signal(self, monitor_with_mocks, monkeypatch):
        m, osm, broker = monitor_with_mocks

        captured = {}
        def fake_update_order(local_oid, **kwargs):
            captured["local_oid"] = local_oid
            captured.update(kwargs)
        monkeypatch.setattr("ap.db.update_order", fake_update_order)

        # Stub process_signal to return a success result.
        def fake_process_signal(broker, client_id, payload):
            return {
                "ok": True,
                "local_order_id": "loc-retry-1",
                "broker_order_id": "br-retry-1",
                "contract": "QCOM260523C00185000",
                "qty": 9,
                "submit_limit": 3.08,
                "selector_ask": 3.05,
                "submit_ask": 3.08,
            }
        monkeypatch.setattr("ap.execution.process_signal", fake_process_signal)

        m._submit_armed_retry(
            local_order_id="loc-prev",
            contract="QCOM260523C00185000",
            retry_payload={"ticker": "QCOM", "direction": "CALL",
                           "score": 90, "retry_attempt": 1,
                           "signal_entry_price": 185.0},
            prior_meta={"signal_entry_price": 185.0},
        )

        # SUBMIT event emitted
        submit_events = [e for e in m._emitted if e.get("decision") == "SUBMIT"]
        assert len(submit_events) == 1
        assert submit_events[0].get("reason_code") == "RETRY_SUBMITTED"

        # Meta stamped with SUBMITTED and the new ids
        meta = captured.get("meta") or {}
        assert meta.get("retry_status") == "SUBMITTED"
        assert meta.get("retry_new_local_order_id") == "loc-retry-1"

    def test_submit_failure_marks_failed(self, monitor_with_mocks, monkeypatch):
        m, osm, broker = monitor_with_mocks

        captured = {}
        def fake_update_order(local_oid, **kwargs):
            captured["local_oid"] = local_oid
            captured.update(kwargs)
        monkeypatch.setattr("ap.db.update_order", fake_update_order)

        def fake_process_signal(broker, client_id, payload):
            return {"ok": False, "error": "symbol_locked"}
        monkeypatch.setattr("ap.execution.process_signal", fake_process_signal)

        m._submit_armed_retry(
            local_order_id="loc-prev",
            contract="QCOM260523C00185000",
            retry_payload={"ticker": "QCOM", "direction": "CALL"},
            prior_meta={},
        )

        abort_events = [e for e in m._emitted if e.get("decision") == "ABORT"]
        assert len(abort_events) == 1
        assert abort_events[0].get("reason_code") == "SUBMIT_REJECT"

        meta = captured.get("meta") or {}
        assert meta.get("retry_status") == "FAILED"

    def test_malformed_payload_marks_failed(self, monitor_with_mocks, monkeypatch):
        m, _, _ = monitor_with_mocks
        captured = {}
        def fake_update_order(local_oid, **kwargs):
            captured["local_oid"] = local_oid
            captured.update(kwargs)
        monkeypatch.setattr("ap.db.update_order", fake_update_order)

        m._submit_armed_retry(
            local_order_id="loc-prev",
            contract="QCOM260523C00185000",
            retry_payload={},  # no ticker
            prior_meta={},
        )

        meta = captured.get("meta") or {}
        assert meta.get("retry_status") == "FAILED"
        assert meta.get("retry_status_detail") == "malformed_payload"
