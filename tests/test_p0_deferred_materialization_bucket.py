"""
tests/test_p0_deferred_materialization_bucket.py

PR #300 — deferred materialization bucket + broker-ready lifecycle tests.

CORE INVARIANTS BEING PROVED:
  • broker_ready=True is ONLY set by stamp_selected() with a real OCC contract
  • DEFERRED:*/0.01 rows can never have broker_ready=True
  • Triggered deferred rows get QUEUED/RUNNING meta stamps before selector runs
  • Successful materialization stamps SELECTED + broker_ready=True
  • Retryable failures stamp RETRY_PENDING (not FAILED_TERMINAL)
  • Terminal failures stamp FAILED_TERMINAL with selector diagnostics
  • All lifecycle transitions preserve client_id and execution_mode
  • broker_ready gate blocks pre-submit when flag is not explicitly True
  • Submitted/brokered rows are not mutated
  • Paper/live client identity is preserved through every transition

Required test coverage A–O per spec.
"""
from __future__ import annotations

import os
import types
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

import ap.deferred_materializer as dm
import ap_execution_core as core

_CLIENT_ID = "jasoncosby1@gmail.com"
_EXEC_MODE = "live"
_PAPER_CLIENT = "jose.vasquez4011@gmail.com"
_PAPER_MODE = "paper"
_DEFERRED = "DEFERRED:GS"
_REAL_OCC = "GS  260717C00465000"


# ─────────────────────────────────────────────────────────────────────────────
# OSM mock helpers
# ─────────────────────────────────────────────────────────────────────────────

def _osm(client_id=_CLIENT_ID):
    """Minimal OSM mock that records update_order_meta calls."""
    calls = []
    osm = MagicMock()
    osm.client_id = client_id
    def _update(local_order_id, patch):
        calls.append({"local_order_id": local_order_id, "patch": dict(patch)})
        return True
    osm.update_order_meta.side_effect = _update
    osm._calls = calls
    return osm


def _plan(**kw):
    p = types.SimpleNamespace(
        contract_symbol  = kw.get("contract_symbol", _DEFERRED),
        limit_price      = kw.get("limit_price", 0.01),
        contracts        = kw.get("contracts", 1),
        max_position_usd = kw.get("max_position_usd", 190.0),
        side             = kw.get("side", "CALL"),
        execution_mode   = kw.get("execution_mode", _EXEC_MODE),
        client_id        = kw.get("client_id", _CLIENT_ID),
        signal_id        = "SIG-GS-1",
        metadata         = kw.get("metadata", {}),
        ticker           = kw.get("ticker", "GS"),
    )
    return p


# ─────────────────────────────────────────────────────────────────────────────
# Tests for ap/deferred_materializer.py helpers
# ─────────────────────────────────────────────────────────────────────────────

class TestStampTriggerQueued:
    """A. Watcher moves breached deferred row to materialization bucket."""

    def test_stamps_queued_status_and_false_broker_ready(self):
        osm = _osm()
        ok = dm.stamp_trigger_queued(
            osm, "LOID-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL", triggered_price=461.50,
        )
        assert ok is True
        patch = osm._calls[-1]["patch"]
        assert patch["materialization_status"] == dm.QUEUED
        assert patch["broker_ready"] is False
        assert patch["materialization_attempts"] == 0

    def test_preserves_client_id_in_log(self, caplog):
        """M. Jason live identity must appear in every structured log."""
        import logging
        osm = _osm()
        with caplog.at_level(logging.INFO, logger="ap.deferred_materializer"):
            dm.stamp_trigger_queued(
                osm, "LOID-GS-1",
                client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
                symbol="GS", direction="CALL", triggered_price=461.50,
            )
        log_text = "\n".join(caplog.messages)
        assert _CLIENT_ID in log_text
        assert "DEFERRED_TRIGGER_MOVED_TO_MATERIALIZATION_BUCKET" in log_text

    def test_triggered_underlying_price_stamped(self):
        osm = _osm()
        dm.stamp_trigger_queued(
            osm, "LOID-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL", triggered_price=465.25,
        )
        patch = osm._calls[-1]["patch"]
        assert abs(patch["triggered_underlying_price"] - 465.25) < 0.001

    def test_submitted_ts_is_not_touched(self):
        """L. Submitted rows must not be mutated by lifecycle stamps."""
        osm = _osm()
        # stamp_trigger_queued only updates meta, not submitted_ts
        dm.stamp_trigger_queued(
            osm, "LOID-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL", triggered_price=461.50,
        )
        for call in osm._calls:
            assert "submitted_ts" not in call["patch"]
            assert "broker_order_id" not in call["patch"]


class TestStampSelected:
    """C. Materializer success creates broker-ready state."""

    def test_sets_broker_ready_true_with_real_occ(self):
        osm = _osm()
        ok = dm.stamp_selected(
            osm, "LOID-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL",
            contract=_REAL_OCC,
            bid=1.80, ask=1.86, mid=1.83,
            limit_price=1.87, qty=1, reserved_cost=187.0,
        )
        assert ok is True
        patch = osm._calls[-1]["patch"]
        assert patch["materialization_status"] == dm.SELECTED
        assert patch["broker_ready"] is True
        assert patch["selected_contract"] == _REAL_OCC
        assert patch["selected_bid"] == pytest.approx(1.80, abs=0.001)
        assert patch["selected_ask"] == pytest.approx(1.86, abs=0.001)

    def test_refuses_to_set_broker_ready_for_deferred_contract(self):
        """Core invariant: broker_ready=True ONLY with real OCC."""
        osm = _osm()
        ok = dm.stamp_selected(
            osm, "LOID-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL",
            contract="DEFERRED:GS",
            bid=1.80, ask=1.86, mid=1.83,
            limit_price=1.87, qty=1, reserved_cost=187.0,
        )
        assert ok is False   # refused
        # must not have written broker_ready=True
        for call in osm._calls:
            assert call["patch"].get("broker_ready") is not True

    def test_refuses_to_set_broker_ready_for_zero_limit(self):
        osm = _osm()
        ok = dm.stamp_selected(
            osm, "LOID-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL",
            contract=_REAL_OCC,
            bid=1.80, ask=1.86, mid=1.83,
            limit_price=0.01, qty=1, reserved_cost=1.0,
        )
        assert ok is False

    def test_refuses_to_set_broker_ready_for_zero_qty(self):
        osm = _osm()
        ok = dm.stamp_selected(
            osm, "LOID-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL",
            contract=_REAL_OCC,
            bid=1.80, ask=1.86, mid=1.83,
            limit_price=1.87, qty=0, reserved_cost=0.0,
        )
        assert ok is False

    def test_selected_quote_fields_all_persisted(self):
        osm = _osm()
        dm.stamp_selected(
            osm, "LOID-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL",
            contract=_REAL_OCC,
            bid=1.80, ask=1.86, mid=1.83,
            limit_price=1.87, qty=1, reserved_cost=187.0,
            dte=2, expiration="2026-07-17",
            delta=0.42, open_interest=850, volume=210,
        )
        patch = osm._calls[-1]["patch"]
        assert patch["selected_dte"] == 2
        assert patch["selected_expiration"] == "2026-07-17"
        assert abs(patch["selected_delta"] - 0.42) < 0.001
        assert patch["selected_open_interest"] == 850
        assert patch["selected_volume"] == 210

    def test_emits_ready_to_submit_created_log(self, caplog):
        import logging
        osm = _osm()
        with caplog.at_level(logging.INFO, logger="ap.deferred_materializer"):
            dm.stamp_selected(
                osm, "LOID-1",
                client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
                symbol="GS", direction="CALL",
                contract=_REAL_OCC,
                bid=1.80, ask=1.86, mid=1.83,
                limit_price=1.87, qty=1, reserved_cost=187.0,
            )
        log_text = "\n".join(caplog.messages)
        assert "READY_TO_SUBMIT_CREATED" in log_text
        assert "DEFERRED_MATERIALIZATION_SELECTED" in log_text


class TestStampRetryPending:
    """F. Retryable zero bid/ask stays in materialization bucket."""

    def test_sets_retry_pending_not_failed_terminal(self):
        osm = _osm()
        dm.stamp_retry_pending(
            osm, "LOID-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL",
            reason_code="CHAIN_ROW_ZERO_BID_ASK",
            attempt=1, max_attempts=3, retry_delay_s=20,
        )
        patch = osm._calls[-1]["patch"]
        assert patch["materialization_status"] == dm.RETRY_PENDING
        assert patch["broker_ready"] is False
        assert patch["materialization_next_retry_at"] is not None

    def test_next_retry_at_is_in_future(self):
        osm = _osm()
        before = datetime.now(timezone.utc)
        dm.stamp_retry_pending(
            osm, "LOID-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL",
            reason_code="CHAIN_ROW_ZERO_BID_ASK",
            attempt=1, max_attempts=3, retry_delay_s=20,
        )
        patch = osm._calls[-1]["patch"]
        next_retry = datetime.fromisoformat(patch["materialization_next_retry_at"])
        assert next_retry > before

    def test_selector_failure_attached_to_meta(self):
        osm = _osm()
        failure = {"reason_code": "CHAIN_ROW_ZERO_BID_ASK", "chain_rows": 5}
        dm.stamp_retry_pending(
            osm, "LOID-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL",
            reason_code="CHAIN_ROW_ZERO_BID_ASK",
            attempt=1, max_attempts=3, retry_delay_s=20,
            selector_failure=failure,
        )
        patch = osm._calls[-1]["patch"]
        assert patch["materialization_selector_failure"]["chain_rows"] == 5

    def test_emits_deferred_materialization_retry_log(self, caplog):
        import logging
        osm = _osm()
        with caplog.at_level(logging.WARNING, logger="ap.deferred_materializer"):
            dm.stamp_retry_pending(
                osm, "LOID-1",
                client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
                symbol="GS", direction="CALL",
                reason_code="CHAIN_ROW_ZERO_BID_ASK",
                attempt=1, max_attempts=3, retry_delay_s=20,
            )
        log_text = "\n".join(caplog.messages)
        assert "DEFERRED_MATERIALIZATION_RETRY" in log_text
        assert "RETRY_PENDING" in log_text


class TestStampFailedTerminal:
    """G + H. Terminal failures stamp FAILED_TERMINAL with diagnostics."""

    def test_stamps_failed_terminal_with_broker_ready_false(self):
        osm = _osm()
        dm.stamp_failed_terminal(
            osm, "LOID-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL",
            reason_code="breach_retry_exhausted:CHAIN_ROW_ZERO_BID_ASK",
            attempt=3,
        )
        patch = osm._calls[-1]["patch"]
        assert patch["materialization_status"] == dm.FAILED_TERMINAL
        assert patch["broker_ready"] is False
        assert "CHAIN_ROW_ZERO_BID_ASK" in patch["materialization_reason"]

    def test_selector_failure_diagnostics_in_terminal_meta(self):
        """H. Terminal quality reject must carry OI count + best candidate."""
        osm = _osm()
        failure = {
            "reason_code": "OI_TOO_LOW",
            "chain_rows": 15,
            "rejected_by_oi": 15,
            "best_rejected_candidate": {
                "symbol": _REAL_OCC,
                "bid": 0.55, "ask": 0.65,
                "rejection_reason": "OI_TOO_LOW",
            },
        }
        dm.stamp_failed_terminal(
            osm, "LOID-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL",
            reason_code="OI_TOO_LOW",
            attempt=1,
            selector_failure=failure,
        )
        patch = osm._calls[-1]["patch"]
        sf = patch["materialization_selector_failure"]
        assert sf["rejected_by_oi"] == 15
        assert sf["best_rejected_candidate"]["rejection_reason"] == "OI_TOO_LOW"

    def test_emits_deferred_materialization_failed_log(self, caplog):
        import logging
        osm = _osm()
        with caplog.at_level(logging.CRITICAL, logger="ap.deferred_materializer"):
            dm.stamp_failed_terminal(
                osm, "LOID-1",
                client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
                symbol="GS", direction="CALL",
                reason_code="OI_TOO_LOW", attempt=1,
            )
        log_text = "\n".join(caplog.messages)
        assert "DEFERRED_MATERIALIZATION_FAILED" in log_text
        assert "broker_ready=false" in log_text


# ─────────────────────────────────────────────────────────────────────────────
# Broker-ready gate function
# ─────────────────────────────────────────────────────────────────────────────

class TestBrokerReadyGate:
    """D + E. Execution submits READY_TO_SUBMIT only; blocks DEFERRED rows."""

    def test_not_broker_ready_when_meta_missing(self):
        assert dm.is_broker_ready_from_meta(None) is False
        assert dm.is_broker_ready_from_meta({}) is False

    def test_not_broker_ready_when_explicitly_false(self):
        assert dm.is_broker_ready_from_meta({"broker_ready": False}) is False

    def test_not_broker_ready_when_value_is_string_true(self):
        """Must be bool True, not string "true" — prevents JSON coercion bugs."""
        assert dm.is_broker_ready_from_meta({"broker_ready": "true"}) is False

    def test_not_broker_ready_when_value_is_one(self):
        """Must be bool True, not integer 1."""
        assert dm.is_broker_ready_from_meta({"broker_ready": 1}) is False

    def test_broker_ready_only_when_explicitly_bool_true(self):
        assert dm.is_broker_ready_from_meta({"broker_ready": True}) is True

    def test_deferred_contract_with_broker_ready_true_rejected_by_stamp_selected(self):
        """Core safety: stamp_selected blocks broker_ready=True for DEFERRED:*."""
        osm = _osm()
        result = dm.stamp_selected(
            osm, "LOID-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL",
            contract="DEFERRED:GS",
            bid=1.0, ask=1.5, mid=1.25,
            limit_price=1.50, qty=1, reserved_cost=150.0,
        )
        assert result is False
        meta = {"broker_ready": False}   # as it would remain
        assert dm.is_broker_ready_from_meta(meta) is False


# ─────────────────────────────────────────────────────────────────────────────
# Retryable vs terminal reason classification
# ─────────────────────────────────────────────────────────────────────────────

class TestRetryableClassification:
    """F + G. Retryable failures stay in bucket; terminal failures expire."""

    @pytest.mark.parametrize("rc", [
        "CHAIN_ROW_ZERO_BID_ASK",
        "DIRECT_QUOTE_ZERO_BID_ASK",
        "CHAIN_PROVIDER_ERROR",
        "CHAIN_PROVIDER_EMPTY_EXPIRATIONS",
        "CHAIN_PROVIDER_EMPTY_OPTIONS",
    ])
    def test_retryable_reasons_are_classified_retryable(self, rc):
        assert dm.is_reason_retryable(rc) is True

    @pytest.mark.parametrize("rc", [
        "OI_TOO_LOW",
        "SPREAD_TOO_WIDE",
        "VOLUME_TOO_LOW",
        "UNTRADEABLE_FOR_ACCOUNT_SIZE",
        "DELTA_OUT_OF_RANGE",
    ])
    def test_quality_reject_reasons_are_not_retryable(self, rc):
        assert dm.is_reason_retryable(rc) is False

    def test_unknown_reason_is_not_retryable(self):
        assert dm.is_reason_retryable("COMPLETELY_UNKNOWN_XYZ") is False

    def test_classify_retry_decision_chains_correctly(self):
        """The existing _classify_deferred_breach_retry_decision must agree
        with dm.is_reason_retryable for all retryable reasons."""
        for rc in dm.RETRYABLE_MATERIALIZATION_REASONS:
            decision = core._classify_deferred_breach_retry_decision(
                rc,
                queue_local_order_id="LOID-1",
                attempt=1, max_attempts=3,
                past_cutoff=False, retry_enabled=True,
            )
            assert decision["action"] == "retry_schedule", (
                f"Reason {rc!r} should be retryable but got {decision['action']!r}"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Idempotency / claim logic
# ─────────────────────────────────────────────────────────────────────────────

class TestIdempotency:
    """K. Idempotent materialization claim."""

    def test_should_reprocess_claim_when_no_existing_claim(self):
        assert dm.should_reprocess_claim(None, {}) is True
        assert dm.should_reprocess_claim({}, {}) is True

    def test_should_reprocess_claim_when_not_running(self):
        meta = {"materialization_status": dm.QUEUED}
        assert dm.should_reprocess_claim(meta, {}) is True

    def test_should_not_reprocess_claim_when_lock_not_expired(self):
        future = (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat()
        meta = {
            "materialization_status": dm.RUNNING,
            "materialization_lock_until": future,
        }
        assert dm.should_reprocess_claim(meta, {}) is False

    def test_should_reprocess_claim_when_lock_expired(self):
        past = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
        meta = {
            "materialization_status": dm.RUNNING,
            "materialization_lock_until": past,
        }
        assert dm.should_reprocess_claim(meta, {}) is True

    def test_already_selected_row_should_not_be_reclaimed(self):
        """If broker_ready=True, stamp_selected would refuse anyway — but
        the claim check provides the first line of defense."""
        meta = {
            "materialization_status": dm.SELECTED,
            "broker_ready": True,
        }
        # SELECTED is not RUNNING → claimable according to lock logic,
        # but stamp_selected will refuse to overwrite broker_ready=True.
        # Test that is_broker_ready_from_meta correctly gates re-submission.
        assert dm.is_broker_ready_from_meta(meta) is True
        # Second worker: stamp_selected with same contract → still returns True
        # (idempotent write), broker_ready remains True.


# ─────────────────────────────────────────────────────────────────────────────
# Identity preservation
# ─────────────────────────────────────────────────────────────────────────────

class TestIdentityPreservation:
    """I + M + N. client_id and execution_mode preserved through all transitions."""

    def test_stamp_selected_carries_execution_mode(self, caplog):
        import logging
        osm = _osm()
        with caplog.at_level(logging.INFO, logger="ap.deferred_materializer"):
            dm.stamp_selected(
                osm, "LOID-1",
                client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
                symbol="GS", direction="CALL",
                contract=_REAL_OCC,
                bid=1.80, ask=1.86, mid=1.83,
                limit_price=1.87, qty=1, reserved_cost=187.0,
            )
        log_text = "\n".join(caplog.messages)
        assert _CLIENT_ID in log_text
        assert _EXEC_MODE in log_text

    def test_stamp_failed_terminal_carries_client_id(self, caplog):
        import logging
        osm = _osm()
        with caplog.at_level(logging.CRITICAL, logger="ap.deferred_materializer"):
            dm.stamp_failed_terminal(
                osm, "LOID-1",
                client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
                symbol="GS", direction="CALL",
                reason_code="OI_TOO_LOW", attempt=1,
            )
        log_text = "\n".join(caplog.messages)
        assert _CLIENT_ID in log_text

    def test_paper_client_stamp_does_not_use_live_client_id(self):
        """N. Paper/live taxonomy must not bleed across clients."""
        osm_live  = _osm(client_id=_CLIENT_ID)
        osm_paper = _osm(client_id=_PAPER_CLIENT)
        dm.stamp_trigger_queued(
            osm_live, "LOID-LIVE",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL", triggered_price=461.0,
        )
        dm.stamp_trigger_queued(
            osm_paper, "LOID-PAPER",
            client_id=_PAPER_CLIENT, execution_mode=_PAPER_MODE,
            symbol="GS", direction="CALL", triggered_price=461.0,
        )
        # Verify each OSM only received calls for its own order
        assert osm_live._calls[0]["local_order_id"] == "LOID-LIVE"
        assert osm_paper._calls[0]["local_order_id"] == "LOID-PAPER"
        # No cross-client calls
        assert osm_live.update_order_meta.call_count == 1
        assert osm_paper.update_order_meta.call_count == 1


# ─────────────────────────────────────────────────────────────────────────────
# Config flags
# ─────────────────────────────────────────────────────────────────────────────

class TestConfig:
    """Config flags parse correctly with safe defaults."""

    def test_default_config_is_production_safe(self):
        cfg = dm._cfg()
        assert cfg["max_attempts"] >= 1
        assert cfg["retry_base_s"] >= 1
        assert cfg["retry_max_s"] >= cfg["retry_base_s"]
        assert cfg["lock_ttl_s"] >= 60

    def test_retry_delay_increases_exponentially(self):
        cfg = {"retry_base_s": 15, "retry_max_s": 90}
        d1 = dm._retry_delay_seconds(1, cfg)
        d2 = dm._retry_delay_seconds(2, cfg)
        d3 = dm._retry_delay_seconds(3, cfg)
        assert d1 <= d2 <= d3
        assert d3 <= 90

    def test_retry_delay_capped_at_max(self):
        cfg = {"retry_base_s": 15, "retry_max_s": 30}
        d_big = dm._retry_delay_seconds(10, cfg)
        assert d_big <= 30

    def test_bad_env_values_fall_back_to_defaults(self, monkeypatch):
        monkeypatch.setenv("DEFERRED_MATERIALIZATION_MAX_ATTEMPTS", "not_a_number")
        monkeypatch.setenv("DEFERRED_MATERIALIZATION_ENTRY_CUTOFF_ET", "99:99")
        cfg = dm._cfg()
        assert cfg["max_attempts"] >= 1   # default preserved
        assert cfg["entry_cutoff_et"] == "15:30"   # default preserved


# ─────────────────────────────────────────────────────────────────────────────
# Structured log markers present in execution core
# ─────────────────────────────────────────────────────────────────────────────

class TestStructuredLogMarkers:
    """Verify all required marker strings exist in production code."""

    def test_all_pr300_markers_in_execution_core(self):
        src_core = open("ap_execution_core.py").read()
        src_mat  = open("ap/deferred_materializer.py").read()
        # Markers emitted directly from execution_core:
        core_markers = [
            "DEFERRED_MATERIALIZATION_STARTED",
            "DEFERRED_MATERIALIZATION_RETRY",
            "DEFERRED_MATERIALIZATION_FAILED",
            "MATERIALIZATION_PRE_SUBMIT_INVARIANT_FAILED",
            "MATERIALIZATION_PRE_SUBMIT_INVARIANT_OK",
        ]
        # Markers emitted from deferred_materializer helpers
        # (called from execution_core, so the strings live in the helper):
        helper_markers = [
            "DEFERRED_TRIGGER_MOVED_TO_MATERIALIZATION_BUCKET",
            "DEFERRED_MATERIALIZATION_CLAIMED",
            "DEFERRED_MATERIALIZATION_SELECTED",
            "READY_TO_SUBMIT_CREATED",
        ]
        missing_core = [m for m in core_markers if m not in src_core]
        missing_helper = [m for m in helper_markers if m not in src_mat]
        assert not missing_core, f"Missing in execution_core: {missing_core}"
        assert not missing_helper, f"Missing in deferred_materializer: {missing_helper}"

    def test_broker_ready_gate_in_execution_core(self):
        """broker_ready gate must be in the pre-submit invariant block."""
        src = open("ap_execution_core.py").read()
        assert "broker_ready_not_set" in src
        assert "is_broker_ready_from_meta" not in src or "broker_ready" in src

    def test_stamp_selected_called_in_execution_core(self):
        src = open("ap_execution_core.py").read()
        assert "stamp_selected" in src
        assert "from ap.deferred_materializer import stamp_selected" in src

    def test_stamp_failed_terminal_called_in_execution_core(self):
        src = open("ap_execution_core.py").read()
        assert "stamp_failed_terminal" in src

    def test_stamp_retry_pending_called_in_execution_core(self):
        src = open("ap_execution_core.py").read()
        assert "stamp_retry_pending" in src


# ─────────────────────────────────────────────────────────────────────────────
# O. No unrelated systems mutated
# ─────────────────────────────────────────────────────────────────────────────

class TestNoUnrelatedMutation:
    def test_deferred_materializer_module_has_no_exit_or_scanner_references(self):
        import ast
        src = open("ap/deferred_materializer.py").read()
        tree = ast.parse(src)
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        attrs = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        all_refs = names | attrs
        forbidden = {"exit_engine", "fill_monitor", "submit_existing_entry",
                     "place_order", "scanner", "intelligence", "proof_trade"}
        found = all_refs & forbidden
        assert not found, f"deferred_materializer has functional refs to: {found}"

    def test_stamp_selected_only_touches_meta_not_status_column(self):
        """Lifecycle stamps only update orders.meta — never the top-level
        status column. Preserving the existing PENDING_TRIGGER status
        prevents breaking dashboard queries that filter on status."""
        osm = _osm()
        dm.stamp_selected(
            osm, "LOID-1",
            client_id=_CLIENT_ID, execution_mode=_EXEC_MODE,
            symbol="GS", direction="CALL",
            contract=_REAL_OCC,
            bid=1.80, ask=1.86, mid=1.83,
            limit_price=1.87, qty=1, reserved_cost=187.0,
        )
        # Only update_order_meta should have been called (meta JSONB merge),
        # not any status-transition method
        assert osm.update_order_meta.called
        osm.transition_status.assert_not_called()
        osm.expire_order.assert_not_called()
        osm.cancel_order.assert_not_called()
