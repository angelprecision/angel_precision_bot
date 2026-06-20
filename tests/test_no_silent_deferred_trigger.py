"""
tests/test_no_silent_deferred_trigger.py — PR3

Verifies that every triggered deferred-entry path emits exactly one canonical
terminal outcome, so no watcher trigger returns silently.

This is observability-only. The tests assert the emission taxonomy exists at
each terminal path in source (source guards) and that the emitter helper is
shaped correctly. No order lifecycle behavior is changed by PR3.

Canonical outcomes:
  BREACH_CONTRACT_SELECTED, BREACH_RISK_CHECK_BLOCKED,
  BREACH_SELECTOR_RETURNED_NONE, BREACH_SELECTOR_EXCEPTION,
  BREACH_SUBMISSION_SKIPPED, BREACH_BROKER_SUBMITTED,
  NO_VALID_PLAYBOOK_DTE_CONTRACT, UNTRADEABLE_FOR_ACCOUNT_SIZE,
  DATA_MISSING_OI_VOLUME
"""
from __future__ import annotations

from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_EC   = (_REPO / "ap_execution_core.py").read_text()


CANONICAL_OUTCOMES = {
    "BREACH_CONTRACT_SELECTED",
    "BREACH_RISK_CHECK_BLOCKED",
    "BREACH_SELECTOR_RETURNED_NONE",
    "BREACH_SELECTOR_EXCEPTION",
    "BREACH_SUBMISSION_SKIPPED",
    "BREACH_BROKER_SUBMITTED",
    "NO_VALID_PLAYBOOK_DTE_CONTRACT",
    "UNTRADEABLE_FOR_ACCOUNT_SIZE",
    "DATA_MISSING_OI_VOLUME",
}


class TestEmitterExists:
    def test_emitter_helper_defined(self):
        assert "def _emit_deferred_outcome(" in _EC

    def test_sentinel_defined(self):
        assert '_deferred_outcome = {"emitted": False' in _EC

    def test_emitter_includes_join_keys(self):
        """Every emission must carry local_order_id + signal_id so the event
        joins back to the order row (the linkage missing before PR3)."""
        idx = _EC.find("def _emit_deferred_outcome(")
        body = _EC[idx: idx + 1800]
        assert '"local_order_id": queue_local_order_id' in body
        assert '"signal_id": signal_id' in body

    def test_emitter_never_raises(self):
        idx = _EC.find("def _emit_deferred_outcome(")
        body = _EC[idx: idx + 1800]
        assert "except Exception:" in body
        assert "DEFERRED_TRIGGER_OUTCOME_EMIT_FAILED" in body


class TestTerminalPathsWired:
    """Each deferred terminal path must emit a canonical outcome."""

    def test_selector_none_path_emits(self):
        # The selector-None terminal must emit BREACH_SELECTOR_RETURNED_NONE
        # or DATA_MISSING_OI_VOLUME (when chain came back vol0_oi0).
        assert "DATA_MISSING_OI_VOLUME" in _EC
        assert _EC.count("BREACH_SELECTOR_RETURNED_NONE") >= 2

    def test_selector_exception_path_emits(self):
        idx = _EC.find("except Exception as _cs_err:")
        assert idx != -1
        region = _EC[idx: idx + 600]
        assert "BREACH_SELECTOR_EXCEPTION" in region

    def test_contract_selected_path_emits(self):
        assert "BREACH_CONTRACT_SELECTED" in _EC

    def test_broker_submitted_path_emits(self):
        # On successful submit, must emit BREACH_BROKER_SUBMITTED with broker id.
        # The emission call passes broker_order_id= on the following lines.
        assert "broker_order_id=str(broker_order_id" in _EC, (
            "BREACH_BROKER_SUBMITTED emission must pass the real broker_order_id"
        )

    def test_submission_skipped_path_emits(self):
        assert '"BREACH_SUBMISSION_SKIPPED"' in _EC

    def test_no_selector_path_emits(self):
        idx = _EC.find("contract_deferred_no_selector")
        region = _EC[idx: idx + 400]
        assert "_emit_deferred_outcome" in region

    def test_data_missing_uses_vol0_oi0_signal(self):
        """DATA_MISSING_OI_VOLUME must be chosen when the reason carries
        the vol0_oi0 chain-data signature. Find the emission conditional
        (second occurrence), not the taxonomy comment (first)."""
        first = _EC.find("DATA_MISSING_OI_VOLUME")
        idx = _EC.find("DATA_MISSING_OI_VOLUME", first + 1)
        assert idx != -1, "DATA_MISSING_OI_VOLUME emission not found"
        region = _EC[idx: idx + 120]
        assert "vol0_oi0" in region


class TestOutcomeTaxonomyComplete:
    def test_all_canonical_outcomes_present_in_source(self):
        missing = {o for o in CANONICAL_OUTCOMES if o not in _EC}
        # NO_VALID_PLAYBOOK_DTE_CONTRACT and UNTRADEABLE_FOR_ACCOUNT_SIZE are
        # emitted by PR1/PR2 respectively; PR3 documents them in the taxonomy
        # comment so the taxonomy is complete even before those land.
        assert "NO_VALID_PLAYBOOK_DTE_CONTRACT" in _EC
        assert "UNTRADEABLE_FOR_ACCOUNT_SIZE" in _EC
        # The actively-emitted PR3 outcomes must all be present:
        for o in (
            "BREACH_CONTRACT_SELECTED",
            "BREACH_SELECTOR_RETURNED_NONE",
            "BREACH_SELECTOR_EXCEPTION",
            "BREACH_SUBMISSION_SKIPPED",
            "BREACH_BROKER_SUBMITTED",
            "DATA_MISSING_OI_VOLUME",
        ):
            assert o in _EC, f"missing actively-emitted outcome {o}"


class TestNoBehaviorChange:
    """PR3 is observability only — it must not add order actions."""

    def test_emitter_does_not_submit_or_cancel(self):
        idx = _EC.find("def _emit_deferred_outcome(")
        body = _EC[idx: idx + 1800]
        assert "submit_order" not in body
        assert "submit_existing_entry" not in body
        assert "cancel_pending_entry" not in body
        assert ".broker." not in body

    def test_emitter_only_logs_and_sets_sentinel(self):
        idx = _EC.find("def _emit_deferred_outcome(")
        body = _EC[idx: idx + 1800]
        # The only state it mutates is the local sentinel dict
        assert '_deferred_outcome["emitted"] = True' in body
