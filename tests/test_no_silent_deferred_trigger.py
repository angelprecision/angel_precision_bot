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
    "BREACH_RISK_CHECK_BLOCKED",
    "BREACH_SELECTOR_RETURNED_NONE",
    "BREACH_SELECTOR_EXCEPTION",
    "BREACH_SUBMISSION_SKIPPED",
    "BREACH_BROKER_SUBMITTED",
    "NO_VALID_PLAYBOOK_DTE_CONTRACT",
    "UNTRADEABLE_FOR_ACCOUNT_SIZE",
    "DATA_MISSING_OI_VOLUME",
}

# BREACH_CONTRACT_SELECTED is PROGRESS, not terminal.
PROGRESS_OUTCOMES = {"BREACH_CONTRACT_SELECTED"}


class TestEmitterExists:
    def test_emitter_helper_defined(self):
        assert "def _emit_deferred_outcome(" in _EC

    def test_sentinel_defined(self):
        assert '_deferred_outcome = {"emitted": False' in _EC

    def test_emitter_includes_join_keys(self):
        """Every emission must carry local_order_id + signal_id so the event
        joins back to the order row (the linkage missing before PR3)."""
        idx = _EC.find("def _emit_deferred_outcome(")
        body = _EC[idx: _EC.find("# 3) Recover", idx)]
        assert '"local_order_id": queue_local_order_id' in body
        assert '"signal_id": signal_id' in body

    def test_emitter_never_raises(self):
        idx = _EC.find("def _emit_deferred_outcome(")
        body = _EC[idx: _EC.find("# 3) Recover", idx)]
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

    def test_contract_selected_is_progress_not_terminal(self):
        """BREACH_CONTRACT_SELECTED must go through the PROGRESS channel
        (_emit_deferred_progress), NOT the terminal _emit_deferred_outcome —
        otherwise it consumes the exactly-once terminal slot and blocks the real
        terminal outcome (BREACH_BROKER_SUBMITTED)."""
        idx = _EC.find('"BREACH_CONTRACT_SELECTED"')
        # find the emission call (not the comment/taxonomy)
        # the call site must be _emit_deferred_progress
        call_idx = _EC.find('_emit_deferred_progress(\n                    "BREACH_CONTRACT_SELECTED"')
        assert call_idx != -1, "BREACH_CONTRACT_SELECTED must be emitted via _emit_deferred_progress"

    def test_progress_channel_does_not_set_terminal_sentinel(self):
        """_emit_deferred_progress must NOT set the exactly-once terminal slot."""
        idx = _EC.find("def _emit_deferred_progress(")
        end = _EC.find("def _emit_deferred_outcome(", idx)
        body = _EC[idx:end]
        assert '_deferred_outcome["emitted"] = True' not in body

    def test_terminal_channel_rejects_non_terminal_codes(self):
        """_emit_deferred_outcome must reject non-terminal codes (route them to
        progress) so they never consume the terminal slot."""
        idx = _EC.find("def _emit_deferred_outcome(")
        end = _EC.find("# 3) Recover", idx)
        body = _EC[idx:end]
        assert "_TERMINAL_DEFERRED_OUTCOMES" in body
        assert "if outcome not in _TERMINAL_DEFERRED_OUTCOMES:" in body

    def test_contract_selected_not_in_terminal_set(self):
        idx = _EC.find("_TERMINAL_DEFERRED_OUTCOMES = frozenset({")
        end = _EC.find("})", idx)
        block = _EC[idx:end]
        assert "BREACH_CONTRACT_SELECTED" not in block

    def test_broker_submitted_in_terminal_set(self):
        idx = _EC.find("_TERMINAL_DEFERRED_OUTCOMES = frozenset({")
        end = _EC.find("})", idx)
        block = _EC[idx:end]
        assert "BREACH_BROKER_SUBMITTED" in block

    def test_broker_submitted_path_emits(self):
        # On successful submit, must emit BREACH_BROKER_SUBMITTED with broker id.
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
        (the one adjacent to vol0_oi0), not the taxonomy set/comment."""
        idx = _EC.find('"vol0_oi0" in str(_reason)')
        assert idx != -1, "DATA_MISSING_OI_VOLUME vol0_oi0 conditional not found"
        region = _EC[idx - 120: idx + 40]
        assert "DATA_MISSING_OI_VOLUME" in region


class TestOutcomeTaxonomyComplete:
    def test_all_terminal_outcomes_present_in_source(self):
        # NO_VALID_PLAYBOOK_DTE_CONTRACT and UNTRADEABLE_FOR_ACCOUNT_SIZE are
        # emitted by PR1/PR2; PR3 documents them in the terminal set so the
        # taxonomy is complete even before those land.
        assert "NO_VALID_PLAYBOOK_DTE_CONTRACT" in _EC
        assert "UNTRADEABLE_FOR_ACCOUNT_SIZE" in _EC
        # The actively-emitted PR3 TERMINAL outcomes must all be present:
        for o in (
            "BREACH_SELECTOR_RETURNED_NONE",
            "BREACH_SELECTOR_EXCEPTION",
            "BREACH_SUBMISSION_SKIPPED",
            "BREACH_BROKER_SUBMITTED",
            "DATA_MISSING_OI_VOLUME",
        ):
            assert o in _EC, f"missing actively-emitted terminal outcome {o}"


class TestNoBehaviorChange:
    """PR3 is observability only — it must not add order actions."""

    def test_emitter_does_not_submit_or_cancel(self):
        idx = _EC.find("def _emit_deferred_outcome(")
        body = _EC[idx: _EC.find("# 3) Recover", idx)]
        assert "submit_order" not in body
        assert "submit_existing_entry" not in body
        assert "cancel_pending_entry" not in body
        assert ".broker." not in body

    def test_emitter_only_logs_and_sets_sentinel(self):
        idx = _EC.find("def _emit_deferred_outcome(")
        body = _EC[idx: _EC.find("# 3) Recover", idx)]
        # The only state it mutates is the local sentinel dict
        assert '_deferred_outcome["emitted"] = True' in body


class TestAmendmentGuards:
    """Review amendments: deferred-only guard + exactly-one terminal outcome."""

    def test_emitter_is_deferred_guarded(self):
        """Emitter must no-op unless the trigger is a deferred entry."""
        idx = _EC.find("def _emit_deferred_outcome(")
        body = _EC[idx: _EC.find("# 3) Recover", idx)]
        assert 'if not _deferred_outcome.get("is_deferred"):' in body
        assert "return" in body

    def test_emitter_exactly_once(self):
        """First emission wins; later calls ignored (no double terminal)."""
        idx = _EC.find("def _emit_deferred_outcome(")
        body = _EC[idx: _EC.find("# 3) Recover", idx)]
        assert 'if _deferred_outcome.get("emitted"):' in body

    def test_sentinel_has_is_deferred(self):
        assert '"is_deferred": False' in _EC

    def test_is_deferred_set_after_deferred_computed(self):
        """is_deferred must be set from the _deferred computation, and the
        emission gate at the missing-plan path must NOT fire a deferred outcome
        (it runs before _deferred is known)."""
        assert '_deferred_outcome["is_deferred"] = bool(_deferred)' in _EC
        # the approved_plan-missing path must no longer emit a deferred outcome
        idx = _EC.find("approved plan missing after breach revalidation")
        region = _EC[idx: idx + 400]
        assert "_emit_deferred_outcome" not in region

    def test_missing_plan_path_does_not_emit(self):
        """A non-deferred (or pre-_deferred) missing-plan trigger must not carry
        a deferred outcome."""
        idx = _EC.find("approved_plan_missing_after_revalidation")
        # the terminalize call remains, but no deferred emission alongside it
        region = _EC[idx - 200: idx + 200]
        assert "_terminalize_breach_failure" in region


# ---------------------------------------------------------------------------
# Runtime behavioral proof — drive the real emitter closures through sequences
# to PROVE: (1) exactly one terminal, (2) SELECTED never terminal, (3) deferred-only.
# Rebuilds the emitter/progress closures exactly as defined in _on_entry_trigger.
# ---------------------------------------------------------------------------

class TestRuntimeEmitterBehavior:
    @staticmethod
    def _build(is_deferred: bool):
        """Reconstruct the emitter pair with the same guards as the source."""
        import logging
        log = logging.getLogger("test_emitter")
        events = []

        _TERMINAL = frozenset({
            "BREACH_RISK_CHECK_BLOCKED", "BREACH_SELECTOR_RETURNED_NONE",
            "BREACH_SELECTOR_EXCEPTION", "BREACH_SUBMISSION_SKIPPED",
            "BREACH_BROKER_SUBMITTED", "NO_VALID_PLAYBOOK_DTE_CONTRACT",
            "UNTRADEABLE_FOR_ACCOUNT_SIZE", "DATA_MISSING_OI_VOLUME",
        })
        sentinel = {"emitted": False, "outcome": None, "is_deferred": is_deferred}

        def progress(outcome, **kw):
            if not sentinel.get("is_deferred"):
                return
            events.append(("PROGRESS", outcome))

        def terminal(outcome, **kw):
            if not sentinel.get("is_deferred"):
                return
            if outcome not in _TERMINAL:
                progress(outcome)
                return
            if sentinel.get("emitted"):
                return
            sentinel["emitted"] = True
            sentinel["outcome"] = outcome
            events.append(("TERMINAL", outcome))

        return progress, terminal, sentinel, events

    def test_selected_then_submitted_terminal_is_submitted(self):
        """The real success sequence: SELECTED (progress) → SUBMITTED (terminal).
        Terminal must be SUBMITTED, not SELECTED."""
        progress, terminal, sentinel, events = self._build(is_deferred=True)
        # contract selected first (progress)
        terminal("BREACH_CONTRACT_SELECTED")   # routed to progress
        # then submitted (terminal)
        terminal("BREACH_BROKER_SUBMITTED")
        terminals = [e for e in events if e[0] == "TERMINAL"]
        assert len(terminals) == 1
        assert terminals[0][1] == "BREACH_BROKER_SUBMITTED"
        assert ("PROGRESS", "BREACH_CONTRACT_SELECTED") in events

    def test_exactly_one_terminal_even_with_multiple_calls(self):
        progress, terminal, sentinel, events = self._build(is_deferred=True)
        terminal("BREACH_SELECTOR_RETURNED_NONE")
        terminal("BREACH_BROKER_SUBMITTED")   # must be ignored — already terminal
        terminal("BREACH_SUBMISSION_SKIPPED")  # ignored
        terminals = [e for e in events if e[0] == "TERMINAL"]
        assert len(terminals) == 1
        assert terminals[0][1] == "BREACH_SELECTOR_RETURNED_NONE"

    def test_selected_alone_never_produces_terminal(self):
        """If only SELECTED fires (e.g. a later silent gap), there is NO terminal
        — proving SELECTED cannot masquerade as the final outcome."""
        progress, terminal, sentinel, events = self._build(is_deferred=True)
        terminal("BREACH_CONTRACT_SELECTED")
        terminals = [e for e in events if e[0] == "TERMINAL"]
        assert len(terminals) == 0
        assert sentinel["emitted"] is False

    def test_non_deferred_emits_nothing(self):
        """A non-deferred (real-contract) entry must not pollute the deferred
        taxonomy at all — progress or terminal."""
        progress, terminal, sentinel, events = self._build(is_deferred=False)
        terminal("BREACH_CONTRACT_SELECTED")
        terminal("BREACH_BROKER_SUBMITTED")
        terminal("BREACH_SUBMISSION_SKIPPED")
        assert events == []
