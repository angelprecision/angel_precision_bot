"""
P0 (PR #262): LIVE confirmation default-required.

check_entry_confirmation is a deliberate NO-OP when the plan lacks
confirmation_required=True. In LIVE that meant a broker submit with ZERO
confirmation whenever the hybrid gate didn't set the flag or plan metadata
was lost through recovery/rescue — confirmation disabled unintentionally.

After this PR, in LIVE:
  - confirmation module missing            → block (live_confirmation_required path)
  - confirmation result None/unknown shape → block (live_confirmation_error:none_result)
  - confirmation raises                    → block (existing fail-closed, unchanged)
  - confirmation passed=False              → block (existing terminalize, unchanged)
  - ONLY passed=True proceeds to submit
Paper behavior is unchanged.
"""

import pathlib
import re
from types import SimpleNamespace

import ap_execution_core as ec
import ap_entry_confirmation as confirmation

SRC = pathlib.Path(ec.__file__).with_suffix(".py").read_text()


# ── Helper semantics ─────────────────────────────────────────────────────────

def test_default_is_required(monkeypatch):
    monkeypatch.delenv("LIVE_CONFIRMATION_REQUIRED", raising=False)
    assert ec._live_confirmation_required() is True


def test_explicit_env_bypass(monkeypatch):
    monkeypatch.setenv("LIVE_CONFIRMATION_REQUIRED", "0")
    assert ec._live_confirmation_required() is False


def test_bypass_is_logged_critical():
    assert "LIVE_CONFIRMATION_BYPASSED_BY_ENV" in SRC
    m = re.search(r"log\.critical\(\s*\n\s*\"\[%s\] LIVE_CONFIRMATION_BYPASSED_BY_ENV", SRC)
    assert m, "env bypass must log at CRITICAL"


# ── LIVE requirement is independent of plan mutation ────────────────────────

def test_live_without_metadata_is_required_without_mutation():
    plan = SimpleNamespace(metadata={})
    result = confirmation.resolve_entry_confirmation_requirement(
        plan, execution_mode="live", live_default_required=True
    )
    assert result.required is True
    assert result.source == "live_default_required"
    assert plan.metadata == {}


def test_paper_without_request_is_not_required_and_not_mutated():
    plan = SimpleNamespace(metadata={})
    result = confirmation.resolve_entry_confirmation_requirement(
        plan, execution_mode="paper", live_default_required=True
    )
    assert result.required is False
    assert plan.metadata == {}


# ── Unknown result shape blocks in LIVE with explicit reason ─────────────────

def test_none_result_blocks_real_live_path(monkeypatch):
    from tests.test_execution_core_entry_confirmation import _run_entry_trigger

    monkeypatch.setattr(confirmation, "check_entry_confirmation", lambda **kwargs: None)
    result = _run_entry_trigger(
        monkeypatch, mode="off", execution_mode="live", confirmation_required=True
    )
    result["osm"].submit_existing_entry.assert_not_called()
    result["osm"].expire_pending_entry.assert_called_once_with(
        "local-1", reason="entry_confirm_error:live_confirmation_error:none_result"
    )


def test_malformed_result_blocks_real_live_path(monkeypatch):
    from tests.test_execution_core_entry_confirmation import _run_entry_trigger

    monkeypatch.setattr(
        confirmation,
        "check_entry_confirmation",
        lambda **kwargs: SimpleNamespace(passed=None),
    )
    result = _run_entry_trigger(
        monkeypatch, mode="off", execution_mode="live", confirmation_required=True
    )
    result["osm"].submit_existing_entry.assert_not_called()
    result["osm"].expire_pending_entry.assert_called_once_with(
        "local-1", reason="entry_confirm_error:live_confirmation_error:malformed_result"
    )


def test_outer_except_still_fails_closed():
    """Existing behavior fence: any confirmation exception blocks the submit."""
    assert "ENTRY_CONFIRM_ERROR — failing closed" in SRC
    m = re.search(
        r'except Exception as _ec_err:.*?_terminalize_breach_failure\(\s*\n\s*f"entry_confirm_error:\{_ec_err\}"',
        SRC, re.S,
    )
    assert m


# ── Module missing blocks in LIVE regardless of plan flag ────────────────────

def test_import_error_blocks_in_live_without_plan_flag():
    block = re.search(r"except ImportError:(.*?)except Exception as _ec_err:", SRC, re.S).group(1)
    assert "_live_needs_confirm_imp" in block
    assert "_top_required_imp" in block
    assert "_nested_required_imp" in block
    assert "NO BROKER SUBMIT" in block


def test_import_error_paper_skip_preserved():
    """Paper (non-client-gated) module absence remains skippable — unchanged."""
    assert "confirmation not required — module absence is safe to skip" in SRC


# ── passed=False still terminalizes; only passed=True proceeds ───────────────

def test_failed_confirmation_still_blocks():
    result = confirmation.check_entry_confirmation(
        plan={"metadata": {}},
        direction="CALL",
        trigger_price=100.0,
        live_bid=1.0,
        live_ask=1.02,
        live_quote_age_ms=11_000,
        underlying_last=101.0,
        decision_option_price=1.0,
        execution_mode="live",
        live_default_required=True,
    )
    assert result.passed is False
    assert result.fail_reason == "entry_confirm_failed_stale_quote"
    # observe-only daily continuation carve-out is explicitly configured and
    # unchanged by this PR (documented; enforce-default is PR #230's scope).
    assert "_observe_only_daily_continuation" in SRC
