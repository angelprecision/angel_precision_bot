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

import ap_execution_core as ec

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


# ── LIVE forces confirmation_required into the plan ─────────────────────────

def test_live_forces_required_flag_before_check():
    block = re.search(
        r"_plan_meta_for_confirm = getattr\(approved_plan, \"metadata\", \{\}\) or \{\}(.*?)_confirm_result = check_entry_confirmation",
        SRC, re.S,
    ).group(1)
    assert '_plan_meta_for_confirm["confirmation_required"] = True' in block
    assert "_is_live_submit" in block
    assert "_live_confirmation_required()" in block


def test_paper_path_does_not_force_the_flag():
    """The forcing is gated on _is_live_submit — paper metadata untouched."""
    block = re.search(
        r"(_is_live_submit = str\(.*?)_confirm_result = check_entry_confirmation",
        SRC, re.S,
    ).group(1)
    force_idx = block.index('_plan_meta_for_confirm["confirmation_required"] = True')
    gate_idx = block.index("if _is_live_submit:")
    assert gate_idx < force_idx


# ── Unknown result shape blocks in LIVE with explicit reason ─────────────────

def test_none_result_raises_into_fail_closed_path():
    assert 'live_confirmation_error:none_result' in SRC
    m = re.search(
        r"if _is_live_submit and \(\s*\n\s*_confirm_result is None or not hasattr\(_confirm_result, \"passed\"\)",
        SRC,
    )
    assert m
    # It must raise INTO the existing fail-closed except (which terminalizes).
    assert 'raise RuntimeError("live_confirmation_error:none_result")' in SRC


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
    assert 'if _live_needs_confirm_imp or _hcqg_imp.get("confirmation_required"):' in block
    assert "NO BROKER SUBMIT" in block


def test_import_error_paper_skip_preserved():
    """Paper (non-client-gated) module absence remains skippable — unchanged."""
    assert "confirmation not required — module absence is safe to skip" in SRC


# ── passed=False still terminalizes; only passed=True proceeds ───────────────

def test_failed_confirmation_still_blocks():
    assert "if not _confirm_result.passed:" in SRC
    # observe-only daily continuation carve-out is explicitly configured and
    # unchanged by this PR (documented; enforce-default is PR #230's scope).
    assert "_observe_only_daily_continuation" in SRC
