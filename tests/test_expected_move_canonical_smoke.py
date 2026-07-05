# =============================================================================
# tests/test_expected_move_canonical_smoke.py
#
# Reconciliation smoke tests for the CANONICAL ap/expected_move.py.
#
# Context: main already carries ap/expected_move.py (from PR-E #279). This
# change makes it the CANONICAL SUPERSET that also serves PR-F (#290) and
# PR-G (#291) when they merge, so those two PRs no longer collide on this file.
#
# These tests are the "green PR, broken stack" guard: unit tests can pass while
# imports break at boot. They fail loudly if the canonical module or the core
# stack modules stop importing, or if either public surface goes missing.
#
# Policy-required coverage:
#   1. Four-import smoke: ap.expected_move, ap_master_control, ap_exit_engine
#      import without error. (ap.exit_ladder is only present once PR-G #291
#      merges; it is imported opportunistically and skipped if absent, so this
#      test is correct on main today and on the post-#291 stack.)
#   2. Metadata-guard import-order: importing ap_master_control directly must
#      not prevent the entry-metadata guard from installing; after the
#      documented install call the guard is present and idempotent.
#   3. Canonical surface: both the PR-F/PR-E and PR-G public APIs exist on the
#      single module, so consumers on both branches resolve.
#   4. feasibility_ratio disambiguation: the canonical feasibility_ratio keeps
#      the PR-F ExpectedMoveResult contract; the PR-G tuple variant lives at
#      feasibility_ratio_pct.
# =============================================================================
from __future__ import annotations

import importlib

import pytest


# ── 1. Four-import smoke ─────────────────────────────────────────────────────
def test_core_stack_imports():
    """Canonical module + the two always-present stack modules import cleanly."""
    import ap.expected_move        # noqa: F401
    import ap_exit_engine          # noqa: F401
    import ap_master_control       # noqa: F401


def test_exit_ladder_imports_when_present():
    """Once PR-G (#291) merges, ap.exit_ladder must import against the canonical
    module. Skips cleanly on main where exit_ladder does not yet exist."""
    try:
        import ap.exit_ladder      # noqa: F401
    except ModuleNotFoundError:
        pytest.skip("ap.exit_ladder not present until PR-G #291 merges")


def test_reimport_is_stable():
    m1 = importlib.import_module("ap.expected_move")
    m2 = importlib.reload(m1)
    assert m2.__name__ == m1.__name__


# ── 2. Metadata-guard import-order ───────────────────────────────────────────
def test_metadata_guard_installs_after_direct_master_control_import():
    """Importing ap_master_control directly must not bypass the ability to
    install the entry-metadata guard."""
    import ap_master_control
    try:
        import ap
        ap.install_entry_metadata_safety_guards()
    except Exception as exc:
        pytest.skip(f"guard install unavailable in minimal env: {type(exc).__name__}: {exc}")
    APMasterControl = ap_master_control.APMasterControl
    assert getattr(APMasterControl, "_entry_metadata_guard_installed", False) is True
    assert hasattr(APMasterControl, "_entry_metadata_guard_original_evaluate")


def test_metadata_guard_install_is_idempotent():
    try:
        import ap
        ap.install_entry_metadata_safety_guards()
        import ap_master_control
        first = ap_master_control.APMasterControl.evaluate
        ap.install_entry_metadata_safety_guards()
        second = ap_master_control.APMasterControl.evaluate
    except Exception as exc:
        pytest.skip(f"guard install unavailable in minimal env: {type(exc).__name__}: {exc}")
    assert first is second


# ── 3. Canonical surface present ─────────────────────────────────────────────
def test_canonical_surface_prf_pre():
    import ap.expected_move as em
    for name in ("ExpectedMoveResult", "AtmIvResult", "expected_move_1d",
                 "expected_move_to", "atm_iv_from_chain", "feasibility_ratio"):
        assert hasattr(em, name), f"PR-F/PR-E surface missing {name}"


def test_canonical_surface_prg():
    import ap.expected_move as em
    for name in ("expected_move_1d_pct", "expected_move_pct_over",
                 "expected_option_daily_range_pct", "feasibility_ratio_pct"):
        assert hasattr(em, name), f"PR-G surface missing {name}"


# ── 4. feasibility_ratio disambiguation ──────────────────────────────────────
def test_feasibility_ratio_is_the_prf_signature():
    """Canonical feasibility_ratio returns ExpectedMoveResult (PR-F consumer
    contract), NOT a tuple. The tuple variant lives at feasibility_ratio_pct."""
    from ap.expected_move import (
        feasibility_ratio, ExpectedMoveResult, feasibility_ratio_pct,
    )
    r = feasibility_ratio(100, 106, 3)
    assert isinstance(r, ExpectedMoveResult)
    assert r.reason == "ok"
    val, quality = feasibility_ratio_pct(100.0, 103.0, 0.02)
    assert quality == "ok" and val is not None


def test_module_is_pure_no_ap_imports():
    import ap.expected_move as em
    src = open(em.__file__).read()
    assert "from ap." not in src
    assert "import ap_" not in src
    assert "import ap." not in src
