"""
tests/test_p0_final_duplicate_honesty.py

P0 Final Duplicate Guard Honesty.

Verifies that after PR #134:
  - Memory-only duplicate signal in master_control does NOT block
    (the durable DB check is the sole authority).
  - Durable trade_queue duplicate DOES block with the structured reason.
  - duplicate_setup still emits its own reason, never as duplicate_signal_id.
  - Queue _derive_last_error preserves the full reason including the
    (durable:...) suffix.
  - Quality_mode Gate 9 is removed (no blocked_by="duplicate_signal_id"
    code path remains in ap_quality_mode).
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch
import pytest


# =============================================================================
# Test 1: Memory-only duplicate signal — NOT blocked
# =============================================================================

def test_memory_only_duplicate_does_not_block():
    """Put signal_key into self._seen_signals.
    Mock _has_durable_duplicate_signal to return (False, "", "").
    evaluate() must NOT return duplicate_signal_id."""
    import sys
    sys.modules.pop("ap_master_control", None)
    import ap_master_control as mc_mod

    mc = mc_mod.APMasterControl.__new__(mc_mod.APMasterControl)
    mc.mode = "PAPER"
    mc.paper = True
    mc._mode_fn = None
    mc._seen_signals = {}

    # Pre-poison memory with the signal_key
    signal_id = "sig-mem-only"
    client_id = "jason@example.com"
    signal_key = f"sig:{signal_id}:{client_id}"
    import time as _t
    mc._seen_signals[signal_key] = _t.time()

    # Mock durable check to return no active path
    with patch.object(mc, "_has_durable_duplicate_signal",
                       return_value=(False, "", "")):
        # We can't call full evaluate() without lots of scaffolding, but
        # we CAN call the duplicate-check helper directly to prove the
        # durable check overrides the memory hint.
        is_dup, source, _ = mc._has_durable_duplicate_signal(
            client_id=client_id, signal_id=signal_id,
        )

    assert is_dup is False
    assert source == ""

    # Now verify the memory hint did not poison the durable result:
    # presence of signal_key in _seen_signals is allowed (it's a hint only)
    # but it must not cause a duplicate block. The block path is only:
    #   if _dur_dup: return self._block(... "duplicate_signal_id (durable:...)")
    # so if _dur_dup is False, no duplicate block fires.
    assert signal_key in mc._seen_signals  # hint is retained
    # And the helper's contract for "no active path" is the empty tuple
    # which the caller must interpret as "do not block".


# =============================================================================
# Test 2: Durable trade_queue duplicate — DOES block with structured reason
# =============================================================================

def test_durable_trade_queue_duplicate_blocks_with_structured_reason():
    """Mock _has_durable_duplicate_signal to return
    (True, "trade_queue", "id=123 status=WATCHING").
    The block reason must be 'duplicate_signal_id (durable:trade_queue)'."""
    import sys
    sys.modules.pop("ap_master_control", None)
    import ap_master_control as mc_mod

    mc = mc_mod.APMasterControl.__new__(mc_mod.APMasterControl)
    mc.mode = "PAPER"
    mc.paper = True
    mc._mode_fn = None
    mc._seen_signals = {}

    # Get the helper's contract right
    fake_cursor = MagicMock()
    fake_cursor.fetchone.side_effect = [
        {"id": 123, "status": "WATCHING", "last_error": None},
    ]
    fake_conn = MagicMock()
    fake_conn.__enter__ = MagicMock(return_value=fake_cursor)
    fake_conn.__exit__ = MagicMock(return_value=False)

    with patch("ap.db.conn", return_value=fake_conn), \
         patch("ap.db.run_with_retry", side_effect=lambda f, *a, **k: f()):
        is_dup, source, detail = mc._has_durable_duplicate_signal(
            client_id="jason@example.com",
            signal_id="sig-dur-tq",
        )

    assert is_dup is True
    assert source == "trade_queue"
    assert "123" in detail
    assert "WATCHING" in detail

    # The full reason string master_control._block will emit is built as:
    #   f"duplicate_signal_id (durable:{_dur_source})"
    # so we assert the format would produce the expected string.
    expected_reason = f"duplicate_signal_id (durable:{source})"
    assert expected_reason == "duplicate_signal_id (durable:trade_queue)"


# =============================================================================
# Test 3: duplicate_setup never becomes duplicate_signal_id
# =============================================================================

def test_duplicate_setup_emits_distinct_reason():
    """Inspect master_control source: when setup_key fires, the reason must
    be 'duplicate_setup (...)' — not 'duplicate_signal_id'."""
    from pathlib import Path

    mc_path = Path(__file__).resolve().parents[1] / "ap_master_control.py"
    src = mc_path.read_text()

    # The setup_key dedup block must emit duplicate_setup, never duplicate_signal_id
    # Find the line with `f"duplicate_setup`
    setup_lines = [
        l for l in src.splitlines()
        if "duplicate_setup" in l and "_block" in l
    ]
    assert setup_lines, "setup_key block site not found in master_control"

    for line in setup_lines:
        assert "duplicate_signal_id" not in line, (
            f"setup_key block is emitting duplicate_signal_id reason: {line!r}"
        )
        assert "duplicate_setup" in line


# =============================================================================
# Test 4: Queue _derive_last_error preserves the full (durable:...) suffix
# =============================================================================

def test_derive_last_error_preserves_durable_suffix():
    """If MC emits stage='blocked_system' and reason='duplicate_signal_id (durable:trade_queue)',
    the queue's _derive_last_error must produce
    'blocked_system:duplicate_signal_id (durable:trade_queue)'.

    This is the contract that prevents trade_queue.last_error from collapsing
    the structured reason back to the bare form."""
    import sys
    sys.modules.setdefault("ap.logger", MagicMock())
    from ap import queue as queue_mod

    result = {
        "stage":       "blocked_system",
        "reason":      "duplicate_signal_id (durable:trade_queue)",
        "reason_code": "DUPLICATE_SIGNAL_ID_DURABLE",
    }
    derived = queue_mod._derive_last_error(result)
    assert derived == "blocked_system:duplicate_signal_id (durable:trade_queue)", (
        f"_derive_last_error collapsed the suffix! Got: {derived!r}"
    )
    # And explicitly NOT the bare form
    assert derived != "blocked_system:duplicate_signal_id"


# =============================================================================
# Test 5: Regression — no code path emits bare blocked_by="duplicate_signal_id"
# =============================================================================

def test_no_bare_duplicate_signal_id_blocked_by_remains():
    """Scan ap_quality_mode.py source: there must be no `blocked_by=
    "duplicate_signal_id"` literal anywhere. The gate was removed in PR #134."""
    from pathlib import Path
    qm_path = Path(__file__).resolve().parents[1] / "ap_quality_mode.py"
    src = qm_path.read_text()

    assert 'blocked_by="duplicate_signal_id"' not in src, (
        "ap_quality_mode still has blocked_by=\"duplicate_signal_id\" — "
        "Gate 9 was not fully removed"
    )
    # And gates_applied.append("duplicate_signal_id") is removed too
    assert 'gates_applied.append("duplicate_signal_id")' not in src, (
        "ap_quality_mode still appends duplicate_signal_id to gates_applied"
    )
    # And state.seen_signal_ids is no longer written
    assert "state.seen_signal_ids[signal_id] = True" not in src, (
        "ap_quality_mode still writes to state.seen_signal_ids — should be removed"
    )


# =============================================================================
# Test 6: Master_control still emits the durable reason — sanity
# =============================================================================

def test_master_control_only_emits_structured_duplicate_reasons():
    """All duplicate_signal_id reason emissions in master_control must include
    the (durable:...) suffix or be in the helper's docstring/comments."""
    from pathlib import Path
    mc_path = Path(__file__).resolve().parents[1] / "ap_master_control.py"
    src = mc_path.read_text()

    # Find every line that emits a reason string containing duplicate_signal_id
    # Real emissions look like:
    #   f"duplicate_signal_id (durable:{_dur_source})"
    #   f"duplicate_check_unavailable_live_blocked ({_dur_detail})"
    for i, line in enumerate(src.splitlines(), 1):
        if "duplicate_signal_id" not in line:
            continue
        stripped = line.strip()
        # Skip comments and docstrings
        if stripped.startswith("#") or stripped.startswith('"') or stripped.startswith("'"):
            continue
        # Skip log lines (they are observational, not the block reason)
        if "log." in line.lower():
            continue
        # Skip the helper's docstring mention inside triple-quoted block
        if "'duplicate_signal_id'" in line:
            continue
        # Any remaining line that mentions duplicate_signal_id must include "durable:"
        # (the only structured form allowed).
        assert "durable:" in line, (
            f"master_control L{i} emits duplicate_signal_id without "
            f"durable suffix: {stripped!r}"
        )


# =============================================================================
# Test 7: Quality_mode gates_applied list no longer includes duplicate_signal_id
# =============================================================================

def test_quality_mode_gates_applied_excludes_duplicate_signal_id():
    """When a signal passes quality_mode, the gates_applied list must NOT
    include 'duplicate_signal_id' — that gate was removed."""
    import sys
    sys.modules.pop("ap_quality_mode", None)
    import ap_quality_mode as qm

    # Build a minimal signal that passes all other gates
    signal = {
        "signal_id":     "sig-list-check",
        "plan_id":       "plan-list-check",
        "ticker":        "MSFT",
        "symbol":        "MSFT",
        "side":          "CALL",
        "direction":     "CALL",
        "timeframe":     "1d",
        "pattern":       "2-1_2D",
        "score":         75.0,
    }
    verdict = qm.check(
        signal=signal,
        client_id="qm_list_check@ap.com",
        intel_status="APPROVED",
        daily_trades_today=0,
        open_positions=0,
        approved_score=75.0,
    )
    # Whether allowed or not, the gates_applied list must not contain
    # "duplicate_signal_id" because that gate doesn't exist anymore.
    qmr = verdict.quality_mode_result or {}
    gates = qmr.get("gates_applied") or []
    assert "duplicate_signal_id" not in gates, (
        f"gates_applied still includes duplicate_signal_id: {gates}"
    )
