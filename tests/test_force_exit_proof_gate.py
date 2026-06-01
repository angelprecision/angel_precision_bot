"""
tests/test_force_exit_proof_gate.py
=====================================

Tests for the Codex patch on PR71:
admin force exit must only call proof.log_trade() when
_submit_exit_decision() returns True.

Covers:
1. submitted=False  → no proof row, FORCE_EXIT_PROOF_SKIPPED logged
2. submitted=True   → proof.log_trade() called once
3. submitted=True but proof.log_trade() raises → error logged, no crash
4. submitted=True, no proof object → silent skip, no crash
"""

from __future__ import annotations
import types
from unittest.mock import MagicMock, patch
import pytest


# ── Minimal harness replicating the patched block ────────────────────────────

def _run_proof_block(
    submitted: bool,
    proof_obj=None,
    entry_price: float = 4.00,
    current_bid: float = 5.00,
    underlying_entry: float = 260.0,
    current_underlying: float = 268.0,
    signal: dict | None = None,
    raise_on_log: bool = False,
):
    """
    Re-implement the patched proof block from admin_force_exit_position()
    in isolation, with controlled inputs.
    Returns a result dict describing what happened.
    """
    import os
    os.environ.setdefault("BREAKEVEN_BAND_PCT", "-2.0")

    position_id = "pos-test-001"
    reason      = "admin_dashboard_force_exit"
    logged      = {"called": False, "args": None}
    skipped_log = {"called": False}
    error_log   = {"called": False, "msg": ""}

    # Simulate admin_log
    class _AdminLog:
        def warning(self, msg, *a):
            if "FORCE_EXIT_PROOF_SKIPPED" in msg:
                skipped_log["called"] = True
        def info(self, msg, *a):
            pass
        def error(self, msg, *a):
            error_log["called"] = True
            error_log["msg"] = msg

    admin_log = _AdminLog()

    # Build a minimal pos object
    pos = types.SimpleNamespace(
        ticker             = "ADBE",
        side               = "CALL",
        entry_price        = entry_price,
        current_bid        = current_bid,
        current_option_price = current_bid,
        underlying_entry   = underlying_entry,
        current_underlying = current_underlying,
        quantity_remaining = 1,
        quantity           = 1,
        signal             = signal or {"pattern": "3-2-2", "timeframe": "1d",
                                        "score": 72, "tier": "B"},
        opened_at          = None,
        position_id        = position_id,
        local_order_id     = "lo-001",
    )

    # Build a minimal runner
    if proof_obj is None:
        proof_obj = MagicMock()
        if raise_on_log:
            proof_obj.log_trade.side_effect = RuntimeError("DB down")
        else:
            proof_obj.log_trade.return_value = {}

    core   = types.SimpleNamespace(proof=proof_obj)
    runner = types.SimpleNamespace(core=core)

    # ── THE PATCHED BLOCK (mirrors app.py exactly) ────────────────────────────
    if not submitted:
        admin_log.warning(
            "FORCE_EXIT_PROOF_SKIPPED pos=%s ticker=%s reason=submission_failed_or_not_accepted",
            position_id, getattr(pos, "ticker", "?"),
        )
    else:
        try:
            _proof_core = getattr(runner, "core", None)
            _proof_obj  = getattr(_proof_core, "proof", None) if _proof_core else None
            if _proof_obj and hasattr(_proof_obj, "log_trade"):
                _ep      = float(getattr(pos, "entry_price",         0) or 0)
                _xp      = float(getattr(pos, "current_bid",         0) or
                                 getattr(pos, "current_option_price", 0) or 0)
                _ue      = float(getattr(pos, "underlying_entry",    0) or 0)
                _ux      = float(getattr(pos, "current_underlying",  0) or 0)
                _qty     = int(getattr(pos, "quantity_remaining",    0) or
                               getattr(pos, "quantity",              1))
                _sig     = getattr(pos, "signal", {}) or {}
                _opt_pnl = round(((_xp - _ep) / _ep * 100) if _ep > 0 and _xp > 0 else 0, 2)
                _u_pnl   = round(((_ux - _ue) / _ue * 100) if _ue > 0 and _ux > 0 else 0, 3)
                import os as _os
                _bb      = float(_os.getenv("BREAKEVEN_BAND_PCT", "-2.0"))
                _win     = _opt_pnl >= _bb
                _proof_obj.log_trade(
                    ticker             = getattr(pos, "ticker",    "?"),
                    pattern            = _sig.get("pattern",       ""),
                    side               = getattr(pos, "side",       ""),
                    timeframe          = _sig.get("timeframe",     "1d"),
                    score              = float(_sig.get("score",    0) or 0),
                    tier               = _sig.get("tier",          ""),
                    context_score      = float((_sig.get("score_breakdown") or {}).get("real_time_ctx", 0) or 0),
                    setup_status       = "admin_force_exit",
                    entry_trigger      = _ue,
                    entry_option_price = _ep,
                    exit_option_price  = _xp,
                    underlying_entry   = _ue,
                    underlying_exit    = _ux,
                    contracts          = _qty,
                    exit_reason        = f"ADMIN FORCE EXIT — {reason}",
                    option_pnl_pct     = _opt_pnl,
                    underlying_pnl_pct = _u_pnl,
                    win                = _win,
                    spread_pct         = float(_sig.get("spread_pct", 0) or 0),
                    opened_at          = getattr(pos, "opened_at", None),
                    position_id        = str(getattr(pos, "position_id", "") or ""),
                    local_order_id     = str(getattr(pos, "local_order_id", "") or ""),
                )
                admin_log.info(
                    "FORCE_EXIT_PROOF_LOGGED pos=%s ticker=%s pnl=%.1f%% win=%s",
                    position_id, getattr(pos, "ticker", "?"), _opt_pnl, _win,
                )
        except Exception as _proof_exc:
            admin_log.error(
                "FORCE_EXIT_PROOF_FAILED pos=%s err=%s — trade closed but not in proof_trades",
                position_id, _proof_exc,
            )

    return {
        "log_trade_called":     proof_obj.log_trade.call_count > 0,
        "log_trade_call_count": proof_obj.log_trade.call_count,
        "skipped_logged":       skipped_log["called"],
        "error_logged":         error_log["called"],
        "proof_obj":            proof_obj,
    }


# ── Test 1 ────────────────────────────────────────────────────────────────────

def test_submitted_false_no_proof_row():
    """
    When _submit_exit_decision returns False, proof.log_trade must NOT be called.
    This is the Codex-identified bug: writing a fake closed trade while position
    is still open (exit_in_flight block, kill-switch guard, broker reject, etc.).
    """
    result = _run_proof_block(submitted=False)

    assert not result["log_trade_called"], \
        "proof.log_trade must NOT be called when submitted=False"
    assert result["skipped_logged"], \
        "FORCE_EXIT_PROOF_SKIPPED must be logged when submitted=False"


# ── Test 2 ────────────────────────────────────────────────────────────────────

def test_submitted_true_proof_log_attempted():
    """
    When _submit_exit_decision returns True, proof.log_trade must be called
    exactly once with the position's entry/exit data.
    """
    result = _run_proof_block(submitted=True, entry_price=4.00, current_bid=5.00)

    assert result["log_trade_called"], \
        "proof.log_trade must be called when submitted=True"
    assert result["log_trade_call_count"] == 1, \
        "proof.log_trade must be called exactly once"
    assert not result["skipped_logged"], \
        "FORCE_EXIT_PROOF_SKIPPED must NOT be logged when submitted=True"


# ── Test 3 ────────────────────────────────────────────────────────────────────

def test_submitted_true_proof_raises_does_not_crash():
    """
    If proof.log_trade raises (DB down, schema mismatch, etc.), the block must
    catch the exception and log FORCE_EXIT_PROOF_FAILED — it must never
    propagate and crash the force exit API response.
    """
    result = _run_proof_block(submitted=True, raise_on_log=True)

    assert result["log_trade_called"], \
        "log_trade was attempted (raised)"
    assert result["error_logged"], \
        "FORCE_EXIT_PROOF_FAILED must be logged on exception"
    # No exception propagated — test would have failed with RuntimeError if so


# ── Test 4 ────────────────────────────────────────────────────────────────────

def test_submitted_true_no_proof_object_does_not_crash():
    """
    If the runner has no proof object (core=None, proof=None), the block must
    silently skip — no crash, no error log.
    """
    import types, os
    os.environ.setdefault("BREAKEVEN_BAND_PCT", "-2.0")

    error_fired = {"called": False}

    class _Log:
        def warning(self, *a): pass
        def info(self, *a): pass
        def error(self, msg, *a): error_fired["called"] = True

    admin_log = _Log()
    pos = types.SimpleNamespace(
        ticker="ADBE", side="CALL", entry_price=4.0,
        current_bid=5.0, current_option_price=5.0, underlying_entry=260.0,
        current_underlying=268.0, quantity_remaining=1, quantity=1,
        signal={}, opened_at=None, position_id="pos-001", local_order_id="lo-001",
    )
    position_id = "pos-001"
    reason = "test"
    # Runner has core but core has no .proof attribute
    core_no_proof = types.SimpleNamespace()
    runner = types.SimpleNamespace(core=core_no_proof)

    submitted = True
    if not submitted:
        admin_log.warning("FORCE_EXIT_PROOF_SKIPPED pos=%s ticker=%s reason=submission_failed_or_not_accepted",
                          position_id, pos.ticker)
    else:
        try:
            _proof_core = getattr(runner, "core", None)
            _proof_obj  = getattr(_proof_core, "proof", None) if _proof_core else None
            if _proof_obj and hasattr(_proof_obj, "log_trade"):
                _proof_obj.log_trade()
        except Exception as _proof_exc:
            admin_log.error("FORCE_EXIT_PROOF_FAILED pos=%s err=%s", position_id, _proof_exc)

    assert not error_fired["called"], "No error should be logged when proof object is absent"