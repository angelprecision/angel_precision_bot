# tests/test_p0_exit_retry_liveness_exit_engine.py
# =============================================================================
# P0 regression: PR #423 Patch 2 — dedicated exit_replace_attempt generation
# counter on ManagedPosition/APExitEngine, exactly-once increment via
# mark_exit_replacement_safe(), submission does not reset it, only proven
# economic completion resets it, and the adaptive pricing ladder reads it
# instead of the diagnostic _exit_stuck_count counter.
# =============================================================================

from __future__ import annotations

import os
from unittest.mock import MagicMock

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_p0_exit_retry_liveness_exit_engine",
)
os.environ.setdefault("ENCRYPTION_KEY", "ap-pr423-exit-replace-attempt-test")

import pytest  # noqa: E402

from ap_exit_engine import APExitEngine, ManagedPosition  # noqa: E402


def _engine() -> APExitEngine:
    eng = APExitEngine(broker=MagicMock())
    eng._emit_exit_event = MagicMock()
    return eng


def _pos(**overrides) -> ManagedPosition:
    defaults = dict(
        ticker="AVGO",
        option_symbol="AVGO260814C00350000",
        side="CALL",
        quantity=7,
        entry_price=2.49,
        underlying_entry=350.0,
        underlying_target=360.0,
        underlying_stop=340.0,
        position_id="pos-avgo-1",
        client_id="client-1",
        execution_mode="paper",
        exit_in_flight=True,
        pending_exit_local_order_id="loc-old-1",
        pending_exit_broker_order_id="bro-old-1",
        pending_exit_qty=2,
    )
    defaults.update(overrides)
    return ManagedPosition(**defaults)


def _add(eng: APExitEngine, pos: ManagedPosition) -> None:
    eng._positions.append(pos)
    eng._positions_by_id[pos.position_id] = pos


# ── Attempt starts at zero, dedicated field exists and is separate ─────────

def test_exit_replace_attempt_field_exists_separate_from_exit_stuck_count():
    pos = _pos()
    assert pos.exit_replace_attempt == 0
    pos._exit_stuck_count = 3
    assert pos.exit_replace_attempt == 0, (
        "exit_replace_attempt must not be aliased to/derived from "
        "_exit_stuck_count; they have independent semantics."
    )


# ── mark_exit_replacement_safe increments exactly once per proven cancel ───

def test_mark_exit_replacement_safe_increments_attempt_once():
    eng = _engine()
    pos = _pos()
    _add(eng, pos)

    eng.mark_exit_replacement_safe(
        "pos-avgo-1",
        reason="order_monitor_stale_exit_broker_confirmed_cancel",
        local_order_id="loc-old-1",
        broker_order_id="bro-old-1",
    )

    assert pos.exit_replace_attempt == 1
    assert pos.pending_exit_replace_allowed is True


def test_duplicate_mark_exit_replacement_safe_same_identity_does_not_double_increment():
    """
    Spec requirement (EXACTLY-ONCE INCREMENT): a repeat call for the SAME
    old exit generation must increment exactly once, not twice.
    """
    eng = _engine()
    pos = _pos()
    _add(eng, pos)

    eng.mark_exit_replacement_safe(
        "pos-avgo-1", reason="proof", local_order_id="loc-old-1", broker_order_id="bro-old-1",
    )
    eng.mark_exit_replacement_safe(
        "pos-avgo-1", reason="proof_retry", local_order_id="loc-old-1", broker_order_id="bro-old-1",
    )

    assert pos.exit_replace_attempt == 1, (
        f"expected exactly-once increment, got {pos.exit_replace_attempt}"
    )


def test_new_generation_after_consumed_grant_increments_again():
    """
    A second, DIFFERENT old-generation proof (after the first grant was
    consumed by a new submit, which resets pending_exit_replace_allowed to
    False) is a genuinely new replacement cycle and must increment again.
    """
    eng = _engine()
    pos = _pos()
    _add(eng, pos)

    eng.mark_exit_replacement_safe(
        "pos-avgo-1", reason="proof-1", local_order_id="loc-old-1", broker_order_id="bro-old-1",
    )
    assert pos.exit_replace_attempt == 1

    # Simulate the replacement submit consuming the grant (this is what
    # _mark_exit_submitted does to pending_exit_replace_allowed).
    pos.pending_exit_replace_allowed = False
    pos.pending_exit_local_order_id = "loc-new-1"
    pos.pending_exit_broker_order_id = "bro-new-1"

    # That replacement itself later goes stale and gets proven-canceled.
    eng.mark_exit_replacement_safe(
        "pos-avgo-1", reason="proof-2", local_order_id="loc-new-1", broker_order_id="bro-new-1",
    )
    assert pos.exit_replace_attempt == 2


# ── Submission must NOT reset the attempt counter ───────────────────────────

def test_mark_exit_submitted_does_not_reset_exit_replace_attempt():
    eng = _engine()
    pos = _pos()
    _add(eng, pos)
    pos.exit_replace_attempt = 2

    from ap_exit_engine import ExitDecision
    decision = ExitDecision(action="STOP", quantity=2, reason="test", urgency="HIGH", pnl_pct=-0.5)
    eng._mark_exit_submitted(pos, decision, local_order_id="loc-new-2", broker_order_id="bro-new-2")

    assert pos.exit_replace_attempt == 2, (
        "submission proves only that a new broker attempt exists, not "
        "completion — exit_replace_attempt must survive it"
    )
    # _exit_stuck_count legitimately resets on submit (separate diagnostic
    # sentinel semantics preserved, unchanged by #423).
    assert pos._exit_stuck_count == 0


# ── Only proven economic completion resets the counter ─────────────────────

def test_mark_position_closed_resets_exit_replace_attempt():
    eng = _engine()
    pos = _pos()
    _add(eng, pos)
    pos.exit_replace_attempt = 3
    pos._exit_replace_attempt_last_ack_identity = "bro-old-1"

    eng.mark_position_closed(
        "pos-avgo-1",
        reason="broker_confirmed_flat",
        qty_filled=2,
        fill_price=2.10,
        local_order_id=pos.pending_exit_local_order_id,
        broker_order_id=pos.pending_exit_broker_order_id,
        reconciled=True,
    )

    assert pos.exit_replace_attempt == 0
    assert pos._exit_replace_attempt_last_ack_identity == ""


# ── Pricing ladder reads exit_replace_attempt, not _exit_stuck_count ───────

def test_pricing_ladder_attempt_source_is_exit_replace_attempt():
    """
    Structural check on the adaptive-pricing block: confirms the assignment
    reads exit_replace_attempt. (Full pricing-tier behavior is covered by
    the existing adaptive-exit-pricing test suite; this test only pins down
    the #423-relevant attempt SOURCE, per the spec's explicit instruction
    not to change the pricing formula itself.)

    Reads the module's source FILE directly rather than using
    inspect.getsource() on the bound method — lifecycle guards installed at
    import time (ap/one_contract_exit_guard.py,
    ap/touched_profit_confirmation_guard.py, etc.) wrap several APExitEngine
    methods, so inspect.getsource() on the class attribute can return the
    wrapper's source instead of the real function body depending on import
    order. The source file on disk is unambiguous.
    """
    import ap_exit_engine as mod

    module_path = mod.__file__
    with open(module_path, "r") as f:
        src = f.read()

    assert '_attempt    = int(getattr(pos, "exit_replace_attempt", 0))' in src
    assert '_attempt    = int(getattr(pos, "_exit_stuck_count", 0))' not in src


# ── Restart persistence/hydration ───────────────────────────────────────────

def test_persist_exit_replace_attempt_uses_nondestructive_meta_merge(monkeypatch):
    """
    Spec requirement (HYDRATION/RESTART): persist under a dedicated nested
    metadata namespace via the existing non-destructive JSONB `meta || patch`
    merge, never overwriting unrelated meta keys.
    """
    eng = _engine()
    pos = _pos()
    _add(eng, pos)
    pos.exit_replace_attempt = 2
    pos._exit_replace_attempt_last_ack_identity = "bro-old-1"

    captured = {}

    class _FakeCursor:
        def execute(self, sql, params):
            captured["sql"] = sql
            captured["params"] = params
        rowcount = 1

    class _FakeConn:
        def __enter__(self):
            return _FakeCursor()
        def __exit__(self, *a):
            return False

    import ap.db as db_mod
    monkeypatch.setattr(db_mod, "conn", lambda: _FakeConn())
    monkeypatch.setattr(db_mod, "run_with_retry", lambda fn, *a, **k: fn())

    ok = eng._persist_exit_replace_attempt_to_db(pos)

    assert ok is True
    assert "COALESCE(meta, '{}'::jsonb) || %s::jsonb" in captured["sql"]
    import json
    patch = json.loads(captured["params"][0])
    assert patch["exit_retry_liveness"]["replace_attempt"] == 2
    assert patch["exit_retry_liveness"]["last_ack_identity"] == "bro-old-1"
    # Confirms the write targets ONLY the exit_retry_liveness namespace —
    # it cannot clobber unrelated meta keys like hard_exit_reference because
    # the SQL itself merges (||) rather than replaces the meta column.


def test_persist_exit_replace_attempt_failure_is_nonfatal(monkeypatch):
    """A DB error during persistence must not raise or block the in-memory
    exactly-once increment that already happened."""
    eng = _engine()
    pos = _pos()
    _add(eng, pos)

    import ap.db as db_mod

    def _boom():
        raise ConnectionError("db unreachable")

    monkeypatch.setattr(db_mod, "conn", _boom)

    # Should not raise.
    eng.mark_exit_replacement_safe(
        "pos-avgo-1", reason="proof", local_order_id="loc-old-1", broker_order_id="bro-old-1",
    )
    assert pos.exit_replace_attempt == 1, (
        "in-memory exactly-once increment must succeed even when the "
        "best-effort DB persist fails"
    )


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
