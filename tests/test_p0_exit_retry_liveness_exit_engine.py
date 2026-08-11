# tests/test_p0_exit_retry_liveness_exit_engine.py
# =============================================================================
# P0 regression: PR #423 Patch 2 — dedicated exit_replace_attempt pricing
# counter on ManagedPosition/APExitEngine, separate from the durable
# replacement_generation identity. It increments exactly once via
# mark_exit_replacement_safe(), submission does not reset it, only proven
# economic completion resets it, and the adaptive pricing ladder reads it
# instead of the diagnostic _exit_stuck_count counter.
# =============================================================================

from __future__ import annotations

import os
from datetime import datetime, timezone
from unittest.mock import MagicMock

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_p0_exit_retry_liveness_exit_engine",
)
os.environ.setdefault("ENCRYPTION_KEY", "ap-pr423-exit-replace-attempt-test")

import pytest  # noqa: E402

from ap_exit_engine import (  # noqa: E402
    APExitEngine,
    ExitDecision,
    ManagedPosition,
    _restore_exit_replace_attempt_from_meta,
)


def _engine(*, durable_persist=True) -> APExitEngine:
    eng = APExitEngine(broker=MagicMock())
    eng._emit_exit_event = MagicMock()
    if durable_persist:
        # Most unit cases are about the in-memory generation protocol.  The
        # persistence-specific cases opt out so they exercise the real DB
        # seam explicitly.
        eng._persist_exit_replace_attempt_to_db = MagicMock(return_value=True)
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


def _pending_callback_position(eng: APExitEngine, callback) -> ManagedPosition:
    now = datetime.now(timezone.utc)
    pos = ManagedPosition(
        ticker="AVGO",
        option_symbol="AVGO260814C00350000",
        side="CALL",
        quantity=5,
        entry_price=2.0,
        underlying_entry=350.0,
        underlying_target=360.0,
        underlying_stop=340.0,
        position_id="pos-callback-claim",
        client_id="client-1",
        execution_mode="paper",
        quantity_remaining=5,
        current_bid=1.0,
        current_ask=1.1,
        current_option_price=1.05,
        current_underlying=350.0,
        last_option_quote_update_ts=now,
        last_underlying_quote_update_ts=now,
    )
    pos.exit_retry_liveness = {
        "state": "REPLACEMENT_PENDING",
        "replace_attempt": 1,
        "replacement_generation": 1,
        "replace_quantity": 1,
        "position_id": pos.position_id,
        "client_id": pos.client_id,
        "execution_mode": pos.execution_mode,
        "old_local_order_id": "loc-old-claim",
        "old_broker_order_id": "bro-old-claim",
        "last_ack_identity": "bro-old-claim",
    }
    eng.order_state_machine = None
    eng.on_exit = callback
    eng._sync_replacement_runtime_from_lifecycle(pos, revalidated=True)
    pos.pending_exit_replace_allowed = True
    pos.pending_exit_replace_revalidated = True
    _add(eng, pos)
    return pos


@pytest.mark.parametrize(
    "callback",
    [
        lambda _pos, _decision: {"accepted": False, "status": "REJECTED"},
        lambda _pos, _decision: (_ for _ in ()).throw(RuntimeError("transport")),
    ],
    ids=["callback_rejected", "callback_raised"],
)
def test_pending_replacement_claim_is_released_after_callback_failure(callback):
    eng = _engine()
    pos = _pending_callback_position(eng, callback)

    result = eng._submit_exit_decision(
        pos,
        ExitDecision(
            "CLOSE_ALL", 1, "HARD_STOP", "IMMEDIATE", suggested_limit=1.0,
        ),
        kill_active=True,
    )

    assert result is False
    assert pos.pending_exit_replace_submit_claimed is False


# ── Attempt starts at zero, dedicated field exists and is separate ─────────

def test_exit_replace_attempt_field_exists_separate_from_exit_stuck_count():
    pos = _pos()
    assert pos.exit_replace_attempt == 0
    pos._exit_stuck_count = 3
    assert pos.exit_replace_attempt == 0, (
        "exit_replace_attempt must not be aliased to/derived from "
        "_exit_stuck_count; they have independent semantics."
    )


def test_partial_replacement_cap_limits_next_submit_to_unfilled_remainder():
    eng = _engine()
    pos = _pos(last_exit_signal_ts=None)
    _add(eng, pos)
    assert eng.mark_exit_replacement_safe(
        pos.position_id,
        reason="durable partial remainder",
        local_order_id="loc-old-1",
        broker_order_id="bro-old-1",
        replacement_qty=1,
    ) is True
    pos.exit_in_flight = False
    eng._can_submit_exit = MagicMock(return_value=False)

    decision = ExitDecision(
        action="STOP", quantity=7, reason="replacement", urgency="HIGH", pnl_pct=-0.5,
    )
    assert eng._submit_exit_decision(pos, decision) is False
    assert decision.quantity == 1
    eng._can_submit_exit.assert_called_once()


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


def test_stale_exit_replacement_generation_waits_for_durable_fence():
    eng = _engine()
    pos = _pos()
    _add(eng, pos)

    assert eng.mark_exit_replacement_safe(
        "pos-avgo-1",
        reason="broker cancel staged",
        local_order_id="loc-old-1",
        broker_order_id="bro-old-1",
        defer_attempt=True,
    ) is True
    assert pos.exit_replace_attempt == 0
    assert pos.pending_exit_replace_allowed is True
    assert pos.pending_exit_replace_durable_pending is True
    assert eng._can_submit_exit(pos, datetime.now(timezone.utc), allow_inflight_override=True) is False

    assert eng.finalize_exit_replacement_safe(
        "pos-avgo-1",
        reason="OSM CANCELED durable",
        local_order_id="loc-old-1",
        broker_order_id="bro-old-1",
    ) is True
    assert pos.exit_replace_attempt == 1
    assert pos.pending_exit_replace_durable_pending is False


def test_staged_replacement_finalize_blocks_when_generation_persist_fails():
    eng = _engine()
    pos = _pos()
    _add(eng, pos)
    eng._persist_exit_replace_attempt_to_db = MagicMock(return_value=False)

    assert eng.mark_exit_replacement_safe(
        "pos-avgo-1",
        reason="broker cancel staged",
        local_order_id="loc-old-1",
        broker_order_id="bro-old-1",
        defer_attempt=True,
    ) is False
    assert pos.pending_exit_replace_allowed is False
    assert pos.pending_exit_replace_durable_pending is False
    assert eng.finalize_exit_replacement_safe(
        "pos-avgo-1",
        reason="OSM CANCELED durable",
        local_order_id="loc-old-1",
        broker_order_id="bro-old-1",
    ) is False
    assert pos.exit_replace_attempt == 0
    assert pos.pending_exit_replace_durable_pending is False


def test_staged_replacement_can_be_revoked_without_clearing_old_owner():
    eng = _engine()
    pos = _pos()
    _add(eng, pos)

    eng.mark_exit_replacement_safe(
        "pos-avgo-1",
        reason="broker cancel staged",
        local_order_id="loc-old-1",
        broker_order_id="bro-old-1",
        defer_attempt=True,
    )
    assert eng.revoke_exit_replacement_safe(
        "pos-avgo-1",
        reason="OSM transition failed",
        local_order_id="loc-old-1",
        broker_order_id="bro-old-1",
    ) is True
    assert pos.exit_in_flight is True
    assert pos.pending_exit_replace_allowed is False
    assert pos.pending_exit_replace_durable_pending is False
    assert pos.exit_replace_attempt == 0


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

    class _OSM:
        def get_order(self, local_order_id):
            return {
                "local_order_id": local_order_id,
                "broker_order_id": "bro-new-1",
                "kind": "EXIT",
                "position_id": pos.position_id,
                "client_id": pos.client_id,
                "execution_mode": pos.execution_mode,
                "status": "EXIT_REQUESTED",
                "qty": 2,
            }

    eng.order_state_machine = _OSM()
    eng.osm = eng.order_state_machine
    assert eng._mark_replacement_owned_by_new_generation(
        pos,
        local_order_id="loc-new-1",
        broker_order_id="bro-new-1",
        qty=2,
    ) is True

    # That replacement itself later goes stale and gets proven-canceled.
    eng.mark_exit_replacement_safe(
        "pos-avgo-1", reason="proof-2", local_order_id="loc-new-1", broker_order_id="bro-new-1",
    )
    assert pos.exit_replace_attempt == 2


def test_pricing_attempt_is_capped_but_replacement_generation_is_monotonic(monkeypatch):
    """Six replacement cycles separate pricing pressure from identity."""
    import ap_exit_engine as exit_engine_module

    monkeypatch.setattr(exit_engine_module, "EXIT_REPLACE_MAX_ATTEMPTS", 4)
    eng = _engine()
    pos = _pos()
    _add(eng, pos)
    persisted_cycles = []
    eng._persist_exit_replace_attempt_to_db = MagicMock(
        side_effect=lambda current_pos: persisted_cycles.append((
            current_pos.exit_replace_attempt,
            current_pos.exit_retry_liveness["replacement_generation"],
        )) or True
    )

    for cycle in range(6):
        if cycle:
            # The prior one-shot grant was consumed by a replacement submit;
            # these are new, independently identified old generations.
            pos.pending_exit_replace_allowed = False
            pos.pending_exit_local_order_id = f"loc-old-{cycle}"
            pos.pending_exit_broker_order_id = f"bro-old-{cycle}"
            pos.exit_retry_liveness = {
                "state": "NONE",
                "replace_attempt": pos.exit_replace_attempt,
                "replacement_generation": cycle,
                "replace_quantity": 0,
                "last_ack_identity": "",
            }
        eng.mark_exit_replacement_safe(
            pos.position_id,
            reason=f"broker-confirmed-cancel-{cycle}",
            local_order_id=pos.pending_exit_local_order_id,
            broker_order_id=pos.pending_exit_broker_order_id,
        )
        assert pos.exit_replace_attempt == min(cycle + 1, 4)
        assert pos.exit_retry_liveness["replacement_generation"] == cycle + 1

    assert exit_engine_module.EXIT_REPLACE_MAX_ATTEMPTS == 4
    assert pos.exit_replace_attempt == 4
    assert persisted_cycles == [(1, 1), (2, 2), (3, 3), (4, 4), (4, 5), (4, 6)]
    assert [generation for _, generation in persisted_cycles] == [1, 2, 3, 4, 5, 6]


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


# ── Proven economic completion resets the counter ──────────────────────────

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


def test_completed_scale_out_resets_and_persists_replacement_generation():
    eng = _engine()
    pos = _pos(
        pending_exit_action="SCALE_OUT",
        pending_exit_filled_qty=0,
        exit_replace_attempt=3,
        _exit_replace_attempt_last_ack_identity="bro-old-1",
    )
    _add(eng, pos)

    eng.note_partial_exit_fill(
        "pos-avgo-1",
        qty_filled=2,
        fill_price=2.10,
        local_order_id="loc-old-1",
        broker_order_id="bro-old-1",
        cumulative_filled=2,
    )

    assert pos.quantity_remaining == 5
    assert pos.exit_in_flight is False
    assert pos.exit_replace_attempt == 0
    assert pos._exit_replace_attempt_last_ack_identity == ""
    eng._persist_exit_replace_attempt_to_db.assert_called_once_with(pos)


def test_fully_filled_owned_replacement_consumes_lifecycle_and_reopens_exit_liveness():
    """A filled replacement tranche must release OWNED for the next exit."""
    eng = _engine()
    pos = _pos(
        quantity=5,
        quantity_remaining=5,
        pending_exit_action="CLOSE_ALL",
        pending_exit_local_order_id="loc-new-1",
        pending_exit_broker_order_id="bro-new-1",
        pending_exit_qty=1,
        pending_exit_filled_qty=0,
        exit_replace_attempt=1,
    )
    pos.exit_retry_liveness = {
        "state": "REPLACEMENT_OWNED_BY_NEW_GENERATION",
        "replace_attempt": 1,
        "replacement_generation": 1,
        "replace_quantity": 1,
        "position_id": pos.position_id,
        "client_id": pos.client_id,
        "execution_mode": pos.execution_mode,
        "old_local_order_id": "loc-old-1",
        "old_broker_order_id": "bro-old-1",
        "new_local_order_id": "loc-new-1",
        "new_broker_order_id": "bro-new-1",
        "last_ack_identity": "bro-old-1",
    }
    _add(eng, pos)
    captured = {}

    def _persist(current_pos, *, expected_state=None, expected_generation=None,
                 expected_new_local_order_id=None, expected_new_broker_order_id=None,
                 expected_filled_qty=None):
        captured.update(
            expected_state=expected_state,
            expected_generation=expected_generation,
            expected_new_local_order_id=expected_new_local_order_id,
            expected_new_broker_order_id=expected_new_broker_order_id,
            expected_filled_qty=expected_filled_qty,
        )
        return True

    eng._persist_exit_replace_attempt_to_db = _persist
    eng.note_partial_exit_fill(
        pos.position_id,
        qty_filled=1,
        fill_price=2.10,
        local_order_id="loc-new-1",
        broker_order_id="bro-new-1",
        cumulative_filled=1,
    )

    assert pos.quantity_remaining == 4
    assert pos.exit_in_flight is False
    assert pos.exit_retry_liveness["state"] == "NONE"
    assert pos.exit_replace_attempt == 0
    assert captured == {
        "expected_state": "REPLACEMENT_OWNED_BY_NEW_GENERATION",
        "expected_generation": 1,
        "expected_new_local_order_id": "loc-new-1",
        "expected_new_broker_order_id": "bro-new-1",
        "expected_filled_qty": 1,
    }
    assert eng._can_submit_exit(pos, datetime.now(timezone.utc)) is True


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
    eng = _engine(durable_persist=False)
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
    assert "replacement_generation')::bigint" in captured["sql"]
    import json
    patch = json.loads(captured["params"][0])
    assert patch["exit_retry_liveness"]["replace_attempt"] == 2
    assert patch["exit_retry_liveness"]["last_ack_identity"] == "bro-old-1"
    # Confirms the write targets ONLY the exit_retry_liveness namespace —
    # it cannot clobber unrelated meta keys like hard_exit_reference because
    # the SQL itself merges (||) rather than replaces the meta column.


def test_persist_exit_replace_attempt_failure_blocks_replacement(monkeypatch):
    """A DB error must fail closed before replacement authority is granted."""
    eng = _engine(durable_persist=False)
    pos = _pos()
    _add(eng, pos)

    import ap.db as db_mod

    def _boom():
        raise ConnectionError("db unreachable")

    monkeypatch.setattr(db_mod, "conn", _boom)

    accepted = eng.mark_exit_replacement_safe(
        "pos-avgo-1", reason="proof", local_order_id="loc-old-1", broker_order_id="bro-old-1",
    )
    assert accepted is False
    assert pos.exit_replace_attempt == 0
    assert pos.pending_exit_replace_allowed is False


@pytest.mark.parametrize(
    "raw_meta, expected",
    [
        ({}, {"replace_attempt": 0, "last_ack_identity": "", "replace_quantity": 0}),
        ({"exit_retry_liveness": {"replace_attempt": "bad", "replace_quantity": -3}},
         {"replace_attempt": 0, "last_ack_identity": "", "replace_quantity": 0}),
        ({"exit_retry_liveness": {"replace_attempt": 2, "last_ack_identity": "bro-old", "replace_quantity": 1}},
         {"replace_attempt": 2, "last_ack_identity": "bro-old", "replace_quantity": 1}),
        ('{"exit_retry_liveness": {"replace_attempt": 3, "replace_quantity": 2}}',
         {"replace_attempt": 3, "last_ack_identity": "", "replace_quantity": 2}),
        ({"exit_retry_liveness": {"replace_attempt": 999}},
         {"replace_attempt": 4, "last_ack_identity": "", "replace_quantity": 0}),
    ],
)
def test_restore_exit_replace_attempt_helper_is_fail_safe(raw_meta, expected):
    assert _restore_exit_replace_attempt_from_meta(raw_meta) == expected


def test_declared_invalid_lifecycle_clears_legacy_pricing_mirrors():
    restored = _restore_exit_replace_attempt_from_meta({
        "exit_retry_liveness": {
            "state": "REPLACEMENT_PENDING",
            "replace_attempt": 4,
            "replacement_generation": "bad",
            "replace_quantity": 1,
        },
    })

    assert restored["replace_attempt"] == 0
    assert restored["last_ack_identity"] == ""
    assert restored["replace_quantity"] == 0
    assert restored["lifecycle"] is None
    assert restored["lifecycle_valid"] is False


def test_seed_from_db_restores_retry_generation_and_replacement_cap(monkeypatch):
    eng = _engine()
    eng.hydrate_pending_exit_identity_from_db = MagicMock(return_value=False)

    class _PositionManager:
        def get_active_positions(self):
            return [{
                "id": "pos-restart",
                "client_id": "client-1",
                "underlying": "AVGO",
                "contract": "AVGO260814C00350000",
                "direction": "CALL",
                "qty": 7,
                "quantity_remaining": 5,
                "avg_fill": 2.49,
                "underlying_entry": 350.0,
                "target_underlying": 360.0,
                "stop_underlying": 340.0,
                "execution_mode": "paper",
                "meta": {
                    "exit_retry_liveness": {
                        "replace_attempt": 2,
                        "last_ack_identity": "bro-old",
                        "replace_quantity": 1,
                    }
                },
            }]

    eng.seed_from_db(_PositionManager())
    restored = eng.get_position("pos-restart")
    assert restored is not None
    assert restored.exit_replace_attempt == 2
    assert restored._exit_replace_attempt_last_ack_identity == "bro-old"
    assert restored.pending_exit_replace_qty == 1


def test_invalid_declared_lifecycle_cannot_change_adaptive_pricing_after_seed(monkeypatch):
    eng = _engine()
    eng.hydrate_pending_exit_identity_from_db = MagicMock(return_value=False)

    class _PositionManager:
        def get_active_positions(self):
            return [{
                "id": "pos-invalid-lifecycle",
                "client_id": "client-1",
                "underlying": "AVGO",
                "contract": "AVGO260814C00350000",
                "direction": "CALL",
                "qty": 7,
                "quantity_remaining": 7,
                "avg_fill": 2.49,
                "underlying_entry": 350.0,
                "target_underlying": 360.0,
                "stop_underlying": 340.0,
                "execution_mode": "paper",
                "meta": {
                    "exit_retry_liveness": {
                        "state": "REPLACEMENT_PENDING",
                        "replace_attempt": 4,
                        "replacement_generation": "bad",
                        "replace_quantity": 1,
                    },
                },
            }]

    eng.seed_from_db(_PositionManager())
    pos = eng.get_position("pos-invalid-lifecycle")
    assert pos is not None
    assert pos.exit_replace_attempt == 0
    assert pos.pending_exit_replace_qty == 0
    assert pos.pending_exit_replace_allowed is False
    assert pos.pending_exit_replace_revalidated is False
    assert pos.pending_exit_replace_submit_claimed is False
    assert pos.exit_retry_liveness == {}

    now = datetime.now(timezone.utc)
    pos.exit_in_flight = False
    pos.current_bid = 2.00
    pos.current_ask = 2.40
    pos.current_option_price = 2.20
    pos.option_bid_valid = True
    pos.option_quote_fresh = True
    pos.last_quote_update_ts = now
    pos.last_option_quote_update_ts = now
    eng._can_submit_exit = MagicMock(return_value=True)
    eng.on_exit = lambda _pos, _decision: {
        "local_order_id": "loc-invalid-lifecycle-new",
        "broker_order_id": "bro-invalid-lifecycle-new",
    }
    import ap.exit_safety as exit_safety
    monkeypatch.setattr(
        exit_safety,
        "resolve_exit_broker_truth",
        lambda **_kwargs: {"broker_truth_open_qty": None, "is_fresh_exact": False},
    )
    monkeypatch.setattr(
        exit_safety,
        "evaluate_exit_submission_safety",
        lambda **_kwargs: {"blocked": False},
    )

    decision = ExitDecision(
        action="CLOSE_ALL",
        quantity=1,
        reason="IMMEDIATE TP",
        reason_code="IMMEDIATE_TP",
        urgency="NORMAL",
        pnl_pct=0.20,
    )
    assert eng._submit_exit_decision(pos, decision) is True
    assert decision._pricing_meta["attempt"] == 0
    assert decision._pricing_meta["tier"] == "PROFIT_MID"
    assert decision.suggested_limit == 2.20


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v"]))
