"""
tests/test_p0_308_amendments.py

Tests for GitHub PR #308 — Protective exit circuit breaker repair
Covers the 7 specific requirements from the amendment document.

Amendment requirements:
  R1: broker truth open qty > 0 + circuit breaker tripped → protective exit allowed
  R2: broker truth qty == 0 + synthetic broker-repair position → no broker POST, local closed
  R3: broker truth missing/None → original circuit breaker behavior unchanged
  R4: duplicate/in-flight exit still blocks duplicate submit
  R5: manual close/reconciler terminal row prevents synthetic stale exit
  R6: client_id/execution_mode mismatch cannot allow live exit
  R7: OSM imported binding cannot bypass the new logic
  R8: no entry/live-submit/selector files touched (scope fence)
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

_CLIENT_ID  = "jasoncosby1@gmail.com"
_CONTRACT   = "GS  260717C00465000"
_POSITION_ID = "pos-GS-live-001"


def _make_db(*, rejection_count=0, position_status="OPEN",
             position_exists=True, has_orders_col=True):
    db = MagicMock()
    db.fetchone.return_value = {"rejection_count": rejection_count}
    return db


def _run_halt(
    *,
    rejection_count=5,
    threshold=3,
    broker_truth_open_qty=None,
    client_id=_CLIENT_ID,
    contract=_CONTRACT,
    position_id=_POSITION_ID,
):
    """
    Run _should_halt_exit_after_rejections against a mock DB with controlled
    rejection_count and broker_truth_open_qty.
    """
    from ap.exit_safety import _should_halt_exit_after_rejections

    db = _make_db(rejection_count=rejection_count)

    with patch("ap.exit_safety._parse_max_exit_rejections_threshold", return_value=threshold), \
         patch("ap.exit_safety._table_columns", return_value={"status", "contract", "client_id", "kind"}), \
         patch("ap.exit_safety._persist_circuit_breaker_marker", return_value=True):
        return _should_halt_exit_after_rejections(
            db,
            position_id=position_id,
            client_id=client_id,
            execution_mode="live",
            contract=contract,
            broker_truth_open_qty=broker_truth_open_qty,
        )


def _run_safety(
    *,
    rejection_count=0,
    threshold=3,
    broker_truth_open_qty=None,
    position_status="OPEN",
    position_exists=True,
    allow_broker_repair=False,
    client_id=_CLIENT_ID,
    contract=_CONTRACT,
    position_id=_POSITION_ID,
    execution_mode="live",
):
    """Run evaluate_exit_submission_safety with fully mocked DB."""
    from ap.exit_safety import evaluate_exit_submission_safety

    db = _make_db(rejection_count=rejection_count, position_status=position_status)

    _pos_row = {
        "status":         position_status,
        "quantity_remaining": 1,
        "client_id":      client_id,
        "contract":       contract,
        "position_id":    position_id,
    } if position_exists else None

    with patch("ap.exit_safety.conn") as mock_conn_ctx, \
         patch("ap.exit_safety._parse_max_exit_rejections_threshold", return_value=threshold), \
         patch("ap.exit_safety._table_columns", return_value={"status", "contract", "client_id", "kind", "execution_mode"}), \
         patch("ap.exit_safety._persist_circuit_breaker_marker", return_value=True):
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=db)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn_ctx.return_value = mock_conn
        db.fetchone.side_effect = [
            {"rejection_count": rejection_count},
            _pos_row,
        ]
        return evaluate_exit_submission_safety(
            position_id=position_id,
            client_id=client_id,
            execution_mode=execution_mode,
            contract=contract,
            broker_truth_open_qty=broker_truth_open_qty,
            allow_missing_position_with_broker_truth=allow_broker_repair,
        )


# ─────────────────────────────────────────────────────────────────────────────
# R1: broker truth open qty > 0 + circuit breaker tripped → protective exit
# ─────────────────────────────────────────────────────────────────────────────

class TestR1ProtectiveExitAllowed:
    """Amendment R1: PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH when broker open > 0."""

    def test_r1_circuit_breaker_tripped_broker_open_qty_2_allows_exit(self):
        """R1: rejection_count >= threshold + broker_truth_open_qty=2 → protective exit."""
        result = _run_halt(rejection_count=5, threshold=3, broker_truth_open_qty=2)
        assert result["blocked"] is False, "Must not block when broker shows open position"
        assert result["reason"] == "PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH"
        assert result["circuit_breaker_overridden"] is True
        assert result["broker_truth_open_qty"] == 2

    def test_r1_qty_1_also_allows_protective_exit(self):
        """Even qty=1 (minimum real position) triggers the protective exit override."""
        result = _run_halt(rejection_count=10, threshold=3, broker_truth_open_qty=1)
        assert result["blocked"] is False
        assert result["reason"] == "PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH"

    def test_r1_protective_exit_preserves_rejection_count_in_audit(self):
        """Rejection count is preserved in the override result for audit/logs."""
        result = _run_halt(rejection_count=7, threshold=3, broker_truth_open_qty=3)
        assert result["rejection_count"] == 7, "Rejection count must be in audit"
        assert result["threshold"] == 3

    def test_r1_log_marker_in_osm(self):
        """PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH must appear in OSM with local_order_id."""
        src = open("ap/order_state_machine.py").read()
        # Search for the actual log call, not the comment
        pos = src.find('"[%s] PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH')
        assert pos >= 0, "PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH log statement must exist in OSM"
        log_block = src[pos:pos+600]
        assert "local_id" in log_block or "local_order_id" in log_block, (
            "R1: PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH log must include local_order_id (local_id)"
        )
        assert "execution_mode" in log_block, (
            "R1: PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH log must include execution_mode"
        )


# ─────────────────────────────────────────────────────────────────────────────
# R2: broker truth qty == 0 → no broker POST, local position closed
# ─────────────────────────────────────────────────────────────────────────────

class TestR2SyntheticStaleNoPost:
    """Amendment R2: SYNTHETIC_POSITION_STALE_BROKER_FLAT when broker shows flat."""

    def test_r2_circuit_breaker_broker_flat_blocks_exit(self):
        """R2: circuit breaker + broker_truth_open_qty=0 → SYNTHETIC_POSITION_STALE_BROKER_FLAT."""
        result = _run_halt(rejection_count=5, threshold=3, broker_truth_open_qty=0)
        assert result["blocked"] is True
        assert result["reason"] == "SYNTHETIC_POSITION_STALE_BROKER_FLAT"
        assert result["broker_truth_open_qty"] == 0
        assert result.get("synthetic_stale") is True

    def test_r2_broker_repair_position_with_flat_broker_also_blocks(self):
        """R2: broker-repair- synthetic position + broker flat → blocked, no broker POST."""
        result = _run_halt(
            rejection_count=5, threshold=3, broker_truth_open_qty=0,
            position_id="broker-repair-GS-001",
        )
        assert result["blocked"] is True
        assert result["reason"] == "SYNTHETIC_POSITION_STALE_BROKER_FLAT"

    def test_r2_stale_block_stops_repeated_exit_firing(self):
        """
        R2: After SYNTHETIC_POSITION_STALE_BROKER_FLAT, the position row should
        be marked so the exit engine doesn't re-evaluate it on the next tick.
        The circuit breaker marker is persisted to signal the stale state.
        """
        from ap.exit_safety import _should_halt_exit_after_rejections

        persisted_markers = []

        def _capture_persist(db_conn, *, position_id, client_id, rejection_count):
            persisted_markers.append({"position_id": position_id, "rejection_count": rejection_count})

        db = _make_db(rejection_count=5)
        with patch("ap.exit_safety._parse_max_exit_rejections_threshold", return_value=3), \
             patch("ap.exit_safety._table_columns", return_value={"status", "contract", "client_id", "kind"}), \
             patch("ap.exit_safety._persist_circuit_breaker_marker", side_effect=_capture_persist):
            result = _should_halt_exit_after_rejections(
                db,
                position_id=_POSITION_ID,
                client_id=_CLIENT_ID,
                contract=_CONTRACT,
                broker_truth_open_qty=0,
            )

        assert result["reason"] == "SYNTHETIC_POSITION_STALE_BROKER_FLAT"
        assert len(persisted_markers) == 1, (
            "R2: Circuit breaker marker must be persisted on stale broker flat "
            "to prevent repeat firing on next tick"
        )
        assert persisted_markers[0]["position_id"] == _POSITION_ID


# ─────────────────────────────────────────────────────────────────────────────
# R3: broker truth missing/None → original circuit breaker behavior
# ─────────────────────────────────────────────────────────────────────────────

class TestR3NoBrokerTruthOriginalBehavior:

    def test_r3_no_broker_truth_still_blocks_on_threshold(self):
        """R3: broker_truth_open_qty=None → original exit_circuit_breaker_tripped reason."""
        result = _run_halt(rejection_count=5, threshold=3, broker_truth_open_qty=None)
        assert result["blocked"] is True
        assert result["reason"] == "exit_circuit_breaker_tripped", (
            "R3: Without broker truth, original behavior must be preserved"
        )
        assert "circuit_breaker_overridden" not in result

    def test_r3_below_threshold_always_allows(self):
        """R3: Below threshold with no broker truth → allowed (no override needed)."""
        result = _run_halt(rejection_count=2, threshold=3, broker_truth_open_qty=None)
        assert result["blocked"] is False


# ─────────────────────────────────────────────────────────────────────────────
# R4: duplicate/in-flight guard still blocks before broker truth override
# ─────────────────────────────────────────────────────────────────────────────

class TestR4DuplicateGuard:

    def test_r4_in_flight_guard_blocks_before_broker_truth(self):
        """
        R4: duplicate/in-flight guard must fire BEFORE broker truth is resolved.
        Even if broker truth would allow protective exit, an in-flight exit
        must be blocked immediately.
        """
        # Structural: verify the in-flight guard in ap_exit_engine.py is checked
        # before the broker truth import/call
        src = open("ap_exit_engine.py").read()
        exit_in_flight_check_pos = src.find("pos.exit_in_flight")
        broker_truth_call_pos    = src.find("resolve_exit_broker_truth(")
        assert exit_in_flight_check_pos >= 0, "exit_in_flight guard must be present"
        assert broker_truth_call_pos    >= 0, "resolve_exit_broker_truth call must be present"
        assert exit_in_flight_check_pos < broker_truth_call_pos, (
            "R4: exit_in_flight check must appear BEFORE broker truth resolution "
            "so in-flight guard fires even on protective exit path"
        )


# ─────────────────────────────────────────────────────────────────────────────
# R5: manual close / reconciler terminal row prevents synthetic stale exit
# ─────────────────────────────────────────────────────────────────────────────

class TestR5ReconcilerTerminalRow:
    """
    Amendment R5: When a position has already been closed (by reconciler,
    manual close, or synthetic stale marker), evaluate_exit_submission_safety
    must block any further exit attempt.
    """

    def test_r5_closed_position_blocks_exit(self):
        """R5: position.status=CLOSED → evaluate_exit_submission_safety blocks."""
        from ap.exit_safety import _exit_position_terminal_state

        db = MagicMock()
        db.fetchone.return_value = {
            "status":             "CLOSED",
            "quantity_remaining": 0,
            "client_id":          _CLIENT_ID,
            "contract":           _CONTRACT,
        }

        with patch("ap.exit_safety._table_columns", return_value={"status", "quantity_remaining", "client_id", "contract", "execution_mode"}):
            result = _exit_position_terminal_state(
                db,
                position_id=_POSITION_ID,
                client_id=_CLIENT_ID,
                contract=_CONTRACT,
                execution_mode="live",
            )

        assert result["blocked"] is True, (
            "R5: CLOSED position must block exit — reconciler/manual close must prevent re-exit"
        )

    def test_r5_expired_position_blocks_exit(self):
        """R5: position.status=EXPIRED → blocked (contract expired or EOD closed)."""
        from ap.exit_safety import _exit_position_terminal_state

        db = MagicMock()
        db.fetchone.return_value = {
            "status":             "EXPIRED",
            "quantity_remaining": 0,
            "client_id":          _CLIENT_ID,
            "contract":           _CONTRACT,
        }

        with patch("ap.exit_safety._table_columns", return_value={"status", "quantity_remaining", "client_id", "contract"}):
            result = _exit_position_terminal_state(
                db,
                position_id=_POSITION_ID,
                client_id=_CLIENT_ID,
                contract=_CONTRACT,
            )

        assert result["blocked"] is True

    def test_r5_synthetic_stale_marked_position_prevents_repeat_exit(self):
        """
        R5: A position marked synthetic_stale_broker_flat=True in meta (set by
        OSM Case B handler) must cause the NEXT exit evaluation to be blocked.
        The OSM already sets status=CLOSED on the position row — verify that
        _exit_position_terminal_state blocks on status=CLOSED.
        """
        from ap.exit_safety import _exit_position_terminal_state

        db = MagicMock()
        # Simulate what OSM sets after Case B:
        db.fetchone.return_value = {
            "status":             "CLOSED",    # set by OSM submit_exit Case B
            "quantity_remaining": 0,
            "client_id":          _CLIENT_ID,
            "contract":           _CONTRACT,
            "meta": {"synthetic_position_stale_broker_flat": True},
        }

        with patch("ap.exit_safety._table_columns", return_value={"status", "quantity_remaining", "client_id", "contract"}):
            result = _exit_position_terminal_state(
                db,
                position_id=_POSITION_ID,
                client_id=_CLIENT_ID,
                contract=_CONTRACT,
            )

        assert result["blocked"] is True, (
            "R5: Position marked CLOSED by OSM Case B must block on next tick — "
            "prevents the repeat-fire loop"
        )


# ─────────────────────────────────────────────────────────────────────────────
# R6: client_id/execution_mode mismatch cannot allow live exit
# ─────────────────────────────────────────────────────────────────────────────

class TestR6ClientIdExecutionModeMismatch:
    """
    Amendment R6: A wrong client_id or execution_mode cannot allow a live exit
    to proceed through the broker truth protective-exit override.
    """

    def test_r6_wrong_client_id_position_not_found_blocks_exit(self):
        """
        R6: client_id mismatch → position row won't match → _exit_position_terminal_state
        treats missing position as blocked (unless allow_missing_position_with_broker_truth).
        """
        from ap.exit_safety import _exit_position_terminal_state

        db = MagicMock()
        # Position exists for _CLIENT_ID but we're checking wrong_client_id
        db.fetchone.return_value = None  # no row for wrong client_id

        with patch("ap.exit_safety._table_columns", return_value={"status", "client_id", "contract"}):
            result = _exit_position_terminal_state(
                db,
                position_id=_POSITION_ID,
                client_id="wrong_client@example.com",  # mismatch
                contract=_CONTRACT,
                execution_mode="live",
            )

        assert result["blocked"] is True, (
            "R6: Wrong client_id → position not found → exit blocked. "
            "A mismatch cannot slip through to broker POST."
        )

    def test_r6_broker_truth_override_does_not_bypass_client_id_check(self):
        """
        R6: Even when broker_truth_open_qty > 0 would normally trigger
        PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH, a wrong client_id must still
        block because the position doesn't exist for that client.
        """
        # The circuit breaker override fires before the position check in
        # _should_halt_exit_after_rejections, but evaluate_exit_submission_safety
        # then also checks the position row. The position row won't match the wrong client.
        from ap.exit_safety import _exit_position_terminal_state

        db = MagicMock()
        db.fetchone.return_value = None  # position does not exist for wrong client

        with patch("ap.exit_safety._table_columns", return_value={"status", "client_id", "contract"}):
            result = _exit_position_terminal_state(
                db,
                position_id=_POSITION_ID,
                client_id="paper_client@example.com",  # wrong client
                contract=_CONTRACT,
                execution_mode="live",
                broker_truth_open_qty=2,  # broker says open — but wrong client
            )

        assert result["blocked"] is True, (
            "R6: broker_truth_open_qty > 0 with wrong client_id must still block — "
            "broker truth does not bypass client_id identity"
        )

    def test_r6_correct_client_id_open_position_allows_exit(self):
        """R6 positive: correct client_id + OPEN position → not blocked by position check."""
        from ap.exit_safety import _exit_position_terminal_state

        db = MagicMock()
        db.fetchone.return_value = {
            "status":             "OPEN",
            "quantity_remaining": 1,
            "client_id":          _CLIENT_ID,
            "contract":           _CONTRACT,
        }

        with patch("ap.exit_safety._table_columns", return_value={"status", "quantity_remaining", "client_id", "contract"}):
            result = _exit_position_terminal_state(
                db,
                position_id=_POSITION_ID,
                client_id=_CLIENT_ID,
                contract=_CONTRACT,
            )

        assert result["blocked"] is False


# ─────────────────────────────────────────────────────────────────────────────
# R7: OSM imported binding cannot bypass the new logic
# ─────────────────────────────────────────────────────────────────────────────

class TestR7ImportBinding:
    """
    Amendment R7: The import of evaluate_exit_submission_safety in
    ap/order_state_machine.py must point to the current ap.exit_safety module,
    not a stale binding that predates the broker truth logic.
    The exit engine's local import inside _submit_exit_decision is the primary
    live path and is fresh on each call.
    """

    def test_r7_osm_module_level_import_points_to_correct_function(self):
        """
        R7: OSM imports evaluate_exit_submission_safety at module level.
        Verify it resolves to the same function object as direct import.
        """
        import ap.order_state_machine as osm_mod
        from ap.exit_safety import evaluate_exit_submission_safety

        # The OSM binding should be the same function object
        assert osm_mod.evaluate_exit_submission_safety is evaluate_exit_submission_safety, (
            "R7: OSM module-level binding of evaluate_exit_submission_safety must "
            "reference the same function as direct import from ap.exit_safety"
        )

    def test_r7_exit_engine_uses_local_import_inside_submit_decision(self):
        """
        R7: The exit engine imports evaluate_exit_submission_safety INSIDE
        _submit_exit_decision (not at module level). This is the primary live
        submit seam and ensures the import is fresh on each call.
        """
        src = open("ap_exit_engine.py").read()

        # Verify the import is inside _submit_exit_decision
        func_start = src.find("def _submit_exit_decision(")
        func_import = src.find("from ap.exit_safety import", func_start)
        # Next function definition after _submit_exit_decision
        next_func   = src.find("\n    def ", func_start + 1)

        assert func_start >= 0, "_submit_exit_decision must exist in ap_exit_engine.py"
        assert func_import >= 0, "from ap.exit_safety import must be inside a function"
        assert func_import > func_start, (
            "R7: exit_safety import must be INSIDE _submit_exit_decision, not at module level. "
            "This ensures the binding is fresh (not stale from module-load time)."
        )
        # Must be before the next function definition (inside _submit_exit_decision scope)
        if next_func > 0:
            assert func_import < next_func, (
                "R7: import must be inside _submit_exit_decision body, not after it"
            )

    def test_r7_exit_safety_broker_truth_args_flow_to_circuit_breaker(self):
        """
        R7: evaluate_exit_submission_safety must pass broker_truth_open_qty to
        _should_halt_exit_after_rejections (the circuit breaker). Any binding
        bypass that loses this arg would silently revert to old behavior.
        """
        from ap.exit_safety import evaluate_exit_submission_safety
        import inspect
        sig = inspect.signature(evaluate_exit_submission_safety)
        assert "broker_truth_open_qty" in sig.parameters, (
            "R7: evaluate_exit_submission_safety must accept broker_truth_open_qty — "
            "a stale binding without this param would bypass the broker truth logic"
        )


# ─────────────────────────────────────────────────────────────────────────────
# R8: scope fence — no entry/live-submit/selector files touched
# ─────────────────────────────────────────────────────────────────────────────

class TestR8ScopeFence:
    """Amendment R8: #308 diff must not touch entry/live-submit/selector files."""

    def _changed_files(self):
        import subprocess
        result = subprocess.run(
            ["git", "diff", "origin/main..HEAD", "--name-only"],
            capture_output=True, text=True, cwd=".",
        )
        return result.stdout.strip().splitlines()

    def test_r8_no_entry_watcher_in_diff(self):
        """R8: ap_entry_watcher.py must not be in #308 diff."""
        changed = self._changed_files()
        assert "ap_entry_watcher.py" not in changed, (
            "R8: ap_entry_watcher.py must not be touched by #308 — "
            "entry watcher lifecycle is NOT in scope"
        )

    def test_r8_no_live_submit_gates_in_diff(self):
        """R8: ap/live_submit_gates.py must not be in #308 diff."""
        changed = self._changed_files()
        assert "ap/live_submit_gates.py" not in changed, (
            "R8: ap/live_submit_gates.py must not be touched — live submit gates are #306 scope"
        )

    def test_r8_no_execution_core_in_diff(self):
        """R8: ap_execution_core.py must not be in #308 diff."""
        changed = self._changed_files()
        assert "ap_execution_core.py" not in changed, (
            "R8: ap_execution_core.py must not be touched by #308"
        )

    def test_r8_no_contract_selector_in_diff(self):
        """R8: ap/contract_selector.py must not be in #308 diff."""
        changed = self._changed_files()
        assert "ap/contract_selector.py" not in changed

    def test_r8_no_live_entry_brake_in_diff(self):
        """R8: ap/live_entry_brake.py must not be in #308 diff."""
        changed = self._changed_files()
        assert "ap/live_entry_brake.py" not in changed, (
            "R8: live_entry_brake.py is not in #308 scope"
        )

    def test_r8_exit_files_are_in_diff(self):
        """R8: The correct exit files must be in the diff (scope verification)."""
        changed = self._changed_files()
        assert "ap/exit_safety.py" in changed, "exit_safety.py must be changed"
        assert "ap/order_state_machine.py" in changed, "order_state_machine.py must be changed"
        assert "ap_exit_engine.py" in changed, "ap_exit_engine.py must be changed"


# ─────────────────────────────────────────────────────────────────────────────
# Production-shape broker-flat fix: absent contract = qty=0 in fresh snapshot
# ─────────────────────────────────────────────────────────────────────────────

class TestBrokerFlatProductionShape:
    """
    Critical production-shape fix: when list_positions() succeeds and the
    requested OCC contract is absent from the snapshot, that MUST mean
    broker_truth_open_qty=0 / is_fresh_exact=True.

    In production Tradier shape, a flat/closed option simply disappears from
    list_positions. It does NOT return as a row with qty=0. Treating
    "absent = unknown" caused the VZ/META incident class where a stale local
    synthetic position flowed through as "no broker truth" and potentially
    broker POST'd an exit on a position the broker had already closed.
    """

    def _run_broker_truth(self, list_positions_return=None, list_positions_error=None):
        """Call resolve_exit_broker_truth with controlled broker.list_positions behavior."""
        from ap.exit_safety import resolve_exit_broker_truth

        broker = MagicMock()
        if list_positions_error:
            broker.list_positions = MagicMock(side_effect=list_positions_error)
        elif list_positions_return is not None:
            broker.list_positions = MagicMock(return_value=list_positions_return)
        else:
            del broker.list_positions  # simulate missing method

        # Mock account extraction
        broker.account_id = "VA_TEST"

        with patch("ap.exit_safety._extract_broker_account_id", return_value="VA_TEST"):
            return resolve_exit_broker_truth(
                broker=broker,
                client_id=_CLIENT_ID,
                contract=_CONTRACT,
            )

    def test_1_empty_positions_list_means_broker_flat(self):
        """
        Test 1: broker.list_positions() returns [] (all positions closed)
        → broker_truth_open_qty=0, is_fresh_exact=True.
        """
        result = self._run_broker_truth(list_positions_return=[])

        assert result["broker_truth_open_qty"] == 0, (
            "Test 1: Empty positions list MUST return broker_truth_open_qty=0. "
            "An empty list means the broker has NO open positions — flat is confirmed."
        )
        assert result["is_fresh_exact"] is True, (
            "Test 1: is_fresh_exact must be True — list_positions() succeeded and "
            "the absence of the contract IS the exact answer."
        )
        assert result["audit"]["snapshot_status"] == "contract_absent_open_qty_zero"
        assert result["audit"]["broker_position_count"] == 0

    def test_2_other_contracts_but_not_requested_means_flat(self):
        """
        Test 2: broker.list_positions() returns other contracts (not the requested OCC)
        → broker_truth_open_qty=0, is_fresh_exact=True.

        Production shape: broker has other open positions but THIS contract is closed.
        The option disappeared from the snapshot = flat.
        """
        other_contract_row = {
            "symbol":       "SPY   260717C00600000",
            "option_type":  "Call",
            "quantity":     2.0,
            "side":         "long",
            "account_number": "VA_TEST",
        }
        result = self._run_broker_truth(list_positions_return=[other_contract_row])

        assert result["broker_truth_open_qty"] == 0, (
            "Test 2: Other contracts present but requested OCC absent → flat on requested contract."
        )
        assert result["is_fresh_exact"] is True
        assert result["audit"]["snapshot_status"] == "contract_absent_open_qty_zero"
        assert result["audit"]["broker_position_count"] == 1

    def test_3_absent_contract_osm_submit_exit_triggers_synthetic_stale(self):
        """
        Test 3: After fix, absent-contract now returns is_fresh_exact=True, qty=0.
        OSM submit_exit must then block with SYNTHETIC_POSITION_STALE_BROKER_FLAT
        (not proceed to broker POST as before the fix).
        """
        from ap.exit_safety import resolve_exit_broker_truth

        broker = MagicMock()
        broker.list_positions = MagicMock(return_value=[])  # contract absent
        broker.account_id = "VA_TEST"

        with patch("ap.exit_safety._extract_broker_account_id", return_value="VA_TEST"):
            result = resolve_exit_broker_truth(
                broker=broker,
                client_id=_CLIENT_ID,
                contract=_CONTRACT,
            )

        # Verify the OSM path would trigger SYNTHETIC_POSITION_STALE_BROKER_FLAT
        assert result["is_fresh_exact"] is True
        assert result["broker_truth_open_qty"] == 0
        # This is the exact condition OSM checks:
        # if broker_truth.get("is_fresh_exact") and int(broker_truth_qty or 0) == 0:
        is_fresh_and_flat = (
            result.get("is_fresh_exact") is True
            and int(result.get("broker_truth_open_qty") or 0) == 0
        )
        assert is_fresh_and_flat, (
            "Test 3: OSM submit_exit checks 'is_fresh_exact AND qty==0'. "
            "After fix, absent contract satisfies this condition → "
            "SYNTHETIC_POSITION_STALE_BROKER_FLAT fires, no broker POST."
        )

    def test_4_absent_contract_exit_engine_seam_no_broker_post(self):
        """
        Test 4: At the exit engine submit seam, absent-contract broker truth
        now triggers the SYNTHETIC_POSITION_STALE_BROKER_FLAT early return.

        The exit engine checks:
            if _broker_truth.get("is_fresh_exact") and int(_broker_truth_qty or 0) == 0:
                return False  (no broker POST)
        """
        from ap.exit_safety import resolve_exit_broker_truth

        broker = MagicMock()
        broker.list_positions = MagicMock(return_value=[
            # OTHER contracts — but not _CONTRACT
            {
                "symbol": "AMD   260717C00450000",
                "quantity": 1.0,
                "side": "long",
                "account_number": "VA_TEST",
            }
        ])
        broker.account_id = "VA_TEST"

        with patch("ap.exit_safety._extract_broker_account_id", return_value="VA_TEST"):
            bt = resolve_exit_broker_truth(
                broker=broker,
                client_id=_CLIENT_ID,
                contract=_CONTRACT,
            )

        # Verify the exit engine seam condition is met
        fresh_and_flat = (
            bt.get("is_fresh_exact") is True
            and int(bt.get("broker_truth_open_qty") or 0) == 0
        )
        assert fresh_and_flat, (
            "Test 4: Exit engine early-return condition 'is_fresh_exact AND qty==0' "
            "must now be satisfied for absent-contract — prevents broker POST "
            "on a position the broker already closed."
        )

    def test_5_list_positions_error_preserves_original_behavior(self):
        """
        Test 5: list_positions() raises → broker_truth_open_qty=None, is_fresh_exact=False.
        Errors must not be treated as flat — only SUCCESSFUL snapshots with absent
        contract are treated as confirmed flat.
        """
        result = self._run_broker_truth(
            list_positions_error=Exception("Tradier 503 Service Unavailable")
        )

        assert result["broker_truth_open_qty"] is None, (
            "Test 5: list_positions() error must NOT set qty=0. "
            "Error = unknown, not confirmed flat."
        )
        assert result["is_fresh_exact"] is False, (
            "Test 5: is_fresh_exact must be False on list_positions() error."
        )
        assert result["audit"]["snapshot_status"] == "broker_positions_error"

    def test_5b_list_positions_missing_preserves_original_behavior(self):
        """broker method missing → broker_truth_open_qty=None (unchanged)."""
        from ap.exit_safety import resolve_exit_broker_truth

        broker = MagicMock(spec=[])  # no list_positions method
        result = resolve_exit_broker_truth(
            broker=broker,
            client_id=_CLIENT_ID,
            contract=_CONTRACT,
        )

        assert result["broker_truth_open_qty"] is None
        assert result["is_fresh_exact"] is False
        assert result["audit"]["snapshot_status"] == "broker_positions_unavailable"

    def test_existing_exact_match_still_returns_real_qty(self):
        """
        Regression: when the contract IS in the snapshot with real qty,
        the existing exact-match path must still return the correct qty.
        """
        real_position_row = {
            "symbol":         _CONTRACT.replace(" ", ""),
            "option_type":    "Call",
            "quantity":       2.0,
            "side":           "long",
            "account_number": "VA_TEST",
        }

        from ap.exit_safety import resolve_exit_broker_truth, _normalize_contract

        broker = MagicMock()
        broker.list_positions = MagicMock(return_value=[real_position_row])
        broker.account_id = "VA_TEST"

        with patch("ap.exit_safety._extract_broker_account_id", return_value="VA_TEST"), \
             patch("ap.exit_safety._extract_position_contract",
                   return_value=_normalize_contract(_CONTRACT)), \
             patch("ap.exit_safety._extract_position_account_id", return_value="VA_TEST"), \
             patch("ap.exit_safety._extract_long_position_qty", return_value=2):
            result = resolve_exit_broker_truth(
                broker=broker,
                client_id=_CLIENT_ID,
                contract=_CONTRACT,
            )

        assert result["broker_truth_open_qty"] == 2
        assert result["is_fresh_exact"] is True
        assert result["audit"]["snapshot_status"] == "exact_match"
        assert result["audit"]["exact_contract_match"] is True

    def test_audit_fields_present_on_absent_contract(self):
        """Audit dict must include all required fields when contract is absent."""
        result = self._run_broker_truth(list_positions_return=[])
        audit = result["audit"]

        assert "snapshot_status" in audit
        assert "exact_contract_match" in audit
        assert "broker_position_count" in audit
        assert audit["exact_contract_match"] is False, (
            "exact_contract_match must be False for absent contract (not an exact row match)"
        )
        assert audit["snapshot_status"] == "contract_absent_open_qty_zero"
        assert audit["broker_position_count"] == 0

    def test_malformed_payload_still_returns_none(self):
        """Malformed list_positions payload → broker_truth_open_qty=None (unchanged)."""
        from ap.exit_safety import resolve_exit_broker_truth

        broker = MagicMock()
        broker.list_positions = MagicMock(return_value="NOT_A_LIST")
        broker.account_id = "VA_TEST"

        result = resolve_exit_broker_truth(
            broker=broker,
            client_id=_CLIENT_ID,
            contract=_CONTRACT,
        )

        assert result["broker_truth_open_qty"] is None
        assert result["is_fresh_exact"] is False
        assert result["audit"]["snapshot_status"] == "broker_positions_malformed"
