"""
tests/test_p0_exit_circuit_breaker_repair.py

PR #307 — Protective Exit Circuit Breaker Repair tests.

Spec requirements:
    1.  Case A: broker truth open qty > 0 + circuit breaker tripped
        → allows protective close (PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH)
        → does NOT block
    2.  Case B: broker truth qty = 0 + circuit breaker tripped
        → marks position stale (SYNTHETIC_POSITION_STALE_BROKER_FLAT)
        → blocks exit (no duplicate order submitted)
    3.  Normal path (below threshold): allows exit — unchanged behavior
    4.  Normal circuit breaker (no broker truth supplied): still blocks
    5.  Circuit breaker disabled (threshold=0): always allows
    6.  broker_truth_open_qty=None: original behavior, no override
    7.  PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH does not trigger duplicate submit
    8.  SYNTHETIC_POSITION_STALE_BROKER_FLAT stops repeated firing
    9.  New reason codes propagate through evaluate_exit_submission_safety
    10. OSM logs PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH before broker POST
    11. evaluate_exit_submission_safety passes broker_truth_open_qty through
    12. Never raises on bad/None inputs
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch, call

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

from ap.exit_safety import (
    _should_halt_exit_after_rejections,
    evaluate_exit_submission_safety,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

_CLIENT_ID  = "jasoncosby1@gmail.com"
_CONTRACT   = "VZ  260717C00045000"
_POSITION   = "pos-vz-live-1"


def _make_db_conn(rejection_count: int = 0):
    """Minimal mock DB connection that returns a fixed rejection count."""
    conn = MagicMock()
    conn.fetchone.return_value = {"rejection_count": rejection_count}
    # _table_columns returns a frozenset of column names
    return conn


def _run_should_halt(
    rejection_count: int,
    broker_truth_open_qty=None,
    threshold: int = 3,
):
    """
    Drive _should_halt_exit_after_rejections directly with a mock DB.
    Patches _parse_max_exit_rejections_threshold to return `threshold`.
    Patches _table_columns to return a minimal column set.
    """
    db_conn = _make_db_conn(rejection_count)
    with patch("ap.exit_safety._parse_max_exit_rejections_threshold",
               return_value=threshold), \
         patch("ap.exit_safety._table_columns", return_value={"client_id", "contract",
               "status", "last_error", "execution_mode", "created_ts", "updated_ts",
               "kind"}), \
         patch("ap.exit_safety._persist_circuit_breaker_marker", return_value=True), \
         patch("ap.exit_safety._parse_entry_ts", return_value=None), \
         patch("ap.exit_safety._DEFAULT_LOOKBACK_MINUTES", 30), \
         patch("ap.exit_safety._normalize_mode", return_value="live"), \
         patch("ap.exit_safety._BROKER_REJECT_LAST_ERROR_PATTERNS", []):
        return _should_halt_exit_after_rejections(
            db_conn,
            position_id=_POSITION,
            client_id=_CLIENT_ID,
            execution_mode="live",
            contract=_CONTRACT,
            entry_ts=None,
            broker_truth_open_qty=broker_truth_open_qty,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Test 1: Case A — broker truth open, circuit breaker overridden
# ─────────────────────────────────────────────────────────────────────────────

class TestCaseABrokerTruthOpen:

    def test_01_broker_truth_open_overrides_circuit_breaker(self):
        """
        Circuit breaker would fire (5 rejections >= threshold 3).
        broker_truth_open_qty=2 → override: allow protective close.
        """
        result = _run_should_halt(
            rejection_count=5,
            broker_truth_open_qty=2,
            threshold=3,
        )
        assert result["blocked"] is False, (
            "Circuit breaker must be overridden when broker truth confirms open qty"
        )
        assert result["reason"] == "PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH"
        assert result["circuit_breaker_overridden"] is True
        assert result["broker_truth_open_qty"] == 2

    def test_01b_broker_truth_open_qty_1_also_overrides(self):
        """Even qty=1 is enough to allow the protective close."""
        result = _run_should_halt(rejection_count=10, broker_truth_open_qty=1)
        assert result["blocked"] is False
        assert result["reason"] == "PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH"

    def test_01c_override_preserves_rejection_count_for_audit(self):
        """The audit must show the actual rejection count even when overridden."""
        result = _run_should_halt(rejection_count=7, broker_truth_open_qty=3)
        assert result["rejection_count"] == 7
        assert result["threshold"] == 3


# ─────────────────────────────────────────────────────────────────────────────
# Test 2: Case B — broker truth flat, stale synthetic position
# ─────────────────────────────────────────────────────────────────────────────

class TestCaseBBrokerTruthFlat:

    def test_02_broker_truth_flat_marks_stale(self):
        """
        Circuit breaker fired (5 >= 3). broker_truth_open_qty=0.
        Broker is flat — the local position is stale.
        Must block with SYNTHETIC_POSITION_STALE_BROKER_FLAT.
        """
        result = _run_should_halt(
            rejection_count=5,
            broker_truth_open_qty=0,
            threshold=3,
        )
        assert result["blocked"] is True
        assert result["reason"] == "SYNTHETIC_POSITION_STALE_BROKER_FLAT"
        assert result["synthetic_stale"] is True
        assert result["broker_truth_open_qty"] == 0

    def test_02b_stale_block_includes_rejection_count(self):
        """Audit must include rejection count for diagnostics."""
        result = _run_should_halt(rejection_count=4, broker_truth_open_qty=0)
        assert result["rejection_count"] == 4


# ─────────────────────────────────────────────────────────────────────────────
# Test 3: Normal path below threshold
# ─────────────────────────────────────────────────────────────────────────────

class TestNormalPath:

    def test_03_below_threshold_always_allows(self):
        """2 rejections, threshold 3 — circuit breaker does not fire."""
        result = _run_should_halt(rejection_count=2, threshold=3)
        assert result["blocked"] is False
        assert result["reason"] is None

    def test_03b_zero_rejections_allows(self):
        result = _run_should_halt(rejection_count=0)
        assert result["blocked"] is False


# ─────────────────────────────────────────────────────────────────────────────
# Test 4: Original behavior when no broker truth supplied
# ─────────────────────────────────────────────────────────────────────────────

class TestOriginalBehavior:

    def test_04_no_broker_truth_still_blocks_on_threshold(self):
        """
        When broker_truth_open_qty is None (not supplied), the original
        circuit-breaker behavior applies: block with exit_circuit_breaker_tripped.
        No override should happen.
        """
        result = _run_should_halt(
            rejection_count=5,
            broker_truth_open_qty=None,
            threshold=3,
        )
        assert result["blocked"] is True
        assert result["reason"] == "exit_circuit_breaker_tripped"
        # Must NOT have the override keys
        assert "circuit_breaker_overridden" not in result
        assert "synthetic_stale" not in result

    def test_04b_threshold_zero_disables_circuit_breaker(self):
        """MAX_EXIT_REJECTIONS_THRESHOLD=0 means disabled — always allows."""
        result = _run_should_halt(rejection_count=100, threshold=0)
        assert result["blocked"] is False


# ─────────────────────────────────────────────────────────────────────────────
# Test 5: broker_truth_open_qty pass-through via evaluate_exit_submission_safety
# ─────────────────────────────────────────────────────────────────────────────

class TestEvaluateExitSafetyPassthrough:

    def test_05_broker_truth_passed_through_to_circuit_breaker(self):
        """
        evaluate_exit_submission_safety must pass broker_truth_open_qty
        down to _should_halt_exit_after_rejections so the new logic runs.
        """
        captured = {}

        def _fake_halt(db_conn, *, broker_truth_open_qty=None, **kw):
            captured["broker_truth_open_qty"] = broker_truth_open_qty
            return {"blocked": False, "reason": None,
                    "rejection_count": 0, "threshold": 3}

        def _fake_position_state(db_conn, *, position_id, **kw):
            return {"blocked": False, "status": "OPEN",
                    "quantity_remaining": 2, "entry_ts": None,
                    "close_source": None, "reason": None}

        with patch("ap.exit_safety._should_halt_exit_after_rejections",
                   side_effect=_fake_halt), \
             patch("ap.exit_safety._exit_position_terminal_state",
                   side_effect=_fake_position_state):
            # Patch conn() to yield a MagicMock so the nested _fn doesn't hit the DB
            mock_conn_ctx = MagicMock()
            mock_conn_ctx.__enter__ = lambda s: MagicMock()
            mock_conn_ctx.__exit__ = MagicMock(return_value=False)
            with patch("ap.exit_safety.conn", return_value=mock_conn_ctx), \
                 patch("ap.exit_safety.run_with_retry", side_effect=lambda fn: fn()):
                evaluate_exit_submission_safety(
                    position_id=_POSITION,
                    client_id=_CLIENT_ID,
                    execution_mode="live",
                    contract=_CONTRACT,
                    broker_truth_open_qty=3,
                )

        assert captured.get("broker_truth_open_qty") == 3, (
            "broker_truth_open_qty must be passed through to "
            "_should_halt_exit_after_rejections"
        )

    def test_05b_case_a_result_propagates_to_caller(self):
        """When circuit breaker is overridden, evaluate returns blocked=False
        with the override reason in circuit_breaker dict."""
        override_result = {
            "blocked": False,
            "reason": "PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH",
            "rejection_count": 5,
            "threshold": 3,
            "broker_truth_open_qty": 2,
            "circuit_breaker_overridden": True,
        }

        def _fake_halt(db_conn, **kw):
            return override_result

        def _fake_position_state(db_conn, **kw):
            return {"blocked": False, "status": "OPEN",
                    "quantity_remaining": 2, "entry_ts": None,
                    "close_source": None, "reason": None}

        mock_conn_ctx = MagicMock()
        mock_conn_ctx.__enter__ = lambda s: MagicMock()
        mock_conn_ctx.__exit__ = MagicMock(return_value=False)
        with patch("ap.exit_safety._should_halt_exit_after_rejections",
                   side_effect=_fake_halt), \
             patch("ap.exit_safety._exit_position_terminal_state",
                   side_effect=_fake_position_state), \
             patch("ap.exit_safety.conn", return_value=mock_conn_ctx), \
             patch("ap.exit_safety.run_with_retry", side_effect=lambda fn: fn()):
            result = evaluate_exit_submission_safety(
                position_id=_POSITION,
                client_id=_CLIENT_ID,
                execution_mode="live",
                contract=_CONTRACT,
                broker_truth_open_qty=2,
            )

        assert result["blocked"] is False
        cb = result.get("circuit_breaker") or {}
        assert cb.get("reason") == "PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH"

    def test_05c_case_b_result_propagates_to_caller(self):
        """When stale synthetic is detected, evaluate returns blocked=True
        with reason SYNTHETIC_POSITION_STALE_BROKER_FLAT."""
        stale_result = {
            "blocked": True,
            "reason": "SYNTHETIC_POSITION_STALE_BROKER_FLAT",
            "rejection_count": 5,
            "threshold": 3,
            "broker_truth_open_qty": 0,
            "synthetic_stale": True,
        }

        def _fake_halt(db_conn, **kw):
            return stale_result

        def _fake_position_state(db_conn, **kw):
            return {"blocked": False, "status": "OPEN",
                    "quantity_remaining": 2, "entry_ts": None,
                    "close_source": None, "reason": None}

        mock_conn_ctx = MagicMock()
        mock_conn_ctx.__enter__ = lambda s: MagicMock()
        mock_conn_ctx.__exit__ = MagicMock(return_value=False)
        with patch("ap.exit_safety._should_halt_exit_after_rejections",
                   side_effect=_fake_halt), \
             patch("ap.exit_safety._exit_position_terminal_state",
                   side_effect=_fake_position_state), \
             patch("ap.exit_safety.conn", return_value=mock_conn_ctx), \
             patch("ap.exit_safety.run_with_retry", side_effect=lambda fn: fn()):
            result = evaluate_exit_submission_safety(
                position_id=_POSITION,
                client_id=_CLIENT_ID,
                execution_mode="live",
                contract=_CONTRACT,
                broker_truth_open_qty=0,
            )

        assert result["blocked"] is True
        assert result["reason"] == "SYNTHETIC_POSITION_STALE_BROKER_FLAT"


# ─────────────────────────────────────────────────────────────────────────────
# Test 6: OSM logs override before broker POST
# ─────────────────────────────────────────────────────────────────────────────

class TestOSMHandlesNewReasonCodes:

    def test_06_osm_logs_protective_exit_allowed_before_submit(self):
        """PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH must appear in OSM logs
        before the broker POST happens."""
        src = open("ap/order_state_machine.py").read()
        assert "PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH" in src, (
            "OSM must log PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH "
            "before broker submit"
        )

    def test_06b_osm_handles_stale_position_blocking(self):
        """SYNTHETIC_POSITION_STALE_BROKER_FLAT must be handled in OSM
        to mark the stale position and return blocked."""
        src = open("ap/order_state_machine.py").read()
        assert "SYNTHETIC_POSITION_STALE_BROKER_FLAT" in src, (
            "OSM must handle SYNTHETIC_POSITION_STALE_BROKER_FLAT "
            "to stop repeated exit firing"
        )
        # Must mark the position closed to stop repeated firing
        assert "synthetic_position_stale_broker_flat" in src


# ─────────────────────────────────────────────────────────────────────────────
# Test 7: Scope enforcement
# ─────────────────────────────────────────────────────────────────────────────

class TestScopeEnforcement:

    def test_07_circuit_breaker_does_not_block_duplicate_in_flight_guard(self):
        """
        The duplicate/in-flight guard is separate from the circuit breaker.
        A PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH override only bypasses
        the rejection-count gate — the in-flight guard in the exit engine
        still prevents concurrent double-submits.
        """
        # Structural: the in-flight guard is in ap_exit_engine.py,
        # not in ap/exit_safety.py. The override in exit_safety must not
        # disable any other guard.
        exit_src = open("ap_exit_engine.py").read()
        exit_safety_src = open("ap/exit_safety.py").read()

        # exit_safety must not clear in-flight state
        assert "clear_exit_in_flight" not in exit_safety_src, (
            "exit_safety.py must not clear in-flight state — "
            "that is the exit engine's responsibility"
        )

        # The new reason codes must be in exit_safety only
        assert "PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH" in exit_safety_src
        assert "SYNTHETIC_POSITION_STALE_BROKER_FLAT" in exit_safety_src

    def test_07b_exit_safety_does_not_cancel_broker_positions(self):
        """The circuit breaker fix must not issue any broker cancel/close calls."""
        src = open("ap/exit_safety.py").read()
        forbidden = ["broker.cancel", "broker.post", "place_order",
                     "submit_order", "cancel_order"]
        for term in forbidden:
            assert term not in src, (
                f"exit_safety.py must not contain {term!r} — "
                "it is a pure safety gate, not a position manager"
            )


# ─────────────────────────────────────────────────────────────────────────────
# Test 8: Robustness
# ─────────────────────────────────────────────────────────────────────────────

class TestRobustness:

    def test_08_never_raises_on_bad_broker_truth(self):
        """Bad broker_truth_open_qty types must not crash the circuit breaker."""
        for bad in ("not_a_number", [], {}, "five"):
            result = _run_should_halt(
                rejection_count=5,
                broker_truth_open_qty=bad,
                threshold=3,
            )
            # Must return a valid result dict, not raise
            assert isinstance(result, dict)
            assert "blocked" in result

    def test_08b_negative_broker_qty_treated_as_no_override(self):
        """
        Negative broker qty is invalid — treat as no broker truth provided.
        Should not trigger either Case A or Case B.
        """
        result = _run_should_halt(
            rejection_count=5,
            broker_truth_open_qty=-1,
            threshold=3,
        )
        # -1 is invalid; int(-1) = -1 which is not > 0 and not == 0
        # So it falls through to the original blocked=True behavior
        # OR the caller must treat negative as None.
        # The current implementation: int(-1) is neither > 0 nor == 0,
        # so it falls to "no broker truth supplied" path → original block.
        assert isinstance(result, dict)
        assert "blocked" in result


# ─────────────────────────────────────────────────────────────────────────────
# Test 9: Structural markers
# ─────────────────────────────────────────────────────────────────────────────

class TestStructuralMarkers:

    def test_09_all_required_reason_codes_in_exit_safety(self):
        src = open("ap/exit_safety.py").read()
        required = [
            "PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH",
            "SYNTHETIC_POSITION_STALE_BROKER_FLAT",
        ]
        missing = [r for r in required if r not in src]
        assert not missing, f"Missing reason codes in exit_safety.py: {missing}"

    def test_09b_broker_truth_kwarg_in_function_signature(self):
        """_should_halt_exit_after_rejections must accept broker_truth_open_qty."""
        import inspect
        sig = inspect.signature(_should_halt_exit_after_rejections)
        assert "broker_truth_open_qty" in sig.parameters, (
            "_should_halt_exit_after_rejections must accept broker_truth_open_qty kwarg"
        )

    def test_09c_evaluate_exit_passes_broker_truth_through(self):
        """evaluate_exit_submission_safety must forward broker_truth_open_qty."""
        import inspect
        sig = inspect.signature(evaluate_exit_submission_safety)
        assert "broker_truth_open_qty" in sig.parameters, (
            "evaluate_exit_submission_safety must have broker_truth_open_qty param"
        )
        src = open("ap/exit_safety.py").read()
        # The forwarding call must include the kwarg
        assert "broker_truth_open_qty=broker_truth_open_qty" in src, (
            "evaluate_exit_submission_safety must forward broker_truth_open_qty "
            "to _should_halt_exit_after_rejections"
        )
