"""P0 — partial EXIT ownership verification guard.

Tests for ap.partial_exit_ownership_guard.

The guard wraps ``_run_reconciliation_attempt``, the shared leaf called by:
    _reconcile_exit_fill(order, result)
    retry_exit_fill_reconciliation(*, client_id, local_order_id)

Both paths pass ``order`` and ``result`` as positional args, so verification
always has the real fields — no reconstruction from the return value.
"""
from __future__ import annotations

import os
os.environ.setdefault("DATABASE_URL", "postgresql://u:p@localhost/db")

from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest

from ap.partial_exit_ownership_guard import (
    _PATCHED_ATTR,
    _ORIGINAL_ATTR,
    is_partial_exit_result,
    partial_exit_ownership_fields,
    verify_partial_exit_ownership,
    wrap_run_reconciliation_attempt,
    install_partial_exit_ownership_guard,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

def _order(
    *,
    qty: int = 5,
    filled_qty: int = 0,
    local_order_id: str = "exit-001",
    broker_order_id: str = "BR-001",
    client_id: str = "test@client.com",
    status: str | None = None,
    state:  str | None = None,
) -> dict[str, Any]:
    d: dict[str, Any] = {
        "qty": qty,
        "filled_qty": filled_qty,
        "local_order_id": local_order_id,
        "broker_order_id": broker_order_id,
        "client_id": client_id,
    }
    if status is not None:
        d["status"] = status
    if state is not None:
        d["state"] = state
    return d


def _result(
    *,
    status: str | None = "PARTIAL_FILL",
    state: str | None = None,
    broker_order_id: str = "BR-001",
    filled_qty: int = 2,
) -> dict[str, Any]:
    d: dict[str, Any] = {"broker_order_id": broker_order_id, "filled_qty": filled_qty}
    if status is not None:
        d["status"] = status
    if state is not None:
        d["state"] = state
    return d


def _reconciled(
    *,
    position_id: str = "pos-001",
    closed: bool = False,
    ordered_qty: int = 5,
    filled_qty: int = 2,
    local_order_id: str = "exit-001",
    broker_order_id: str = "BR-001",
) -> dict[str, Any]:
    remaining = max(0, ordered_qty - filled_qty)
    ownership: dict | None = {
        "exit_in_flight": True,
        "pending_exit_local_order_id":  local_order_id,
        "pending_exit_broker_order_id": broker_order_id,
        "pending_exit_qty": remaining or None,
    } if not closed else None
    return {
        "position_id": position_id,
        "projection": SimpleNamespace(closed=closed),
        "exit_ownership": ownership,
    }


# ─────────────────────────────────────────────────────────────────────────────
# is_partial_exit_result
# ─────────────────────────────────────────────────────────────────────────────

class TestIsPartialExitResult:
    """Status resolution: result.status → result.state → order.status."""

    def test_result_status_triggers(self) -> None:
        assert is_partial_exit_result(_order(), _result(status="PARTIAL_FILL")) is True

    def test_result_state_triggers(self) -> None:
        """result.state used when result.status absent."""
        result = {"state": "PARTIALLY_FILLED", "broker_order_id": "BR-001", "filled_qty": 2}
        assert is_partial_exit_result(_order(status=None), result) is True

    def test_order_status_triggers(self) -> None:
        """order.status used when result.status and result.state both absent."""
        result = {"broker_order_id": "BR-001", "filled_qty": 2}
        assert is_partial_exit_result(_order(status="PARTIAL_FILL"), result) is True

    def test_order_state_alone_does_not_trigger(self) -> None:
        """order.state is not in the canonical resolution chain — must not trigger."""
        result = {"broker_order_id": "BR-001", "filled_qty": 2}
        assert is_partial_exit_result(_order(state="PARTIAL_FILL", status=None), result) is False

    @pytest.mark.parametrize("status", [
        "PARTIAL_FILL", "PARTIALLY_FILLED", "PARTIAL", "EXIT_PARTIAL_FILL",
    ])
    def test_all_partial_statuses_recognized(self, status: str) -> None:
        assert is_partial_exit_result(_order(), _result(status=status)) is True

    @pytest.mark.parametrize("status", [
        "FILLED", "EXIT_FILLED", "CANCELED", "CANCELLED",
        "REJECTED", "EXPIRED", "ERROR", "",
    ])
    def test_terminal_statuses_excluded(self, status: str) -> None:
        assert is_partial_exit_result(_order(), _result(status=status)) is False

    # ── Regressions ───────────────────────────────────────────────────────────

    def test_result_state_partially_filled_verifies_ownership(self) -> None:
        """Regression: result.state='PARTIALLY_FILLED', no status key → verified."""
        order = _order(qty=5, filled_qty=0, local_order_id="exit-r1", broker_order_id="BR-R1")
        result = {"state": "PARTIALLY_FILLED", "broker_order_id": "BR-R1", "filled_qty": 2}
        reconciled = {
            "position_id": "pos-r1",
            "projection":  SimpleNamespace(closed=False),
            "exit_ownership": {
                "exit_in_flight": True,
                "pending_exit_local_order_id":  "exit-r1",
                "pending_exit_broker_order_id": "BR-R1",
                "pending_exit_qty": 3,
            },
        }
        out = verify_partial_exit_ownership(order, result, reconciled)
        assert out["partial_exit_ownership_verified"] is True

    def test_no_partial_status_or_state_skips_verifier(self) -> None:
        """Regression: neither partial status nor partial state → verifier skipped."""
        order      = _order()
        result     = {"status": "FILLED", "broker_order_id": "BR-001", "filled_qty": 5}
        reconciled = {
            "position_id": "pos-002",
            "projection":  SimpleNamespace(closed=False),
            "exit_ownership": None,
        }
        out = verify_partial_exit_ownership(order, result, reconciled)
        assert "partial_exit_ownership_verified" not in out


# ─────────────────────────────────────────────────────────────────────────────
# verify_partial_exit_ownership
# ─────────────────────────────────────────────────────────────────────────────

class TestVerifyPartialExitOwnership:
    def test_verified_true_when_ownership_matches(self) -> None:
        order = _order(qty=5, filled_qty=0)
        result = _result(filled_qty=2)
        out = verify_partial_exit_ownership(order, result, _reconciled(ordered_qty=5, filled_qty=2))
        assert out["partial_exit_ownership_verified"] is True

    def test_verified_false_when_ownership_missing(self) -> None:
        order = _order()
        result = _result(filled_qty=2)
        reconciled = _reconciled(ordered_qty=5, filled_qty=2)
        reconciled["exit_ownership"] = None
        out = verify_partial_exit_ownership(order, result, reconciled)
        assert out["partial_exit_ownership_verified"] is False

    def test_closed_projection_skipped(self) -> None:
        out = verify_partial_exit_ownership(
            _order(), _result(filled_qty=2), _reconciled(closed=True)
        )
        assert "partial_exit_ownership_verified" not in out

    def test_remaining_qty_same_order_only(self) -> None:
        assert partial_exit_ownership_fields(_order(qty=5), _result(filled_qty=2))["pending_exit_qty"] == 3

    def test_later_cumulative_fill(self) -> None:
        assert partial_exit_ownership_fields(_order(qty=5), _result(filled_qty=4))["pending_exit_qty"] == 1

    def test_idempotent_same_fill(self) -> None:
        f1 = partial_exit_ownership_fields(_order(qty=5), _result(filled_qty=2))
        f2 = partial_exit_ownership_fields(_order(qty=5), _result(filled_qty=2))
        assert f1["pending_exit_qty"] == f2["pending_exit_qty"] == 3

    def test_no_db_in_guard_module(self) -> None:
        import ap.partial_exit_ownership_guard as g
        src = open(g.__file__).read()
        for forbidden in ("from ap.db import", "import ap.db", "conn()", "run_with_retry"):
            assert forbidden not in src

    def test_no_broker_call_in_guard_module(self) -> None:
        import ap.partial_exit_ownership_guard as g
        src = open(g.__file__).read()
        for forbidden in ("broker.submit", "broker.cancel", "submit_order", "cancel_order"):
            assert forbidden not in src


# ─────────────────────────────────────────────────────────────────────────────
# wrap_run_reconciliation_attempt — direct path
# ─────────────────────────────────────────────────────────────────────────────

class TestDirectPath:
    """_reconcile_exit_fill → _run_reconciliation_attempt → guarded."""

    def test_verified_on_partial_direct(self) -> None:
        order      = _order(qty=5)
        result     = _result(filled_qty=2)
        reconciled = _reconciled(ordered_qty=5, filled_qty=2)
        calls = []

        def mock_leaf(o, r, *, attempt_count):
            calls.append(1)
            return reconciled

        out = wrap_run_reconciliation_attempt(mock_leaf)(order, result, attempt_count=1)
        assert len(calls) == 1
        assert out["partial_exit_ownership_verified"] is True

    def test_canonical_runs_exactly_once_direct(self) -> None:
        calls = []
        def mock_leaf(o, r, *, attempt_count):
            calls.append(1)
            return _reconciled()
        wrap_run_reconciliation_attempt(mock_leaf)(_order(), _result(filled_qty=2), attempt_count=1)
        assert len(calls) == 1

    def test_filled_not_verified_direct(self) -> None:
        out = wrap_run_reconciliation_attempt(
            lambda o, r, *, attempt_count: {"position_id": "p", "projection": SimpleNamespace(closed=False)}
        )(_order(), _result(status="FILLED", filled_qty=5), attempt_count=1)
        assert "partial_exit_ownership_verified" not in out

    def test_closed_projection_not_reopened_direct(self) -> None:
        rec = _reconciled(closed=True)
        rec["exit_ownership"] = None
        out = wrap_run_reconciliation_attempt(
            lambda o, r, *, attempt_count: rec
        )(_order(), _result(filled_qty=2), attempt_count=1)
        assert "partial_exit_ownership_verified" not in out
        assert out["projection"].closed is True


# ─────────────────────────────────────────────────────────────────────────────
# wrap_run_reconciliation_attempt — startup retry path
# ─────────────────────────────────────────────────────────────────────────────

class TestRetryPath:
    """retry_exit_fill_reconciliation → _run_reconciliation_attempt → guarded.

    Production-shaped: the retry function loads the order from DB, constructs
    a result dict from order fields, then calls _run_reconciliation_attempt with
    both.  The guard wraps the leaf and receives the REAL order and result —
    not a reconstruction from the return value (which omits status/qty/filled_qty).
    """

    def _production_shaped_retry_call(
        self,
        *,
        order_status: str = "EXIT_PARTIAL_FILL",
        filled_qty: int = 2,
        ordered_qty: int = 5,
        local_order_id: str = "exit-retry-001",
        broker_order_id: str = "BR-RETRY-001",
        client_id: str = "test@client.com",
        reconciled_exit_ownership: dict | None = None,
    ):
        """Simulate the exact sequence in retry_exit_fill_reconciliation.

        retry_exit_fill_reconciliation:
          1. order = run_with_retry(_load)           ← order row from DB
          2. result = {"status": order.get("status"),
                       "broker_order_id": ...,
                       "filled_qty": ..., ...}       ← built from order fields
          3. return _run_reconciliation_attempt(order, result, attempt_count=...)
                                                     ← _run_reconciliation_attempt
                                                        is what the guard wraps

        The guard must see the real order.status and result.status here,
        not fields reconstructed from the return dict (which lacks them).
        """
        # Simulate the order row as loaded from the DB by the retry path
        order = {
            "client_id":      client_id,
            "local_order_id": local_order_id,
            "broker_order_id": broker_order_id,
            "status":         order_status,   # EXIT_PARTIAL_FILL as stored
            "filled_qty":     filled_qty,
            "qty":            ordered_qty,
            "fill_price":     1.50,
            "filled_ts":      "2026-07-18T10:00:00Z",
        }
        # Simulate the result dict constructed by retry_exit_fill_reconciliation
        result = {
            "status":         order.get("status"),      # EXIT_PARTIAL_FILL
            "broker_order_id": order.get("broker_order_id"),
            "filled_qty":     order.get("filled_qty"),
            "fill_price":     order.get("fill_price"),
            "filled_ts":      order.get("filled_ts"),
        }
        # The reconciled return from _run_reconciliation_attempt contains only
        # position/projection/exit_ownership — NOT status/qty/filled_qty
        remaining = max(0, ordered_qty - filled_qty)
        if reconciled_exit_ownership is None:
            reconciled_exit_ownership = {
                "exit_in_flight": True,
                "pending_exit_local_order_id":  local_order_id,
                "pending_exit_broker_order_id": broker_order_id,
                "pending_exit_qty": remaining or None,
            }
        production_return = {
            "position_id":    "pos-retry-001",
            "projection":     SimpleNamespace(closed=False),
            "exit_ownership": reconciled_exit_ownership,
            "execution_mode": "live",
            # NOTE: no "status", no "qty", no "filled_qty" — production reality
        }
        calls = []
        def mock_leaf(o, r, *, attempt_count):
            calls.append((o, r))
            return production_return

        wrapped = wrap_run_reconciliation_attempt(mock_leaf)
        out = wrapped(order, result, attempt_count=1)
        return out, calls

    def test_retry_path_verified_production_shaped(self) -> None:
        """Production-shaped: retry path verified using the real order/result fields."""
        out, calls = self._production_shaped_retry_call()
        assert len(calls) == 1
        assert "partial_exit_ownership_verified" in out, (
            "Verification must run on the retry path — the guard wraps "
            "_run_reconciliation_attempt which receives the real order/result"
        )
        assert out["partial_exit_ownership_verified"] is True

    def test_retry_path_uses_result_status_not_return_status(self) -> None:
        """Guard uses result.status (from order row) — not reconciled['status'] (absent)."""
        out, _ = self._production_shaped_retry_call(order_status="EXIT_PARTIAL_FILL")
        # If guard incorrectly read reconciled['status'] (missing), it would see None
        # and skip verification.  The correct wrap point ensures it sees result.status.
        assert out.get("partial_exit_ownership_verified") is True, (
            "If this fails, the guard is reading status from the return dict "
            "(which has no 'status' key) instead of from the result parameter"
        )

    def test_retry_path_canonical_runs_exactly_once(self) -> None:
        out, calls = self._production_shaped_retry_call()
        assert len(calls) == 1

    def test_retry_path_closed_projection_not_reopened(self) -> None:
        out, _ = self._production_shaped_retry_call(
            reconciled_exit_ownership=None,
        )
        # Set closed after building — simulate full position close on retry
        # (re-run with closed=True projection)
        reconciled_closed = {
            "position_id":    "pos-retry-closed",
            "projection":     SimpleNamespace(closed=True),
            "exit_ownership": None,
            "execution_mode": "live",
        }
        calls = []
        def mock_leaf(o, r, *, attempt_count):
            calls.append(1)
            return reconciled_closed

        order  = {"client_id": "c@c.com", "status": "EXIT_PARTIAL_FILL", "qty": 5, "filled_qty": 5}
        result = {"status": "EXIT_PARTIAL_FILL", "filled_qty": 5}
        out = wrap_run_reconciliation_attempt(mock_leaf)(order, result, attempt_count=1)
        assert "partial_exit_ownership_verified" not in out
        assert out["projection"].closed is True

    def test_retry_path_ownership_mismatch_reported(self) -> None:
        """Missing exit_ownership on retry path → verified=False, no second write."""
        out, _ = self._production_shaped_retry_call(
            reconciled_exit_ownership={}  # empty — canonical didn't write fields
        )
        assert out["partial_exit_ownership_verified"] is False

    def test_retry_path_no_db_write(self) -> None:
        import ap.partial_exit_ownership_guard as g
        src = open(g.__file__).read()
        assert "conn(" not in src
        assert "run_with_retry" not in src

    def test_retry_path_no_broker_call(self) -> None:
        import ap.partial_exit_ownership_guard as g
        src = open(g.__file__).read()
        for forbidden in ("broker.submit", "broker.cancel"):
            assert forbidden not in src


# ─────────────────────────────────────────────────────────────────────────────
# Installation
# ─────────────────────────────────────────────────────────────────────────────

class TestInstallation:
    def _clean(self, eftg) -> tuple:
        for attr in (_PATCHED_ATTR, _ORIGINAL_ATTR):
            if hasattr(eftg, attr):
                delattr(eftg, attr)
        return eftg._run_reconciliation_attempt

    def test_patches_run_reconciliation_attempt(self) -> None:
        from ap import exit_fill_truth_guard as eftg
        orig = self._clean(eftg)
        install_partial_exit_ownership_guard()
        assert eftg._run_reconciliation_attempt is not orig
        eftg._run_reconciliation_attempt = orig
        delattr(eftg, _PATCHED_ATTR)

    def test_idempotent(self) -> None:
        from ap import exit_fill_truth_guard as eftg
        orig = self._clean(eftg)
        install_partial_exit_ownership_guard()
        after_first = eftg._run_reconciliation_attempt
        install_partial_exit_ownership_guard()
        assert eftg._run_reconciliation_attempt is after_first
        eftg._run_reconciliation_attempt = orig
        delattr(eftg, _PATCHED_ATTR)

    def test_does_not_patch_outer_retry_function(self) -> None:
        """Guard now wraps _run_reconciliation_attempt, NOT retry_exit_fill_reconciliation.

        Patching the outer retry function would require reconstructing order/result
        from the return value, which omits status/qty/filled_qty.  The leaf-wrap
        approach is production-correct and this test verifies the design choice.
        """
        from ap import exit_fill_truth_guard as eftg
        orig_retry = eftg.retry_exit_fill_reconciliation
        orig_leaf  = self._clean(eftg)
        install_partial_exit_ownership_guard()
        assert eftg.retry_exit_fill_reconciliation is orig_retry, (
            "Guard must not wrap retry_exit_fill_reconciliation — "
            "it wraps _run_reconciliation_attempt instead"
        )
        eftg._run_reconciliation_attempt = orig_leaf
        delattr(eftg, _PATCHED_ATTR)
