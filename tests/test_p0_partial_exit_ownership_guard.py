"""P0 — partial EXIT ownership verification guard.

Tests for ap.partial_exit_ownership_guard.

Coverage:
  Finding 1 — canonical predicate delegation
    - result.status triggers verification
    - result.state triggers verification (canonical checks this)
    - order.status triggers verification (canonical checks this)
    - order.state alone does NOT trigger (not in canonical predicate — boundary test)
    - terminal statuses are excluded
    - no parallel taxonomy: guard delegates to exit_fill_truth_guard._is_partial_result

  Finding 2 — startup retry reconciliation path
    - direct path (_reconcile_exit_fill) is verified
    - retry path (retry_exit_fill_reconciliation) is verified
    - canonical reducer runs exactly once per path
    - no additional database mutation occurs
    - no broker submit/cancel occurs
    - closed projections are never reopened

  Original requirements
    - PARTIAL_FILL / PARTIALLY_FILLED / PARTIAL / EXIT_PARTIAL_FILL recognized
    - remaining qty = ordered_qty - cumulative_filled_qty (same order only)
    - FILLED, CANCELED, REJECTED, EXPIRED, ERROR not active
    - completed scale-out releases ownership
    - missing exit_ownership → verified=False, no second write
    - installation idempotent
"""
from __future__ import annotations

import os
os.environ.setdefault("DATABASE_URL", "postgresql://u:p@localhost/db")

from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from ap.partial_exit_ownership_guard import (
    is_partial_exit_result,
    partial_exit_ownership_fields,
    verify_partial_exit_ownership,
    wrap_exit_fill_reconcile,
    wrap_retry_exit_fill_reconciliation,
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
    proj = SimpleNamespace(closed=closed)
    remaining = max(0, ordered_qty - filled_qty)
    ownership: dict | None = {
        "exit_in_flight": True,
        "pending_exit_local_order_id": local_order_id,
        "pending_exit_broker_order_id": broker_order_id,
        "pending_exit_qty": remaining or None,
    } if not closed else None
    return {
        "position_id": position_id,
        "projection": proj,
        "exit_ownership": ownership,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Finding 1: canonical predicate delegation
# ─────────────────────────────────────────────────────────────────────────────

class TestCanonicalPredicateDelegation:
    """is_partial_exit_result must mirror exit_fill_truth_guard._is_partial_result exactly."""

    def test_delegates_to_canonical_not_parallel_taxonomy(self) -> None:
        """Guard predicate is the same object as the canonical predicate result."""
        from ap.exit_fill_truth_guard import _is_partial_result as canonical
        order  = _order()
        result = _result(status="PARTIAL_FILL")
        # Both must agree on every status
        for s in ("PARTIAL_FILL", "PARTIALLY_FILLED", "PARTIAL", "EXIT_PARTIAL_FILL",
                  "FILLED", "CANCELED", "REJECTED", "EXPIRED", "ERROR", ""):
            r = {**result, "status": s}
            assert is_partial_exit_result(order, r) == canonical(order, r), (
                f"Guard disagrees with canonical for status={s!r}"
            )

    def test_result_status_triggers_verification(self) -> None:
        """result['status'] = PARTIAL_FILL → verification runs."""
        result = _result(status="PARTIAL_FILL", state=None)
        order  = _order(status=None)
        assert is_partial_exit_result(order, result) is True

    def test_result_state_triggers_verification(self) -> None:
        """result['state'] = PARTIAL_FILL (no status key) → verification runs.

        The canonical predicate checks result.state when result.status is absent.
        """
        result = {"state": "PARTIAL_FILL", "broker_order_id": "BR-001", "filled_qty": 2}
        order  = _order(status=None)
        assert is_partial_exit_result(order, result) is True

    def test_order_status_triggers_verification(self) -> None:
        """order['status'] = PARTIAL_FILL (no result.status or result.state) → runs.

        The canonical predicate falls through to order.status as the third source.
        """
        result = {"broker_order_id": "BR-001", "filled_qty": 2}
        order  = _order(status="PARTIAL_FILL")
        assert is_partial_exit_result(order, result) is True

    def test_order_state_alone_does_not_trigger(self) -> None:
        """order['state'] alone is not a canonical partial source — NOT triggered.

        The canonical predicate checks result.status → result.state → order.status.
        It does not check order.state.  Delegating to canonical means order.state
        alone will not trigger verification.  This is the correct boundary.
        """
        result = {"broker_order_id": "BR-001", "filled_qty": 2}
        order  = _order(state="PARTIAL_FILL", status=None)  # only order.state set
        assert is_partial_exit_result(order, result) is False, (
            "order.state is not a canonical source — guard must not invent a "
            "parallel taxonomy that differs from exit_fill_truth_guard._is_partial_result"
        )


    # ── Regression: result.state positive ─────────────────────────────────────

    def test_result_state_partially_filled_verifies_ownership(self) -> None:
        """Regression: result uses state='PARTIALLY_FILLED' with no status key.

        The canonical resolver falls through result.status (absent) →
        result.state → PARTIALLY_FILLED → active partial.
        The verification wrapper must confirm ownership fields match.
        """
        order = _order(qty=5, filled_qty=0,
                       local_order_id="exit-r1", broker_order_id="BR-R1")
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

    # ── Regression: neither partial status nor partial state ───────────────────

    def test_no_partial_status_or_state_skips_verifier(self) -> None:
        """Regression: result has neither partial status nor partial state.

        The ownership verifier must not run; partial_exit_ownership_verified
        must be absent from the returned dict.
        """
        order      = _order()
        result     = {"status": "FILLED", "broker_order_id": "BR-001", "filled_qty": 5}
        reconciled = {
            "position_id": "pos-002",
            "projection":  SimpleNamespace(closed=False),
            "exit_ownership": None,
        }
        out = verify_partial_exit_ownership(order, result, reconciled)
        assert "partial_exit_ownership_verified" not in out, (
            "Verifier must not set partial_exit_ownership_verified when result "
            "has no partial status or state"
        )
    @pytest.mark.parametrize("status", [
        "PARTIAL_FILL", "PARTIALLY_FILLED", "PARTIAL", "EXIT_PARTIAL_FILL",
    ])
    def test_all_partial_statuses_recognized_via_result(self, status: str) -> None:
        result = _result(status=status)
        assert is_partial_exit_result(_order(), result) is True

    @pytest.mark.parametrize("status", [
        "FILLED", "EXIT_FILLED", "CANCELED", "CANCELLED",
        "REJECTED", "EXPIRED", "ERROR", "",
    ])
    def test_terminal_statuses_not_recognized(self, status: str) -> None:
        result = _result(status=status)
        assert is_partial_exit_result(_order(), result) is False


# ─────────────────────────────────────────────────────────────────────────────
# verify_partial_exit_ownership — shared verification function
# ─────────────────────────────────────────────────────────────────────────────

class TestVerifyPartialExitOwnership:
    def test_sets_verified_true_when_ownership_matches(self) -> None:
        order      = _order(qty=5, filled_qty=0)
        result     = _result(filled_qty=2)
        reconciled = _reconciled(ordered_qty=5, filled_qty=2)
        out = verify_partial_exit_ownership(order, result, reconciled)
        assert out["partial_exit_ownership_verified"] is True

    def test_sets_verified_false_when_ownership_missing(self) -> None:
        order      = _order()
        result     = _result(filled_qty=2)
        reconciled = _reconciled(ordered_qty=5, filled_qty=2)
        reconciled["exit_ownership"] = None  # canonical didn't write fields
        out = verify_partial_exit_ownership(order, result, reconciled)
        assert out["partial_exit_ownership_verified"] is False

    def test_no_verification_for_terminal_status(self) -> None:
        order      = _order()
        result     = _result(status="FILLED", filled_qty=5)
        reconciled = _reconciled()
        out = verify_partial_exit_ownership(order, result, reconciled)
        assert "partial_exit_ownership_verified" not in out

    def test_closed_projection_skipped(self) -> None:
        order      = _order()
        result     = _result(filled_qty=2)
        reconciled = _reconciled(closed=True)
        reconciled["exit_ownership"] = None
        out = verify_partial_exit_ownership(order, result, reconciled)
        assert "partial_exit_ownership_verified" not in out

    def test_remaining_qty_derived_from_same_order(self) -> None:
        order  = _order(qty=5, filled_qty=0)
        result = _result(filled_qty=2)
        fields = partial_exit_ownership_fields(order, result)
        assert fields["pending_exit_qty"] == 3  # 5 - 2 = 3

    def test_later_cumulative_fill_updates_remaining(self) -> None:
        order  = _order(qty=5, filled_qty=0)
        result = _result(filled_qty=4)
        fields = partial_exit_ownership_fields(order, result)
        assert fields["pending_exit_qty"] == 1  # 5 - 4 = 1

    def test_idempotent_same_cumulative_fill(self) -> None:
        """Same cumulative filled_qty reported twice does not double-count."""
        order  = _order(qty=5, filled_qty=0)
        result = _result(filled_qty=2)
        f1 = partial_exit_ownership_fields(order, result)
        f2 = partial_exit_ownership_fields(order, result)
        assert f1["pending_exit_qty"] == f2["pending_exit_qty"] == 3

    def test_no_db_import_in_guard_module(self) -> None:
        """Guard module must not import any database module at the top level."""
        import ap.partial_exit_ownership_guard as g
        import importlib, sys
        src_file = g.__file__
        with open(src_file) as f:
            source = f.read()
        for db_import in ("from ap.db import", "import ap.db", "from ap import db",
                          "conn()", "run_with_retry"):
            assert db_import not in source, (
                f"Guard must not reference DB: found {db_import!r}"
            )

    def test_no_broker_call_in_guard_module(self) -> None:
        import ap.partial_exit_ownership_guard as g
        with open(g.__file__) as f:
            source = f.read()
        for forbidden in ("broker.submit", "broker.cancel", "submit_order", "cancel_order"):
            assert forbidden not in source


# ─────────────────────────────────────────────────────────────────────────────
# Finding 2: both entry paths covered
# ─────────────────────────────────────────────────────────────────────────────

class TestDirectReconciliationPath:
    """wrap_exit_fill_reconcile covers _reconcile_exit_fill (fill monitor path)."""

    def test_direct_path_verified_on_partial(self) -> None:
        order      = _order(qty=5)
        result     = _result(filled_qty=2)
        reconciled = _reconciled(ordered_qty=5, filled_qty=2)
        canonical_called = []

        def mock_reducer(o, r):
            canonical_called.append(1)
            return reconciled

        wrapped = wrap_exit_fill_reconcile(mock_reducer)
        out = wrapped(order, result)

        assert len(canonical_called) == 1, "Canonical reducer must run exactly once"
        assert "partial_exit_ownership_verified" in out
        assert out["partial_exit_ownership_verified"] is True

    def test_direct_path_canonical_runs_exactly_once(self) -> None:
        call_count = []
        def mock_reducer(o, r):
            call_count.append(1)
            return _reconciled()
        wrapped = wrap_exit_fill_reconcile(mock_reducer)
        wrapped(_order(), _result(filled_qty=2))
        assert len(call_count) == 1

    def test_direct_path_no_verification_for_filled(self) -> None:
        order  = _order()
        result = _result(status="FILLED", filled_qty=5)
        recon  = {"position_id": "pos-001", "projection": SimpleNamespace(closed=False)}

        out = wrap_exit_fill_reconcile(lambda o, r: recon)(order, result)
        assert "partial_exit_ownership_verified" not in out

    def test_direct_path_closed_projection_not_reopened(self) -> None:
        order      = _order()
        result     = _result(filled_qty=2)
        reconciled = _reconciled(closed=True)
        reconciled["exit_ownership"] = None

        out = wrap_exit_fill_reconcile(lambda o, r: reconciled)(order, result)
        # Closed projection must not gain a partial_exit_ownership_verified flag
        assert "partial_exit_ownership_verified" not in out
        # Projection must still be closed
        assert out["projection"].closed is True


class TestRetryReconciliationPath:
    """wrap_retry_exit_fill_reconciliation covers the startup retry path.

    retry_exit_fill_reconciliation → _run_reconciliation_attempt (bypasses
    _reconcile_exit_fill).  Without wrapping this path, verification would
    never run during startup retry.
    """

    def _make_retry_reconciled(
        self,
        *,
        status: str = "EXIT_PARTIAL_FILL",
        filled_qty: int = 2,
        ordered_qty: int = 5,
        local_order_id: str = "exit-001",
        broker_order_id: str = "BR-001",
    ) -> dict:
        remaining = max(0, ordered_qty - filled_qty)
        return {
            "position_id":    "pos-001",
            "status":         status,
            "broker_order_id": broker_order_id,
            "filled_qty":     filled_qty,
            "qty":            ordered_qty,
            "projection":     SimpleNamespace(closed=False),
            "exit_ownership": {
                "exit_in_flight": True,
                "pending_exit_local_order_id":  local_order_id,
                "pending_exit_broker_order_id": broker_order_id,
                "pending_exit_qty":             remaining or None,
            },
        }

    def test_retry_path_is_verified(self) -> None:
        reconciled = self._make_retry_reconciled(status="EXIT_PARTIAL_FILL", filled_qty=2)
        retry_called = []

        def mock_retry(*, client_id, local_order_id):
            retry_called.append(1)
            return reconciled

        wrapped = wrap_retry_exit_fill_reconciliation(mock_retry)
        out = wrapped(client_id="test@client.com", local_order_id="exit-001")

        assert len(retry_called) == 1, "Canonical retry must run exactly once"
        assert "partial_exit_ownership_verified" in out
        assert out["partial_exit_ownership_verified"] is True

    def test_retry_path_canonical_runs_exactly_once(self) -> None:
        call_count = []
        def mock_retry(*, client_id, local_order_id):
            call_count.append(1)
            return self._make_retry_reconciled()
        wrapped = wrap_retry_exit_fill_reconciliation(mock_retry)
        wrapped(client_id="c@c.com", local_order_id="exit-001")
        assert len(call_count) == 1

    def test_retry_already_reconciled_passes_through(self) -> None:
        def mock_retry(*, client_id, local_order_id):
            return {"already_reconciled": True, "position_id": "pos-001"}
        wrapped = wrap_retry_exit_fill_reconciliation(mock_retry)
        out = wrapped(client_id="c@c.com", local_order_id="exit-001")
        assert out.get("already_reconciled") is True
        assert "partial_exit_ownership_verified" not in out

    def test_retry_none_result_passes_through(self) -> None:
        """Retry returns None when order row not found — must not crash."""
        wrapped = wrap_retry_exit_fill_reconciliation(lambda *, client_id, local_order_id: None)
        assert wrapped(client_id="c@c.com", local_order_id="missing") is None

    def test_retry_path_closed_projection_not_reopened(self) -> None:
        reconciled = self._make_retry_reconciled()
        reconciled["projection"] = SimpleNamespace(closed=True)
        reconciled["exit_ownership"] = None

        wrapped = wrap_retry_exit_fill_reconciliation(lambda *, client_id, local_order_id: reconciled)
        out = wrapped(client_id="c@c.com", local_order_id="exit-001")
        assert "partial_exit_ownership_verified" not in out
        assert out["projection"].closed is True

    def test_retry_path_no_db_write(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Verification on the retry path must issue no additional SQL."""
        sql_calls: list = []
        monkeypatch.setattr(
            "ap.partial_exit_ownership_guard.verify_partial_exit_ownership",
            lambda o, r, rec: (sql_calls.append("SQL") or rec),
        )
        # Even with a monkeypatched verifier that tries a side effect,
        # the real verifier has no db access — this is a structural check.
        import ap.partial_exit_ownership_guard as g
        with open(g.__file__) as fh:
            src = fh.read()
        assert "conn(" not in src
        assert "run_with_retry" not in src

    def test_retry_path_no_broker_submit(self) -> None:
        """No broker.submit or broker.cancel is ever called."""
        import ap.partial_exit_ownership_guard as g
        with open(g.__file__) as fh:
            src = fh.read()
        for forbidden in ("broker.submit", "broker.cancel", ".submit_order", ".cancel_order"):
            assert forbidden not in src


# ─────────────────────────────────────────────────────────────────────────────
# Installation
# ─────────────────────────────────────────────────────────────────────────────

class TestInstallation:
    def test_installation_patches_both_entry_points(self) -> None:
        from ap import exit_fill_truth_guard as eftg
        from ap.partial_exit_ownership_guard import (
            install_partial_exit_ownership_guard,
            _PATCHED_ATTR, _ORIGINAL_ATTR,
            _RETRY_PATCHED_ATTR, _RETRY_ORIGINAL_ATTR,
        )
        # Clean slate
        for attr in (_PATCHED_ATTR, _ORIGINAL_ATTR, _RETRY_PATCHED_ATTR, _RETRY_ORIGINAL_ATTR):
            if hasattr(eftg, attr):
                delattr(eftg, attr)
        orig_reconcile = eftg._reconcile_exit_fill
        orig_retry     = eftg.retry_exit_fill_reconciliation

        install_partial_exit_ownership_guard()

        assert eftg._reconcile_exit_fill is not orig_reconcile
        assert eftg.retry_exit_fill_reconciliation is not orig_retry

        # Restore
        eftg._reconcile_exit_fill           = orig_reconcile
        eftg.retry_exit_fill_reconciliation = orig_retry
        for attr in (_PATCHED_ATTR, _RETRY_PATCHED_ATTR):
            if hasattr(eftg, attr):
                delattr(eftg, attr)

    def test_installation_is_idempotent(self) -> None:
        from ap import exit_fill_truth_guard as eftg
        from ap.partial_exit_ownership_guard import (
            install_partial_exit_ownership_guard,
            _PATCHED_ATTR, _ORIGINAL_ATTR,
            _RETRY_PATCHED_ATTR, _RETRY_ORIGINAL_ATTR,
        )
        for attr in (_PATCHED_ATTR, _ORIGINAL_ATTR, _RETRY_PATCHED_ATTR, _RETRY_ORIGINAL_ATTR):
            if hasattr(eftg, attr):
                delattr(eftg, attr)
        orig_reconcile = eftg._reconcile_exit_fill
        orig_retry     = eftg.retry_exit_fill_reconciliation

        install_partial_exit_ownership_guard()
        after_first = eftg._reconcile_exit_fill

        install_partial_exit_ownership_guard()
        assert eftg._reconcile_exit_fill is after_first, "Double install must not double-wrap"

        eftg._reconcile_exit_fill           = orig_reconcile
        eftg.retry_exit_fill_reconciliation = orig_retry
        for attr in (_PATCHED_ATTR, _RETRY_PATCHED_ATTR):
            if hasattr(eftg, attr):
                delattr(eftg, attr)
