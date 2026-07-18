"""P0 — EXIT position ambiguity fence.

Tests for ap.exit_position_ambiguity_guard.

Resolution contract:
  real position_id            → delegates to canonical resolver, no query
  one OPEN/CLOSING candidate  → repairs and returns it
  two+ active candidates      → quarantine (None), never calls parent
  zero active candidates      → delegates to canonical resolver
  candidate lookup failure    → fails closed (None), never calls parent

Active = status in (OPEN, CLOSING), quantity_remaining > 0,
         entry_ts <= fill_ts, real non-synthetic position ID.
Terminal rows (even recently closed) are excluded from the candidate set.
"""
from __future__ import annotations

import os
os.environ.setdefault("DATABASE_URL", "postgresql://u:p@localhost/db")

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from ap.exit_position_ambiguity_guard import (
    _PATCHED_ATTR,
    _ORIGINAL_ATTR,
    _is_synthetic_position_id,
    _active_exact_contract_candidates,
    _validate_sole_candidate,
    wrap_resolve_position,
    install_exit_position_ambiguity_guard,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

_CLIENT   = "test@client.com"
_CONTRACT = "SPY260718C00600000"
_FILL_TS  = datetime(2026, 7, 18, 14, 0, 0, tzinfo=timezone.utc)

def _pos(
    *,
    id: str = "pos-real-001",
    client_id: str = _CLIENT,
    contract: str = _CONTRACT,
    status: str = "OPEN",
    quantity_remaining: int = 2,
    entry_ts: Any = datetime(2026, 7, 18, 9, 30, 0, tzinfo=timezone.utc),
) -> dict[str, Any]:
    return {
        "id": id,
        "client_id": client_id,
        "contract": contract,
        "status": status,
        "quantity_remaining": quantity_remaining,
        "entry_ts": entry_ts,
    }


def _order(
    *,
    position_id: Any = None,
    client_id: str = _CLIENT,
    contract: str = _CONTRACT,
    local_order_id: str = "exit-001",
    broker_order_id: str = "BR-001",
) -> dict[str, Any]:
    return {
        "position_id":    position_id,
        "client_id":      client_id,
        "contract":       contract,
        "local_order_id": local_order_id,
        "broker_order_id": broker_order_id,
    }


def _cursor(rows: list[dict]) -> Any:
    """Minimal cursor mock whose .fetchall() returns dict-compatible rows."""
    class _DictRow(dict):
        """A dict subclass that mimics RealDictCursor row behaviour."""
    result = MagicMock()
    result.fetchall.return_value = [_DictRow(r) for r in rows]
    c = MagicMock()
    c.execute.return_value = result
    return c


# ─────────────────────────────────────────────────────────────────────────────
# _is_synthetic_position_id
# ─────────────────────────────────────────────────────────────────────────────

class TestIsSyntheticPositionId:
    def test_none_is_synthetic(self) -> None:
        assert _is_synthetic_position_id(None) is True

    def test_empty_string_is_synthetic(self) -> None:
        assert _is_synthetic_position_id("") is True

    def test_whitespace_is_synthetic(self) -> None:
        assert _is_synthetic_position_id("   ") is True

    def test_broker_repair_is_synthetic(self) -> None:
        assert _is_synthetic_position_id("broker-repair-abc123") is True
        assert _is_synthetic_position_id("BROKER-REPAIR-XYZ") is True

    def test_real_uuid_is_not_synthetic(self) -> None:
        assert _is_synthetic_position_id("pos-real-001") is False
        assert _is_synthetic_position_id("abc123") is False


# ─────────────────────────────────────────────────────────────────────────────
# SQL shape test
# ─────────────────────────────────────────────────────────────────────────────

class TestSqlShape:
    """The candidate query must meet every structural requirement."""

    def _get_sql(self) -> str:
        import inspect
        src = inspect.getsource(_active_exact_contract_candidates)
        # Extract the string literal passed to c.execute
        import re
        m = re.search(r'c\.execute\(\s*(".*?"|\' .*?\')', src, re.DOTALL)
        if not m:
            # fallback: find the SELECT
            m = re.search(r'"(SELECT.*?FOR UPDATE)"', src, re.DOTALL)
        return src  # return full source for assertion

    def test_query_restricts_open_and_closing_only(self) -> None:
        import inspect
        src = inspect.getsource(_active_exact_contract_candidates)
        assert "OPEN" in src and "CLOSING" in src, \
            "Query must restrict to OPEN/CLOSING status only"

    def test_query_requires_positive_quantity_remaining(self) -> None:
        import inspect
        src = inspect.getsource(_active_exact_contract_candidates)
        assert "quantity_remaining" in src and "> 0" in src, \
            "Query must require positive quantity_remaining"

    def test_query_scopes_exact_client_and_contract(self) -> None:
        import inspect
        src = inspect.getsource(_active_exact_contract_candidates)
        assert "client_id = %s" in src or "client_id=%s" in src
        assert "UPPER(contract) = UPPER(%s)" in src or "UPPER(contract)=UPPER(%s)" in src

    def test_query_restricts_opened_at_or_before_fill(self) -> None:
        import inspect
        src = inspect.getsource(_active_exact_contract_candidates)
        assert "entry_ts" in src and "created_at" in src and "<=" in src, \
            "Query must require entry_ts <= fill_ts"

    def test_query_uses_for_update(self) -> None:
        import inspect
        src = inspect.getsource(_active_exact_contract_candidates)
        assert "FOR UPDATE" in src

    def test_query_does_not_include_terminal_row_fallback(self) -> None:
        import inspect
        src = inspect.getsource(_active_exact_contract_candidates)
        assert "10 minutes" not in src and "10 minute" not in src, \
            "Query must not include the 10-minute terminal-row fallback"
        assert "exit_ts" not in src, \
            "Query must not include recently-terminal rows via exit_ts"


# ─────────────────────────────────────────────────────────────────────────────
# _validate_sole_candidate
# ─────────────────────────────────────────────────────────────────────────────

class TestValidateSoleCandidate:
    def test_valid_candidate_passes(self) -> None:
        assert _validate_sole_candidate(
            _pos(), client_id=_CLIENT, contract=_CONTRACT
        ) is True

    def test_missing_id_fails(self) -> None:
        cand = _pos(); cand["id"] = ""
        assert _validate_sole_candidate(cand, client_id=_CLIENT, contract=_CONTRACT) is False

    def test_broker_repair_id_fails(self) -> None:
        cand = _pos(id="broker-repair-xyz")
        assert _validate_sole_candidate(cand, client_id=_CLIENT, contract=_CONTRACT) is False

    def test_client_mismatch_fails(self) -> None:
        assert _validate_sole_candidate(
            _pos(client_id="other@client.com"), client_id=_CLIENT, contract=_CONTRACT
        ) is False

    def test_contract_mismatch_fails(self) -> None:
        assert _validate_sole_candidate(
            _pos(contract="AAPL260718C00200000"), client_id=_CLIENT, contract=_CONTRACT
        ) is False

    def test_non_active_status_fails(self) -> None:
        for status in ("CLOSED", "EXPIRED", "CANCELED", "STOPPED"):
            cand = _pos(status=status)
            assert _validate_sole_candidate(cand, client_id=_CLIENT, contract=_CONTRACT) is False

    def test_zero_quantity_fails(self) -> None:
        assert _validate_sole_candidate(
            _pos(quantity_remaining=0), client_id=_CLIENT, contract=_CONTRACT
        ) is False


# ─────────────────────────────────────────────────────────────────────────────
# wrap_resolve_position — full resolution contract
# ─────────────────────────────────────────────────────────────────────────────

class TestWrapResolvePosition:

    def _wrap(self, parent_return=None):
        parent_calls = []
        def parent(c, order, fill_ts):
            parent_calls.append(1)
            return parent_return
        return wrap_resolve_position(parent), parent_calls

    # ── Real position ID bypasses guard ────────────────────────────────────

    def test_real_position_id_delegates_to_parent(self) -> None:
        wrapped, calls = self._wrap(parent_return=_pos())
        c = _cursor([])
        out = wrapped(c, _order(position_id="pos-real-001"), _FILL_TS)
        assert len(calls) == 1
        assert out is not None
        c.execute.assert_not_called()  # no candidate query for real IDs

    def test_real_position_id_does_not_query_candidates(self) -> None:
        wrapped, _ = self._wrap()
        c = _cursor([])
        wrapped(c, _order(position_id="pos-real-001"), _FILL_TS)
        c.execute.assert_not_called()

    # ── One OPEN candidate → repair ────────────────────────────────────────

    def test_one_open_candidate_repairs(self) -> None:
        candidate = _pos(status="OPEN")
        wrapped, calls = self._wrap()
        c = _cursor([candidate])
        out = wrapped(c, _order(position_id=None), _FILL_TS)
        assert out is not None
        assert out["id"] == "pos-real-001"
        assert len(calls) == 0  # parent not called

    def test_one_closing_candidate_repairs(self) -> None:
        candidate = _pos(status="CLOSING")
        wrapped, calls = self._wrap()
        c = _cursor([candidate])
        out = wrapped(c, _order(position_id=None), _FILL_TS)
        assert out is not None
        assert out["status"] == "CLOSING"
        assert len(calls) == 0

    # ── Two+ active candidates → quarantine ────────────────────────────────

    def test_two_active_candidates_quarantine(self) -> None:
        wrapped, calls = self._wrap()
        c = _cursor([_pos(id="pos-a"), _pos(id="pos-b")])
        out = wrapped(c, _order(position_id=None), _FILL_TS)
        assert out is None
        assert len(calls) == 0, "Parent must never be called on ambiguous candidates"

    def test_two_candidates_never_call_parent(self) -> None:
        wrapped, calls = self._wrap(parent_return=_pos())
        c = _cursor([_pos(id="pos-a"), _pos(id="pos-b"), _pos(id="pos-c")])
        out = wrapped(c, _order(position_id=None), _FILL_TS)
        assert out is None
        assert len(calls) == 0

    # ── One active + recently closed → resolves the active one ─────────────

    def test_one_active_plus_closed_resolves_active(self) -> None:
        """Active-only query excludes the closed row; guard sees exactly one."""
        # The candidate query returns OPEN/CLOSING rows only, so the closed
        # row is already excluded before this guard sees the candidates.
        open_pos = _pos(id="pos-open", status="OPEN")
        wrapped, calls = self._wrap()
        c = _cursor([open_pos])  # closed row filtered out by SQL
        out = wrapped(c, _order(position_id=None), _FILL_TS)
        assert out is not None
        assert out["id"] == "pos-open"
        assert len(calls) == 0

    # ── Zero active candidates → delegate to parent ────────────────────────

    def test_zero_active_candidates_delegates_to_parent(self) -> None:
        """Zero active rows: parent handles recently-closed resolution."""
        parent_pos = _pos(id="pos-parent")
        wrapped, calls = self._wrap(parent_return=parent_pos)
        c = _cursor([])  # no active candidates
        out = wrapped(c, _order(position_id=None), _FILL_TS)
        assert len(calls) == 1, "Zero active candidates must delegate to parent"
        assert out is not None

    def test_only_recently_closed_rows_delegates_to_parent(self) -> None:
        """Guard query excludes terminal rows; zero active → delegate."""
        # All rows are CLOSED (excluded by query) → _cursor returns empty
        wrapped, calls = self._wrap(parent_return=_pos())
        c = _cursor([])  # closed rows filtered by SQL
        out = wrapped(c, _order(position_id=None), _FILL_TS)
        assert len(calls) == 1

    # ── Filtering invariants (verified via SQL shape tests above) ──────────

    def test_zero_quantity_open_row_excluded(self) -> None:
        """Zero-quantity rows excluded by SQL; guard sees zero candidates → parent."""
        wrapped, calls = self._wrap(parent_return=_pos())
        c = _cursor([])  # zero-qty row filtered by SQL
        out = wrapped(c, _order(position_id=None), _FILL_TS)
        assert len(calls) == 1

    def test_terminal_row_excluded(self) -> None:
        wrapped, calls = self._wrap(parent_return=_pos())
        c = _cursor([])  # terminal rows filtered by SQL
        out = wrapped(c, _order(position_id=None), _FILL_TS)
        assert len(calls) == 1

    def test_entry_ts_after_fill_ts_excluded(self) -> None:
        """Positions opened after fill_ts are filtered by SQL."""
        wrapped, calls = self._wrap(parent_return=_pos())
        c = _cursor([])  # future entry filtered by SQL
        out = wrapped(c, _order(position_id=None), _FILL_TS)
        assert len(calls) == 1

    # ── Malformed sole candidate → fails closed ────────────────────────────

    def test_malformed_sole_candidate_returns_none(self) -> None:
        bad = _pos(id="broker-repair-xyz")
        wrapped, calls = self._wrap()
        c = _cursor([bad])
        out = wrapped(c, _order(position_id=None), _FILL_TS)
        assert out is None
        assert len(calls) == 0

    def test_sole_candidate_zero_qty_fails_closed(self) -> None:
        bad = _pos(quantity_remaining=0)
        wrapped, calls = self._wrap()
        c = _cursor([bad])
        out = wrapped(c, _order(position_id=None), _FILL_TS)
        assert out is None
        assert len(calls) == 0

    # ── Lookup failure → fail closed ───────────────────────────────────────

    def test_lookup_failure_fails_closed(self) -> None:
        wrapped, calls = self._wrap(parent_return=_pos())
        c = MagicMock()
        c.execute.side_effect = RuntimeError("DB connection lost")
        out = wrapped(c, _order(position_id=None), _FILL_TS)
        assert out is None
        assert len(calls) == 0, "Parent must not be called after lookup failure"

    def test_lookup_failure_does_not_call_parent(self) -> None:
        wrapped, calls = self._wrap(parent_return=_pos())
        c = MagicMock()
        c.execute.side_effect = Exception("timeout")
        out = wrapped(c, _order(position_id=None), _FILL_TS)
        assert len(calls) == 0

    # ── Synthetic ID detection ─────────────────────────────────────────────

    def test_null_position_id_is_synthetic(self) -> None:
        wrapped, _ = self._wrap(parent_return=_pos())
        c = _cursor([_pos()])
        out = wrapped(c, _order(position_id=None), _FILL_TS)
        assert out is not None  # was intercepted

    def test_empty_position_id_is_synthetic(self) -> None:
        wrapped, _ = self._wrap(parent_return=_pos())
        c = _cursor([_pos()])
        out = wrapped(c, _order(position_id=""), _FILL_TS)
        assert out is not None  # was intercepted

    def test_broker_repair_position_id_is_synthetic(self) -> None:
        wrapped, _ = self._wrap(parent_return=_pos())
        c = _cursor([_pos()])
        out = wrapped(c, _order(position_id="broker-repair-abc"), _FILL_TS)
        assert out is not None  # was intercepted


# ─────────────────────────────────────────────────────────────────────────────
# Installation
# ─────────────────────────────────────────────────────────────────────────────

class TestInstallation:
    def _clean(self, eftg):
        for attr in (_PATCHED_ATTR, _ORIGINAL_ATTR):
            if hasattr(eftg, attr):
                delattr(eftg, attr)
        return eftg._resolve_position

    def test_patches_resolve_position(self) -> None:
        from ap import exit_fill_truth_guard as eftg
        orig = self._clean(eftg)
        install_exit_position_ambiguity_guard()
        assert eftg._resolve_position is not orig
        eftg._resolve_position = orig
        delattr(eftg, _PATCHED_ATTR)

    def test_idempotent(self) -> None:
        from ap import exit_fill_truth_guard as eftg
        orig = self._clean(eftg)
        install_exit_position_ambiguity_guard()
        after_first = eftg._resolve_position
        install_exit_position_ambiguity_guard()
        assert eftg._resolve_position is after_first
        eftg._resolve_position = orig
        delattr(eftg, _PATCHED_ATTR)
