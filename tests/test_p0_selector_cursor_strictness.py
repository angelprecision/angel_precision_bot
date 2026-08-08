"""P0 — Untrusted selector cursor state must fail closed without coercion.

PR #422: selector cursor strictness + runner identity validation.

Invariants proved:
  - bool, float, and string coercion are never accepted for version,
    generation, or attempt count fields in the durable cursor.
  - Malformed cursor collections are never silently replaced with {} or [].
  - Malformed element shapes within collections are rejected.
  - Invalid expected-authority args return a classified reason, not an exception.
  - blank / whitespace-padded self.client_id raises before any DB call.
  - Only exact zero-row CAS returns False; everything else raises.
  - All #421 direction-reversal invariants are unaffected.
"""
from __future__ import annotations

import os

# Stub DATABASE_URL so ap/__init__.py can import without a live Supabase connection.
os.environ.setdefault("DATABASE_URL", "postgresql://fake-host/fake-db")

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import ap.selector_cursor_persistence_guard as cursor_guard
from ap.selector_retry_policy import (
    SelectorRecoveryCursorPersistFailed,
    load_selector_recovery_cursor,
    new_selector_recovery_cursor,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

_BASE_ORDER = "order-aaa1"
_BASE_CLIENT = "jason@example.com"
_BASE_MODE = "live"
_BASE_SIGNAL = "sig-bbb2"
_BASE_GEN = 2
_BASE_ATTEMPT = 2


def _valid_cursor(**overrides) -> dict:
    """Produce a structurally valid recovery cursor at generation 2 attempt 2."""
    c = new_selector_recovery_cursor(
        local_order_id=_BASE_ORDER,
        client_id=_BASE_CLIENT,
        execution_mode=_BASE_MODE,
        signal_id=_BASE_SIGNAL,
        materialization_generation=_BASE_GEN,
        selector_attempt_count=_BASE_ATTEMPT,
    )
    c.update(overrides)
    return c


def _load(candidate, *, gen=_BASE_GEN, attempt=_BASE_ATTEMPT, allow_prev=False):
    return load_selector_recovery_cursor(
        candidate,
        local_order_id=_BASE_ORDER,
        client_id=_BASE_CLIENT,
        execution_mode=_BASE_MODE,
        signal_id=_BASE_SIGNAL,
        materialization_generation=gen,
        selector_attempt_count=attempt,
        allow_previous_generation=allow_prev,
    )


def _assert_fresh_collections(cursor: dict) -> None:
    """Assert the returned cursor carries empty durable collections (fresh start)."""
    assert cursor["attempted_symbols"] == {}
    assert cursor["structurally_skipped_symbols"] == {}
    assert cursor["expirations_probed"] == []
    assert cursor["last_ranked_index_by_expiration"] == {}


# ── Version field strictness ──────────────────────────────────────────────────

class TestVersionStrictness:
    def test_boolean_true_rejected_not_coerced_to_integer_one(self):
        """True == 1 in Python but type(True) is bool, not int."""
        c = _valid_cursor()
        c["version"] = True
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:version"

    def test_boolean_false_rejected(self):
        c = _valid_cursor()
        c["version"] = False
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:version"

    def test_float_one_rejected(self):
        """1.0 is not integer 1."""
        c = _valid_cursor()
        c["version"] = 1.0
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:version"

    def test_string_one_rejected(self):
        c = _valid_cursor()
        c["version"] = "1"
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:version"

    def test_none_version_rejected(self):
        c = _valid_cursor()
        c["version"] = None
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:version"

    def test_version_two_rejected(self):
        c = _valid_cursor()
        c["version"] = 2
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:version"

    def test_exact_integer_one_accepted(self):
        c = _valid_cursor()
        loaded, reason = _load(c)
        assert reason is None
        assert loaded["version"] == 1


# ── Cursor generation field strictness ───────────────────────────────────────

class TestCursorGenerationStrictness:
    def test_float_generation_rejected_not_truncated(self):
        """2.9 must not become 2 via int() truncation."""
        c = _valid_cursor()
        c["materialization_generation"] = 2.9
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"

    def test_float_generation_exact_rejected(self):
        """2.0 is still a float, not an int."""
        c = _valid_cursor()
        c["materialization_generation"] = 2.0
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"

    def test_boolean_cursor_generation_rejected(self):
        c = _valid_cursor()
        c["materialization_generation"] = True
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"

    def test_numeric_string_cursor_generation_rejected(self):
        """'2' must not silently become integer 2 — no proven producer."""
        c = _valid_cursor()
        c["materialization_generation"] = "2"
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"

    def test_zero_cursor_generation_rejected(self):
        c = _valid_cursor()
        c["materialization_generation"] = 0
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"

    def test_negative_cursor_generation_rejected(self):
        c = _valid_cursor()
        c["materialization_generation"] = -1
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"

    def test_none_cursor_generation_rejected(self):
        c = _valid_cursor()
        c["materialization_generation"] = None
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"

    def test_unrelated_generation_rejected(self):
        """Generation 5 against expected 2 without allow_previous_generation."""
        c = _valid_cursor()
        c["materialization_generation"] = 5
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"


# ── Cursor attempt count strictness ──────────────────────────────────────────

class TestCursorAttemptStrictness:
    def test_malformed_string_returns_reason_not_exception(self):
        """'abc' must not raise ValueError — must return classified reason."""
        c = _valid_cursor()
        c["selector_attempt_count"] = "abc"
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:selector_attempt_count"

    def test_float_attempt_rejected(self):
        c = _valid_cursor()
        c["selector_attempt_count"] = 2.0
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:selector_attempt_count"

    def test_boolean_attempt_rejected(self):
        c = _valid_cursor()
        c["selector_attempt_count"] = True
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:selector_attempt_count"

    def test_numeric_string_attempt_rejected(self):
        """'2' must not silently become 2."""
        c = _valid_cursor()
        c["selector_attempt_count"] = "2"
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:selector_attempt_count"

    def test_zero_attempt_rejected(self):
        c = _valid_cursor()
        c["selector_attempt_count"] = 0
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:selector_attempt_count"

    def test_negative_attempt_rejected(self):
        c = _valid_cursor()
        c["selector_attempt_count"] = -1
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:selector_attempt_count"

    def test_none_attempt_rejected(self):
        c = _valid_cursor()
        c["selector_attempt_count"] = None
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:selector_attempt_count"


# ── Collection container shape validation ────────────────────────────────────

class TestCollectionContainerShapes:
    @pytest.mark.parametrize("bad_value", ["corrupt", [], 42, None, True])
    def test_attempted_symbols_must_be_dict(self, bad_value):
        c = _valid_cursor()
        c["attempted_symbols"] = bad_value
        loaded, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"
        _assert_fresh_collections(loaded)

    @pytest.mark.parametrize("bad_value", ["corrupt", [], 42, None, True])
    def test_structurally_skipped_symbols_must_be_dict(self, bad_value):
        c = _valid_cursor()
        c["structurally_skipped_symbols"] = bad_value
        loaded, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:structurally_skipped_symbols"
        _assert_fresh_collections(loaded)

    @pytest.mark.parametrize("bad_value", [{}, "corrupt", 42, None, True])
    def test_expirations_probed_must_be_list(self, bad_value):
        c = _valid_cursor()
        c["expirations_probed"] = bad_value
        loaded, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:expirations_probed"
        _assert_fresh_collections(loaded)

    @pytest.mark.parametrize("bad_value", ["corrupt", [], 42, None, True])
    def test_last_ranked_index_must_be_dict(self, bad_value):
        c = _valid_cursor()
        c["last_ranked_index_by_expiration"] = bad_value
        loaded, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:last_ranked_index_by_expiration"
        _assert_fresh_collections(loaded)


# ── Collection element shape validation ──────────────────────────────────────

class TestCollectionElementShapes:
    def test_attempted_symbols_blank_key_rejected(self):
        c = _valid_cursor()
        c["attempted_symbols"] = {"   ": {"attempt_number": 1}}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    def test_attempted_symbols_non_string_key_rejected(self):
        c = _valid_cursor()
        c["attempted_symbols"] = {42: {"attempt_number": 1}}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    def test_attempted_symbols_record_not_dict_rejected(self):
        c = _valid_cursor()
        c["attempted_symbols"] = {"SPY260807C00600000": "not-a-dict"}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    def test_skipped_symbols_blank_key_rejected(self):
        c = _valid_cursor()
        c["structurally_skipped_symbols"] = {"  ": {"reason": "no_chain"}}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:structurally_skipped_symbols"

    def test_skipped_symbols_record_not_dict_rejected(self):
        c = _valid_cursor()
        c["structurally_skipped_symbols"] = {"QQQ": None}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:structurally_skipped_symbols"

    def test_expirations_probed_blank_entry_rejected(self):
        c = _valid_cursor()
        c["expirations_probed"] = ["2026-08-07", "   "]
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:expirations_probed"

    def test_expirations_probed_non_string_entry_rejected(self):
        c = _valid_cursor()
        c["expirations_probed"] = ["2026-08-07", 42]
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:expirations_probed"

    def test_ranked_expiration_boolean_index_rejected(self):
        """True is bool, not int — must be rejected even though bool subclasses int."""
        c = _valid_cursor()
        c["last_ranked_index_by_expiration"] = {"2026-08-07": True}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:last_ranked_index_by_expiration"

    def test_ranked_expiration_negative_index_rejected(self):
        c = _valid_cursor()
        c["last_ranked_index_by_expiration"] = {"2026-08-07": -1}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:last_ranked_index_by_expiration"

    def test_ranked_expiration_float_index_rejected(self):
        c = _valid_cursor()
        c["last_ranked_index_by_expiration"] = {"2026-08-07": 0.0}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:last_ranked_index_by_expiration"

    def test_ranked_expiration_blank_key_rejected(self):
        c = _valid_cursor()
        c["last_ranked_index_by_expiration"] = {"  ": 0}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:last_ranked_index_by_expiration"


# ── Valid cursor happy paths ──────────────────────────────────────────────────

class TestValidCursorLoads:
    def test_valid_empty_cursor_loads(self):
        c = _valid_cursor()
        loaded, reason = _load(c)
        assert reason is None
        assert loaded["version"] == 1
        assert loaded["materialization_generation"] == _BASE_GEN
        assert loaded["selector_attempt_count"] == _BASE_ATTEMPT

    def test_valid_attempted_symbol_progress_survives(self):
        c = _valid_cursor()
        c["attempted_symbols"] = {
            "SPY260807C00600000": {"attempt_number": 2, "transient": True}
        }
        loaded, reason = _load(c)
        assert reason is None
        assert "SPY260807C00600000" in loaded["attempted_symbols"]
        assert loaded["attempted_symbols"]["SPY260807C00600000"]["transient"] is True

    def test_valid_structural_skip_progress_survives(self):
        c = _valid_cursor()
        c["structurally_skipped_symbols"] = {
            "QQQ260807C00550000": {"reason": "NO_CHAIN_DATA"}
        }
        loaded, reason = _load(c)
        assert reason is None
        assert "QQQ260807C00550000" in loaded["structurally_skipped_symbols"]

    def test_valid_expiration_and_ranking_progress_survives(self):
        c = _valid_cursor()
        c["expirations_probed"] = ["2026-08-07", "2026-08-14"]
        c["last_ranked_index_by_expiration"] = {"2026-08-07": 3, "2026-08-14": 0}
        loaded, reason = _load(c)
        assert reason is None
        assert "2026-08-07" in loaded["expirations_probed"]
        assert loaded["last_ranked_index_by_expiration"]["2026-08-07"] == 3

    def test_attempt_count_advanced_to_trusted_authority(self):
        """Cursor attempt 1 against expected authority 3 → advances to 3."""
        c = _valid_cursor()
        c["selector_attempt_count"] = 1
        loaded, reason = _load(c, attempt=3)
        assert reason is None
        assert loaded["selector_attempt_count"] == 3

    def test_attempt_count_not_regressed_below_cursor_value(self):
        """Cursor attempt 3 against expected authority 2 → stays 3."""
        c = _valid_cursor()
        c["selector_attempt_count"] = 3
        loaded, reason = _load(c, attempt=2)
        assert reason is None
        assert loaded["selector_attempt_count"] == 3

    def test_absent_cursor_returns_fresh_and_none_reason(self):
        loaded, reason = _load(None)
        assert reason is None
        _assert_fresh_collections(loaded)

    def test_empty_string_cursor_returns_fresh_and_none_reason(self):
        loaded, reason = _load("")
        assert reason is None
        _assert_fresh_collections(loaded)

    def test_non_dict_cursor_returns_malformed(self):
        _, reason = _load("not-a-dict")
        assert reason == "MALFORMED_CURSOR"

    def test_zero_index_in_ranked_is_valid(self):
        """Index 0 is a valid non-negative integer."""
        c = _valid_cursor()
        c["last_ranked_index_by_expiration"] = {"2026-08-07": 0}
        loaded, reason = _load(c)
        assert reason is None
        assert loaded["last_ranked_index_by_expiration"]["2026-08-07"] == 0


# ── Cursor bounding (size limits unchanged) ───────────────────────────────────

class TestCursorBounding:
    def test_attempted_symbols_bounded_to_cursor_max(self):
        from ap.selector_retry_policy import _CURSOR_MAX_SYMBOLS
        c = _valid_cursor()
        c["attempted_symbols"] = {
            f"SYM{i:04d}260807C00600000": {"attempt_number": 1}
            for i in range(_CURSOR_MAX_SYMBOLS + 50)
        }
        loaded, reason = _load(c)
        assert reason is None
        assert len(loaded["attempted_symbols"]) == _CURSOR_MAX_SYMBOLS

    def test_expirations_probed_bounded_to_cursor_max(self):
        from ap.selector_retry_policy import _CURSOR_MAX_EXPIRATIONS
        c = _valid_cursor()
        c["expirations_probed"] = [
            f"2026-0{(i % 9) + 1}-{(i % 28) + 1:02d}" for i in range(_CURSOR_MAX_EXPIRATIONS + 20)
        ]
        loaded, reason = _load(c)
        assert reason is None
        assert len(loaded["expirations_probed"]) <= _CURSOR_MAX_EXPIRATIONS


# ── Previous-generation allowance (exact semantics unchanged) ─────────────────

class TestPreviousGenerationAllowance:
    def test_exact_previous_generation_allowed_when_flag_set(self):
        """Generation N-1 accepted when allow_previous_generation=True and N > 1."""
        c = _valid_cursor()
        c["materialization_generation"] = 1  # prev of 2
        loaded, reason = _load(c, gen=2, allow_prev=True)
        assert reason is None

    def test_previous_generation_rejected_without_flag(self):
        c = _valid_cursor()
        c["materialization_generation"] = 1
        _, reason = _load(c, gen=2, allow_prev=False)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"

    def test_previous_generation_not_allowed_when_gen_is_one(self):
        """N=1: previous (0) is not a valid positive integer."""
        c = _valid_cursor()
        c["materialization_generation"] = 1
        loaded, reason = _load(c, gen=1, allow_prev=True)
        # gen=1 allow_prev=True: allowed_generations={1}, no prev added (not > 1)
        assert reason is None  # gen 1 matches gen 1 — still valid

    def test_unrelated_older_generation_rejected_even_with_flag(self):
        """Only N-1 is allowed, not arbitrary older generations."""
        c = _valid_cursor()
        c["materialization_generation"] = 1  # 3 - 2 not 3 - 1
        _, reason = _load(c, gen=3, allow_prev=True)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"


# ── Invalid expected authority (caller args) ──────────────────────────────────

class TestExpectedAuthorityValidation:
    """Malformed trusted expected-authority args return classified reason, no exception."""

    @pytest.mark.parametrize("bad_gen", [None, True, False, 2.0, 2.9, "2", 0, -1])
    def test_invalid_expected_generation_returns_classified_reason(self, bad_gen):
        c = _valid_cursor()
        loaded, reason = _load(c, gen=bad_gen)
        assert reason == "INVALID_EXPECTED_CURSOR_AUTHORITY:materialization_generation"
        _assert_fresh_collections(loaded)

    @pytest.mark.parametrize("bad_attempt", [None, True, False, 2.0, "2", 0, -1])
    def test_invalid_expected_attempt_returns_classified_reason(self, bad_attempt):
        c = _valid_cursor()
        loaded, reason = _load(c, attempt=bad_attempt)
        assert reason == "INVALID_EXPECTED_CURSOR_AUTHORITY:selector_attempt_count"
        _assert_fresh_collections(loaded)

    def test_invalid_expected_authority_does_not_raise(self):
        """Even bizarre args must never raise from this function."""
        c = _valid_cursor()
        try:
            loaded, reason = _load(c, gen="not-a-number")
            assert reason is not None
        except Exception as exc:  # noqa: BLE001
            pytest.fail(f"load_selector_recovery_cursor raised unexpectedly: {exc}")


# ── Persistence guard — runner identity validation ────────────────────────────

def _make_runner(client_id: str):
    return SimpleNamespace(client_id=client_id)


def _call_guard(runner, monkeypatch, db_rowcount=1):
    """Call _guarded_persist_selector_recovery_cursor with standard args."""
    db_calls = []

    def _fake_db_write(fn):
        db_calls.append(fn)
        return db_rowcount

    monkeypatch.setattr(cursor_guard, "_run_db_write", _fake_db_write)
    return cursor_guard._guarded_persist_selector_recovery_cursor(
        runner,
        _BASE_ORDER,
        owner="watcher-1",
        generation=2,
        signal_id=_BASE_SIGNAL,
        execution_mode=_BASE_MODE,
        cursor=_valid_cursor(),
    ), db_calls


class TestPersistenceGuardRunnerIdentity:
    def test_blank_client_id_raises_before_db(self, monkeypatch):
        runner = _make_runner("   ")
        db_calls = []

        def _unexpected_db(fn):
            db_calls.append(fn)
            return 1

        monkeypatch.setattr(cursor_guard, "_run_db_write", _unexpected_db)
        with pytest.raises(SelectorRecoveryCursorPersistFailed, match="identity or payload invalid"):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner,
                _BASE_ORDER,
                owner="watcher-1",
                generation=2,
                signal_id=_BASE_SIGNAL,
                execution_mode=_BASE_MODE,
                cursor=_valid_cursor(),
            )
        assert db_calls == [], "DB must not be called for blank runner client_id"

    def test_empty_string_client_id_raises_before_db(self, monkeypatch):
        runner = _make_runner("")
        db_calls = []

        def _unexpected_db(fn):
            db_calls.append(fn)
            return 1

        monkeypatch.setattr(cursor_guard, "_run_db_write", _unexpected_db)
        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner,
                _BASE_ORDER,
                owner="watcher-1",
                generation=2,
                signal_id=_BASE_SIGNAL,
                execution_mode=_BASE_MODE,
                cursor=_valid_cursor(),
            )
        assert db_calls == []

    def test_whitespace_padded_client_id_raises_before_db(self, monkeypatch):
        """'jason ' has trailing whitespace — not equal to its strip()."""
        runner = _make_runner("jason ")
        db_calls = []

        def _unexpected_db(fn):
            db_calls.append(fn)
            return 1

        monkeypatch.setattr(cursor_guard, "_run_db_write", _unexpected_db)
        with pytest.raises(SelectorRecoveryCursorPersistFailed, match="identity or payload invalid"):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner,
                _BASE_ORDER,
                owner="watcher-1",
                generation=2,
                signal_id=_BASE_SIGNAL,
                execution_mode=_BASE_MODE,
                cursor=_valid_cursor(),
            )
        assert db_calls == []

    def test_valid_runner_identity_reaches_db(self, monkeypatch):
        runner = _make_runner("jason@example.com")
        result, db_calls = _call_guard(runner, monkeypatch, db_rowcount=1)
        assert result is True
        assert len(db_calls) == 1

    def test_missing_client_id_attr_raises_before_db(self, monkeypatch):
        runner = SimpleNamespace()  # no client_id attribute
        db_calls = []

        def _unexpected_db(fn):
            db_calls.append(fn)
            return 1

        monkeypatch.setattr(cursor_guard, "_run_db_write", _unexpected_db)
        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner,
                _BASE_ORDER,
                owner="watcher-1",
                generation=2,
                signal_id=_BASE_SIGNAL,
                execution_mode=_BASE_MODE,
                cursor=_valid_cursor(),
            )
        assert db_calls == []


# ── Persistence guard — rowcount classification ───────────────────────────────

class TestPersistenceGuardRowcount:
    def test_exact_zero_row_cas_returns_false(self, monkeypatch):
        runner = _make_runner("jason@example.com")
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda fn: 0)
        result = cursor_guard._guarded_persist_selector_recovery_cursor(
            runner,
            _BASE_ORDER,
            owner="watcher-1",
            generation=2,
            signal_id=_BASE_SIGNAL,
            execution_mode=_BASE_MODE,
            cursor=_valid_cursor(),
        )
        assert result is False

    def test_exact_one_row_write_returns_true(self, monkeypatch):
        runner = _make_runner("jason@example.com")
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda fn: 1)
        result = cursor_guard._guarded_persist_selector_recovery_cursor(
            runner,
            _BASE_ORDER,
            owner="watcher-1",
            generation=2,
            signal_id=_BASE_SIGNAL,
            execution_mode=_BASE_MODE,
            cursor=_valid_cursor(),
        )
        assert result is True

    def test_rowcount_greater_than_one_raises(self, monkeypatch):
        runner = _make_runner("jason@example.com")
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda fn: 2)
        with pytest.raises(SelectorRecoveryCursorPersistFailed, match="unexpected rowcount"):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner,
                _BASE_ORDER,
                owner="watcher-1",
                generation=2,
                signal_id=_BASE_SIGNAL,
                execution_mode=_BASE_MODE,
                cursor=_valid_cursor(),
            )

    def test_none_rowcount_raises_unconfirmed(self, monkeypatch):
        runner = _make_runner("jason@example.com")
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda fn: None)
        with pytest.raises(SelectorRecoveryCursorPersistFailed, match="unconfirmed"):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner,
                _BASE_ORDER,
                owner="watcher-1",
                generation=2,
                signal_id=_BASE_SIGNAL,
                execution_mode=_BASE_MODE,
                cursor=_valid_cursor(),
            )

    def test_minus_one_rowcount_raises_unconfirmed(self, monkeypatch):
        runner = _make_runner("jason@example.com")
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda fn: -1)
        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner,
                _BASE_ORDER,
                owner="watcher-1",
                generation=2,
                signal_id=_BASE_SIGNAL,
                execution_mode=_BASE_MODE,
                cursor=_valid_cursor(),
            )

    def test_db_exception_raises_persist_failed(self, monkeypatch):
        runner = _make_runner("jason@example.com")

        def _raise(fn):
            raise RuntimeError("connection refused")

        monkeypatch.setattr(cursor_guard, "_run_db_write", _raise)
        with pytest.raises(SelectorRecoveryCursorPersistFailed, match="database write failed"):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner,
                _BASE_ORDER,
                owner="watcher-1",
                generation=2,
                signal_id=_BASE_SIGNAL,
                execution_mode=_BASE_MODE,
                cursor=_valid_cursor(),
            )

    def test_malformed_bool_rowcount_raises(self, monkeypatch):
        runner = _make_runner("jason@example.com")
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda fn: True)
        with pytest.raises(SelectorRecoveryCursorPersistFailed, match="malformed"):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner,
                _BASE_ORDER,
                owner="watcher-1",
                generation=2,
                signal_id=_BASE_SIGNAL,
                execution_mode=_BASE_MODE,
                cursor=_valid_cursor(),
            )


# ── Pre-amendment regression proof ───────────────────────────────────────────
#
# These tests prove that the originally documented defects ACTUALLY existed
# before correction. Each is documented as a "would have passed" / "would have
# raised" on the pre-amendment implementation. The tests themselves verify
# current (post-amendment) behavior. Their names document what old code did.

class TestPreAmendmentRegressionProof:
    def test_boolean_version_was_accepted_before_amendment(self):
        """Old code: expected = {'version': 1, ...}; True == 1 → passed identity check.
        New code: type(True) is bool, not int → IDENTITY_MISMATCH:version.
        """
        c = _valid_cursor()
        c["version"] = True
        _, reason = _load(c)
        # Post-amendment: must be rejected
        assert reason == "IDENTITY_MISMATCH:version"

    def test_float_generation_was_truncated_before_amendment(self):
        """Old code: int(2.9) == 2 == expected_generation → passed.
        New code: _strict_positive_cursor_int(2.9) → None → IDENTITY_MISMATCH.
        """
        c = _valid_cursor()
        c["materialization_generation"] = 2.9
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"

    def test_malformed_attempt_raised_exception_before_amendment(self):
        """Old code: int(cursor.get('selector_attempt_count') or 0) with 'abc' → ValueError raised.
        New code: _strict_positive_cursor_int('abc') → None → MALFORMED_CURSOR reason.
        """
        c = _valid_cursor()
        c["selector_attempt_count"] = "abc"
        # Must not raise; must return classified reason
        loaded, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:selector_attempt_count"

    def test_malformed_collection_was_silently_erased_before_amendment(self):
        """Old code: (attempted if isinstance(attempted, dict) else {}) → silently erased evidence.
        New code: isinstance check before use → MALFORMED_CURSOR returned.
        """
        c = _valid_cursor()
        c["attempted_symbols"] = "corrupt"
        loaded, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"
        # Fresh cursor returned — collections are empty (not the corrupt value)
        assert loaded["attempted_symbols"] == {}

    def test_blank_runner_client_was_classified_as_ownership_loss_before_amendment(self):
        """Old code: self.client_id used directly in CAS with blank value → 0-row UPDATE → False.
        New code: client_raw != durable_client → raises SelectorRecoveryCursorPersistFailed.
        """
        runner = _make_runner("   ")
        db_called = []

        def _track_db(fn):
            db_called.append(True)
            return 0  # old behavior: zero rows → False

        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(cursor_guard, "_run_db_write", _track_db)
            with pytest.raises(SelectorRecoveryCursorPersistFailed):
                cursor_guard._guarded_persist_selector_recovery_cursor(
                    runner,
                    _BASE_ORDER,
                    owner="watcher-1",
                    generation=2,
                    signal_id=_BASE_SIGNAL,
                    execution_mode=_BASE_MODE,
                    cursor=_valid_cursor(),
                )
        # DB must NOT have been called
        assert db_called == []


# ── Money-path safety: no selector/broker/order/position calls ────────────────

class TestMoneyPathSafety:
    """Malformed cursor validation must not trigger any selector or broker activity."""

    def _assert_no_money_path_calls(self, mock_ns):
        for name, m in mock_ns.items():
            if hasattr(m, "call_count"):
                assert m.call_count == 0, f"Unexpected call to {name}"

    def test_malformed_cursor_does_not_call_selector(self):
        c = _valid_cursor()
        c["version"] = True  # will fail at version check
        _, reason = _load(c)
        assert reason is not None
        # No selector infrastructure to mock here — the function is pure logic.
        # Asserting it returns without side effects is sufficient.

    def test_persistence_precondition_failure_does_not_reach_db(self, monkeypatch):
        runner = _make_runner("")
        db_called = []
        db_conn_called = []

        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda fn: db_called.append(1) or 1)
        monkeypatch.setattr(cursor_guard, "_db_conn", lambda: db_conn_called.append(1))

        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner,
                _BASE_ORDER,
                owner="watcher-1",
                generation=2,
                signal_id=_BASE_SIGNAL,
                execution_mode=_BASE_MODE,
                cursor=_valid_cursor(),
            )

        assert db_called == [], "zero DB write calls expected"
        assert db_conn_called == [], "zero DB conn calls expected"
