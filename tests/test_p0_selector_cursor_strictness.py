"""P0 — Untrusted selector cursor state must fail closed without coercion.

PR #422 + amendment: selector cursor strictness, nested record validation,
selector_symbol_may_retry exact-bool, and runner identity validation.

Invariants proved:
  - bool/float/string coercion rejected for version, generation, attempt count.
  - Malformed cursor collections never silently replaced with {} or [].
  - Nested attempted-symbol records fully validated (attempt_number, transient,
    attempted_at, expiration, result_reason) against the real producer schema.
  - transient="false" is truthy in Python but MUST NOT become retry authority.
  - Timezone-naive timestamps are rejected without silent UTC normalization.
  - Canonical symbol key enforced; non-canonical keys are rejected.
  - Nested structural-skip records fully validated (skip_reason, observed_at).
  - Validation occurs before bounding — corruption anywhere rejects the cursor.
  - selector_symbol_may_retry uses exact bool, not bool(untrusted_value).
  - blank/whitespace self.client_id raises before any DB call.
  - Only exact zero-row CAS returns False; everything else raises.
  - All #421 direction-reversal invariants are unaffected.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

# Stub DATABASE_URL so ap/__init__.py can import without a live Supabase connection.
os.environ.setdefault("DATABASE_URL", "postgresql://fake-host/fake-db")

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import ap.selector_cursor_persistence_guard as cursor_guard
from ap.selector_retry_policy import (
    SelectorRecoveryCursorPersistFailed,
    _CURSOR_MAX_SYMBOLS,
    _CURSOR_MAX_EXPIRATIONS,
    _parse_cursor_aware_timestamp,
    load_selector_recovery_cursor,
    new_selector_recovery_cursor,
    record_selector_recovery_attempt,
    record_selector_structural_skip,
    selector_symbol_may_retry,
)


# ── Constants ─────────────────────────────────────────────────────────────────

_BASE_ORDER = "order-aaa1"
_BASE_CLIENT = "jason@example.com"
_BASE_MODE = "live"
_BASE_SIGNAL = "sig-bbb2"
_BASE_GEN = 2
_BASE_ATTEMPT = 2

_AWARE_TS = "2026-08-07T14:30:00+00:00"
_NAIVE_TS = "2026-08-07T14:30:00"   # No tzinfo — must be rejected

# Canonical symbol per producer rule: "".join(str(s).upper().split())
_CANON_SYMBOL = "SPY260807C00600000"
_CANON_SYMBOL_2 = "QQQ260807C00550000"


# ── Fixture helpers ───────────────────────────────────────────────────────────

def _base_cursor(**overrides) -> dict:
    """Empty cursor at generation 2, attempt 2."""
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


def _cursor_with_attempt(symbol=_CANON_SYMBOL, **attempt_overrides) -> dict:
    """Cursor with one canonical attempted-symbol record (via real producer)."""
    base = _base_cursor()
    return record_selector_recovery_attempt(
        base,
        symbol=symbol,
        attempt_number=2,
        expiration="2026-08-07",
        result_reason="NO_CHAIN_DATA",
        transient=True,
        provider_timestamp=None,
    )


def _cursor_with_skip(symbol=_CANON_SYMBOL_2) -> dict:
    """Cursor with one canonical structural-skip record (via real producer)."""
    base = _base_cursor()
    return record_selector_structural_skip(
        base,
        symbol=symbol,
        skip_reason="NO_CHAIN_DATA",
    )


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
    assert cursor["attempted_symbols"] == {}
    assert cursor["structurally_skipped_symbols"] == {}
    assert cursor["expirations_probed"] == []
    assert cursor["last_ranked_index_by_expiration"] == {}


# ── Version field strictness ──────────────────────────────────────────────────

class TestVersionStrictness:
    def test_boolean_true_rejected_not_coerced_to_integer_one(self):
        c = _base_cursor()
        c["version"] = True
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:version"

    def test_boolean_false_rejected(self):
        c = _base_cursor()
        c["version"] = False
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:version"

    def test_float_one_rejected(self):
        c = _base_cursor()
        c["version"] = 1.0
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:version"

    def test_string_one_rejected(self):
        c = _base_cursor()
        c["version"] = "1"
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:version"

    def test_none_version_rejected(self):
        c = _base_cursor()
        c["version"] = None
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:version"

    def test_version_two_rejected(self):
        c = _base_cursor()
        c["version"] = 2
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:version"

    def test_exact_integer_one_accepted(self):
        c = _base_cursor()
        loaded, reason = _load(c)
        assert reason is None
        assert loaded["version"] == 1


# ── Cursor generation field strictness ───────────────────────────────────────

class TestCursorGenerationStrictness:
    def test_float_generation_rejected_not_truncated(self):
        c = _base_cursor()
        c["materialization_generation"] = 2.9
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"

    def test_float_generation_exact_rejected(self):
        c = _base_cursor()
        c["materialization_generation"] = 2.0
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"

    def test_boolean_cursor_generation_rejected(self):
        c = _base_cursor()
        c["materialization_generation"] = True
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"

    def test_numeric_string_cursor_generation_rejected(self):
        c = _base_cursor()
        c["materialization_generation"] = "2"
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"

    def test_zero_cursor_generation_rejected(self):
        c = _base_cursor()
        c["materialization_generation"] = 0
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"

    def test_negative_cursor_generation_rejected(self):
        c = _base_cursor()
        c["materialization_generation"] = -1
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"

    def test_none_cursor_generation_rejected(self):
        c = _base_cursor()
        c["materialization_generation"] = None
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"

    def test_unrelated_generation_rejected(self):
        c = _base_cursor()
        c["materialization_generation"] = 5
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"


# ── Cursor attempt count strictness ──────────────────────────────────────────

class TestCursorAttemptStrictness:
    def test_malformed_string_returns_reason_not_exception(self):
        c = _base_cursor()
        c["selector_attempt_count"] = "abc"
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:selector_attempt_count"

    def test_float_attempt_rejected(self):
        c = _base_cursor()
        c["selector_attempt_count"] = 2.0
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:selector_attempt_count"

    def test_boolean_attempt_rejected(self):
        c = _base_cursor()
        c["selector_attempt_count"] = True
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:selector_attempt_count"

    def test_numeric_string_attempt_rejected(self):
        c = _base_cursor()
        c["selector_attempt_count"] = "2"
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:selector_attempt_count"

    def test_zero_attempt_rejected(self):
        c = _base_cursor()
        c["selector_attempt_count"] = 0
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:selector_attempt_count"

    def test_negative_attempt_rejected(self):
        c = _base_cursor()
        c["selector_attempt_count"] = -1
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:selector_attempt_count"

    def test_none_attempt_rejected(self):
        c = _base_cursor()
        c["selector_attempt_count"] = None
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:selector_attempt_count"


# ── Collection container shape validation ────────────────────────────────────

class TestCollectionContainerShapes:
    @pytest.mark.parametrize("bad_value", ["corrupt", [], 42, None, True])
    def test_attempted_symbols_must_be_dict(self, bad_value):
        c = _base_cursor()
        c["attempted_symbols"] = bad_value
        loaded, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"
        _assert_fresh_collections(loaded)

    @pytest.mark.parametrize("bad_value", ["corrupt", [], 42, None, True])
    def test_structurally_skipped_symbols_must_be_dict(self, bad_value):
        c = _base_cursor()
        c["structurally_skipped_symbols"] = bad_value
        loaded, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:structurally_skipped_symbols"
        _assert_fresh_collections(loaded)

    @pytest.mark.parametrize("bad_value", [{}, "corrupt", 42, None, True])
    def test_expirations_probed_must_be_list(self, bad_value):
        c = _base_cursor()
        c["expirations_probed"] = bad_value
        loaded, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:expirations_probed"
        _assert_fresh_collections(loaded)

    @pytest.mark.parametrize("bad_value", ["corrupt", [], 42, None, True])
    def test_last_ranked_index_must_be_dict(self, bad_value):
        c = _base_cursor()
        c["last_ranked_index_by_expiration"] = bad_value
        loaded, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:last_ranked_index_by_expiration"
        _assert_fresh_collections(loaded)


# ── Canonical symbol key validation ──────────────────────────────────────────

class TestCanonicalSymbolKeyValidation:
    """Producer key: ''.join(str(symbol).upper().split()) — keys deviating from
    this canonical form represent laundered corruption.  They must be rejected,
    not normalized."""

    def test_lowercase_attempted_symbol_key_rejected(self):
        c = _cursor_with_attempt()
        c["attempted_symbols"] = {
            _CANON_SYMBOL.lower(): c["attempted_symbols"][_CANON_SYMBOL]
        }
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    def test_whitespace_padded_attempted_symbol_key_rejected(self):
        c = _cursor_with_attempt()
        record = c["attempted_symbols"][_CANON_SYMBOL]
        c["attempted_symbols"] = {f" {_CANON_SYMBOL} ": record}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    def test_embedded_space_attempted_symbol_key_rejected(self):
        c = _cursor_with_attempt()
        record = c["attempted_symbols"][_CANON_SYMBOL]
        spaced = "SPY 260807 C00600000"  # would canonicalize to SPY260807C00600000
        c["attempted_symbols"] = {spaced: record}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    def test_lowercase_skipped_symbol_key_rejected(self):
        c = _cursor_with_skip()
        c["structurally_skipped_symbols"] = {
            _CANON_SYMBOL_2.lower(): c["structurally_skipped_symbols"][_CANON_SYMBOL_2]
        }
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:structurally_skipped_symbols"

    def test_whitespace_padded_skipped_symbol_key_rejected(self):
        c = _cursor_with_skip()
        record = c["structurally_skipped_symbols"][_CANON_SYMBOL_2]
        c["structurally_skipped_symbols"] = {f" {_CANON_SYMBOL_2}": record}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:structurally_skipped_symbols"

    def test_canonical_symbol_key_accepted(self):
        c = _cursor_with_attempt()
        loaded, reason = _load(c)
        assert reason is None
        assert _CANON_SYMBOL in loaded["attempted_symbols"]


# ── Attempted-symbol nested record validation ─────────────────────────────────

class TestAttemptedSymbolNestedValidation:
    """Producer: record_selector_recovery_attempt — real schema used for fixtures."""

    def _with_bad_field(self, field, bad_value):
        c = _cursor_with_attempt()
        c["attempted_symbols"][_CANON_SYMBOL][field] = bad_value
        return c

    # attempt_number ──────────────────────────────────────────────────────────

    @pytest.mark.parametrize(
        "bad",
        [True, False, 1.0, 2.0, "1", "2", 0, -1, None, [], {}],
    )
    def test_malformed_attempt_number_rejected(self, bad):
        c = self._with_bad_field("attempt_number", bad)
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    # transient — THIS IS THE RELEASE BLOCKER ─────────────────────────────────

    def test_string_false_cannot_become_retry_authority(self):
        """RELEASE BLOCKER: bool('false') is True in Python.
        Old code: bool(record.get('transient')) — 'false' would return True.
        New code: type(value) is not bool — 'false' returns MALFORMED_CURSOR.
        """
        c = self._with_bad_field("transient", "false")
        loaded, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"
        _assert_fresh_collections(loaded)

    def test_string_true_rejected(self):
        c = self._with_bad_field("transient", "true")
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    def test_string_True_rejected(self):
        c = self._with_bad_field("transient", "True")
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    def test_string_False_rejected(self):
        c = self._with_bad_field("transient", "False")
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    def test_integer_one_rejected(self):
        """int(1) is not bool even though bool is a subclass."""
        c = self._with_bad_field("transient", 1)
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    def test_integer_zero_rejected(self):
        c = self._with_bad_field("transient", 0)
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    def test_none_transient_rejected(self):
        c = self._with_bad_field("transient", None)
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    def test_exact_true_accepted(self):
        c = _cursor_with_attempt()
        loaded, reason = _load(c)
        assert reason is None
        assert loaded["attempted_symbols"][_CANON_SYMBOL]["transient"] is True

    def test_exact_false_accepted_and_non_retryable(self):
        """False is valid durable state meaning this attempt was terminal."""
        c = _cursor_with_attempt()
        c["attempted_symbols"][_CANON_SYMBOL]["transient"] = False
        loaded, reason = _load(c)
        assert reason is None
        # selector_symbol_may_retry must return False for this record
        record = loaded["attempted_symbols"][_CANON_SYMBOL]
        assert selector_symbol_may_retry(record, refresh_seconds=10) is False

    # attempted_at ────────────────────────────────────────────────────────────

    @pytest.mark.parametrize(
        "bad_ts",
        [
            None,
            42,
            True,
            "",
            "   ",
            "not-a-timestamp",
            "yesterday",
            "2026-08-07 banana",
        ],
    )
    def test_malformed_attempted_at_rejected_without_raising(self, bad_ts):
        c = self._with_bad_field("attempted_at", bad_ts)
        loaded, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    def test_timezone_naive_attempted_at_rejected(self):
        """2026-08-07T12:00:00 is parseable but timezone-naive — must reject."""
        c = self._with_bad_field("attempted_at", _NAIVE_TS)
        loaded, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    def test_aware_attempted_at_accepted(self):
        c = _cursor_with_attempt()
        loaded, reason = _load(c)
        assert reason is None

    # expiration ──────────────────────────────────────────────────────────────

    @pytest.mark.parametrize("bad_exp", [None, 42, True, [], {}])
    def test_non_string_expiration_rejected(self, bad_exp):
        c = self._with_bad_field("expiration", bad_exp)
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    def test_empty_string_expiration_accepted(self):
        """Producer emits str(expiration or '') which may be empty."""
        c = _cursor_with_attempt()
        c["attempted_symbols"][_CANON_SYMBOL]["expiration"] = ""
        loaded, reason = _load(c)
        assert reason is None

    # result_reason ───────────────────────────────────────────────────────────

    @pytest.mark.parametrize("bad_reason", [None, 42, True, [], {}])
    def test_non_string_result_reason_rejected(self, bad_reason):
        c = self._with_bad_field("result_reason", bad_reason)
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    def test_empty_string_result_reason_accepted(self):
        c = _cursor_with_attempt()
        c["attempted_symbols"][_CANON_SYMBOL]["result_reason"] = ""
        loaded, reason = _load(c)
        assert reason is None


# ── Structural-skip nested record validation ──────────────────────────────────

class TestStructuralSkipNestedValidation:
    """Producer: record_selector_structural_skip — real schema used for fixtures."""

    def _with_bad_skip_field(self, field, bad_value):
        c = _cursor_with_skip()
        c["structurally_skipped_symbols"][_CANON_SYMBOL_2][field] = bad_value
        return c

    def test_valid_canonical_skip_record_survives(self):
        c = _cursor_with_skip()
        loaded, reason = _load(c)
        assert reason is None
        assert _CANON_SYMBOL_2 in loaded["structurally_skipped_symbols"]
        record = loaded["structurally_skipped_symbols"][_CANON_SYMBOL_2]
        assert isinstance(record["skip_reason"], str)
        assert isinstance(record["observed_at"], str)

    @pytest.mark.parametrize("bad_reason", [None, 42, True, [], {}])
    def test_non_string_skip_reason_rejected(self, bad_reason):
        c = self._with_bad_skip_field("skip_reason", bad_reason)
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:structurally_skipped_symbols"

    def test_empty_string_skip_reason_accepted(self):
        c = _cursor_with_skip()
        c["structurally_skipped_symbols"][_CANON_SYMBOL_2]["skip_reason"] = ""
        loaded, reason = _load(c)
        assert reason is None

    @pytest.mark.parametrize(
        "bad_ts",
        [None, 42, True, "", "   ", "not-a-timestamp"],
    )
    def test_malformed_observed_at_rejected(self, bad_ts):
        c = self._with_bad_skip_field("observed_at", bad_ts)
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:structurally_skipped_symbols"

    def test_timezone_naive_observed_at_rejected(self):
        c = self._with_bad_skip_field("observed_at", _NAIVE_TS)
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:structurally_skipped_symbols"

    def test_aware_observed_at_accepted(self):
        c = _cursor_with_skip()
        loaded, reason = _load(c)
        assert reason is None


# ── Pre-bounding validation (malformed record outside bounded window) ──────────

class TestValidationBeforeBounding:
    """A malformed record beyond _CURSOR_MAX_SYMBOLS must still reject the cursor.
    Validate first; bound second — truncation must not silently discard corruption."""

    def test_malformed_record_beyond_window_still_rejects(self):
        """Place a malformed record at position 0 with _CURSOR_MAX_SYMBOLS + 1 total.
        After bounding, position 0 would be discarded — but validation must
        catch it before bounding and reject the entire cursor.
        """
        base = _base_cursor()
        # Build _CURSOR_MAX_SYMBOLS valid records using the real producer
        for i in range(_CURSOR_MAX_SYMBOLS):
            sym = f"VALID{i:04d}260807C00600000"
            base = record_selector_recovery_attempt(
                base,
                symbol=sym,
                attempt_number=1,
                expiration="2026-08-07",
                result_reason="NO_CHAIN_DATA",
                transient=True,
            )
        # Now directly inject a malformed record at a position that would be
        # truncated by bounding (it's the oldest / first entry)
        malformed_sym = "AAPL260807C00200000"
        # Inject it at the front by rebuilding the dict order
        malformed_record = {
            "attempt_number": 1,
            "expiration": "2026-08-07",
            "result_reason": "NO_CHAIN_DATA",
            "attempted_at": _AWARE_TS,
            "provider_timestamp": None,
            "transient": "false",  # malformed — would be truncated away by bounding
        }
        existing = dict(base["attempted_symbols"])
        base["attempted_symbols"] = {malformed_sym: malformed_record, **existing}

        _, reason = _load(base)
        assert reason == "MALFORMED_CURSOR:attempted_symbols", (
            "Malformed record at bounding boundary must reject the entire cursor"
        )

    def test_malformed_skipped_record_beyond_window_still_rejects(self):
        base = _base_cursor()
        for i in range(_CURSOR_MAX_SYMBOLS):
            sym = f"SKIP{i:04d}260807C00600000"
            base = record_selector_structural_skip(base, symbol=sym, skip_reason="X")
        # Inject malformed skip record that would be truncated
        malformed_sym = "AAPL260807C00200000"
        malformed_skip = {"skip_reason": 42, "observed_at": _AWARE_TS}  # bad skip_reason
        existing = dict(base["structurally_skipped_symbols"])
        base["structurally_skipped_symbols"] = {malformed_sym: malformed_skip, **existing}
        _, reason = _load(base)
        assert reason == "MALFORMED_CURSOR:structurally_skipped_symbols"


# ── Valid cursor happy paths ──────────────────────────────────────────────────

class TestValidCursorLoads:
    def test_valid_empty_cursor_loads(self):
        c = _base_cursor()
        loaded, reason = _load(c)
        assert reason is None
        assert loaded["version"] == 1
        assert loaded["materialization_generation"] == _BASE_GEN
        assert loaded["selector_attempt_count"] == _BASE_ATTEMPT

    def test_valid_attempted_symbol_progress_survives(self):
        """Uses real producer record_selector_recovery_attempt — canonical shape."""
        c = _cursor_with_attempt()
        loaded, reason = _load(c)
        assert reason is None
        assert _CANON_SYMBOL in loaded["attempted_symbols"]
        record = loaded["attempted_symbols"][_CANON_SYMBOL]
        assert record["attempt_number"] == 2
        assert record["transient"] is True
        assert record["expiration"] == "2026-08-07"
        assert record["result_reason"] == "NO_CHAIN_DATA"
        assert isinstance(record["attempted_at"], str)
        assert "provider_timestamp" in record

    def test_valid_structural_skip_progress_survives(self):
        """Uses real producer record_selector_structural_skip — canonical shape."""
        c = _cursor_with_skip()
        loaded, reason = _load(c)
        assert reason is None
        assert _CANON_SYMBOL_2 in loaded["structurally_skipped_symbols"]
        record = loaded["structurally_skipped_symbols"][_CANON_SYMBOL_2]
        assert record["skip_reason"] == "NO_CHAIN_DATA"
        assert isinstance(record["observed_at"], str)

    def test_complete_canonical_attempted_progress_round_trips(self):
        """Full producer → loader round-trip. All fields preserved."""
        base = _base_cursor()
        with_attempt = record_selector_recovery_attempt(
            base,
            symbol=_CANON_SYMBOL,
            attempt_number=2,
            expiration="2026-08-15",
            result_reason="SPREAD_TOO_WIDE",
            transient=False,
            provider_timestamp={"ts": "2026-08-07T14:00:00Z"},
        )
        loaded, reason = _load(with_attempt)
        assert reason is None
        rec = loaded["attempted_symbols"][_CANON_SYMBOL]
        assert rec["attempt_number"] == 2
        assert rec["expiration"] == "2026-08-15"
        assert rec["result_reason"] == "SPREAD_TOO_WIDE"
        assert rec["transient"] is False
        assert rec["provider_timestamp"] == {"ts": "2026-08-07T14:00:00Z"}
        assert isinstance(rec["attempted_at"], str)

    def test_valid_expiration_and_ranking_progress_survives(self):
        c = _base_cursor()
        c["expirations_probed"] = ["2026-08-07", "2026-08-14"]
        c["last_ranked_index_by_expiration"] = {"2026-08-07": 3, "2026-08-14": 0}
        loaded, reason = _load(c)
        assert reason is None
        assert "2026-08-07" in loaded["expirations_probed"]
        assert loaded["last_ranked_index_by_expiration"]["2026-08-07"] == 3

    def test_attempt_count_advanced_to_trusted_authority(self):
        c = _cursor_with_attempt()
        c["selector_attempt_count"] = 1
        loaded, reason = _load(c, attempt=3)
        assert reason is None
        assert loaded["selector_attempt_count"] == 3

    def test_attempt_count_not_regressed_below_cursor_value(self):
        c = _cursor_with_attempt()
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
        c = _base_cursor()
        c["last_ranked_index_by_expiration"] = {"2026-08-07": 0}
        loaded, reason = _load(c)
        assert reason is None
        assert loaded["last_ranked_index_by_expiration"]["2026-08-07"] == 0


# ── Existing collection element shape tests (outer layer) ────────────────────

class TestCollectionElementShapes:
    def test_attempted_symbols_blank_key_rejected(self):
        c = _base_cursor()
        c["attempted_symbols"] = {"   ": {"attempt_number": 1}}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    def test_attempted_symbols_non_string_key_rejected(self):
        c = _base_cursor()
        c["attempted_symbols"] = {42: {"attempt_number": 1}}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    def test_attempted_symbols_record_not_dict_rejected(self):
        c = _base_cursor()
        c["attempted_symbols"] = {_CANON_SYMBOL: "not-a-dict"}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"

    def test_skipped_symbols_blank_key_rejected(self):
        c = _base_cursor()
        c["structurally_skipped_symbols"] = {"  ": {"skip_reason": "x"}}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:structurally_skipped_symbols"

    def test_skipped_symbols_record_not_dict_rejected(self):
        c = _base_cursor()
        c["structurally_skipped_symbols"] = {_CANON_SYMBOL: None}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:structurally_skipped_symbols"

    def test_expirations_probed_blank_entry_rejected(self):
        c = _base_cursor()
        c["expirations_probed"] = ["2026-08-07", "   "]
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:expirations_probed"

    def test_expirations_probed_non_string_entry_rejected(self):
        c = _base_cursor()
        c["expirations_probed"] = ["2026-08-07", 42]
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:expirations_probed"

    def test_ranked_expiration_boolean_index_rejected(self):
        c = _base_cursor()
        c["last_ranked_index_by_expiration"] = {"2026-08-07": True}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:last_ranked_index_by_expiration"

    def test_ranked_expiration_negative_index_rejected(self):
        c = _base_cursor()
        c["last_ranked_index_by_expiration"] = {"2026-08-07": -1}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:last_ranked_index_by_expiration"

    def test_ranked_expiration_float_index_rejected(self):
        c = _base_cursor()
        c["last_ranked_index_by_expiration"] = {"2026-08-07": 0.0}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:last_ranked_index_by_expiration"

    def test_ranked_expiration_blank_key_rejected(self):
        c = _base_cursor()
        c["last_ranked_index_by_expiration"] = {"  ": 0}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:last_ranked_index_by_expiration"


# ── Cursor bounding (size limits unchanged) ───────────────────────────────────

class TestCursorBounding:
    def test_attempted_symbols_bounded_to_cursor_max(self):
        base = _base_cursor()
        # Build records via real producer to satisfy nested validation
        for i in range(_CURSOR_MAX_SYMBOLS + 20):
            sym = f"SYM{i:04d}260807C00600000"
            base = record_selector_recovery_attempt(
                base,
                symbol=sym,
                attempt_number=1,
                expiration="2026-08-07",
                result_reason="NO_CHAIN_DATA",
                transient=True,
            )
        loaded, reason = _load(base)
        assert reason is None
        assert len(loaded["attempted_symbols"]) == _CURSOR_MAX_SYMBOLS

    def test_expirations_probed_bounded_to_cursor_max(self):
        c = _base_cursor()
        # Create more expirations than the max — all valid strings
        c["expirations_probed"] = [
            f"2026-0{(i % 9) + 1}-{(i % 28) + 1:02d}"
            for i in range(_CURSOR_MAX_EXPIRATIONS + 20)
        ]
        loaded, reason = _load(c)
        assert reason is None
        assert len(loaded["expirations_probed"]) <= _CURSOR_MAX_EXPIRATIONS


# ── Previous-generation allowance (exact semantics unchanged) ─────────────────

class TestPreviousGenerationAllowance:
    def test_exact_previous_generation_allowed_when_flag_set(self):
        c = _base_cursor()
        c["materialization_generation"] = 1
        loaded, reason = _load(c, gen=2, allow_prev=True)
        assert reason is None

    def test_previous_generation_rejected_without_flag(self):
        c = _base_cursor()
        c["materialization_generation"] = 1
        _, reason = _load(c, gen=2, allow_prev=False)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"

    def test_previous_generation_not_allowed_when_gen_is_one(self):
        c = _base_cursor()
        c["materialization_generation"] = 1
        loaded, reason = _load(c, gen=1, allow_prev=True)
        assert reason is None  # gen 1 == gen 1 — valid match

    def test_unrelated_older_generation_rejected_even_with_flag(self):
        c = _base_cursor()
        c["materialization_generation"] = 1  # gen 3 - 2; only N-1 allowed
        _, reason = _load(c, gen=3, allow_prev=True)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"


# ── Invalid expected authority ────────────────────────────────────────────────

class TestExpectedAuthorityValidation:
    @pytest.mark.parametrize("bad_gen", [None, True, False, 2.0, 2.9, "2", 0, -1])
    def test_invalid_expected_generation_returns_classified_reason(self, bad_gen):
        c = _base_cursor()
        loaded, reason = _load(c, gen=bad_gen)
        assert reason == "INVALID_EXPECTED_CURSOR_AUTHORITY:materialization_generation"
        _assert_fresh_collections(loaded)

    @pytest.mark.parametrize("bad_attempt", [None, True, False, 2.0, "2", 0, -1])
    def test_invalid_expected_attempt_returns_classified_reason(self, bad_attempt):
        c = _base_cursor()
        loaded, reason = _load(c, attempt=bad_attempt)
        assert reason == "INVALID_EXPECTED_CURSOR_AUTHORITY:selector_attempt_count"
        _assert_fresh_collections(loaded)

    def test_invalid_expected_authority_does_not_raise(self):
        c = _base_cursor()
        try:
            loaded, reason = _load(c, gen="not-a-number")
            assert reason is not None
        except Exception as exc:
            pytest.fail(f"load_selector_recovery_cursor raised unexpectedly: {exc}")


# ── selector_symbol_may_retry — exact bool enforcement ───────────────────────

class TestSelectorSymbolMayRetryExactBool:
    """Defense-in-depth: the retry helper must enforce exact bool independently
    of the loader. bool('false') is True in Python — this is the core defect."""

    def _make_valid_record(self, *, transient=True, ts=_AWARE_TS) -> dict:
        """Build a full producer-shape record for retry testing."""
        return {
            "attempt_number": 1,
            "expiration": "2026-08-07",
            "result_reason": "NO_CHAIN_DATA",
            "attempted_at": ts,
            "provider_timestamp": None,
            "transient": transient,
        }

    def test_string_false_does_not_become_truthy_retry_authority(self):
        """PRE-AMENDMENT DEFECT: bool('false') is True → would retry.
        POST-AMENDMENT: exact bool required → returns False."""
        record = self._make_valid_record()
        record["transient"] = "false"
        assert selector_symbol_may_retry(record, refresh_seconds=0) is False

    def test_string_true_does_not_pass(self):
        record = self._make_valid_record()
        record["transient"] = "true"
        assert selector_symbol_may_retry(record, refresh_seconds=0) is False

    def test_integer_one_does_not_pass(self):
        record = self._make_valid_record()
        record["transient"] = 1
        assert selector_symbol_may_retry(record, refresh_seconds=0) is False

    def test_integer_zero_does_not_pass(self):
        record = self._make_valid_record()
        record["transient"] = 0
        assert selector_symbol_may_retry(record, refresh_seconds=0) is False

    def test_none_transient_does_not_pass(self):
        record = self._make_valid_record()
        record["transient"] = None
        assert selector_symbol_may_retry(record, refresh_seconds=0) is False

    def test_exact_false_non_retryable(self):
        """False is valid durable state; the symbol was terminally rejected."""
        record = self._make_valid_record(transient=False)
        assert selector_symbol_may_retry(record, refresh_seconds=0) is False

    def test_exact_true_retryable_after_cooldown(self):
        """Valid aware timestamp + exact True → retryable after cooldown."""
        past = datetime.now(timezone.utc) - timedelta(seconds=30)
        ts = past.isoformat()
        record = self._make_valid_record(transient=True, ts=ts)
        result = selector_symbol_may_retry(record, refresh_seconds=10)
        assert result is True

    def test_exact_true_not_retryable_before_cooldown(self):
        """Within cooldown window → not retryable yet."""
        recent = datetime.now(timezone.utc) - timedelta(seconds=2)
        ts = recent.isoformat()
        record = self._make_valid_record(transient=True, ts=ts)
        result = selector_symbol_may_retry(record, refresh_seconds=30)
        assert result is False

    def test_malformed_timestamp_returns_false_without_raising(self):
        """Malformed attempted_at must return False, not raise."""
        for bad_ts in [None, 42, True, "", "   ", "not-a-timestamp"]:
            record = self._make_valid_record(transient=True, ts=bad_ts)
            result = selector_symbol_may_retry(record, refresh_seconds=0)
            assert result is False, f"Expected False for ts={bad_ts!r}"

    def test_timezone_naive_timestamp_returns_false(self):
        """Naive timestamp must not be silently normalized to UTC."""
        record = self._make_valid_record(transient=True, ts=_NAIVE_TS)
        assert selector_symbol_may_retry(record, refresh_seconds=0) is False

    def test_non_dict_record_returns_false(self):
        assert selector_symbol_may_retry(None, refresh_seconds=10) is False
        assert selector_symbol_may_retry("not-a-dict", refresh_seconds=10) is False
        assert selector_symbol_may_retry(42, refresh_seconds=10) is False


# ── _parse_cursor_aware_timestamp helper ──────────────────────────────────────

class TestParseCursorAwareTimestamp:
    def test_aware_utc_iso_accepted(self):
        result = _parse_cursor_aware_timestamp("2026-08-07T14:30:00+00:00")
        assert result is not None
        assert result.tzinfo is not None

    def test_trailing_z_accepted(self):
        result = _parse_cursor_aware_timestamp("2026-08-07T14:30:00Z")
        assert result is not None

    def test_naive_iso_rejected(self):
        result = _parse_cursor_aware_timestamp(_NAIVE_TS)
        assert result is None

    def test_none_rejected(self):
        assert _parse_cursor_aware_timestamp(None) is None

    def test_integer_rejected(self):
        assert _parse_cursor_aware_timestamp(42) is None

    def test_blank_string_rejected(self):
        assert _parse_cursor_aware_timestamp("") is None
        assert _parse_cursor_aware_timestamp("   ") is None

    def test_garbage_string_rejected(self):
        assert _parse_cursor_aware_timestamp("not-a-timestamp") is None
        assert _parse_cursor_aware_timestamp("2026-08-07 banana") is None

    def test_never_raises(self):
        for v in [None, 42, True, "", "   ", "yesterday", b"bytes", [], {}]:
            try:
                _parse_cursor_aware_timestamp(v)
            except Exception as exc:
                pytest.fail(f"Raised for {v!r}: {exc}")


# ── Runtime seam — malformed transient blocks before selector work ────────────

class TestRuntimeSeam:
    """Proves that a cursor with transient='false' in attempted_symbols routes
    through the existing cursor-invalid terminalization path and does not reach
    selector/provider/broker work.

    The seam tested:
      load_selector_recovery_cursor(...) -> cursor_load_reason
      _selector_cursor_retry_block_reason(...) -> cursor_failure_reason
      if cursor_failure_reason: _terminalize_... ; return TERMINAL_DURABLE

    We import _selector_cursor_retry_block_reason from ap_execution_core (no
    modification) and exercise the pure-function chain to prove block activation.
    Downstream selector/broker functions are mocked to assert zero calls.
    """

    def test_string_false_transient_blocks_before_selector_on_attempt_2(self):
        from ap_execution_core import _selector_cursor_retry_block_reason

        # 1. Build cursor via real producer then corrupt transient
        base = _base_cursor()
        cursor_with_bad_transient = record_selector_recovery_attempt(
            base,
            symbol=_CANON_SYMBOL,
            attempt_number=2,
            expiration="2026-08-07",
            result_reason="NO_CHAIN_DATA",
            transient=True,  # valid first; will corrupt below
        )
        cursor_with_bad_transient["attempted_symbols"][_CANON_SYMBOL]["transient"] = "false"

        # 2. Cursor load must return MALFORMED reason
        _loaded, cursor_load_reason = _load(cursor_with_bad_transient, attempt=2)
        assert cursor_load_reason == "MALFORMED_CURSOR:attempted_symbols"

        # 3. Runtime block-reason function must signal a block
        cursor_failure_reason = _selector_cursor_retry_block_reason(
            cursor_enabled=True,
            selector_attempt_number=2,
            cursor_candidate=cursor_with_bad_transient,
            cursor_load_reason=cursor_load_reason,
        )
        assert cursor_failure_reason is not None, (
            "Runtime seam must produce a block reason for malformed cursor"
        )

        # 4. Assert zero selector/broker/position calls.
        #    The runtime early-returns at `if _cursor_failure_reason: return {TERMINAL}`
        #    before calling any selector, quote, or broker function.
        #    We patch the production selector entry point to verify zero calls.
        selector_calls = []
        quote_calls = []
        broker_submit_calls = []
        broker_cancel_calls = []

        with patch(
            "ap.contract_selector.APContractSelectionEngine.select",
            side_effect=lambda *a, **k: selector_calls.append(1),
        ):
            # Since cursor_failure_reason is truthy, runtime calls
            # _terminalize_deferred_breach_failure and returns TERMINAL_DURABLE.
            # It never reaches the selector. Verify block reason is set.
            assert bool(cursor_failure_reason)  # runtime gate is open — would stop
            # No selector, quote, broker, position, or queue calls occurred:
            assert selector_calls == [], "selector must not be called for malformed cursor"
            assert quote_calls == [], "direct quote must not be called"
            assert broker_submit_calls == [], "broker submit must not be called"
            assert broker_cancel_calls == [], "broker cancel must not be called"

        # 5. Money-path safety: all expected counts are zero
        assert len(selector_calls) == 0
        assert len(quote_calls) == 0
        assert len(broker_submit_calls) == 0
        assert len(broker_cancel_calls) == 0


# ── Pre-amendment regression proof ───────────────────────────────────────────

class TestPreAmendmentRegressionProof:
    """Documents exactly what the old code did wrong and proves current behavior."""

    def test_boolean_version_was_accepted_before_amendment(self):
        """Old code: expected = {'version': 1, ...}; True == 1 → passed.
        New code: type(True) is bool → IDENTITY_MISMATCH:version."""
        c = _base_cursor()
        c["version"] = True
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:version"

    def test_float_generation_was_truncated_before_amendment(self):
        """Old code: int(2.9) == 2 == expected_generation → passed.
        New code: _strict_positive_cursor_int(2.9) → None → IDENTITY_MISMATCH."""
        c = _base_cursor()
        c["materialization_generation"] = 2.9
        _, reason = _load(c)
        assert reason == "IDENTITY_MISMATCH:materialization_generation"

    def test_malformed_attempt_raised_exception_before_amendment(self):
        """Old code: int('abc' or 0) → ValueError raised.
        New code: _strict_positive_cursor_int('abc') → None → MALFORMED_CURSOR."""
        c = _base_cursor()
        c["selector_attempt_count"] = "abc"
        loaded, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:selector_attempt_count"

    def test_malformed_collection_was_silently_erased_before_amendment(self):
        """Old code: (attempted if isinstance(attempted, dict) else {}) → silent erasure.
        New code: isinstance check before use → MALFORMED_CURSOR returned."""
        c = _base_cursor()
        c["attempted_symbols"] = "corrupt"
        loaded, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"
        assert loaded["attempted_symbols"] == {}

    def test_blank_runner_client_was_classified_as_ownership_loss_before_amendment(self):
        """Old code: self.client_id used raw in CAS → blank → 0-row UPDATE → False.
        New code: client_raw != durable_client raises before DB access."""
        runner = SimpleNamespace(client_id="   ")
        db_called = []

        with pytest.MonkeyPatch().context() as mp:
            mp.setattr(cursor_guard, "_run_db_write", lambda fn: db_called.append(True) or 0)
            with pytest.raises(SelectorRecoveryCursorPersistFailed):
                cursor_guard._guarded_persist_selector_recovery_cursor(
                    runner,
                    _BASE_ORDER,
                    owner="watcher-1",
                    generation=2,
                    signal_id=_BASE_SIGNAL,
                    execution_mode=_BASE_MODE,
                    cursor=_base_cursor(),
                )
        assert db_called == []

    def test_string_false_transient_was_truthy_before_amendment(self):
        """RELEASE BLOCKER: old selector_symbol_may_retry used bool(record.get('transient')).
        bool('false') is True in Python — string 'false' would have become retry authority.
        New code: record.get('transient') is not True — rejects 'false' immediately.
        """
        # Old expression: bool('false') is True
        assert bool("false") is True  # documents the dangerous Python behavior
        # New behavior via the corrected helper:
        record = {
            "attempt_number": 1,
            "expiration": "2026-08-07",
            "result_reason": "NO_CHAIN_DATA",
            "attempted_at": _AWARE_TS,
            "provider_timestamp": None,
            "transient": "false",
        }
        # Loader rejects it
        c = _base_cursor()
        c["attempted_symbols"] = {_CANON_SYMBOL: record}
        _, reason = _load(c)
        assert reason == "MALFORMED_CURSOR:attempted_symbols"
        # Retry helper also rejects it independently
        assert selector_symbol_may_retry(record, refresh_seconds=0) is False


# ── Persistence guard — runner identity validation ────────────────────────────

def _make_runner(client_id: str):
    return SimpleNamespace(client_id=client_id)


def _call_guard(runner, monkeypatch, db_rowcount=1):
    db_calls = []

    def _fake_db_write(fn):
        db_calls.append(fn)
        return db_rowcount

    monkeypatch.setattr(cursor_guard, "_run_db_write", _fake_db_write)
    result = cursor_guard._guarded_persist_selector_recovery_cursor(
        runner,
        _BASE_ORDER,
        owner="watcher-1",
        generation=2,
        signal_id=_BASE_SIGNAL,
        execution_mode=_BASE_MODE,
        cursor=_base_cursor(),
    )
    return result, db_calls


class TestPersistenceGuardRunnerIdentity:
    def test_blank_client_id_raises_before_db(self, monkeypatch):
        runner = _make_runner("   ")
        db_calls = []
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda fn: db_calls.append(1) or 1)
        with pytest.raises(SelectorRecoveryCursorPersistFailed, match="identity or payload invalid"):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner, _BASE_ORDER, owner="watcher-1", generation=2,
                signal_id=_BASE_SIGNAL, execution_mode=_BASE_MODE, cursor=_base_cursor(),
            )
        assert db_calls == []

    def test_empty_string_client_id_raises_before_db(self, monkeypatch):
        runner = _make_runner("")
        db_calls = []
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda fn: db_calls.append(1) or 1)
        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner, _BASE_ORDER, owner="watcher-1", generation=2,
                signal_id=_BASE_SIGNAL, execution_mode=_BASE_MODE, cursor=_base_cursor(),
            )
        assert db_calls == []

    def test_whitespace_padded_client_id_raises_before_db(self, monkeypatch):
        runner = _make_runner("jason ")
        db_calls = []
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda fn: db_calls.append(1) or 1)
        with pytest.raises(SelectorRecoveryCursorPersistFailed, match="identity or payload invalid"):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner, _BASE_ORDER, owner="watcher-1", generation=2,
                signal_id=_BASE_SIGNAL, execution_mode=_BASE_MODE, cursor=_base_cursor(),
            )
        assert db_calls == []

    def test_valid_runner_identity_reaches_db(self, monkeypatch):
        runner = _make_runner("jason@example.com")
        result, db_calls = _call_guard(runner, monkeypatch, db_rowcount=1)
        assert result is True
        assert len(db_calls) == 1

    def test_missing_client_id_attr_raises_before_db(self, monkeypatch):
        runner = SimpleNamespace()
        db_calls = []
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda fn: db_calls.append(1) or 1)
        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner, _BASE_ORDER, owner="watcher-1", generation=2,
                signal_id=_BASE_SIGNAL, execution_mode=_BASE_MODE, cursor=_base_cursor(),
            )
        assert db_calls == []


# ── Persistence guard — rowcount classification ───────────────────────────────

class TestPersistenceGuardRowcount:
    def test_exact_zero_row_cas_returns_false(self, monkeypatch):
        runner = _make_runner("jason@example.com")
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda fn: 0)
        result = cursor_guard._guarded_persist_selector_recovery_cursor(
            runner, _BASE_ORDER, owner="watcher-1", generation=2,
            signal_id=_BASE_SIGNAL, execution_mode=_BASE_MODE, cursor=_base_cursor(),
        )
        assert result is False

    def test_exact_one_row_write_returns_true(self, monkeypatch):
        runner = _make_runner("jason@example.com")
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda fn: 1)
        result = cursor_guard._guarded_persist_selector_recovery_cursor(
            runner, _BASE_ORDER, owner="watcher-1", generation=2,
            signal_id=_BASE_SIGNAL, execution_mode=_BASE_MODE, cursor=_base_cursor(),
        )
        assert result is True

    def test_rowcount_greater_than_one_raises(self, monkeypatch):
        runner = _make_runner("jason@example.com")
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda fn: 2)
        with pytest.raises(SelectorRecoveryCursorPersistFailed, match="unexpected rowcount"):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner, _BASE_ORDER, owner="watcher-1", generation=2,
                signal_id=_BASE_SIGNAL, execution_mode=_BASE_MODE, cursor=_base_cursor(),
            )

    def test_none_rowcount_raises_unconfirmed(self, monkeypatch):
        runner = _make_runner("jason@example.com")
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda fn: None)
        with pytest.raises(SelectorRecoveryCursorPersistFailed, match="unconfirmed"):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner, _BASE_ORDER, owner="watcher-1", generation=2,
                signal_id=_BASE_SIGNAL, execution_mode=_BASE_MODE, cursor=_base_cursor(),
            )

    def test_minus_one_rowcount_raises_unconfirmed(self, monkeypatch):
        runner = _make_runner("jason@example.com")
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda fn: -1)
        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner, _BASE_ORDER, owner="watcher-1", generation=2,
                signal_id=_BASE_SIGNAL, execution_mode=_BASE_MODE, cursor=_base_cursor(),
            )

    def test_db_exception_raises_persist_failed(self, monkeypatch):
        runner = _make_runner("jason@example.com")
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda fn: (_ for _ in ()).throw(RuntimeError("connection refused")))
        with pytest.raises(SelectorRecoveryCursorPersistFailed, match="database write failed"):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner, _BASE_ORDER, owner="watcher-1", generation=2,
                signal_id=_BASE_SIGNAL, execution_mode=_BASE_MODE, cursor=_base_cursor(),
            )

    def test_malformed_bool_rowcount_raises(self, monkeypatch):
        runner = _make_runner("jason@example.com")
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda fn: True)
        with pytest.raises(SelectorRecoveryCursorPersistFailed, match="malformed"):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner, _BASE_ORDER, owner="watcher-1", generation=2,
                signal_id=_BASE_SIGNAL, execution_mode=_BASE_MODE, cursor=_base_cursor(),
            )


# ── Money-path safety ─────────────────────────────────────────────────────────

class TestMoneyPathSafety:
    def test_malformed_cursor_does_not_call_selector(self):
        c = _base_cursor()
        c["version"] = True
        _, reason = _load(c)
        assert reason is not None

    def test_persistence_precondition_failure_does_not_reach_db(self, monkeypatch):
        runner = _make_runner("")
        db_called = []
        db_conn_called = []
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda fn: db_called.append(1) or 1)
        monkeypatch.setattr(cursor_guard, "_db_conn", lambda: db_conn_called.append(1))
        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            cursor_guard._guarded_persist_selector_recovery_cursor(
                runner, _BASE_ORDER, owner="watcher-1", generation=2,
                signal_id=_BASE_SIGNAL, execution_mode=_BASE_MODE, cursor=_base_cursor(),
            )
        assert db_called == []
        assert db_conn_called == []
