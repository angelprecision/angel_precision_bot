"""Unit tests for ap/deferred_breach_retry.py.

Covers the classification of retryable vs terminal reason codes, the max
attempts config, and the sleep schedule. Zero I/O — pure policy tests.
"""
import pytest

from ap import deferred_breach_retry as dbr


# ─────────────────────────────────────────────────────────────────────────────
# Classification
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("code", [
    "CHAIN_ROW_ZERO_BID_ASK",
    "DIRECT_QUOTE_ZERO_BID_ASK",
    "QUOTE_FETCH_FAILED",
    "CHAIN_EMPTY",
    "CHAIN_FETCH_FAILED",
    "NO_VALID_PLAYBOOK_DTE_CONTRACT",
])
def test_retryable_transient_codes(code):
    assert dbr.is_retryable(code) is True
    assert dbr.is_terminal(code) is False


@pytest.mark.parametrize("code", [
    "UNTRADEABLE_FOR_ACCOUNT_SIZE",
    "OI_TOO_LOW",
    "VOLUME_TOO_LOW",
    "BID_BELOW_MIN",
    "SPREAD_TOO_WIDE",
    "DELTA_OUT_OF_RANGE",
    "PREMIUM_CAP_EXCEEDED",
    "EARNINGS_LOCKOUT",
    "EARNINGS_GUARD_ERROR",
    "INVALID_PLAN",
    "UNSUPPORTED_INDEX_MAPPING",
    "NO_CHAIN_DATA",
    "DEFERRED_UNRESOLVED_AT_BREACH",
])
def test_terminal_structural_codes(code):
    assert dbr.is_terminal(code) is True
    assert dbr.is_retryable(code) is False


@pytest.mark.parametrize("bad", [None, "", "   ", "UNKNOWN_MAGIC_CODE", "some_lower_case", 0, 42])
def test_unknown_or_missing_is_neither(bad):
    # Unknown / missing codes are NEVER retried (safer to fail visibly than
    # loop on a code we can't reason about). They are also not classified as
    # explicit terminal — the caller can distinguish "known bad" from "?".
    assert dbr.is_retryable(bad) is False
    assert dbr.is_terminal(bad) is False


def test_retryable_and_terminal_sets_are_disjoint():
    """Guardrail: no code may be both retryable and terminal — that would make
    is_retryable/is_terminal ambiguous and the retry loop would misbehave."""
    overlap = dbr.RETRYABLE_REASON_CODES & dbr.TERMINAL_REASON_CODES
    assert overlap == set(), f"reason codes appear in both sets: {overlap}"


# ─────────────────────────────────────────────────────────────────────────────
# max_attempts
# ─────────────────────────────────────────────────────────────────────────────

def test_max_attempts_default_is_three():
    assert dbr.max_attempts({}) == 3


@pytest.mark.parametrize("raw,expected", [
    ("1", 1),
    ("2", 2),
    ("3", 3),
    ("5", 5),
    ("0", 1),      # clamped to floor
    ("-3", 1),     # clamped to floor
    ("99", 5),     # clamped to ceiling
    ("not_a_number", 3),  # falls back to default
    ("", 3),
])
def test_max_attempts_respects_env_with_clamps(raw, expected):
    assert dbr.max_attempts({"DEFERRED_BREACH_MAX_ATTEMPTS": raw}) == expected


# ─────────────────────────────────────────────────────────────────────────────
# sleep_seconds_for_attempt
# ─────────────────────────────────────────────────────────────────────────────

def _env(initial="5", step="5", cap="15"):
    return {
        "DEFERRED_BREACH_RETRY_INITIAL_SLEEP_SEC": initial,
        "DEFERRED_BREACH_RETRY_STEP_SLEEP_SEC":    step,
        "DEFERRED_BREACH_RETRY_MAX_SLEEP_SEC":     cap,
    }


def test_first_attempt_never_sleeps():
    assert dbr.sleep_seconds_for_attempt(1) == 0.0
    assert dbr.sleep_seconds_for_attempt(1, _env()) == 0.0


def test_default_sleep_schedule_is_5_10_15():
    env = _env(initial="5", step="5", cap="15")
    assert dbr.sleep_seconds_for_attempt(2, env) == 5.0
    assert dbr.sleep_seconds_for_attempt(3, env) == 10.0
    assert dbr.sleep_seconds_for_attempt(4, env) == 15.0
    assert dbr.sleep_seconds_for_attempt(5, env) == 15.0  # cap holds


def test_sleep_is_capped_by_max_sleep_sec():
    env = _env(initial="10", step="10", cap="15")
    assert dbr.sleep_seconds_for_attempt(2, env) == 10.0
    assert dbr.sleep_seconds_for_attempt(3, env) == 15.0
    assert dbr.sleep_seconds_for_attempt(4, env) == 15.0


def test_sleep_never_negative_from_misconfig():
    env = _env(initial="-100", step="-50", cap="-1")
    # All negatives are floored to 0; cap of 0 pins the result to 0.
    for a in range(1, 6):
        assert dbr.sleep_seconds_for_attempt(a, env) == 0.0


def test_sleep_handles_non_numeric_env_gracefully():
    env = _env(initial="oops", step="???", cap="???")
    # Falls back to defaults 5 / 5 / 15.
    assert dbr.sleep_seconds_for_attempt(2, env) == 5.0
    assert dbr.sleep_seconds_for_attempt(3, env) == 10.0
    assert dbr.sleep_seconds_for_attempt(5, env) == 15.0


# ─────────────────────────────────────────────────────────────────────────────
# Contract stability — freeze frozen sets so silent policy drift trips CI
# ─────────────────────────────────────────────────────────────────────────────

def test_retryable_set_is_a_frozenset():
    assert isinstance(dbr.RETRYABLE_REASON_CODES, frozenset)


def test_terminal_set_is_a_frozenset():
    assert isinstance(dbr.TERMINAL_REASON_CODES, frozenset)


def test_retryable_reason_codes_snapshot():
    """Snapshot lock so any additions/removals are intentional and reviewed."""
    assert dbr.RETRYABLE_REASON_CODES == frozenset({
        "CHAIN_ROW_ZERO_BID_ASK",
        "DIRECT_QUOTE_ZERO_BID_ASK",
        "QUOTE_FETCH_FAILED",
        "CHAIN_EMPTY",
        "CHAIN_FETCH_FAILED",
        "NO_VALID_PLAYBOOK_DTE_CONTRACT",
    })


def test_terminal_reason_codes_snapshot():
    """Snapshot lock so any additions/removals are intentional and reviewed."""
    assert dbr.TERMINAL_REASON_CODES == frozenset({
        "UNTRADEABLE_FOR_ACCOUNT_SIZE",
        "OI_TOO_LOW",
        "VOLUME_TOO_LOW",
        "BID_BELOW_MIN",
        "SPREAD_TOO_WIDE",
        "DELTA_OUT_OF_RANGE",
        "PREMIUM_CAP_EXCEEDED",
        "EARNINGS_LOCKOUT",
        "EARNINGS_GUARD_ERROR",
        "INVALID_PLAN",
        "UNSUPPORTED_INDEX_MAPPING",
        "NO_CHAIN_DATA",
        "DEFERRED_UNRESOLVED_AT_BREACH",
    })
