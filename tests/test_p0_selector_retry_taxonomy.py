"""P0 tests for Seam 3: canonical selector/materialization retry taxonomy.

Proves that:
  1. Every reason code emitted or mapped in ap/contract_selector.py has
     exactly one classification in ap/selector_retry_policy.py.
  2. Unknown reasons fail closed — they are NOT retryable.
  3. is_retryable_selector_reason is consistent with RETRYABLE_BREACH_SELECTOR_REASONS.
  4. RETRYABLE_MATERIALIZATION_REASONS is identical to RETRYABLE_BREACH_SELECTOR_REASONS
     (deduplication complete — no silent divergence).
  5. Every classification is one of the six defined constants.
  6. Policy fields are logically consistent (e.g. terminal reasons don't
     allow retry delay without max_attempts).
"""

from __future__ import annotations

import pytest

import ap.selector_retry_policy as policy_mod
from ap.selector_retry_policy import (
    get_policy,
    classify_selector_reason,
    is_retryable_selector_reason,
    RETRYABLE_BREACH_SELECTOR_REASONS,
    RETRYABLE_MATERIALIZATION_REASONS,
    RETRYABLE_DATA,
    TERMINAL_QUALITY,
    TERMINAL_POLICY,
    TERMINAL_AUTH,
    TERMINAL_INVARIANT,
    UNKNOWN_FAIL_CLOSED,
    _POLICY_TABLE,
    _FALLBACK_UNKNOWN,
)
from tests.test_p0_selector_production_metadata_integrity import (
    FakeSelector,
    _option,
    _plan_dict,
)

VALID_CLASSIFICATIONS = {
    RETRYABLE_DATA, TERMINAL_QUALITY, TERMINAL_POLICY,
    TERMINAL_AUTH, TERMINAL_INVARIANT, UNKNOWN_FAIL_CLOSED,
}

# ── All reason codes emitted directly or via the selector's canonical-code table
# These are the codes that ap/contract_selector.py can surface to execution_core
# or deferred_materializer.  Each must have a classification in the policy table.
SELECTOR_EMITTED_CODES = {
    # Direct reason_code fields in selector
    "CAPITAL_NO_REMAINING",
    "DTE_LADDER_ERROR",
    "INVALID_POSITION_BUDGET",
    "NO_VALID_PLAYBOOK_DTE_CONTRACT",
    "UNTRADEABLE_FOR_ACCOUNT_SIZE",
    # Normalised canonical codes the selector builds its result from
    "BID_BELOW_MIN",
    "CHAIN_AUTH_ERROR",
    "CHAIN_EMPTY",
    "CHAIN_FETCH_FAILED",
    "CHAIN_PARSE_EMPTY",
    "CHAIN_PROVIDER_EMPTY_EXPIRATIONS",
    "CHAIN_PROVIDER_EMPTY_OPTIONS",
    "CHAIN_PROVIDER_ERROR",
    "CHAIN_ROW_ZERO_BID_ASK",
    "CHEAP_CONTRACT_NO_UPGRADE",
    "CHEAP_CONTRACT_ONLY_CHOICE",
    "DELTA_OUT_OF_RANGE",
    "DIRECT_QUOTE_UNAVAILABLE",
    "DIRECT_QUOTE_ZERO_BID_ASK",
    "DTE_OUT_OF_RANGE",
    "EARNINGS_LOCKOUT",
    "FINAL_CONTRACT_UNAFFORDABLE",
    "FINAL_SPREAD_TOO_WIDE",
    "MARKET_DATA_THROTTLE_UNAVAILABLE",
    "NO_AFFORDABLE_CONTRACT",
    "NO_CHAIN_DATA",
    "NO_CONTRACT_AFTER_FILTERS",
    "NO_EXPIRATION_IN_DTE_WINDOW",
    "OI_TOO_LOW",
    "PREMIUM_CAP_EXCEEDED",
    "PROVIDER_RATE_LIMITED",
    "PROVIDER_TIMEOUT",
    "QUOTE_FETCH_FAILED",
    "QUOTE_ZERO_BID_ASK",
    "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
    "SPREAD_TOO_WIDE",
    "UNKNOWN_REJECTION",
    "VOLUME_TOO_LOW",
}

# ── Codes also used by execution_core / materializer (additional taxonomy)
EXECUTION_CORE_EXTRA_CODES = {
    "REJECT_UNAVAILABLE",
    "REJECT_DIRECT_ZERO",
    "DEEP_OTM_REJECT",
    "OTM_TOO_FAR",
    "PREMIUM_TOO_LOW",
    "PREMIUM_TOO_HIGH",
    "LIQUIDITY_BELOW_THRESHOLD",
    "IV_RANK_TOO_HIGH",
    "IV_ZONE_SCORE_TOO_LOW",
    "IV_FILTER_ERROR",
    "IV_GATE",
    "EARNINGS_GUARD_ERROR",
    "INVALID_PLAN",
    "INVALID_EXECUTION_MODE",
    "EXECUTION_MODE_MISMATCH",
    "BLOCKED",
    "UNSUPPORTED_INDEX_MAPPING",
}

ALL_KNOWN_CODES = SELECTOR_EMITTED_CODES | EXECUTION_CORE_EXTRA_CODES


# ═══════════════════════════════════════════════════════════════════════
# 1. Every selector reason code has exactly one classification
# ═══════════════════════════════════════════════════════════════════════

def test_all_selector_emitted_codes_are_in_policy_table():
    """Every code the selector can surface must have a policy entry."""
    missing = SELECTOR_EMITTED_CODES - set(_POLICY_TABLE)
    assert not missing, f"Selector codes missing from policy table: {missing}"


def test_each_code_maps_to_exactly_one_classification():
    """Each key in the policy table maps to exactly one classification."""
    # The NamedTuple structure enforces this, but prove classification is valid.
    for code, pol in _POLICY_TABLE.items():
        assert pol.classification in VALID_CLASSIFICATIONS, (
            f"{code!r} has invalid classification {pol.classification!r}"
        )


def test_no_reason_code_appears_in_multiple_classifications():
    """Each code must be in at most one classification bucket."""
    classification_map: dict[str, set] = {c: set() for c in VALID_CLASSIFICATIONS}
    for code, pol in _POLICY_TABLE.items():
        classification_map[pol.classification].add(code)
    # Verify no code appears in more than one bucket
    for code in _POLICY_TABLE:
        buckets = [
            c for c, codes in classification_map.items() if code in codes
        ]
        assert len(buckets) == 1, (
            f"{code!r} appears in multiple classifications: {buckets}"
        )


# ═══════════════════════════════════════════════════════════════════════
# 2. Unknown reasons fail closed
# ═══════════════════════════════════════════════════════════════════════

def test_unknown_reason_returns_fallback_not_retryable():
    pol = get_policy("TOTALLY_MADE_UP_CODE_XYZ")
    assert pol.classification == UNKNOWN_FAIL_CLOSED
    assert pol.selector_rerun_allowed is False
    assert pol.retain_existing_contract is False


def test_none_reason_returns_fallback_not_retryable():
    pol = get_policy(None)
    assert pol.classification == UNKNOWN_FAIL_CLOSED
    assert not is_retryable_selector_reason(None)


def test_empty_string_reason_returns_fallback_not_retryable():
    pol = get_policy("")
    assert pol.classification == UNKNOWN_FAIL_CLOSED


def test_unknown_reason_is_not_retryable():
    assert not is_retryable_selector_reason("NONEXISTENT_CODE_12345")


# ═══════════════════════════════════════════════════════════════════════
# 3. is_retryable_selector_reason consistent with RETRYABLE_BREACH_SELECTOR_REASONS
# ═══════════════════════════════════════════════════════════════════════

def test_is_retryable_matches_frozenset_for_all_known_codes():
    """is_retryable_selector_reason and RETRYABLE_BREACH_SELECTOR_REASONS must agree."""
    for code in ALL_KNOWN_CODES:
        via_fn = is_retryable_selector_reason(code)
        via_set = code in RETRYABLE_BREACH_SELECTOR_REASONS
        assert via_fn == via_set, (
            f"{code!r}: is_retryable={via_fn} but frozenset membership={via_set}"
        )


def test_retryable_data_codes_are_retryable():
    retryable = {c for c, p in _POLICY_TABLE.items() if p.classification == RETRYABLE_DATA}
    for code in retryable:
        assert is_retryable_selector_reason(code), f"{code!r} should be retryable"


def test_non_retryable_codes_are_not_retryable():
    non_retryable = {
        c for c, p in _POLICY_TABLE.items()
        if p.classification != RETRYABLE_DATA
    }
    for code in non_retryable:
        assert not is_retryable_selector_reason(code), (
            f"{code!r} classified as {_POLICY_TABLE[code].classification} should NOT be retryable"
        )


# ═══════════════════════════════════════════════════════════════════════
# 4. RETRYABLE_MATERIALIZATION_REASONS equals RETRYABLE_BREACH_SELECTOR_REASONS
# ═══════════════════════════════════════════════════════════════════════

def test_materialization_reasons_equals_selector_reasons():
    """The two previously separate frozensets are now identical — derived
    from the same policy table so they can never diverge."""
    assert RETRYABLE_MATERIALIZATION_REASONS == RETRYABLE_BREACH_SELECTOR_REASONS, (
        f"Divergence!\n"
        f"In SELECTOR only: {RETRYABLE_BREACH_SELECTOR_REASONS - RETRYABLE_MATERIALIZATION_REASONS}\n"
        f"In MATERIALIZER only: {RETRYABLE_MATERIALIZATION_REASONS - RETRYABLE_BREACH_SELECTOR_REASONS}"
    )


# ═══════════════════════════════════════════════════════════════════════
# 5. Policy field logical consistency
# ═══════════════════════════════════════════════════════════════════════

def test_terminal_codes_do_not_allow_selector_rerun():
    terminal_classes = {TERMINAL_QUALITY, TERMINAL_POLICY, TERMINAL_AUTH, TERMINAL_INVARIANT}
    for code, pol in _POLICY_TABLE.items():
        if pol.classification in terminal_classes:
            assert not pol.selector_rerun_allowed, (
                f"{code!r} is {pol.classification} but selector_rerun_allowed=True"
            )


def test_terminal_codes_have_no_max_attempts():
    terminal_classes = {TERMINAL_QUALITY, TERMINAL_POLICY, TERMINAL_AUTH, TERMINAL_INVARIANT}
    for code, pol in _POLICY_TABLE.items():
        if pol.classification in terminal_classes:
            assert not pol.max_attempts_applies, (
                f"{code!r} is {pol.classification} but max_attempts_applies=True"
            )


def test_retryable_data_codes_allow_selector_rerun():
    for code, pol in _POLICY_TABLE.items():
        if pol.classification == RETRYABLE_DATA:
            assert pol.selector_rerun_allowed, (
                f"{code!r} is RETRYABLE_DATA but selector_rerun_allowed=False"
            )


def test_retryable_data_codes_apply_retry_delay():
    for code, pol in _POLICY_TABLE.items():
        if pol.classification == RETRYABLE_DATA:
            assert pol.retry_delay_applies, (
                f"{code!r} is RETRYABLE_DATA but retry_delay_applies=False"
            )


def test_retryable_data_codes_have_max_attempts():
    for code, pol in _POLICY_TABLE.items():
        if pol.classification == RETRYABLE_DATA:
            assert pol.max_attempts_applies, (
                f"{code!r} is RETRYABLE_DATA but max_attempts_applies=False"
            )


def test_all_policies_have_non_empty_final_reason_and_queue_reason():
    for code, pol in _POLICY_TABLE.items():
        assert pol.final_reason_code, f"{code!r} has empty final_reason_code"
        assert pol.queue_facing_reason, f"{code!r} has empty queue_facing_reason"


# ═══════════════════════════════════════════════════════════════════════
# 6. Classify helper returns valid classification string
# ═══════════════════════════════════════════════════════════════════════

def test_classify_selector_reason_returns_valid_string_for_known():
    for code in ALL_KNOWN_CODES:
        cls = classify_selector_reason(code)
        assert cls in VALID_CLASSIFICATIONS, (
            f"{code!r} -> {cls!r} is not a valid classification"
        )


def test_classify_selector_reason_returns_unknown_fail_closed_for_garbage():
    assert classify_selector_reason("GARBAGE_XYZ_999") == UNKNOWN_FAIL_CLOSED


# ═══════════════════════════════════════════════════════════════════════
# 7. Back-compat: RETRYABLE_BREACH_SELECTOR_REASONS is a frozenset
# ═══════════════════════════════════════════════════════════════════════

def test_retryable_breach_selector_reasons_is_frozenset():
    assert isinstance(RETRYABLE_BREACH_SELECTOR_REASONS, frozenset)


def test_retryable_materialization_reasons_is_frozenset():
    assert isinstance(RETRYABLE_MATERIALIZATION_REASONS, frozenset)


def test_known_retryable_codes_present_in_frozenset():
    expected_retryable = {
        "NO_CHAIN_DATA", "CHAIN_PROVIDER_ERROR", "CHAIN_ROW_ZERO_BID_ASK",
        "DIRECT_QUOTE_ZERO_BID_ASK", "QUOTE_FETCH_FAILED",
        "SELECTOR_REQUEST_BUDGET_EXHAUSTED", "PROVIDER_RATE_LIMITED",
    }
    missing = expected_retryable - RETRYABLE_BREACH_SELECTOR_REASONS
    assert not missing, f"Expected retryable codes missing from frozenset: {missing}"


def test_known_terminal_codes_absent_from_retryable_frozenset():
    expected_terminal = {
        "OI_TOO_LOW", "EARNINGS_LOCKOUT", "CHAIN_AUTH_ERROR",
        "INVALID_PLAN", "CAPITAL_NO_REMAINING",
    }
    contaminating = expected_terminal & RETRYABLE_BREACH_SELECTOR_REASONS
    assert not contaminating, (
        f"Terminal codes found in RETRYABLE frozenset: {contaminating}"
    )


def test_allow_cheap_only_choice_live_missing_and_malformed_env_fail_closed(monkeypatch):
    cheap = _option(bid=0.48, ask=0.49, symbol="SPY260717C00495000")

    monkeypatch.delenv("ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE", raising=False)
    selector_missing = FakeSelector(mode="LIVE", chain=[cheap])
    result_missing = selector_missing.select(_plan_dict())
    assert result_missing is None
    assert selector_missing.get_last_failure()["reason_code"] == "CHEAP_CONTRACT_NO_UPGRADE"

    monkeypatch.setenv("ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE", "garbage")
    selector_bad = FakeSelector(mode="LIVE", chain=[cheap])
    result_bad = selector_bad.select(_plan_dict())
    assert result_bad is None
    assert selector_bad.get_last_failure()["reason_code"] == "CHEAP_CONTRACT_NO_UPGRADE"


def test_allow_cheap_only_choice_live_true_comes_from_real_env_path(monkeypatch):
    cheap = _option(bid=0.48, ask=0.49, symbol="SPY260717C00495000")

    monkeypatch.setenv("ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE", "true")
    selector = FakeSelector(mode="LIVE", chain=[cheap])
    plan = _plan_dict(metadata={"allow_cheap_contract_if_only_choice": False})

    result = selector.select(plan)

    assert result is not None
    assert result.contract_symbol == cheap["symbol"]
    assert result.execution_price_per_share == pytest.approx(0.49)
    assert result.selection_reason == "cheap_contract_only_choice"
    assert selector.get_last_failure() is None
