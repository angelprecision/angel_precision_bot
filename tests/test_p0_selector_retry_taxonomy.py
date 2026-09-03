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
    deferred_retry_count_exhaustion_applies,
    resolve_selector_recovery_final_reason,
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
    "MONEYNESS_OUT_OF_RANGE",
    "TERMINAL_POLICY_REJECT",
    "UNKNOWN_SELECTOR_RECOVERY_FAILURE",
    "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED",
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


def test_duplicate_conflict_reason_has_runtime_restart_materializer_parity():
    """Every durable consumer sees the same bounded data-retry authority."""
    from ap import deferred_materializer
    from ap_execution_core import _classify_deferred_breach_retry_decision

    reason = "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
    policy = get_policy(reason)
    runtime = _classify_deferred_breach_retry_decision(
        reason,
        queue_local_order_id="local-pr408-parity",
        attempt=1,
        max_attempts=5,
        past_cutoff=False,
        retry_enabled=True,
    )
    restart_reduced = resolve_selector_recovery_final_reason({
        "quality_rejections": {reason: 2},
        "quality_rejection_records": [
            {"symbol": "SPY270101C00100000", "reason": reason}
        ],
        "attempted_results": {},
        "structural_skip_results": {},
        "eligible_unattempted_symbols": [],
    })

    assert policy.classification == RETRYABLE_DATA
    assert policy.selector_rerun_allowed is True
    assert policy.retry_delay_applies is True
    assert policy.max_attempts_applies is False
    assert runtime == {
        "action": "retry_schedule",
        "reason_code": reason,
        "retryable_reason": True,
    }
    assert restart_reduced == reason
    assert reason in RETRYABLE_BREACH_SELECTOR_REASONS
    assert deferred_materializer.is_reason_retryable(reason) is True
    assert reason in RETRYABLE_MATERIALIZATION_REASONS


def test_moneyness_has_terminal_runtime_restart_materializer_parity():
    """A proven request-level moneyness result is known terminal quality."""
    from ap import deferred_materializer
    from ap_execution_core import _classify_deferred_breach_retry_decision

    reason = "MONEYNESS_OUT_OF_RANGE"
    symbol = "DDOG270101P00150000"
    policy = get_policy(reason)
    runtime = _classify_deferred_breach_retry_decision(
        reason,
        queue_local_order_id="local-moneyness-parity",
        attempt=1,
        max_attempts=5,
        past_cutoff=False,
        retry_enabled=True,
    )
    restart_reduced = resolve_selector_recovery_final_reason({
        "structural_skip_records": [
            {
                "symbol": symbol,
                "skip_reason": "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            }
        ],
        "attempted_results": {},
        "eligible_unattempted_symbols": [],
        "direct_quote_known_eligible_symbols": [symbol],
        "quality_rejections": {},
        "quality_rejection_records": [],
    })

    assert policy.classification == TERMINAL_QUALITY
    assert policy.selector_rerun_allowed is False
    assert policy.retry_delay_applies is False
    assert policy.max_attempts_applies is False
    assert policy.final_reason_code == reason
    assert policy.queue_facing_reason == "TERMINAL_NO_TRADEABLE_CONTRACT"
    assert runtime == {
        "action": "terminal_quality",
        "reason_code": reason,
        "retryable_reason": False,
    }
    assert restart_reduced == reason
    assert reason not in RETRYABLE_BREACH_SELECTOR_REASONS
    assert deferred_materializer.is_reason_retryable(reason) is False
    assert reason not in RETRYABLE_MATERIALIZATION_REASONS


@pytest.mark.parametrize(
    ("structural_reason", "expected_reason", "expected_classification", "expected_action", "queue_reason"),
    [
        (
            "STRUCTURAL_DTE_OUT_OF_RANGE",
            "DTE_OUT_OF_RANGE",
            TERMINAL_QUALITY,
            "terminal_quality",
            "TERMINAL_NO_TRADEABLE_CONTRACT",
        ),
        (
            "STRUCTURAL_DELTA_OUT_OF_RANGE",
            "DELTA_OUT_OF_RANGE",
            TERMINAL_QUALITY,
            "terminal_quality",
            "TERMINAL_NO_TRADEABLE_CONTRACT",
        ),
        (
            "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            "MONEYNESS_OUT_OF_RANGE",
            TERMINAL_QUALITY,
            "terminal_quality",
            "TERMINAL_NO_TRADEABLE_CONTRACT",
        ),
        (
            "STRUCTURAL_TERMINAL_POLICY_REJECT",
            "TERMINAL_POLICY_REJECT",
            TERMINAL_POLICY,
            "terminal_policy",
            "TERMINAL_POLICY_BLOCK",
        ),
    ],
    ids=["dte", "delta", "moneyness", "terminal-policy"],
)
def test_governed_structural_families_have_runtime_restart_materializer_parity(
    structural_reason,
    expected_reason,
    expected_classification,
    expected_action,
    queue_reason,
):
    """The exact governed reason survives every deferred consumer seam."""
    from unittest.mock import MagicMock

    from ap import deferred_materializer
    from ap_execution_core import _classify_deferred_breach_retry_decision

    symbol = "SPY270101C00100000"
    evidence = {
        "structural_skip_records": [
            {"symbol": symbol, "skip_reason": structural_reason},
        ],
        "attempted_results": {},
        "eligible_unattempted_symbols": [],
        "direct_quote_known_eligible_symbols": [symbol],
        "quality_rejections": {},
        "quality_rejection_records": [],
    }

    reduced = resolve_selector_recovery_final_reason(evidence)
    policy = get_policy(reduced)
    runtime = _classify_deferred_breach_retry_decision(
        reduced,
        queue_local_order_id="local-structural-parity",
        attempt=1,
        max_attempts=5,
        past_cutoff=False,
        retry_enabled=True,
    )
    restart_reduced = resolve_selector_recovery_final_reason(dict(evidence))

    assert reduced == expected_reason
    assert restart_reduced == expected_reason
    assert policy.classification == expected_classification
    assert policy.selector_rerun_allowed is False
    assert policy.retry_delay_applies is False
    assert policy.max_attempts_applies is False
    assert policy.final_reason_code == expected_reason
    assert policy.queue_facing_reason == queue_reason
    assert runtime == {
        "action": expected_action,
        "reason_code": expected_reason,
        "retryable_reason": False,
    }
    assert deferred_materializer.is_reason_retryable(expected_reason) is False

    osm = MagicMock()
    osm.update_order_meta.return_value = True
    assert deferred_materializer.stamp_failed_terminal(
        osm,
        "local-structural-parity",
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        symbol="SPY",
        direction="CALL",
        reason_code=expected_reason,
        attempt=1,
        selector_failure={"reason_code": expected_reason},
    ) is True
    materializer_patch = osm.update_order_meta.call_args.args[1]
    assert materializer_patch["materialization_reason"] == expected_reason
    assert materializer_patch["materialization_status"] == "FAILED_TERMINAL"
    assert materializer_patch["broker_ready"] is False


@pytest.mark.parametrize(
    "reason",
    [
        "TERMINAL_POLICY_REJECT",
    ],
    ids=["structural-policy"],
)
def test_structural_policy_reason_keeps_policy_action_and_block_status(reason):
    """The request-scope structural policy reason is not a quality reject."""
    from ap_execution_core import (
        _classify_deferred_breach_retry_decision,
        _deferred_selector_status,
    )

    policy = get_policy(reason)
    decision = _classify_deferred_breach_retry_decision(
        reason,
        queue_local_order_id="local-terminal-policy",
        attempt=1,
        max_attempts=5,
        past_cutoff=False,
        retry_enabled=True,
    )

    assert policy.classification == TERMINAL_POLICY
    assert decision == {
        "action": "terminal_policy",
        "reason_code": reason,
        "retryable_reason": False,
    }
    assert _deferred_selector_status(decision) == "CONTRACT_SELECTION_BLOCKED"


@pytest.mark.parametrize(
    "reason",
    ["EARNINGS_LOCKOUT", "NO_AFFORDABLE_CONTRACT", "UNTRADEABLE_FOR_ACCOUNT_SIZE"],
)
def test_existing_policy_reasons_keep_historical_deferred_quality_action(reason):
    """PR #504 must not broaden the structural-policy runtime seam."""
    from ap_execution_core import (
        _classify_deferred_breach_retry_decision,
        _deferred_selector_status,
    )

    decision = _classify_deferred_breach_retry_decision(
        reason,
        queue_local_order_id="local-existing-policy",
        attempt=1,
        max_attempts=5,
        past_cutoff=False,
        retry_enabled=True,
    )

    assert get_policy(reason).classification == TERMINAL_POLICY
    assert decision["action"] == "terminal_quality"
    assert _deferred_selector_status(decision) == "CONTRACT_SELECTION_QUALITY_REJECT"


def test_selector_recovery_failure_has_invariant_runtime_restart_materializer_parity(
    monkeypatch,
):
    """Reducer invariant failures retain their exact reason at every seam."""
    from unittest.mock import MagicMock

    import ap.selector_retry_policy as retry_policy
    from ap import deferred_materializer
    from ap.contract_selector import (
        _resolve_deferred_recovery_final_reason_fail_closed,
    )
    from ap_execution_core import _classify_deferred_breach_retry_decision

    with monkeypatch.context() as reducer_patch:
        reducer_patch.setattr(
            retry_policy,
            "resolve_selector_recovery_final_reason",
            lambda _evidence: (_ for _ in ()).throw(
                RuntimeError("forced recovery reducer invariant failure")
            ),
        )
        reason = _resolve_deferred_recovery_final_reason_fail_closed({
            "fallback_selector_reason": "MONEYNESS_OUT_OF_RANGE",
        })

    assert reason == "UNKNOWN_SELECTOR_RECOVERY_FAILURE"

    policy = get_policy(reason)
    assert policy.classification == TERMINAL_INVARIANT
    assert policy.selector_rerun_allowed is False
    assert policy.retain_existing_contract is False
    assert policy.retry_delay_applies is False
    assert policy.max_attempts_applies is False
    assert policy.final_reason_code == reason
    assert policy.final_reason_code != _FALLBACK_UNKNOWN.final_reason_code
    assert policy.queue_facing_reason == "TERMINAL_INVARIANT_VIOLATION"

    runtime = _classify_deferred_breach_retry_decision(
        reason,
        queue_local_order_id="local-recovery-invariant",
        attempt=1,
        max_attempts=5,
        past_cutoff=False,
        retry_enabled=True,
    )
    assert runtime == {
        "action": "terminal_invariant",
        "reason_code": reason,
        "retryable_reason": False,
    }

    restart_reduced = resolve_selector_recovery_final_reason({
        "attempted_results": {},
        "structural_skip_records": [],
        "eligible_unattempted_symbols": [],
        "direct_quote_known_eligible_symbols": [],
        "quality_rejections": {},
        "quality_rejection_records": [],
        "fallback_selector_reason": reason,
    })
    assert restart_reduced == reason
    assert get_policy(restart_reduced).final_reason_code == reason

    assert is_retryable_selector_reason(reason) is False
    assert reason not in RETRYABLE_BREACH_SELECTOR_REASONS
    assert deferred_materializer.is_reason_retryable(reason) is False
    assert reason not in RETRYABLE_MATERIALIZATION_REASONS

    osm = MagicMock()
    osm.update_order_meta.return_value = True
    assert deferred_materializer.stamp_failed_terminal(
        osm,
        "local-recovery-invariant",
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        symbol="SPY",
        direction="CALL",
        reason_code=reason,
        attempt=1,
        selector_failure={"reason_code": reason},
    ) is True
    materializer_patch = osm.update_order_meta.call_args.args[1]
    assert materializer_patch["materialization_reason"] == reason
    assert materializer_patch["materialization_status"] == "FAILED_TERMINAL"
    assert materializer_patch["broker_ready"] is False


# ═══════════════════════════════════════════════════════════════════════
# 4b. Direct runtime classifier evidence for DUPLICATE_QUOTE_CONFLICT_UNRESOLVED
#
# The parity test above proves attempt=1 end-to-end. These four tests close
# the remaining evidence gap: attempt 2+, max-attempts exhaustion, past
# cutoff, and retry-disabled were previously proven only by reading
# _classify_deferred_breach_retry_decision and confirming it has no
# per-reason branching (structural guarantee). These tests call the real
# production classifier directly with the exact canonical reason string —
# no monkeypatch of the classifier itself, no duplicated logic.
# ═══════════════════════════════════════════════════════════════════════

_DUPLICATE_QUOTE_CONFLICT_REASON = "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
_GENERIC_RELABEL_REASONS = frozenset({
    "CONTRACT_SELECTION_QUALITY_REJECT",
    "UNKNOWN_REJECTION",
    "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
})


def test_duplicate_conflict_attempt_two_remains_retryable():
    """Attempt 2 (of 5) is still under bounded data-retry authority.

    attempt=2 counts as two attempts already made; the classifier's own
    contract is `attempt < max_attempts` for retry_schedule, so 2 < 5
    must still schedule. This directly exercises attempt indexing rather
    than inferring it from the attempt=1 case.
    """
    from ap_execution_core import _classify_deferred_breach_retry_decision

    result = _classify_deferred_breach_retry_decision(
        _DUPLICATE_QUOTE_CONFLICT_REASON,
        queue_local_order_id="local-pr408-attempt-2",
        attempt=2,
        max_attempts=5,
        past_cutoff=False,
        retry_enabled=True,
    )

    assert result["action"] == "retry_schedule"
    assert result["reason_code"] == _DUPLICATE_QUOTE_CONFLICT_REASON
    assert result["retryable_reason"] is True
    assert result["reason_code"] not in _GENERIC_RELABEL_REASONS


def test_duplicate_conflict_at_max_attempts_remains_retryable():
    """A proven transient data miss is not terminalized by the count ceiling."""
    from ap_execution_core import _classify_deferred_breach_retry_decision

    result = _classify_deferred_breach_retry_decision(
        _DUPLICATE_QUOTE_CONFLICT_REASON,
        queue_local_order_id="local-pr408-attempt-exhausted",
        attempt=5,
        max_attempts=5,
        past_cutoff=False,
        retry_enabled=True,
    )

    assert result["action"] == "retry_schedule"
    assert result["reason_code"] == _DUPLICATE_QUOTE_CONFLICT_REASON
    assert result["retryable_reason"] is True
    assert result["reason_code"] not in _GENERIC_RELABEL_REASONS


def test_duplicate_conflict_past_cutoff_does_not_schedule_retry():
    """Past the wall-clock cutoff, no further retry is scheduled — even
    when attempt count would otherwise still allow one. past_cutoff takes
    priority ahead of the attempt/max_attempts check in the real function.
    """
    from ap_execution_core import _classify_deferred_breach_retry_decision

    result = _classify_deferred_breach_retry_decision(
        _DUPLICATE_QUOTE_CONFLICT_REASON,
        queue_local_order_id="local-pr408-past-cutoff",
        attempt=2,
        max_attempts=5,
        past_cutoff=True,
        retry_enabled=True,
    )

    assert result["action"] == "retry_cutoff"
    assert result["reason_code"] == _DUPLICATE_QUOTE_CONFLICT_REASON
    assert result["retryable_reason"] is True
    assert result["terminal_reason"] == (
        f"breach_retry_cutoff:{_DUPLICATE_QUOTE_CONFLICT_REASON}"
    )
    assert result["action"] != "retry_schedule"
    assert result["reason_code"] not in _GENERIC_RELABEL_REASONS
    assert _DUPLICATE_QUOTE_CONFLICT_REASON in result["terminal_reason"]


def test_duplicate_conflict_retry_disabled_does_not_schedule_retry():
    """The BREACH_SELECTOR_RETRY_ENABLED kill switch blocks scheduling
    even on attempt 1 with room remaining and no cutoff reached.
    """
    from ap_execution_core import _classify_deferred_breach_retry_decision

    result = _classify_deferred_breach_retry_decision(
        _DUPLICATE_QUOTE_CONFLICT_REASON,
        queue_local_order_id="local-pr408-retry-disabled",
        attempt=1,
        max_attempts=5,
        past_cutoff=False,
        retry_enabled=False,
    )

    assert result["action"] == "retry_disabled"
    assert result["reason_code"] == _DUPLICATE_QUOTE_CONFLICT_REASON
    assert result["retryable_reason"] is True
    assert result["terminal_reason"] == (
        f"breach_retry_disabled:{_DUPLICATE_QUOTE_CONFLICT_REASON}"
    )
    assert result["action"] != "retry_schedule"
    assert result["reason_code"] not in _GENERIC_RELABEL_REASONS
    assert _DUPLICATE_QUOTE_CONFLICT_REASON in result["terminal_reason"]


def test_duplicate_conflict_is_not_retryable_would_fail_all_four_cases():
    """Positive control: prove these tests would actually fail if the
    reason were removed from RETRYABLE_DATA authority, rather than passing
    for an unrelated reason (e.g. an exception swallowed before the real
    branch is reached).
    """
    from ap_execution_core import _classify_deferred_breach_retry_decision

    _fake_non_retryable_reason = "NOT_A_REGISTERED_RETRYABLE_REASON_XYZ"
    assert _fake_non_retryable_reason not in RETRYABLE_BREACH_SELECTOR_REASONS

    result = _classify_deferred_breach_retry_decision(
        _fake_non_retryable_reason,
        queue_local_order_id="local-pr408-control",
        attempt=2,
        max_attempts=5,
        past_cutoff=False,
        retry_enabled=True,
    )

    # An unregistered reason must fall through to terminal_quality, not any
    # of the four retry-authority actions the real reason produces above.
    assert result["action"] == "terminal_quality"
    assert result["retryable_reason"] is False
    assert result["action"] not in (
        "retry_schedule", "retry_exhausted", "retry_cutoff", "retry_disabled",
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


def test_retryable_data_codes_are_validity_bound():
    for code, pol in _POLICY_TABLE.items():
        if pol.classification == RETRYABLE_DATA:
            assert not pol.max_attempts_applies, (
                f"{code!r} is RETRYABLE_DATA but max_attempts_applies=True"
            )


def test_count_helper_keeps_unknowns_fail_closed():
    assert deferred_retry_count_exhaustion_applies("UNKNOWN_REASON", selector_failure={})
    assert deferred_retry_count_exhaustion_applies(
        "UNKNOWN_RETRY_REASON",
        selector_failure={"market_truth_outcome": "HOLD_MARKET_TRUTH_UNAVAILABLE"},
    )
    assert deferred_retry_count_exhaustion_applies(
        "OI_TOO_LOW",
        selector_failure={
            "last_breach_selector_audit": {
                "market_truth_outcome": "HOLD_MARKET_TRUTH_UNAVAILABLE",
            },
        },
    )
    assert deferred_retry_count_exhaustion_applies(
        "NO_VALID_PLAYBOOK_DTE_CONTRACT",
        selector_failure={"market_truth_outcome": "HOLD_MARKET_TRUTH_UNAVAILABLE"},
    )
    assert not deferred_retry_count_exhaustion_applies(
        "CURRENT_PRICE_FETCH_FAILED",
        selector_failure={"market_truth_outcome": "HOLD_MARKET_TRUTH_UNAVAILABLE"},
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


@pytest.mark.parametrize(
    "reason",
    ["INVALID_PLAN", "INVALID_EXECUTION_MODE", "EXECUTION_MODE_MISMATCH"],
)
def test_preexisting_terminal_invariants_keep_historical_deferred_quality_action(reason):
    """PR #504 invariant handling is exact-reason scoped, not category-wide."""
    from ap_execution_core import (
        _classify_deferred_breach_retry_decision,
        _deferred_selector_status,
    )

    assert get_policy(reason).classification == TERMINAL_INVARIANT
    decision = _classify_deferred_breach_retry_decision(
        reason,
        queue_local_order_id="local-existing-invariant",
        attempt=1,
        max_attempts=5,
        past_cutoff=False,
        retry_enabled=True,
    )
    assert decision == {
        "action": "terminal_quality",
        "reason_code": reason,
        "retryable_reason": False,
    }
    assert _deferred_selector_status(decision) == "CONTRACT_SELECTION_QUALITY_REJECT"
