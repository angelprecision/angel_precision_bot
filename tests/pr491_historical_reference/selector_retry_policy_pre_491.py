"""Minimal deterministic pre-#491 reducer reference for production replays.

The replay tests need one pure function from the pre-#491 implementation:
``resolve_selector_recovery_final_reason``.  The former fixture copied the
entire production module, including cursor persistence and retry-ownership
machinery that the replay never calls.  This reference retains the historical
reason classifications and reducer order only; it has no production imports,
I/O, or stateful recovery behavior.
"""

from __future__ import annotations

from types import SimpleNamespace


RETRYABLE_DATA = "RETRYABLE_DATA"
TERMINAL_QUALITY = "TERMINAL_QUALITY"
TERMINAL_POLICY = "TERMINAL_POLICY"
TERMINAL_AUTH = "TERMINAL_AUTH"
TERMINAL_INVARIANT = "TERMINAL_INVARIANT"
UNKNOWN_FAIL_CLOSED = "UNKNOWN_FAIL_CLOSED"


# Classification-only projection of the pre-#491 policy table.  The replayed
# reducer reads no other policy fields.
_POLICY_CLASSIFICATION = {
    **dict.fromkeys(
        (
            "NO_CHAIN_DATA",
            "CHAIN_PROVIDER_ERROR",
            "CHAIN_PROVIDER_EMPTY_EXPIRATIONS",
            "CHAIN_PROVIDER_EMPTY_OPTIONS",
            "CHAIN_PARSE_EMPTY",
            "CHAIN_EMPTY",
            "CHAIN_FETCH_FAILED",
            "NO_EXPIRATION_IN_DTE_WINDOW",
            "DIRECT_QUOTE_UNAVAILABLE",
            "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED",
            "CHAIN_ROW_ZERO_BID_ASK",
            "DIRECT_QUOTE_ZERO_BID_ASK",
            "QUOTE_FETCH_FAILED",
            "QUOTE_ZERO_BID_ASK",
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
            "MARKET_DATA_THROTTLE_UNAVAILABLE",
            "PROVIDER_RATE_LIMITED",
            "PROVIDER_TIMEOUT",
            "REJECT_UNAVAILABLE",
        ),
        RETRYABLE_DATA,
    ),
    **dict.fromkeys(
        (
            "OI_TOO_LOW",
            "VOLUME_TOO_LOW",
            "LIQUIDITY_BELOW_THRESHOLD",
            "SPREAD_TOO_WIDE",
            "FINAL_SPREAD_TOO_WIDE",
            "CHEAP_CONTRACT_NO_UPGRADE",
            "BID_BELOW_MIN",
            "DEEP_OTM_REJECT",
            "OTM_TOO_FAR",
            "DELTA_OUT_OF_RANGE",
            "DTE_OUT_OF_RANGE",
            "PREMIUM_TOO_LOW",
            "PREMIUM_TOO_HIGH",
            "PREMIUM_CAP_EXCEEDED",
            "NO_CONTRACT_AFTER_FILTERS",
            "IV_RANK_TOO_HIGH",
            "IV_ZONE_SCORE_TOO_LOW",
            "REJECT_DIRECT_ZERO",
            "DTE_LADDER_ERROR",
            "NO_VALID_PLAYBOOK_DTE_CONTRACT",
        ),
        TERMINAL_QUALITY,
    ),
    **dict.fromkeys(
        (
            "EARNINGS_LOCKOUT",
            "EARNINGS_GUARD_ERROR",
            "UNTRADEABLE_FOR_ACCOUNT_SIZE",
            "CHEAP_CONTRACT_ONLY_CHOICE",
            "NO_AFFORDABLE_CONTRACT",
            "FINAL_CONTRACT_UNAFFORDABLE",
            "CAPITAL_NO_REMAINING",
            "INVALID_POSITION_BUDGET",
            "UNSUPPORTED_INDEX_MAPPING",
            "BLOCKED",
            "IV_GATE",
            "IV_FILTER_ERROR",
        ),
        TERMINAL_POLICY,
    ),
    "CHAIN_AUTH_ERROR": TERMINAL_AUTH,
    **dict.fromkeys(
        ("INVALID_PLAN", "INVALID_EXECUTION_MODE", "EXECUTION_MODE_MISMATCH"),
        TERMINAL_INVARIANT,
    ),
}


def get_policy(reason_code: str | None):
    key = str(reason_code or "").strip()
    return SimpleNamespace(
        classification=_POLICY_CLASSIFICATION.get(key, UNKNOWN_FAIL_CLOSED)
    )


_NO_AFFORDABLE_REASONS = frozenset(
    ("NO_AFFORDABLE_CONTRACT", "STRUCTURAL_CLEARLY_UNAFFORDABLE")
)
_PREMIUM_CAP_REASONS = frozenset(
    ("PREMIUM_CAP_EXCEEDED", "STRUCTURAL_PREMIUM_CAP_EXCEEDED")
)
_AFFORDABILITY_REASONS = _NO_AFFORDABLE_REASONS | _PREMIUM_CAP_REASONS


def resolve_selector_recovery_final_reason(evidence: dict) -> str:
    """Return the historical pre-#491 terminal reason without side effects."""
    data = evidence if isinstance(evidence, dict) else {}
    market_outcome = str(data.get("market_truth_outcome") or "").upper()
    market_reason = str(data.get("market_truth_reason") or "").strip()
    if market_outcome == "TERMINAL_SETUP_COMPLETE":
        return market_reason or "MARKET_SETUP_INVALIDATED"
    if market_reason == "MARKET_SETUP_INVALIDATED":
        return market_reason

    quality = data.get("quality_rejections")
    quality = quality if isinstance(quality, dict) else {}
    attempted = data.get("attempted_results")
    attempted = attempted if isinstance(attempted, dict) else {}
    skipped = data.get("structural_skip_results")
    skipped = skipped if isinstance(skipped, dict) else {}

    terminal_policy = next(
        (
            reason
            for reason in quality
            if reason not in _AFFORDABILITY_REASONS
            and get_policy(reason).classification == TERMINAL_POLICY
        ),
        None,
    )
    if terminal_policy:
        return terminal_policy

    structural_values = set(skipped.values())
    for structural, canonical in (
        ("STRUCTURAL_DTE_OUT_OF_RANGE", "DTE_OUT_OF_RANGE"),
        ("STRUCTURAL_MONEYNESS_OUT_OF_RANGE", "MONEYNESS_OUT_OF_RANGE"),
        ("STRUCTURAL_DELTA_OUT_OF_RANGE", "DELTA_OUT_OF_RANGE"),
        ("STRUCTURAL_TERMINAL_POLICY_REJECT", "TERMINAL_POLICY_REJECT"),
    ):
        if structural in structural_values:
            return canonical

    terminal_quality = next(
        (
            reason
            for reason in quality
            if reason not in _AFFORDABILITY_REASONS
            and get_policy(reason).classification == TERMINAL_QUALITY
        ),
        None,
    )
    if terminal_quality:
        return terminal_quality

    eligible = list(data.get("eligible_unattempted_symbols") or [])
    if (
        bool(data.get("actual_limit_reached"))
        and bool(data.get("budget_exhausted_stage"))
        and eligible
    ):
        return "SELECTOR_REQUEST_BUDGET_EXHAUSTED"

    transient_counts: dict[str, int] = {}
    for record in attempted.values():
        reason = (
            str(record.get("result_reason") or "")
            if isinstance(record, dict)
            else str(record or "")
        )
        if reason and get_policy(reason).classification == RETRYABLE_DATA:
            transient_counts[reason] = transient_counts.get(reason, 0) + 1
    if transient_counts:
        return sorted(
            transient_counts.items(), key=lambda item: (-item[1], item[0])
        )[0][0]

    retryable_quality = [
        (str(reason), int(count or 0))
        for reason, count in quality.items()
        if get_policy(str(reason)).classification == RETRYABLE_DATA
    ]
    if retryable_quality:
        return sorted(retryable_quality, key=lambda item: (-item[1], item[0]))[0][0]

    accounted_reasons: list[str] = []
    for record in attempted.values():
        accounted_reasons.append(
            str(record.get("result_reason") or "")
            if isinstance(record, dict)
            else str(record or "")
        )
    accounted_reasons.extend(str(value or "") for value in skipped.values())
    accounted_reasons.extend(str(reason or "") for reason in quality.keys())
    accounted_reasons = [reason for reason in accounted_reasons if reason]

    if (
        not eligible
        and accounted_reasons
        and all(reason in _AFFORDABILITY_REASONS for reason in accounted_reasons)
    ):
        if any(reason in _NO_AFFORDABLE_REASONS for reason in accounted_reasons):
            return "NO_AFFORDABLE_CONTRACT"
        return "PREMIUM_CAP_EXCEEDED"

    return "UNKNOWN_SELECTOR_RECOVERY_FAILURE"
