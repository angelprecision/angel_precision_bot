from __future__ import annotations

import copy
from pathlib import Path

import pytest

from ap.selector_retry_policy import (
    RETRYABLE_DATA,
    TERMINAL_POLICY,
    TERMINAL_QUALITY,
    get_policy,
    resolve_selector_recovery_final_reason,
)
from ap.contract_selector import (
    _resolve_deferred_recovery_final_reason_fail_closed,
    _to_queue_reason,
)


MONEY = "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"
DTE = "STRUCTURAL_DTE_OUT_OF_RANGE"
DELTA = "STRUCTURAL_DELTA_OUT_OF_RANGE"
POLICY = "STRUCTURAL_TERMINAL_POLICY_REJECT"
RETRYABLE = "DIRECT_QUOTE_ZERO_BID_ASK"

_STRUCTURAL_CASES = (
    ("moneyness", MONEY, "MONEYNESS_OUT_OF_RANGE", TERMINAL_QUALITY,
     "TERMINAL_NO_TRADEABLE_CONTRACT"),
    ("dte", DTE, "DTE_OUT_OF_RANGE", TERMINAL_QUALITY,
     "TERMINAL_NO_TRADEABLE_CONTRACT"),
    ("delta", DELTA, "DELTA_OUT_OF_RANGE", TERMINAL_QUALITY,
     "TERMINAL_NO_TRADEABLE_CONTRACT"),
    ("terminal_policy", POLICY, "TERMINAL_POLICY_REJECT", TERMINAL_POLICY,
     "TERMINAL_POLICY_BLOCK"),
)
_STRUCTURAL_CASE_IDS = [case[0] for case in _STRUCTURAL_CASES]
_STRUCTURAL_CANONICAL_REASONS = {case[2] for case in _STRUCTURAL_CASES}


def _evidence(
    *,
    skipped,
    attempted=None,
    eligible=None,
    known=None,
    fallback=None,
    quality=None,
    quality_records=None,
):
    data = {
        "structural_skip_records": [
            {"symbol": symbol, "skip_reason": reason}
            for symbol, reason in skipped.items()
        ],
        # Legacy key remains present in current production evidence.
        "structural_skip_results": dict(skipped),
        "attempted_results": dict(attempted or {}),
        "eligible_unattempted_symbols": list(eligible or []),
        "direct_quote_known_eligible_symbols": list(known or []),
        "quality_rejections": dict(quality or {}),
        "quality_rejection_records": list(quality_records or []),
        "fallback_selector_reason": fallback,
    }
    return data


def test_moneyness_plus_retryable_candidate_is_not_request_moneyness():
    evidence = _evidence(
        skipped={"CRM260821P00170000": MONEY},
        attempted={"CRM260821P00172500": RETRYABLE},
        known=["CRM260821P00170000", "CRM260821P00172500"],
        fallback=RETRYABLE,
    )
    assert resolve_selector_recovery_final_reason(evidence) == RETRYABLE


def test_production_shaped_retryable_candidate_outranks_terminal_quality_histogram():
    """Fail-first regression for the Aug 19 Jason LIVE evidence shape.

    SPREAD_TOO_WIDE/OI_TOO_LOW are real candidate-level observations, but
    they cannot erase a separate candidate's retryable direct-quote truth.
    """
    evidence = _evidence(
        skipped={"AEP260821P00110000": MONEY},
        attempted={
            "AEP260821P00115000": {
                "result_reason": RETRYABLE,
                "attempt_number": 1,
            }
        },
        known=["AEP260821P00110000", "AEP260821P00115000"],
        fallback="CHAIN_ROW_ZERO_BID_ASK",
        quality={
            "CHAIN_ROW_ZERO_BID_ASK": 11,
            "DIRECT_QUOTE_ZERO_BID_ASK": 1,
            "OI_TOO_LOW": 4,
            "SPREAD_TOO_WIDE": 1,
        },
    )

    final_reason = resolve_selector_recovery_final_reason(evidence)
    policy = get_policy(final_reason)

    assert final_reason == RETRYABLE
    assert policy.classification == RETRYABLE_DATA
    assert policy.selector_rerun_allowed is True
    assert policy.queue_facing_reason == "RETRY_LATER_DATA_UNAVAILABLE"


def test_retryable_symbol_bound_quality_record_outranks_terminal_quality_histogram():
    evidence = _evidence(
        skipped={"AEP260821P00110000": MONEY},
        known=["AEP260821P00110000", "AEP260821P00115000"],
        quality={
            "CHAIN_ROW_ZERO_BID_ASK": 11,
            "OI_TOO_LOW": 4,
            "SPREAD_TOO_WIDE": 1,
        },
        quality_records=[
            {"symbol": "AEP260821P00115000", "reason": "CHAIN_ROW_ZERO_BID_ASK"}
        ],
    )
    assert (
        resolve_selector_recovery_final_reason(evidence)
        == "CHAIN_ROW_ZERO_BID_ASK"
    )


def test_structural_histogram_contamination_cannot_manufacture_retryability():
    """Aggregate chain-zero counts may belong to structurally skipped rows.

    With no attempted or symbol-bound retryable candidate, that diagnostic
    bucket must not override the one genuine candidate's terminal spread
    result or authorize another selector pass.
    """
    evidence = _evidence(
        skipped={"AEP260821P00110000": MONEY},
        attempted={
            "AEP260821P00115000": {
                "result_reason": "SPREAD_TOO_WIDE",
                "attempt_number": 1,
            }
        },
        known=["AEP260821P00110000", "AEP260821P00115000"],
        fallback="CHAIN_ROW_ZERO_BID_ASK",
        quality={
            "CHAIN_ROW_ZERO_BID_ASK": 11,
            "SPREAD_TOO_WIDE": 1,
        },
        quality_records=[
            {"symbol": "AEP260821P00115000", "reason": "SPREAD_TOO_WIDE"}
        ],
    )

    final_reason = resolve_selector_recovery_final_reason(evidence)
    policy = get_policy(final_reason)

    assert final_reason == "SPREAD_TOO_WIDE"
    assert policy.classification != RETRYABLE_DATA
    assert policy.selector_rerun_allowed is False
    assert policy.queue_facing_reason == "TERMINAL_NO_TRADEABLE_CONTRACT"


def test_moneyness_plus_eligible_unattempted_candidate_is_not_terminal():
    evidence = _evidence(
        skipped={"AEP260821P00110000": MONEY},
        eligible=["AEP260821P00115000"],
        known=["AEP260821P00110000", "AEP260821P00115000"],
    )
    assert resolve_selector_recovery_final_reason(evidence) != "MONEYNESS_OUT_OF_RANGE"


def test_eligible_unattempted_candidate_outranks_terminal_quality_when_budget_exhausted():
    evidence = _evidence(
        skipped={"AEP260821P00110000": MONEY},
        eligible=["AEP260821P00115000"],
        known=["AEP260821P00110000", "AEP260821P00115000"],
        quality={"OI_TOO_LOW": 4, "SPREAD_TOO_WIDE": 1},
    )
    evidence["actual_limit_reached"] = True
    evidence["budget_exhausted_stage"] = "direct_quote"

    final_reason = resolve_selector_recovery_final_reason(evidence)
    policy = get_policy(final_reason)

    assert final_reason == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
    assert policy.classification == RETRYABLE_DATA
    assert policy.selector_rerun_allowed is True
    assert policy.queue_facing_reason == "RETRY_LATER_DATA_UNAVAILABLE"


def test_terminal_quality_remains_terminal_after_candidate_set_is_exhausted():
    evidence = _evidence(
        skipped={},
        known=["AEP260821P00115000"],
        quality={"SPREAD_TOO_WIDE": 1},
        quality_records=[
            {"symbol": "AEP260821P00115000", "reason": "SPREAD_TOO_WIDE"}
        ],
    )
    assert resolve_selector_recovery_final_reason(evidence) == "SPREAD_TOO_WIDE"


def test_mixed_structural_reasons_are_not_request_moneyness():
    evidence = _evidence(
        skipped={
            "WDAY260821C00217500": DELTA,
            "WDAY260821C00220000": MONEY,
        },
        known=["WDAY260821C00217500", "WDAY260821C00220000"],
    )
    assert resolve_selector_recovery_final_reason(evidence) != "MONEYNESS_OUT_OF_RANGE"


def test_unaccounted_known_candidate_refuses_exhaustive_claim():
    evidence = _evidence(
        skipped={"DDOG260821P00150000": MONEY},
        known=["DDOG260821P00150000", "DDOG260821P00155000"],
    )
    assert resolve_selector_recovery_final_reason(evidence) != "MONEYNESS_OUT_OF_RANGE"


def test_bounded_evidence_overflow_refuses_exhaustive_claim():
    evidence = _evidence(
        skipped={"DDOG260821P00150000": MONEY},
        known=["DDOG260821P00150000"],
    )
    evidence["structural_skip_records_overflowed"] = True
    assert resolve_selector_recovery_final_reason(evidence) != "MONEYNESS_OUT_OF_RANGE"


def test_homogeneous_exhaustive_moneyness_still_terminalizes():
    symbols = ["DDOG260821P00150000", "DDOG260821P00155000"]
    evidence = _evidence(skipped={symbol: MONEY for symbol in symbols}, known=symbols)
    assert resolve_selector_recovery_final_reason(evidence) == "MONEYNESS_OUT_OF_RANGE"


@pytest.mark.parametrize("case", _STRUCTURAL_CASES, ids=_STRUCTURAL_CASE_IDS)
def test_every_governed_structural_family_has_exhaustive_positive_control(case):
    """Every canonical structural family must prove the whole request.

    This calls the production request-scope reducer and its canonical policy
    table together.  A mapping-only assertion would miss policy drift such as
    TERMINAL_POLICY_REJECT falling through to UNKNOWN_SELECTOR_REASON.
    """
    _name, structural_reason, canonical_reason, classification, queue_reason = case
    symbols = ["SPY260821C00100000", "SPY260821C00101000"]
    evidence = _evidence(
        skipped={symbol: structural_reason for symbol in symbols},
        known=symbols,
    )

    result = resolve_selector_recovery_final_reason(evidence)
    policy = get_policy(result)

    assert result == canonical_reason
    assert policy.classification == classification
    assert policy.final_reason_code == canonical_reason
    assert policy.selector_rerun_allowed is False
    assert policy.retry_delay_applies is False
    assert policy.max_attempts_applies is False
    assert policy.queue_facing_reason == queue_reason
    # Selector failure metadata uses the existing selector-facing mapping;
    # terminal-quality geometry remains its canonical reason while the policy
    # table owns the broader terminal outcome label.
    expected_selector_queue_reason = (
        queue_reason if classification == TERMINAL_POLICY else result
    )
    assert _to_queue_reason(result) == expected_selector_queue_reason


@pytest.mark.parametrize(
    ("left", "right"),
    [
        (MONEY, DTE),
        (MONEY, DELTA),
        (DTE, DELTA),
        (DELTA, POLICY),
    ],
    ids=["moneyness+dte", "moneyness+delta", "dte+delta", "delta+terminal-policy"],
)
def test_mixed_structural_families_are_order_independent_and_fail_closed(left, right):
    symbols = ["SPY260821C00100000", "SPY260821C00101000"]

    def _resolve(order):
        skipped = {}
        for index in order:
            skipped[symbols[index]] = (left, right)[index]
        return resolve_selector_recovery_final_reason(
            _evidence(skipped=skipped, known=symbols)
        )

    forward = _resolve((0, 1))
    reverse = _resolve((1, 0))

    assert forward == reverse
    assert forward not in _STRUCTURAL_CANONICAL_REASONS


@pytest.mark.parametrize("case", _STRUCTURAL_CASES, ids=_STRUCTURAL_CASE_IDS)
def test_structural_plus_retryable_attempted_candidate_preserves_retryable_truth(case):
    _name, structural_reason, _canonical_reason, _classification, _queue_reason = case
    structural_symbol = "SPY260821C00100000"
    attempted_symbol = "SPY260821C00101000"
    evidence = _evidence(
        skipped={structural_symbol: structural_reason},
        attempted={attempted_symbol: RETRYABLE},
        known=[structural_symbol, attempted_symbol],
        fallback=RETRYABLE,
    )

    assert resolve_selector_recovery_final_reason(evidence) == RETRYABLE


@pytest.mark.parametrize("case", _STRUCTURAL_CASES, ids=_STRUCTURAL_CASE_IDS)
def test_structural_plus_eligible_unattempted_candidate_cannot_terminalize(case):
    _name, structural_reason, canonical_reason, _classification, _queue_reason = case
    structural_symbol = "SPY260821C00100000"
    unattempted_symbol = "SPY260821C00101000"
    evidence = _evidence(
        skipped={structural_symbol: structural_reason},
        eligible=[unattempted_symbol],
        known=[structural_symbol, unattempted_symbol],
    )

    result = resolve_selector_recovery_final_reason(evidence)
    assert result not in _STRUCTURAL_CANONICAL_REASONS
    assert result != canonical_reason


@pytest.mark.parametrize(
    ("case", "ordinary_reason"),
    [
        ((_STRUCTURAL_CASES[0]), "SPREAD_TOO_WIDE"),
        ((_STRUCTURAL_CASES[1]), "OI_TOO_LOW"),
        ((_STRUCTURAL_CASES[2]), "CHAIN_ROW_ZERO_BID_ASK"),
        ((_STRUCTURAL_CASES[3]), "SPREAD_TOO_WIDE"),
    ],
    ids=["moneyness+spread", "dte+oi", "delta+chain-zero", "policy+spread"],
)
def test_structural_plus_ordinary_quality_rejection_fails_closed(case, ordinary_reason):
    _name, structural_reason, _canonical_reason, _classification, _queue_reason = case
    structural_symbol = "SPY260821C00100000"
    ordinary_symbol = "SPY260821C00101000"
    evidence = _evidence(
        skipped={structural_symbol: structural_reason},
        known=[structural_symbol, ordinary_symbol],
        quality={ordinary_reason: 1},
        quality_records=[{"symbol": ordinary_symbol, "reason": ordinary_reason}],
    )

    result = resolve_selector_recovery_final_reason(evidence)
    assert result == ordinary_reason
    assert result not in _STRUCTURAL_CANONICAL_REASONS


@pytest.mark.parametrize("case", _STRUCTURAL_CASES, ids=_STRUCTURAL_CASE_IDS)
@pytest.mark.parametrize(
    "failure_kind",
    [
        "structural_shape",
        "attempted_shape",
        "quality_aggregate_shape",
        "quality_shape",
        "eligible_shape",
        "structural_overflow",
        "quality_overflow",
        "duplicate_conflict",
        "known_candidate_missing",
    ],
)
def test_malformed_conflicting_overflow_or_incomplete_evidence_fails_closed(
    case, failure_kind
):
    _name, structural_reason, _canonical_reason, _classification, _queue_reason = case
    symbol = "SPY260821C00100000"
    evidence = _evidence(skipped={symbol: structural_reason}, known=[symbol])

    if failure_kind == "structural_shape":
        evidence["structural_skip_records"] = "malformed"
    elif failure_kind == "attempted_shape":
        evidence["attempted_results"] = "malformed"
    elif failure_kind == "quality_aggregate_shape":
        evidence["quality_rejections"] = "malformed"
    elif failure_kind == "quality_shape":
        evidence["quality_rejection_records"] = "malformed"
    elif failure_kind == "eligible_shape":
        evidence["eligible_unattempted_symbols"] = "malformed"
    elif failure_kind == "structural_overflow":
        evidence["structural_skip_records_overflowed"] = True
    elif failure_kind == "quality_overflow":
        evidence["quality_rejection_records_overflowed"] = True
    elif failure_kind == "duplicate_conflict":
        evidence["structural_skip_records"] = [
            {"symbol": symbol, "skip_reason": structural_reason},
            {"symbol": symbol, "skip_reason": MONEY if structural_reason != MONEY else DTE},
        ]
    elif failure_kind == "known_candidate_missing":
        evidence["direct_quote_known_eligible_symbols"] = [
            symbol,
            "SPY260821C00101000",
        ]

    result = resolve_selector_recovery_final_reason(evidence)
    assert result == "UNKNOWN_SELECTOR_RECOVERY_FAILURE"
    assert result not in _STRUCTURAL_CANONICAL_REASONS


def test_reducer_exception_fails_closed_without_resurrecting_moneyness(
    monkeypatch,
):
    import ap.selector_retry_policy as retry_policy

    def _raise_reducer_error(_evidence):
        raise RuntimeError("forced reducer failure")

    monkeypatch.setattr(
        retry_policy,
        "resolve_selector_recovery_final_reason",
        _raise_reducer_error,
    )
    evidence = _evidence(
        skipped={"DDOG260821P00150000": MONEY},
        known=["DDOG260821P00150000"],
        fallback="MONEYNESS_OUT_OF_RANGE",
    )

    assert (
        _resolve_deferred_recovery_final_reason_fail_closed(evidence)
        == "UNKNOWN_SELECTOR_RECOVERY_FAILURE"
    )


def test_reducer_unusable_return_fails_closed(monkeypatch):
    import ap.selector_retry_policy as retry_policy

    monkeypatch.setattr(
        retry_policy,
        "resolve_selector_recovery_final_reason",
        lambda _evidence: None,
    )

    assert (
        _resolve_deferred_recovery_final_reason_fail_closed({
            "fallback_selector_reason": "MONEYNESS_OUT_OF_RANGE",
        })
        == "UNKNOWN_SELECTOR_RECOVERY_FAILURE"
    )


def test_ordinary_known_reason_parity_is_unchanged():
    assert resolve_selector_recovery_final_reason({
        "attempted_results": {"CRM260821P00172500": RETRYABLE},
        "quality_rejections": {},
        "eligible_unattempted_symbols": [],
    }) == RETRYABLE


def test_live_paper_identity_is_not_mutated():
    for mode in ("live", "paper"):
        evidence = _evidence(
            skipped={"WDAY260821C00220000": MONEY},
            attempted={"WDAY260821C00217500": RETRYABLE},
            known=["WDAY260821C00217500", "WDAY260821C00220000"],
            fallback=RETRYABLE,
        )
        evidence["identity"] = {
            "client_id": "jasoncosby1@gmail.com",
            "execution_mode": mode,
            "signal_id": "production-shaped-signal",
        }
        before = copy.deepcopy(evidence)
        resolve_selector_recovery_final_reason(evidence)
        assert evidence == before


def test_scope_adds_no_broker_submit_or_cancel_authority():
    source = Path(__file__).parents[1] / "ap" / "selector_retry_policy.py"
    text = source.read_text(encoding="utf-8")
    assert "submit_order" not in text
    assert "cancel_order" not in text
    assert "proof_trades" not in text
