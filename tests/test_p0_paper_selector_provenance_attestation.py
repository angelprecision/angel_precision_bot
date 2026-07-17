from __future__ import annotations

from types import SimpleNamespace

import ap.paper_selector_provenance_guard as guard


def _selector(*, mode="paper", data_base="https://api.tradier.com", execution_base="https://sandbox.tradier.com"):
    return SimpleNamespace(
        mode=mode,
        data_broker=SimpleNamespace(base_url=data_base),
        broker=SimpleNamespace(base_url=execution_base),
    )


def test_paper_attestation_requires_live_data_and_sandbox_execution() -> None:
    attestation = guard.selector_transport_attestation(_selector())
    assert attestation["selector_execution_mode"] == "paper"
    assert attestation["selector_data_domain"] == "live"
    assert attestation["selector_execution_broker_domain"] == "sandbox"
    assert attestation["selector_data_order_transport_separated"] is True
    assert attestation["selector_provenance_valid"] is True
    assert attestation["quote_source"] == "tradier_live"


def test_paper_sandbox_data_is_invalid_even_when_order_broker_is_sandbox() -> None:
    attestation = guard.selector_transport_attestation(
        _selector(data_base="https://sandbox.tradier.com")
    )
    assert attestation["selector_data_domain"] == "sandbox"
    assert attestation["selector_provenance_valid"] is False


def test_paper_same_object_transport_is_invalid() -> None:
    broker = SimpleNamespace(base_url="https://api.tradier.com")
    selector = SimpleNamespace(mode="paper", data_broker=broker, broker=broker)
    attestation = guard.selector_transport_attestation(selector)
    assert attestation["selector_data_order_transport_separated"] is False
    assert attestation["selector_provenance_valid"] is False


def test_strict_domain_check_blocks_unknown_and_sandbox() -> None:
    for audit in (
        {},
        {"tradier_base_url": "https://sandbox.tradier.com", "quote_source": "tradier_sandbox"},
        {"tradier_base_url": "", "quote_source": "tradier_live"},
    ):
        blocked, reason = guard.strict_check_paper_selector_data_domain(
            is_paper=True,
            selector_audit=audit,
            broker_base_url="https://sandbox.tradier.com",
        )
        assert blocked is True
        assert reason == guard.PAPER_SELECTOR_PROVENANCE_BLOCKED


def test_strict_domain_check_accepts_attested_live_selector() -> None:
    blocked, reason = guard.strict_check_paper_selector_data_domain(
        is_paper=True,
        selector_audit={
            "tradier_base_url": "https://api.tradier.com",
            "quote_source": "tradier_live",
            "selector_provenance_valid": True,
        },
        broker_base_url="https://sandbox.tradier.com",
    )
    assert blocked is False
    assert reason is None


def test_domain_fields_never_substitute_execution_broker_for_missing_selector() -> None:
    fields = guard.truthful_build_paper_domain_fields(
        selector_audit={},
        broker_base_url="https://sandbox.tradier.com",
    )
    assert fields["paper_selector_base_url"] == ""
    assert fields["paper_selector_data_domain"] == "unknown"
    assert fields["paper_order_broker_domain"] == "sandbox"
    assert fields["paper_selector_provenance_valid"] is False
    assert fields["paper_data_order_domain_expected_split"] is False


def test_expected_live_data_sandbox_order_split_is_explicit_not_an_error() -> None:
    fields = guard.truthful_build_paper_domain_fields(
        selector_audit={
            "tradier_base_url": "https://api.tradier.com",
            "quote_source": "tradier_live",
            "selector_provenance_valid": True,
        },
        broker_base_url="https://sandbox.tradier.com",
    )
    assert fields["paper_selector_data_domain"] == "live"
    assert fields["paper_order_broker_domain"] == "sandbox"
    assert fields["paper_data_order_domain_mismatch"] is True
    assert fields["paper_data_order_domain_expected_split"] is True
    assert fields["paper_selector_provenance_valid"] is True


def test_select_wrapper_stamps_successful_selection() -> None:
    result = SimpleNamespace(candidate_audit={}, selection_diagnostics={})
    selector = _selector()
    selector._set_last_failure = lambda value: None
    plan = {"metadata": {}}
    wrapped = guard.wrap_select(lambda self, plan, *args, **kwargs: result)

    returned = wrapped(selector, plan)
    assert returned is result
    assert result.candidate_audit["tradier_base_url"] == "https://api.tradier.com"
    assert result.candidate_audit["selector_provenance_valid"] is True
    assert plan["metadata"]["paper_selector_provenance_valid"] is True
    assert result.selection_diagnostics["transport_attestation"]["quote_source"] == "tradier_live"


def test_select_wrapper_fails_closed_before_submit_on_bad_paper_transport() -> None:
    result = SimpleNamespace(candidate_audit={}, selection_diagnostics={})
    failures = []
    selector = _selector(data_base="https://sandbox.tradier.com")
    selector._set_last_failure = failures.append
    plan = {"metadata": {}}
    wrapped = guard.wrap_select(lambda self, plan, *args, **kwargs: result)

    assert wrapped(selector, plan) is None
    assert failures[-1]["reason_code"] == "PAPER_SELECTOR_PROVENANCE_BLOCKED"
    assert plan["metadata"]["selector_failure"]["reason_code"] == "PAPER_SELECTOR_PROVENANCE_BLOCKED"


def test_live_selector_requires_production_data_but_not_sandbox_execution() -> None:
    attestation = guard.selector_transport_attestation(
        _selector(mode="live", execution_base="https://api.tradier.com")
    )
    assert attestation["selector_provenance_valid"] is True
