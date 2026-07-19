from __future__ import annotations

import math
from datetime import datetime, timezone
from types import MappingProxyType, SimpleNamespace

import pytest

from ap_entry_confirmation import (
    check_entry_confirmation,
    parse_confirmation_bool,
    resolve_entry_confirmation_requirement,
)
from ap.underlying_confirmation_entry_guard import (
    DIRECTION_NOT_CONFIRMED,
    check_underlying_confirmation,
    requires_underlying_confirmation,
)


def _plan(metadata=None, *, side="CALL"):
    return SimpleNamespace(metadata={} if metadata is None else metadata, side=side)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (True, True), (1, True), ("1", True), ("true", True),
        ("yes", True), ("on", True), (False, False), (0, False),
        ("0", False), ("false", False), ("no", False), ("off", False),
        ("disabled", None), (2, None), (math.nan, None), (None, None),
    ],
)
def test_strict_confirmation_boolean_parser(raw, expected):
    assert parse_confirmation_bool(raw) is expected


@pytest.mark.parametrize(
    ("mode", "metadata", "live_default", "required", "source", "conflict", "malformed"),
    [
        ("live", {}, True, True, "live_default_required", False, False),
        ("live", {"confirmation_required": True}, True, True, "live_default_plus_explicit", False, False),
        ("live", {"hybrid_client_quality_gate": {"confirmation_required": True}}, True, True, "live_default_plus_explicit", False, False),
        ("live", {"confirmation_required": False}, True, True, "live_default_required", False, False),
        ("live", {}, False, False, "live_break_glass_default_disabled", False, False),
        ("live", {"confirmation_required": True}, False, True, "top_level_metadata", False, False),
        ("live", {"hybrid_client_quality_gate": {"confirmation_required": True}}, False, True, "legacy_nested_metadata", False, False),
        ("live", {"confirmation_required": True, "hybrid_client_quality_gate": {"confirmation_required": True}}, True, True, "live_default_plus_explicit", False, False),
        ("paper", {"confirmation_required": False, "hybrid_client_quality_gate": {"confirmation_required": False}}, True, False, "top_level_metadata", False, False),
        ("paper", {"confirmation_required": True, "hybrid_client_quality_gate": {"confirmation_required": False}}, True, True, "metadata_conflict_fail_closed", True, False),
        ("paper", {"confirmation_required": False, "hybrid_client_quality_gate": {"confirmation_required": True}}, True, True, "metadata_conflict_fail_closed", True, False),
        ("live", {"confirmation_required": "garbage"}, True, True, "live_default_required", False, True),
        ("paper", {"confirmation_required": "garbage"}, True, False, "malformed_metadata_ignored", False, True),
        ("paper", {"confirmation_required": "false"}, True, False, "top_level_metadata", False, False),
        ("paper", {"confirmation_required": "true"}, True, True, "top_level_metadata", False, False),
    ],
)
def test_requirement_resolution(mode, metadata, live_default, required, source, conflict, malformed):
    result = resolve_entry_confirmation_requirement(
        _plan(metadata), execution_mode=mode, live_default_required=live_default
    )
    assert (result.required, result.source) == (required, source)
    assert result.conflict is conflict
    assert result.malformed is malformed


def test_requirement_supports_dict_object_nonmapping_and_does_not_mutate():
    metadata = {"confirmation_required": "true"}
    before = dict(metadata)
    assert resolve_entry_confirmation_requirement(
        {"metadata": metadata}, execution_mode="paper"
    ).required is True
    assert resolve_entry_confirmation_requirement(
        _plan(metadata), execution_mode="paper"
    ).required is True
    malformed = resolve_entry_confirmation_requirement(
        _plan("not-a-mapping"), execution_mode="live", live_default_required=True
    )
    assert malformed.required is True
    assert metadata == before


def _confirm(**overrides):
    values = dict(
        plan=_plan({"confirmation_required": True}),
        direction="CALL",
        trigger_price=100.0,
        live_bid=1.00,
        live_ask=1.02,
        live_quote_age_ms=1000,
        underlying_last=100.5,
        underlying_quote_age_ms=1000,
        decision_option_price=1.01,
        score=78.0,
        tier="A",
        timeframe="1d",
        sandbox_mode=False,
        execution_mode="live",
        live_default_required=True,
    )
    values.update(overrides)
    return check_entry_confirmation(**values)


def test_valid_required_live_evidence_passes_with_diagnostics():
    result = _confirm()
    assert result.passed is True
    assert result.metadata["confirmation_requirement_source"] == "live_default_plus_explicit"
    assert result.metadata["confirmation_execution_mode"] == "live"
    assert result.metadata["live_entry_bid"] == 1.0
    assert result.metadata["underlying_quote_age_seconds"] == 1.0
    assert result.metadata["underlying_quote_max_age_seconds"] == 10.0


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"live_bid": None, "live_ask": None}, "entry_confirm_failed_missing_quote"),
        ({"live_ask": None}, "entry_confirm_failed_missing_quote"),
        ({"live_bid": None}, "entry_confirm_failed_missing_quote"),
        ({"live_bid": 0}, "entry_confirm_failed_invalid_quote"),
        ({"live_ask": 0}, "entry_confirm_failed_invalid_quote"),
        ({"live_bid": -1}, "entry_confirm_failed_invalid_quote"),
        ({"live_bid": math.nan}, "entry_confirm_failed_invalid_quote"),
        ({"live_ask": math.inf}, "entry_confirm_failed_invalid_quote"),
        ({"live_bid": "bad"}, "entry_confirm_failed_invalid_quote"),
        ({"live_bid": 1.2, "live_ask": 1.0}, "entry_confirm_failed_crossed_quote"),
        ({"live_quote_age_ms": None}, "entry_confirm_failed_missing_quote_age"),
        ({"live_quote_age_ms": "bad"}, "entry_confirm_failed_invalid_quote_age"),
        ({"live_quote_age_ms": -1}, "entry_confirm_failed_invalid_quote_age"),
        ({"live_quote_age_ms": math.inf}, "entry_confirm_failed_invalid_quote_age"),
        ({"live_quote_age_ms": 11_000}, "entry_confirm_failed_stale_quote"),
        ({"underlying_quote_age_ms": None}, "entry_confirm_failed_missing_underlying_quote_age"),
        ({"underlying_quote_age_ms": "bad"}, "entry_confirm_failed_invalid_underlying_quote_age"),
        ({"underlying_quote_age_ms": -1}, "entry_confirm_failed_invalid_underlying_quote_age"),
        ({"underlying_quote_age_ms": math.inf}, "entry_confirm_failed_invalid_underlying_quote_age"),
        ({"underlying_quote_age_ms": 11_000}, "entry_confirm_failed_stale_underlying_quote"),
        ({"live_bid": 0.8, "live_ask": 1.2}, "entry_confirm_failed_spread"),
        ({"live_bid": 0.89, "live_ask": 0.91}, "entry_confirm_failed_option_fade"),
        ({"underlying_last": 99.0}, "entry_confirm_failed_underlying_reversal"),
        ({"direction": "PUT", "underlying_last": 101.0}, "entry_confirm_failed_underlying_reversal"),
        ({"decision_option_price": None}, "entry_confirm_failed_missing_decision_price"),
        ({"decision_option_price": 0}, "entry_confirm_failed_invalid_decision_price"),
        ({"trigger_price": None}, "entry_confirm_failed_missing_trigger"),
        ({"underlying_last": None}, "entry_confirm_failed_missing_underlying"),
        ({"direction": ""}, "entry_confirm_failed_invalid_direction"),
    ],
)
def test_required_live_evidence_fails_closed(overrides, reason):
    result = _confirm(**overrides)
    assert result.passed is False
    assert result.fail_reason == reason


def test_thresholds_remain_existing_values(monkeypatch):
    monkeypatch.setenv("CLIENT_PROOF_MAX_SPREAD_PCT", "0.10")
    monkeypatch.setenv("MAX_PRE_ENTRY_OPTION_FADE_PCT", "8")
    monkeypatch.setenv("MAX_PRE_ENTRY_UNDERLYING_REVERSAL_PCT", "0.25")
    assert _confirm(live_bid=0.96, live_ask=1.04).passed is True
    blocked = _confirm(live_bid=0.94, live_ask=1.06)
    assert blocked.fail_reason == "entry_confirm_failed_spread"
    assert blocked.metadata["max_spread_pct"] == 0.10


def test_paper_without_request_keeps_fast_path():
    result = _confirm(
        plan=_plan({}), execution_mode="paper", live_default_required=True,
        live_bid=None, live_ask=None, live_quote_age_ms=None,
        decision_option_price=None, trigger_price=None, underlying_last=None,
        underlying_quote_age_ms=None,
    )
    assert result.passed is True
    assert result.metadata["confirmation_required"] is False


def test_underlying_age_cannot_borrow_option_quote_age():
    result = _confirm(live_quote_age_ms=1000, underlying_quote_age_ms=15_000)
    assert result.passed is False
    assert result.fail_reason == "entry_confirm_failed_stale_underlying_quote"
    assert result.metadata["quote_age_seconds"] == 1.0
    assert result.metadata["underlying_quote_age_seconds"] == 15.0
    assert result.metadata["underlying_quote_age_status"] == "stale"


def _underlying_plan(*, side="CALL", required=True):
    trigger, stop, target = (100.0, 95.0, 110.0) if side == "CALL" else (100.0, 105.0, 90.0)
    return {
        "ticker": "AAPL", "side": side, "trigger_price": trigger,
        "stop_price": stop, "target_price": target,
        "metadata": {"confirmation_required": required},
    }


def test_underlying_guard_default_off(monkeypatch):
    monkeypatch.delenv("UNDERLYING_CONFIRMATION_ENTRY_GUARD_ENABLED", raising=False)
    assert requires_underlying_confirmation(plan=_underlying_plan()) is False


@pytest.mark.parametrize("raw", [False, "false", "0", "no", "off"])
def test_underlying_guard_strict_false(monkeypatch, raw):
    monkeypatch.setenv("UNDERLYING_CONFIRMATION_ENTRY_GUARD_ENABLED", "1")
    assert requires_underlying_confirmation(plan=_underlying_plan(required=raw)) is False


@pytest.mark.parametrize(
    ("side", "current", "passed"),
    [("CALL", 99.0, False), ("PUT", 101.0, False), ("CALL", 101.0, True), ("PUT", 99.0, True)],
)
def test_underlying_guard_directional_result(monkeypatch, side, current, passed):
    monkeypatch.setenv("UNDERLYING_CONFIRMATION_ENTRY_GUARD_ENABLED", "1")
    now = datetime.now(timezone.utc)
    plan = _underlying_plan(side=side)
    plan["metadata"]["underlying_quote"] = {"price": current, "timestamp": now.isoformat()}
    result = check_underlying_confirmation(
        plan=plan, client_id="client@example.com", execution_mode="live", now=now
    )
    assert result.passed is passed
    assert result.metadata["client_id"] == "client@example.com"
    assert result.metadata["execution_mode"] == "live"
    if not passed:
        assert result.reason == DIRECTION_NOT_CONFIRMED


def test_immutable_metadata_is_supported_without_mutation():
    metadata = MappingProxyType({"confirmation_required": True})
    result = _confirm(plan=_plan(metadata), live_quote_age_ms=11_000)
    assert result.fail_reason == "entry_confirm_failed_stale_quote"
    assert dict(metadata) == {"confirmation_required": True}


def _production_result(monkeypatch, **kwargs):
    from tests.test_execution_core_entry_confirmation import _run_entry_trigger

    return _run_entry_trigger(
        monkeypatch,
        mode="off",
        execution_mode="live",
        **kwargs,
    )


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        ({"plan_metadata": {"confirmation_required": True}, "quote_age_ms": 11_000}, "entry_confirm_failed_stale_quote"),
        ({"plan_metadata": {}, "quote_age_ms": 11_000}, "entry_confirm_failed_stale_quote"),
        ({"plan_metadata": MappingProxyType({}), "quote_age_ms": 11_000}, "entry_confirm_failed_stale_quote"),
        ({"submit_bid": None, "submit_ask": 1.02}, "entry_confirm_failed_missing_quote"),
        ({"submit_bid": 0.94, "submit_ask": 1.06}, "entry_confirm_failed_spread"),
        ({"submit_bid": 0.89, "submit_ask": 0.91}, "entry_confirm_failed_option_fade"),
        ({"underlying_last": 99.0}, "entry_confirm_failed_underlying_reversal"),
        ({"plan_side": "PUT", "underlying_last": 101.0}, "entry_confirm_failed_underlying_reversal"),
        ({"quote_age_ms": None}, "entry_confirm_failed_missing_quote_age"),
        ({"underlying_quote_age_ms": None}, "entry_confirm_failed_missing_underlying_quote_age"),
        ({"underlying_quote_age_ms": 11_000}, "entry_confirm_failed_stale_underlying_quote"),
    ],
)
def test_real_live_entry_trigger_blocks_before_submit(monkeypatch, kwargs, reason):
    result = _production_result(monkeypatch, **kwargs)
    result["osm"].submit_existing_entry.assert_not_called()
    result["osm"].expire_pending_entry.assert_called_once_with("local-1", reason=reason)
    blocked = [
        call.args[1]
        for call in result["store"].update_signal_fields.call_args_list
        if call.args[1].get("decision_status") == "blocked_at_breach"
    ]
    assert blocked and blocked[-1]["context_notes"] == reason
    assert result["ledger_events"][-1][1]["miss_reason"] == reason


def test_real_live_valid_confirmation_reaches_submit_once(monkeypatch):
    result = _production_result(
        monkeypatch, plan_metadata={"confirmation_required": True}
    )
    result["osm"].submit_existing_entry.assert_called_once()
    result["osm"].expire_pending_entry.assert_not_called()


def test_real_paper_unrequested_confirmation_preserves_submit(monkeypatch):
    from tests.test_execution_core_entry_confirmation import _run_entry_trigger

    result = _run_entry_trigger(
        monkeypatch,
        mode="off",
        execution_mode="paper",
        confirmation_required=False,
        plan_metadata={},
    )
    result["osm"].submit_existing_entry.assert_called_once()
