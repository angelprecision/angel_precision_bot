"""Focused P1 coverage for the current-main BREACH identity owner."""

from __future__ import annotations

import copy
import ast
import importlib
import inspect
import os
import sys
from collections import OrderedDict
from pathlib import Path

import pytest

os.environ["INTELLIGENCE_CONTEXT_STORE_BACKEND"] = "memory"

from ap.intelligence_breach_snapshot_assembly import (  # noqa: E402
    ASSEMBLY_VERSION,
    build_breach_snapshot_envelope,
    hash_breach_identity,
    hash_breach_snapshot_input,
    normalize_breach_identity,
    project_breach_evidence,
    validate_breach_parent_snapshot,
)
import ap.intelligence_breach_snapshot_assembly as breach_owner  # noqa: E402


AS_OF = "2026-09-11T17:30:00+00:00"
PROFILE = "profile-breach-v1"
GENERATION = 7
REPO_ROOT = Path(__file__).resolve().parents[1]


def _signal(**updates: object) -> dict:
    value = {
        "client_id": "client@example.com",
        "execution_mode": "PAPER",
        "signal_id": "signal-breach-1",
        # Plain signal IDs canonicalize to themselves; distinct canonical
        # values are covered by the REEVAL normalization tests below.
        "canonical_signal_id": "signal-breach-1",
        "ticker": "SPY",
        "side": "CALL",
        "trigger_crossed_at": AS_OF,
        "materialization_generation": GENERATION,
        "profile_version": PROFILE,
        "model_version": "model-435b-v1",
        "trigger_price": 500.0,
    }
    value.update(updates)
    return value


def _identity(**updates: object) -> dict:
    selected_profile = str(updates.pop("profile_version", PROFILE))
    result = normalize_breach_identity(_signal(**updates), profile_version=selected_profile)
    assert result["ok"], result
    return result


def _parent(
    phase: str,
    *,
    snapshot_id: str = "snapshot-1",
    client_id: str = "client@example.com",
    execution_mode: str = "PAPER",
    canonical_signal_id: str = "signal-breach-1",
    signal_id: str = "signal-breach-1",
    ticker: str = "SPY",
    side: str = "CALL",
    local_order_id: str = "",
    generation: int | None = GENERATION,
    profile_version: str = PROFILE,
    status: str | None = "COMPLETE",
    context_revision: int | None = 1,
    parent_snapshot_id: str | None = None,
) -> dict:
    signal = _signal(
        client_id=client_id,
        execution_mode=execution_mode,
        signal_id=signal_id,
        canonical_signal_id=canonical_signal_id,
        ticker=ticker,
        side=side,
        local_order_id=local_order_id if phase == "PREOPEN" else "",
        materialization_generation=generation,
        profile_version=profile_version,
    )
    payload = {
        "phase": phase,
        "signal": signal,
        "client_id": client_id,
        "execution_mode": execution_mode,
        "canonical_signal_id": canonical_signal_id,
        "local_order_id": local_order_id if phase == "PREOPEN" else "",
        "ticker": ticker,
        "side": side,
        "profile_version": profile_version,
        "status": status,
        "parent_snapshot_id": parent_snapshot_id,
        "observe_only": True,
        "affected_eligibility": False,
    }
    if generation is not None:
        payload["materialization_generation"] = generation
    return {
        "id": snapshot_id,
        "client_id": client_id,
        "execution_mode": execution_mode,
        "signal_id": signal_id,
        "canonical_signal_id": canonical_signal_id,
        "local_order_id": local_order_id if phase == "PREOPEN" else "",
        "phase": phase,
        "context_revision": context_revision,
        "profile_version": profile_version,
        "status": status,
        "parent_snapshot_id": parent_snapshot_id,
        "payload": payload,
    }


def _evidence(*, collected_at: str = "2026-09-12T00:00:00+00:00") -> dict:
    return {
        "phase": "BREACH",
        "as_of": AS_OF,
        "collected_at": collected_at,
        "data_sources": {"candles": {"5m": [], "15m": [], "1h": [], "4h": []}},
        "underlying_observation": {
            "price": 501.25,
            "source": "signal.breach_price",
            "as_of": AS_OF,
        },
        "provenance": {"intraday": "signal_frozen_15min"},
        "errors": [],
    }


def _envelope(*, evidence: dict | None = None, **kwargs: object) -> dict:
    pretrigger_snapshot = kwargs.pop(
        "pretrigger_snapshot", _parent("PRETRIGGER", snapshot_id="pretrigger-1")
    )
    preopen_snapshot = kwargs.pop(
        "preopen_snapshot",
        _parent("PREOPEN", snapshot_id="preopen-1", local_order_id="local-order-1"),
    )
    return build_breach_snapshot_envelope(
        _identity(local_order_id="local-order-1"),
        evidence if evidence is not None else _evidence(),
        pretrigger_snapshot=pretrigger_snapshot,
        preopen_snapshot=preopen_snapshot,
        **kwargs,
    )


def test_live_and_paper_identity_are_canonical_and_isolated():
    live = _identity(execution_mode="live")
    paper = _identity(execution_mode="paper")
    assert live["execution_mode"] == "LIVE"
    assert paper["execution_mode"] == "PAPER"
    assert live["ticker"] == paper["ticker"] == "SPY"
    assert hash_breach_identity(live) != hash_breach_identity(paper)


def test_same_ticker_across_clients_remains_isolated():
    first = _identity(client_id="first@example.com")
    second = _identity(client_id="second@example.com")
    assert first["ticker"] == second["ticker"]
    assert hash_breach_identity(first) != hash_breach_identity(second)


def _reeval_signal(**updates: object) -> dict:
    bare = "603bce5b-352e-44e0-b00b-6baa826ca2c9"
    value = _signal(
        signal_id=f"REEVAL:{bare}:deadbeef",
        canonical_signal_id=f"REEVAL:{bare}",
    )
    value.update(updates)
    return value


def test_reeval_canonical_helper_success_strips_suffix():
    result = normalize_breach_identity(_reeval_signal())
    assert result["ok"] is True
    assert result["canonical_signal_id"] == "REEVAL:603bce5b-352e-44e0-b00b-6baa826ca2c9"


def test_canonical_helper_exception_fails_closed(monkeypatch):
    import ap_canonical_signal

    def fail(*_args: object, **_kwargs: object) -> str:
        raise RuntimeError("canonical helper unavailable")

    monkeypatch.setattr(ap_canonical_signal, "build_canonical_signal_id", fail)
    result = normalize_breach_identity(_reeval_signal())
    assert result["ok"] is False
    assert result["identity"] is None
    assert "canonical_signal_derivation_failed" in result["errors"]
    assert "REEVAL:603bce5b-352e-44e0-b00b-6baa826ca2c9" not in str(result)


def test_explicit_canonical_must_match_raw_signal_derivation():
    matching = normalize_breach_identity(_reeval_signal())
    conflicting = normalize_breach_identity(
        _reeval_signal(canonical_signal_id="REEVAL:603bce5b-352e-44e0-b00b-6baa826ca2c9:other")
    )
    plain = normalize_breach_identity(_signal(canonical_signal_id="signal-breach-1"))
    assert matching["ok"] is True
    assert conflicting["ok"] is False
    assert "canonical_signal_id_mismatch" in conflicting["errors"]
    assert plain["ok"] is True


@pytest.mark.parametrize(
    ("field", "value", "error_fragment"),
    [
        ("client_id", "other@example.com", "client_id_mismatch"),
        ("execution_mode", "LIVE", "execution_mode_mismatch"),
        ("canonical_signal_id", "other-canonical", "canonical_signal_id_mismatch"),
        ("ticker", "QQQ", "ticker_mismatch"),
        ("side", "PUT", "side_mismatch"),
        ("profile_version", "profile-other", "profile_version_mismatch"),
    ],
)
def test_incompatible_parent_identity_is_rejected(field, value, error_fragment):
    identity = _identity()
    parent_values = {
        "client_id": identity["client_id"],
        "execution_mode": identity["execution_mode"],
        "canonical_signal_id": identity["canonical_signal_id"],
        "ticker": identity["ticker"],
        "side": identity["side"],
        "profile_version": identity["profile_version"],
    }
    parent_values[field] = value
    result = validate_breach_parent_snapshot(
        _parent("PREOPEN", local_order_id="local-order-1", **parent_values),
        identity=identity,
        phase="PREOPEN",
    )
    assert result["ok"] is False
    assert result["accepted"] is False
    assert result["status"] == "REJECTED"
    assert any(error_fragment in error for error in result["errors"])
    assert result["snapshot_id"] is None


def test_local_order_and_generation_are_exact_parent_authority():
    identity = _identity(local_order_id="local-order-1")
    wrong_local = validate_breach_parent_snapshot(
        _parent("PREOPEN", local_order_id="local-order-2"),
        identity=identity,
        phase="PREOPEN",
    )
    wrong_generation = validate_breach_parent_snapshot(
        _parent("PREOPEN", local_order_id="local-order-1", generation=8),
        identity=identity,
        phase="PREOPEN",
    )
    assert wrong_local["accepted"] is False
    assert any("local_order_id_mismatch" in error for error in wrong_local["errors"])
    assert wrong_generation["accepted"] is False
    assert any("generation_mismatch" in error for error in wrong_generation["errors"])


@pytest.mark.parametrize("status", ["ERROR", "UNAVAILABLE", "PARTIAL", "STALE", None])
def test_non_complete_parent_status_is_unproven_not_accepted(status):
    result = validate_breach_parent_snapshot(
        _parent("PREOPEN", local_order_id="local-order-1", status=status),
        identity=_identity(local_order_id="local-order-1"),
        phase="PREOPEN",
    )
    assert result["ok"] is True
    assert result["accepted"] is False
    assert result["proven"] is False
    assert result["status"] == "UNPROVEN"
    assert result["candidate_snapshot_id"] == "snapshot-1"
    assert result["authoritative_parent_snapshot_id"] is None


def test_missing_generation_authority_is_unproven_and_retains_candidate_id():
    result = validate_breach_parent_snapshot(
        _parent("PREOPEN", local_order_id="local-order-1", generation=None),
        identity=_identity(local_order_id="local-order-1"),
        phase="PREOPEN",
    )
    assert result["ok"] is True
    assert result["accepted"] is False
    assert result["status"] == "UNPROVEN"
    assert result["candidate_snapshot_id"] == "snapshot-1"
    assert result["authoritative_parent_snapshot_id"] is None
    assert "PREOPEN_materialization_generation_unavailable" in result["warnings"]


def test_context_revision_is_bookkeeping_not_semantic_identity_or_parent_equality():
    first = _identity(context_revision=1)
    second = _identity(context_revision=99)
    assert first["context_revision"] == 1
    assert second["context_revision"] == 99
    assert "context_revision" not in first["identity"]
    assert hash_breach_identity(first) == hash_breach_identity(second)
    assert hash_breach_snapshot_input(first, _evidence()) == hash_breach_snapshot_input(
        second, _evidence()
    )
    pretrigger = _parent("PRETRIGGER", snapshot_id="pretrigger-revision", context_revision=1)
    preopen = _parent(
        "PREOPEN",
        snapshot_id="preopen-revision",
        local_order_id="local-order-1",
        context_revision=99,
        parent_snapshot_id="pretrigger-revision",
    )
    result = build_breach_snapshot_envelope(
        _identity(local_order_id="local-order-1"),
        _evidence(),
        pretrigger_snapshot=pretrigger,
        preopen_snapshot=preopen,
    )
    assert result["ok"] is True
    assert result["parent_lineage"]["status"] == "PROVEN"
    assert not any("context_revision" in error for error in result["errors"])


def test_exact_pretrigger_and_preopen_parents_are_accepted():
    identity = _identity(local_order_id="local-order-1")
    pretrigger = validate_breach_parent_snapshot(
        _parent("PRETRIGGER", snapshot_id="pretrigger-exact"),
        identity=identity,
        phase="PRETRIGGER",
    )
    preopen = validate_breach_parent_snapshot(
        _parent("PREOPEN", snapshot_id="preopen-exact", local_order_id="local-order-1"),
        identity=identity,
        phase="PREOPEN",
    )
    assert pretrigger["accepted"] is True
    assert preopen["accepted"] is True
    assert pretrigger["snapshot_id"] == "pretrigger-exact"
    assert preopen["snapshot_id"] == "preopen-exact"


def test_missing_optional_parent_is_explicit_partial_and_never_invented():
    result = build_breach_snapshot_envelope(_identity(), _evidence())
    assert result["ok"] is True
    assert result["status"] == "PARTIAL"
    assert result["parent_snapshot_ids"] == {"PRETRIGGER": None, "PREOPEN": None}
    assert set(result["missing_parent_phases"]) == {"PRETRIGGER", "PREOPEN"}
    assert "PRETRIGGER_parent_missing" in result["warnings"]
    assert "PREOPEN_parent_missing" in result["warnings"]


def test_wrong_parent_is_rejected_and_not_silently_consumed():
    result = build_breach_snapshot_envelope(
        _identity(),
        _evidence(),
        preopen_snapshot=_parent(
            "PREOPEN", snapshot_id="wrong-parent", local_order_id="local-order-1", ticker="QQQ"
        ),
    )
    assert result["ok"] is False
    assert result["status"] == "REJECTED"
    assert result["parent_snapshot_ids"]["PREOPEN"] is None
    assert result["parent_validation"]["PREOPEN"]["accepted"] is False
    assert any("PREOPEN_ticker_mismatch" in error for error in result["errors"])


@pytest.mark.parametrize(
    ("field", "value", "error_fragment"),
    [
        ("ticker", "QQQ", "breach_evidence_ticker_mismatch"),
        ("execution_mode", "LIVE", "breach_evidence_execution_mode_mismatch"),
        ("canonical_signal_id", "canonical-other", "breach_evidence_canonical_signal_id_mismatch"),
    ],
)
def test_present_evidence_identity_conflict_is_rejected(field, value, error_fragment):
    evidence = _evidence()
    evidence[field] = value
    result = _envelope(evidence=evidence)
    assert result["ok"] is False
    assert result["status"] == "REJECTED"
    assert error_fragment in result["errors"]


@pytest.mark.parametrize(
    ("field", "value", "error_fragment"),
    [
        ("side", "PUT", "breach_structure_side_mismatch"),
        ("materialization_generation", 8, "breach_structure_materialization_generation_mismatch"),
    ],
)
def test_present_structure_identity_conflict_is_rejected(field, value, error_fragment):
    structure = {
        "schema_version": "breach_market_structure_v1",
        "observe_only": True,
        "affected_eligibility": False,
        field: value,
    }
    result = _envelope(structure=structure)
    assert result["ok"] is False
    assert result["status"] == "REJECTED"
    assert error_fragment in result["errors"]


def test_missing_optional_evidence_identity_field_is_not_a_mismatch():
    result = _envelope(evidence=_evidence())
    assert result["ok"] is True
    assert not any("ticker_mismatch" in error for error in result["errors"])


def test_parent_ids_are_exact_and_hash_is_restart_stable():
    first = _envelope()
    restarted = _envelope()
    assert first["parent_snapshot_ids"] == {
        "PRETRIGGER": "pretrigger-1",
        "PREOPEN": "preopen-1",
    }
    assert first["identity_hash"] == restarted["identity_hash"]
    assert first["input_hash"] == restarted["input_hash"]


def test_preopen_lineage_requires_exact_pretrigger_pointer():
    correct = _envelope(
        pretrigger_snapshot=_parent("PRETRIGGER", snapshot_id="pretrigger-exact"),
        preopen_snapshot=_parent(
            "PREOPEN",
            snapshot_id="preopen-exact",
            local_order_id="local-order-1",
            parent_snapshot_id="pretrigger-exact",
        ),
    )
    wrong = _envelope(
        pretrigger_snapshot=_parent("PRETRIGGER", snapshot_id="pretrigger-exact"),
        preopen_snapshot=_parent(
            "PREOPEN",
            snapshot_id="preopen-exact",
            local_order_id="local-order-1",
            parent_snapshot_id="pretrigger-other",
        ),
    )
    assert correct["parent_lineage"]["status"] == "PROVEN"
    assert correct["status"] == "PARTIAL"  # structure remains reserved for #621
    assert wrong["status"] == "REJECTED"
    assert "PREOPEN_parent_snapshot_id_mismatch" in wrong["errors"]


def test_unrelated_valid_pretrigger_and_unresolved_lineage_are_not_consumed():
    unrelated = _envelope(
        pretrigger_snapshot=_parent("PRETRIGGER", snapshot_id="pretrigger-a"),
        preopen_snapshot=_parent(
            "PREOPEN",
            snapshot_id="preopen-a",
            local_order_id="local-order-1",
            parent_snapshot_id="pretrigger-b",
        ),
    )
    unresolved = _envelope(
        pretrigger_snapshot=None,
        preopen_snapshot=_parent(
            "PREOPEN",
            snapshot_id="preopen-unresolved",
            local_order_id="local-order-1",
            parent_snapshot_id="pretrigger-missing",
        ),
    )
    assert unrelated["status"] == "REJECTED"
    assert unresolved["ok"] is True
    assert unresolved["status"] == "PARTIAL"
    assert unresolved["parent_lineage"]["status"] == "UNPROVEN"
    assert unresolved["candidate_parent_snapshot_ids"] == {
        "PRETRIGGER": None,
        "PREOPEN": "preopen-unresolved",
    }


def test_dictionary_insertion_order_does_not_change_hash():
    identity = _identity()
    ordered = OrderedDict((key, value) for key, value in identity["identity"].items())
    reversed_order = OrderedDict(reversed(list(ordered.items())))
    evidence = _evidence()
    reordered_evidence = OrderedDict(reversed(list(evidence.items())))
    assert hash_breach_snapshot_input(ordered, evidence) == hash_breach_snapshot_input(
        reversed_order, reordered_evidence
    )


def test_worker_collected_at_is_not_identity_or_hash_input():
    first = _envelope()
    later = build_breach_snapshot_envelope(
        _identity(local_order_id="local-order-1"),
        _evidence(collected_at="2026-09-12T02:00:00+00:00"),
        pretrigger_snapshot=_parent("PRETRIGGER", snapshot_id="pretrigger-1"),
        preopen_snapshot=_parent(
            "PREOPEN", snapshot_id="preopen-1", local_order_id="local-order-1"
        ),
    )
    assert first["identity_hash"] == later["identity_hash"]
    assert first["input_hash"] == later["input_hash"]


def test_arbitrary_current_quote_is_excluded_from_canonical_hash():
    first = _evidence()
    second = _evidence()
    first["current_quote"] = {"price": 1.0, "worker": "a"}
    second["current_quote"] = {"price": 999.0, "worker": "b"}
    assert hash_breach_snapshot_input(_identity(), first) == hash_breach_snapshot_input(
        _identity(), second
    )
    assert "current_quote" not in project_breach_evidence(first)


@pytest.mark.parametrize(
    "mutator",
    [
        lambda value: {**value, "as_of": value["as_of"].replace("17:30:00", "17:31:00")},
        lambda value: {**value, "underlying_observation": {**value["underlying_observation"], "price": 502.25}},
        lambda value: {**value, "data_sources": {**value["data_sources"], "candles": {**value["data_sources"]["candles"], "5m": [{"time": AS_OF, "open": 1.0, "high": 2.0, "low": 1.0, "close": 2.0}]}}},
        lambda value: {**value, "data_sources": {**value["data_sources"], "coverage": {"5m": {"status": "COMPLETE", "coverage_complete": False}}}},
        lambda value: {**value, "provenance": {"intraday": "different-authority"}},
    ],
)
def test_authoritative_decision_time_evidence_changes_hash(mutator):
    first = _evidence()
    changed = mutator(copy.deepcopy(first))
    assert hash_breach_snapshot_input(_identity(), first) != hash_breach_snapshot_input(
        _identity(), changed
    )


def test_frozen_observation_current_price_is_canonical_breach_evidence():
    first = _evidence()
    second = _evidence()
    first["underlying_observation"]["current_price"] = 501.25
    second["underlying_observation"]["current_price"] = 502.25
    assert hash_breach_snapshot_input(_identity(), first) != hash_breach_snapshot_input(
        _identity(), second
    )


@pytest.mark.parametrize("bad_value", [0, 1, "true", "false", None, [], {}])
@pytest.mark.parametrize("field", ["observe_only", "affected_eligibility"])
def test_evidence_safety_flags_require_exact_booleans(field, bad_value):
    evidence = _evidence()
    evidence[field] = bad_value
    result = _envelope(evidence=evidence)
    assert result["ok"] is False
    assert result["status"] == "REJECTED"
    assert any(field in error for error in result["errors"])


@pytest.mark.parametrize("bad_value", [0, 1, "true", "false", None, [], {}])
@pytest.mark.parametrize("field", ["observe_only", "affected_eligibility"])
def test_structure_safety_flags_require_exact_booleans(field, bad_value):
    structure = {
        "schema_version": "breach_market_structure_v1",
        "observe_only": True,
        "affected_eligibility": False,
    }
    structure[field] = bad_value
    result = _envelope(structure=structure)
    assert result["ok"] is False
    assert result["status"] == "REJECTED"
    assert any(field in error for error in result["errors"])


@pytest.mark.parametrize("field", ["observe_only", "affected_eligibility"])
def test_parent_safety_flags_require_exact_booleans(field):
    parent = _parent("PREOPEN", local_order_id="local-order-1")
    parent["payload"][field] = 0
    result = validate_breach_parent_snapshot(
        parent,
        identity=_identity(local_order_id="local-order-1"),
        phase="PREOPEN",
    )
    assert result["accepted"] is False
    assert result["status"] == "REJECTED"
    assert any(field in error for error in result["errors"])


def test_exact_parent_id_changes_hash_even_when_semantics_match():
    identity = _identity()
    evidence = _evidence()
    assert hash_breach_snapshot_input(
        identity, evidence, parent_snapshot_ids={"PRETRIGGER": "parent-a"}
    ) != hash_breach_snapshot_input(
        identity, evidence, parent_snapshot_ids={"PRETRIGGER": "parent-b"}
    )


@pytest.mark.parametrize("field", ["ticker", "side", "materialization_generation"])
def test_semantic_identity_changes_change_hash(field):
    first = _identity()
    changed = _identity(**{field: {"ticker": "QQQ", "side": "PUT", "materialization_generation": 8}[field]})
    assert hash_breach_identity(first) != hash_breach_identity(changed)
    assert hash_breach_snapshot_input(first, _evidence()) != hash_breach_snapshot_input(
        changed, _evidence()
    )


def test_trigger_crossed_at_changes_canonical_evidence_hash():
    first = _identity()
    changed = _identity(trigger_crossed_at="2026-09-11T17:31:00Z")
    assert first["trigger_crossed_at"] != changed["trigger_crossed_at"]
    assert hash_breach_snapshot_input(first, _evidence()) != hash_breach_snapshot_input(
        changed, _evidence()
    )


@pytest.mark.parametrize(
    "timestamp",
    ["2026-09-11T17:30:00", "not-a-timestamp", True, 1726075800],
)
def test_malformed_or_naive_breach_timestamp_fails_closed(timestamp):
    result = normalize_breach_identity(_signal(trigger_crossed_at=timestamp), profile_version=PROFILE)
    assert result["ok"] is False
    assert any("trigger_crossed_at" in error for error in result["errors"])


@pytest.mark.parametrize("mode", ["LIVEISH", "", True, 1])
def test_malformed_execution_mode_never_cross_routes(mode):
    result = normalize_breach_identity(_signal(execution_mode=mode), profile_version=PROFILE)
    assert result["ok"] is False
    assert result["identity"] is None
    assert any("execution_mode" in error for error in result["errors"])


def test_envelope_is_always_observe_only_and_structure_slot_is_reserved():
    result = _envelope()
    assert result["assembly_version"] == ASSEMBLY_VERSION
    assert result["phase"] == "BREACH"
    assert result["observe_only"] is True
    assert result["affected_eligibility"] is False
    assert result["structure"] is None
    assert result["structure_slot"] == {
        "status": "RESERVED_FOR_621",
        "value": None,
    }
    assert result["source_versions"]["profile_version"] == PROFILE


def test_already_frozen_structure_is_passed_through_without_recalculation():
    structure = {
        "schema_version": "breach_market_structure_v1",
        "model_version": "canonical_fvg_435b_v1",
        "observe_only": True,
        "affected_eligibility": False,
        "fvg_zones": [],
    }
    result = _envelope(structure=structure)
    assert result["ok"] is True
    assert result["structure"] == structure
    assert result["structure_slot"]["status"] == "ATTACHED"
    assert result["source_versions"]["structure_schema_version"] == structure["schema_version"]


def test_public_owner_has_behavioral_zero_side_effect_proof(monkeypatch):
    forbidden_calls = (
        ("ap.intelligence_snapshot_store", "write_snapshot"),
        ("ap.intelligence_snapshot_store", "complete_job_with_snapshot"),
        ("ap.intelligence_snapshot_store", "get_latest_snapshot"),
        ("ap.intelligence_snapshot_store", "enqueue_intelligence_job"),
    )

    def fail_if_called(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("forbidden side effect called")

    for module_name, attribute in forbidden_calls:
        module = importlib.import_module(module_name)
        if hasattr(module, attribute):
            monkeypatch.setattr(module, attribute, fail_if_called)

    identity = _identity(local_order_id="local-order-1")
    evidence = _evidence()
    parent = _parent("PREOPEN", local_order_id="local-order-1")
    assert breach_owner.normalize_breach_identity(_signal())["ok"] is True
    assert breach_owner.validate_breach_parent_snapshot(
        parent, identity=identity, phase="PREOPEN"
    )["accepted"] is True
    result = breach_owner.build_breach_snapshot_envelope(
        identity,
        evidence,
        pretrigger_snapshot=_parent("PRETRIGGER", snapshot_id="pretrigger-side-effect"),
        preopen_snapshot=parent,
    )
    assert result["ok"] is True
    assert breach_owner.hash_breach_identity(identity)
    assert breach_owner.hash_breach_snapshot_input(identity, evidence)

    module_source = (REPO_ROOT / "ap" / "intelligence_breach_snapshot_assembly.py").read_text()
    tree = ast.parse(module_source)
    imported_modules = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.append(node.module)
    forbidden_modules = ("ap_execution_core", "broker", "selector", "watcher", "osm")
    assert not any(
        any(fragment in module_name.lower() for fragment in forbidden_modules)
        for module_name in imported_modules
    )


def test_pure_owner_has_no_runtime_or_money_path_parameters():
    forbidden = {
        "broker",
        "selector",
        "watcher",
        "osm",
        "order",
        "position",
        "queue",
        "plan",
    }
    for function in (
        normalize_breach_identity,
        validate_breach_parent_snapshot,
        build_breach_snapshot_envelope,
        hash_breach_snapshot_input,
    ):
        assert forbidden.isdisjoint(inspect.signature(function).parameters)


def test_current_327_snapshot_shape_retains_non_authoritative_row_identity():
    """Use #327's real memory job -> worker -> snapshot row shape.

    With no market-data authority configured, #327 honestly emits
    ``UNAVAILABLE`` rows.  #625 must retain their exact IDs and lineage while
    refusing to bless them as proven parents.
    """
    from ap.intelligence_context_materializer import (
        enqueue_preopen_context,
        enqueue_pretrigger_context,
    )
    from ap.intelligence_context_worker import process_due_intelligence_jobs_once
    from ap.intelligence_snapshot_store import (
        DEFAULT_PROFILE_VERSION,
        _reset_memory_store_for_tests,
        get_latest_snapshot,
    )

    _reset_memory_store_for_tests()
    signal = _signal(local_order_id="")
    enqueue_pretrigger_context(
        signal,
        client_id=signal["client_id"],
        execution_mode=signal["execution_mode"],
        canonical_signal_id=signal["canonical_signal_id"],
    )
    process_due_intelligence_jobs_once(
        claim_owner="p1-pretrigger",
        client_id=signal["client_id"],
        execution_mode=signal["execution_mode"],
    )
    pretrigger = get_latest_snapshot(
        client_id=signal["client_id"],
        execution_mode=signal["execution_mode"],
        canonical_signal_id=signal["canonical_signal_id"],
        phase="PRETRIGGER",
    )["snapshot"]
    assert pretrigger["payload"]["phase"] == "PRETRIGGER"
    assert pretrigger["payload"]["ticker"] == "SPY"

    signal_with_order = _signal(local_order_id="local-order-1")
    enqueue_preopen_context(
        signal_with_order,
        client_id=signal["client_id"],
        execution_mode=signal["execution_mode"],
        canonical_signal_id=signal["canonical_signal_id"],
        local_order_id="local-order-1",
    )
    process_due_intelligence_jobs_once(
        claim_owner="p1-preopen",
        client_id=signal["client_id"],
        execution_mode=signal["execution_mode"],
    )
    preopen = get_latest_snapshot(
        client_id=signal["client_id"],
        execution_mode=signal["execution_mode"],
        canonical_signal_id=signal["canonical_signal_id"],
        phase="PREOPEN",
    )["snapshot"]
    assert preopen["payload"]["phase"] == "PREOPEN"
    assert preopen["payload"]["local_order_id"] == "local-order-1"

    identity = _identity(
        local_order_id="local-order-1",
        profile_version=DEFAULT_PROFILE_VERSION,
    )
    pretrigger_result = validate_breach_parent_snapshot(
        pretrigger, identity=identity, phase="PRETRIGGER"
    )
    preopen_result = validate_breach_parent_snapshot(
        preopen, identity=identity, phase="PREOPEN"
    )
    assert pretrigger_result["accepted"] is False
    assert preopen_result["accepted"] is False
    assert pretrigger_result["status"] == "UNPROVEN"
    assert preopen_result["status"] == "UNPROVEN"
    assert pretrigger_result["candidate_snapshot_id"] == pretrigger["id"]
    assert preopen_result["candidate_snapshot_id"] == preopen["id"]
    assert preopen_result["parent_snapshot_id"] == pretrigger["id"]
    envelope = build_breach_snapshot_envelope(
        identity,
        _evidence(),
        pretrigger_snapshot=pretrigger,
        preopen_snapshot=preopen,
    )
    assert envelope["ok"] is True
    assert envelope["parent_snapshot_ids"] == {
        "PRETRIGGER": None,
        "PREOPEN": None,
    }
    assert envelope["candidate_parent_snapshot_ids"] == {
        "PRETRIGGER": pretrigger["id"],
        "PREOPEN": preopen["id"],
    }
    assert envelope["parent_lineage"]["status"] == "UNPROVEN"


def test_pure_assembly_does_not_mutate_inputs():
    identity = _identity()
    evidence = _evidence()
    pretrigger = _parent("PRETRIGGER", snapshot_id="pretrigger-1")
    preopen = _parent("PREOPEN", snapshot_id="preopen-1", local_order_id="local-order-1")
    before = copy.deepcopy((identity, evidence, pretrigger, preopen))
    build_breach_snapshot_envelope(
        identity,
        evidence,
        pretrigger_snapshot=pretrigger,
        preopen_snapshot=preopen,
    )
    assert (identity, evidence, pretrigger, preopen) == before
