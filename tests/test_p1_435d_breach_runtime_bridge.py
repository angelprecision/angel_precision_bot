"""Focused PR #622 tests for the observe-only BREACH runtime sideband."""

from __future__ import annotations

import ast
import copy
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

# Importing ``ap`` installs the repository's startup guards.  Keep the dummy
# import-only database configuration local to collection so the shared P0
# process retains its configured database/schema environment at test runtime.
_original_database_url = os.environ.get("DATABASE_URL")
_original_schema_attestation = os.environ.get("SCHEMA_ATTESTATION_ENABLED")
if _original_database_url is None:
    os.environ["DATABASE_URL"] = "postgresql://dummy:dummy@127.0.0.1:1/dummy"
if _original_schema_attestation is None:
    os.environ["SCHEMA_ATTESTATION_ENABLED"] = "0"
try:
    import ap.intelligence_breach_market_structure as structure_mod  # noqa: E402
    import ap.intelligence_breach_snapshot_adapter as adapter_mod  # noqa: E402
    import ap.intelligence_breach_snapshot_assembly as assembly_mod  # noqa: E402
    import ap.intelligence_breach_runtime_bridge as bridge  # noqa: E402
    from ap.intelligence_context_materializer import (  # noqa: E402
        build_snapshot_kwargs,
    )
    from ap.intelligence_snapshot_store import (  # noqa: E402
        _MEMORY_JOBS,
        _reset_memory_store_for_tests,
    )
finally:
    if _original_database_url is None:
        os.environ.pop("DATABASE_URL", None)
    if _original_schema_attestation is None:
        os.environ.pop("SCHEMA_ATTESTATION_ENABLED", None)


TRIGGER = "2026-09-12T16:00:00+00:00"
CLIENT = "client@example.com"
SIGNAL_ID = "sig-622"
LOCAL_ORDER_ID = "order-622"


def _pit(as_of: str = TRIGGER) -> dict:
    return {
        "phase": "BREACH",
        "as_of": as_of,
        "collected_at": "2026-09-12T16:00:02+00:00",
        "data_sources": {
            "candles": {"5m": [], "15m": [], "1h": [], "4h": []},
            "coverage": {},
        },
        "underlying_observation": {
            "as_of": as_of,
            "price": 500.25,
            "source": "test.pit.quote",
            "observed_at": as_of,
        },
        "provenance": {"source": "test.614"},
    }


def _signal(
    *,
    client_id: str = CLIENT,
    execution_mode: str = "PAPER",
    signal_id: str = SIGNAL_ID,
    local_order_id: str = LOCAL_ORDER_ID,
    ticker: str = "SPY",
    side: str = "CALL",
    generation: int | None = 7,
    pit: dict | None = None,
    **extra,
) -> dict:
    value = {
        "client_id": client_id,
        "execution_mode": execution_mode,
        "signal_id": signal_id,
        "local_order_id": local_order_id,
        "ticker": ticker,
        "side": side,
        "trigger_price": 500.0,
        "trigger_crossed_at": TRIGGER,
        "materialization_generation": generation,
        "point_in_time": copy.deepcopy(_pit() if pit is None else pit),
    }
    value.update(extra)
    return value


def _plan(
    *,
    client_id: str = CLIENT,
    execution_mode: str = "PAPER",
    signal_id: str = SIGNAL_ID,
    ticker: str = "SPY",
    side: str = "CALL",
    materialization_generation: int | None = None,
    **metadata,
) -> SimpleNamespace:
    return SimpleNamespace(
        client_id=client_id,
        execution_mode=execution_mode,
        signal_id=signal_id,
        ticker=ticker,
        side=side,
        trigger_price=500.0,
        materialization_generation=materialization_generation,
        metadata=dict(metadata),
    )


def _watched(signal: dict | None = None, *, trigger=TRIGGER) -> SimpleNamespace:
    signal = signal or _signal()
    return SimpleNamespace(
        signal=signal,
        ticker=signal["ticker"],
        side=signal["side"],
        signal_id=signal["signal_id"],
        local_order_id=signal["local_order_id"],
        trigger_crossed_at=(
            datetime.fromisoformat(trigger) if isinstance(trigger, str) else trigger
        ),
        trigger_price=signal["trigger_price"],
    )


@pytest.fixture(autouse=True)
def _reset_bridge_and_store(monkeypatch):
    monkeypatch.setenv("INTELLIGENCE_CONTEXT_STORE_BACKEND", "memory")
    bridge.reset_breach_bridge_state_for_tests()
    _reset_memory_store_for_tests()
    yield
    bridge.reset_breach_bridge_state_for_tests()
    _reset_memory_store_for_tests()


def test_freeze_copies_only_runtime_evidence_and_preserves_identity():
    signal = _signal()
    watched = _watched(signal)
    artifact = bridge.freeze_breach_runtime_artifact(signal, _plan(), watched)

    assert artifact["ok"] is True
    assert artifact["identity"]["client_id"] == CLIENT
    assert artifact["identity"]["execution_mode"] == "PAPER"
    assert artifact["identity"]["signal_id"] == SIGNAL_ID
    assert artifact["identity"]["local_order_id"] == LOCAL_ORDER_ID
    assert artifact["identity"]["materialization_generation"] == 7
    assert artifact["identity"]["trigger_crossed_at"] == watched.trigger_crossed_at
    assert artifact["evidence_status"] == "AVAILABLE"
    assert "current_quote" not in artifact["signal"]

    signal["point_in_time"]["underlying_observation"]["price"] = 999.0
    signal["point_in_time"]["data_sources"]["candles"]["5m"].append({"close": 999.0})
    assert artifact["point_in_time"]["underlying_observation"]["price"] == 500.25
    assert artifact["point_in_time"]["data_sources"]["candles"]["5m"] == []


def test_missing_evidence_is_explicit_partial_diagnostic_without_clock_fallback():
    signal = _signal()
    signal.pop("point_in_time")
    artifact = bridge.freeze_breach_runtime_artifact(signal, _plan(), _watched(signal))

    assert artifact["ok"] is True
    assert artifact["evidence_status"] == "MISSING"
    assert artifact["fallback_reason"] == "BREACH_INTEL_EVIDENCE_MISSING"
    assert artifact["evidence"]["as_of"] == datetime.fromisoformat(TRIGGER)
    assert artifact["evidence"]["errors"] == ["BREACH_INTEL_EVIDENCE_MISSING"]
    assert artifact["evidence"]["point_in_time"]["data_sources"]["candles"] == {}


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("materialization_generation", "7", "materialization_generation_invalid"),
        ("materialization_generation", True, "materialization_generation_invalid"),
        ("trigger_crossed_at", "2026-09-12T16:00:00", "trigger_crossed_at_invalid"),
    ],
)
def test_unproven_identity_is_not_handed_off(field, value, reason):
    signal = _signal()
    signal[field] = value
    watched = _watched(signal)
    artifact = bridge.freeze_breach_runtime_artifact(signal, _plan(), watched)

    assert artifact["ok"] is False
    assert reason in artifact["fallback_reason"]


def test_missing_generation_is_not_handed_off():
    signal = _signal(materialization_generation=None)
    artifact = bridge.freeze_breach_runtime_artifact(signal, _plan(), _watched(signal))

    assert artifact["ok"] is False
    assert "materialization_generation_missing" in artifact["fallback_reason"]


def test_stale_evidence_becomes_non_authoritative_partial_diagnostic():
    signal = _signal(pit=_pit("2026-09-12T15:59:00+00:00"))
    artifact = bridge.freeze_breach_runtime_artifact(signal, _plan(), _watched(signal))

    assert artifact["ok"] is True
    assert artifact["evidence_status"] == "STALE"
    assert artifact["fallback_reason"] == "BREACH_INTEL_EVIDENCE_STALE_OR_MISMATCH"
    assert artifact["evidence"]["as_of"] == datetime.fromisoformat(TRIGGER)
    assert artifact["evidence"]["point_in_time"]["errors"] == [
        "BREACH_INTEL_EVIDENCE_STALE_OR_MISMATCH"
    ]


def test_handoff_returns_before_background_and_suppresses_repeated_callback(monkeypatch):
    captured: list[tuple] = []
    release = threading.Event()

    def fake_handoff(fn, artifact, **kwargs):
        captured.append((fn, artifact, kwargs))
        assert not release.is_set()
        return {"ok": True, "accepted": True}

    monkeypatch.setattr(bridge, "submit_intelligence_enqueue", fake_handoff, raising=False)
    signal = _signal()
    started = time.perf_counter()
    first = bridge.submit_breach_intelligence_nonblocking(signal, _plan(), _watched(signal))
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    second = bridge.submit_breach_intelligence_nonblocking(signal, _plan(), _watched(signal))

    assert first["accepted"] is True
    assert first["handoff_status"] == "ACCEPTED"
    assert first["handoff_ms"] >= 0
    assert elapsed_ms < 250.0
    assert len(captured) == 1
    assert second["duplicate"] is True
    assert second["handoff_status"] == "DUPLICATE"
    release.set()


def test_blocked_background_does_not_delay_selector_continuation(monkeypatch):
    background_started = threading.Event()
    release_background = threading.Event()
    background_finished = threading.Event()
    workers: list[threading.Thread] = []

    def fake_handoff(_fn, _artifact, **_kwargs):
        def worker():
            background_started.set()
            release_background.wait(5.0)
            background_finished.set()

        thread = threading.Thread(target=worker, daemon=True)
        workers.append(thread)
        thread.start()
        assert background_started.wait(1.0)
        return {"ok": True, "accepted": True}

    monkeypatch.setattr(bridge, "submit_intelligence_enqueue", fake_handoff, raising=False)
    selector_calls: list[str] = []
    result = bridge.submit_breach_intelligence_nonblocking(_signal(), _plan(), _watched())
    # This is the existing callback's next operation in the production seam.
    selector_calls.append("selector")

    assert result["accepted"] is True
    assert selector_calls == ["selector"]
    assert background_started.is_set()
    assert not background_finished.is_set()

    release_background.set()
    assert background_finished.wait(1.0)
    for thread in workers:
        thread.join(timeout=1.0)


def test_saturated_handoff_does_not_poison_future_retry(monkeypatch):
    responses = iter(({"ok": False, "accepted": False, "error": "full"},
                      {"ok": True, "accepted": True}))
    monkeypatch.setattr(
        bridge,
        "submit_intelligence_enqueue",
        lambda *_a, **_k: next(responses),
        raising=False,
    )
    signal = _signal()
    first = bridge.submit_breach_intelligence_nonblocking(signal, _plan(), _watched(signal))
    second = bridge.submit_breach_intelligence_nonblocking(signal, _plan(), _watched(signal))

    assert first["accepted"] is False
    assert first["handoff_status"] == "SATURATED_OR_REJECTED"
    assert second["accepted"] is True


def test_older_generation_is_rejected_after_newer_generation_is_seen(monkeypatch):
    accepted: list[dict] = []

    def fake_handoff(_fn, artifact, **_kwargs):
        accepted.append(artifact)
        return {"ok": True, "accepted": True}

    monkeypatch.setattr(bridge, "submit_intelligence_enqueue", fake_handoff, raising=False)
    new_signal = _signal(materialization_generation=8)
    old_signal = _signal(materialization_generation=7)
    new_result = bridge.submit_breach_intelligence_nonblocking(
        new_signal,
        _plan(materialization_generation=8),
        _watched(new_signal),
    )
    old_result = bridge.submit_breach_intelligence_nonblocking(
        old_signal,
        _plan(materialization_generation=7),
        _watched(old_signal),
    )

    assert new_result["accepted"] is True
    assert old_result["accepted"] is False
    assert old_result["handoff_status"] == "STALE_GENERATION"
    assert old_result["fallback_reason"] == "stale_generation"
    assert len(accepted) == 1


def test_background_rechecks_generation_before_persistence(monkeypatch):
    calls: list[str] = []
    structure = {"schema_version": "breach_market_structure_v1", "observe_only": True,
                 "affected_eligibility": False}

    def fake_freeze(signal, pit):
        calls.append("615")
        bridge.submit_breach_intelligence_nonblocking(
            _signal(materialization_generation=8),
            _plan(materialization_generation=8),
            _watched(_signal(materialization_generation=8)),
        )
        return structure

    def fake_assemble(*_args, **_kwargs):
        calls.append("625")
        return {"ok": True, "status": "PARTIAL"}

    def fake_enqueue(*_args, **_kwargs):
        calls.append("621")
        return {"ok": True, "enqueued": True}

    monkeypatch.setattr(structure_mod, "freeze_breach_market_structure_from_pit", fake_freeze)
    monkeypatch.setattr(assembly_mod, "build_breach_snapshot_envelope", fake_assemble)
    monkeypatch.setattr(adapter_mod, "enqueue_breach_snapshot_job", fake_enqueue)
    monkeypatch.setattr(bridge, "submit_intelligence_enqueue", lambda *_a, **_k: {
        "ok": True, "accepted": True
    }, raising=False)

    old_signal = _signal(materialization_generation=7)
    artifact = bridge.freeze_breach_runtime_artifact(
        old_signal, _plan(materialization_generation=7), _watched(old_signal)
    )
    result = bridge._assemble_and_enqueue(artifact)

    assert result["status"] == "STALE_GENERATION"
    assert calls == ["615"]


def test_background_chain_calls_615_then_625_then_621(monkeypatch):
    calls: list[str] = []
    structure = {"schema_version": "breach_market_structure_v1", "observe_only": True,
                 "affected_eligibility": False}

    def fake_freeze(signal, pit):
        calls.append("615")
        assert pit["as_of"] == TRIGGER
        return structure

    def fake_assemble(identity, evidence, **kwargs):
        calls.append("625")
        assert identity["ok"] is True
        assert kwargs["structure"] is structure
        return {"ok": True, "status": "PARTIAL"}

    def fake_enqueue(envelope):
        calls.append("621")
        assert envelope["status"] == "PARTIAL"
        return {"ok": True, "enqueued": True, "job_id": "job-622"}

    monkeypatch.setattr(structure_mod, "freeze_breach_market_structure_from_pit", fake_freeze)
    monkeypatch.setattr(assembly_mod, "build_breach_snapshot_envelope", fake_assemble)
    monkeypatch.setattr(adapter_mod, "enqueue_breach_snapshot_job", fake_enqueue)

    artifact = bridge.freeze_breach_runtime_artifact(_signal(), _plan(), _watched())
    result = bridge._assemble_and_enqueue(artifact)

    assert result["ok"] is True
    assert result["status"] == "ENQUEUED"
    assert calls == ["615", "625", "621"]
    assert result["structure_ms"] >= 0
    assert result["assembly_ms"] >= 0
    assert result["enqueue_ms"] >= 0
    assert result["background_ms"] >= 0
    assert result["background_started_at"] <= result["background_completed_at"]
    assert result["assembly_status"] == "PARTIAL"
    assert result["snapshot_enqueue_status"] == "ENQUEUED"


def test_background_615_exception_is_fail_soft(monkeypatch):
    def explode(*_args, **_kwargs):
        raise RuntimeError("PIT freezer unavailable")

    monkeypatch.setattr(structure_mod, "freeze_breach_market_structure_from_pit", explode)
    result = bridge._assemble_and_enqueue(
        bridge.freeze_breach_runtime_artifact(_signal(), _plan(), _watched())
    )

    assert result["ok"] is False
    assert result["status"] == "BACKGROUND_FAILED"
    assert "PIT freezer unavailable" in result["fallback_reason"]


@pytest.mark.parametrize("failure_stage", ["assembly", "enqueue", "store"])
def test_background_assembly_and_persistence_failures_are_fail_soft(
    monkeypatch, failure_stage
):
    structure = {"schema_version": "breach_market_structure_v1", "observe_only": True,
                 "affected_eligibility": False}
    monkeypatch.setattr(structure_mod, "freeze_breach_market_structure_from_pit",
                        lambda *_args: structure)
    if failure_stage == "assembly":
        monkeypatch.setattr(
            assembly_mod,
            "build_breach_snapshot_envelope",
            lambda *_args, **_kwargs: {
                "ok": False,
                "status": "REJECTED",
                "errors": ["assembly_rejected"],
            },
        )
    else:
        monkeypatch.setattr(
            assembly_mod,
            "build_breach_snapshot_envelope",
            lambda *_args, **_kwargs: {"ok": True, "status": "PARTIAL"},
        )
        if failure_stage == "enqueue":
            monkeypatch.setattr(
                adapter_mod,
                "enqueue_breach_snapshot_job",
                lambda *_args: {
                    "ok": False,
                    "enqueued": False,
                    "error_code": "BREACH_ENQUEUE_REJECTED",
                },
            )
        else:
            def explode(*_args, **_kwargs):
                raise RuntimeError("#327 store unavailable")

            monkeypatch.setattr(adapter_mod, "enqueue_breach_snapshot_job", explode)

    result = bridge._assemble_and_enqueue(
        bridge.freeze_breach_runtime_artifact(_signal(), _plan(), _watched())
    )

    assert result["ok"] is False
    assert result["status"] in {"ASSEMBLY_REJECTED", "ENQUEUE_FAILED", "BACKGROUND_FAILED"}


def test_real_615_625_621_327_enqueue_round_trip_is_partial_not_trading_authority():
    artifact = bridge.freeze_breach_runtime_artifact(_signal(), _plan(), _watched())
    result = bridge._assemble_and_enqueue(artifact)

    assert result["ok"] is True
    assert result["enqueue_result"]["enqueued"] is True
    assert len(_MEMORY_JOBS) == 1
    job = next(iter(_MEMORY_JOBS.values()))
    assert job["phase"] == "BREACH"
    assert job["payload"]["payload_kind"] == "FROZEN_BREACH_V1"
    assert job["payload"]["observe_only"] is True
    assert job["payload"]["affected_eligibility"] is False
    assert job["payload"]["trigger_crossed_at"] == TRIGGER
    assert build_snapshot_kwargs(job)["status"] == "PARTIAL"


@pytest.mark.parametrize(
    "changes",
    [
        {"client_id": "other@example.com"},
        {"execution_mode": "LIVE"},
        {"side": "PUT"},
        {"materialization_generation": 8},
    ],
)
def test_identity_dimensions_isolate_sideband_events(monkeypatch, changes):
    accepted: list[dict] = []

    def fake_handoff(_fn, artifact, **_kwargs):
        accepted.append(artifact)
        return {"ok": True, "accepted": True}

    monkeypatch.setattr(bridge, "submit_intelligence_enqueue", fake_handoff, raising=False)
    first_signal = _signal()
    second_signal = _signal(**changes)
    second_plan = _plan(
        client_id=changes.get("client_id", CLIENT),
        execution_mode=changes.get("execution_mode", "PAPER"),
        signal_id=changes.get("signal_id", SIGNAL_ID),
        ticker=changes.get("ticker", "SPY"),
        side=changes.get("side", "CALL"),
        materialization_generation=changes.get("materialization_generation", 7),
    )
    first = bridge.submit_breach_intelligence_nonblocking(
        first_signal, _plan(), _watched(first_signal)
    )
    second = bridge.submit_breach_intelligence_nonblocking(
        second_signal, second_plan, _watched(second_signal)
    )

    assert first["accepted"] is True
    assert second["accepted"] is True
    assert len(accepted) == 2
    assert accepted[0]["identity"] != accepted[1]["identity"]


def test_handoff_and_background_exceptions_never_escape(monkeypatch):
    monkeypatch.setattr(
        bridge,
        "submit_intelligence_enqueue",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("executor down")),
        raising=False,
    )
    result = bridge.submit_breach_intelligence_nonblocking(_signal(), _plan(), _watched())
    assert result["ok"] is True
    assert result["accepted"] is False
    assert result["handoff_status"] == "HANDOFF_FAILED"


def test_bridge_has_no_money_path_imports_or_synchronous_waits():
    path = Path(bridge.__file__)
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.append(node.module or "")
    forbidden_imports = (
        "ap_execution_core",
        "ap_entry_watcher",
        "ap.order_state_machine",
        "ap.broker",
        "ap.position_manager",
        "ap.contract_selector",
        "ap.queue",
        "ap.intelligence_market_data",
    )
    assert not any(name.startswith(forbidden) for name in imported for forbidden in forbidden_imports)
    source = path.read_text(encoding="utf-8")
    assert "Future.result" not in source
    assert ".result(" not in source
    assert "thread.join(" not in source
    assert "time.sleep" not in source


def test_execution_core_has_one_existing_selector_and_bridge_is_before_it():
    source = Path("ap_execution_core.py").read_text(encoding="utf-8")
    start = source.index("    def _on_entry_trigger(")
    end = source.find("\n    def ", start + 20)
    body = source[start:end if end != -1 else None]
    bridge_pos = body.index("_submit_breach_intel")
    selector_pos = body.index("self.contract_selector.select(")
    hydration_pos = body.index("_refresh_hydrated_prebreach_plan")
    assert bridge_pos < hydration_pos < selector_pos
    assert body.count("self.contract_selector.select(") == 1


def test_late_sideband_result_has_no_trading_callback_surface():
    signal = _signal()
    artifact = bridge.freeze_breach_runtime_artifact(signal, _plan(), _watched(signal))
    assert set(artifact) >= {"identity", "signal", "evidence", "parent_snapshots"}
    # The frozen background input contains no runtime object or callback that
    # could invoke selector/broker/order/watch lifecycle work after submission.
    assert not any(callable(value) for value in artifact.values())
    assert artifact["identity"]["trigger_crossed_at"] == datetime.fromisoformat(TRIGGER)
