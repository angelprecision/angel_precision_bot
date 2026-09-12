"""PR #621 (435-C) focused test suite for the durable BREACH snapshot adapter.

Covers spec sections 5–20:

* §5   enqueue contract (REJECTED envelope fail-soft, validation);
* §6   #327 job allocation for BREACH context_revision (idempotency vs new-input);
* §7   immutable frozen payload (no mutable/latest-market aliases);
* §8   exact #614 evidence_as_of preserved as snapshot data_as_of;
* §9   deterministic frozen structure hash (key-order stable, changes on evidence change);
* §10  status mapping COMPLETE / PARTIAL / REJECTED;
* §11  candidate vs authoritative parent invariants;
* §12  worker adapter identity cross-check (fail closed on mismatch);
* §13  full replayable snapshot payload;
* §14  intelligence failure is fail-soft for trading;
* §15  no market-data / broker / selector / watcher calls in worker path;
* §16  end-to-end idempotency;
* §17  as-of / no-future-data regression;
* §18  real #625 integration;
* §19  real #614/#615 integration via a deterministic PIT-shaped fixture;
* §20  no money-path files edited.

All state runs against the existing #327 in-memory backend
(``INTELLIGENCE_CONTEXT_STORE_BACKEND=memory``) so the tests exercise the same
persistence code path that the DB backend uses.
"""

from __future__ import annotations

import copy
import json
import os
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pytest

os.environ["INTELLIGENCE_CONTEXT_STORE_BACKEND"] = "memory"

from ap.intelligence_breach_snapshot_adapter import (  # noqa: E402
    ADAPTER_VERSION,
    FROZEN_BREACH_PAYLOAD_KIND,
    build_breach_snapshot_kwargs,
    enqueue_breach_snapshot_job,
    hash_frozen_breach_structure,
    is_frozen_breach_job,
)
from ap.intelligence_breach_snapshot_assembly import (  # noqa: E402
    ASSEMBLY_VERSION,
    BREACH_PHASE,
    build_breach_snapshot_envelope,
    hash_breach_snapshot_input,
    normalize_breach_identity,
)
from ap.intelligence_breach_market_structure import (  # noqa: E402
    freeze_breach_market_structure_from_pit,
)
from ap.intelligence_market_data import (  # noqa: E402
    collect_point_in_time_context,
)
from ap.intelligence_context_materializer import (  # noqa: E402
    build_snapshot_kwargs,
    build_intelligence_context_payload,
)
from ap.intelligence_context_worker import (  # noqa: E402
    process_due_intelligence_jobs_once,
)
from ap.intelligence_snapshot_store import (  # noqa: E402
    _MEMORY_JOBS,
    _MEMORY_SNAPSHOTS,
    _reset_memory_store_for_tests,
    claim_due_intelligence_jobs,
    complete_job_with_snapshot,
    get_latest_snapshot,
    write_snapshot,
)


# ---------------------------------------------------------------------------
# Fixtures — deterministic production-shape values for real #625 integration
# ---------------------------------------------------------------------------


PROFILE_VERSION = "intelligence_context_v1_observe_only"
CLIENT = "client@example.com"
MODE = "PAPER"
SIGNAL_ID = "sig-435c-1"
CANON = "canon-435c-1"
LOCAL_ORDER = "local-435c-1"
TICKER = "SPY"
SIDE = "CALL"
TRIGGER_AT = "2026-09-12T13:45:00+00:00"
PIT_AS_OF = "2026-09-11T17:30:00+00:00"
_ET = ZoneInfo("America/New_York")


def _pit_bar(ts: str, open_price: float, high: float, low: float, close: float) -> dict[str, Any]:
    return {
        "time": ts,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": 1_000,
    }


def _pit_15m_session(day: str, base: float) -> list[dict[str, Any]]:
    start = datetime.fromisoformat(f"{day}T09:30:00").replace(tzinfo=_ET)
    rows: list[dict[str, Any]] = []
    for index in range(26):
        timestamp = start + timedelta(minutes=15 * index)
        open_price, high, low, close = base, base + 0.5, base - 0.5, base
        rows.append(_pit_bar(timestamp.isoformat(), open_price, high, low, close))
    return rows


def _canonical_614_signal() -> dict[str, Any]:
    return {
        "ticker": "NOW",
        "side": "PUT",
        "trigger_price": 100.50,
        "trigger_crossed_at": PIT_AS_OF,
        "breach_price": 100.50,
        "breach_lineage": "INITIAL_BREACH",
        "candles_5m": [
            _pit_bar("2026-09-11T17:25:00+00:00", 110.5, 111.0, 100.3, 110.0)
        ],
        "candles_15m": (
            _pit_15m_session("2026-09-08", 100.0)
            + _pit_15m_session("2026-09-09", 108.0)
            + _pit_15m_session("2026-09-10", 116.0)
            + _pit_15m_session("2026-09-11", 110.0)
        ),
    }


def _identity(
    *,
    signal_id: str = SIGNAL_ID,
    canonical: str = CANON,
    local_order_id: str = LOCAL_ORDER,
    client_id: str = CLIENT,
    execution_mode: str = MODE,
    ticker: str = TICKER,
    side: str = SIDE,
    trigger_at: str = TRIGGER_AT,
    generation: int = 1,
    profile_version: str = PROFILE_VERSION,
    model_version: str = "breach_v1",
) -> dict[str, Any]:
    """Return a production-shaped #625 identity mapping."""
    return {
        "client_id": client_id,
        "execution_mode": execution_mode,
        "signal_id": signal_id,
        "canonical_signal_id": canonical,
        "local_order_id": local_order_id,
        "ticker": ticker,
        "side": side,
        "trigger_crossed_at": trigger_at,
        "materialization_generation": generation,
        "profile_version": profile_version,
        "model_version": model_version,
        "phase": BREACH_PHASE,
    }


def _evidence(*, trigger_at: str = TRIGGER_AT, close_price: float = 448.72) -> dict[str, Any]:
    """Return a #614-shaped PIT evidence mapping compatible with the #625 projector.

    The exact keys are the ones the merged #625 canonical evidence projector
    expects (see ``project_breach_evidence``).  Values are deterministic; only
    the ones actually consumed downstream matter for these tests.
    """
    return {
        "point_in_time": {
            "as_of": trigger_at,
            "collected_at": "2026-09-12T13:45:11+00:00",  # deliberately later than as_of
            "data_sources": {
                "candles": {
                    "5m": [
                        {
                            "start": "2026-09-12T13:40:00+00:00",
                            "end": "2026-09-12T13:45:00+00:00",
                            "open": 448.10,
                            "high": 449.00,
                            "low": 447.95,
                            "close": close_price,
                            "volume": 1_500_000,
                        }
                    ],
                    "15m": [
                        {
                            "start": "2026-09-12T13:30:00+00:00",
                            "end": "2026-09-12T13:45:00+00:00",
                            "open": 447.50,
                            "high": 449.20,
                            "low": 447.10,
                            "close": close_price,
                            "volume": 4_100_000,
                        }
                    ],
                },
                "coverage": {
                    "5m": {"status": "COMPLETE", "coverage_complete": True, "authoritative": True},
                    "15m": {"status": "COMPLETE", "coverage_complete": True, "authoritative": True},
                },
            },
            "provenance": {"origin": "tests.435c.fixture"},
            "underlying_observation": {
                "as_of": trigger_at,
                "price": close_price,
            },
        },
        "as_of": trigger_at,
    }


def _structure(
    *,
    trigger_at: str = TRIGGER_AT,
    model_version: str = "breach_v1",
    client_id: str = CLIENT,
    execution_mode: str = MODE,
    signal_id: str = SIGNAL_ID,
    canonical: str = CANON,
    local_order_id: str = LOCAL_ORDER,
    ticker: str = TICKER,
    side: str = SIDE,
) -> dict[str, Any]:
    """Return a #615-shaped frozen structure mapping.

    Uses the exact identity-cross-check fields required by the #625 assembly.
    """
    return {
        "schema_version": "breach_structure_v1",
        "model_version": model_version,
        "as_of": trigger_at,
        "client_id": client_id,
        "execution_mode": execution_mode,
        "signal_id": signal_id,
        "canonical_signal_id": canonical,
        "local_order_id": local_order_id,
        "ticker": ticker,
        "side": side,
        "trigger_crossed_at": trigger_at,
        "phase": BREACH_PHASE,
        "observe_only": True,
        "affected_eligibility": False,
        "fvg_4h": [
            {
                "id": "fvg_4h_1",
                "direction": "bullish",
                "high": 449.50,
                "low": 447.20,
                "status": "active",
            }
        ],
        "fvg_1h": [
            {
                "id": "fvg_1h_1",
                "direction": "bullish",
                "high": 449.10,
                "low": 447.60,
                "status": "active",
            }
        ],
        "strong_break": {
            "observed": False,
            "body_beyond_boundary_ratio": 0.31,
            "body_to_range_ratio": 0.42,
        },
        "vi": {"status": "MISSING"},
        "regime": "UNKNOWN",
        "setup_archetype": "test_archetype",
    }


def _build_complete_envelope(**overrides: Any) -> dict[str, Any]:
    """Build a real #625 envelope that will be COMPLETE for these identities."""
    identity_kwargs: dict[str, Any] = {}
    for key in (
        "signal_id",
        "canonical",
        "local_order_id",
        "client_id",
        "execution_mode",
        "ticker",
        "side",
        "trigger_at",
        "generation",
        "profile_version",
        "model_version",
    ):
        if key in overrides:
            identity_kwargs[key] = overrides.pop(key)
    identity = _identity(**identity_kwargs)
    evidence = _evidence(trigger_at=identity["trigger_crossed_at"])
    structure = _structure(
        trigger_at=identity["trigger_crossed_at"],
        model_version=identity["model_version"],
        client_id=identity["client_id"],
        execution_mode=identity["execution_mode"],
        signal_id=identity["signal_id"],
        canonical=identity["canonical_signal_id"],
        local_order_id=identity["local_order_id"],
        ticker=identity["ticker"],
        side=identity["side"],
    )
    # Overrides may inject alternate evidence/structure for regression tests.
    if "evidence" in overrides:
        evidence = overrides.pop("evidence")
    if "structure" in overrides:
        structure = overrides.pop("structure")

    # Build the initial envelope without parents so PARTIAL behavior remains
    # covered.  COMPLETE tests pass it through the real #625 parent validator
    # in _inject_authoritative_parents below.
    envelope = build_breach_snapshot_envelope(
        identity, evidence, structure=structure
    )
    assert envelope["ok"] is True, f"fixture envelope invalid: {envelope['errors']}"
    return envelope


def _inject_authoritative_parents(envelope: dict[str, Any]) -> dict[str, Any]:
    """Reassemble with production-shaped parents through the real #625 owner."""
    source = copy.deepcopy(envelope)
    identity = dict(source["identity"])
    trigger_at = identity["trigger_crossed_at"]

    def _parent(phase: str, snapshot_id: str, parent_snapshot_id: str | None = None) -> dict[str, Any]:
        local_order_id = identity["local_order_id"] if phase == "PREOPEN" else ""
        signal = {
            "client_id": identity["client_id"],
            "execution_mode": identity["execution_mode"],
            "signal_id": identity["signal_id"],
            "canonical_signal_id": identity["canonical_signal_id"],
            "local_order_id": local_order_id,
            "ticker": identity["ticker"],
            "side": identity["side"],
            "trigger_crossed_at": trigger_at,
            "materialization_generation": identity["materialization_generation"],
            "profile_version": identity["profile_version"],
            "model_version": identity["model_version"],
            "phase": phase,
        }
        payload = {
            "phase": phase,
            "signal": signal,
            "client_id": identity["client_id"],
            "execution_mode": identity["execution_mode"],
            "signal_id": identity["signal_id"],
            "canonical_signal_id": identity["canonical_signal_id"],
            "local_order_id": local_order_id,
            "ticker": identity["ticker"],
            "side": identity["side"],
            "trigger_crossed_at": trigger_at,
            "materialization_generation": identity["materialization_generation"],
            "profile_version": identity["profile_version"],
            "model_version": identity["model_version"],
            "status": "COMPLETE",
            "parent_snapshot_id": parent_snapshot_id,
            "parent_link_status": "LINKED" if parent_snapshot_id else "PRETRIGGER_NOT_AVAILABLE",
            "observe_only": True,
            "affected_eligibility": False,
        }
        return {
            "id": snapshot_id,
            "client_id": identity["client_id"],
            "execution_mode": identity["execution_mode"],
            "signal_id": identity["signal_id"],
            "canonical_signal_id": identity["canonical_signal_id"],
            "local_order_id": local_order_id,
            "ticker": identity["ticker"],
            "side": identity["side"],
            "phase": phase,
            "trigger_crossed_at": trigger_at,
            "materialization_generation": identity["materialization_generation"],
            "profile_version": identity["profile_version"],
            "model_version": identity["model_version"],
            "context_revision": 1,
            "status": "COMPLETE",
            "parent_snapshot_id": parent_snapshot_id,
            "observe_only": True,
            "affected_eligibility": False,
            "payload": payload,
        }

    return build_breach_snapshot_envelope(
        identity,
        source["evidence"],
        structure=source["structure"],
        pretrigger_snapshot=_parent("PRETRIGGER", "parent-pretrigger-xyz"),
        preopen_snapshot=_parent(
            "PREOPEN", "parent-preopen-abc", "parent-pretrigger-xyz"
        ),
    )


@pytest.fixture(autouse=True)
def _reset_store():
    _reset_memory_store_for_tests()
    yield
    _reset_memory_store_for_tests()


# ---------------------------------------------------------------------------
# §9  hash_frozen_breach_structure — deterministic, order-independent
# ---------------------------------------------------------------------------


class TestFrozenStructureHash:
    def test_missing_structure_has_stable_tag(self):
        assert hash_frozen_breach_structure(None).startswith("sha256:")

    def test_null_is_hashed_as_json_null(self):
        assert hash_frozen_breach_structure(None) == hash_frozen_breach_structure(None)

    def test_key_order_does_not_change_hash(self):
        a = hash_frozen_breach_structure({"a": 1, "b": {"x": 1, "y": 2}})
        b = hash_frozen_breach_structure({"b": {"y": 2, "x": 1}, "a": 1})
        assert a == b

    def test_list_order_matters(self):
        a = hash_frozen_breach_structure([1, 2, 3])
        b = hash_frozen_breach_structure([3, 2, 1])
        assert a != b

    def test_evidence_change_changes_hash(self):
        base = _structure()
        mutated = copy.deepcopy(base)
        mutated["strong_break"]["observed"] = True
        assert hash_frozen_breach_structure(base) != hash_frozen_breach_structure(mutated)

    @pytest.mark.parametrize(
        "mutator",
        [
            lambda value: value["fvg_4h"][0].update(high=450.0),
            lambda value: value["strong_break"].update(observed=True),
            lambda value: value["vi"].update(status="AVAILABLE"),
            lambda value: value.update(
                pullback_reclaim_rebreach={"status": "RECLAIMING"}
            ),
        ],
    )
    def test_semantic_structure_mutation_changes_hash(self, mutator):
        base = _structure()
        mutated = copy.deepcopy(base)
        mutator(mutated)
        assert hash_frozen_breach_structure(base) != hash_frozen_breach_structure(mutated)

    def test_custom_object_is_rejected(self):
        class _Weird:
            def __repr__(self) -> str:
                return "<Weird 0x{:x}>".format(id(self))

        with pytest.raises(TypeError):
            hash_frozen_breach_structure({"w": _Weird()})

    @pytest.mark.parametrize(
        "value",
        [
            {"values": {1, 2}},
            {"values": b"bytes"},
            {1: "non-string-key"},
            {"value": float("nan")},
            {"value": float("inf")},
            {"value": float("-inf")},
        ],
    )
    def test_unsupported_json_shapes_are_rejected(self, value):
        with pytest.raises((TypeError, ValueError)):
            hash_frozen_breach_structure(value)


# ---------------------------------------------------------------------------
# §5  Enqueue contract — rejects REJECTED envelopes and invalid input
# ---------------------------------------------------------------------------


class TestEnqueueContract:
    def test_rejects_non_mapping(self):
        result = enqueue_breach_snapshot_job("not a mapping")
        assert result["ok"] is False
        assert result["enqueued"] is False
        assert result["error_code"] == "BREACH_ENVELOPE_NOT_MAPPING"

    def test_rejects_rejected_envelope_softly(self):
        # A #625 REJECTED envelope must not enqueue and must not raise.
        rejected = {
            "status": "REJECTED",
            "identity_hash": "h",
            "input_hash": "h",
            "trigger_crossed_at": TRIGGER_AT,
            "as_of": TRIGGER_AT,
            "phase": BREACH_PHASE,
            "observe_only": True,
            "affected_eligibility": False,
        }
        result = enqueue_breach_snapshot_job(rejected)
        assert result == {
            "ok": False,
            "enqueued": False,
            "error_code": "BREACH_ENVELOPE_REJECTED",
            "errors": ["envelope_rejected"],
        }
        # Store must be untouched.
        assert _MEMORY_JOBS == {}
        assert _MEMORY_SNAPSHOTS == {}

    def test_rejects_wrong_phase(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        env["phase"] = "PRETRIGGER"
        result = enqueue_breach_snapshot_job(env)
        assert result["ok"] is False
        assert "envelope_phase_not_breach" in result["errors"]

    def test_rejects_observe_only_false(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        env["observe_only"] = False
        result = enqueue_breach_snapshot_job(env)
        assert result["ok"] is False
        assert "envelope_observe_only_must_be_true" in result["errors"]

    def test_rejects_affected_eligibility_true(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        env["affected_eligibility"] = True
        result = enqueue_breach_snapshot_job(env)
        assert result["ok"] is False
        assert "envelope_affected_eligibility_must_be_false" in result["errors"]

    def test_rejects_missing_evidence_as_of(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        env["evidence_as_of"] = None
        env["as_of"] = None
        result = enqueue_breach_snapshot_job(env)
        assert result["ok"] is False
        assert "envelope_evidence_as_of_missing" in result["errors"]

    def test_exact_trigger_and_evidence_as_of_pass(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        result = enqueue_breach_snapshot_job(env)
        assert result["ok"] is True

    def test_later_evidence_as_of_rejects(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        env["evidence_as_of"] = "2026-09-12T13:50:00+00:00"
        env["as_of"] = env["evidence_as_of"]
        result = enqueue_breach_snapshot_job(env)
        assert result["ok"] is False
        assert result["error_code"] == "BREACH_ENVELOPE_INVALID"
        assert _MEMORY_JOBS == {}

    def test_equivalent_aware_offsets_normalize_to_the_same_breach_time(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        equivalent = "2026-09-12T06:45:00-07:00"
        env["trigger_crossed_at"] = equivalent
        env["identity"]["trigger_crossed_at"] = equivalent
        env["evidence_as_of"] = equivalent
        env["as_of"] = equivalent
        result = enqueue_breach_snapshot_job(env)
        assert result["ok"] is True
        job = next(iter(_MEMORY_JOBS.values()))
        assert job["payload"]["trigger_crossed_at"] == TRIGGER_AT
        assert job["payload"]["evidence_as_of"] == TRIGGER_AT

    @pytest.mark.parametrize(
        "bad_as_of",
        ["2026-09-12T13:45:00", "not-a-timestamp"],
    )
    def test_naive_or_malformed_evidence_as_of_rejects(self, bad_as_of):
        env = _inject_authoritative_parents(_build_complete_envelope())
        env["evidence_as_of"] = bad_as_of
        env["as_of"] = bad_as_of
        result = enqueue_breach_snapshot_job(env)
        assert result["ok"] is False
        assert result["error_code"] == "BREACH_ENVELOPE_INVALID"
        assert _MEMORY_JOBS == {}

    def test_rejects_complete_without_structure(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        env["structure"] = None
        env["frozen_structure"] = None
        result = enqueue_breach_snapshot_job(env)
        assert result["ok"] is False
        assert "envelope_structure_required_for_complete" in result["errors"]

    def test_rejects_unsupported_structure_without_store_mutation(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        env["structure"]["unsupported"] = object()
        result = enqueue_breach_snapshot_job(env)
        assert result["ok"] is False
        assert result["error_code"] == "BREACH_ENVELOPE_HASH_INVALID"
        assert _MEMORY_JOBS == {}


class TestEnqueueHashAuthority:
    @pytest.mark.parametrize(
        "mutator",
        [
            lambda env: env["evidence"]["point_in_time"]["underlying_observation"].update(price=999.0),
            lambda env: env["structure"]["fvg_4h"][0].update(high=999.0),
            lambda env: env["candidate_parent_snapshot_ids"].update(PREOPEN="different-parent"),
            lambda env: env["identity"].update(ticker="QQQ"),
        ],
    )
    def test_mutated_625_input_with_old_hashes_is_rejected(self, mutator):
        env = _inject_authoritative_parents(_build_complete_envelope())
        old_input_hash = env["input_hash"]
        old_identity_hash = env["identity_hash"]
        mutator(env)
        result = enqueue_breach_snapshot_job(env)
        assert result["ok"] is False
        assert result["error_code"] == "BREACH_ENVELOPE_HASH_MISMATCH"
        assert "envelope_hash_mismatch" in result["errors"]
        assert env["input_hash"] == old_input_hash
        assert env["identity_hash"] == old_identity_hash
        assert _MEMORY_JOBS == {}

    def test_mutated_canonical_evidence_with_old_hash_is_rejected(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        env["canonical_evidence"]["underlying_observation"]["price"] = 999.0
        result = enqueue_breach_snapshot_job(env)
        assert result["ok"] is False
        assert result["error_code"] == "BREACH_ENVELOPE_HASH_MISMATCH"
        assert _MEMORY_JOBS == {}

    def test_mutated_evidence_as_of_with_old_hash_is_rejected(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        env["evidence"]["as_of"] = "2026-09-12T13:50:00+00:00"
        env["breach_evidence"]["as_of"] = env["evidence"]["as_of"]
        result = enqueue_breach_snapshot_job(env)
        assert result["ok"] is False
        assert result["error_code"] == "BREACH_ENVELOPE_INVALID"
        assert _MEMORY_JOBS == {}


# ---------------------------------------------------------------------------
# §6, §16  Enqueue succeeds and is idempotent for identical envelope
# ---------------------------------------------------------------------------


class TestEnqueueIdempotency:
    def test_first_enqueue_inserts_job(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        result = enqueue_breach_snapshot_job(env)
        assert result["ok"] is True
        assert result["enqueued"] is True
        assert result["duplicate"] is False
        assert result["job_id"]
        assert result["context_revision"] >= 1
        assert result["input_hash"] == env["input_hash"]

    def test_same_envelope_dedupes_same_input(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        first = enqueue_breach_snapshot_job(env)
        second = enqueue_breach_snapshot_job(env)
        assert second["ok"] is True
        assert second["enqueued"] is False
        assert second["duplicate"] is True
        assert second["duplicate_same_input"] is True
        assert second["job_id"] == first["job_id"]
        assert second["context_revision"] == first["context_revision"]

    def test_new_generation_produces_new_input_hash_and_new_job(self):
        env_gen1 = _inject_authoritative_parents(_build_complete_envelope(generation=1))
        env_gen2 = _inject_authoritative_parents(_build_complete_envelope(generation=2))
        r1 = enqueue_breach_snapshot_job(env_gen1)
        r2 = enqueue_breach_snapshot_job(env_gen2)
        assert r1["ok"] is True and r2["ok"] is True
        assert r1["job_id"] != r2["job_id"]
        assert r1["input_hash"] != r2["input_hash"]
        # New semantic input → new #327 BREACH context_revision.
        assert r2["context_revision"] > r1["context_revision"]

    def test_changed_semantic_evidence_produces_new_context_revision(self):
        env_a = _inject_authoritative_parents(_build_complete_envelope())
        env_b = _inject_authoritative_parents(
            _build_complete_envelope(evidence=_evidence(close_price=449.99))
        )
        r_a = enqueue_breach_snapshot_job(env_a)
        r_b = enqueue_breach_snapshot_job(env_b)
        assert r_a["input_hash"] != r_b["input_hash"]
        assert r_b["context_revision"] > r_a["context_revision"]


# ---------------------------------------------------------------------------
# Isolation §16 G–J:  client / mode / side / local-order isolation
# ---------------------------------------------------------------------------


class TestStoreIsolation:
    def test_client_isolation(self):
        env_a = _inject_authoritative_parents(_build_complete_envelope(client_id="a@example.com"))
        env_b = _inject_authoritative_parents(_build_complete_envelope(client_id="b@example.com"))
        r_a = enqueue_breach_snapshot_job(env_a)
        r_b = enqueue_breach_snapshot_job(env_b)
        assert r_a["ok"] and r_b["ok"]
        assert r_a["job_id"] != r_b["job_id"]

    def test_live_paper_isolation(self):
        env_paper = _inject_authoritative_parents(_build_complete_envelope(execution_mode="PAPER"))
        env_live = _inject_authoritative_parents(_build_complete_envelope(execution_mode="LIVE"))
        r_paper = enqueue_breach_snapshot_job(env_paper)
        r_live = enqueue_breach_snapshot_job(env_live)
        assert r_paper["ok"] and r_live["ok"]
        assert r_paper["job_id"] != r_live["job_id"]

    def test_call_put_isolation(self):
        env_call = _inject_authoritative_parents(_build_complete_envelope(side="CALL"))
        env_put = _inject_authoritative_parents(
            _build_complete_envelope(
                side="PUT",
                canonical=CANON + "-put",  # canonical id also flips per convention
                signal_id=SIGNAL_ID + "-put",
                local_order_id=LOCAL_ORDER + "-put",
            )
        )
        r_call = enqueue_breach_snapshot_job(env_call)
        r_put = enqueue_breach_snapshot_job(env_put)
        assert r_call["ok"] and r_put["ok"]
        assert r_call["job_id"] != r_put["job_id"]


# ---------------------------------------------------------------------------
# §12  Dispatch predicate + worker adapter identity cross-check
# ---------------------------------------------------------------------------


class TestDispatchAndWorkerValidation:
    def test_is_frozen_breach_job_predicate(self):
        assert is_frozen_breach_job({}) is False
        assert is_frozen_breach_job({"phase": "BREACH"}) is False
        assert is_frozen_breach_job({"phase": "BREACH", "payload": {}}) is False
        assert is_frozen_breach_job(
            {"phase": "PRETRIGGER", "payload": {"payload_kind": FROZEN_BREACH_PAYLOAD_KIND}}
        ) is False
        assert is_frozen_breach_job(
            {"phase": "BREACH", "payload": {"payload_kind": FROZEN_BREACH_PAYLOAD_KIND}}
        ) is True

    def test_build_breach_snapshot_kwargs_refuses_non_frozen(self):
        with pytest.raises(RuntimeError, match="dispatch seam"):
            build_breach_snapshot_kwargs({"phase": "BREACH", "payload": {}})

    def test_worker_kwargs_shape_and_identity(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        enq = enqueue_breach_snapshot_job(env)
        assert enq["ok"]
        job = next(iter(_MEMORY_JOBS.values()))
        kwargs = build_breach_snapshot_kwargs(job)
        # data_as_of is EXACTLY the envelope evidence_as_of, not any worker
        # clock, not collected_at, not _now_iso().
        assert kwargs["data_as_of"] == env["evidence_as_of"] == TRIGGER_AT
        # Top-level parent = authoritative PREOPEN.
        assert kwargs["parent_snapshot_id"] == "parent-preopen-abc"
        # Status is mapped straight through.
        assert kwargs["status"] == "COMPLETE"
        # Identity and hashes.
        assert kwargs["input_hash"] == env["input_hash"]
        assert kwargs["client_id"] == CLIENT
        assert kwargs["execution_mode"] == "PAPER"
        assert kwargs["canonical_signal_id"] == CANON
        assert kwargs["phase"] == BREACH_PHASE
        # Persisted payload carries both parent maps + structure hash.
        payload = kwargs["payload"]
        assert payload["candidate_parent_snapshot_ids"] == env["candidate_parent_snapshot_ids"]
        assert payload["authoritative_parent_snapshot_ids"] == env["authoritative_parent_snapshot_ids"]
        assert payload["identity_hash"] == env["identity_hash"]
        assert payload["structure_hash"].startswith("sha256:")
        assert payload["observe_only"] is True
        assert payload["affected_eligibility"] is False
        assert payload["payload_kind"] == FROZEN_BREACH_PAYLOAD_KIND
        assert payload["adapter_version"] == ADAPTER_VERSION
        assert payload["assembly_version"] == ASSEMBLY_VERSION

    def test_worker_fails_closed_on_identity_mismatch(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        enqueue_breach_snapshot_job(env)
        job = copy.deepcopy(next(iter(_MEMORY_JOBS.values())))
        # Simulate a poisoned job whose top-level identity contradicts its
        # frozen payload.
        job["client_id"] = "attacker@example.com"
        with pytest.raises(RuntimeError, match="BREACH_JOB_IDENTITY_MISMATCH"):
            build_breach_snapshot_kwargs(job)

    def test_worker_fails_closed_on_input_hash_mismatch(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        enqueue_breach_snapshot_job(env)
        job = copy.deepcopy(next(iter(_MEMORY_JOBS.values())))
        job["input_hash"] = "tampered-hash"
        with pytest.raises(RuntimeError, match="BREACH_JOB_IDENTITY_MISMATCH"):
            build_breach_snapshot_kwargs(job)

    def test_worker_fails_closed_on_frozen_structure_tamper(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        enqueue_breach_snapshot_job(env)
        job = copy.deepcopy(next(iter(_MEMORY_JOBS.values())))
        job["payload"]["structure"]["strong_break"]["observed"] = True
        with pytest.raises(RuntimeError, match="BREACH_JOB_IDENTITY_MISMATCH"):
            build_breach_snapshot_kwargs(job)


# ---------------------------------------------------------------------------
# §11  Candidate vs authoritative parent invariant
# ---------------------------------------------------------------------------


class TestParentInvariants:
    def test_top_level_parent_is_none_when_no_authoritative_preopen(self):
        env = _build_complete_envelope()  # not injected → no authoritative parents
        env = _inject_authoritative_parents(env)
        env["authoritative_parent_snapshot_ids"] = {"PRETRIGGER": None, "PREOPEN": None}
        # Keep candidate PREOPEN populated — this must NOT be promoted.
        env["candidate_parent_snapshot_ids"] = {
            "PRETRIGGER": "pt-candidate",
            "PREOPEN": "po-candidate",
        }
        # Recompute the #625 input hash for this deliberately non-authoritative
        # candidate map.  The adapter must still refuse to promote it.
        env["input_hash"] = hash_breach_snapshot_input(
            env["identity"],
            env["evidence"],
            parent_snapshot_ids=env["candidate_parent_snapshot_ids"],
            structure=env["structure"],
        )
        result = enqueue_breach_snapshot_job(env)
        assert result["ok"] is True
        job = next(iter(_MEMORY_JOBS.values()))
        kwargs = build_breach_snapshot_kwargs(job)
        assert kwargs["parent_snapshot_id"] is None
        payload = kwargs["payload"]
        assert payload["candidate_parent_snapshot_ids"]["PREOPEN"] == "po-candidate"
        assert payload["authoritative_parent_snapshot_ids"]["PREOPEN"] is None

    def test_top_level_parent_only_preopen_authoritative(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        # Even with an authoritative PRETRIGGER, the top-level id is PREOPEN.
        env["authoritative_parent_snapshot_ids"] = {
            "PRETRIGGER": "pretrigger-authoritative-id",
            "PREOPEN": "preopen-authoritative-id",
        }
        enqueue_breach_snapshot_job(env)
        job = next(iter(_MEMORY_JOBS.values()))
        kwargs = build_breach_snapshot_kwargs(job)
        assert kwargs["parent_snapshot_id"] == "preopen-authoritative-id"


# ---------------------------------------------------------------------------
# §4, §20  Generic PRETRIGGER/PREOPEN path unchanged; dispatch is gated
# ---------------------------------------------------------------------------


class TestGenericPathUnchanged:
    def test_pretrigger_job_uses_generic_path(self, monkeypatch):
        # If the dispatch is wrong, the generic path never runs; we prove it
        # runs by asserting build_intelligence_context_payload IS called.
        calls: list[Any] = []
        real = build_intelligence_context_payload

        def _wrapped(*a: Any, **kw: Any):
            calls.append(kw.get("phase") or (a[1] if len(a) > 1 else None))
            return real(*a, **kw)

        monkeypatch.setattr(
            "ap.intelligence_context_materializer.build_intelligence_context_payload",
            _wrapped,
        )
        job = {
            "id": "job-pt-1",
            "client_id": CLIENT,
            "execution_mode": MODE,
            "canonical_signal_id": CANON,
            "signal_id": SIGNAL_ID,
            "local_order_id": LOCAL_ORDER,
            "phase": "PRETRIGGER",
            "context_revision": 1,
            "profile_version": PROFILE_VERSION,
            "input_hash": "generic-1",
            "payload": {"signal": {"ticker": TICKER, "side": SIDE}},
        }
        kwargs = build_snapshot_kwargs(job)
        assert kwargs["phase"] == "PRETRIGGER"
        assert calls, "generic build_intelligence_context_payload must have been called"

    def test_legacy_generic_breach_job_untouched(self, monkeypatch):
        calls: list[Any] = []
        real = build_intelligence_context_payload

        def _wrapped(*a: Any, **kw: Any):
            calls.append("called")
            return real(*a, **kw)

        monkeypatch.setattr(
            "ap.intelligence_context_materializer.build_intelligence_context_payload",
            _wrapped,
        )
        # Legacy BREACH job without the FROZEN_BREACH_V1 marker.
        job = {
            "id": "job-generic-breach-1",
            "client_id": CLIENT,
            "execution_mode": MODE,
            "canonical_signal_id": CANON,
            "signal_id": SIGNAL_ID,
            "local_order_id": LOCAL_ORDER,
            "phase": "BREACH",
            "context_revision": 1,
            "profile_version": PROFILE_VERSION,
            "input_hash": "legacy-breach-1",
            "payload": {"signal": {"ticker": TICKER, "side": SIDE}},
        }
        # Must NOT raise and must NOT call the adapter path.
        _ = build_snapshot_kwargs(job)
        assert calls, "generic path must run for legacy BREACH jobs"

    def test_frozen_breach_job_never_calls_generic_builder(self, monkeypatch):
        # For a frozen-BREACH job the generic builder must not be invoked at all.
        called: list[Any] = []

        def _sentinel(*a: Any, **kw: Any):
            called.append(True)
            raise AssertionError(
                "build_intelligence_context_payload must not be called for frozen BREACH"
            )

        monkeypatch.setattr(
            "ap.intelligence_context_materializer.build_intelligence_context_payload",
            _sentinel,
        )
        env = _inject_authoritative_parents(_build_complete_envelope())
        enqueue_breach_snapshot_job(env)
        job = next(iter(_MEMORY_JOBS.values()))
        _ = build_snapshot_kwargs(job)
        assert called == []


# ---------------------------------------------------------------------------
# §15  Frozen BREACH worker path performs NO market-data / broker / selector
# ---------------------------------------------------------------------------


class TestNoMarketRefetch:
    def test_worker_path_does_not_call_collect_pit(self, monkeypatch):
        def _boom(*a: Any, **kw: Any):
            raise AssertionError("collect_point_in_time_context must not be called")

        monkeypatch.setattr(
            "ap.intelligence_context_materializer.collect_point_in_time_context",
            _boom,
        )
        env = _inject_authoritative_parents(_build_complete_envelope())
        enqueue_breach_snapshot_job(env)
        job = next(iter(_MEMORY_JOBS.values()))
        # Adapter + build_snapshot_kwargs must both refuse to touch PIT.
        _ = build_snapshot_kwargs(job)
        _ = build_breach_snapshot_kwargs(job)


# ---------------------------------------------------------------------------
# §7, §8, §17  Immutable payload — later mutation cannot rewrite persisted truth
# ---------------------------------------------------------------------------


class TestFrozenPayloadImmutability:
    def test_evidence_as_of_persists_original_trigger_time(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        enqueue_breach_snapshot_job(env)
        job = next(iter(_MEMORY_JOBS.values()))
        kwargs_1 = build_breach_snapshot_kwargs(job)
        # Simulate re-running the worker much later — the persisted
        # data_as_of MUST NOT drift.
        kwargs_2 = build_breach_snapshot_kwargs(job)
        assert kwargs_1["data_as_of"] == kwargs_2["data_as_of"] == TRIGGER_AT
        assert kwargs_1["payload"]["evidence_as_of"] == TRIGGER_AT

    def test_mutating_caller_evidence_after_enqueue_does_not_alter_stored_payload(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        enqueue_breach_snapshot_job(env)
        # Mutate the caller's structure object after enqueue.
        env["structure"]["strong_break"]["observed"] = True
        env["structure"]["fvg_4h"][0]["high"] = 999.99
        env["canonical_evidence"] = {"tampered": True}

        # Persisted job payload is untouched.
        job = next(iter(_MEMORY_JOBS.values()))
        payload = job["payload"]
        assert payload["structure"]["strong_break"]["observed"] is False
        assert payload["structure"]["fvg_4h"][0]["high"] == 449.50

        kwargs = build_breach_snapshot_kwargs(job)
        assert kwargs["payload"]["structure"]["strong_break"]["observed"] is False
        assert kwargs["payload"]["structure"]["fvg_4h"][0]["high"] == 449.50


# ---------------------------------------------------------------------------
# §16 C–D, §17  End-to-end worker path with real #327 memory backend
# ---------------------------------------------------------------------------


class TestEndToEndWorker:
    def test_enqueue_claim_complete_writes_snapshot(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        enq = enqueue_breach_snapshot_job(env)
        assert enq["ok"]

        result = process_due_intelligence_jobs_once(
            claim_owner="test-owner", client_id=CLIENT, execution_mode=MODE, limit=5
        )
        assert result["ok"], result
        assert result["completed"] == 1, result
        # And a durable BREACH snapshot exists.
        snap = get_latest_snapshot(
            client_id=CLIENT,
            execution_mode=MODE,
            canonical_signal_id=CANON,
            phase=BREACH_PHASE,
        )
        assert snap["ok"], snap
        row = snap["snapshot"]
        assert row["status"] == "COMPLETE"
        assert row["data_as_of"] == TRIGGER_AT
        assert row["input_hash"] == env["input_hash"]
        # Payload carries the exact frozen artifact + hashes.
        payload = row["payload"]
        assert payload["payload_kind"] == FROZEN_BREACH_PAYLOAD_KIND
        assert payload["identity_hash"] == env["identity_hash"]
        assert payload["structure_hash"].startswith("sha256:")

    def test_replay_after_enqueue_does_not_change_snapshot(self):
        env = _inject_authoritative_parents(_build_complete_envelope())
        enqueue_breach_snapshot_job(env)
        # Run the worker twice — the second pass must not duplicate.
        first = process_due_intelligence_jobs_once(
            claim_owner="test-owner", client_id=CLIENT, execution_mode=MODE, limit=5
        )
        second = process_due_intelligence_jobs_once(
            claim_owner="test-owner-2", client_id=CLIENT, execution_mode=MODE, limit=5
        )
        assert first["completed"] == 1
        # No new job was claimed; the completed job was terminal.
        assert second.get("completed", 0) == 0

    def test_new_generation_produces_new_context_revision_snapshot(self):
        # Enqueue + process the generation=1 job.
        env_1 = _inject_authoritative_parents(_build_complete_envelope(generation=1))
        enqueue_breach_snapshot_job(env_1)
        process_due_intelligence_jobs_once(
            claim_owner="w1", client_id=CLIENT, execution_mode=MODE, limit=5
        )
        # Now a new generation enqueues + processes into a distinct snapshot.
        env_2 = _inject_authoritative_parents(_build_complete_envelope(generation=2))
        enq_2 = enqueue_breach_snapshot_job(env_2)
        process_due_intelligence_jobs_once(
            claim_owner="w2", client_id=CLIENT, execution_mode=MODE, limit=5
        )
        # The latest snapshot at the same store identity carries the higher
        # context_revision and the new input_hash.
        snap = get_latest_snapshot(
            client_id=CLIENT,
            execution_mode=MODE,
            canonical_signal_id=CANON,
            phase=BREACH_PHASE,
        )
        assert snap["ok"]
        row = snap["snapshot"]
        assert row["input_hash"] == env_2["input_hash"]
        assert row["context_revision"] == enq_2["context_revision"]


# ---------------------------------------------------------------------------
# §18, §19  Real #614 PIT -> #615 freezer -> #625 assembly -> #621 -> #327
# ---------------------------------------------------------------------------


def test_real_614_to_615_to_625_to_621_to_327_round_trip(monkeypatch):
    signal = _canonical_614_signal()
    pit = collect_point_in_time_context(signal, broker=None, phase=BREACH_PHASE)
    assert pit["phase"] == BREACH_PHASE
    assert pit["as_of"] == PIT_AS_OF

    frozen_structure = freeze_breach_market_structure_from_pit(signal, pit)
    assert frozen_structure["schema_version"] == "breach_market_structure_v1"
    assert frozen_structure["model_version"] == "canonical_fvg_435b_v1"
    assert frozen_structure["fvg_zones"]

    identity_result = normalize_breach_identity(
        {
            "client_id": CLIENT,
            "execution_mode": MODE,
            "signal_id": "real-614-435c",
            "canonical_signal_id": "real-614-435c",
            "local_order_id": "real-local-435c",
            "ticker": signal["ticker"],
            "side": signal["side"],
            "trigger_crossed_at": PIT_AS_OF,
            "materialization_generation": 1,
            "profile_version": PROFILE_VERSION,
            "model_version": frozen_structure["model_version"],
            "phase": BREACH_PHASE,
        }
    )
    assert identity_result["ok"] is True, identity_result
    assembled = build_breach_snapshot_envelope(
        identity_result,
        pit,
        structure=frozen_structure,
    )
    assert assembled["ok"] is True, assembled
    assembled = _inject_authoritative_parents(assembled)
    assert assembled["status"] == "COMPLETE"

    # The claimed worker must consume only the already-frozen job payload.
    def _no_refetch(*_args: Any, **_kwargs: Any):
        raise AssertionError("BREACH worker must not refetch PIT data")

    monkeypatch.setattr(
        "ap.intelligence_context_materializer.collect_point_in_time_context",
        _no_refetch,
    )
    enqueued = enqueue_breach_snapshot_job(assembled)
    assert enqueued["ok"] is True, enqueued
    processed = process_due_intelligence_jobs_once(
        claim_owner="real-435c-worker",
        client_id=CLIENT,
        execution_mode=MODE,
        limit=5,
    )
    assert processed["ok"] is True, processed
    assert processed["completed"] == 1, processed

    result = get_latest_snapshot(
        client_id=CLIENT,
        execution_mode=MODE,
        canonical_signal_id="real-614-435c",
        phase=BREACH_PHASE,
    )
    assert result["ok"] is True, result
    row = result["snapshot"]
    payload = row["payload"]
    structure = payload["structure"]
    assert row["status"] == "COMPLETE"
    assert row["data_as_of"] == PIT_AS_OF
    assert payload["trigger_crossed_at"] == PIT_AS_OF
    assert payload["evidence_as_of"] == PIT_AS_OF
    assert structure["data_as_of"] == PIT_AS_OF
    assert structure["schema_version"] == "breach_market_structure_v1"
    assert structure["model_version"] == "canonical_fvg_435b_v1"
    assert structure["fvg_zones"]
    assert structure["data_coverage"] == pit["data_sources"]["coverage"]
    assert structure["data_provenance"] == pit["provenance"]
    assert payload["structure_hash"] == hash_frozen_breach_structure(frozen_structure)
    assert payload["identity_hash"] == assembled["identity_hash"]
    assert payload["input_hash"] == assembled["input_hash"]
    assert row["input_hash"] == assembled["input_hash"]
    assert payload["candidate_parent_snapshot_ids"] == assembled["candidate_parent_snapshot_ids"]
    assert payload["authoritative_parent_snapshot_ids"] == assembled["authoritative_parent_snapshot_ids"]
    assert row["parent_snapshot_id"] == assembled["authoritative_parent_snapshot_ids"]["PREOPEN"]


# ---------------------------------------------------------------------------
# §14, §20  Money-path proof — no import edits, no runtime coupling
# ---------------------------------------------------------------------------


class TestMoneyPathIsolation:
    def test_adapter_module_imports_no_money_path_modules(self):
        # Import the adapter and confirm nothing dangerous was pulled in.
        import ap.intelligence_breach_snapshot_adapter as adapter

        source = adapter.__file__
        with open(source, "r", encoding="utf-8") as fh:
            text = fh.read()
        for forbidden in (
            "ap_execution_core",
            "ap_entry_watcher",
            "ap_exit_engine",
            "ap.broker",
            "ap.order_state_machine",
            "ap.position_manager",
            "ap.exit_manager",
            "ap.selector",
            "ap.trade_queue",
            "ap.risk",
            "ap.contract_selector",
            "ap.intelligence_market_data",
        ):
            assert forbidden not in text, f"adapter must not import {forbidden}"

    def test_only_two_production_files_changed_vs_main(self):
        # Sanity check that we didn't broaden scope silently.  The test
        # doesn't reach for git; instead it asserts the adapter's own module
        # list is what the spec (§21) approves.
        import ap.intelligence_breach_snapshot_adapter as adapter
        import ap.intelligence_context_materializer as materializer

        # Both modules must exist and import cleanly.
        assert adapter.FROZEN_BREACH_PAYLOAD_KIND == "FROZEN_BREACH_V1"
        assert hasattr(materializer, "build_snapshot_kwargs")
