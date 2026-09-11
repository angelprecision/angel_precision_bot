"""PR #435: enrichment / handoff failure-shape matrix (money-path non-authority).

Expands the observe-only proofs in test_p0_435_exactly_one_submit_and_zero_broker
and test_p0_435_money_path_non_authority with a table-driven matrix covering:

  * enrichment exception shapes (TypeError / ValueError / RuntimeError / KeyError /
    timeout-like / BreachMaterializationRejected) — pre-submit diagnostic abort:
    submit_count == 0, zero broker cancel / position / size mutation
  * handoff shapes (disabled / capacity full / enqueue raises) — non-gating:
    submit_existing_entry.call_count == 1, zero broker cancel / position mutation

Production seam (ap_execution_core._on_entry_trigger):
  1) enqueue_breach_context_best_effort  (observe-only; try/except; never gates)
  2) optional enrichment / materializer work (must not own money-path authority)
  3) order_state_machine.submit_existing_entry  (existing submit path)

HARD HOLD / observe_only — no production money-path changes.
"""
from __future__ import annotations

import os
import sys
from concurrent.futures import TimeoutError as FuturesTimeoutError
from pathlib import Path
from typing import Any, Callable
from unittest import mock

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

os.environ.setdefault("INTELLIGENCE_CONTEXT_STORE_BACKEND", "memory")

import ap.intelligence_context_handoff as handoff  # noqa: E402
from ap.intelligence_context_materializer import (  # noqa: E402
    BreachMaterializationRejected,
)
from ap.intelligence_snapshot_store import _reset_memory_store_for_tests  # noqa: E402

# Reuse the proven money-path harness (prefer extend, don't invent parallel stubs).
from tests.test_p0_435_exactly_one_submit_and_zero_broker import (  # noqa: E402
    MutationLedger,
    _breach_signal,
    _run_breach_then_submit,
)


def setup_function():
    _reset_memory_store_for_tests()
    while True:
        try:
            handoff._CAPACITY.release()
        except ValueError:
            break


_ZERO_MUTATIONS = {
    "submit": 0,
    "cancel": 0,
    "order_meta": 0,
    "positions": 0,
    "proof": 0,
    "queue": 0,
    "broker_cancel": 0,
}


def _assert_zero_money_path_mutation(out: dict[str, Any], *, ledger: MutationLedger) -> None:
    """Intelligence failure must never cancel, close, or resize positions."""
    assert out["broker_cancel_count"] == 0
    assert ledger.cancel_calls == []
    assert ledger.broker_cancels == []
    assert ledger.position_mutations == []
    assert ledger.order_meta_patches == []
    assert ledger.proof_mutations == []
    assert ledger.queue_mutations == []
    mutations = out.get("mutations") or ledger.snapshot()
    for key in ("cancel", "broker_cancel", "positions", "order_meta", "proof", "queue"):
        assert mutations.get(key, 0) == 0, f"unexpected {key} mutation: {mutations}"


def _assert_non_authority_handoff(handoff_result: dict[str, Any] | None) -> None:
    if not isinstance(handoff_result, dict):
        return
    assert handoff_result.get("affected_eligibility") in (None, False)
    assert handoff_result.get("submit") is not True
    assert handoff_result.get("cancel") is not True
    assert handoff_result.get("mutate_eligibility") is not True
    assert handoff_result.get("broker_submit") is not True
    assert handoff_result.get("broker_cancel") is not True


def _make_enrich_raiser(exc: BaseException) -> Callable[..., Any]:
    def _enrich(signal, **kwargs):
        # Hostile enrichment must not be treated as money-path authority even if
        # it *attempts* broker/OSM calls before exploding — the money-path fence
        # after the exception must not submit/cancel further. We only raise here
        # (no mutation) so the matrix isolates the fence, not enricher hygiene.
        raise exc

    return _enrich


def _ok_sync_handoff(monkeypatch):
    """Enable worker + sync materializer enqueue so handoff is accepted."""

    def _ok_enqueue(signal, **kwargs):
        return {"ok": True, "inserted": True, "job_id": "j-shape"}

    def _sync_submit(enqueue, *args, phase, signal_id, **kwargs):
        return {"ok": True, "accepted": True, **(enqueue(*args, **kwargs) or {})}

    fake_mat = mock.Mock(enqueue_breach_context=_ok_enqueue)
    monkeypatch.setenv("INTELLIGENCE_CONTEXT_WORKER_ENABLED", "1")
    monkeypatch.setattr(handoff, "submit_intelligence_enqueue", _sync_submit)
    return fake_mat


# ---------------------------------------------------------------------------
# Enrichment exception shapes → pre-submit diagnostic abort (submit == 0)
# ---------------------------------------------------------------------------

_ENRICHMENT_SHAPES = [
    pytest.param(TypeError("enrichment_type_error"), id="TypeError"),
    pytest.param(ValueError("enrichment_value_error"), id="ValueError"),
    pytest.param(RuntimeError("enrichment_runtime_error"), id="RuntimeError"),
    pytest.param(KeyError("missing_enrichment_field"), id="KeyError"),
    pytest.param(TimeoutError("enrichment_timed_out"), id="TimeoutError"),
    pytest.param(FuturesTimeoutError("executor_enrichment_timeout"), id="FuturesTimeoutError"),
    pytest.param(
        BreachMaterializationRejected("BREACH_IDENTITY_CONFLICT_CLIENT_ID"),
        id="BreachMaterializationRejected",
    ),
]


@pytest.mark.parametrize("exc", _ENRICHMENT_SHAPES)
def test_enrichment_exception_shape_zero_submit_zero_broker(monkeypatch, exc):
    """Each enrichment failure shape: diagnostic-only abort; no money-path authority.

    Zero submit is correct here: the harness models a *pre-submit* enrichment
    abort (exception between handoff and submit_existing_entry). Production
    async materializer enrichment is off the money path; either way intelligence
    failure must not cancel / close / resize.
    """
    fake_mat = _ok_sync_handoff(monkeypatch)
    ledger = MutationLedger()
    with mock.patch.dict(sys.modules, {"ap.intelligence_context_materializer": fake_mat}):
        out = _run_breach_then_submit(
            ledger=ledger,
            enrich_fn=_make_enrich_raiser(exc),
        )

    assert out["disposition"] == "ENRICHMENT_DIAGNOSTIC_ONLY"
    assert out["submit_count"] == 0
    assert out["enrichment_error"]
    _assert_zero_money_path_mutation(out, ledger=ledger)
    assert out["mutations"] == _ZERO_MUTATIONS
    _assert_non_authority_handoff(out.get("handoff"))
    # OSM submit never invoked; cancel never invoked by the fence.
    # (enrich_fn did not receive a live call recording path beyond ledger.)
    assert "osm" not in out or out["osm"].submit_existing_entry.call_count == 0


@pytest.mark.parametrize("exc", _ENRICHMENT_SHAPES)
def test_enrichment_exception_shape_does_not_call_cancel_or_close(monkeypatch, exc):
    """After enrichment boom, money-path fence must not invoke cancel/close/size."""
    fake_mat = _ok_sync_handoff(monkeypatch)
    ledger = MutationLedger()

    def _hostile_enrich(signal, **kwargs):
        # Record a would-be mutation attempt on a side channel, then raise.
        # The fence must not add cancel/submit after the exception.
        kwargs["ledger"].position_mutations.append(
            {"attempt": "size_mutation_blocked_by_exception", "size": 0}
        )
        raise exc

    with mock.patch.dict(sys.modules, {"ap.intelligence_context_materializer": fake_mat}):
        out = _run_breach_then_submit(ledger=ledger, enrich_fn=_hostile_enrich)

    assert out["disposition"] == "ENRICHMENT_DIAGNOSTIC_ONLY"
    assert out["submit_count"] == 0
    assert out["broker_cancel_count"] == 0
    # Hostile enrich may have dirtied the ledger side-channel; the money-path
    # fence itself must still show zero submit / zero broker cancel.
    assert len(ledger.submit_calls) == 0
    assert len(ledger.broker_cancels) == 0
    assert len(ledger.cancel_calls) == 0
    _assert_non_authority_handoff(out.get("handoff"))


# ---------------------------------------------------------------------------
# Handoff shapes → non-gating (exactly one submit)
# ---------------------------------------------------------------------------

def test_handoff_disabled_shape_exactly_one_submit(monkeypatch):
    monkeypatch.delenv("INTELLIGENCE_CONTEXT_WORKER_ENABLED", raising=False)
    ledger = MutationLedger()
    out = _run_breach_then_submit(ledger=ledger)
    assert out["handoff"] == {"ok": True, "accepted": False, "disabled": True}
    assert out["submit_count"] == 1
    assert out["osm"].submit_existing_entry.call_count == 1
    assert out["broker_cancel_count"] == 0
    out["osm"].cancel_pending_entry.assert_not_called()
    out["broker"].cancel.assert_not_called()
    _assert_non_authority_handoff(out["handoff"])
    # Submit expected (==1); cancel/position/size must stay zero.
    assert ledger.cancel_calls == []
    assert ledger.broker_cancels == []
    assert ledger.position_mutations == []
    assert ledger.order_meta_patches == []
    assert out["mutations"]["cancel"] == 0
    assert out["mutations"]["broker_cancel"] == 0
    assert out["mutations"]["positions"] == 0


def test_handoff_capacity_full_shape_exactly_one_submit(monkeypatch):
    monkeypatch.setenv("INTELLIGENCE_CONTEXT_WORKER_ENABLED", "1")
    ledger = MutationLedger()
    acquired = 0
    while handoff._CAPACITY.acquire(blocking=False):
        acquired += 1
    try:
        out = _run_breach_then_submit(ledger=ledger)
    finally:
        for _ in range(acquired):
            try:
                handoff._CAPACITY.release()
            except ValueError:
                break
    assert out["handoff"]["error"] == "handoff_capacity_exhausted"
    assert out["handoff"]["accepted"] is False
    assert out["submit_count"] == 1
    assert out["osm"].submit_existing_entry.call_count == 1
    assert out["broker_cancel_count"] == 0
    out["osm"].cancel_pending_entry.assert_not_called()
    _assert_non_authority_handoff(out["handoff"])


_HANDOFF_RAISE_SHAPES = [
    pytest.param(RuntimeError("enqueue_pool_rejected"), id="RuntimeError"),
    pytest.param(TypeError("enqueue_bad_callable"), id="TypeError"),
    pytest.param(ValueError("enqueue_bad_args"), id="ValueError"),
    pytest.param(KeyError("enqueue_missing_key"), id="KeyError"),
    pytest.param(TimeoutError("enqueue_submit_timeout"), id="TimeoutError"),
    pytest.param(
        BreachMaterializationRejected("BREACH_EVIDENCE_INVALID_TRIGGER_CROSSED_AT"),
        id="BreachMaterializationRejected",
    ),
]


@pytest.mark.parametrize("exc", _HANDOFF_RAISE_SHAPES)
def test_handoff_enqueue_raises_shape_exactly_one_submit(monkeypatch, exc):
    """Executor/submit rejection shapes must not gate the existing submit path."""
    monkeypatch.setenv("INTELLIGENCE_CONTEXT_WORKER_ENABLED", "1")
    ledger = MutationLedger()

    with mock.patch.object(handoff._EXECUTOR, "submit", side_effect=exc):
        out = _run_breach_then_submit(ledger=ledger)

    assert out["handoff"]["ok"] is False
    assert out["handoff"]["accepted"] is False
    assert out["handoff"].get("error")
    assert out["submit_count"] == 1
    assert out["osm"].submit_existing_entry.call_count == 1
    assert out["broker_cancel_count"] == 0
    out["osm"].cancel_pending_entry.assert_not_called()
    out["broker"].cancel.assert_not_called()
    _assert_non_authority_handoff(out["handoff"])
    assert ledger.position_mutations == []


@pytest.mark.parametrize("exc", _HANDOFF_RAISE_SHAPES)
def test_handoff_fn_raises_into_runner_still_exactly_one_submit(monkeypatch, exc):
    """Production try/except around best-effort handoff: exception → continue submit."""
    monkeypatch.setenv("INTELLIGENCE_CONTEXT_WORKER_ENABLED", "1")
    ledger = MutationLedger()

    def _raising_handoff(*_a, **_k):
        raise exc

    out = _run_breach_then_submit(ledger=ledger, handoff_fn=_raising_handoff)
    assert out["handoff"]["ok"] is False
    assert out["handoff"]["accepted"] is False
    assert out["submit_count"] == 1
    assert out["osm"].submit_existing_entry.call_count == 1
    assert out["broker_cancel_count"] == 0
    out["osm"].cancel_pending_entry.assert_not_called()
    assert ledger.position_mutations == []


def test_valid_enrichment_none_still_exactly_one_submit(monkeypatch):
    """Control row: no enrichment step → exactly one submit, zero cancel."""
    fake_mat = _ok_sync_handoff(monkeypatch)
    ledger = MutationLedger()
    with mock.patch.dict(sys.modules, {"ap.intelligence_context_materializer": fake_mat}):
        out = _run_breach_then_submit(ledger=ledger, enrich_fn=None)
    assert out["disposition"] == "SUBMITTED"
    assert out["submit_count"] == 1
    assert out["osm"].submit_existing_entry.call_count == 1
    assert out["broker_cancel_count"] == 0
    out["osm"].cancel_pending_entry.assert_not_called()
    _assert_non_authority_handoff(out["handoff"])


def test_matrix_eligibility_unchanged_across_enrichment_and_handoff_failures(monkeypatch):
    """Intelligence failure must not flip eligibility / money-path authority flags."""
    shapes_and_expected_submit = []

    # Enrichment boom → 0 submit (pre-submit abort)
    fake_mat = _ok_sync_handoff(monkeypatch)
    ledger = MutationLedger()
    with mock.patch.dict(sys.modules, {"ap.intelligence_context_materializer": fake_mat}):
        out = _run_breach_then_submit(
            ledger=ledger,
            enrich_fn=_make_enrich_raiser(RuntimeError("eligibility_probe")),
        )
    shapes_and_expected_submit.append(("enrichment", out, 0))

    # Handoff disabled → 1 submit
    monkeypatch.delenv("INTELLIGENCE_CONTEXT_WORKER_ENABLED", raising=False)
    ledger2 = MutationLedger()
    out2 = _run_breach_then_submit(ledger=ledger2)
    shapes_and_expected_submit.append(("handoff_disabled", out2, 1))

    for label, result, expected_submit in shapes_and_expected_submit:
        assert result["submit_count"] == expected_submit, label
        assert result["broker_cancel_count"] == 0, label
        _assert_non_authority_handoff(result.get("handoff"))
        # No eligibility authority keys on the disposition envelope.
        assert result.get("affected_eligibility") in (None, False)
        assert result.get("mutate_eligibility") is not True
