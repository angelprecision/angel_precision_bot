"""PR #435 concrete runtime/restart/materializer parity matrix.

Resolves ACTUAL values (not prose) for LIVE/PAPER accept paths and reject
shapes across:
  - normal runtime enqueue_breach_context
  - restart recover_missing_intelligence_jobs
  - deferred materializer build_intelligence_context_payload (accept only)

Reject scenarios assert error_code + no execution-mutation flags.
Ownership loss / missing store assert execution-continues / diagnostic-only.
"""
from __future__ import annotations

import os
import sys
import types
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

os.environ["INTELLIGENCE_CONTEXT_STORE_BACKEND"] = "memory"

from ap.intelligence_context_materializer import (  # noqa: E402
    build_intelligence_context_payload,
    enqueue_breach_context,
    recover_missing_intelligence_jobs,
    resolve_breach_lineage,
    resolve_public_breach_lifecycle,
)
from ap.intelligence_snapshot_store import (  # noqa: E402
    _MEMORY_JOBS,
    _reset_memory_store_for_tests,
    complete_job_with_snapshot,
    claim_due_intelligence_jobs,
)


def setup_function():
    _reset_memory_store_for_tests()


def _base_signal(**overrides: Any) -> dict[str, Any]:
    sig = {
        "signal_id": "sig-parity-1",
        "canonical_signal_id": "canon-parity-1",
        "client_id": "client@example.com",
        "execution_mode": "PAPER",
        "local_order_id": "loid-parity-1",
        "ticker": "SPY",
        "side": "CALL",
        "timeframe": "1d",
        "pattern": "2-3",
        "trigger_price": 500.0,
        "stop_price": 495.0,
        "target_price": 510.0,
        "underlying_price": 501.25,
        "breach_price": 501.25,
        "trigger_crossed_at": "2026-07-14T13:30:00+00:00",
        "trigger_confirmed_at": "2026-07-14T13:30:02+00:00",
        "first_breach_bid": 501.20,
        "first_breach_ask": 501.30,
        "breach_count": 1,
        "candles_5m": [
            {
                "time": "2026-07-14T13:20:00+00:00",
                "open": 500.0, "high": 501.0, "low": 499.5, "close": 500.8, "volume": 1000,
            },
            {
                "time": "2026-07-14T13:30:00+00:00",
                "open": 501.0, "high": 502.0, "low": 500.8, "close": 501.5, "volume": 1200,
            },
        ],
        "candles_15m": [
            {
                "time": "2026-07-14T13:00:00+00:00",
                "open": 499.0, "high": 500.5, "low": 498.5, "close": 500.2, "volume": 3000,
            },
            {
                "time": "2026-07-14T13:30:00+00:00",
                "open": 501.0, "high": 502.0, "low": 500.8, "close": 501.5, "volume": 3500,
            },
        ],
    }
    sig.update(overrides)
    return sig


def _assert_no_execution_mutation(result: dict[str, Any]) -> None:
    assert result.get("affected_eligibility") in (None, False)
    assert result.get("submit") is not True
    assert result.get("cancel") is not True
    assert result.get("broker_submit") is not True
    assert result.get("broker_cancel") is not True
    assert result.get("mutate_eligibility") is not True
    assert result.get("mutate_orders") is not True
    assert result.get("mutate_positions") is not True


def _install_recovery_cursor(monkeypatch, breach_rows: list[dict], *, pretrigger=None, preopen=None):
    class Cursor:
        query = 0

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def execute(self, _sql, _params):
            self.query += 1

        def fetchall(self):
            if self.query == 1:
                return list(pretrigger or [])
            if self.query == 2:
                return list(preopen or [])
            return list(breach_rows)

    cursor = Cursor()
    monkeypatch.setitem(
        sys.modules,
        "ap.db",
        types.SimpleNamespace(conn=lambda: cursor, run_with_retry=lambda fn: fn()),
    )
    return cursor


# ---------------------------------------------------------------------------
# Concrete parity matrix — resolved values only
# ---------------------------------------------------------------------------

# Columns: scenario, mode, path, signal_mutator, kwargs_mutator, expected
# path ∈ {runtime, restart, materializer}
# expected keys: ok, inserted (runtime), error_code, breach_lineage, execution_mode,
#                client_id, execution_continues, diagnostic_only, recovery_shape


def _mut_first_breach(sig: dict) -> dict:
    sig["breach_count"] = 1
    sig.pop("breach_reset", None)
    return sig


def _mut_rebreach(sig: dict) -> dict:
    sig["breach_count"] = 2
    sig["breach_reset"] = True
    return sig


def _mut_generation_mismatch(sig: dict) -> dict:
    sig["materialization_generation"] = 4
    sig["recovery_submit_generation"] = 5
    return sig


def _mut_retry_contradiction(sig: dict) -> dict:
    sig["retry_attempt"] = 2
    sig["materialization_retry_attempt"] = 3
    return sig


def _mut_malformed_ts(sig: dict) -> dict:
    sig["trigger_crossed_at"] = "not-a-timestamp"
    return sig


def _mut_client_conflict(sig: dict) -> dict:
    sig["client_id"] = "other@example.com"
    return sig


def _mut_mode_conflict(sig: dict) -> dict:
    sig["execution_mode"] = "LIVE"
    return sig


PARITY_ROWS = [
    # --- LIVE / PAPER accept: first breach ---
    pytest.param(
        "LIVE_first_breach",
        "LIVE",
        "runtime",
        _mut_first_breach,
        {},
        {
            "ok": True,
            "inserted": True,
            "error_code": None,
            "breach_lineage": "INITIAL_BREACH",
            "execution_mode": "LIVE",
            "client_id": "client@example.com",
            "canonical_signal_id": "canon-parity-1",
            "local_order_id": "loid-parity-1",
            "signal_id": "sig-parity-1",
            "observe_only": True,
            "affected_eligibility": False,
        },
        id="live-first-breach-runtime",
    ),
    pytest.param(
        "PAPER_first_breach",
        "PAPER",
        "runtime",
        _mut_first_breach,
        {},
        {
            "ok": True,
            "inserted": True,
            "error_code": None,
            "breach_lineage": "INITIAL_BREACH",
            "execution_mode": "PAPER",
            "client_id": "client@example.com",
            "canonical_signal_id": "canon-parity-1",
            "local_order_id": "loid-parity-1",
            "signal_id": "sig-parity-1",
            "observe_only": True,
            "affected_eligibility": False,
        },
        id="paper-first-breach-runtime",
    ),
    # --- rebreach ---
    pytest.param(
        "PAPER_rebreach",
        "PAPER",
        "runtime",
        _mut_rebreach,
        {},
        {
            "ok": True,
            "inserted": True,
            "error_code": None,
            "breach_lineage": "REBREACH_AFTER_RESET",
            "execution_mode": "PAPER",
            "client_id": "client@example.com",
            "canonical_signal_id": "canon-parity-1",
            "local_order_id": "loid-parity-1",
            "signal_id": "sig-parity-1",
            "observe_only": True,
            "affected_eligibility": False,
        },
        id="paper-rebreach-runtime",
    ),
    # --- reject: generation mismatch ---
    pytest.param(
        "generation_mismatch",
        "PAPER",
        "runtime",
        _mut_generation_mismatch,
        {},
        {
            "ok": False,
            "inserted": False,
            "error_code": "BREACH_LIFECYCLE_GENERATION_CONFLICT",
            "execution_continues": True,
            "no_job": True,
        },
        id="generation-mismatch-runtime",
    ),
    # --- reject: retry contradiction ---
    pytest.param(
        "retry_contradiction",
        "PAPER",
        "runtime",
        _mut_retry_contradiction,
        {},
        {
            "ok": False,
            "inserted": False,
            "error_code": "BREACH_LIFECYCLE_ATTEMPT_CONFLICT",
            "execution_continues": True,
            "no_job": True,
        },
        id="retry-contradiction-runtime",
    ),
    # --- reject: malformed timestamp ---
    pytest.param(
        "malformed_timestamp",
        "PAPER",
        "runtime",
        _mut_malformed_ts,
        {},
        {
            "ok": False,
            "inserted": False,
            "error_code": "BREACH_EVIDENCE_INVALID_TRIGGER_CROSSED_AT",
            "execution_continues": True,
            "no_job": True,
        },
        id="malformed-timestamp-runtime",
    ),
    # --- reject: client conflict ---
    pytest.param(
        "client_conflict",
        "PAPER",
        "runtime",
        _mut_client_conflict,
        {},
        {
            "ok": False,
            "inserted": False,
            "error_code": "BREACH_IDENTITY_CONFLICT_CLIENT_ID",
            "execution_continues": True,
            "no_job": True,
        },
        id="client-conflict-runtime",
    ),
    # --- reject: execution-mode conflict ---
    pytest.param(
        "execution_mode_conflict",
        "PAPER",
        "runtime",
        _mut_mode_conflict,
        {},
        {
            "ok": False,
            "inserted": False,
            "error_code": "BREACH_IDENTITY_CONFLICT_EXECUTION_MODE",
            "execution_continues": True,
            "no_job": True,
        },
        id="execution-mode-conflict-runtime",
    ),
]


@pytest.mark.parametrize("scenario,mode,path,mutator,extra_kwargs,expected", PARITY_ROWS)
def test_runtime_parity_matrix(scenario, mode, path, mutator, extra_kwargs, expected):
    assert path == "runtime"
    signal = mutator(_base_signal(execution_mode=mode))
    result = enqueue_breach_context(
        signal,
        client_id="client@example.com",
        execution_mode=mode,
        canonical_signal_id="canon-parity-1",
        local_order_id="loid-parity-1",
        signal_id="sig-parity-1",
        **extra_kwargs,
    )
    _assert_no_execution_mutation(result)

    assert result.get("ok") is expected["ok"]
    if "inserted" in expected:
        assert result.get("inserted") is expected["inserted"]
    if expected.get("error_code"):
        assert result.get("error_code") == expected["error_code"]
        assert result.get("inserted") is False
    if expected.get("no_job"):
        assert _MEMORY_JOBS == {}

    if expected.get("ok"):
        job = next(iter(_MEMORY_JOBS.values()))
        frozen = job["payload"]["signal"]
        assert job["execution_mode"] == expected["execution_mode"]
        assert job["client_id"] == expected["client_id"]
        assert job["canonical_signal_id"] == expected["canonical_signal_id"]
        assert job["local_order_id"] == expected["local_order_id"]
        assert job["signal_id"] == expected["signal_id"]
        assert job["phase"] == "BREACH"
        assert frozen["breach_lineage"] == expected["breach_lineage"]
        assert job["payload"]["observe_only"] is True
        assert job["payload"]["affected_eligibility"] is False
        assert job["payload"]["execution_mode"] == expected["execution_mode"]
        # Lineage helper agrees with frozen payload.
        lineage = resolve_breach_lineage(signal)
        assert lineage["breach_lineage"] == expected["breach_lineage"]


def test_materializer_parity_matches_runtime_identity_for_live_and_paper():
    """Accept path: runtime enqueue identity == materializer payload identity."""
    for mode in ("LIVE", "PAPER"):
        _reset_memory_store_for_tests()
        signal = _mut_first_breach(_base_signal(execution_mode=mode))
        enq = enqueue_breach_context(
            signal,
            client_id="client@example.com",
            execution_mode=mode,
            canonical_signal_id="canon-parity-1",
            local_order_id="loid-parity-1",
            signal_id="sig-parity-1",
        )
        assert enq == {
            "ok": True,
            "inserted": True,
            "duplicate": False,
            "job_id": enq["job_id"],
            "context_revision": 1,
        } or (enq.get("ok") is True and enq.get("inserted") is True)

        job = next(iter(_MEMORY_JOBS.values()))
        frozen = job["payload"]["signal"]
        payload = build_intelligence_context_payload(
            frozen,
            phase="BREACH",
            client_id=job["client_id"],
            execution_mode=job["execution_mode"],
            canonical_signal_id=job["canonical_signal_id"],
            local_order_id=job["local_order_id"],
            broker=None,
        )
        # Concrete resolved parity values
        assert payload["client_id"] == "client@example.com"
        assert payload["execution_mode"] == mode
        assert payload["canonical_signal_id"] == "canon-parity-1"
        assert payload["local_order_id"] == "loid-parity-1"
        assert payload["signal_id"] == "sig-parity-1"
        assert payload["observe_only"] is True
        assert payload["affected_eligibility"] is False
        assert payload["underlying_observation"]["price"] == 501.25
        assert frozen["breach_lineage"] == "INITIAL_BREACH"
        readiness = payload.get("entry_timing_candidate_observe_only") or {}
        if readiness:
            assert readiness.get("observe_only") is True
            assert readiness.get("affected_eligibility") is False


def test_restart_recovery_rebuilds_first_breach_and_rebreach(monkeypatch):
    """Restart path shapes: first breach + rebreach insert BREACH jobs with resolved lineage."""
    cases = [
        ("INITIAL_BREACH", _mut_first_breach(_base_signal()), "PAPER"),
        ("REBREACH_AFTER_RESET", _mut_rebreach(_base_signal()), "LIVE"),
    ]
    for expected_lineage, signal, mode in cases:
        _reset_memory_store_for_tests()
        signal = dict(signal)
        signal["execution_mode"] = mode
        _install_recovery_cursor(
            monkeypatch,
            [{
                "signal_id": signal["signal_id"],
                "local_order_id": signal["local_order_id"],
                "client_id": "client@example.com",
                "execution_mode": mode,
                "meta": {
                    "trigger_crossed_at": signal["trigger_crossed_at"],
                    "trigger_confirmed_at": signal["trigger_confirmed_at"],
                    "breach_count": signal.get("breach_count"),
                    **({"breach_reset": True} if signal.get("breach_reset") else {}),
                },
                "payload": signal,
            }],
        )
        result = recover_missing_intelligence_jobs(
            client_id="client@example.com", execution_mode=mode
        )
        assert result["ok"] is True
        assert result["breach"] == 1
        assert result["breach_errors"] == 0
        assert result["pretrigger"] == 0
        assert result["preopen"] == 0
        job = next(iter(_MEMORY_JOBS.values()))
        assert job["phase"] == "BREACH"
        assert job["execution_mode"] == mode
        assert job["payload"]["signal"]["breach_lineage"] == expected_lineage
        assert job["payload"]["observe_only"] is True
        assert job["payload"]["affected_eligibility"] is False


@pytest.mark.parametrize(
    "mutator,error_code",
    [
        (_mut_generation_mismatch, "BREACH_LIFECYCLE_GENERATION_CONFLICT"),
        (_mut_retry_contradiction, "BREACH_LIFECYCLE_ATTEMPT_CONFLICT"),
        (_mut_malformed_ts, "BREACH_EVIDENCE_INVALID_TRIGGER_CROSSED_AT"),
        (_mut_client_conflict, "BREACH_IDENTITY_CONFLICT_CLIENT_ID"),
        (_mut_mode_conflict, "BREACH_IDENTITY_CONFLICT_EXECUTION_MODE"),
    ],
)
def test_restart_reject_scenarios_count_breach_errors(monkeypatch, mutator, error_code):
    signal = mutator(_base_signal())
    # For mode conflict, payload says LIVE while recovery runs PAPER.
    row_mode = "PAPER"
    _install_recovery_cursor(
        monkeypatch,
        [{
            "signal_id": "sig-parity-1",
            "local_order_id": "loid-parity-1",
            "client_id": "client@example.com",
            "execution_mode": row_mode,
            "meta": {
                "trigger_crossed_at": signal.get("trigger_crossed_at"),
                "trigger_confirmed_at": signal.get("trigger_confirmed_at"),
            },
            "payload": signal,
        }],
    )
    result = recover_missing_intelligence_jobs(
        client_id="client@example.com", execution_mode="PAPER"
    )
    assert result["ok"] is True
    assert result["breach"] == 0
    assert result["breach_errors"] == 1
    assert _MEMORY_JOBS == {}
    # Prove the same signal would reject with the concrete error_code on runtime path.
    runtime = enqueue_breach_context(
        deepcopy(signal),
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-parity-1",
        local_order_id="loid-parity-1",
        signal_id="sig-parity-1",
    )
    assert runtime == {"ok": False, "inserted": False, "error_code": error_code}
    _assert_no_execution_mutation(runtime)


def test_missing_intelligence_store_is_diagnostic_only_execution_continues(monkeypatch):
    """Missing store → error_code reported; no job; execution-continues semantics."""
    monkeypatch.setattr(
        "ap.intelligence_context_materializer.enqueue_intelligence_job",
        lambda **_kwargs: {
            "ok": False,
            "inserted": False,
            "error_code": "INTELLIGENCE_STORE_UNAVAILABLE",
        },
    )
    result = enqueue_breach_context(
        _base_signal(),
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-parity-1",
        local_order_id="loid-parity-1",
        signal_id="sig-parity-1",
    )
    assert result["ok"] is False
    assert result["inserted"] is False
    assert result["error_code"] == "INTELLIGENCE_STORE_UNAVAILABLE"
    assert _MEMORY_JOBS == {}
    _assert_no_execution_mutation(result)
    # Diagnostic-only / execution continues — no mutation authority flags.
    assert result.get("affected_eligibility") in (None, False)
    assert result.get("terminalize") is not True
    assert result.get("block_submit") is not True


def test_ownership_loss_is_diagnostic_only_execution_continues():
    """Worker ownership loss → JOB_CLAIM_OWNERSHIP_LOST; execution continues."""
    enq = enqueue_breach_context(
        _base_signal(),
        client_id="client@example.com",
        execution_mode="PAPER",
        canonical_signal_id="canon-parity-1",
        local_order_id="loid-parity-1",
        signal_id="sig-parity-1",
    )
    assert enq["inserted"] is True
    claimed = claim_due_intelligence_jobs(
        claim_owner="owner-a",
        client_id="client@example.com",
        execution_mode="PAPER",
        limit=1,
    )
    assert claimed["ok"] is True
    job = claimed["jobs"][0]
    # Different claim owner → ownership loss diagnostic.
    lost = complete_job_with_snapshot(
        job,
        claim_owner="owner-b-not-holder",
        snapshot_kwargs={
            "client_id": "client@example.com",
            "execution_mode": "PAPER",
            "canonical_signal_id": "canon-parity-1",
            "local_order_id": "loid-parity-1",
            "phase": "BREACH",
            "input_hash": "x",
            "status": "COMPLETE",
            "payload": {"observe_only": True, "affected_eligibility": False},
        },
    )
    assert lost["ok"] is False
    assert lost["completed"] is False
    assert lost["error_code"] == "JOB_CLAIM_OWNERSHIP_LOST"
    _assert_no_execution_mutation(lost)
    # Job remains RUNNING under original owner — no execution mutation implied.
    stored = next(iter(_MEMORY_JOBS.values()))
    assert stored["status"] == "RUNNING"
    assert stored["claim_owner"] == "owner-a"


def test_public_lifecycle_resolved_values_for_fenced_live_and_paper():
    """Concrete lifecycle resolution values shared by runtime freeze."""
    for mode in ("LIVE", "PAPER"):
        signal = _base_signal(
            execution_mode=mode,
            materialization_generation=7,
            recovery_submit_generation=7,
            retry_attempt=2,
            recovery_submit_fenced=True,
            ownership_token="tok-7",
        )
        life = resolve_public_breach_lifecycle(signal)
        assert life["generation"] == 7
        assert life["attempt"] == 2
        assert life["recovered"] is True
        assert life["execution_mode"] == mode
        assert life["client_id"] == "client@example.com"
        assert life["owner"] == "tok-7"


def test_restart_db_unavailable_shape_is_diagnostic(monkeypatch):
    """recover_missing_intelligence_jobs DB failure → ok=False counts zero; no jobs."""
    monkeypatch.setitem(
        sys.modules,
        "ap.db",
        types.SimpleNamespace(
            conn=lambda: (_ for _ in ()).throw(RuntimeError("pg_down")),
            run_with_retry=lambda fn: fn(),
        ),
    )
    result = recover_missing_intelligence_jobs(
        client_id="client@example.com", execution_mode="PAPER"
    )
    assert result == {
        "ok": False,
        "pretrigger": 0,
        "preopen": 0,
        "breach": 0,
        "breach_errors": 0,
        "error": "pg_down",
    }
    assert _MEMORY_JOBS == {}
