"""P0 regressions for scheduled-job startup and broker-intent reconciler wiring."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/intelligence_test?sslmode=disable",
)

import ap.db as db
import ap.pending_trigger_restart_recovery as ptr_module
from ap_reconciler import APBrokerReconciler, _empty_summary


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_scheduled_morning_workflows_install_python_dotenv():
    install_step = (
        "python -m pip install --disable-pip-version-check "
        "--no-input python-dotenv"
    )
    for workflow in (
        ".github/workflows/paper-morning-jobs.yml",
        ".github/workflows/overnight-reeval.yml",
    ):
        source = (REPO_ROOT / workflow).read_text(encoding="utf-8")
        assert install_step in source, workflow


def test_reconciler_forwards_execution_core_to_broker_intent_recovery(monkeypatch):
    execution_core = object()
    captured = {}

    class FakePendingTriggerRestartRecovery:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def recover_one_row(self, _row):
            return ptr_module._RowOutcome.UNRESOLVED

    row = {
        "local_order_id": "local-1",
        "client_id": "client@example.com",
        "execution_mode": "paper",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": "2026-08-13T16:00:00Z",
        "contract": "SPY260821C00650000",
        "meta": {"submit_intent_at": "2026-08-13T16:00:00Z"},
    }

    monkeypatch.setattr(
        ptr_module,
        "PendingTriggerRestartRecovery",
        FakePendingTriggerRestartRecovery,
    )
    monkeypatch.setattr(
        db,
        "get_open_orders_with_invalid_execution_mode",
        lambda **_kwargs: [],
    )
    monkeypatch.setattr(
        db,
        "get_open_orders_for_reconcile",
        lambda **_kwargs: [row],
    )
    monkeypatch.setattr(db, "run_with_retry", lambda fn: fn())

    with patch.object(APBrokerReconciler, "_register_health", return_value=None):
        reconciler = APBrokerReconciler(
            broker=object(),
            client_id="client@example.com",
            osm=object(),
            pm=object(),
            execution_mode="paper",
            execution_core=execution_core,
        )

    reconciler._check_ghost_fills = lambda _summary: None
    reconciler._reconcile_orders(_empty_summary("client@example.com"))

    assert captured["execution_core"] is execution_core


def test_client_runner_wires_execution_core_into_reconciler_constructor():
    source = (REPO_ROOT / "client_runner.py").read_text(encoding="utf-8")
    start = source.index("    def _start_reconciler(")
    end = source.index("    def _start_fill_monitor(", start)
    assert "execution_core=self.core" in source[start:end]
