"""P0 tests for the read-only PR #425 production release preflight."""
from __future__ import annotations

import importlib
import os
import sys
from types import SimpleNamespace

import pytest


def _module(monkeypatch):
    # Import ap.db only after a scoped URL is present.  No connection is made
    # by these tests; all database reads are replaced below.
    monkeypatch.setenv("DATABASE_URL", "postgresql://test.invalid/db")
    sys.modules.pop("ap.broker_owned_exit_recovery_preflight", None)
    return importlib.import_module("ap.broker_owned_exit_recovery_preflight")


def _claim(**overrides):
    row = {
        "generation_key": "client|position|4|1",
        "client_id": "client@example.com",
        "position_id": "position-1",
        "requested_qty": 1,
        "claim_state": "BROKER_OWNED",
        "local_order_id": "exit-local-1",
        "claim_broker_order_id": "broker-1",
        "order_local_order_id": "exit-local-1",
        "order_client_id": "client@example.com",
        "order_position_id": "position-1",
        "order_kind": "EXIT",
        "order_status": "EXIT_SUBMITTED",
        "order_execution_mode": "live",
        "order_qty": 1,
        "order_broker_order_id": "broker-1",
        "order_meta": {},
    }
    row.update(overrides)
    return row


def test_schema_contract_requires_requested_qty(monkeypatch):
    module = _module(monkeypatch)
    schema_attestation = importlib.import_module("ap.schema_attestation")
    assert "requested_qty" in schema_attestation.REQUIRED_SCHEMA[
        "exit_decision_generation_claims"
    ]
    assert module._MIGRATIONS == (
        "20260717_exit_decision_generation_claims.sql",
        "20260809_exit_decision_generation_requested_qty.sql",
    )


def test_classify_claim_rejects_identity_and_requested_qty_gaps(monkeypatch):
    module = _module(monkeypatch)
    result = module._classify_claim(
        _claim(
            requested_qty=None,
            order_execution_mode="PAPER",
            order_qty=4,
            claim_broker_order_id="",
            order_broker_order_id="",
            order_status="EXIT_REQUESTED",
            order_meta={"submit_intent_at": "2026-08-09T12:00:00+00:00"},
        ),
        "live",
    )
    assert result["safe"] is False
    assert {
        "REQUESTED_QTY_MISSING_OR_INVALID",
        "DURABLE_MODE_INVALID",
        "BROKER_OWNERSHIP_UNRESOLVED",
        "EXIT_REQUESTED_SUBMIT_EVIDENCE_UNRESOLVED",
    }.issubset(result["findings"])

    qty_result = module._classify_claim(
        _claim(requested_qty=1, order_qty=4),
        "live",
    )
    assert "REQUESTED_QTY_ORDER_MISMATCH" in qty_result["findings"]

    for state, finding in (
        ("CLAIMED", "CLAIM_STILL_CLAIMED"),
        ("AMBIGUOUS", "CLAIM_RECONCILIATION_UNRESOLVED"),
        ("STALE_CLAIM_RECONCILING", "CLAIM_RECONCILIATION_UNRESOLVED"),
    ):
        state_result = module._classify_claim(_claim(claim_state=state), "live")
        assert state_result["safe"] is False
        assert finding in state_result["findings"]


def test_run_preflight_requires_exact_runtime_mode_and_is_read_only(monkeypatch):
    module = _module(monkeypatch)
    monkeypatch.setenv("BOT_MODE", " LIVE ")
    monkeypatch.setattr(module, "attest_schema", lambda **kwargs: {
        "ok": True,
        "skipped": False,
    })
    snapshot_called = {"value": False}

    def _snapshot(_mode):
        snapshot_called["value"] = True
        raise AssertionError("database snapshot must not run without runtime mode")

    monkeypatch.setattr(module, "_read_snapshot", _snapshot)
    result = module.run_preflight()
    assert result["safe"] is False
    assert "runtime_mode_unproven" in result["findings"]
    assert snapshot_called["value"] is False
    assert result["broker_calls"] == 0
    assert result["writes"] == 0


def test_run_preflight_accepts_exact_clean_snapshot(monkeypatch):
    module = _module(monkeypatch)
    monkeypatch.setenv("BOT_MODE", "LIVE")
    monkeypatch.setattr(module, "attest_schema", lambda **kwargs: {
        "ok": True,
        "skipped": False,
    })
    checksums = {
        filename: f"checksum-{index}"
        for index, filename in enumerate(module._MIGRATIONS)
    }
    monkeypatch.setattr(module, "_migration_checksums", lambda: checksums)
    monkeypatch.setattr(module, "_read_snapshot", lambda mode: {
        "migration_checksums": checksums,
        "ledger_rows": [
            {"filename": filename, "checksum": checksums[filename], "applied_at": "now", "baselined": False}
            for filename in module._MIGRATIONS
        ],
        "claim_rows": [_claim()],
        "duplicate_rows": [],
    })

    result = module.run_preflight()
    assert result["safe"] is True
    assert result["findings"] == []
    assert result["unsafe_claim_count"] == 0
    assert result["duplicate_active_exit_count"] == 0
    assert result["broker_calls"] == 0
    assert result["writes"] == 0


@pytest.mark.parametrize("raw", [None, "", "PAPER ", "live", "UNKNOWN"])
def test_runtime_mode_is_not_normalized_into_authority(monkeypatch, raw):
    module = _module(monkeypatch)
    if raw is None:
        monkeypatch.delenv("BOT_MODE", raising=False)
        monkeypatch.delenv("MODE", raising=False)
    else:
        monkeypatch.setenv("BOT_MODE", raw)
    mode, error = module._runtime_mode()
    assert mode == ""
    assert error in {"runtime_mode_missing", "runtime_mode_unproven"}
