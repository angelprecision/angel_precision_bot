"""Closure tests for the final PR #401 deployment blockers."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ap.selector_cursor_persistence_guard import (
    install_selector_cursor_persistence_guard,
)
from ap.selector_recovery_deploy_preflight import _classify_row
from ap.selector_retry_policy import SelectorRecoveryCursorPersistFailed
from ap_canonical_signal import build_canonical_signal_id

install_selector_cursor_persistence_guard()

from ap.order_state_machine import APOrderStateMachine
import ap.selector_cursor_persistence_guard as cursor_guard


def _persist(osm: APOrderStateMachine, **overrides):
    kwargs = {
        "owner": "materializer:test-owner",
        "generation": 2,
        "signal_id": "sig-pr401",
        "execution_mode": "live",
        "cursor": {"version": 1, "attempted_symbols": ["SPY260807C00600000"]},
    }
    kwargs.update(overrides)
    return osm.persist_selector_recovery_cursor("order-pr401", **kwargs)


class TestActualOSMCursorPersistenceClassification:
    def test_guard_is_installed_on_actual_order_state_machine(self):
        assert getattr(
            APOrderStateMachine,
            "_AP_SELECTOR_CURSOR_PERSIST_GUARD_PATCHED",
            False,
        ) is True
        assert hasattr(
            APOrderStateMachine,
            "_AP_SELECTOR_CURSOR_PERSIST_GUARD_ORIGINAL",
        )

    def test_exact_owner_cas_miss_is_the_only_false_outcome(self, monkeypatch):
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda _fn: 0)
        assert _persist(APOrderStateMachine("client@example.com")) is False

    def test_database_failure_raises_persist_failed_not_ownership_loss(self, monkeypatch):
        def _raise(_fn):
            raise TimeoutError("database unavailable")

        monkeypatch.setattr(cursor_guard, "_run_db_write", _raise)
        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            _persist(APOrderStateMachine("client@example.com"))

    @pytest.mark.parametrize("rowcount", [None, -1, 2, True, "unknown"])
    def test_unconfirmed_or_malformed_rowcount_raises(self, monkeypatch, rowcount):
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda _fn: rowcount)
        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            _persist(APOrderStateMachine("client@example.com"))

    def test_confirmed_single_row_write_succeeds(self, monkeypatch):
        monkeypatch.setattr(cursor_guard, "_run_db_write", lambda _fn: 1)
        assert _persist(APOrderStateMachine("client@example.com")) is True

    @pytest.mark.parametrize(
        "overrides",
        [
            {"owner": ""},
            {"owner": " materializer:test-owner "},
            {"signal_id": ""},
            {"signal_id": " sig-pr401 "},
            {"execution_mode": "observe"},
            {"generation": 0},
            {"generation": True},
            {"generation": 2.0},
            {"cursor": ["not", "a", "dict"]},
            {"cursor": {"bad": float("nan")}},
        ],
    )
    def test_non_cas_failures_raise_typed_persistence_error(self, overrides):
        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            _persist(APOrderStateMachine("client@example.com"), **overrides)


NOW = datetime(2026, 8, 6, 16, 50, tzinfo=timezone.utc)


def _row(**overrides):
    signal_id = overrides.pop("signal_id", "sig-pr401")
    row = {
        "local_order_id": "order-pr401",
        "client_id": "client@example.com",
        "execution_mode": "live",
        "signal_id": signal_id,
        "canonical_signal_id": build_canonical_signal_id(signal_id),
        "status": "PENDING_TRIGGER",
        "meta": {
            "lifecycle_state": "MATERIALIZING",
            "materialization_status": "RUNNING",
            "materialization_generation": 1,
        },
        "updated_ts": None,
    }
    row.update(overrides)
    return row


class TestDeployPreflightIdentityClosure:
    @pytest.mark.parametrize(
        ("field", "value", "finding"),
        [
            ("local_order_id", "", "MISSING_IDENTITY_FIELD:local_order_id"),
            ("client_id", "", "MISSING_IDENTITY_FIELD:client_id"),
            ("signal_id", "", "MISSING_IDENTITY_FIELD:signal_id"),
            (
                "canonical_signal_id",
                "",
                "MISSING_IDENTITY_FIELD:canonical_signal_id",
            ),
            (
                "execution_mode",
                "observe",
                "INVALID_IDENTITY_FIELD:execution_mode",
            ),
        ],
    )
    def test_incomplete_or_invalid_top_level_identity_is_unsafe(
        self, field, value, finding
    ):
        result = _classify_row(_row(**{field: value}), now=NOW)
        assert result["safe"] is False
        assert finding in result["findings"]

    def test_canonical_signal_mismatch_is_unsafe(self):
        result = _classify_row(
            _row(canonical_signal_id="different-canonical-id"),
            now=NOW,
        )
        assert "IDENTITY_MISMATCH:canonical_signal_id" in result["findings"]

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("local_order_id", "other-order"),
            ("client_id", "other@example.com"),
            ("signal_id", "other-signal"),
            ("execution_mode", "paper"),
            ("canonical_signal_id", "other-canonical"),
        ],
    )
    def test_present_meta_identity_mirror_must_match(self, field, value):
        row = _row()
        row["meta"] = {**row["meta"], field: value}
        result = _classify_row(row, now=NOW)
        assert f"IDENTITY_MIRROR_MISMATCH:{field}" in result["findings"]

    def test_valid_identity_remains_safe(self):
        result = _classify_row(_row(), now=NOW)
        assert result["safe"] is True
        assert result["findings"] == []


class TestDeployPreflightStaleRetryScheduleClosure:
    def test_expired_lease_with_stale_retry_schedule_is_unsafe(self):
        row = _row()
        row["meta"] = {
            **row["meta"],
            "materialization_lease_until": "2026-08-06T14:53:26+00:00",
            "next_retry_at": "2026-08-06T14:51:14+00:00",
            "materialization_next_retry_at": "2026-08-06T14:51:14+00:00",
        }
        result = _classify_row(row, now=NOW)
        assert "EXPIRED_LEASE_STALE_RETRY_SCHEDULE" in result["findings"]
        assert result["retry_schedule_stale"] is True

    def test_expired_lease_with_future_retry_schedule_is_not_stale(self):
        row = _row()
        row["meta"] = {
            **row["meta"],
            "materialization_lease_until": "2026-08-06T16:45:00+00:00",
            "next_retry_at": "2026-08-06T16:55:00+00:00",
            "materialization_next_retry_at": "2026-08-06T16:55:00+00:00",
        }
        result = _classify_row(row, now=NOW)
        assert "EXPIRED_LEASE_STALE_RETRY_SCHEDULE" not in result["findings"]
        assert result["retry_schedule_stale"] is False

    def test_conflicting_retry_schedule_mirrors_are_unsafe(self):
        row = _row()
        row["meta"] = {
            **row["meta"],
            "next_retry_at": "2026-08-06T16:55:00+00:00",
            "materialization_next_retry_at": "2026-08-06T16:56:00+00:00",
        }
        result = _classify_row(row, now=NOW)
        assert "RETRY_SCHEDULE_CONFLICT" in result["findings"]
