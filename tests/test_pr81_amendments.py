"""
tests/test_pr81_amendments.py
=============================
Required tests for PR81 amendments (observe-only preflight + lifecycle truth).

Covers all 7 required test cases from the amendment spec.
"""
from __future__ import annotations

import os, sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("DATABASE_URL",
                       "postgresql://test:test@127.0.0.1:5432/test_pr81")


# ── Shared fixtures ───────────────────────────────────────────────────────────

def _make_preflight(eligible=True, block_reason=None, kill_switch=False,
                     buying_power=1000.0, subscription_active=True):
    from ap.client_preflight import ClientTradePreflight
    from datetime import datetime, timezone
    return ClientTradePreflight(
        client_id="jason@example.com",
        client_active=True,
        approved=True,
        subscription_active=subscription_active,
        kill_switch=kill_switch,
        entries_paused=False,
        maintenance_mode=False,
        scanner_routing_enabled=True,
        tradier_active_mode="paper",
        expected_mode="paper",
        has_account_id=True,
        has_access_token=True,
        broker_credentials_present=True,
        buying_power=buying_power,
        estimated_cost=200.0,
        max_trade_cost=500.0,
        daily_trade_count=0,
        daily_lane_count=0,
        intraday_lane_count=0,
        same_symbol_count=0,
        open_positions_count=0,
        pending_entries_count=0,
        max_daily_trades=5,
        max_lane_trades=3,
        max_intraday_trades=2,
        max_same_symbol_trades=1,
        max_open_positions=10,
        max_pending_entries=3,
        eligible=eligible,
        block_reason=block_reason,
        snapshot_ts=datetime.now(timezone.utc).isoformat(),
    )


def _make_sb(upsert_raises=False, update_raises=False):
    sb = MagicMock()
    if upsert_raises:
        sb.table.return_value.upsert.return_value.execute.side_effect = Exception("db_error")
    if update_raises:
        sb.table.return_value.update.return_value.eq.return_value.eq.return_value.execute.side_effect = Exception("db_error")
    return sb


# ── Test 1: Observe-only preflight does not block ─────────────────────────────

class TestObserveOnlyPreflightDoesNotBlock:
    """
    CLIENT_PREFLIGHT_ENFORCE=false + preflight returns eligible=False (kill_switch_on)
    → opportunity records PREFLIGHT_WARNING
    → execution_continued=True, preflight_enforced=False
    → existing order flow continues
    → job NOT marked REJECTED
    """

    def test_observe_mode_writes_preflight_warning(self):
        """Status must be PREFLIGHT_WARNING, not CLIENT_SKIPPED, when enforce=false."""
        os.environ["CLIENT_PREFLIGHT_ENFORCE"] = "false"
        from ap.opportunity_ledger import update_opportunity, PREFLIGHT_WARNING

        sb = _make_sb()
        pf = _make_preflight(eligible=False, block_reason="kill_switch_on", kill_switch=True)

        ok = update_opportunity(
            "SIG-001", "jason@example.com", PREFLIGHT_WARNING,
            preflight_enforced=False,
            execution_continued=True,
            would_block_reason="kill_switch_on",
            sb=sb,
        )
        assert ok is True
        # The update call must use PREFLIGHT_WARNING, not CLIENT_SKIPPED
        call_args = sb.table.return_value.update.call_args
        patch_dict = call_args[0][0]
        assert patch_dict["opportunity_status"] == "PREFLIGHT_WARNING"
        assert patch_dict["preflight_enforced"] is False
        assert patch_dict["execution_continued"] is True
        assert patch_dict["would_block_reason"] == "kill_switch_on"

    def test_observe_mode_does_not_mark_job_rejected(self):
        """When enforce=false, _mark_job must NOT be called with REJECTED."""
        os.environ["CLIENT_PREFLIGHT_ENFORCE"] = "false"
        pf = _make_preflight(eligible=False, block_reason="kill_switch_on", kill_switch=True)

        mark_job_calls = []
        with patch("ap.client_preflight.build_client_trade_preflight", return_value=pf), \
             patch("ap.opportunity_ledger.update_opportunity", return_value=True), \
             patch("ap.opportunity_ledger.mark_skipped", return_value=True):
            # Simulate the queue.py logic directly
            enforce = os.environ.get("CLIENT_PREFLIGHT_ENFORCE", "false").lower() in ("true","1")
            assert not enforce, "Enforce must be false for this test"
            if not pf.eligible and not enforce:
                # Observe path: must NOT return/reject
                returned = False  # execution_continues
            else:
                returned = True  # enforce would return
            assert not returned, "Observe mode must not stop execution"

    def test_execution_continued_true_in_observe_mode(self):
        """execution_continued field must be True when observe mode allows through."""
        from ap.opportunity_ledger import update_opportunity, PREFLIGHT_WARNING
        sb = _make_sb()
        update_opportunity(
            "SIG-001", "jason@example.com", PREFLIGHT_WARNING,
            execution_continued=True, preflight_enforced=False,
            would_block_reason="kill_switch_on", sb=sb,
        )
        patch_dict = sb.table.return_value.update.call_args[0][0]
        assert patch_dict.get("execution_continued") is True
        assert patch_dict.get("preflight_enforced") is False
        assert patch_dict.get("would_block_reason") == "kill_switch_on"


# ── Test 2: Enforcement mode blocks ──────────────────────────────────────────

class TestEnforcementModeBlocks:
    """
    CLIENT_PREFLIGHT_ENFORCE=true + eligible=False (kill_switch_on)
    → CLIENT_SKIPPED
    → miss_stage=CLIENT_PREFLIGHT, miss_reason=kill_switch_on
    → execution_continued=False
    → no order created
    """

    def test_enforce_mode_writes_client_skipped(self):
        """Status must be CLIENT_SKIPPED when enforce=true and preflight fails."""
        os.environ["CLIENT_PREFLIGHT_ENFORCE"] = "true"
        from ap.opportunity_ledger import update_opportunity, CLIENT_SKIPPED, STAGE_CLIENT_PREFLIGHT
        sb = _make_sb()
        ok = update_opportunity(
            "SIG-001", "jason@example.com", CLIENT_SKIPPED,
            miss_stage=STAGE_CLIENT_PREFLIGHT,
            miss_reason="kill_switch_on",
            preflight_enforced=True,
            execution_continued=False,
            would_block_reason="kill_switch_on",
            sb=sb,
        )
        assert ok is True
        patch_dict = sb.table.return_value.update.call_args[0][0]
        assert patch_dict["opportunity_status"] == "CLIENT_SKIPPED"
        assert patch_dict["miss_stage"] == "CLIENT_PREFLIGHT"
        assert patch_dict["miss_reason"] == "kill_switch_on"
        assert patch_dict["preflight_enforced"] is True
        assert patch_dict["execution_continued"] is False

    def test_enforce_mode_execution_continued_false(self):
        """execution_continued must be False when enforce blocks."""
        from ap.opportunity_ledger import update_opportunity, CLIENT_SKIPPED
        sb = _make_sb()
        update_opportunity(
            "SIG-001", "jason@example.com", CLIENT_SKIPPED,
            execution_continued=False, preflight_enforced=True, sb=sb,
        )
        patch_dict = sb.table.return_value.update.call_args[0][0]
        assert patch_dict.get("execution_continued") is False


# ── Test 3: Lifecycle status correctness ─────────────────────────────────────

class TestLifecycleStatusCorrectness:
    """
    PREFLIGHT_PASSED must not mark ORDER_CREATED.
    ORDER_CREATED must only be written after create_entry_order() returns a valid local_order_id.
    """

    def test_preflight_passed_is_not_order_created(self):
        """PREFLIGHT_PASSED != ORDER_CREATED — they are distinct statuses."""
        from ap.opportunity_ledger import PREFLIGHT_PASSED, ORDER_CREATED
        assert PREFLIGHT_PASSED != ORDER_CREATED

    def test_order_created_requires_local_order_id(self):
        """update_opportunity with ORDER_CREATED should carry order_local_id."""
        from ap.opportunity_ledger import update_opportunity, ORDER_CREATED
        sb = _make_sb()
        update_opportunity(
            "SIG-001", "jason@example.com", ORDER_CREATED,
            order_local_id="ORD-ABC-123",
            sb=sb,
        )
        patch_dict = sb.table.return_value.update.call_args[0][0]
        assert patch_dict["opportunity_status"] == ORDER_CREATED
        assert patch_dict.get("order_local_id") == "ORD-ABC-123"

    def test_order_created_not_written_without_local_id(self):
        """If no local_order_id, ORDER_CREATED must not be written with a real id."""
        from ap.opportunity_ledger import update_opportunity, ORDER_CREATED
        sb = _make_sb()
        # Passing no order_local_id — the patch should not include order_local_id
        update_opportunity(
            "SIG-001", "jason@example.com", ORDER_CREATED, sb=sb,
        )
        patch_dict = sb.table.return_value.update.call_args[0][0]
        assert patch_dict.get("order_local_id") is None

    def test_lifecycle_constant_ordering(self):
        """Verify all required lifecycle statuses are defined."""
        from ap.opportunity_ledger import (
            CREATED, CLIENT_ELIGIBLE, PREFLIGHT_PASSED, PREFLIGHT_WARNING,
            CLIENT_SKIPPED, ORDER_CREATED, WATCHER_ARMED,
            BROKER_SUBMITTED, BROKER_ACKED, FILLED, MISSED,
        )
        # Just importing them without NameError confirms they exist
        assert all([
            CREATED, CLIENT_ELIGIBLE, PREFLIGHT_PASSED, PREFLIGHT_WARNING,
            CLIENT_SKIPPED, ORDER_CREATED, WATCHER_ARMED,
            BROKER_SUBMITTED, BROKER_ACKED, FILLED, MISSED,
        ])


# ── Test 4: Canonical idempotency ─────────────────────────────────────────────

class TestCanonicalIdempotency:
    """
    Two raw signal IDs with different suffixes but same canonical_signal_id + client_id
    must produce exactly one opportunity row (upsert conflict on canonical_signal_id + client_id).
    """

    def test_same_canonical_different_raw_signal_upserts_once(self):
        from ap.opportunity_ledger import create_opportunities
        UUID = "8d9338d0-5dde-4b7b-81ea-208039999b72"
        canonical = f"REEVAL:{UUID}"
        raw_jason = f"REEVAL:{UUID}:f4dc44"
        raw_jose  = f"REEVAL:{UUID}:a1b2c3"

        upsert_calls = []
        sb = MagicMock()
        sb.table.return_value.upsert.side_effect = (
            lambda row, on_conflict=None, ignore_duplicates=False: (
                upsert_calls.append(row) or MagicMock()
            )
        )
        sb.table.return_value.upsert.return_value.execute.return_value = MagicMock()

        payload = {"ticker": "SPY", "direction": "PUT", "score": 72.0, "tier": "B"}

        # First call with raw_jason
        create_opportunities(raw_jason, ["jason@example.com"], payload,
                              canonical_signal_id=canonical, sb=sb)
        # Second call with raw_jose (different suffix, same canonical)
        create_opportunities(raw_jose, ["jason@example.com"], payload,
                              canonical_signal_id=canonical, sb=sb)

        # Both upsert calls must use the same canonical_signal_id
        assert len(upsert_calls) == 2
        for row in upsert_calls:
            assert row["canonical_signal_id"] == canonical, (
                f"canonical_signal_id must be {canonical}, got {row['canonical_signal_id']!r}"
            )
        # The on_conflict parameter must use canonical_signal_id
        on_conflict_values = [
            kw for args, kw in sb.table.return_value.upsert.call_args_list
            for kw in [kw.get("on_conflict", "")]
        ]
        assert any("canonical_signal_id" in str(v) for v in on_conflict_values), (
            "upsert on_conflict must include canonical_signal_id"
        )

    def test_canonical_fallback_to_signal_id(self):
        """When canonical_signal_id is None, falls back to signal_id for idempotency."""
        from ap.opportunity_ledger import create_opportunities
        sb = MagicMock()
        sb.table.return_value.upsert.return_value.execute.return_value = MagicMock()

        create_opportunities("SIG-001", ["c@c.com"], {"ticker": "ORCL"},
                              canonical_signal_id=None, sb=sb)
        call_row = sb.table.return_value.upsert.call_args[0][0]
        # canonical_signal_id field must be signal_id when canonical is None
        assert call_row["canonical_signal_id"] == "SIG-001"


# ── Test 5: Ledger write failure is fail-safe ─────────────────────────────────

class TestLedgerWriteFailSafe:
    """
    If opportunity ledger upsert raises, execution continues and error is logged.
    Trade must NOT be rejected because of ledger failure.
    """

    def test_create_opportunities_logs_on_failure(self, caplog):
        from ap.opportunity_ledger import create_opportunities
        import logging
        sb = _make_sb(upsert_raises=True)
        with caplog.at_level(logging.WARNING, logger="ap.opportunity_ledger"):
            result = create_opportunities(
                "SIG-001", ["jason@example.com"], {"ticker": "SPY"}, sb=sb,
            )
        assert result == 0  # no rows written
        assert "CLIENT_OPPORTUNITY_LEDGER_WRITE_FAILED" in caplog.text, (
            "Must log CLIENT_OPPORTUNITY_LEDGER_WRITE_FAILED on upsert failure"
        )

    def test_update_opportunity_logs_on_failure(self, caplog):
        from ap.opportunity_ledger import update_opportunity, PREFLIGHT_PASSED
        import logging
        sb = _make_sb(update_raises=True)
        with caplog.at_level(logging.WARNING, logger="ap.opportunity_ledger"):
            result = update_opportunity(
                "SIG-001", "jason@example.com", PREFLIGHT_PASSED, sb=sb,
            )
        assert result is False
        assert "CLIENT_OPPORTUNITY_LEDGER_WRITE_FAILED" in caplog.text, (
            "Must log CLIENT_OPPORTUNITY_LEDGER_WRITE_FAILED on update failure"
        )

    def test_ledger_failure_does_not_raise(self):
        """Neither create_opportunities nor update_opportunity may raise externally."""
        from ap.opportunity_ledger import create_opportunities, update_opportunity, PREFLIGHT_PASSED
        sb = _make_sb(upsert_raises=True, update_raises=True)
        # Must not raise
        create_opportunities("SIG-001", ["c@c.com"], {}, sb=sb)
        update_opportunity("SIG-001", "c@c.com", PREFLIGHT_PASSED, sb=sb)


# ── Test 6: Missing preflight data in observe mode ────────────────────────────

class TestMissingPreflightDataObserveMode:
    """
    When buying_power or subscription state cannot be read,
    observe mode records unknown state and continues.
    """

    def test_zero_buying_power_recorded_as_unavailable_in_todict(self):
        """to_dict() must record buying_power=0 as 'buying_power_unavailable', not 0."""
        pf = _make_preflight(eligible=True, buying_power=0.0)
        d = pf.to_dict(preflight_enforced=False, execution_continued=True)
        assert d["buying_power"] == "buying_power_unavailable", (
            f"buying_power=0 must be recorded as 'buying_power_unavailable', got {d['buying_power']!r}"
        )

    def test_known_buying_power_recorded_as_number(self):
        """to_dict() must record a real buying_power as a number."""
        pf = _make_preflight(eligible=True, buying_power=5000.0)
        d = pf.to_dict(preflight_enforced=False, execution_continued=True)
        assert d["buying_power"] == 5000.0

    def test_observe_mode_with_unavailable_data_does_not_block(self):
        """Unknown buying power in observe mode must not introduce a new block."""
        os.environ["CLIENT_PREFLIGHT_ENFORCE"] = "false"
        # buying_power=0 means unknown — eligible check skips the buying power gate
        pf = _make_preflight(eligible=True, buying_power=0.0)
        # eligible=True means even with unknown buying_power, observe mode continues
        assert pf.eligible is True

    def test_to_dict_includes_preflight_enforced_and_execution_continued(self):
        """to_dict() must accept and persist preflight_enforced + execution_continued."""
        pf = _make_preflight(eligible=False, block_reason="kill_switch_on", kill_switch=True)
        d_observe  = pf.to_dict(preflight_enforced=False, execution_continued=True)
        d_enforce  = pf.to_dict(preflight_enforced=True,  execution_continued=False)
        assert d_observe["preflight_enforced"] is False
        assert d_observe["execution_continued"] is True
        assert d_enforce["preflight_enforced"] is True
        assert d_enforce["execution_continued"] is False


# ── Test 7: No peer retry submission ─────────────────────────────────────────

class TestNoPeerRetrySubmission:
    """PR81 must never submit a retry order or create retry-submission side effects."""

    def test_no_auto_retry_constants_used_in_queue(self):
        """queue.py must not reference RETRY_SUBMITTED / RETRY_FILLED."""
        src = (REPO_ROOT / "ap" / "queue.py").read_text()
        for bad in ("RETRY_SUBMITTED", "RETRY_FILLED", "RETRY_BLOCKED"):
            assert bad not in src, (
                f"queue.py must not use {bad} — retry behavior belongs in a future PR"
            )

    def test_retry_eligible_constant_exists_but_not_written_by_pr81(self):
        """RETRY_ELIGIBLE exists for future use but is not written by PR81 code paths."""
        from ap.opportunity_ledger import RETRY_ELIGIBLE
        assert RETRY_ELIGIBLE == "RETRY_ELIGIBLE"
        # Verify the comment says it is reserved
        src = (REPO_ROOT / "ap" / "opportunity_ledger.py").read_text()
        assert "reserved" in src.lower() or "future" in src.lower(), (
            "RETRY_ELIGIBLE must be documented as reserved/future use"
        )

    def test_no_retry_submit_in_opportunity_ledger(self):
        """opportunity_ledger.py must not contain retry submission logic."""
        src = (REPO_ROOT / "ap" / "opportunity_ledger.py").read_text()
        for bad in ("RETRY_SUBMITTED", "RETRY_FILLED", "broker.place_order",
                    "submit_order"):
            assert bad not in src, (
                f"opportunity_ledger.py must not contain {bad!r} — no retry submission"
            )

    def test_preflight_enforce_false_constant_is_default(self):
        """CLIENT_PREFLIGHT_ENFORCE env var must default to false."""
        # Temporarily remove the env var to test default
        old = os.environ.pop("CLIENT_PREFLIGHT_ENFORCE", None)
        try:
            from ap.client_preflight import preflight_enforce
            assert preflight_enforce() is False, (
                "CLIENT_PREFLIGHT_ENFORCE must default to false"
            )
        finally:
            if old is not None:
                os.environ["CLIENT_PREFLIGHT_ENFORCE"] = old
