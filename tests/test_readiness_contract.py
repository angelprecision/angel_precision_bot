"""
tests/test_readiness_contract.py — Single readiness contract codified.

Covers every invariant of ap/readiness.py:
  1. ready=True iff all critical checks pass
  2. Missing organ data → not ready (fail-closed)
  3. allow_live=True implies live_operator_approved=True
  4. kill_switch on disables BOTH allow_paper and allow_live
  5. Organ must be HEALTHY AND fresh heartbeat to count as online

Run with:
    DATABASE_URL=postgresql://x python3 -m pytest tests/test_readiness_contract.py
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))

from ap.readiness import (
    compute_readiness,
    ReadinessReport,
    _organ_is_online,
    DEFAULT_ORGAN_FRESHNESS_SEC,
    REQUIRED_ORGANS_LIVE,
)


# Reference clock for deterministic tests
NOW = datetime(2026, 5, 21, 18, 0, tzinfo=timezone.utc)
HB_FRESH = NOW - timedelta(seconds=30)
HB_STALE = NOW - timedelta(seconds=600)


def _organs_all_healthy_fresh():
    return (
        {organ: "HEALTHY" for organ in REQUIRED_ORGANS_LIVE},
        {organ: HB_FRESH for organ in REQUIRED_ORGANS_LIVE},
    )


def _paper_member(**overrides):
    base = {
        "email":                       "test@x.com",
        "subscription_active":         True,
        "tradier_active_mode":         "paper",
        "tradier_account_id":          "VAPAPER",
        "tradier_access_token":        "paper_tok",
        "tradier_live_account_id":     None,
        "tradier_live_access_token":   None,
        "allow_live_trading":          False,
        "kill_switch":                 False,
    }
    base.update(overrides)
    return base


def _live_member(**overrides):
    base = {
        "email":                       "test@x.com",
        "subscription_active":         True,
        "tradier_active_mode":         "live",
        "tradier_account_id":          "VAPAPER",
        "tradier_access_token":        "paper_tok",
        "tradier_live_account_id":     "VALIVE",
        "tradier_live_access_token":   "live_tok",
        "allow_live_trading":          True,
        "kill_switch":                 False,
    }
    base.update(overrides)
    return base


class TestHappyPaths:
    def test_paper_client_all_healthy(self):
        st, hb = _organs_all_healthy_fresh()
        r = compute_readiness(_paper_member(), st, hb, now=NOW)
        assert r.ready is True
        assert r.allow_paper is True
        assert r.allow_live is False
        assert r.mode == "PAPER"
        assert r.blockers == []

    def test_live_client_fully_approved(self):
        st, hb = _organs_all_healthy_fresh()
        r = compute_readiness(_live_member(), st, hb, now=NOW)
        assert r.ready is True
        assert r.allow_live is True
        assert r.allow_paper is True  # paper creds also present
        assert r.mode == "LIVE"
        assert r.blockers == []


class TestKillSwitch:
    def test_kill_switch_disables_paper(self):
        m = _paper_member(kill_switch=True)
        st, hb = _organs_all_healthy_fresh()
        r = compute_readiness(m, st, hb, now=NOW)
        assert r.ready is False
        assert r.allow_paper is False
        assert "kill_switch_off" in r.blockers

    def test_kill_switch_disables_live(self):
        m = _live_member(kill_switch=True)
        st, hb = _organs_all_healthy_fresh()
        r = compute_readiness(m, st, hb, now=NOW)
        assert r.ready is False
        assert r.allow_live is False
        assert r.allow_paper is False
        assert "kill_switch_off" in r.blockers


class TestLiveSpecificGates:
    def test_live_without_live_creds_fails(self):
        m = _live_member(
            tradier_live_account_id=None,
            tradier_live_access_token=None,
        )
        st, hb = _organs_all_healthy_fresh()
        r = compute_readiness(m, st, hb, now=NOW)
        assert r.ready is False
        assert "live_connected" in r.blockers
        assert r.allow_live is False

    def test_live_without_operator_approval_fails(self):
        m = _live_member(allow_live_trading=False)
        st, hb = _organs_all_healthy_fresh()
        r = compute_readiness(m, st, hb, now=NOW)
        assert r.ready is False
        assert "live_operator_approved" in r.blockers
        assert r.allow_live is False

    def test_invariant_allow_live_requires_operator_approval(self):
        """The KEY safety invariant. allow_live must NEVER be True if
        operator hasn't approved."""
        for approved in [False, None, 0, "", "false"]:
            m = _live_member(allow_live_trading=approved)
            st, hb = _organs_all_healthy_fresh()
            r = compute_readiness(m, st, hb, now=NOW)
            assert r.allow_live is False, (
                f"INVARIANT VIOLATED: allow_live=True with "
                f"allow_live_trading={approved!r}"
            )


class TestOrganHealth:
    def test_stale_organ_heartbeat_blocks(self):
        m = _paper_member()
        st, hb = _organs_all_healthy_fresh()
        hb["reconciler"] = HB_STALE
        r = compute_readiness(m, st, hb, now=NOW)
        assert r.ready is False
        assert "reconciler_online" in r.blockers

    def test_degraded_organ_status_blocks(self):
        m = _paper_member()
        st, hb = _organs_all_healthy_fresh()
        st["exit_engine"] = "DEGRADED"
        r = compute_readiness(m, st, hb, now=NOW)
        assert r.ready is False
        assert "exit_engine_online" in r.blockers

    def test_missing_organ_data_fails_closed(self):
        """Empty organ data must produce not-ready — NEVER 'assume healthy'."""
        m = _paper_member()
        r = compute_readiness(m, {}, {}, now=NOW)
        assert r.ready is False
        for organ in [
            "runner_online", "fill_monitor_online",
            "reconciler_online", "exit_engine_online",
        ]:
            assert organ in r.blockers

    def test_organ_healthy_but_no_heartbeat_blocks(self):
        """Status=HEALTHY but missing heartbeat timestamp → not online.
        Defensive against partial data."""
        m = _paper_member()
        status_only = {organ: "HEALTHY" for organ in REQUIRED_ORGANS_LIVE}
        r = compute_readiness(m, status_only, {}, now=NOW)
        assert r.ready is False
        for organ in [
            "runner_online", "fill_monitor_online",
            "reconciler_online", "exit_engine_online",
        ]:
            assert organ in r.blockers

    def test_organ_freshness_boundary(self):
        """Heartbeat exactly at the freshness boundary should be considered
        fresh (<=). Heartbeat 1s past boundary should be stale (>)."""
        m = _paper_member()
        status = {organ: "HEALTHY" for organ in REQUIRED_ORGANS_LIVE}

        # All organs at exactly the boundary → still fresh
        hb_at_boundary = {
            organ: NOW - timedelta(seconds=DEFAULT_ORGAN_FRESHNESS_SEC)
            for organ in REQUIRED_ORGANS_LIVE
        }
        r = compute_readiness(m, status, hb_at_boundary, now=NOW)
        assert r.ready is True, f"At-boundary heartbeat should be fresh: {r.blockers}"

        # One organ 1 second past → stale
        hb_past = dict(hb_at_boundary)
        hb_past["fill_monitor"] = NOW - timedelta(seconds=DEFAULT_ORGAN_FRESHNESS_SEC + 1)
        r = compute_readiness(m, status, hb_past, now=NOW)
        assert r.ready is False
        assert "fill_monitor_online" in r.blockers


class TestModeConsistency:
    def test_mode_paper_requires_paper_creds(self):
        m = _paper_member(
            tradier_account_id=None,
            tradier_access_token=None,
        )
        st, hb = _organs_all_healthy_fresh()
        r = compute_readiness(m, st, hb, now=NOW)
        assert r.ready is False
        assert "paper_connected" in r.blockers
        assert "mode_consistent" in r.blockers

    def test_mode_live_inconsistent_when_missing_live_creds(self):
        m = _live_member(tradier_live_account_id=None)
        st, hb = _organs_all_healthy_fresh()
        r = compute_readiness(m, st, hb, now=NOW)
        assert r.ready is False
        assert "mode_consistent" in r.blockers


class TestSerialization:
    def test_report_serializes_to_dict(self):
        m = _paper_member()
        st, hb = _organs_all_healthy_fresh()
        r = compute_readiness(m, st, hb, now=NOW)
        d = r.to_dict()
        assert isinstance(d, dict)
        for required in ["email", "mode", "ready", "checks", "blockers",
                         "allow_paper", "allow_live", "critical_checks",
                         "organ_status", "organ_last_heartbeat", "computed_at"]:
            assert required in d, f"Missing field in report dict: {required}"

    def test_report_json_serializable(self):
        import json
        m = _paper_member()
        st, hb = _organs_all_healthy_fresh()
        r = compute_readiness(m, st, hb, now=NOW)
        # Default str fallback for datetime values
        s = json.dumps(r.to_dict(), default=str)
        # Round-trip
        d = json.loads(s)
        assert d["email"] == "test@x.com"
        assert d["ready"] is True


class TestOrganOnlineHelper:
    def test_organ_online_requires_healthy_status(self):
        assert _organ_is_online(
            {"runner": "HEALTHY"},
            {"runner": HB_FRESH},
            "runner", NOW,
        ) is True
        assert _organ_is_online(
            {"runner": "DEGRADED"},
            {"runner": HB_FRESH},
            "runner", NOW,
        ) is False

    def test_organ_online_requires_fresh_heartbeat(self):
        assert _organ_is_online(
            {"runner": "HEALTHY"},
            {"runner": HB_STALE},
            "runner", NOW,
        ) is False

    def test_organ_online_handles_missing_data(self):
        # Missing status
        assert _organ_is_online({}, {"runner": HB_FRESH}, "runner", NOW) is False
        # Missing heartbeat
        assert _organ_is_online({"runner": "HEALTHY"}, {}, "runner", NOW) is False
        # Both missing
        assert _organ_is_online({}, {}, "runner", NOW) is False

    def test_organ_status_case_insensitive(self):
        # Status comparison is uppercase
        assert _organ_is_online(
            {"runner": "healthy"},
            {"runner": HB_FRESH},
            "runner", NOW,
        ) is True
