"""P0 — Critical money-at-risk fence alerting.

Incident pinned (2026-07-17/18 audit, PR-G): the events that mean client
capital is exposed were log-only. exit_decision_generation_claims was
missing from production for a full trading day; LIVE silently suppressed
every actionable exit decision and nobody was paged.

Pins:
  1. alert_critical delivers through the wired HEALTH alert path;
  2. dedup: one delivery per (event, dedup_key) per window; different
     keys/events deliver independently; window expiry re-delivers;
  3. never raises — delivery-path explosions degrade to logging;
  4. CRITICAL_ALERTS_ENABLED=0 suppresses delivery but still records;
  5. the recent-alert ring buffer records delivered AND suppressed;
  6. wiring: the three exit-decision fence sites and the OSM tag-recovery
     site actually invoke alert_critical (source-level contract check).
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.modules.setdefault("psycopg2", MagicMock())
sys.modules.setdefault("psycopg2.extras", MagicMock())
sys.modules.setdefault("psycopg2.pool", MagicMock())

import ap.critical_alerts as ca


@pytest.fixture(autouse=True)
def _reset(monkeypatch):
    ca._reset_for_tests()
    monkeypatch.delenv("CRITICAL_ALERTS_ENABLED", raising=False)
    monkeypatch.delenv("CRITICAL_ALERT_DEDUP_SECONDS", raising=False)
    yield
    ca._reset_for_tests()


@pytest.fixture()
def wired_health(monkeypatch):
    """Wire a capturing alert_fn into the real HEALTH registry."""
    from ap_health_registry import HEALTH

    captured: list[str] = []
    original = getattr(HEALTH, "_alert_fn", None)
    HEALTH.set_alert_fn(captured.append)
    yield captured
    HEALTH._alert_fn = original


def test_delivers_through_health_alert_path(wired_health):
    delivered = ca.alert_critical("EVT_A", "hello", dedup_key="k1")
    assert delivered is True
    assert len(wired_health) == 1
    assert "EVT_A" in wired_health[0] and "hello" in wired_health[0]
    assert wired_health[0].startswith("[P0]")


def test_dedup_suppresses_within_window(wired_health):
    assert ca.alert_critical("EVT_A", "first", dedup_key="k1", now_monotonic=1000.0) is True
    assert ca.alert_critical("EVT_A", "repeat", dedup_key="k1", now_monotonic=1010.0) is False
    assert len(wired_health) == 1


def test_dedup_expires_and_redelivers(wired_health):
    assert ca.alert_critical("EVT_A", "first", dedup_key="k1",
                             window_seconds=60.0, now_monotonic=1000.0) is True
    assert ca.alert_critical("EVT_A", "later", dedup_key="k1",
                             window_seconds=60.0, now_monotonic=1061.0) is True
    assert len(wired_health) == 2


def test_distinct_keys_and_events_deliver_independently(wired_health):
    assert ca.alert_critical("EVT_A", "m", dedup_key="pos-1", now_monotonic=1000.0) is True
    assert ca.alert_critical("EVT_A", "m", dedup_key="pos-2", now_monotonic=1000.0) is True
    assert ca.alert_critical("EVT_B", "m", dedup_key="pos-1", now_monotonic=1000.0) is True
    assert len(wired_health) == 3


def test_never_raises_when_delivery_path_explodes(monkeypatch):
    from ap_health_registry import HEALTH

    original = getattr(HEALTH, "_alert_fn", None)

    def _boom(msg):
        raise RuntimeError("discord down")

    HEALTH.set_alert_fn(_boom)
    try:
        # Must not raise; returns True because a delivery attempt was made.
        assert ca.alert_critical("EVT_A", "m", dedup_key="k") is True
    finally:
        HEALTH._alert_fn = original


def test_no_alert_fn_wired_degrades_to_logging(monkeypatch):
    from ap_health_registry import HEALTH

    original = getattr(HEALTH, "_alert_fn", None)
    HEALTH._alert_fn = None
    try:
        # No delivery path — still returns True (attempted, not suppressed) and never raises.
        assert ca.alert_critical("EVT_A", "m", dedup_key="k") is True
    finally:
        HEALTH._alert_fn = original


def test_env_kill_switch_suppresses_delivery(wired_health, monkeypatch):
    monkeypatch.setenv("CRITICAL_ALERTS_ENABLED", "0")
    assert ca.alert_critical("EVT_A", "m", dedup_key="k") is False
    assert wired_health == []
    records = ca.get_recent_critical_alerts()
    assert len(records) == 1  # still recorded for observability


def test_recent_buffer_records_delivered_and_suppressed(wired_health):
    ca.alert_critical("EVT_A", "one", dedup_key="k", now_monotonic=1000.0)
    ca.alert_critical("EVT_A", "two", dedup_key="k", now_monotonic=1001.0)
    records = ca.get_recent_critical_alerts()
    assert [r["suppressed"] for r in records] == [False, True]
    assert all(r["event"] == "EVT_A" for r in records)


# ---------------------------------------------------------------------------
# Wiring contract — the fence sites must actually call alert_critical.
# Source-level checks: cheap, unambiguous, and immune to the heavy import
# environment those modules need at runtime.
# ---------------------------------------------------------------------------

_ROOT = Path(__file__).resolve().parent.parent


def _source(rel: str) -> str:
    return (_ROOT / rel).read_text(encoding="utf-8")


def test_exit_decision_guard_wires_all_three_fence_alerts():
    src = _source("ap/exit_decision_idempotency_guard.py")
    for event in (
        "EXIT_DECISION_STALE_CLAIM_AMBIGUOUS",
        "EXIT_DECISION_GENERATION_READ_UNAVAILABLE",
        "EXIT_DECISION_GENERATION_CLAIM_FAILED",
    ):
        # Each event name must appear as an alert_critical() argument, not
        # merely inside a log line.
        assert f'alert_critical(\n                            "{event}"' in src \
            or f'alert_critical(\n                                "{event}"' in src \
            or f'alert_critical(\n                        "{event}"' in src, (
            f"{event} is not wired to alert_critical"
        )


def test_osm_wires_tag_recovery_alert():
    src = _source("ap/order_state_machine.py")
    assert '"ENTRY_BROKER_TAG_RECOVERY"' in src
    assert "alert_critical" in src


def test_fence_wiring_is_defensive_nonraising():
    """Every wiring site must guard the import/call so alerting can never
    break the trade path."""
    for rel in ("ap/exit_decision_idempotency_guard.py", "ap/order_state_machine.py"):
        src = _source(rel)
        for chunk in src.split("from ap.critical_alerts import alert_critical")[1:]:
            # The wrapping try/except sits before the import; the matching
            # handler must appear shortly after the call.
            assert "except Exception:" in chunk[:1400], (
                f"unguarded alert_critical wiring in {rel}"
            )
