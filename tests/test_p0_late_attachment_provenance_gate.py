"""
tests/test_p0_late_attachment_provenance_gate.py
==================================================
PR #388 Blocker 1 — the arm-time late-attachment classifier applies ONLY
to plans that explicitly opted in via `late_attachment_policy_eligible`
(set by run_overnight_reeval new watchers, REATTACH_WATCHER reconstructed
plans, and the open-revalidation seam).

Ordinary intraday/direct arms whose plan does NOT carry the flag must
keep committed-main's strict arm_already_through_trigger anti-chase
invariant: no WITHIN / WAITING / AWAITING seeding, no watcher registration,
watch() returns False, and on_invalidate is called for non-recovery arms.
"""
from __future__ import annotations

import pathlib
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import ap_entry_watcher as ew


def _force_regular_session(monkeypatch):
    """Freeze the base module's datetime.now to Thursday 2026-07-23 10:15 ET so
    the arm-time gate treats the call as regular-session (not pre_market,
    not post_session). The production import is a shim package; the arm-time
    logic executes in the loaded base module through super().watch()."""
    from datetime import datetime as _real_datetime
    from zoneinfo import ZoneInfo
    _fixed = _real_datetime(2026, 7, 23, 10, 15, tzinfo=ZoneInfo("America/New_York"))

    class _FrozenDT(_real_datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is not None:
                return _fixed.astimezone(tz)
            return _fixed.replace(tzinfo=None)

    # Patch the EXACT globals dict the base watch() method resolves
    # LOAD_GLOBAL against, not the shim module's copied datetime binding.
    base_watch_globals = ew._BaseAPEntryWatcher.watch.__globals__
    monkeypatch.setitem(base_watch_globals, "datetime", _FrozenDT)
    monkeypatch.setattr(ew, "_datetime", _FrozenDT)   # shim-local callers

    assert ew._BaseAPEntryWatcher.watch.__globals__["datetime"] is _FrozenDT


REPO = pathlib.Path(__file__).parent.parent
EW_SRC = (REPO / "ap_entry_watcher.py").read_text()
OV_SRC = (REPO / "ap_overnight_reeval.py").read_text()


# ── Structural: eligibility flag surfaces + seams opt in ──────────────────

def test_arm_time_gate_reads_eligibility_flag():
    assert "late_attachment_policy_eligible" in EW_SRC
    assert 'signal_dict.get("late_attachment_policy_eligible")' in EW_SRC


def test_overnight_reeval_new_watchers_opt_in():
    """The reeval seam that constructs a fresh plan must set the flag on
    both plan.metadata and as a top-level plan attribute."""
    assert '"late_attachment_policy_eligible": True,' in OV_SRC
    assert 'setattr(decision.plan, "late_attachment_policy_eligible", True)' in OV_SRC


def test_reattach_watcher_reconstructed_plan_opts_in():
    """The REATTACH_WATCHER path must set the flag on both the metadata
    dict and the reconstructed SimpleNamespace plan."""
    assert '"late_attachment_policy_eligible":  True,' in OV_SRC
    assert "late_attachment_policy_eligible = True" in OV_SRC


# ── Runtime: ordinary direct arm keeps committed-main strict behavior ─────

def _make_min_watcher(*, on_invalidate=None):
    """Construct an APEntryWatcher-shaped instance without going through
    the full __init__ (which requires broker / OSM). Enough plumbing for
    the arm-time gate to run."""
    import threading
    w = object.__new__(ew.APEntryWatcher)
    w._pending = []
    w._dedup_set = set()
    w._lock = threading.RLock()
    w._last_reject_reason = ""
    w.on_invalidate = on_invalidate
    w.order_state_machine = None
    w.mode = "PAPER"
    w.broker = SimpleNamespace(session=None)
    w._last_quote_fetch_proof = {}
    w.owner_token = "test-owner"
    w.core = None
    return w


def _ineligible_plan(*, side="CALL", trigger=100.0, stop=90.0, target=None):
    """Ordinary direct/intraday plan — NO late_attachment_policy_eligible flag."""
    return SimpleNamespace(
        signal_id="sig-ordinary-1",
        canonical_signal_id="sig-ordinary-1",
        ticker="AAPL",
        side=side,
        direction=side,
        score=80.0,
        tier="A",
        timeframe="5m",         # NOT "1d"; NOT overnight
        entry_trigger=trigger,
        trigger_price=trigger,
        stop_underlying=stop,
        target_underlying=target,
        pattern="2-1-2",
        contract_symbol=f"{side}:AAPL:100",
        contracts=2,
        limit_price=1.50,
        plan_id="plan-ordinary-1",
        client_id="jose@example.com",
        execution_mode="paper",
        prior_day_high=None,
        prior_day_low=None,
        # NO late_attachment_policy_eligible attribute
        metadata={},           # NO late_attachment_policy_eligible key
    )


def _eligible_plan(**overrides):
    plan = _ineligible_plan(**overrides)
    plan.late_attachment_policy_eligible = True
    plan.metadata = {"late_attachment_policy_eligible": True}
    return plan


def test_ordinary_arm_already_through_trigger_rejected_no_late_seed(monkeypatch):
    """Ordinary direct arm slightly through trigger — must reject with
    arm_already_through_trigger (committed-main behavior). NO late state
    seeded on the signal payload."""
    invalidate_calls = []
    def _on_inv(w_shim):
        invalidate_calls.append(w_shim._pending_audit)

    w = _make_min_watcher(on_invalidate=_on_inv)
    # Stub the arm-time quote fetch — ask through trigger by only 5 cents.
    monkeypatch.setattr(w, "_get_quote", lambda _t: {"bid": 100.05, "ask": 100.10})
    monkeypatch.setattr(w, "_persist_watcher_audit", lambda *_a, **_kw: None)
    monkeypatch.setattr(w, "_is_live_runtime", lambda: False)
    # Force regular session.
    _force_regular_session(monkeypatch)

    captured_signal = {}
    def _capture_add_signal(sig):
        captured_signal.update(sig)
        return True
    monkeypatch.setattr(w, "add_signal", _capture_add_signal)

    plan = _ineligible_plan(side="CALL", trigger=100.0, stop=90.0)
    result = w.watch(plan, local_order_id="local-ordinary-1")

    # Committed-main strict behavior: refuse to arm.
    assert result is False
    # add_signal MUST NOT have been called (watcher not registered).
    assert captured_signal == {}
    # on_invalidate MUST have been called with arm_already_through_trigger.
    assert len(invalidate_calls) == 1
    assert invalidate_calls[0]["reason_code"] == "arm_already_through_trigger"


def test_eligible_arm_within_continuation_seeds_late_state(monkeypatch):
    """Same shape as the previous test but with the eligibility flag SET.
    The PR#388 classifier must engage and seed WITHIN_CONTINUATION."""
    invalidate_calls = []
    def _on_inv(w_shim):
        invalidate_calls.append(w_shim._pending_audit)

    w = _make_min_watcher(on_invalidate=_on_inv)
    monkeypatch.setattr(w, "_get_quote", lambda _t: {"bid": 100.05, "ask": 100.06})
    monkeypatch.setattr(w, "_persist_watcher_audit", lambda *_a, **_kw: None)
    monkeypatch.setattr(w, "_is_live_runtime", lambda: False)
    _force_regular_session(monkeypatch)

    captured = {}
    def _capture_add_signal(sig):
        captured.update(sig)
        return True
    monkeypatch.setattr(w, "add_signal", _capture_add_signal)

    plan = _eligible_plan(side="CALL", trigger=100.0, stop=90.0)
    result = w.watch(plan, local_order_id="local-eligible-1")

    # Eligible arm accepted; late state seeded on the signal.
    assert result is True
    seed = captured.get("_late_attachment_seed") or {}
    from ap.pending_trigger_classifier import LATE_ATTACHMENT_WITHIN_CONTINUATION as _WITHIN
    assert seed.get("state") == _WITHIN
    # on_invalidate must NOT have been called.
    assert invalidate_calls == []


def test_ordinary_put_arm_slightly_below_trigger_rejected(monkeypatch):
    """PUT symmetric: ordinary direct arm with bid slightly below trigger
    must be rejected (committed-main strict behavior)."""
    invalidate_calls = []
    def _on_inv(w_shim):
        invalidate_calls.append(w_shim._pending_audit)
    w = _make_min_watcher(on_invalidate=_on_inv)
    monkeypatch.setattr(w, "_get_quote", lambda _t: {"bid": 99.95, "ask": 100.00})
    monkeypatch.setattr(w, "_persist_watcher_audit", lambda *_a, **_kw: None)
    monkeypatch.setattr(w, "_is_live_runtime", lambda: False)
    _force_regular_session(monkeypatch)
    monkeypatch.setattr(w, "add_signal", lambda _s: True)

    plan = _ineligible_plan(side="PUT", trigger=100.0, stop=110.0)
    result = w.watch(plan, local_order_id="local-ordinary-put-1")

    assert result is False
    assert len(invalidate_calls) == 1
    assert invalidate_calls[0]["reason_code"] == "arm_already_through_trigger"
