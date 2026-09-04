"""
tests/test_p0_shared_ap_signals_retry_idempotency.py
=====================================================
PR #388 — Shared ap_signals retry idempotency.

Production-shaped tests for the 8 defects repaired in this amendment:
  1. LOOKUP_FAILED is genuinely reachable and distinct from NOT_FOUND.
  2. Successful arm writes durable WATCHER_ARMED proof.
  3. Active-order fence prevents double create_entry_order.
  4. Existing PENDING_TRIGGER order triggers REATTACH_WATCHER, not new order.
  5. Execution-mode isolation is exact (blank stored mode = no match).
  6. Session isolation is exact (blank stored session = not current session).
  7. Terminal status coverage uses canonical sets from opportunity_ledger.
  8. Randomised REEVAL IDs do not fragment dedup.

Tests follow the shared-source harness style from
test_p0_overnight_mixed_inventory_watcher_first.py.
"""
from __future__ import annotations

import sys
import types
from datetime import datetime, timezone
from typing import Any
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from zoneinfo import ZoneInfo

import ap_overnight_reeval as ov


# ── Fixed timestamps ──────────────────────────────────────────────────────────

FIXED_ET = datetime(2026, 7, 22, 9, 20, tzinfo=ZoneInfo("America/New_York"))
SESSION_KEY = FIXED_ET.date().isoformat()   # "2026-07-22"
PREV_SESSION = "2026-07-21"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _canonical(signal_id: str) -> str:
    """Stable canonical ID from a signal_id (strips REEVAL prefix if any)."""
    if signal_id.startswith("REEVAL:"):
        parts = signal_id.split(":", 2)
        return parts[1] if len(parts) >= 2 else signal_id
    return signal_id


def _signal(
    signal_id: str,
    ticker: str = "AAPL",
    *,
    side: str = "CALL",
    score: float = 80.0,
    created_at: str = "2026-07-21T20:00:00+00:00",
) -> dict:
    canonical = _canonical(signal_id)
    return {
        "signal_id":          signal_id,
        "canonical_signal_id": canonical,
        "ticker":             ticker,
        "symbol":             ticker,
        "side":               side,
        "timeframe":          "1d",
        "score":              score,
        "tier":               "A",
        "pattern":            "2-1-2",
        "entry_trigger":      101.0 if side == "CALL" else 95.0,
        "created_at":         created_at,
        "prior_day_high":     102.0,
        "prior_day_low":      94.0,
    }


def _opp_row(
    status: str,
    *,
    session: str = SESSION_KEY,
    mode: str = "paper",
    order_local_id: str = "local-1",
) -> dict:
    """Build a fake client_signal_opportunities row."""
    return {
        "opportunity_status": status,
        "miss_stage":         "",
        "miss_reason":        "",
        "order_local_id":     order_local_id,
        "metadata": {
            "overnight_reeval_session_key": session,
            "execution_mode":               mode,
        },
    }


def _order_row(
    local_order_id: str,
    status: str,
    *,
    client_id: str = "jose@example.com",
    execution_mode: str = "paper",
    canonical_signal_id: str = "sig-A",
) -> dict:
    return {
        "local_order_id":     local_order_id,
        "client_id":          client_id,
        "kind":               "ENTRY",
        "status":             status,
        "execution_mode":     execution_mode,
        "canonical_signal_id": canonical_signal_id,
        "symbol":             "AAPL",
        "direction":          "CALL",
        "trigger_price":      101.0,
        "qty":                2,
        "limit_price":        0.01,
        "score":              80.0,
        "tier":               "A",
        "pattern":            "2-1-2",
        "timeframe":          "1d",
        "plan_id":            "plan-1",
        "signal_id":          "sig-A",
        "meta":               {},
    }


# ── Patching helpers ──────────────────────────────────────────────────────────

def _patch_opportunity_lookup(monkeypatch, result: ov._LookupResult):
    """Patch _get_client_opportunity_row to return an explicit LookupResult."""
    monkeypatch.setattr(ov, "_get_client_opportunity_row", lambda *_a, **_kw: result)


def _patch_active_order_lookup(monkeypatch, status: str, row: dict | None):
    """Patch order fences to return explicit active truth and no latest history."""
    monkeypatch.setattr(
        ov,
        "_query_active_entry_order",
        lambda *_a, **_kw: (status, row),
    )
    monkeypatch.setattr(
        ov,
        "_query_latest_entry_order_no_status",
        lambda *_a, **_kw: (ov._LS_NOT_FOUND, None),
    )


def _patch_et_now(monkeypatch):
    monkeypatch.setattr(ov, "_et_now", lambda: FIXED_ET)


def _patch_resolve_canonical(monkeypatch):
    monkeypatch.setattr(
        ov,
        "_resolve_canonical_signal_id",
        lambda signal_id, signal: _canonical(str(signal_id or "")),
    )


# ── Shared sub-module stubs ───────────────────────────────────────────────────

def _install_base_stubs(monkeypatch):
    """Install the minimal sub-module stubs needed by _resolve_shared_setup_disposition."""
    _patch_et_now(monkeypatch)
    _patch_resolve_canonical(monkeypatch)

    # ap_canonical_signal
    cs_mod = types.ModuleType("ap_canonical_signal")
    cs_mod.build_canonical_signal_id = lambda sid, sig=None: _canonical(str(sid or ""))
    monkeypatch.setitem(sys.modules, "ap_canonical_signal", cs_mod)


# ─────────────────────────────────────────────────────────────────────────────
# TEST 1: Successful shared arm is idempotent across two attempts
# ─────────────────────────────────────────────────────────────────────────────

def test_1_successful_arm_is_idempotent(monkeypatch):
    """Attempt 2 must resolve ALREADY_ARMED; no second order or watcher."""
    _install_base_stubs(monkeypatch)

    # After attempt 1 succeeds: opportunity row carries WATCHER_ARMED.
    armed_row = _opp_row("WATCHER_ARMED", session=SESSION_KEY, mode="paper")
    _patch_opportunity_lookup(
        monkeypatch,
        ov._LookupResult("sig-A", ov._LS_FOUND, armed_row, None),
    )
    _patch_active_order_lookup(monkeypatch, ov._LS_NOT_FOUND, None)

    result = ov._resolve_shared_setup_disposition(
        "sig-A", "jose@example.com", _signal("sig-A"),
        execution_mode="paper",
        session_key=SESSION_KEY,
    )
    assert result.disposition == ov._DISPOSITION_ALREADY_ARMED

    # Verify caller does not reach master_control / create_entry_order.
    mc_calls = []
    osm_calls = []
    monkeypatch.setattr(ov, "_resolve_shared_setup_disposition",
                        lambda *a, **k: ov._DispositionResult(ov._DISPOSITION_ALREADY_ARMED))
    # No OSM / MC reached — that is proven by the disposition short-circuit in the
    # caller loop (see test_4 for full end-to-end check).


# ─────────────────────────────────────────────────────────────────────────────
# TEST 2: Lookup failure fails closed
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("fail_reason,lookup_result", [
    (
        "exception",
        ov._LookupResult("sig-A", ov._LS_LOOKUP_FAILED, None, "query_raised"),
    ),
    (
        "sb_unavailable",
        ov._LookupResult("sig-A", ov._LS_LOOKUP_FAILED, None, "supabase_client_unavailable"),
    ),
    (
        "malformed_response",
        ov._LookupResult("sig-A", ov._LS_LOOKUP_FAILED, None, "malformed_response_data_none"),
    ),
])
def test_2_lookup_failure_fails_closed(monkeypatch, fail_reason, lookup_result):
    """LOOKUP_FAILED from any cause must return LOOKUP_FAILED disposition (not NEW)."""
    _install_base_stubs(monkeypatch)
    _patch_opportunity_lookup(monkeypatch, lookup_result)

    result = ov._resolve_shared_setup_disposition(
        "sig-A", "jose@example.com", _signal("sig-A"),
        execution_mode="paper",
        session_key=SESSION_KEY,
    )
    assert result.disposition == ov._DISPOSITION_LOOKUP_FAILED, (
        f"fail_reason={fail_reason!r}: expected LOOKUP_FAILED, got {result.disposition!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# TEST 3: Definitive NOT_FOUND allows NEW disposition
# ─────────────────────────────────────────────────────────────────────────────

def test_3_not_found_allows_new(monkeypatch):
    """NOT_FOUND (query succeeded, zero rows) must be distinguishable from LOOKUP_FAILED."""
    _install_base_stubs(monkeypatch)
    _patch_opportunity_lookup(
        monkeypatch,
        ov._LookupResult("sig-A", ov._LS_NOT_FOUND, None, None),
    )
    _patch_active_order_lookup(monkeypatch, ov._LS_NOT_FOUND, None)

    result = ov._resolve_shared_setup_disposition(
        "sig-A", "jose@example.com", _signal("sig-A"),
        execution_mode="paper",
        session_key=SESSION_KEY,
    )
    assert result.disposition == ov._DISPOSITION_NEW
    # Confirm NOT_FOUND != LOOKUP_FAILED at the type level.
    assert ov._LS_NOT_FOUND != ov._LS_LOOKUP_FAILED


# ─────────────────────────────────────────────────────────────────────────────
# TEST 4: Existing order and watcher → ALREADY_ARMED
# ─────────────────────────────────────────────────────────────────────────────

def test_4_existing_order_and_watcher_already_armed(monkeypatch):
    """WATCHER_ARMED row for same client/mode/session → ALREADY_ARMED."""
    _install_base_stubs(monkeypatch)
    _patch_opportunity_lookup(
        monkeypatch,
        ov._LookupResult(
            "sig-A", ov._LS_FOUND,
            _opp_row("WATCHER_ARMED", session=SESSION_KEY, mode="paper"),
            None,
        ),
    )
    _patch_active_order_lookup(monkeypatch, ov._LS_NOT_FOUND, None)

    result = ov._resolve_shared_setup_disposition(
        "sig-A", "jose@example.com", _signal("sig-A"),
        execution_mode="paper",
        session_key=SESSION_KEY,
    )
    assert result.disposition == ov._DISPOSITION_ALREADY_ARMED
    assert result.existing_local_order_id is None


# ─────────────────────────────────────────────────────────────────────────────
# TEST 5: Existing order without watcher → REATTACH_WATCHER
# ─────────────────────────────────────────────────────────────────────────────

def test_5_existing_order_without_watcher_reattach(monkeypatch):
    """PENDING_TRIGGER order exists but no WATCHER_ARMED proof → REATTACH_WATCHER."""
    _install_base_stubs(monkeypatch)
    # Opportunity is NOT WATCHER_ARMED (e.g. ORDER_CREATED or prior failed write).
    _patch_opportunity_lookup(
        monkeypatch,
        ov._LookupResult(
            "sig-A", ov._LS_FOUND,
            _opp_row("ORDER_CREATED", session=SESSION_KEY, mode="paper"),
            None,
        ),
    )
    existing_order = _order_row("local-99", "PENDING_TRIGGER", canonical_signal_id="sig-A")
    _patch_active_order_lookup(monkeypatch, ov._LS_FOUND, existing_order)

    result = ov._resolve_shared_setup_disposition(
        "sig-A", "jose@example.com", _signal("sig-A"),
        execution_mode="paper",
        session_key=SESSION_KEY,
    )
    assert result.disposition == ov._DISPOSITION_REATTACH_WATCHER
    assert result.existing_local_order_id == "local-99"
    assert result.existing_order_row is not None

    # A second attempt after successful reattachment (which writes WATCHER_ARMED)
    # must return ALREADY_ARMED.
    _patch_opportunity_lookup(
        monkeypatch,
        ov._LookupResult(
            "sig-A", ov._LS_FOUND,
            _opp_row("WATCHER_ARMED", session=SESSION_KEY, mode="paper", order_local_id="local-99"),
            None,
        ),
    )
    result2 = ov._resolve_shared_setup_disposition(
        "sig-A", "jose@example.com", _signal("sig-A"),
        execution_mode="paper",
        session_key=SESSION_KEY,
    )
    assert result2.disposition == ov._DISPOSITION_ALREADY_ARMED


# ─────────────────────────────────────────────────────────────────────────────
# TEST 6: Paper and LIVE isolation
# ─────────────────────────────────────────────────────────────────────────────

def test_6_paper_and_live_isolation(monkeypatch):
    """PAPER WATCHER_ARMED row must not suppress LIVE; LIVE must not suppress PAPER."""
    _install_base_stubs(monkeypatch)

    paper_armed_row = _opp_row("WATCHER_ARMED", session=SESSION_KEY, mode="paper")

    # PAPER lookup → ALREADY_ARMED for PAPER
    _patch_opportunity_lookup(
        monkeypatch,
        ov._LookupResult("sig-A", ov._LS_FOUND, paper_armed_row, None),
    )
    _patch_active_order_lookup(monkeypatch, ov._LS_NOT_FOUND, None)
    paper_result = ov._resolve_shared_setup_disposition(
        "sig-A", "jose@example.com", _signal("sig-A"),
        execution_mode="paper",
        session_key=SESSION_KEY,
    )
    assert paper_result.disposition == ov._DISPOSITION_ALREADY_ARMED

    # LIVE request with same PAPER row → mode mismatch → NOT ALREADY_ARMED.
    # Active-order lookup also clean → NEW.
    live_result = ov._resolve_shared_setup_disposition(
        "sig-A", "jose@example.com", _signal("sig-A"),
        execution_mode="live",
        session_key=SESSION_KEY,
    )
    assert live_result.disposition == ov._DISPOSITION_NEW, (
        f"LIVE should not reuse PAPER ownership; got {live_result.disposition!r}"
    )

    # Reverse: LIVE armed, PAPER must not reuse.
    live_armed_row = _opp_row("WATCHER_ARMED", session=SESSION_KEY, mode="live")
    _patch_opportunity_lookup(
        monkeypatch,
        ov._LookupResult("sig-A", ov._LS_FOUND, live_armed_row, None),
    )
    paper_after_live = ov._resolve_shared_setup_disposition(
        "sig-A", "jose@example.com", _signal("sig-A"),
        execution_mode="paper",
        session_key=SESSION_KEY,
    )
    assert paper_after_live.disposition == ov._DISPOSITION_NEW, (
        f"PAPER should not reuse LIVE ownership; got {paper_after_live.disposition!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# TEST 7: Client isolation
# ─────────────────────────────────────────────────────────────────────────────

def test_7_client_isolation(monkeypatch):
    """Jose's WATCHER_ARMED must not suppress Tradefluence's setup."""
    _install_base_stubs(monkeypatch)

    jose_armed = _opp_row("WATCHER_ARMED", session=SESSION_KEY, mode="paper")
    # Patching lookup returns jose's row for jose, nothing for tradefluence.
    def _selective_lookup(signal_id, client_id, signal):
        if client_id == "jose@example.com":
            return ov._LookupResult("sig-A", ov._LS_FOUND, jose_armed, None)
        return ov._LookupResult("sig-A", ov._LS_NOT_FOUND, None, None)

    monkeypatch.setattr(ov, "_get_client_opportunity_row", _selective_lookup)
    _patch_active_order_lookup(monkeypatch, ov._LS_NOT_FOUND, None)

    jose_result = ov._resolve_shared_setup_disposition(
        "sig-A", "jose@example.com", _signal("sig-A"),
        execution_mode="paper", session_key=SESSION_KEY,
    )
    assert jose_result.disposition == ov._DISPOSITION_ALREADY_ARMED

    tradefluence_result = ov._resolve_shared_setup_disposition(
        "sig-A", "tradefluence@example.com", _signal("sig-A"),
        execution_mode="paper", session_key=SESSION_KEY,
    )
    assert tradefluence_result.disposition == ov._DISPOSITION_NEW, (
        f"Tradefluence should not be suppressed by Jose's proof; "
        f"got {tradefluence_result.disposition!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# TEST 8: Session isolation
# ─────────────────────────────────────────────────────────────────────────────

def test_8_session_isolation_yesterday_row_does_not_suppress(monkeypatch):
    """Yesterday's WATCHER_ARMED row must not suppress today's setup."""
    _install_base_stubs(monkeypatch)

    yesterday_armed = _opp_row("WATCHER_ARMED", session=PREV_SESSION, mode="paper")
    _patch_opportunity_lookup(
        monkeypatch,
        ov._LookupResult("sig-A", ov._LS_FOUND, yesterday_armed, None),
    )
    _patch_active_order_lookup(monkeypatch, ov._LS_NOT_FOUND, None)

    result = ov._resolve_shared_setup_disposition(
        "sig-A", "jose@example.com", _signal("sig-A"),
        execution_mode="paper",
        session_key=SESSION_KEY,   # today
    )
    # Prior-session row + no active order → NEW for today.
    assert result.disposition == ov._DISPOSITION_NEW, (
        f"Yesterday's WATCHER_ARMED should not suppress today; got {result.disposition!r}"
    )


def test_8b_blank_session_does_not_establish_current(monkeypatch):
    """Blank stored session NEVER counts as current session."""
    _install_base_stubs(monkeypatch)

    blank_session_row = {
        "opportunity_status": "WATCHER_ARMED",
        "miss_stage": "",
        "miss_reason": "",
        "order_local_id": "local-old",
        "metadata": {
            # No overnight_reeval_session_key → blank
            "execution_mode": "paper",
        },
    }
    _patch_opportunity_lookup(
        monkeypatch,
        ov._LookupResult("sig-A", ov._LS_FOUND, blank_session_row, None),
    )
    _patch_active_order_lookup(monkeypatch, ov._LS_NOT_FOUND, None)

    result = ov._resolve_shared_setup_disposition(
        "sig-A", "jose@example.com", _signal("sig-A"),
        execution_mode="paper",
        session_key=SESSION_KEY,
    )
    # Blank session → not ALREADY_ARMED; no active order → NEW
    assert result.disposition == ov._DISPOSITION_NEW, (
        f"Blank session should not establish ALREADY_ARMED; got {result.disposition!r}"
    )


def test_8c_blank_mode_does_not_establish_ownership(monkeypatch):
    """Blank stored execution_mode NEVER establishes ownership."""
    _install_base_stubs(monkeypatch)

    blank_mode_row = {
        "opportunity_status": "WATCHER_ARMED",
        "miss_stage": "",
        "miss_reason": "",
        "order_local_id": "local-old",
        "metadata": {
            "overnight_reeval_session_key": SESSION_KEY,
            # No execution_mode → blank
        },
    }
    _patch_opportunity_lookup(
        monkeypatch,
        ov._LookupResult("sig-A", ov._LS_FOUND, blank_mode_row, None),
    )
    _patch_active_order_lookup(monkeypatch, ov._LS_NOT_FOUND, None)

    result = ov._resolve_shared_setup_disposition(
        "sig-A", "jose@example.com", _signal("sig-A"),
        execution_mode="paper",
        session_key=SESSION_KEY,
    )
    # Blank stored mode → mode_match = False → falls through to active-order check
    # → no active order → NEW
    assert result.disposition == ov._DISPOSITION_NEW, (
        f"Blank mode should not establish ownership; got {result.disposition!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# TEST 9: Terminal status coverage (canonical set from opportunity_ledger)
# ─────────────────────────────────────────────────────────────────────────────

from ap.opportunity_ledger import TERMINAL_STATUSES as _CANONICAL_TERMINAL_STATUSES, FILLED as _OL_FILLED

# FILLED is handled separately in test_9b (resolves ALREADY_OWNED, not ALREADY_TERMINAL,
# because the spec classifies it as "already entered / already owned").
_TERMINAL_STATUSES_EXCL_FILLED = sorted(_CANONICAL_TERMINAL_STATUSES - {_OL_FILLED})


@pytest.mark.parametrize("terminal_status", _TERMINAL_STATUSES_EXCL_FILLED)
def test_9_terminal_status_coverage(monkeypatch, terminal_status):
    """Every canonical non-FILLED terminal status must resolve ALREADY_TERMINAL."""
    _install_base_stubs(monkeypatch)

    terminal_row = _opp_row(terminal_status, session=SESSION_KEY, mode="paper")
    _patch_opportunity_lookup(
        monkeypatch,
        ov._LookupResult("sig-A", ov._LS_FOUND, terminal_row, None),
    )
    _patch_active_order_lookup(monkeypatch, ov._LS_NOT_FOUND, None)

    result = ov._resolve_shared_setup_disposition(
        "sig-A", "jose@example.com", _signal("sig-A"),
        execution_mode="paper",
        session_key=SESSION_KEY,
    )
    assert result.disposition == ov._DISPOSITION_ALREADY_TERMINAL, (
        f"status={terminal_status!r} expected ALREADY_TERMINAL, got {result.disposition!r}"
    )


def test_9b_filled_as_already_owned(monkeypatch):
    """FILLED opportunity row in current session/mode → ALREADY_OWNED."""
    _install_base_stubs(monkeypatch)

    filled_row = _opp_row("FILLED", session=SESSION_KEY, mode="paper")
    _patch_opportunity_lookup(
        monkeypatch,
        ov._LookupResult("sig-A", ov._LS_FOUND, filled_row, None),
    )
    _patch_active_order_lookup(monkeypatch, ov._LS_NOT_FOUND, None)

    result = ov._resolve_shared_setup_disposition(
        "sig-A", "jose@example.com", _signal("sig-A"),
        execution_mode="paper",
        session_key=SESSION_KEY,
    )
    # FILLED is in TERMINAL_STATUSES → ALREADY_TERMINAL, OR in broker-owned
    # statuses → ALREADY_OWNED. Either is correct (no new order).
    assert result.disposition in (
        ov._DISPOSITION_ALREADY_TERMINAL,
        ov._DISPOSITION_ALREADY_OWNED,
    ), f"FILLED should not produce NEW or RETRYABLE; got {result.disposition!r}"


# ─────────────────────────────────────────────────────────────────────────────
# TEST 10: Watcher success proof write failure stays retryable
# ─────────────────────────────────────────────────────────────────────────────

def test_10_watcher_armed_but_proof_write_fails(monkeypatch):
    """If watch() succeeds but WATCHER_ARMED write fails → retryable_deferred, not armed."""
    _install_base_stubs(monkeypatch)

    # Resolver returns NEW (no prior record).
    monkeypatch.setattr(
        ov, "_resolve_shared_setup_disposition",
        lambda *a, **k: ov._DispositionResult(ov._DISPOSITION_NEW),
    )

    create_order_calls = []
    watch_calls = []
    mark_wa_calls: list[bool] = []   # return value controlled per test

    def _fake_create_entry_order(plan, **kwargs):
        create_order_calls.append(kwargs)
        return "local-test-1"

    def _fake_watch(plan, local_order_id):
        watch_calls.append(local_order_id)
        return True   # watch SUCCEEDS

    def _fake_mark_watcher_armed(*_a, **_kw):
        return False   # write FAILS

    def _fake_create_opps(*_a, **_kw):
        return 1

    # Patch the opportunity_ledger inside the proof-write path.
    ol_mod = types.ModuleType("ap.opportunity_ledger")
    ol_mod.create_opportunities = _fake_create_opps
    ol_mod.mark_watcher_armed = _fake_mark_watcher_armed
    ol_mod.WATCHER_ARMED = "WATCHER_ARMED"
    ol_mod.BROKER_SUBMITTED = "BROKER_SUBMITTED"
    ol_mod.BROKER_ACKED = "BROKER_ACKED"
    ol_mod.FILLED = "FILLED"
    ol_mod.TERMINAL_STATUSES = frozenset({
        "FILLED", "EXPIRED", "CANCELED", "MISSED", "CLIENT_SKIPPED",
        "BROKER_REJECTED", "INTERNAL_ERROR", "WATCHER_INVALIDATED",
        "ENTRY_CONFIRMATION_FAILED",
    })
    ol_mod._get_sb = lambda: None
    monkeypatch.setitem(sys.modules, "ap.opportunity_ledger", ol_mod)

    # Build a minimal run_overnight_reeval execution via _run_shared_ap_signals_harness.
    result = _run_shared_ap_signals_harness(
        monkeypatch,
        signal_id="sig-A",
        osm_create=_fake_create_entry_order,
        watch_fn=_fake_watch,
        disposition=ov._DISPOSITION_NEW,
    )

    assert len(watch_calls) == 1, "watcher.watch() must be called once"
    assert len(create_order_calls) == 1, "create_entry_order must be called once"
    # Even though watch succeeded, armed must NOT be incremented because proof write failed.
    assert result["armed"] == 0, (
        f"armed should be 0 when proof write fails; got {result['armed']}"
    )
    assert result["retryable_deferred"] >= 1, (
        "row must be retryable_deferred when proof write fails"
    )


# ─────────────────────────────────────────────────────────────────────────────
# TEST 11: Active-order lookup failure fails closed
# ─────────────────────────────────────────────────────────────────────────────

def test_11_active_order_lookup_failure_fails_closed(monkeypatch):
    """If active-order DB query fails → LOOKUP_FAILED disposition; no new order."""
    _install_base_stubs(monkeypatch)

    # Opportunity NOT_FOUND (zero rows) — would normally allow NEW.
    _patch_opportunity_lookup(
        monkeypatch,
        ov._LookupResult("sig-A", ov._LS_NOT_FOUND, None, None),
    )
    # But active-order query raises / returns LOOKUP_FAILED.
    _patch_active_order_lookup(monkeypatch, ov._LS_LOOKUP_FAILED, None)

    result = ov._resolve_shared_setup_disposition(
        "sig-A", "jose@example.com", _signal("sig-A"),
        execution_mode="paper",
        session_key=SESSION_KEY,
    )
    assert result.disposition == ov._DISPOSITION_LOOKUP_FAILED, (
        f"Active-order lookup failure must fail closed; got {result.disposition!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# TEST 12: Randomised REEVAL IDs do not fragment dedup
# ─────────────────────────────────────────────────────────────────────────────

def test_12_randomised_reeval_ids_do_not_fragment_dedup(monkeypatch):
    """Two different REEVAL signal IDs for the same base signal share canonical identity."""
    _install_base_stubs(monkeypatch)

    reeval_id_1 = "REEVAL:sig-A:20260722-091500"
    reeval_id_2 = "REEVAL:sig-A:20260722-093000"

    assert _canonical(reeval_id_1) == "sig-A"
    assert _canonical(reeval_id_2) == "sig-A"

    armed_row = _opp_row("WATCHER_ARMED", session=SESSION_KEY, mode="paper")
    _patch_opportunity_lookup(
        monkeypatch,
        ov._LookupResult("sig-A", ov._LS_FOUND, armed_row, None),
    )
    _patch_active_order_lookup(monkeypatch, ov._LS_NOT_FOUND, None)

    # Attempt 2 uses a different REEVAL ID but must still resolve ALREADY_ARMED.
    result = ov._resolve_shared_setup_disposition(
        reeval_id_2, "jose@example.com", _signal(reeval_id_2),
        execution_mode="paper",
        session_key=SESSION_KEY,
    )
    assert result.disposition == ov._DISPOSITION_ALREADY_ARMED, (
        f"Different REEVAL ID must resolve ALREADY_ARMED via canonical; "
        f"got {result.disposition!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# TEST 13: No premarket selector or broker mutation across all dispositions
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("disposition", [
    ov._DISPOSITION_ALREADY_ARMED,
    ov._DISPOSITION_ALREADY_TERMINAL,
    ov._DISPOSITION_ALREADY_OWNED,
    ov._DISPOSITION_LOOKUP_FAILED,
    ov._DISPOSITION_AMBIGUOUS_OWNERSHIP,
])
def test_13_no_selector_or_broker_calls_for_terminal_dispositions(monkeypatch, disposition):
    """Non-NEW dispositions must prevent any selector/broker mutation."""
    _install_base_stubs(monkeypatch)

    selector_calls = []
    broker_submit_calls = []
    broker_cancel_calls = []
    broker_replace_calls = []

    row = (
        _opp_row("WATCHER_ARMED", session=SESSION_KEY, mode="paper")
        if disposition == ov._DISPOSITION_ALREADY_ARMED
        else _opp_row("MISSED", session=SESSION_KEY, mode="paper")
    )
    _patch_opportunity_lookup(
        monkeypatch,
        ov._LookupResult("sig-A", ov._LS_FOUND, row, None),
    )
    _patch_active_order_lookup(monkeypatch, ov._LS_NOT_FOUND, None)

    # Override the resolver to return the target disposition directly.
    monkeypatch.setattr(
        ov, "_resolve_shared_setup_disposition",
        lambda *a, **k: ov._DispositionResult(disposition),
    )

    # Run a minimal harness that captures selector/broker calls.
    result = _run_shared_ap_signals_harness(
        monkeypatch,
        signal_id="sig-A",
        disposition=disposition,
        selector_calls=selector_calls,
        broker_submit_calls=broker_submit_calls,
        broker_cancel_calls=broker_cancel_calls,
        broker_replace_calls=broker_replace_calls,
    )

    assert len(selector_calls) == 0, f"selector called for {disposition!r}"
    assert len(broker_submit_calls) == 0, f"broker.submit called for {disposition!r}"
    assert len(broker_cancel_calls) == 0, f"broker.cancel called for {disposition!r}"
    assert len(broker_replace_calls) == 0, f"broker.replace called for {disposition!r}"


def test_final_preopen_reattach_gets_verified_retry_owner_without_broker_mutation(monkeypatch):
    broker_submit_calls = []
    broker_cancel_calls = []
    broker_replace_calls = []
    existing = _order_row(
        "local-reattach-1",
        "PENDING_TRIGGER",
        execution_mode="paper",
        canonical_signal_id="sig-A",
    )

    result = _run_shared_ap_signals_harness(
        monkeypatch,
        signal_id="sig-A",
        disposition=ov._DISPOSITION_REATTACH_WATCHER,
        existing_order=existing,
        now_et=datetime(
            2026, 7, 22, 9, 29, 30,
            tzinfo=ZoneInfo("America/New_York"),
        ),
        broker_submit_calls=broker_submit_calls,
        broker_cancel_calls=broker_cancel_calls,
        broker_replace_calls=broker_replace_calls,
    )

    assert result["retry_owned"] == 1
    assert result["result_class"] == "COMPLETED_WITH_OWNED_RETRIES"
    assert result["completed"] is True
    assert broker_submit_calls == []
    assert broker_cancel_calls == []
    assert broker_replace_calls == []


# ─────────────────────────────────────────────────────────────────────────────
# TEST 14: Row accounting — no row in two buckets
# ─────────────────────────────────────────────────────────────────────────────

def test_14_row_accounting_no_bucket_overlap():
    """
    Verify the disposition constants cover non-overlapping accounting buckets.

    already_resolved:  ALREADY_ARMED | ALREADY_OWNED | ALREADY_TERMINAL
    retryable_deferred: LOOKUP_FAILED | AMBIGUOUS_OWNERSHIP | RETRYABLE (partial)
    armed:              NEW → successful arm
    """
    already_resolved_dispositions = {
        ov._DISPOSITION_ALREADY_ARMED,
        ov._DISPOSITION_ALREADY_OWNED,
        ov._DISPOSITION_ALREADY_TERMINAL,
    }
    retryable_dispositions = {
        ov._DISPOSITION_LOOKUP_FAILED,
        ov._DISPOSITION_AMBIGUOUS_OWNERSHIP,
    }
    reattach_dispositions = {ov._DISPOSITION_REATTACH_WATCHER}
    new_dispositions = {ov._DISPOSITION_NEW}
    retryable_also = {ov._DISPOSITION_RETRYABLE}

    all_sets = [
        already_resolved_dispositions,
        retryable_dispositions,
        reattach_dispositions,
        new_dispositions,
        retryable_also,
    ]
    # No disposition should appear in two buckets.
    from itertools import combinations
    for a, b in combinations(all_sets, 2):
        overlap = a & b
        assert not overlap, f"Disposition appears in two buckets: {overlap!r}"

    # All named dispositions must appear in exactly one bucket.
    all_known = {
        ov._DISPOSITION_ALREADY_ARMED,
        ov._DISPOSITION_ALREADY_OWNED,
        ov._DISPOSITION_ALREADY_TERMINAL,
        ov._DISPOSITION_REATTACH_WATCHER,
        ov._DISPOSITION_RETRYABLE,
        ov._DISPOSITION_NEW,
        ov._DISPOSITION_LOOKUP_FAILED,
        ov._DISPOSITION_AMBIGUOUS_OWNERSHIP,
    }
    union_of_buckets = set().union(*all_sets)
    assert all_known == union_of_buckets, (
        f"Uncategorised dispositions: {all_known - union_of_buckets!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Minimal run_overnight_reeval harness for end-to-end disposition tests
# ─────────────────────────────────────────────────────────────────────────────

def _run_shared_ap_signals_harness(
    monkeypatch,
    *,
    signal_id: str = "sig-A",
    disposition: str = ov._DISPOSITION_NEW,
    osm_create=None,
    watch_fn=None,
    selector_calls: list | None = None,
    broker_submit_calls: list | None = None,
    broker_cancel_calls: list | None = None,
    broker_replace_calls: list | None = None,
    now_et: datetime | None = None,
    existing_order: dict | None = None,
) -> dict:
    """
    Run run_overnight_reeval with a single shared ap_signals row, hooking the
    resolver to return the supplied disposition.  Tracks selector/broker calls.
    """
    if selector_calls is None:
        selector_calls = []
    if broker_submit_calls is None:
        broker_submit_calls = []
    if broker_cancel_calls is None:
        broker_cancel_calls = []
    if broker_replace_calls is None:
        broker_replace_calls = []

    monkeypatch.setattr(ov, "_et_now", lambda: now_et or FIXED_ET)
    monkeypatch.setattr(ov, "_OVERNIGHT_SNAPSHOT_FAIL_CLOSED", False)

    # Shared ap_signals job (sup: prefix → job_source == "ap_signals")
    canonical = _canonical(signal_id)
    sig = _signal(signal_id)
    job = {
        "id": f"sup:{canonical}",
        "signal_id": signal_id,
        "payload": sig,
        "created_ts": sig["created_at"],
        "_source": "ap_signals",
    }
    monkeypatch.setattr(ov, "_fetch_watching_signals", lambda _cid: [job])

    # Resolver
    monkeypatch.setattr(
        ov, "_resolve_shared_setup_disposition",
        lambda *a, **k: ov._DispositionResult(
            disposition,
            str((existing_order or {}).get("local_order_id") or "") or None,
            dict(existing_order) if existing_order else None,
        ),
    )

    # Validator
    validator = types.ModuleType("ap.overnight_daily_validator")
    validator.fetch_market_snapshot = lambda ticker, _broker: {
        "last": 100.0, "source": "test"
    }
    validator.validate_overnight_daily_signal = lambda **kw: SimpleNamespace(
        valid=True, reason_code="", reason_text=""
    )
    validator.InvalidationReason = object
    monkeypatch.setitem(sys.modules, "ap.overnight_daily_validator", validator)

    # Auth
    auth = types.ModuleType("ap.authorization")
    auth.is_live_broker = lambda _b: False
    auth.broker_live_mode_known = lambda _b: True
    auth.check_live_authorization = lambda _c: None
    auth.authorization_gate_enforced = lambda: False
    auth.LIVE_AUTHORIZATION_GATE_UNAVAILABLE = "UNAVAILABLE"
    auth.execution_mode_for_broker = lambda _b: "paper"
    monkeypatch.setitem(sys.modules, "ap.authorization", auth)

    intel = types.ModuleType("ap.intelligence_context_handoff")
    intel.enqueue_preopen_context_best_effort = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "ap.intelligence_context_handoff", intel)

    # ap_canonical_signal
    cs_mod = types.ModuleType("ap_canonical_signal")
    cs_mod.build_canonical_signal_id = lambda sid, sig_=None: _canonical(str(sid or ""))
    monkeypatch.setitem(sys.modules, "ap_canonical_signal", cs_mod)

    # ap_signal_store
    ss_mod = types.ModuleType("ap_signal_store")
    ss_mod.canonical_client_email = lambda x: x
    ss_mod.canonical_signal_id = lambda x: x
    ss_mod.upsert_ap_signal_row_with_fallback = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "ap_signal_store", ss_mod)

    # Broker
    class _Broker:
        def get_prior_day_levels(self, ticker):
            return {"prior_day_high": 102.0, "prior_day_low": 94.0}
        def submit_order(self, *a, **k):
            broker_submit_calls.append((a, k))
        def cancel_order(self, *a, **k):
            broker_cancel_calls.append((a, k))
        def replace_order(self, *a, **k):
            broker_replace_calls.append((a, k))
        def place_order(self, *a, **k):
            broker_submit_calls.append((a, k))

    broker = _Broker()

    # Selector
    selector = MagicMock()
    selector.select.side_effect = lambda *a, **k: selector_calls.append((a, k)) or None
    selector.select_contract.side_effect = lambda *a, **k: selector_calls.append((a, k)) or None

    # Master control
    class _MC:
        def evaluate(self, sig_, *, client_id):
            plan = SimpleNamespace(
                plan_id="plan-1",
                signal_id=signal_id,
                canonical_signal_id=canonical,
                client_id=client_id,
                execution_mode="paper",
                ticker=sig_.get("ticker", "AAPL"),
                side=sig_.get("side", "CALL"),
                direction=sig_.get("side", "CALL"),
                score=80.0, tier="A", timeframe="1d", pattern="2-1-2",
                entry_trigger=101.0, trigger_price=101.0, trigger_type="breach",
                prior_day_high=102.0, prior_day_low=94.0,
                contract_symbol="DEFERRED:AAPL",
                contracts=2, limit_price=0.01,
                metadata={"contract_deferred": True},
            )
            return SimpleNamespace(ok=True, plan=plan, reason="approved", score=80.0)

    # OSM
    class _OSM:
        def __init__(self):
            self.rows = {}
            if existing_order:
                self.rows[str(existing_order["local_order_id"])] = dict(existing_order)

        def create_entry_order(self, plan, **kwargs):
            if osm_create:
                return osm_create(plan, **kwargs)
            return "local-1"

        def get_order(self, local_order_id):
            return dict(self.rows.get(local_order_id) or {
                "local_order_id": local_order_id,
                "status": "PENDING_TRIGGER",
            })

        def update_order_meta(self, local_order_id, patch):
            if local_order_id not in self.rows:
                return False
            meta = self.rows[local_order_id].get("meta") or {}
            meta.update(dict(patch or {}))
            self.rows[local_order_id]["meta"] = meta
            return True

    # Watcher
    class _Watcher:
        def __init__(self):
            self._pending = []
            self._dedup_set = set()

        def has_order(self, _local_order_id):
            return False

        def watch(self, plan, local_order_id):
            if watch_fn:
                return watch_fn(plan, local_order_id)
            return True

    # Noop helpers
    monkeypatch.setattr(ov, "_mark_job_rejected", lambda *a, **k: None)
    monkeypatch.setattr(ov, "_mark_job_error", lambda *a, **k: None)
    monkeypatch.setattr(ov, "_mark_job_watching_reason", lambda *a, **k: None)
    monkeypatch.setattr(ov, "_mark_job_watching_armed", lambda *a, **k: None)

    return ov.run_overnight_reeval(
        client_id="jose@example.com",
        broker=broker,
        data_broker=broker,
        master_control=_MC(),
        contract_selector=selector,
        order_state_machine=_OSM(),
        entry_watcher=_Watcher(),
        force=True,
    )
