"""P0 tests for PR #323 Seam 2: trigger-age vs recovery-age timing semantics.

MISMATCH BEING FIXED:
  * watcher polling = 15 s
  * two confirmations required
  * selector retries default ≈ 20 s × 3 attempts = 60 s
  * recovery window ≈ 300 s
  * old trigger-age gate default = 120 s → a legitimate transient-quote recovery
    that takes 90 s would be rejected by the gate even though the underlying
    has been freshly reconfirmed.

AMENDMENT:
  check_trigger_age_gate gains last_confirmed_trigger_at.  When a fresh
  underlying reconfirmation is obtained (within the current watcher pass or
  recovery pass), the age limit applies to that timestamp, not the original
  breach timestamp.  The limit itself is unchanged.

Tests A–G mirror the specification precisely.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ap.live_submit_gates import check_trigger_age_gate, GateOutcome


def test_production_final_gate_persists_and_passes_fresh_confirmation(monkeypatch):
    """The actual execution callback supplies the exact durable confirmation."""
    import sys
    import types
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    import ap_execution_core as core_mod
    import ap.live_submit_gates as gates_mod

    now = datetime.now(timezone.utc)
    crossed = (now - timedelta(seconds=150)).isoformat()
    row = {
        "local_order_id": "oid-live-confirm", "client_id": "client@example.com",
        "execution_mode": "live", "kind": "ENTRY", "status": "PENDING_TRIGGER",
        "contract": "SPY260717C00600000", "qty": 1, "limit_price": 1.25,
        "reserved_cost": 125.0, "broker_order_id": None, "submitted_ts": None,
        "meta": {"trigger_crossed_at": crossed},
    }
    plan = SimpleNamespace(
        contract_symbol=row["contract"], execution_price_per_share=1.25,
        ask=1.25, mid=1.20, affordable_contracts=1, premium_per_contract=125.0,
        contracts=1, limit_price=1.25, side="CALL", direction="CALL",
        execution_mode="live", client_id=row["client_id"], signal_id="sig-live-confirm",
        trigger_price=600.0, stop_underlying=595.0, target_underlying=610.0,
        metadata={"queue_id": 11},
    )
    watched = SimpleNamespace(
        signal={"ticker": "SPY", "side": "CALL", "entry_price": 600.0,
                "stop_price": 595.0, "target_price": 610.0,
                "signal_id": plan.signal_id, "local_order_id": row["local_order_id"],
                "client_id": row["client_id"], "execution_mode": "live"},
        trigger_price=600.0, ticker="SPY", trigger_crossed_at=datetime.fromisoformat(crossed),
    )
    core = core_mod.APExecutionCore.__new__(core_mod.APExecutionCore)
    core.paper = False
    core.mode = core.execution_mode = "LIVE"
    core.client_id = core.client_email = row["client_id"]
    core.broker = SimpleNamespace(
        cfg=SimpleNamespace(base_url="https://api.tradier.com"),
        get_quote=lambda _ticker: {"bid": 601.0, "ask": 601.2, "quote_age_ms": 25, "source": "test"},
    )
    core.store = MagicMock()
    core.contract_selector = MagicMock()
    core._breach_risk_check = MagicMock(return_value=True)
    core._recover_plan_for_revalidation = MagicMock(return_value=plan)
    core._refresh_hydrated_prebreach_plan = MagicMock(return_value=False)
    core._cleanup_pending_entry_order = types.MethodType(core_mod.APExecutionCore._cleanup_pending_entry_order, core)
    osm = MagicMock(client_id=row["client_id"], execution_mode="live")
    osm.get_order.side_effect = lambda _oid: row
    def _update(_oid, patch):
        row["meta"].update(patch)
        return True
    osm.update_order_meta.side_effect = _update
    osm.submit_existing_entry.return_value = {"ok": True, "broker_order_id": "TR-1"}
    core.order_state_machine = osm

    fake_execution = types.ModuleType("ap.execution")
    fake_execution._refresh_ask_at_submit = lambda *_a, **_k: (1.25, 5, True, "", {
        "submit_bid": 1.20, "submit_ask": 1.25, "submit_last": 1.23,
        "submit_mid": 1.225, "spread_pct": 0.04,
    })
    monkeypatch.setitem(sys.modules, "ap.execution", fake_execution)
    monkeypatch.setenv("LIVE_CONFIRMATION_REQUIRED", "0")
    monkeypatch.setenv("ENTRY_CUTOFF_ET_HHMM", "2359")
    captured = {}
    real_gate = gates_mod.check_trigger_age_gate
    def _capture_gate(**kwargs):
        captured.update(kwargs)
        return real_gate(**kwargs)
    monkeypatch.setattr(gates_mod, "check_trigger_age_gate", _capture_gate)

    core_mod.APExecutionCore._on_entry_trigger(core, watched)

    assert captured["trigger_crossed_at"] == crossed
    assert captured["last_confirmed_trigger_at"] == row["meta"]["last_confirmed_trigger_at"]
    assert datetime.fromisoformat(captured["last_confirmed_trigger_at"]) > datetime.fromisoformat(crossed)
    osm.submit_existing_entry.assert_called_once()


def _now():
    return datetime.now(timezone.utc)


def _iso(dt):
    return dt.isoformat()


def _ago(seconds):
    return _now() - timedelta(seconds=seconds)


def _from_now(seconds):
    return _now() + timedelta(seconds=seconds)


# ═══════════════════════════════════════════════════════════════════════
# Test A: transient zero quotes; retry within budget; fresh reconfirm; PASS
# ═══════════════════════════════════════════════════════════════════════


def test_A_recovery_within_budget_fresh_reconfirm_passes():
    """First selector attempt returns transient zero quotes; retry succeeds
    within the recovery budget; fresh trigger reconfirmation (30 s ago)
    passes when the old original breach was 90 s ago (which would fail with
    the old 120 s gate applied to the original breach time)."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(90)),          # 90 s ago — old gate would BLOCK at 120 s
        last_confirmed_trigger_at=_iso(_ago(30)),   # fresh: 30 s ago — within 120 s → PASS
        execution_mode="live",
        max_age_seconds=120,
    )
    assert result.passed, f"Expected PASS but got: {result.detail}"
    assert result.audit["effective_anchor"] == "last_confirmed_trigger_at"


def test_A_recovery_uses_fresh_anchor_not_original_breach():
    """The audit must record that last_confirmed_trigger_at was the effective anchor."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(150)),
        last_confirmed_trigger_at=_iso(_ago(10)),
        execution_mode="live",
        max_age_seconds=120,
    )
    assert result.passed
    assert result.audit["effective_anchor"] == "last_confirmed_trigger_at"
    # Original breach age is recorded for diagnostics even when not the anchor
    assert "original_breach_age_seconds" in result.audit


# ═══════════════════════════════════════════════════════════════════════
# Test B: succeeds after old 120-s boundary but before recovery deadline
# ═══════════════════════════════════════════════════════════════════════


def test_B_succeeds_after_old_boundary_with_fresh_reconfirm():
    """Same path as A: original breach was 130 s ago (past old 120 s limit),
    fresh confirmation was 45 s ago (within limit) → broker POST occurs."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(130)),
        last_confirmed_trigger_at=_iso(_ago(45)),
        execution_mode="live",
        max_age_seconds=120,
    )
    assert result.passed, result.detail


# ═══════════════════════════════════════════════════════════════════════
# Test C: price reverses during recovery → zero broker POST
# ═══════════════════════════════════════════════════════════════════════


def test_C_without_fresh_reconfirm_stale_original_blocks():
    """When no fresh reconfirmation is provided and the original breach
    was 130 s ago, the gate fails — price may have reversed."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(130)),
        last_confirmed_trigger_at=None,     # no fresh confirmation
        execution_mode="live",
        max_age_seconds=120,
    )
    assert not result.passed
    assert result.reason_code == GateOutcome.STALE_TRIGGER_BREACH


def test_C_fresh_confirm_older_than_limit_also_blocks():
    """A fresh confirmation that is itself stale (> max_age) does not help."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(200)),
        last_confirmed_trigger_at=_iso(_ago(130)),
        execution_mode="live",
        max_age_seconds=120,
    )
    assert not result.passed


# ═══════════════════════════════════════════════════════════════════════
# Test D: last_confirmed_trigger_at in the future → use original breach
# ═══════════════════════════════════════════════════════════════════════


def test_D_future_last_confirmed_falls_back_to_original_breach():
    """A last_confirmed_trigger_at in the future is a clock-skew signal.
    The gate falls back to trigger_crossed_at as anchor; if that is also
    stale the gate blocks."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(130)),
        last_confirmed_trigger_at=_iso(_from_now(5)),   # 5 s in future → invalid
        execution_mode="live",
        max_age_seconds=120,
    )
    # Falls back to original breach (130 s) → blocked
    assert not result.passed
    assert result.audit["effective_anchor"] == "trigger_crossed_at"


def test_D_future_last_confirmed_but_original_within_limit_passes():
    """If original breach is within limit but last_confirmed is in the
    future, gate still passes using original breach as fallback anchor."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(50)),              # 50 s → within 120 s
        last_confirmed_trigger_at=_iso(_from_now(10)),  # invalid
        execution_mode="live",
        max_age_seconds=120,
    )
    assert result.passed
    assert result.audit["effective_anchor"] == "trigger_crossed_at"


# ═══════════════════════════════════════════════════════════════════════
# Test E: last_confirmed earlier than original breach → regression guard
# ═══════════════════════════════════════════════════════════════════════


def test_E_last_confirmed_earlier_than_breach_uses_breach():
    """A last_confirmed_trigger_at that is BEFORE trigger_crossed_at is
    logically impossible and is ignored (regression guard).  The gate
    uses trigger_crossed_at instead."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(50)),              # 50 s ago
        last_confirmed_trigger_at=_iso(_ago(100)),      # 100 s ago — BEFORE original
        execution_mode="live",
        max_age_seconds=120,
    )
    # Should use trigger_crossed_at (50 s) → PASS
    assert result.passed
    assert result.audit["effective_anchor"] == "trigger_crossed_at"


# ═══════════════════════════════════════════════════════════════════════
# Test F: missing trigger_crossed_at → fail closed
# ═══════════════════════════════════════════════════════════════════════


def test_F_missing_trigger_crossed_at_fails_closed_live():
    result = check_trigger_age_gate(
        trigger_crossed_at=None,
        last_confirmed_trigger_at=_iso(_ago(10)),
        execution_mode="live",
        max_age_seconds=120,
    )
    # last_confirmed_trigger_at cannot substitute for missing original —
    # without the original we cannot compute the anchor correctly.
    # LIVE must fail closed.
    assert not result.passed


def test_F_missing_both_fails_closed_live():
    result = check_trigger_age_gate(
        trigger_crossed_at=None,
        last_confirmed_trigger_at=None,
        execution_mode="live",
    )
    assert not result.passed
    assert result.reason_code == GateOutcome.STALE_TRIGGER_BREACH


def test_F_missing_both_passes_paper():
    """Paper mode passes with a warning when both timestamps are absent."""
    result = check_trigger_age_gate(
        trigger_crossed_at=None,
        last_confirmed_trigger_at=None,
        execution_mode="paper",
    )
    assert result.passed


# ═══════════════════════════════════════════════════════════════════════
# Test G: backward compatibility — gate without last_confirmed
# ═══════════════════════════════════════════════════════════════════════


def test_G_backward_compat_no_last_confirmed_within_limit():
    """Existing callers that don't supply last_confirmed_trigger_at work unchanged."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(30)),
        execution_mode="live",
        max_age_seconds=120,
    )
    assert result.passed
    assert result.audit["effective_anchor"] == "trigger_crossed_at"


def test_G_backward_compat_no_last_confirmed_over_limit():
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(130)),
        execution_mode="live",
        max_age_seconds=120,
    )
    assert not result.passed


# ═══════════════════════════════════════════════════════════════════════
# Audit field completeness
# ═══════════════════════════════════════════════════════════════════════


def test_audit_includes_all_timing_fields():
    """The gate audit must expose all Seam 2 timing fields for diagnostics."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(90)),
        last_confirmed_trigger_at=_iso(_ago(20)),
        execution_mode="live",
        max_age_seconds=120,
    )
    audit = result.audit
    assert "trigger_crossed_at" in audit
    assert "last_confirmed_trigger_at" in audit
    assert "effective_anchor" in audit
    assert "age_seconds" in audit
    assert "original_breach_age_seconds" in audit


def test_audit_records_original_breach_age_when_fresh_anchor_used():
    """When last_confirmed is the effective anchor, original_breach_age_seconds
    must still appear in audit for forensic attribution."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(200)),
        last_confirmed_trigger_at=_iso(_ago(10)),
        execution_mode="live",
        max_age_seconds=120,
    )
    assert result.audit["effective_anchor"] == "last_confirmed_trigger_at"
    assert "original_breach_age_seconds" in result.audit
    assert result.audit["original_breach_age_seconds"] > 120
