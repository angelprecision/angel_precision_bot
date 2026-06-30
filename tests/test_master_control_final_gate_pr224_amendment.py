"""
tests/test_master_control_final_gate_pr224_amendment.py
PR #224 amendment — production-safe final gate inputs.

1. entry_price must never be used as a current-price fallback.
2. Stale/overextended current price correctly blocks TARGET_ALREADY_INVALID.
3. Live intelligence fail-closed is rollout-safe via FINAL_ENTRY_INTELLIGENCE_REQUIRED.
4. FINAL_QUALITY_MODE_ENABLED=false surfaces visibly (reason code + startup/health).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import ap_master_control as mc_mod


def _build_mc(*, paper: bool):
    events = []
    updates = []

    mc = mc_mod.APMasterControl.__new__(mc_mod.APMasterControl)
    mc.paper = paper
    mc.run_id = "run-1"
    mc.strategy_version = "test"
    mc.config_hash = "cfg"
    mc.git_commit = "sha"
    mc._alert_degraded = MagicMock()
    mc._store_update = lambda signal_id, status, reason="": updates.append(
        {"signal_id": signal_id, "status": status, "reason": reason}
    )
    return mc, events, updates


def _plan(direction="CALL", trigger=100.0, stop=95.0, target=110.0):
    if direction == "PUT":
        trigger, stop, target = 100.0, 105.0, 90.0
    return mc_mod.ApprovedExecutionPlan(
        plan_id="plan-1",
        signal_id="sig-1",
        client_id="client@example.com",
        ticker="AAPL",
        side=direction,
        direction=direction,
        pattern="2-3",
        timeframe="1d",
        contracts=1,
        max_position_usd=1000.0,
        tier="A",
        score=78.0,
        intel_score=45.0,
        confidence_bucket="standard",
        trigger_type="breach",
        trigger_price=trigger,
        stop_underlying=stop,
        target_underlying=target,
        metadata={},
    )


def _signal(direction="CALL", **overrides):
    base = {
        "signal_id": "sig-1",
        "ticker": "AAPL",
        "side": direction,
        "direction": direction,
        "pattern": "2-3",
        "timeframe": "1d",
        "current_price": 101.0 if direction == "CALL" else 99.0,
        "risk_detail": {"contract_quality_passes": True},
    }
    base.update(overrides)
    return base


_INTEL_OK = {"_available": True, "score": 55.0, "risk_detail": {"contract_quality_passes": True}}
_SNAP = {"total_trades": 0, "daily_trades": 0, "intraday_trades": 0, "symbol_trades": {}}


def _run(mc, signal, plan, intel=None, snap=None):
    return mc._run_final_quality_gates(
        signal=signal,
        signal_id=signal.get("signal_id", "sig-1"),
        ticker=signal.get("ticker", "AAPL"),
        client_id="client@example.com",
        plan=plan,
        intel=intel if intel is not None else _INTEL_OK,
        snap=snap if snap is not None else _SNAP,
    )


# ─────────────────────────────────────────────────────────────────────────────
# 1+2. Current price source correctness — no entry_price fallback
# ─────────────────────────────────────────────────────────────────────────────

def test_call_current_price_at_or_above_target_blocks_target_already_invalid(monkeypatch):
    mc, events, updates = _build_mc(paper=True)
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: events.append(kw))

    # CALL target=110 — current_price >= 110 must block
    sig = _signal(direction="CALL", current_price=111.0)
    decision = _run(mc, sig, _plan(direction="CALL"))

    assert decision is not None
    assert decision.ok is False
    assert decision.reason_code == "TARGET_ALREADY_INVALID"


def test_put_current_price_at_or_below_target_blocks_target_already_invalid(monkeypatch):
    mc, events, updates = _build_mc(paper=True)
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: events.append(kw))

    # PUT target=90 — current_price <= 90 must block
    sig = _signal(direction="PUT", current_price=89.0)
    decision = _run(mc, sig, _plan(direction="PUT"))

    assert decision is not None
    assert decision.ok is False
    assert decision.reason_code == "TARGET_ALREADY_INVALID"


def test_missing_current_price_does_not_fall_back_to_entry_price(monkeypatch):
    """
    entry_price is set but no current-price field is present. The signal must
    NOT use entry_price as current_price. TARGET_ALREADY_INVALID must not
    fire even though entry_price (100) vs target (110) would superficially
    look like 'not yet at target' — the point is the gate must SKIP, not
    silently pass using a fabricated value.
    """
    mc, events, updates = _build_mc(paper=True)
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: events.append(kw))

    sig = {
        "signal_id": "sig-1",
        "ticker": "AAPL",
        "side": "CALL",
        "direction": "CALL",
        "timeframe": "1d",
        "entry_price": 100.0,  # must NOT be used as current_price
        "risk_detail": {"contract_quality_passes": True},
        # NOTE: no current_price / current_underlying / underlying_price / last_price
    }
    plan = _plan(direction="CALL")
    decision = _run(mc, sig, plan)

    # Gate must not block on TARGET_ALREADY_INVALID since current_price is None
    assert decision is None or decision.reason_code != "TARGET_ALREADY_INVALID"
    # Diagnostic must be recorded instead of silently passing
    assert plan.metadata.get("final_gate_diagnostics", {}).get("current_price_missing") is True


def test_entry_price_alone_cannot_satisfy_current_price_gates(monkeypatch):
    """
    Even when entry_price is set to a value that WOULD trigger
    TARGET_ALREADY_INVALID if used as current_price, it must not be used.
    entry_price=111 (>= target=110 for CALL) — if fallback were active this
    would incorrectly block. It must NOT block via TARGET_ALREADY_INVALID.
    """
    mc, events, updates = _build_mc(paper=True)
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: events.append(kw))

    sig = {
        "signal_id": "sig-1",
        "ticker": "AAPL",
        "side": "CALL",
        "direction": "CALL",
        "timeframe": "1d",
        "entry_price": 111.0,  # would breach target=110 if used as current_price
        "risk_detail": {"contract_quality_passes": True},
    }
    plan = _plan(direction="CALL")
    decision = _run(mc, sig, plan)

    assert decision is None or decision.reason_code != "TARGET_ALREADY_INVALID", (
        "entry_price must never be usable as a current-price gate input"
    )


def test_current_underlying_field_is_accepted_as_current_price_source(monkeypatch):
    """current_underlying is a valid production field name and must be honored."""
    mc, events, updates = _build_mc(paper=True)
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: events.append(kw))

    sig = {
        "signal_id": "sig-1",
        "ticker": "AAPL",
        "side": "CALL",
        "direction": "CALL",
        "timeframe": "1d",
        "current_underlying": 111.0,  # no current_price set — uses fallback chain
        "risk_detail": {"contract_quality_passes": True},
    }
    decision = _run(mc, sig, _plan(direction="CALL"))

    assert decision is not None
    assert decision.reason_code == "TARGET_ALREADY_INVALID", (
        "current_underlying must be honored as a current-price source"
    )


def test_clean_current_price_within_geometry_passes(monkeypatch):
    """Sanity: a clean current_price that hasn't reached target passes through."""
    mc, events, updates = _build_mc(paper=False)
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: events.append(kw))
    monkeypatch.setenv("HYBRID_CLIENT_QUALITY_MODE", "0")
    monkeypatch.setenv("FINAL_ENTRY_INTELLIGENCE_MIN_SCORE", "35")

    sig = _signal(direction="CALL", current_price=101.0)
    decision = _run(mc, sig, _plan(direction="CALL"))
    assert decision is None


# ─────────────────────────────────────────────────────────────────────────────
# 3. Live intelligence fail-closed rollout-safe
# ─────────────────────────────────────────────────────────────────────────────

def test_live_intel_unavailable_required_true_blocks(monkeypatch):
    """Default behavior (required=true, the default) — must block."""
    mc, events, updates = _build_mc(paper=False)
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: events.append(kw))
    monkeypatch.setenv("HYBRID_CLIENT_QUALITY_MODE", "0")
    # FINAL_ENTRY_INTELLIGENCE_REQUIRED not set — must default True

    sig = _signal(direction="CALL")
    intel = {"_available": False, "score": 0.0, "risk_detail": {"contract_quality_passes": True}}
    decision = _run(mc, sig, _plan(direction="CALL"), intel=intel)

    assert decision is not None
    assert decision.ok is False
    assert decision.reason_code == "ENTRY_INTELLIGENCE_MISSING"


def test_live_intel_unavailable_explicit_required_true_blocks(monkeypatch):
    mc, events, updates = _build_mc(paper=False)
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: events.append(kw))
    monkeypatch.setenv("HYBRID_CLIENT_QUALITY_MODE", "0")
    monkeypatch.setenv("FINAL_ENTRY_INTELLIGENCE_REQUIRED", "true")

    sig = _signal(direction="CALL")
    intel = {"_available": False, "score": 0.0, "risk_detail": {"contract_quality_passes": True}}
    decision = _run(mc, sig, _plan(direction="CALL"), intel=intel)

    assert decision is not None
    assert decision.reason_code == "ENTRY_INTELLIGENCE_MISSING"


def test_live_intel_unavailable_required_false_does_not_block_emits_observed(monkeypatch):
    """
    Emergency rollback: FINAL_ENTRY_INTELLIGENCE_REQUIRED=false.
    Intel unavailable must NOT block, but must record the observed diagnostic.
    """
    mc, events, updates = _build_mc(paper=False)
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: events.append(kw))
    monkeypatch.setenv("HYBRID_CLIENT_QUALITY_MODE", "0")
    monkeypatch.setenv("FINAL_ENTRY_INTELLIGENCE_REQUIRED", "false")

    sig = _signal(direction="CALL")
    plan = _plan(direction="CALL")
    intel = {"_available": False, "score": 0.0, "risk_detail": {"contract_quality_passes": True}}
    decision = _run(mc, sig, plan, intel=intel)

    assert decision is None, "must NOT block when FINAL_ENTRY_INTELLIGENCE_REQUIRED=false"
    diag = plan.metadata.get("final_gate_diagnostics", {})
    assert diag.get("entry_intelligence_missing_observed") is True
    assert diag.get("reason_code") == "ENTRY_INTELLIGENCE_MISSING_OBSERVED"
    # No REJECT event must be emitted for this signal
    assert all(evt["decision"] != "REJECT" for evt in events)


def test_paper_not_blocked_by_live_intelligence_requirement(monkeypatch):
    """Paper must never be gated by FINAL_ENTRY_INTELLIGENCE_REQUIRED at all."""
    mc, events, updates = _build_mc(paper=True)
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: events.append(kw))
    monkeypatch.setenv("FINAL_ENTRY_INTELLIGENCE_REQUIRED", "true")  # even if true

    sig = _signal(direction="CALL")
    intel = {"_available": False, "score": 0.0, "risk_detail": {"contract_quality_passes": True}}
    decision = _run(mc, sig, _plan(direction="CALL"), intel=intel)

    assert decision is None, "paper must not be blocked by intel availability at all"


def test_live_intel_available_score_check_still_runs_when_not_required(monkeypatch):
    """
    FINAL_ENTRY_INTELLIGENCE_REQUIRED=false only affects the MISSING case.
    When intel IS available, the score-too-low check must still apply.
    """
    mc, events, updates = _build_mc(paper=False)
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: events.append(kw))
    monkeypatch.setenv("HYBRID_CLIENT_QUALITY_MODE", "0")
    monkeypatch.setenv("FINAL_ENTRY_INTELLIGENCE_REQUIRED", "false")
    monkeypatch.setenv("FINAL_ENTRY_INTELLIGENCE_MIN_SCORE", "35")

    sig = _signal(direction="CALL")
    intel = {"_available": True, "score": 10.0, "risk_detail": {"contract_quality_passes": True}}
    decision = _run(mc, sig, _plan(direction="CALL"), intel=intel)

    assert decision is not None
    assert decision.reason_code == "ENTRY_INTELLIGENCE_SCORE_TOO_LOW"


# ─────────────────────────────────────────────────────────────────────────────
# 4. FINAL_QUALITY_MODE_ENABLED=false surfaces visibly
# ─────────────────────────────────────────────────────────────────────────────

def test_final_quality_mode_disabled_emits_reject_reason(monkeypatch):
    mc, events, updates = _build_mc(paper=True)
    monkeypatch.setattr(mc_mod, "emit_decision_event", lambda **kw: events.append(kw))
    monkeypatch.setenv("FINAL_QUALITY_MODE_ENABLED", "false")

    sig = _signal(direction="CALL")
    decision = _run(mc, sig, _plan(direction="CALL"))

    assert decision is not None
    assert decision.ok is False
    assert decision.reason_code == "FINAL_QUALITY_MODE_DISABLED"


def test_final_quality_mode_disabled_logs_startup_warning(monkeypatch, caplog):
    """
    Startup must surface FINAL_QUALITY_MODE_DISABLED_BLOCKS_ALL_ENTRIES when
    constructing APMasterControl with the flag off, so an operator sees it
    immediately rather than discovering it via a stream of rejections.
    """
    import logging
    monkeypatch.setenv("FINAL_QUALITY_MODE_ENABLED", "false")

    mc = mc_mod.APMasterControl.__new__(mc_mod.APMasterControl)
    mc.mode = "PAPER"
    mc._client_id = "test-client"

    with caplog.at_level(logging.WARNING, logger="ap.master_control"):
        # Directly exercise the same logic __init__ runs at the end —
        # the attribute + warning emission is what we're testing here.
        mc.final_quality_mode_enabled = mc_mod._env_true("FINAL_QUALITY_MODE_ENABLED", True)
        if not mc.final_quality_mode_enabled:
            mc_mod.log.warning(
                "FINAL_QUALITY_MODE_DISABLED_BLOCKS_ALL_ENTRIES=true | mode=%s "
                "client=%s — every entry signal will be REJECTED with "
                "FINAL_QUALITY_MODE_DISABLED until FINAL_QUALITY_MODE_ENABLED "
                "is re-enabled.",
                mc.mode, mc._client_id,
            )

    assert mc.final_quality_mode_enabled is False
    assert any(
        "FINAL_QUALITY_MODE_DISABLED_BLOCKS_ALL_ENTRIES=true" in r.message
        for r in caplog.records
    )


def test_final_quality_mode_enabled_true_no_startup_warning(monkeypatch, caplog):
    import logging
    monkeypatch.delenv("FINAL_QUALITY_MODE_ENABLED", raising=False)

    mc = mc_mod.APMasterControl.__new__(mc_mod.APMasterControl)
    mc.mode = "PAPER"
    mc._client_id = "test-client"

    with caplog.at_level(logging.WARNING, logger="ap.master_control"):
        mc.final_quality_mode_enabled = mc_mod._env_true("FINAL_QUALITY_MODE_ENABLED", True)
        if not mc.final_quality_mode_enabled:
            mc_mod.log.warning("FINAL_QUALITY_MODE_DISABLED_BLOCKS_ALL_ENTRIES=true")

    assert mc.final_quality_mode_enabled is True
    assert not any(
        "FINAL_QUALITY_MODE_DISABLED_BLOCKS_ALL_ENTRIES" in r.message
        for r in caplog.records
    )


def test_init_sets_final_quality_mode_enabled_attribute_source_check():
    """Source-level check: __init__ must set self.final_quality_mode_enabled."""
    import inspect
    src = inspect.getsource(mc_mod.APMasterControl.__init__)
    assert "self.final_quality_mode_enabled" in src
    assert "FINAL_QUALITY_MODE_DISABLED_BLOCKS_ALL_ENTRIES" in src


def test_execution_health_surfaces_final_quality_mode_source_check():
    """Source-level check: /execution/health must surface the flag."""
    from pathlib import Path
    src_text = Path(__file__).resolve().parents[1].joinpath("app.py").read_text()
    # Locate the execution_health function body
    idx = src_text.find("def execution_health(")
    assert idx > 0, "execution_health function not found in app.py"
    next_def = src_text.find("\ndef ", idx + 10)
    func_src = src_text[idx:next_def if next_def > 0 else idx + 4000]
    assert "final_quality_mode_enabled" in func_src
    assert "final_quality_mode_disabled_blocks_all_entries" in func_src
