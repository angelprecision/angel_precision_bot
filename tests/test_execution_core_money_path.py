"""
PR B — Pre-LIVE Execution-Core Money-Path Hardening
====================================================

Tests for the eight verified findings in the institutional audit of
ap_execution_core.py (audit dated 2026-05-25). Tests-first: every fix
landed only after its test was red on the audit-flagged code.

Coverage map (17 IDs + 1 invariant):

  FIX-1 Ghost fields declared on ManagedPosition (6)
    - _exit_submit_ts, _exit_attempts, _integrity_logged,
      _proof_staged, _proof_finalized, proof_logged are all real fields
    - Survive dataclasses.replace() roundtrip

  FIX-2 decision.suggested_limit as FIRST exit-price authority (3)
    - TRAIL decision with suggested_limit > 0 is passed to OSM
      submit_exit verbatim (NOT overridden with bid)
    - Ladder fallback fires only when suggested_limit <= 0 or missing
    - Market order ONLY on explicit emergency (HARD_STOP / EOD /
      SENTINEL); never just because urgency=IMMEDIATE

  FIX-3 Canonical mode derived BEFORE submodule construction (1)
    - APProofLogger is constructed with master_control.mode, not
      BOT_MODE

  FIX-4 master_control passed to APExitEngine constructor (1)
    - APExitEngine.__init__ accepts master_control kwarg
    - exit_eng.master_control is set immediately after construction

  FIX-5 Preflight gates (4)
    - start() raises if exit_eng.on_exit is None
    - start() raises if exit_eng.on_scale is None
    - start() raises if exit_eng.master_control is None
    - start() raises in LIVE if position_manager is None

  FIX-6 _record_intel_outcome moved to _finalize_proof (1)
    - Not called on exit-submit; called on broker fill confirmation
      with actual fill price (not staged estimate)

  FIX-7 Edge logger payload uses explicit allowlist (1)
    - No private/internal keys (_proof_staged, _submit_generation,
      pending_exit_local_order_id, etc.) leak through

  FIX-8 Env/config hardening (4)
    - AP_MODE/BOT_MODE conflict raises in __init__
    - SCORE_FLOOR_LIVE, SCORE_FLOOR_PAPER, CONTEXT_FLOOR_LIVE,
      CONTEXT_FLOOR_PAPER are env-overridable
    - BREAKEVEN_BAND_PCT is a single module-level constant

  INV  Structural invariant (1)
    - decision.suggested_limit is honored as first authority anywhere
      the execution core hands a limit to the OSM
"""
from __future__ import annotations

import inspect
import os
import re
import sys
import types
from dataclasses import fields as dc_fields, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_execution_core_money_path",
)
os.environ.setdefault("ENCRYPTION_KEY", "ap-exec-core-pr-b-2026")

if "supabase" not in sys.modules:
    _supa_stub = types.ModuleType("supabase")
    _supa_stub.create_client = lambda *a, **kw: None
    _supa_stub.Client = type("Client", (), {})
    sys.modules["supabase"] = _supa_stub


import ap_exit_engine  # noqa: E402
import ap_execution_core  # noqa: E402
from ap_exit_engine import ManagedPosition, ExitDecision, APExitEngine  # noqa: E402
from ap_execution_core import APExecutionCore  # noqa: E402


# ──────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────

def _new_position(**overrides) -> ManagedPosition:
    """Build a ManagedPosition and attach a `signal` dict the same way
    production code does (post-construction set, not a constructor arg).
    """
    signal_dict = overrides.pop(
        "signal",
        {
            "signal_id":  "sig-pr-b-1",
            "pattern":    "test",
            "timeframe":  "1d",
            "score":      80,
            "tier":       "A",
        },
    )
    defaults = dict(
        ticker="SPY",
        option_symbol="SPY260530C00500000",
        side="CALL",
        quantity=1,
        entry_price=1.00,
        underlying_entry=500.00,
        underlying_target=505.00,
        underlying_stop=499.00,
        position_id="pos-pr-b-1",
        client_id="alice@x.com",
        signal_id="sig-pr-b-1",
        current_option_price=1.20,
        current_bid=1.18,
        current_ask=1.22,
        current_underlying=502.00,
        quantity_remaining=1,
        peak_pnl_pct=0.20,
        opened_at=datetime.now(timezone.utc),
    )
    defaults.update(overrides)
    pos = ManagedPosition(**defaults)
    # Production code does pos.signal = sig (set on the instance dict);
    # mirror that here for fixture realism.
    pos.signal = signal_dict  # type: ignore[attr-defined]
    return pos


def _make_mc(mode="paper", max_positions=7):
    """A minimal master_control-shaped mock that the execution core accepts."""
    mc = MagicMock()
    mc.mode = mode
    mc.max_positions = max_positions
    mc._kill_switch_fn = lambda: False
    return mc


def _make_osm():
    osm = MagicMock()
    # submit_exit returns the "ok" shape execution core expects
    osm.submit_exit.return_value = {
        "ok": True,
        "local_order_id": "lo-exit-1",
        "broker_order_id": "br-exit-1",
        "error": None,
    }
    return osm


def _make_core(mode="paper", position_manager=None, master_control=None, env=None):
    """Build a real APExecutionCore with mock dependencies.

    env: dict of env overrides applied via os.environ during construction.
    """
    if env:
        original = {k: os.environ.get(k) for k in env}
        for k, v in env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    try:
        broker = MagicMock()
        osm = _make_osm()
        mc = master_control or _make_mc(mode=mode)
        core = APExecutionCore(
            broker=broker,
            supabase_client=None,
            email="alice@x.com",
            position_manager=position_manager,
            order_state_machine=osm,
            data_broker=broker,
            master_control=mc,
        )
        return core, osm, mc
    finally:
        if env:
            for k, v in original.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v


# ══════════════════════════════════════════════════════════════════
# FIX-1 — Ghost fields declared on ManagedPosition
# ══════════════════════════════════════════════════════════════════

_GHOST_FIELDS = (
    "_exit_submit_ts",
    "_exit_attempts",
    "_integrity_logged",
    "_proof_staged",
    "_proof_finalized",
    "proof_logged",
)


class TestFix1GhostFieldsDeclared:
    @pytest.mark.parametrize("name", _GHOST_FIELDS)
    def test_field_is_declared_on_dataclass(self, name):
        field_names = {f.name for f in dc_fields(ManagedPosition)}
        assert name in field_names, (
            f"{name} must be a declared field on ManagedPosition; "
            f"currently written via `# type: ignore[attr-defined]` "
            f"and lost on dataclass-replace / restart"
        )

    def test_ghost_fields_default_safely(self):
        pos = _new_position()
        assert pos._exit_submit_ts == 0.0
        assert pos._exit_attempts == 0
        assert pos._integrity_logged is False
        assert pos._proof_staged is None
        assert pos._proof_finalized is False
        assert pos.proof_logged is False

    def test_ghost_fields_survive_dataclass_replace(self):
        """A dataclasses.replace() must preserve stamped state."""
        pos = _new_position()
        pos._exit_submit_ts = 1234567890.5
        pos._exit_attempts = 3
        pos._proof_staged = {"ticker": "SPY", "exit_option_price": 1.50}
        pos._proof_finalized = True
        pos.proof_logged = True

        clone = replace(pos, current_option_price=1.30)
        assert clone._exit_submit_ts == 1234567890.5
        assert clone._exit_attempts == 3
        assert clone._proof_staged == {"ticker": "SPY", "exit_option_price": 1.50}
        assert clone._proof_finalized is True
        assert clone.proof_logged is True


# ══════════════════════════════════════════════════════════════════
# FIX-2 — decision.suggested_limit as FIRST exit-price authority
# ══════════════════════════════════════════════════════════════════

class TestFix2SuggestedLimitAuthority:
    def test_trail_decision_suggested_limit_passes_through_to_osm(self):
        """When decision.suggested_limit > 0 and urgency != IMMEDIATE,
        the execution core MUST pass suggested_limit to OSM submit_exit
        verbatim — not override it with bid.
        """
        core, osm, _ = _make_core()
        pos = _new_position()
        # Trail decision priced at (mid+bid)/2 = 1.20
        decision = ExitDecision(
            action="CLOSE_ALL",
            quantity=1,
            reason="RUNNER TRAIL — peak +30%, trailed back to +20%",
            urgency="HIGH",
            pnl_pct=0.20,
            reason_code="RUNNER_TRAIL",
            suggested_limit=1.20,  # exit engine priced at mid
        )

        core._on_position_close(pos, decision)

        assert osm.submit_exit.called, "OSM submit_exit must be called"
        kwargs = osm.submit_exit.call_args.kwargs
        assert kwargs["limit_price"] == 1.20, (
            f"execution core must honor decision.suggested_limit=1.20; "
            f"got limit_price={kwargs['limit_price']} "
            f"(this is the trail-exit spread leak)"
        )
        assert kwargs["order_type"] == "limit"

    def test_ladder_fallback_fires_only_when_suggested_limit_missing(self):
        """When decision.suggested_limit is 0 / unset, the existing
        bid-ladder is still allowed to fire (deferred refactor)."""
        core, osm, _ = _make_core()
        pos = _new_position(current_bid=1.18)
        decision = ExitDecision(
            action="CLOSE_ALL",
            quantity=1,
            reason="RUNNER TRAIL",
            urgency="HIGH",
            pnl_pct=0.20,
            reason_code="RUNNER_TRAIL",
            suggested_limit=0.0,  # exit engine did not price
        )

        core._on_position_close(pos, decision)

        assert osm.submit_exit.called
        # When suggested_limit is missing, fallback to bid is acceptable
        # (this is the deferred ladder; the TODO is recorded in source).
        assert osm.submit_exit.call_args.kwargs["limit_price"] == 1.18

    def test_market_order_only_on_explicit_emergency(self):
        """urgency=IMMEDIATE alone is NOT enough; must also be a true
        emergency code (HARD_STOP / EOD / SENTINEL)."""
        core, osm, _ = _make_core()
        pos = _new_position(current_bid=1.18)
        # IMMEDIATE urgency but NOT an emergency code (e.g. profit exit)
        decision = ExitDecision(
            action="CLOSE_ALL",
            quantity=1,
            reason="IMMEDIATE TAKE PROFIT — peak +30%",
            urgency="IMMEDIATE",
            pnl_pct=0.30,
            reason_code="IMMEDIATE_TP",
            suggested_limit=1.18,
        )

        core._on_position_close(pos, decision)

        assert osm.submit_exit.called
        kwargs = osm.submit_exit.call_args.kwargs
        # Must be a LIMIT order, not market (suggested_limit was honored)
        assert kwargs["order_type"] == "limit", (
            f"IMMEDIATE urgency on a profit-exit must NOT submit market; "
            f"got order_type={kwargs['order_type']}"
        )
        assert kwargs["limit_price"] == 1.18


# ══════════════════════════════════════════════════════════════════
# FIX-3 — Canonical mode derived BEFORE submodule construction
# ══════════════════════════════════════════════════════════════════

class TestFix3CanonicalModeBeforeSubmodules:
    def test_approoflogger_receives_canonical_mode(self):
        """The APProofLogger constructor must see mode='live' when
        master_control.mode='live', regardless of BOT_MODE env."""
        captured_modes: list[str] = []

        real_apl = ap_execution_core.APProofLogger

        def _capturing_apl(*args, **kwargs):
            captured_modes.append(kwargs.get("mode", "?"))
            return real_apl(*args, **kwargs)

        # Force BOT_MODE divergence from master_control.mode
        with patch.dict(os.environ, {"BOT_MODE": "PAPER", "AP_MODE": "PAPER"}):
            with patch.object(ap_execution_core, "APProofLogger", side_effect=_capturing_apl):
                mc = _make_mc(mode="live")
                core, _, _ = _make_core(master_control=mc)

        assert captured_modes, "APProofLogger must have been constructed"
        assert captured_modes[0] == "live", (
            f"APProofLogger first construction must use canonical "
            f"master_control.mode='live'; got {captured_modes[0]} "
            f"(this is the mode-derivation window bug)"
        )

    def test_apexitengine_constructed_after_canonical_mode(self):
        """When BOT_MODE=PAPER but master_control.mode=LIVE, the exit
        engine must be constructed in the LIVE-aware path."""
        # We assert via execution core's self.mode after construction;
        # if mode is canonical, self.mode == "LIVE" not "PAPER".
        with patch.dict(os.environ, {"BOT_MODE": "PAPER"}):
            mc = _make_mc(mode="live")
            core, _, _ = _make_core(master_control=mc)
        assert core.mode == "LIVE"
        assert core.paper is False


# ══════════════════════════════════════════════════════════════════
# FIX-4 — master_control passed to APExitEngine constructor
# ══════════════════════════════════════════════════════════════════

class TestFix4MasterControlInExitEngine:
    def test_apexitengine_init_accepts_master_control_kwarg(self):
        """APExitEngine.__init__ must accept master_control as a kwarg."""
        sig = inspect.signature(APExitEngine.__init__)
        assert "master_control" in sig.parameters, (
            "APExitEngine.__init__ must accept master_control kwarg so "
            "execution core can pass it at construction time"
        )

    def test_exit_engine_has_master_control_immediately(self):
        """After APExecutionCore construction, exit_eng.master_control
        must be set (no post-assignment window)."""
        core, _, mc = _make_core()
        assert core.exit_eng.master_control is mc


# ══════════════════════════════════════════════════════════════════
# FIX-5 — Preflight gates
# ══════════════════════════════════════════════════════════════════

class TestFix5PreflightGates:
    def test_start_raises_when_on_exit_not_wired(self):
        core, _, _ = _make_core()
        core.exit_eng.on_exit = None
        with pytest.raises(RuntimeError) as exc:
            core.start()
        assert "exit_eng_on_exit_not_wired" in str(exc.value)

    def test_start_raises_when_on_scale_not_wired(self):
        core, _, _ = _make_core()
        core.exit_eng.on_scale = None
        with pytest.raises(RuntimeError) as exc:
            core.start()
        assert "exit_eng_on_scale_not_wired" in str(exc.value)

    def test_start_raises_when_exit_eng_master_control_not_wired(self):
        core, _, _ = _make_core()
        core.exit_eng.master_control = None
        with pytest.raises(RuntimeError) as exc:
            core.start()
        assert "exit_eng_master_control_not_wired" in str(exc.value)

    def test_start_raises_in_live_when_position_manager_missing(self):
        """LIVE mode must not start without a position_manager."""
        mc = _make_mc(mode="live")
        core, _, _ = _make_core(master_control=mc, position_manager=None)
        with pytest.raises(RuntimeError) as exc:
            core.start()
        assert "position_manager_missing" in str(exc.value)


# ══════════════════════════════════════════════════════════════════
# FIX-6 — _record_intel_outcome moved to _finalize_proof
# ══════════════════════════════════════════════════════════════════

class TestFix6IntelOutcomeOnFill:
    def test_close_callback_returns_broker_identity_after_local_persistence_gap(self):
        """A broker-owned CLOSE_ALL result must reach the adoption seam."""
        core, osm, _ = _make_core()
        broker_owned_gap = {
            "ok": False,
            "local_order_id": "lo-close-gap-425",
            "broker_order_id": "br-close-gap-425",
            "status": "ERROR",
            "error": "exit_submitted_transition_failed_after_broker_accept",
            "split_brain": True,
        }
        osm.submit_exit.return_value = broker_owned_gap
        pos = _new_position(pending_exit_local_order_id="lo-close-gap-425")
        decision = ExitDecision(
            action="CLOSE_ALL",
            quantity=1,
            reason="HARD STOP",
            urgency="HIGH",
            reason_code="HARD_STOP",
            suggested_limit=1.18,
        )

        result = core._on_position_close(pos, decision)

        assert result == broker_owned_gap
        assert result["local_order_id"] == "lo-close-gap-425"
        assert result["broker_order_id"] == "br-close-gap-425"

    def test_record_intel_outcome_NOT_called_on_exit_submit(self):
        """_on_position_close (exit submit, not yet filled) must NOT
        call _record_intel_outcome — P/L is estimated at submit."""
        intel_mock = MagicMock()
        with patch.object(ap_execution_core, "_record_intel_outcome", intel_mock):
            core, _, _ = _make_core()
            pos = _new_position()
            decision = ExitDecision(
                action="CLOSE_ALL", quantity=1,
                reason="RUNNER TRAIL", urgency="HIGH",
                reason_code="RUNNER_TRAIL", suggested_limit=1.20,
            )
            core._on_position_close(pos, decision)
        assert not intel_mock.called, (
            "_record_intel_outcome must NOT be called at exit-submit; "
            "the P/L is estimated. It must run in _finalize_proof "
            "with actual broker fill."
        )

    def test_record_intel_outcome_called_in_finalize_proof_with_actual_fill(self):
        """_finalize_proof must call _record_intel_outcome with the
        ACTUAL broker fill price, not the staged estimate."""
        intel_mock = MagicMock()
        with patch.object(ap_execution_core, "_record_intel_outcome", intel_mock):
            core, _, _ = _make_core()
            # Stage a proof manually (as if exit submit happened)
            pos = _new_position()
            pos._proof_staged = {
                "ticker": "SPY",
                "pattern": "test",
                "side": "CALL",
                "timeframe": "1d",
                "score": 80,
                "tier": "A",
                "context_score": 0,
                "setup_status": "",
                "entry_trigger": 500.0,
                "entry_option_price": 1.00,
                "exit_option_price": 1.20,  # estimated submit price
                "underlying_entry": 500.0,
                "underlying_exit": 502.0,
                "contracts": 1,
                "exit_reason": "RUNNER TRAIL",
                "opt_pnl": 20.0,
                "win": True,
                "spread_pct": 0.0,
                "chain_grade": "",
                "opened_at": pos.opened_at,
                "synthetic_entry": False,
                "position_id": "pos-pr-b-1",
                "local_order_id": "lo-1",
                "signal": {"signal_id": "sig-pr-b-1"},
                "paper": True,
            }
            # Broker confirms fill at $1.18 (worse than $1.20 estimate)
            core._finalize_proof(pos, actual_fill_price=1.18)

        assert intel_mock.called, (
            "_record_intel_outcome must be called from _finalize_proof "
            "once broker-confirmed fill is known"
        )
        call_kwargs = intel_mock.call_args.kwargs
        # The pnl passed must reflect the ACTUAL fill ($1.18), not the
        # estimate ($1.20). Computed: (1.18 - 1.00) / 1.00 = 0.18
        assert abs(call_kwargs["pnl_pct"] - 0.18) < 0.001, (
            f"pnl_pct must be computed from ACTUAL fill ($1.18 -> 18%); "
            f"got pnl_pct={call_kwargs['pnl_pct']}"
        )


# ══════════════════════════════════════════════════════════════════
# FIX-7 — Edge logger payload uses explicit allowlist
# ══════════════════════════════════════════════════════════════════

_FORBIDDEN_INTERNAL_KEYS = (
    "_proof_staged",
    "_proof_finalized",
    "_submit_generation",
    "_stop_breach_ts",
    "_underlying_stop_breach_ts",
    "pending_exit_local_order_id",
    "pending_exit_broker_order_id",
    "exit_identity_quarantine",
    "_exit_stuck_count",
    "_exit_submit_ts",
    "_exit_attempts",
    "last_applied_exit_cum_fill_by_order",
)


class TestFix7EdgeLoggerAllowlist:
    def test_edge_logger_payload_excludes_internal_fields(self):
        """The edge logger payload must NOT contain ManagedPosition
        internals — only allowlisted trade-relevant fields."""
        captured: dict = {}

        class _CapturingEdge:
            def log_trade(self, **kwargs):
                captured.update(kwargs)

        core, _, _ = _make_core()
        core._edge_logger = _CapturingEdge()
        pos = _new_position()
        # Set internal state to prove it doesn't leak
        pos._proof_staged = {"x": 1}
        pos._submit_generation = 5
        pos.pending_exit_local_order_id = "lo-abc"

        decision = ExitDecision(
            action="CLOSE_ALL", quantity=1,
            reason="RUNNER TRAIL", urgency="HIGH",
            reason_code="RUNNER_TRAIL", suggested_limit=1.20,
        )
        core._on_position_close(pos, decision)

        assert "position" in captured, "edge logger must receive 'position' dict"
        pos_dict = captured["position"]
        for key in _FORBIDDEN_INTERNAL_KEYS:
            assert key not in pos_dict, (
                f"Edge logger payload leaked internal field '{key}'. "
                f"Use an explicit allowlist, not pos.__dict__."
            )

    def test_edge_logger_payload_includes_required_trade_fields(self):
        captured: dict = {}

        class _CapturingEdge:
            def log_trade(self, **kwargs):
                captured.update(kwargs)

        core, _, _ = _make_core()
        core._edge_logger = _CapturingEdge()
        pos = _new_position()
        decision = ExitDecision(
            action="CLOSE_ALL", quantity=1,
            reason="RUNNER TRAIL", urgency="HIGH",
            reason_code="RUNNER_TRAIL", suggested_limit=1.20,
        )
        core._on_position_close(pos, decision)

        pos_dict = captured["position"]
        for required in ("ticker", "side", "entry_price", "quantity",
                          "position_id", "signal", "direction", "timeframe"):
            assert required in pos_dict, (
                f"Edge logger payload missing required trade field '{required}'"
            )


# ══════════════════════════════════════════════════════════════════
# FIX-8 — Env / config hardening
# ══════════════════════════════════════════════════════════════════

class TestFix8EnvHardening:
    def test_apmode_botmode_conflict_raises_in_init(self):
        """AP_MODE=LIVE + BOT_MODE=PAPER must raise on construction."""
        with patch.dict(os.environ, {"AP_MODE": "LIVE", "BOT_MODE": "PAPER"}):
            mc = _make_mc(mode="live")
            with pytest.raises(RuntimeError) as exc:
                broker = MagicMock()
                osm = _make_osm()
                APExecutionCore(
                    broker=broker,
                    supabase_client=None,
                    email="alice@x.com",
                    order_state_machine=osm,
                    master_control=mc,
                )
            assert "AP_MODE" in str(exc.value)
            assert "BOT_MODE" in str(exc.value)

    def test_score_floor_live_env_overridable(self):
        """SCORE_FLOOR_LIVE must read from env."""
        # Reload the module with overridden env to verify the constant updates.
        import importlib
        with patch.dict(os.environ, {"SCORE_FLOOR_LIVE": "82"}):
            importlib.reload(ap_execution_core)
            try:
                assert ap_execution_core.SCORE_FLOOR_LIVE == 82
            finally:
                # Reload back to default for downstream tests in this session.
                os.environ.pop("SCORE_FLOOR_LIVE", None)
                importlib.reload(ap_execution_core)

    def test_breakeven_band_pct_module_constant(self):
        """BREAKEVEN_BAND_PCT must be a module-level attribute."""
        assert hasattr(ap_execution_core, "BREAKEVEN_BAND_PCT"), (
            "BREAKEVEN_BAND_PCT must be elevated from in-line os.getenv "
            "to a module-level constant; both _on_position_close and "
            "_finalize_proof must reference the constant, not getenv"
        )
        assert isinstance(ap_execution_core.BREAKEVEN_BAND_PCT, float)
        assert ap_execution_core.BREAKEVEN_BAND_PCT == -2.0  # current default

    def test_breakeven_band_single_source_in_money_path(self):
        """No inline os.getenv('BREAKEVEN_BAND_PCT', ...) should remain
        in the money-path functions (_on_position_close, _finalize_proof).
        The constant is the single source of truth."""
        src = Path(REPO_ROOT, "ap_execution_core.py").read_text()
        pattern = re.compile(
            r'os\.getenv\(\s*["\']BREAKEVEN_BAND_PCT["\']'
        )
        # All inline reads must be gone. The module-level definition is
        # the only allowed reference.
        # Count: should be ZERO inline reads (module-level uses the same
        # constant via a single os.getenv at top, not in functions).
        # Approach: assert no os.getenv call appears INSIDE function bodies.
        # Crude check: count total occurrences; module-level only = 1.
        matches = pattern.findall(src)
        assert len(matches) <= 1, (
            f"Found {len(matches)} os.getenv('BREAKEVEN_BAND_PCT', ...) "
            f"references; must be at most 1 (the module-level definition). "
            f"Inline reads in _on_position_close / _finalize_proof must "
            f"be replaced with BREAKEVEN_BAND_PCT constant."
        )


# ══════════════════════════════════════════════════════════════════
# INV — Structural invariant: suggested_limit is price authority
# ══════════════════════════════════════════════════════════════════

class TestInvariantSuggestedLimitAuthority:
    def test_on_position_close_consults_suggested_limit(self):
        """Source of _on_position_close must reference
        decision.suggested_limit as the first authority. Prevents future
        refactors from quietly resurrecting the duplicate pricer."""
        src = inspect.getsource(APExecutionCore._on_position_close)
        assert "suggested_limit" in src, (
            "_on_position_close must read decision.suggested_limit as "
            "the first exit-price authority. Without this reference the "
            "duplicate pricing engine in execution core silently overrides "
            "the exit engine — the trail-exit spread leak."
        )

    def test_suggested_limit_referenced_before_ladder_logic(self):
        """In source order, suggested_limit must appear BEFORE the
        step-down ladder block. This guarantees the ladder is a
        fallback, not the primary path."""
        src = inspect.getsource(APExecutionCore._on_position_close)
        idx_suggested = src.find("suggested_limit")
        # Look for any of the ladder markers
        ladder_markers = ("STEP-DOWN", "step-down", "ladder", "EXIT STEP-DOWN")
        idx_ladder = min(
            (src.find(m) for m in ladder_markers if src.find(m) >= 0),
            default=-1,
        )
        assert idx_suggested > 0, "suggested_limit must be referenced"
        if idx_ladder > 0:
            assert idx_suggested < idx_ladder, (
                "decision.suggested_limit must be read BEFORE the "
                "step-down ladder block — it is the primary authority; "
                "the ladder is a fallback."
            )


# ══════════════════════════════════════════════════════════════════
# CODEX-1 — Edge logger payload preserves contract_symbol
# ══════════════════════════════════════════════════════════════════
#
# Codex P2 review on PR #35 flagged:
#   "_on_position_close now sends an allowlisted `position` dict to
#    `_edge_logger.log_trade`, but the allowlist omits both
#    `option_symbol` and `contract`. In APTradeLogger.log_trade
#    (`ap_edge_intelligence.py`), `contract_symbol` is derived only
#    from those keys, so this change causes every logged trade to
#    lose its contract identifier."
#
# Verified at ap_edge_intelligence.py:110 —
#     contract_symbol = position.get("option_symbol") or position.get("contract") or ""
#
# Fix: add option_symbol (canonical), underlying_entry, underlying_stop,
# underlying_target, and contracts/qty alias coverage to the allowlist.
# These are all trade-relevant fields (not internals) and the logger
# explicitly looks for them.

class TestCodex1EdgeLoggerContractSymbol:
    def test_edge_logger_payload_includes_option_symbol(self):
        """Codex P2: edge logger payload must include option_symbol so
        ap_edge_intelligence.APTradeLogger.log_trade can derive
        contract_symbol. Without this every logged trade loses its
        contract identifier — analytics & postmortem are broken."""
        captured: dict = {}

        class _CapturingEdge:
            def log_trade(self, **kwargs):
                captured.update(kwargs)

        core, _, _ = _make_core()
        core._edge_logger = _CapturingEdge()
        pos = _new_position(option_symbol="SPY260530C00500000")

        decision = ExitDecision(
            action="CLOSE_ALL", quantity=1,
            reason="RUNNER TRAIL", urgency="HIGH",
            reason_code="RUNNER_TRAIL", suggested_limit=1.20,
        )
        core._on_position_close(pos, decision)

        assert "position" in captured, "edge logger must receive 'position' dict"
        pos_dict = captured["position"]
        assert pos_dict.get("option_symbol") == "SPY260530C00500000", (
            "Edge logger payload must include option_symbol so "
            "APTradeLogger.log_trade can derive contract_symbol. "
            f"Got option_symbol={pos_dict.get('option_symbol')!r}; "
            f"full pos_dict keys={sorted(pos_dict.keys())}"
        )

    def test_contract_symbol_derived_downstream_is_non_empty(self):
        """End-to-end: when the real APTradeLogger.log_trade derivation
        rule runs on the edge logger payload, contract_symbol must NOT
        be empty. This is the exact bug codex flagged.

        Mirrors ap_edge_intelligence.py:110 verbatim:
            contract_symbol = position.get("option_symbol")
                              or position.get("contract") or ""
        """
        captured: dict = {}

        class _CapturingEdge:
            def log_trade(self, **kwargs):
                captured.update(kwargs)

        core, _, _ = _make_core()
        core._edge_logger = _CapturingEdge()
        pos = _new_position(option_symbol="SPY260530C00500000")

        decision = ExitDecision(
            action="CLOSE_ALL", quantity=1,
            reason="RUNNER TRAIL", urgency="HIGH",
            reason_code="RUNNER_TRAIL", suggested_limit=1.20,
        )
        core._on_position_close(pos, decision)

        pos_dict = captured["position"]
        # Replicate the EXACT derivation from ap_edge_intelligence.py:110
        contract_symbol = pos_dict.get("option_symbol") or pos_dict.get("contract") or ""
        assert contract_symbol != "", (
            "contract_symbol derived from edge-logger payload using the "
            "exact rule from ap_edge_intelligence.py:110 must NOT be "
            "empty. An empty contract_symbol breaks trade analytics, "
            "postmortem queries, and the proof-trades→trades_intel "
            "join. This is the codex P2 finding."
        )

    def test_edge_logger_payload_includes_underlying_levels(self):
        """The edge logger also uses underlying_entry, planned_stop /
        underlying_stop, planned_target / underlying_target to compute
        r-multiple and risk metrics. Allowlist must expose them."""
        captured: dict = {}

        class _CapturingEdge:
            def log_trade(self, **kwargs):
                captured.update(kwargs)

        core, _, _ = _make_core()
        core._edge_logger = _CapturingEdge()
        pos = _new_position(
            underlying_entry=500.00,
            underlying_stop=499.00,
            underlying_target=505.00,
        )
        decision = ExitDecision(
            action="CLOSE_ALL", quantity=1,
            reason="RUNNER TRAIL", urgency="HIGH",
            reason_code="RUNNER_TRAIL", suggested_limit=1.20,
        )
        core._on_position_close(pos, decision)

        pos_dict = captured["position"]
        # underlying_entry is required for r_multiple calc
        assert "underlying_entry" in pos_dict, (
            "Edge logger payload must include underlying_entry — "
            "APTradeLogger uses it for r-multiple and underlying_pnl_pct."
        )
        # stop/target accepted under EITHER alias (underlying_stop or planned_stop)
        has_stop = (
            "underlying_stop" in pos_dict
            or "planned_stop" in pos_dict
            or "stop_underlying" in pos_dict
        )
        has_target = (
            "underlying_target" in pos_dict
            or "planned_target" in pos_dict
            or "target_underlying" in pos_dict
        )
        assert has_stop, (
            "Edge logger payload must include a stop level alias "
            "(underlying_stop / planned_stop / stop_underlying)."
        )
        assert has_target, (
            "Edge logger payload must include a target level alias "
            "(underlying_target / planned_target / target_underlying)."
        )


# ══════════════════════════════════════════════════════════════════
# CODEX-2 — _finalize_proof preserves intel outcome on proof error
# ══════════════════════════════════════════════════════════════════
#
# Codex P2 review on PR #35 flagged:
#   "The intel callback was moved to _finalize_proof, but this method
#    still returns immediately if self.proof.log_trade(...) raises.
#    Because _record_intel_outcome now runs after that early return,
#    any transient proof-log failure drops the intelligence outcome
#    entirely for that trade (previously it was recorded from
#    _on_position_close regardless of proof write status). This
#    creates silent data loss exactly in degraded DB conditions."
#
# Fix: replace the bare `return` after proof.log_trade exception with a
# flag that skips proof-dependent finalization (feedback/shadow are
# already in their own try/except blocks) but allows intel to run.
# Intel is purely computational from the staged dict + actual fill —
# it has no dependency on proof DB success.

class TestCodex2IntelSurvivesProofError:
    def _staged_dict(self, pos):
        """Realistic _proof_staged payload matching what _on_position_close
        writes at submit-time."""
        return {
            "ticker": pos.ticker,
            "pattern": "test",
            "side": pos.side,
            "timeframe": "1d",
            "score": 80,
            "tier": "A",
            "context_score": 0,
            "setup_status": "",
            "entry_trigger": 500.0,
            "entry_option_price": 1.00,
            "exit_option_price": 1.20,
            "underlying_entry": 500.0,
            "underlying_exit": 502.0,
            "contracts": 1,
            "exit_reason": "RUNNER TRAIL",
            "opt_pnl": 20.0,
            "win": True,
            "spread_pct": 0.0,
            "chain_grade": "",
            "opened_at": pos.opened_at,
            "synthetic_entry": False,
            "position_id": pos.position_id,
            "local_order_id": "lo-1",
            "signal": {"signal_id": "sig-pr-b-1"},
            "paper": True,
        }

    def test_intel_outcome_recorded_when_proof_log_trade_raises(self):
        """Codex P2: when self.proof.log_trade(...) raises in
        _finalize_proof, _record_intel_outcome must STILL be called.

        Intel data is computational (staged dict + actual fill), not
        dependent on the proof DB. Dropping intel because proof failed
        is silent data loss in exactly the conditions where intel
        matters most — degraded DB."""
        intel_mock = MagicMock()
        with patch.object(ap_execution_core, "_record_intel_outcome", intel_mock):
            core, _, _ = _make_core()
            # Force proof.log_trade to raise (simulate transient DB outage)
            core.proof = MagicMock()
            core.proof.log_trade.side_effect = RuntimeError(
                "transient DB connection lost"
            )
            pos = _new_position()
            pos._proof_staged = self._staged_dict(pos)

            # Should NOT raise — _finalize_proof must absorb proof errors
            core._finalize_proof(pos, actual_fill_price=1.18)

        assert intel_mock.called, (
            "_record_intel_outcome must run even when proof.log_trade "
            "raises. The intel callback is computational and independent "
            "of proof DB success. Dropping it on proof errors creates "
            "silent intelligence data loss in degraded DB conditions — "
            "exactly when intel matters most for diagnosis."
        )
        # And the pnl_pct passed must still reflect the actual fill
        call_kwargs = intel_mock.call_args.kwargs
        assert abs(call_kwargs["pnl_pct"] - 0.18) < 0.001, (
            f"Intel pnl_pct must be computed from actual fill even when "
            f"proof errored. Got pnl_pct={call_kwargs.get('pnl_pct')}"
        )

    def test_intel_outcome_recorded_when_feedback_record_outcome_raises(self):
        """Secondary path: feedback.record_outcome raises → intel still
        runs. (Feedback is already in its own try/except, so this should
        already be true; assert it as a regression guard.)"""
        intel_mock = MagicMock()
        with patch.object(ap_execution_core, "_record_intel_outcome", intel_mock):
            core, _, _ = _make_core()
            core.feedback = MagicMock()
            core.feedback.record_outcome.side_effect = RuntimeError("feedback DB down")
            # proof.log_trade succeeds
            core.proof = MagicMock()
            pos = _new_position()
            pos._proof_staged = self._staged_dict(pos)

            core._finalize_proof(pos, actual_fill_price=1.18)

        assert intel_mock.called, (
            "_record_intel_outcome must run even when "
            "feedback.record_outcome raises."
        )

    def test_finalize_proof_does_not_reraise_on_proof_error(self):
        """_finalize_proof is called from the exit engine's
        on_exit_fill_confirmed callback. It must NEVER propagate
        exceptions back to the exit engine — that would block other
        positions from finalizing."""
        with patch.object(ap_execution_core, "_record_intel_outcome", MagicMock()):
            core, _, _ = _make_core()
            core.proof = MagicMock()
            core.proof.log_trade.side_effect = RuntimeError("DB hard down")
            pos = _new_position()
            pos._proof_staged = self._staged_dict(pos)

            # Must not raise
            try:
                core._finalize_proof(pos, actual_fill_price=1.18)
            except Exception as e:
                pytest.fail(
                    f"_finalize_proof must absorb all internal errors. "
                    f"Propagating to exit engine breaks position cleanup. "
                    f"Raised: {type(e).__name__}: {e}"
                )

    def test_finalize_proof_marks_finalized_even_on_proof_error(self):
        """Idempotency: even when proof fails, _proof_finalized must be
        set so retry attempts don't double-log intel. (The fix must
        preserve the existing idempotency guard.)"""
        with patch.object(ap_execution_core, "_record_intel_outcome", MagicMock()):
            core, _, _ = _make_core()
            core.proof = MagicMock()
            core.proof.log_trade.side_effect = RuntimeError("DB down")
            pos = _new_position()
            pos._proof_staged = self._staged_dict(pos)

            core._finalize_proof(pos, actual_fill_price=1.18)

            assert getattr(pos, "_proof_finalized", False) is True, (
                "_proof_finalized must be set True even when proof errors, "
                "to prevent duplicate intel writes on retry."
            )
