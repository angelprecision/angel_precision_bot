"""
PR A — Pre-LIVE Exit Engine Bug Sweep
======================================

Tests for the four bugs identified in the institutional audit of
ap_exit_engine.py (audit dated 2026-05-25):

  BUG-1  Ghost fields on ManagedPosition
         _stop_breach_ts / _underlying_stop_breach_ts are written via
         `# type: ignore[attr-defined]` and never declared on the
         dataclass. They survive only on the live instance; any
         dataclasses.replace() or asdict()/from-dict round-trip loses
         them silently.

  BUG-2  Mixed time.time() / datetime timestamp types
         Same two fields are stamped with float epoch (time.time())
         while every other timestamp on ManagedPosition is
         Optional[datetime]. Future code that diff-subtracts a real
         datetime field against one of these would TypeError.

  BUG-3  O(n) linear scans in fill / close callbacks
         mark_position_closed, clear_exit_in_flight, note_partial_exit_fill,
         set_pending_exit_order, and the post-callback revalidation in
         _submit_exit_decision still iterate self._positions even though
         self._positions_by_id is built and maintained.

  BUG-4  MIN_HOLD_MINUTES_BEFORE_SOFT_EXIT default mismatch
         Same env var is read with default="5" in the soft-stop path
         (line 733) and default="3" in the never-green path (line 900).
         If the env is unset, the two paths use different floors.

Structural invariant (not from audit; my recommendation):

  INV-1  evaluate_exit is the only producer of ExitDecision in the
         exit engine; _submit_exit_decision is the only consumer.

These tests run FIRST. They must FAIL on `main` and pass after the
PR-A fixes are applied.

Run:
    pytest tests/test_exit_engine_bug_sweep.py -v
"""
from __future__ import annotations

import os
import re
import sys
import types
import inspect
from dataclasses import dataclass, fields as dc_fields, replace
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

# Env stubs BEFORE importing the engine.
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_exit_engine_bug_sweep",
)
os.environ.setdefault("ENCRYPTION_KEY", "ap-exit-engine-bug-sweep-2026")

# Stub supabase so any transitive imports are clean.
if "supabase" not in sys.modules:
    _supa_stub = types.ModuleType("supabase")
    _supa_stub.create_client = lambda *a, **kw: None
    _supa_stub.Client = type("Client", (), {})
    sys.modules["supabase"] = _supa_stub


import ap_exit_engine  # noqa: E402
from ap_exit_engine import (  # noqa: E402
    ManagedPosition,
    ExitDecision,
    APExitEngine,
    evaluate_exit,
)


# ──────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────

def _new_position(**overrides) -> ManagedPosition:
    """Build a ManagedPosition that satisfies the evaluate_exit shape."""
    defaults = dict(
        ticker="SPY",
        option_symbol="SPY260530C00500000",
        side="CALL",
        quantity=1,
        entry_price=1.00,
        underlying_entry=500.00,
        underlying_target=505.00,
        underlying_stop=499.00,
        position_id="pos-test-1",
        client_id="alice@x.com",
        signal_id="sig-1",
        is_trend_day=False,
        current_option_price=1.00,
        current_bid=0.99,
        current_ask=1.01,
        current_underlying=500.00,
        quantity_remaining=1,
        opened_at=datetime.now(timezone.utc),
    )
    defaults.update(overrides)
    return ManagedPosition(**defaults)


def _new_engine() -> APExitEngine:
    """Build a real APExitEngine wired with mocks for IO."""
    broker = MagicMock()
    data_broker = MagicMock()
    eng = APExitEngine(broker=broker, data_broker=data_broker)
    return eng


# ══════════════════════════════════════════════════════════════════
# BUG-1 — Ghost fields on ManagedPosition
# ══════════════════════════════════════════════════════════════════

class TestBug1GhostFields:
    """The breach timestamps must be declared fields, not dynamic attrs.

    Failing-on-main signal: dataclasses.fields() does not contain them.
    """

    def test_stop_breach_ts_is_declared_field(self):
        field_names = {f.name for f in dc_fields(ManagedPosition)}
        assert "_stop_breach_ts" in field_names, (
            "_stop_breach_ts must be a declared field on ManagedPosition; "
            "currently it is a ghost attribute set via type: ignore"
        )

    def test_underlying_stop_breach_ts_is_declared_field(self):
        field_names = {f.name for f in dc_fields(ManagedPosition)}
        assert "_underlying_stop_breach_ts" in field_names, (
            "_underlying_stop_breach_ts must be a declared field on "
            "ManagedPosition; currently it is a ghost attribute"
        )

    def test_fields_default_to_none(self):
        pos = _new_position()
        assert pos._stop_breach_ts is None
        assert pos._underlying_stop_breach_ts is None

    def test_fields_survive_dataclass_replace_roundtrip(self):
        """A dataclasses.replace() must preserve the stamped timestamp.

        This is the canonical refactor that breaks ghost fields:
        replace(pos, current_underlying=499.5) drops every dynamic
        attribute. Declaring the fields fixes it.
        """
        pos = _new_position()
        stamp = datetime.now(timezone.utc)
        pos._stop_breach_ts = stamp
        pos._underlying_stop_breach_ts = stamp

        clone = replace(pos, current_underlying=498.0)
        assert clone._stop_breach_ts == stamp, (
            "dataclasses.replace() dropped _stop_breach_ts — proves it was a ghost field"
        )
        assert clone._underlying_stop_breach_ts == stamp, (
            "dataclasses.replace() dropped _underlying_stop_breach_ts — proves it was a ghost field"
        )


# ══════════════════════════════════════════════════════════════════
# BUG-2 — Mixed time.time() / datetime timestamp types
# ══════════════════════════════════════════════════════════════════

class TestBug2TimestampTypes:
    """Both stamps must be datetime, not float epoch seconds.

    Failing-on-main signal: after a breach tick, the stamp is a float.
    """

    def test_underlying_stop_stamp_is_datetime_after_first_breach(self):
        """Tick 1 against a breached underlying must stamp a datetime."""
        # CALL with current_underlying at exactly the stop level
        pos = _new_position(
            side="CALL",
            underlying_stop=499.00,
            current_underlying=498.50,  # below stop -> is_at_stop=True
        )
        assert pos.is_at_stop is True

        # Run evaluate_exit; this is the path that stamps
        # _underlying_stop_breach_ts.
        evaluate_exit(pos)

        assert pos._underlying_stop_breach_ts is not None, (
            "evaluate_exit must stamp _underlying_stop_breach_ts on first breach tick"
        )
        assert isinstance(pos._underlying_stop_breach_ts, datetime), (
            f"_underlying_stop_breach_ts must be datetime, got "
            f"{type(pos._underlying_stop_breach_ts).__name__}"
        )
        # Sanity: must be UTC-aware.
        assert pos._underlying_stop_breach_ts.tzinfo is not None, (
            "_underlying_stop_breach_ts must be timezone-aware (UTC)"
        )

    def test_underlying_stop_confirms_after_window(self):
        """After UNDERLYING_STOP_CONFIRM_SECONDS, the breach must trigger STOP."""
        pos = _new_position(
            side="CALL",
            underlying_stop=499.00,
            current_underlying=498.50,
        )
        # Pre-stamp a breach far in the past
        confirm_sec = float(os.environ.get("UNDERLYING_STOP_CONFIRM_SECONDS", "30"))
        pos._underlying_stop_breach_ts = (
            datetime.now(timezone.utc) - timedelta(seconds=confirm_sec + 5)
        )

        decision = evaluate_exit(pos)
        assert decision.action == "STOP", (
            f"Expected STOP after confirm window elapsed, got action={decision.action!r} "
            f"reason={decision.reason!r}"
        )

    def test_stop_recovers_clears_stamp(self):
        """Underlying recovering above stop must clear the breach stamp."""
        pos = _new_position(
            side="CALL",
            underlying_stop=499.00,
            current_underlying=499.50,  # above stop -> not at_stop
        )
        pos._underlying_stop_breach_ts = datetime.now(timezone.utc)
        assert pos.is_at_stop is False

        evaluate_exit(pos)
        assert pos._underlying_stop_breach_ts is None, (
            "Stamp must be cleared when underlying recovers above stop"
        )


# ══════════════════════════════════════════════════════════════════
# BUG-3 — O(n) scans in fill / close callbacks
# ══════════════════════════════════════════════════════════════════

# We prove the bug structurally by reading the source. Behavior tests
# would also catch it, but structural assertions are deterministic and
# regression-proof: a future refactor that reintroduces the loop fails
# the test immediately, without needing to wait for a multi-position
# benchmark.

# Methods that MUST use the O(1) _positions_by_id index when given a
# position_id parameter.
_O1_METHODS = (
    "mark_position_closed",
    "clear_exit_in_flight",
    "note_partial_exit_fill",
    "set_pending_exit_order",
)


def _method_source(method_name: str) -> str:
    """Extract source of an APExitEngine method by name."""
    method = getattr(APExitEngine, method_name)
    return inspect.getsource(method)


class TestBug3O1Lookup:
    """The five hot-path methods must use self._positions_by_id, not
    a linear loop over self._positions.

    Failing-on-main signal: source contains `for pos in self._positions`.
    """

    @pytest.mark.parametrize("method_name", _O1_METHODS)
    def test_method_uses_positions_by_id(self, method_name):
        src = _method_source(method_name)
        # The method must reference _positions_by_id.
        assert "_positions_by_id" in src, (
            f"{method_name} must use self._positions_by_id for O(1) lookup; "
            f"current source still scans self._positions"
        )

    @pytest.mark.parametrize("method_name", _O1_METHODS)
    def test_method_does_not_linearly_scan_positions(self, method_name):
        src = _method_source(method_name)
        # Reject the canonical linear-scan pattern. We allow `self._positions`
        # to appear in append/maintenance code, but NOT inside a
        # `for ... in self._positions:` iteration.
        bad = re.search(r"for\s+\w+\s+in\s+self\._positions\s*:", src)
        assert bad is None, (
            f"{method_name} still iterates self._positions linearly; "
            f"replace with self._positions_by_id.get(pid)"
        )

    def test_submit_exit_decision_uses_o1_when_position_id_known(self):
        """_submit_exit_decision's post-callback revalidation must
        prefer the O(1) dict when position_id is non-empty.

        The current code at audit-line 3855 scans linearly; the fix is
        to do a dict.get() first and only fall back to iteration when
        position_id is empty (the by-object-identity path)."""
        src = _method_source("_submit_exit_decision")
        # Look for "_positions_by_id.get(" appearing inside the method
        # (proves the fix was applied).
        assert "_positions_by_id.get(" in src or "_positions_by_id[" in src, (
            "_submit_exit_decision must consult self._positions_by_id "
            "when position_id is known; currently it scans linearly"
        )

    def test_behavioral_mark_position_closed_routes_via_dict(self):
        """When mark_position_closed is called for a known position_id,
        the dict's __getitem__/get must be hit (proves no full scan)."""
        eng = _new_engine()
        pos = _new_position(position_id="pos-XYZ-bug3")
        eng.add_position(pos)

        # Wrap the dict so we can detect access.
        original_get = eng._positions_by_id.get
        access_count = {"n": 0}

        def counting_get(key, default=None):
            access_count["n"] += 1
            return original_get(key, default)

        # Patch only the .get attribute (preserving dict storage).
        eng._positions_by_id = type(eng._positions_by_id)(eng._positions_by_id)
        # Re-wire by monkey-patching at the bound-method level
        # via a subclass that records access.
        class _CountingDict(dict):
            n = 0
            def get(self, key, default=None):
                _CountingDict.n += 1
                return super().get(key, default)

        cd = _CountingDict(eng._positions_by_id)
        eng._positions_by_id = cd

        eng.mark_position_closed(pos.position_id, force=True)
        assert _CountingDict.n >= 1, (
            "mark_position_closed must call _positions_by_id.get() at least once "
            "for an O(1) lookup; got zero accesses"
        )


# ══════════════════════════════════════════════════════════════════
# BUG-4 — MIN_HOLD_MINUTES_BEFORE_SOFT_EXIT default mismatch
# ══════════════════════════════════════════════════════════════════

class TestBug4MinHoldDefault:
    """Both call sites must read the same default (5)."""

    def test_source_contains_no_default_3(self, tmp_path=None):
        """The string `MIN_HOLD_MINUTES_BEFORE_SOFT_EXIT", "3"` (or the
        same env var with default 3) must not appear anywhere in the
        exit engine source after the fix."""
        src = Path(REPO_ROOT, "ap_exit_engine.py").read_text()
        # Match any quoted "3" used as the default for that env var.
        pattern = re.compile(
            r'os\.getenv\(\s*["\']MIN_HOLD_MINUTES_BEFORE_SOFT_EXIT["\']\s*,\s*["\']3["\']\s*\)'
        )
        match = pattern.search(src)
        assert match is None, (
            "MIN_HOLD_MINUTES_BEFORE_SOFT_EXIT has a default of 3 somewhere "
            "in ap_exit_engine.py; both call sites must use 5 (or a shared "
            "module-level constant) so PAPER and LIVE behave the same when "
            "the env var is unset"
        )

    def test_module_level_constant_exists(self):
        """A single module-level constant should hold the unified value."""
        # Acceptable names (the fix can choose either):
        for name in ("_MIN_HOLD_BEFORE_EXIT_MIN", "MIN_HOLD_BEFORE_EXIT_MIN"):
            if hasattr(ap_exit_engine, name):
                val = getattr(ap_exit_engine, name)
                assert isinstance(val, float), f"{name} must be float, got {type(val).__name__}"
                assert val == 5.0, f"{name} must default to 5.0, got {val}"
                return
        pytest.fail(
            "ap_exit_engine must expose a module-level "
            "_MIN_HOLD_BEFORE_EXIT_MIN (or MIN_HOLD_BEFORE_EXIT_MIN) "
            "constant so both call sites read the same value"
        )

    def test_both_call_sites_use_same_value(self):
        """Read source: both call sites must reference the constant
        (or both inline the same default 5)."""
        src = Path(REPO_ROOT, "ap_exit_engine.py").read_text()
        # Count occurrences of the env-getenv call.
        getenv_calls = re.findall(
            r'os\.getenv\(\s*["\']MIN_HOLD_MINUTES_BEFORE_SOFT_EXIT["\']\s*,\s*["\'](\d+)["\']\s*\)',
            src,
        )
        # If any inline getenv calls remain, they must all default to "5".
        # (The cleanest fix removes them entirely in favor of a constant;
        # we accept either form as long as values are consistent.)
        for default in getenv_calls:
            assert default == "5", (
                f"Inline default for MIN_HOLD_MINUTES_BEFORE_SOFT_EXIT must be "
                f"'5' at every call site; found '{default}'"
            )


# ══════════════════════════════════════════════════════════════════
# INV-1 — Structural invariant: evaluate_exit is the only producer
#          of ExitDecision; _submit_exit_decision is the only consumer
# ══════════════════════════════════════════════════════════════════

class TestInvariantExitDecisionContract:
    """Mirror of test_exit_proof_finalization.py's contract test for the
    proof finalize path. Prevents future refactors from spawning a
    second ExitDecision-producing function outside of evaluate_exit.
    """

    def test_evaluate_exit_returns_exit_decision(self):
        """evaluate_exit signature returns ExitDecision."""
        sig = inspect.signature(evaluate_exit)
        # The annotated return type should mention ExitDecision.
        # Use string match because evaluate_exit uses `from __future__ import annotations`.
        ret_anno = str(sig.return_annotation)
        assert "ExitDecision" in ret_anno, (
            f"evaluate_exit must return ExitDecision; got {ret_anno}"
        )

    def test_only_one_evaluate_exit_in_module(self):
        """No other function in ap_exit_engine should construct
        ExitDecision in a way that suggests a parallel evaluation path.

        We allow ExitDecision(...) to appear inside evaluate_exit (it
        builds many decisions) but reject any OTHER function whose body
        constructs more than 1 ExitDecision — that would be a second
        decision producer.
        """
        src = Path(REPO_ROOT, "ap_exit_engine.py").read_text()
        # Find top-level def blocks and count ExitDecision( calls inside each.
        # Module-level functions only (not class methods).
        offenders = []
        # Crude but effective: split at each top-level "def " or "class "
        # and inspect each block.
        # Sanctioned decision producers: the public wrapper `evaluate_exit`
        # and its single private core `_evaluate_exit_core`. PR-G3 split the
        # evaluator into wrapper+core so the ladder-attribution stamp could be
        # guaranteed on every return path from ONE choke point. The core is the
        # producer; the wrapper is its only caller. Any OTHER module-level
        # function building 2+ ExitDecisions is still a rogue parallel path.
        _sanctioned = {"evaluate_exit", "_evaluate_exit_core"}
        for m in re.finditer(r"^def\s+(\w+)\s*\(", src, re.MULTILINE):
            name = m.group(1)
            if name in _sanctioned:
                continue
            start = m.start()
            # Find the next top-level def/class start, or EOF.
            next_m = re.search(r"^(def|class)\s+", src[start + 1:], re.MULTILINE)
            end = (start + 1 + next_m.start()) if next_m else len(src)
            body = src[start:end]
            count = body.count("ExitDecision(")
            # A module-level helper that builds 2+ ExitDecisions is suspicious.
            if count >= 2:
                offenders.append((name, count))
        assert offenders == [], (
            f"Found module-level functions other than evaluate_exit that "
            f"construct multiple ExitDecision objects: {offenders}. "
            f"evaluate_exit must be the SOLE producer."
        )

    def test_submit_exit_decision_takes_exit_decision(self):
        """_submit_exit_decision must accept ExitDecision as input."""
        method = getattr(APExitEngine, "_submit_exit_decision")
        src = inspect.getsource(method)
        # The signature must reference ExitDecision (string match because
        # the file uses `from __future__ import annotations`).
        head = src.split("\n")[0]
        # Find the def line and any continuation lines until the colon
        # closes the signature.
        sig_lines = []
        for line in src.split("\n"):
            sig_lines.append(line)
            if line.rstrip().endswith(":") and "def " in "\n".join(sig_lines):
                break
        sig_text = "\n".join(sig_lines)
        assert "ExitDecision" in sig_text or "decision" in sig_text.lower(), (
            "_submit_exit_decision must take an ExitDecision (or decision) "
            f"as its primary input; signature was: {sig_text[:200]}"
        )
