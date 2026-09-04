"""
tests/test_p0_pr516_amend_degraded_retry_convergence.py

PR #557 amendment — degraded broker-truth owner → durable owner convergence.

Blockers covered:
  * Blocker 1 (from earlier amendment): degraded owner is retried for durable
    recovery on subsequent cycles.
  * Blocker 2 (from earlier amendment): canonical convergence uses the actual
    helper APIs and consumes the (value, contradiction) tuple returns.
  * Blocker 3 (this amendment): durable recovery actually REPLACES the
    degraded owner rather than being rejected as a duplicate; in-place
    convergence preserves live exit state; retry failure retains the
    existing degraded owner instead of leaking a new broker-degraded-<uuid>.

Every test is stated in behavioural terms and asserts the invariants
listed in the amendment spec.  Real behavioural tests run only when the
ap_exit_engine module can be imported (skipped otherwise).  Source-pattern
tests always run and catch regressions of the exact call/method shape.
"""
from __future__ import annotations

import importlib.util
import sys
import threading
import types
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_REPO = Path(__file__).resolve().parents[1]


def _load_engine_module():
    """Import ap_exit_engine cleanly — snapshot-and-restore sys.modules."""
    touched = ["ap_exit_engine", "ap", "ap.db", "ap.position_manager", "ap_tradier"]
    saved = {name: sys.modules.get(name) for name in touched}
    inserted = {name: False for name in touched}
    try:
        for name in ["ap.db", "ap.position_manager", "ap_tradier"]:
            if name not in sys.modules:
                sys.modules[name] = types.ModuleType(name)
                inserted[name] = True
        spec = importlib.util.spec_from_file_location(
            "ap_exit_engine", _REPO / "ap_exit_engine.py"
        )
        mod = importlib.util.module_from_spec(spec)
        sys.modules["ap_exit_engine"] = mod
        inserted["ap_exit_engine"] = True
        try:
            spec.loader.exec_module(mod)
            return mod
        except Exception:
            return None
    finally:
        for name in touched:
            prior = saved[name]
            if prior is not None:
                sys.modules[name] = prior
            elif inserted[name]:
                sys.modules.pop(name, None)


_EE_MOD = _load_engine_module()
_skip_if_no_mod = pytest.mark.skipif(
    _EE_MOD is None, reason="ap_exit_engine could not be imported in test env"
)


_CLIENT = "jasoncosby1@gmail.com"
_MODE = "live"
_CONTRACT = "SPY260919C00450000"
_UNDERLYING = "SPY"
_BROKER_QTY = 2
_ENTRY_PX = 3.25
_COST_BASIS = _ENTRY_PX * _BROKER_QTY * 100.0


def _stub_ap_db():
    ap_stub = types.ModuleType("ap")
    db_stub = types.ModuleType("ap.db")
    db_stub.run_with_retry = lambda f: (f() if callable(f) else None)
    db_stub.conn = MagicMock()
    ap_stub.db = db_stub
    sys.modules.setdefault("ap", ap_stub)
    sys.modules.setdefault("ap.db", db_stub)


def _new_engine():
    engine_cls = getattr(_EE_MOD, "APExitEngine", None)
    if engine_cls is None:
        pytest.skip("APExitEngine not exported in test env")
    eng = engine_cls.__new__(engine_cls)
    eng._email = _CLIENT
    eng._lock = threading.RLock()
    eng._positions = []
    eng._positions_by_id = {}
    eng._resolved_execution_mode = lambda: _MODE
    return eng


def _broker_mock():
    m = MagicMock()
    m.account_id = "VA_TEST"
    m.mode = _MODE
    m.list_positions.return_value = [{
        "symbol": _CONTRACT,
        "quantity": _BROKER_QTY,
        "cost_basis": _COST_BASIS,
    }]
    return m


def _assert_no_broker_writes(broker, context: str = ""):
    """The amendment must never submit an ENTRY or issue broad cancels."""
    for forbidden in (
        "submit_order", "submit_entry", "submit_exit",
        "cancel_order", "cancel_all_orders", "cancel_all",
    ):
        fn = getattr(broker, forbidden, None)
        if fn is None:
            continue
        assert not fn.called, (
            f"broker.{forbidden} was called during {context or 'precheck'} — "
            f"amendment must never submit/cancel from precheck"
        )


def _active_for_sym(eng, sym):
    with eng._lock:
        return [
            p for p in eng._positions
            if getattr(p, "quantity_remaining", 0) > 0
            and str(getattr(p, "option_symbol", "")).upper() == sym
            and not getattr(p, "closed", False)
        ]


# ─────────────────────────────────────────────────────────────────────────────
# Behavioural — three-cycle convergence + idempotency (durable UUID case)
# ─────────────────────────────────────────────────────────────────────────────

@_skip_if_no_mod
def test_pr557_amend_three_cycle_convergence_durable_uuid():
    """
    CYCLE 1: broker OPEN + durable repair unavailable → degraded owner installed.
    CYCLE 2: broker still OPEN + durable UUID now persists (no canonical filled
             ENTRY yet) → degraded owner IS REPLACED by durable provisional
             owner in-place, live exit state preserved, exactly one active.
    CYCLE 3: broker still OPEN + no new work → precheck is idempotent: the
             durable provisional owner remains unchanged, no new degraded id
             created, no duplicate lifecycle.
    """
    _stub_ap_db()
    eng = _new_engine()
    broker = _broker_mock()
    eng.broker = broker
    eng._quote_broker = broker
    eng._fetch_broker_quote = lambda _sym: {
        "mark": _ENTRY_PX, "bid": _ENTRY_PX - 0.05, "ask": _ENTRY_PX + 0.05,
        "mid": _ENTRY_PX, "last": _ENTRY_PX,
    }
    eng._underlying_from_occ = lambda _sym: _UNDERLYING
    eng._parse_occ_side = lambda _sym: "CALL"

    # ── CYCLE 1: force durable repair failure ────────────────────────────
    eng._load_db_position_row = lambda sym: None
    eng._upsert_broker_position_to_db = MagicMock(
        side_effect=RuntimeError("cycle1: durable repair unavailable")
    )
    result1 = eng._broker_position_precheck()
    # Amended contract: degraded owner installed → precheck returns True
    # because the position is exit-visible.
    assert result1 is True, (
        f"cycle 1 precheck must return True when degraded owner installed; got {result1}"
    )

    active1 = _active_for_sym(eng, _CONTRACT)
    assert len(active1) == 1, f"cycle 1 must install exactly one owner; got {len(active1)}"
    degraded = active1[0]
    degraded_id = str(getattr(degraded, "position_id", "") or "")
    assert degraded_id, "degraded owner must have a position_id"
    assert getattr(degraded, "broker_repair_degraded", False) is True
    assert getattr(degraded, "broker_repair_provisional", False) is True
    assert str(getattr(degraded, "execution_mode", "")).strip().lower() == _MODE

    # Simulate accumulated live exit state on the degraded owner.
    degraded.peak_pnl_pct = 12.5
    degraded.touched_profit = True
    degraded.current_bid = 3.20
    degraded.current_ask = 3.30

    _assert_no_broker_writes(broker, context="cycle 1")

    # ── CYCLE 2: durable UUID now persists via upsert ────────────────────
    provisional_uuid = f"pos-provisional-{uuid.uuid4()}"

    class _StubDBRow(dict):
        pass

    # After upsert returns the id, the precheck loads the row via
    # _load_db_position_row to build the managed position.  Return a row
    # matching the same OCC/client/mode/qty but with provisional flags.
    provisional_row = {
        "id": provisional_uuid,
        "position_id": provisional_uuid,
        "client_id": _CLIENT,
        "contract": _CONTRACT,
        "option_symbol": _CONTRACT,
        "underlying": _UNDERLYING,
        "side": "CALL",
        "direction": "CALL",
        "qty": _BROKER_QTY,
        "quantity_remaining": _BROKER_QTY,
        "entry_price": _ENTRY_PX,
        "avg_fill": _ENTRY_PX,
        "execution_mode": _MODE,
        "status": "OPEN",
        "broker_repair_provisional": True,
        "broker_repair_degraded": False,
    }
    eng._upsert_broker_position_to_db = MagicMock(return_value=provisional_uuid)
    eng._load_db_position_row = lambda sym: (
        provisional_row if sym.upper() == _CONTRACT else None
    )

    result2 = eng._broker_position_precheck()
    assert result2 is True, f"cycle 2 precheck must return True; got {result2}"

    active2 = _active_for_sym(eng, _CONTRACT)
    assert len(active2) == 1, (
        f"cycle 2 must leave exactly one active owner; got {len(active2)}: "
        f"{[getattr(p, 'position_id', '?') for p in active2]}"
    )
    surviving = active2[0]
    surviving_id = str(getattr(surviving, "position_id", "") or "")

    # Convergence invariants:
    assert surviving_id != degraded_id, (
        "convergence regression: degraded position_id still present after cycle 2 — "
        "ownership was never transferred to the durable owner"
    )
    assert getattr(surviving, "broker_repair_degraded", False) is False, (
        "cycle 2 surviving owner must have broker_repair_degraded=False"
    )
    assert getattr(surviving, "broker_repair_provisional", False) is True, (
        "durable UUID owner remains provisional until canonical ENTRY proves identity"
    )
    assert str(getattr(surviving, "option_symbol", "")).upper() == _CONTRACT
    assert str(getattr(surviving, "execution_mode", "")).strip().lower() == _MODE

    # Live state preservation invariants (accumulated on the degraded owner):
    assert getattr(surviving, "peak_pnl_pct", 0.0) == 12.5, (
        "convergence must preserve peak_pnl_pct from the degraded owner"
    )
    assert getattr(surviving, "touched_profit", False) is True, (
        "convergence must preserve touched_profit from the degraded owner"
    )
    assert getattr(surviving, "current_bid", 0.0) == 3.20, (
        "convergence must preserve current_bid quote state"
    )
    # Degraded owner must no longer be an active owner:
    with eng._lock:
        assert not any(
            str(getattr(p, "position_id", "") or "") == degraded_id
            and not getattr(p, "closed", False)
            for p in eng._positions
        ), "old degraded id must no longer be an active owner"
        assert eng._positions_by_id.get(degraded_id) is None or \
               eng._positions_by_id.get(degraded_id) is not degraded, (
            "_positions_by_id must not point to the closed degraded object"
        )

    _assert_no_broker_writes(broker, context="cycle 2")

    # ── CYCLE 3: idempotency ─────────────────────────────────────────────
    # Broker still says OPEN with same qty; DB still returns the same
    # provisional row.  precheck should be a no-op: same owner, no new
    # degraded identity, no duplicate.
    eng._upsert_broker_position_to_db = MagicMock(return_value=provisional_uuid)
    result3 = eng._broker_position_precheck()
    assert result3 is True

    active3 = _active_for_sym(eng, _CONTRACT)
    assert len(active3) == 1, f"cycle 3 must remain at one owner; got {len(active3)}"
    still_owner = active3[0]
    assert str(getattr(still_owner, "position_id", "")).strip() == surviving_id, (
        "cycle 3 must leave the durable owner unchanged"
    )
    # No new broker-degraded-<uuid> was created:
    with eng._lock:
        degraded_after_c3 = [
            p for p in eng._positions
            if getattr(p, "broker_repair_degraded", False)
            and not getattr(p, "closed", False)
        ]
    assert degraded_after_c3 == [], (
        f"cycle 3 must not create any new degraded owner; got "
        f"{[getattr(p,'position_id','?') for p in degraded_after_c3]}"
    )

    _assert_no_broker_writes(broker, context="cycle 3")


# ─────────────────────────────────────────────────────────────────────────────
# Behavioural — retry failure retains the existing degraded owner
# ─────────────────────────────────────────────────────────────────────────────

@_skip_if_no_mod
def test_pr557_amend_retry_failure_retains_existing_degraded_no_new_uuid():
    """
    CYCLE 1: install degraded owner.
    CYCLE 2: durable repair fails AGAIN → the amendment must retain the
             EXISTING degraded owner and NOT create a new
             broker-degraded-<uuid>.  Exactly one degraded owner remains
             with the same position_id as cycle 1.
    """
    _stub_ap_db()
    eng = _new_engine()
    broker = _broker_mock()
    eng.broker = broker
    eng._quote_broker = broker
    eng._fetch_broker_quote = lambda _sym: {
        "mark": _ENTRY_PX, "bid": _ENTRY_PX, "ask": _ENTRY_PX,
        "mid": _ENTRY_PX, "last": _ENTRY_PX,
    }
    eng._underlying_from_occ = lambda _sym: _UNDERLYING
    eng._parse_occ_side = lambda _sym: "CALL"

    eng._load_db_position_row = lambda sym: None
    eng._upsert_broker_position_to_db = MagicMock(
        side_effect=RuntimeError("repair unavailable")
    )

    eng._broker_position_precheck()
    active1 = _active_for_sym(eng, _CONTRACT)
    assert len(active1) == 1 and getattr(active1[0], "broker_repair_degraded", False)
    degraded_id_c1 = str(getattr(active1[0], "position_id", "") or "")

    # Cycle 2: same failure
    eng._broker_position_precheck()
    active2 = _active_for_sym(eng, _CONTRACT)
    assert len(active2) == 1, (
        f"retry failure must not create a second owner; got {len(active2)}: "
        f"{[getattr(p, 'position_id', '?') for p in active2]}"
    )
    still_degraded = active2[0]
    assert str(getattr(still_degraded, "position_id", "")).strip() == degraded_id_c1, (
        "retry failure must retain the EXACT existing degraded position_id — "
        "a new broker-degraded-<uuid> would leak identities every cycle"
    )
    assert getattr(still_degraded, "broker_repair_degraded", False) is True

    _assert_no_broker_writes(broker)


# ─────────────────────────────────────────────────────────────────────────────
# Behavioural — caller-boundary through _check_all_positions
# ─────────────────────────────────────────────────────────────────────────────

def _stub_check_all_positions_collaborators(eng):
    """Fully stub the deterministic collaborators that _check_all_positions
    depends on outside the precheck path we are exercising.  This lets the
    caller-boundary test fail on REAL lifecycle regressions rather than
    tripping on unrelated missing wiring."""
    from datetime import datetime, timezone

    eng._emit_exit_event = lambda *a, **kw: None
    eng.on_exit = None
    eng.on_scale = None
    # Quote monitor / quote-arrived events are optional collaborators used
    # by the exit-loop timing logic; stub as no-ops if the loop consults them.
    eng.quote_monitor = None
    import threading as _t
    eng._quote_arrived_event = _t.Event()
    eng._flattening = _t.Event()
    # Kill switch and master control — precheck+loop consult these.
    eng._kill_switch_fn = None
    eng.master_control = None
    # DB persistence throttle plumbing referenced by the exit loop.
    if not hasattr(eng, "_last_db_persist_ts"):
        eng._last_db_persist_ts = {}
    # Position manager reference (unused when None but read defensively).
    eng._position_manager = None
    # Time helper stubs — no clock drift into rule evaluation.
    eng._now_utc = lambda: datetime.now(timezone.utc)


@_skip_if_no_mod
def test_pr557_amend_caller_boundary_check_all_positions_two_cycles():
    """
    Exercise the real _check_all_positions() caller (not just precheck).

    CYCLE 1: broker OPEN + durable repair unavailable → position is still
             an exit-visible owner after _check_all_positions returns.
    CYCLE 2: durable identity available → same position under durable
             identity, position never disappeared from exit ownership,
             no ENTRY submit, no broad CANCEL, exactly one active owner.

    PR #557 amendment (Blocker 3): production-path failures inside
    _check_all_positions() MUST fail the test.  Only setup-time /
    import-time prerequisites may cause a skip (via @_skip_if_no_mod).
    Do NOT wrap the caller invocation in try/except-skip.
    """
    _stub_ap_db()
    eng = _new_engine()
    broker = _broker_mock()
    eng.broker = broker
    eng._quote_broker = broker
    eng._fetch_broker_quote = lambda _sym: {
        "mark": _ENTRY_PX, "bid": _ENTRY_PX, "ask": _ENTRY_PX,
        "mid": _ENTRY_PX, "last": _ENTRY_PX,
    }
    eng._underlying_from_occ = lambda _sym: _UNDERLYING
    eng._parse_occ_side = lambda _sym: "CALL"
    _stub_check_all_positions_collaborators(eng)

    # Cycle 1 — durable repair fails
    eng._load_db_position_row = lambda sym: None
    eng._upsert_broker_position_to_db = MagicMock(
        side_effect=RuntimeError("repair unavailable")
    )
    # Real invocation — any unexpected runtime error is a FAILURE, not a skip.
    eng._check_all_positions()

    active1 = _active_for_sym(eng, _CONTRACT)
    assert len(active1) == 1, (
        "position must remain exit-visible after _check_all_positions cycle 1"
    )
    degraded_id_c1 = str(getattr(active1[0], "position_id", "") or "")
    assert getattr(active1[0], "broker_repair_degraded", False) is True, (
        "cycle 1 owner must carry broker_repair_degraded=True"
    )
    assert str(getattr(active1[0], "option_symbol", "")).upper() == _CONTRACT
    assert str(getattr(active1[0], "execution_mode", "")).strip().lower() == _MODE
    _assert_no_broker_writes(broker, context="cycle 1 _check_all_positions")

    # Cycle 2 — durable identity available via upsert + row load
    provisional_uuid = f"pos-provisional-{uuid.uuid4()}"
    provisional_row = {
        "id": provisional_uuid,
        "position_id": provisional_uuid,
        "client_id": _CLIENT,
        "contract": _CONTRACT,
        "option_symbol": _CONTRACT,
        "underlying": _UNDERLYING,
        "side": "CALL",
        "direction": "CALL",
        "qty": _BROKER_QTY,
        "quantity_remaining": _BROKER_QTY,
        "entry_price": _ENTRY_PX,
        "avg_fill": _ENTRY_PX,
        "execution_mode": _MODE,
        "status": "OPEN",
        "broker_repair_provisional": True,
        "broker_repair_degraded": False,
    }
    eng._upsert_broker_position_to_db = MagicMock(return_value=provisional_uuid)
    eng._load_db_position_row = lambda sym: (
        provisional_row if sym.upper() == _CONTRACT else None
    )
    # Real invocation — any unexpected runtime error is a FAILURE, not a skip.
    eng._check_all_positions()

    active2 = _active_for_sym(eng, _CONTRACT)
    assert len(active2) == 1, (
        f"cycle 2 must leave exactly one owner; got {len(active2)}"
    )
    surviving = active2[0]
    assert str(getattr(surviving, "position_id", "")).strip() != degraded_id_c1, (
        "convergence failed: degraded id still active after cycle 2"
    )
    assert getattr(surviving, "broker_repair_degraded", False) is False, (
        "cycle 2 owner must have broker_repair_degraded=False"
    )
    assert str(getattr(surviving, "option_symbol", "")).upper() == _CONTRACT
    assert str(getattr(surviving, "execution_mode", "")).strip().lower() == _MODE
    _assert_no_broker_writes(broker, context="cycle 2 _check_all_positions")


# ─────────────────────────────────────────────────────────────────────────────
# Helper API unit tests (Blocker 2)
# ─────────────────────────────────────────────────────────────────────────────

def test_pr557_amend_broker_repair_historical_value_takes_keyword_keys():
    fn = getattr(_EE_MOD, "_broker_repair_historical_value", None)
    if fn is None:
        pytest.skip("_broker_repair_historical_value not exported")
    keys = ("underlying_entry",)
    value, conflict = fn({"underlying_entry": 100.0}, {}, keys=keys)
    assert value == 100.0 and conflict is False
    _, conflict2 = fn({"underlying_entry": 100.0}, {"underlying_entry": 101.0}, keys=keys)
    assert conflict2 is True


def test_pr557_amend_broker_repair_text_value_takes_order_meta_key():
    fn = getattr(_EE_MOD, "_broker_repair_text_value", None)
    if fn is None:
        pytest.skip("_broker_repair_text_value not exported")
    value, conflict = fn({"signal_id": "sig-1"}, {}, "signal_id")
    assert value == "sig-1" and conflict is False
    _, conflict2 = fn({"signal_id": "sig-1"}, {"signal_id": "sig-2"}, "signal_id")
    assert conflict2 is True


# ─────────────────────────────────────────────────────────────────────────────
# Source-pattern anti-regression (always runs)
# ─────────────────────────────────────────────────────────────────────────────

def test_pr557_amend_converge_helper_present_and_wired():
    """The convergence helper must exist and be called from both precheck sites."""
    src = (_REPO / "ap_exit_engine.py").read_text()
    assert "def _converge_degraded_owner_to_durable" in src, (
        "amendment converger method missing"
    )
    # Called at both add_position sites in precheck:
    assert src.count("_converge_degraded_owner_to_durable(sym, pos)") >= 2, (
        "converger must be called from both DB-loaded and post-upsert branches"
    )
    # Log marker present:
    assert "EXIT_BROKER_DEGRADED_OWNER_CONVERGED" in src


def test_pr557_amend_retry_failure_retains_existing_degraded_marker_present():
    """Retry-failure branch must retain the existing degraded owner instead of
    creating a new broker-degraded-<uuid>."""
    src = (_REPO / "ap_exit_engine.py").read_text()
    assert "EXIT_BROKER_DEGRADED_OWNER_RETAINED" in src, (
        "retry-failure retention log marker missing — new broker-degraded-<uuid> "
        "would leak every retry cycle"
    )


def test_pr557_amend_convergence_call_uses_correct_helper_signatures():
    src = (_REPO / "ap_exit_engine.py").read_text()
    assert "EXIT_BROKER_REPAIR_IDENTITY_HOLD" in src
    for pattern in (
        "_entry_geom, _entry_conflict = _broker_repair_historical_value(",
        "_signal_id, _signal_conflict = _broker_repair_text_value(",
        "keys=_BROKER_REPAIR_ENTRY_GEOMETRY_KEYS",
        "keys=_BROKER_REPAIR_STOP_GEOMETRY_KEYS",
        "keys=_BROKER_REPAIR_TARGET_GEOMETRY_KEYS",
    ):
        assert pattern in src, f"convergence call shape regression: missing `{pattern}`"


def test_pr557_amend_degraded_retry_set_included_in_precheck_iteration():
    src = (_REPO / "ap_exit_engine.py").read_text()
    for pattern in (
        "degraded_retry_syms",
        "missing_from_engine_syms",
        'getattr(p, "broker_repair_degraded", False)',
    ):
        assert pattern in src, f"degraded-retry regression: missing `{pattern}`"


def test_pr557_amend_precheck_return_contract_documented():
    """Precheck return contract must be the documented one: True when every
    broker-open position has safe behavior-visible exit ownership."""
    src = (_REPO / "ap_exit_engine.py").read_text()
    assert "EXIT_BROKER_PRECHECK_UNOWNED_POSITIONS" in src, (
        "return-contract log marker missing — degraded-owner install must "
        "not be reported as unowned"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Failure cases — fencing must fail closed; invariant failure must be safe
# ─────────────────────────────────────────────────────────────────────────────

@_skip_if_no_mod
def test_pr557_amend_converger_fails_closed_on_wrong_client():
    """Wrong client_id on incoming → converger returns False, degraded retained."""
    _stub_ap_db()
    eng = _new_engine()
    broker = _broker_mock()
    eng.broker = broker
    eng._quote_broker = broker
    eng._fetch_broker_quote = lambda _sym: {
        "mark": _ENTRY_PX, "bid": _ENTRY_PX, "ask": _ENTRY_PX,
        "mid": _ENTRY_PX, "last": _ENTRY_PX,
    }
    eng._underlying_from_occ = lambda _sym: _UNDERLYING
    eng._parse_occ_side = lambda _sym: "CALL"
    eng._load_db_position_row = lambda sym: None
    eng._upsert_broker_position_to_db = MagicMock(
        side_effect=RuntimeError("cycle1: unavailable")
    )
    eng._broker_position_precheck()
    degraded = _active_for_sym(eng, _CONTRACT)[0]

    # Craft an incoming pos with a foreign client_id and try to converge.
    from types import SimpleNamespace
    incoming = SimpleNamespace(
        position_id=f"pos-foreign-{uuid.uuid4()}",
        option_symbol=_CONTRACT,
        client_id="not_the_runner@example.com",  # wrong client
        execution_mode=_MODE,
        quantity=_BROKER_QTY, quantity_remaining=_BROKER_QTY,
        pending_exit_qty=0, pending_exit_filled_qty=0,
        broker_repair_degraded=False, brokerrepairdegraded=False,
        broker_repair_provisional=True, closed=False,
    )
    result = eng._converge_degraded_owner_to_durable(_CONTRACT, incoming)
    assert result is False, "wrong client_id must fail closed"
    # Degraded owner must still be active with its original id
    still_degraded = _active_for_sym(eng, _CONTRACT)
    assert len(still_degraded) == 1
    assert str(getattr(still_degraded[0], "position_id", "")).strip() == \
        str(getattr(degraded, "position_id", "")).strip()


@_skip_if_no_mod
def test_pr557_amend_converger_fails_closed_on_blank_client():
    """Blank incoming client_id → converger returns False (fail closed)."""
    _stub_ap_db()
    eng = _new_engine()
    broker = _broker_mock()
    eng.broker = broker
    eng._quote_broker = broker
    eng._fetch_broker_quote = lambda _sym: {"mark": _ENTRY_PX, "bid": _ENTRY_PX,
                                             "ask": _ENTRY_PX, "mid": _ENTRY_PX,
                                             "last": _ENTRY_PX}
    eng._underlying_from_occ = lambda _sym: _UNDERLYING
    eng._parse_occ_side = lambda _sym: "CALL"
    eng._load_db_position_row = lambda sym: None
    eng._upsert_broker_position_to_db = MagicMock(side_effect=RuntimeError("unav"))
    eng._broker_position_precheck()

    from types import SimpleNamespace
    incoming = SimpleNamespace(
        position_id=f"pos-blank-{uuid.uuid4()}",
        option_symbol=_CONTRACT,
        client_id="",  # blank — MUST fail closed
        execution_mode=_MODE,
        quantity=_BROKER_QTY, quantity_remaining=_BROKER_QTY,
        pending_exit_qty=0, pending_exit_filled_qty=0,
        broker_repair_degraded=False, brokerrepairdegraded=False,
        broker_repair_provisional=True, closed=False,
    )
    assert eng._converge_degraded_owner_to_durable(_CONTRACT, incoming) is False


@_skip_if_no_mod
def test_pr557_amend_converger_fails_closed_on_mode_mismatch():
    """LIVE/PAPER mismatch → converger returns False (fail closed)."""
    _stub_ap_db()
    eng = _new_engine()
    broker = _broker_mock()
    eng.broker = broker
    eng._quote_broker = broker
    eng._fetch_broker_quote = lambda _sym: {"mark": _ENTRY_PX, "bid": _ENTRY_PX,
                                             "ask": _ENTRY_PX, "mid": _ENTRY_PX,
                                             "last": _ENTRY_PX}
    eng._underlying_from_occ = lambda _sym: _UNDERLYING
    eng._parse_occ_side = lambda _sym: "CALL"
    eng._load_db_position_row = lambda sym: None
    eng._upsert_broker_position_to_db = MagicMock(side_effect=RuntimeError("unav"))
    eng._broker_position_precheck()

    from types import SimpleNamespace
    incoming = SimpleNamespace(
        position_id=f"pos-modemis-{uuid.uuid4()}",
        option_symbol=_CONTRACT,
        client_id=_CLIENT,
        execution_mode="paper",  # engine is live; must not cross
        quantity=_BROKER_QTY, quantity_remaining=_BROKER_QTY,
        pending_exit_qty=0, pending_exit_filled_qty=0,
        broker_repair_degraded=False, brokerrepairdegraded=False,
        broker_repair_provisional=True, closed=False,
    )
    assert eng._converge_degraded_owner_to_durable(_CONTRACT, incoming) is False


@_skip_if_no_mod
def test_pr557_amend_converger_fails_closed_on_invariant_failure():
    """Invariant failure on the merged incoming object → converger returns
    False; existing degraded owner is retained; no ownership swap happened."""
    _stub_ap_db()
    eng = _new_engine()
    broker = _broker_mock()
    eng.broker = broker
    eng._quote_broker = broker
    eng._fetch_broker_quote = lambda _sym: {"mark": _ENTRY_PX, "bid": _ENTRY_PX,
                                             "ask": _ENTRY_PX, "mid": _ENTRY_PX,
                                             "last": _ENTRY_PX}
    eng._underlying_from_occ = lambda _sym: _UNDERLYING
    eng._parse_occ_side = lambda _sym: "CALL"
    eng._load_db_position_row = lambda sym: None
    eng._upsert_broker_position_to_db = MagicMock(side_effect=RuntimeError("unav"))
    eng._broker_position_precheck()
    degraded = _active_for_sym(eng, _CONTRACT)[0]
    degraded_id = str(getattr(degraded, "position_id", "") or "")

    # Craft an incoming that violates the invariant:
    # quantity_remaining > quantity
    from types import SimpleNamespace
    incoming = SimpleNamespace(
        position_id=f"pos-invalid-{uuid.uuid4()}",
        option_symbol=_CONTRACT,
        client_id=_CLIENT,
        execution_mode=_MODE,
        quantity=1, quantity_remaining=99,  # invariant violation
        pending_exit_qty=0, pending_exit_filled_qty=0,
        broker_repair_degraded=False, brokerrepairdegraded=False,
        broker_repair_provisional=True, closed=False,
    )
    assert eng._converge_degraded_owner_to_durable(_CONTRACT, incoming) is False, (
        "invariant failure must abort convergence"
    )
    # Existing degraded owner must remain active with its original id
    still_degraded = _active_for_sym(eng, _CONTRACT)
    assert len(still_degraded) == 1, (
        "invariant-abort must leave exactly one owner (the existing degraded)"
    )
    assert str(getattr(still_degraded[0], "position_id", "")).strip() == degraded_id


def test_pr557_amend_converger_enforces_strict_client_fence_source():
    """Source-pattern anti-regression: converger must NOT use the truthy
    idiom `if _incoming_client and _incoming_client != _runner_client`
    which lets blank client bypass the fence."""
    src = (_REPO / "ap_exit_engine.py").read_text()
    # The converger block must contain strict equality patterns:
    assert "if not _runner_client:" in src, (
        "converger must fail closed when runner client is blank"
    )
    assert "if _incoming_client != _runner_client:" in src, (
        "converger must use strict client equality (not truthy short-circuit)"
    )


def test_pr557_amend_converger_calls_invariant_check_before_swap():
    """Source-pattern anti-regression: converger must call
    _assert_position_invariants with the convergence context BEFORE the
    ownership swap.  If reverted, a merged object could be installed
    without passing invariants."""
    src = (_REPO / "ap_exit_engine.py").read_text()
    marker = '_assert_position_invariants('
    conv_start = src.find("def _converge_degraded_owner_to_durable")
    conv_end = src.find("\n    def ", conv_start + 1)
    conv_body = src[conv_start:conv_end]
    assert marker in conv_body, (
        "converger must call _assert_position_invariants on the merged incoming"
    )
    assert '"degraded_to_durable_convergence"' in conv_body, (
        "converger must pass the documented context string"
    )
    assert "EXIT_BROKER_DEGRADED_OWNER_CONVERGENCE_ABORTED" in conv_body, (
        "converger must log the invariant-abort event"
    )
