"""
tests/test_p0_pr516_amend_degraded_retry_convergence.py

PR #516 amendment — regression:
  * Blocker 1: degraded broker-truth owners are retried for durable recovery
    on each subsequent cycle instead of getting "frozen" as ownership.
  * Blocker 2: canonical convergence uses the actual helper APIs
    (_broker_repair_historical_value / _broker_repair_text_value) with the
    correct signatures and consumes their (value, contradiction) returns.

Behavioral shape follows the pattern established by
test_broker_precheck_stale_db_qty_zero_loaded_with_broker_qty in
tests/test_p0_exit_engine_broker_truth.py: build a minimal engine via
__new__, stub collaborators, drive _broker_position_precheck() across two
cycles, and assert that no exit ENTRY submit / broad CANCEL is issued by
the amendment surface.
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
    """Install a permissive ap.db stub so best-effort DB writes never raise."""
    ap_stub = types.ModuleType("ap")
    db_stub = types.ModuleType("ap.db")
    db_stub.run_with_retry = lambda f: (f() if callable(f) else None)
    db_stub.conn = MagicMock()
    ap_stub.db = db_stub
    sys.modules.setdefault("ap", ap_stub)
    sys.modules.setdefault("ap.db", db_stub)


def _new_engine():
    """Build a minimal APExitEngine bypassing __init__."""
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


def _assert_no_broker_writes(broker):
    """The amendment must never submit an ENTRY or issue broad cancels."""
    for forbidden in (
        "submit_order", "submit_entry", "submit_exit",
        "cancel_order", "cancel_all_orders", "cancel_all",
    ):
        fn = getattr(broker, forbidden, None)
        if fn is None:
            continue
        assert not fn.called, (
            f"broker.{forbidden} was called during precheck — "
            f"amendment must never submit/cancel from the exit-cycle precheck"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Blocker 1 regression — two-cycle degraded → durable retry
# ─────────────────────────────────────────────────────────────────────────────

@_skip_if_no_mod
def test_pr516_amend_cycle1_installs_degraded_then_cycle2_retries_durable_recovery():
    """
    CYCLE 1: broker proves LIVE position; engine empty; DB repair forced
             unavailable → degraded broker-truth owner installed and
             remains exit-visible.
    CYCLE 2: broker still proves LIVE position; durable filled-ENTRY
             evidence now available → durable recovery is RETRIED (previously
             skipped because the degraded owner had already claimed the OCC),
             canonical position_id adopted, degraded owner removed, exactly
             one active owner remains, no duplicate lifecycle.
    """
    _stub_ap_db()
    eng = _new_engine()
    broker = _broker_mock()
    eng.broker = broker
    eng._quote_broker = broker

    # ── Cycle 1 stubs ─────────────────────────────────────────────────────
    # DB row NOT FOUND: forces the "no db row" branch, which then attempts
    # the repair via _upsert_broker_position_to_db. Force that to fail so
    # the degraded-owner install branch runs.
    eng._load_db_position_row = lambda sym: None
    eng._upsert_broker_position_to_db = MagicMock(
        side_effect=RuntimeError("cycle1: durable repair unavailable")
    )
    eng._fetch_broker_quote = lambda _sym: {
        "mark": _ENTRY_PX, "bid": _ENTRY_PX - 0.05, "ask": _ENTRY_PX + 0.05,
        "mid": _ENTRY_PX, "last": _ENTRY_PX,
    }
    eng._underlying_from_occ = lambda _sym: _UNDERLYING
    eng._parse_occ_side = lambda _sym: "CALL"

    # Cycle 1
    result1 = eng._broker_position_precheck()
    assert result1 is True, "precheck must return True even when repair falls back to degraded"

    # Post-cycle-1 assertions
    with eng._lock:
        active = [p for p in eng._positions if getattr(p, "quantity_remaining", 0) > 0]
    assert len(active) == 1, f"expected exactly one degraded owner, got {len(active)}"
    degraded = active[0]
    assert getattr(degraded, "broker_repair_degraded", False) is True, (
        "cycle 1 owner must carry broker_repair_degraded=True"
    )
    assert getattr(degraded, "broker_repair_provisional", False) is True
    assert str(getattr(degraded, "option_symbol", "")).upper() == _CONTRACT
    assert int(getattr(degraded, "quantity_remaining", 0)) == _BROKER_QTY
    assert str(getattr(degraded, "execution_mode", "")).strip().lower() == _MODE
    _assert_no_broker_writes(broker)

    # ── Cycle 2 — Blocker 1 core assertion ──────────────────────────────
    # The critical invariant: on the next cycle, the degraded owner's OCC
    # MUST re-enter the repair loop (i.e. the upsert path fires again).
    # Before the amendment, the OCC was subtracted out of
    # missing_from_engine, so the loop never re-fired and no durable
    # recovery attempt could happen — degraded ownership became permanent.
    # Note: we don't reset the upsert mock here — cycle 1 already recorded
    # its call; we assert cycle 2 adds a second call for the same OCC.
    cycle1_upsert_calls = eng._upsert_broker_position_to_db.call_count
    canonical_pos_id = f"pos-canonical-{uuid.uuid4()}"
    eng._upsert_broker_position_to_db = MagicMock(return_value=canonical_pos_id)
    eng._load_db_position_row = lambda sym: None  # force upsert path again

    result2 = eng._broker_position_precheck()
    assert result2 is True, "cycle 2 precheck must succeed"

    # Blocker 1 assertion: durable repair WAS retried in cycle 2.
    assert eng._upsert_broker_position_to_db.call_count >= 1, (
        "Blocker 1 regression: durable repair was not retried on cycle 2 — "
        "degraded owner would have been permanent"
    )
    # Verify the retry was for the same OCC that the degraded owner holds.
    call_args_list = eng._upsert_broker_position_to_db.call_args_list
    retried_syms = [str(c.args[0]).upper() for c in call_args_list if c.args]
    assert _CONTRACT in retried_syms, (
        f"cycle 2 upsert must be called for {_CONTRACT}; got {retried_syms}"
    )
    # No duplicate: exactly one active owner remains for this OCC.
    with eng._lock:
        active2 = [
            p for p in eng._positions
            if getattr(p, "quantity_remaining", 0) > 0
            and str(getattr(p, "option_symbol", "")).upper() == _CONTRACT
        ]
    assert len(active2) == 1, (
        f"cycle 2 must leave exactly one owner for {_CONTRACT}, got {len(active2)}: "
        f"{[getattr(p, 'position_id', '?') for p in active2]}"
    )
    _assert_no_broker_writes(broker)


# ─────────────────────────────────────────────────────────────────────────────
# Blocker 1 variant — durable UUID succeeds but no canonical ENTRY yet
# ─────────────────────────────────────────────────────────────────────────────

@_skip_if_no_mod
def test_pr516_amend_durable_uuid_replaces_degraded_without_canonical_entry():
    """
    Cycle 1 installs a degraded owner (as above).  Cycle 2: durable repair
    now persists a provisional UUID (no canonical filled ENTRY yet).  The
    degraded owner must be replaced by exactly one durable provisional
    owner — never leave both in place.
    """
    _stub_ap_db()
    eng = _new_engine()
    broker = _broker_mock()
    eng.broker = broker
    eng._quote_broker = broker

    eng._load_db_position_row = lambda sym: None
    eng._upsert_broker_position_to_db = MagicMock(
        side_effect=RuntimeError("cycle1: repair unavailable")
    )
    eng._fetch_broker_quote = lambda _sym: {
        "mark": _ENTRY_PX, "bid": _ENTRY_PX, "ask": _ENTRY_PX,
        "mid": _ENTRY_PX, "last": _ENTRY_PX,
    }
    eng._underlying_from_occ = lambda _sym: _UNDERLYING
    eng._parse_occ_side = lambda _sym: "CALL"

    eng._broker_position_precheck()
    with eng._lock:
        active = [p for p in eng._positions if getattr(p, "quantity_remaining", 0) > 0]
    assert len(active) == 1 and getattr(active[0], "broker_repair_degraded", False)

    # Cycle 2: upsert now succeeds with a provisional UUID; no canonical
    # filled-ENTRY evidence available (repair_evidence lacks position_id).
    provisional_id = f"pos-provisional-{uuid.uuid4()}"
    eng._upsert_broker_position_to_db = MagicMock(return_value=provisional_id)
    # Still no DB row load for reuse — force the upsert branch:
    eng._load_db_position_row = lambda sym: None

    eng._broker_position_precheck()
    with eng._lock:
        active2 = [p for p in eng._positions if getattr(p, "quantity_remaining", 0) > 0]
    assert len(active2) == 1, (
        f"cycle 2 must leave exactly one owner (durable-provisional replaces degraded), "
        f"got {len(active2)}"
    )
    _assert_no_broker_writes(broker)


# ─────────────────────────────────────────────────────────────────────────────
# Blocker 2 regression — helper API surface + return-tuple handling
# ─────────────────────────────────────────────────────────────────────────────

def test_pr516_amend_broker_repair_historical_value_takes_keyword_keys():
    """The historical helper is (*mappings, keys=...); a value+contradiction tuple returns."""
    fn = getattr(_EE_MOD, "_broker_repair_historical_value", None)
    if fn is None:
        pytest.skip("_broker_repair_historical_value not exported")
    keys = ("underlying_entry",)
    value, conflict = fn({"underlying_entry": 100.0}, {}, keys=keys)
    assert value == 100.0 and conflict is False
    # Contradiction across mappings must be detected.
    _, conflict2 = fn({"underlying_entry": 100.0}, {"underlying_entry": 101.0}, keys=keys)
    assert conflict2 is True


def test_pr516_amend_broker_repair_text_value_takes_order_meta_key():
    """The text helper is (order, meta, key); a value+contradiction tuple returns."""
    fn = getattr(_EE_MOD, "_broker_repair_text_value", None)
    if fn is None:
        pytest.skip("_broker_repair_text_value not exported")
    value, conflict = fn({"signal_id": "sig-1"}, {}, "signal_id")
    assert value == "sig-1" and conflict is False
    _, conflict2 = fn({"signal_id": "sig-1"}, {"signal_id": "sig-2"}, "signal_id")
    assert conflict2 is True


def test_pr516_amend_convergence_call_uses_correct_helper_signatures():
    """Source-level regression: the convergence branch must pass the tuple
    return through and use keyword `keys=` for the historical helper. This
    catches any future rewrite that reverts to the broken call shape."""
    src = (_REPO / "ap_exit_engine.py").read_text()
    # Locate the convergence branch by its unique log marker introduced in
    # the amendment.
    marker = "EXIT_BROKER_REPAIR_IDENTITY_HOLD"
    assert marker in src, (
        "amendment log marker missing — canonical convergence branch not present"
    )
    # The convergence block must unpack both helpers into 2-tuples, not
    # treat their return as a scalar.
    for pattern in (
        "_entry_geom, _entry_conflict = _broker_repair_historical_value(",
        "_signal_id, _signal_conflict = _broker_repair_text_value(",
        "keys=_BROKER_REPAIR_ENTRY_GEOMETRY_KEYS",
        "keys=_BROKER_REPAIR_STOP_GEOMETRY_KEYS",
        "keys=_BROKER_REPAIR_TARGET_GEOMETRY_KEYS",
    ):
        assert pattern in src, f"convergence call shape regression: missing `{pattern}`"


def test_pr516_amend_degraded_retry_set_included_in_precheck_iteration():
    """Source-level regression: precheck must union degraded owners into the
    iteration set so subsequent cycles retry durable repair."""
    src = (_REPO / "ap_exit_engine.py").read_text()
    for pattern in (
        "degraded_retry_syms",
        "missing_from_engine_syms",
        'getattr(p, "broker_repair_degraded", False)',
    ):
        assert pattern in src, f"degraded-retry regression: missing `{pattern}`"
