"""P0 regression: reconciler canonical direction identity must be OCC-proven.

The 2026-08-25 incident involved a NOW PUT position.  The pre-fix reconciler
resolved direction as ``pos.get("direction") or pos.get("side") or "CALL"``,
meaning a NULL DB direction column silently manufactured a CALL side and passed
it into canonical exit-engine adoption.  Because ``ManagedPosition.side``
governs stop/target/thesis evaluation direction, this is a capital-loss vector.

These regressions fix the line at the seam; they do not simply test the helper.
"""

from __future__ import annotations

import inspect
import sys
from types import SimpleNamespace

import ap_reconciler as rec


class _FakeManagedPosition:
    """Minimal ManagedPosition stand-in — no DATABASE_URL required.
    Accepts the kwargs that _seed_exit_engine_from_import passes to ManagedPosition()
    and supports every attribute assignment the method makes afterwards.
    """

    def __init__(
        self,
        ticker="",
        option_symbol="",
        side="",
        quantity=0,
        entry_price=0.0,
        underlying_entry=0.0,
        underlying_target=0.0,
        underlying_stop=0.0,
    ):
        self.ticker = ticker
        self.option_symbol = option_symbol
        self.side = side
        self.quantity = quantity
        self.entry_price = entry_price
        self.underlying_entry = underlying_entry
        self.underlying_target = underlying_target
        self.underlying_stop = underlying_stop
        self.position_id = ""
        self.client_id = ""
        self.signal_id = ""
        self.canonical_signal_id = ""
        self.execution_mode = ""
        self.current_option_price = 0.0
        self.price_untrusted = False
        self.underlying_entry_untrusted = False
        self.imported_by_reconciler = False
        self.entry_local_order_id = ""
        self.entry_broker_order_id = ""

CLIENT = "jasoncosby1@gmail.com"

# Actual contracts from the NOW 2026-08-25 incident and a symmetric CALL.
CONTRACT_PUT = "NOW260828P00122000"   # OCC: P → PUT
CONTRACT_CALL = "NOW260828C00122000"  # OCC: C → CALL

# A malformed symbol that cannot prove C/P.
CONTRACT_BAD = "NOWBADCONTRACT"

POSITION_ID = "2fe10f52-e459-4bc5-a57a-1a80e3618040"


# ── helper: _strict_option_side unit tests ──────────────────────────────────

def test_put_contract_with_put_direction_returns_put():
    """Case 1: PUT contract + persisted PUT → PUT."""
    assert rec._strict_option_side(CONTRACT_PUT, "PUT") == "PUT"


def test_call_contract_with_call_direction_returns_call():
    """Case 2: CALL contract + persisted CALL → CALL."""
    assert rec._strict_option_side(CONTRACT_CALL, "CALL") == "CALL"


def test_put_contract_with_missing_direction_derives_put():
    """Case 3: PUT contract + no persisted direction → derive PUT from OCC."""
    assert rec._strict_option_side(CONTRACT_PUT, None) == "PUT"
    assert rec._strict_option_side(CONTRACT_PUT, "") == "PUT"
    assert rec._strict_option_side(CONTRACT_PUT, "  ") == "PUT"


def test_call_contract_with_missing_direction_derives_call():
    """Case 4: CALL contract + no persisted direction → derive CALL from OCC."""
    assert rec._strict_option_side(CONTRACT_CALL, None) == "CALL"
    assert rec._strict_option_side(CONTRACT_CALL, "") == "CALL"


def test_put_contract_persisted_call_fails_closed():
    """Case 5: PUT contract + persisted CALL → conflict → None (fail closed)."""
    assert rec._strict_option_side(CONTRACT_PUT, "CALL") is None
    assert rec._strict_option_side(CONTRACT_PUT, "C") is None


def test_call_contract_persisted_put_fails_closed():
    """Case 6: CALL contract + persisted PUT → conflict → None (fail closed)."""
    assert rec._strict_option_side(CONTRACT_CALL, "PUT") is None
    assert rec._strict_option_side(CONTRACT_CALL, "P") is None


def test_malformed_occ_contract_fails_closed():
    """Case 7: OCC contract unparseable → None regardless of persisted value."""
    for persisted in (None, "CALL", "PUT", ""):
        assert rec._strict_option_side(CONTRACT_BAD, persisted) is None, persisted
    assert rec._strict_option_side("", None) is None
    assert rec._strict_option_side("ONLY3", "CALL") is None


def test_unrecognized_direction_tokens_fail_closed():
    """Case 8: Unrecognized persisted tokens ('BUY', 'P', 'C', 'unknown', blank-like)
    that are NOT exact 'CALL'/'PUT'/'C'/'P' must fail closed even when OCC is valid.
    """
    # 'P' and 'C' ARE recognized single-letter tokens — they normalize correctly.
    assert rec._strict_option_side(CONTRACT_PUT, "P") == "PUT"
    assert rec._strict_option_side(CONTRACT_CALL, "C") == "CALL"

    # These are not recognized and must fail closed.
    for bad in ("BUY", "SELL", "LONG", "SHORT", "1", "0", "true", "unknown", "call ", " put"):
        result = rec._strict_option_side(CONTRACT_PUT, bad)
        assert result is None, f"Expected None for persisted={bad!r}, got {result!r}"


def test_now_incident_shape_with_null_direction_adopts_put():
    """Case 9: Exact NOW incident shape — DB direction=NULL → adopted side must be PUT."""
    result = rec._strict_option_side(CONTRACT_PUT, None)
    assert result == "PUT", f"Incident regression: expected PUT, got {result!r}"
    assert result != "CALL", "CRITICAL: would have seeded wrong directional geometry"


# ── integration: side propagates identically through both adoption paths ─────

class _ExitEngine:
    def __init__(self):
        self.adopt_calls = []
        self.added = []

    def adopt_canonical_position_identity(self, **kwargs):
        self.adopt_calls.append(kwargs)
        return SimpleNamespace(
            disposition="NO_REPAIR_FOUND",
            adopted=False,
            retryable=False,
            safe_to_seed=True,
        )

    def add_position(self, pos):
        self.added.append(pos)


class _Broker:
    execution_mode = "live"


class _OSM:
    pass


class _PM:
    pass


def _reconciler():
    return rec.APBrokerReconciler(
        broker=_Broker(),
        client_id=CLIENT,
        osm=_OSM(),
        pm=_PM(),
        execution_mode="live",
    )


def _pos_row(contract, direction):
    """Canonical DB row with optionally null direction."""
    return {
        "id": POSITION_ID,
        "client_id": CLIENT,
        "execution_mode": "live",
        "underlying": contract[:3],
        "contract": contract,
        "direction": direction,
        "quantity_remaining": 1,
        "avg_fill": 1.30,
        "stop_underlying": 130.44,
        "target_underlying": 124.78,
    }


def test_proven_side_is_identical_in_adopt_call_and_managed_position(monkeypatch):
    """Case 10: Side passed to adopt_canonical_position_identity and ManagedPosition
    must be identical and OCC-proven — both paths must see the same direction.
    """
    reconciler = _reconciler()
    engine = _ExitEngine()
    reconciler.exit_engine = engine
    monkeypatch.setitem(
        sys.modules,
        "ap_exit_engine",
        SimpleNamespace(ManagedPosition=_FakeManagedPosition),
    )

    # Stub out DB calls; provide enough evidence for adoption to proceed.
    evidence = {
        "position_id": POSITION_ID,
        "local_order_id": "lo-1",
        "broker_order_id": "143201293",
        "signal_id": "sig-1",
        "canonical_signal_id": "sig-1",
        "fill_price": 1.30,
        "filled_qty": 1,
        "filled_ts": "2026-08-25T13:54:01.682835+00:00",
        "stop_underlying": 130.44,
        "target_underlying": 124.78,
        "underlying_entry": 127.425,
        "underlying_entry_source": "meta.underlying_entry",
    }
    monkeypatch.setattr(reconciler, "_filled_entry_evidence", lambda *a, **k: evidence)

    # NULL direction in DB row — must derive PUT from OCC.
    reconciler._seed_exit_engine_from_position(_pos_row(CONTRACT_PUT, None))

    assert len(engine.adopt_calls) == 1, "adopt_canonical_position_identity was not called"
    assert len(engine.added) == 1, "ManagedPosition was not seeded"

    adopt_direction = engine.adopt_calls[0].get("direction")
    mp_side = engine.added[0].side

    assert adopt_direction == "PUT", (
        f"adopt call carried wrong direction: {adopt_direction!r}"
    )
    assert mp_side == "PUT", (
        f"ManagedPosition seeded wrong side: {mp_side!r}"
    )
    assert adopt_direction == mp_side, (
        "adopt call and ManagedPosition must carry identical proven direction"
    )


# ── seam-level: missing direction blocks seed before adoption ────────────────

def test_conflict_direction_blocks_seed_without_calling_adopt_or_add(monkeypatch):
    """PUT contract + persisted CALL → direction_unproven → no adopt, no add."""
    reconciler = _reconciler()
    engine = _ExitEngine()
    reconciler.exit_engine = engine
    monkeypatch.setattr(reconciler, "_filled_entry_evidence", lambda *a, **k: {})

    reconciler._seed_exit_engine_from_position(_pos_row(CONTRACT_PUT, "CALL"))

    assert engine.adopt_calls == [], "adopt must not be called when direction conflicts"
    assert engine.added == [], "add_position must not be called when direction conflicts"


def test_malformed_contract_blocks_seed_without_calling_adopt_or_add(monkeypatch):
    """Malformed OCC → direction_unproven → no adopt, no add."""
    reconciler = _reconciler()
    engine = _ExitEngine()
    reconciler.exit_engine = engine
    monkeypatch.setattr(reconciler, "_filled_entry_evidence", lambda *a, **k: {})

    reconciler._seed_exit_engine_from_position(_pos_row(CONTRACT_BAD, None))

    assert engine.adopt_calls == []
    assert engine.added == []


# ── static regression ─────────────────────────────────────────────────────────

def test_static_no_call_default_in_direction_resolution():
    """Sentinel: the literal `or "CALL"` must not appear in canonical direction resolution.

    This guard catches any future edit that re-introduces a silent directional default.
    It reads the source of the seam method, not the entire module, to avoid false
    positives from comments or unrelated code.
    """
    source = inspect.getsource(rec.APBrokerReconciler._seed_exit_engine_from_position)
    assert 'or "CALL"' not in source, (
        'REGRESSION: `or "CALL"` default found in _seed_exit_engine_from_position. '
        "Canonical direction must be OCC-proven; never defaulted."
    )
    assert 'or "PUT"' not in source, (
        'REGRESSION: `or "PUT"` default found in _seed_exit_engine_from_position. '
        "Canonical direction must be OCC-proven; never defaulted."
    )
