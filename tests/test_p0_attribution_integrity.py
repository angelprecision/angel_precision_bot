# tests/test_p0_attribution_integrity.py
# P0: lineage recovery for broker-imported positions.
#
# Evidence base (live Supabase, 2026-07-04): 118/178 closed positions (66%)
# unattributed (BROKER_IMPORT / blank / NULL pattern); 78/132 BROKER_IMPORT
# positions had a matching real ENTRY order for the same client+contract
# within 21 days — attribution existed and the import path discarded it.
#
# Invariants:
#   T1  Recovery happy path: matching ENTRY order + signal pattern →
#       ImportIdentity carries REAL signal_id + REAL pattern, attributed=True,
#       provenance preserved (plan_id keeps 'reconciled:' prefix).
#   T2  Fail-closed: DB error / no match / blank pattern → byte-compatible
#       fabricated identity (BROKER_IMPORT[_PRICE_UNTRUSTED], reconciled:*
#       signal_id), attributed=False. Never raises.
#   T3  Never invent attribution: matched order whose signal row lacks a
#       pattern → NO recovery (invariant 3, the FALLBACK_DEFAULT lesson).
#   T4  Price-trust separation: price_untrusted + recovered lineage → real
#       pattern with attribution_source='recovered_price_untrusted';
#       price_untrusted + no lineage → BROKER_IMPORT_PRICE_UNTRUSTED.
#   T5  SQL shape: ENTRY matched case-insensitively (kind values are
#       uppercase in production), fabricated 'reconciled:%' excluded, FILLED
#       ranked first, composite (signal_id::text, client_email) join for the
#       pattern, psycopg2 %s placeholders throughout.

from __future__ import annotations

import sys
import types
from typing import Any, Optional

import pytest

import ap.attribution_integrity as ai
from ap.attribution_integrity import (
    ImportIdentity,
    canonical_signal_id_for_lookup,
    import_identity,
    recover_lineage,
)


class _FakeCursor:
    """Scripted cursor: pops one canned result per execute()."""
    def __init__(self, script):
        self.script = list(script)
        self.executed: list[tuple[str, tuple]] = []
        self._current: Optional[Any] = None

    def execute(self, sql, params):
        self.executed.append((sql, params))
        self._current = self.script.pop(0) if self.script else None

    def fetchone(self):
        return self._current

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _wire(monkeypatch, script):
    cur = _FakeCursor(script)
    db_mod = types.ModuleType("ap.db")
    db_mod.conn = lambda: cur
    db_mod.run_with_retry = lambda fn: fn()
    monkeypatch.setitem(sys.modules, "ap.db", db_mod)
    return cur


# ── T1: happy path ───────────────────────────────────────────────────────────

def test_t1_recovery_returns_real_lineage(monkeypatch):
    cur = _wire(monkeypatch, [
        {"order_id": "ord-123", "order_signal_id": "3f6a9c1e-1111-2222-3333-444455556666", "order_status": "FILLED"},
        {"pattern": "2-3"},
    ])
    ident = import_identity(
        contract="GOOGL260710C00360000",
        client_id="jasoncosby1@gmail.com",
    )
    assert ident.attributed is True
    assert ident.signal_id == "3f6a9c1e-1111-2222-3333-444455556666"
    assert ident.pattern == "2-3"
    assert ident.attribution_source == "recovered"
    assert ident.matched_order_id == "ord-123"
    assert ident.matched_order_status == "FILLED"
    assert ident.plan_id.startswith("reconciled:GOOGL260710C00360000:")  # provenance


# ── T2: fail-closed to current behavior ──────────────────────────────────────

def test_t2a_no_matching_order_fabricates(monkeypatch):
    _wire(monkeypatch, [None])
    ident = import_identity(contract="SPY260717P00550000", client_id="c@x.com")
    assert ident.attributed is False
    assert ident.pattern == "BROKER_IMPORT"
    assert ident.signal_id.startswith("reconciled:SPY260717P00550000:")
    assert len(ident.signal_id.split(":")[-1]) == 8   # uuid8, byte-compatible
    assert ident.attribution_source == "fabricated"


def test_t2b_db_error_fabricates_never_raises(monkeypatch):
    db_mod = types.ModuleType("ap.db")

    def _boom():
        raise RuntimeError("supabase down")

    db_mod.conn = _boom
    db_mod.run_with_retry = lambda fn: fn()
    monkeypatch.setitem(sys.modules, "ap.db", db_mod)
    ident = import_identity(contract="QQQ260710C00500000", client_id="c@x.com")
    assert ident.attributed is False
    assert ident.pattern == "BROKER_IMPORT"


def test_t2c_blank_inputs_fabricate():
    assert recover_lineage(contract="", client_id="c@x.com") is None
    assert recover_lineage(contract="X", client_id="") is None


# ── T3: never invent attribution ─────────────────────────────────────────────

def test_t3_matched_order_without_pattern_does_not_recover(monkeypatch):
    _wire(monkeypatch, [
        {"order_id": "ord-9", "order_signal_id": "aaaa1111-2222-3333-4444-555566667777", "order_status": "FILLED"},
        {"pattern": None},            # signal row exists, pattern empty
    ])
    ident = import_identity(contract="TSLA260710C00250000", client_id="c@x.com")
    assert ident.attributed is False
    assert ident.pattern == "BROKER_IMPORT"


# ── T4: price-trust separation ───────────────────────────────────────────────

def test_t4a_recovered_with_untrusted_price_keeps_real_pattern(monkeypatch):
    _wire(monkeypatch, [
        {"order_id": "ord-7", "order_signal_id": "bbbb1111-2222-3333-4444-555566667777", "order_status": "SUBMITTED"},
        {"pattern": "FAILED_DIR_2U_30min+60min"},
    ])
    ident = import_identity(
        contract="GOOGL260710P00355000", client_id="c@x.com", price_untrusted=True
    )
    assert ident.attributed is True
    assert ident.pattern == "FAILED_DIR_2U_30min+60min"
    assert ident.attribution_source == "recovered_price_untrusted"


def test_t4b_unrecovered_untrusted_price_keeps_legacy_pattern(monkeypatch):
    _wire(monkeypatch, [None])
    ident = import_identity(contract="X", client_id="c@x.com", price_untrusted=True)
    assert ident.pattern == "BROKER_IMPORT_PRICE_UNTRUSTED"


# ── T5: SQL shape ────────────────────────────────────────────────────────────

def test_t5_sql_shape(monkeypatch):
    cur = _wire(monkeypatch, [
        {"order_id": "ord-1", "order_signal_id": "cccc1111-2222-3333-4444-555566667777", "order_status": "FILLED"},
        {"pattern": "3-2-2"},
    ])
    recover_lineage(contract="AAPL260710C00230000",
                    client_id="jasoncosby1@gmail.com", lookback_days=21)
    order_sql, order_params = cur.executed[0]
    assert "upper(o.kind) = 'ENTRY'" in order_sql          # prod kinds uppercase
    assert "NOT LIKE %s" in order_sql                       # fabricated excluded
    assert "(o.status = 'FILLED') DESC" in order_sql        # FILLED ranked first
    assert "%s" in order_sql and "%(" not in order_sql      # psycopg2 positional
    assert order_params[0] == "jasoncosby1@gmail.com"       # full email, not slug
    assert order_params[2] == "reconciled:%"

    sig_sql, sig_params = cur.executed[1]
    assert "signal_id::text = %s" in sig_sql
    assert "client_email = %s" in sig_sql                   # composite key (#260)
    assert sig_params[1] == "jasoncosby1@gmail.com"


def test_dict_row_pattern_lookup_uses_alias(monkeypatch):
    cur = _wire(monkeypatch, [
        {
            "order_id": "ord-alias",
            "order_signal_id": "eeee1111-2222-3333-4444-555566667777",
            "order_status": "SUBMITTED",
        },
        {"pattern": "3-1-2"},
    ])
    lineage = recover_lineage(
        contract="MSFT260710C00400000",
        client_id="client@example.com",
        lookback_days=21,
    )
    assert lineage == {
        "signal_id": "eeee1111-2222-3333-4444-555566667777",
        "pattern": "3-1-2",
        "order_id": "ord-alias",
        "order_status": "SUBMITTED",
        "order_signal_id": "eeee1111-2222-3333-4444-555566667777",
    }
    assert "AS order_id" in cur.executed[0][0]
    assert "AS order_signal_id" in cur.executed[0][0]
    assert "AS order_status" in cur.executed[0][0]
    assert "AS pattern" in cur.executed[1][0]


def test_tuple_fixture_still_supported(monkeypatch):
    _wire(monkeypatch, [
        ("ord-legacy", "dddd1111-2222-3333-4444-555566667777", "FILLED"),
        ("1-2_2D",),
    ])
    ident = import_identity(contract="AMD260710C00150000", client_id="legacy@example.com")
    assert ident.attributed is True
    assert ident.signal_id == "dddd1111-2222-3333-4444-555566667777"
    assert ident.pattern == "1-2_2D"


def test_reeval_wrapped_order_signal_id_looks_up_bare_uuid_pattern(monkeypatch):
    bare_uuid = "603bce5b-352e-44e0-b00b-6baa826ca2c9"
    cur = _wire(monkeypatch, [
        {
            "order_id": "ord-reeval",
            "order_signal_id": f"REEVAL:{bare_uuid}:f4dc44",
            "order_status": "FILLED",
        },
        {"pattern": "2-1-2"},
    ])
    ident = import_identity(contract="UNH260710C00500000", client_id="reeval@example.com")
    assert ident.attributed is True
    assert ident.signal_id == bare_uuid
    assert ident.pattern == "2-1-2"
    sig_sql, sig_params = cur.executed[1]
    assert "signal_id::text = %s" in sig_sql
    assert sig_params[0] == bare_uuid


def test_canonical_signal_id_for_lookup_strips_reeval_suffix():
    bare_uuid = "603bce5b-352e-44e0-b00b-6baa826ca2c9"
    assert canonical_signal_id_for_lookup(f"REEVAL:{bare_uuid}:f4dc44") == bare_uuid
    assert canonical_signal_id_for_lookup(f"REEVAL:{bare_uuid}") == bare_uuid
    assert canonical_signal_id_for_lookup(bare_uuid) == bare_uuid


# ── reconciler wiring smoke: patched call sites import cleanly ───────────────

def test_reconciler_imports_with_patch():
    import importlib
    mod = importlib.import_module("ap_reconciler")
    src = open(mod.__file__).read()
    assert "from ap.attribution_integrity import import_identity" in src
    assert 'pattern="BROKER_IMPORT_PRICE_UNTRUSTED"' not in src  # fabrication removed
    assert "imported_pattern" in src
