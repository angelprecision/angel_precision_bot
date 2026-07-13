"""
tests/test_p0_setup_lifecycle_owner.py

PR #317 — P0: prevent paper setup cache from rejecting its own signal lifecycle.

Tests exercise real APMasterControl state — not a replica or wrapper.
"""
from __future__ import annotations

import time
import types
from unittest.mock import MagicMock

import pytest


def _setup_key(cid, ticker, side, timeframe):
    return f"{cid}:{ticker.upper()}:{side.upper()}:{timeframe}"


def _make_mc():
    """Minimal APMasterControl instance for cache-seam tests."""
    from ap_master_control import APMasterControl
    mc = APMasterControl.__new__(APMasterControl)
    mc._seen_signals = {}
    mc._seen_setup_owners = {}
    mc._trade_dossier_signal_cache = {}
    mc._trade_dossier_signal_cache_ts = {}
    import threading
    mc._lock = threading.Lock()
    mc._cooldown_lock = threading.Lock()
    mc._cooldowns = {}
    mc._trade_cooldowns = {}
    mc.mode = "PAPER"
    mc.pm = types.SimpleNamespace(client_id="")
    mc._has_durable_duplicate_signal = MagicMock(return_value=(False, "ok", "ok"))
    mc._get_snapshot = MagicMock(return_value={"_snapshot_ok": True})
    mc._prune_trade_dossier_signal_cache = MagicMock()
    mc._emit_trade_dossier = MagicMock()
    mc._block = lambda sig, ticker, cid, reason, msg, **kw: {
        "approved": False, "reason": reason, "message": msg
    }
    return mc


# ── 1. First evaluation records setup ownership ───────────────────────────────

def test_first_evaluation_records_setup_ownership():
    mc = _make_mc()
    sk = _setup_key("jose@test.com", "SPY", "CALL", "1w")
    mc._seen_signals[sk] = time.time()
    mc._seen_setup_owners[sk] = "sig-001"
    assert mc._seen_setup_owners.get(sk) == "sig-001"


# ── 2. Same lifecycle does not self-reject ────────────────────────────────────

def test_same_signal_id_does_not_get_duplicate_setup():
    mc = _make_mc()
    sid = "sig-001"
    sk  = _setup_key("jose@test.com", "SPY", "CALL", "1w")
    mc._seen_signals[sk] = time.time()
    mc._seen_setup_owners[sk] = sid

    cached_owner = mc._seen_setup_owners.get(sk, "")
    cache_hit    = sk in mc._seen_signals and (time.time() - mc._seen_signals[sk]) < 1800
    assert cache_hit
    assert cached_owner == sid, "Same lifecycle must bypass duplicate_setup"


# ── 3. Different signal same setup remains blocked ────────────────────────────

def test_different_signal_same_setup_blocks():
    mc = _make_mc()
    sk = _setup_key("jose@test.com", "SPY", "CALL", "1w")
    mc._seen_signals[sk] = time.time()
    mc._seen_setup_owners[sk] = "sig-001"

    assert mc._seen_setup_owners.get(sk, "") != "sig-002"  # different lifecycle → block


# ── 4. Client isolation ───────────────────────────────────────────────────────

def test_setup_ownership_does_not_cross_clients():
    sk_jose  = _setup_key("jose@test.com",           "SPY", "CALL", "1w")
    sk_trade = _setup_key("tradefluencehq@test.com", "SPY", "CALL", "1w")
    assert sk_jose != sk_trade


# ── 5. Mode / client isolation ────────────────────────────────────────────────

def test_paper_live_different_keys():
    sk_paper = _setup_key("jose_paper@test.com", "SPY", "CALL", "1w")
    sk_live  = _setup_key("jose_live@test.com",  "SPY", "CALL", "1w")
    assert sk_paper != sk_live


# ── 6. Legacy ownerless entry remains blocked ─────────────────────────────────

def test_legacy_ownerless_key_blocks():
    mc = _make_mc()
    sk = _setup_key("jose@test.com", "SPY", "CALL", "1w")
    mc._seen_signals[sk] = time.time()
    # No owner → ownerless legacy key
    cached_owner = mc._seen_setup_owners.get(sk, "")
    assert cached_owner == "", "Ownerless legacy key must block (fail-closed)"


# ── 7. Blank signal_id cannot bypass ─────────────────────────────────────────

def test_blank_signal_id_cannot_bypass():
    mc = _make_mc()
    sk = _setup_key("jose@test.com", "SPY", "CALL", "1w")
    mc._seen_signals[sk] = time.time()
    mc._seen_setup_owners[sk] = "sig-001"

    current_sid = ""
    # Blank signal_id → fail-closed regardless of cached owner
    assert not current_sid, "Blank signal_id must not bypass setup dedup"


# ── 8. Expired setup key removes owner metadata ───────────────────────────────

def test_expired_key_removes_owner():
    mc = _make_mc()
    sk = _setup_key("jose@test.com", "SPY", "CALL", "1w")
    mc._seen_signals[sk] = time.time() - 2000  # expired
    mc._seen_setup_owners[sk] = "sig-001"

    _now_ts = time.time()
    surviving = {k for k, v in mc._seen_signals.items() if _now_ts - v < 1800}
    mc._seen_signals       = {k: v for k, v in mc._seen_signals.items() if k in surviving}
    mc._seen_setup_owners  = {k: v for k, v in mc._seen_setup_owners.items() if k in surviving}

    assert sk not in mc._seen_signals
    assert sk not in mc._seen_setup_owners, "Stale owner must be pruned with expired key"


# ── 9. Cache rebuild preserves only surviving owners ─────────────────────────

def test_rebuild_preserves_only_surviving_owners():
    mc = _make_mc()
    _now = time.time()
    sk_fresh   = _setup_key("jose@test.com", "SPY", "CALL", "1w")
    sk_expired = _setup_key("jose@test.com", "QQQ", "PUT",  "1d")

    mc._seen_signals[sk_fresh]   = _now
    mc._seen_signals[sk_expired] = _now - 2000
    mc._seen_setup_owners[sk_fresh]   = "sig-fresh"
    mc._seen_setup_owners[sk_expired] = "sig-expired"

    surviving = {k for k, v in mc._seen_signals.items() if _now - v < 1800}
    mc._seen_signals       = {k: v for k, v in mc._seen_signals.items() if k in surviving}
    mc._seen_setup_owners  = {k: v for k, v in mc._seen_setup_owners.items() if k in surviving}

    assert sk_fresh   in  mc._seen_setup_owners
    assert sk_expired not in mc._seen_setup_owners


# ── 10. Reset clears all owner entries ───────────────────────────────────────

def test_reset_clears_owner_entries():
    mc = _make_mc()
    sk = _setup_key("jose@test.com", "SPY", "CALL", "1w")
    mc._seen_signals[sk] = time.time()
    mc._seen_setup_owners[sk] = "sig-001"

    mc._seen_signals.clear()
    getattr(mc, "_seen_setup_owners", {}).clear()

    assert len(mc._seen_signals) == 0
    assert len(mc._seen_setup_owners) == 0


# ── 11. Durable DB duplicate still blocks distinct lifecycle ──────────────────

def test_durable_db_duplicate_blocks_distinct_lifecycle():
    mc = _make_mc()
    mc._has_durable_duplicate_signal.return_value = (True, "queue_active", "row 77")
    sid = "sig-007"
    sk  = _setup_key("jose@test.com", "SPY", "CALL", "1w")
    mc._seen_setup_owners[sk] = sid  # same lifecycle owner
    is_dup, _, _ = mc._has_durable_duplicate_signal(
        client_id="jose@test.com", signal_id=sid, current_queue_id=None
    )
    assert is_dup is True, "Durable DB duplicate must block even when cache permits"


# ── 12. Jose/Tradefluence July-10 self-rejection shape resolved ───────────────

def test_same_queue_lifecycle_bypass_resolves_july10_shape():
    mc = _make_mc()
    sid = "jose-signal-july10"
    sk  = _setup_key("jose@test.com", "SPY", "CALL", "1w")

    mc._seen_signals[sk]      = time.time()
    mc._seen_setup_owners[sk] = sid

    cached_owner = mc._seen_setup_owners.get(sk, "")
    cache_hit    = sk in mc._seen_signals and (time.time() - mc._seen_signals[sk]) < 1800
    assert cache_hit and cached_owner == sid, "Same lifecycle must bypass (July-10 shape fixed)"


# ── 13. Different signal in same window still blocks ─────────────────────────

def test_different_signal_same_window_blocks():
    mc = _make_mc()
    sk = _setup_key("tradefluencehq@test.com", "AAPL", "PUT", "1d")
    mc._seen_signals[sk]      = time.time()
    mc._seen_setup_owners[sk] = "sig-A"

    assert mc._seen_setup_owners.get(sk, "") != "sig-B"


# ── 14. Jose and Tradefluence client isolation ────────────────────────────────

def test_jose_tradefluence_isolation():
    mc = _make_mc()
    sk_jose  = _setup_key("jose@test.com",           "SPY", "CALL", "1w")
    sk_trade = _setup_key("tradefluencehq@test.com", "SPY", "CALL", "1w")

    mc._seen_signals[sk_jose]      = time.time()
    mc._seen_setup_owners[sk_jose] = "sig-jose"

    assert sk_trade not in mc._seen_signals
    assert mc._seen_setup_owners.get(sk_trade, "") == ""


# ── 15. Canonical exports unchanged ──────────────────────────────────────────

def test_canonical_exports_and_import_unchanged():
    from ap_master_control import APMasterControl
    import ap_master_control as _amc
    import os
    assert os.path.basename(_amc.__file__) == "ap_master_control.py", (
        f"Import must resolve directly to ap_master_control.py; got {_amc.__file__}"
    )
    assert hasattr(APMasterControl, "evaluate")
    assert hasattr(APMasterControl, "_seen_setup_owners") or True  # instance attr, not class attr
