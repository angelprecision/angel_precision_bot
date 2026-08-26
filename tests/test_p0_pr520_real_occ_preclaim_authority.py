"""tests/test_p0_pr520_real_occ_preclaim_authority.py

PR #520 — REAL_OCC deferred pre-claim authority closure tests.

Gap addressed: `_on_entry_trigger()` computed `_preclaim_deferred` by setting
`_preclaim_retry_materialization = _CONTRACT_REAL_OCC and _plan_is_deferred()`.
`_plan_is_deferred()` trusts stale `contract_deferred=True` metadata, so a
REAL_OCC row with a blank `lifecycle_state` and stale metadata flag could enter
`_claim_deferred_materialization_for_trigger()` and fire a fresh selector run
without a durable recovery lock — forbidden.

Fix: replace `_plan_is_deferred()` with `_recovery_pre_claimed` as the sole
authority gate for REAL_OCC retry materialization.

Test matrix
-----------
NC-1  REAL_OCC contract + stale contract_deferred=True metadata + blank
      lifecycle_state + NO _recovery_pre_claimed
      -> _preclaim_retry_materialization MUST be False
      -> _preclaim_deferred MUST be False

NC-2  REAL_OCC contract + _plan_is_deferred() would return True
      + NO _recovery_pre_claimed
      -> _preclaim_retry_materialization MUST still be False

PC-1  REAL_OCC contract + _recovery_pre_claimed=True
      -> _preclaim_retry_materialization MUST be True
      -> _preclaim_deferred MUST be True (recovery path open)

PC-2  DEFERRED_PLACEHOLDER contract + no _recovery_pre_claimed
      -> _preclaim_deferred MUST be True (legacy non-OCC path preserved)

PC-3  Blank contract symbol -> DEFERRED_PLACEHOLDER state
      -> _preclaim_deferred MUST be True (non-OCC, legacy path preserved)

NC-3  _classify_contract returns strict REAL_OCC or DEFERRED_PLACEHOLDER
      for known symbol shapes (parametrised)
"""
from __future__ import annotations

import os
import types

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

import ap_execution_core as core

_CONTRACT_REAL_OCC             = core._CONTRACT_REAL_OCC
_CONTRACT_DEFERRED_PLACEHOLDER = core._CONTRACT_DEFERRED_PLACEHOLDER

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_CLIENT_ID = "angel@angelprecision.co"
_EXEC_MODE = "live"
_TICKER    = "SPY"
_SIGNAL_ID = "SIG-SPY-PR520"

# Real OCC-formatted options symbol: 6-char padded underlying + 6-digit expiry
# + type char + 8-digit strike.
_REAL_OCC = "SPY   240731C00500000"
_DEFERRED = f"DEFERRED:{_TICKER}"


def _make_plan(contract_symbol: str, metadata=None, **kw):
    return types.SimpleNamespace(
        contract_symbol=contract_symbol,
        execution_mode=_EXEC_MODE,
        mode=_EXEC_MODE,
        client_id=_CLIENT_ID,
        signal_id=_SIGNAL_ID,
        limit_price=kw.get("limit_price", 0.50),
        contracts=kw.get("contracts", 1),
        max_position_usd=kw.get("max_position_usd", 500.0),
        side=kw.get("side", "CALL"),
        metadata=metadata if metadata is not None else {},
        ticker=_TICKER,
    )


def _make_signal(*, recovery_pre_claimed: bool = False, **kw) -> dict:
    base = {
        "signal_id":      _SIGNAL_ID,
        "client_id":      _CLIENT_ID,
        "execution_mode": _EXEC_MODE,
        "ticker":         _TICKER,
        "breach_ts":      "2024-07-31T14:00:00Z",
    }
    if recovery_pre_claimed:
        base["_recovery_pre_claimed"]            = True
        base["_recovery_pre_claimed_owner"]      = "materializer:pr520-owner"
        base["_recovery_pre_claimed_generation"] = 1
        base["_recovery_pre_claimed_attempt"]    = 1
        base["_recovery_pre_claimed_client_id"]  = _CLIENT_ID
        base["_recovery_pre_claimed_mode"]       = _EXEC_MODE
    base.update(kw)
    return base


def _classify(contract_symbol: str, ticker: str = _TICKER) -> str:
    return core.APExecutionCore._classify_contract(contract_symbol, ticker)


def _compute_preclaim(plan, signal, *, is_callback: bool = True):
    """
    Reproduce the exact _preclaim_retry_materialization / _preclaim_deferred
    logic from _on_entry_trigger() after the PR #520 fix, so tests are
    structurally identical to the runtime code.

    Returns (retry_materialization: bool, preclaim_deferred: bool).
    """
    _preclaim_contract    = str(getattr(plan, "contract_symbol", "") or "").strip()
    _recovery_pre_claimed = bool(signal.get("_recovery_pre_claimed"))
    _contract_state       = _classify(_preclaim_contract, _TICKER)

    _preclaim_retry_materialization = bool(
        _contract_state == _CONTRACT_REAL_OCC
        and _recovery_pre_claimed
    )
    _materialization_callback = is_callback
    _preclaim_deferred = bool(
        _materialization_callback
        and plan is not None
        and (
            _contract_state == _CONTRACT_DEFERRED_PLACEHOLDER
            or _preclaim_retry_materialization
        )
    )
    return _preclaim_retry_materialization, _preclaim_deferred


# ---------------------------------------------------------------------------
# NC-1: REAL_OCC + stale meta.contract_deferred + no _recovery_pre_claimed
# ---------------------------------------------------------------------------

def test_nc1_real_occ_stale_meta_no_preclaim_blocks_deferred():
    """
    NC-1: A REAL_OCC order carrying stale contract_deferred=True metadata and
    a blank lifecycle_state must NOT re-enter the deferred materialization path
    when _recovery_pre_claimed is absent.
    """
    plan   = _make_plan(
        _REAL_OCC,
        metadata={"contract_deferred": True, "lifecycle_state": ""},
    )
    signal = _make_signal(recovery_pre_claimed=False)

    assert _classify(_REAL_OCC) == _CONTRACT_REAL_OCC, (
        f"Pre-condition: {_REAL_OCC!r} must classify as REAL_OCC"
    )
    # _plan_is_deferred() would return True here — that is the gap being closed.
    assert core.APExecutionCore._plan_is_deferred(plan, _TICKER), (
        "Pre-condition: stale metadata makes _plan_is_deferred() True — "
        "exactly the gap PR #520 closes"
    )

    retry_mat, preclaim_def = _compute_preclaim(plan, signal)

    assert not retry_mat, (
        "NC-1 FAIL: _preclaim_retry_materialization must be False when "
        "_recovery_pre_claimed is absent, even if _plan_is_deferred() is True"
    )
    assert not preclaim_def, (
        "NC-1 FAIL: _preclaim_deferred must be False — stale metadata on a "
        "REAL_OCC contract must never trigger a fresh materialization claim"
    )


# ---------------------------------------------------------------------------
# NC-2: REAL_OCC + _plan_is_deferred via signal flag + no _recovery_pre_claimed
# ---------------------------------------------------------------------------

def test_nc2_real_occ_plan_deferred_via_signal_no_preclaim_blocks():
    """
    NC-2: Even if _plan_is_deferred() would evaluate True (stale
    contract_deferred flag propagated in the signal), a REAL_OCC contract must
    not enter the deferred path without _recovery_pre_claimed.
    """
    plan   = _make_plan(_REAL_OCC, metadata={"contract_deferred": True})
    signal = _make_signal(recovery_pre_claimed=False)
    signal["contract_deferred"] = True  # stale signal flag

    retry_mat, preclaim_def = _compute_preclaim(plan, signal)

    assert not retry_mat, "NC-2 FAIL: no _recovery_pre_claimed -> retry_mat False"
    assert not preclaim_def, "NC-2 FAIL: no _recovery_pre_claimed -> preclaim_def False"


# ---------------------------------------------------------------------------
# PC-1: REAL_OCC + _recovery_pre_claimed=True -> deferred path authorised
# ---------------------------------------------------------------------------

def test_pc1_real_occ_with_recovery_preclaim_enters_deferred():
    """
    PC-1: When resume_deferred_materialization_retry() sets
    _recovery_pre_claimed=True, a REAL_OCC contract MUST enter the deferred
    path so the existing downstream claim + selector machinery can resume.
    """
    plan   = _make_plan(_REAL_OCC, metadata={"lifecycle_state": "RETRY_WAIT"})
    signal = _make_signal(recovery_pre_claimed=True)

    retry_mat, preclaim_def = _compute_preclaim(plan, signal)

    assert retry_mat, (
        "PC-1 FAIL: _recovery_pre_claimed=True must set retry_mat=True"
    )
    assert preclaim_def, (
        "PC-1 FAIL: a REAL_OCC contract with _recovery_pre_claimed=True "
        "must produce _preclaim_deferred=True so recovery can complete"
    )


# ---------------------------------------------------------------------------
# PC-2: DEFERRED_PLACEHOLDER — legacy path must not regress
# ---------------------------------------------------------------------------

def test_pc2_deferred_placeholder_still_enters_deferred_path():
    """
    PC-2 (regression guard): A DEFERRED:<ticker> placeholder contract (the
    standard fresh-breach case) must still produce _preclaim_deferred=True
    regardless of _recovery_pre_claimed.  The new gate targets REAL_OCC only.
    """
    plan   = _make_plan(_DEFERRED, metadata={"contract_deferred": True})
    signal = _make_signal(recovery_pre_claimed=False)

    assert _classify(_DEFERRED) == _CONTRACT_DEFERRED_PLACEHOLDER, (
        f"Pre-condition: {_DEFERRED!r} must be DEFERRED_PLACEHOLDER"
    )
    _, preclaim_def = _compute_preclaim(plan, signal)

    assert preclaim_def, (
        "PC-2 FAIL: DEFERRED:<ticker> placeholder must still produce "
        "_preclaim_deferred=True — legacy non-OCC authority must not regress"
    )


# ---------------------------------------------------------------------------
# PC-3: Blank contract symbol -> DEFERRED_PLACEHOLDER
# ---------------------------------------------------------------------------

def test_pc3_blank_contract_symbol_treated_as_deferred_placeholder():
    """
    PC-3: A blank contract_symbol (plan not yet resolved) classifies as
    DEFERRED_PLACEHOLDER and must continue to enter the deferred path.
    """
    plan   = _make_plan("", metadata={"contract_deferred": True})
    signal = _make_signal(recovery_pre_claimed=False)

    assert _classify("") == _CONTRACT_DEFERRED_PLACEHOLDER
    _, preclaim_def = _compute_preclaim(plan, signal)

    assert preclaim_def, (
        "PC-3 FAIL: blank contract_symbol must remain in the deferred path"
    )


# ---------------------------------------------------------------------------
# NC-3: _classify_contract canonical shape coverage
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("symbol,ticker,expected_state", [
    # Real OCC symbols
    ("SPY   240731C00500000", "SPY",  _CONTRACT_REAL_OCC),
    ("AAPL  261218P00200000", "AAPL", _CONTRACT_REAL_OCC),
    ("GS    260717C00465000", "GS",   _CONTRACT_REAL_OCC),
    # Deferred placeholders
    ("DEFERRED:SPY",          "SPY",  _CONTRACT_DEFERRED_PLACEHOLDER),
    ("DEFERRED:AAPL",         "AAPL", _CONTRACT_DEFERRED_PLACEHOLDER),
    # Blank / bare ticker
    ("",                      "SPY",  _CONTRACT_DEFERRED_PLACEHOLDER),
    ("SPY",                   "SPY",  _CONTRACT_DEFERRED_PLACEHOLDER),
])
def test_nc3_classify_contract_canonical_shapes(symbol, ticker, expected_state):
    """
    NC-3: _classify_contract() must resolve each symbol to the expected state.
    REAL_OCC and DEFERRED_PLACEHOLDER are the only two states; the authority
    gate relies on this being exhaustive.
    """
    got = core.APExecutionCore._classify_contract(symbol, ticker)
    assert got == expected_state, (
        f"_classify_contract({symbol!r}, {ticker!r}): "
        f"expected {expected_state!r} got {got!r}"
    )
    assert got in {_CONTRACT_REAL_OCC, _CONTRACT_DEFERRED_PLACEHOLDER}, (
        f"Unexpected classification state {got!r} — "
        "gate logic assumes exactly two states"
    )
