"""tests/test_p0_pr520_real_occ_preclaim_authority.py

PR #520 — REAL_OCC fresh-claim authority guard (production-path behavioral tests).

Every NC/PC case calls the REAL production guard inside
_claim_deferred_materialization_for_trigger().  No local boolean formula is
duplicated here; the authority logic lives exclusively in the production method.

The gap this PR closes
-----------------------
`_on_entry_trigger()` computed `_preclaim_deferred` via
`_preclaim_retry_materialization = REAL_OCC AND _plan_is_deferred()`.
`_plan_is_deferred()` trusts stale `contract_deferred=True` metadata, so a
REAL_OCC row with a blank `lifecycle_state` and stale metadata flag could enter
`_claim_deferred_materialization_for_trigger()` and fire a fresh selector run
without a durable recovery lock.

The fix (inside `_claim_deferred_materialization_for_trigger`) reads the order
status BEFORE the CAS.  Without `_recovery_pre_claimed`, a REAL_OCC contract
is refused outright.  SUBMITTED rows return DONE immediately (duplicate
idempotency preserved).  Only the `_recovery_pre_claimed` path is granted
authority.

Test matrix
-----------
NC-1  REAL_OCC + stale contract_deferred=True meta + PENDING_TRIGGER row
      + NO _recovery_pre_claimed
      → guard fires: status == REAL_OCC_FRESH_CLAIM_REFUSED
      → claim_deferred_materialization never called (claim_attempts == 0)

NC-2  REAL_OCC + stale flag also on signal dict + NO _recovery_pre_claimed
      → same guard fires, claim_attempts == 0

NC-3  REAL_OCC + no preclaim + get_order raises (db hiccup)
      → guard fails closed: still returns REAL_OCC_FRESH_CLAIM_REFUSED
      → claim_attempts == 0

PC-1  REAL_OCC + MATERIALIZING row + valid _recovery_pre_claimed fields
      → guard block skipped; result["status"] == "OWNED"
      → claim_attempts == 0 (ownership proved via pre-claim, no fresh CAS)

PC-2  DEFERRED:<ticker> + no _recovery_pre_claimed
      → guard NOT triggered (DEFERRED_PLACEHOLDER is not REAL_OCC)
      → claim_deferred_materialization IS attempted (claim_attempts == 1)

PC-3  REAL_OCC + no preclaim + row status SUBMITTED
      → guard short-circuits to DONE/SUBMITTED (duplicate idempotency)
      → claim_attempts == 0

PC-4  REAL_OCC + no preclaim + row in terminal status (parametrised)
      → guard short-circuits to DONE/TERMINAL_DURABLE
      → claim_attempts == 0

NC-4  _classify_contract canonical symbol shapes (parametrised)
      → production classifier returns REAL_OCC / DEFERRED_PLACEHOLDER
"""
from __future__ import annotations

import copy
import os
import types
from datetime import datetime, timezone

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

import ap_execution_core as core_mod

_CONTRACT_REAL_OCC             = core_mod._CONTRACT_REAL_OCC
_CONTRACT_DEFERRED_PLACEHOLDER = core_mod._CONTRACT_DEFERRED_PLACEHOLDER

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

_CLIENT_ID      = "angel@angelprecision.co"
_EXEC_MODE      = "live"
_TICKER         = "SPY"
_SIGNAL_ID      = "SIG-SPY-PR520"
_LOCAL_ORDER_ID = "oid-pr520-spy"

# A valid OCC-formatted options symbol: 6-char padded underlying + 6-digit
# expiry + type char + 8-digit strike.
_REAL_OCC_SYM = "SPY   240731C00500000"
_DEFERRED_SYM = f"DEFERRED:{_TICKER}"

# ---------------------------------------------------------------------------
# Minimal OSM that tracks claim_deferred_materialization attempts
# ---------------------------------------------------------------------------


class _PR520OSM:
    """Minimal in-memory OSM for PR #520 production-path guard tests.

    Tracks `claim_attempts` so callers can assert the guard fires (or does
    not fire) before the CAS write.
    """

    def __init__(
        self,
        *,
        row_status: str = "PENDING_TRIGGER",
        meta: dict | None = None,
        raise_on_get: bool = False,
    ) -> None:
        self.client_id      = _CLIENT_ID
        self.execution_mode = _EXEC_MODE
        self._raise_on_get  = raise_on_get
        self.claim_attempts  = 0
        self.claim_successes = 0
        self.post_payloads: list[dict] = []
        self.row: dict = {
            "local_order_id":  _LOCAL_ORDER_ID,
            "client_id":       _CLIENT_ID,
            "execution_mode":  _EXEC_MODE,
            "signal_id":       _SIGNAL_ID,
            "status":          row_status,
            "broker_order_id": None,
            "submitted_ts":    None,
            "meta":            meta if meta is not None else {},
        }
        # submit_existing_entry must exist on the OSM; _on_entry_trigger
        # checks hasattr() before proceeding.
        self.submit_existing_entry = lambda *_a, **_k: None

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def _copy_row(self) -> dict:
        return copy.deepcopy(self.row)

    def get_order(self, local_order_id: str) -> dict | None:
        if self._raise_on_get:
            raise RuntimeError("db_hiccup_pr520")
        return self._copy_row()

    def get_order_by_signal(self, signal_id: str) -> dict | None:
        return self._copy_row()

    def has_order(self, local_order_id: str) -> bool:
        return local_order_id == _LOCAL_ORDER_ID

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------

    def update_order_meta(self, local_order_id: str, patch: dict) -> bool:
        self.row.setdefault("meta", {}).update(patch)
        return True

    def claim_deferred_materialization(self, local_order_id: str, **kwargs) -> bool:
        """CAS write — tracks attempts; succeeds unless row is ineligible."""
        self.claim_attempts += 1
        meta   = self.row.get("meta") or {}
        lc     = str(meta.get("lifecycle_state") or "").upper()
        status = str(self.row.get("status") or "").upper()
        # Fail CAS if already active or terminal
        if status not in {"PENDING_TRIGGER", ""}:
            return False
        if lc in {"MATERIALIZING", "BROKER_READY", "SUBMITTING"}:
            return False
        generation = int(
            kwargs.get("generation") or kwargs.get("new_generation") or 1
        )
        meta.update({
            "lifecycle_state":            "MATERIALIZING",
            "materialization_status":     "RUNNING",
            "materialization_in_flight":  True,
            "materialization_owner":      str(kwargs.get("owner") or ""),
            "materialization_generation": generation,
        })
        self.claim_successes += 1
        return True

    def schedule_deferred_materialization_retry(
        self, local_order_id: str, **kwargs
    ) -> bool:
        self.row.setdefault("meta", {}).update({
            "lifecycle_state":        "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
        })
        return True

    def persist_deferred_broker_ready(self, local_order_id: str, **kwargs) -> bool:
        return False  # not exercised in guard-path tests

    def terminalize_deferred_breach(
        self, local_order_id: str, *, reason_code: str, terminal_status: str, **_kw
    ) -> bool:
        self.row["status"] = terminal_status
        return True

    def expire_pending_entry(self, local_order_id: str, *, reason: str) -> bool:
        return self.terminalize_deferred_breach(
            local_order_id, reason_code=reason, terminal_status="EXPIRED"
        )

    def cancel_pending_entry(self, local_order_id: str, *, reason: str) -> bool:
        return self.terminalize_deferred_breach(
            local_order_id, reason_code=reason, terminal_status="CANCELED"
        )

    # Stubs required by production code paths reachable after ownership.
    def _flag_split_brain_order(self, *_a, **_kw) -> None:        return None
    def _emit_transition_event(self, **_kw) -> None:              return None
    def _lookup_order_by_tag(self, *_a, **_kw) -> None:           return None
    def _submit_order_with_retry(self, **kwargs) -> tuple:
        self.post_payloads.append(copy.deepcopy(kwargs.get("order_data", {})))
        return ({"id": "TR-520", "status": "open"}, None, "TR-520", "ACK")


# ---------------------------------------------------------------------------
# Builder helpers
# ---------------------------------------------------------------------------


def _make_plan(
    contract_symbol: str,
    *,
    meta: dict | None = None,
) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        contract_symbol=contract_symbol,
        execution_mode=_EXEC_MODE,
        mode=_EXEC_MODE,
        client_id=_CLIENT_ID,
        signal_id=_SIGNAL_ID,
        limit_price=0.50,
        contracts=1,
        max_position_usd=500.0,
        side="CALL",
        direction="CALL",
        metadata=meta if meta is not None else {},
        ticker=_TICKER,
        trigger_price=600.0,
        underlying_price=600.0,
        stop_underlying=595.0,
        target_underlying=605.0,
        plan_id="plan-pr520",
        score=90.0,
        tier="A",
        timeframe="1d",
        pattern="breakout",
    )


def _make_watched(signal_extras: dict | None = None) -> types.SimpleNamespace:
    """Build a minimal WatchedSignal-shaped object for production method calls."""
    crossed = datetime(2024, 7, 31, 14, 0, 0, tzinfo=timezone.utc)
    sig: dict = {
        "signal_id":      _SIGNAL_ID,
        "local_order_id": _LOCAL_ORDER_ID,
        "client_id":      _CLIENT_ID,
        "execution_mode": _EXEC_MODE,
        "ticker":         _TICKER,
        "breach_ts":      "2024-07-31T14:00:00Z",
    }
    if signal_extras:
        sig.update(signal_extras)
    return types.SimpleNamespace(
        signal=sig,
        ticker=_TICKER,
        trigger_price=600.0,
        trigger_crossed_at=crossed,
        triggered_at=crossed,
        breach_price=600.20,
    )


def _bind_claim_helper(osm: _PR520OSM) -> object:
    """Return a minimal core with the production _claim_deferred_... method bound."""
    c = types.SimpleNamespace(
        client_id=_CLIENT_ID,
        email=_CLIENT_ID,
        execution_mode=_EXEC_MODE,
        order_state_machine=osm,
    )
    c._classify_contract = core_mod.APExecutionCore._classify_contract
    c._claim_deferred_materialization_for_trigger = (
        core_mod.APExecutionCore._claim_deferred_materialization_for_trigger
        .__get__(c, type(c))
    )
    return c


def _call_claim(core_obj, watched, plan, *, deferred_claim_context=None) -> dict:
    """Call the production guard helper with standard PR #520 test arguments."""
    if deferred_claim_context is None:
        deferred_claim_context = {}
    return core_obj._claim_deferred_materialization_for_trigger(
        watched,
        approved_plan=plan,
        queue_local_order_id=_LOCAL_ORDER_ID,
        ticker=_TICKER,
        signal_id=_SIGNAL_ID,
        materialization_client_id=_CLIENT_ID,
        materialization_execution_mode=_EXEC_MODE,
        deferred_claim_context=deferred_claim_context,
    )


# ---------------------------------------------------------------------------
# NC-1: REAL_OCC + stale meta.contract_deferred + PENDING_TRIGGER + no preclaim
# ---------------------------------------------------------------------------


def test_nc1_real_occ_stale_meta_no_preclaim_refuses():
    """
    NC-1: A REAL_OCC plan whose metadata still carries the stale
    contract_deferred=True flag MUST be refused by the PR #520 guard when
    _recovery_pre_claimed is absent.  The guard must return
    REAL_OCC_FRESH_CLAIM_REFUSED and claim_deferred_materialization must
    never be invoked.
    """
    osm  = _PR520OSM(row_status="PENDING_TRIGGER")
    plan = _make_plan(
        _REAL_OCC_SYM,
        meta={"contract_deferred": True, "lifecycle_state": ""},
    )
    # Verify the pre-condition the PR targets: stale metadata makes
    # _plan_is_deferred() return True.
    assert core_mod.APExecutionCore._plan_is_deferred(plan, _TICKER), (
        "Pre-condition: stale metadata must make _plan_is_deferred() True "
        "(that is the gap PR #520 closes)"
    )

    watched = _make_watched()  # no _recovery_pre_claimed
    c       = _bind_claim_helper(osm)
    result  = _call_claim(c, watched, plan)

    assert result["status"] == "REAL_OCC_FRESH_CLAIM_REFUSED", (
        f"NC-1 FAIL: expected REAL_OCC_FRESH_CLAIM_REFUSED, got {result['status']!r}"
    )
    inner = result.get("result") or {}
    assert inner.get("reason_code") == "REAL_OCC_DEFERRED_CLAIM_NOT_AUTHORISED", (
        f"NC-1 FAIL: wrong reason_code: {inner.get('reason_code')!r}"
    )
    assert inner.get("disposition") == "KEEP_WATCHER", (
        f"NC-1 FAIL: expected KEEP_WATCHER, got {inner.get('disposition')!r}"
    )
    assert osm.claim_attempts == 0, (
        "NC-1 FAIL: claim_deferred_materialization must NOT be called when "
        f"the guard fires; claim_attempts={osm.claim_attempts}"
    )


# ---------------------------------------------------------------------------
# NC-2: REAL_OCC + stale flags on both plan AND signal dict + no preclaim
# ---------------------------------------------------------------------------


def test_nc2_real_occ_stale_signal_flag_no_preclaim_refuses():
    """
    NC-2: Stale contract_deferred flags propagated to both the plan metadata
    AND the signal dict must not grant authority for a REAL_OCC fresh claim
    when _recovery_pre_claimed is absent.
    """
    osm  = _PR520OSM(row_status="PENDING_TRIGGER")
    plan = _make_plan(_REAL_OCC_SYM, meta={"contract_deferred": True})
    watched = _make_watched({"contract_deferred": True})  # stale signal flag
    c       = _bind_claim_helper(osm)
    result  = _call_claim(c, watched, plan)

    assert result["status"] == "REAL_OCC_FRESH_CLAIM_REFUSED", (
        f"NC-2 FAIL: {result['status']!r}"
    )
    inner = result.get("result") or {}
    assert inner.get("reason_code") == "REAL_OCC_DEFERRED_CLAIM_NOT_AUTHORISED", (
        f"NC-2 FAIL: reason_code={inner.get('reason_code')!r}"
    )
    assert osm.claim_attempts == 0, (
        f"NC-2 FAIL: claim_attempts={osm.claim_attempts}"
    )


# ---------------------------------------------------------------------------
# NC-3: REAL_OCC + no preclaim + get_order raises → fail closed
# ---------------------------------------------------------------------------


def test_nc3_real_occ_get_order_raises_fails_closed():
    """
    NC-3: A DB error during get_order() must not allow the guard to be
    bypassed.  With a REAL_OCC plan and no _recovery_pre_claimed, the guard
    must still return REAL_OCC_FRESH_CLAIM_REFUSED (fail closed).
    No CAS must be attempted.
    """
    osm  = _PR520OSM(row_status="PENDING_TRIGGER", raise_on_get=True)
    plan = _make_plan(_REAL_OCC_SYM, meta={"contract_deferred": True})
    watched = _make_watched()
    c       = _bind_claim_helper(osm)
    result  = _call_claim(c, watched, plan)

    assert result["status"] == "REAL_OCC_FRESH_CLAIM_REFUSED", (
        "NC-3 FAIL: guard must refuse even when get_order raises; "
        f"got {result['status']!r}"
    )
    inner = result.get("result") or {}
    assert inner.get("reason_code") == "REAL_OCC_DEFERRED_CLAIM_NOT_AUTHORISED", (
        f"NC-3 FAIL: reason_code={inner.get('reason_code')!r}"
    )
    assert osm.claim_attempts == 0, (
        f"NC-3 FAIL: claim_attempts={osm.claim_attempts}"
    )


# ---------------------------------------------------------------------------
# PC-1: REAL_OCC + MATERIALIZING row + valid _recovery_pre_claimed → OWNED
# ---------------------------------------------------------------------------


def test_pc1_real_occ_valid_recovery_preclaim_returns_owned():
    """
    PC-1: When _recovery_pre_claimed is set with owner/generation/attempt
    that match a live MATERIALIZING row, the PR #520 guard block is skipped
    and the function returns OWNED.  No fresh CAS is written.
    """
    _owner   = "materializer:pr520-owner"
    _gen     = 3
    _attempt = 1

    osm = _PR520OSM(
        row_status="PENDING_TRIGGER",
        meta={
            "lifecycle_state":            "MATERIALIZING",
            "materialization_status":     "RUNNING",
            "materialization_in_flight":  True,
            "materialization_owner":      _owner,
            "materialization_generation": _gen,
            "retry_attempt":              _attempt,
        },
    )
    osm.row["client_id"]      = _CLIENT_ID
    osm.row["execution_mode"] = _EXEC_MODE

    plan    = _make_plan(
        _REAL_OCC_SYM,
        meta={"contract_deferred": True, "lifecycle_state": "RETRY_WAIT"},
    )
    watched = _make_watched({
        "_recovery_pre_claimed":            True,
        "_recovery_pre_claimed_owner":      _owner,
        "_recovery_pre_claimed_generation": _gen,
        "_recovery_pre_claimed_attempt":    _attempt,
        "_recovery_pre_claimed_client_id":  _CLIENT_ID,
        "_recovery_pre_claimed_mode":       _EXEC_MODE,
    })
    c      = _bind_claim_helper(osm)
    result = _call_claim(c, watched, plan)

    assert result.get("status") == "OWNED", (
        "PC-1 FAIL: expected OWNED (recovery pre-claim verified and guard "
        f"bypassed); got {result!r}"
    )
    assert result.get("recovery_pre_claimed") is True, (
        "PC-1 FAIL: result must carry recovery_pre_claimed=True"
    )
    # No fresh CAS must have been attempted — ownership was proved via the
    # pre-claim, not written.
    assert osm.claim_attempts == 0, (
        "PC-1 FAIL: claim_deferred_materialization must not be called when "
        f"ownership proved via pre-claim; claim_attempts={osm.claim_attempts}"
    )


# ---------------------------------------------------------------------------
# PC-2: DEFERRED:<ticker> + no preclaim → guard NOT triggered, CAS attempted
# ---------------------------------------------------------------------------


def test_pc2_deferred_placeholder_bypasses_guard_and_reaches_cas():
    """
    PC-2 (regression guard): A DEFERRED:<ticker> plan is not a REAL_OCC
    contract.  The PR #520 guard must not block it.
    claim_deferred_materialization must be called exactly once.
    """
    osm  = _PR520OSM(row_status="PENDING_TRIGGER")
    plan = _make_plan(_DEFERRED_SYM, meta={"contract_deferred": True})

    assert (
        core_mod.APExecutionCore._classify_contract(_DEFERRED_SYM, _TICKER)
        == _CONTRACT_DEFERRED_PLACEHOLDER
    ), f"Pre-condition: {_DEFERRED_SYM!r} must be DEFERRED_PLACEHOLDER"

    watched = _make_watched()  # no _recovery_pre_claimed
    c       = _bind_claim_helper(osm)
    _call_claim(c, watched, plan)

    assert osm.claim_attempts >= 1, (
        "PC-2 FAIL: DEFERRED_PLACEHOLDER must NOT be blocked by the REAL_OCC "
        f"guard; claim_attempts={osm.claim_attempts}"
    )


# ---------------------------------------------------------------------------
# PC-3: REAL_OCC + no preclaim + SUBMITTED row → idempotent DONE/SUBMITTED
# ---------------------------------------------------------------------------


def test_pc3_real_occ_already_submitted_returns_done_submitted():
    """
    PC-3: An already-SUBMITTED REAL_OCC order must be handled idempotently.
    The guard reads the status before the CAS and returns DONE/SUBMITTED.
    No CAS is attempted.
    """
    osm  = _PR520OSM(row_status="SUBMITTED")
    plan = _make_plan(_REAL_OCC_SYM, meta={"contract_deferred": True})
    watched = _make_watched()
    c       = _bind_claim_helper(osm)
    result  = _call_claim(c, watched, plan)

    assert result["status"] == "DONE", (
        f"PC-3 FAIL: expected DONE, got {result['status']!r}"
    )
    inner = result.get("result") or {}
    assert inner.get("disposition") == "SUBMITTED", (
        f"PC-3 FAIL: disposition must be SUBMITTED; got {result!r}"
    )
    assert osm.claim_attempts == 0, (
        "PC-3 FAIL: CAS must not be attempted when row is already SUBMITTED; "
        f"claim_attempts={osm.claim_attempts}"
    )


# ---------------------------------------------------------------------------
# PC-4: REAL_OCC + no preclaim + terminal row → DONE/TERMINAL_DURABLE
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("terminal_status", [
    "REJECTED", "EXPIRED", "CANCELED", "ERROR",
])
def test_pc4_real_occ_terminal_row_returns_terminal_durable(terminal_status: str):
    """
    PC-4: A REAL_OCC order in a terminal state must be short-circuited to
    DONE/TERMINAL_DURABLE.  No CAS is attempted.
    """
    osm  = _PR520OSM(row_status=terminal_status)
    plan = _make_plan(_REAL_OCC_SYM, meta={"contract_deferred": True})
    watched = _make_watched()
    c       = _bind_claim_helper(osm)
    result  = _call_claim(c, watched, plan)

    assert result["status"] == "DONE", (
        f"PC-4 FAIL [{terminal_status}]: expected DONE, got {result['status']!r}"
    )
    inner = result.get("result") or {}
    assert inner.get("disposition") == "TERMINAL_DURABLE", (
        f"PC-4 FAIL [{terminal_status}]: {result!r}"
    )
    assert osm.claim_attempts == 0, (
        f"PC-4 FAIL [{terminal_status}]: claim_attempts must be 0; "
        f"got {osm.claim_attempts}"
    )


# ---------------------------------------------------------------------------
# NC-4: _classify_contract canonical shape coverage (production classifier)
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
def test_nc4_classify_contract_canonical_shapes(
    symbol: str,
    ticker: str,
    expected_state: str,
) -> None:
    """
    NC-4: The production _classify_contract() must resolve each symbol to the
    expected state.  REAL_OCC and DEFERRED_PLACEHOLDER are the only two
    states; the authority gate relies on this being exhaustive.
    """
    got = core_mod.APExecutionCore._classify_contract(symbol, ticker)
    assert got == expected_state, (
        f"_classify_contract({symbol!r}, {ticker!r}): "
        f"expected {expected_state!r} got {got!r}"
    )
    assert got in {_CONTRACT_REAL_OCC, _CONTRACT_DEFERRED_PLACEHOLDER}, (
        f"Unexpected classification state {got!r} — "
        "gate logic assumes exactly two states"
    )
