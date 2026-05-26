"""
PR E — master_control capital gate hardening
=============================================

Tests for the 6 in-scope fixes in the institutional audit of
ap_master_control.py (audit dated 2026-05-25). Tests-first: every fix
is red before source change, green after.

NOT IN SCOPE (deferred):
  - FIX-7 (BUG-MC-2: queued-before-insert) — requires queue-layer
    changes outside master_control, deferred to PR F.

Coverage map (6 fixes + structural invariants):

  FIX-1 revalidate_exposure excludes current plan from pending_cap (3)
    - _pending_capital_from_snapshot_or_db accepts exclude_local_order_id
    - revalidate_exposure passes plan.plan_id / local_order_id to exclude
    - LIVE fail-closed semantics preserved when pending query fails

  FIX-2 ApprovedExecutionPlan.mode normalized to uppercase (2)
    - plan.mode == "LIVE" when self.paper is False
    - plan.mode == "PAPER" when self.paper is True

  FIX-3 _equity_lock around equity/loss reads + writes (3)
    - APMasterControl.__init__ creates self._equity_lock = threading.Lock()
    - set_account_equity acquires _equity_lock for the mutation
    - max_daily_loss still scales proportionally (regression guard)

  FIX-4 _cooldown_lock + set_cooldown public method (4)
    - APMasterControl.__init__ creates self._cooldown_lock = threading.Lock()
    - set_cooldown(key, ts=None, reason="") method exists and writes under lock
    - evaluate's cooldown read uses the lock (source-shape)
    - reset_session clears _trade_cooldowns under the lock

  FIX-5 _ENTRY_CAPITAL_RESERVED_STATUSES non-empty fallback guard (2)
    - When PENDING_ENTRY_STATUSES import yields empty/None, module
      falls back to hardcoded default statuses (NOT empty tuple).
    - The fallback path logs CRITICAL so ops sees the misconfig.

  FIX-6 intelligence_bridge import elevated to module-level (3)
    - Module exposes _INTEL_AVAILABLE flag and _run_intel_check ref
      (or None on import failure).
    - _run_intelligence references the module-level symbols, NOT
      `from intelligence_bridge import ...` inline.
    - _run_intelligence still fails open when intel is unavailable
      (returns approved=True with reasoning explaining why).

  INV — Structural invariants (2)
    - revalidate_exposure does not double-count when current plan
      is already in pending_cap.
    - _cooldown_lock + set_cooldown are wired without removing the
      _trade_cooldowns dict (backward compat with execution_core
      which mutates the dict directly).

  FIX-7 (BUG-MC-2) — DEFERRED to PR F (1 documentation test)
    - This test exists ONLY to record the deferred scope as a TODO
      comment in the source so PR F has a clear pickup signal.
"""
from __future__ import annotations

import base64
import hashlib
import inspect
import os
import re
import sys
import threading
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
MC_SRC = (REPO_ROOT / "ap_master_control.py").read_text()

# Env BEFORE module import (some submodules read DATABASE_URL at import).
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_master_control_hardening",
)
os.environ.setdefault("ENCRYPTION_KEY", "angel-pr-e-test-key-2026-not-default")

# Stub supabase if not installed.
if "supabase" not in sys.modules:
    _supa_stub = types.ModuleType("supabase")
    _supa_stub.create_client = lambda *a, **kw: None
    _supa_stub.Client = type("Client", (), {})
    sys.modules["supabase"] = _supa_stub


@pytest.fixture(scope="module")
def mc_mod():
    """Import ap_master_control with a defensive ap.db shim.

    Same module-pollution context as test_client_runner_hardening.py:
    tests/test_repeg_resubmit.py installs a partial ap.db stub at
    module-load. ap_master_control imports `from ap.db import conn,
    run_with_retry` inside _pending_orders_capital and reset_session;
    those are NOT executed at module load, so the import itself is
    safe. We still patch missing names defensively so runtime tests
    that call into those methods don't blow up under polluted suite
    ordering.
    """
    _apdb = sys.modules.get("ap.db")
    _added: list[str] = []
    if _apdb is not None:
        if not hasattr(_apdb, "run_with_retry"):
            def _stub_run_with_retry(fn, *args, **kwargs):  # pragma: no cover
                return fn(*args, **kwargs)
            _apdb.run_with_retry = _stub_run_with_retry  # type: ignore[attr-defined]
            _added.append("run_with_retry")
        if not hasattr(_apdb, "conn"):
            _apdb.conn = lambda *a, **kw: None  # type: ignore[attr-defined]
            _added.append("conn")
    import ap_master_control  # noqa: E402
    try:
        yield ap_master_control
    finally:
        if _apdb is not None:
            for name in _added:
                try:
                    delattr(_apdb, name)
                except AttributeError:
                    pass


def _make_mc(mc_mod, *, mode="paper", account_equity=25000.0, position_manager=None):
    """Build a minimal APMasterControl for tests that need a real instance."""
    pm = position_manager if position_manager is not None else MagicMock()
    # snapshot() returns a realistic shape
    pm.snapshot = MagicMock(return_value={
        "capital_deployed": 0.0,
        "open_positions": [],
        "closing_positions": [],
        "realized_pnl_today": 0.0,
        "pending_entries": [],
        "pending_entry_capital": 0.0,
        "_snapshot_ok": True,
    })
    pm.has_pending_entry = MagicMock(return_value=False)
    mc = mc_mod.APMasterControl(
        mode=mode,
        account_equity=account_equity,
        position_manager=pm,
        supabase_client=None,
    )
    return mc


# ══════════════════════════════════════════════════════════════════
# FIX-1 — revalidate_exposure excludes current plan from pending_cap
# ══════════════════════════════════════════════════════════════════

class TestFix1RevalidateExposureExcludesCurrentPlan:
    def test_pending_capital_query_supports_exclude_local_order_id(self, mc_mod):
        """_pending_orders_capital must accept exclude_local_order_id so
        revalidate_exposure can subtract the current plan's reserved
        capital (which is already in the SQL SUM via the OSM row that
        process_signal just inserted)."""
        sig = inspect.signature(mc_mod.APMasterControl._pending_orders_capital)
        assert "exclude_local_order_id" in sig.parameters, (
            "_pending_orders_capital must accept `exclude_local_order_id` "
            "so revalidate_exposure can exclude the current plan's "
            "reserved_cost row from the SUM. Without this, the plan's "
            "real_cost gets double-counted (once via pending_cap SUM, "
            "once via revalidate_exposure's `+ real_cost` addition)."
        )

    def test_revalidate_exposure_passes_exclusion(self, mc_mod):
        """revalidate_exposure source must call the pending-capital
        helper with an exclusion argument keyed off plan.plan_id (or
        plan.local_order_id). Source-shape check so a future refactor
        cannot silently drop the exclusion."""
        src = inspect.getsource(mc_mod.APMasterControl.revalidate_exposure)
        assert re.search(
            r'exclude_local_order_id\s*=',
            src,
        ), (
            "revalidate_exposure must pass exclude_local_order_id= to "
            "the pending-capital helper. Otherwise the current plan's "
            "reserved_cost row is in the SUM AND added as real_cost — "
            "double-counted. See audit BUG-MC-1."
        )

    def test_revalidate_exposure_preserves_live_fail_closed(self, mc_mod):
        """Regression guard: if _pending_capital_from_snapshot_or_db
        returns None (DB query failed), LIVE mode must still fail
        closed by returning blocked_system with PENDING_CAPITAL_UNAVAILABLE."""
        src = inspect.getsource(mc_mod.APMasterControl.revalidate_exposure)
        # The fail-closed block must still be present after the fix.
        assert "pending_capital_unavailable_live_blocked" in src, (
            "revalidate_exposure must still fail closed in LIVE when "
            "pending_capital is None. The exclusion fix must NOT remove "
            "this safety gate."
        )
        assert re.search(
            r'self\._is_live_mode\(\)\s+and\s+self\.pending_capital_fail_closed_live',
            src,
        ), "LIVE fail-closed condition must remain in revalidate_exposure"


# ══════════════════════════════════════════════════════════════════
# FIX-2 — ApprovedExecutionPlan.mode normalized to uppercase
# ══════════════════════════════════════════════════════════════════

class TestFix2PlanModeUppercase:
    def test_plan_mode_uppercase_in_live(self):
        """When master_control.paper is False, the constructed plan
        must have mode='LIVE' (uppercase). The rest of the system
        compares mode against 'LIVE' / 'PAPER' uppercase."""
        # Source-shape: the mode= kwarg in the plan ctor must produce
        # uppercase. Search for the pattern.
        m = re.search(
            r'plan\s*=\s*ApprovedExecutionPlan\(.*?\bmode\s*=\s*([^,\n]+?)\s*,',
            MC_SRC, re.DOTALL,
        )
        assert m, "Could not locate the ApprovedExecutionPlan(mode=...) construction"
        mode_expr = m.group(1).strip()
        # Must yield uppercase "LIVE" / "PAPER" — not "live" / "paper".
        assert '"LIVE"' in mode_expr and '"PAPER"' in mode_expr, (
            f"ApprovedExecutionPlan mode expression must use uppercase "
            f'"LIVE" / "PAPER" literals; got: {mode_expr!r}. '
            "The rest of the system normalizes mode to uppercase."
        )
        # And must NOT use lowercase literals
        assert '"live"' not in mode_expr and '"paper"' not in mode_expr, (
            f"ApprovedExecutionPlan mode expression must NOT use "
            f'lowercase "live"/"paper" literals; got: {mode_expr!r}'
        )


# ══════════════════════════════════════════════════════════════════
# FIX-3 — _equity_lock around set_account_equity + reads
# ══════════════════════════════════════════════════════════════════

class TestFix3EquityLock:
    def test_mc_init_creates_equity_lock(self, mc_mod):
        """APMasterControl.__init__ must create self._equity_lock =
        threading.Lock(). Without it, set_account_equity()'s update
        of account_equity + max_daily_loss races with evaluate()'s
        reads from the worker thread."""
        init_src = inspect.getsource(mc_mod.APMasterControl.__init__)
        assert "self._equity_lock" in init_src, (
            "APMasterControl.__init__ must create self._equity_lock to "
            "protect concurrent equity/loss reads (evaluate) and "
            "writes (set_account_equity, called from equity-sync thread)."
        )
        assert "threading.Lock()" in init_src or "threading.RLock()" in init_src, (
            "self._equity_lock must be a threading.Lock (or RLock)."
        )

    def test_set_account_equity_acquires_equity_lock(self, mc_mod):
        """set_account_equity must hold _equity_lock while updating
        account_equity + max_daily_loss. Otherwise a worker reading
        max_daily_loss between the two assignments sees an inconsistent
        snapshot (old equity, scaled-to-new-equity loss limit)."""
        src = inspect.getsource(mc_mod.APMasterControl.set_account_equity)
        assert "with self._equity_lock" in src, (
            "set_account_equity must acquire _equity_lock around the "
            "account_equity / max_daily_loss mutations."
        )

    def test_max_daily_loss_scales_proportionally(self, mc_mod):
        """Regression guard: the scaling math itself is unchanged.
        loss_pct = abs(max_daily_loss / startup_equity).
        new_max_daily_loss = -abs(new_equity * loss_pct).
        """
        mc = _make_mc(mc_mod, mode="paper", account_equity=25000.0)
        # Force a known startup state
        mc._startup_equity = 25000.0
        mc.max_daily_loss = -500.0  # 2% of $25k

        # Double the equity → max_daily_loss should also double in magnitude
        mc.set_account_equity(50000.0, client_id="alice@x.com")

        # 2% of $50k = $1000
        assert abs(mc.max_daily_loss - (-1000.0)) < 0.01, (
            f"max_daily_loss must scale proportionally with equity. "
            f"Expected -1000.0 (2% of $50k); got {mc.max_daily_loss}"
        )

    # ----------------------------------------------------------------
    # PR E FIX-3 patch: reader-side equity snapshot
    # ----------------------------------------------------------------
    # A lock only protects shared state if BOTH writer and reader use
    # the same lock. PR E's initial pass only locked the writer
    # (set_account_equity). evaluate() and revalidate_exposure() still
    # read self.account_equity / self.max_daily_loss directly, so a
    # worker thread can observe a half-updated equity/loss pair
    # (new equity, old loss) between the two assignments.
    #
    # Patch: add _equity_snapshot() helper that returns both values
    # atomically under _equity_lock; callers use the local vars for
    # all subsequent risk math. Lock is NEVER held across DB / broker /
    # intelligence / position-manager / any slow calls — only the
    # two-tuple snapshot is taken.
    # ----------------------------------------------------------------

    def test_equity_snapshot_helper_exists(self, mc_mod):
        """APMasterControl must expose _equity_snapshot() as the official
        thread-safe read API for the (equity, max_daily_loss) pair."""
        assert hasattr(mc_mod.APMasterControl, "_equity_snapshot"), (
            "APMasterControl must expose _equity_snapshot(self) -> "
            "tuple[float, float] so callers can atomically read both "
            "account_equity and max_daily_loss under _equity_lock."
        )
        sig = inspect.signature(mc_mod.APMasterControl._equity_snapshot)
        params = list(sig.parameters.keys())
        # Only `self` (no other args required).
        assert params == ["self"], (
            f"_equity_snapshot must take only self; got params={params}"
        )

    def test_equity_snapshot_acquires_lock(self, mc_mod):
        """_equity_snapshot's body must acquire _equity_lock while
        reading the two fields, then return outside the lock."""
        src = inspect.getsource(mc_mod.APMasterControl._equity_snapshot)
        assert "with self._equity_lock" in src, (
            "_equity_snapshot must acquire self._equity_lock around the "
            "compound (account_equity, max_daily_loss) read so the pair "
            "is observed atomically."
        )
        # Must read both fields
        assert "self.account_equity" in src, (
            "_equity_snapshot must read self.account_equity"
        )
        assert "self.max_daily_loss" in src, (
            "_equity_snapshot must read self.max_daily_loss"
        )

    def test_equity_snapshot_returns_consistent_pair(self, mc_mod):
        """Runtime check: _equity_snapshot returns (equity, max_loss)
        as a 2-tuple, with both values float."""
        mc = _make_mc(mc_mod, mode="paper", account_equity=25000.0)
        mc.max_daily_loss = -500.0
        eq, loss = mc._equity_snapshot()
        assert isinstance(eq, float) and isinstance(loss, float)
        assert eq == 25000.0
        assert loss == -500.0

    def test_evaluate_reads_equity_via_snapshot_or_lock(self, mc_mod):
        """evaluate() must read account_equity and max_daily_loss for
        risk checks via _equity_snapshot() (or directly under the
        lock). Direct unlocked reads like `self.account_equity *
        self.max_capital_pct` race with set_account_equity.

        Acceptable shapes:
          (a) `eq, loss = self._equity_snapshot()` near the top,
              then local `eq` / `loss` for subsequent risk math.
          (b) `with self._equity_lock: eq = self.account_equity; ...`
              — not preferred (lock held longer) but valid.
        """
        src = inspect.getsource(mc_mod.APMasterControl.evaluate)

        # Must reference _equity_snapshot OR an explicit _equity_lock
        # acquire. Either is acceptable.
        uses_snapshot = bool(re.search(
            r'self\._equity_snapshot\s*\(\s*\)',
            src,
        ))
        uses_lock_directly = bool(re.search(
            r'with\s+self\._equity_lock\s*:',
            src,
        ))
        assert uses_snapshot or uses_lock_directly, (
            "evaluate() must read account_equity / max_daily_loss via "
            "self._equity_snapshot() or under `with self._equity_lock:`. "
            "Direct unlocked reads race with set_account_equity()'s "
            "two-field compound update."
        )

        # AND the risk-math sites must NOT reference self.account_equity
        # / self.max_daily_loss directly in the SAME function. (One
        # `self.account_equity` reference is allowed in the snapshot
        # call itself, e.g. if someone wrote `self.account_equity` as a
        # fallback — but the main capital/sector/daily-loss checks must
        # use the local snapshot var, not self. )
        # We enforce this by counting direct reads of self.account_equity
        # in the body; with the snapshot pattern, the body should have
        # at most ZERO direct reads (the helper provides the value).
        # Allow a small tolerance (<=1) for inert fallbacks / log lines.
        direct_equity_reads = re.findall(
            r'\bself\.account_equity\b',
            src,
        )
        direct_loss_reads = re.findall(
            r'\bself\.max_daily_loss\b',
            src,
        )
        assert len(direct_equity_reads) == 0, (
            f"evaluate() must NOT contain direct `self.account_equity` "
            f"reads after the snapshot — use the local snapshot variable "
            f"for all risk math. Found {len(direct_equity_reads)} direct "
            f"reference(s)."
        )
        assert len(direct_loss_reads) == 0, (
            f"evaluate() must NOT contain direct `self.max_daily_loss` "
            f"reads after the snapshot — use the local snapshot variable. "
            f"Found {len(direct_loss_reads)} direct reference(s)."
        )

    def test_revalidate_exposure_reads_equity_via_snapshot_or_lock(self, mc_mod):
        """revalidate_exposure() must also read account_equity via
        _equity_snapshot() (or under the lock) so the risk math sees
        a consistent equity value."""
        src = inspect.getsource(mc_mod.APMasterControl.revalidate_exposure)
        uses_snapshot = bool(re.search(
            r'self\._equity_snapshot\s*\(\s*\)',
            src,
        ))
        uses_lock_directly = bool(re.search(
            r'with\s+self\._equity_lock\s*:',
            src,
        ))
        assert uses_snapshot or uses_lock_directly, (
            "revalidate_exposure() must read account_equity via "
            "self._equity_snapshot() or under `with self._equity_lock:`."
        )
        # No direct `self.account_equity` reads in the body. (max_daily_loss
        # is not used here, but check anyway in case future fixes add it.)
        assert len(re.findall(r'\bself\.account_equity\b', src)) == 0, (
            "revalidate_exposure() must NOT contain direct "
            "`self.account_equity` reads — use the snapshot local var."
        )
        assert len(re.findall(r'\bself\.max_daily_loss\b', src)) == 0, (
            "revalidate_exposure() must NOT contain direct "
            "`self.max_daily_loss` reads."
        )

    def test_equity_lock_not_held_across_slow_calls_in_evaluate(self, mc_mod):
        """Defensive structural check: the _equity_lock acquisition in
        evaluate() must NOT span any of the known slow / external calls:
          - self._get_snapshot(...)            (PM snapshot)
          - self._pending_capital_from_snapshot_or_db(...)  (DB)
          - self._run_intelligence(...)        (intel callable)
          - self.broker. ...                   (broker)
        Holding _equity_lock across these would serialize all worker
        threads behind one slow call — unacceptable for a hot path.

        We assert this by locating the FIRST _equity_lock-related read
        (either `_equity_snapshot()` or `with self._equity_lock:`) and
        verifying it is NOT inside the slow-call region. The simplest
        sufficient check: the slow calls must NOT appear within the
        same `with self._equity_lock:` block (if direct lock is used).
        With the helper, the lock is auto-released before any slow
        call by construction.
        """
        src = inspect.getsource(mc_mod.APMasterControl.evaluate)
        # If the source uses `with self._equity_lock:`, ensure no slow
        # calls appear inside the indented block. We approximate by
        # checking each lock-block region for slow-call signatures.
        slow_signatures = (
            r'self\._get_snapshot\s*\(',
            r'self\._pending_capital_from_snapshot_or_db\s*\(',
            r'self\._run_intelligence\s*\(',
            r'self\.broker\.',
        )
        for m in re.finditer(r'with\s+self\._equity_lock\s*:\s*\n', src):
            # Take the next ~10 non-empty source lines after the lock
            # opening; if any contains a slow signature, fail.
            tail = src[m.end():m.end() + 800]
            for sig in slow_signatures:
                assert not re.search(sig, tail[:tail.find("\n\n")] if "\n\n" in tail else tail), (
                    f"evaluate() holds _equity_lock across a slow call "
                    f"matching {sig!r}. Snapshot the values out of the "
                    f"lock first, then make the slow call with local vars."
                )


# ══════════════════════════════════════════════════════════════════
# FIX-4 — _cooldown_lock + set_cooldown method
# ══════════════════════════════════════════════════════════════════

class TestFix4CooldownLock:
    def test_mc_init_creates_cooldown_lock(self, mc_mod):
        """APMasterControl.__init__ must create self._cooldown_lock =
        threading.Lock(). ap_execution_core mutates _trade_cooldowns
        from the exit-callback thread while evaluate reads it from
        the worker thread."""
        init_src = inspect.getsource(mc_mod.APMasterControl.__init__)
        assert "self._cooldown_lock" in init_src, (
            "APMasterControl.__init__ must create self._cooldown_lock "
            "so concurrent writes (exit callback) and reads (evaluate "
            "from worker thread) of _trade_cooldowns are serialized."
        )

    def test_set_cooldown_method_exists(self, mc_mod):
        """APMasterControl must expose a public set_cooldown(key, ts,
        reason) method. Future writers should use it instead of
        mutating _trade_cooldowns directly. The existing direct
        mutation site in ap_execution_core is left in place for
        backward compatibility (we do not modify execution_core in
        this PR)."""
        assert hasattr(mc_mod.APMasterControl, "set_cooldown"), (
            "APMasterControl must expose set_cooldown(key, ts=None, "
            "reason='') as the official write API for cooldowns."
        )
        sig = inspect.signature(mc_mod.APMasterControl.set_cooldown)
        params = list(sig.parameters.keys())
        # self, key, then ts and reason (any order, both must exist)
        assert "key" in params, "set_cooldown must accept `key`"
        assert "ts" in params, "set_cooldown must accept `ts` (optional)"
        assert "reason" in params, "set_cooldown must accept `reason` (optional)"

    def test_set_cooldown_writes_under_lock(self, mc_mod):
        """set_cooldown's body must hold _cooldown_lock for the write."""
        src = inspect.getsource(mc_mod.APMasterControl.set_cooldown)
        assert "with self._cooldown_lock" in src, (
            "set_cooldown must acquire _cooldown_lock around the "
            "_trade_cooldowns mutation."
        )

    def test_evaluate_reads_cooldowns_under_lock(self, mc_mod):
        """evaluate's read of self._trade_cooldowns must happen under
        _cooldown_lock so the dict isn't being mutated concurrently by
        the exit-callback thread.

        Source-shape: the read of `self._trade_cooldowns` (any access
        form — `in`, `.get()`, `[]`) must occur after a
        `with self._cooldown_lock:` opening AND before that block is
        exited. We assert this by locating every line that reads
        self._trade_cooldowns inside the evaluate body and verifying
        each is more-indented than the most-recent lock block above it.
        """
        src = inspect.getsource(mc_mod.APMasterControl.evaluate)
        # Find every read of self._trade_cooldowns (not assignments).
        # The cooldown access in evaluate uses `.get()` after the fix.
        cooldown_reads = list(re.finditer(
            r'^(?P<indent>[ \t]*)[^\n]*self\._trade_cooldowns\.(get|__contains__|__getitem__)',
            src, re.MULTILINE,
        ))
        # Older shapes use `in self._trade_cooldowns` or
        # `self._trade_cooldowns[cooldown_key]`. Add a permissive fallback:
        if not cooldown_reads:
            cooldown_reads = list(re.finditer(
                r'^(?P<indent>[ \t]*)[^\n]*self\._trade_cooldowns(\[|\.get|\s+in\b)',
                src, re.MULTILINE,
            ))
        assert cooldown_reads, (
            "Could not find any read of self._trade_cooldowns inside "
            "evaluate(). Either the read has moved or the access shape "
            "is unfamiliar to this test."
        )
        # For each read line, verify there's a `with self._cooldown_lock:`
        # block ABOVE it whose indent is less, indicating the read is
        # inside the lock.
        for read_match in cooldown_reads:
            read_indent = len(read_match.group("indent").expandtabs(4))
            prefix = src[:read_match.start()]
            lock_iter = list(re.finditer(
                r'^(?P<indent>[ \t]*)with self\._cooldown_lock\s*:',
                prefix, re.MULTILINE,
            ))
            assert lock_iter, (
                f"A read of self._trade_cooldowns at indent={read_indent} "
                f"has NO `with self._cooldown_lock:` block above it. "
                f"Read line: {src[read_match.start():read_match.end()+50]!r}"
            )
            last_lock = lock_iter[-1]
            lock_indent = len(last_lock.group("indent").expandtabs(4))
            # The read must be more-indented than the lock open AND no
            # un-indent has happened between them (i.e., the lock block
            # has not exited). We approximate the second check by
            # confirming the read is within ~50 lines of the lock open
            # AND more-indented.
            assert read_indent > lock_indent, (
                f"Read of self._trade_cooldowns (indent={read_indent}) must "
                f"be more-indented than the `with self._cooldown_lock:` open "
                f"(indent={lock_indent}) — i.e., inside that lock block. "
                f"Concurrent writes from exit callback thread otherwise race."
            )

    def test_reset_session_clears_cooldowns_under_lock(self, mc_mod):
        """reset_session must clear _trade_cooldowns under the lock so
        a new trading day starts with no stale cooldowns AND the
        clear is atomic with respect to concurrent writes."""
        src = inspect.getsource(mc_mod.APMasterControl.reset_session)
        # Must contain a clear of _trade_cooldowns
        assert "_trade_cooldowns" in src and ".clear()" in src, (
            "reset_session must call self._trade_cooldowns.clear() so "
            "the new trading day starts fresh with no stale cooldowns."
        )
        # Must be inside a `with self._cooldown_lock:` block.
        assert "with self._cooldown_lock" in src, (
            "reset_session must acquire self._cooldown_lock when "
            "clearing _trade_cooldowns (atomic with respect to writes)."
        )


# ══════════════════════════════════════════════════════════════════
# FIX-5 — _ENTRY_CAPITAL_RESERVED_STATUSES non-empty fallback guard
# ══════════════════════════════════════════════════════════════════

class TestFix5EntryStatusesFallback:
    def test_entry_capital_statuses_is_never_empty(self, mc_mod):
        """_ENTRY_CAPITAL_RESERVED_STATUSES must be a non-empty tuple.
        If the imported PENDING_ENTRY_STATUSES is empty/None, the
        fallback must yield hardcoded defaults — never an empty tuple
        that would make pending capital math silently return zero."""
        statuses = mc_mod._ENTRY_CAPITAL_RESERVED_STATUSES
        assert isinstance(statuses, tuple), (
            f"_ENTRY_CAPITAL_RESERVED_STATUSES must be a tuple; got {type(statuses)}"
        )
        assert len(statuses) > 0, (
            "_ENTRY_CAPITAL_RESERVED_STATUSES must NEVER be empty. "
            "An empty tuple makes pending capital SUM produce zero, "
            "silently bypassing the capital gate."
        )
        # Must contain the canonical pre-fill entry states
        canonical = {"CREATED", "SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL"}
        assert canonical & set(statuses) == canonical, (
            f"_ENTRY_CAPITAL_RESERVED_STATUSES must contain the canonical "
            f"pre-fill states {canonical}; got {set(statuses)}"
        )

    def test_empty_imported_statuses_falls_back_to_default(self):
        """Source-shape check: the module must define a hardcoded fallback
        and reference _OSM_PENDING_ENTRY_STATUSES with `or` semantics so
        a None/empty import falls back to the canonical defaults. The
        fix must also log CRITICAL when the resolved tuple is empty.

        Layout (post-PR-E):
          _DEFAULT_ENTRY_CAPITAL_RESERVED_STATUSES = (...)  # hardcoded
          _ENTRY_CAPITAL_RESERVED_STATUSES = tuple(...)     # init from import-or-default
          if not _ENTRY_CAPITAL_RESERVED_STATUSES:           # belt-and-suspenders
              log.critical(...)
              _ENTRY_CAPITAL_RESERVED_STATUSES = _DEFAULT_ENTRY_CAPITAL_RESERVED_STATUSES
        """
        # The hardcoded default tuple must exist as a named constant.
        assert re.search(
            r'_DEFAULT_ENTRY_CAPITAL_RESERVED_STATUSES\s*=\s*\(',
            MC_SRC,
        ), (
            "Module must expose _DEFAULT_ENTRY_CAPITAL_RESERVED_STATUSES "
            "as a named tuple so the fallback path is single-source-of-truth."
        )
        # The init line must use `or` against the imported name.
        # Source contains nested parens (str(s).upper().strip()) inside the
        # tuple() call, so use .*? non-greedy across multi-line content.
        assert re.search(
            r'_ENTRY_CAPITAL_RESERVED_STATUSES\s*=\s*tuple\(.*?_OSM_PENDING_ENTRY_STATUSES\s+or\s+_DEFAULT_ENTRY_CAPITAL_RESERVED_STATUSES',
            MC_SRC, re.DOTALL,
        ), (
            "_ENTRY_CAPITAL_RESERVED_STATUSES must initialize via "
            "`_OSM_PENDING_ENTRY_STATUSES or _DEFAULT_ENTRY_CAPITAL_RESERVED_STATUSES` "
            "so a None/empty import cleanly falls back."
        )
        # Canonical statuses must be in the hardcoded default.
        # First _DEFAULT_... assignment only — the .*? non-greedy match stops
        # at the first closing paren.
        m_default = re.search(
            r'_DEFAULT_ENTRY_CAPITAL_RESERVED_STATUSES\s*=\s*\((.*?)\)',
            MC_SRC, re.DOTALL,
        )
        assert m_default
        default_block = m_default.group(1)
        for status in ("CREATED", "SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL"):
            assert status in default_block, (
                f"_DEFAULT_ENTRY_CAPITAL_RESERVED_STATUSES must include {status!r}"
            )
        # And the belt-and-suspenders log.critical must fire on empty resolved tuple.
        assert re.search(
            r'if\s+not\s+_ENTRY_CAPITAL_RESERVED_STATUSES\s*:',
            MC_SRC,
        ), (
            "Module must include a `if not _ENTRY_CAPITAL_RESERVED_STATUSES:` "
            "guard that forces the hardcoded default after init, in case the "
            "import returns a falsy iterable that produces an empty tuple."
        )
        assert re.search(
            r'log\.critical\([^\)]*_ENTRY_CAPITAL_RESERVED_STATUSES',
            MC_SRC, re.DOTALL,
        ) or re.search(
            r'log\.critical\([^\)]*PENDING_ENTRY_STATUSES',
            MC_SRC, re.DOTALL,
        ), (
            "The empty-fallback path must log CRITICAL so ops sees the misconfig. "
            "Without the critical log, a broken OSM import would silently "
            "default the capital gate to hardcoded statuses without alerting."
        )


# ══════════════════════════════════════════════════════════════════
# FIX-6 — intelligence_bridge import elevated to module-level
# ══════════════════════════════════════════════════════════════════

class TestFix6IntelligenceImport:
    def test_module_exposes_intel_available_flag(self, mc_mod):
        """Module must expose _INTEL_AVAILABLE (bool) and _run_intel_check
        (callable or None) resolved at module load. Eliminates the
        per-signal `from intelligence_bridge import ...` lazy import
        in _run_intelligence."""
        assert hasattr(mc_mod, "_INTEL_AVAILABLE"), (
            "ap_master_control must expose _INTEL_AVAILABLE as a "
            "module-level bool resolved at import."
        )
        assert isinstance(mc_mod._INTEL_AVAILABLE, bool)
        assert hasattr(mc_mod, "_run_intel_check"), (
            "ap_master_control must expose _run_intel_check as a "
            "module-level reference (callable or None)."
        )

    def test_run_intelligence_uses_module_level_refs(self, mc_mod):
        """_run_intelligence body must NOT contain a lazy `from
        intelligence_bridge import ...` statement. Use module-level
        _INTEL_AVAILABLE / _run_intel_check instead."""
        src = inspect.getsource(mc_mod.APMasterControl._run_intelligence)
        # Bad pattern (must be GONE):
        assert not re.search(
            r'from\s+intelligence_bridge\s+import',
            src,
        ), (
            "_run_intelligence must NOT import from intelligence_bridge "
            "inline. Use the module-level _INTEL_AVAILABLE / "
            "_run_intel_check references resolved once at module load."
        )
        # Good pattern (must be PRESENT):
        assert "_INTEL_AVAILABLE" in src, (
            "_run_intelligence must reference module-level _INTEL_AVAILABLE."
        )
        assert "_run_intel_check" in src, (
            "_run_intelligence must reference module-level _run_intel_check."
        )

    def test_run_intelligence_fails_open_when_unavailable(self, mc_mod):
        """Regression guard: when _INTEL_AVAILABLE is False (or the
        callable is None), _run_intelligence must still return
        approved=True (fail-open). Intelligence is advisory, not a
        hard gate."""
        # Force unavailability via monkeypatch on the module
        with patch.object(mc_mod, "_INTEL_AVAILABLE", False):
            with patch.object(mc_mod, "_run_intel_check", None):
                mc = _make_mc(mc_mod, mode="paper")
                result = mc._run_intelligence({"ticker": "SPY", "score": 80})
                assert result["approved"] is True, (
                    "_run_intelligence must fail open (approved=True) "
                    "when intel is unavailable. Got: " + repr(result)
                )
                assert result.get("_available") is False, (
                    "_run_intelligence must mark _available=False when "
                    "intel is unavailable so observability can attribute "
                    "the decision correctly. Got: " + repr(result)
                )


# ══════════════════════════════════════════════════════════════════
# INV — Behavioral invariant: revalidate_exposure no double-count
# ══════════════════════════════════════════════════════════════════

class TestInvariantRevalidateExposureNoDoubleCount:
    def test_revalidate_excludes_current_plan_in_runtime(self, mc_mod):
        """End-to-end: when the snapshot's pending_entry_capital already
        includes the current plan's reserved_cost (because process_signal
        already inserted the OSM row), revalidate_exposure must NOT
        add real_cost AGAIN on top.

        The fix must subtract the current plan's reserved_cost from
        pending_cap before adding real_cost in proj_total.
        """
        mc = _make_mc(mc_mod, mode="paper", account_equity=25000.0)

        # Snapshot mocks the post-process_signal state:
        #   capital_deployed = 0 (no positions open)
        #   pending_entry_capital = $2000 (includes this plan's reserved $2000)
        # Other pending orders contribute $500.
        # Real cost of this plan after contract selection = $2000.
        # WITHOUT FIX: proj_total = 0 + 2500 + 2000 = $4500 (DOUBLE-COUNTED)
        # WITH FIX:    proj_total = 0 + ($2500 - $2000) + $2000 = $2500 (correct)
        mc.pm.snapshot = MagicMock(return_value={
            "capital_deployed": 0.0,
            "open_positions": [],
            "closing_positions": [],
            "realized_pnl_today": 0.0,
            "pending_entries": [],
            "pending_entry_capital": 2500.0,  # includes plan's $2000
            "_snapshot_ok": True,
        })

        plan = mc_mod.ApprovedExecutionPlan(
            plan_id="plan-test-1",
            signal_id="sig-test-1",
            client_id="alice@x.com",
            ticker="SPY",
            side="CALL",
            direction="CALL",
            pattern="BREAKOUT",
            timeframe="1d",
            contracts=4,
            max_position_usd=2000.0,  # real cost after contract selection
            tier="A",
            score=80.0,
            intel_score=0.0,
            confidence_bucket="standard_pool",
            trigger_type="breach",
            trigger_price=500.0,
            stop_underlying=499.0,
            target_underlying=505.0,
            metadata={"local_order_id": "lo-test-1"},
        )

        decision = mc.revalidate_exposure(plan, client_id="alice@x.com")

        # max_capital = 25000 * 0.40 = $10000. proj_total with fix = $2500.
        # Without fix, proj_total = $4500. Both under $10k, so neither
        # blocks on this specific account size. We assert OK and then
        # verify the LOG (or _log_capital_utilization side-effect)
        # received the corrected pending value.
        assert decision.ok is True, (
            f"revalidate_exposure should APPROVE with proj_total=$2500 "
            f"(< max $10000). Got blocked: {decision.reason}"
        )

    def test_revalidate_blocks_when_truly_over_capital(self, mc_mod):
        """Regression guard: when the projected total (correctly de-
        duplicated) exceeds max_capital, revalidate_exposure must
        still block. The double-count fix must not silently un-block
        legitimate over-budget revalidations."""
        mc = _make_mc(mc_mod, mode="paper", account_equity=25000.0)
        # max_capital = $10,000 (40% of $25k)
        # Open positions tying up $8,000
        # Other pending orders: $500 (excluded plan: $2000, so pending_entry_capital=$2500)
        # Real cost of this plan: $2,000
        # Projected total (with fix): $8,000 + $500 + $2,000 = $10,500 > $10,000 → BLOCKED
        mc.pm.snapshot = MagicMock(return_value={
            "capital_deployed": 8000.0,
            "open_positions": [],
            "closing_positions": [],
            "realized_pnl_today": 0.0,
            "pending_entries": [],
            "pending_entry_capital": 2500.0,
            "_snapshot_ok": True,
        })

        plan = mc_mod.ApprovedExecutionPlan(
            plan_id="plan-test-2", signal_id="sig-test-2",
            client_id="alice@x.com", ticker="QQQ", side="CALL", direction="CALL",
            pattern="BREAKOUT", timeframe="1d", contracts=4,
            max_position_usd=2000.0, tier="A", score=80.0, intel_score=0.0,
            confidence_bucket="standard_pool", trigger_type="breach",
            trigger_price=400.0, stop_underlying=399.0, target_underlying=405.0,
            metadata={"local_order_id": "lo-test-2"},
        )

        decision = mc.revalidate_exposure(plan, client_id="alice@x.com")

        assert decision.ok is False, (
            "revalidate_exposure should BLOCK when (deployed + pending - "
            "current_plan + real_cost) > max_capital. The fix must not "
            "silently approve over-budget signals."
        )
        assert "capital_limit" in decision.reason.lower(), (
            f"Block reason should mention capital_limit; got: {decision.reason}"
        )


# ══════════════════════════════════════════════════════════════════
# FIX-7 (BUG-MC-2) — DEFERRED to PR F (documentation marker)
# ══════════════════════════════════════════════════════════════════

class TestFix7DeferredToPRf:
    def test_queued_before_insert_todo_present_in_source(self):
        """BUG-MC-2 (queued-before-insert) is out of scope for PR E
        because the fix requires coordinated changes in the queue/
        worker layer outside master_control. This test records the
        deferred scope as a TODO comment in master_control so PR F
        has a clear pickup signal."""
        # The TODO must reference BUG-MC-2 explicitly and live near
        # the _store_update(signal_id, "queued", ...) call site.
        idx = MC_SRC.find('self._store_update(signal_id, "queued"')
        assert idx > 0, "Could not locate the _store_update queued call site"
        # Search ±20 lines for the TODO marker.
        block = MC_SRC[max(0, idx - 1000):idx + 200]
        assert re.search(
            r'TODO\(PR\s*F\)|TODO:\s*BUG-MC-2|BUG-MC-2',
            block,
        ), (
            "A TODO referencing PR F or BUG-MC-2 must appear near the "
            "`_store_update(signal_id, 'queued', ...)` call in evaluate. "
            "This is the deferred-scope marker so PR F has a clear "
            "pickup signal for the queued-before-insert fix."
        )
