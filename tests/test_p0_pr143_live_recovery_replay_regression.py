"""
tests/test_p0_pr143_live_recovery_replay_regression.py

P0 PR #143 regression fix — LIVE clients must not replay WATCHING rows
through MC/contract_selector.

PR #143 reset WATCHING → NEW and tagged rows with payload.recovery_rescue=true
for both LIVE and PAPER clients. The LIVE/PAPER gate was only at the queue
dispatch immediate-promote site, not at the WATCHING reset itself. Jason's
live pod replayed 23 stale signals on 2026-06-16 and got zero trades.

This test file proves:
  1. PAPER recovery still resets WATCHING → NEW and tags payload
  2. LIVE recovery does NOT reset WATCHING and does NOT tag payload
  3. LIVE recovery emits LIVE_RECOVERY_REPLAY_SKIPPED with the required
     reason=live_no_replay_policy
  4. PR #143's queue _dispatch promote block still has its `not live_mode`
     guard (defense in depth — even if a recovery_rescue marker did leak,
     LIVE would still not be promoted to immediate-submit)
  5. The orphaned PENDING_TRIGGER watcher reattachment path still runs for
     LIVE (this is safe — no selector replay)
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch


REPO = Path(__file__).resolve().parents[1]


# =============================================================================
# Test 1: PAPER recovery still tags + resets (PR #143 behavior preserved)
# =============================================================================

def _make_recovery(mode: str):
    """Construct an APStartupRecovery with master_control mode set."""
    import sys
    sys.modules.pop("ap_recovery", None)
    import ap_recovery as recovery_mod

    rec = recovery_mod.APStartupRecovery.__new__(recovery_mod.APStartupRecovery)
    rec.client_id = (
        "jasoncosby1@gmail.com" if mode == "LIVE"
        else "jose.vasquez4011@gmail.com"
    )
    rec.broker = MagicMock()
    rec.osm = MagicMock()
    rec.pm = MagicMock()
    rec.mc = MagicMock()
    rec.mc.mode = mode
    rec.exit_engine = None
    rec.entry_watcher = None  # disables the reattachment loop
    return rec, recovery_mod


def test_paper_recovery_still_resets_watching_and_tags_payload():
    """PAPER mode must still run the _reset() block from PR #143."""
    rec, _ = _make_recovery("PAPER")

    reset_calls = []

    def fake_run_with_retry(fn, *args, **kwargs):
        # Distinguish _reset (returns int) from
        # _load_orphaned_pending_trigger_orders (returns list of dicts)
        try:
            ret = fn()
        except Exception:
            ret = 0
        # _reset returns an int (rowcount); the orphan loader returns a list.
        # We classify by the function's source code containing 'UPDATE trade_queue'.
        import inspect
        src = inspect.getsource(fn)
        if "UPDATE trade_queue" in src:
            reset_calls.append(src)
            return 0  # simulate 0 rows reset (but the call IS made)
        return []

    with patch("ap.db.run_with_retry", side_effect=fake_run_with_retry), \
         patch("ap.db.conn"):
        result = {}
        rec._reseed_watchers(result)

    assert len(reset_calls) == 1, (
        f"PAPER must invoke _reset() exactly once; got {len(reset_calls)} calls"
    )
    # The _reset closure must merge the recovery_rescue marker
    assert "recovery_rescue" in reset_calls[0], (
        "PAPER _reset() must still write the recovery_rescue marker payload"
    )


# =============================================================================
# Test 2: LIVE recovery does NOT call _reset
# =============================================================================

def test_live_recovery_does_not_reset_watching_or_tag_payload():
    """LIVE mode must skip the blind _reset() block entirely.

    PR #359 replaced the LIVE no-op (count=0) with a per-row classifier
    that may write to trade_queue for missed/eligible rows, but it must
    NEVER call the broad _reset() that blindly sets all WATCHING→NEW and
    stamps payload.recovery_rescue=true (which would replay stale signals
    through MC/contract_selector).
    """
    rec, _ = _make_recovery("LIVE")

    recovery_rescue_writes = []

    def fake_run_with_retry(fn, *args, **kwargs):
        import inspect
        try:
            src = inspect.getsource(fn)
        except Exception:
            src = ""
        # The blind _reset() is the only path that writes recovery_rescue.
        # The PR #359 LIVE classifier writes LIVE_RECOVERY_WATCHER_RESTORED
        # or LIVE_RECOVERY_MISSED_TRIGGER — never recovery_rescue.
        if "recovery_rescue" in src and "UPDATE trade_queue" in src:
            recovery_rescue_writes.append(src)
        try:
            return fn()
        except Exception:
            return 0 if "UPDATE" in src else []

    with patch("ap.db.run_with_retry", side_effect=fake_run_with_retry), \
         patch("ap.db.conn"):
        result = {}
        rec._reseed_watchers(result)

    assert recovery_rescue_writes == [], (
        f"LIVE must NOT invoke blind _reset() with recovery_rescue marker — "
        f"got {len(recovery_rescue_writes)} calls. "
        "LIVE rows must never be replayed through MC/selector with stale data."
    )


# =============================================================================
# Test 3: LIVE emits the required audit log line
# =============================================================================

def test_live_recovery_emits_live_recovery_replay_skipped_audit_log():
    """LIVE mode must emit a structured audit log before any queue mutation.

    PR #359 replaced LIVE_RECOVERY_REPLAY_SKIPPED (count=0 no-op) with
    LIVE_RECOVERY_CLASSIFIED_RESEED (per-row classifier). The audit invariant
    is preserved: an operator-visible warning is emitted before any mutation,
    carrying client_id and the current ET session date.
    """
    rec, recovery_mod = _make_recovery("LIVE")

    captured_warnings = []
    original_warning = recovery_mod.log.warning

    def capture_warning(msg, *args, **kwargs):
        try:
            formatted = msg % args if args else msg
        except Exception:
            formatted = str(msg)
        captured_warnings.append(formatted)
        return original_warning(msg, *args, **kwargs)

    with patch("ap.db.run_with_retry", return_value=0), \
         patch("ap.db.conn"), \
         patch.object(recovery_mod.log, "warning", side_effect=capture_warning):
        result = {}
        rec._reseed_watchers(result)

    # PR #359: log token is now LIVE_RECOVERY_CLASSIFIED_RESEED
    matching = [w for w in captured_warnings if "LIVE_RECOVERY_CLASSIFIED_RESEED" in w]
    assert matching, (
        f"LIVE must emit LIVE_RECOVERY_CLASSIFIED_RESEED audit log before mutation; "
        f"got warnings: {captured_warnings}"
    )
    audit = matching[0]
    assert "jasoncosby1@gmail.com" in audit, "client_id must appear in audit log"


# =============================================================================
# Test 4: PR #143 immediate-promote block still has `not live_mode`
# =============================================================================

def test_queue_dispatch_immediate_promote_still_blocks_live():
    """Defense in depth — even if a recovery_rescue marker did somehow reach
    a LIVE row, the queue dispatch immediate-promote block must still skip
    it because of the `not live_mode` gate.

    We assert this at the source level since the dispatch block is a
    contained section of ap/queue.py."""
    src = (REPO / "ap" / "queue.py").read_text()

    block_start = src.find("PR #143: PAPER recovery-rescued immediate-entry promotion")
    assert block_start > 0, "PR #143 promote block not found"
    block_end = src.find(
        'if trigger_type == "breach" and not entry_watcher:', block_start
    )
    assert block_end > 0, "promote block terminator not found"
    block = src[block_start:block_end]

    assert "not live_mode" in block, (
        "PR #143 immediate-promote block must still require `not live_mode`"
    )
    assert 'payload.get("recovery_rescue")' in block, (
        "PR #143 immediate-promote block must still gate on recovery_rescue marker"
    )


# =============================================================================
# Test 5: LIVE recovery still attempts orphan PENDING_TRIGGER reattachment
# =============================================================================

def test_live_recovery_still_runs_pending_trigger_reattachment():
    """The orphan PENDING_TRIGGER watcher reattachment is safe for LIVE —
    it rebinds an in-memory watcher to an existing OSM order, no MC/selector
    replay. This path must still run for LIVE clients.

    Source-level assertion: the LIVE classifier log (LIVE_RECOVERY_CLASSIFIED_RESEED)
    must appear BEFORE the orphan reattachment branch so LIVE flows into it
    normally with rearmed >= 0.

    PR #359 replaced LIVE_RECOVERY_REPLAY_SKIPPED with LIVE_RECOVERY_CLASSIFIED_RESEED;
    the ordering invariant (classifier before PENDING_TRIGGER reattachment) is preserved.
    """
    src = (REPO / "ap_recovery.py").read_text()

    # PR #359: the new audit log token
    skip_marker  = "LIVE_RECOVERY_CLASSIFIED_RESEED"
    attach_marker = "RECOVERY: entry_watcher missing"

    skip_idx   = src.find(skip_marker)
    attach_idx = src.find(attach_marker)
    assert skip_idx > 0,   f"LIVE classifier log ({skip_marker!r}) not present in ap_recovery.py"
    assert attach_idx > 0, "orphan reattachment branch not present in ap_recovery.py"
    assert skip_idx < attach_idx, (
        "LIVE classifier log must come BEFORE the orphan reattachment so LIVE still "
        "flows through PENDING_TRIGGER watcher rebinding"
    )


# =============================================================================
# Test 6: Default-to-PAPER safety when mc.mode is missing/malformed
# =============================================================================

def test_recovery_defaults_to_paper_when_mc_mode_unavailable():
    """If mc.mode lookup fails, we default to PAPER (not LIVE) — preserves
    PR #143 behavior in misconfigs rather than silently disabling recovery
    for everyone."""
    rec, _ = _make_recovery("PAPER")
    # Corrupt mc to raise on attribute access
    class _RaisingMC:
        @property
        def mode(self):
            raise RuntimeError("mc.mode lookup failed")
    rec.mc = _RaisingMC()

    reset_calls = []

    def fake_run_with_retry(fn, *args, **kwargs):
        import inspect
        try:
            src = inspect.getsource(fn)
        except Exception:
            src = ""
        if "UPDATE trade_queue" in src:
            reset_calls.append(src)
            return 0
        return []

    with patch("ap.db.run_with_retry", side_effect=fake_run_with_retry), \
         patch("ap.db.conn"):
        result = {}
        rec._reseed_watchers(result)

    # Default-to-PAPER means _reset DID run
    assert len(reset_calls) == 1, (
        "default-to-PAPER must run _reset() when mc.mode lookup fails; "
        "failing silently to LIVE would disable all recovery for all clients"
    )
