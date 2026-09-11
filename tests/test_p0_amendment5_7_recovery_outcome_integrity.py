"""P0 tests for Amendments §5 and §7 (recovery outcome integrity).

§5 — durable ownership on failed/impossible recovery:
  A resumable deferred row must never be left ownerless. When the rearm
  cannot happen (no entry_watcher wired) or the rearm returns False, the
  recovery pass records a durable recovery-ownership marker (non-destructive
  meta patch) instead of silently dropping the row. Only recovery_* keys
  are written; contract, qty, limit, tp/sl, direction and selector evidence
  are preserved.

§7 — verified terminalization / cleanup return values:
  terminalize_deferred_breach returns True only when Postgres confirmed
  rowcount > 0. The recovery pass must not treat a False return as success.
  A failed terminalize is surfaced (errors entry) and, where the row would
  otherwise be dropped, retained with durable ownership (§5).
"""

from __future__ import annotations

import types
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from ap_recovery import APStartupRecovery


# ─────────────────────────── Recovery scaffold ──────────────────────────


class _RecoveryCursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, *_a, **_k):
        return self

    def fetchall(self):
        return self.rows


class _RecoveryConn:
    def __init__(self, rows):
        self.cursor = _RecoveryCursor(rows)

    def __enter__(self):
        return self.cursor

    def __exit__(self, *_):
        return False


def _recovery_row(lifecycle, *, created_ts=None, contract="DEFERRED:SPY"):
    meta = {
        "lifecycle_state": lifecycle,
        "materialization_status": (
            "RETRY_PENDING" if lifecycle == "RETRY_WAIT"
            else "RUNNING" if lifecycle == "MATERIALIZING"
            else "SELECTED" if lifecycle == "BROKER_READY"
            else "WAITING_FOR_TRIGGER"
        ),
        "materialization_generation": 2,
        "broker_ready": lifecycle == "BROKER_READY",
        "next_retry_at": (
            (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
            if lifecycle == "RETRY_WAIT" else None
        ),
        "materialization_lease_until": None,
        "signal_entry_price": 500.0,
        "execution_mode": "paper",
    }
    return {
        "local_order_id": f"oid-{lifecycle or 'waiting'}",
        "client_id": "client@example.com",
        "signal_id": f"sig-{lifecycle or 'waiting'}",
        "plan_id": "plan-1",
        "symbol": "SPY",
        "contract": contract,
        "direction": "CALL",
        "score": 80,
        "tier": "A",
        "trigger_price": 500.0,
        "stop_underlying": 495.0,
        "target_underlying": 510.0,
        "pattern": "2-1-2",
        "timeframe": "1d",
        "execution_mode": "paper",
        "qty": 1,
        "limit_price": 1.25,
        "reserved_cost": 125.0,
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "created_ts": created_ts or datetime.now(timezone.utc),
        "meta": meta,
    }


def _run(
    monkeypatch,
    rows,
    *,
    osm,
    entry_watcher,
    execution_core=None,
    recovery_client_id="client@example.com",
    master_mode="PAPER",
):
    import ap.db as ap_db
    monkeypatch.setattr(ap_db, "conn", lambda: _RecoveryConn(rows))
    monkeypatch.setattr(ap_db, "run_with_retry", lambda fn, *a, **k: fn())
    recovery = APStartupRecovery(
        client_id=recovery_client_id,
        broker=object(),
        osm=osm,
        pm=None,
        master_control=types.SimpleNamespace(mode=master_mode),
        entry_watcher=entry_watcher,
        execution_core=execution_core,
    )
    result = {"deferred_lifecycles_recovered": 0}
    recovery._recover_deferred_breach_lifecycles(result)
    return result


def _osm(**overrides):
    osm = types.SimpleNamespace(
        client_id="client@example.com",
        terminalize_deferred_breach=MagicMock(return_value=True),
        update_order_meta=MagicMock(return_value=True),
        submit_existing_entry=MagicMock(return_value={"ok": True}),
    )
    for k, v in overrides.items():
        setattr(osm, k, v)
    return osm


def _watcher(watch_return=True, has_order_return=False):
    return types.SimpleNamespace(
        has_order=lambda _oid: has_order_return,
        watch=MagicMock(return_value=watch_return),
    )


# ═══════════════════════════════════════════════════════════════════════
# §7 — verified terminalization
# ═══════════════════════════════════════════════════════════════════════


def test_stale_row_terminalize_failure_is_recorded(monkeypatch):
    """A stale (>72h) row whose terminalize returns False must surface a
    recovery_terminalize_failed error, not be treated as terminalized."""
    osm = _osm(terminalize_deferred_breach=MagicMock(return_value=False))
    old = datetime.now(timezone.utc) - timedelta(hours=100)
    result = _run(
        monkeypatch, [_recovery_row("", created_ts=old)],
        osm=osm, entry_watcher=_watcher(),
    )
    osm.terminalize_deferred_breach.assert_called_once()
    assert "recovery_terminalize_failed" in result.get("errors", [])


def test_stale_row_terminalize_success_records_no_error(monkeypatch):
    osm = _osm(terminalize_deferred_breach=MagicMock(return_value=True))
    old = datetime.now(timezone.utc) - timedelta(hours=100)
    result = _run(
        monkeypatch, [_recovery_row("", created_ts=old)],
        osm=osm, entry_watcher=_watcher(),
    )
    osm.terminalize_deferred_breach.assert_called_once()
    assert "recovery_terminalize_failed" not in result.get("errors", [])


def test_terminal_meta_row_terminalize_failure_is_recorded(monkeypatch):
    """A REJECTED-lifecycle pending row whose terminalize returns False
    surfaces the failure."""
    osm = _osm(terminalize_deferred_breach=MagicMock(return_value=False))
    result = _run(
        monkeypatch, [_recovery_row("REJECTED")],
        osm=osm, entry_watcher=_watcher(),
    )
    osm.terminalize_deferred_breach.assert_called_once()
    assert "recovery_terminalize_failed" in result.get("errors", [])


def test_terminalize_unavailable_is_recorded(monkeypatch):
    """If the OSM lacks terminalize_deferred_breach, that's surfaced."""
    osm = _osm()
    del osm.terminalize_deferred_breach
    old = datetime.now(timezone.utc) - timedelta(hours=100)
    result = _run(
        monkeypatch, [_recovery_row("", created_ts=old)],
        osm=osm, entry_watcher=_watcher(),
    )
    assert "recovery_terminalize_unavailable" in result.get("errors", [])


def test_broker_ready_terminal_durable_failure_retains_ownership(monkeypatch):
    """§2 returns TERMINAL_DURABLE but terminalize fails → §5 kicks in:
    the row is retained with durable ownership rather than left ownerless,
    and the terminalize failure is surfaced."""
    osm = _osm(terminalize_deferred_breach=MagicMock(return_value=False))
    ec = types.SimpleNamespace(
        resume_deferred_broker_ready_order=MagicMock(return_value={
            "disposition": "TERMINAL_DURABLE",
            "reason_code": "RECOVERY_TRIGGER_TOO_OLD",
            "terminal_status": "EXPIRED",
            "attempt": 1, "max_attempts": 20,
            "owner": "recovery_scheduler:client@example.com",
        }),
    )
    result = _run(
        monkeypatch, [_recovery_row("BROKER_READY")],
        osm=osm, entry_watcher=_watcher(), execution_core=ec,
    )
    osm.terminalize_deferred_breach.assert_called_once()
    assert "recovery_terminalize_failed" in result.get("errors", [])
    # §5 retention marker written after the failed terminalize
    osm.update_order_meta.assert_called_once()
    patch = osm.update_order_meta.call_args.args[1]
    assert patch["recovery_retention_reason"] == "broker_ready_terminalize_failed"


# ═══════════════════════════════════════════════════════════════════════
# §5 — durable ownership on failed / impossible rearm
# ═══════════════════════════════════════════════════════════════════════


def test_resumable_row_without_watcher_retains_ownership(monkeypatch):
    """A RETRY_WAIT row with no entry_watcher wired must NOT be dropped —
    a durable recovery-ownership marker is written instead."""
    osm = _osm()
    result = _run(
        monkeypatch, [_recovery_row("RETRY_WAIT")],
        osm=osm, entry_watcher=None,
    )
    osm.update_order_meta.assert_called_once()
    loid, patch = osm.update_order_meta.call_args.args
    assert loid == "oid-RETRY_WAIT"
    assert patch["recovery_ownership"] == "recovery_scheduler"
    assert patch["recovery_owner"] == "recovery_scheduler:client@example.com"
    assert patch["recovery_retention_reason"] == "entry_watcher_unavailable"
    assert patch["recovery_retention_mode"] == "PAPER"


def test_rearm_returns_false_retains_ownership(monkeypatch):
    """When watch() returns False the watcher did not take ownership;
    §5 records a durable marker so the row is resumed later."""
    osm = _osm()
    result = _run(
        monkeypatch, [_recovery_row("RETRY_WAIT")],
        osm=osm, entry_watcher=_watcher(watch_return=False),
    )
    osm.update_order_meta.assert_called_once()
    _, patch = osm.update_order_meta.call_args.args
    assert patch["recovery_retention_reason"] == "watcher_rearm_returned_false"
    assert result["deferred_lifecycles_recovered"] == 0


def test_rearm_success_does_not_retain_ownership(monkeypatch):
    """When watch() succeeds the watcher owns the row — no §5 marker."""
    osm = _osm()
    w = _watcher(watch_return=True)
    result = _run(
        monkeypatch, [_recovery_row("RETRY_WAIT")],
        osm=osm, entry_watcher=w,
    )
    w.watch.assert_called_once()
    osm.update_order_meta.assert_not_called()
    assert result["deferred_lifecycles_recovered"] == 1


def test_rtx_live_materialization_cas_miss_retention_failure_is_explicit(monkeypatch):
    """The exact RTX retry shape has one #596 owner or an auditable hold."""
    import ap_entry_watcher as ew
    import ap_lifecycle as lifecycle

    row = _recovery_row("RETRY_WAIT", contract="DEFERRED:RTX")
    row.update({
        "local_order_id": "84d9106b-7b67-4d58-b479-e9e65b9eb289",
        "client_id": "jasoncosby1@gmail.com",
        "signal_id": "322adca3-c407-491f-b5f5-102c2b0a5701",
        "symbol": "RTX",
        "direction": "PUT",
        "execution_mode": "live",
        "trigger_price": 198.13,
        "stop_underlying": 200.0,
        "target_underlying": 190.0,
    })
    row["meta"].update({
        "lifecycle_state": "RETRY_WAIT",
        "materialization_status": "RETRY_PENDING",
        "materialization_generation": 19,
        "retry_attempt": 13,
        "next_retry_at": "2099-01-01T00:00:00+00:00",
        "materialization_next_retry_at": "2099-01-01T00:00:00+00:00",
        "execution_mode": "live",
        "contract_deferred": True,
    })

    osm = _osm(
        client_id="jasoncosby1@gmail.com",
        adopt_deferred_retry_watcher=MagicMock(return_value=False),
        retain_recovery_ownership_if_no_watcher=MagicMock(return_value=False),
    )
    osm.cancel_pending_entry = MagicMock()
    broker = MagicMock()
    watcher = ew.APEntryWatcher(
        broker=broker,
        order_state_machine=osm,
        require_on_trigger=False,
        mode="LIVE",
    )
    watcher._persist_watcher_audit = lambda *a, **kw: None
    watcher._validate_local_order_id = MagicMock(return_value=True)
    watcher._is_live_runtime = MagicMock(return_value=False)
    watcher._get_quote = lambda _ticker: {
        "bid": 199.0, "ask": 199.1, "quote_age_ms": 1,
    }
    with lifecycle.LEDGER._entry_lock:
        lifecycle.LEDGER._current_state.clear()

    result = _run(
        monkeypatch,
        [row],
        osm=osm,
        entry_watcher=watcher,
        recovery_client_id="jasoncosby1@gmail.com",
        master_mode="LIVE",
    )

    osm.adopt_deferred_retry_watcher.assert_called_once_with(
        row["local_order_id"],
        watcher_token=watcher.owner_token,
        generation=19,
        retry_attempt=13,
        next_retry_at="2099-01-01T00:00:00+00:00",
        execution_mode="live",
    )
    osm.retain_recovery_ownership_if_no_watcher.assert_called_once_with(
        row["local_order_id"],
        recovery_owner="recovery_scheduler:jasoncosby1@gmail.com",
        reason="watcher_rearm_returned_false",
        recovery_retention_mode="LIVE",
    )
    assert "recovery_retention_write_failed" in result.get("errors", [])
    assert result["deferred_lifecycles_recovered"] == 0
    assert watcher._pending == []
    assert watcher._dedup_set == set()
    assert lifecycle.LEDGER.current_state(row["signal_id"]) is None
    assert not broker.mock_calls
    assert not osm.submit_existing_entry.mock_calls
    assert not osm.cancel_pending_entry.mock_calls


def test_retention_marker_writes_only_recovery_keys(monkeypatch):
    """§5 preservation: the durable ownership marker must never mutate a
    trade-policy or selector field — only recovery_* / recovery ownership
    keys are permitted."""
    osm = _osm()
    _run(
        monkeypatch, [_recovery_row("RETRY_WAIT")],
        osm=osm, entry_watcher=None,
    )
    _, patch = osm.update_order_meta.call_args.args
    forbidden = {
        "contract", "qty", "limit_price", "reserved_cost", "direction",
        "stop_underlying", "target_underlying", "broker_ready",
        "selected_contract", "selector_diagnostics", "trigger_price",
        "materialization_generation", "lifecycle_state",
    }
    assert not (set(patch) & forbidden), (
        f"§5 marker mutated protected field(s): {set(patch) & forbidden}"
    )
    assert all(
        k.startswith("recovery_") for k in patch
    ), f"§5 marker wrote non-recovery key(s): {[k for k in patch if not k.startswith('recovery_')]}"


def test_retention_write_failure_is_recorded(monkeypatch):
    """If the durable ownership write itself fails, that is surfaced."""
    osm = _osm(update_order_meta=MagicMock(return_value=False))
    result = _run(
        monkeypatch, [_recovery_row("RETRY_WAIT")],
        osm=osm, entry_watcher=None,
    )
    osm.update_order_meta.assert_called_once()
    assert "recovery_retention_write_failed" in result.get("errors", [])


def test_retention_unavailable_is_recorded(monkeypatch):
    """If OSM lacks update_order_meta, §5 cannot record ownership — surfaced."""
    osm = _osm()
    del osm.update_order_meta
    result = _run(
        monkeypatch, [_recovery_row("RETRY_WAIT")],
        osm=osm, entry_watcher=None,
    )
    assert "recovery_retention_unavailable" in result.get("errors", [])
