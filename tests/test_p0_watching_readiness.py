"""P0 (monday-trade-flow-readiness): WATCHING readiness pass.

Proves (amendment requirement 4):
  • Disabled by default (AP_WATCHING_READINESS_PASS unset → no-op, no DB reads)
  • Stale rows created before the cutoff → ARCHIVED (READINESS_ARCHIVED_STALE)
  • Rows created on a non-trading day (holiday/weekend) → ARCHIVED
  • WATCHING rows whose paired ENTRY order is terminal → EXPIRED
    (READINESS_ORPHANED_ORDER_<status>) — the permanent-orphan dedup clog fix
  • Fresh same-session trading-day rows → left WATCHING (eligible) so the
    existing reseed machinery can re-arm them — the pass itself NEVER replays,
    NEVER resets to NEW (live_no_replay preserved)
  • Rows with broker proof (submitted_ts / broker_order_id) are NEVER mutated
    (inconsistent_submitted — surfaced for manual review)
  • dry_run performs full classification with ZERO writes
  • UPDATE statements touch ONLY status + last_error — client_id and
    execution_mode/payload are preserved byte-for-byte (identity preservation)
  • Allowlist archival only when env set; default-open when unset
"""
from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

import ap.db as apdb
import ap.watching_readiness as wr

ET = ZoneInfo("America/New_York")

# Fixed "now": Monday 2026-07-06 08:30 ET (pre-open acceptance morning).
NOW_ET = datetime(2026, 7, 6, 8, 30, tzinfo=ET)


def _utc(y, m, d, hh=14, mm=0):
    return datetime(y, m, d, hh, mm, tzinfo=timezone.utc)


class _FakeCursor:
    def __init__(self, rows, captured):
        self._rows = rows
        self.captured = captured
        self.rowcount = 0

    def execute(self, sql, params=None):
        self.captured["calls"].append((" ".join(sql.split()), params))
        if sql.strip().upper().startswith("SELECT"):
            self._result = self._rows
        else:
            # UPDATE: emulate CAS — succeed unless the row was marked moved
            qid = params[-1]
            moved = self.captured.get("moved_ids", set())
            self.rowcount = 0 if qid in moved else 1
            if self.rowcount:
                self.captured["updates"].append((sql, params))

    def fetchall(self):
        return self._result


class _FakeConnCtx:
    def __init__(self, rows, captured):
        self.rows = rows
        self.captured = captured

    def __enter__(self):
        return _FakeCursor(self.rows, self.captured)

    def __exit__(self, *exc):
        return False


def _install_fake_db(monkeypatch, rows, captured, moved_ids=None):
    captured.setdefault("calls", [])
    captured.setdefault("updates", [])
    captured["moved_ids"] = set(moved_ids or [])
    monkeypatch.setattr(apdb, "conn", lambda: _FakeConnCtx(rows, captured))
    monkeypatch.setattr(apdb, "run_with_retry", lambda fn, *a, **k: fn())


def _row(qid, signal_id, created, ticker="SPY", order_status=None,
         broker_order_id=None, submitted_ts=None):
    return {
        "id": qid,
        "signal_id": signal_id,
        "created_ts": created,
        "payload": {"ticker": ticker, "client_id": "jason@example.com",
                    "execution_mode": "live"},
        "order_status": order_status,
        "broker_order_id": broker_order_id,
        "submitted_ts": submitted_ts,
    }


@pytest.fixture
def enabled_env(monkeypatch):
    monkeypatch.setenv("AP_WATCHING_READINESS_PASS", "1")
    monkeypatch.setenv("AP_WATCHING_ARCHIVE_BEFORE_ET", "2026-07-02")
    monkeypatch.delenv("AP_ENTRY_TICKER_ALLOWLIST", raising=False)


def test_disabled_by_default_is_noop(monkeypatch):
    monkeypatch.delenv("AP_WATCHING_READINESS_PASS", raising=False)
    captured = {}
    _install_fake_db(monkeypatch, [], captured)
    res = wr.run_watching_readiness_pass("jason@example.com", dry_run=False, now=NOW_ET)
    assert res["enabled"] is False
    assert captured.get("calls", []) == [], "disabled pass must not touch the DB"


def test_stale_before_cutoff_archives(monkeypatch, enabled_env):
    captured = {}
    rows = [_row(1, "sig-old", _utc(2026, 6, 25))]  # June 25 < Jul 2 cutoff
    _install_fake_db(monkeypatch, rows, captured)
    res = wr.run_watching_readiness_pass("jason@example.com", dry_run=False, now=NOW_ET)
    assert res["archived_stale"] == 1
    sql, params = captured["updates"][0]
    assert params[0] == "ARCHIVED"
    assert params[1].startswith("READINESS_ARCHIVED_STALE:2026-06-25")


def test_non_trading_day_origin_archives(monkeypatch, enabled_env):
    captured = {}
    # 2026-07-03 = Independence Day observed (NYSE closed) — after the cutoff,
    # so it hits the non-trading-day rule, not the stale rule.
    rows = [_row(2, "sig-holiday", _utc(2026, 7, 3, 18))]
    _install_fake_db(monkeypatch, rows, captured)
    res = wr.run_watching_readiness_pass("jason@example.com", dry_run=False, now=NOW_ET)
    assert res["archived_non_trading_day"] == 1
    _, params = captured["updates"][0]
    assert params[0] == "ARCHIVED"
    assert "NON_TRADING_DAY:2026-07-03" in params[1]


def test_orphaned_terminal_order_expires_queue_row(monkeypatch, enabled_env):
    captured = {}
    # Fresh Jul-2 row (trading day, after cutoff) whose ENTRY order EXPIRED —
    # the permanent-orphan case: live reseed can't reattach it, dedup blocks
    # the signal forever. Must terminalize as EXPIRED.
    rows = [_row(3, "sig-orphan", _utc(2026, 7, 2, 15), order_status="EXPIRED")]
    _install_fake_db(monkeypatch, rows, captured)
    res = wr.run_watching_readiness_pass("jason@example.com", dry_run=False, now=NOW_ET)
    assert res["orphaned_terminalized"] == 1
    _, params = captured["updates"][0]
    assert params[0] == "EXPIRED"
    assert params[1] == "READINESS_ORPHANED_ORDER_EXPIRED"


def test_fresh_same_session_row_left_watching_for_reseed(monkeypatch, enabled_env):
    captured = {}
    # Jul-2 trading-day row, order still PENDING_TRIGGER, no broker proof →
    # ELIGIBLE. The pass must NOT touch it (no UPDATE) and NEVER reset to NEW;
    # re-arming is the existing reseed machinery's job.
    rows = [_row(4, "sig-fresh", _utc(2026, 7, 2, 15), order_status="PENDING_TRIGGER")]
    _install_fake_db(monkeypatch, rows, captured)
    res = wr.run_watching_readiness_pass("jason@example.com", dry_run=False, now=NOW_ET)
    assert res["eligible"] == 1
    assert captured["updates"] == []


def test_broker_proof_rows_never_mutated(monkeypatch, enabled_env):
    captured = {}
    rows = [_row(5, "sig-submitted", _utc(2026, 6, 20),  # stale AND submitted
                 broker_order_id="BRK-1", submitted_ts=_utc(2026, 6, 20, 15))]
    _install_fake_db(monkeypatch, rows, captured)
    res = wr.run_watching_readiness_pass("jason@example.com", dry_run=False, now=NOW_ET)
    assert res["inconsistent_submitted"] == 1
    assert captured["updates"] == [], "rows with broker proof are manual-review only"


def test_dry_run_classifies_but_writes_nothing(monkeypatch, enabled_env):
    captured = {}
    rows = [
        _row(1, "sig-old", _utc(2026, 6, 25)),
        _row(3, "sig-orphan", _utc(2026, 7, 2, 15), order_status="EXPIRED"),
        _row(4, "sig-fresh", _utc(2026, 7, 2, 15), order_status="PENDING_TRIGGER"),
    ]
    _install_fake_db(monkeypatch, rows, captured)
    res = wr.run_watching_readiness_pass("jason@example.com", dry_run=True, now=NOW_ET)
    assert res["archived_stale"] == 1
    assert res["orphaned_terminalized"] == 1
    assert res["eligible"] == 1
    assert captured["updates"] == [], "dry_run must write NOTHING"


def test_updates_touch_only_status_and_last_error(monkeypatch, enabled_env):
    """Identity preservation: client_id / execution_mode / payload survive."""
    captured = {}
    rows = [_row(1, "sig-old", _utc(2026, 6, 25))]
    _install_fake_db(monkeypatch, rows, captured)
    wr.run_watching_readiness_pass("jason@example.com", dry_run=False, now=NOW_ET)
    sql, _ = captured["updates"][0]
    normalized = " ".join(sql.split()).upper()
    assert "SET STATUS = %S, LAST_ERROR = %S" in normalized
    for forbidden in ("CLIENT_ID =", "PAYLOAD =", "EXECUTION_MODE =", "SIGNAL_ID ="):
        assert forbidden not in normalized, f"UPDATE must not touch {forbidden}"
    assert "AND STATUS = 'WATCHING'" in normalized, "CAS guard missing"


def test_cas_lost_row_is_skipped_not_counted(monkeypatch, enabled_env):
    captured = {}
    rows = [_row(9, "sig-race", _utc(2026, 6, 25))]
    _install_fake_db(monkeypatch, rows, captured, moved_ids={9})
    res = wr.run_watching_readiness_pass("jason@example.com", dry_run=False, now=NOW_ET)
    assert res["archived_stale"] == 0, "CAS-lost transition must not count"
    assert res["ok"] is True


def test_allowlist_default_open_when_unset(monkeypatch, enabled_env):
    captured = {}
    rows = [_row(6, "sig-nvda", _utc(2026, 7, 2, 15), ticker="NVDA",
                 order_status="PENDING_TRIGGER")]
    _install_fake_db(monkeypatch, rows, captured)
    res = wr.run_watching_readiness_pass("jason@example.com", dry_run=False, now=NOW_ET)
    assert res["eligible"] == 1, "no allowlist env → all scanner tickers eligible"
    assert res["archived_not_in_allowlist"] == 0


def test_allowlist_archives_only_when_env_set(monkeypatch, enabled_env):
    monkeypatch.setenv("AP_ENTRY_TICKER_ALLOWLIST", "SPY,QQQ,IWM")
    captured = {}
    rows = [_row(7, "sig-nvda", _utc(2026, 7, 2, 15), ticker="NVDA",
                 order_status="PENDING_TRIGGER")]
    _install_fake_db(monkeypatch, rows, captured)
    res = wr.run_watching_readiness_pass("jason@example.com", dry_run=False, now=NOW_ET)
    assert res["archived_not_in_allowlist"] == 1
    _, params = captured["updates"][0]
    assert params[1] == "READINESS_ARCHIVED_NOT_IN_ALLOWLIST:NVDA"
