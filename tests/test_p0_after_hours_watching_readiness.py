"""P0 #573: after-hours WATCHING readiness boundary — behavioral matrix.

Binding spec + amendment 2026-09-03:
  docs/pr_specs/p0_after_hours_watching_readiness_20260902.md
  PR #573

Fix the ONE proven readiness defect and nothing else. Real 244-row
production shape from the September 2 incident:

  {
    "id": <int>,
    "client_id": "jasoncosby1@gmail.com",
    "signal_id": "<uuid>",
    "status": "WATCHING",
    "created_ts": <aware datetime, Wed 2026-09-02 20:54:05Z .. 22:50:38Z>,
    "last_error": "after_hours_deferred:awaiting_overnight_reeval",
    "payload": {"ticker": "ACN", "side": "CALL", "score": 70,
                "timeframe": "1d", ...}   # NO execution_mode
  }

Under the amendment, a row earns the deferred exemption iff:
  - client_id matches the readiness client
  - status is exactly WATCHING
  - last_error is exactly the marker
  - created_ts is tz-aware, non-future, within lookback
  - source session is post_close on an NYSE trading day
    (created_ts_ET >= 16:00 on that trading day)
  - no matching ENTRY order exists (proven by SQL WHERE NOT EXISTS)
  - the next-trading-session 09:18 ET deadline has not passed
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import ap.preopen_readiness as pr


REPO_ROOT = Path(__file__).resolve().parents[1]
CLIENT = "jasoncosby1@gmail.com"
MARKER = pr.AFTER_HOURS_DEFERRED_MARKER


# ─── Fixed timeline anchors (all UTC) ────────────────────────────────────
WED_1655_ET_AS_UTC = datetime(2026, 9, 2, 20, 55, tzinfo=timezone.utc)
THU_0910_ET_AS_UTC = datetime(2026, 9, 3, 13, 10, tzinfo=timezone.utc)
THU_0918_ET_AS_UTC = datetime(2026, 9, 3, 13, 18, tzinfo=timezone.utc)
THU_0925_ET_AS_UTC = datetime(2026, 9, 3, 13, 25, tzinfo=timezone.utc)
THU_0930_ET_AS_UTC = datetime(2026, 9, 3, 13, 30, tzinfo=timezone.utc)
FRI_1700_ET_AS_UTC = datetime(2026, 9, 4, 21, 0,  tzinfo=timezone.utc)
TUE_HOLIDAY_DEADLINE = datetime(2026, 9, 8, 13, 18, tzinfo=timezone.utc)


# ─── Row builder — matches the exact production projection ───────────────
# The production shape does NOT include payload.execution_mode. Default here
# mirrors that reality. Tests that want empty/None payloads override.
_UNSET = object()


def _make_row(
    *,
    row_id: int = 1,
    signal_id: str = "sig-1",
    client_id: str = CLIENT,
    status: str = "WATCHING",
    last_error: str | None = MARKER,
    created_ts: datetime | None = None,
    payload_override: Any = _UNSET,
) -> dict:
    if payload_override is _UNSET:
        payload = {"ticker": "ACN", "side": "CALL", "score": 70,
                   "timeframe": "1d"}
    else:
        payload = payload_override
    return {
        "id": row_id,
        "signal_id": signal_id,
        "client_id": client_id,
        "status": status,
        "last_error": last_error,
        "created_ts": created_ts,
        "payload": payload,
    }


# ═══════════════════════════════════════════════════════════════════════════
# UNIT — classifier
# ═══════════════════════════════════════════════════════════════════════════

class TestClassifier:
    def _classify(self, row, *, now, client=CLIENT):
        return pr._classify_watching_row(
            row, client_id=client, execution_mode="live", now_utc=now,
        )

    # ── Amendment §A: exact 244-row production shape ─────────────────
    def test_post_close_row_without_payload_execution_mode_is_expected(self):
        """The critical amendment test: production payload has NO
        execution_mode field. Row must still earn the exemption."""
        row = _make_row(created_ts=WED_1655_ET_AS_UTC)
        assert "execution_mode" not in row["payload"]
        kind, diag = self._classify(row, now=THU_0910_ET_AS_UTC)
        assert kind == "expected_after_hours_deferred"
        assert diag["source_session"] == "post_close"
        assert diag["next_deadline_et"] == "2026-09-03T09:18:00-04:00"

    def test_post_close_row_with_empty_payload_still_qualifies(self):
        row = _make_row(created_ts=WED_1655_ET_AS_UTC, payload_override={})
        kind, _ = self._classify(row, now=THU_0910_ET_AS_UTC)
        assert kind == "expected_after_hours_deferred"

    def test_post_close_row_with_null_payload_still_qualifies(self):
        row = _make_row(created_ts=WED_1655_ET_AS_UTC, payload_override=None)
        kind, _ = self._classify(row, now=THU_0910_ET_AS_UTC)
        assert kind == "expected_after_hours_deferred"

    # ── Amendment §B: deadline behavior ──────────────────────────────
    def test_exactly_at_deadline_is_overdue(self):
        row = _make_row(created_ts=WED_1655_ET_AS_UTC)
        kind, _ = self._classify(row, now=THU_0918_ET_AS_UTC)
        assert kind == "after_hours_deferred_overdue"

    def test_after_deadline_is_overdue(self):
        row = _make_row(created_ts=WED_1655_ET_AS_UTC)
        kind, _ = self._classify(row, now=THU_0925_ET_AS_UTC)
        assert kind == "after_hours_deferred_overdue"

    # ── Amendment §C: ordinary orphan preserved ──────────────────────
    def test_missing_marker_falls_to_ordinary_orphan(self):
        row = _make_row(created_ts=WED_1655_ET_AS_UTC, last_error=None)
        assert self._classify(row, now=THU_0910_ET_AS_UTC)[0] == "ordinary_orphan"

    def test_wrong_marker_falls_to_ordinary_orphan(self):
        row = _make_row(created_ts=WED_1655_ET_AS_UTC,
                        last_error="after_hours_something_else")
        assert self._classify(row, now=THU_0910_ET_AS_UTC)[0] == "ordinary_orphan"

    def test_substring_of_marker_does_not_qualify(self):
        row = _make_row(created_ts=WED_1655_ET_AS_UTC,
                        last_error="prefix:" + MARKER + ":suffix")
        assert self._classify(row, now=THU_0910_ET_AS_UTC)[0] == "ordinary_orphan"

    def test_case_mutation_of_marker_does_not_qualify(self):
        row = _make_row(created_ts=WED_1655_ET_AS_UTC, last_error=MARKER.upper())
        assert self._classify(row, now=THU_0910_ET_AS_UTC)[0] == "ordinary_orphan"

    def test_whitespace_around_marker_does_not_qualify(self):
        row = _make_row(created_ts=WED_1655_ET_AS_UTC,
                        last_error=f" {MARKER} ")
        assert self._classify(row, now=THU_0910_ET_AS_UTC)[0] == "ordinary_orphan"

    # ── Amendment §D: regular-session contradiction ──────────────────
    def test_regular_session_marker_row_fails_closed(self):
        thu_1000_utc = datetime(2026, 9, 3, 14, 0, tzinfo=timezone.utc)
        row = _make_row(created_ts=thu_1000_utc)
        kind, diag = self._classify(row, now=THU_0930_ET_AS_UTC + timedelta(hours=1))
        assert kind == "ordinary_orphan"
        assert diag["source_session"] == "regular_session"

    def test_boundary_1559_et_is_regular_session(self):
        thu_1559_utc = datetime(2026, 9, 3, 19, 59, tzinfo=timezone.utc)
        row = _make_row(created_ts=thu_1559_utc)
        kind, diag = self._classify(
            row, now=datetime(2026, 9, 3, 20, 30, tzinfo=timezone.utc),
        )
        assert kind == "ordinary_orphan"
        assert diag["source_session"] == "regular_session"

    def test_boundary_1600_et_is_post_close(self):
        thu_1600_utc = datetime(2026, 9, 3, 20, 0, tzinfo=timezone.utc)
        row = _make_row(created_ts=thu_1600_utc)
        kind, diag = self._classify(
            row, now=datetime(2026, 9, 3, 20, 30, tzinfo=timezone.utc),
        )
        assert kind == "expected_after_hours_deferred"
        assert diag["source_session"] == "post_close"
        assert diag["next_deadline_et"].startswith("2026-09-04T09:18")

    # ── Amendment §E: pre-market contradiction ───────────────────────
    def test_premarket_marker_row_does_not_qualify(self):
        """A pre-market row (Thu 08:00 ET) carrying the marker must NOT
        earn the exemption under the amendment."""
        thu_0800_utc = datetime(2026, 9, 3, 12, 0, tzinfo=timezone.utc)
        row = _make_row(created_ts=thu_0800_utc)
        kind, diag = self._classify(row, now=THU_0910_ET_AS_UTC)
        assert kind == "ordinary_orphan"
        assert diag["source_session"] == "pre_market"

    def test_boundary_0929_et_is_pre_market(self):
        thu_0929_utc = datetime(2026, 9, 3, 13, 29, tzinfo=timezone.utc)
        row = _make_row(created_ts=thu_0929_utc)
        kind, diag = self._classify(
            row, now=datetime(2026, 9, 3, 13, 29, 30, tzinfo=timezone.utc),
        )
        assert kind == "ordinary_orphan"
        assert diag["source_session"] == "pre_market"

    # ── Amendment §F: corrupt timestamp fails closed ─────────────────
    def test_null_timestamp_fails_closed(self):
        row = _make_row(created_ts=None)
        assert self._classify(row, now=THU_0910_ET_AS_UTC)[0] == "ordinary_orphan"

    def test_malformed_timestamp_fails_closed(self):
        row = _make_row(created_ts="not-a-timestamp")
        assert self._classify(row, now=THU_0910_ET_AS_UTC)[0] == "ordinary_orphan"

    def test_naive_timestamp_fails_closed(self):
        row = _make_row(created_ts=datetime(2026, 9, 2, 20, 55))
        assert self._classify(row, now=THU_0910_ET_AS_UTC)[0] == "ordinary_orphan"

    def test_future_timestamp_fails_closed(self):
        row = _make_row(created_ts=THU_0910_ET_AS_UTC + timedelta(hours=1))
        assert self._classify(row, now=THU_0910_ET_AS_UTC)[0] == "ordinary_orphan"

    def test_row_older_than_lookback_fails_closed(self):
        old = THU_0910_ET_AS_UTC - timedelta(
            days=pr.AFTER_HOURS_DEFERRED_MAX_LOOKBACK_DAYS + 1
        )
        row = _make_row(created_ts=old)
        assert self._classify(row, now=THU_0910_ET_AS_UTC)[0] == "ordinary_orphan"

    # ── Amendment §G: weekend/holiday deadline resolution ────────────
    def test_friday_post_close_deadline_skips_weekend_and_labor_day(self):
        """Friday 17:00 ET post-close: deadline must resolve to the next
        actual NYSE trading session, skipping Sat, Sun, and Labor Day
        (Mon 2026-09-07). Correct deadline is Tue 2026-09-08 09:18 ET."""
        row = _make_row(created_ts=FRI_1700_ET_AS_UTC)
        sun = datetime(2026, 9, 6, 15, 0, tzinfo=timezone.utc)
        kind, diag = self._classify(row, now=sun)
        assert kind == "expected_after_hours_deferred"
        assert diag["source_session"] == "post_close"
        assert diag["next_deadline_utc"] == TUE_HOLIDAY_DEADLINE.isoformat()

    def test_saturday_created_row_does_not_qualify(self):
        """Amendment narrows to post_close on a trading day only. A row
        created Saturday (non-trading day) does not earn the exemption —
        the amendment only honors weekend transitions for Friday-close
        rows resolving forward."""
        sat = datetime(2026, 9, 5, 16, 0, tzinfo=timezone.utc)
        row = _make_row(created_ts=sat)
        kind, diag = self._classify(row, now=sat + timedelta(hours=1))
        assert kind == "ordinary_orphan"
        assert diag["source_session"] == "non_trading_day"

    # ── Client identity ──────────────────────────────────────────────
    def test_wrong_client_falls_to_ordinary_orphan(self):
        row = _make_row(created_ts=WED_1655_ET_AS_UTC,
                        client_id="other@example.com")
        assert self._classify(row, now=THU_0910_ET_AS_UTC)[0] == "ordinary_orphan"

    def test_blank_client_falls_to_ordinary_orphan(self):
        row = _make_row(created_ts=WED_1655_ET_AS_UTC, client_id="")
        assert self._classify(row, now=THU_0910_ET_AS_UTC)[0] == "ordinary_orphan"

    def test_processing_status_does_not_qualify(self):
        row = _make_row(created_ts=WED_1655_ET_AS_UTC, status="PROCESSING")
        assert self._classify(row, now=THU_0910_ET_AS_UTC)[0] == "ordinary_orphan"


# ═══════════════════════════════════════════════════════════════════════════
# UNIT — session/deadline helper
# ═══════════════════════════════════════════════════════════════════════════

class TestSessionAndDeadlineHelper:
    def test_post_close_trading_day(self):
        kind, dl = pr._resolve_source_session_and_deadline_et(WED_1655_ET_AS_UTC)
        assert kind == "post_close"
        assert dl.isoformat() == "2026-09-03T09:18:00-04:00"

    def test_friday_post_close_deadline_is_tuesday_after_labor_day(self):
        kind, dl = pr._resolve_source_session_and_deadline_et(FRI_1700_ET_AS_UTC)
        assert kind == "post_close"
        assert dl.isoformat() == "2026-09-08T09:18:00-04:00"

    def test_regular_session_returns_no_deadline(self):
        thu_1200_utc = datetime(2026, 9, 3, 16, 0, tzinfo=timezone.utc)
        kind, dl = pr._resolve_source_session_and_deadline_et(thu_1200_utc)
        assert kind == "regular_session"
        assert dl is None

    def test_pre_market_classified_but_amendment_does_not_exempt(self):
        thu_0800_utc = datetime(2026, 9, 3, 12, 0, tzinfo=timezone.utc)
        kind, _dl = pr._resolve_source_session_and_deadline_et(thu_0800_utc)
        assert kind == "pre_market"

    def test_non_trading_day_classified_but_amendment_does_not_exempt(self):
        sat = datetime(2026, 9, 5, 16, 0, tzinfo=timezone.utc)
        kind, _dl = pr._resolve_source_session_and_deadline_et(sat)
        assert kind == "non_trading_day"

    def test_unknown_year_fails_closed(self):
        kind, dl = pr._resolve_source_session_and_deadline_et(
            datetime(2050, 6, 3, 20, 55, tzinfo=timezone.utc),
        )
        assert kind == "unknown"
        assert dl is None

    def test_naive_input_fails_closed(self):
        kind, dl = pr._resolve_source_session_and_deadline_et(
            datetime(2026, 9, 2, 20, 55),
        )
        assert kind == "unknown"
        assert dl is None


# ═══════════════════════════════════════════════════════════════════════════
# END-TO-END — run_preopen_autonomous_readiness
# ═══════════════════════════════════════════════════════════════════════════

def _stub_readiness_common(monkeypatch, *, handoff=True, post_overnight=False,
                            trading_date="2026-09-03"):
    monkeypatch.setattr(pr, "_upsert_preopen_row",
                        lambda **kwargs: None)
    monkeypatch.setattr(pr, "_morning_handoff_success_exists",
                        lambda *a, **kw: handoff)
    monkeypatch.setattr(pr, "_post_overnight_reeval_success_exists",
                        lambda *a, **kw: post_overnight)
    monkeypatch.setattr(pr, "_trading_date", lambda now=None: trading_date)
    monkeypatch.setattr(pr, "_pod_mode", lambda: "live")


def _stub_query_client_state(monkeypatch, *, expected=(), overdue=(), orphans=(),
                              pending_trigger=(), stale_processing=()):
    exp = list(expected); ov = list(overdue); orph = list(orphans)
    pt = list(pending_trigger); sp = list(stale_processing)
    total = len(exp) + len(ov) + len(orph)

    def _fake(client_id, **_):
        return {
            "stale_processing_ids": sp,
            "watching_orphans": orph,
            "expected_after_hours_deferred": exp,
            "after_hours_deferred_overdue": ov,
            "pending_trigger_rows": pt,
            "watching_count": total,
        }
    monkeypatch.setattr(pr, "_query_client_state", _fake)


class _Broker:
    def __init__(self):
        self.submitted: list[dict] = []
        self.cancelled: list[dict] = []
    def submit_order(self, **kw):  # pragma: no cover
        self.submitted.append(kw)
    def cancel_order(self, **kw):  # pragma: no cover
        self.cancelled.append(kw)


class _Watcher:
    def __init__(self, owned: set[str] | None = None):
        self._owned = owned or set()
        self.registrations: list[str] = []
    def has_order(self, loid: str) -> bool:
        return loid in self._owned
    def register(self, loid: str) -> None:  # pragma: no cover
        self.registrations.append(loid)


def _make_runner(mode: str = "live", *, broker=None, watcher=None):
    return SimpleNamespace(
        email=CLIENT, mode=mode,
        initialized=SimpleNamespace(is_set=lambda: True),
        worker_thread=SimpleNamespace(is_alive=lambda: True),
        order_state_machine=object(),
        position_manager=object(),
        master_control=SimpleNamespace(mode=mode),
        core=SimpleNamespace(
            entry_watcher=watcher or _Watcher(),
            broker=broker or _Broker(),
            exit_eng=object(),
        ),
        contract_selector=SimpleNamespace(
            data_broker=SimpleNamespace(
                cfg=SimpleNamespace(
                    base_url="https://api.tradier.com" if mode == "live"
                    else "https://sandbox.tradier.com"
                )
            )
        ),
        base_url="https://api.tradier.com" if mode == "live"
                 else "https://sandbox.tradier.com",
        account_id="acct-1",
        _resolved_tradier_token="tok",
        _last_overnight_reeval_date="2026-09-03",
        _overnight_reeval_success_date="2026-09-03",
        is_alive=lambda: True,
        _get_token=lambda: "tok",
    )


class TestReadinessDecision:
    def test_244_expected_deferred_does_not_block_live(self, monkeypatch):
        _stub_readiness_common(monkeypatch)
        _stub_query_client_state(
            monkeypatch,
            expected=[{"id": i, "signal_id": f"sig-{i}"} for i in range(244)],
        )
        result = pr.run_preopen_autonomous_readiness(
            CLIENT, "live", dry_run=True, runner=_make_runner(),
            now=THU_0910_ET_AS_UTC,
        )
        assert result["status"] == "OK", result
        assert "watching_rows_missing_orders_recommend_new_rescue" \
            not in result["errors"]
        assert "after_hours_deferred_overdue" not in result["errors"]
        assert result["details"]["expected_after_hours_deferred_count"] == 244

    def test_overdue_blocks_live(self, monkeypatch):
        _stub_readiness_common(monkeypatch)
        _stub_query_client_state(monkeypatch,
                                 overdue=[{"id": 1, "signal_id": "sig-1"}])
        result = pr.run_preopen_autonomous_readiness(
            CLIENT, "live", dry_run=True, runner=_make_runner(),
            now=THU_0925_ET_AS_UTC,
        )
        assert result["status"] == "BLOCKED"
        assert "after_hours_deferred_overdue" in result["errors"]

    def test_ordinary_orphan_still_blocks(self, monkeypatch):
        _stub_readiness_common(monkeypatch)
        _stub_query_client_state(monkeypatch,
                                 orphans=[{"id": 1, "signal_id": "sig-1"}])
        result = pr.run_preopen_autonomous_readiness(
            CLIENT, "live", dry_run=True, runner=_make_runner(),
            now=THU_0910_ET_AS_UTC,
        )
        assert result["status"] in {"BLOCKED", "DEGRADED"}
        assert "watching_rows_missing_orders_recommend_new_rescue" \
            in result["errors"]

    def test_global_overnight_success_does_not_excuse_overdue(self, monkeypatch):
        _stub_readiness_common(monkeypatch, post_overnight=True)
        _stub_query_client_state(monkeypatch,
                                 overdue=[{"id": 1, "signal_id": "sig-1"}])
        result = pr.run_preopen_autonomous_readiness(
            CLIENT, "live", dry_run=True, runner=_make_runner(),
            now=THU_0925_ET_AS_UTC,
        )
        assert result["status"] == "BLOCKED"
        assert "after_hours_deferred_overdue" in result["errors"]

    def test_unowned_pending_trigger_still_blocks(self, monkeypatch):
        _stub_readiness_common(monkeypatch)
        _stub_query_client_state(
            monkeypatch,
            expected=[{"id": 1, "signal_id": "s1"}],
            pending_trigger=[{"local_order_id": "L-2", "signal_id": "s2"}],
        )
        result = pr.run_preopen_autonomous_readiness(
            CLIENT, "live", dry_run=True,
            runner=_make_runner(watcher=_Watcher(owned=set())),
            now=THU_0910_ET_AS_UTC,
        )
        assert result["status"] == "BLOCKED"
        assert "pending_trigger_without_watcher_ownership" in result["errors"]

    def test_stale_processing_still_blocks(self, monkeypatch):
        _stub_readiness_common(monkeypatch)
        _stub_query_client_state(monkeypatch, stale_processing=[42])
        result = pr.run_preopen_autonomous_readiness(
            CLIENT, "live", dry_run=True, runner=_make_runner(),
            now=THU_0910_ET_AS_UTC,
        )
        assert "stale_processing_rows" in result["errors"]

    def test_zero_lifecycle_authority(self, monkeypatch):
        """Amendment §H: broker/watcher calls must be zero across all
        readiness paths, at any timestamp, in all bucket combinations."""
        _stub_readiness_common(monkeypatch)
        _stub_query_client_state(
            monkeypatch,
            expected=[{"id": 1, "signal_id": "s1"}],
            overdue=[{"id": 2, "signal_id": "s2"}],
        )
        broker = _Broker()
        watcher = _Watcher()
        runner = _make_runner(broker=broker, watcher=watcher)
        for now_ts in (THU_0910_ET_AS_UTC, THU_0925_ET_AS_UTC,
                       THU_0930_ET_AS_UTC):
            pr.run_preopen_autonomous_readiness(
                CLIENT, "live", dry_run=True, runner=runner, now=now_ts,
            )
        assert broker.submitted == []
        assert broker.cancelled == []
        assert watcher.registrations == []


# ═══════════════════════════════════════════════════════════════════════════
# STRUCTURAL — invariants against the module source itself
# ═══════════════════════════════════════════════════════════════════════════

class TestModuleSourceInvariants:
    def test_no_mutation_verbs_added(self):
        src = (REPO_ROOT / "ap" / "preopen_readiness.py").read_text()
        for verb in ("INSERT INTO trade_queue", "UPDATE trade_queue",
                     "DELETE FROM trade_queue", "INSERT INTO orders",
                     "UPDATE orders", "DELETE FROM orders",
                     "INSERT INTO positions", "UPDATE positions",
                     "INSERT INTO proof_trades", "INSERT INTO ap_signals",
                     "UPDATE ap_signals"):
            assert verb not in src, f"mutation SQL added: {verb!r}"

    def test_marker_constant_matches_producer(self):
        queue_src = (REPO_ROOT / "ap" / "queue.py").read_text()
        assert '_PAPER_OVERNIGHT_REEVAL_ONLY_ERROR = ' \
            f'"{MARKER}"' in queue_src
        assert pr.AFTER_HOURS_DEFERRED_MARKER == MARKER


# ═══════════════════════════════════════════════════════════════════════════
# REAL-POSTGRES INTEGRATION — the merge-gate proof against production shape
# ═══════════════════════════════════════════════════════════════════════════

import os as _os  # noqa: E402
_DB_URL = _os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "")
_pg_skip = pytest.mark.skipif(
    not _DB_URL,
    reason="disposable PostgreSQL URL not configured (INTELLIGENCE_POSTGRES_TEST_URL)",
)


@_pg_skip
class TestRealPostgresProductionShape:
    """Amendment §Required production-shaped test: at least the 244-row
    shape with the SAME payload the September 2 incident actually
    produced (NO execution_mode field). Uses real Postgres 17."""

    @pytest.fixture
    def db(self, monkeypatch):
        import psycopg2
        import psycopg2.extras

        conn = psycopg2.connect(_DB_URL)
        conn.autocommit = True
        conn.cursor_factory = psycopg2.extras.RealDictCursor
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS trade_queue CASCADE")
            cur.execute("DROP TABLE IF EXISTS orders CASCADE")
            cur.execute(
                """
                CREATE TABLE trade_queue (
                    id           SERIAL PRIMARY KEY,
                    client_id    TEXT NOT NULL,
                    signal_id    TEXT,
                    status       TEXT NOT NULL,
                    last_error   TEXT,
                    created_ts   TIMESTAMPTZ NOT NULL,
                    started_ts   TIMESTAMPTZ,
                    payload      JSONB
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE orders (
                    id                SERIAL PRIMARY KEY,
                    client_id         TEXT NOT NULL,
                    signal_id         TEXT,
                    kind              TEXT NOT NULL,
                    status            TEXT NOT NULL,
                    local_order_id    TEXT,
                    broker_order_id   TEXT,
                    submitted_ts      TIMESTAMPTZ,
                    filled_ts         TIMESTAMPTZ,
                    created_ts        TIMESTAMPTZ NOT NULL
                )
                """
            )

        import ap.db as _db

        @contextmanager
        def _real_conn():
            c = psycopg2.connect(_DB_URL)
            try:
                cur = c.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                try:
                    yield cur
                    c.commit()
                except Exception:
                    c.rollback()
                    raise
                finally:
                    cur.close()
            finally:
                c.close()

        monkeypatch.setattr(_db, "conn", _real_conn)
        monkeypatch.setattr(_db, "run_with_retry", lambda fn, **_: fn())

        yield conn

        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS trade_queue CASCADE")
            cur.execute("DROP TABLE IF EXISTS orders CASCADE")
        conn.close()

    def test_244_rows_matching_september_2_production_shape(self, db):
        """The definitive merge-gate: replay 244 rows in the EXACT shape
        produced by the September 2 incident (no execution_mode in
        payload), classify through the real production SQL path, and
        prove they are all expected_after_hours_deferred."""
        import json as _json

        real_payload = _json.dumps({
            "ticker": "ACN", "side": "CALL", "score": 70, "timeframe": "1d",
        })
        now_utc = THU_0910_ET_AS_UTC

        with db.cursor() as cur:
            for i in range(244):
                created_ts = datetime(
                    2026, 9, 2, 20, 54, 5, tzinfo=timezone.utc
                ) + timedelta(seconds=i * 28)  # ~2-hour span, matches production
                cur.execute(
                    """
                    INSERT INTO trade_queue
                      (client_id, signal_id, status, last_error,
                       created_ts, payload)
                    VALUES (%s, %s, 'WATCHING', %s, %s, %s::jsonb)
                    """,
                    (CLIENT, f"sig-{i}", MARKER, created_ts, real_payload),
                )

        state = pr._query_client_state(
            CLIENT, execution_mode="live", now=now_utc,
        )

        assert len(state["expected_after_hours_deferred"]) == 244, (
            f"expected all 244 rows in the deferred bucket; got: "
            f"{len(state['expected_after_hours_deferred'])}"
        )
        assert state["after_hours_deferred_overdue"] == []
        assert state["watching_orphans"] == []

    def test_mixed_state_including_contradictory_downstream_entry(self, db):
        """One row per category through real SQL:
          A: legitimate expected deferred
          B: overdue (Mon post-close, Tue 09:18 deadline passed)
          C: ordinary orphan (no marker, stale, no ENTRY)
          D: CONTRADICTORY — marker AND matching FILLED ENTRY (WHERE NOT
             EXISTS must remove it from every bucket)
        """
        import json as _json

        wed_close = datetime(2026, 9, 2, 20, 55, tzinfo=timezone.utc)
        mon_close = datetime(2026, 8, 31, 20, 55, tzinfo=timezone.utc)
        now_utc = THU_0910_ET_AS_UTC
        stale = now_utc - timedelta(minutes=30)
        real_payload = _json.dumps({"ticker": "SPY", "side": "CALL"})

        with db.cursor() as cur:
            cur.execute(
                """INSERT INTO trade_queue
                     (client_id, signal_id, status, last_error, created_ts, payload)
                   VALUES (%s, 'SIG-A', 'WATCHING', %s, %s, %s::jsonb)
                   RETURNING id""",
                (CLIENT, MARKER, wed_close, real_payload),
            )
            id_a = cur.fetchone()["id"]

            cur.execute(
                """INSERT INTO trade_queue
                     (client_id, signal_id, status, last_error, created_ts, payload)
                   VALUES (%s, 'SIG-B', 'WATCHING', %s, %s, %s::jsonb)
                   RETURNING id""",
                (CLIENT, MARKER, mon_close, real_payload),
            )
            id_b = cur.fetchone()["id"]

            cur.execute(
                """INSERT INTO trade_queue
                     (client_id, signal_id, status, last_error, created_ts, payload)
                   VALUES (%s, 'SIG-C', 'WATCHING', NULL, %s, %s::jsonb)
                   RETURNING id""",
                (CLIENT, stale, real_payload),
            )
            id_c = cur.fetchone()["id"]

            cur.execute(
                """INSERT INTO trade_queue
                     (client_id, signal_id, status, last_error, created_ts, payload)
                   VALUES (%s, 'SIG-D', 'WATCHING', %s, %s, %s::jsonb)
                   RETURNING id""",
                (CLIENT, MARKER, wed_close, real_payload),
            )
            id_d = cur.fetchone()["id"]
            cur.execute(
                """INSERT INTO orders
                     (client_id, signal_id, kind, status, local_order_id,
                      broker_order_id, submitted_ts, filled_ts, created_ts)
                   VALUES (%s, 'SIG-D', 'ENTRY', 'FILLED', 'L-D', 'B-D',
                           %s, %s, %s)""",
                (CLIENT,
                 wed_close + timedelta(minutes=1),
                 wed_close + timedelta(minutes=2),
                 wed_close + timedelta(minutes=1)),
            )

        state = pr._query_client_state(
            CLIENT, execution_mode="live", now=now_utc,
        )

        exp = {r["id"] for r in state["expected_after_hours_deferred"]}
        ov = {r["id"] for r in state["after_hours_deferred_overdue"]}
        orph = {r["id"] for r in state["watching_orphans"]}

        assert id_a in exp
        assert id_b in ov
        assert id_c in orph
        assert id_d not in exp
        assert id_d not in ov
        assert id_d not in orph

    def test_future_timestamp_row_lands_in_watching_orphans(self, db):
        """PR #573 amendment (2026-09-03 review 5101059272): a WATCHING
        row with the exact marker but a future created_ts must appear
        in watching_orphans, not silently vanish. Without the guard,
        `created_utc >= watching_cutoff` is trivially true for a
        future timestamp and the row was being skipped by the grace
        window — invisible to both the deferred buckets and the
        orphan bucket."""
        import json as _json

        now_utc = THU_0910_ET_AS_UTC
        future_ts = now_utc + timedelta(hours=2)  # clearly future
        real_payload = _json.dumps({"ticker": "SPY"})

        with db.cursor() as cur:
            cur.execute(
                """INSERT INTO trade_queue
                     (client_id, signal_id, status, last_error, created_ts, payload)
                   VALUES (%s, 'SIG-FUTURE', 'WATCHING', %s, %s, %s::jsonb)
                   RETURNING id""",
                (CLIENT, MARKER, future_ts, real_payload),
            )
            id_future = cur.fetchone()["id"]

        state = pr._query_client_state(
            CLIENT, execution_mode="live", now=now_utc,
        )

        exp = {r["id"] for r in state["expected_after_hours_deferred"]}
        ov = {r["id"] for r in state["after_hours_deferred_overdue"]}
        orph = {r["id"] for r in state["watching_orphans"]}

        assert id_future not in exp
        assert id_future not in ov
        assert id_future in orph, (
            "future-timestamp WATCHING row must appear in watching_orphans; "
            "without the created_utc <= now_utc guard it silently disappears "
            f"from readiness. state={state}"
        )

    def test_premarket_and_regular_session_rows_do_not_qualify(self, db):
        """PR #573 amendment (2026-09-03 review 5101059272): a WATCHING
        row with the exact marker but a future created_ts must appear
        in watching_orphans, not silently vanish. Without the guard,
        `created_utc >= watching_cutoff` is trivially true for a
        future timestamp and the row was being skipped by the grace
        window — invisible to both the deferred buckets and the
        orphan bucket."""
        import json as _json

        now_utc = THU_0910_ET_AS_UTC
        future_ts = now_utc + timedelta(hours=2)  # clearly future
        real_payload = _json.dumps({"ticker": "SPY"})

        with db.cursor() as cur:
            cur.execute(
                """INSERT INTO trade_queue
                     (client_id, signal_id, status, last_error, created_ts, payload)
                   VALUES (%s, 'SIG-FUTURE', 'WATCHING', %s, %s, %s::jsonb)
                   RETURNING id""",
                (CLIENT, MARKER, future_ts, real_payload),
            )
            id_future = cur.fetchone()["id"]

        state = pr._query_client_state(
            CLIENT, execution_mode="live", now=now_utc,
        )

        exp = {r["id"] for r in state["expected_after_hours_deferred"]}
        ov = {r["id"] for r in state["after_hours_deferred_overdue"]}
        orph = {r["id"] for r in state["watching_orphans"]}

        assert id_future not in exp
        assert id_future not in ov
        assert id_future in orph, (
            "future-timestamp WATCHING row must appear in watching_orphans; "
            "without the created_utc <= now_utc guard it silently disappears "
            f"from readiness. state={state}"
        )
        """Amendment §D, §E via real SQL."""
        import json as _json

        premkt = datetime(2026, 9, 3, 12, 0, tzinfo=timezone.utc)   # 08:00 ET
        regsess = datetime(2026, 9, 3, 16, 0, tzinfo=timezone.utc)  # 12:00 ET
        now_utc = datetime(2026, 9, 3, 20, 30, tzinfo=timezone.utc)
        real_payload = _json.dumps({"ticker": "SPY"})

        with db.cursor() as cur:
            cur.execute(
                """INSERT INTO trade_queue
                     (client_id, signal_id, status, last_error, created_ts, payload)
                   VALUES (%s, 'SIG-PM', 'WATCHING', %s, %s, %s::jsonb)
                   RETURNING id""",
                (CLIENT, MARKER, premkt, real_payload),
            )
            id_pm = cur.fetchone()["id"]
            cur.execute(
                """INSERT INTO trade_queue
                     (client_id, signal_id, status, last_error, created_ts, payload)
                   VALUES (%s, 'SIG-RS', 'WATCHING', %s, %s, %s::jsonb)
                   RETURNING id""",
                (CLIENT, MARKER, regsess, real_payload),
            )
            id_rs = cur.fetchone()["id"]

        state = pr._query_client_state(
            CLIENT, execution_mode="live", now=now_utc,
        )
        exp = {r["id"] for r in state["expected_after_hours_deferred"]}
        ov = {r["id"] for r in state["after_hours_deferred_overdue"]}
        assert id_pm not in exp and id_pm not in ov
        assert id_rs not in exp and id_rs not in ov
