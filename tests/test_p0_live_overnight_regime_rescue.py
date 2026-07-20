"""
tests/test_p0_live_overnight_regime_rescue.py
=============================================
Rescue tests — P0 compliant:
  - Patches: ap.live_overnight_rescue.conn / run_with_retry (module-level seams)
             ap_overnight_reeval.run_overnight_reeval
             ap_morning_handoff_audit.run_morning_handoff_audit
             ap.preopen_readiness.run_preopen_autonomous_readiness
  - Proves every orchestration stop condition
  - Proves dry-run returns eligible=N and verified=N (CTE-verified)
  - PostgreSQL large-volume test: seeds 53 eligible + ~3000 unrelated events
  - Tests INSERT failure after UPDATE causes full rollback
"""
from __future__ import annotations
import json
import os
import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch, call

import pytest

from ap.live_overnight_rescue import (
    INCIDENT_CLIENT_ID, INCIDENT_TRADING_DATE, INCIDENT_EXPECTED_ROWS,
    RECOVER_PR379_REGIME_TAXONOMY, _EXACT_REASONING,
    _DE_STAGE, _DE_DECISION, _DE_REASON_CODE, _DE_EXPLANATION,
    _build_rescue_population_sql,
)

JASON   = INCIDENT_CLIENT_ID
DATE    = INCIDENT_TRADING_DATE
PG_URL  = os.environ.get("INTELLIGENCE_POSTGRES_TEST_URL") or os.environ.get("DATABASE_URL")
FRIDAY_TS = "2026-07-17 20:30:00+00"
CALL_REASONING = _EXACT_REASONING[0]
PUT_REASONING  = _EXACT_REASONING[1]


def _mock_runner(mode="LIVE", email=JASON, initialized=True):
    r = MagicMock()
    r.mode  = mode; r.email = email
    r.initialized = MagicMock()
    r.initialized.is_set.return_value = initialized
    r.broker = MagicMock(); r.master_control = MagicMock()
    r.contract_selector = MagicMock(); r.order_state_machine = MagicMock()
    core = MagicMock(); core.entry_watcher = MagicMock()
    r.core = core
    return r


def _fake_db(n=53):
    """Returns a context manager that pretends to be conn() with COUNT=n."""
    class FakeCursor:
        description = [("n",)]
        def execute(self, sql, params): self._sql = sql
        def fetchone(self): return {"n": n}
        def fetchall(self): return []
        @property
        def rowcount(self): return n
    class FakeConn:
        def __enter__(self): return FakeCursor()
        def __exit__(self, *a): pass
    return FakeConn


# ── Guard tests ────────────────────────────────────────────────────────────────

def test_rescue_refuses_paper():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
    r = rescue_live_overnight_regime_rejections(
        client_id=JASON, execution_mode="paper", trading_date=DATE, dry_run=True)
    assert r["errors"] and r["writes"] == 0

def test_rescue_refuses_wrong_client():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
    r = rescue_live_overnight_regime_rejections(
        client_id="other@example.com", execution_mode="live", trading_date=DATE, dry_run=True)
    assert r["errors"] and INCIDENT_CLIENT_ID in r["errors"][0]

def test_rescue_refuses_wrong_date():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
    r = rescue_live_overnight_regime_rejections(
        client_id=JASON, execution_mode="live", trading_date="2026-07-21", dry_run=True)
    assert r["errors"] and r["writes"] == 0

def test_rescue_requires_runner_for_writes():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
    r = rescue_live_overnight_regime_rejections(
        client_id=JASON, execution_mode="live", trading_date=DATE,
        dry_run=False, runner=None)
    assert r["errors"] and "runner" in r["errors"][0].lower()

def test_rescue_refuses_paper_runner():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
    r = rescue_live_overnight_regime_rejections(
        client_id=JASON, execution_mode="live", trading_date=DATE,
        dry_run=True, runner=_mock_runner(mode="PAPER"))
    assert r["errors"] and r["writes"] == 0


# ── CTE query correctness ──────────────────────────────────────────────────────

def test_population_sql_uses_cte_not_correlated_like():
    """Verify the population query uses split_part CTE, not LIKE per row."""
    from ap.live_overnight_rescue import _build_rescue_population_sql
    from datetime import timezone
    w = datetime(2026, 7, 17, 20, 0, tzinfo=timezone.utc)
    sql, params = _build_rescue_population_sql(
        client_id=JASON, trading_date=DATE,
        tq_window_start_utc=w, tq_window_end_utc=w + timedelta(hours=18),
    )
    # Must use CTE with split_part
    assert "MATERIALIZED" in sql.upper()
    assert "split_part" in sql
    assert "original_signal_id" in sql
    # Must NOT use a correlated LIKE on candidate_id in the CTE
    cte_part = sql.split("FROM trade_queue")[0]
    assert "LIKE" not in cte_part, "CTE must not use LIKE for candidate_id join"
    # Must use UTC range, not DATE(ts AT TIME ZONE ...)
    assert "DATE(" not in cte_part
    assert "ts >= %s" in cte_part
    assert "ts <" in cte_part
    # Must use ANY() for reasoning
    assert "ANY(%s" in cte_part
    # Must use bigint[] for the UPDATE (in caller, but verify cast is referenced)


def test_population_sql_params_contain_reasoning_list():
    from ap.live_overnight_rescue import _build_rescue_population_sql
    from datetime import timezone
    w = datetime(2026, 7, 17, 20, 0, tzinfo=timezone.utc)
    _, params = _build_rescue_population_sql(
        client_id=JASON, trading_date=DATE,
        tq_window_start_utc=w, tq_window_end_utc=w + timedelta(hours=18),
    )
    reasoning_param = next(p for p in params if isinstance(p, list))
    assert CALL_REASONING in reasoning_param
    assert PUT_REASONING  in reasoning_param
    assert len(reasoning_param) == 2, "Must be exactly 2 exact reasoning strings"


def test_dry_run_returns_eligible_equals_verified():
    """Dry-run eligible == verified (CTE JOIN guarantees DE proof)."""
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections

    with (
        patch("ap.live_overnight_rescue.conn", return_value=_fake_db(53)()),
        patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()),
    ):
        r = rescue_live_overnight_regime_rejections(
            client_id=JASON, execution_mode="live", trading_date=DATE,
            dry_run=True, expected_count=53, runner=_mock_runner(),
        )

    assert r["eligible"] == 53
    assert r["verified"] == 53, f"verified must == eligible. Got {r['verified']}"
    assert r["writes"]   == 0
    assert not r["errors"]
    assert r["count_match"] is True


def test_dry_run_passes_statement_timeout_sql():
    """Dry-run must set statement_timeout before the count query."""
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
    seen_sql = []

    class TrackingCursor:
        description = [("n",)]
        def execute(self, sql, params=None):
            seen_sql.append(sql.strip().lower())
        def fetchone(self): return {"n": 53}
        def fetchall(self): return []
        @property
        def rowcount(self): return 0
    class TrackingConn:
        def __enter__(self): return TrackingCursor()
        def __exit__(self, *a): pass

    with (
        patch("ap.live_overnight_rescue.conn", return_value=TrackingConn()),
        patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()),
    ):
        rescue_live_overnight_regime_rejections(
            client_id=JASON, execution_mode="live", trading_date=DATE,
            dry_run=True, runner=_mock_runner(),
        )

    first_sql = seen_sql[0] if seen_sql else ""
    assert "statement_timeout" in first_sql, (
        f"First SQL must set statement_timeout. Got: {first_sql!r}"
    )


def test_expected_count_mismatch_aborts():
    with (
        patch("ap.live_overnight_rescue.conn", return_value=_fake_db(40)()),
        patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()),
    ):
        from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
        r = rescue_live_overnight_regime_rejections(
            client_id=JASON, execution_mode="live", trading_date=DATE,
            dry_run=True, expected_count=53, runner=_mock_runner(),
        )
    assert r["count_match"] is False and r["writes"] == 0 and r["errors"]


# ── Atomic rescue error conditions ────────────────────────────────────────────

def test_atomic_rescue_raises_on_locked_count_mismatch():
    from ap.live_overnight_rescue import _execute_atomic_rescue
    from datetime import timezone

    w = datetime(2026, 7, 17, 20, 0, tzinfo=timezone.utc)

    class TooFewCursor:
        description = [("id",),("signal_id",),("payload",),
                       ("last_error",),("result_json",),("finished_ts",)]
        def execute(self, sql, params=None): pass
        def fetchall(self):
            return [{"id": i, "signal_id": f"sig-{i}", "payload": None,
                     "last_error": "mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK",
                     "result_json": None, "finished_ts": None}
                    for i in range(40)]
        @property
        def rowcount(self): return 40
    class FakeConn:
        def __enter__(self): return TooFewCursor()
        def __exit__(self, *a): pass

    with patch("ap.live_overnight_rescue.conn", return_value=FakeConn()):
        with pytest.raises(ValueError, match="40 rows"):
            _execute_atomic_rescue(
                client_id=JASON, trading_date=DATE,
                tq_window_start_utc=w, tq_window_end_utc=w+timedelta(hours=18),
                expected_count=53, recovery_run_id="test",
                recovered_at="2026-07-20T09:00:00Z",
            )


def test_atomic_rescue_propagates_db_exception():
    from ap.live_overnight_rescue import _execute_atomic_rescue
    from datetime import timezone
    w = datetime(2026, 7, 17, 20, 0, tzinfo=timezone.utc)

    class BoomCursor:
        description = [("id",)]
        def execute(self, sql, params=None): raise Exception("connection lost")
        def fetchall(self): return []
        @property
        def rowcount(self): return 0
    class FakeConn:
        def __enter__(self): return BoomCursor()
        def __exit__(self, *a): pass

    with patch("ap.live_overnight_rescue.conn", return_value=FakeConn()):
        with pytest.raises(Exception, match="connection lost"):
            _execute_atomic_rescue(
                client_id=JASON, trading_date=DATE,
                tq_window_start_utc=w, tq_window_end_utc=w+timedelta(hours=18),
                expected_count=53, recovery_run_id="test",
                recovered_at="2026-07-20T09:00:00Z",
            )


# ── Orchestration stop conditions ──────────────────────────────────────────────

def test_orchestration_stops_on_missing_handoff_import():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight
    rescue_ok = {"eligible":53,"verified":53,"writes":53,"count_match":True,"errors":[],"dry_run":False}
    reeval_ok = {"processed":53,"armed":50,"rejected":3,"errors":0,"stalled":False}
    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections", return_value=rescue_ok),
        patch("ap_overnight_reeval.run_overnight_reeval", return_value=reeval_ok),
        patch.dict("sys.modules", {"ap_morning_handoff_audit": None}),
    ):
        r = recover_and_rerun_live_overnight(runner=_mock_runner(), trading_date=DATE, dry_run=False)
    assert r["errors"] and r["overall_status"] == "FAILED_HANDOFF"
    assert any("morning_handoff_audit" in e.lower() for e in r["errors"])


def test_orchestration_stops_when_reeval_has_errors():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight
    rescue_ok = {"eligible":53,"verified":53,"writes":53,"count_match":True,"errors":[],"dry_run":False}
    reeval_with_errors = {"processed":53,"armed":50,"errors":3,"stalled":False}
    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections", return_value=rescue_ok),
        patch("ap_overnight_reeval.run_overnight_reeval", return_value=reeval_with_errors),
    ):
        r = recover_and_rerun_live_overnight(runner=_mock_runner(), trading_date=DATE, dry_run=False)
    assert r["overall_status"] == "FAILED_REEVAL"
    assert r["handoff_result"] is None


def test_orchestration_stops_when_reeval_stalled():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight
    rescue_ok = {"eligible":53,"verified":53,"writes":53,"count_match":True,"errors":[],"dry_run":False}
    stalled = {"processed":53,"armed":0,"errors":0,"stalled":True}
    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections", return_value=rescue_ok),
        patch("ap_overnight_reeval.run_overnight_reeval", return_value=stalled),
    ):
        r = recover_and_rerun_live_overnight(runner=_mock_runner(), trading_date=DATE, dry_run=False)
    assert r["overall_status"] == "FAILED_REEVAL"
    assert r["handoff_result"] is None


def test_orchestration_stops_when_reeval_zero_armed():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight
    rescue_ok = {"eligible":53,"verified":53,"writes":53,"count_match":True,"errors":[],"dry_run":False}
    zero_armed = {"processed":53,"armed":0,"errors":0,"stalled":False}
    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections", return_value=rescue_ok),
        patch("ap_overnight_reeval.run_overnight_reeval", return_value=zero_armed),
    ):
        r = recover_and_rerun_live_overnight(runner=_mock_runner(), trading_date=DATE, dry_run=False)
    assert r["overall_status"] == "FAILED_REEVAL"
    assert "armed=0" in r["errors"][0]


def test_orchestration_stops_on_partial_rescue():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight
    partial = {"eligible":53,"verified":53,"writes":50,"count_match":False,"errors":[],"dry_run":False}
    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections", return_value=partial),
        patch("ap_overnight_reeval.run_overnight_reeval") as mock_r,
    ):
        r = recover_and_rerun_live_overnight(runner=_mock_runner(), trading_date=DATE, dry_run=False)
    mock_r.assert_not_called()
    assert r["overall_status"] == "FAILED_RESCUE"


def test_orchestration_stops_on_blocked_readiness():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight
    rescue_ok = {"eligible":53,"verified":53,"writes":53,"count_match":True,"errors":[],"dry_run":False}
    reeval_ok = {"processed":53,"armed":50,"errors":0,"stalled":False}
    handoff_ok = {"ok": True, "errors": []}
    blocked = {"status": "BLOCKED", "errors": ["live_watching_rows_not_materialized"]}
    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections", return_value=rescue_ok),
        patch("ap_overnight_reeval.run_overnight_reeval", return_value=reeval_ok),
        patch("ap_morning_handoff_audit.run_morning_handoff_audit", return_value=handoff_ok),
        patch("ap.preopen_readiness.run_preopen_autonomous_readiness", return_value=blocked),
    ):
        r = recover_and_rerun_live_overnight(runner=_mock_runner(), trading_date=DATE, dry_run=False)
    assert r["overall_status"] == "FAILED_READINESS"
    assert r["errors"]


def test_orchestration_complete_sets_recovery_complete():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight
    rescue_ok  = {"eligible":53,"verified":53,"writes":53,"count_match":True,"errors":[],"dry_run":False}
    reeval_ok  = {"processed":53,"armed":50,"errors":0,"stalled":False}
    handoff_ok = {"ok": True, "errors": []}
    ready_ok   = {"status": "OK", "errors": []}
    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections", return_value=rescue_ok),
        patch("ap_overnight_reeval.run_overnight_reeval", return_value=reeval_ok),
        patch("ap_morning_handoff_audit.run_morning_handoff_audit", return_value=handoff_ok),
        patch("ap.preopen_readiness.run_preopen_autonomous_readiness", return_value=ready_ok),
    ):
        r = recover_and_rerun_live_overnight(runner=_mock_runner(), trading_date=DATE, dry_run=False)
    assert r["overall_status"] == "RECOVERY_COMPLETE"
    assert not r["errors"]


def test_dry_run_sets_dry_run_ok():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight
    dry_preview = {"eligible":53,"verified":53,"writes":0,"count_match":True,"errors":[],"dry_run":True}
    with patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections", return_value=dry_preview):
        r = recover_and_rerun_live_overnight(runner=_mock_runner(), trading_date=DATE, dry_run=True)
    assert r["overall_status"] == "DRY_RUN_OK"
    assert r["reeval_result"] is None


def test_handoff_called_with_correct_signature():
    """run_morning_handoff_audit must be called with production signature."""
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight
    rescue_ok = {"eligible":53,"verified":53,"writes":53,"count_match":True,"errors":[],"dry_run":False}
    reeval_ok = {"processed":53,"armed":50,"errors":0,"stalled":False}
    handoff_ok = {"ok": True, "errors": []}
    ready_ok   = {"status": "OK", "errors": []}
    mock_handoff = MagicMock(return_value=handoff_ok)

    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections", return_value=rescue_ok),
        patch("ap_overnight_reeval.run_overnight_reeval", return_value=reeval_ok),
        patch("ap_morning_handoff_audit.run_morning_handoff_audit", mock_handoff),
        patch("ap.preopen_readiness.run_preopen_autonomous_readiness", return_value=ready_ok),
    ):
        recover_and_rerun_live_overnight(runner=_mock_runner(), trading_date=DATE, dry_run=False)

    mock_handoff.assert_called_once()
    kwargs = mock_handoff.call_args.kwargs
    assert "stage"  not in kwargs, f"stage= must not be passed. kwargs={kwargs}"
    assert "runner" not in kwargs, f"runner= must not be passed. kwargs={kwargs}"
    assert "dry_run" in kwargs
    assert kwargs["dry_run"] is False       # not `or False` — must be exactly False
    assert kwargs.get("execution_mode") == "live"


# ── Readiness tests ────────────────────────────────────────────────────────────

def test_readiness_degraded_zero_armed():
    from ap.preopen_readiness import _overnight_status
    reeval_row = {"status":"partial","details":{"fetched":53,"armed":0,"rejected":53,"errors":0}}
    runner = MagicMock(); runner._last_overnight_reeval_date = None
    with patch("ap.preopen_readiness._load_preopen_row", return_value=reeval_row):
        status, details = _overnight_status(
            runner, {"watching_count":53,"pending_trigger_rows":[]},
            DATE, client_id=JASON, execution_mode="live",
        )
    assert status == "degraded"
    assert details.get("error_code") == "OVERNIGHT_REEVAL_ZERO_ARMED"


def test_handoff_success_cannot_override_zero_armed():
    from ap.preopen_readiness import _overnight_status
    reeval_row = {"status":"partial","details":{"fetched":53,"armed":0,"rejected":53,"errors":0}}
    runner = MagicMock(); runner._last_overnight_reeval_date = None
    with (
        patch("ap.preopen_readiness._load_preopen_row", return_value=reeval_row),
        patch("ap.preopen_readiness._post_overnight_reeval_success_exists", return_value=True),
    ):
        status, _ = _overnight_status(
            runner, {"watching_count":0,"pending_trigger_rows":[]},
            DATE, client_id=JASON, execution_mode="live",
        )
    assert status == "degraded"


# ── Readiness persistence truth — all 5 classifications ──────────────────────

def _run_upsert(result_dict, existing_details=None):
    """Helper: runs _upsert_preopen_row_idempotent_overnight and returns what was upserted."""
    from ap.preopen_readiness import _upsert_preopen_row_idempotent_overnight
    existing = {"details": existing_details} if existing_details else None
    calls = []
    def capture(**kwargs): calls.append(kwargs)
    with (
        patch("ap.preopen_readiness._load_preopen_row", return_value=existing),
        patch("ap.preopen_readiness._upsert_preopen_row", side_effect=capture),
    ):
        _upsert_preopen_row_idempotent_overnight(
            client_id=JASON, execution_mode="live",
            trading_date=DATE, result=result_dict,
        )
    return calls[0] if calls else None


def test_readiness_class1_empty_no_errors():
    """fetched=0, errors=0, stalled=False → ok (genuine no-op)."""
    call = _run_upsert({"fetched":0,"armed":0,"errors":0,"stalled":False})
    assert call is not None and call["status"] == "ok" and call["mark_success"] is True


def test_readiness_class2_empty_with_errors():
    """fetched=0, errors>0 → partial/degraded."""
    call = _run_upsert({"fetched":0,"armed":0,"errors":1,"stalled":False})
    assert call is not None and call["status"] == "partial" and call["mark_success"] is False


def test_readiness_class2b_empty_stalled():
    """fetched=0, stalled=True → partial/degraded."""
    call = _run_upsert({"fetched":0,"armed":0,"errors":0,"stalled":True})
    assert call is not None and call["status"] == "partial" and call["mark_success"] is False


def test_readiness_class3_full_success():
    """fetched>0, armed>0, errors=0, stalled=False → ok."""
    call = _run_upsert({"fetched":53,"armed":50,"errors":0,"stalled":False})
    assert call is not None and call["status"] == "ok" and call["mark_success"] is True


def test_readiness_class4_partial_with_errors():
    """fetched>0, armed>0, errors>0 → partial."""
    call = _run_upsert({"fetched":53,"armed":40,"errors":3,"stalled":False})
    assert call is not None and call["status"] == "partial" and call["mark_success"] is False


def test_readiness_class5_all_terminal():
    """fetched>0, armed=0 → partial/degraded."""
    call = _run_upsert({"fetched":53,"armed":0,"errors":0,"stalled":False})
    assert call is not None and call["status"] == "partial" and call["mark_success"] is False


# ── PostgreSQL large-volume production-shape test ──────────────────────────────

@pytest.mark.skipif(not PG_URL, reason="No PostgreSQL URL")
def test_postgres_large_volume_and_rollback():
    """
    Seed 53 eligible rows + 53 matching DEs + ~3000 unrelated DEs.
    Verifies:
      - Optimized CTE dry-run returns eligible=53 verified=53 without timeout.
      - Optimized write updates exactly 53 atomically.
      - INSERT failure after UPDATE rolls back all queue updates.
      - One missing canonical DE makes write lock <53 → raises → zero mutations.
      - Second run is idempotent (finds 0 eligible).
      - 6 structural invalidations remain REJECTED.
      - No orders/positions mutated.
    Uses production key types: trade_queue.id BIGINT, positions.id TEXT.
    """
    import psycopg2, psycopg2.extras, time

    conn_pg = psycopg2.connect(PG_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    conn_pg.autocommit = False

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS trade_queue (
        id BIGSERIAL PRIMARY KEY,
        client_id TEXT, signal_id TEXT, status TEXT, payload JSONB,
        last_error TEXT, result_json JSONB,
        created_ts TIMESTAMPTZ DEFAULT NOW(),
        started_ts TIMESTAMPTZ, finished_ts TIMESTAMPTZ
    );
    CREATE TABLE IF NOT EXISTS decision_events (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT, candidate_id TEXT, client_id TEXT,
        stage TEXT, decision TEXT, reason_code TEXT, explanation TEXT,
        context_json JSONB, ts TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS orders (
        id BIGSERIAL PRIMARY KEY,
        client_id TEXT, signal_id TEXT, kind TEXT,
        broker_order_id TEXT, submitted_ts TIMESTAMPTZ, filled_ts TIMESTAMPTZ,
        status TEXT, created_ts TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS positions (
        id TEXT PRIMARY KEY,
        client_id TEXT, signal_id TEXT, status TEXT,
        created_ts TIMESTAMPTZ DEFAULT NOW()
    );
    """

    try:
        with conn_pg.cursor() as c:
            c.execute(SCHEMA)
            conn_pg.commit()

            # Seed 53 eligible trade_queue rows
            for i in range(53):
                sig = f"uuid-{i:04d}"   # UUID-style signal_id (no colons)
                c.execute("""
                    INSERT INTO trade_queue (client_id, signal_id, status, last_error, created_ts, payload)
                    VALUES (%s, %s, 'REJECTED', 'mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK',
                            '2026-07-17 20:30:00+00'::timestamptz, %s::jsonb)
                """, (JASON, sig, json.dumps({"ticker": f"T{i}", "side": "CALL"})))

            # Seed 6 structural invalidation rows
            for i in range(6):
                c.execute("""
                    INSERT INTO trade_queue (client_id, signal_id, status, last_error, created_ts, payload)
                    VALUES (%s, %s, 'REJECTED', 'overnight_invalidated:INVALIDATED_PRIOR_HIGH_BREACHED',
                            '2026-07-17 20:30:00+00'::timestamptz, '{}')
                """, (JASON, f"invalid-{i:04d}"))

            # Seed 53 canonical matching decision_events
            for i in range(53):
                sig    = f"uuid-{i:04d}"
                reason = CALL_REASONING if i % 2 == 0 else PUT_REASONING
                c.execute("""
                    INSERT INTO decision_events
                        (run_id, candidate_id, client_id, stage, decision,
                         reason_code, explanation, context_json, ts)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                            '2026-07-20 09:15:00 America/New_York'::timestamptz)
                """, (
                    f"reeval-run-{i}",
                    f"REEVAL:{sig}:abc{i:03d}",
                    JASON,
                    _DE_STAGE, _DE_DECISION, _DE_REASON_CODE, _DE_EXPLANATION,
                    json.dumps({
                        "intel_reason_code":    "INTEL_AUTHORITATIVE_VETO_RISK",
                        "intel_raw_status":     "RISK_VETO",
                        "intel_execution_mode": "LIVE",
                        "intel_reasoning":      reason,
                    }),
                ))

            # Seed ~3000 unrelated decision_events (different client/stage/date)
            # to detect an accidental O(queue × all_events) scan
            for batch in range(30):
                c.execute("""
                    INSERT INTO decision_events
                        (run_id, candidate_id, client_id, stage, decision,
                         reason_code, explanation, context_json, ts)
                    SELECT
                        'noise-run-' || gs,
                        'REEVAL:noise-' || gs || ':suffix',
                        'other@example.com',
                        'blocked_intel', 'REJECT', 'SESSION_RULE_BLOCK',
                        'INTEL_AUTHORITATIVE_VETO_RISK',
                        '{"intel_reasoning":"some other reason"}'::jsonb,
                        '2026-07-19 12:00:00+00'  -- different date
                    FROM generate_series(%s, %s) AS gs
                """, (batch * 100 + 1, batch * 100 + 100))

            conn_pg.commit()

        # Wire ap.db to use our test connection
        import contextlib

        @contextlib.contextmanager
        def _test_conn_cm():
            cur = conn_pg.cursor()
            try:
                yield cur
                conn_pg.commit()
            except Exception:
                conn_pg.rollback()
                raise

        # Test 1: Dry-run — must return 53 quickly (CTE, not correlated)
        t0 = time.monotonic()
        with (
            patch("ap.live_overnight_rescue.conn",          side_effect=_test_conn_cm),
            patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()),
        ):
            from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
            r_dry = rescue_live_overnight_regime_rejections(
                client_id=JASON, execution_mode="live", trading_date=DATE,
                dry_run=True, expected_count=53, runner=_mock_runner(),
            )
        elapsed = time.monotonic() - t0

        assert r_dry["eligible"] == 53, f"dry-run eligible={r_dry['eligible']}"
        assert r_dry["verified"] == 53, f"dry-run verified={r_dry['verified']}"
        assert not r_dry["errors"]
        assert elapsed < 8.0, f"Dry-run took {elapsed:.2f}s — too slow (CTE not working?)"

        # Test 2: Live write — updates exactly 53 atomically
        with patch("ap.live_overnight_rescue.conn", side_effect=_test_conn_cm):
            r_write = rescue_live_overnight_regime_rejections(
                client_id=JASON, execution_mode="live", trading_date=DATE,
                dry_run=False, expected_count=53, runner=_mock_runner(),
            )

        assert r_write["writes"] == 53, f"writes={r_write['writes']}"
        assert not r_write["errors"]

        # Verify WATCHING status and recovery_context stamped
        with conn_pg.cursor() as c:
            c.execute("SELECT COUNT(*) AS n FROM trade_queue WHERE client_id=%s AND status='WATCHING'",
                      (JASON,))
            assert int(c.fetchone()["n"]) == 53

        # Test 3: Recovery events use canonical columns
        with conn_pg.cursor() as c:
            c.execute("""
                SELECT COUNT(*) AS n FROM decision_events
                WHERE client_id=%s AND stage='overnight_recovery'
                  AND reason_code=%s AND run_id IS NOT NULL
                  AND candidate_id LIKE 'RECOVER:%%'
            """, (JASON, RECOVER_PR379_REGIME_TAXONOMY))
            assert int(c.fetchone()["n"]) == 53

        # Test 4: Idempotent second dry-run finds 0
        with (
            patch("ap.live_overnight_rescue.conn",          side_effect=_test_conn_cm),
            patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()),
        ):
            r_dry2 = rescue_live_overnight_regime_rejections(
                client_id=JASON, execution_mode="live", trading_date=DATE,
                dry_run=True, runner=_mock_runner(),
            )
        assert r_dry2["eligible"] == 0, f"Second dry-run must find 0. Got {r_dry2['eligible']}"

        # Test 5: 6 structural invalidations untouched
        with conn_pg.cursor() as c:
            c.execute("""
                SELECT COUNT(*) AS n FROM trade_queue
                WHERE client_id=%s AND last_error LIKE 'overnight_invalidated:%%'
                  AND status='REJECTED'
            """, (JASON,))
            assert int(c.fetchone()["n"]) == 6

        # Test 6: Missing DE causes locked_count < 53 → raises → zero mutations
        # Reset queue back to REJECTED
        with conn_pg.cursor() as c:
            c.execute("""
                UPDATE trade_queue
                SET status='REJECTED',
                    last_error='mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK',
                    payload=(payload - 'recovery_context')
                WHERE client_id=%s AND status='WATCHING'
            """, (JASON,))
            # Remove prior recovery DEs (idempotency guard would exclude rows otherwise)
            c.execute("DELETE FROM decision_events WHERE client_id=%s AND stage='overnight_recovery'",
                      (JASON,))
            # Delete one matching canonical DE
            c.execute("""
                DELETE FROM decision_events
                WHERE id = (
                    SELECT id FROM decision_events
                    WHERE client_id=%s AND stage='blocked_intel' AND reason_code='SESSION_RULE_BLOCK'
                    ORDER BY id LIMIT 1
                )
            """, (JASON,))
            conn_pg.commit()

        with patch("ap.live_overnight_rescue.conn", side_effect=_test_conn_cm):
            r_missing = rescue_live_overnight_regime_rejections(
                client_id=JASON, execution_mode="live", trading_date=DATE,
                dry_run=False, expected_count=53, runner=_mock_runner(),
            )

        assert r_missing["writes"] == 0, f"Missing DE must cause 0 writes. Got {r_missing['writes']}"
        assert r_missing["errors"]

        # Verify queue is still REJECTED (rolled back)
        with conn_pg.cursor() as c:
            c.execute("SELECT COUNT(*) AS n FROM trade_queue WHERE client_id=%s AND status='WATCHING'",
                      (JASON,))
            assert int(c.fetchone()["n"]) == 0, "Queue must still be REJECTED after rollback"

        # Test 7: INSERT failure after UPDATE → rollback all queue updates
        # Restore the missing DE first
        sig_0 = "uuid-0000"
        with conn_pg.cursor() as c:
            c.execute("""
                INSERT INTO decision_events
                    (run_id, candidate_id, client_id, stage, decision,
                     reason_code, explanation, context_json, ts)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                        '2026-07-20 09:15:00 America/New_York'::timestamptz)
            """, (
                "reeval-run-restored",
                f"REEVAL:{sig_0}:restored",
                JASON,
                _DE_STAGE, _DE_DECISION, _DE_REASON_CODE, _DE_EXPLANATION,
                json.dumps({
                    "intel_reason_code": "INTEL_AUTHORITATIVE_VETO_RISK",
                    "intel_raw_status": "RISK_VETO",
                    "intel_execution_mode": "LIVE",
                    "intel_reasoning": CALL_REASONING,
                }),
            ))
            conn_pg.commit()

        # Add a CHECK constraint that makes the INSERT fail for our recovery events
        with conn_pg.cursor() as c:
            c.execute("""
                ALTER TABLE decision_events
                ADD CONSTRAINT fail_recovery_insert
                CHECK (stage != 'overnight_recovery')
            """)
            conn_pg.commit()

        try:
            with patch("ap.live_overnight_rescue.conn", side_effect=_test_conn_cm):
                r_insert_fail = rescue_live_overnight_regime_rejections(
                    client_id=JASON, execution_mode="live", trading_date=DATE,
                    dry_run=False, expected_count=53, runner=_mock_runner(),
                )

            assert r_insert_fail["writes"] == 0, (
                f"INSERT failure must roll back UPDATE. writes={r_insert_fail['writes']}"
            )
            assert r_insert_fail["errors"]

            # Queue must still be REJECTED
            with conn_pg.cursor() as c:
                c.execute("SELECT COUNT(*) AS n FROM trade_queue WHERE client_id=%s AND status='WATCHING'",
                          (JASON,))
                assert int(c.fetchone()["n"]) == 0, "All queue rows must be REJECTED after rollback"
        finally:
            with conn_pg.cursor() as c:
                c.execute("ALTER TABLE decision_events DROP CONSTRAINT IF EXISTS fail_recovery_insert")
            conn_pg.commit()

        # Test 8: No orders/positions mutated
        with conn_pg.cursor() as c:
            c.execute("SELECT COUNT(*) AS n FROM orders WHERE client_id=%s", (JASON,))
            assert int(c.fetchone()["n"]) == 0
            c.execute("SELECT COUNT(*) AS n FROM positions WHERE client_id=%s", (JASON,))
            assert int(c.fetchone()["n"]) == 0

    finally:
        try:
            with conn_pg.cursor() as c:
                c.execute("DROP TABLE IF EXISTS trade_queue, decision_events, orders, positions CASCADE")
            conn_pg.commit()
        except Exception:
            pass
        conn_pg.close()
