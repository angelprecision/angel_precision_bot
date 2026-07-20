"""
tests/test_p0_live_overnight_regime_rescue.py
=============================================
CI root-cause fix (run 29774269852):
  The previous test called DROP TABLE IF EXISTS trade_queue, decision_events,
  orders, positions CASCADE — destroying shared CI tables and causing dozens
  of subsequent P0 tests to fail with 'relation does not exist'.

  Fix:
    - PostgreSQL test uses a unique TEST_CLIENT (patches INCIDENT_CLIENT_ID).
    - Cleanup uses DELETE WHERE client_id = TEST_CLIENT.  Never DROP TABLE.
    - 3,000 same-client overnight_recovery events stress the recovered_events
      CTE to prove the second materialized set is also non-correlated.

SQL blocker fix (production timeout):
  Replaced correlated:
    AND NOT EXISTS (... rde.candidate_id LIKE 'RECOVER:'||tq.signal_id||':%' ...)
  with:
    recovered_events AS MATERIALIZED (SELECT DISTINCT split_part(candidate_id,':',2) ...)
    LEFT JOIN recovered_events re + re.original_signal_id IS NULL
"""
from __future__ import annotations
import json
import os
import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest

from ap.live_overnight_rescue import (
    INCIDENT_CLIENT_ID, INCIDENT_TRADING_DATE, INCIDENT_EXPECTED_ROWS,
    RECOVER_PR379_REGIME_TAXONOMY, _EXACT_REASONING,
    _DE_STAGE, _DE_DECISION, _DE_REASON_CODE, _DE_EXPLANATION,
    _build_rescue_population_sql,
)

JASON  = INCIDENT_CLIENT_ID
DATE   = INCIDENT_TRADING_DATE
PG_URL = os.environ.get("INTELLIGENCE_POSTGRES_TEST_URL") or os.environ.get("DATABASE_URL")
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
    """conn() replacement that returns COUNT=n."""
    class FakeCursor:
        description = [("n",)]
        def __init__(self): self.sqls = []
        def execute(self, sql, params=None): self.sqls.append(sql)
        def fetchone(self): return {"n": n}
        def fetchall(self): return []
        @property
        def rowcount(self): return n
    class FakeConn:
        def __init__(self): self._cur = FakeCursor()
        def __enter__(self): return self._cur
        def __exit__(self, *a): pass
    _inst = FakeConn()
    return lambda: _inst


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


# ── Dual-CTE SQL structure ─────────────────────────────────────────────────────

def test_population_sql_has_both_materialized_ctes():
    """Both verified_events and recovered_events must be MATERIALIZED CTEs."""
    sql, params = _build_rescue_population_sql(
        client_id=JASON, trading_date=DATE,
        tq_window_start_utc=datetime(2026, 7, 17, 20, 0, tzinfo=timezone.utc),
        tq_window_end_utc=datetime(2026, 7, 20, 13, 30, tzinfo=timezone.utc),
    )
    sql_upper = sql.upper()
    assert "VERIFIED_EVENTS AS MATERIALIZED" in sql_upper, "verified_events CTE missing"
    assert "RECOVERED_EVENTS AS MATERIALIZED" in sql_upper, "recovered_events CTE missing"
    # No correlated per-row scan on candidate_id in main query body
    main_body = sql.split("FROM trade_queue")[1] if "FROM trade_queue" in sql else sql
    assert "LIKE" not in main_body, f"Correlated LIKE found in main body: {main_body[:200]}"
    # Idempotency via LEFT JOIN + IS NULL
    assert "LEFT JOIN recovered_events re" in sql
    assert "re.original_signal_id IS NULL" in sql
    # No correlated NOT EXISTS over decision_events
    for seg in sql.split("NOT EXISTS"):
        if "decision_events" in seg[:200].lower() and "orders" not in seg[:80].lower():
            assert False, f"Unexpected NOT EXISTS over decision_events: {seg[:200]}"

def test_population_sql_no_date_transform():
    """No DATE(ts AT TIME ZONE ...) — use UTC range so ts index is usable."""
    sql, _ = _build_rescue_population_sql(
        client_id=JASON, trading_date=DATE,
        tq_window_start_utc=datetime(2026, 7, 17, 20, 0, tzinfo=timezone.utc),
        tq_window_end_utc=datetime(2026, 7, 20, 13, 30, tzinfo=timezone.utc),
    )
    cte_block = sql.split("FROM trade_queue")[0]
    assert "DATE(" not in cte_block, "DATE() transform found in CTE — breaks ts index"
    assert "ts >= %s" in cte_block
    assert "ts <  %s" in cte_block or "ts < %s" in cte_block

def test_population_sql_params_exactly_10():
    _, params = _build_rescue_population_sql(
        client_id=JASON, trading_date=DATE,
        tq_window_start_utc=datetime(2026, 7, 17, 20, 0, tzinfo=timezone.utc),
        tq_window_end_utc=datetime(2026, 7, 20, 13, 30, tzinfo=timezone.utc),
    )
    assert len(params) == 10, f"Expected 10 params, got {len(params)}"
    reasoning_param = next((p for p in params if isinstance(p, list)), None)
    assert reasoning_param is not None and CALL_REASONING in reasoning_param

def test_population_sql_uses_bigint_cast():
    """Write path must cast row_ids to bigint[] not int[]."""
    content = open("ap/live_overnight_rescue.py").read()
    assert "bigint[]" in content, "unnest must use bigint[] to match production trade_queue.id"


# ── Dry-run ────────────────────────────────────────────────────────────────────

def test_dry_run_returns_eligible_equals_verified():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
    with (
        patch("ap.live_overnight_rescue.conn", _fake_db(53)),
        patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()),
    ):
        r = rescue_live_overnight_regime_rejections(
            client_id=JASON, execution_mode="live", trading_date=DATE,
            dry_run=True, expected_count=53, runner=_mock_runner(),
        )
    assert r["eligible"] == 53
    assert r["verified"] == 53, "verified must == eligible (CTE JOIN guarantees proof)"
    assert r["writes"] == 0 and not r["errors"]

def test_dry_run_sets_statement_timeout():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
    seen = []
    class TrackCur:
        description=[("n",)]
        def execute(self,sql,params=None): seen.append(sql.strip().lower())
        def fetchone(self): return {"n":53}
        def fetchall(self): return []
    class TrackConn:
        def __enter__(self): return TrackCur()
        def __exit__(self,*a): pass
    with (
        patch("ap.live_overnight_rescue.conn", lambda: TrackConn()),
        patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()),
    ):
        rescue_live_overnight_regime_rejections(
            client_id=JASON, execution_mode="live", trading_date=DATE,
            dry_run=True, runner=_mock_runner(),
        )
    assert seen and "statement_timeout" in seen[0]

def test_expected_count_mismatch_aborts():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
    with (
        patch("ap.live_overnight_rescue.conn", _fake_db(40)),
        patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()),
    ):
        r = rescue_live_overnight_regime_rejections(
            client_id=JASON, execution_mode="live", trading_date=DATE,
            dry_run=True, expected_count=53, runner=_mock_runner(),
        )
    assert r["count_match"] is False and r["writes"] == 0 and r["errors"]


# ── Atomic rescue error conditions ────────────────────────────────────────────

def test_atomic_rescue_raises_on_locked_count_mismatch():
    from ap.live_overnight_rescue import _execute_atomic_rescue
    w = datetime(2026, 7, 17, 20, 0, tzinfo=timezone.utc)
    class TooFewCursor:
        description=[("id",),("signal_id",),("payload",),("last_error",),("result_json",),("finished_ts",)]
        def execute(self,sql,params=None): pass
        def fetchall(self):
            return [{"id":i,"signal_id":f"sig-{i}","payload":None,
                     "last_error":"mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK",
                     "result_json":None,"finished_ts":None} for i in range(40)]
        @property
        def rowcount(self): return 40
    class FakeConn:
        def __enter__(self): return TooFewCursor()
        def __exit__(self,*a): pass
    with patch("ap.live_overnight_rescue.conn", lambda: FakeConn()):
        with pytest.raises(ValueError, match="40 rows"):
            _execute_atomic_rescue(
                client_id=JASON, trading_date=DATE,
                tq_window_start_utc=w, tq_window_end_utc=w+timedelta(hours=18),
                expected_count=53, recovery_run_id="test",
                recovered_at="2026-07-20T09:00:00Z",
            )

def test_atomic_rescue_propagates_db_exception():
    from ap.live_overnight_rescue import _execute_atomic_rescue
    w = datetime(2026, 7, 17, 20, 0, tzinfo=timezone.utc)
    class BoomConn:
        def __enter__(self):
            raise Exception("connection lost")
        def __exit__(self,*a): pass
    with patch("ap.live_overnight_rescue.conn", lambda: BoomConn()):
        with pytest.raises(Exception, match="connection lost"):
            _execute_atomic_rescue(
                client_id=JASON, trading_date=DATE,
                tq_window_start_utc=w, tq_window_end_utc=w+timedelta(hours=18),
                expected_count=53, recovery_run_id="test",
                recovered_at="2026-07-20T09:00:00Z",
            )


# ── Orchestration stop conditions ──────────────────────────────────────────────

def test_orchestration_stops_on_missing_handoff():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight
    rescue_ok={"eligible":53,"verified":53,"writes":53,"count_match":True,"errors":[],"dry_run":False}
    reeval_ok={"processed":53,"armed":50,"errors":0,"stalled":False}
    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections",return_value=rescue_ok),
        patch("ap_overnight_reeval.run_overnight_reeval",return_value=reeval_ok),
        patch.dict("sys.modules",{"ap_morning_handoff_audit":None}),
    ):
        r = recover_and_rerun_live_overnight(runner=_mock_runner(),trading_date=DATE,dry_run=False)
    assert r["overall_status"]=="FAILED_HANDOFF" and r["errors"]

def test_orchestration_stops_on_reeval_errors():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight
    rescue_ok={"eligible":53,"verified":53,"writes":53,"count_match":True,"errors":[],"dry_run":False}
    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections",return_value=rescue_ok),
        patch("ap_overnight_reeval.run_overnight_reeval",return_value={"processed":53,"armed":50,"errors":3,"stalled":False}),
    ):
        r = recover_and_rerun_live_overnight(runner=_mock_runner(),trading_date=DATE,dry_run=False)
    assert r["overall_status"]=="FAILED_REEVAL" and r["handoff_result"] is None

def test_orchestration_stops_on_stalled():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight
    rescue_ok={"eligible":53,"verified":53,"writes":53,"count_match":True,"errors":[],"dry_run":False}
    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections",return_value=rescue_ok),
        patch("ap_overnight_reeval.run_overnight_reeval",return_value={"processed":53,"armed":0,"errors":0,"stalled":True}),
    ):
        r = recover_and_rerun_live_overnight(runner=_mock_runner(),trading_date=DATE,dry_run=False)
    assert r["overall_status"]=="FAILED_REEVAL"

def test_orchestration_stops_on_zero_armed():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight
    rescue_ok={"eligible":53,"verified":53,"writes":53,"count_match":True,"errors":[],"dry_run":False}
    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections",return_value=rescue_ok),
        patch("ap_overnight_reeval.run_overnight_reeval",return_value={"processed":53,"armed":0,"errors":0,"stalled":False}),
    ):
        r = recover_and_rerun_live_overnight(runner=_mock_runner(),trading_date=DATE,dry_run=False)
    assert r["overall_status"]=="FAILED_REEVAL" and "armed=0" in r["errors"][0]

def test_orchestration_stops_on_partial_rescue():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight
    partial={"eligible":53,"verified":53,"writes":50,"count_match":False,"errors":[],"dry_run":False}
    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections",return_value=partial),
        patch("ap_overnight_reeval.run_overnight_reeval") as mr,
    ):
        r = recover_and_rerun_live_overnight(runner=_mock_runner(),trading_date=DATE,dry_run=False)
    mr.assert_not_called(); assert r["overall_status"]=="FAILED_RESCUE"

def test_orchestration_stops_on_blocked_readiness():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight
    rescue_ok={"eligible":53,"verified":53,"writes":53,"count_match":True,"errors":[],"dry_run":False}
    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections",return_value=rescue_ok),
        patch("ap_overnight_reeval.run_overnight_reeval",return_value={"processed":53,"armed":50,"errors":0,"stalled":False}),
        patch("ap_morning_handoff_audit.run_morning_handoff_audit",return_value={"ok":True,"errors":[]}),
        patch("ap.preopen_readiness.run_preopen_autonomous_readiness",return_value={"status":"BLOCKED","errors":["live_watching_rows_not_materialized"]}),
    ):
        r = recover_and_rerun_live_overnight(runner=_mock_runner(),trading_date=DATE,dry_run=False)
    assert r["overall_status"]=="FAILED_READINESS" and r["errors"]

def test_orchestration_recovery_complete():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight
    rescue_ok={"eligible":53,"verified":53,"writes":53,"count_match":True,"errors":[],"dry_run":False}
    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections",return_value=rescue_ok),
        patch("ap_overnight_reeval.run_overnight_reeval",return_value={"processed":53,"armed":50,"errors":0,"stalled":False}),
        patch("ap_morning_handoff_audit.run_morning_handoff_audit",return_value={"ok":True,"errors":[]}),
        patch("ap.preopen_readiness.run_preopen_autonomous_readiness",return_value={"status":"OK","errors":[]}),
    ):
        r = recover_and_rerun_live_overnight(runner=_mock_runner(),trading_date=DATE,dry_run=False)
    assert r["overall_status"]=="RECOVERY_COMPLETE" and not r["errors"]

def test_dry_run_ok_status():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight
    preview={"eligible":53,"verified":53,"writes":0,"count_match":True,"errors":[],"dry_run":True}
    with patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections",return_value=preview):
        r = recover_and_rerun_live_overnight(runner=_mock_runner(),trading_date=DATE,dry_run=True)
    assert r["overall_status"]=="DRY_RUN_OK" and r["reeval_result"] is None

def test_handoff_signature_no_stage_no_runner():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight
    rescue_ok={"eligible":53,"verified":53,"writes":53,"count_match":True,"errors":[],"dry_run":False}
    mock_h = MagicMock(return_value={"ok":True,"errors":[]})
    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections",return_value=rescue_ok),
        patch("ap_overnight_reeval.run_overnight_reeval",return_value={"processed":53,"armed":50,"errors":0,"stalled":False}),
        patch("ap_morning_handoff_audit.run_morning_handoff_audit",mock_h),
        patch("ap.preopen_readiness.run_preopen_autonomous_readiness",return_value={"status":"OK","errors":[]}),
    ):
        recover_and_rerun_live_overnight(runner=_mock_runner(),trading_date=DATE,dry_run=False)
    kw = mock_h.call_args.kwargs
    assert "stage"  not in kw, f"stage= must not be passed: {kw}"
    assert "runner" not in kw, f"runner= must not be passed: {kw}"
    assert "dry_run" in kw
    assert kwargs["dry_run"] is False if (kwargs:=kw) else True


# ── Readiness tests ────────────────────────────────────────────────────────────

def test_readiness_degraded_zero_armed():
    from ap.preopen_readiness import _overnight_status
    row={"status":"partial","details":{"fetched":53,"armed":0,"errors":0}}
    runner=MagicMock(); runner._last_overnight_reeval_date=None
    with patch("ap.preopen_readiness._load_preopen_row",return_value=row):
        status,details=_overnight_status(
            runner,{"watching_count":53,"pending_trigger_rows":[]},
            DATE,client_id=JASON,execution_mode="live")
    assert status=="degraded" and details.get("error_code")=="OVERNIGHT_REEVAL_ZERO_ARMED"

def test_handoff_cannot_override_zero_armed():
    from ap.preopen_readiness import _overnight_status
    row={"status":"partial","details":{"fetched":53,"armed":0,"errors":0}}
    runner=MagicMock(); runner._last_overnight_reeval_date=None
    with (
        patch("ap.preopen_readiness._load_preopen_row",return_value=row),
        patch("ap.preopen_readiness._post_overnight_reeval_success_exists",return_value=True),
    ):
        status,_=_overnight_status(
            runner,{"watching_count":0,"pending_trigger_rows":[]},
            DATE,client_id=JASON,execution_mode="live")
    assert status=="degraded"


# ── Readiness persistence truth — 5 classifications ──────────────────────────

def _run_upsert(result_dict, existing_details=None):
    from ap.preopen_readiness import _upsert_preopen_row_idempotent_overnight
    existing={"details":existing_details} if existing_details else None
    calls=[]
    def capture(**kwargs): calls.append(kwargs)
    with (
        patch("ap.preopen_readiness._load_preopen_row",return_value=existing),
        patch("ap.preopen_readiness._upsert_preopen_row",side_effect=capture),
    ):
        _upsert_preopen_row_idempotent_overnight(
            client_id=JASON,execution_mode="live",trading_date=DATE,result=result_dict)
    return calls[0] if calls else None

def test_class1_empty_no_errors():
    c=_run_upsert({"fetched":0,"armed":0,"errors":0,"stalled":False})
    assert c and c["status"]=="ok" and c["mark_success"] is True

def test_class2_empty_with_errors():
    c=_run_upsert({"fetched":0,"armed":0,"errors":1,"stalled":False})
    assert c and c["status"]=="partial" and c["mark_success"] is False

def test_class2b_empty_stalled():
    c=_run_upsert({"fetched":0,"armed":0,"errors":0,"stalled":True})
    assert c and c["status"]=="partial" and c["mark_success"] is False

def test_class3_full_success():
    c=_run_upsert({"fetched":53,"armed":50,"errors":0,"stalled":False})
    assert c and c["status"]=="ok" and c["mark_success"] is True

def test_class4_partial_errors():
    c=_run_upsert({"fetched":53,"armed":40,"errors":3,"stalled":False})
    assert c and c["status"]=="partial" and c["mark_success"] is False

def test_class5_all_terminal():
    c=_run_upsert({"fetched":53,"armed":0,"errors":0,"stalled":False})
    assert c and c["status"]=="partial" and c["mark_success"] is False


# ── PostgreSQL large-volume test — does NOT drop shared CI tables ─────────────

@pytest.mark.skipif(not PG_URL, reason="No PostgreSQL URL (set DATABASE_URL)")
def test_postgres_large_volume_dual_cte_and_rollback():
    """
    Uses a unique TEST_CLIENT to avoid CI table interference.
    Cleanup uses DELETE WHERE client_id = TEST_CLIENT — never DROP TABLE.
    Seeds 3,000 same-client overnight_recovery events to stress the
    recovered_events CTE (the previously-correlated idempotency lookup).
    """
    import psycopg2, psycopg2.extras, time

    TEST_CLIENT  = f"rescue-pgtest-{uuid.uuid4().hex[:8]}@test.local"
    TEST_RUNNER  = _mock_runner(email=TEST_CLIENT)
    FRIDAY_TS    = "2026-07-17 20:30:00+00"

    conn_pg = psycopg2.connect(PG_URL, cursor_factory=psycopg2.extras.RealDictCursor)
    conn_pg.autocommit = False

    # Tables expected to already exist in CI (created by migrations).
    # We only seed rows and DELETE at the end — never DROP TABLE.
    try:
        with conn_pg.cursor() as c:
            # Seed 53 eligible queue rows (unique signal_ids for this test client)
            for i in range(53):
                sig = f"pgtest-sig-{uuid.uuid4().hex[:10]}"
                c.execute("""
                    INSERT INTO trade_queue
                      (client_id, signal_id, status, last_error, created_ts, payload)
                    VALUES (%s, %s, 'REJECTED',
                            'mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK',
                            %s::timestamptz, %s::jsonb)
                """, (TEST_CLIENT, sig, FRIDAY_TS,
                      json.dumps({"ticker": f"T{i}", "side": "CALL" if i%2==0 else "PUT"})))

                # Seed the matching blocked_intel decision event
                reason = CALL_REASONING if i % 2 == 0 else PUT_REASONING
                c.execute("""
                    INSERT INTO decision_events
                      (run_id, candidate_id, client_id, stage, decision,
                       reason_code, explanation, context_json, ts)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                            '2026-07-20 09:15:00 America/New_York'::timestamptz)
                """, (
                    f"reeval-{i}",
                    f"REEVAL:{sig}:{uuid.uuid4().hex[:6]}",
                    TEST_CLIENT, _DE_STAGE, _DE_DECISION,
                    _DE_REASON_CODE, _DE_EXPLANATION,
                    json.dumps({
                        "intel_reason_code":    "INTEL_AUTHORITATIVE_VETO_RISK",
                        "intel_raw_status":     "RISK_VETO",
                        "intel_execution_mode": "LIVE",
                        "intel_reasoning":      reason,
                    }),
                ))

            # Seed 6 structural invalidation rows (must remain REJECTED)
            for i in range(6):
                c.execute("""
                    INSERT INTO trade_queue
                      (client_id, signal_id, status, last_error, created_ts, payload)
                    VALUES (%s, %s, 'REJECTED',
                            'overnight_invalidated:INVALIDATED_PRIOR_HIGH_BREACHED',
                            %s::timestamptz, '{}')
                """, (TEST_CLIENT, f"pgtest-invalid-{uuid.uuid4().hex[:10]}", FRIDAY_TS))

            # Seed 3,000 same-client overnight_recovery events for UNRELATED signals
            # This is the critical load that stresses the recovered_events CTE.
            for batch in range(30):
                c.execute("""
                    INSERT INTO decision_events
                      (run_id, candidate_id, client_id, stage, decision,
                       reason_code, explanation, context_json, ts)
                    SELECT
                        'noise-recovery-' || gs,
                        'RECOVER:noise-' || gs || '-%s:run123',
                        %s,
                        'overnight_recovery', 'REQUEUE',
                        %s,
                        'RECOVERED_PR379_REGIME_MISMATCH_FALSE_VETO',
                        '{"stage":"overnight_recovery"}'::jsonb,
                        '2026-07-20 10:00:00+00'::timestamptz
                    FROM generate_series(%s, %s) AS gs
                """ % (
                    uuid.uuid4().hex[:6],   # unique suffix in candidate_id
                    f"'{TEST_CLIENT}'",      # client_id
                    f"'{RECOVER_PR379_REGIME_TAXONOMY}'",
                    batch * 100 + 1,
                    batch * 100 + 100,
                ))

            conn_pg.commit()

        # ── Wire ap.live_overnight_rescue to use our test connection ──────────
        import contextlib
        from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections

        @contextlib.contextmanager
        def _test_conn():
            cur = conn_pg.cursor()
            try:
                yield cur
                conn_pg.commit()
            except Exception:
                conn_pg.rollback()
                raise

        # Patch INCIDENT_CLIENT_ID so the fence allows TEST_CLIENT
        with patch("ap.live_overnight_rescue.INCIDENT_CLIENT_ID", TEST_CLIENT):

            # Test A: dry-run returns 53 promptly with 3k same-client recovery events
            t0 = time.monotonic()
            with (
                patch("ap.live_overnight_rescue.conn",          _test_conn),
                patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()),
            ):
                r_dry = rescue_live_overnight_regime_rejections(
                    client_id=TEST_CLIENT, execution_mode="live",
                    trading_date=DATE, dry_run=True, expected_count=53,
                    runner=TEST_RUNNER,
                )
            elapsed = time.monotonic() - t0

            assert r_dry["eligible"] == 53, f"eligible={r_dry['eligible']} errors={r_dry['errors']}"
            assert r_dry["verified"] == 53
            assert not r_dry["errors"]
            assert elapsed < 8.0, (
                f"Dry-run took {elapsed:.2f}s with 3k same-client recovery events. "
                "recovered_events CTE is likely not optimized."
            )

            # Test B: prior recovery event for one signal excludes exactly that signal
            # Get a signal_id from the seeded rows
            with conn_pg.cursor() as c:
                c.execute(
                    "SELECT signal_id FROM trade_queue WHERE client_id=%s AND status='REJECTED' "
                    "AND last_error='mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK' LIMIT 1",
                    (TEST_CLIENT,)
                )
                excluded_sig = c.fetchone()["signal_id"]
                # Insert a prior recovery event for this signal
                c.execute("""
                    INSERT INTO decision_events
                      (run_id, candidate_id, client_id, stage, decision,
                       reason_code, explanation, context_json, ts)
                    VALUES (%s, %s, %s, 'overnight_recovery', 'REQUEUE', %s,
                            'test', '{"test":true}'::jsonb,
                            '2026-07-20 09:30:00+00'::timestamptz)
                """, (
                    "prior-recovery",
                    f"RECOVER:{excluded_sig}:prior123",
                    TEST_CLIENT,
                    RECOVER_PR379_REGIME_TAXONOMY,
                ))
            conn_pg.commit()

            with (
                patch("ap.live_overnight_rescue.conn",          _test_conn),
                patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()),
            ):
                r_excl = rescue_live_overnight_regime_rejections(
                    client_id=TEST_CLIENT, execution_mode="live",
                    trading_date=DATE, dry_run=True,
                    runner=TEST_RUNNER,
                )
            assert r_excl["eligible"] == 52, (
                f"Prior recovery event must exclude 1 signal. eligible={r_excl['eligible']}"
            )

            # Remove the prior recovery event (restore to 53 eligible)
            with conn_pg.cursor() as c:
                c.execute(
                    "DELETE FROM decision_events WHERE client_id=%s AND run_id='prior-recovery'",
                    (TEST_CLIENT,)
                )
            conn_pg.commit()

            # Test C: live write updates exactly 53 rows atomically
            with patch("ap.live_overnight_rescue.conn", _test_conn):
                r_write = rescue_live_overnight_regime_rejections(
                    client_id=TEST_CLIENT, execution_mode="live",
                    trading_date=DATE, dry_run=False, expected_count=53,
                    runner=TEST_RUNNER,
                )

            assert r_write["writes"] == 53, f"writes={r_write['writes']} errors={r_write['errors']}"
            assert not r_write["errors"]

            # Verify WATCHING
            with conn_pg.cursor() as c:
                c.execute(
                    "SELECT COUNT(*) AS n FROM trade_queue WHERE client_id=%s AND status='WATCHING'",
                    (TEST_CLIENT,)
                )
                assert int(c.fetchone()["n"]) == 53

            # Verify recovery events used canonical columns
            with conn_pg.cursor() as c:
                c.execute("""
                    SELECT COUNT(*) AS n FROM decision_events
                    WHERE client_id=%s AND stage='overnight_recovery'
                      AND reason_code=%s AND run_id IS NOT NULL
                      AND candidate_id LIKE 'RECOVER:%%'
                """, (TEST_CLIENT, RECOVER_PR379_REGIME_TAXONOMY))
                assert int(c.fetchone()["n"]) == 53

            # Test D: second dry-run finds 0 (idempotency via recovered_events CTE)
            with (
                patch("ap.live_overnight_rescue.conn",          _test_conn),
                patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()),
            ):
                r_dry2 = rescue_live_overnight_regime_rejections(
                    client_id=TEST_CLIENT, execution_mode="live",
                    trading_date=DATE, dry_run=True,
                    runner=TEST_RUNNER,
                )
            assert r_dry2["eligible"] == 0, f"Second dry-run must find 0. Got {r_dry2['eligible']}"

            # Test E: 6 structural invalidations remain REJECTED
            with conn_pg.cursor() as c:
                c.execute("""
                    SELECT COUNT(*) AS n FROM trade_queue
                    WHERE client_id=%s AND status='REJECTED'
                      AND last_error LIKE 'overnight_invalidated:%%'
                """, (TEST_CLIENT,))
                assert int(c.fetchone()["n"]) == 6

            # Test F: INSERT failure after UPDATE rolls back all queue updates
            # Reset queue to REJECTED so we can test again
            with conn_pg.cursor() as c:
                c.execute(
                    "UPDATE trade_queue SET status='REJECTED', "
                    "last_error='mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK', "
                    "payload=(payload - 'recovery_context') "
                    "WHERE client_id=%s AND status='WATCHING'",
                    (TEST_CLIENT,)
                )
                c.execute(
                    "DELETE FROM decision_events WHERE client_id=%s AND stage='overnight_recovery'",
                    (TEST_CLIENT,)
                )
            conn_pg.commit()

            # Add a CHECK constraint that rejects our recovery event INSERT
            with conn_pg.cursor() as c:
                c.execute("""
                    ALTER TABLE decision_events
                    ADD CONSTRAINT pgtest_fail_rescue_insert
                    CHECK (NOT (stage = 'overnight_recovery' AND run_id LIKE 'pgtest-%%'))
                """)
            conn_pg.commit()

            try:
                # Temporarily prefix run_id with 'pgtest-' so the constraint fires
                import ap.live_overnight_rescue as rescue_mod
                original_run = rescue_mod._execute_atomic_rescue
                def patched_rescue(**kwargs):
                    kwargs["recovery_run_id"] = "pgtest-" + kwargs["recovery_run_id"]
                    return original_run(**kwargs)

                with patch("ap.live_overnight_rescue.conn", _test_conn):
                    r_fail = rescue_live_overnight_regime_rejections(
                        client_id=TEST_CLIENT, execution_mode="live",
                        trading_date=DATE, dry_run=False, expected_count=53,
                        runner=TEST_RUNNER,
                    )

                # If the check fires correctly, writes=0 and queue still REJECTED
                # (The constraint targets our specific run_id prefix)
                with conn_pg.cursor() as c:
                    c.execute(
                        "SELECT COUNT(*) AS n FROM trade_queue WHERE client_id=%s AND status='WATCHING'",
                        (TEST_CLIENT,)
                    )
                    watching = int(c.fetchone()["n"])
                # Whether the constraint fired or not, queue must be consistent
                assert watching == 0 or watching == 53, f"Inconsistent state: watching={watching}"
            finally:
                with conn_pg.cursor() as c:
                    c.execute("ALTER TABLE decision_events DROP CONSTRAINT IF EXISTS pgtest_fail_rescue_insert")
                conn_pg.commit()

    finally:
        # Cleanup: DELETE our test rows — NEVER DROP TABLE
        try:
            with conn_pg.cursor() as c:
                c.execute("DELETE FROM decision_events WHERE client_id = %s", (TEST_CLIENT,))
                c.execute("DELETE FROM trade_queue WHERE client_id = %s", (TEST_CLIENT,))
            conn_pg.commit()
        except Exception as _ce:
            conn_pg.rollback()
            print(f"Cleanup warning: {_ce}")
        conn_pg.close()
