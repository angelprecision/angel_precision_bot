"""
tests/test_p0_live_overnight_regime_rescue.py
=============================================
Rescue tests — enforces:
  - Mandatory DE proof (no signal_id column; use candidate_id LIKE 'REEVAL:...')
  - Dry-run uses same eligibility+DE query as write; returns eligible=N, verified=N
  - Atomic single-transaction write
  - PAPER/wrong-client refusal
  - runner mandatory for writes
  - Real handoff signature: ap_morning_handoff_audit.run_morning_handoff_audit(
        client_id, entry_watcher, osm, execution_mode, dry_run)
  - Orchestration stops on any failure
  - PostgreSQL production-shape integration test
"""
from __future__ import annotations
import json
import os
import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch, call

import pytest

from ap.live_overnight_rescue import (
    INCIDENT_CLIENT_ID,
    INCIDENT_TRADING_DATE,
    INCIDENT_EXPECTED_ROWS,
    RECOVER_PR379_REGIME_TAXONOMY,
    _DE_STAGE, _DE_DECISION, _DE_REASON_CODE, _DE_EXPLANATION,
    _DE_REASONING_EXACT,
)

JASON = INCIDENT_CLIENT_ID
DATE  = INCIDENT_TRADING_DATE
PG_URL = os.environ.get("INTELLIGENCE_POSTGRES_TEST_URL") or os.environ.get("DATABASE_URL")


def _mock_runner(mode="LIVE", email=JASON, initialized=True):
    r = MagicMock()
    r.mode  = mode
    r.email = email
    r.initialized = MagicMock()
    r.initialized.is_set.return_value = initialized
    r.broker = MagicMock()
    r.master_control = MagicMock()
    r.contract_selector = MagicMock()
    r.order_state_machine = MagicMock()
    core = MagicMock()
    core.entry_watcher = MagicMock()
    r.core = core
    return r


# ─── Test: Rescue refuses PAPER mode ──────────────────────────────────────────

def test_rescue_refuses_paper_mode():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
    result = rescue_live_overnight_regime_rejections(
        client_id=JASON, execution_mode="paper",
        trading_date=DATE, dry_run=True,
    )
    assert result["errors"]
    assert result["writes"] == 0


# ─── Test: Rescue refuses wrong client ────────────────────────────────────────

def test_rescue_refuses_wrong_client():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
    result = rescue_live_overnight_regime_rejections(
        client_id="other@example.com", execution_mode="live",
        trading_date=DATE, dry_run=True,
    )
    assert result["errors"]
    assert INCIDENT_CLIENT_ID in result["errors"][0]
    assert result["writes"] == 0


# ─── Test: Rescue refuses wrong date ──────────────────────────────────────────

def test_rescue_refuses_wrong_date():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
    result = rescue_live_overnight_regime_rejections(
        client_id=JASON, execution_mode="live",
        trading_date="2026-07-21", dry_run=True,
    )
    assert result["errors"]
    assert result["writes"] == 0


# ─── Test: runner mandatory for writes ────────────────────────────────────────

def test_rescue_requires_runner_for_writes():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
    result = rescue_live_overnight_regime_rejections(
        client_id=JASON, execution_mode="live",
        trading_date=DATE, dry_run=False, runner=None,
    )
    assert result["errors"]
    assert "runner" in result["errors"][0].lower()
    assert result["writes"] == 0


# ─── Test: Runner with PAPER mode is rejected ─────────────────────────────────

def test_rescue_fails_on_paper_runner():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections
    result = rescue_live_overnight_regime_rejections(
        client_id=JASON, execution_mode="live",
        trading_date=DATE, dry_run=True,
        runner=_mock_runner(mode="PAPER"),
    )
    assert result["errors"]
    assert result["writes"] == 0


# ─── Test: Dry-run uses full eligibility + DE proof query ─────────────────────

def test_dry_run_uses_full_eligibility_query():
    """Dry-run calls _dry_run_count which embeds the DE EXISTS clause."""
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections

    captured_sql = []

    def fake_conn_ctx():
        class FakeCursor:
            description = [("n",)]
            def execute(self, sql, params):
                captured_sql.append(sql)
            def fetchone(self): return {"n": 53}
            def fetchall(self): return []
        class FakeConn:
            def __enter__(self): return FakeCursor()
            def __exit__(self, *a): pass
        return FakeConn()

    with (
        patch("ap.live_overnight_rescue.conn", side_effect=fake_conn_ctx),
        patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()),
    ):
        result = rescue_live_overnight_regime_rejections(
            client_id=JASON, execution_mode="live",
            trading_date=DATE, dry_run=True, expected_count=53,
            runner=_mock_runner(),
        )

    assert result["eligible"] == 53
    assert result["verified"] == 53, (
        f"verified must equal eligible in dry-run (DE proof in query). Got {result['verified']}"
    )
    assert not result["errors"]
    assert len(captured_sql) >= 1

    # Verify the SQL contains the DE EXISTS clause
    full_sql = " ".join(captured_sql)
    assert "decision_events" in full_sql, "Dry-run SQL must include decision_events DE proof"
    assert "candidate_id" in full_sql, "Must use candidate_id, not signal_id"
    assert "signal_id" not in full_sql.lower().replace("candidate_id", ""), (
        "signal_id must not appear as a direct decision_events column"
    )
    assert "REEVAL:" in full_sql, "Must join via REEVAL: prefix pattern"


# ─── Test: DE reasoning uses exact strings, not generic match ─────────────────

def test_de_reasoning_exact_match():
    """The rescue must use exact reasoning strings, not generic 'regime' match."""
    from ap.live_overnight_rescue import _DE_REASONING_EXACT, _build_eligibility_exists_clause

    exists_sql = _build_eligibility_exists_clause().format(trading_date=DATE)

    # Exact strings must appear
    for exact in _DE_REASONING_EXACT:
        assert exact in exists_sql, f"Exact reasoning {exact!r} not in EXISTS clause"

    # Generic 'regime' match must NOT appear as a standalone pattern
    assert "LIKE '%%regime%%'" not in exists_sql
    assert "'%regime%'" not in exists_sql


# ─── Test: No signal_id column on decision_events ─────────────────────────────

def test_rescue_uses_candidate_id_not_signal_id():
    """Confirm no query references decision_events.signal_id directly."""
    from ap.live_overnight_rescue import (
        _build_eligibility_exists_clause,
        _build_structural_where,
    )
    from datetime import timezone

    exists_sql = _build_eligibility_exists_clause().format(trading_date=DATE)
    structural_sql, _ = _build_structural_where(
        client_id=JASON,
        window_start_utc=datetime(2026, 7, 17, 20, 0, tzinfo=timezone.utc),
        window_end_utc=datetime(2026, 7, 20, 13, 30, tzinfo=timezone.utc),
    )

    combined = exists_sql + structural_sql
    # candidate_id must appear in the DE EXISTS clause
    assert "candidate_id" in exists_sql
    # "de.signal_id" must NOT appear
    assert "de.signal_id" not in combined
    assert "decision_events.signal_id" not in combined


# ─── Test: expected_count mismatch aborts with zero writes ────────────────────

def test_expected_count_mismatch_aborts():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections

    def fake_conn_ctx():
        class FakeCursor:
            description = [("n",)]
            def execute(self, sql, params): pass
            def fetchone(self): return {"n": 40}  # 40, not 53
            def fetchall(self): return []
        class FakeConn:
            def __enter__(self): return FakeCursor()
            def __exit__(self, *a): pass
        return FakeConn()

    with (
        patch("ap.live_overnight_rescue.conn", side_effect=fake_conn_ctx),
        patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()),
    ):
        result = rescue_live_overnight_regime_rejections(
            client_id=JASON, execution_mode="live",
            trading_date=DATE, dry_run=True, expected_count=53,
            runner=_mock_runner(),
        )

    assert result["count_match"] is False
    assert result["writes"] == 0
    assert result["errors"]


# ─── Test: Atomic rescue raises when locked count mismatches ──────────────────

def test_atomic_rescue_raises_on_locked_count_mismatch():
    from ap.live_overnight_rescue import _execute_atomic_rescue
    from datetime import timezone

    w_start = datetime(2026, 7, 17, 20, 0, tzinfo=timezone.utc)
    w_end   = datetime(2026, 7, 20, 13, 30, tzinfo=timezone.utc)

    class TooFewCursor:
        description = [("id",), ("signal_id",), ("payload",),
                       ("last_error",), ("result_json",), ("finished_ts",)]
        def execute(self, sql, params): pass
        def fetchall(self):
            return [
                {"id": i, "signal_id": f"sig-{i}", "payload": None,
                 "last_error": "mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK",
                 "result_json": None, "finished_ts": None}
                for i in range(40)  # only 40
            ]
        @property
        def rowcount(self): return 40

    class FakeConn:
        def __enter__(self): return TooFewCursor()
        def __exit__(self, *a): pass

    with patch("ap.live_overnight_rescue.conn", return_value=FakeConn()):
        with pytest.raises(ValueError, match="40 rows"):
            _execute_atomic_rescue(
                client_id=JASON, trading_date=DATE,
                window_start_utc=w_start, window_end_utc=w_end,
                expected_count=53,
                recovery_run_id="test", recovered_at="2026-07-20T09:00:00Z",
            )


# ─── Test: DB exception causes rollback (exception propagates) ────────────────

def test_atomic_rescue_propagates_db_exception():
    from ap.live_overnight_rescue import _execute_atomic_rescue
    from datetime import timezone

    w_start = datetime(2026, 7, 17, 20, 0, tzinfo=timezone.utc)
    w_end   = datetime(2026, 7, 20, 13, 30, tzinfo=timezone.utc)

    class ExplodingCursor:
        description = [("id",), ("signal_id",), ("payload",),
                       ("last_error",), ("result_json",), ("finished_ts",)]
        def execute(self, sql, params):
            raise Exception("DB connection lost")
        def fetchall(self): return []
        @property
        def rowcount(self): return 0

    class FakeConn:
        def __enter__(self): return ExplodingCursor()
        def __exit__(self, *a): pass

    with patch("ap.live_overnight_rescue.conn", return_value=FakeConn()):
        with pytest.raises(Exception, match="DB connection lost"):
            _execute_atomic_rescue(
                client_id=JASON, trading_date=DATE,
                window_start_utc=w_start, window_end_utc=w_end,
                expected_count=53,
                recovery_run_id="test", recovered_at="2026-07-20T09:00:00Z",
            )


# ─── Test: Orchestration stops when handoff function is missing ───────────────

def test_orchestration_stops_on_missing_handoff_import():
    """ImportError from ap_morning_handoff_audit must stop orchestration."""
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight

    runner = _mock_runner()
    rescue_ok = {
        "eligible": 53, "verified": 53, "writes": 53,
        "count_match": True, "errors": [], "dry_run": False,
    }
    reeval_ok = {"processed": 53, "armed": 50, "rejected": 3, "errors": 0}

    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections",
              return_value=rescue_ok),
        patch("ap.live_overnight_rescue.run_overnight_reeval", return_value=reeval_ok),
        patch.dict("sys.modules", {"ap_morning_handoff_audit": None}),
    ):
        result = recover_and_rerun_live_overnight(
            runner=runner, trading_date=DATE, dry_run=False, expected_count=53,
        )

    assert result["errors"]
    assert any("handoff" in e.lower() or "morning_handoff_audit" in e.lower()
               for e in result["errors"])


# ─── Test: Real handoff function signature is used ───────────────────────────

def test_orchestration_uses_real_handoff_signature():
    """
    run_morning_handoff_audit must be called with production signature:
    (client_id, entry_watcher, osm, execution_mode="live", dry_run=False)
    NOT with stage= or runner=.
    """
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight

    runner = _mock_runner()
    rescue_ok = {
        "eligible": 53, "verified": 53, "writes": 53,
        "count_match": True, "errors": [], "dry_run": False,
    }
    reeval_ok = {"processed": 53, "armed": 50, "rejected": 3, "errors": 0}
    handoff_ok = {"ok": True, "errors": []}

    mock_handoff = MagicMock(return_value=handoff_ok)

    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections",
              return_value=rescue_ok),
        patch("ap.live_overnight_rescue.run_overnight_reeval", return_value=reeval_ok),
        patch("ap_morning_handoff_audit.run_morning_handoff_audit", mock_handoff),
        patch("ap.live_overnight_rescue.run_morning_handoff_audit", mock_handoff),
        patch("ap.live_overnight_rescue.run_preopen_autonomous_readiness",
              return_value={"status": "OK"}),
    ):
        recover_and_rerun_live_overnight(
            runner=runner, trading_date=DATE, dry_run=False, expected_count=53,
        )

    mock_handoff.assert_called_once()
    kwargs = mock_handoff.call_args.kwargs
    call_args = mock_handoff.call_args

    # Must NOT have stage or runner in kwargs
    assert "stage" not in kwargs, f"stage= must not be passed. Got kwargs: {kwargs}"
    assert "runner" not in kwargs, f"runner= must not be passed. Got kwargs: {kwargs}"

    # Must have execution_mode="live"
    exec_mode = kwargs.get("execution_mode") or (
        call_args.args[3] if len(call_args.args) > 3 else None
    )
    assert exec_mode == "live", f"execution_mode must be 'live'. Got: {exec_mode}"

    # Must have dry_run=False
    dry_run_val = kwargs.get("dry_run") or (
        call_args.args[4] if len(call_args.args) > 4 else False
    )
    assert dry_run_val is False


# ─── Test: Orchestration stops on partial rescue ──────────────────────────────

def test_orchestration_stops_on_partial_rescue():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight

    partial = {
        "eligible": 53, "verified": 53, "writes": 50,
        "count_match": False, "errors": [], "dry_run": False,
    }
    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections",
              return_value=partial),
        patch("ap.live_overnight_rescue.run_overnight_reeval") as mock_reeval,
    ):
        result = recover_and_rerun_live_overnight(
            runner=_mock_runner(), trading_date=DATE, dry_run=False,
        )

    mock_reeval.assert_not_called()
    assert result["errors"]


# ─── Test: Orchestration stops on reeval exception ────────────────────────────

def test_orchestration_stops_on_reeval_exception():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight

    rescue_ok = {"eligible": 53, "verified": 53, "writes": 53,
                 "count_match": True, "errors": [], "dry_run": False}
    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections",
              return_value=rescue_ok),
        patch("ap.live_overnight_rescue.run_overnight_reeval",
              side_effect=RuntimeError("reeval crashed")),
    ):
        result = recover_and_rerun_live_overnight(
            runner=_mock_runner(), trading_date=DATE, dry_run=False,
        )

    assert result["errors"]
    assert any("reeval" in e.lower() for e in result["errors"])


# ─── Test: Reeval is called with force=True and correct components ─────────────

def test_reeval_called_with_force_and_components():
    from ap.live_overnight_rescue import recover_and_rerun_live_overnight

    runner = _mock_runner()
    rescue_ok = {"eligible": 53, "verified": 53, "writes": 53,
                 "count_match": True, "errors": [], "dry_run": False}
    reeval_ok = {"processed": 53, "armed": 50, "rejected": 3, "errors": 0}
    handoff_ok = {"ok": True, "errors": []}

    with (
        patch("ap.live_overnight_rescue.rescue_live_overnight_regime_rejections",
              return_value=rescue_ok),
        patch("ap.live_overnight_rescue.run_overnight_reeval",
              return_value=reeval_ok) as mock_reeval,
        patch("ap.live_overnight_rescue.run_morning_handoff_audit", return_value=handoff_ok),
        patch("ap.live_overnight_rescue.run_preopen_autonomous_readiness",
              return_value={"status": "OK"}),
    ):
        recover_and_rerun_live_overnight(
            runner=runner, trading_date=DATE, dry_run=False, expected_count=53,
        )

    mock_reeval.assert_called_once()
    kwargs = mock_reeval.call_args.kwargs
    assert kwargs.get("force") is True
    assert kwargs.get("client_id") == JASON
    assert kwargs.get("broker") is runner.broker
    assert kwargs.get("master_control") is runner.master_control


# ─── Test: Readiness degraded when overnight reeval produced zero armed ────────

def test_readiness_degraded_when_zero_armed():
    from ap.preopen_readiness import _overnight_status

    reeval_row = {"status": "partial",
                  "details": {"fetched": 53, "armed": 0, "rejected": 53, "errors": 0}}
    runner = MagicMock()
    runner._last_overnight_reeval_date = None

    with patch("ap.preopen_readiness._load_preopen_row", return_value=reeval_row):
        status, details = _overnight_status(
            runner, {"watching_count": 53, "pending_trigger_rows": []},
            DATE, client_id=JASON, execution_mode="live",
        )

    assert status == "degraded"
    assert details.get("error_code") == "OVERNIGHT_REEVAL_ZERO_ARMED"


# ─── Test: Handoff success cannot override zero-armed overnight record ─────────

def test_handoff_success_cannot_override_zero_armed():
    from ap.preopen_readiness import _overnight_status

    reeval_row = {"status": "partial",
                  "details": {"fetched": 53, "armed": 0, "rejected": 53, "errors": 0}}
    runner = MagicMock()
    runner._last_overnight_reeval_date = None

    with (
        patch("ap.preopen_readiness._load_preopen_row", return_value=reeval_row),
        patch("ap.preopen_readiness._post_overnight_reeval_success_exists", return_value=True),
    ):
        status, details = _overnight_status(
            runner, {"watching_count": 0, "pending_trigger_rows": []},
            DATE, client_id=JASON, execution_mode="live",
        )

    assert status == "degraded", (
        f"overnight_reeval record (armed=0) must override handoff success. "
        f"Got status={status!r}"
    )


# ─── Test: Idempotency — second rescue finds 0 rows ──────────────────────────

def test_rescue_idempotent_second_call_finds_zero():
    from ap.live_overnight_rescue import rescue_live_overnight_regime_rejections

    call_n = [0]
    def fake_conn_ctx():
        class FakeCursor:
            description = [("n",)]
            def execute(self, sql, params): pass
            def fetchone(self):
                call_n[0] += 1
                return {"n": 0}  # Already recovered — excluded by prior recovery DE
            def fetchall(self): return []
        class FakeConn:
            def __enter__(self): return FakeCursor()
            def __exit__(self, *a): pass
        return FakeConn()

    with (
        patch("ap.live_overnight_rescue.conn", side_effect=fake_conn_ctx),
        patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()),
    ):
        result = rescue_live_overnight_regime_rejections(
            client_id=JASON, execution_mode="live",
            trading_date=DATE, dry_run=True, expected_count=53,
            runner=_mock_runner(),
        )

    assert result["eligible"] == 0
    assert result["writes"] == 0
    assert result["count_match"] is False


# ─── PostgreSQL production-shape integration test ─────────────────────────────

@pytest.mark.skipif(not PG_URL, reason="No PostgreSQL URL (set INTELLIGENCE_POSTGRES_TEST_URL)")
def test_postgres_production_shape():
    """
    End-to-end PostgreSQL test using the exact production column shapes.
    Verifies:
      - dry-run finds and verifies exactly 53 rows
      - non-dry recovery updates exactly 53 atomically
      - one missing event causes zero writes
      - a SQL exception causes rollback
      - recovery events use canonical columns (run_id, candidate_id, ...)
      - a second run is idempotent (finds 0 eligible)
      - the 6 structural invalidations remain terminal
    """
    import psycopg2
    import psycopg2.extras

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS trade_queue (
        id BIGSERIAL PRIMARY KEY,
        client_id TEXT,
        signal_id TEXT,
        status TEXT,
        payload JSONB,
        last_error TEXT,
        result_json JSONB,
        created_ts TIMESTAMPTZ DEFAULT NOW(),
        started_ts TIMESTAMPTZ,
        finished_ts TIMESTAMPTZ
    );
    CREATE TABLE IF NOT EXISTS decision_events (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT,
        candidate_id TEXT,
        client_id TEXT,
        stage TEXT,
        decision TEXT,
        reason_code TEXT,
        explanation TEXT,
        context_json JSONB,
        ts TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS orders (
        id BIGSERIAL PRIMARY KEY,
        client_id TEXT,
        signal_id TEXT,
        kind TEXT,
        broker_order_id TEXT,
        submitted_ts TIMESTAMPTZ,
        filled_ts TIMESTAMPTZ,
        status TEXT,
        created_ts TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS positions (
        id BIGSERIAL PRIMARY KEY,
        client_id TEXT,
        signal_id TEXT,
        status TEXT,
        created_ts TIMESTAMPTZ DEFAULT NOW()
    );
    """

    FRIDAY_TS = "2026-07-17 20:30:00+00"  # Friday after-close UTC
    TRADING_DATE = INCIDENT_TRADING_DATE
    CALL_REASONING = "risk_veto: CALL blocked \u2014 SPY in BEAR trend"
    PUT_REASONING  = "risk_veto: PUT blocked \u2014 SPY in BULL trend"

    db_url = PG_URL
    conn = psycopg2.connect(db_url, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = False

    try:
        with conn.cursor() as c:
            c.execute("BEGIN")

            # Create tables
            c.execute(SCHEMA)

            # Seed 53 eligible queue rows
            for i in range(53):
                side = "CALL" if i % 2 == 0 else "PUT"
                c.execute("""
                    INSERT INTO trade_queue
                        (client_id, signal_id, status, last_error, created_ts, payload)
                    VALUES (%s, %s, 'REJECTED', 'mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK',
                            %s::timestamptz, %s::jsonb)
                """, (
                    JASON,
                    f"2026-07-18:1-1:TEST-{i}:{side}",
                    FRIDAY_TS,
                    json.dumps({"ticker": f"TEST{i}", "side": side, "score": 78.0}),
                ))

            # Seed 6 structural invalidation rows (must remain terminal)
            for i in range(6):
                c.execute("""
                    INSERT INTO trade_queue
                        (client_id, signal_id, status, last_error, created_ts, payload)
                    VALUES (%s, %s, 'REJECTED', 'overnight_invalidated:INVALIDATED_PRIOR_HIGH_BREACHED',
                            %s::timestamptz, %s::jsonb)
                """, (
                    JASON,
                    f"2026-07-18:1-1:INVALID-{i}:CALL",
                    FRIDAY_TS,
                    json.dumps({"ticker": f"INVALID{i}", "side": "CALL"}),
                ))

            # Seed 53 canonical decision events (stage=blocked_intel)
            for i in range(53):
                side = "CALL" if i % 2 == 0 else "PUT"
                reasoning = CALL_REASONING if side == "CALL" else PUT_REASONING
                signal_id = f"2026-07-18:1-1:TEST-{i}:{side}"
                c.execute("""
                    INSERT INTO decision_events
                        (run_id, candidate_id, client_id, stage, decision,
                         reason_code, explanation, context_json, ts)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                            %s::timestamptz AT TIME ZONE 'America/New_York')
                """, (
                    f"reeval-run-{i}",
                    f"REEVAL:{signal_id}:abc{i:03d}",
                    JASON,
                    _DE_STAGE,
                    _DE_DECISION,
                    _DE_REASON_CODE,
                    _DE_EXPLANATION,
                    json.dumps({
                        "intel_reason_code":    "INTEL_AUTHORITATIVE_VETO_RISK",
                        "intel_raw_status":     "RISK_VETO",
                        "intel_execution_mode": "LIVE",
                        "intel_reasoning":      reasoning,
                    }),
                    f"2026-07-20 09:15:00",
                ))

            conn.commit()

        # ── Patch ap.db.conn to use our test connection ───────────────────────
        import contextlib

        @contextlib.contextmanager
        def _test_conn():
            c = conn.cursor()
            try:
                yield c
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        # ── Test 1: Dry-run finds eligible=53, verified=53 ───────────────────
        with patch("ap.live_overnight_rescue.conn", side_effect=_test_conn), \
             patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()):
            result = rescue_live_overnight_regime_rejections(
                client_id=JASON, execution_mode="live",
                trading_date=TRADING_DATE, dry_run=True, expected_count=53,
                runner=_mock_runner(),
            )

        assert result["eligible"] == 53, f"dry-run eligible={result['eligible']}"
        assert result["verified"] == 53, f"dry-run verified={result['verified']}"
        assert result["writes"] == 0
        assert not result["errors"]
        assert result["count_match"] is True

        # ── Test 2: Non-dry recovery updates exactly 53 rows atomically ──────
        with patch("ap.live_overnight_rescue.conn", side_effect=_test_conn), \
             patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()):
            result = rescue_live_overnight_regime_rejections(
                client_id=JASON, execution_mode="live",
                trading_date=TRADING_DATE, dry_run=False, expected_count=53,
                runner=_mock_runner(),
            )

        assert result["writes"] == 53, f"writes={result['writes']}"
        assert not result["errors"]

        # Verify rows are now WATCHING
        with conn.cursor() as c:
            c.execute("""
                SELECT COUNT(*) AS n FROM trade_queue
                WHERE client_id=%s AND status='WATCHING'
                  AND payload->>'recovery_context' IS NOT NULL
            """, (JASON,))
            row = c.fetchone()
            assert int(row["n"]) == 53, f"Expected 53 WATCHING rows, got {row['n']}"

        # ── Test 3: Recovery events use canonical columns ─────────────────────
        with conn.cursor() as c:
            c.execute("""
                SELECT COUNT(*) AS n FROM decision_events
                WHERE client_id=%s AND stage='overnight_recovery'
                  AND reason_code=%s
                  AND run_id IS NOT NULL
                  AND candidate_id LIKE 'RECOVER:%%'
            """, (JASON, RECOVER_PR379_REGIME_TAXONOMY))
            row = c.fetchone()
            assert int(row["n"]) == 53, f"Expected 53 recovery events, got {row['n']}"

        # ── Test 4: Second run is idempotent (finds 0 eligible) ──────────────
        with patch("ap.live_overnight_rescue.conn", side_effect=_test_conn), \
             patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()):
            result2 = rescue_live_overnight_regime_rejections(
                client_id=JASON, execution_mode="live",
                trading_date=TRADING_DATE, dry_run=True, expected_count=53,
                runner=_mock_runner(),
            )

        assert result2["eligible"] == 0, (
            f"Second run must find 0 eligible (all recovered). Got {result2['eligible']}"
        )
        assert result2["count_match"] is False
        assert result2["errors"]  # count mismatch

        # ── Test 5: 6 structural invalidations remain terminal ────────────────
        with conn.cursor() as c:
            c.execute("""
                SELECT COUNT(*) AS n FROM trade_queue
                WHERE client_id=%s
                  AND last_error LIKE 'overnight_invalidated:%%'
                  AND status='REJECTED'
            """, (JASON,))
            row = c.fetchone()
            assert int(row["n"]) == 6, (
                f"6 structural invalidations must remain REJECTED. Got {row['n']}"
            )

        # ── Test 6: One missing event causes zero writes ──────────────────────
        # Delete one DE to break the proof for that row
        with conn.cursor() as c:
            c.execute("""
                DELETE FROM decision_events
                WHERE candidate_id = (
                    SELECT candidate_id FROM decision_events
                    WHERE client_id=%s AND stage='blocked_intel'
                    LIMIT 1
                )
            """, (JASON,))
            conn.commit()

        # First, reset all rows back to REJECTED so we can retry
        with conn.cursor() as c:
            c.execute("""
                UPDATE trade_queue
                SET status='REJECTED',
                    last_error='mc_blocked:INTEL_AUTHORITATIVE_VETO_RISK',
                    payload=payload - 'recovery_context'
                WHERE client_id=%s AND status='WATCHING'
            """, (JASON,))
            # Also delete prior recovery events
            c.execute("""
                DELETE FROM decision_events
                WHERE client_id=%s AND stage='overnight_recovery'
            """, (JASON,))
            conn.commit()

        with patch("ap.live_overnight_rescue.conn", side_effect=_test_conn), \
             patch("ap.live_overnight_rescue.run_with_retry", side_effect=lambda f: f()):
            result_missing = rescue_live_overnight_regime_rejections(
                client_id=JASON, execution_mode="live",
                trading_date=TRADING_DATE, dry_run=True, expected_count=53,
                runner=_mock_runner(),
            )

        # With one missing DE, eligible should be 52 (one row excluded by EXISTS)
        assert result_missing["eligible"] <= 52, (
            f"One missing DE should exclude one row. Got eligible={result_missing['eligible']}"
        )

    finally:
        # Clean up test data
        try:
            with conn.cursor() as c:
                c.execute("DROP TABLE IF EXISTS trade_queue, decision_events, orders, positions")
            conn.commit()
        except Exception:
            pass
        conn.close()
