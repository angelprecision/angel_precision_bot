from __future__ import annotations

import json
import pathlib
from datetime import date, datetime, timezone

from ap.position_quote_monitor import APPositionQuoteMonitor
from scripts.daily_proof_report import _aggregate


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


def _apply_path(values):
    meta = {}
    now = datetime(2026, 7, 3, 15, 0, tzinfo=timezone.utc)
    for pnl in values:
        patch = APPositionQuoteMonitor._mfe_mae_patch(meta, pnl, now, "unit_test")
        meta.update(patch)
    return meta


def test_simulated_price_path_gives_correct_mfe_mae():
    meta = _apply_path([0.02, 0.18, -0.07, 0.11, -0.12, 0.24])

    assert meta["mfe_pct"] == 0.24
    assert meta["mae_pct"] == -0.12
    assert meta["mfe_mae_source"] == "unit_test"
    assert meta["mfe_at"]
    assert meta["mae_at"]


def test_restart_does_not_reset_prior_mfe_mae_to_zero():
    prior = {
        "mfe_pct": 0.32,
        "mae_pct": -0.18,
        "mfe_at": "2026-07-03T14:00:00+00:00",
        "mae_at": "2026-07-03T14:05:00+00:00",
    }

    patch = APPositionQuoteMonitor._mfe_mae_patch(
        prior,
        0.0,
        datetime(2026, 7, 3, 15, 0, tzinfo=timezone.utc),
        "unit_test",
    )

    assert patch == {}


def test_missing_quote_marks_unavailable_without_fake_zero():
    src = (REPO_ROOT / "ap" / "position_quote_monitor.py").read_text()

    assert "mfe_mae_unavailable_reason" in src
    assert "missing_option_quote" in src
    unavailable_body = src[src.find("def _mark_mfe_mae_unavailable"):src.find("def _prune_closed")]
    assert '"mfe_pct": 0' not in unavailable_body
    assert '"mae_pct": 0' not in unavailable_body


def test_zero_excursion_is_valid_only_from_real_pnl_path():
    patch = APPositionQuoteMonitor._mfe_mae_patch(
        {},
        0.0,
        datetime(2026, 7, 3, 15, 0, tzinfo=timezone.utc),
        "unit_test",
    )

    assert patch["mfe_pct"] == 0.0
    assert patch["mae_pct"] == 0.0


def test_writer_uses_orders_meta_jsonb_not_result_json():
    src = (REPO_ROOT / "ap" / "position_quote_monitor.py").read_text()
    body = src[src.find("def _persist_mfe_mae_to_orders"):src.find("def _mark_mfe_mae_unavailable")]

    assert "UPDATE orders" in body
    assert "mfe_mae_unavailable_reason" in body
    assert "COALESCE(meta, '{{}}'::jsonb)" in body or "COALESCE(meta, '{}'::jsonb)" in body
    assert "result_json" not in body
    assert "information_schema.columns" in src
    assert "column_name = 'meta'" in src


def test_writer_failure_does_not_affect_position_lifecycle(monkeypatch):
    monitor = APPositionQuoteMonitor(broker=object(), client_id="client-a", exit_engine=object())
    monitor._orders_meta_available = True
    monkeypatch.setattr(monitor, "_fetch_prior_mfe_mae_meta", lambda **kwargs: {})

    class _BadDB:
        @staticmethod
        def conn():
            raise RuntimeError("db down")

        @staticmethod
        def run_with_retry(fn):
            return fn()

    import sys
    import types
    db_mod = types.ModuleType("ap.db")
    db_mod.conn = _BadDB.conn
    db_mod.run_with_retry = _BadDB.run_with_retry
    monkeypatch.setitem(sys.modules, "ap.db", db_mod)

    assert monitor._persist_mfe_mae_to_orders(
        position_id="pos-1",
        contract="SPY260717C00500000",
        option_pnl_pct=0.12,
        now_utc=datetime(2026, 7, 3, 15, 0, tzinfo=timezone.utc),
        source="unit_test",
    ) is False


def test_daily_proof_report_flags_mfe_mae_coverage_under_95():
    report = _aggregate(
        rows=[],
        audit_counts={},
        exit_submitted=0,
        exit_filled=0,
        mfe_mae_coverage={"closed_count": 10, "covered_count": 9},
        active_clients=[],
        target_date=date(2026, 7, 3),
        mode_hint="paper",
    )

    assert report.mfe_mae_coverage_pct == 90.0
    assert report.mfe_mae_coverage_warning
    assert "MFE/MAE coverage" in report.errors[-1]


def test_daily_operator_folder_includes_mfe_mae_coverage_section():
    src = (REPO_ROOT / "ap_operator_daily_folders.py").read_text()

    assert "mfe_mae_closed_count" in src
    assert "mfe_mae_covered_count" in src
    assert '"mfe_mae_coverage_pct"' in src
    assert "meta ? 'mfe_pct'" in src


def test_observability_exposes_pr_body_audit_query():
    src = (REPO_ROOT / "ap" / "observability.py").read_text()

    assert "MFE_MAE_COVERAGE_AUDIT_SQL" in src
    assert "COUNT(*) AS closed_count" in src
    assert "meta ? 'mfe_pct'" in src
    assert "meta ? 'mae_pct'" in src
    assert "meta ? 'mfe_mae_unavailable_reason'" in src
    assert "created_ts >= now() - interval '30 days'" in src


class _FakeCursor:
    def __init__(self, *, fetchone_results=None, rowcount=1):
        self.fetchone_results = list(fetchone_results or [])
        self.rowcount = rowcount
        self.executed: list[tuple[str, tuple]] = []

    def execute(self, sql, params=()):
        self.executed.append((sql, tuple(params)))

    def fetchone(self):
        if self.fetchone_results:
            return self.fetchone_results.pop(0)
        return None


class _FakeConnCtx:
    def __init__(self, cursor):
        self.cursor = cursor

    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc, tb):
        return False


def _install_fake_db(monkeypatch, cursor):
    class _DB:
        @staticmethod
        def conn():
            return _FakeConnCtx(cursor)

        @staticmethod
        def run_with_retry(fn):
            return fn()

    import sys
    import types

    db_mod = types.ModuleType("ap.db")
    db_mod.conn = _DB.conn
    db_mod.run_with_retry = _DB.run_with_retry
    monkeypatch.setitem(sys.modules, "ap.db", db_mod)


def test_two_terminal_orders_same_contract_only_matching_position_row_updates(monkeypatch):
    monitor = APPositionQuoteMonitor(broker=object(), client_id="client-a", exit_engine=object())
    monitor._orders_meta_available = True
    cursor = _FakeCursor(fetchone_results=[{"meta": {}}], rowcount=1)
    _install_fake_db(monkeypatch, cursor)

    ok = monitor._persist_mfe_mae_to_orders(
        position_id="pos-2",
        contract="SPY260717C00500000",
        option_pnl_pct=0.12,
        now_utc=datetime(2026, 7, 3, 15, 0, tzinfo=timezone.utc),
        source="unit_test",
    )

    assert ok is True
    select_sql, select_params = cursor.executed[0]
    update_sql, update_params = cursor.executed[1]
    assert "position_id = %s" in select_sql
    assert "position_id = %s" in update_sql
    assert "client_id = %s" in update_sql
    assert "contract = %s" in update_sql
    assert "local_order_id = %s" not in update_sql
    assert "broker_order_id = %s" not in update_sql
    assert "WHERE client_id = %s\n                          AND contract = %s\n                          AND status IN" not in update_sql
    assert select_params == ("client-a", "SPY260717C00500000", "pos-2")
    assert update_params[1:] == ("client-a", "SPY260717C00500000", "pos-2")
    patch = json.loads(update_params[0])
    assert patch["mfe_mae_position_id"] == "pos-2"


def test_same_contract_reused_later_old_row_is_not_overwritten(monkeypatch):
    monitor = APPositionQuoteMonitor(broker=object(), client_id="client-a", exit_engine=object())
    monitor._orders_meta_available = True
    cursor = _FakeCursor(fetchone_results=[{"meta": {}}], rowcount=1)
    _install_fake_db(monkeypatch, cursor)

    ok = monitor._mark_mfe_mae_unavailable(
        position_id="pos-new",
        contract="SPY260717C00500000",
        reason="missing_option_quote",
    )

    assert ok is True
    update_sql, update_params = cursor.executed[1]
    assert "position_id = %s" in update_sql
    assert update_params[1:] == ("client-a", "SPY260717C00500000", "pos-new")
    assert "created_ts DESC" not in update_sql


def test_missing_position_id_does_not_fan_out_to_all_rows(monkeypatch):
    monitor = APPositionQuoteMonitor(broker=object(), client_id="client-a", exit_engine=object())
    monitor._orders_meta_available = True
    cursor = _FakeCursor(fetchone_results=[], rowcount=99)
    _install_fake_db(monkeypatch, cursor)

    ok = monitor._persist_mfe_mae_to_orders(
        position_id="",
        contract="SPY260717C00500000",
        option_pnl_pct=0.12,
        now_utc=datetime(2026, 7, 3, 15, 0, tzinfo=timezone.utc),
        source="unit_test",
    )

    assert ok is False
    assert cursor.executed == []
