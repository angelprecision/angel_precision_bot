from __future__ import annotations

import importlib
import io
import json
import os
import sys
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))


def _load_module():
    mod = importlib.import_module("ap_armed_deferred_rescue")
    return importlib.reload(mod)


def _payload(ticker="KHC", side="CALL", score=77.0, trigger_entry=32.5):
    return {
        "ticker": ticker,
        "symbol": ticker,
        "side": side,
        "timeframe": "1d",
        "score": score,
        "pattern": "2-1-2",
        "tier": "A",
        "max_position_usd": 250.0,
        "trigger": {
            "entry": trigger_entry,
            "stop": 31.0,
            "pt1": 35.0,
            "pt3": 30.0,
        },
    }


def _row(
    queue_id=1,
    signal_id="sig-1",
    ticker="KHC",
    created_ts="2026-07-01T13:25:00+00:00",
    last_error="armed:contract=DEFERRED:KHC",
    payload=None,
):
    return {
        "id": queue_id,
        "signal_id": signal_id,
        "created_ts": created_ts,
        "last_error": last_error,
        "payload": payload if payload is not None else _payload(ticker=ticker),
    }


class _OSM:
    def __init__(self, local_order_id="local-1", exc=None):
        self.local_order_id = local_order_id
        self.exc = exc
        self.calls = []

    def create_entry_order(self, plan, **kwargs):
        self.calls.append((plan, kwargs))
        if self.exc:
            raise self.exc
        return self.local_order_id


class _Watcher:
    def __init__(self, result=True, exc=None):
        self.result = result
        self.exc = exc
        self.calls = []

    def watch(self, plan, local_order_id, **kwargs):
        self.calls.append((plan, local_order_id, kwargs))
        if self.exc:
            raise self.exc
        return self.result


def test_dry_run_finds_jul1_rows_and_creates_no_orders(monkeypatch):
    mod = _load_module()
    monkeypatch.setattr(mod, "_load_eligible_rows", lambda *a, **k: [_row()])
    monkeypatch.setattr(mod, "_order_already_exists", lambda *a, **k: None)

    osm = _OSM()
    watcher = _Watcher()
    result = mod.run_armed_deferred_rescue(
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        entry_watcher=watcher,
        osm=osm,
        dry_run=True,
        start_ts_utc="2026-07-01T00:00:00+00:00",
        end_ts_utc="2026-07-02T00:00:00+00:00",
    )
    assert result["scanned"] == 1
    assert result["repaired"] == 1
    assert result["rows"][0]["action"] == "dry_run_would_repair"
    assert osm.calls == []
    assert watcher.calls == []


def test_repair_calls_create_entry_order_once_per_eligible_row(monkeypatch):
    mod = _load_module()
    monkeypatch.setattr(mod, "_load_eligible_rows", lambda *a, **k: [_row()])
    monkeypatch.setattr(mod, "_order_already_exists", lambda *a, **k: None)
    writes = []
    monkeypatch.setattr(mod, "_write_tq_result", lambda *a, **k: writes.append((a, k)))

    osm = _OSM(local_order_id="local-123")
    watcher = _Watcher(result=True)
    result = mod.run_armed_deferred_rescue(
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        entry_watcher=watcher,
        osm=osm,
        dry_run=False,
        start_ts_utc="2026-07-01T00:00:00+00:00",
        end_ts_utc="2026-07-02T00:00:00+00:00",
    )
    assert len(osm.calls) == 1
    _, kwargs = osm.calls[0]
    assert kwargs["initial_status"] == "PENDING_TRIGGER"
    assert kwargs["execution_mode"] == "live"
    assert kwargs["meta"]["contract_deferred"] is True
    assert result["rows"][0]["action"] == "repaired"
    assert writes


def test_watcher_called_only_after_order_creation_succeeds(monkeypatch):
    mod = _load_module()
    monkeypatch.setattr(mod, "_load_eligible_rows", lambda *a, **k: [_row()])
    monkeypatch.setattr(mod, "_order_already_exists", lambda *a, **k: None)
    monkeypatch.setattr(mod, "_write_tq_result", lambda *a, **k: None)

    osm = _OSM(local_order_id="local-123")
    watcher = _Watcher(result=True)
    mod.run_armed_deferred_rescue(
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        entry_watcher=watcher,
        osm=osm,
        dry_run=False,
        start_ts_utc="2026-07-01T00:00:00+00:00",
        end_ts_utc="2026-07-02T00:00:00+00:00",
    )
    assert len(osm.calls) == 1
    assert len(watcher.calls) == 1
    _, local_order_id, kwargs = watcher.calls[0]
    assert local_order_id == "local-123"
    assert kwargs["recovery_rearm"] is True
    assert kwargs["no_cancel_on_reject"] is True


def test_existing_order_skips_as_already_repaired(monkeypatch):
    mod = _load_module()
    monkeypatch.setattr(mod, "_load_eligible_rows", lambda *a, **k: [_row()])
    monkeypatch.setattr(
        mod,
        "_order_already_exists",
        lambda *a, **k: {"local_order_id": "existing-1", "status": "PENDING_TRIGGER"},
    )

    result = mod.run_armed_deferred_rescue(
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        entry_watcher=_Watcher(),
        osm=_OSM(),
        dry_run=False,
        start_ts_utc="2026-07-01T00:00:00+00:00",
        end_ts_utc="2026-07-02T00:00:00+00:00",
    )
    assert result["already_repaired"] == 1
    assert result["rows"][0]["action"] == "already_repaired"


def test_awaiting_overnight_reeval_rows_are_ignored(monkeypatch):
    mod = _load_module()
    monkeypatch.setattr(
        mod,
        "_load_eligible_rows",
        lambda *a, **k: [_row(last_error="after_hours_deferred:awaiting_overnight_reeval")],
    )
    monkeypatch.setattr(mod, "_order_already_exists", lambda *a, **k: None)

    result = mod.run_armed_deferred_rescue(
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        entry_watcher=_Watcher(),
        osm=_OSM(),
        dry_run=False,
        start_ts_utc="2026-07-01T00:00:00+00:00",
        end_ts_utc="2026-07-02T00:00:00+00:00",
    )
    assert result["skipped"] == 1
    assert result["rows"][0]["skip_reason"] == "awaiting_overnight_reeval"


def test_jun28_jun29_rows_ignored_for_jul1_window(monkeypatch):
    mod = _load_module()
    captured = {}

    def _capture(*args, **kwargs):
        captured["start"] = args[2]
        captured["end"] = args[3]
        return []

    monkeypatch.setattr(mod, "_load_eligible_rows", _capture)
    result = mod.run_armed_deferred_rescue(
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        entry_watcher=_Watcher(),
        osm=_OSM(),
        dry_run=True,
        start_ts_utc="2026-07-01T00:00:00+00:00",
        end_ts_utc="2026-07-02T00:00:00+00:00",
    )
    assert captured["start"] == "2026-07-01T00:00:00+00:00"
    assert captured["end"] == "2026-07-02T00:00:00+00:00"
    assert result["scanned"] == 0


def test_malformed_payload_skips_no_order_created(monkeypatch):
    mod = _load_module()
    monkeypatch.setattr(mod, "_load_eligible_rows", lambda *a, **k: [_row(payload={})])
    monkeypatch.setattr(mod, "_order_already_exists", lambda *a, **k: None)
    osm = _OSM()
    result = mod.run_armed_deferred_rescue(
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        entry_watcher=_Watcher(),
        osm=osm,
        dry_run=False,
        start_ts_utc="2026-07-01T00:00:00+00:00",
        end_ts_utc="2026-07-02T00:00:00+00:00",
    )
    assert result["rows"][0]["skip_reason"] == "empty_payload"
    assert osm.calls == []


def test_last_error_ticker_mismatch_skips(monkeypatch):
    mod = _load_module()
    monkeypatch.setattr(
        mod,
        "_load_eligible_rows",
        lambda *a, **k: [_row(ticker="KHC", last_error="armed:contract=DEFERRED:AVGO")],
    )
    monkeypatch.setattr(mod, "_order_already_exists", lambda *a, **k: None)

    result = mod.run_armed_deferred_rescue(
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        entry_watcher=_Watcher(),
        osm=_OSM(),
        dry_run=False,
        start_ts_utc="2026-07-01T00:00:00+00:00",
        end_ts_utc="2026-07-02T00:00:00+00:00",
    )
    assert result["rows"][0]["skip_reason"] == "ticker_mismatch"


def test_create_entry_order_failure_writes_rescue_failed(monkeypatch):
    mod = _load_module()
    monkeypatch.setattr(mod, "_load_eligible_rows", lambda *a, **k: [_row()])
    monkeypatch.setattr(mod, "_order_already_exists", lambda *a, **k: None)
    writes = []
    monkeypatch.setattr(mod, "_write_tq_result", lambda *a, **k: writes.append((a, k)))

    result = mod.run_armed_deferred_rescue(
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        entry_watcher=_Watcher(),
        osm=_OSM(exc=RuntimeError("insert failed")),
        dry_run=False,
        start_ts_utc="2026-07-01T00:00:00+00:00",
        end_ts_utc="2026-07-02T00:00:00+00:00",
    )
    assert result["failed"] == 1
    assert result["rows"][0]["error"].startswith("create_entry_order:RuntimeError")
    assert "rescue_failed:create_entry_order:RuntimeError" in writes[0][1]["last_error"]


def test_watcher_failure_records_false_and_does_not_fake_success(monkeypatch):
    mod = _load_module()
    monkeypatch.setattr(mod, "_load_eligible_rows", lambda *a, **k: [_row()])
    monkeypatch.setattr(mod, "_order_already_exists", lambda *a, **k: None)
    writes = []
    monkeypatch.setattr(mod, "_write_tq_result", lambda *a, **k: writes.append((a, k)))

    result = mod.run_armed_deferred_rescue(
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        entry_watcher=_Watcher(exc=RuntimeError("arm failed")),
        osm=_OSM(local_order_id="local-123"),
        dry_run=False,
        start_ts_utc="2026-07-01T00:00:00+00:00",
        end_ts_utc="2026-07-02T00:00:00+00:00",
    )
    row = result["rows"][0]
    assert row["action"] == "repaired"
    assert row["watcher_armed"] is False
    assert row["watcher_error"].startswith("RuntimeError")
    assert "watcher=False" in writes[0][1]["last_error"]


def test_cli_dry_run_prints_summary(monkeypatch):
    mod = _load_module()
    monkeypatch.setattr(mod, "_load_eligible_rows", lambda *a, **k: [_row(queue_id=17, signal_id="sig-17")])
    monkeypatch.setattr(mod, "_order_already_exists", lambda *a, **k: None)

    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = mod.main([
            "--client-id", "jasoncosby1@gmail.com",
            "--execution-mode", "live",
            "--start-date", "2026-07-01",
            "--end-date", "2026-07-02",
            "--dry-run",
        ])
    out = json.loads(buf.getvalue())
    assert rc == 0
    assert out["scanned"] == 1
    assert out["rows"][0]["queue_id"] == 17
