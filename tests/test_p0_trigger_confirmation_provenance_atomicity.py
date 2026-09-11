"""P0 trigger-confirmation provenance and durable CAS coverage.

The dispatch tests enter through the real watcher poll/confirmation seam.  The
PostgreSQL tests exercise the exact OSM mutation used by that seam, including
the transaction and competing-writer boundary.
"""
from __future__ import annotations

import os
import json
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from ap_entry_watcher import (
    APEntryWatcher,
    WatchedSignal,
    _trigger_crossed_at_provenance_matches,
)


def _signal(*, local_order_id="local-confirm-603", execution_mode="paper"):
    signal_id = "signal-confirm-603"
    canonical_signal_id = "canonical-confirm-603"
    return {
        "signal_id": signal_id,
        "canonical_signal_id": canonical_signal_id,
        "local_order_id": local_order_id,
        "client_id": "confirm@example.com",
        "client_email": "confirm@example.com",
        "execution_mode": execution_mode,
        "ticker": "AAPL",
        "side": "CALL",
        "entry_price": 100.0,
        "stop_price": 95.0,
        "target_price": 105.0,
        "score": 80.0,
        "grade": "A",
        "metadata": {
            "signal_id": signal_id,
            "canonical_signal_id": canonical_signal_id,
            "local_order_id": local_order_id,
            "client_id": "confirm@example.com",
            "execution_mode": execution_mode,
        },
    }


class _DispatchOSM:
    client_id = "confirm@example.com"

    def __init__(self, row, *, update_result=True):
        self.row = row
        self.update_result = update_result
        self.calls = []

    def update_order_meta(self, local_order_id, patch, **kwargs):
        self.calls.append((local_order_id, dict(patch), dict(kwargs)))
        if not self.update_result:
            return False
        if local_order_id != self.row["local_order_id"]:
            return False
        if kwargs.get("expected_status") != self.row["status"]:
            return False
        if kwargs.get("expected_execution_mode") != self.row["execution_mode"]:
            return False
        if kwargs.get("expected_signal_id") != self.row["signal_id"]:
            return False
        if kwargs.get("expected_canonical_signal_id") != self.row["canonical_signal_id"]:
            return False
        if "expected_materialization_generation" in kwargs:
            if self.row["meta"].get("materialization_generation") != kwargs[
                "expected_materialization_generation"
            ]:
                return False
        if kwargs.get("expected_new_trigger_authority") and any(
            key in self.row["meta"]
            for key in (
                "trigger_crossed_at",
                "trigger_crossed_at_provenance",
                "broker_order_id",
                "submit_intent_at",
            )
        ):
            return False
        self.row["meta"].update(patch)
        return True


class _AuditWatcher(APEntryWatcher):
    def _persist_watcher_audit(self, *_args, **_kwargs):
        return None


class _ReadbackOSM(_DispatchOSM):
    def update_order_meta(self, local_order_id, patch, **kwargs):
        self.calls.append((local_order_id, dict(patch), dict(kwargs)))
        if kwargs.get("expected_new_trigger_authority"):
            return False
        if kwargs.get("expected_existing_trigger_authority"):
            return (
                patch.get("trigger_crossed_at") == self.row["meta"].get("trigger_crossed_at")
                and patch.get("trigger_crossed_at_provenance")
                == self.row["meta"].get("trigger_crossed_at_provenance")
            )
        return False

    def read_trigger_confirmation_authority(
        self,
        local_order_id,
        *,
        client_id,
        execution_mode,
        signal_id,
        canonical_signal_id,
        expected_materialization_generation=None,
    ):
        if local_order_id != self.row["local_order_id"]:
            return None
        return {
            "proven": True,
            "trigger_crossed_at": self.row["meta"].get("trigger_crossed_at"),
            "trigger_crossed_at_provenance": dict(
                self.row["meta"].get("trigger_crossed_at_provenance") or {}
            ),
        }


def _confirmed_dispatch(osm, signal, *, mode="PAPER", callback=None):
    watcher = _AuditWatcher(MagicMock(), order_state_machine=osm, mode=mode)
    watched = WatchedSignal(signal, overnight=False)
    watched._watcher_ref = watcher
    watcher._pending.append(watched)
    watcher._dedup_set.add(signal["signal_id"])
    callback = callback or MagicMock(return_value=None)
    watcher.on_trigger = callback

    with patch.object(watcher, "_fetch_quotes", return_value={
        "AAPL": {"bid": 99.0, "ask": 100.5},
    }):
        watcher._poll_active_signals(open_protect_active=False)
    with patch.object(watcher, "_fetch_quotes", return_value={
        "AAPL": {"bid": 99.2, "ask": 100.6},
    }):
        watcher._poll_active_signals(open_protect_active=False)
    return watcher, watched, callback


def _row(signal, *, meta=None):
    return {
        "local_order_id": signal["local_order_id"],
        "client_id": signal["client_id"],
        "execution_mode": signal["execution_mode"],
        "signal_id": signal["signal_id"],
        "canonical_signal_id": signal["canonical_signal_id"],
        "status": "PENDING_TRIGGER",
        "meta": dict(meta or {}),
    }


def test_trigger_authority_provenance_rejects_extra_fields():
    signal = _signal()
    provenance = {
        "canonical_signal_id": signal["canonical_signal_id"],
        "client_id": signal["client_id"],
        "execution_mode": signal["execution_mode"],
        "local_order_id": signal["local_order_id"],
        "diagnostic": "must-live-beside-authority",
    }

    assert not _trigger_crossed_at_provenance_matches(
        provenance, signal, signal["local_order_id"]
    )


def test_real_confirmation_dispatch_writes_timestamp_and_complete_provenance_atomically():
    signal = _signal()
    osm = _DispatchOSM(_row(signal))

    _watcher, _watched, callback = _confirmed_dispatch(osm, signal)

    assert callback.call_count == 1
    assert len(osm.calls) == 1
    local_order_id, patch, expected = osm.calls[0]
    assert local_order_id == signal["local_order_id"]
    assert patch["trigger_crossed_at"]
    assert patch["trigger_crossed_at_provenance"] == {
        "canonical_signal_id": signal["canonical_signal_id"],
        "client_id": signal["client_id"],
        "execution_mode": "paper",
        "local_order_id": signal["local_order_id"],
    }
    assert expected == {
        "expected_status": "PENDING_TRIGGER",
        "expected_execution_mode": "paper",
        "expected_signal_id": signal["signal_id"],
        "expected_canonical_signal_id": signal["canonical_signal_id"],
        "expected_new_trigger_authority": True,
    }
    assert set(osm.row["meta"]) >= {
        "trigger_crossed_at",
        "trigger_crossed_at_provenance",
    }


def test_deferred_confirmation_carries_exact_generation_cas():
    signal = _signal()
    signal["materialization_generation"] = 7
    signal["metadata"]["materialization_generation"] = 7
    osm = _DispatchOSM(_row(signal, meta={"materialization_generation": 7}))

    _watcher, _watched, callback = _confirmed_dispatch(osm, signal)

    assert callback.call_count == 1
    assert osm.calls[0][2]["expected_materialization_generation"] == 7


@pytest.mark.parametrize(
    "mutator",
    [
        pytest.param(lambda sig: sig.__setitem__("materialization_generation", True), id="bool"),
        pytest.param(lambda sig: sig.__setitem__("materialization_generation", "7"), id="string"),
        pytest.param(lambda sig: sig.__setitem__("materialization_generation", 0), id="zero"),
        pytest.param(
            lambda sig: (
                sig.__setitem__("materialization_generation", 7),
                sig["metadata"].__setitem__("materialization_generation", 8),
            ),
            id="source-conflict",
        ),
    ],
)
def test_confirmation_rejects_bad_generation_without_side_effects(mutator):
    signal = _signal()
    mutator(signal)
    osm = _DispatchOSM(_row(signal))

    _watcher, _watched, callback = _confirmed_dispatch(osm, signal)

    callback.assert_not_called()
    assert osm.calls == []
    assert osm.row["meta"] == {}


@pytest.mark.parametrize(
    "mutator",
    [
        pytest.param(lambda sig: sig["metadata"].update({"client_id": "other@example.com"}), id="client-conflict"),
        pytest.param(lambda sig: sig["metadata"].update({"execution_mode": "live"}), id="mode-conflict"),
        pytest.param(lambda sig: sig["metadata"].update({"canonical_signal_id": "stale-canonical"}), id="canonical-conflict"),
    ],
)
def test_confirmation_dispatch_rejects_contradictory_identity_without_callback(mutator):
    signal = _signal()
    mutator(signal)
    osm = _DispatchOSM(_row(signal))

    _watcher, _watched, callback = _confirmed_dispatch(osm, signal)

    callback.assert_not_called()
    assert osm.calls == []
    assert osm.row["meta"] == {}


def test_confirmation_mutation_failure_leaves_no_timestamp_or_provenance():
    signal = _signal()
    osm = _DispatchOSM(_row(signal), update_result=False)

    _watcher, _watched, callback = _confirmed_dispatch(osm, signal)

    callback.assert_not_called()
    assert osm.row["meta"] == {}


def test_legacy_timestamp_only_row_is_not_repaired_by_new_confirmation_writer():
    signal = _signal()
    osm = _DispatchOSM(
        _row(signal, meta={"trigger_crossed_at": "2026-09-09T13:56:51+00:00"})
    )

    _watcher, _watched, callback = _confirmed_dispatch(osm, signal)

    callback.assert_not_called()
    assert set(osm.row["meta"]) == {"trigger_crossed_at"}


class _SecondCasLossOSM(_ReadbackOSM):
    def update_order_meta(self, local_order_id, patch, **kwargs):
        self.calls.append((local_order_id, dict(patch), dict(kwargs)))
        return False


def test_response_loss_second_cas_failure_does_not_publish_in_memory_authority():
    signal = _signal(local_order_id="local-confirm-603-second-cas-loss")
    durable_provenance = {
        "canonical_signal_id": signal["canonical_signal_id"],
        "client_id": signal["client_id"],
        "execution_mode": "paper",
        "local_order_id": signal["local_order_id"],
    }
    osm = _SecondCasLossOSM(
        _row(
            signal,
            meta={
                "trigger_crossed_at": "2026-09-09T20:00:00Z",
                "trigger_crossed_at_provenance": durable_provenance,
            },
        )
    )
    watcher = _AuditWatcher(MagicMock(), order_state_machine=osm, mode="PAPER")
    watched = WatchedSignal(signal, overnight=False)
    watched._watcher_ref = watcher

    assert watcher._persist_trigger_confirmation_authority(watched) is False
    assert not getattr(watched, "_trigger_authority_persisted", False)
    assert getattr(watched, "_durable_trigger_crossed_at_raw", None) is None
    assert getattr(watched, "_durable_trigger_crossed_at_provenance", None) is None


def test_response_loss_readback_normalizes_z_and_preserves_durable_spelling():
    signal = _signal(local_order_id="local-confirm-603-z")
    durable_timestamp = "2026-09-09T20:00:00Z"
    signal["trigger_crossed_at"] = "2026-09-09T22:00:00+02:00"
    durable_provenance = {
        "canonical_signal_id": signal["canonical_signal_id"],
        "client_id": signal["client_id"],
        "execution_mode": "paper",
        "local_order_id": signal["local_order_id"],
    }
    osm = _ReadbackOSM(
        _row(
            signal,
            meta={
                "trigger_crossed_at": durable_timestamp,
                "trigger_crossed_at_provenance": durable_provenance,
                "broker_ready": "false",
            },
        )
    )
    watcher = _AuditWatcher(MagicMock(), order_state_machine=osm, mode="PAPER")
    watched = WatchedSignal(signal, overnight=False)
    watched._watcher_ref = watcher

    assert watcher._persist_trigger_confirmation_authority(watched) is True
    assert [call[2].get("expected_new_trigger_authority") for call in osm.calls] == [
        True,
        None,
    ]
    assert osm.calls[1][2]["expected_existing_trigger_authority"] is True
    assert osm.calls[1][1]["trigger_crossed_at"] == durable_timestamp
    assert osm.calls[1][1]["trigger_crossed_at_provenance"] == durable_provenance


class _ReadbackCursor:
    def __init__(self, row):
        self.row = row

    def execute(self, _sql, _params):
        return self

    def fetchall(self):
        return [self.row]


@contextmanager
def _readback_conn(row):
    yield _ReadbackCursor(row)


def _durable_authority_row(signal, *, broker_ready):
    provenance = {
        "canonical_signal_id": signal["canonical_signal_id"],
        "client_id": signal["client_id"],
        "execution_mode": signal["execution_mode"],
        "local_order_id": signal["local_order_id"],
    }
    return {
        "local_order_id": signal["local_order_id"],
        "client_id": signal["client_id"],
        "signal_id": signal["signal_id"],
        "canonical_signal_id": signal["canonical_signal_id"],
        "execution_mode": signal["execution_mode"],
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {
            "trigger_crossed_at": "2026-09-09T20:00:00Z",
            "trigger_crossed_at_provenance": provenance,
            "broker_ready": broker_ready,
        },
    }


@pytest.mark.parametrize(
    "broker_ready",
    [None, False, "", "false", " FALSE "],
    ids=["absent-or-null", "bool-false", "empty", "text-false", "trimmed-text-false"],
)
def test_readback_accepts_explicit_broker_not_ready_values(monkeypatch, broker_ready):
    import ap.order_state_machine as osm_module

    signal = _signal(local_order_id=f"local-confirm-606-positive-{str(broker_ready)!r}")
    row = _durable_authority_row(signal, broker_ready=broker_ready)
    if broker_ready is None:
        row["meta"].pop("broker_ready")
    monkeypatch.setattr(osm_module, "conn", lambda: _readback_conn(row))
    monkeypatch.setattr(osm_module, "run_with_retry", lambda fn: fn())
    osm = osm_module.APOrderStateMachine.__new__(osm_module.APOrderStateMachine)
    osm.client_id = signal["client_id"]

    authority = osm.read_trigger_confirmation_authority(
        signal["local_order_id"],
        client_id=signal["client_id"],
        execution_mode=signal["execution_mode"],
        signal_id=signal["signal_id"],
        canonical_signal_id=signal["canonical_signal_id"],
    )

    assert authority["proven"] is True
    assert authority["trigger_crossed_at"] == row["meta"]["trigger_crossed_at"]


@pytest.mark.parametrize(
    "broker_ready",
    [True, "true", "broker-free", 0, 1, [], {}],
    ids=["bool-true", "text-true", "malformed-text", "zero", "one", "list", "object"],
)
def test_readback_rejects_truthy_or_malformed_broker_ready_values(monkeypatch, broker_ready):
    import ap.order_state_machine as osm_module

    signal = _signal(local_order_id=f"local-confirm-606-negative-{type(broker_ready).__name__}")
    row = _durable_authority_row(signal, broker_ready=broker_ready)
    monkeypatch.setattr(osm_module, "conn", lambda: _readback_conn(row))
    monkeypatch.setattr(osm_module, "run_with_retry", lambda fn: fn())
    osm = osm_module.APOrderStateMachine.__new__(osm_module.APOrderStateMachine)
    osm.client_id = signal["client_id"]

    assert osm.read_trigger_confirmation_authority(
        signal["local_order_id"],
        client_id=signal["client_id"],
        execution_mode=signal["execution_mode"],
        signal_id=signal["signal_id"],
        canonical_signal_id=signal["canonical_signal_id"],
    ) is None


def test_response_loss_legacy_false_reaches_callback_only_after_existing_cas(monkeypatch):
    import ap.order_state_machine as osm_module

    signal = _signal(local_order_id="local-confirm-606-response-loss")
    durable_timestamp = "2026-09-09T20:00:00Z"
    signal["trigger_crossed_at"] = durable_timestamp
    row = _durable_authority_row(signal, broker_ready="false")
    row["meta"]["trigger_crossed_at"] = durable_timestamp
    osm = _ReadbackOSM(row)

    def _production_readback(*args, **kwargs):
        return osm_module.APOrderStateMachine.read_trigger_confirmation_authority(
            osm, *args, **kwargs
        )

    osm.read_trigger_confirmation_authority = _production_readback
    monkeypatch.setattr(osm_module, "conn", lambda: _readback_conn(row))
    monkeypatch.setattr(osm_module, "run_with_retry", lambda fn: fn())

    def _callback(*_args, **_kwargs):
        assert len(osm.calls) == 2
        assert osm.calls[0][2]["expected_new_trigger_authority"] is True
        assert osm.calls[1][2]["expected_existing_trigger_authority"] is True

    callback = MagicMock(side_effect=_callback)
    _watcher, watched, _ = _confirmed_dispatch(
        osm, signal, callback=callback
    )

    assert callback.call_count == 1
    assert len(osm.calls) == 2
    assert watched._trigger_authority_persisted is True


def test_ccep_shaped_live_timestamp_only_row_is_held_without_runner_backfill():
    signal = _signal(
        local_order_id="30a7ec8e-7c7a-4bf3-b7be-d73134c534d1",
        execution_mode="live",
    )
    signal["signal_id"] = "9bf75901-5d27-4617-b365-29b660a55a18"
    signal["canonical_signal_id"] = signal["signal_id"]
    signal["client_id"] = "jasoncosby1@gmail.com"
    signal["client_email"] = signal["client_id"]
    signal["metadata"].update(
        {
            "signal_id": signal["signal_id"],
            "canonical_signal_id": signal["canonical_signal_id"],
            "client_id": signal["client_id"],
            "execution_mode": "live",
        }
    )
    osm = _DispatchOSM(
        _row(
            signal,
            meta={
                "trigger_crossed_at": "2026-09-09T13:56:51.229915+00:00",
            },
        )
    )

    _watcher, _watched, callback = _confirmed_dispatch(osm, signal, mode="LIVE")

    callback.assert_not_called()
    assert osm.row["meta"] == {
        "trigger_crossed_at": "2026-09-09T13:56:51.229915+00:00",
    }


def _postgres_url_or_skip():
    url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL")
    if not url:
        if os.getenv("GITHUB_ACTIONS") == "true":
            pytest.fail("INTELLIGENCE_POSTGRES_TEST_URL is required in GitHub Actions")
        pytest.skip("disposable PostgreSQL URL not configured")
    return url


class _ScopedPostgres:
    def __init__(self, url, schema):
        self.url = url
        self.schema = schema

    @contextmanager
    def conn(self):
        import psycopg2

        connection = psycopg2.connect(self.url)
        cursor = connection.cursor()
        try:
            cursor.execute(f'SET search_path TO "{self.schema}"')
            yield _CursorWrapper(cursor)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()


class _CursorWrapper:
    def __init__(self, cursor):
        self.cursor = cursor

    @property
    def rowcount(self):
        return self.cursor.rowcount

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params)
        return self


def _read_meta(url, schema, local_order_id):
    import psycopg2
    import psycopg2.extras

    connection = psycopg2.connect(url)
    try:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(
                f'SELECT meta FROM "{schema}".orders WHERE local_order_id=%s',
                (local_order_id,),
            )
            row = cursor.fetchone()
            return dict(row["meta"] or {}) if row else None
    finally:
        connection.close()


def test_postgres_confirmation_cas_has_one_winner_and_never_repairs_timestamp_only(monkeypatch):
    """Real PostgreSQL gate for atomic shape and the competing-writer CAS."""
    pytest.importorskip("psycopg2")
    url = _postgres_url_or_skip()
    import psycopg2

    schema = f"trigger_603_{uuid.uuid4().hex}"
    setup = psycopg2.connect(url)
    try:
        with setup.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{schema}"')
            cursor.execute(f'''
                CREATE TABLE "{schema}".orders (
                    local_order_id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    kind TEXT,
                    execution_mode TEXT,
                    signal_id TEXT,
                    canonical_signal_id TEXT,
                    status TEXT,
                    broker_order_id TEXT,
                    submitted_ts TIMESTAMPTZ,
                    meta JSONB,
                    updated_ts TIMESTAMPTZ
                )
            ''')
        setup.commit()

        import ap.order_state_machine as osm_module

        scoped = _ScopedPostgres(url, schema)
        monkeypatch.setattr(osm_module, "conn", scoped.conn)
        monkeypatch.setattr(osm_module, "run_with_retry", lambda fn: fn())
        osm = osm_module.APOrderStateMachine.__new__(osm_module.APOrderStateMachine)
        osm.client_id = "confirm@example.com"

        signal = _signal()
        signal["materialization_generation"] = 7
        signal["metadata"]["materialization_generation"] = 7
        with scoped.conn() as connection:
            connection.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, execution_mode, signal_id,
                    canonical_signal_id, status, meta, updated_ts
                ) VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,NOW())
                """,
                (
                    signal["local_order_id"], signal["client_id"],
                    signal["execution_mode"], signal["signal_id"],
                    signal["canonical_signal_id"], "PENDING_TRIGGER",
                    json.dumps({"materialization_generation": 7}),
                ),
            )

        patch = {
            "trigger_crossed_at": datetime.now(timezone.utc).isoformat(),
            "trigger_crossed_at_provenance": {
                "canonical_signal_id": signal["canonical_signal_id"],
                "client_id": signal["client_id"],
                "execution_mode": signal["execution_mode"],
                "local_order_id": signal["local_order_id"],
            },
        }
        kwargs = {
            "expected_status": "PENDING_TRIGGER",
            "expected_execution_mode": "paper",
            "expected_signal_id": signal["signal_id"],
            "expected_canonical_signal_id": signal["canonical_signal_id"],
            "expected_new_trigger_authority": True,
            "expected_materialization_generation": 7,
        }

        winners = []
        barrier = threading.Barrier(2)

        def worker():
            barrier.wait()
            if osm.update_order_meta(signal["local_order_id"], patch, **kwargs):
                winners.append(True)

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
        assert all(not thread.is_alive() for thread in threads)
        assert len(winners) == 1
        durable_meta = _read_meta(url, schema, signal["local_order_id"])
        assert durable_meta["trigger_crossed_at"] == patch["trigger_crossed_at"]
        assert durable_meta["trigger_crossed_at_provenance"] == patch[
            "trigger_crossed_at_provenance"
        ]

        crash_id = "local-confirm-603-before-commit"
        with scoped.conn() as connection:
            connection.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, execution_mode, signal_id,
                    canonical_signal_id, status, meta, updated_ts
                ) VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,NOW())
                """,
                (
                    crash_id, signal["client_id"], signal["execution_mode"],
                    signal["signal_id"], signal["canonical_signal_id"],
                    "PENDING_TRIGGER",
                    json.dumps({"materialization_generation": 7}),
                ),
            )

        @contextmanager
        def _crash_after_mutation():
            with scoped.conn() as connection:
                yield connection
                raise RuntimeError("simulated process death before commit")

        monkeypatch.setattr(osm_module, "conn", _crash_after_mutation)
        assert not osm.update_order_meta(crash_id, patch, **kwargs)
        assert _read_meta(url, schema, crash_id) == {
            "materialization_generation": 7,
        }

        legacy_id = "local-confirm-603-legacy"
        with scoped.conn() as connection:
            connection.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, execution_mode, signal_id,
                    canonical_signal_id, status, meta, updated_ts
                ) VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb,NOW())
                """,
                (
                    legacy_id, signal["client_id"], signal["execution_mode"],
                    signal["signal_id"], signal["canonical_signal_id"],
                    "PENDING_TRIGGER",
                    json.dumps({
                        "materialization_generation": 7,
                        "trigger_crossed_at": "2026-09-09T13:56:51+00:00",
                    }),
                ),
            )
        assert not osm.update_order_meta(
            legacy_id, patch, **kwargs
        )
        assert _read_meta(url, schema, legacy_id) == {
            "materialization_generation": 7,
            "trigger_crossed_at": "2026-09-09T13:56:51+00:00",
        }

        # PR #606: exercise the production readback against JSONB text
        # broker_ready="false".  The SQL selector and Python validator must
        # agree before an existing-authority CAS can be attempted.
        monkeypatch.setattr(osm_module, "conn", scoped.conn)
        legacy_false_id = "local-confirm-606-postgres-false"
        legacy_false_timestamp = "2026-09-09T20:00:00Z"
        legacy_false_provenance = {
            "canonical_signal_id": signal["canonical_signal_id"],
            "client_id": signal["client_id"],
            "execution_mode": signal["execution_mode"],
            "local_order_id": legacy_false_id,
        }
        with scoped.conn() as connection:
            connection.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, kind, execution_mode, signal_id,
                    canonical_signal_id, status, meta, updated_ts
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,NOW())
                """,
                (
                    legacy_false_id, signal["client_id"], "ENTRY",
                    signal["execution_mode"],
                    signal["signal_id"], signal["canonical_signal_id"],
                    "PENDING_TRIGGER",
                    json.dumps({
                        "materialization_generation": 7,
                        "trigger_crossed_at": legacy_false_timestamp,
                        "trigger_crossed_at_provenance": legacy_false_provenance,
                        "broker_ready": "false",
                    }),
                ),
            )
        authority = osm.read_trigger_confirmation_authority(
            legacy_false_id,
            client_id=signal["client_id"],
            execution_mode=signal["execution_mode"],
            signal_id=signal["signal_id"],
            canonical_signal_id=signal["canonical_signal_id"],
            expected_materialization_generation=7,
        )
        assert authority is not None
        assert authority["proven"] is True
        assert authority["trigger_crossed_at"] == legacy_false_timestamp
        assert authority["trigger_crossed_at_provenance"] == legacy_false_provenance
    finally:
        with setup.cursor() as cursor:
            setup.rollback()
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        setup.commit()
        setup.close()
