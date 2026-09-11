import json
from types import SimpleNamespace

import pytest

import ap.order_state_machine as osm_mod
from ap.order_state_machine import APOrderStateMachine
from ap_canonical_signal import build_canonical_signal_id


CLIENT_ID = "client@example.com"
ORDER_ID = "order-trigger-authority-1"
SIGNAL_ID = "sig-trigger-authority-1"
TRIGGER_TS = "2026-09-10T15:00:00+00:00"


def _claim_kwargs(trigger_crossed_at=TRIGGER_TS):
    return {
        "owner": "watcher:test",
        "generation": 1,
        "lease_until": "2026-09-10T15:01:00+00:00",
        "trigger_crossed_at": trigger_crossed_at,
        "trigger_price": 100.0,
        "observed_underlying_price": 100.25,
        "signal_id": SIGNAL_ID,
        "execution_mode": "live",
    }


class _CaptureConn:
    def __init__(self, capture):
        self.capture = capture
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.capture["sql"] = sql
        self.capture["params"] = params
        self.rowcount = 1
        return SimpleNamespace(rowcount=1)


def _json_params(params):
    decoded = []
    for value in params:
        if not isinstance(value, str) or not value.lstrip().startswith("{"):
            continue
        try:
            decoded.append(json.loads(value))
        except json.JSONDecodeError:
            pass
    return decoded


def test_claim_persists_trigger_authority_as_one_atomic_pair(monkeypatch):
    capture = {}
    monkeypatch.setattr(osm_mod, "conn", lambda: _CaptureConn(capture))
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: fn())

    osm = APOrderStateMachine(CLIENT_ID)
    assert osm.claim_deferred_materialization(ORDER_ID, **_claim_kwargs()) is True

    params = capture["params"]
    patch = json.loads(params[0])
    expected_provenance = {
        "canonical_signal_id": build_canonical_signal_id(SIGNAL_ID),
        "client_id": CLIENT_ID,
        "execution_mode": "live",
        "local_order_id": ORDER_ID,
    }

    assert patch["trigger_crossed_at"] == TRIGGER_TS
    assert patch["trigger_crossed_at_provenance"] == expected_provenance

    sql = capture["sql"]
    assert "? 'trigger_crossed_at'" in sql
    assert "? 'trigger_crossed_at_provenance'" in sql
    assert "meta->>'trigger_crossed_at' = %s" in sql
    assert "::timestamptz" not in sql
    assert "trigger_crossed_at_provenance' = %s::jsonb" in sql
    assert TRIGGER_TS in params
    assert expected_provenance in _json_params(params)


@pytest.mark.parametrize(
    "bad_timestamp",
    [None, "", "not-a-time", "2026-09-10T15:00:00"],
)
def test_claim_refuses_missing_malformed_or_naive_trigger_time(monkeypatch, bad_timestamp):
    def _unexpected_conn():
        raise AssertionError("invalid trigger authority must fail before touching the DB")

    monkeypatch.setattr(osm_mod, "conn", _unexpected_conn)
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: fn())

    osm = APOrderStateMachine(CLIENT_ID)
    assert (
        osm.claim_deferred_materialization(
            ORDER_ID,
            **_claim_kwargs(trigger_crossed_at=bad_timestamp),
        )
        is False
    )
