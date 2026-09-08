"""P0 tests for cross-source WATCHING authority classification.

These fixtures use the production-shaped Postgres row and Supabase row
envelopes.  The fetch implementation must inspect both source authorities
before any signal_id/setup deduplication is allowed to remove a row.
"""
from __future__ import annotations

import sys
import types
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import ap_overnight_reeval as ov


def _payload(**overrides):
    value = {
        "signal_id": "S1",
        "canonical_signal_id": "CANONICAL-S1",
        "ticker": "AAPL",
        "side": "CALL",
        "timeframe": "1d",
        "entry_trigger": 100.0,
        "strategy": "STRAT-A",
        "pattern": "2-1-2",
        "score": 80.0,
        "tier": "A",
        "stop_price": 90.0,
        "target_price": 115.0,
        "underlying_at_signal": 100.0,
    }
    value.update(overrides)
    return value


def _source_rows(*, trade_payload=None, ap_payload=None, ap_columns=None):
    trade_payload = dict(trade_payload or _payload())
    ap_payload = ap_payload if ap_payload is not None else dict(trade_payload)
    ap_columns = dict(ap_columns or {})
    return (
        {
            "id": "tq-job-S1",
            "signal_id": "S1",
            "payload": trade_payload,
            "created_ts": "2026-09-08T13:00:00+00:00",
        },
        {
            "signal_id": "S1",
            "signal_payload": ap_payload,
            "raw_payload": None,
            "created_at": "2026-09-08T13:00:01+00:00",
            "ticker": ap_columns.get("ticker", trade_payload.get("ticker")),
            "side": ap_columns.get("side", trade_payload.get("side")),
            "score": ap_columns.get("score", trade_payload.get("score")),
            "timeframe": ap_columns.get("timeframe", trade_payload.get("timeframe")),
            "pattern": ap_columns.get("pattern", trade_payload.get("pattern")),
            "tier": ap_columns.get("tier", trade_payload.get("tier")),
            "decision_status": "WATCHING",
            "entry_trigger": ap_columns.get("entry_trigger", trade_payload.get("entry_trigger")),
            "stop_price": ap_columns.get("stop_price", trade_payload.get("stop_price")),
            "target_price": ap_columns.get("target_price", trade_payload.get("target_price")),
            "underlying_at_signal": ap_columns.get(
                "underlying_at_signal", trade_payload.get("underlying_at_signal")
            ),
        },
    )


class _FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        self.executed.append((str(sql), params))
        assert "SELECT id, signal_id, payload, created_ts" in str(sql)
        assert "UPDATE" not in str(sql).upper()

    def fetchall(self):
        return list(self.rows)


class _FakeSupabaseQuery:
    def __init__(self, rows, seen_limits):
        self.rows = rows
        self.seen_limits = seen_limits

    def select(self, *_args, **_kwargs):
        return self

    def eq(self, *_args, **_kwargs):
        return self

    def gte(self, *_args, **_kwargs):
        return self

    def order(self, *_args, **_kwargs):
        return self

    def limit(self, value):
        self.seen_limits.append(int(value))
        return self

    def execute(self):
        return SimpleNamespace(data=list(self.rows))


class _FakeSupabase:
    def __init__(self, rows, seen_limits):
        self.rows = rows
        self.seen_limits = seen_limits

    def table(self, _name):
        return _FakeSupabaseQuery(self.rows, self.seen_limits)


def _fetch_from_sources(monkeypatch, *, trade_payload=None, ap_payload=None, ap_columns=None):
    trade_row, ap_row = _source_rows(
        trade_payload=trade_payload,
        ap_payload=ap_payload,
        ap_columns=ap_columns,
    )
    fake_cursor = _FakeCursor([trade_row])
    from ap import db as apdb

    monkeypatch.setattr(apdb, "conn", lambda: fake_cursor)
    monkeypatch.setattr(apdb, "run_with_retry", lambda fn: fn())

    seen_limits = []
    fake_supabase = types.ModuleType("supabase")
    fake_supabase.create_client = lambda _url, _key: _FakeSupabase([ap_row], seen_limits)
    monkeypatch.setitem(sys.modules, "supabase", fake_supabase)
    monkeypatch.setenv("SUPABASE_URL", "https://supabase.example.test")
    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "service-key")
    monkeypatch.setenv("OVERNIGHT_FETCH_LIMIT", "500")

    result = ov._fetch_watching_signals_with_status_impl("jose@example.com")
    return result, fake_cursor, seen_limits


@pytest.mark.parametrize(
    "field,trade_value,ap_value",
    [
        ("ticker", "AAPL", "NVDA"),
        ("side", "CALL", "PUT"),
        ("canonical_signal_id", "CANONICAL-S1-A", "CANONICAL-S1-B"),
        ("entry_trigger", 100.0, 101.0),
        ("timeframe", "1d", "4h"),
        ("strategy", "STRAT-A", "STRAT-B"),
    ],
)
def test_cross_source_authority_conflict_is_retained_before_dedup(
    monkeypatch, field, trade_value, ap_value
):
    trade = _payload(**{field: trade_value})
    ap = _payload(**{field: ap_value})

    result, _cursor, _limits = _fetch_from_sources(
        monkeypatch,
        trade_payload=trade,
        ap_payload=ap,
    )

    assert result.source_identity_conflicts
    conflict = result.source_identity_conflicts[0]
    assert conflict["signal_id"] == "S1"
    assert field in conflict["conflicting_fields"]
    assert {item["source"] for item in conflict["authorities"]} == {
        "trade_queue",
        "ap_signals",
    }
    assert {item["job_id"] for item in conflict["authorities"]} == {
        "tq-job-S1",
        "sup:S1",
    }
    # Both raw rows remain visible; no upstream signal_id dedup erased the
    # contradictory source authority.
    assert len(result.rows) == 2
    assert result.source_row_accounting["raw_source_rows_fetched"] == 2
    assert result.source_row_accounting["source_identity_conflict_rows"] == 2


def test_malformed_payload_on_one_source_is_a_conflict_and_is_diagnostic(monkeypatch):
    result, _cursor, _limits = _fetch_from_sources(
        monkeypatch,
        trade_payload=_payload(),
        ap_payload="{not-valid-json",
    )

    assert result.source_identity_conflicts
    conflict = result.source_identity_conflicts[0]
    assert "payload" in conflict["conflicting_fields"]
    assert len(conflict["authorities"]) == 2
    malformed = [
        authority
        for authority in conflict["authorities"]
        if authority["source"] == "ap_signals"
    ][0]
    assert any("json_invalid" in error for error in malformed["field_errors"])
    assert len(result.rows) == 2


def test_blank_required_identity_does_not_override_authoritative_source_value(monkeypatch):
    trade = _payload(ticker="")
    ap = _payload(ticker="AAPL")
    result, _cursor, _limits = _fetch_from_sources(
        monkeypatch,
        trade_payload=trade,
        ap_payload=ap,
    )

    assert result.source_identity_conflicts
    assert "ticker" in result.source_identity_conflicts[0]["conflicting_fields"]
    assert len(result.rows) == 2


def test_blank_canonical_identity_does_not_override_authoritative_source_value(monkeypatch):
    result, _cursor, _limits = _fetch_from_sources(
        monkeypatch,
        trade_payload=_payload(canonical_signal_id=""),
        ap_payload=_payload(canonical_signal_id="CANONICAL-S1"),
    )

    assert result.source_identity_conflicts
    assert "canonical_signal_id" in result.source_identity_conflicts[0]["conflicting_fields"]


def test_same_source_duplicate_jobs_keep_independent_ownership_rows():
    trade = {"id": "tq-1", "signal_id": "S1", "payload": _payload(), "_source": "trade_queue"}
    duplicate = {"id": "tq-2", "signal_id": "S1", "payload": _payload(), "_source": "trade_queue"}

    selected, conflicts, equivalents, accounting = ov._classify_source_authorities(
        [trade, duplicate]
    )

    assert selected == [trade, duplicate]
    assert conflicts == []
    assert equivalents == []
    assert accounting["raw_source_rows_fetched"] == 2
    assert accounting["logical_rows_before_setup_dedup"] == 2


def test_same_source_duplicate_authority_conflict_is_not_deduped():
    first = {"id": "tq-1", "signal_id": "S1", "payload": _payload(), "_source": "trade_queue"}
    second = {
        "id": "tq-2",
        "signal_id": "S1",
        "payload": _payload(ticker="NVDA"),
        "_source": "trade_queue",
    }

    selected, conflicts, equivalents, accounting = ov._classify_source_authorities(
        [first, second]
    )

    assert len(selected) == 2
    assert not equivalents
    assert conflicts and "ticker" in conflicts[0]["conflicting_fields"]
    assert accounting["source_identity_conflict_rows"] == 2


def test_single_source_malformed_payload_fails_closed():
    selected, conflicts, _equivalents, _accounting = ov._classify_source_authorities(
        [{
            "id": "tq-malformed",
            "signal_id": "S1",
            "payload": "{not-json",
            "_source": "trade_queue",
        }]
    )

    assert len(selected) == 1
    assert conflicts
    assert "payload" in conflicts[0]["conflicting_fields"]


def test_payload_column_contradiction_is_visible_before_source_precedence(monkeypatch):
    result, _cursor, _limits = _fetch_from_sources(
        monkeypatch,
        trade_payload=_payload(),
        ap_payload=_payload(),
        ap_columns={"ticker": "NVDA"},
    )

    assert result.source_identity_conflicts
    assert "ticker" in result.source_identity_conflicts[0]["conflicting_fields"]
    assert len(result.rows) == 2


def test_signal_payload_and_raw_payload_contradiction_is_preserved():
    row = {
        "id": "sup:S1",
        "signal_id": "S1",
        "signal_payload": _payload(ticker="AAPL"),
        "raw_payload": _payload(ticker="NVDA"),
        "_source": "ap_signals",
    }

    selected, conflicts, _equivalents, _accounting = ov._classify_source_authorities([row])

    assert len(selected) == 1
    assert conflicts
    assert "ticker" in conflicts[0]["conflicting_fields"]


def test_canonical_identity_connects_rows_even_when_raw_signal_ids_differ():
    first = {
        "id": "tq-1",
        "signal_id": "RAW-1",
        "payload": _payload(signal_id="RAW-1", canonical_signal_id="CANONICAL-S1"),
        "_source": "trade_queue",
    }
    second = {
        "id": "sup:RAW-2",
        "signal_id": "RAW-2",
        "payload": _payload(signal_id="RAW-2", canonical_signal_id="CANONICAL-S1", side="PUT"),
        "_source": "ap_signals",
    }

    selected, conflicts, _equivalents, _accounting = ov._classify_source_authorities(
        [first, second]
    )

    assert len(selected) == 2
    assert conflicts and "side" in conflicts[0]["conflicting_fields"]


def test_equivalent_cross_source_duplicate_has_explicit_stable_precedence(monkeypatch):
    first, _cursor, _limits = _fetch_from_sources(
        monkeypatch,
        trade_payload=_payload(),
        ap_payload=_payload(),
    )
    second, _cursor, _limits = _fetch_from_sources(
        monkeypatch,
        trade_payload=_payload(),
        ap_payload=_payload(),
    )

    for result in (first, second):
        assert result.source_identity_conflicts == []
        assert len(result.rows) == 1
        assert result.rows[0]["_source"] == "trade_queue"
        assert result.rows[0]["id"] == "tq-job-S1"
        assert result.source_row_accounting == {
            "raw_source_rows_fetched": 2,
            "raw_trade_queue_rows": 1,
            "raw_ap_signals_rows": 1,
            "authority_components": 1,
            "equivalent_duplicate_groups": 1,
            "equivalent_duplicate_rows_collapsed": 1,
            "source_identity_conflict_groups": 0,
            "source_identity_conflict_rows": 0,
            "authority_rows_after_classification": 1,
            "logical_rows_before_setup_dedup": 1,
            "logical_rows_after_setup_dedup": 1,
            "setup_duplicate_rows_collapsed": 0,
        }
        assert result.equivalent_source_duplicates[0]["classification"] == (
            "EQUIVALENT_DUPLICATE"
        )
        assert result.equivalent_source_duplicates[0]["chosen_source"] == "trade_queue"
        assert result.equivalent_source_duplicates[0]["chosen_job_id"] == "tq-job-S1"

    # The chosen provenance and diagnostic shape are restart-stable.
    assert first.rows == second.rows
    assert first.equivalent_source_duplicates == second.equivalent_source_duplicates


def _classify_trigger_authority_pair(*, trade_payload, ap_payload):
    trade_row, ap_row = _source_rows(
        trade_payload=trade_payload,
        ap_payload=ap_payload,
    )
    return ov._classify_source_authorities([
        {**trade_row, "_source": "trade_queue"},
        {**ap_row, "id": "sup:S1", "_source": "ap_signals"},
    ])


def test_explicit_trigger_authority_agreement_is_equivalent():
    selected, conflicts, equivalents, _accounting = _classify_trigger_authority_pair(
        trade_payload=_payload(trigger_authority="breach"),
        ap_payload=_payload(trigger_authority="BREACH"),
    )

    assert len(selected) == 1
    assert conflicts == []
    assert equivalents and equivalents[0]["classification"] == "EQUIVALENT_DUPLICATE"


def test_explicit_trigger_authority_disagreement_fails_closed():
    selected, conflicts, equivalents, _accounting = _classify_trigger_authority_pair(
        trade_payload=_payload(trigger_authority="breach"),
        ap_payload=_payload(trigger_authority="entry_trigger"),
    )

    assert len(selected) == 2
    assert equivalents == []
    assert conflicts and "trigger_authority" in conflicts[0]["conflicting_fields"]


def test_sparse_trigger_authority_is_compatible_with_rich_same_trigger():
    selected, conflicts, equivalents, _accounting = _classify_trigger_authority_pair(
        trade_payload=_payload(),
        ap_payload=_payload(trigger_authority="breach"),
    )

    assert len(selected) == 1
    assert conflicts == []
    assert equivalents and equivalents[0]["classification"] == "EQUIVALENT_DUPLICATE"


def test_sparse_trigger_authority_is_compatible_with_sparse_same_trigger():
    selected, conflicts, equivalents, _accounting = _classify_trigger_authority_pair(
        trade_payload=_payload(),
        ap_payload=_payload(),
    )

    assert len(selected) == 1
    assert conflicts == []
    assert equivalents and equivalents[0]["classification"] == "EQUIVALENT_DUPLICATE"


@pytest.mark.parametrize("malformed", ["", {"label": "breach"}])
def test_blank_or_malformed_trigger_authority_fails_closed(malformed):
    selected, conflicts, _equivalents, _accounting = _classify_trigger_authority_pair(
        trade_payload=_payload(trigger_authority=malformed),
        ap_payload=_payload(trigger_authority="breach"),
    )

    assert len(selected) == 2
    assert conflicts and "trigger_authority" in conflicts[0]["conflicting_fields"]


def test_concrete_trigger_disagreement_conflicts_even_with_compatible_labels():
    selected, conflicts, _equivalents, _accounting = _classify_trigger_authority_pair(
        trade_payload=_payload(entry_trigger=100.0, trigger_authority="breach"),
        ap_payload=_payload(entry_trigger=101.0, trigger_authority="breach"),
    )

    assert len(selected) == 2
    assert conflicts and "entry_trigger" in conflicts[0]["conflicting_fields"]


def _conflict_fetch_result():
    trade, _ = _source_rows()
    _selected, conflicts, equivalents, accounting = ov._classify_source_authorities(
        [
            {**trade, "_source": "trade_queue"},
            {"id": "sup:S1", "signal_id": "S1", "payload": _payload(ticker="NVDA"), "_source": "ap_signals"},
        ]
    )
    return ov._FetchWatchingSignalsResult(
        rows=[
            {**trade, "_source": "trade_queue"},
            {"id": "sup:S1", "signal_id": "S1", "payload": _payload(ticker="NVDA"), "_source": "ap_signals"},
        ],
        trade_queue_status=ov._SOURCE_STATUS_SUCCESS,
        ap_signals_status=ov._SOURCE_STATUS_SUCCESS,
        trade_queue_error=None,
        ap_signals_error=None,
        source_identity_conflicts=conflicts,
        equivalent_source_duplicates=equivalents,
        source_row_accounting=accounting,
    )


@pytest.mark.parametrize(
    "mode_url",
    ["https://sandbox.tradier.com", "https://api.tradier.com"],
    ids=["PAPER", "LIVE"],
)
def test_conflicted_opportunity_fails_closed_without_downstream_mutation(
    monkeypatch, mode_url
):
    fetch_result = _conflict_fetch_result()
    monkeypatch.setattr(ov, "_fetch_watching_signals_with_status", lambda _client: fetch_result)

    queue_mutations = [
        MagicMock(name="mark_job_rejected"),
        MagicMock(name="mark_job_error"),
        MagicMock(name="mark_job_watching_reason"),
        MagicMock(name="mark_job_watching_armed"),
    ]
    for name, mock in zip(
        ("_mark_job_rejected", "_mark_job_error", "_mark_job_watching_reason", "_mark_job_watching_armed"),
        queue_mutations,
    ):
        monkeypatch.setattr(ov, name, mock)
    signal_writes = MagicMock(name="upsert_ap_signal_row_with_fallback")
    monkeypatch.setattr(ov, "upsert_ap_signal_row_with_fallback", signal_writes)

    watcher = SimpleNamespace(watch=MagicMock(), _running=True)
    broker = SimpleNamespace(
        base_url=mode_url,
        submit_order=MagicMock(),
        place_order=MagicMock(),
        cancel_order=MagicMock(),
        replace_order=MagicMock(),
    )
    osm = MagicMock(name="order_state_machine")
    positions = MagicMock(name="position_manager")

    result = ov.run_overnight_reeval(
        client_id="jose@example.com",
        broker=broker,
        data_broker=broker,
        master_control=MagicMock(),
        contract_selector=MagicMock(),
        order_state_machine=osm,
        entry_watcher=watcher,
        position_manager=positions,
        force=True,
    )

    assert result["result_class"] == "SOURCE_IDENTITY_CONFLICT"
    assert result["completed"] is False
    assert result["retryable"] is False
    assert result["source_lookup_partial"] is False
    assert result["fetched"] == 2
    assert result["unresolved"] == 2
    assert result["source_identity_conflict"] is True
    assert len(result["source_identity_conflicts"][0]["authorities"]) == 2

    watcher.watch.assert_not_called()
    osm.assert_not_called()
    positions.assert_not_called()
    signal_writes.assert_not_called()
    for mock in queue_mutations:
        mock.assert_not_called()
    for method in (
        broker.submit_order,
        broker.place_order,
        broker.cancel_order,
        broker.replace_order,
    ):
        method.assert_not_called()


def test_conflict_cannot_be_certified_by_safe_partial_guard(monkeypatch):
    from ap import morning_handoff
    from ap import preopen_safe_partial_guard as guard

    conflict = {
        "signal_id": "S1",
        "conflicting_fields": ["ticker"],
        "authorities": [
            {"source": "trade_queue", "job_id": "tq-job-S1", "signal_id": "S1"},
            {"source": "ap_signals", "job_id": "sup:S1", "signal_id": "S1"},
        ],
    }
    monkeypatch.setattr(
        morning_handoff,
        "_latest_handoff_rows",
        lambda _date: [
            {
                "client_id": "jose@example.com",
                "execution_mode": "live",
                "stage": "overnight_reeval",
                "status": "partial",
                "last_error": "OVERNIGHT_REEVAL_RETRY_EXHAUSTED",
                "details": {
                    "source_identity_conflict": True,
                    "source_identity_conflicts": [conflict],
                },
            }
        ],
    )

    ok, diagnostic = guard.classify_safe_exhausted_partial(
        None,
        runner=SimpleNamespace(),
        client_state={},
        client_id="jose@example.com",
        execution_mode="live",
        trading_date="2026-09-08",
    )
    assert ok is False
    assert diagnostic == {"reason": "overnight_source_identity_conflict"}


def test_conflict_diagnostics_survive_durable_overnight_lock_persistence(monkeypatch):
    import client_runner as cr

    calls = []
    handoff = types.ModuleType("ap.morning_handoff")
    handoff._upsert_handoff_run_lock = lambda **kwargs: calls.append(kwargs)
    monkeypatch.setitem(sys.modules, "ap.morning_handoff", handoff)

    runner = object.__new__(cr.ClientRunner)
    runner.email = "jose@example.com"
    runner.mode = "LIVE"
    runner._overnight_reeval_attempt_count = 3
    runner._overnight_reeval_last_attempt_at = None
    runner._overnight_reeval_next_retry_at = None

    conflict = {
        "signal_id": "S1",
        "conflicting_fields": ["ticker"],
        "authorities": [
            {"source": "trade_queue", "job_id": "tq-job-S1", "signal_id": "S1"},
            {"source": "ap_signals", "job_id": "sup:S1", "signal_id": "S1"},
        ],
    }
    accounting = {"raw_source_rows_fetched": 2, "source_identity_conflict_rows": 2}
    cr.ClientRunner._persist_overnight_reeval_lock(
        runner,
        {
            "fetched": 2,
            "processed": 0,
            "armed": 0,
            "rejected": 0,
            "terminal_rejected": 0,
            "skipped": 0,
            "errors": 0,
            "terminal_errors": 0,
            "retryable_deferred": 0,
            "retryable_rows": [],
            "source_lookup_partial": False,
            "trade_queue_status": "SUCCESS",
            "ap_signals_status": "SUCCESS",
            "trade_queue_error": None,
            "ap_signals_error": None,
            "source_identity_conflict": True,
            "source_identity_conflicts": [conflict],
            "equivalent_source_duplicates": [],
            "source_row_accounting": accounting,
            "already_resolved": 0,
            "unresolved": 2,
            "stale_skipped": 0,
            "fresh_processed": 0,
            "fresh_armed": 0,
            "stalled": False,
            "result_class": "SOURCE_IDENTITY_CONFLICT",
            "completed": False,
            "retryable": False,
            "retry_reason": "source_identity_conflict",
            "last_error": "SOURCE_IDENTITY_CONFLICT",
        },
        today=__import__("datetime").date(2026, 9, 8),
        source="scheduler",
        now_et=__import__("datetime").datetime(2026, 9, 8, 9, 15),
    )

    assert len(calls) == 1
    persisted = calls[0]
    assert persisted["status"] == "partial"
    assert persisted["last_error"] == "SOURCE_IDENTITY_CONFLICT"
    assert persisted["details"]["source_identity_conflicts"] == [conflict]
    assert persisted["details"]["source_row_accounting"] == accounting
