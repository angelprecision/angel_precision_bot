from __future__ import annotations

import json
import re
from pathlib import Path
from types import SimpleNamespace

from ap.entry_metadata_guard import (
    ZERO_UNDERLYING,
    _allow_deferred_overnight_watcher_create,
    _mark_deferred_watcher_data_pending,
    validate_entry_metadata,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_daily_after_hours_to_pending_trigger_contract_is_mandatory():
    """Daily/overnight handoff may materialize only a watcher row.

    This protects the live path:
      daily after-hours -> WATCHING -> overnight reeval -> PENDING_TRIGGER

    A missing underlying remains invalid under strict validation, but the
    contract-deferred overnight watcher handoff is allowed only for
    PENDING_TRIGGER so breach-time execution can select the live contract later.
    """
    plan = SimpleNamespace(
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        signal_id="REEVAL:daily-after-hours:CVS:CALL",
        ticker="CVS",
        symbol="CVS",
        side="CALL",
        direction="CALL",
        timeframe="1d",
        score=70.0,
        trigger_price=104.67,
        target_underlying=105.93,
        stop_underlying=102.69,
        contract_symbol="DEFERRED:CVS",
        metadata={"contract_deferred": True, "overnight": True},
    )

    strict = validate_entry_metadata(plan=plan, client_id="jasoncosby1@gmail.com", execution_mode="live")
    assert not strict.ok
    assert strict.reason == ZERO_UNDERLYING

    assert _allow_deferred_overnight_watcher_create(
        plan=plan,
        caller_meta=plan.metadata,
        initial_status="PENDING_TRIGGER",
    ) is True


def test_weekly_scanner_payload_contract_is_mandatory():
    """Weekly scanner payloads must pass metadata guard using scanner evidence.

    This protects:
      weekly scanner -> queue -> metadata guard -> Master Control
    """
    payload = {
        "side": "PUT",
        "score": 65,
        "symbol": "WBD",
        "ticker": "WBD",
        "trigger": {
            "pt1": 26.25,
            "stop": 26.88,
            "entry": 26.57,
            "source": "scanner_consolidation_v3_weekly",
            "expiry_hint": "WEEKLY",
            "current_price": 26.66,
        },
        "ev_score": 65,
        "direction": "PUT",
        "signal_id": "2026-07-01:1-1:WBD:Weekly:PUT",
        "pattern_id": "1-1",
    }

    result = validate_entry_metadata(
        plan=payload,
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
    )

    assert result.ok
    assert result.reason is None


def test_contract_deferred_is_watcher_only_not_created_or_execution_ready():
    """Contract-deferred flow must not become a CREATED or execution-ready order."""
    plan = SimpleNamespace(
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        signal_id="REEVAL:contract-deferred:CVS:CALL",
        ticker="CVS",
        symbol="CVS",
        side="CALL",
        direction="CALL",
        timeframe="1d",
        score=70.0,
        trigger_price=104.67,
        target_underlying=105.93,
        stop_underlying=102.69,
        contract_symbol="DEFERRED:CVS",
        metadata={"contract_deferred": True, "overnight": True},
    )

    assert _allow_deferred_overnight_watcher_create(
        plan=plan,
        caller_meta=plan.metadata,
        initial_status="CREATED",
    ) is False

    caller_meta = dict(plan.metadata)
    _mark_deferred_watcher_data_pending(plan, caller_meta, ZERO_UNDERLYING)
    assert plan.metadata["allowed_for_watcher"] is True
    assert plan.metadata["allowed_for_execution"] is False
    assert caller_meta["allowed_for_execution"] is False


def test_breach_time_submit_contract_requires_final_complete_metadata():
    """Breach-time broker submit must still fail closed until metadata is complete.

    This protects:
      breach-time contract selection -> broker submit only after final metadata complete
    """
    incomplete_order = {
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "signal_id": "REEVAL:breach-submit:CVS:CALL",
        "symbol": "CVS",
        "direction": "CALL",
        "timeframe": "1d",
        "score": 70.0,
        "trigger_price": 104.67,
        "target_underlying": 105.93,
        "stop_underlying": 102.69,
        "meta": json.dumps({
            "contract_deferred": True,
            "overnight": True,
            "underlying_data_pending": True,
            "allowed_for_execution": False,
        }),
    }
    blocked = validate_entry_metadata(
        order=incomplete_order,
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
    )
    assert not blocked.ok
    assert blocked.reason == ZERO_UNDERLYING

    complete_order = dict(incomplete_order)
    complete_order["underlying_entry"] = 104.80
    complete_order["meta"] = json.dumps({
        "contract_deferred": False,
        "overnight": True,
        "allowed_for_execution": True,
    })
    allowed = validate_entry_metadata(
        order=complete_order,
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
    )
    assert allowed.ok


def test_trade_queue_schema_contract_does_not_use_updated_ts():
    """trade_queue has created_ts/started_ts/finished_ts, not updated_ts.

    This catches the exact syntax regression class that breaks operator/debug
    queries by accidentally treating trade_queue like orders.
    """
    offenders: list[str] = []
    patterns = [
        re.compile(r"\bUPDATE\s+trade_queue\b[^;]*\bupdated_ts\b", re.IGNORECASE),
        re.compile(r"\bINSERT\s+INTO\s+trade_queue\b[^;]*\bupdated_ts\b", re.IGNORECASE),
        re.compile(r"\btrade_queue\s+SET\b[^;]*\bupdated_ts\b", re.IGNORECASE),
        re.compile(r"\bFROM\s+trade_queue\b[^;\n]*\bupdated_ts\b", re.IGNORECASE),
        re.compile(r"\bSELECT\b[^;\n]*\bupdated_ts\b[^;\n]*\bFROM\s+trade_queue\b", re.IGNORECASE),
    ]
    for path in REPO_ROOT.rglob("*.py"):
        rel = path.relative_to(REPO_ROOT).as_posix()
        if rel.startswith((".venv/", "venv/")):
            continue
        text = path.read_text(errors="ignore")
        if any(pattern.search(text) for pattern in patterns):
            offenders.append(rel)

    assert offenders == [], f"Do not reference trade_queue.updated_ts; use created_ts/started_ts/finished_ts: {offenders}"


def test_flow_contract_suite_is_mandatory_in_p0_workflow():
    workflow = (REPO_ROOT / ".github" / "workflows" / "p0_regression.yml").read_text()
    assert "tests/test_p0_trade_flow_contracts.py" in workflow
