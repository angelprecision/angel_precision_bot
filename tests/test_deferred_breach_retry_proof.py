from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "ap_execution_core.py"


def _load_retry_helpers():
    src = SOURCE.read_text()
    mod = ast.parse(src)
    wanted = {
        "RETRYABLE_BREACH_SELECTOR_REASONS",
        "_classify_deferred_breach_retry_decision",
        "_build_deferred_retry_schedule_meta",
        "_build_deferred_retry_terminal_meta",
        "_build_deferred_retry_stale_abort_meta",
    }
    selected = []
    for node in mod.body:
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id in wanted:
                selected.append(node)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in wanted:
                    selected.append(node)
                    break
        elif isinstance(node, ast.FunctionDef) and node.name in wanted:
            selected.append(node)
    mini = ast.Module(body=selected, type_ignores=[])
    ns = {
        "datetime": datetime,
        "timezone": timezone,
        "timedelta": timedelta,
        "Optional": Optional,
        "__builtins__": __builtins__,
    }
    exec(compile(mini, str(SOURCE), "exec"), ns)
    return ns


def test_retry_disabled_stays_retryable_with_explicit_terminal_reason():
    ns = _load_retry_helpers()
    classify = ns["_classify_deferred_breach_retry_decision"]
    out = classify(
        "CHAIN_ROW_ZERO_BID_ASK",
        queue_local_order_id="ord-1",
        attempt=1,
        max_attempts=3,
        past_cutoff=False,
        retry_enabled=False,
        ladder_retryable=False,
    )
    assert out["action"] == "retry_disabled"
    assert out["retryable_reason"] is True
    assert out["terminal_reason"] == "breach_retry_disabled:CHAIN_ROW_ZERO_BID_ASK"


def test_retry_unavailable_stays_retryable_with_explicit_terminal_reason():
    ns = _load_retry_helpers()
    classify = ns["_classify_deferred_breach_retry_decision"]
    out = classify(
        "DIRECT_QUOTE_ZERO_BID_ASK",
        queue_local_order_id="",
        attempt=1,
        max_attempts=3,
        past_cutoff=False,
        retry_enabled=True,
        ladder_retryable=False,
    )
    assert out["action"] == "retry_unavailable"
    assert out["retryable_reason"] is True
    assert out["terminal_reason"] == "breach_retry_unavailable:DIRECT_QUOTE_ZERO_BID_ASK"


def test_quality_reason_stays_terminal_quality():
    ns = _load_retry_helpers()
    classify = ns["_classify_deferred_breach_retry_decision"]
    out = classify(
        "OI_TOO_LOW",
        queue_local_order_id="ord-1",
        attempt=1,
        max_attempts=3,
        past_cutoff=False,
        retry_enabled=True,
        ladder_retryable=False,
    )
    assert out["action"] == "terminal_quality"
    assert out["retryable_reason"] is False
    assert "terminal_reason" not in out


def test_stale_abort_meta_never_decreases_breach_attempt_count():
    ns = _load_retry_helpers()
    build = ns["_build_deferred_retry_stale_abort_meta"]
    meta = build(
        current_status="PENDING_TRIGGER",
        broker_order_id="",
        submitted_ts=None,
        current_contract="DEFERRED:AVGO",
        current_attempt=3,
        thread_attempt=1,
        selector_audit={"reason_code": "CHAIN_ROW_ZERO_BID_ASK"},
        client_id="jason@example.com",
        execution_mode="live",
        local_order_id="ord-1",
        signal_id="sig-1",
        reason="attempt_count_advanced",
        now=datetime(2026, 7, 3, 15, 30, tzinfo=timezone.utc),
    )
    assert meta["breach_attempt_count"] == 3
    assert meta["deferred_retry_stale_abort_details"]["db_attempt"] == 3
    assert meta["deferred_retry_stale_abort_details"]["thread_attempt"] == 1


def test_source_wires_central_classifier_and_stale_abort_meta():
    src = SOURCE.read_text()
    assert "_decision_a = _classify_deferred_breach_retry_decision(" in src
    assert "_build_deferred_retry_stale_abort_meta(" in src
    assert "breach_retry_cutoff:" in src
    assert "breach_retry_exhausted:" in src
    assert "breach_retry_disabled:" in src
    assert "breach_retry_unavailable:" in src
