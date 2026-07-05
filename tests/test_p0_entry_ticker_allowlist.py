"""P0 (monday-trade-flow-readiness): AP_ENTRY_TICKER_ALLOWLIST dispatch gate.

Proves:
  1. Env unset → gate inactive; a non-allowlisted ticker proceeds past the
     gate (reaches Master Control — behavior byte-for-byte unchanged).
  2. Env set + ticker outside list → job REJECTED at stage
     entry_ticker_allowlist with reason_code TICKER_NOT_IN_ALLOWLIST,
     rejection ledger row written, and Master Control NEVER reached.
  3. Env set + ticker inside list → gate passes (Master Control reached).
  4. Index alias normalization: ^GSPC dispatches as SPY, so an allowlist of
     SPY,QQQ,IWM admits ^GSPC rather than rejecting the proxy.
  5. Gate fires BEFORE any capital/selector/watcher machinery: the reject
     path invokes no selector, OSM, or watcher.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

import ap.queue as queue


def _mc(mode="PAPER"):
    """MC spy: records that evaluate() was reached (proves the gate passed),
    then raises so dispatch stops there via its ERROR handler — no further
    machinery runs. _dispatch swallows the exception and marks the job ERROR,
    so 'reached' is the ground truth, not exception propagation."""
    ns = SimpleNamespace(mode=mode, reached=False)

    def _evaluate(*a, **k):
        ns.reached = True
        raise RuntimeError("stop-at-mc (test sentinel)")

    ns.evaluate = _evaluate
    return ns


def _run_dispatch(monkeypatch, ticker, allowlist_env):
    mark_calls = []
    rejection_logs = []
    monkeypatch.setattr(queue, "_mark_job", lambda *a, **k: mark_calls.append((a, k)))
    monkeypatch.setattr(queue, "_log_rejection_to_db", lambda **k: rejection_logs.append(k))
    monkeypatch.setattr(queue, "ALLOW_IMMEDIATE_EXECUTION", False)
    if allowlist_env is None:
        monkeypatch.delenv("AP_ENTRY_TICKER_ALLOWLIST", raising=False)
    else:
        monkeypatch.setenv("AP_ENTRY_TICKER_ALLOWLIST", allowlist_env)

    mc = _mc()
    queue._dispatch(
        11,
        "client-a",
        f"sig-{ticker}",
        {"ticker": ticker, "score": 90, "side": "CALL"},
        master_control=mc,
        contract_selector=None,
        order_state_machine=object(),
        entry_watcher=object(),
    )
    return mark_calls, rejection_logs, mc.reached


def test_gate_inactive_when_env_unset(monkeypatch):
    mark_calls, rejection_logs, reached_mc = _run_dispatch(monkeypatch, "NVDA", None)
    assert reached_mc, "with env unset, dispatch must proceed past the gate"
    assert not any(
        (kw.get("result") or {}).get("stage") == "entry_ticker_allowlist"
        for _, kw in mark_calls
    )


def test_rejects_ticker_outside_allowlist(monkeypatch):
    mark_calls, rejection_logs, reached_mc = _run_dispatch(
        monkeypatch, "NVDA", "SPY,QQQ,IWM"
    )
    assert not reached_mc, "Master Control must never see a non-allowlisted ticker"
    assert mark_calls, "_mark_job was not invoked"
    args, kwargs = mark_calls[0]
    assert args[0] == 11
    assert args[1] == "REJECTED"
    assert kwargs["error"] == "entry_ticker_allowlist:TICKER_NOT_IN_ALLOWLIST"
    assert kwargs["result"]["stage"] == "entry_ticker_allowlist"
    assert kwargs["result"]["reason_code"] == "TICKER_NOT_IN_ALLOWLIST"
    assert kwargs["result"]["allowlist"] == ["IWM", "QQQ", "SPY"]

    assert rejection_logs, "rejection ledger row missing — reject must be LOUD"
    rl = rejection_logs[0]
    assert rl["stage"] == "entry_ticker_allowlist"
    assert rl["reason_code"] == "TICKER_NOT_IN_ALLOWLIST"
    assert "NVDA" in rl["human_reason"]


def test_allows_ticker_inside_allowlist(monkeypatch):
    _, rejection_logs, reached_mc = _run_dispatch(monkeypatch, "SPY", "SPY,QQQ,IWM")
    assert reached_mc, "allowlisted ticker must proceed past the gate"
    assert not rejection_logs


def test_index_alias_normalizes_to_allowlisted_proxy(monkeypatch):
    _, rejection_logs, reached_mc = _run_dispatch(monkeypatch, "^GSPC", "SPY,QQQ,IWM")
    assert reached_mc, "^GSPC maps to SPY and must pass an allowlist containing SPY"
    assert not rejection_logs


def test_allowlist_is_case_and_whitespace_tolerant(monkeypatch):
    _, _, reached_mc = _run_dispatch(monkeypatch, "qqq", " spy , qqq ,iwm ")
    assert reached_mc, "case/whitespace variants must normalize"
