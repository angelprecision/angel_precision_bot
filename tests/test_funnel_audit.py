"""
tests/test_funnel_audit.py
==========================
PR89 — unit tests for ap/funnel_audit.

These tests run without any database. They exercise:

  * Drop-reason normalization vocabulary (all 21 canonical reasons).
  * Window resolution + default to market-open-today.
  * build_funnel_report() with monkeypatched _safe_select / _safe_select_all
    so we feed deterministic synthetic data and assert the shape and the
    deterministic action-item output.

All assertions are against the same READ-ONLY funnel — no mutation paths
are tested because none exist (by design).
"""

from __future__ import annotations

import importlib
from datetime import datetime, timezone

import pytest


@pytest.fixture(autouse=True)
def _reload_module():
    """Reload funnel_audit before each test so monkeypatches don't leak."""
    import ap.funnel_audit as fa
    importlib.reload(fa)
    return fa


# ─────────────────────────────────────────────────────────────────────────────
# Drop-reason normalization
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("score_too_low",                       "SCORE_TOO_LOW"),
    ("IV_ZONE_SCORE_TOO_LOW",               "SCORE_TOO_LOW"),
    ("Score below threshold 0.65",          "SCORE_TOO_LOW"),
    ("client_quality_gate blocked",         "QUALITY_GATE_BLOCK"),
    ("hybrid_gate failed",                  "QUALITY_GATE_BLOCK"),
    ("client_not_active:PAUSED",            "CLIENT_NOT_APPROVED"),
    ("authorization_required",              "AUTHORIZATION_REQUIRED"),
    ("kill_switch on",                      "KILL_SWITCH"),
    ("entries_paused=true",                 "ENTRIES_PAUSED"),
    ("insufficient_buying_power $0",        "BUYING_POWER"),
    ("daily_cap reached 5/5",               "DAILY_CAP"),
    ("lane_cap exceeded",                   "LANE_CAP"),
    ("same_symbol_cap=1",                   "SAME_SYMBOL_CAP"),
    ("max_positions reached",               "CAPITAL_CAP"),
    ("contract_selector returned no_strike", "CONTRACT_SELECTOR_FAILED"),
    ("spread_too_wide 14%",                 "SPREAD_TOO_WIDE"),
    ("quote_stale age=120s",                "QUOTE_STALE"),
    ("entry_confirmation_failed",           "ENTRY_CONFIRMATION_FAILED"),
    ("watcher_invalidated",                 "WATCHER_INVALIDATED"),
    ("broker_rejected order",               "BROKER_REJECTED"),
    ("broker_no_fill order timed out",      "BROKER_NO_FILL"),
    ("fill_integrity unproven",             "FILL_INTEGRITY_UNPROVEN"),
    ("pod_delivery missing for pod-2",      "POD_DELIVERY_MISSING"),
    ("opportunity_row_missing",             "OPPORTUNITY_ROW_MISSING"),
    ("",                                    "UNKNOWN"),
    (None,                                  "UNKNOWN"),
    ("some_brand_new_string",               "UNKNOWN"),
])
def test_normalize_drop_reason(_reload_module, raw, expected):
    assert _reload_module.normalize_drop_reason(raw) == expected


def test_all_normalized_reasons_present(_reload_module):
    # PR89 §3 requires these 21 canonical reasons to all exist.
    required = {
        "SCORE_TOO_LOW", "QUALITY_GATE_BLOCK", "CLIENT_NOT_APPROVED",
        "AUTHORIZATION_REQUIRED", "KILL_SWITCH", "ENTRIES_PAUSED",
        "BUYING_POWER", "CAPITAL_CAP", "DAILY_CAP", "LANE_CAP",
        "SAME_SYMBOL_CAP", "CONTRACT_SELECTOR_FAILED", "SPREAD_TOO_WIDE",
        "QUOTE_STALE", "ENTRY_CONFIRMATION_FAILED", "WATCHER_INVALIDATED",
        "BROKER_REJECTED", "BROKER_NO_FILL", "FILL_INTEGRITY_UNPROVEN",
        "POD_DELIVERY_MISSING", "OPPORTUNITY_ROW_MISSING", "UNKNOWN",
    }
    assert required.issubset(set(_reload_module.NORMALIZED_REASONS))


# ─────────────────────────────────────────────────────────────────────────────
# Window resolution
# ─────────────────────────────────────────────────────────────────────────────

def test_resolve_window_defaults_to_today_market_open(_reload_module):
    start, end = _reload_module.resolve_window(None, None)
    assert start.tzinfo is not None and end.tzinfo is not None
    assert start < end
    # The window should be at most ~16h wide (overnight + premarket safety).
    assert (end - start).total_seconds() < 24 * 3600


def test_resolve_window_parses_iso(_reload_module):
    start, end = _reload_module.resolve_window(
        "2026-06-06T13:30:00+00:00", "2026-06-06T20:00:00+00:00",
    )
    assert start == datetime(2026, 6, 6, 13, 30, tzinfo=timezone.utc)
    assert end   == datetime(2026, 6, 6, 20, 0,  tzinfo=timezone.utc)


def test_resolve_window_clamps_inverted_range(_reload_module):
    start, end = _reload_module.resolve_window(
        "2026-06-06T20:00:00+00:00", "2026-06-06T13:30:00+00:00",
    )
    assert end > start


# ─────────────────────────────────────────────────────────────────────────────
# build_funnel_report — partial-data tolerance
# ─────────────────────────────────────────────────────────────────────────────

def test_report_partial_data_tolerance(_reload_module, monkeypatch):
    """When ALL sources are missing, report still returns ok=True with
    data_quality warnings (the operator must always see a structured
    response, never an exception)."""
    monkeypatch.setattr(_reload_module, "_sb_client", lambda: None)
    report = _reload_module.build_funnel_report()
    assert report["ok"] is True
    assert isinstance(report["data_quality"], list)
    # Every source should have raised a supabase_unavailable warning.
    assert any("supabase_unavailable" in w for w in report["data_quality"])
    # Funnel still has all 11 steps.
    assert [step["step"] for step in report["funnel"]] == list(
        _reload_module.FUNNEL_STEPS
    )
    # All counts zero, but no exception.
    assert all(s["count"] == 0 for s in report["funnel"])
    # Action items always have at least one entry.
    assert len(report["action_items"]) >= 1


# ─────────────────────────────────────────────────────────────────────────────
# build_funnel_report — synthetic full lifecycle
# ─────────────────────────────────────────────────────────────────────────────

def test_report_full_lifecycle_synthetic(_reload_module, monkeypatch):
    """Feed a synthetic 22-signal / 3-client day and assert the deterministic
    funnel + action items match the PR89 spec examples."""
    fa = _reload_module

    # 22 scanner signals, all intraday. 0 pass quality gates → trigger the
    # "Intraday scanner generated 22 signals but 0 passed quality gates" item.
    signals_intraday = [
        {
            "signal_id":       f"sig-{i:02d}",
            "client_email":    "main@angel",
            "ticker":          "AAPL",
            "pattern":         "1m_breakout",
            "timeframe":       "1m",
            "side":            "CALL",
            "score":           50.0,
            "tier":            "C",
            "decision_status": "rejected",
            "context_notes":   "score_too_low",
            "created_at":      "2026-06-06T14:00:00+00:00",
        }
        for i in range(22)
    ]
    # 4 daily signals, 3 fill → triggers BEST_SCANNER item.
    signals_daily = [
        {
            "signal_id":       f"d-{i}",
            "client_email":    "main@angel",
            "ticker":          "UNH",
            "pattern":         "daily_232",
            "timeframe":       "1d",
            "side":            "CALL",
            "score":           82.5,
            "tier":            "A",
            "decision_status": "ARMED",
            "context_notes":   None,
            "created_at":      "2026-06-06T14:30:00+00:00",
        }
        for i in range(4)
    ]
    signals = signals_intraday + signals_daily

    members = [
        {"email": "main@angel",  "execution_pod": "pod-1",
         "approved": True, "subscription_active": True, "allow_live_trading": True},
        {"email": "jason@angel", "execution_pod": "pod-2",
         "approved": True, "subscription_active": True, "allow_live_trading": True},
        {"email": "jose@angel",  "execution_pod": "pod-1",
         "approved": True, "subscription_active": True, "allow_live_trading": True},
    ]

    # 4 daily signals × 3 clients = 12 opportunity rows.
    opps = []
    for i in range(4):
        for j, m in enumerate(members):
            # Jason gets blocked by buying_power 4 times → CLIENT_PREFLIGHT_BLOCKS item.
            if m["email"] == "jason@angel":
                status = "CLIENT_SKIPPED"
                miss   = "insufficient_buying_power"
            elif m["email"] == "main@angel":
                status = "FILLED" if i < 3 else "BROKER_SUBMITTED"
                miss   = None
            else:
                status = "FILLED" if i < 1 else "BROKER_SUBMITTED"
                miss   = None
            opps.append({
                "canonical_signal_id":   f"d-{i}",
                "signal_id":             f"d-{i}",
                "client_id":             m["email"],
                "symbol":                "UNH",
                "direction":             "CALL",
                "timeframe":             "1d",
                "pattern":               "daily_232",
                "score":                 82.5,
                "tier":                  "A",
                "scanner_type":          "daily",
                "scanner_name":          "daily_232",
                "opportunity_status":    status,
                "miss_stage":            None,
                "miss_reason":           miss,
                "would_block_reason":    miss,
                "order_local_id":        f"o-{i}-{j}" if status != "CLIENT_SKIPPED" else None,
                "broker_order_id":       f"b-{i}-{j}" if status in ("FILLED", "BROKER_SUBMITTED") else None,
                "created_at":            "2026-06-06T14:30:00+00:00",
                "updated_at":            "2026-06-06T14:35:00+00:00",
                "preflight_enforced":    True,
                "execution_continued":   status != "CLIENT_SKIPPED",
            })

    orders = []
    for op in opps:
        if op["order_local_id"]:
            orders.append({
                "local_order_id":      op["order_local_id"],
                "client_id":           op["client_id"],
                "kind":                "ENTRY",
                "status":              "FILLED" if op["opportunity_status"] == "FILLED" else "SUBMITTED",
                "broker_order_id":     op["broker_order_id"],
                "symbol":              op["symbol"],
                "contract":            "UNH 240614C500",
                "qty":                 1,
                "limit_price":         5.50,
                "fill_price":          5.45,
                "filled_qty":          1 if op["opportunity_status"] == "FILLED" else 0,
                "score":               op["score"],
                "tier":                op["tier"],
                "pattern":             op["pattern"],
                "timeframe":           op["timeframe"],
                "direction":           op["direction"],
                "signal_id":           op["signal_id"],
                "canonical_signal_id": op["canonical_signal_id"],
                "last_error":          None,
                "created_ts":          "2026-06-06T14:30:00+00:00",
                "updated_ts":          "2026-06-06T14:35:00+00:00",
                "meta":                {"mode": "live"},
            })

    proofs = []

    # Patch the safe-select layer to return our synthetic data.
    def fake_safe_select(table, sb, *, start_iso, end_iso, ts_col,
                        columns="*", page=2000):
        return {
            "ap_signals":                  (signals, None),
            "orders":                       (orders, None),
            "client_signal_opportunities":  (opps,   None),
            "proof_trades":                 (proofs, None),
        }.get(table, ([], f"{table}:not_mocked"))

    def fake_safe_select_all(table, sb, *, columns="*", filters=None):
        return {"members": (members, None)}.get(table, ([], f"{table}:not_mocked"))

    monkeypatch.setattr(fa, "_safe_select", fake_safe_select)
    monkeypatch.setattr(fa, "_safe_select_all", fake_safe_select_all)
    monkeypatch.setattr(fa, "_sb_client", lambda: object())  # truthy

    report = fa.build_funnel_report(
        start="2026-06-06T13:30:00+00:00",
        end="2026-06-06T20:00:00+00:00",
    )

    assert report["ok"] is True
    s = report["summary"]
    # 22 rejected intraday + 4 eligible daily = 26 scanner_signals_total.
    assert s["scanner_signals_total"] == 26
    # Only 4 passed gates (the daily ones).
    assert s["client_eligible_signals"] == 4
    assert s["active_clients"] == 3
    # 4 signals × 3 clients = 12 expected opportunities, 12 actually created.
    assert s["expected_client_opportunities"] == 12
    assert s["opportunity_rows_created"] == 12
    # main filled 3, jose filled 1 → 4 fills.
    assert s["filled"] == 4
    # jason blocked 4 times.
    blocks = [c for c in report["client_breakdown"] if c["client_id"] == "jason@angel"][0]
    assert blocks["preflight_blocks"] == 4
    assert blocks["top_block_reason"] == "BUYING_POWER"

    codes = {item["code"] for item in report["action_items"]}
    # Must explicitly call out: (a) buying-power blocks on jason,
    # (b) scanner output → 0 passed-quality, (c) best scanner.
    assert "CLIENT_PREFLIGHT_BLOCKS" in codes
    assert "BEST_SCANNER" in codes

    # Funnel ordering preserved.
    assert [f["step"] for f in report["funnel"]] == list(fa.FUNNEL_STEPS)
