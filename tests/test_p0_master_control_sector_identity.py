"""Behavioral proof for degraded sector-identity admission.

These tests execute the Master Control helpers and both sector-cap call paths.
They deliberately use canonical-known tickers, unrelated unknown tickers, and
tight independent caps so a hidden "other" fallback cannot pass by accident.

Canonical executable-underlying coverage is supplied by dependency #589 on
main. These tests consume that one map authority and prove incomplete sector
metadata cannot become a portfolio-wide tradeflow kill switch or silently
disable sector protection for known names.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import ap_master_control as mc_mod


@pytest.fixture(autouse=True)
def _disable_decision_event_db_writes(monkeypatch):
    monkeypatch.setattr(mc_mod, "emit_decision_event", None)


def _bare_control():
    return mc_mod.APMasterControl.__new__(mc_mod.APMasterControl)


def _position(ticker: str, *, price: float = 2.0, quantity: int = 1) -> dict:
    return {
        "underlying": ticker,
        "avg_fill": price,
        "quantity_remaining": quantity,
    }


def _snapshot(
    open_positions=None,
    closing_positions=None,
    *,
    capital_deployed: float = 0.0,
) -> dict:
    return {
        "_snapshot_ok": True,
        "_snapshot_ts": "2026-08-31T00:00:00Z",
        "_snapshot_age_sec": 0.1,
        "open_count": len(open_positions or []),
        "pending_entries": 0,
        "capital_deployed": capital_deployed,
        "position_capital_deployed": capital_deployed,
        "calls_open": 0,
        "puts_open": 0,
        "filled_unreconciled_calls": 0,
        "filled_unreconciled_puts": 0,
        "trades_today": 0,
        "total_trades": 0,
        "realized_pnl_today": 0.0,
        "trade_count_query_status": "ok",
        "watcher_count": 0,
        "entry_attempt_lock_count": 0,
        "open_positions": open_positions or [],
        "closing_positions": closing_positions or [],
    }


def _control(**overrides):
    kwargs = {
        "mode": "paper",
        "score_floor": 60.0,
        "context_floor": 0.0,
        "account_equity": 5000.0,
        "max_capital_pct": 0.10,
        "max_sector_pct": 0.25,
        "max_ticker_pct": 0.10,
        "max_daily_loss": -500.0,
        "pending_capital_fail_closed_live": False,
        "require_snapshot_freshness_live": False,
    }
    kwargs.update(overrides)
    with patch.object(mc_mod.APMasterControl, "_seed_dedup_from_db", return_value=None):
        control = mc_mod.APMasterControl(**kwargs)
    control._kill_switch_fn = lambda: False
    control._equity_snapshot = MagicMock(
        return_value=(kwargs["account_equity"], kwargs["max_daily_loss"])
    )
    control._pending_capital_from_snapshot_or_db = MagicMock(return_value=0.0)
    control._get_pending_capital_breakdown = MagicMock(
        return_value={
            "pending_submitted_entry_exposure": 0.0,
            "filled_unreconciled_exposure": 0.0,
            "pending_total_capital_reserved": 0.0,
        }
    )
    control._log_capital_utilization = MagicMock()
    return control


def _plan(
    ticker: str,
    cost: float,
    *,
    contracts: int = 1,
) -> mc_mod.ApprovedExecutionPlan:
    return mc_mod.ApprovedExecutionPlan(
        plan_id="plan-548",
        signal_id="signal-548",
        client_id="client@example.com",
        ticker=ticker,
        side="CALL",
        direction="CALL",
        pattern="BREAKOUT",
        timeframe="1d",
        contracts=contracts,
        max_position_usd=cost,
        tier="A",
        score=80.0,
        intel_score=0.0,
        confidence_bucket="standard_pool",
        trigger_type="breach",
        trigger_price=100.0,
        stop_underlying=99.0,
        target_underlying=105.0,
        metadata={"local_order_id": "local-548"},
        mode="PAPER",
        paper_sim=True,
    )


def _signal(ticker: str, signal_id: str = "signal-548-evaluate") -> dict:
    return {
        "signal_id": signal_id,
        "ticker": ticker,
        "side": "CALL",
        "direction": "CALL",
        "score": 80.0,
        "underlying_price": 100.0,
        "timeframe": "1d",
        "pattern": "BREAKOUT",
        "score_breakdown": {"real_time_ctx": 10.0},
        "trigger": {"entry": 100.0, "stop": 99.0, "pt1": 105.0},
    }


def _prepare_evaluate(control, snapshot, *, duplicate=None):
    control._get_snapshot = MagicMock(return_value=snapshot)
    control._has_durable_duplicate_signal = MagicMock(
        return_value=duplicate if duplicate is not None else (False, "", "")
    )
    control._persist_dedup = MagicMock()
    control._run_intelligence = MagicMock(
        return_value={
            "approved": True,
            "score": 0.0,
            "contracts": 1,
            "reasoning": "test",
            "_available": False,
        }
    )
    control._run_final_quality_gates = MagicMock(return_value=None)
    control._emit_trade_dossier = MagicMock()


@pytest.mark.parametrize(
    ("symbol", "expected"),
    [
        (" qcom ", "tech"),
        ("TSLA", "auto"),
        ("SPY", "index"),
        ("QQQ", "index"),
        ("BMY", "healthcare"),
        ("NEE", "utilities"),
        ("PEP", "consumer"),
        ("ZZUNKNOWN1", None),
        ("", None),
        (None, None),
        ({"not": "a symbol"}, None),
    ],
)
def test_resolver_uses_canonical_identity_and_normalizes(symbol, expected):
    assert mc_mod.APMasterControl._resolve_sector(symbol) == expected


@pytest.mark.parametrize(
    "resolved",
    ["OTHER", " unknown ", "MISC", "UNMAPPED", {"sector": "tech"}, None],
)
def test_resolver_rejects_non_authoritative_canonical_output(monkeypatch, resolved):
    monkeypatch.setattr(mc_mod, "_canonical_get_sector", lambda _symbol: resolved)
    assert mc_mod.APMasterControl._resolve_sector("ZZRESOLVER") is None


def test_canonical_known_positions_are_counted_together():
    control = _bare_control()
    positions = [_position("AAPL"), _position("QCOM")]
    assert control._sector_capital_deployed(positions, "tech") == 400.0


def test_unknown_positions_never_become_a_risk_sector():
    control = _bare_control()
    positions = [_position("ZZUNKNOWN1", price=5.0), _position("ZZUNKNOWN2", price=6.0)]

    assert control._sector_capital_deployed(positions, None) == 0.0
    assert control._sector_capital_deployed(positions, "sector_unknown") == 0.0


@pytest.mark.parametrize(
    ("existing", "candidate", "sector"),
    [
        ("UNH", "BMY", "healthcare"),
        ("WMT", "PEP", "consumer"),
        ("AAPL", "QCOM", "tech"),
        ("T", "VZ", "communication"),
        ("COIN", "MSTR", "crypto"),
        ("AMT", "SPG", "real_estate"),
        ("LIN", "DOW", "materials"),
        ("SPY", "SPX", "index"),
    ],
)
def test_all_required_known_sector_pairs_aggregate(existing, candidate, sector):
    control = _bare_control()
    assert control._resolve_sector(candidate) == sector
    assert control._sector_capital_deployed(
        [_position(existing)],
        sector,
    ) == 200.0


@pytest.mark.parametrize(
    ("existing", "candidate"),
    [
        ("QQQ", "BMY"),
        ("QQQ", "NEE"),
        ("CSCO", "KO"),
        ("T", "AAPL"),
        ("AMT", "JPM"),
        ("COIN", "NVDA"),
    ],
)
def test_required_cross_sector_pairs_do_not_aggregate(existing, candidate):
    control = _bare_control()
    candidate_sector = control._resolve_sector(candidate)
    assert candidate_sector is not None
    assert control._sector_capital_deployed(
        [_position(existing)],
        candidate_sector,
    ) == 0.0


@pytest.mark.parametrize(
    ("existing", "candidate", "sector"),
    [
        ("UNH", "BMY", "healthcare"),
        ("WMT", "PEP", "consumer"),
        ("AAPL", "QCOM", "tech"),
        ("T", "VZ", "communication"),
        ("COIN", "MSTR", "crypto"),
        ("AMT", "SPG", "real_estate"),
        ("LIN", "DOW", "materials"),
        ("SPY", "SPX", "index"),
    ],
)
def test_required_known_pairs_saturate_only_their_sector_cap(existing, candidate, sector):
    control = _control(
        account_equity=1000.0,
        max_capital_pct=0.90,
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.15,
    )
    control._get_snapshot = MagicMock(
        return_value=_snapshot([_position(existing, price=1.0)], capital_deployed=100.0)
    )

    decision = control.revalidate_exposure(_plan(candidate, 100.0))

    assert decision.ok is False
    assert f"revalidate_sector_cap_{sector}" in decision.reason


@pytest.mark.parametrize(
    ("existing", "candidate"),
    [
        ("QQQ", "BMY"),
        ("QQQ", "NEE"),
        ("CSCO", "KO"),
        ("T", "AAPL"),
        ("AMT", "JPM"),
        ("COIN", "NVDA"),
    ],
)
def test_required_cross_sector_pairs_do_not_trigger_sector_cap(existing, candidate):
    control = _control(
        account_equity=1000.0,
        max_capital_pct=0.90,
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.15,
    )
    control._get_snapshot = MagicMock(
        return_value=_snapshot([_position(existing, price=1.0)], capital_deployed=100.0)
    )

    decision = control.revalidate_exposure(_plan(candidate, 100.0))

    assert decision.ok is True
    assert "sector_cap" not in decision.reason


@pytest.mark.parametrize("mode", ["paper", "live"])
def test_unknown_candidate_revalidation_skips_only_sector_cap_in_each_mode(mode):
    control = _control(
        mode=mode,
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.01,
    )

    control._get_snapshot = MagicMock(return_value=_snapshot())
    plan = _plan("ZZUNKNOWN1", 100.0)

    decision = control.revalidate_exposure(plan)

    assert decision.ok is True, decision.reason
    assert decision.reason_code == ""
    assert plan.metadata["candidate_sector_resolved"] is False
    assert plan.metadata["sector_identity_complete"] is False
    assert plan.metadata["sector_cap_applied"] is False
    assert plan.metadata["sector_cap_partial"] is False
    assert plan.metadata["sector_cap_skip_reason"] == "candidate_sector_unresolved"
    assert plan.metadata["unresolved_sector_positions"] == []


@pytest.mark.parametrize("mode", ["paper", "live"])
def test_unknown_candidate_evaluate_reaches_downstream_admission_in_each_mode(
    mode,
    monkeypatch,
):
    control = _control(
        mode=mode,
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.01,
    )
    snapshot = _snapshot()
    _prepare_evaluate(control, snapshot)
    events = []
    monkeypatch.setattr(
        mc_mod,
        "emit_decision_event",
        lambda **kwargs: events.append(kwargs),
    )

    decision = control.evaluate(
        _signal("ZZUNKNOWN1", f"signal-548-mode-{mode}"),
        client_id="client@example.com",
    )

    assert decision.ok is True, decision.reason
    assert decision.plan is not None
    assert control._run_final_quality_gates.called
    metadata = decision.plan.metadata
    assert metadata["candidate_sector_resolved"] is False
    assert metadata["sector_identity_complete"] is False
    assert metadata["sector_cap_applied"] is False
    assert metadata["sector_cap_skip_reason"] == "candidate_sector_unresolved"
    assert metadata["unresolved_sector_positions"] == []
    approval = [event for event in events if event.get("decision") == "APPROVE"][-1]
    assert approval["context"]["candidate_sector_resolved"] is False
    assert approval["context"]["sector_cap_applied"] is False
    assert approval["context"]["sector_cap_skip_reason"] == "candidate_sector_unresolved"


def test_reporting_keeps_unresolved_names_separate_without_shared_sector():
    control = _bare_control()
    exposure = control.get_sector_exposure(
        [_position("ZZUNKNOWN1", price=5.0), _position("ZZUNKNOWN2", price=6.0), _position("AAPL")]
    )

    assert "other" not in exposure
    assert "sector_unknown" not in exposure
    assert exposure["sector_identity_unproven:ZZUNKNOWN1"] == 500.0
    assert exposure["sector_identity_unproven:ZZUNKNOWN2"] == 600.0
    assert exposure["tech"] == 200.0


def test_unknown_candidate_revalidation_still_runs_other_cap_math():
    control = _control(
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.01,
    )
    control._get_snapshot = MagicMock(
        return_value=_snapshot([_position("ZZUNKNOWN2", price=6.0)])
    )
    plan = _plan("ZZUNKNOWN1", 100.0)

    decision = control.revalidate_exposure(plan)

    assert decision.ok is True, decision.reason
    assert plan.metadata["candidate_sector_resolved"] is False
    assert plan.metadata["sector_cap_applied"] is False
    assert plan.metadata["sector_cap_skip_reason"] == "candidate_sector_unresolved"
    assert [row["symbol"] for row in plan.metadata["unresolved_sector_positions"]] == [
        "ZZUNKNOWN2"
    ]
    control._get_snapshot.assert_called_once()


def test_unknown_candidate_does_not_bypass_total_capital_gate():
    control = _control(
        account_equity=1000.0,
        max_capital_pct=0.90,
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.90,
    )
    control._get_snapshot = MagicMock(
        return_value=_snapshot([_position("ZZUNKNOWN2", price=6.0)], capital_deployed=900.0)
    )
    plan = _plan("ZZUNKNOWN1", 200.0)

    decision = control.revalidate_exposure(plan)

    assert decision.ok is False
    assert decision.reason_code in {
        "CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED",
        "ACTUAL_CONTRACT_COST_EXCEEDS_REMAINING_TOTAL_CAPACITY",
    }
    assert "SECTOR_IDENTITY_UNPROVEN" not in decision.reason


def test_known_canonical_sector_still_enforces_sector_cap():
    control = _control(
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.10,
    )
    control._get_snapshot = MagicMock(
        return_value=_snapshot([_position("AAPL", price=4.0)])
    )

    decision = control.revalidate_exposure(_plan("QCOM", 200.0))

    assert decision.ok is False
    assert "revalidate_sector_cap_tech" in decision.reason


def test_sep4_cost_shape_qqq_cannot_block_bmy_sector():
    """Isolate the exact Sep 4 false-sector arithmetic.

    Preserve the real $148 QQQ and $168 BMY costs, but use $5,000 equity and a
    4% sector cap ($200) so independent total/broker capacity has headroom.

    If QQQ were still incorrectly grouped with BMY, projected healthcare would
    be $316 and this test would block. With canonical identity, QQQ is INDEX,
    healthcare deployed is $0, and BMY projects to only $168.

    The fact that Jason's actual stale OPEN QQQ row can independently consume
    total/broker capacity is a separate lifecycle defect owned outside #548.
    """
    control = _control(
        account_equity=5000.0,
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.04,
    )
    qqq = _position("QQQ", price=1.48, quantity=1)
    control._get_snapshot = MagicMock(return_value=_snapshot([qqq], capital_deployed=148.0))

    assert control._sector_capital_deployed([qqq], "healthcare") == 0.0
    decision = control.revalidate_exposure(_plan("BMY", 168.0))

    assert decision.ok is True
    assert "sector_cap" not in decision.reason


def test_sep4_cost_shape_qqq_cannot_block_nee_sector():
    """QQQ INDEX exposure cannot become NEE UTILITIES exposure."""
    control = _control(
        account_equity=5000.0,
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.04,
    )
    qqq = _position("QQQ", price=1.48, quantity=1)
    control._get_snapshot = MagicMock(return_value=_snapshot([qqq], capital_deployed=148.0))

    assert control._sector_capital_deployed([qqq], "utilities") == 0.0
    decision = control.revalidate_exposure(_plan("NEE", 127.0))

    assert decision.ok is True
    assert "sector_cap" not in decision.reason


@pytest.mark.parametrize(
    ("candidate", "existing", "sector"),
    [
        ("BMY", "UNH", "healthcare"),
        ("PEP", "WMT", "consumer"),
    ],
)
def test_sep4_known_names_keep_real_sector_cap(candidate, existing, sector):
    """The throughput repair must not convert known names into cap-free unknowns."""
    control = _control(
        account_equity=1000.0,
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.15,
    )
    control._get_snapshot = MagicMock(
        return_value=_snapshot([_position(existing, price=1.0)], capital_deployed=100.0)
    )

    decision = control.revalidate_exposure(_plan(candidate, 100.0))

    assert decision.ok is False
    assert f"revalidate_sector_cap_{sector}" in decision.reason


def test_sector_telemetry_distinguishes_unknown_from_known():
    control = _bare_control()

    unknown = control._sector_telemetry(None)
    assert unknown["candidate_sector_resolved"] is False
    assert unknown["sector_identity_complete"] is False
    assert unknown["sector_cap_applied"] is False
    assert unknown["sector_cap_partial"] is False
    assert unknown["sector_cap_skip_reason"] == "candidate_sector_unresolved"
    assert unknown["unresolved_sector_positions"] == []

    known = control._sector_telemetry("tech")
    assert known["candidate_sector_resolved"] is True
    assert known["sector_identity_complete"] is True
    assert known["sector_cap_applied"] is True
    assert known["sector_cap_partial"] is False
    assert known["sector_cap_skip_reason"] is None

    partial = control._sector_telemetry(
        "tech",
        unresolved_positions=[{"symbol": "ZZUNKNOWN1"}],
    )
    assert partial["candidate_sector_resolved"] is True
    assert partial["sector_identity_complete"] is False
    assert partial["active_sector_identity_complete"] is False
    assert partial["sector_cap_applied"] is True
    assert partial["sector_cap_partial"] is True
    assert partial["sector_cap_skip_reason"] == "unresolved_active_position_identity"


def test_unresolved_active_position_does_not_bypass_total_capital_gate(monkeypatch):
    control = _control(
        account_equity=1000.0,
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.90,
        max_daily_loss=-500.0,
    )
    snapshot = _snapshot(
        [_position("ZZUNKNOWN1", price=3.0)],
        capital_deployed=800.0,
    )
    _prepare_evaluate(control, snapshot)
    events = []
    monkeypatch.setattr(
        mc_mod,
        "emit_decision_event",
        lambda **kwargs: events.append(kwargs),
    )

    decision = control.evaluate(
        _signal("BMY", "signal-548-capital-with-unresolved"),
        client_id="client@example.com",
    )

    assert decision.ok is False
    assert decision.plan is None
    assert decision.reason_code == "CAPITAL_UTIL_BLOCK"
    assert "SECTOR_IDENTITY_UNPROVEN" not in decision.reason
    rejection = [event for event in events if event.get("decision") == "REJECT"][-1]
    assert rejection["context"]["sector_identity_complete"] is False
    assert rejection["context"]["sector_cap_partial"] is True
    assert rejection["context"]["unresolved_sector_positions"][0]["symbol"] == "ZZUNKNOWN1"


def test_sector_exposure_authority_reports_completeness_and_diagnostics():
    control = _bare_control()

    complete = control._sector_exposure_authority([_position("AAPL")], "tech")
    assert complete == {
        "capital": 200.0,
        "identity_complete": True,
        "unresolved": [],
    }

    cross_sector = control._sector_exposure_authority([_position("QQQ")], "healthcare")
    assert cross_sector == {
        "capital": 0.0,
        "identity_complete": True,
        "unresolved": [],
    }

    incomplete = control._sector_exposure_authority(
        [_position("ZZUNKNOWN1"), _position("ZZUNKNOWN2")],
        "healthcare",
    )
    assert incomplete["capital"] == 0.0
    assert incomplete["identity_complete"] is False
    assert [row["symbol"] for row in incomplete["unresolved"]] == [
        "ZZUNKNOWN1",
        "ZZUNKNOWN2",
    ]
    assert all(
        row["reason"] == "SECTOR_IDENTITY_UNPROVEN"
        for row in incomplete["unresolved"]
    )

    partial = control._sector_exposure_authority(
        [_position("AAPL"), _position("ZZUNKNOWN1", price=9.0)],
        "tech",
    )
    assert partial["capital"] == 200.0
    assert partial["identity_complete"] is False
    assert [row["symbol"] for row in partial["unresolved"]] == ["ZZUNKNOWN1"]


@pytest.mark.parametrize("position_bucket", ["open_positions", "closing_positions"])
def test_unresolved_active_position_is_partial_diagnostic_only_in_both_paths(
    position_bucket,
    monkeypatch,
):
    control = _control(
        mode="paper",
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.90,
    )
    snapshot = _snapshot(
        **{position_bucket: [_position("ZZUNKNOWN1", price=3.0)]},
        capital_deployed=300.0,
    )
    _prepare_evaluate(control, snapshot)
    events = []
    monkeypatch.setattr(
        mc_mod,
        "emit_decision_event",
        lambda **kwargs: events.append(kwargs),
    )

    evaluation = control.evaluate(
        _signal("BMY", f"signal-548-unresolved-{position_bucket}"),
        client_id="client@example.com",
    )

    assert evaluation.ok is True, evaluation.reason
    assert evaluation.plan is not None
    assert control._run_final_quality_gates.called
    metadata = evaluation.plan.metadata
    assert metadata["candidate_sector_resolved"] is True
    assert metadata["sector_identity_complete"] is False
    assert metadata["sector_cap_applied"] is True
    assert metadata["sector_cap_partial"] is True
    assert metadata["sector_cap_skip_reason"] == "unresolved_active_position_identity"
    assert metadata["unresolved_sector_positions"][0]["symbol"] == "ZZUNKNOWN1"

    # Use the actual plan passed through revalidation so its fresh diagnostic
    # snapshot is observable without treating a revalidation success as an
    # execution mutation.
    revalidated_plan = _plan("BMY", 150.0)
    revalidation = control.revalidate_exposure(revalidated_plan)
    assert revalidation.ok is True, revalidation.reason
    assert revalidated_plan.metadata["sector_identity_complete"] is False
    assert revalidated_plan.metadata["sector_cap_partial"] is True
    assert revalidated_plan.metadata["sector_cap_skip_reason"] == "unresolved_active_position_identity"
    assert revalidated_plan.metadata["unresolved_sector_positions"][0]["symbol"] == "ZZUNKNOWN1"
    log_kwargs = control._log_capital_utilization.call_args.kwargs
    assert log_kwargs["sector_identity_complete"] is False
    assert log_kwargs["sector_cap_partial"] is True
    assert log_kwargs["unresolved_sector_positions"][0]["symbol"] == "ZZUNKNOWN1"

    approval = [event for event in events if event.get("decision") == "APPROVE"][-1]
    assert approval["context"]["sector_identity_complete"] is False
    assert approval["context"]["sector_cap_partial"] is True


@pytest.mark.parametrize(
    ("bad_position", "expected_symbol"),
    [
        ({"avg_fill": 3.0, "quantity_remaining": 1}, "<missing>"),
        ({"underlying": "   ", "avg_fill": 3.0, "quantity_remaining": 1}, "<missing>"),
        ({"underlying": {"not": "a symbol"}, "avg_fill": 3.0, "quantity_remaining": 1}, "{'NOT': 'A SYMBOL'}"),
        ("legacy-position-row", "<invalid_position>"),
        (_position("ZZUNKNOWN1", price=3.0), "ZZUNKNOWN1"),
    ],
)
def test_malformed_or_unmapped_active_identity_does_not_halt_tradeflow(
    bad_position,
    expected_symbol,
    monkeypatch,
):
    control = _control(
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.90,
    )
    control._get_snapshot = MagicMock(
        return_value=_snapshot([bad_position], capital_deployed=300.0)
    )

    authority = control._sector_exposure_authority([bad_position], "healthcare")
    assert authority["identity_complete"] is False
    assert authority["unresolved"][0]["symbol"] == expected_symbol

    _prepare_evaluate(
        control,
        _snapshot([bad_position], capital_deployed=300.0),
    )
    monkeypatch.setattr(mc_mod, "emit_decision_event", None)
    evaluation = control.evaluate(
        _signal("BMY", f"signal-548-malformed-{expected_symbol}"),
        client_id="client@example.com",
    )
    assert evaluation.ok is True, evaluation.reason
    assert evaluation.plan.metadata["unresolved_sector_positions"][0]["symbol"] == expected_symbol

    revalidated_plan = _plan("BMY", 150.0)
    decision = control.revalidate_exposure(revalidated_plan)

    assert decision.ok is True, decision.reason
    assert revalidated_plan.metadata["unresolved_sector_positions"][0]["symbol"] == expected_symbol


def test_revalidate_resolver_error_is_partial_sector_diagnostic(monkeypatch):
    original_resolver = mc_mod._canonical_get_sector

    def raising_resolver(symbol):
        if str(symbol).strip().upper() == "ZZUNKNOWN1":
            raise RuntimeError("resolver unavailable")
        return original_resolver(symbol)

    monkeypatch.setattr(mc_mod, "_canonical_get_sector", raising_resolver)
    control = _control(
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.90,
    )
    control._get_snapshot = MagicMock(
        return_value=_snapshot([_position("ZZUNKNOWN1", price=3.0)], capital_deployed=300.0)
    )

    plan = _plan("BMY", 150.0)
    decision = control.revalidate_exposure(plan)

    assert decision.ok is True, decision.reason
    assert plan.metadata["sector_identity_complete"] is False
    assert plan.metadata["sector_cap_partial"] is True
    assert plan.metadata["sector_cap_skip_reason"] == "unresolved_active_position_identity"
    assert plan.metadata["unresolved_sector_positions"][0]["symbol"] == "ZZUNKNOWN1"


@pytest.mark.parametrize("mode", ["paper", "live"])
def test_known_candidate_with_unresolved_active_position_degrades_identically_in_both_modes(mode):
    control = _control(
        mode=mode,
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.90,
    )
    control._get_snapshot = MagicMock(
        return_value=_snapshot([_position("ZZUNKNOWN1", price=3.0)], capital_deployed=300.0)
    )

    plan = _plan("BMY", 150.0)
    decision = control.revalidate_exposure(plan)

    assert decision.ok is True, decision.reason
    assert plan.metadata["candidate_sector_resolved"] is True
    assert plan.metadata["sector_identity_complete"] is False
    assert plan.metadata["sector_cap_partial"] is True
    assert plan.metadata["sector_cap_skip_reason"] == "unresolved_active_position_identity"
    assert plan.metadata["unresolved_sector_positions"][0]["symbol"] == "ZZUNKNOWN1"


def test_evaluate_known_candidate_with_unresolved_open_position_reaches_approval():
    control = _control(
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.90,
    )
    _prepare_evaluate(
        control,
        _snapshot([_position("ZZUNKNOWN1", price=3.0)], capital_deployed=300.0),
    )
    decision = control.evaluate(
        _signal("BMY", "signal-548-existing-unknown"),
        client_id="client@example.com",
    )

    assert decision.ok is True, decision.reason
    assert decision.plan is not None
    assert decision.plan.metadata["unresolved_sector_positions"][0]["symbol"] == "ZZUNKNOWN1"
    assert decision.plan.metadata["sector_cap_partial"] is True
    assert decision.plan.metadata["sector_cap_skip_reason"] == "unresolved_active_position_identity"
    control._run_intelligence.assert_called_once()
    control._run_final_quality_gates.assert_called_once()


def test_duplicate_gate_remains_authoritative_when_sector_metadata_is_incomplete():
    control = _control(
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.90,
    )
    control._has_durable_duplicate_signal = MagicMock(
        return_value=(True, "orders", "id=existing-entry")
    )
    control._get_snapshot = MagicMock(
        side_effect=AssertionError("duplicate protection should stop before snapshot")
    )

    decision = control.evaluate(
        _signal("BMY", "signal-548-duplicate-with-unresolved"),
        client_id="client@example.com",
    )

    assert decision.ok is False
    assert decision.reason_code == "DEDUP_BLOCK"
    assert "SECTOR_IDENTITY_UNPROVEN" not in decision.reason
    control._get_snapshot.assert_not_called()


def test_partial_sector_cap_still_blocks_when_proven_sector_exceeds_cap():
    control = _control(
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.15,
        account_equity=1000.0,
    )
    control._get_snapshot = MagicMock(
        return_value=_snapshot(
            [
                _position("AAPL", price=1.0),
                _position("ZZUNKNOWN1", price=9.0),
            ],
            capital_deployed=100.0,
        )
    )
    plan = _plan("QCOM", 100.0)

    decision = control.revalidate_exposure(plan)

    assert decision.ok is False
    assert decision.reason_code == "SECTOR_CAP_BLOCK"
    assert "revalidate_sector_cap_tech" in decision.reason
    assert plan.metadata["sector_cap_applied"] is True
    assert plan.metadata["sector_cap_partial"] is True
    assert plan.metadata["unresolved_sector_positions"][0]["symbol"] == "ZZUNKNOWN1"
