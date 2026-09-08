"""Behavioral proof for the surgical #548 sector-identity repair.

These tests execute the Master Control helpers and both sector-cap call paths.
They deliberately use canonical-known tickers, unrelated unknown tickers, and
tight independent caps so a hidden "other" fallback cannot pass by accident.

Canonical executable-underlying coverage is supplied by dependency #589 on
main. These tests consume that one map authority and prove Master Control
cannot silently disable sector protection for known names.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import ap_master_control as mc_mod


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
def test_unknown_revalidation_has_same_fail_closed_result_in_each_mode(mode):
    control = _control(
        mode=mode,
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.90,
    )

    decision = control.revalidate_exposure(_plan("ZZUNKNOWN1", 100.0))

    assert (decision.ok, decision.reason, decision.reason_code) == (
        False,
        "SECTOR_IDENTITY_UNPROVEN",
        "SECTOR_IDENTITY_UNPROVEN",
    )

@pytest.mark.parametrize("mode", ["paper", "live"])
def test_unknown_evaluate_has_same_fail_closed_result_in_each_mode(mode):
    control = _control(mode=mode)
    control._get_snapshot = MagicMock(
        side_effect=AssertionError("identity must fail before snapshot")
    )

    decision = control.evaluate(
        {
            "signal_id": f"signal-548-mode-{mode}",
            "ticker": "ZZUNKNOWN1",
            "side": "CALL",
            "direction": "CALL",
            "score": 80.0,
            "timeframe": "1d",
            "pattern": "BREAKOUT",
            "score_breakdown": {"real_time_ctx": 10.0},
            "trigger": {"entry": 100.0, "stop": 99.0, "pt1": 105.0},
        },
        client_id="client@example.com",
    )

    assert (decision.ok, decision.reason, decision.reason_code) == (
        False,
        "SECTOR_IDENTITY_UNPROVEN",
        "SECTOR_IDENTITY_UNPROVEN",
    )
    control._get_snapshot.assert_not_called()


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


def test_unknown_revalidation_fails_closed_before_cap_math():
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
    before_plan = (plan.contracts, plan.max_position_usd)

    decision = control.revalidate_exposure(plan)

    assert decision.ok is False
    assert decision.reason == "SECTOR_IDENTITY_UNPROVEN"
    assert decision.reason_code == "SECTOR_IDENTITY_UNPROVEN"
    assert plan.contracts == before_plan[0]
    assert plan.max_position_usd == before_plan[1]
    control._get_snapshot.assert_not_called()


def test_unknown_identity_precedes_ticker_total_and_broker_cap_gates():
    """Independent capacity headroom cannot substitute for risk identity."""
    control = _control(
        max_capital_pct=0.90,
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.90,
    )
    control._get_snapshot = MagicMock(
        side_effect=AssertionError("identity must fail before capital reads")
    )

    decision = control.revalidate_exposure(_plan("ZZUNKNOWN1", 100.0))

    assert decision.ok is False
    assert decision.reason == "SECTOR_IDENTITY_UNPROVEN"
    assert decision.reason_code == "SECTOR_IDENTITY_UNPROVEN"
    control._get_snapshot.assert_not_called()


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

    assert control._sector_telemetry(None) == {
        "sector_resolution": "unknown",
        "sector_cap_applied": False,
        "sector_cap_skip_reason": "unknown_sector_identity",
    }
    assert control._sector_telemetry("tech") == {
        "sector_resolution": "known",
        "sector_cap_applied": True,
        "sector_cap_skip_reason": None,
    }


def test_evaluate_unknown_sector_fails_closed_before_other_cap_gates(monkeypatch):
    monkeypatch.setenv("ENABLE_TRADE_DOSSIER", "false")
    control = _control(
        max_position_pct=0.90,
        max_total_capital_pct=0.90,
        max_ticker_pct=0.90,
        max_sector_pct=0.01,
        max_daily_loss=-500.0,
    )
    control._get_snapshot = MagicMock(side_effect=AssertionError("unknown sector must fail before snapshot"))
    control._has_durable_duplicate_signal = MagicMock(
        side_effect=AssertionError("unknown sector must fail before dedup")
    )
    control._persist_dedup = MagicMock()
    control._emit_trade_dossier = MagicMock()
    monkeypatch.setattr(mc_mod, "emit_decision_event", None)

    decision = control.evaluate(
        {
            "signal_id": "signal-548-evaluate",
            "ticker": "ZZUNKNOWN1",
            "side": "CALL",
            "direction": "CALL",
            "score": 80.0,
            "timeframe": "1d",
            "pattern": "BREAKOUT",
            "score_breakdown": {"real_time_ctx": 10.0},
            "trigger": {"entry": 100.0, "stop": 99.0, "pt1": 105.0},
        },
        client_id="client@example.com",
    )

    assert decision.ok is False
    assert decision.plan is None
    assert decision.reason == "SECTOR_IDENTITY_UNPROVEN"
    assert decision.reason_code == "SECTOR_IDENTITY_UNPROVEN"
    control._get_snapshot.assert_not_called()
    control._has_durable_duplicate_signal.assert_not_called()
    control._persist_dedup.assert_not_called()
