from __future__ import annotations

import pytest

from ap.exit_fill_truth_guard import (
    LifecycleProjectionError,
    official_live_eligibility,
    project_position_from_exit_fills,
)


def _position(*, qty: int, entry: float) -> dict:
    return {"qty": qty, "avg_fill": entry}


def test_spy_full_exit_projection_uses_all_broker_fills() -> None:
    projection = project_position_from_exit_fills(
        _position(qty=14, entry=0.61),
        [
            {"filled_ts": "2026-07-16T18:55:15Z", "filled_qty": 5, "fill_price": 1.26},
            {"filled_ts": "2026-07-16T18:55:21Z", "filled_qty": 3, "fill_price": 1.25},
            {"filled_ts": "2026-07-16T18:55:37Z", "filled_qty": 2, "fill_price": 1.25},
            {"filled_ts": "2026-07-16T18:55:52Z", "filled_qty": 1, "fill_price": 1.40},
            {"filled_ts": "2026-07-16T18:56:08Z", "filled_qty": 1, "fill_price": 1.35},
            {"filled_ts": "2026-07-16T18:56:23Z", "filled_qty": 1, "fill_price": 1.35},
            {"filled_ts": "2026-07-16T18:56:39Z", "filled_qty": 1, "fill_price": 1.35},
        ],
    )

    assert projection.exited_qty == 14
    assert projection.remaining_qty == 0
    assert projection.closed is True
    assert projection.weighted_exit_price == pytest.approx(1.285714, abs=1e-6)
    assert projection.realized_pnl == pytest.approx(946.0)
    assert projection.realized_pnl_pct == pytest.approx(110.7728)
    assert projection.final_fill_ts == "2026-07-16T18:56:39Z"


def test_spy_partial_exit_preserves_four_open_contracts() -> None:
    projection = project_position_from_exit_fills(
        _position(qty=14, entry=0.61),
        [
            {"filled_ts": "2026-07-16T18:55:13Z", "filled_qty": 5, "fill_price": 1.26},
            {"filled_ts": "2026-07-16T18:55:28Z", "filled_qty": 3, "fill_price": 1.23},
            {"filled_ts": "2026-07-16T18:55:44Z", "filled_qty": 2, "fill_price": 1.27},
        ],
    )

    assert projection.exited_qty == 10
    assert projection.remaining_qty == 4
    assert projection.closed is False
    assert projection.realized_pnl == pytest.approx(643.0)
    assert projection.weighted_exit_price == pytest.approx(1.253)


def test_partial_googl_exit_does_not_falsely_close_position() -> None:
    projection = project_position_from_exit_fills(
        _position(qty=4, entry=2.75),
        [{"filled_ts": "2026-07-16T17:47:54Z", "filled_qty": 2, "fill_price": 4.20}],
    )

    assert projection.exited_qty == 2
    assert projection.remaining_qty == 2
    assert projection.closed is False
    assert projection.realized_pnl == pytest.approx(290.0)
    assert projection.realized_pnl_pct == pytest.approx(52.7273)


def test_exit_overfill_is_quarantined_not_clamped() -> None:
    with pytest.raises(LifecycleProjectionError, match="exit_overfill"):
        project_position_from_exit_fills(
            _position(qty=1, entry=1.27),
            [{"filled_ts": "2026-07-16T18:40:51Z", "filled_qty": 2, "fill_price": 1.35}],
        )


def test_rows_without_positive_broker_fill_are_not_counted() -> None:
    with pytest.raises(LifecycleProjectionError, match="no_positive_exit_fills"):
        project_position_from_exit_fills(
            _position(qty=3, entry=2.63),
            [
                {"filled_qty": 0, "fill_price": 3.30},
                {"filled_qty": 1, "fill_price": None},
            ],
        )


@pytest.mark.parametrize(
    (
        "execution_mode",
        "closed",
        "entry_id",
        "exit_id",
        "all_exit_fills_broker_backed",
        "expected",
    ),
    [
        ("live", True, "entry-1", "exit-1", True, True),
        ("LIVE", True, "entry-1", "exit-1", True, True),
        ("paper", True, "entry-1", "exit-1", True, False),
        ("unknown", True, "entry-1", "exit-1", True, False),
        ("live", False, "entry-1", "exit-1", True, False),
        ("live", True, "", "exit-1", True, False),
        ("live", True, "entry-1", "", True, False),
        ("live", True, "entry-1", "exit-2", False, False),
    ],
)
def test_official_live_proof_requires_complete_broker_lifecycle(
    execution_mode: str,
    closed: bool,
    entry_id: str,
    exit_id: str,
    all_exit_fills_broker_backed: bool,
    expected: bool,
) -> None:
    assert official_live_eligibility(
        execution_mode=execution_mode,
        closed=closed,
        entry_broker_order_id=entry_id,
        exit_broker_order_id=exit_id,
        all_exit_fills_broker_backed=all_exit_fills_broker_backed,
    ) is expected
