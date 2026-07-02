from __future__ import annotations

from dataclasses import dataclass

from ap.fvg_exit_guidance import evaluate_fvg_exit_guidance


@dataclass
class PositionStub:
    side: str = "CALL"
    current_underlying: float = 102.0
    underlying_target: float = 102.0
    entry_price: float = 1.00
    current_option_price: float = 1.15
    peak_pnl_pct: float = 0.15
    max_profit_seen: float = 0.15
    fvg_target_guidance: dict | None = None

    @property
    def option_pnl_pct(self) -> float:
        return (self.current_option_price - self.entry_price) / self.entry_price

    @property
    def is_at_target(self) -> bool:
        if self.side == "CALL":
            return self.current_underlying >= self.underlying_target
        return self.current_underlying <= self.underlying_target


def test_no_guidance_is_noop():
    pos = PositionStub(fvg_target_guidance=None)
    result = evaluate_fvg_exit_guidance(pos)
    assert result.action == "NO_GUIDANCE"
    assert result.should_override_target_exit is False
    assert result.should_close_at_fvg_front is False


def test_extension_candidate_holds_past_original_target_when_continuation_strong():
    pos = PositionStub(
        side="CALL",
        current_underlying=102.10,
        underlying_target=102.00,
        current_option_price=1.16,
        peak_pnl_pct=0.16,
        max_profit_seen=0.16,
        fvg_target_guidance={
            "action": "extension_candidate_to_opposing_fvg_front",
            "original_target": 102.00,
            "suggested_target": 103.50,
        },
    )

    result = evaluate_fvg_exit_guidance(pos)

    assert result.action == "HOLD_FOR_FVG_EXTENSION"
    assert result.should_override_target_exit is True
    assert result.should_close_at_fvg_front is False
    assert result.reason_code == "FVG_HOLD_FOR_EXTENSION"
    assert result.suggested_underlying_exit == 103.50


def test_extension_candidate_keeps_original_target_when_continuation_is_weak():
    pos = PositionStub(
        side="CALL",
        current_underlying=102.10,
        underlying_target=102.00,
        current_option_price=1.04,
        peak_pnl_pct=0.04,
        max_profit_seen=0.04,
        fvg_target_guidance={
            "action": "extension_candidate_to_opposing_fvg_front",
            "original_target": 102.00,
            "suggested_target": 103.50,
        },
    )

    result = evaluate_fvg_exit_guidance(pos)

    assert result.action == "KEEP_ORIGINAL_TARGET"
    assert result.should_override_target_exit is False
    assert result.should_close_at_fvg_front is False
    assert result.reason_code == "FVG_EXTENSION_NOT_CONFIRMED"


def test_extension_candidate_closes_when_fvg_front_is_reached():
    pos = PositionStub(
        side="CALL",
        current_underlying=103.50,
        underlying_target=102.00,
        current_option_price=1.28,
        peak_pnl_pct=0.28,
        max_profit_seen=0.28,
        fvg_target_guidance={
            "action": "extension_candidate_to_opposing_fvg_front",
            "original_target": 102.00,
            "suggested_target": 103.50,
        },
    )

    result = evaluate_fvg_exit_guidance(pos)

    assert result.action == "CLOSE_AT_FVG_FRONT"
    assert result.should_override_target_exit is False
    assert result.should_close_at_fvg_front is True
    assert result.reason_code == "FVG_EXTENSION_TARGET_EXIT"


def test_put_extension_candidate_uses_lower_fvg_front():
    pos = PositionStub(
        side="PUT",
        current_underlying=97.90,
        underlying_target=98.00,
        current_option_price=1.16,
        peak_pnl_pct=0.16,
        max_profit_seen=0.16,
        fvg_target_guidance={
            "action": "extension_candidate_to_opposing_fvg_front",
            "original_target": 98.00,
            "suggested_target": 96.50,
        },
    )

    result = evaluate_fvg_exit_guidance(pos)

    assert result.action == "HOLD_FOR_FVG_EXTENSION"
    assert result.should_override_target_exit is True
    assert result.suggested_underlying_exit == 96.50


def test_cap_guidance_closes_before_unconfirmed_opposing_wall():
    pos = PositionStub(
        side="CALL",
        current_underlying=103.50,
        underlying_target=105.00,
        current_option_price=1.20,
        fvg_target_guidance={
            "action": "cap_before_opposing_fvg_or_block_entry",
            "original_target": 105.00,
            "suggested_target": 103.50,
        },
    )

    result = evaluate_fvg_exit_guidance(pos)

    assert result.action == "CLOSE_AT_FVG_FRONT"
    assert result.should_close_at_fvg_front is True
    assert result.reason_code == "FVG_FRONT_TARGET_EXIT"


def test_cap_guidance_holds_until_front_when_not_reached():
    pos = PositionStub(
        side="CALL",
        current_underlying=102.25,
        underlying_target=105.00,
        current_option_price=1.08,
        fvg_target_guidance={
            "action": "cap_before_opposing_fvg_or_block_entry",
            "original_target": 105.00,
            "suggested_target": 103.50,
        },
    )

    result = evaluate_fvg_exit_guidance(pos)

    assert result.action == "HOLD_UNTIL_FVG_FRONT_OR_NORMAL_EXIT"
    assert result.should_override_target_exit is False
    assert result.should_close_at_fvg_front is False
