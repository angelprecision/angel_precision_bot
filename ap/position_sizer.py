# ap/position_sizer.py -- Kelly + Drawdown-Adjusted Position Sizing
# =============================================================================
# Position sizing for Angel Precision.
#
# IMPORTANT UNIT CONTRACT:
#   premium_per_contract MUST be TOTAL DOLLARS for one options contract.
#   Example: a 2.35 option premium must be passed as 235.0, not 2.35.
#
# MODES:
#   1. Kelly sizing   -- activates after >= min_history closed trades.
#   2. Tier fallback -- used before min_history, when Kelly is invalid/no-edge,
#                       or when history has no losses yet.
#
# SAFETY RULES:
#   - Daily stop blocks all sizing.
#   - Daily throttle reduces size but never increases size.
#   - All-wins history does NOT block sizing; it falls back to tier sizing.
#   - SHADOW tier always returns 0.
#   - Returned contracts are capped by tier and max_positions.
#   - Thresholds can be explicit per client; otherwise they scale from equity.
# =============================================================================

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Optional

import ap.db as db

log = logging.getLogger("ap.position_sizer")

_TIER_MAX: dict[str, int] = {
    "A+": 20,
    "A": 10,
    "B": 5,
    "SHADOW": 0,
}

_HISTORY_SQL = """
SELECT realized_pnl, avg_fill, exit_price, qty
FROM positions
WHERE client_id=%s
  AND status IN ('CLOSED','STOPPED','TAKEN_PROFIT','EXPIRED')
  AND realized_pnl IS NOT NULL
ORDER BY entry_ts DESC
LIMIT 100
"""


@dataclass(frozen=True)
class SizingResult:
    contracts: int
    method: str  # "kelly" | "tier_fallback" | "throttled" | "blocked"
    win_rate: Optional[float]
    kelly_raw: Optional[float]
    throttle_applied: bool
    drawdown_today: float
    reason: str


class APPositionSizer:
    """
    Kelly + drawdown-adjusted position sizer.

    Args:
        throttle_threshold:
            Daily PnL level that triggers size throttle. Negative dollars.
            If None, compute() derives it from account_equity * throttle_pct.

        stop_threshold:
            Daily PnL level that blocks all trades. Negative dollars.
            If None, compute() derives it from account_equity * stop_pct.

        throttle_pct:
            Equity-scaled default for throttle threshold.

        stop_pct:
            Equity-scaled default for daily stop threshold.

        throttle_factor:
            Multiplier applied when throttle fires.

        min_history:
            Minimum closed trades required before Kelly activates.

        kelly_fraction:
            Fraction of full Kelly used for sizing. Default 0.50 = half-Kelly.
    """

    def __init__(
        self,
        *,
        throttle_threshold: float | None = None,
        stop_threshold: float | None = None,
        throttle_pct: float = 0.02,
        stop_pct: float = 0.05,
        throttle_factor: float = 0.5,
        min_history: int = 20,
        kelly_fraction: float = 0.5,
    ):
        self.throttle_threshold = throttle_threshold
        self.stop_threshold = stop_threshold
        self.throttle_pct = abs(float(throttle_pct or 0.02))
        self.stop_pct = abs(float(stop_pct or 0.05))
        self.throttle_factor = max(0.0, min(float(throttle_factor), 1.0))
        self.min_history = max(1, int(min_history))
        self.kelly_fraction = max(0.0, min(float(kelly_fraction), 1.0))

        self.account_equity = 0.0
        self._last_premium_per_contract = 0.0

    def compute(
        self,
        *,
        client_id: str,
        tier: str,
        premium_per_contract: float,
        account_equity: float,
        realized_pnl_today: float,
        position_manager,
        max_positions: int = 7,
    ) -> SizingResult:
        del position_manager  # Reserved for future exposure-aware sizing.

        account_equity = float(account_equity or 0.0)
        premium_per_contract = float(premium_per_contract or 0.0)
        # Guard: premium_per_contract must be TOTAL contract dollars (e.g., 950.0 for a $9.50 option).
        # If caller passes per-share price (9.50), Kelly sizing produces ~100x too many contracts.
        assert premium_per_contract == 0.0 or premium_per_contract >= 10.0, (
            f"premium_per_contract={premium_per_contract:.4f} appears to be per-share, not per-contract. "
            f"Must be total contract dollars (e.g., 950.0 for a $9.50 option × 100 shares). "
            f"Pass 0.0 to skip sizing."
        )
        drawdown = float(realized_pnl_today or 0.0)
        tier_upper = str(tier or "B").upper()
        max_positions = max(0, int(max_positions or 0))

        if tier_upper not in _TIER_MAX:
            log.warning("Unknown tier %s; defaulting to B", tier_upper)
            tier_upper = "B"

        self.account_equity = account_equity
        self._last_premium_per_contract = premium_per_contract

        throttle_threshold = self._resolve_threshold(
            explicit=self.throttle_threshold,
            account_equity=account_equity,
            pct=self.throttle_pct,
            fallback=-200.0,
            label="throttle_threshold",
        )
        stop_threshold = self._resolve_threshold(
            explicit=self.stop_threshold,
            account_equity=account_equity,
            pct=self.stop_pct,
            fallback=-500.0,
            label="stop_threshold",
        )

        if stop_threshold > throttle_threshold:
            log.warning(
                "Sizer thresholds misordered (stop less negative than throttle); "
                "swapping to preserve user intent | throttle=%.2f stop=%.2f",
                throttle_threshold,
                stop_threshold,
            )
            stop_threshold, throttle_threshold = throttle_threshold, stop_threshold

        if drawdown <= stop_threshold:
            log.warning(
                "Daily stop hit: drawdown=%.2f <= stop_threshold=%.2f -- sizing 0",
                drawdown,
                stop_threshold,
            )
            return SizingResult(
                contracts=0,
                method="blocked",
                win_rate=None,
                kelly_raw=None,
                throttle_applied=False,
                drawdown_today=drawdown,
                reason=f"daily_stop: drawdown={drawdown:.2f} <= {stop_threshold:.2f}",
            )

        rows = self._fetch_history(client_id)
        n = len(rows)

        log.debug(
            "client=%s tier=%s history_rows=%d premium_per_contract=%.2f equity=%.2f throttle=%.2f stop=%.2f",
            client_id,
            tier_upper,
            n,
            premium_per_contract,
            account_equity,
            throttle_threshold,
            stop_threshold,
        )

        if n < self.min_history:
            contracts = self._tier_fallback(tier_upper)
            method = "tier_fallback"
            win_rate = None
            kelly_raw = None
            reason_base = (
                f"tier_fallback ({n}<{self.min_history} trades): "
                f"tier={tier_upper} -> {contracts} contracts"
            )
        else:
            contracts, method, win_rate, kelly_raw = self._kelly_size(
                rows=rows,
                premium_per_contract=premium_per_contract,
                account_equity=account_equity,
            )

            if method == "tier_fallback":
                contracts = self._tier_fallback(tier_upper)
                reason_base = (
                    f"kelly_tier_fallback: win_rate={float(win_rate or 0.0):.3f} "
                    f"kelly_raw={float(kelly_raw or 0.0):.4f} "
                    f"tier={tier_upper} -> {contracts} contracts"
                )
            else:
                reason_base = (
                    f"kelly: win_rate={float(win_rate or 0.0):.3f} "
                    f"kelly_raw={float(kelly_raw or 0.0):.4f} "
                    f"kelly_fraction={self.kelly_fraction:.2f} -> {contracts} contracts"
                )

        tier_max = _TIER_MAX.get(tier_upper, _TIER_MAX["B"])
        contracts = min(int(contracts), int(tier_max), max_positions)

        if tier_upper == "SHADOW":
            contracts = 0
            method = "tier_fallback"
            reason_base = "shadow_tier_forces_zero"

        throttle_applied = False
        if contracts > 0 and drawdown <= throttle_threshold:
            pre_throttle = contracts
            contracts = max(1, math.floor(contracts * self.throttle_factor))
            contracts = min(contracts, pre_throttle)
            throttle_applied = True
            method = "throttled"
            log.info(
                "Drawdown throttle: drawdown=%.2f threshold=%.2f contracts %d->%d factor=%.2f",
                drawdown,
                throttle_threshold,
                pre_throttle,
                contracts,
                self.throttle_factor,
            )

        reason = reason_base
        if throttle_applied:
            reason += f" [throttled: drawdown={drawdown:.2f} <= {throttle_threshold:.2f}]"

        log.info(
            "Sized: client=%s tier=%s contracts=%d method=%s drawdown=%.2f",
            client_id,
            tier_upper,
            contracts,
            method,
            drawdown,
        )

        return SizingResult(
            contracts=contracts,
            method=method,
            win_rate=win_rate,
            kelly_raw=kelly_raw,
            throttle_applied=throttle_applied,
            drawdown_today=drawdown,
            reason=reason,
        )

    @staticmethod
    def _resolve_threshold(
        *,
        explicit: float | None,
        account_equity: float,
        pct: float,
        fallback: float,
        label: str,
    ) -> float:
        if explicit is not None:
            return -abs(float(explicit))

        if account_equity > 0:
            return -abs(account_equity * abs(float(pct)))

        log.warning(
            "%s using dollar fallback %.2f because account_equity is missing/zero",
            label,
            fallback,
        )
        return -abs(float(fallback))

    def _tier_fallback(self, tier: str) -> int:
        tier_upper = (tier or "B").upper()
        if tier_upper == "SHADOW":
            return 0

        equity = float(getattr(self, "account_equity", 0.0) or 0.0)
        premium_per_contract = float(getattr(self, "_last_premium_per_contract", 0.0) or 0.0)

        if equity > 0 and premium_per_contract > 0:
            if equity <= 10_000:
                risk_pct = 0.15
            elif equity <= 25_000:
                risk_pct = 0.10
            else:
                risk_pct = 0.05

            budget = max(300.0, min(equity * risk_pct, equity * 0.25))
            raw_qty = int(budget // premium_per_contract) if premium_per_contract > 0 else 1
            tier_cap = _TIER_MAX.get(tier_upper, _TIER_MAX["B"])
            return max(1, min(raw_qty, tier_cap))

        return _TIER_MAX.get(tier_upper, _TIER_MAX["B"])

    def _kelly_size(
        self,
        *,
        rows: list[dict],
        premium_per_contract: float,
        account_equity: float,
    ) -> tuple[int, str, float, float]:
        """
        Compute Kelly-fraction contract count from closed-trade history.

        Returns:
            (contracts, method, win_rate, kelly_raw)

        kelly_raw is the full-Kelly fraction.
        returned contracts use self.kelly_fraction * kelly_raw.
        Default self.kelly_fraction=0.5 means true half-Kelly.
        """
        premium_per_contract = float(premium_per_contract or 0.0)
        account_equity = float(account_equity or 0.0)

        if premium_per_contract <= 0:
            log.warning("premium_per_contract=%.4f invalid -- tier fallback", premium_per_contract)
            return 0, "tier_fallback", 0.0, 0.0

        if account_equity <= 0:
            log.warning("account_equity=%.2f invalid -- tier fallback", account_equity)
            return 0, "tier_fallback", 0.0, 0.0

        wins = [r for r in rows if float(r.get("realized_pnl") or 0.0) > 0]
        losses = [r for r in rows if float(r.get("realized_pnl") or 0.0) <= 0]
        n = len(rows)

        if n == 0:
            return 0, "tier_fallback", 0.0, 0.0

        win_rate = len(wins) / n
        # Normalize P&L by historical qty so avg_win/avg_loss are per-contract values.
        # Without this, multi-contract historical trades inflate the ratio vs current
        # single-contract premium, causing Kelly to over-size when history had large
        # positions and under-size when history had small ones.
        avg_win = (
            sum(
                float(r.get("realized_pnl") or 0.0) / max(1, int(r.get("qty") or 1))
                for r in wins
            ) / len(wins)
            if wins else 0.0
        )
        avg_loss = (
            sum(
                abs(float(r.get("realized_pnl") or 0.0)) / max(1, int(r.get("qty") or 1))
                for r in losses
            ) / len(losses)
            if losses else 0.0
        )

        if avg_loss == 0:
            log.info("avg_loss=0 (no losses recorded) -- tier fallback, not blocked")
            return 0, "tier_fallback", win_rate, 0.0

        avg_win_r = avg_win / premium_per_contract
        avg_loss_r = avg_loss / premium_per_contract

        if avg_loss_r <= 0:
            log.info("avg_loss_r<=0 -- tier fallback")
            return 0, "tier_fallback", win_rate, 0.0

        reward_to_risk = avg_win_r / avg_loss_r
        if reward_to_risk <= 0:
            return 0, "tier_fallback", win_rate, 0.0

        kelly_raw = win_rate - ((1.0 - win_rate) / reward_to_risk)

        if kelly_raw <= 0:
            return 0, "tier_fallback", win_rate, kelly_raw

        applied_kelly_fraction = self.kelly_fraction * kelly_raw
        risk_budget_dollars = applied_kelly_fraction * account_equity
        raw_contracts = risk_budget_dollars / premium_per_contract
        contracts = max(1, math.floor(raw_contracts))

        log.debug(
            "Kelly calc: win_rate=%.3f avg_win=%.2f avg_loss=%.2f reward_to_risk=%.4f "
            "kelly_raw=%.4f kelly_fraction=%.2f applied_fraction=%.4f raw_contracts=%.2f -> %d",
            win_rate,
            avg_win,
            avg_loss,
            reward_to_risk,
            kelly_raw,
            self.kelly_fraction,
            applied_kelly_fraction,
            raw_contracts,
            contracts,
        )

        return contracts, "kelly", win_rate, kelly_raw

    def _fetch_history(self, client_id: str) -> list[dict]:
        rows: list[dict] = []

        def _query():
            with db.conn() as c:
                c.execute(_HISTORY_SQL, (client_id,))
                return c.fetchall()

        try:
            raw = db.run_with_retry(_query) or []
            for row in raw:
                if isinstance(row, dict):
                    rows.append({
                        "realized_pnl": row.get("realized_pnl"),
                        "avg_fill": row.get("avg_fill"),
                        "exit_price": row.get("exit_price"),
                        "qty": row.get("qty"),
                    })
                else:
                    rows.append({
                        "realized_pnl": row[0],
                        "avg_fill": row[1],
                        "exit_price": row[2],
                        "qty": row[3],
                    })
        except Exception as exc:
            log.error("_fetch_history failed for client=%s: %s", client_id, exc)

        return rows
