#ap/position_sizer.py -- Kelly + Drawdown-Adjusted Position Sizing
#=================================================================
#Replaces fixed tier-based contract sizing with edge-responsive sizing.

#Two modes:
 # 1. Kelly sizing  (requires >= min_history closed trades)
 # 2. Tier fallback (fewer than min_history trades)

#Drawdown throttle is always active, independent of sizing mode.

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Optional

import ap.db as db

log = logging.getLogger("ap.position_sizer")

# ── Tier max-contract limits ──────────────────────────────────────────────────
# Scaled to account size — Kelly/budget sizing fills up to these per-tier caps.
# Tier B is no longer capped at 1 — it buys however many contracts 2% budget allows.
_TIER_MAX: dict[str, int] = {
    "A+": 20,   # up to 20 contracts if Kelly supports it
    "A":  10,
    "B":  5,    # was 1 — now budget-driven (2% of equity / premium)
    "SHADOW": 0,
}

# SQL: last 100 closed positions for Kelly calculation
_HISTORY_SQL = """
SELECT realized_pnl, avg_fill, exit_price, qty
FROM positions
WHERE client_id=%s
  AND status IN ('CLOSED','STOPPED','TAKEN_PROFIT','EXPIRED')
  AND realized_pnl IS NOT NULL
ORDER BY entry_ts DESC
LIMIT 100
"""


# =============================================================================
# RESULT DATACLASS
# =============================================================================

@dataclass
class SizingResult:
    contracts:        int
    method:           str          # "kelly" | "tier_fallback" | "throttled" | "blocked"
    win_rate:         Optional[float]
    kelly_raw:        Optional[float]
    throttle_applied: bool
    drawdown_today:   float
    reason:           str


# =============================================================================
# POSITION SIZER
# =============================================================================

class APPositionSizer:
    """
    Kelly + drawdown-adjusted position sizer.

    Args:
        throttle_threshold: Daily PnL level that triggers half-size throttle.
                            Should be a negative value (default -200.0).
        stop_threshold:     Daily PnL level that blocks all trades.
                            Should be a negative value (default -500.0).
        throttle_factor:    Multiplier applied to contracts when throttle fires
                            (default 0.5 → half size).
        min_history:        Minimum number of closed trades required before
                            Kelly activates; below this, use tier fallback.
    """

    def __init__(
        self,
        *,
        throttle_threshold: float = -200.0,
        stop_threshold:     float = -500.0,
        throttle_factor:    float = 0.5,
        min_history:        int   = 20,
    ):
        self.throttle_threshold = throttle_threshold
        self.stop_threshold     = stop_threshold
        self.throttle_factor    = throttle_factor
        self.min_history        = min_history

    # ── Public interface ──────────────────────────────────────────────────────

    def compute(
        self,
        *,
        client_id:            str,
        tier:                 str,
        premium_per_contract: float,
        account_equity:       float,
        realized_pnl_today:   float,
        position_manager,                     # APPositionManager instance
        max_positions:        int = 7,
    ) -> SizingResult:
        """
        Compute the recommended contract count.

        Returns a SizingResult with full audit trail.
        """
        tier_upper = str(tier).upper()
        drawdown   = realized_pnl_today

        # ── Hard stop: daily loss exceeded ───────────────────────────────────
        if drawdown <= self.stop_threshold:
            log.warning(
                "Daily stop hit: drawdown=%.2f <= stop_threshold=%.2f -- sizing 0",
                drawdown, self.stop_threshold,
            )
            return SizingResult(
                contracts=0,
                method="blocked",
                win_rate=None,
                kelly_raw=None,
                throttle_applied=False,
                drawdown_today=drawdown,
                reason=f"daily_stop: drawdown={drawdown:.2f} <= {self.stop_threshold:.2f}",
            )

        # ── Fetch trade history ───────────────────────────────────────────────
        rows = self._fetch_history(client_id)
        n    = len(rows)

        log.debug("client=%s tier=%s history_rows=%d premium=%.2f equity=%.2f",
                  client_id, tier_upper, n, premium_per_contract, account_equity)

        # ── Choose sizing mode ────────────────────────────────────────────────
        if n < self.min_history:
            contracts, method, win_rate, kelly_raw = (
                self._tier_fallback(tier_upper), "tier_fallback", None, None
            )
            reason_base = (
                f"tier_fallback ({n}<{self.min_history} trades): "
                f"tier={tier_upper} → {contracts} contracts"
            )
        else:
            contracts, method, win_rate, kelly_raw = self._kelly_size(
                rows, premium_per_contract, account_equity
            )
            if kelly_raw is not None and kelly_raw <= 0:
                # FIX: zero losses (all wins) or no edge → fall back to tier, not block.
                # Blocking a trade because a client has only won is wrong behavior.
                log.info(
                    "Kelly edge <= 0 (raw=%.4f) -- falling back to tier sizing (not blocking)",
                    kelly_raw
                )
                fallback_qty = self._tier_fallback(tier_upper)
                return SizingResult(
                    contracts=fallback_qty,
                    method="tier_fallback",
                    win_rate=win_rate,
                    kelly_raw=kelly_raw,
                    throttle_applied=False,
                    drawdown_today=drawdown,
                    reason=(f"kelly_no_edge_tier_fallback: win_rate={win_rate:.3f} "
                            f"kelly_raw={kelly_raw:.4f} → tier={tier_upper} {fallback_qty}c"),
                )
            edge_per_dollar = (
                (win_rate * kelly_raw) if (win_rate is not None and kelly_raw is not None)
                else 0.0
            )
            reason_base = (
                f"Kelly: win_rate={win_rate:.2f} "
                f"edge={edge_per_dollar:.2f}$/$ → {contracts} contracts"
            )

        # ── Apply tier max cap ────────────────────────────────────────────────
        tier_max  = _TIER_MAX.get(tier_upper, 1)
        contracts = min(contracts, tier_max)

        # ── Apply account-level max_positions cap ─────────────────────────────
        contracts = min(contracts, max_positions)

        # ── Drawdown throttle ─────────────────────────────────────────────────
        throttle_applied = False
        if drawdown <= self.throttle_threshold:
            pre_throttle  = contracts
            contracts     = max(1, math.floor(contracts * self.throttle_factor))
            throttle_applied = True
            method           = "throttled"
            log.info(
                "Drawdown throttle: drawdown=%.2f threshold=%.2f "
                "contracts %d→%d (factor=%.2f)",
                drawdown, self.throttle_threshold,
                pre_throttle, contracts, self.throttle_factor,
            )

        # ── SHADOW tier always yields 0 ───────────────────────────────────────
        if tier_upper == "SHADOW":
            contracts = 0
            method    = "tier_fallback"

        reason = reason_base
        if throttle_applied:
            reason += f" [throttled: drawdown={drawdown:.2f}]"

        log.info(
            "Sized: client=%s tier=%s contracts=%d method=%s drawdown=%.2f",
            client_id, tier_upper, contracts, method, drawdown,
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

    # ── Internal: tier fallback ───────────────────────────────────────────────

    def _tier_fallback(self, tier: str) -> int:
        """Return contract count based on tier alone (pre-Kelly fallback)."""
        mapping = {"A+": 4, "A": 2, "B": 1, "SHADOW": 0}
        return mapping.get(tier, 1)

    # ── Internal: Kelly sizing ────────────────────────────────────────────────

    def _kelly_size(
        self,
        rows: list[dict],
        premium_per_contract: float,
        account_equity: float,
    ) -> tuple[int, str, float, float]:
        """
        Compute half-Kelly contract count from closed-trade history.

        Returns (contracts, method, win_rate, kelly_raw).
        kelly_raw is the raw half-Kelly fraction (before equity scaling).
        """
        if premium_per_contract <= 0:
            log.warning("premium_per_contract=%.4f invalid -- falling back to 0", premium_per_contract)
            return 0, "blocked", 0.0, 0.0

        wins   = [r for r in rows if r["realized_pnl"] > 0]
        losses = [r for r in rows if r["realized_pnl"] <= 0]
        n      = len(rows)

        if n == 0:
            return 0, "blocked", 0.0, 0.0

        win_rate = len(wins) / n

        avg_win  = (sum(r["realized_pnl"] for r in wins)  / len(wins))  if wins   else 0.0
        avg_loss = (sum(abs(r["realized_pnl"]) for r in losses) / len(losses)) if losses else 0.0

        if avg_loss == 0:
            log.info("avg_loss=0 (no losses recorded) -- using tier fallback")
            return 0, "blocked", win_rate, 0.0

        # Normalise to per-dollar-of-premium terms
        avg_win_pct  = avg_win  / premium_per_contract
        avg_loss_pct = avg_loss / premium_per_contract

        # Half-Kelly fraction
        half_kelly = (win_rate * avg_win_pct - (1 - win_rate) * avg_loss_pct) / avg_loss_pct

        if half_kelly <= 0:
            return 0, "blocked", win_rate, half_kelly

        # Contract count: half_kelly * equity / (2 * premium)
        raw_contracts  = half_kelly * account_equity / (2.0 * premium_per_contract)
        contracts      = max(1, math.floor(raw_contracts))

        log.debug(
            "Kelly calc: win_rate=%.3f avg_win=%.2f avg_loss=%.2f "
            "half_kelly=%.4f raw_contracts=%.2f → %d",
            win_rate, avg_win, avg_loss, half_kelly, raw_contracts, contracts,
        )

        return contracts, "kelly", win_rate, half_kelly

    # ── Internal: DB history fetch ────────────────────────────────────────────

    def _fetch_history(self, client_id: str) -> list[dict]:
        """
        Fetch last 100 closed positions for Kelly calculation.
        Uses ap.db conn() + run_with_retry with %s placeholders.
        Returns list of dicts with keys: realized_pnl, avg_fill, exit_price, qty.
        """
        rows: list[dict] = []

        def _query():
            with db.conn() as c:
                c.execute(_HISTORY_SQL, (client_id,))
                return c.fetchall()

        try:
            raw = db.run_with_retry(_query)
            # psycopg2 RealDictCursor or tuple rows -- normalise to dicts
            for row in raw:
                if isinstance(row, dict):
                    rows.append(row)
                else:
                    rows.append({
                        "realized_pnl": row[0],
                        "avg_fill":     row[1],
                        "exit_price":   row[2],
                        "qty":          row[3],
                    })
        except Exception as exc:
            log.error("_fetch_history failed for client=%s: %s", client_id, exc)
            # Return empty list -- will trigger tier fallback

        return rows
