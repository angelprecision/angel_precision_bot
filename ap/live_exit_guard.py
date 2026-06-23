"""
ap/live_exit_guard.py
PR #175 — P0: Block live soft exits when critical context data is missing or unconfirmed.

Rules enforced:
  1. DATA_DEGRADED_HOLD — block if any critical field is None for a live position.
  2. Direction-aware confirmation:
       PUT  → confirming if current_underlying <= trigger/reference
              invalidated only if current_underlying >= stop_underlying (fresh, confirmed)
       CALL → confirming if current_underlying >= trigger/reference
              invalidated only if current_underlying <= stop_underlying (fresh, confirmed)
  3. Option-loss alone cannot trigger live thesis-fail exit unless:
       - option quote is fresh (< FRESH_QUOTE_MAX_AGE_SECS)
       - spread is sane (within SANE_SPREAD_MAX_RATIO)
       - two consecutive polls confirm the loss
       - underlying invalidation is also confirmed
       - minimum hold has passed, UNLESS hard disaster stop
  4. Emit DATA_DEGRADED_HOLD, log missing fields — do NOT submit broker exit.

Exit outcomes emitted by this module:
    PROCEED          — all conditions met, exit engine may proceed with its decision
    DATA_DEGRADED_HOLD  — block; one or more critical fields are missing
    UNDERLYING_CONFIRMING  — underlying is still on the right side of stop, hold
    OPTION_LOSS_UNCONFIRMED  — option loss not yet confirmed by 2 consecutive polls
    MINIMUM_HOLD_NOT_MET  — minimum hold period has not elapsed (non-disaster)

Author: Angel Precision Intelligence
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import ClassVar, Dict, Optional, Tuple

from ap.exit_context_hydrator import ExitContext, FRESH_QUOTE_MAX_AGE_SECS

log = logging.getLogger(__name__)

# Minimum seconds a live position must be held before option-loss soft exits are
# allowed.  Hard disaster stops (HARD_DISASTER_STOP) are not gated by this.
MINIMUM_LIVE_HOLD_SECS = 240   # 4 minutes — matches the NKE/RIVN scenario

# Consecutive polls needed before option-loss alone can trigger a live soft exit
OPTION_LOSS_CONFIRM_POLLS_REQUIRED = 2

# Underlying quote staleness ceiling for underlying-invalidation decisions
UNDERLYING_STALE_SECS = 60

# Exit codes (strings) that this guard classifies as "soft" and will gate
SOFT_EXIT_CODES = frozenset(
    {
        "SOFT_LOSS",
        "THESIS_FAIL_SOFT_STOP",
        "NEVER_GREEN_STOP",
        "THESIS_FAIL_HARD_STOP",   # still gated on data presence
    }
)

# Exit codes that bypass the minimum-hold gate (but not the data-degraded gate)
HARD_DISASTER_CODES = frozenset({"HARD_DISASTER_STOP", "BROKER_FORCE_CLOSE"})


# ---------------------------------------------------------------------------
# GuardDecision
# ---------------------------------------------------------------------------

@dataclass
class GuardDecision:
    outcome: str          # PROCEED | DATA_DEGRADED_HOLD | UNDERLYING_CONFIRMING | …
    reason: str           # human-readable explanation
    missing_fields: list  # populated when outcome == DATA_DEGRADED_HOLD
    block: bool           # True → do NOT proceed with exit, do NOT submit broker exit


# ---------------------------------------------------------------------------
# Poll state — keyed by position_id, tracks consecutive option-loss polls
# ---------------------------------------------------------------------------

_option_loss_poll_counts: Dict[str, int] = {}
_option_loss_first_seen: Dict[str, float] = {}


def _record_option_loss_poll(position_id: str) -> int:
    """Increment consecutive option-loss poll count. Returns new count."""
    if position_id not in _option_loss_poll_counts:
        _option_loss_poll_counts[position_id] = 0
        _option_loss_first_seen[position_id] = time.time()
    _option_loss_poll_counts[position_id] += 1
    return _option_loss_poll_counts[position_id]


def _clear_option_loss_poll(position_id: str) -> None:
    """Reset poll state when position is no longer in loss."""
    _option_loss_poll_counts.pop(position_id, None)
    _option_loss_first_seen.pop(position_id, None)


def _get_option_loss_poll_count(position_id: str) -> int:
    return _option_loss_poll_counts.get(position_id, 0)


# ---------------------------------------------------------------------------
# LiveExitGuard
# ---------------------------------------------------------------------------

class LiveExitGuard:
    """
    Called by the exit engine before every non-manual exit decision on a
    live position.

    Usage:
        guard = LiveExitGuard()
        decision = guard.evaluate(ctx, proposed_exit_code="SOFT_LOSS")
        if decision.block:
            log_and_return(decision.outcome)  # do NOT submit broker exit

    The guard is stateless across instances but uses module-level poll
    counters (process-local).  In a multi-process deploy, each pod maintains
    its own counters — this is acceptable because the 2-poll requirement is a
    per-pod safety net.
    """

    def evaluate(
        self,
        ctx: ExitContext,
        proposed_exit_code: str,
        option_pnl_pct: Optional[float] = None,  # e.g. -0.35 for 35% loss on option
    ) -> GuardDecision:
        """
        Main guard entry point.

        Args:
            ctx:                  Fully-hydrated ExitContext (from ExitContextHydrator).
            proposed_exit_code:   What the exit engine wants to emit.
            option_pnl_pct:       Option P&L as a fraction (negative = loss).
                                  If None, computed from ctx entry_price vs option_mid.

        Returns:
            GuardDecision.  If decision.block is True, the caller MUST:
              - NOT submit a broker exit order.
              - NOT write an EXIT row to proof_trades.
              - Return decision.outcome as the cycle result.
              - Log decision.reason and decision.missing_fields.
        """

        # Non-live positions: guard does not apply
        if not ctx.is_live:
            return GuardDecision(
                outcome="PROCEED",
                reason="paper mode — guard not active",
                missing_fields=[],
                block=False,
            )

        # Non-soft exits that are also not hard disaster: still check data
        if proposed_exit_code not in SOFT_EXIT_CODES and proposed_exit_code not in HARD_DISASTER_CODES:
            return GuardDecision(
                outcome="PROCEED",
                reason=f"exit_code={proposed_exit_code} is not a gated code",
                missing_fields=[],
                block=False,
            )

        # ── Gate 1: DATA_DEGRADED ─────────────────────────────────────────
        missing = ctx.missing_live_critical_fields
        if missing:
            log.error(
                "[LIVE_EXIT_GUARD] DATA_DEGRADED_HOLD position_id=%s "
                "proposed=%s missing_fields=%s",
                ctx.position_id, proposed_exit_code, missing,
            )
            return GuardDecision(
                outcome="DATA_DEGRADED_HOLD",
                reason=(
                    f"live exit blocked: missing critical fields={missing}; "
                    f"proposed={proposed_exit_code}"
                ),
                missing_fields=missing,
                block=True,
            )

        # ── Gate 2: Underlying staleness ──────────────────────────────────
        if (
            ctx.underlying_quote_age_secs is not None
            and ctx.underlying_quote_age_secs > UNDERLYING_STALE_SECS
        ):
            log.warning(
                "[LIVE_EXIT_GUARD] DATA_DEGRADED_HOLD underlying_quote_age=%ss > %ss "
                "position_id=%s",
                ctx.underlying_quote_age_secs, UNDERLYING_STALE_SECS, ctx.position_id,
            )
            return GuardDecision(
                outcome="DATA_DEGRADED_HOLD",
                reason=(
                    f"live exit blocked: underlying quote is stale "
                    f"({ctx.underlying_quote_age_secs:.0f}s > {UNDERLYING_STALE_SECS}s)"
                ),
                missing_fields=["current_underlying (stale)"],
                block=True,
            )

        # ── Gate 3: Direction-aware underlying confirmation ───────────────
        underlying_invalidated, underlying_reason = self._check_underlying_invalidation(ctx)
        if not underlying_invalidated and proposed_exit_code in SOFT_EXIT_CODES:
            # Underlying is still on the thesis side — reset the option-loss
            # poll counter since the confirming condition overrides any prior loss polls.
            _clear_option_loss_poll(ctx.position_id)
            log.info(
                "[LIVE_EXIT_GUARD] UNDERLYING_CONFIRMING position_id=%s "
                "direction=%s current=%s stop=%s trigger=%s — holding",
                ctx.position_id, ctx.direction,
                ctx.current_underlying, ctx.stop_underlying, ctx.trigger_underlying,
            )
            return GuardDecision(
                outcome="UNDERLYING_CONFIRMING",
                reason=(
                    f"live soft exit blocked: underlying not invalidated. "
                    f"{underlying_reason}"
                ),
                missing_fields=[],
                block=True,
            )

        # Hard disaster codes bypass minimum hold and option poll gates
        if proposed_exit_code in HARD_DISASTER_CODES:
            return GuardDecision(
                outcome="PROCEED",
                reason=f"hard disaster exit — bypassing hold/poll gates. {underlying_reason}",
                missing_fields=[],
                block=False,
            )

        # ── Gate 4: Minimum hold ──────────────────────────────────────────
        hold_secs = self._hold_seconds(ctx)
        if hold_secs is not None and hold_secs < MINIMUM_LIVE_HOLD_SECS:
            log.info(
                "[LIVE_EXIT_GUARD] MINIMUM_HOLD_NOT_MET position_id=%s "
                "hold_secs=%.1f < %s",
                ctx.position_id, hold_secs, MINIMUM_LIVE_HOLD_SECS,
            )
            return GuardDecision(
                outcome="MINIMUM_HOLD_NOT_MET",
                reason=(
                    f"live soft exit blocked: hold time {hold_secs:.0f}s < "
                    f"{MINIMUM_LIVE_HOLD_SECS}s minimum"
                ),
                missing_fields=[],
                block=True,
            )

        # ── Gate 5: Option quote freshness + spread sanity ────────────────
        option_quote_ok, option_quote_reason = self._check_option_quote_quality(ctx)
        if not option_quote_ok:
            log.warning(
                "[LIVE_EXIT_GUARD] OPTION_LOSS_UNCONFIRMED position_id=%s "
                "reason=%s",
                ctx.position_id, option_quote_reason,
            )
            _clear_option_loss_poll(ctx.position_id)
            return GuardDecision(
                outcome="OPTION_LOSS_UNCONFIRMED",
                reason=option_quote_reason,
                missing_fields=[],
                block=True,
            )

        # ── Gate 6: Two consecutive option-loss polls ─────────────────────
        computed_pnl_pct = self._compute_option_pnl_pct(ctx)
        effective_pnl_pct = option_pnl_pct if option_pnl_pct is not None else computed_pnl_pct

        if effective_pnl_pct is not None and effective_pnl_pct < 0:
            poll_count = _record_option_loss_poll(ctx.position_id)
            if poll_count < OPTION_LOSS_CONFIRM_POLLS_REQUIRED:
                log.info(
                    "[LIVE_EXIT_GUARD] OPTION_LOSS_UNCONFIRMED position_id=%s "
                    "pnl_pct=%.2f poll=%d/%d",
                    ctx.position_id, effective_pnl_pct,
                    poll_count, OPTION_LOSS_CONFIRM_POLLS_REQUIRED,
                )
                return GuardDecision(
                    outcome="OPTION_LOSS_UNCONFIRMED",
                    reason=(
                        f"live soft exit blocked: option loss not yet confirmed "
                        f"({poll_count}/{OPTION_LOSS_CONFIRM_POLLS_REQUIRED} polls)"
                    ),
                    missing_fields=[],
                    block=True,
                )
            log.info(
                "[LIVE_EXIT_GUARD] option loss confirmed poll=%d pnl_pct=%.2f "
                "position_id=%s",
                poll_count, effective_pnl_pct, ctx.position_id,
            )
        else:
            # Not in loss — reset poll counter
            _clear_option_loss_poll(ctx.position_id)

        # All gates passed — exit is allowed
        log.info(
            "[LIVE_EXIT_GUARD] PROCEED position_id=%s proposed=%s "
            "hold_secs=%s underlying_invalidated=%s",
            ctx.position_id, proposed_exit_code, hold_secs, underlying_invalidated,
        )
        return GuardDecision(
            outcome="PROCEED",
            reason=(
                f"all live exit gates passed. {underlying_reason} "
                f"hold_secs={hold_secs}"
            ),
            missing_fields=[],
            block=False,
        )

    # ------------------------------------------------------------------ #
    #  Direction-aware underlying confirmation                             #
    # ------------------------------------------------------------------ #

    def _check_underlying_invalidation(
        self, ctx: ExitContext
    ) -> Tuple[bool, str]:
        """
        Returns (is_invalidated, reason_string).

        For PUT:
          - Confirming: current_underlying <= trigger/reference (still below)
          - Invalidated: current_underlying >= stop_underlying (broken above stop)

        For CALL:
          - Confirming: current_underlying >= trigger/reference (still above)
          - Invalidated: current_underlying <= stop_underlying (broken below stop)

        If direction or prices are missing, returns (False, reason) so that the
        DATA_DEGRADED gate earlier will have already blocked the exit.
        """
        direction = (ctx.direction or "").upper()
        cu = ctx.current_underlying
        stop = ctx.stop_underlying
        trigger = ctx.trigger_underlying or ctx.underlying_entry

        if cu is None or stop is None:
            return False, "cannot determine invalidation — underlying/stop missing"

        if direction == "PUT":
            # Invalidated: price has risen to or above stop
            if cu >= stop:
                return True, (
                    f"PUT invalidated: current_underlying={cu} >= stop={stop}"
                )
            # Still confirming: price at or below trigger
            if trigger is not None and cu <= trigger:
                return False, (
                    f"PUT confirming: current_underlying={cu} <= trigger={trigger}"
                )
            # Between trigger and stop — confirming (give the trade room)
            return False, (
                f"PUT between trigger and stop: current={cu} trigger={trigger} stop={stop}"
            )

        elif direction == "CALL":
            # Invalidated: price has fallen to or below stop
            if cu <= stop:
                return True, (
                    f"CALL invalidated: current_underlying={cu} <= stop={stop}"
                )
            # Still confirming: price at or above trigger
            if trigger is not None and cu >= trigger:
                return False, (
                    f"CALL confirming: current_underlying={cu} >= trigger={trigger}"
                )
            # Between stop and trigger — confirming
            return False, (
                f"CALL between stop and trigger: current={cu} trigger={trigger} stop={stop}"
            )

        else:
            # Unknown direction — do not allow soft exit
            return False, f"direction='{direction}' unknown — defaulting to confirming (safe)"

    # ------------------------------------------------------------------ #
    #  Option quote quality                                                #
    # ------------------------------------------------------------------ #

    def _check_option_quote_quality(
        self, ctx: ExitContext
    ) -> Tuple[bool, str]:
        """Returns (is_ok, reason)."""
        if ctx.option_mid is None and ctx.option_bid is None and ctx.option_ask is None:
            return False, "option quote missing entirely"

        if (
            ctx.option_quote_age_secs is not None
            and ctx.option_quote_age_secs > FRESH_QUOTE_MAX_AGE_SECS
        ):
            return False, (
                f"option quote stale: age={ctx.option_quote_age_secs:.0f}s "
                f"> {FRESH_QUOTE_MAX_AGE_SECS}s"
            )

        if not ctx.option_spread_sane:
            return False, "option spread is not sane (too wide)"

        return True, "option quote ok"

    # ------------------------------------------------------------------ #
    #  Helpers                                                             #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _hold_seconds(ctx: ExitContext) -> Optional[float]:
        if ctx.hold_since is None:
            return None
        now = datetime.now(timezone.utc)
        hs = ctx.hold_since
        if hs.tzinfo is None:
            hs = hs.replace(tzinfo=timezone.utc)
        return (now - hs).total_seconds()

    @staticmethod
    def _compute_option_pnl_pct(ctx: ExitContext) -> Optional[float]:
        """Return option P&L as fraction vs entry price.  Negative = loss."""
        entry = ctx.entry_price
        current = ctx.option_mid or ctx.option_bid
        if entry and current and entry > 0:
            return (current - entry) / entry
        return None
