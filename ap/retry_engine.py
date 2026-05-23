"""
ap/retry_engine.py — alignment-aware limit-order re-peg.

WHY THIS EXISTS
---------------
Today's failure mode (observed 2026-05-19):
  - Bot posts limit @ $2.17 for MSFT $427.50 calls
  - Underlying ticks higher, option mid drifts to $2.32-$2.45
  - At t+75s, MISSED_MOVE_CANCEL fires (current > limit × 1.07) -> CANCELED
  - Result: missed the move
  - BUT: in retrospect, every position canceled today would have been a loss
    if filled. The thesis broke shortly after entry would have happened.

The naive fix is "retry with a higher limit," but blind retries are how you
buy late and bleed. The right fix is: re-peg ONLY when the alignment that
generated the signal is still holding.

THREE-GATE DECISION
-------------------
Before a re-peg is approved, ALL three gates must pass:

  Gate 1 — TIME
    Re-peg attempts must be spaced (default 30s apart). Max 2 re-pegs per
    order. Total order lifetime <= MISSED_MOVE_MIN_SECS + 2 * REPEG_INTERVAL.
    Prevents thrashing.

  Gate 2 — PROXIMITY
    Option price must be within REPEG_PROXIMITY_PCT of our limit (default 8%).
    If the option has run >8% above us, the trade is gone — chasing is a
    capitulation, not an edge.

  Gate 3 — UNDERLYING ALIGNMENT
    For a CALL: underlying spot must be >= signal_entry_price * (1 - drift_tol).
    For a PUT:  underlying spot must be <= signal_entry_price * (1 + drift_tol).
    Default drift_tol = 0.002 (0.2% give). If the underlying has unwound
    from the signal level, the thesis is dead — don't re-peg.

If ALL three pass: new limit = current_limit + 0.50 * (current_option - current_limit).
  (Move halfway to the market, not all the way. Preserves edge if the
  option ticks back down.)

If ANY fail: caller proceeds with the existing cancel path. Re-peg was just
not attempted; the order's normal stale-cancel logic still owns the outcome.

USAGE
-----
    from ap.retry_engine import decide_repeg, apply_repeg

    decision = decide_repeg(order_row=order, current_option_price=mid,
                             underlying_spot=spot)
    if decision.ok:
        apply_repeg(broker, order, decision.new_limit_price)
    else:
        log.info("repeg declined: %s", decision.reason)
        # caller continues to its normal cancel-on-stale path

ENV VARS
--------
    REPEG_ENABLED              (default 1)         master switch
    REPEG_MAX_ATTEMPTS         (default 2)         per-order cap
    REPEG_INTERVAL_SECS        (default 30)        min seconds between re-pegs
    REPEG_PROXIMITY_PCT        (default 0.08)      max gap option vs limit
    REPEG_UNDERLYING_DRIFT_PCT (default 0.002)     thesis drift tolerance
    REPEG_STEP_FRACTION        (default 0.50)      how much of the gap to close
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger("ap.retry_engine")


# ── Config (env-tunable) ─────────────────────────────────────────────────────
# P1 ENTRY FIX (2026-05-21):
# Old: 30s between repegs, 50% gap-close (mid + half of (option_mid - limit)).
# Production: orders sat 150s+ at broker, only 2 fills out of 392 entries.
# New: 6s between repegs. Attempt 0 at ask, Attempt 1 ask+0.01, Attempt 2 ask+0.02.
# Cap 2 attempts, total order lifetime ~25s.
REPEG_ENABLED               = (os.getenv("REPEG_ENABLED", "1").strip().lower()
                               in ("1", "true", "yes", "on"))
REPEG_MAX_ATTEMPTS          = int(os.getenv("REPEG_MAX_ATTEMPTS", "2"))
REPEG_INTERVAL_SECS         = int(os.getenv("REPEG_INTERVAL_SECS", "6"))
REPEG_PROXIMITY_PCT         = float(os.getenv("REPEG_PROXIMITY_PCT", "0.08"))
REPEG_UNDERLYING_DRIFT_PCT  = float(os.getenv("REPEG_UNDERLYING_DRIFT_PCT", "0.002"))
REPEG_STEP_FRACTION         = float(os.getenv("REPEG_STEP_FRACTION", "0.50"))

# P1 ENTRY FIX: fixed-step ladder for entry-side repegs.
# When the order is an ENTRY, apply_repeg uses ask + ladder[attempt] instead
# of the legacy 50%-gap formula. EXITS still use the legacy gap-close.
ENTRY_REPEG_LADDER_PENNIES = os.getenv("ENTRY_REPEG_LADDER_PENNIES", "0.01,0.02")
try:
    ENTRY_REPEG_LADDER = [float(p.strip()) for p in ENTRY_REPEG_LADDER_PENNIES.split(",") if p.strip()]
except Exception:
    ENTRY_REPEG_LADDER = [0.01, 0.02]


@dataclass
class RepegDecision:
    ok: bool
    reason: str              # short reason code
    new_limit_price: float = 0.0
    attempts_used: int = 0
    detail: dict = None      # arbitrary diagnostics

    def __post_init__(self):
        if self.detail is None:
            self.detail = {}


def decide_repeg(
    *,
    order_row: dict,
    current_option_price: float,
    underlying_spot: float,
) -> RepegDecision:
    """Decide whether to re-peg this order's limit price.

    order_row must include (at minimum):
      - limit_price                   (current order limit)
      - direction or side             ('CALL' or 'PUT')
      - signal_entry_price            (underlying price at signal time;
                                       may be stored in meta as JSON or as
                                       a column. Caller resolves and passes
                                       via order_row['signal_entry_price']
                                       or order_row['meta']['signal_entry_price'])
      - repeg_attempts                (int, default 0)
      - last_repeg_ts                 (unix seconds float, default 0)

    Returns RepegDecision with .ok and .new_limit_price.
    """
    if not REPEG_ENABLED:
        return RepegDecision(ok=False, reason="repeg_disabled")

    # Pull the inputs with safe defaults.
    try:
        limit_price = float(order_row.get("limit_price") or order_row.get("price") or 0)
    except (TypeError, ValueError):
        limit_price = 0.0
    if limit_price <= 0:
        return RepegDecision(ok=False, reason="no_limit_price")

    if current_option_price is None or current_option_price <= 0:
        return RepegDecision(ok=False, reason="no_current_price")

    direction = str(
        order_row.get("direction")
        or order_row.get("side")
        or (order_row.get("meta") or {}).get("direction")
        or ""
    ).upper()
    if direction not in ("CALL", "PUT"):
        return RepegDecision(ok=False, reason="unknown_direction", detail={"direction": direction})

    # Resolve signal_entry_price (the underlying price the signal was based on).
    meta = order_row.get("meta") or {}
    sig_entry = (order_row.get("signal_entry_price")
                 or meta.get("signal_entry_price")
                 or meta.get("entry_price"))
    try:
        sig_entry = float(sig_entry) if sig_entry is not None else None
    except (TypeError, ValueError):
        sig_entry = None

    attempts = int(order_row.get("repeg_attempts") or meta.get("repeg_attempts") or 0)
    last_ts  = float(order_row.get("last_repeg_ts") or meta.get("last_repeg_ts") or 0)
    now      = time.time()

    # ─── Gate 1: TIME ────────────────────────────────────────────────────
    if attempts >= REPEG_MAX_ATTEMPTS:
        return RepegDecision(ok=False, reason="max_attempts_reached",
                              attempts_used=attempts,
                              detail={"max": REPEG_MAX_ATTEMPTS})
    if last_ts and (now - last_ts) < REPEG_INTERVAL_SECS:
        return RepegDecision(ok=False, reason="too_soon_since_last_repeg",
                              attempts_used=attempts,
                              detail={"elapsed": now - last_ts,
                                      "min_interval": REPEG_INTERVAL_SECS})

    # ─── Gate 2: PROXIMITY ───────────────────────────────────────────────
    # If the option has run too far above our limit, chasing is capitulation.
    gap_pct = (current_option_price / limit_price) - 1.0
    if gap_pct > REPEG_PROXIMITY_PCT:
        return RepegDecision(ok=False, reason="runaway_quote",
                              attempts_used=attempts,
                              detail={"gap_pct": gap_pct,
                                      "max_proximity": REPEG_PROXIMITY_PCT,
                                      "limit": limit_price,
                                      "current": current_option_price})
    if gap_pct <= 0:
        # Option price is at or below our limit \u2014 there's nothing to chase.
        # The order should fill at current limit; no re-peg needed.
        return RepegDecision(ok=False, reason="no_gap_to_close",
                              attempts_used=attempts,
                              detail={"gap_pct": gap_pct})

    # ─── Gate 3: UNDERLYING ALIGNMENT ────────────────────────────────────
    # If we don't know the original signal level, we can't validate the thesis.
    # Default to ALLOW re-peg in that case (back-compat for legacy orders),
    # but log so we can spot it. Going forward all new orders should carry
    # signal_entry_price in meta.
    if sig_entry is not None and underlying_spot is not None and underlying_spot > 0:
        if direction == "CALL":
            # Thesis: underlying should be >= sig_entry * (1 - drift_tol).
            floor = sig_entry * (1.0 - REPEG_UNDERLYING_DRIFT_PCT)
            if underlying_spot < floor:
                return RepegDecision(ok=False, reason="stale_thesis_call",
                                      attempts_used=attempts,
                                      detail={"spot": underlying_spot,
                                              "signal_entry": sig_entry,
                                              "floor": floor})
        else:  # PUT
            ceil = sig_entry * (1.0 + REPEG_UNDERLYING_DRIFT_PCT)
            if underlying_spot > ceil:
                return RepegDecision(ok=False, reason="stale_thesis_put",
                                      attempts_used=attempts,
                                      detail={"spot": underlying_spot,
                                              "signal_entry": sig_entry,
                                              "ceil": ceil})
    else:
        log.info("repeg: no signal_entry_price; allowing repeg without alignment check "
                 "(order=%s direction=%s)", order_row.get("id") or order_row.get("local_order_id"), direction)

    # ─── ALL GATES PASS: compute new limit ───────────────────────────────
    step = (current_option_price - limit_price) * REPEG_STEP_FRACTION
    # Round to penny (option ticks are typically $0.01 above $3, $0.05 below \u2014
    # rounding to penny is conservative and broker will normalize).
    # P1 ENTRY FIX (2026-05-21): for ENTRY orders use fixed-step ladder.
    # Attempt-0 already at ask (set by contract_selector). Attempt 1=ask+0.01,
    # Attempt 2=ask+0.02. Provably better than 50%-gap-close for entries.
    # EXITS keep the legacy 50%-gap-close (they already work fine).
    kind = str(order_row.get("kind") or (order_row.get("meta") or {}).get("kind") or "").upper()
    current_ask = None
    try:
        current_ask = float(order_row.get("current_ask") or 0) or None
    except (TypeError, ValueError):
        current_ask = None

    if kind == "ENTRY" and ENTRY_REPEG_LADDER:
        idx = min(attempts, len(ENTRY_REPEG_LADDER) - 1)
        bump = ENTRY_REPEG_LADDER[idx]
        anchor = current_ask if (current_ask and current_ask > 0) else current_option_price
        new_limit = round(anchor + bump, 2)
        if new_limit <= limit_price:
            new_limit = round(limit_price + 0.01, 2)
        reason = f"entry_ladder_attempt{attempts + 1}_bump+{bump:.2f}"
        step_detail = {"ladder_idx": idx, "ladder_bump": bump, "anchor": anchor}
    else:
        step = (current_option_price - limit_price) * REPEG_STEP_FRACTION
        new_limit = round(limit_price + step, 2)
        if new_limit <= limit_price:
            new_limit = round(limit_price + 0.01, 2)
        reason = "aligned_repeg"
        step_detail = {"step": step}

    return RepegDecision(
        ok=True, reason=reason,
        new_limit_price=new_limit,
        attempts_used=attempts + 1,
        detail={"prev_limit": limit_price,
                "current_option": current_option_price,
                "current_ask": current_ask,
                "gap_pct": gap_pct,
                "signal_entry": sig_entry,
                "underlying_spot": underlying_spot,
                "direction": direction,
                "kind": kind,
                **step_detail},
    )


def apply_repeg(
    *,
    broker,
    osm,
    order_row: dict,
    decision: RepegDecision,
    client_id: str,
) -> bool:
    """Apply the re-peg by canceling the broker order and submitting a NEW
    one at decision.new_limit_price. Records the attempt in orders.meta.

    BLOCKER-2 FIX (post-review): the previous version canceled the broker
    order, flipped local status to CREATED, and said "the next worker tick
    will pick it up." Reality: nothing in the codebase re-submits CREATED
    entries. They sit until the 2-minute stale-watchdog cancels them locally.
    Result: a 'missed move' would have become 'cancel + dead local order, no
    replacement at the broker.' Exactly the failure mode the reviewer caught.

    Now: cancel at broker, then immediately call broker.place_order() with
    the new limit. On submit success: local order -> SUBMITTED with the new
    broker_order_id. On submit failure: local order -> CANCELED so the slot
    is freed cleanly (never leave a stuck CREATED row).

    Returns True iff a new broker order is now live.
    """
    broker_oid = order_row.get("broker_order_id")
    local_oid  = order_row.get("id") or order_row.get("local_order_id")
    symbol     = order_row.get("symbol")
    contract   = order_row.get("contract")
    qty        = order_row.get("qty") or order_row.get("quantity")
    if not broker_oid or not local_oid:
        log.warning("apply_repeg: missing ids broker=%s local=%s", broker_oid, local_oid)
        return False
    if not symbol or not contract or qty in (None, "", 0):
        log.warning("apply_repeg: missing required fields local=%s sym=%s contract=%s qty=%s",
                    local_oid, symbol, contract, qty)
        return False
    try:
        qty = int(qty)
    except (TypeError, ValueError):
        log.warning("apply_repeg: bad qty local=%s qty=%r", local_oid, qty)
        return False
    if qty <= 0:
        log.warning("apply_repeg: non-positive qty local=%s qty=%s", local_oid, qty)
        return False

    from ap.db import update_order

    # Step 1: cancel the existing broker order.
    try:
        cancel_result = broker.cancel_order(broker_oid)
        if not cancel_result or (isinstance(cancel_result, dict) and cancel_result.get("error")):
            log.warning("apply_repeg: cancel_order failed broker=%s err=%s",
                        broker_oid, cancel_result)
            return False
    except Exception as e:
        log.warning("apply_repeg: cancel exception broker=%s err=%s", broker_oid, e)
        return False

    # Step 2: mark intent in DB BEFORE resubmit so a process crash mid-flow
    # leaves a clear forensic record (CREATED with bumped repeg_attempts and
    # the old broker_order_id stashed in meta).
    meta = dict(order_row.get("meta") or {})
    meta["repeg_attempts"]       = decision.attempts_used
    meta["last_repeg_ts"]        = time.time()
    meta["last_repeg_reason"]    = decision.reason
    meta["prev_broker_order_id"] = broker_oid
    try:
        update_order(
            local_oid,
            status="CREATED",
            limit_price=decision.new_limit_price,
            meta=meta,
            broker_order_id=None,  # old broker_oid is dead; clear it
        )
    except Exception as e:
        log.error("apply_repeg: meta update failed id=%s err=%s", local_oid, e)
        return False

    # Step 3: submit the NEW order at the new limit price.
    # Pass local_oid as tag so the broker-side record is tied to our ID — if
    # this submit times out mid-response, the next reconciler pass can find
    # the order by tag instead of double-submitting.
    try:
        resp = broker.place_order(
            symbol=symbol,
            contract=contract,
            qty=qty,
            limit_price=decision.new_limit_price,
            side="buy_to_open",
            tag=str(local_oid),
        )
    except Exception as e:
        log.error("[%s] REPEG_RESUBMIT_FAILED local=%s err=%s", client_id, local_oid, e)
        # Old broker order is gone, new submit failed. Free the slot by
        # transitioning to CANCELED so the daily-cap gate counts it as free
        # and the operator sees a clear forensic row.
        try:
            update_order(local_oid, status="CANCELED",
                         last_error=f"repeg_resubmit_exception:{e}")
        except Exception:
            log.exception("[%s] apply_repeg: also failed to mark CANCELED after resubmit failure", client_id)
        return False

    # Parse broker response.
    new_broker_oid = None
    submit_error   = None
    submit_status  = None
    if isinstance(resp, dict):
        new_broker_oid = resp.get("broker_order_id") or resp.get("order_id") or resp.get("id")
        submit_status  = str(resp.get("status") or "").upper()
        submit_error   = resp.get("error")
    else:
        new_broker_oid = getattr(resp, "broker_order_id", None) or getattr(resp, "order_id", None)
        submit_status  = str(getattr(resp, "status", "") or "").upper()
        submit_error   = getattr(resp, "error", None)

    accepted = (not submit_error) and submit_status in (
        "ACK", "ACKED", "FILLED", "SUBMITTED", "OK", "ACCEPTED", "PENDING", "OPEN",
    )
    if not accepted:
        log.error("[%s] REPEG_RESUBMIT_REJECTED local=%s status=%s err=%s",
                  client_id, local_oid, submit_status, submit_error)
        try:
            update_order(local_oid, status="REJECTED",
                         last_error=f"repeg_resubmit_rejected:{submit_status}:{submit_error}")
        except Exception:
            log.exception("[%s] apply_repeg: failed to mark REJECTED after resubmit reject", client_id)
        return False

    # Success path: transition local to SUBMITTED with the new broker id.
    try:
        update_order(
            local_oid,
            status="SUBMITTED",
            broker_order_id=new_broker_oid,
        )
    except Exception as e:
        log.critical(
            "[%s] REPEG_DB_DRIFT local=%s new_broker_oid=%s err=%s -- "
            "broker has a live order at the new limit, DB does not. "
            "Reconciler should resolve next tick.",
            client_id, local_oid, new_broker_oid, e,
        )
        return False

    # P1 ENTRY FIX (2026-05-21): emit explicit entry_attempt= token so the
    # dashboard log-parser can bucket by attempt number.
    # attempts_used semantics (matches spec):
    #   entry_attempt=0 = original ask submit (no repeg)
    #   entry_attempt=1 = first repeg (ask + 0.01 ladder)
    #   entry_attempt=2 = second repeg (ask + 0.02 ladder)
    log.info(
        "[%s] REPEG_APPLIED order=%s entry_attempt=%d old_broker=%s new_broker=%s "
        "prev_limit=%.2f new_limit=%.2f attempt=%d/%d reason=%s",
        client_id, local_oid, decision.attempts_used,
        broker_oid, new_broker_oid,
        decision.detail.get("prev_limit", 0.0),
        decision.new_limit_price,
        decision.attempts_used, REPEG_MAX_ATTEMPTS,
        decision.reason,
    )
    return True
