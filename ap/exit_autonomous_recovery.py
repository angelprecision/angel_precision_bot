"""
ap.exit_autonomous_recovery
===========================
Conservative autonomous recovery helper for APExitEngine quarantine/stale in-flight states.

Safety rules
------------
1. Never clear an exit quarantine by time alone.
2. Never authorize replacement if ANY matching live broker exit order is found.
3. Prefer fill/close truth when broker/DB evidence exists.
4. On ambiguous matching live exits, try broker cancel first and only unlock after
   cancel proof or terminal broker confirmation.
5. If broker truth is ambiguous, alert/no-op.
6. Quote staleness is reported as a kill-switch signal for new entries, not a
   reason to guess exit truth.
"""

from __future__ import annotations

import logging
import math
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

# PR #481 amendment: import the canonical broker-truth resolver so that
# autonomous recovery consumes the same quantity-conflict semantics as the
# rest of the exit pipeline — one definition of "broker flat", not two.
#
# PR #481 amendment 4 (blockers 1 & 2): also import the canonical exact-OCC
# contract validator so every contract-based decision in this module uses
# the same proven-identity predicate as the resolver — one definition of
# "this is a real contract identity", not two.
from ap.exit_safety import resolve_exit_broker_truth, _normalize_exact_occ_contract

log = logging.getLogger("ap.exit_autonomous_recovery")

OPEN_BROKER_STATUSES = {"open", "pending", "accepted", "submitted", "queued", "working", "acknowledged", "partially_filled"}
TERMINAL_BROKER_STATUSES = {"filled", "canceled", "cancelled", "rejected", "expired"}
CANCEL_CONFIRMED_STATUSES = {"canceled", "cancelled", "rejected", "expired"}
QUOTE_STALE_WARN_SEC = int(os.getenv("EXIT_RECOVERY_QUOTE_STALE_SEC", "30"))
CANCEL_PROOF_RETRIES = int(os.getenv("EXIT_RECOVERY_CANCEL_RETRIES", "3"))
CANCEL_PROOF_DELAY_SEC = float(os.getenv("EXIT_RECOVERY_CANCEL_DELAY_SEC", "1.0"))


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _norm(value: Any) -> str:
    return str(value or "").strip()


def _norm_contract(value: Any) -> str:
    return _norm(value).upper().replace(" ", "")


def _status(raw: dict) -> str:
    return _norm(raw.get("status") or raw.get("Status") or raw.get("state") or raw.get("order_status")).lower()


def _broker_order_id(raw: dict) -> str:
    return _norm(raw.get("broker_order_id") or raw.get("order_id") or raw.get("id") or raw.get("orderId"))


def _contract(raw: dict) -> str:
    # P0 amendment 4 (blocker 2): the raw underlying `symbol` must NEVER
    # shadow a valid `option_symbol`. Real Tradier option orders carry TWO
    # distinct fields -- symbol=underlying (e.g. "SMCI"), option_symbol=
    # exact OCC contract (e.g. "SMCI260626P00032500") -- and the previous
    # `raw.get("contract") or raw.get("symbol") or raw.get("option_symbol")
    # or raw.get("instrument")` short-circuited on the first truthy field,
    # returning the underlying instead of the exact OCC contract for any
    # production-shaped order row. That could cause an already-live
    # same-contract exit order to be missed by exact-OCC matching
    # elsewhere in this module, risking a duplicate exit submission.
    #
    # Fix: check each candidate field in priority order, but only ACCEPT a
    # candidate if it proves out as a complete exact OCC option symbol.
    # A bare underlying ticker fails that proof and is skipped rather than
    # blindly accepted via short-circuit `or`.
    for key in ("contract", "option_symbol", "symbol", "instrument"):
        candidate = _normalize_exact_occ_contract(raw.get(key))
        if candidate:
            return candidate
    return ""


def _qty(raw: dict) -> int:
    for key in ("qty", "quantity", "order_qty", "remaining_qty", "remaining_quantity", "filled_qty", "filled_quantity", "exec_quantity"):
        try:
            val = raw.get(key)
            if val not in (None, ""):
                return abs(int(float(val)))
        except Exception:
            pass
    return 0


# P0 (post-#481 final narrow merge-gate amendment, blocker 2): a
# dedicated, strict parser for a broker-confirmed FILLED exit order's
# fill quantity -- deliberately separate from the lenient, magnitude-
# oriented _qty() helper above, which is still used elsewhere in this
# module (order-adoption quantity tracking, not fill-quantity truth) and
# is out of scope for this amendment.
#
# _qty() silently produces a WRONG value for malformed FILLED quantity
# rather than surfacing it as a conflict: a fractional value (0.5)
# truncates to a believable-looking 0 via int(0.5)==0; a negative value
# (-1) sign-flips to a believable-looking 1 via abs(); a boolean (True)
# masquerades as a real quantity of 1. Combined with the FILLED-status
# call site's `filled_qty = _qty(raw) or pending_exit_qty` fallback
# pattern, a fractional/zero-collapsing malformed value silently
# manufactured a FULL close (fell back to the position's entire
# pending_exit_qty), while a sign-flipped/boolean value silently applied
# a WRONG partial fill -- neither is proof of what the broker actually
# filled.
_FILLED_QTY_KEYS = ("quantity", "qty", "filled_qty", "filled_quantity", "exec_quantity")


def _strict_filled_exit_qty(raw: dict) -> tuple:
    """Strict parser for a broker-confirmed FILLED exit order's fill
    quantity. Returns (qty, field_was_present):

      (positive_int, True)  -- one or more recognized keys are present in
        the payload (checked via `key in raw`), every present key's value
        independently proves valid (non-boolean, finite, mathematically
        integral, strictly positive), and if more than one key is
        present, they all agree on the same value.

      (None, False)         -- genuine ABSENCE: none of the recognized
        keys exist in the payload at all. Callers MAY apply an existing,
        narrowly-scoped ABSENT-only fallback (e.g. the position's own
        pending_exit_qty) when the broker order identity is otherwise
        exactly confirmed by broker_order_id + status=="filled".

      (None, True)          -- CONFLICT: at least one recognized key IS
        present but its value is None, an empty/whitespace-only string,
        boolean, zero, negative, fractional (non-integral), non-finite
        (NaN/inf), or unparseable -- OR multiple present keys carry
        individually-valid but mutually disagreeing values (e.g.
        quantity=4 and exec_quantity=1). This is CONFLICT EVIDENCE, not
        absence. Callers must NEVER fall back to pending_exit_qty, apply
        any cumulative fill, call mark_position_closed, or run any
        terminal proof/economics in this case -- hold instead.

    Key presence is checked with `key in raw`, not `raw.get(key)`, so a
    key present with value None/""/whitespace is correctly distinguished
    from a key that is truly absent from the payload -- these previously
    collapsed to the same outcome, letting a present-but-empty field
    silently qualify for the ABSENT-only pending_exit_qty fallback
    instead of holding as the conflict evidence it actually is.

    Every recognized key that IS present is inspected (not just the
    first) so that a valid secondary field can never mask a malformed
    primary field, and so that mutually disagreeing present fields
    (e.g. a generic order-quantity field and a fill-specific field that
    report different amounts) are detected as conflicting broker
    evidence rather than one silently overriding the other.
    """
    present_keys = [key for key in _FILLED_QTY_KEYS if key in raw]
    if not present_keys:
        return None, False

    valid_values: set = set()
    for key in present_keys:
        val = raw[key]
        if val is None:
            return None, True
        if isinstance(val, str) and val.strip() == "":
            return None, True
        if isinstance(val, bool):
            return None, True
        try:
            qty_float = float(val)
        except (TypeError, ValueError):
            return None, True
        if not math.isfinite(qty_float):
            return None, True
        if not qty_float.is_integer():
            return None, True
        qty = int(qty_float)
        if qty <= 0:
            return None, True
        valid_values.add(qty)

    if len(valid_values) != 1:
        # Multiple present fields disagree on the value -- conflicting
        # broker evidence, never silently pick one.
        return None, True

    return valid_values.pop(), True


def _is_exit_like(raw: dict) -> bool:
    text = " ".join(str(raw.get(k) or "") for k in (
        "side", "action", "instruction", "order_action", "transaction_type", "trade_action", "type", "description", "memo", "notes"
    )).lower()
    compact = text.replace("_", "").replace("-", "").replace(" ", "")
    return "selltoclose" in compact or compact == "stc" or "sell to close" in text


def _dt_age_seconds(dt: Any) -> Optional[float]:
    if not dt:
        return None
    try:
        if getattr(dt, "tzinfo", None) is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return max(0.0, (_now() - dt).total_seconds())
    except Exception:
        return None


def _looks_like_broker_order_row(d: dict) -> bool:
    """A dict returned from a broker order-query method is only accepted
    as a genuine single order row if it carries actual broker-order
    identity. Without this check, error-shaped payloads such as
    ``{"error": "rate_limited"}``, ``{"message": "broker unavailable"}``,
    ``{"status": "ERROR", "reason": "timeout"}``, or an empty ``{}`` would
    be silently wrapped as ``[result]`` and treated as a successful,
    authoritative single-order snapshot. Downstream contract/status
    filtering would then discard that fake row (it matches no real
    contract and has no real status), producing a confirmed-empty ``[]``
    match list that looks exactly like a genuine "broker confirms zero
    open orders" result -- even though the broker query actually failed
    or returned garbage. That confirmed-but-wrong empty could then
    authorize replacement-safe as though a real scan had proven no live
    exit exists.
    """
    if not isinstance(d, dict) or not d:
        return False
    # An explicit error/message key is disqualifying on its own, even if
    # the payload happens to also carry an id-like field by coincidence.
    if any(k in d for k in ("error", "errors", "message")):
        return False
    status_val = str(d.get("status") or "").strip().lower()
    if status_val in ("error", "fail", "failed", "failure"):
        return False
    return bool(_broker_order_id(d))


def _list_open_orders(broker: Any) -> Optional[list[dict]]:
    """Query the broker for open orders.

    P0 amendment 5 (blocker 2): broker-order truth must be tri-state, not
    binary. This function returns exactly one of:

      - a list (possibly empty)  -> AVAILABLE. A query succeeded and
        returned a usable payload. An empty list is authoritative negative
        proof: the broker confirms zero open orders exist.
      - ``None``                 -> UNKNOWN / UNAVAILABLE. Every candidate
        query method was either missing, raised, returned ``None``, or
        returned a payload shape this function cannot interpret. This is
        NOT proof that zero matching exit orders exist -- callers must
        NEVER collapse this into ``[]`` and treat it as negative broker-
        order proof. Doing so previously let a broker query failure be
        silently reinterpreted as "no live exit exists", authorizing a
        duplicate replacement exit while the real one was still working
        at the broker.
    """
    for method_name in ("list_open_orders", "get_open_orders", "list_orders", "orders"):
        method = getattr(broker, method_name, None)
        if not callable(method):
            continue
        try:
            try:
                result = method(status="open")
            except TypeError:
                result = method()
            if result is None:
                log.warning("broker.%s returned None during autonomous recovery", method_name)
                continue
            if isinstance(result, dict):
                for key in ("orders", "data", "results", "items"):
                    if isinstance(result.get(key), list):
                        return [dict(x) for x in result[key] if isinstance(x, dict)]
                if _looks_like_broker_order_row(result):
                    return [result]
                # An unrecognized/error-like/empty dict is NOT authoritative
                # order truth. It is not a recognized container, and it does
                # not carry genuine broker-order identity -- accepting it as
                # [result] would let a broker error response masquerade as a
                # successful single-order snapshot, which downstream
                # contract/status filtering would then reduce to a
                # confirmed-empty match list indistinguishable from a real
                # "broker confirms zero open orders" result.
                log.warning(
                    "broker.%s returned an unrecognized/error-like dict "
                    "shape during autonomous recovery: keys=%s",
                    method_name, sorted(result.keys()),
                )
                continue
            if isinstance(result, list):
                return [dict(x) for x in result if isinstance(x, dict)]
            # A non-None result that isn't a recognized dict/list shape is
            # unusable -- not the same as a confirmed-empty response.
            log.warning(
                "broker.%s returned unusable payload type=%s during autonomous recovery",
                method_name, type(result).__name__,
            )
        except Exception as exc:
            log.warning("broker.%s failed during autonomous recovery: %s", method_name, exc)
    # Every candidate method was unavailable, raised, returned None, or
    # returned an unusable shape. Broker-order truth is UNKNOWN -- never
    # normalize this to [] and use it as negative proof.
    return None


def _get_order(broker: Any, broker_order_id: str) -> Optional[dict]:
    if not broker_order_id:
        return None
    method = getattr(broker, "get_order", None)
    if not callable(method):
        log.warning("broker.get_order missing during autonomous recovery")
        return None
    try:
        raw = method(broker_order_id)
        if isinstance(raw, dict):
            return dict(raw)
        log.warning("broker.get_order(%s) returned non-dict payload: %r", broker_order_id, raw)
        return None
    except Exception as exc:
        log.warning("broker.get_order(%s) failed: %s", broker_order_id, exc)
        return None


def _matching_open_exit_orders(broker: Any, contract: str, *, exclude_broker_id: str = "") -> Optional[list[tuple[str, dict]]]:
    # P0 amendment 3: defense-in-depth. An empty/unproven contract identity
    # must NEVER act as a wildcard match across every open exit-like broker
    # order in the account. Without this guard, `if contract and ...` short-
    # circuits to False for an empty contract and no row is ever filtered
    # out — every live sell-to-close order in the account (belonging to
    # ANY position, ANY client) would match. Callers must still perform
    # their own explicit contract-identity check before calling this
    # helper (see recover_exit_position) rather than relying on this
    # early-return alone: an empty result here must not be silently
    # reinterpreted by a caller as "no matches -> replacement-safe" when
    # the real reason is "identity unknown, scan never meaningfully ran".
    #
    # This early return is a confirmed EMPTY (by construction: we refuse
    # to scan at all), not UNKNOWN -- it is a distinct invariant from
    # amendment 5's broker-order-query tri-state below, and takes
    # precedence over it.
    if not contract:
        return []
    # P0 amendment 5 (blocker 2): broker-order query truth is tri-state.
    # _list_open_orders() returns None for UNKNOWN/UNAVAILABLE (query
    # failure, no usable method, unusable payload) -- this must propagate
    # as None here too, never be silently treated as "confirmed zero open
    # orders". Every caller of this function must check `is None`
    # explicitly rather than relying on a falsy/empty-list check, which
    # cannot distinguish "confirmed no matches" from "never actually
    # looked".
    open_orders = _list_open_orders(broker)
    if open_orders is None:
        return None
    matches: list[tuple[str, dict]] = []
    for raw in open_orders:
        if contract and _contract(raw) != contract:
            continue
        if not _is_exit_like(raw):
            continue
        st = _status(raw)
        if st and st not in OPEN_BROKER_STATUSES:
            continue
        bid = _broker_order_id(raw)
        if not bid or (exclude_broker_id and bid == exclude_broker_id):
            continue
        matches.append((bid, raw))
    return matches


def _cancel_order_with_proof(
    broker: Any,
    broker_order_id: str,
    *,
    max_retries: int = CANCEL_PROOF_RETRIES,
    retry_delay: float = CANCEL_PROOF_DELAY_SEC,
) -> tuple[bool, dict]:
    """Attempt broker cancel and wait briefly for terminal/canceled proof."""
    if not broker_order_id:
        return False, {"error": "missing_broker_order_id"}
    cancel = getattr(broker, "cancel_order", None)
    if not callable(cancel):
        return False, {"error": "broker_cancel_order_missing", "broker_order_id": broker_order_id}
    try:
        raw = cancel(broker_order_id)
        raw = dict(raw) if isinstance(raw, dict) else {"raw": raw}
    except Exception as exc:
        return False, {"error": str(exc), "broker_order_id": broker_order_id}

    status_val = _status(raw)
    ok_flag = bool(raw.get("ok"))
    confirmed_status = status_val
    confirmed_payload: Optional[dict] = None

    for attempt in range(max(1, int(max_retries))):
        confirmed = _get_order(broker, broker_order_id)
        confirmed_payload = confirmed
        confirmed_status = _status(confirmed or {}) if confirmed else status_val
        if confirmed_status in CANCEL_CONFIRMED_STATUSES:
            raw["confirmed_status"] = confirmed_status
            raw["confirmation_attempts"] = attempt + 1
            return True, raw
        if attempt < max_retries - 1:
            time.sleep(max(0.0, float(retry_delay)))

    # If broker accepted cancel but status has not propagated, do NOT unlock.
    raw["confirmed_status"] = confirmed_status
    raw["confirmation_attempts"] = max_retries
    raw["confirmed_payload"] = confirmed_payload
    raw["ok_flag"] = ok_flag
    return False, raw


def _position_contract(pos: Any) -> str:
    # P0 amendment 4 (blocker 1): a bare `_norm_contract()` here only
    # strips/uppercases/removes spaces -- it does not prove the position's
    # option_symbol/contract/symbol attribute is a complete exact OCC
    # option identity. A non-empty but malformed value ("UNKNOWN", a bare
    # underlying ticker, a placeholder) would previously pass through as
    # "identity established" and could reach every contract-based decision
    # in recover_exit_position() below. Using the same validated normalizer
    # as ap.exit_safety.resolve_exit_broker_truth() means every existing
    # `if not contract:` HOLD guard already in this module (from
    # amendments 2 and 3) now also correctly fires for invalid non-empty
    # identity, with no new guard code required.
    for attr in ("option_symbol", "contract", "symbol"):
        candidate = _normalize_exact_occ_contract(getattr(pos, attr, ""))
        if candidate:
            return candidate
    return ""


def _position_id(pos: Any) -> str:
    return _norm(getattr(pos, "position_id", "") or getattr(pos, "id", ""))


def _pending_identity(pos: Any) -> tuple[str, str]:
    return (
        _norm(getattr(pos, "pending_exit_local_order_id", "")),
        _norm(getattr(pos, "pending_exit_broker_order_id", "")),
    )


def quote_health(pos: Any, *, stale_sec: int = QUOTE_STALE_WARN_SEC) -> dict:
    option_age = _dt_age_seconds(getattr(pos, "last_option_quote_update_ts", None) or getattr(pos, "last_quote_update_ts", None))
    underlying_age = _dt_age_seconds(getattr(pos, "last_underlying_quote_update_ts", None) or getattr(pos, "last_quote_update_ts", None))
    option_missing = getattr(pos, "last_option_quote_missing_ts", None) is not None
    underlying_missing = getattr(pos, "last_underlying_quote_missing_ts", None) is not None
    return {
        "option_quote_age_sec": option_age,
        "underlying_quote_age_sec": underlying_age,
        "option_quote_stale": option_age is None or option_age > stale_sec or bool(option_missing),
        "underlying_quote_stale": underlying_age is None or underlying_age > stale_sec or bool(underlying_missing),
        "stale_sec": stale_sec,
    }


@dataclass
class RecoveryAction:
    action: str
    reason: str
    position_id: str = ""
    local_order_id: str = ""
    broker_order_id: str = ""
    details: dict = field(default_factory=dict)


def _mark_replacement_safe(exit_engine: Any, pid: str, *, reason: str, local_id: str, broker_id: str, details: dict) -> RecoveryAction:
    if exit_engine and hasattr(exit_engine, "mark_exit_replacement_safe"):
        exit_engine.mark_exit_replacement_safe(
            pid,
            reason=reason,
            local_order_id=local_id,
            broker_order_id=broker_id,
            reconciled=True,
        )
        return RecoveryAction("REPLACEMENT_SAFE", reason, pid, local_id, broker_id, details)
    if exit_engine and hasattr(exit_engine, "clear_exit_in_flight"):
        exit_engine.clear_exit_in_flight(
            pid,
            reason=reason,
            local_order_id=local_id,
            broker_order_id=broker_id,
            reconciled=True,
        )
        return RecoveryAction("CLEARED_IN_FLIGHT", reason, pid, local_id, broker_id, details)
    return RecoveryAction("NOOP", "no_replacement_or_clear_hook_available", pid, local_id, broker_id, details)


def recover_exit_position(pos: Any, *, broker: Any, exit_engine: Any = None, osm: Any = None) -> RecoveryAction:
    pid = _position_id(pos)
    local_id, pending_broker_id = _pending_identity(pos)
    contract = _position_contract(pos)
    qh = quote_health(pos)

    if not broker or not pid:
        return RecoveryAction("NOOP", "missing_broker_or_position_id", pid, local_id, pending_broker_id, {"quote_health": qh})

    # Exact broker identity path.
    if pending_broker_id:
        raw = _get_order(broker, pending_broker_id)
        if raw:
            st = _status(raw)
            if st in OPEN_BROKER_STATUSES:
                if exit_engine and hasattr(exit_engine, "set_pending_exit_order"):
                    exit_engine.set_pending_exit_order(
                        pid,
                        local_order_id=local_id,
                        broker_order_id=pending_broker_id,
                        qty=int(getattr(pos, "pending_exit_qty", 0) or 0),
                        reason="autonomous_recovery_confirmed_broker_open_exit",
                    )
                return RecoveryAction("CONFIRMED_OPEN", "broker_order_still_open", pid, local_id, pending_broker_id, {"status": st, "quote_health": qh})
            if st == "filled":
                # P0 (post-#481 final narrow merge-gate amendment,
                # blocker 2): FILLED status and FILLED quantity are
                # separate broker truth fields. The previous
                # `filled_qty = _qty(raw) or pending_exit_qty` pattern let
                # a malformed-but-present quantity (fractional, boolean,
                # negative, non-finite, unparseable) either silently
                # produce a WRONG nonzero value or collapse to a falsy 0
                # that triggered the pending_exit_qty fallback --
                # manufacturing a full close (or a wrong partial fill)
                # from broker data that never actually proved that
                # amount. MALFORMED is conflict evidence, not absence: it
                # must HOLD, never fall back, never apply a fill.
                _strict_qty, _qty_field_present = _strict_filled_exit_qty(raw)
                if _strict_qty is None and _qty_field_present:
                    log.warning(
                        "exit_autonomous_recovery: pid=%s broker FILLED order "
                        "broker_order_id=%s reported a present-but-malformed/"
                        "conflicting fill quantity — NOOP/HOLD rather than "
                        "manufacture a fill from unproven quantity truth",
                        pid, pending_broker_id,
                    )
                    return RecoveryAction(
                        "NOOP", "broker_filled_quantity_malformed_hold",
                        pid, local_id, pending_broker_id,
                        {"status": st, "quote_health": qh},
                    )
                if _strict_qty is not None:
                    filled_qty = _strict_qty
                else:
                    # Genuine ABSENCE (no recognized quantity field at
                    # all): the existing exact-broker-order-identity
                    # fallback to this position's own pending_exit_qty is
                    # preserved -- broker_order_id + status=="filled" for
                    # the EXACT order this position submitted is already
                    # confirmed at this point in the exact-broker-identity
                    # path, so inferring the fill quantity from what we
                    # ourselves submitted for that specific order is a
                    # narrowly-scoped, provably-bounded fallback, not a
                    # guess from unrelated data.
                    filled_qty = int(getattr(pos, "pending_exit_qty", 0) or 0)
                fill_price = None
                for key in ("avg_fill_price", "average_fill_price", "fill_price", "filled_avg_price", "price"):
                    try:
                        if raw.get(key) not in (None, ""):
                            fill_price = float(raw.get(key))
                            break
                    except Exception:
                        pass
                # P0 amendment 5 (blocker 3): a broker "filled" status on
                # the pending exit order does NOT by itself prove the
                # position's ENTIRE remaining exposure was closed. A
                # SCALE_OUT tranche can independently report "filled" for
                # just that tranche's quantity while genuine broker
                # exposure remains open. Blindly calling
                # mark_position_closed() here set quantity_remaining=0 and
                # pos.closed=True unconditionally, which could silently
                # drop real remaining exposure from management.
                #
                # Reuse the existing canonical partial-fill/full-close
                # classification (note_partial_exit_fill) instead of
                # duplicating a second scale-out algorithm here. That
                # helper is idempotent per order-key cumulative-fill
                # tracking (safe against duplicate FILLED callbacks/
                # recovery passes), reduces quantity_remaining by the
                # actual fill delta, and only marks the position closed
                # when quantity_remaining reaches zero -- exactly the
                # semantics this recovery path needs.
                if exit_engine and hasattr(exit_engine, "note_partial_exit_fill"):
                    # Pass filled_qty as CUMULATIVE for this specific
                    # broker_order_id, not as an incremental qty_filled
                    # delta. A single Tradier order's reported fill
                    # quantity at "filled" terminal status is fixed/final
                    # for that order_key -- using the cumulative-tracking
                    # path in note_partial_exit_fill (keyed by
                    # broker_order_id) makes a duplicate FILLED callback
                    # or duplicate recovery pass for the SAME order
                    # correctly compute delta=0 (already applied) instead
                    # of double-decrementing quantity_remaining. Passing
                    # this as a plain qty_filled increment would NOT be
                    # idempotent against a repeated call.
                    exit_engine.note_partial_exit_fill(
                        pid,
                        cumulative_filled=filled_qty,
                        fill_price=fill_price,
                        local_order_id=local_id,
                        broker_order_id=pending_broker_id,
                    )
                    if bool(getattr(pos, "closed", False)):
                        return RecoveryAction(
                            "MARKED_CLOSED", "broker_order_filled_full_close",
                            pid, local_id, pending_broker_id,
                            {"status": st, "filled_qty": filled_qty, "quote_health": qh},
                        )
                    return RecoveryAction(
                        "PARTIAL_FILL_APPLIED", "broker_order_filled_partial_scale_out",
                        pid, local_id, pending_broker_id,
                        {"status": st, "filled_qty": filled_qty, "quote_health": qh},
                    )

                # Fallback for an exit_engine that does not implement the
                # canonical partial-fill helper. Only allow a full close
                # when the fill quantity is PROVEN to consume all
                # remaining exposure (quantity_remaining is tracked and
                # the fill covers it); otherwise never fabricate a full
                # close from an uncertain/partial fill -- hold instead.
                # When quantity_remaining isn't tracked on this position
                # object at all, we have no better information than the
                # pre-amendment behavior, so we fall through to the
                # existing mark_position_closed() call unchanged.
                _remaining_before = getattr(pos, "quantity_remaining", None)
                if _remaining_before is not None and int(filled_qty) < int(_remaining_before or 0):
                    log.warning(
                        "exit_autonomous_recovery: pid=%s broker FILLED qty=%s is less than "
                        "quantity_remaining=%s and no canonical partial-fill helper is "
                        "available on this exit_engine — NOOP/HOLD rather than fabricate a full close",
                        pid, filled_qty, _remaining_before,
                    )
                    return RecoveryAction(
                        "NOOP", "broker_order_filled_partial_no_canonical_handler_hold",
                        pid, local_id, pending_broker_id,
                        {"status": st, "filled_qty": filled_qty, "quantity_remaining": _remaining_before, "quote_health": qh},
                    )
                if exit_engine and hasattr(exit_engine, "mark_position_closed"):
                    exit_engine.mark_position_closed(
                        pid,
                        reason="AUTONOMOUS_RECOVERY_BROKER_FILLED",
                        qty_filled=filled_qty,
                        fill_price=fill_price,
                        local_order_id=local_id,
                        broker_order_id=pending_broker_id,
                        cumulative_filled=filled_qty,
                        reconciled=True,
                    )
                return RecoveryAction("MARKED_CLOSED", "broker_order_filled", pid, local_id, pending_broker_id, {"status": st, "filled_qty": filled_qty, "quote_health": qh})
            if st in TERMINAL_BROKER_STATUSES:
                # CRITICAL safety: terminal status for the old pending id is NOT enough.
                # Scan broker for a different live exit on the same contract before allowing replacement.
                #
                # P0 amendment 3: this scan must never run against an unproven
                # contract identity. _matching_open_exit_orders() is now
                # hardened to return [] for an empty contract (defense in
                # depth), but that alone is NOT safe here: an empty result
                # would fall through to len(other_matches) == 0 below, which
                # authorizes _mark_replacement_safe() from terminal status
                # alone -- exactly the "duplicate scan cannot be performed,
                # so let's assume it's safe" failure mode this amendment
                # forbids. Hold explicitly instead of letting that fallthrough
                # fire.
                if not contract:
                    log.warning(
                        "exit_autonomous_recovery: contract identity unestablished for pid=%s "
                        "during terminal-status duplicate-exit scan — NOOP/HOLD",
                        pid,
                    )
                    return RecoveryAction(
                        "NOOP",
                        "broker_contract_identity_unknown_hold",
                        pid, local_id, pending_broker_id,
                        {"status": st, "quote_health": qh},
                    )
                other_matches = _matching_open_exit_orders(broker, contract, exclude_broker_id=pending_broker_id)
                # P0 amendment 5 (blocker 2): broker-order truth is
                # UNKNOWN -- the query failed, no usable method existed,
                # or the payload was unusable. This must NEVER be treated
                # as "confirmed no other live exit exists". Without this
                # check, `len(other_matches)` on a None would raise, or
                # (if this were ever weakened to a bare `if not
                # other_matches` check) None would be silently treated
                # exactly like a confirmed-empty [] and fall through to
                # _mark_replacement_safe() below -- authorizing a
                # duplicate replacement exit while the real one might
                # still be working at the broker, unseen only because the
                # query failed.
                if other_matches is None:
                    log.warning(
                        "exit_autonomous_recovery: broker open-order truth UNKNOWN for pid=%s "
                        "during terminal-status duplicate-exit scan — NOOP/HOLD",
                        pid,
                    )
                    return RecoveryAction(
                        "NOOP",
                        "broker_order_truth_unknown_hold",
                        pid, local_id, pending_broker_id,
                        {"status": st, "quote_health": qh},
                    )
                if len(other_matches) == 1:
                    other_bid, other_raw = other_matches[0]
                    if exit_engine and hasattr(exit_engine, "set_pending_exit_order"):
                        exit_engine.set_pending_exit_order(
                            pid,
                            local_order_id=local_id,
                            broker_order_id=other_bid,
                            qty=int(_qty(other_raw) or getattr(pos, "pending_exit_qty", 0) or 0),
                            reason="autonomous_recovery_found_different_open_exit",
                        )
                    return RecoveryAction("CONFIRMED_OPEN", "different_broker_exit_still_open", pid, local_id, other_bid, {"old_status": st, "contract": contract, "quote_health": qh})
                if len(other_matches) > 1:
                    return RecoveryAction("NOOP", "multiple_different_open_exits_block_replacement", pid, local_id, pending_broker_id, {"old_status": st, "matches": [m[0] for m in other_matches], "quote_health": qh})
                return _mark_replacement_safe(
                    exit_engine,
                    pid,
                    reason=f"autonomous_recovery_broker_terminal_{st}",
                    local_id=local_id,
                    broker_id=pending_broker_id,
                    details={"status": st, "quote_health": qh},
                )
            return RecoveryAction("NOOP", "broker_order_ambiguous_status", pid, local_id, pending_broker_id, {"status": st, "quote_health": qh})

    # Missing broker id: scan open orders for matching exit order.
    #
    # P0 amendment 3: this path is also reached when pending_broker_id WAS
    # set but the exact-order-id lookup failed/was unavailable (raw was
    # falsy above, so none of the `if raw:` branches returned) -- the
    # "lookup unavailable" case is the same fallthrough as "no pending
    # broker id at all". Either way, this scan must never run against an
    # unproven contract identity: _matching_open_exit_orders() is hardened
    # to return [] for an empty contract, but relying on that alone would
    # let an empty result fall through to the "no matching open sell-to-
    # close order" negative-proof block below as if the scan had
    # meaningfully run and found nothing -- it never actually looked.
    # Hold explicitly instead.
    if not contract:
        log.warning(
            "exit_autonomous_recovery: contract identity unestablished for pid=%s "
            "before open-order scan — NOOP/HOLD",
            pid,
        )
        return RecoveryAction(
            "NOOP",
            "broker_contract_identity_unknown_hold",
            pid, local_id, pending_broker_id,
            {"quote_health": qh},
        )

    matches = _matching_open_exit_orders(broker, contract)

    # P0 amendment 5 (blocker 2): broker-order truth is UNKNOWN -- must
    # NEVER be treated as "confirmed zero matching exit orders". Without
    # this explicit check, a None here would fall through toward the
    # negative-proof block below as though the scan had genuinely found
    # nothing, when it never actually looked.
    if matches is None:
        log.warning(
            "exit_autonomous_recovery: broker open-order truth UNKNOWN for pid=%s "
            "during open-order scan — NOOP/HOLD",
            pid,
        )
        return RecoveryAction(
            "NOOP",
            "broker_order_truth_unknown_hold",
            pid, local_id, pending_broker_id,
            {"contract": contract, "quote_health": qh},
        )

    if len(matches) == 1:
        recovered_broker_id, raw = matches[0]
        if exit_engine and hasattr(exit_engine, "set_pending_exit_order"):
            exit_engine.set_pending_exit_order(
                pid,
                local_order_id=local_id,
                broker_order_id=recovered_broker_id,
                qty=int(getattr(pos, "pending_exit_qty", 0) or _qty(raw) or 0),
                reason="autonomous_recovery_matched_live_exit_order",
            )
        return RecoveryAction("RECOVERED_BROKER_ID", "matched_single_live_exit_order", pid, local_id, recovered_broker_id, {"contract": contract, "quote_health": qh})

    if len(matches) > 1:
        cancel_results = []
        all_canceled = True
        for bid, raw in matches:
            ok, proof = _cancel_order_with_proof(broker, bid)
            cancel_results.append({"broker_order_id": bid, "ok": ok, "proof": proof})
            if not ok:
                all_canceled = False
        if all_canceled:
            return _mark_replacement_safe(
                exit_engine,
                pid,
                reason="autonomous_recovery_multiple_live_exit_orders_canceled",
                local_id=local_id,
                broker_id="",
                details={"match_count": len(matches), "cancel_results": cancel_results, "quote_health": qh},
            )
        return RecoveryAction("NOOP", "multiple_live_exit_orders_cancel_not_proven", pid, local_id, "", {"match_count": len(matches), "cancel_results": cancel_results, "quote_health": qh})

    # Negative proof: no matching open sell-to-close order currently at broker.
    # Before acting, verify the contract's position truth via the canonical resolver.
    #
    # PR #481 amendment — do NOT re-implement exact-contract quantity semantics
    # locally.  The previous try/except block called broker.list_positions() and
    # evaluated int(p.get("quantity") or 0) != 0, which silently collapsed an
    # explicit-zero row, a boolean-False row, or a fractional row to
    # _contract_held=False and called mark_position_closed() — real live exposure
    # was falsely terminalized.  The exception path fell through to
    # _mark_replacement_safe(), so a failed broker query could by itself authorize
    # replacement — also forbidden.
    #
    # resolve_exit_broker_truth() is the single canonical definition of "broker
    # flat" for this contract.  It enforces all PR #481 quantity-conflict guards.
    #
    # Decision table (amendment spec §REQUIRED BOUNDED REPAIR):
    #   missing/unproven contract                → UNKNOWN → NOOP / HOLD
    #   broker_truth_open_qty=None               → UNKNOWN → NOOP / HOLD
    #   broker_truth_open_qty=0, is_fresh_exact  → authoritative flat → mark_position_closed()
    #   broker_truth_open_qty>0, is_fresh_exact  → position held → replacement-safe
    #
    # P0 amendment 2: defense-in-depth against missing contract identity.
    # Do NOT rely solely on the resolver's internal guard — a position
    # whose exact OCC contract cannot be established must hold here too,
    # before resolve_exit_broker_truth() is even called.
    if not contract:
        log.warning(
            "exit_autonomous_recovery: contract identity unestablished for pid=%s — NOOP/HOLD",
            pid,
        )
        return RecoveryAction(
            "NOOP",
            "broker_contract_identity_unknown_hold",
            pid, local_id, "",
            {"quote_health": qh},
        )

    _client_id = str(getattr(pos, "client_id", "") or "")
    _bt = resolve_exit_broker_truth(broker=broker, client_id=_client_id, contract=contract)
    _bt_qty: Any = _bt.get("broker_truth_open_qty")
    _bt_exact: bool = bool(_bt.get("is_fresh_exact", False))
    _bt_audit: dict = _bt.get("audit", {})

    if _bt_qty is None:
        # Broker position truth is UNKNOWN: exception, malformed payload,
        # conflicting / explicit-zero / boolean / fractional exact-match row,
        # or adapter error.  UNKNOWN position truth must NEVER authorize
        # mark_position_closed() or _mark_replacement_safe() on its own.
        log.warning(
            "exit_autonomous_recovery: broker position truth UNKNOWN for pid=%s "
            "contract=%s — NOOP/HOLD; snapshot_status=%s",
            pid, contract, _bt_audit.get("snapshot_status", ""),
        )
        return RecoveryAction(
            "NOOP",
            "broker_position_truth_unknown_hold",
            pid, local_id, "",
            {"contract": contract, "quote_health": qh, "broker_truth_audit": _bt_audit},
        )

    if _bt_qty == 0 and _bt_exact:
        # Authoritative broker flat: contract absent from a successful fresh
        # snapshot.  Only this path may call mark_position_closed() from the
        # negative-proof block — the exact-OCC row is genuinely absent, not
        # merely zero/malformed.
        if exit_engine and hasattr(exit_engine, "mark_position_closed"):
            exit_engine.mark_position_closed(
                pid, exit_price=None, filled_qty=getattr(pos, "contracts", 0)
            )
        return RecoveryAction(
            "MARKED_CLOSED",
            "autonomous_recovery_contract_flat_at_broker",
            pid, local_id, "",
            {
                "contract": contract,
                "quote_health": qh,
                "source": "negative_proof_position_check",
                "broker_truth_audit": _bt_audit,
            },
        )

    # _bt_qty > 0: broker confirms position is held (positive integral quantity).
    # No open exit orders exist (we reached this point); position is confirmed
    # held — fall through to replacement-safe so a fresh exit may be submitted.
    return _mark_replacement_safe(
        exit_engine,
        pid,
        reason="autonomous_recovery_no_matching_live_exit_order",
        local_id=local_id,
        broker_id="",
        details={"contract": contract, "quote_health": qh, "broker_truth_audit": _bt_audit},
    )


def recover_exit_engine(exit_engine: Any, *, broker: Any, osm: Any = None, max_positions: int = 10) -> list[RecoveryAction]:
    if exit_engine is None or broker is None:
        return []
    try:
        if hasattr(exit_engine, "active_positions"):
            positions = list(exit_engine.active_positions())
        else:
            positions = [p for p in getattr(exit_engine, "_positions", []) if not getattr(p, "closed", False)]
    except Exception:
        positions = []

    actions: list[RecoveryAction] = []
    for pos in positions[:max_positions]:
        if not (getattr(pos, "exit_identity_quarantine", False) or getattr(pos, "last_callback_identity_missing", False) or getattr(pos, "exit_in_flight", False)):
            continue
        try:
            actions.append(recover_exit_position(pos, broker=broker, exit_engine=exit_engine, osm=osm))
        except Exception as exc:
            log.exception("autonomous recovery failed for pos=%s: %s", _position_id(pos), exc)
            actions.append(RecoveryAction("ERROR", str(exc), _position_id(pos)))
    return actions


def scan_quote_staleness(exit_engine: Any, *, stale_sec: int = QUOTE_STALE_WARN_SEC) -> list[dict]:
    out = []
    if exit_engine is None:
        return out
    try:
        positions = list(exit_engine.active_positions()) if hasattr(exit_engine, "active_positions") else list(getattr(exit_engine, "_positions", []))
    except Exception:
        positions = []
    for pos in positions:
        if getattr(pos, "closed", False):
            continue
        qh = quote_health(pos, stale_sec=stale_sec)
        if qh.get("option_quote_stale") or qh.get("underlying_quote_stale"):
            out.append({
                "position_id": _position_id(pos),
                "ticker": getattr(pos, "ticker", ""),
                "contract": _position_contract(pos),
                **qh,
            })
    return out


__all__ = ["RecoveryAction", "recover_exit_position", "recover_exit_engine", "scan_quote_staleness", "quote_health"]
