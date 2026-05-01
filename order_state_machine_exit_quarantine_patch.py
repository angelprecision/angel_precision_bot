"""
order_state_machine_exit_quarantine_patch.py
===========================================
Runtime safety patch for APOrderStateMachine -> APExitEngine exit hooks.

Fixes two production-risk gaps:
1. EXIT_SUBMITTED with missing broker_order_id is bridged into the exit engine
   as identity_quarantine=True instead of being silently skipped.
2. EXIT_FILLED classification is made more conservative and identity-aware so a
   completed scale-out is not misclassified as a full position close when
   remaining-size context is missing/stale.

Install before APOrderStateMachine instances are used:
    from order_state_machine_exit_quarantine_patch import install_exit_quarantine_patch
    install_exit_quarantine_patch(APOrderStateMachine)
"""

from __future__ import annotations

import logging
from functools import wraps
from typing import Any

log = logging.getLogger("ap.order_state_machine.exit_quarantine_patch")

_PATCH_FLAG = "__ap_exit_quarantine_patch_installed__"
_ORIGINAL_ATTR = "__ap_exit_quarantine_original_handle_hooks__"


def _safe_int(value: Any, default: int | None = 0) -> int | None:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _get_exit_engine(self):
    try:
        from ap.order_state_machine import _get_exit_engine_for_client
        return _get_exit_engine_for_client(getattr(self, "client_id", ""))
    except Exception:
        return None


def _call_exit_engine(ee, name: str, *args, **kwargs) -> bool:
    fn = getattr(ee, name, None)
    if not callable(fn):
        return False
    try:
        fn(*args, **kwargs)
        return True
    except TypeError:
        # Backward compatibility for older hooks: remove newer kwargs and retry once.
        filtered = dict(kwargs)
        for k in ("identity_quarantine", "reconciled", "status"):
            filtered.pop(k, None)
        try:
            fn(*args, **filtered)
            return True
        except Exception:
            log.exception("exit engine hook %s failed after filtered retry", name)
            return True
    except Exception:
        log.exception("exit engine hook %s failed", name)
        return True


def _find_exit_engine_position(ee, position_id: str):
    pid = str(position_id or "")
    if not pid:
        return None
    for attr in ("_positions", "positions", "managed_positions", "open_positions", "active_positions"):
        container = getattr(ee, attr, None)
        if callable(container):
            try:
                container = container()
            except Exception:
                continue
        if container is None:
            continue
        if isinstance(container, dict):
            if pid in container:
                return container[pid]
            values = container.values()
        else:
            values = container
        try:
            for pos in values:
                if str(getattr(pos, "position_id", getattr(pos, "id", "")) or "") == pid:
                    return pos
        except Exception:
            continue
    return None


def _position_qty_context(ee, position_id: str) -> tuple[int | None, int | None]:
    """Return (original_qty, remaining_qty) from live exit-engine state if available."""
    pos = _find_exit_engine_position(ee, position_id)
    if pos is None:
        return None, None
    original = _safe_int(getattr(pos, "quantity", None), None)
    remaining = _safe_int(getattr(pos, "quantity_remaining", None), None)
    return original, remaining


def install_exit_quarantine_patch(osm_cls):
    """Install OSM exit-hook safety patch."""
    if getattr(osm_cls, _PATCH_FLAG, False):
        return osm_cls

    original = getattr(osm_cls, "_handle_exit_engine_hooks", None)
    if not callable(original):
        raise AttributeError("APOrderStateMachine._handle_exit_engine_hooks is missing")

    setattr(osm_cls, _ORIGINAL_ATTR, original)

    @wraps(original)
    def patched(self, *args, **kwargs):
        current = dict(kwargs.get("current") or {})
        new_status = str(kwargs.get("new_status") or "").upper()
        position_id = kwargs.get("position_id") or current.get("position_id")
        broker_order_id = kwargs.get("broker_order_id") or current.get("broker_order_id")
        local_order_id = kwargs.get("local_order_id") or current.get("local_order_id")
        filled_qty = kwargs.get("filled_qty")
        fill_price = kwargs.get("fill_price")
        kind = str(current.get("kind") or "").upper()

        if kind != "EXIT" or not position_id:
            return original(self, *args, **kwargs)

        ee = _get_exit_engine(self)
        if ee is None:
            return original(self, *args, **kwargs)

        order_qty = _safe_int(current.get("qty"), 0) or 0
        prev_filled = _safe_int(current.get("filled_qty"), 0) or 0
        cum_filled = _safe_int(filled_qty, None)

        # Native bridge for accepted-but-missing-identity exits.
        if new_status == "EXIT_SUBMITTED" and not broker_order_id:
            _call_exit_engine(
                ee,
                "set_pending_exit_order",
                str(position_id),
                local_order_id=str(local_order_id or ""),
                broker_order_id="",
                qty=order_qty,
                reason=str(current.get("last_error") or "broker_accepted_missing_order_id_quarantine"),
                identity_quarantine=True,
            )
            log.critical(
                "[%s] EXIT_SUBMITTED missing broker_order_id bridged to exit engine quarantine | order=%s pos=%s",
                getattr(self, "client_id", "?"),
                local_order_id or "?",
                position_id,
            )
            # Do not call original; native original returns early after logging anyway.
            return None

        # Safer EXIT_FILLED classification. If this order's cumulative fill is
        # less than original position size, it cannot be a full position close.
        if new_status == "EXIT_FILLED":
            if cum_filled is None or cum_filled <= 0:
                cum_filled = order_qty
            delta = max(0, int(cum_filled) - int(prev_filled))
            original_qty, remaining_qty = _position_qty_context(ee, str(position_id))

            if delta <= 0:
                _call_exit_engine(
                    ee,
                    "clear_exit_in_flight",
                    str(position_id),
                    local_order_id=str(local_order_id or ""),
                    broker_order_id=str(broker_order_id or ""),
                    reason="osm_exit_filled_no_new_delta",
                )
                return None

            if original_qty is not None and int(cum_filled) < int(original_qty):
                _call_exit_engine(
                    ee,
                    "note_partial_exit_fill",
                    str(position_id),
                    delta,
                    fill_price=fill_price,
                    local_order_id=str(local_order_id or ""),
                    broker_order_id=str(broker_order_id or ""),
                    cumulative_filled=int(cum_filled),
                )
                log.info(
                    "[%s] EXIT_FILLED treated as scale-out by cumulative/original proof | order=%s pos=%s cum=%s original=%s delta=%s",
                    getattr(self, "client_id", "?"),
                    local_order_id or "?",
                    position_id,
                    cum_filled,
                    original_qty,
                    delta,
                )
                return None

            if remaining_qty is None:
                _call_exit_engine(
                    ee,
                    "note_partial_exit_fill",
                    str(position_id),
                    delta,
                    fill_price=fill_price,
                    local_order_id=str(local_order_id or ""),
                    broker_order_id=str(broker_order_id or ""),
                    cumulative_filled=int(cum_filled),
                )
                log.critical(
                    "[%s] EXIT_FILLED remaining/original unknown — conservative partial handling | order=%s pos=%s delta=%s cum=%s",
                    getattr(self, "client_id", "?"),
                    local_order_id or "?",
                    position_id,
                    delta,
                    cum_filled,
                )
                return None

            if delta < int(remaining_qty):
                _call_exit_engine(
                    ee,
                    "note_partial_exit_fill",
                    str(position_id),
                    delta,
                    fill_price=fill_price,
                    local_order_id=str(local_order_id or ""),
                    broker_order_id=str(broker_order_id or ""),
                    cumulative_filled=int(cum_filled),
                )
                return None

            _call_exit_engine(
                ee,
                "mark_position_closed",
                str(position_id),
                reason="EXIT_FILLED",
                qty_filled=delta,
                fill_price=fill_price,
                local_order_id=str(local_order_id or ""),
                broker_order_id=str(broker_order_id or ""),
                cumulative_filled=int(cum_filled),
            )
            return None

        return original(self, *args, **kwargs)

    setattr(osm_cls, "_handle_exit_engine_hooks", patched)
    setattr(osm_cls, _PATCH_FLAG, True)
    return osm_cls


__all__ = ["install_exit_quarantine_patch"]
