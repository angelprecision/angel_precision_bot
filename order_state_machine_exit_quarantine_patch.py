"""
order_state_machine_exit_quarantine_patch.py
===========================================
Runtime compatibility patch for APOrderStateMachine -> APExitEngine quarantine.

Purpose
-------
When a broker appears to accept an EXIT order but no broker_order_id is returned,
OSM parks the order in EXIT_SUBMITTED quarantine. Native OSM hooks often skip the
exit engine because broker_order_id is missing. This patch bridges that gap by
calling APExitEngine.set_pending_exit_order(..., identity_quarantine=True) so the
exit engine treats the accepted/missing-ID exit as an active quarantined pending
exit, not as a failed/no-op callback.

Install before APOrderStateMachine instances are used:
    from order_state_machine_exit_quarantine_patch import install_exit_quarantine_patch
    install_exit_quarantine_patch(APOrderStateMachine)
"""

from __future__ import annotations

import logging
from functools import wraps

log = logging.getLogger("ap.order_state_machine.exit_quarantine_patch")

_PATCH_FLAG = "__ap_exit_quarantine_patch_installed__"
_ORIGINAL_ATTR = "__ap_exit_quarantine_original_handle_hooks__"


def install_exit_quarantine_patch(osm_cls):
    """Install missing-ID EXIT_SUBMITTED -> exit-engine quarantine bridge."""
    if getattr(osm_cls, _PATCH_FLAG, False):
        return osm_cls

    original = getattr(osm_cls, "_handle_exit_engine_hooks", None)
    if not callable(original):
        raise AttributeError("APOrderStateMachine._handle_exit_engine_hooks is missing")

    setattr(osm_cls, _ORIGINAL_ATTR, original)

    @wraps(original)
    def patched(self, *args, **kwargs):
        current = dict(kwargs.get("current") or {})
        new_status = kwargs.get("new_status")
        position_id = kwargs.get("position_id") or current.get("position_id")
        broker_order_id = kwargs.get("broker_order_id") or current.get("broker_order_id")
        local_order_id = kwargs.get("local_order_id") or current.get("local_order_id")

        # Bridge only the exact quarantine gap: an EXIT order marked EXIT_SUBMITTED
        # with no broker identity. Normal broker-ID cases continue through native OSM.
        if str(new_status or "").upper() == "EXIT_SUBMITTED" and not broker_order_id:
            kind = str(current.get("kind") or "").upper()
            if kind == "EXIT" and position_id:
                try:
                    get_exit_engine = getattr(self.__class__, "_get_exit_engine_for_client", None)
                    exit_engine = None
                    if callable(get_exit_engine):
                        exit_engine = get_exit_engine(getattr(self, "client_id", ""))
                    if exit_engine is None:
                        try:
                            from ap.order_state_machine import _get_exit_engine_for_client
                            exit_engine = _get_exit_engine_for_client(getattr(self, "client_id", ""))
                        except Exception:
                            exit_engine = None

                    if exit_engine and hasattr(exit_engine, "set_pending_exit_order"):
                        exit_engine.set_pending_exit_order(
                            str(position_id),
                            local_order_id=str(local_order_id or ""),
                            broker_order_id="",
                            qty=int(current.get("qty") or 0),
                            reason=str(current.get("last_error") or "broker_accepted_missing_order_id_quarantine"),
                            identity_quarantine=True,
                        )
                        log.critical(
                            "[%s] EXIT_SUBMITTED missing broker_order_id bridged to exit engine quarantine | order=%s pos=%s",
                            getattr(self, "client_id", "?"),
                            local_order_id or "?",
                            position_id,
                        )
                except Exception as exc:
                    log.exception(
                        "[%s] exit quarantine bridge failed | order=%s pos=%s err=%s",
                        getattr(self, "client_id", "?"),
                        local_order_id or "?",
                        position_id,
                        exc,
                    )

        return original(self, *args, **kwargs)

    setattr(osm_cls, "_handle_exit_engine_hooks", patched)
    setattr(osm_cls, _PATCH_FLAG, True)
    return osm_cls


__all__ = ["install_exit_quarantine_patch"]
