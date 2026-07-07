from __future__ import annotations

from ap.exit_broker_truth_flat import clear_flat_repair_position_memory

_PATCHED = "_AP_FLAT_REPAIR_RUNTIME_GUARD"


def _is_flat_repair(pos) -> bool:
    pid = str(getattr(pos, "position_id", "") or "")
    if not pid.startswith("broker-repair-"):
        return False
    try:
        qty = int(float(getattr(pos, "quantity_remaining", 0) or 0))
    except Exception:
        qty = 0
    return bool(getattr(pos, "closed", False)) or qty <= 0


def install_repair_position_flat_guard() -> None:
    import ap_exit_engine

    cls = getattr(ap_exit_engine, "APExitEngine", None)
    if cls is None or getattr(cls, _PATCHED, False):
        return

    original = cls._submit_exit_decision

    def guarded(self, pos, decision, *args, **kwargs):
        if _is_flat_repair(pos):
            pid = str(getattr(pos, "position_id", "") or "")
            with self._lock:
                clear_flat_repair_position_memory(pos)
                by_id = getattr(self, "_positions_by_id", None)
                if isinstance(by_id, dict):
                    by_id.pop(pid, None)
            emit = getattr(self, "_emit_exit_event", None)
            if callable(emit):
                emit(
                    pos,
                    decision="CLOSED",
                    reason_code="BROKER_TRUTH_POSITION_FLAT",
                    explanation="Flat broker-repair position cleared before callback.",
                    stage="exit_reconciliation",
                    extra_inputs={"position_id": pid},
                )
            return False
        return original(self, pos, decision, *args, **kwargs)

    cls._submit_exit_decision = guarded
    setattr(cls, _PATCHED, True)
