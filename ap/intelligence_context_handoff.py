from __future__ import annotations

import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable

log = logging.getLogger("ap.intelligence_context_handoff")

_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="intelligence-handoff")
_CAPACITY = threading.BoundedSemaphore(128)


def _log_result(future: Future, *, phase: str, signal_id: str) -> None:
    try:
        result = future.result()
        if not result.get("ok"):
            log.warning("%s intelligence enqueue failed signal_id=%s reason=%s",
                        phase, signal_id, result.get("error"))
    except Exception as exc:
        log.warning("%s intelligence enqueue error signal_id=%s: %s",
                    phase, signal_id, exc)


def submit_intelligence_enqueue(
    enqueue: Callable[..., dict[str, Any]],
    *args: Any,
    phase: str,
    signal_id: str,
    **kwargs: Any,
) -> dict[str, Any]:
    """Hand persistence to a local executor without waiting on database I/O."""
    try:
        if not _CAPACITY.acquire(blocking=False):
            return {"ok": False, "accepted": False, "error": "handoff_capacity_exhausted"}
        future = _EXECUTOR.submit(enqueue, *args, **kwargs)
        future.add_done_callback(lambda _done: _CAPACITY.release())
        future.add_done_callback(
            lambda done: _log_result(done, phase=phase, signal_id=signal_id)
        )
        return {"ok": True, "accepted": True}
    except Exception as exc:
        try:
            _CAPACITY.release()
        except ValueError:
            pass
        log.warning("%s intelligence handoff rejected signal_id=%s: %s",
                    phase, signal_id, exc)
        return {"ok": False, "accepted": False, "error": str(exc)[:500]}


def enqueue_pretrigger_context_best_effort(signal: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    from ap.intelligence_context_materializer import enqueue_pretrigger_context

    return submit_intelligence_enqueue(
        enqueue_pretrigger_context,
        dict(signal or {}),
        phase="PRETRIGGER",
        signal_id=str((signal or {}).get("signal_id") or ""),
        **kwargs,
    )


def enqueue_preopen_context_best_effort(signal: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
    from ap.intelligence_context_materializer import enqueue_preopen_context

    return submit_intelligence_enqueue(
        enqueue_preopen_context,
        dict(signal or {}),
        phase="PREOPEN",
        signal_id=str((signal or {}).get("signal_id") or ""),
        **kwargs,
    )
