"""Compatibility loader for lifecycle-safe Master Control setup dedup.

The production implementation remains in the repository-root
``ap_master_control.py``.  This package executes that implementation in the
canonical ``ap_master_control`` module namespace, then installs a narrow
process-local dedup adapter.

Why this exists:
A signal can legitimately enter Master Control more than once during the same
queue lifecycle (for example after a bounded reclaim/recovery pass).  The
legacy setup cache stores only ``setup_key -> timestamp``.  After the first
approval, the second pass sees the same setup key and rejects its own signal as
``duplicate_setup`` even when durable DB truth proves there is no second active
path.

The adapter records the owning signal_id for each setup key.  Membership is
hidden only when the current evaluation is the exact owner.  Different signals
and legacy ownerless keys remain blocked.  The durable duplicate guard inside
Master Control remains authoritative and unchanged.
"""
from __future__ import annotations

from pathlib import Path as _Path

_IMPL_PATH = _Path(__file__).resolve().parents[1] / "ap_master_control.py"
_IMPL_SOURCE = _IMPL_PATH.read_text(encoding="utf-8")
exec(compile(_IMPL_SOURCE, str(_IMPL_PATH), "exec"), globals(), globals())

del _IMPL_SOURCE

import threading as _threading
import time as _time
from typing import Any as _Any


class _LifecycleSeenSignals(dict):
    """Dict-compatible setup cache with per-thread lifecycle ownership.

    Existing Master Control code continues to read/write timestamps through the
    normal dict API.  The only semantic change is ``setup_key in cache``:
    membership is False when the active thread is evaluating the exact signal
    that owns that key.  This prevents self-rejection without weakening the
    guard for another signal.
    """

    def __init__(self, initial: dict | None = None, *, owners: dict | None = None):
        super().__init__(initial or {})
        self._owners: dict[str, str] = owners if isinstance(owners, dict) else {}
        self._context = _threading.local()
        self._lock = _threading.RLock()

    def bind(self, *, setup_key: str, signal_id: str) -> None:
        self._context.setup_key = str(setup_key or "")
        self._context.signal_id = str(signal_id or "")

    def unbind(self) -> None:
        for attr in ("setup_key", "signal_id"):
            try:
                delattr(self._context, attr)
            except AttributeError:
                pass

    def prune(self, *, now_ts: float, ttl_sec: float = 1800.0, max_entries: int = 450) -> None:
        """Remove stale keys and cap size before legacy code's 500-key rebuild.

        Keeping the adapter below the legacy rebuild threshold prevents the
        original ``self._seen_signals = {...}`` assignment from replacing this
        dict subclass during the current evaluation.
        """
        now = float(now_ts)
        ttl = float(ttl_sec)
        with self._lock:
            stale = []
            for key, inserted_at in dict.items(self):
                try:
                    age = now - float(inserted_at)
                except (TypeError, ValueError):
                    stale.append(key)
                    continue
                if age >= ttl:
                    stale.append(key)
            for key in stale:
                dict.pop(self, key, None)
                self._owners.pop(str(key), None)

            overflow = len(self) - int(max_entries)
            if overflow > 0:
                ordered = sorted(
                    dict.items(self),
                    key=lambda item: float(item[1]) if isinstance(item[1], (int, float)) else float("-inf"),
                )
                for key, _ in ordered[:overflow]:
                    dict.pop(self, key, None)
                    self._owners.pop(str(key), None)

            live_keys = set(dict.keys(self))
            for key in list(self._owners):
                if key not in live_keys:
                    self._owners.pop(key, None)

    def __contains__(self, key: object) -> bool:
        with self._lock:
            if not dict.__contains__(self, key):
                return False
            setup_key = getattr(self._context, "setup_key", "")
            signal_id = getattr(self._context, "signal_id", "")
            if str(key) == setup_key and signal_id:
                owner = str(self._owners.get(str(key)) or "")
                if owner and owner == signal_id:
                    return False
            return True

    def __setitem__(self, key: object, value: _Any) -> None:
        with self._lock:
            dict.__setitem__(self, key, value)
            setup_key = getattr(self._context, "setup_key", "")
            signal_id = getattr(self._context, "signal_id", "")
            if str(key) == setup_key and signal_id:
                self._owners[str(key)] = signal_id

    def __delitem__(self, key: object) -> None:
        with self._lock:
            dict.__delitem__(self, key)
            self._owners.pop(str(key), None)

    def pop(self, key: object, default: _Any = None) -> _Any:
        with self._lock:
            value = dict.pop(self, key, default)
            self._owners.pop(str(key), None)
            return value

    def clear(self) -> None:
        with self._lock:
            dict.clear(self)
            self._owners.clear()


_original_evaluate = APMasterControl.evaluate


def _evaluate_with_lifecycle_owned_setup_cache(self, signal: dict, client_id: str = "default"):
    """Run the original evaluator with ownership-aware setup-cache semantics."""
    if not isinstance(signal, dict):
        return _original_evaluate(self, signal, client_id=client_id)

    signal_id = str(signal.get("signal_id") or uuid.uuid4())
    signal["signal_id"] = signal_id

    ticker = str(signal.get("ticker") or signal.get("symbol") or "?")
    ticker = _INDEX_TO_ETF.get(ticker.upper(), ticker)
    direction = _normalize_signal_side(signal.get("side") or signal.get("direction"))
    timeframe = str(signal.get("timeframe", "1d"))

    if direction is None:
        return _original_evaluate(self, signal, client_id=client_id)

    setup_key = f"{client_id}:{ticker.upper()}:{direction}:{timeframe}"
    owners = getattr(self, "_seen_setup_owners", None)
    if not isinstance(owners, dict):
        owners = {}

    cache = getattr(self, "_seen_signals", None)
    if not isinstance(cache, _LifecycleSeenSignals):
        cache = _LifecycleSeenSignals(cache if isinstance(cache, dict) else {}, owners=owners)
        self._seen_signals = cache
    self._seen_setup_owners = cache._owners

    cache.prune(now_ts=_time.time())
    cache.bind(setup_key=setup_key, signal_id=signal_id)
    try:
        decision = _original_evaluate(self, signal, client_id=client_id)
        if getattr(decision, "ok", False):
            log.info(
                "setup_lifecycle_owner_recorded client_id=%s signal_id=%s setup_key=%s",
                client_id,
                signal_id,
                setup_key,
            )
        return decision
    finally:
        cache.unbind()


APMasterControl.evaluate = _evaluate_with_lifecycle_owned_setup_cache

__all__ = [name for name in globals() if not name.startswith("_")]
