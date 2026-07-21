"""Production QPM wrapper for coherent option/underlying exit snapshots.

The legacy QPM remains the only market-data poller. This wrapper captures raw
option and underlying observations from the same refresh cycle, exposes
executable-bid accounting to the legacy QPM through a thread-local read adapter,
and hands one coherent snapshot to the existing APExitEngine before wake-up.

Canonical ``execution_mode`` is never mutated.
"""
from __future__ import annotations

import contextvars as _contextvars
import importlib.util as _importlib_util
import sys as _sys
import time as _time
from datetime import datetime as _datetime, timezone as _timezone
from pathlib import Path as _Path

_BASE_PATH = _Path(__file__).resolve().parent.parent / "position_quote_monitor.py"
_BASE_MODULE_NAME = "_ap_position_quote_monitor_base"
_spec = _importlib_util.spec_from_file_location(_BASE_MODULE_NAME, _BASE_PATH)
if _spec is None or _spec.loader is None:  # pragma: no cover
    raise ImportError(f"Unable to load legacy quote monitor from {_BASE_PATH}")
_base = _importlib_util.module_from_spec(_spec)
_sys.modules[_BASE_MODULE_NAME] = _base
_spec.loader.exec_module(_base)
for _name in dir(_base):
    if not _name.startswith("__") or _name == "__doc__":
        globals()[_name] = getattr(_base, _name)

_BaseAPPositionQuoteMonitor = _base.APPositionQuoteMonitor
_ORIGINAL_GET_ATTR = _base._get_attr
_MODE_OVERRIDES: _contextvars.ContextVar[dict[int, str]] = _contextvars.ContextVar(
    "p0_qpm_execution_mode_view", default={}
)


def _mode(value) -> str:
    return str(value or "").strip().lower()


def _get_attr_with_execution_view(obj, *names, default=None):
    overrides = _MODE_OVERRIDES.get()
    if id(obj) in overrides and any(
        name in {"executionmode", "execution_mode"} for name in names
    ):
        return overrides[id(obj)]
    return _ORIGINAL_GET_ATTR(obj, *names, default=default)


# ContextVar state is isolated per QPM thread, so one client cycle cannot alter
# another client's pricing view.
_base._get_attr = _get_attr_with_execution_view


def _install_underlying_level_truth_guards() -> bool:
    """Prevent missing/stale underlying from impersonating target/stop truth.

    Legacy properties compare numeric levels directly. For example, a missing
    CALL underlying represented as 0 can satisfy ``0 <= underlying_stop``.
    Protected positions therefore require the canonical snapshot validator
    before either target or stop can report True. Non-protected positions retain
    the legacy properties unchanged.
    """
    try:
        import ap_exit_engine as exit_mod

        cls = exit_mod.ManagedPosition
        if getattr(cls, "_soft_exit_level_truth_guard_installed", False):
            return True
        target_descriptor = getattr(cls, "is_at_target", None)
        stop_descriptor = getattr(cls, "is_at_stop", None)
        target_getter = getattr(target_descriptor, "fget", None)
        stop_getter = getattr(stop_descriptor, "fget", None)
        if not callable(target_getter) or not callable(stop_getter):
            return False

        def truth_is_valid(pos) -> bool:
            if getattr(pos, "executable_bid_soft_exit_truth_enabled", False) is not True:
                return True
            try:
                return bool(exit_mod._validate_exit_truth_snapshot(pos).valid)
            except Exception:
                return False

        def guarded_target(pos):
            return bool(target_getter(pos)) if truth_is_valid(pos) else False

        def guarded_stop(pos):
            return bool(stop_getter(pos)) if truth_is_valid(pos) else False

        cls.is_at_target = property(guarded_target)
        cls.is_at_stop = property(guarded_stop)
        cls._soft_exit_level_truth_guard_installed = True
        return True
    except Exception:
        return False


def _broker_provider_domain(broker) -> tuple[str, str]:
    cfg = getattr(broker, "cfg", None)
    base_url = str(
        getattr(broker, "base_url", None)
        or getattr(broker, "baseurl", None)
        or getattr(broker, "quote_base_url", None)
        or getattr(cfg, "base_url", None)
        or getattr(cfg, "baseurl", None)
        or ""
    ).strip().lower()
    class_name = broker.__class__.__name__.lower() if broker is not None else "unknown"
    provider = (
        "tradier"
        if "tradier" in class_name or "tradier" in base_url
        else class_name or "unknown"
    )
    if "sandbox" in base_url:
        domain = f"{provider}_sandbox_market_data"
    elif base_url:
        domain = f"{provider}_live_market_data"
    else:
        domain = f"{provider}_domain_unproven"
    return provider, domain


def _cache_observation_epoch(symbol: str, fallback: float) -> float:
    try:
        lock = getattr(_base, "_SHARED_CACHE_LOCK")
        cache = getattr(_base, "_SHARED_CACHE")
        with lock:
            row = dict(cache.get(symbol) or {})
        ts = float(row.get("ts") or 0.0)
        return ts if ts > 0 else fallback
    except Exception:
        return fallback


class APPositionQuoteMonitor(_BaseAPPositionQuoteMonitor):
    def _refresh_once(self):
        _install_underlying_level_truth_guards()
        positions = list(self.exit_engine.active_positions() or [])
        production_positions = [
            pos
            for pos in positions
            if getattr(pos, "executable_bid_soft_exit_truth_enabled", False) is True
        ]
        if not production_positions:
            return super()._refresh_once()

        original_modes: dict[str, str] = {}
        original_modes_by_symbol: dict[str, str] = {}
        mode_overrides: dict[int, str] = {}
        for pos in production_positions:
            pid = str(getattr(pos, "position_id", "") or "")
            mode = _mode(getattr(pos, "execution_mode", ""))
            original_modes[pid] = mode
            symbol = str(
                getattr(pos, "option_symbol", "")
                or getattr(pos, "optionsymbol", "")
                or ""
            ).upper()
            if symbol:
                original_modes_by_symbol[symbol] = mode
            if mode == "paper":
                # Present a bid-authoritative view only to the legacy QPM's
                # local field reader. The position remains PAPER throughout.
                mode_overrides[id(pos)] = "live"

        option_symbols = {
            str(
                getattr(pos, "option_symbol", "")
                or getattr(pos, "optionsymbol", "")
                or ""
            ).upper()
            for pos in production_positions
        }
        tickers = {
            str(
                getattr(pos, "ticker", "")
                or getattr(pos, "underlying", "")
                or ""
            ).upper()
            for pos in production_positions
        }

        captured_options: dict[str, dict] = {}
        captured_underlyings: dict[str, dict] = {}
        option_timestamps: dict[str, _datetime] = {}
        underlying_timestamps: dict[str, _datetime] = {}
        original_fetch = self._fetch_batch_cached

        provider, domain = _broker_provider_domain(getattr(self, "broker", None))
        cycle_id = (
            f"qpm-{getattr(self, '_cycles', 0) + 1}-"
            f"{int(_time.time() * 1_000_000)}"
        )
        context = {
            "option_quotes": captured_options,
            "underlying_quotes": captured_underlyings,
            "option_quote_timestamps": option_timestamps,
            "underlying_quote_timestamps": underlying_timestamps,
            "original_modes": original_modes,
            "original_modes_by_symbol": original_modes_by_symbol,
            "default_execution_mode": "",
            "quote_provider": provider,
            "quote_domain": domain,
            "snapshot_timestamp": _datetime.now(_timezone.utc),
            "cycle_id": cycle_id,
        }
        self.exit_engine._qpm_cycle_context = context

        def capturing_fetch(symbols):
            result = original_fetch(symbols)
            observed_epoch = _time.time()
            context["snapshot_timestamp"] = _datetime.fromtimestamp(
                observed_epoch, _timezone.utc
            )
            for symbol, quote in dict(result or {}).items():
                upper = str(symbol or "").upper()
                quote_dict = dict(quote or {})
                observation_epoch = _cache_observation_epoch(upper, observed_epoch)
                observation_ts = _datetime.fromtimestamp(
                    observation_epoch, _timezone.utc
                )
                if upper in option_symbols:
                    captured_options[upper] = quote_dict
                    option_timestamps[upper] = observation_ts
                if upper in tickers:
                    captured_underlyings[upper] = quote_dict
                    underlying_timestamps[upper] = observation_ts
            return result

        self._fetch_batch_cached = capturing_fetch
        token = _MODE_OVERRIDES.set(mode_overrides)
        try:
            return super()._refresh_once()
        finally:
            _MODE_OVERRIDES.reset(token)
            self._fetch_batch_cached = original_fetch
            try:
                delattr(self.exit_engine, "_qpm_cycle_context")
            except Exception:
                pass


_install_underlying_level_truth_guards()
_base.APPositionQuoteMonitor = APPositionQuoteMonitor
globals()["APPositionQuoteMonitor"] = APPositionQuoteMonitor
