"""Production QPM wrapper for coherent option/underlying exit snapshots.

The legacy QPM remains the only market-data poller. This wrapper captures the
raw option and underlying observations from the same refresh cycle, forces
production-owned PAPER positions through executable-bid accounting, and hands a
single coherent snapshot to the existing APExitEngine owner before the wake
signal is released.
"""
from __future__ import annotations

import importlib.util as _importlib_util
import time as _time
import sys as _sys
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


def _mode(value) -> str:
    return str(value or "").strip().lower()


def _broker_provider_domain(broker) -> tuple[str, str]:
    cfg = getattr(broker, "cfg", None)
    base_url = str(
        getattr(broker, "base_url", None)
        or getattr(cfg, "base_url", None)
        or ""
    ).strip().lower()
    class_name = broker.__class__.__name__.lower() if broker is not None else "unknown"
    provider = "tradier" if "tradier" in class_name or "tradier" in base_url else class_name or "unknown"
    if "sandbox" in base_url:
        domain = f"{provider}_sandbox_market_data"
    elif base_url:
        domain = f"{provider}_live_market_data"
    else:
        domain = f"{provider}_domain_unproven"
    return provider, domain


class APPositionQuoteMonitor(_BaseAPPositionQuoteMonitor):
    def _refresh_once(self):
        positions = list(self.exit_engine.active_positions() or [])
        production_positions = [
            p for p in positions
            if getattr(p, "executable_bid_soft_exit_truth_enabled", False) is True
        ]
        if not production_positions:
            return super()._refresh_once()

        original_modes: dict[str, str] = {}
        original_modes_by_symbol: dict[str, str] = {}
        original_mode_aliases: dict[int, tuple[object, object]] = {}
        for pos in production_positions:
            pid = str(getattr(pos, "position_id", "") or "")
            mode = _mode(getattr(pos, "execution_mode", ""))
            original_modes[pid] = mode
            symbol = str(getattr(pos, "option_symbol", "") or getattr(pos, "optionsymbol", "") or "").upper()
            if symbol:
                original_modes_by_symbol[symbol] = mode
            original_mode_aliases[id(pos)] = (
                getattr(pos, "execution_mode", None),
                getattr(pos, "executionmode", None),
            )
            # The legacy QPM already has the correct bid-only branch for LIVE.
            # Temporarily route production PAPER through that branch, then restore
            # identity before the cycle ends.
            if mode == "paper":
                setattr(pos, "execution_mode", "live")
                try:
                    setattr(pos, "executionmode", "live")
                except Exception:
                    pass

        option_symbols = {
            str(getattr(p, "option_symbol", "") or getattr(p, "optionsymbol", "") or "").upper()
            for p in production_positions
        }
        tickers = {
            str(getattr(p, "ticker", "") or getattr(p, "underlying", "") or "").upper()
            for p in production_positions
        }
        captured_options: dict[str, dict] = {}
        captured_underlyings: dict[str, dict] = {}
        original_fetch = self._fetch_batch_cached

        def capturing_fetch(symbols):
            result = original_fetch(symbols)
            for symbol, quote in dict(result or {}).items():
                upper = str(symbol or "").upper()
                if upper in option_symbols:
                    captured_options[upper] = dict(quote or {})
                if upper in tickers:
                    captured_underlyings[upper] = dict(quote or {})
            return result

        provider, domain = _broker_provider_domain(getattr(self, "broker", None))
        cycle_ts = _datetime.now(_timezone.utc)
        cycle_id = f"qpm-{getattr(self, '_cycles', 0) + 1}-{int(_time.time() * 1_000_000)}"
        self.exit_engine._qpm_cycle_context = {
            "option_quotes": captured_options,
            "underlying_quotes": captured_underlyings,
            "original_modes": original_modes,
            "original_modes_by_symbol": original_modes_by_symbol,
            "default_execution_mode": "",
            "quote_provider": provider,
            "quote_domain": domain,
            "snapshot_timestamp": cycle_ts,
            "cycle_id": cycle_id,
        }
        self._fetch_batch_cached = capturing_fetch
        try:
            return super()._refresh_once()
        finally:
            self._fetch_batch_cached = original_fetch
            for pos in production_positions:
                prior_mode, prior_alias = original_mode_aliases[id(pos)]
                setattr(pos, "execution_mode", prior_mode)
                try:
                    setattr(pos, "executionmode", prior_alias)
                except Exception:
                    pass
            try:
                delattr(self.exit_engine, "_qpm_cycle_context")
            except Exception:
                pass


_base.APPositionQuoteMonitor = APPositionQuoteMonitor
globals()["APPositionQuoteMonitor"] = APPositionQuoteMonitor
