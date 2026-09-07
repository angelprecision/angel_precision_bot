"""P0 canonical option/underlying quote snapshot shim.

The package shadows ``ap/position_quote_monitor.py`` only for real
``ManagedPosition`` objects. Legacy duck-typed fixtures continue through the
legacy implementation; production positions use one same-cycle snapshot and
executable-bid authority in PAPER and LIVE.
"""
from __future__ import annotations

import importlib.util as _importlib_util
import logging as _logging
import os as _os
import sys as _sys
from datetime import datetime as _datetime, timezone as _timezone
from pathlib import Path as _Path
from typing import Any as _Any, Optional as _Optional

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

log = _logging.getLogger("ap.position_quote_monitor.exit_truth")
_BaseAPPositionQuoteMonitor = _base.APPositionQuoteMonitor
_safe_float = _base._safe_float
_get_attr = _base._get_attr

TOUCHED_PROFIT_ARM_PCT = float(_os.getenv("TOUCHED_PROFIT_ARM_PCT", "0.04"))
TOUCHED_PROFIT_CONFIRM_SNAPSHOTS = max(
    2, int(_os.getenv("TOUCHED_PROFIT_CONFIRM_SNAPSHOTS", "2"))
)
TOUCHED_PROFIT_CONFIRM_MAX_GAP_SEC = float(
    _os.getenv("TOUCHED_PROFIT_CONFIRM_MAX_GAP_SEC", "10")
)
EXIT_DECISION_QUOTE_MAX_AGE_SEC = float(
    _os.getenv("EXIT_DECISION_QUOTE_MAX_AGE_SEC", "5")
)
EXIT_DECISION_QUOTE_FUTURE_TOLERANCE_SEC = float(
    _os.getenv("EXIT_DECISION_QUOTE_FUTURE_TOLERANCE_SEC", "2")
)
EXIT_DECISION_QUOTE_MAX_SKEW_SEC = float(
    _os.getenv("EXIT_DECISION_QUOTE_MAX_SKEW_SEC", "2")
)


def _coerce_dt(value: _Any, fallback: _Optional[_datetime] = None) -> _Optional[_datetime]:
    if isinstance(value, _datetime):
        return value if value.tzinfo else value.replace(tzinfo=_timezone.utc)
    if isinstance(value, (int, float)):
        try:
            value_f = float(value)
            if value_f > 10_000_000_000:
                value_f /= 1000.0
            return _datetime.fromtimestamp(value_f, _timezone.utc)
        except Exception:
            return fallback
    if isinstance(value, str) and value.strip():
        try:
            parsed = _datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=_timezone.utc)
        except Exception:
            return fallback
    return fallback


def _quote_ts(raw: dict[str, _Any], observed_at: _datetime) -> _datetime:
    # Tradier production quote objects commonly expose bid_date/ask_date and
    # trade_date as epoch milliseconds. Prefer the executable book timestamps;
    # a repeated unchanged bid_date across polls must not count as two fresh
    # profit-lock confirmations.
    book_times = [
        parsed for parsed in (
            _coerce_dt(raw.get("bid_date")),
            _coerce_dt(raw.get("ask_date")),
        ) if parsed is not None
    ]
    if book_times:
        return max(book_times)
    for key in (
        "quote_timestamp",
        "quote_ts",
        "timestamp",
        "updated_at",
        "as_of",
        "trade_date",
        "last_trade_date",
    ):
        parsed = _coerce_dt(raw.get(key))
        if parsed is not None:
            return parsed
    return observed_at


def _midpoint(bid: float, ask: float, raw: dict[str, _Any]) -> float:
    if bid > 0 and ask > 0:
        return round((bid + ask) / 2.0, 4)
    for key in ("mid", "mark", "last", "price"):
        value = _safe_float(raw.get(key), 0.0)
        if value > 0:
            return value
    return 0.0


def _provider_domain(raw: dict[str, _Any], broker: _Any) -> tuple[str, str]:
    provider = ""
    for key in ("provider", "vendor", "feed", "market_data_provider"):
        value = str(raw.get(key) or "").strip()
        if value:
            provider = value
            break
    if not provider:
        provider = type(broker).__name__ or "unknown_provider"

    domain = ""
    for key in ("domain", "data_domain", "market_data_domain", "environment"):
        value = str(raw.get(key) or "").strip()
        if value:
            domain = value
            break
    if not domain:
        cfg = getattr(broker, "cfg", None)
        domain = str(
            getattr(broker, "market_data_base_url", None)
            or getattr(broker, "base_url", None)
            or getattr(cfg, "market_data_base_url", None)
            or getattr(cfg, "base_url", None)
            or f"{type(broker).__module__}.{type(broker).__name__}:default"
        ).strip()
    return provider, domain


def _freshness_reasons(label: str, ts: _datetime, now: _datetime) -> list[str]:
    age = (now - ts).total_seconds()
    reasons: list[str] = []
    if age > EXIT_DECISION_QUOTE_MAX_AGE_SEC:
        reasons.append(f"stale_{label}_quote")
    if age < -EXIT_DECISION_QUOTE_FUTURE_TOLERANCE_SEC:
        reasons.append(f"future_dated_{label}_quote")
    return reasons


def _is_canonical_position(pos: _Any) -> bool:
    try:
        from ap_exit_engine import ManagedPosition
        return isinstance(pos, ManagedPosition)
    except Exception:
        return False


class APPositionQuoteMonitor(_BaseAPPositionQuoteMonitor):
    """Production quote monitor with same-cycle executable exit truth."""

    def _refresh_once(self):
        positions = list(self.exit_engine.active_positions() or [])
        if not positions or not any(_is_canonical_position(p) for p in positions):
            return super()._refresh_once()
        return self._refresh_canonical_positions(positions)

    def _refresh_canonical_positions(self, positions: list[_Any]):
        self._cycles += 1
        self._metrics["cycles"] += 1
        self._last_cycle_ts = __import__("time").time()
        cycle_ts = _datetime.now(_timezone.utc)
        cycle_id = cycle_ts.isoformat()

        active_ids: set[str] = set()
        active_contracts: set[str] = set()
        underlyings_set: set[str] = set()
        contracts_set: set[str] = set()
        for pos in positions:
            pid = str(_get_attr(pos, "positionid", "position_id", default="") or "")
            ticker = str(_get_attr(pos, "ticker", "underlying", default="") or "").upper()
            contract = str(
                _get_attr(pos, "optionsymbol", "option_symbol", "contract", default="") or ""
            ).upper()
            if pid:
                active_ids.add(pid)
            if ticker:
                underlyings_set.add(ticker)
            if contract:
                contracts_set.add(contract)
                active_contracts.add(contract)

        underlying_quotes = self._fetch_batch_cached(sorted(underlyings_set))
        option_quotes = self._fetch_batch_cached(sorted(contracts_set))
        snapshots: list[dict[str, _Any]] = []
        wake_engine = False

        engine_lock = getattr(self.exit_engine, "_lock", None)
        lock_ctx = engine_lock if engine_lock is not None else _base._NullCtx()
        with lock_ctx:
            for pos in positions:
                pid = str(_get_attr(pos, "positionid", "position_id", default="") or "")
                ticker = str(_get_attr(pos, "ticker", "underlying", default="") or "").upper()
                contract = str(
                    _get_attr(pos, "optionsymbol", "option_symbol", "contract", default="") or ""
                ).upper()
                mode = str(_get_attr(pos, "executionmode", "execution_mode", default="") or "").lower().strip()
                oq = dict(option_quotes.get(contract) or {})
                uq = dict(underlying_quotes.get(ticker) or {})

                option_bid = _safe_float(oq.get("bid"), 0.0)
                option_ask = _safe_float(oq.get("ask"), 0.0)
                option_mid = _midpoint(option_bid, option_ask, oq)
                underlying_bid = _safe_float(uq.get("bid"), 0.0)
                underlying_ask = _safe_float(uq.get("ask"), 0.0)
                underlying_mid = _midpoint(underlying_bid, underlying_ask, uq)
                if underlying_mid <= 0:
                    underlying_mid = _safe_float(self._extract_underlying_price(uq), 0.0)

                option_ts = _quote_ts(oq, cycle_ts)
                underlying_ts = _quote_ts(uq, cycle_ts)
                option_provider, option_domain = _provider_domain(oq, self.broker)
                underlying_provider, underlying_domain = _provider_domain(uq, self.broker)

                invalid: list[str] = []
                option_invalid = _freshness_reasons("option", option_ts, cycle_ts)
                underlying_invalid = _freshness_reasons("underlying", underlying_ts, cycle_ts)
                if option_bid <= 0:
                    option_invalid.append("missing_executable_option_bid")
                if underlying_mid <= 0:
                    underlying_invalid.append("missing_underlying_mid")
                invalid.extend(option_invalid)
                invalid.extend(underlying_invalid)
                same_provider = option_provider == underlying_provider
                same_domain = option_domain == underlying_domain
                if not same_provider:
                    invalid.append("cross_provider_quote_pair")
                if not same_domain:
                    invalid.append("cross_domain_quote_pair")
                quote_skew = abs((option_ts - underlying_ts).total_seconds())
                if quote_skew > EXIT_DECISION_QUOTE_MAX_SKEW_SEC:
                    invalid.append("cross_cycle_quote_pair")
                if mode not in {"paper", "live"}:
                    invalid.append("execution_mode_unproven")

                option_valid = not option_invalid
                underlying_valid = not underlying_invalid
                coherent = option_valid and underlying_valid and same_provider and same_domain and quote_skew <= EXIT_DECISION_QUOTE_MAX_SKEW_SEC and mode in {"paper", "live"}

                # Clear stale prior-poll values before publishing current-cycle truth.
                pos.current_bid = option_bid if option_valid else 0.0
                pos.current_ask = option_ask if option_ask > 0 else 0.0
                pos.current_option_price = option_bid if option_valid else 0.0
                pos.analytics_mark_price = option_mid
                pos.executable_exit_price = option_bid if option_valid else 0.0
                pos.executable_quote_valid = bool(option_valid)
                pos.live_executable_price_source = "bid" if option_valid else "bid_unavailable_or_stale"
                pos.pricing_mode = mode if mode in {"paper", "live"} else "live_risk_unproven"
                pos.raw_execution_mode = mode
                pos.current_underlying = underlying_mid if underlying_valid else 0.0
                pos.last_option_quote_update_ts = option_ts if option_valid else None
                pos.last_underlying_quote_update_ts = underlying_ts if underlying_valid else None
                pos.last_option_quote_missing_ts = None if option_valid else cycle_ts
                pos.last_underlying_quote_missing_ts = None if underlying_valid else cycle_ts
                pos.last_quote_update_ts = cycle_ts if option_valid else None
                pos.last_quote_missing_ts = None if option_valid else cycle_ts
                for alias, value in (
                    ("currentbid", pos.current_bid),
                    ("currentask", pos.current_ask),
                    ("currentoptionprice", pos.current_option_price),
                    ("analyticsmarkprice", option_mid),
                    ("liveexecutablepricesource", pos.live_executable_price_source),
                    ("currentunderlying", pos.current_underlying),
                    ("lastoptionquoteupdatets", pos.last_option_quote_update_ts),
                    ("lastunderlyingquoteupdatets", pos.last_underlying_quote_update_ts),
                ):
                    try:
                        setattr(pos, alias, value)
                    except Exception:
                        pass

                entry = _safe_float(_get_attr(pos, "entryprice", "entry_price", default=None), 0.0)
                bid_pnl = ((option_bid - entry) / entry) if option_valid and entry > 0 else 0.0
                try:
                    pos.optionpnlpct = bid_pnl
                except Exception:
                    pass

                # Peak authority is always the executable bid, never midpoint.
                if option_valid and entry > 0:
                    prior_peak = float(getattr(pos, "peak_executable_pnl_pct", 0.0) or 0.0)
                    prior_peak_bid = float(getattr(pos, "peak_executable_bid", 0.0) or 0.0)
                    if bid_pnl >= prior_peak or prior_peak_bid <= 0:
                        pos.peak_executable_pnl_pct = max(prior_peak, bid_pnl)
                        if bid_pnl >= prior_peak:
                            pos.peak_executable_bid = option_bid
                    pos.peak_pnl_pct = pos.peak_executable_pnl_pct
                    pos.max_profit_seen = pos.peak_executable_pnl_pct
                    try:
                        pos.peakpnlpct = pos.peak_pnl_pct
                        pos.maxprofitseen = pos.max_profit_seen
                    except Exception:
                        pass

                snapshot_id = f"{cycle_id}:{pid or contract}"
                last_confirm_ts = getattr(pos, "profit_lock_last_confirmation_quote_ts", None)
                already_armed = bool(getattr(pos, "profit_lock_armed_at", None))
                count = int(getattr(pos, "profit_lock_confirmation_count", 0) or 0)
                if not already_armed:
                    # Any pre-PR touched_profit without the new durable arm proof is unproven.
                    pos.touched_profit = False
                    try:
                        pos.touchedprofit = False
                    except Exception:
                        pass
                    if coherent and bid_pnl >= TOUCHED_PROFIT_ARM_PCT:
                        gap_ok = True
                        if isinstance(last_confirm_ts, _datetime):
                            gap_ok = 0 < (option_ts - last_confirm_ts).total_seconds() <= TOUCHED_PROFIT_CONFIRM_MAX_GAP_SEC
                        if getattr(pos, "profit_lock_last_confirmation_snapshot_id", "") == snapshot_id:
                            pass
                        elif count > 0 and gap_ok:
                            count += 1
                        else:
                            count = 1
                        pos.profit_lock_confirmation_count = count
                        pos.profit_lock_last_confirmation_snapshot_id = snapshot_id
                        pos.profit_lock_last_confirmation_quote_ts = option_ts
                        if count >= TOUCHED_PROFIT_CONFIRM_SNAPSHOTS:
                            pos.profit_lock_armed_at = cycle_ts
                            pos.profit_lock_arm_bid = option_bid
                            pos.profit_lock_arm_pnl_pct = bid_pnl
                            pos.profit_lock_option_quote_source = f"{option_provider}|{option_domain}"
                            pos.profit_lock_underlying_quote_source = f"{underlying_provider}|{underlying_domain}"
                            pos.touched_profit = True
                            try:
                                pos.touchedprofit = True
                            except Exception:
                                pass
                    else:
                        pos.profit_lock_confirmation_count = 0
                        pos.profit_lock_last_confirmation_snapshot_id = ""
                        pos.profit_lock_last_confirmation_quote_ts = None
                else:
                    pos.touched_profit = True
                    try:
                        pos.touchedprofit = True
                    except Exception:
                        pass

                snapshot = {
                    "snapshot_version": "p0_exit_truth_v1",
                    "cycle_id": cycle_id,
                    "same_evaluation_cycle": True,
                    "position_id": pid,
                    "positionid": pid,
                    "client_id": str(getattr(pos, "client_id", "") or self.client_id or ""),
                    "execution_mode": mode,
                    "ticker": ticker,
                    "option_symbol": contract,
                    "optionsymbol": contract,
                    "option_bid": option_bid,
                    "option_ask": option_ask,
                    "option_mid": option_mid,
                    "option_quote_timestamp": option_ts.isoformat(),
                    "underlying_bid": underlying_bid,
                    "underlying_ask": underlying_ask,
                    "underlying_mid": underlying_mid,
                    "underlying_quote_timestamp": underlying_ts.isoformat(),
                    "option_quote_provider": option_provider,
                    "underlying_quote_provider": underlying_provider,
                    "option_quote_domain": option_domain,
                    "underlying_quote_domain": underlying_domain,
                    "quote_provider": option_provider if same_provider else "mixed",
                    "quote_domain": option_domain if same_domain else "mixed",
                    "same_provider": same_provider,
                    "same_domain": same_domain,
                    "quote_skew_seconds": quote_skew,
                    "option_executable_valid": option_valid,
                    "underlying_truth_valid": underlying_valid,
                    "coherent": coherent,
                    "invalid_reasons": sorted(set(invalid)),
                    "snapshot_timestamp": cycle_ts.isoformat(),
                    "quote_source": "ap_position_quote_monitor",
                    "option_pnl_pct": bid_pnl,
                    "analytics_only_midpoint": option_mid,
                }
                pos.exit_decision_quote_snapshot = snapshot
                snapshots.append(snapshot)

                # QuoteAuthority is supplemental only. Publish zeros for invalid
                # current-cycle observations so its fresh cache cannot resurrect a
                # stale prior-poll underlying or non-executable option mark.
                try:
                    from ap_quote_authority import QUOTES as _QA
                    _QA.write(
                        writer_id="ap_position_quote_monitor",
                        symbol=contract,
                        underlying=ticker,
                        bid=option_bid if option_valid else 0.0,
                        ask=option_ask if option_valid else 0.0,
                        last=option_bid if option_valid else 0.0,
                        underlying_price=underlying_mid if underlying_valid else 0.0,
                        source=f"{option_provider}|{option_domain}|bid_exit_authority",
                    )
                except Exception as exc:
                    log.debug("QuoteAuthority canonical write failed for %s: %s", contract, exc)

                if option_valid and self._should_wake(contract, option_bid):
                    wake_engine = True
                    self._last_push_price[contract] = option_bid

                if option_valid and entry > 0:
                    self._persist_quote_to_db(
                        position_id=pid,
                        option_price=option_bid,
                        underlying_price=underlying_mid if underlying_valid else 0.0,
                        option_pnl_pct=bid_pnl,
                        now_utc=cycle_ts,
                    )
                    self._persist_mfe_mae_to_orders(
                        position_id=pid,
                        contract=contract,
                        option_pnl_pct=bid_pnl,
                        now_utc=cycle_ts,
                        source="bid_exit_authority",
                    )
                elif entry > 0:
                    self._mark_mfe_mae_unavailable(
                        position_id=pid,
                        contract=contract,
                        reason="missing_executable_option_bid",
                    )

                if not coherent:
                    self.request_immediate_refresh(contract, ticker)
                    log.warning(
                        "[%s] SOFT_EXIT_TRUTH_REFRESH_REQUESTED | pos=%s contract=%s "
                        "mode=%s invalid=%s",
                        self.client_id,
                        pid,
                        contract,
                        mode,
                        sorted(set(invalid)),
                    )
                self._classify_health(pid, contract, ticker, pos)

        applier = getattr(self.exit_engine, "apply_quote_snapshots", None) or getattr(
            self.exit_engine, "applyquotesnapshots", None
        )
        if callable(applier):
            applier(snapshots)

        self._prune_closed(active_ids, active_contracts)
        if wake_engine:
            waker = getattr(self.exit_engine, "_quote_arrived_event", None) or getattr(
                self.exit_engine, "quote_arrived_event", None
            )
            if waker is not None:
                try:
                    waker.set()
                except Exception:
                    pass


_base.APPositionQuoteMonitor = APPositionQuoteMonitor
globals()["APPositionQuoteMonitor"] = APPositionQuoteMonitor
