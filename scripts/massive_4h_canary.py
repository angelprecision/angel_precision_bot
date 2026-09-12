#!/usr/bin/env python3
"""Run the operator-only Massive 4H qualification canary.

The canary is intentionally not part of a request, watcher, selector, broker,
order, or position path.  It performs read-only provider calls and emits a
sanitized JSON report.  A local or Render environment must provide
``MASSIVE_API_KEY`` (``POLYGON_API_KEY`` is a temporary compatibility alias),
an approved less-liquid ticker, and a validation-only reference file.

Exit status 0 means every gate passed.  No key, authorization header, URL
cursor, response body, or exception text is printed or persisted by this
script.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import importlib.util
import logging
import json
import math
import os
from pathlib import Path
import sys
import threading
import time as wall_time
import types
from typing import Any, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

import requests

# Keep the documented ``python scripts/massive_4h_canary.py`` invocation
# runnable from a checkout without requiring an installed package.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

def _load_observer_modules_without_package_side_effects() -> tuple[Any, Any]:
    """Load only the two observe-only modules for a standalone canary.

    Importing the application package installs execution guards and may probe
    its database.  A provider preflight is read-only and must remain runnable
    before that application boot path, so direct script execution supplies a
    tiny in-process package/logger shim and loads the source files explicitly.
    Render's normal application import path is unchanged.
    """
    root = Path(__file__).resolve().parents[1]
    package = types.ModuleType("ap")
    package.__path__ = [str(root / "ap")]  # type: ignore[attr-defined]
    sys.modules["ap"] = package

    logger_module = types.ModuleType("ap.logger")
    logger_module.get_logger = lambda name: logging.getLogger(name)  # type: ignore[attr-defined]
    sys.modules["ap.logger"] = logger_module

    def load(name: str, path: Path) -> Any:
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise RuntimeError("observer_module_load_failed")
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module

    load("ap.score_profile_side", root / "ap" / "score_profile_side.py")
    fair_value_gap = load("ap.fair_value_gap", root / "ap" / "fair_value_gap.py")
    massive_market_data = load("ap.massive_market_data", root / "ap" / "massive_market_data.py")
    return fair_value_gap, massive_market_data


if __name__ == "__main__":
    _fair_value_gap, _massive_market_data = _load_observer_modules_without_package_side_effects()
    detect_fair_value_gaps = _fair_value_gap.detect_fair_value_gaps
    MASSIVE_ALIGNMENT_VERSION = _massive_market_data.MASSIVE_ALIGNMENT_VERSION
    MASSIVE_MAX_LOOKBACK_DAYS = _massive_market_data.MASSIVE_MAX_LOOKBACK_DAYS
    MassiveClient = _massive_market_data.MassiveClient
    MassiveError = _massive_market_data.MassiveError
    Canonical4HBar = _massive_market_data.Canonical4HBar
    build_native_4h_bars = _massive_market_data.build_native_4h_bars
    build_rth_4h_bars = _massive_market_data.build_rth_4h_bars
    clear_massive_caches_for_tests = _massive_market_data.clear_massive_caches_for_tests
    compare_native_to_rth = _massive_market_data.compare_native_to_rth
    get_canonical_4h_bars = _massive_market_data.get_canonical_4h_bars
else:
    from ap.fair_value_gap import detect_fair_value_gaps
    from ap.massive_market_data import (
        MASSIVE_ALIGNMENT_VERSION,
        MASSIVE_MAX_LOOKBACK_DAYS,
        MassiveClient,
        MassiveError,
        Canonical4HBar,
        build_native_4h_bars,
        build_rth_4h_bars,
        clear_massive_caches_for_tests,
        compare_native_to_rth,
        get_canonical_4h_bars,
    )


ET = ZoneInfo("America/New_York")
UTC = timezone.utc
CORE_TICKERS = ("SPY", "QQQ", "NVDA", "MSFT")
DEFAULT_UNIVERSE = (
    "SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "AMD", "TSLA", "META",
    "AMZN", "GOOGL", "AVGO", "NFLX", "COIN", "BMY", "NEE", "WMT", "PEP",
    "HOOD", "SMCI", "MU", "CRWD", "LULU", "XOM", "ORCL",
)
DEFAULT_HISTORY_DAYS = 365
DEFAULT_REQUIRED_HISTORY_DAYS = 30
DEFAULT_UNIVERSE_SIZE = 25
DEFAULT_PRICE_TOLERANCE = 0.0
DEFAULT_CACHE_LOOKBACK_DAYS = 60
MAX_REFERENCE_BYTES = 5 * 1024 * 1024


@dataclass(frozen=True)
class ReferenceCase:
    case_id: str
    ticker: str
    from_date: date
    to_date: date
    tags: frozenset[str]
    bars: tuple[Mapping[str, Any], ...]
    fvgs: tuple[Mapping[str, Any], ...]


class CountingSession:
    """Thread-safe request counter that does not inspect or copy secrets."""

    def __init__(self) -> None:
        self._session = requests.Session()
        self._lock = threading.Lock()
        self.calls = 0

    def get(self, *args: Any, **kwargs: Any) -> Any:
        with self._lock:
            self.calls += 1
        return self._session.get(*args, **kwargs)


class RequestPacer:
    def __init__(self, interval_seconds: float) -> None:
        self.interval_seconds = max(0.0, float(interval_seconds))
        self._lock = threading.Lock()
        self._last_request = 0.0

    def wait(self) -> None:
        if self.interval_seconds <= 0:
            return
        with self._lock:
            now = wall_time.monotonic()
            remaining = self.interval_seconds - (now - self._last_request)
            if remaining > 0:
                wall_time.sleep(remaining)
            self._last_request = wall_time.monotonic()


def _gate(passed: bool, **fields: Any) -> dict[str, Any]:
    return {"passed": bool(passed), **fields}


def _safe_error(error: MassiveError) -> dict[str, Any]:
    return error.diagnostic()


def _safe_float(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _parse_date(value: Any) -> Optional[date]:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value.strip())
    except ValueError:
        return None


def _parse_datetime(value: Any) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _ticker(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    candidate = value.strip().upper()
    if not candidate or len(candidate) > 16:
        return None
    if any(not (char.isalnum() or char in ".-_/") for char in candidate):
        return None
    return candidate


def _reference_bar_shape(value: Any) -> Optional[dict[str, Any]]:
    if not isinstance(value, Mapping):
        return None
    started_at = _parse_datetime(value.get("started_at"))
    numbers = {
        field: _safe_float(value.get(field))
        for field in ("open", "high", "low", "close", "volume")
    }
    if started_at is None or any(number is None for number in numbers.values()):
        return None
    return {
        "started_at": _iso(started_at),
        **{field: float(number) for field, number in numbers.items() if number is not None},
    }


def _reference_fvg_shape(value: Any) -> Optional[dict[str, Any]]:
    if not isinstance(value, Mapping):
        return None
    direction = value.get("direction")
    if direction not in {"bullish", "bearish"}:
        return None
    numbers = {field: _safe_float(value.get(field)) for field in ("low", "high", "midpoint")}
    indexes = {field: value.get(field) for field in ("start_index", "end_index")}
    if any(number is None for number in numbers.values()):
        return None
    if any(isinstance(index, bool) or not isinstance(index, int) or index < 0 for index in indexes.values()):
        return None
    return {
        "direction": direction,
        **{field: float(number) for field, number in numbers.items() if number is not None},
        **{field: int(index) for field, index in indexes.items()},
    }


def _load_reference_cases(path_value: Optional[str]) -> tuple[list[ReferenceCase], dict[str, Any]]:
    if not path_value:
        return [], _gate(False, reason="reference_file_required", minimum_cases=20)
    try:
        path = Path(path_value)
        if path.stat().st_size > MAX_REFERENCE_BYTES:
            return [], _gate(False, reason="reference_file_too_large")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return [], _gate(False, reason="reference_file_invalid")
    if not isinstance(payload, Mapping) or payload.get("version") != 1:
        return [], _gate(False, reason="reference_schema_invalid")
    raw_cases = payload.get("cases")
    if not isinstance(raw_cases, list):
        return [], _gate(False, reason="reference_cases_invalid")

    cases: list[ReferenceCase] = []
    seen_ids: set[str] = set()
    for raw in raw_cases:
        if not isinstance(raw, Mapping):
            return [], _gate(False, reason="reference_case_invalid")
        case_id = str(raw.get("id") or "").strip()
        ticker = _ticker(raw.get("ticker"))
        from_value = _parse_date(raw.get("from_date"))
        to_value = _parse_date(raw.get("to_date"))
        tags_value = raw.get("tags")
        raw_bars = raw.get("bars")
        raw_fvgs = raw.get("fvg")
        if (
            not case_id
            or case_id in seen_ids
            or ticker is None
            or from_value is None
            or to_value is None
            or from_value > to_value
            or not isinstance(tags_value, list)
            or not isinstance(raw_bars, list)
            or not isinstance(raw_fvgs, list)
        ):
            return [], _gate(False, reason="reference_case_invalid")
        bars = tuple(_reference_bar_shape(item) for item in raw_bars)
        fvgs = tuple(_reference_fvg_shape(item) for item in raw_fvgs)
        if any(item is None for item in bars) or any(item is None for item in fvgs):
            return [], _gate(False, reason="reference_case_shape_invalid")
        seen_ids.add(case_id)
        cases.append(
            ReferenceCase(
                case_id=case_id,
                ticker=ticker,
                from_date=from_value,
                to_date=to_value,
                tags=frozenset(str(tag).strip().lower() for tag in tags_value),
                bars=tuple(item for item in bars if item is not None),
                fvgs=tuple(item for item in fvgs if item is not None),
            )
        )
    return cases, _gate(
        len(cases) >= 20,
        reason=None if len(cases) >= 20 else "reference_case_count_too_low",
        case_count=len(cases),
        minimum_cases=20,
    )


def _build_universe(less_liquid: str, requested_size: int, configured: Optional[str], reference_tickers: Sequence[str]) -> list[str]:
    configured_items = (
        [item.strip().upper() for item in configured.split(",") if item.strip()]
        if configured
        else list(DEFAULT_UNIVERSE)
    )
    ordered: list[str] = []
    for item in (*CORE_TICKERS, less_liquid, *configured_items):
        ticker = _ticker(item)
        if ticker and ticker not in ordered:
            ordered.append(ticker)
    target_size = max(5, min(100, requested_size, len(ordered)))
    selected = ordered[:target_size]
    # Reference tickers are mandatory validation inputs, even when the
    # operator requested a small smoke universe.  They are appended and the
    # load gate reports the resulting tested count.
    for item in reference_tickers:
        ticker = _ticker(item)
        if ticker and ticker not in selected:
            selected.append(ticker)
    return selected


def _fetch(
    client: MassiveClient,
    pacer: RequestPacer,
    ticker: str,
    *,
    multiplier: int,
    timespan: str,
    from_date: date,
    to_date: date,
) -> tuple[Any, Optional[dict[str, Any]]]:
    pacer.wait()
    try:
        return (
            client.fetch_aggregates(
                ticker,
                multiplier=multiplier,
                timespan=timespan,
                from_date=from_date,
                to_date=to_date,
            ),
            None,
        )
    except MassiveError as error:
        return None, _safe_error(error)
    except Exception:
        return None, {"reason": "unexpected_provider_error", "retryable": False}


def _canonical_core(bar: Canonical4HBar) -> dict[str, Any]:
    return {
        "started_at": _iso(bar.started_at),
        "open": float(bar.open),
        "high": float(bar.high),
        "low": float(bar.low),
        "close": float(bar.close),
        "volume": float(bar.volume),
    }


def _numbers_equal(left: float, right: float, tolerance: float) -> bool:
    if tolerance == 0:
        return left == right
    return math.isclose(left, right, rel_tol=0.0, abs_tol=tolerance)


def _bars_match(actual: Sequence[Canonical4HBar], expected: Sequence[Mapping[str, Any]], tolerance: float) -> bool:
    if len(actual) != len(expected):
        return False
    fields = ("open", "high", "low", "close", "volume")
    for actual_bar, expected_bar in zip(actual, expected):
        actual_shape = _canonical_core(actual_bar)
        if actual_shape["started_at"] != expected_bar.get("started_at"):
            return False
        if any(not _numbers_equal(actual_shape[field], float(expected_bar[field]), tolerance) for field in fields):
            return False
    return True


def _fvg_geometry(candles: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    # The reference window is already bounded by the operator.  Do not let a
    # detector convenience lookback silently remove an early chart gap.
    gaps = detect_fair_value_gaps(candles, timeframe="4h", lookback=0)
    return [
        {
            "direction": gap.direction,
            "low": float(gap.low),
            "high": float(gap.high),
            "midpoint": float(gap.midpoint),
            "start_index": int(gap.start_index),
            "end_index": int(gap.end_index),
        }
        for gap in gaps
    ]


def _fvgs_match(actual: Sequence[dict[str, Any]], expected: Sequence[Mapping[str, Any]], tolerance: float) -> bool:
    if len(actual) != len(expected):
        return False
    numeric = ("low", "high", "midpoint")
    for actual_gap, expected_gap in zip(actual, expected):
        if actual_gap["direction"] != expected_gap.get("direction"):
            return False
        if any(not _numbers_equal(actual_gap[field], float(expected_gap[field]), tolerance) for field in numeric):
            return False
        if any(actual_gap[field] != int(expected_gap[field]) for field in ("start_index", "end_index")):
            return False
    return True


def _case_bars(bars: Sequence[Canonical4HBar], case: ReferenceCase) -> list[Canonical4HBar]:
    return [
        bar
        for bar in bars
        if case.from_date <= bar.started_at.astimezone(ET).date() <= case.to_date
    ]


def _reference_parity(
    cases: Sequence[ReferenceCase],
    fallback_by_ticker: Mapping[str, Sequence[Canonical4HBar]],
    *,
    tolerance: float,
) -> tuple[dict[str, Any], set[str]]:
    passed_cases = 0
    failures: list[str] = []
    covered_tags: set[str] = set()
    for case in cases:
        actual = _case_bars(fallback_by_ticker.get(case.ticker, ()), case)
        candle_dicts = [_canonical_core(bar) for bar in actual]
        bars_ok = _bars_match(actual, case.bars, tolerance)
        fvgs_ok = _fvgs_match(_fvg_geometry(candle_dicts), case.fvgs, tolerance) if bars_ok else False
        if bars_ok and fvgs_ok:
            passed_cases += 1
            covered_tags.update(case.tags)
        else:
            failures.append(case.case_id)
    return (
        _gate(
            len(cases) >= 20 and passed_cases == len(cases),
            case_count=len(cases),
            passed_case_count=passed_cases,
            failed_case_count=len(failures),
            failed_case_ids=failures[:20],
            required_tags=["dst", "early_close", "holiday"],
        ),
        covered_tags,
    )


def _history_gate(
    required_tickers: Sequence[str],
    fallback_by_ticker: Mapping[str, Sequence[Canonical4HBar]],
    *,
    required_history_days: int,
) -> dict[str, Any]:
    details: dict[str, Any] = {}
    all_passed = True
    for ticker in required_tickers:
        bars = tuple(fallback_by_ticker.get(ticker, ()))
        first = bars[0].started_at if bars else None
        last = bars[-1].started_at if bars else None
        span_days = (last - first).total_seconds() / 86400.0 if first and last else 0.0
        passed = bool(bars) and span_days >= required_history_days
        details[ticker] = {
            "passed": passed,
            "canonical_bar_count": len(bars),
            "first_bar_timestamp": _iso(first) if first else None,
            "last_bar_timestamp": _iso(last) if last else None,
            "span_days": round(span_days, 3),
        }
        all_passed = all_passed and passed
    return _gate(all_passed, required_history_days=required_history_days, tickers=details)


def _native_alignment_gate(
    cases: Sequence[ReferenceCase],
    fallback_by_ticker: Mapping[str, Sequence[Canonical4HBar]],
    native_by_ticker: Mapping[str, Sequence[Canonical4HBar]],
    *,
    tolerance: float,
) -> dict[str, Any]:
    comparisons: dict[str, Any] = {}
    all_native_cases_match = True
    for ticker in sorted(set(case.ticker for case in cases)):
        fallback = tuple(fallback_by_ticker.get(ticker, ()))
        native = tuple(native_by_ticker.get(ticker, ()))
        comparison = compare_native_to_rth(native, fallback, price_tolerance=tolerance)
        comparisons[ticker] = comparison
        for case in (item for item in cases if item.ticker == ticker):
            expected = case.bars
            if not _bars_match(_case_bars(native, case), expected, tolerance):
                all_native_cases_match = False
    native_exact = bool(cases) and all_native_cases_match and all(item.get("aligned") for item in comparisons.values())
    fallback_available = all(bool(fallback_by_ticker.get(case.ticker)) for case in cases)
    native_available = all(bool(native_by_ticker.get(case.ticker)) for case in cases)
    if native_exact:
        decision = "native"
    elif fallback_available and native_available:
        decision = "rth_15m"
    else:
        decision = "undetermined"
    return _gate(
        len(cases) >= 20 and decision in {"native", "rth_15m"},
        reference_case_count=len(cases),
        native_exact=native_exact,
        native_data_available=native_available,
        fallback_data_available=fallback_available,
        accepted_alignment=decision,
        alignment_version=MASSIVE_ALIGNMENT_VERSION,
        comparisons=comparisons,
    )


def _cache_singleflight_gate(
    client: MassiveClient,
    *,
    as_of: datetime,
    lookback_days: int,
    ticker: str = "SPY",
) -> dict[str, Any]:
    clear_massive_caches_for_tests()
    session = CountingSession()
    counted_client = MassiveClient(api_key=client._api_key, session=session, base_url=client.base_url)

    def read() -> Any:
        return get_canonical_4h_bars(
            ticker,
            as_of=as_of,
            lookback_days=lookback_days,
            client=counted_client,
        )

    try:
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _index: read(), range(8)))
    except Exception:
        return _gate(False, reason="singleflight_unexpected_error", request_count=session.calls)
    identities = [tuple(bar.source_bar_id for bar in result.bars) for result in results]
    passed = (
        len(results) == 8
        and session.calls == 1
        and all(result.status == "AVAILABLE" and result.bars for result in results)
        and len(set(identities)) == 1
    )
    return _gate(
        passed,
        ticker=ticker,
        concurrent_read_count=len(results),
        provider_request_count=session.calls,
        shared_identity=bool(identities and len(set(identities)) == 1),
        reason=None if passed else "duplicate_refresh_or_unavailable_dataset",
    )


def _env_float(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
    return value if math.isfinite(value) and value >= 0 else default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def run_canary(args: argparse.Namespace) -> dict[str, Any]:
    today_et = datetime.now(ET).date()
    parsed_from = _parse_date(args.from_date)
    parsed_to = _parse_date(args.to_date)
    from_date = parsed_from or (today_et - timedelta(days=args.history_days))
    to_date = parsed_to or today_et
    as_of = datetime.combine(to_date, time(23, 59, 59), tzinfo=ET).astimezone(UTC)
    less_liquid = _ticker(args.less_liquid_ticker or os.getenv("MASSIVE_CANARY_LESS_LIQUID_TICKER"))
    cases, reference_gate = _load_reference_cases(args.reference_file or os.getenv("MASSIVE_CANARY_REFERENCE_FILE"))
    if less_liquid is None:
        return {
            "canary_version": 1,
            "source": "massive",
            "status": "HARD_HOLD",
            "gates": {
                "credential_access": _gate(False, reason="less_liquid_ticker_required"),
                "references": reference_gate,
            },
        }
    if (
        (args.from_date and parsed_from is None)
        or (args.to_date and parsed_to is None)
        or from_date > to_date
        or args.history_days < 1
        or args.history_days > MASSIVE_MAX_LOOKBACK_DAYS
        or args.required_history_days < 1
        or args.request_interval_seconds < 0
    ):
        return {
            "canary_version": 1,
            "source": "massive",
            "status": "HARD_HOLD",
            "gates": {
                "credential_access": _gate(False, reason="date_range_invalid"),
                "references": reference_gate,
            },
        }

    universe = _build_universe(
        less_liquid,
        args.universe_size,
        args.tickers or os.getenv("MASSIVE_CANARY_TICKERS"),
        [case.ticker for case in cases],
    )
    required_tickers = list(dict.fromkeys((*CORE_TICKERS, less_liquid)))
    try:
        client = MassiveClient()
    except MassiveError as error:
        return {
            "canary_version": 1,
            "source": "massive",
            "status": "HARD_HOLD",
            "gates": {
                "credential_access": _gate(False, **_safe_error(error)),
                "references": reference_gate,
                "history": _gate(False, reason="credential_access_not_attempted"),
                "rth_geometry": _gate(False, reason="credential_access_not_attempted"),
                "native_alignment": _gate(False, reason="credential_access_not_attempted"),
                "universe_load": _gate(False, reason="credential_access_not_attempted"),
                "cache_singleflight": _gate(False, reason="credential_access_not_attempted"),
            },
            "universe": universe,
        }

    interval = args.request_interval_seconds
    pacer = RequestPacer(interval)
    fallback_fetches: dict[str, Any] = {}
    fallback_errors: dict[str, dict[str, Any]] = {}
    fallback_by_ticker: dict[str, tuple[Canonical4HBar, ...]] = {}
    provider_429_count = 0
    rate_limit_observations: list[dict[str, Any]] = []

    for ticker in universe:
        fetched, error = _fetch(
            client,
            pacer,
            ticker,
            multiplier=15,
            timespan="minute",
            from_date=from_date,
            to_date=to_date,
        )
        if error is not None:
            fallback_errors[ticker] = error
            if error.get("reason") == "rate_limited":
                provider_429_count += 1
            continue
        fallback_fetches[ticker] = fetched
        rate_limits = fetched.diagnostics.get("rate_limits")
        if isinstance(rate_limits, Mapping) and rate_limits:
            rate_limit_observations.append(dict(rate_limits))
        try:
            fallback_by_ticker[ticker] = build_rth_4h_bars(
                fetched.bars,
                ticker=ticker,
                as_of=as_of,
            )
        except MassiveError as error:
            fallback_errors[ticker] = _safe_error(error)

    credential_passed = all(ticker in fallback_fetches and bool(fallback_fetches[ticker].bars) for ticker in required_tickers)
    credential_gate = _gate(
        credential_passed,
        required_tickers=required_tickers,
        successful_tickers=sorted(ticker for ticker in required_tickers if ticker in fallback_fetches and fallback_fetches[ticker].bars),
        failed_tickers={ticker: fallback_errors.get(ticker, {"reason": "no_usable_bars"}) for ticker in required_tickers if ticker not in fallback_fetches or not fallback_fetches[ticker].bars},
    )

    native_by_ticker: dict[str, tuple[Canonical4HBar, ...]] = {}
    native_errors: dict[str, dict[str, Any]] = {}
    reference_tickers = sorted(set(case.ticker for case in cases))
    for ticker in reference_tickers:
        fetched, error = _fetch(
            client,
            pacer,
            ticker,
            multiplier=4,
            timespan="hour",
            from_date=from_date,
            to_date=to_date,
        )
        if error is not None:
            native_errors[ticker] = error
            if error.get("reason") == "rate_limited":
                provider_429_count += 1
            continue
        try:
            native_by_ticker[ticker] = build_native_4h_bars(fetched.bars, ticker=ticker, as_of=as_of)
        except MassiveError as error:
            native_errors[ticker] = _safe_error(error)

    history_gate = _history_gate(
        required_tickers,
        fallback_by_ticker,
        required_history_days=args.required_history_days,
    )
    parity_gate, covered_tags = _reference_parity(
        cases,
        fallback_by_ticker,
        tolerance=args.price_tolerance,
    )
    references_passed = bool(reference_gate["passed"] and parity_gate["passed"] and {"dst", "early_close", "holiday"}.issubset(covered_tags))
    references_gate = dict(reference_gate)
    references_gate.update(
        {
            "passed": references_passed,
            "parity": parity_gate,
            "covered_tags": sorted(covered_tags),
        }
    )
    native_gate = _native_alignment_gate(
        cases,
        fallback_by_ticker,
        native_by_ticker,
        tolerance=args.price_tolerance,
    )
    native_gate["passed"] = bool(native_gate["passed"] and references_passed)
    rth_geometry_gate = _gate(
        bool(fallback_by_ticker) and all(
            bar.constituent_count == bar.expected_constituent_count
            for bars in fallback_by_ticker.values()
            for bar in bars
        ),
        checked_ticker_count=len(fallback_by_ticker),
        incomplete_bars=sum(
            1
            for bars in fallback_by_ticker.values()
            for bar in bars
            if bar.constituent_count != bar.expected_constituent_count
        ),
        timezone="America/New_York",
        extended_hours_excluded=True,
    )
    universe_passed = (
        len(universe) >= max(5, args.universe_size)
        and all(ticker in fallback_fetches and ticker in fallback_by_ticker and fallback_by_ticker[ticker] for ticker in universe)
        and provider_429_count == 0
    )
    universe_gate = _gate(
        universe_passed,
        requested_ticker_count=args.universe_size,
        tested_ticker_count=len(universe),
        successful_ticker_count=sum(1 for ticker in universe if ticker in fallback_by_ticker and fallback_by_ticker[ticker]),
        provider_429_count=provider_429_count,
        failed_tickers={ticker: fallback_errors.get(ticker, {"reason": "no_completed_canonical_bars"}) for ticker in universe if ticker not in fallback_by_ticker or not fallback_by_ticker[ticker]},
    )
    cache_gate = _cache_singleflight_gate(
        client,
        as_of=as_of,
        lookback_days=min(DEFAULT_CACHE_LOOKBACK_DAYS, args.history_days),
        ticker="SPY",
    )
    gates = {
        "credential_access": credential_gate,
        "history": history_gate,
        "rth_geometry": rth_geometry_gate,
        "references": references_gate,
        "native_alignment": native_gate,
        "universe_load": universe_gate,
        "cache_singleflight": cache_gate,
        "no_unacceptable_429s": _gate(provider_429_count == 0, provider_429_count=provider_429_count),
    }
    passed = all(bool(gate.get("passed")) for gate in gates.values())
    return {
        "canary_version": 1,
        "source": "massive",
        "status": "PASS" if passed else "HARD_HOLD",
        "as_of": _iso(as_of),
        "requested_range": {"from_date": from_date.isoformat(), "to_date": to_date.isoformat()},
        "alignment_version": MASSIVE_ALIGNMENT_VERSION,
        "accepted_alignment": native_gate.get("accepted_alignment"),
        "universe": universe,
        "gates": gates,
        "diagnostics": {
            "fallback_request_count": sum(int(item.diagnostics.get("request_count") or 0) for item in fallback_fetches.values()),
            "native_error_count": len(native_errors),
            "provider_429_count": provider_429_count,
            "rate_limit_observations": rate_limit_observations[:10],
            "native_errors": native_errors,
        },
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the sanitized Massive 4H qualification canary")
    parser.add_argument("--from-date", help="Inclusive YYYY-MM-DD provider history start")
    parser.add_argument("--to-date", help="Inclusive YYYY-MM-DD provider history end")
    parser.add_argument("--history-days", type=int, default=DEFAULT_HISTORY_DAYS)
    parser.add_argument("--required-history-days", type=int, default=DEFAULT_REQUIRED_HISTORY_DAYS)
    parser.add_argument("--tickers", help="Comma-separated universe override")
    parser.add_argument("--less-liquid-ticker", help="Approved less-liquid AP underlying; required")
    parser.add_argument("--universe-size", type=int, default=DEFAULT_UNIVERSE_SIZE)
    parser.add_argument("--reference-file", help="Validation-only JSON fixture with at least 20 chart cases")
    parser.add_argument("--price-tolerance", type=float, default=DEFAULT_PRICE_TOLERANCE)
    parser.add_argument(
        "--request-interval-seconds",
        type=float,
        default=_env_float("MASSIVE_CANARY_REQUEST_INTERVAL_SECONDS", 0.0),
        help="Minimum delay between provider calls; set for a rate-limited plan",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parser().parse_args(argv)
    if args.price_tolerance < 0 or not math.isfinite(args.price_tolerance):
        args.price_tolerance = DEFAULT_PRICE_TOLERANCE
    if args.universe_size < 5:
        args.universe_size = 5
    try:
        report = run_canary(args)
    except MassiveError as error:
        report = {
            "canary_version": 1,
            "source": "massive",
            "status": "HARD_HOLD",
            "gates": {"credential_access": _gate(False, **_safe_error(error))},
        }
    except Exception:
        report = {
            "canary_version": 1,
            "source": "massive",
            "status": "HARD_HOLD",
            "gates": {"canary_runtime": _gate(False, reason="unexpected_canary_error")},
        }
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0 if report.get("status") == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
