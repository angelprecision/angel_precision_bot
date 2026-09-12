"""Massive stock aggregate transport and canonical completed 4H bars.

This module is deliberately an observe-only market-data boundary.  It owns
Massive authentication, response validation, session geometry, and the
per-ticker cache used by later structure work.  It does not know about
signals, clients, watchers, selectors, brokers, orders, positions, queues, or
eligibility.

The safe default is ``rth_15m``.  Massive's native ``4/hour`` endpoint can
include extended hours, so native bars are only used when an operator has
separately cleared the alignment canary and explicitly selects
``MASSIVE_4H_ALIGNMENT=native``.  The fallback still uses Massive, but builds
Angel Precision's 09:30 ET RTH buckets from Massive 15-minute aggregates.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
import math
import os
from threading import Event, Lock
from time import monotonic
from typing import Any, Mapping, Optional, Sequence
from urllib.parse import parse_qsl, quote, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import requests

from ap.logger import get_logger


log = get_logger("ap.massive_market_data")

ET = ZoneInfo("America/New_York")
MASSIVE_BASE_URL = "https://api.massive.com"
MASSIVE_SOURCE = "massive"
MASSIVE_PROVIDER_SCHEMA = "massive_v2_aggs_v1"
MASSIVE_ALIGNMENT_VERSION = "massive_4h_alignment_v1"
MASSIVE_CACHE_SCHEMA = "massive_canonical_4h_cache_v1"
MASSIVE_DEFAULT_LOOKBACK_DAYS = 60
MASSIVE_MAX_LOOKBACK_DAYS = 365
MASSIVE_DEFAULT_CACHE_TTL_SECONDS = 900.0
MASSIVE_DEFAULT_FAILURE_CACHE_TTL_SECONDS = 30.0
MASSIVE_SINGLEFLIGHT_WAIT_SECONDS = 30.0
MASSIVE_MAX_PAGES = 20


class MassiveError(RuntimeError):
    """A sanitized, machine-classifiable provider or validation failure."""

    def __init__(
        self,
        reason_code: str,
        *,
        status_code: Optional[int] = None,
        retryable: bool = False,
        rate_limits: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.reason_code = str(reason_code)
        self.status_code = status_code
        self.retryable = bool(retryable)
        self.rate_limits = dict(rate_limits or {})
        # Never include a URL, response body, request headers, or exception
        # text here.  The API key is sent in a header and must never reach a
        # loggable exception/diagnostic path.
        super().__init__(f"massive_{self.reason_code}")

    def diagnostic(self) -> dict[str, Any]:
        """Return only safe, machine-classifiable failure metadata."""
        result: dict[str, Any] = {
            "reason": self.reason_code,
            "retryable": self.retryable,
        }
        if self.status_code is not None:
            result["status_code"] = self.status_code
        if self.rate_limits:
            result["rate_limits"] = dict(self.rate_limits)
        return result


def _finite(value: Any, *, positive: bool = False) -> Optional[float]:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed) or (positive and parsed <= 0):
        return None
    return parsed


def _safe_int(value: Any, *, nonnegative: bool = False) -> Optional[int]:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    try:
        if float(value) != float(parsed):
            return None
    except (TypeError, ValueError, OverflowError):
        return None
    if nonnegative and parsed < 0:
        return None
    return parsed


def _iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_aware_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _parse_provider_timestamp(value: Any) -> Optional[datetime]:
    """Parse Massive's Unix-millisecond timestamp without guessing units."""
    if isinstance(value, bool) or value in (None, ""):
        return None
    if isinstance(value, (int, float)) or (
        isinstance(value, str) and value.strip().lstrip("-").isdigit()
    ):
        try:
            millis = int(value)
            if float(value) != float(millis) or millis <= 0:
                return None
            # A stock aggregate timestamp in this API is milliseconds.  A
            # seconds-sized value is ambiguous and is rejected, not guessed.
            if abs(millis) < 100_000_000_000:
                return None
            return datetime.fromtimestamp(millis / 1000.0, tz=timezone.utc)
        except (TypeError, ValueError, OverflowError, OSError):
            return None
    return _parse_aware_datetime(value)


def _normalize_ticker(value: Any) -> str:
    ticker = str(value or "").strip().upper()
    if not ticker or len(ticker) > 16 or any(
        not (char.isalnum() or char in ".-_" or char == "/") for char in ticker
    ):
        raise MassiveError("invalid_ticker")
    return ticker


def resolve_massive_api_key(env: Optional[Mapping[str, str]] = None) -> tuple[str, str]:
    """Resolve an environment-only credential without exposing its value."""
    values = os.environ if env is None else env
    preferred = str(values.get("MASSIVE_API_KEY") or "").strip()
    legacy = str(values.get("POLYGON_API_KEY") or "").strip()
    if preferred and legacy and preferred != legacy:
        raise MassiveError("ambiguous_credentials")
    if preferred:
        return preferred, "MASSIVE_API_KEY"
    if legacy:
        return legacy, "POLYGON_API_KEY"
    raise MassiveError("credential_missing")


def _safe_base_url(value: Any) -> str:
    raw = str(value or MASSIVE_BASE_URL).strip().rstrip("/")
    parts = urlsplit(raw)
    if parts.scheme != "https" or not parts.netloc or parts.query or parts.fragment:
        raise MassiveError("invalid_base_url")
    return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), "", ""))


def _rate_limit_headers(headers: Any) -> dict[str, Any]:
    """Copy only non-secret, scalar rate-limit metadata."""
    if not isinstance(headers, Mapping):
        return {}
    result: dict[str, Any] = {}
    normalized_headers = {str(key).lower(): value for key, value in headers.items()}
    for key in ("x-ratelimit-limit", "x-ratelimit-remaining", "retry-after"):
        value = normalized_headers.get(key)
        if value in (None, ""):
            continue
        text = str(value).strip()
        if len(text) <= 64:
            result[key.lower().replace("-", "_")] = text
    return result


def _stable_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _number_token(value: Optional[float]) -> Optional[str]:
    return None if value is None else float(value).hex()


@dataclass(frozen=True)
class MassiveAggregate:
    """Strictly normalized one provider aggregate."""

    ticker: str
    started_at: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: Optional[float]
    transactions: Optional[int]
    adjusted: bool
    source_bar_id: str
    interval_minutes: int = 15

    def fingerprint(self) -> tuple[Any, ...]:
        return (
            self.ticker,
            self.started_at,
            _number_token(self.open),
            _number_token(self.high),
            _number_token(self.low),
            _number_token(self.close),
            _number_token(self.volume),
            _number_token(self.vwap),
            self.transactions,
            self.adjusted,
            self.interval_minutes,
        )


@dataclass(frozen=True)
class MassiveFetchResult:
    ticker: str
    bars: tuple[MassiveAggregate, ...]
    diagnostics: Mapping[str, Any]


@dataclass(frozen=True)
class RTHSession:
    session_date: date
    open_at: datetime
    close_at: datetime
    early_close: bool = False


@dataclass(frozen=True)
class Canonical4HBar:
    ticker: str
    timeframe: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: Optional[float]
    transactions: Optional[int]
    started_at: datetime
    completed_at: datetime
    source: str
    adjusted: bool
    provider_schema: str
    alignment_version: str
    source_bar_id: str
    constituent_count: int
    expected_constituent_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "timeframe": self.timeframe,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "vwap": self.vwap,
            "transactions": self.transactions,
            "started_at": _iso(self.started_at),
            "completed_at": _iso(self.completed_at),
            "source": self.source,
            "adjusted": self.adjusted,
            "provider_schema": self.provider_schema,
            "alignment_version": self.alignment_version,
            "source_bar_id": self.source_bar_id,
            "constituent_count": self.constituent_count,
            "expected_constituent_count": self.expected_constituent_count,
        }


@dataclass(frozen=True)
class Canonical4HBarSet:
    ticker: str
    bars: tuple[Canonical4HBar, ...]
    status: str
    reason: Optional[str]
    source: str
    alignment_version: str
    as_of: datetime
    lookback_days: int
    fetched_at: Optional[datetime]
    stale_since: Optional[datetime]
    provider_status: str
    provider_request_count: int
    provider_result_count: int
    diagnostics: Mapping[str, Any]

    @property
    def available(self) -> bool:
        return self.status in {"AVAILABLE", "STALE", "EMPTY"} and bool(self.bars)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "timeframe": "4h",
            "bars": [bar.to_dict() for bar in self.bars],
            "status": self.status,
            "reason": self.reason,
            "source": self.source,
            "alignment_version": self.alignment_version,
            "as_of": _iso(self.as_of),
            "lookback_days": self.lookback_days,
            "fetched_at": _iso(self.fetched_at),
            "stale_since": _iso(self.stale_since),
            "provider_status": self.provider_status,
            "provider_request_count": self.provider_request_count,
            "provider_result_count": self.provider_result_count,
            "diagnostics": dict(self.diagnostics),
        }


def serialize_canonical_4h_bar_set(value: Canonical4HBarSet) -> dict[str, Any]:
    """Return a versioned, JSON-safe immutable snapshot for persistence."""
    payload = value.to_dict()
    payload["cache_schema"] = MASSIVE_CACHE_SCHEMA
    return payload


def deserialize_canonical_4h_bar_set(payload: Mapping[str, Any]) -> Canonical4HBarSet:
    """Strictly reload a previously serialized canonical snapshot.

    Persistence is intentionally caller-owned.  This helper gives a restart
    path without making a database, filesystem, or execution component part of
    the Massive provider boundary.
    """
    if not isinstance(payload, Mapping) or payload.get("cache_schema") != MASSIVE_CACHE_SCHEMA:
        raise MassiveError("cache_schema_invalid")
    ticker = _normalize_ticker(payload.get("ticker"))
    if payload.get("timeframe") != "4h" or payload.get("source") != MASSIVE_SOURCE:
        raise MassiveError("cache_identity_invalid")
    alignment_version = payload.get("alignment_version")
    if alignment_version != MASSIVE_ALIGNMENT_VERSION:
        raise MassiveError("cache_alignment_invalid")
    as_of = _parse_aware_datetime(payload.get("as_of"))
    if as_of is None:
        raise MassiveError("cache_as_of_invalid")
    lookback_days = _safe_int(payload.get("lookback_days"), nonnegative=True)
    if lookback_days is None or not 1 <= lookback_days <= MASSIVE_MAX_LOOKBACK_DAYS:
        raise MassiveError("cache_lookback_invalid")
    status = payload.get("status")
    if status not in {"AVAILABLE", "EMPTY", "STALE", "UNAVAILABLE"}:
        raise MassiveError("cache_status_invalid")
    reason = payload.get("reason")
    if reason is not None and (not isinstance(reason, str) or not reason):
        raise MassiveError("cache_reason_invalid")
    fetched_at = _parse_aware_datetime(payload.get("fetched_at")) if payload.get("fetched_at") else None
    stale_since = _parse_aware_datetime(payload.get("stale_since")) if payload.get("stale_since") else None
    if payload.get("fetched_at") and fetched_at is None:
        raise MassiveError("cache_fetched_at_invalid")
    if payload.get("stale_since") and stale_since is None:
        raise MassiveError("cache_stale_since_invalid")
    bars_value = payload.get("bars")
    if not isinstance(bars_value, list):
        raise MassiveError("cache_bars_invalid")

    bars: list[Canonical4HBar] = []
    for raw_bar in bars_value:
        if not isinstance(raw_bar, Mapping):
            raise MassiveError("cache_bar_invalid")
        if raw_bar.get("ticker") != ticker or raw_bar.get("timeframe") != "4h":
            raise MassiveError("cache_bar_identity_invalid")
        if raw_bar.get("source") != MASSIVE_SOURCE or raw_bar.get("adjusted") is not True:
            raise MassiveError("cache_bar_source_invalid")
        if raw_bar.get("provider_schema") != MASSIVE_PROVIDER_SCHEMA or raw_bar.get("alignment_version") != MASSIVE_ALIGNMENT_VERSION:
            raise MassiveError("cache_bar_schema_invalid")
        started_at = _parse_aware_datetime(raw_bar.get("started_at"))
        completed_at = _parse_aware_datetime(raw_bar.get("completed_at"))
        if started_at is None or completed_at is None or completed_at <= started_at:
            raise MassiveError("cache_bar_time_invalid")
        values = {
            field: _finite(raw_bar.get(field), positive=field != "volume")
            for field in ("open", "high", "low", "close", "volume")
        }
        if any(value is None for value in values.values()):
            raise MassiveError("cache_bar_ohlcv_invalid")
        opened = values["open"]
        high = values["high"]
        low = values["low"]
        close = values["close"]
        volume = values["volume"]
        assert opened is not None and high is not None and low is not None
        assert close is not None and volume is not None
        if high < max(opened, close) or low > min(opened, close) or high < low:
            raise MassiveError("cache_bar_geometry_invalid")
        vwap = _finite(raw_bar.get("vwap"), positive=True) if raw_bar.get("vwap") is not None else None
        if raw_bar.get("vwap") is not None and vwap is None:
            raise MassiveError("cache_bar_vwap_invalid")
        transactions = _safe_int(raw_bar.get("transactions"), nonnegative=True) if raw_bar.get("transactions") is not None else None
        if raw_bar.get("transactions") is not None and transactions is None:
            raise MassiveError("cache_bar_transactions_invalid")
        constituent_count = _safe_int(raw_bar.get("constituent_count"), nonnegative=True)
        expected_constituent_count = _safe_int(raw_bar.get("expected_constituent_count"), nonnegative=True)
        source_bar_id = raw_bar.get("source_bar_id")
        if (
            not source_bar_id
            or not isinstance(source_bar_id, str)
            or constituent_count is None
            or expected_constituent_count is None
            or constituent_count != expected_constituent_count
        ):
            raise MassiveError("cache_bar_constituents_invalid")
        bars.append(
            Canonical4HBar(
                ticker=ticker,
                timeframe="4h",
                open=opened,
                high=high,
                low=low,
                close=close,
                volume=volume,
                vwap=vwap,
                transactions=transactions,
                started_at=started_at,
                completed_at=completed_at,
                source=MASSIVE_SOURCE,
                adjusted=True,
                provider_schema=MASSIVE_PROVIDER_SCHEMA,
                alignment_version=MASSIVE_ALIGNMENT_VERSION,
                source_bar_id=source_bar_id,
                constituent_count=constituent_count,
                expected_constituent_count=expected_constituent_count,
            )
        )
    diagnostics = payload.get("diagnostics")
    if not isinstance(diagnostics, Mapping):
        raise MassiveError("cache_diagnostics_invalid")
    provider_request_count = _safe_int(payload.get("provider_request_count"), nonnegative=True)
    provider_result_count = _safe_int(payload.get("provider_result_count"), nonnegative=True)
    provider_status = payload.get("provider_status")
    if provider_request_count is None or provider_result_count is None or not isinstance(provider_status, str):
        raise MassiveError("cache_provider_metadata_invalid")
    return Canonical4HBarSet(
        ticker=ticker,
        bars=tuple(bars),
        status=status,
        reason=reason,
        source=MASSIVE_SOURCE,
        alignment_version=MASSIVE_ALIGNMENT_VERSION,
        as_of=as_of,
        lookback_days=lookback_days,
        fetched_at=fetched_at,
        stale_since=stale_since,
        provider_status=provider_status,
        provider_request_count=provider_request_count,
        provider_result_count=provider_result_count,
        diagnostics=dict(diagnostics),
    )


class MassiveClient:
    """Small header-authenticated Massive REST client with safe diagnostics."""

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        session: Any = None,
        base_url: Optional[str] = None,
        timeout: tuple[float, float] = (3.05, 20.0),
    ) -> None:
        if api_key is None:
            api_key, credential_source = resolve_massive_api_key()
        else:
            api_key = str(api_key).strip()
            if not api_key:
                raise MassiveError("credential_missing")
            credential_source = "explicit_test_or_operator_value"
        self._api_key = api_key
        self.credential_source = credential_source
        self.base_url = _safe_base_url(base_url or os.getenv("MASSIVE_API_BASE_URL"))
        self.session = session if session is not None else requests.Session()
        self.timeout = timeout

    @property
    def cache_namespace(self) -> str:
        # A cache shared by clients must still be isolated across credentials;
        # plan entitlements and accessible history can differ.  The key is
        # never returned, logged, or used as a URL component.
        return _stable_hash(
            {"base_url": self.base_url, "credential": self._api_key}
        )[:24]

    def _request_page(
        self,
        *,
        url: str,
        params: Optional[Mapping[str, Any]],
        endpoint: str,
        ticker: str,
    ) -> tuple[Mapping[str, Any], dict[str, Any]]:
        started = monotonic()
        headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._api_key}",
        }
        try:
            response = self.session.get(
                url,
                params=dict(params or {}),
                headers=headers,
                timeout=self.timeout,
            )
        except requests.exceptions.Timeout as exc:
            del exc
            raise MassiveError("timeout", retryable=True) from None
        except requests.exceptions.RequestException as exc:
            del exc
            raise MassiveError("network_error", retryable=True) from None
        except Exception as exc:
            del exc
            raise MassiveError("transport_error", retryable=True) from None

        status_code = _safe_int(getattr(response, "status_code", 200))
        if status_code is None or not 100 <= status_code <= 599:
            raise MassiveError("provider_http_status_invalid")
        rate_limits = _rate_limit_headers(getattr(response, "headers", None))
        if status_code in {401, 403}:
            raise MassiveError(
                "authentication_failed",
                status_code=status_code,
                rate_limits=rate_limits,
            )
        if status_code == 429:
            raise MassiveError(
                "rate_limited",
                status_code=status_code,
                retryable=True,
                rate_limits=rate_limits,
            )
        if status_code is not None and status_code >= 500:
            raise MassiveError(
                "provider_5xx",
                status_code=status_code,
                retryable=True,
                rate_limits=rate_limits,
            )
        if status_code is not None and status_code >= 400:
            raise MassiveError(
                "provider_http_error",
                status_code=status_code,
                rate_limits=rate_limits,
            )
        try:
            payload = response.json()
        except Exception as exc:
            del exc
            raise MassiveError("malformed_json") from None
        if not isinstance(payload, Mapping):
            raise MassiveError("malformed_payload")
        return payload, {
            "endpoint": endpoint,
            "ticker": ticker,
            "status_code": status_code,
            "duration_ms": round((monotonic() - started) * 1000.0, 3),
            "rate_limits": rate_limits,
        }

    def fetch_aggregates(
        self,
        ticker: str,
        *,
        multiplier: int,
        timespan: str,
        from_date: date,
        to_date: date,
        limit: int = 50_000,
    ) -> MassiveFetchResult:
        key = _normalize_ticker(ticker)
        if timespan not in {"minute", "hour", "day"}:
            raise MassiveError("unsupported_timespan")
        if not isinstance(multiplier, int) or isinstance(multiplier, bool) or multiplier <= 0:
            raise MassiveError("invalid_multiplier")
        if from_date > to_date:
            raise MassiveError("invalid_date_range")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 50_000:
            raise MassiveError("invalid_limit")

        escaped = quote(key, safe="")
        endpoint = f"/v2/aggs/ticker/{escaped}/range/{multiplier}/{timespan}"
        url = f"{self.base_url}{endpoint}/{from_date.isoformat()}/{to_date.isoformat()}"
        params: Optional[dict[str, Any]] = {
            "adjusted": "true",
            "sort": "asc",
            "limit": str(limit),
        }
        all_bars: list[MassiveAggregate] = []
        schema_keys: set[str] = set()
        page_diagnostics: list[dict[str, Any]] = []
        request_count = 0
        provider_status = "UNKNOWN"
        adjusted_seen: Optional[bool] = None
        interval_minutes = multiplier * {"minute": 1, "hour": 60, "day": 1440}[timespan]

        for page_number in range(MASSIVE_MAX_PAGES):
            payload, request_meta = self._request_page(
                url=url,
                params=params,
                endpoint=endpoint,
                ticker=key,
            )
            request_count += 1
            page_diagnostics.append(request_meta)
            raw_status = payload.get("status")
            provider_status = str(raw_status or "").strip().upper()
            if provider_status != "OK":
                raise MassiveError("provider_status_invalid")

            adjusted = payload.get("adjusted")
            if not isinstance(adjusted, bool):
                raise MassiveError("adjustment_flag_missing")
            if not adjusted:
                raise MassiveError("unadjusted_response")
            if adjusted_seen is None:
                adjusted_seen = adjusted
            elif adjusted_seen != adjusted:
                raise MassiveError("adjustment_flag_changed")

            results = payload.get("results", [])
            if results is None:
                results = []
            if not isinstance(results, list):
                raise MassiveError("results_not_a_list")
            count = payload.get("resultsCount")
            if count is not None:
                parsed_count = _safe_int(count, nonnegative=True)
                if parsed_count is None or parsed_count != len(results):
                    raise MassiveError("results_count_mismatch")

            for row in results:
                if not isinstance(row, Mapping):
                    raise MassiveError("result_row_not_an_object")
                schema_keys.update(str(field) for field in row.keys())
                all_bars.append(
                    self._normalize_row(
                        key,
                        row,
                        adjusted=adjusted,
                        interval_minutes=interval_minutes,
                    )
                )

            next_url = payload.get("next_url")
            if not next_url:
                break
            url, params = self._next_page_url(str(next_url), expected_host=urlsplit(self.base_url).netloc)
        else:
            raise MassiveError("pagination_limit")

        bars = _dedupe_ordered_bars(all_bars)
        first = bars[0].started_at if bars else None
        last = bars[-1].started_at if bars else None
        duration_ms = round(sum(float(item.get("duration_ms") or 0) for item in page_diagnostics), 3)
        diagnostics = {
            "status": provider_status,
            "ticker": key,
            "result_count": len(bars),
            "request_count": request_count,
            "first_bar_timestamp": _iso(first),
            "last_bar_timestamp": _iso(last),
            "schema_keys": sorted(schema_keys),
            "duration_ms": duration_ms,
            "adjusted": bool(adjusted_seen),
            "provider_schema": MASSIVE_PROVIDER_SCHEMA,
            "rate_limits": _merge_rate_limits(page_diagnostics),
        }
        return MassiveFetchResult(key, tuple(bars), diagnostics)

    @staticmethod
    def _normalize_row(
        ticker: str,
        row: Mapping[str, Any],
        *,
        adjusted: bool,
        interval_minutes: int = 15,
    ) -> MassiveAggregate:
        started_at = _parse_provider_timestamp(row.get("t"))
        if started_at is None:
            raise MassiveError("timestamp_invalid")
        values = {
            name: _finite(row.get(field), positive=name != "volume")
            for name, field in (
                ("open", "o"),
                ("high", "h"),
                ("low", "l"),
                ("close", "c"),
                ("volume", "v"),
            )
        }
        if any(value is None for value in values.values()):
            raise MassiveError("ohlcv_invalid")
        opened = values["open"]
        high = values["high"]
        low = values["low"]
        close = values["close"]
        volume = values["volume"]
        assert opened is not None and high is not None and low is not None
        assert close is not None and volume is not None
        if high < max(opened, close) or low > min(opened, close) or high < low:
            raise MassiveError("ohlc_geometry_invalid")
        vwap = _finite(row.get("vw"), positive=True) if row.get("vw") not in (None, "") else None
        if row.get("vw") not in (None, "") and vwap is None:
            raise MassiveError("vwap_invalid")
        transactions = _safe_int(row.get("n"), nonnegative=True) if row.get("n") not in (None, "") else None
        if row.get("n") not in (None, "") and transactions is None:
            raise MassiveError("transactions_invalid")
        source_bar_id = _stable_hash(
            {
                "source": MASSIVE_SOURCE,
                "schema": MASSIVE_PROVIDER_SCHEMA,
                "ticker": ticker,
                "started_at": _iso(started_at),
                "o": _number_token(opened),
                "h": _number_token(high),
                "l": _number_token(low),
                "c": _number_token(close),
                "v": _number_token(volume),
                "vw": _number_token(vwap),
                "n": transactions,
                "adjusted": adjusted,
                "interval_minutes": interval_minutes,
            }
        )
        return MassiveAggregate(
            ticker=ticker,
            started_at=started_at,
            open=opened,
            high=high,
            low=low,
            close=close,
            volume=volume,
            vwap=vwap,
            transactions=transactions,
            adjusted=adjusted,
            source_bar_id=source_bar_id,
            interval_minutes=interval_minutes,
        )

    def _next_page_url(self, raw_url: str, *, expected_host: str) -> tuple[str, dict[str, str]]:
        parts = urlsplit(raw_url)
        if parts.scheme != "https" or parts.netloc != expected_host or not parts.path.startswith("/v2/aggs/"):
            raise MassiveError("pagination_url_invalid")
        # Header authentication means no credential needs to be appended to a
        # provider cursor URL.  Strip a key defensively if a provider ever
        # returns one in a cursor URL.
        query = [(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True) if key.lower() != "apikey"]
        clean = urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))
        return clean, dict(query)


def _dedupe_ordered_bars(bars: Sequence[MassiveAggregate]) -> list[MassiveAggregate]:
    result: list[MassiveAggregate] = []
    prior: Optional[datetime] = None
    for bar in bars:
        if prior is not None and bar.started_at < prior:
            raise MassiveError("unordered_results")
        if result and bar.started_at == result[-1].started_at:
            if bar.fingerprint() != result[-1].fingerprint():
                raise MassiveError("duplicate_conflict")
            continue
        result.append(bar)
        prior = bar.started_at
    return result


def _nth_weekday(year: int, month: int, weekday: int, occurrence: int) -> date:
    first = date(year, month, 1)
    delta = (weekday - first.weekday()) % 7
    return first + timedelta(days=delta + 7 * (occurrence - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    last = next_month - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def _easter_sunday(year: int) -> date:
    # Gregorian computus; sufficient for the US-equity calendar range.
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _observed_fixed_holiday(year: int, month: int, day_number: int) -> date:
    actual = date(year, month, day_number)
    if actual.weekday() == 5:
        return actual - timedelta(days=1)
    if actual.weekday() == 6:
        return actual + timedelta(days=1)
    return actual


def _extra_holidays() -> frozenset[str]:
    raw = os.getenv("MARKET_HOLIDAYS_EXTRA", "")
    return frozenset(item.strip() for item in raw.split(",") if item.strip())


def _full_close_holidays(year: int) -> frozenset[date]:
    thanksgiving = _nth_weekday(year, 11, 3, 4)
    return frozenset(
        {
            _observed_fixed_holiday(year, 1, 1),
            _nth_weekday(year, 1, 0, 3),
            _nth_weekday(year, 2, 0, 3),
            _easter_sunday(year) - timedelta(days=2),
            _last_weekday(year, 5, 0),
            _observed_fixed_holiday(year, 6, 19),
            _observed_fixed_holiday(year, 7, 4),
            _nth_weekday(year, 9, 0, 1),
            thanksgiving,
            _observed_fixed_holiday(year, 12, 25),
            # When January 1 falls on Saturday, its observed Friday closure is
            # December 31 of the prior calendar year.  Include the next year's
            # observed New Year's Day so that _is_session_date(Dec 31) is
            # correct without importing an exchange-calendar dependency.
            _observed_fixed_holiday(year + 1, 1, 1),
        }
    )


def _is_session_date(value: date) -> bool:
    return value.weekday() < 5 and value.isoformat() not in _extra_holidays() and value not in _full_close_holidays(value.year)


def _last_session_before(value: date) -> date:
    probe = value - timedelta(days=1)
    for _ in range(370):
        if _is_session_date(probe):
            return probe
        probe -= timedelta(days=1)
    raise MassiveError("calendar_previous_session_missing")


def _early_close_dates(year: int) -> frozenset[date]:
    thanksgiving = _nth_weekday(year, 11, 3, 4)
    candidates = {
        _last_session_before(_observed_fixed_holiday(year, 7, 4)),
        _last_session_before(_observed_fixed_holiday(year, 12, 25)),
        thanksgiving + timedelta(days=1),
    }
    return frozenset(item for item in candidates if _is_session_date(item))


def session_window(session_date: date) -> Optional[RTHSession]:
    if not _is_session_date(session_date):
        return None
    early = session_date in _early_close_dates(session_date.year)
    close_time = time(13, 0) if early else time(16, 0)
    open_at = datetime.combine(session_date, time(9, 30), tzinfo=ET)
    close_at = datetime.combine(session_date, close_time, tzinfo=ET)
    return RTHSession(session_date, open_at, close_at, early)


def _bucket_specs(session: RTHSession) -> list[tuple[datetime, datetime]]:
    first_end = min(session.open_at + timedelta(hours=4), session.close_at)
    specs = [(session.open_at, first_end)]
    second_start = session.open_at + timedelta(hours=4)
    if second_start < session.close_at:
        specs.append((second_start, session.close_at))
    return specs


def _date_range(start: date, end: date) -> list[date]:
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]


def _canonical_data_boundary(as_of: datetime) -> datetime:
    """Return the latest canonical 4H boundary known at ``as_of``."""
    local = as_of.astimezone(ET)
    current = session_window(local.date())
    if current is None or local < current.open_at:
        prior_date = _last_session_before(local.date())
        prior = session_window(prior_date)
        assert prior is not None
        return prior.close_at.astimezone(timezone.utc)
    specs = _bucket_specs(current)
    completed = [end for _start, end in specs if end <= local]
    if completed:
        return completed[-1].astimezone(timezone.utc)
    prior_date = _last_session_before(local.date())
    prior = session_window(prior_date)
    assert prior is not None
    return prior.close_at.astimezone(timezone.utc)


def build_rth_4h_bars(
    source_bars: Sequence[MassiveAggregate],
    *,
    ticker: str,
    as_of: Optional[datetime] = None,
) -> tuple[Canonical4HBar, ...]:
    """Build AP's completed 09:30-anchored RTH bars from Massive 15m bars."""
    key = _normalize_ticker(ticker)
    cutoff = as_of or datetime.now(timezone.utc)
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise MassiveError("as_of_timezone_required")
    cutoff = cutoff.astimezone(timezone.utc)

    by_session: dict[date, list[MassiveAggregate]] = {}
    for bar in _dedupe_ordered_bars(source_bars):
        if bar.ticker != key:
            raise MassiveError("ticker_mismatch")
        if bar.started_at.tzinfo is None or bar.started_at.utcoffset() is None:
            raise MassiveError("timestamp_timezone_missing")
        if bar.interval_minutes != 15:
            raise MassiveError("rth_constituent_interval_invalid")
        local = bar.started_at.astimezone(ET)
        window = session_window(local.date())
        if window is None or local < window.open_at or local >= window.close_at:
            # Extended-hours and closed-day bars are intentionally excluded.
            continue
        if local.minute % 15 != 0 or local.second or local.microsecond:
            raise MassiveError("rth_constituent_alignment_invalid")
        if local < window.open_at or local + timedelta(minutes=15) > window.close_at:
            raise MassiveError("rth_constituent_boundary_invalid")
        by_session.setdefault(local.date(), []).append(bar)

    output: list[Canonical4HBar] = []
    for session_date in sorted(by_session):
        window = session_window(session_date)
        assert window is not None
        rows = sorted(by_session[session_date], key=lambda item: item.started_at)
        for bucket_start, bucket_end in _bucket_specs(window):
            if bucket_end.astimezone(timezone.utc) > cutoff:
                continue
            members = [
                row
                for row in rows
                if bucket_start <= row.started_at.astimezone(ET) < bucket_end
            ]
            if not members:
                continue
            expected = int((bucket_end - bucket_start).total_seconds() // (15 * 60))
            expected_starts = {
                bucket_start + timedelta(minutes=15 * index)
                for index in range(expected)
            }
            member_by_start: dict[datetime, MassiveAggregate] = {}
            for member in members:
                member_start = member.started_at.astimezone(ET)
                if member_start not in expected_starts:
                    raise MassiveError("rth_constituent_alignment_invalid")
                if member_start in member_by_start:
                    raise MassiveError("rth_constituent_duplicate")
                member_by_start[member_start] = member
            # Do not synthesize a partial canonical bar when a provider
            # omitted a no-trade/interval row.  The incomplete bucket simply
            # remains unavailable; other complete historical buckets survive.
            if set(member_by_start) != expected_starts:
                continue
            members = [member_by_start[start] for start in sorted(expected_starts)]
            first = members[0]
            last = members[-1]
            if any(member.adjusted is not True for member in members):
                raise MassiveError("unadjusted_constituent")
            vwap = None
            if all(member.vwap is not None for member in members):
                total_volume = sum(member.volume for member in members)
                if total_volume > 0:
                    vwap = sum(float(member.vwap) * member.volume for member in members) / total_volume
            transactions = (
                sum(member.transactions for member in members)
                if all(member.transactions is not None for member in members)
                else None
            )
            source_bar_id = _stable_hash(
                {
                    "source": MASSIVE_SOURCE,
                    "schema": MASSIVE_PROVIDER_SCHEMA,
                    "alignment": MASSIVE_ALIGNMENT_VERSION,
                    "ticker": key,
                    "started_at": _iso(bucket_start),
                    "completed_at": _iso(bucket_end),
                    "constituents": [member.source_bar_id for member in members],
                }
            )
            output.append(
                Canonical4HBar(
                    ticker=key,
                    timeframe="4h",
                    open=first.open,
                    high=max(member.high for member in members),
                    low=min(member.low for member in members),
                    close=last.close,
                    volume=sum(member.volume for member in members),
                    vwap=vwap,
                    transactions=transactions,
                    started_at=bucket_start.astimezone(timezone.utc),
                    completed_at=bucket_end.astimezone(timezone.utc),
                    source=MASSIVE_SOURCE,
                    adjusted=True,
                    provider_schema=MASSIVE_PROVIDER_SCHEMA,
                    alignment_version=MASSIVE_ALIGNMENT_VERSION,
                    source_bar_id=source_bar_id,
                    constituent_count=len(members),
                    expected_constituent_count=expected,
                )
            )
    return tuple(output)


def build_native_4h_bars(
    source_bars: Sequence[MassiveAggregate],
    *,
    ticker: str,
    as_of: Optional[datetime] = None,
) -> tuple[Canonical4HBar, ...]:
    """Normalize native 4/hour bars only at exact AP RTH bucket starts."""
    key = _normalize_ticker(ticker)
    cutoff = as_of or datetime.now(timezone.utc)
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        raise MassiveError("as_of_timezone_required")
    cutoff = cutoff.astimezone(timezone.utc)
    output: list[Canonical4HBar] = []
    for source in _dedupe_ordered_bars(source_bars):
        if source.ticker != key:
            raise MassiveError("ticker_mismatch")
        if source.started_at.tzinfo is None or source.started_at.utcoffset() is None:
            raise MassiveError("timestamp_timezone_missing")
        local = source.started_at.astimezone(ET)
        window = session_window(local.date())
        if window is None:
            continue
        specs = _bucket_specs(window)
        match = next(((start, end) for start, end in specs if start == local), None)
        if match is None:
            # A provider bar inside the RTH window but at another boundary
            # proves native alignment is not the accepted contract.
            if window.open_at <= local < window.close_at:
                raise MassiveError("native_alignment_mismatch")
            continue
        bucket_start, bucket_end = match
        if source.interval_minutes != 240:
            raise MassiveError("native_interval_invalid")
        if window.early_close and bucket_end - bucket_start != timedelta(hours=4):
            # Native 4/hour output cannot represent AP's explicit early-close
            # partial bucket without an independent duration contract.  Leave
            # it out so the alignment canary selects the deterministic RTH
            # fallback instead of treating a possibly extended bar as truth.
            continue
        if bucket_end.astimezone(timezone.utc) > cutoff:
            continue
        if source.adjusted is not True:
            raise MassiveError("unadjusted_constituent")
        output.append(
            Canonical4HBar(
                ticker=key,
                timeframe="4h",
                open=source.open,
                high=source.high,
                low=source.low,
                close=source.close,
                volume=source.volume,
                vwap=source.vwap,
                transactions=source.transactions,
                started_at=bucket_start.astimezone(timezone.utc),
                completed_at=bucket_end.astimezone(timezone.utc),
                source=MASSIVE_SOURCE,
                adjusted=True,
                provider_schema=MASSIVE_PROVIDER_SCHEMA,
                alignment_version=MASSIVE_ALIGNMENT_VERSION,
                source_bar_id=source.source_bar_id,
                constituent_count=1,
                expected_constituent_count=1,
            )
        )
    return tuple(output)


def compare_native_to_rth(
    native: Sequence[Canonical4HBar],
    fallback: Sequence[Canonical4HBar],
    *,
    price_tolerance: float = 1e-9,
) -> dict[str, Any]:
    """Return a sanitized alignment comparison for the preflight canary."""
    native_by_start = {bar.started_at: bar for bar in native}
    fallback_by_start = {bar.started_at: bar for bar in fallback}
    starts = sorted(set(native_by_start) | set(fallback_by_start))
    mismatches: list[dict[str, Any]] = []
    for started_at in starts:
        left = native_by_start.get(started_at)
        right = fallback_by_start.get(started_at)
        if left is None or right is None:
            mismatches.append({"started_at": _iso(started_at), "reason": "missing_overlap"})
            continue
        fields = ("open", "high", "low", "close", "volume")
        if any(abs(float(getattr(left, field)) - float(getattr(right, field))) > price_tolerance for field in fields):
            mismatches.append({"started_at": _iso(started_at), "reason": "ohlcv_mismatch"})
    return {
        "native_count": len(native),
        "fallback_count": len(fallback),
        "overlap_count": len(set(native_by_start) & set(fallback_by_start)),
        "mismatch_count": len(mismatches),
        "mismatches": mismatches[:25],
        "aligned": not mismatches and len(native) == len(fallback),
    }


@dataclass
class _CacheEntry:
    stored_at: float
    result: Canonical4HBarSet
    is_failure: bool = False


@dataclass
class _Flight:
    event: Event
    result: Optional[Canonical4HBarSet] = None


_cache_lock = Lock()
_cache: dict[tuple[Any, ...], _CacheEntry] = {}
_last_good: dict[tuple[Any, ...], Canonical4HBarSet] = {}
_inflight: dict[tuple[Any, ...], _Flight] = {}


def _float_env(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
    return value if math.isfinite(value) and value >= 0 else default


def _int_env(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
    return value


def _configured_alignment(value: Optional[str] = None) -> str:
    alignment = str(value or os.getenv("MASSIVE_4H_ALIGNMENT", "rth_15m")).strip().lower()
    if alignment not in {"rth_15m", "native"}:
        raise MassiveError("alignment_unconfigured")
    return alignment


def _empty_result(
    ticker: str,
    *,
    status: str,
    reason: Optional[str],
    source: str,
    alignment: str,
    as_of: datetime,
    lookback_days: int,
    provider_status: str,
    fetched_at: Optional[datetime] = None,
    stale_since: Optional[datetime] = None,
    diagnostics: Optional[Mapping[str, Any]] = None,
) -> Canonical4HBarSet:
    return Canonical4HBarSet(
        ticker=ticker,
        bars=(),
        status=status,
        reason=reason,
        source=source,
        alignment_version=MASSIVE_ALIGNMENT_VERSION,
        as_of=as_of,
        lookback_days=lookback_days,
        fetched_at=fetched_at,
        stale_since=stale_since,
        provider_status=provider_status,
        provider_request_count=int((diagnostics or {}).get("request_count") or 0),
        provider_result_count=int((diagnostics or {}).get("result_count") or 0),
        diagnostics=dict(diagnostics or {}),
    )


def _with_bars(
    *,
    ticker: str,
    bars: tuple[Canonical4HBar, ...],
    status: str,
    reason: Optional[str],
    alignment: str,
    as_of: datetime,
    lookback_days: int,
    fetched_at: datetime,
    diagnostics: Mapping[str, Any],
) -> Canonical4HBarSet:
    return Canonical4HBarSet(
        ticker=ticker,
        bars=bars,
        status=status,
        reason=reason,
        source=MASSIVE_SOURCE,
        alignment_version=MASSIVE_ALIGNMENT_VERSION,
        as_of=as_of,
        lookback_days=lookback_days,
        fetched_at=fetched_at,
        stale_since=None,
        provider_status=str(diagnostics.get("status") or "OK"),
        provider_request_count=int(diagnostics.get("request_count") or 0),
        provider_result_count=int(diagnostics.get("result_count") or 0),
        diagnostics=dict(diagnostics),
    )


def _failure_result(
    *,
    key: tuple[Any, ...],
    ticker: str,
    alignment: str,
    as_of: datetime,
    lookback_days: int,
    error: MassiveError,
) -> Canonical4HBarSet:
    failure_time = datetime.now(timezone.utc)
    with _cache_lock:
        prior = _last_good.get((key[0], key[1], key[2], key[3]))
    if prior is not None:
        return Canonical4HBarSet(
            ticker=ticker,
            bars=prior.bars,
            status="STALE",
            reason=error.reason_code,
            source=MASSIVE_SOURCE,
            alignment_version=MASSIVE_ALIGNMENT_VERSION,
            as_of=as_of,
            lookback_days=lookback_days,
            fetched_at=prior.fetched_at,
            stale_since=failure_time,
            provider_status="ERROR",
            provider_request_count=0,
            provider_result_count=len(prior.bars),
            diagnostics={
                "status": "ERROR",
                **error.diagnostic(),
                "last_good_fetched_at": _iso(prior.fetched_at),
                "last_good_bar_count": len(prior.bars),
            },
        )
    return _empty_result(
        ticker,
        status="UNAVAILABLE",
        reason=error.reason_code,
        source=MASSIVE_SOURCE,
        alignment=alignment,
        as_of=as_of,
        lookback_days=lookback_days,
        provider_status="ERROR",
        stale_since=failure_time,
        diagnostics={"status": "ERROR", **error.diagnostic()},
    )


def _refresh(
    client: MassiveClient,
    *,
    ticker: str,
    alignment: str,
    as_of: datetime,
    lookback_days: int,
) -> Canonical4HBarSet:
    end_date = as_of.astimezone(ET).date()
    start_date = end_date - timedelta(days=lookback_days)
    if alignment == "native":
        fetched = client.fetch_aggregates(
            ticker,
            multiplier=4,
            timespan="hour",
            from_date=start_date,
            to_date=end_date,
        )
        bars = build_native_4h_bars(fetched.bars, ticker=ticker, as_of=as_of)
    else:
        fetched = client.fetch_aggregates(
            ticker,
            multiplier=15,
            timespan="minute",
            from_date=start_date,
            to_date=end_date,
        )
        bars = build_rth_4h_bars(fetched.bars, ticker=ticker, as_of=as_of)
    now = datetime.now(timezone.utc)
    diagnostics = dict(fetched.diagnostics)
    diagnostics.update(
        {
            "alignment": alignment,
            "alignment_version": MASSIVE_ALIGNMENT_VERSION,
            "lookback_start": start_date.isoformat(),
            "lookback_end": end_date.isoformat(),
            "canonical_bar_count": len(bars),
        }
    )
    return _with_bars(
        ticker=_normalize_ticker(ticker),
        bars=bars,
        status="AVAILABLE" if bars else "EMPTY",
        reason=None if bars else "no_completed_rth_bars",
        alignment=alignment,
        as_of=as_of,
        lookback_days=lookback_days,
        fetched_at=now,
        diagnostics=diagnostics,
    )


def get_canonical_4h_bars(
    ticker: str,
    *,
    as_of: Optional[datetime] = None,
    lookback_days: Optional[int] = None,
    client: Optional[MassiveClient] = None,
    alignment: Optional[str] = None,
    force_refresh: bool = False,
) -> Canonical4HBarSet:
    """Read/refresh one shared, completed Massive 4H dataset per ticker.

    This boundary never raises provider failures into callers.  It returns
    ``UNAVAILABLE`` or ``STALE`` with an explicit reason and retains the last
    good immutable dataset when one exists.
    """
    key_ticker = _normalize_ticker(ticker)
    cutoff = as_of or datetime.now(timezone.utc)
    if cutoff.tzinfo is None or cutoff.utcoffset() is None:
        return _empty_result(
            key_ticker,
            status="UNAVAILABLE",
            reason="as_of_timezone_required",
            source=MASSIVE_SOURCE,
            alignment="rth_15m",
            as_of=datetime.now(timezone.utc),
            lookback_days=MASSIVE_DEFAULT_LOOKBACK_DAYS,
            provider_status="NOT_REQUESTED",
        )
    cutoff = cutoff.astimezone(timezone.utc)
    try:
        selected_alignment = _configured_alignment(alignment)
        days = _int_env("MASSIVE_4H_LOOKBACK_DAYS", MASSIVE_DEFAULT_LOOKBACK_DAYS) if lookback_days is None else int(lookback_days)
        if not 1 <= days <= MASSIVE_MAX_LOOKBACK_DAYS:
            raise MassiveError("lookback_out_of_range")
        active_client = client or MassiveClient()
        boundary = _canonical_data_boundary(cutoff)
        cache_key = (
            active_client.cache_namespace,
            key_ticker,
            selected_alignment,
            days,
            cutoff.astimezone(ET).date().isoformat(),
            boundary.isoformat(),
        )
    except MassiveError as exc:
        return _empty_result(
            key_ticker,
            status="UNAVAILABLE",
            reason=exc.reason_code,
            source=MASSIVE_SOURCE,
            alignment=str(alignment or "rth_15m"),
            as_of=cutoff,
            lookback_days=MASSIVE_DEFAULT_LOOKBACK_DAYS,
            provider_status="NOT_REQUESTED",
        )
    now_mono = monotonic()
    cache_ttl = _float_env("MASSIVE_CACHE_TTL_SECONDS", MASSIVE_DEFAULT_CACHE_TTL_SECONDS)
    failure_ttl = _float_env("MASSIVE_FAILURE_CACHE_TTL_SECONDS", MASSIVE_DEFAULT_FAILURE_CACHE_TTL_SECONDS)

    if not force_refresh:
        with _cache_lock:
            entry = _cache.get(cache_key)
            if entry is not None:
                ttl = failure_ttl if entry.is_failure else cache_ttl
                if now_mono - entry.stored_at < ttl:
                    return entry.result

    with _cache_lock:
        flight = _inflight.get(cache_key)
        if flight is None:
            flight = _Flight(Event())
            _inflight[cache_key] = flight
            leader = True
        else:
            leader = False

    if not leader:
        if flight.event.wait(MASSIVE_SINGLEFLIGHT_WAIT_SECONDS) and flight.result is not None:
            return flight.result
        # A timed-out follower must not create a second provider request.
        with _cache_lock:
            prior = _last_good.get((cache_key[0], cache_key[1], cache_key[2], cache_key[3]))
        if prior is not None:
            return Canonical4HBarSet(
                ticker=key_ticker,
                bars=prior.bars,
                status="STALE",
                reason="singleflight_timeout",
                source=MASSIVE_SOURCE,
                alignment_version=MASSIVE_ALIGNMENT_VERSION,
                as_of=cutoff,
                lookback_days=days,
                fetched_at=prior.fetched_at,
                stale_since=datetime.now(timezone.utc),
                provider_status="UNKNOWN",
                provider_request_count=0,
                provider_result_count=len(prior.bars),
                diagnostics={"status": "STALE", "reason": "singleflight_timeout"},
            )
        return _empty_result(
            key_ticker,
            status="UNAVAILABLE",
            reason="singleflight_timeout",
            source=MASSIVE_SOURCE,
            alignment=selected_alignment,
            as_of=cutoff,
            lookback_days=days,
            provider_status="UNKNOWN",
        )

    result: Canonical4HBarSet
    try:
        try:
            result = _refresh(
                active_client,
                ticker=key_ticker,
                alignment=selected_alignment,
                as_of=cutoff,
                lookback_days=days,
            )
        except MassiveError as exc:
            result = _failure_result(
                key=cache_key,
                ticker=key_ticker,
                alignment=selected_alignment,
                as_of=cutoff,
                lookback_days=days,
                error=exc,
            )
        except Exception as exc:
            del exc
            result = _failure_result(
                key=cache_key,
                ticker=key_ticker,
                alignment=selected_alignment,
                as_of=cutoff,
                lookback_days=days,
                error=MassiveError("unexpected_refresh_error"),
            )
        is_failure = result.status in {"STALE", "UNAVAILABLE"}
        with _cache_lock:
            _cache[cache_key] = _CacheEntry(monotonic(), result, is_failure=is_failure)
            if result.status in {"AVAILABLE", "EMPTY"}:
                _last_good[(cache_key[0], cache_key[1], cache_key[2], cache_key[3])] = result
        return result
    finally:
        with _cache_lock:
            flight.result = locals().get("result")
            _inflight.pop(cache_key, None)
        flight.event.set()


def clear_massive_caches_for_tests() -> None:
    with _cache_lock:
        _cache.clear()
        _last_good.clear()
        _inflight.clear()


def _merge_rate_limits(pages: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for page in pages:
        values = page.get("rate_limits")
        if isinstance(values, Mapping):
            merged.update({str(key): value for key, value in values.items()})
    return merged
