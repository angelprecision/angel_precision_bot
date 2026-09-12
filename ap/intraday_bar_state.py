"""Deterministic, session-anchored intraday forming-bar state.

This module owns market bar state only.  It deliberately has no runtime
transport, clock reads, persistence, or downstream trading authority.  A
caller supplies source-timestamped observations and, separately, an optional
one-shot seed provider.

The live observation contract is intentionally small:

* ``ticker`` is one of :data:`BOUNDED_UNIVERSE`;
* ``timestamp``/``source_timestamp`` is timezone-aware and is the only value
  used for session and bucket placement;
* either ``price`` (a point observation) or a complete ``open``/``high``/
  ``low``/``close`` snapshot is supplied;
* ``volume``, when present, is a non-negative *incremental* contribution and
  must declare ``volume_kind="incremental"``.  Cumulative volume is not
  accepted because the state engine cannot infer its reset/interval scope.

The default calendar delegates full-session/holiday truth to the existing
``ap.flatline_alarm.is_trading_day`` authority.  Early closes are supplied as
an explicit schedule and the calendar can be replaced with an injected
authority when the deployment has a richer exchange-calendar source.
"""

from __future__ import annotations

import json
import math
import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Optional, Protocol
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")

BOUNDED_UNIVERSE = frozenset({"SPY", "QQQ", "IWM", "AAPL", "GOOGL", "NVDA", "MSFT"})
SUPPORTED_TIMEFRAMES = ("5m", "15m", "30m", "60m")
TIMEFRAME_MINUTES = {"5m": 5, "15m": 15, "30m": 30, "60m": 60}
STATE_SCHEMA_VERSION = "intraday_bar_state_v1"
DEFAULT_SOURCE_IDENTITY = "supplied_observation"
DEFAULT_SOURCE_VERSION = "1"

FORMING = "FORMING"
COMPLETED = "COMPLETED"

ACCEPTED = "ACCEPTED"
DUPLICATE = "DUPLICATE"
CONFLICTING_DUPLICATE = "CONFLICTING_DUPLICATE"
LATE_COMPLETED_BUCKET = "LATE_COMPLETED_BUCKET"
OUT_OF_ORDER_FORMING = "OUT_OF_ORDER_FORMING"
REJECTED = "REJECTED"
NOOP = "NOOP"


def _is_aware(value: Any) -> bool:
    return isinstance(value, datetime) and value.tzinfo is not None and value.utcoffset() is not None


def _parse_timestamp(value: Any) -> Optional[datetime]:
    """Parse an explicitly timezone-aware source timestamp into UTC."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if not _is_aware(parsed):
        return None
    return parsed.astimezone(timezone.utc)


def _iso_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _finite_number(value: Any, *, positive: bool = False, nonnegative: bool = False) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    if positive and number <= 0:
        return None
    if nonnegative and number < 0:
        return None
    return number


def _metadata_string(value: Any, default: str) -> Optional[str]:
    if value is None:
        return default
    if isinstance(value, (Mapping, list, tuple, set)):
        return None
    text = str(value).strip()
    return text or None


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


@dataclass(frozen=True)
class TradingSession:
    """One official RTH session in the exchange's local timezone."""

    session_date: date
    open: datetime
    close: datetime
    source: str = "injected_session_authority"

    def __post_init__(self) -> None:
        if not _is_aware(self.open) or not _is_aware(self.close):
            raise ValueError("TradingSession boundaries must be timezone-aware")
        open_local = self.open.astimezone(ET)
        close_local = self.close.astimezone(ET)
        if open_local.date() != self.session_date or close_local.date() != self.session_date:
            raise ValueError("TradingSession boundaries must match session_date")
        if close_local <= open_local:
            raise ValueError("TradingSession close must be after open")
        object.__setattr__(self, "open", open_local)
        object.__setattr__(self, "close", close_local)


class SessionCalendar(Protocol):
    def session_for(self, session_date: date) -> Optional[TradingSession]:
        """Return the official session, or ``None`` when the exchange is closed."""


# The existing main-branch calendar owns full closures.  This table contains
# only official NYSE early-close exceptions needed by the current supported
# schedule window.  A deployment with a richer calendar can inject it instead
# of relying on this compact fallback table.
NYSE_EARLY_CLOSES_ET: dict[date, time] = {
    date(2025, 7, 3): time(13, 0),
    date(2025, 11, 28): time(13, 0),
    date(2025, 12, 24): time(13, 0),
    date(2026, 11, 27): time(13, 0),
    date(2026, 12, 24): time(13, 0),
    date(2027, 7, 2): time(13, 0),
    date(2027, 11, 26): time(13, 0),
    date(2027, 12, 23): time(13, 0),
}


class NYSESessionCalendar:
    """Compact default adapter over the repository's NYSE day authority."""

    def __init__(self, *, early_closes: Optional[Mapping[date, time]] = None) -> None:
        self._early_closes = dict(early_closes or NYSE_EARLY_CLOSES_ET)

    def session_for(self, session_date: date) -> Optional[TradingSession]:
        try:
            from ap.flatline_alarm import is_trading_day
        except Exception:
            # Unknown calendar truth is not permission to fabricate a session.
            return None
        try:
            if not bool(is_trading_day(session_date)):
                return None
        except Exception:
            return None

        close_time = self._early_closes.get(session_date, time(16, 0))
        return TradingSession(
            session_date=session_date,
            open=datetime.combine(session_date, time(9, 30), tzinfo=ET),
            close=datetime.combine(session_date, close_time, tzinfo=ET),
            source="ap.flatline_alarm+NYSE_EARLY_CLOSES_ET",
        )


@dataclass(frozen=True)
class MarketObservation:
    """Typed form of the source-agnostic observation input contract."""

    ticker: str
    timestamp: datetime
    price: Optional[float] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None
    volume_kind: Optional[str] = None
    source_observation_id: Optional[str] = None
    source_identity: str = DEFAULT_SOURCE_IDENTITY
    source_version: str = DEFAULT_SOURCE_VERSION
    timeframe: Optional[str] = None

    def to_mapping(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "ticker": self.ticker,
            "timestamp": self.timestamp,
            "source_identity": self.source_identity,
            "source_version": self.source_version,
        }
        if self.timeframe is not None:
            result["timeframe"] = self.timeframe
        if self.price is not None:
            result["price"] = self.price
        if self.open is not None:
            result["open"] = self.open
        if self.high is not None:
            result["high"] = self.high
        if self.low is not None:
            result["low"] = self.low
        if self.close is not None:
            result["close"] = self.close
        if self.volume is not None:
            result["volume"] = self.volume
        if self.volume_kind is not None:
            result["volume_kind"] = self.volume_kind
        if self.source_observation_id is not None:
            result["source_observation_id"] = self.source_observation_id
        return result


@dataclass(frozen=True)
class Bar:
    ticker: str
    timeframe: str
    session_date: date
    bucket_start: datetime
    bucket_end: datetime
    status: str
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float]
    first_source_timestamp: datetime
    last_source_timestamp: datetime
    source_identity: str
    source_version: str
    state_schema_version: str = STATE_SCHEMA_VERSION

    @property
    def bar_id(self) -> str:
        local_start = self.bucket_start.astimezone(ET)
        return (
            f"{self.ticker}|{self.timeframe}|{self.session_date.isoformat()}|"
            f"{local_start.strftime('%H:%M:%S')}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "bar_id": self.bar_id,
            "ticker": self.ticker,
            "timeframe": self.timeframe,
            "session_date": self.session_date.isoformat(),
            "bucket_start": _iso_timestamp(self.bucket_start),
            "bucket_end": _iso_timestamp(self.bucket_end),
            "status": self.status,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "first_source_timestamp": _iso_timestamp(self.first_source_timestamp),
            "last_source_timestamp": _iso_timestamp(self.last_source_timestamp),
            "source_identity": self.source_identity,
            "source_version": self.source_version,
            "state_schema_version": self.state_schema_version,
        }


@dataclass(frozen=True)
class TimeframeUpdate:
    timeframe: str
    status: str
    bar_id: Optional[str] = None
    finalized_bar_id: Optional[str] = None
    reason: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "timeframe": self.timeframe,
            "status": self.status,
            "bar_id": self.bar_id,
            "finalized_bar_id": self.finalized_bar_id,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class UpdateResult:
    status: str
    ticker: Optional[str] = None
    source_timestamp: Optional[datetime] = None
    accepted: bool = False
    reason: Optional[str] = None
    timeframes: tuple[TimeframeUpdate, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "ticker": self.ticker,
            "source_timestamp": (
                _iso_timestamp(self.source_timestamp)
                if self.source_timestamp is not None
                else None
            ),
            "accepted": self.accepted,
            "reason": self.reason,
            "timeframes": [item.to_dict() for item in self.timeframes],
        }


@dataclass(frozen=True)
class AdvanceResult:
    status: str
    finalized: tuple[Bar, ...] = ()
    reason: Optional[str] = None


@dataclass(frozen=True)
class SeedResult:
    status: str
    rows_seen: int = 0
    accepted_rows: int = 0
    rejected_rows: int = 0
    results: tuple[UpdateResult, ...] = ()
    reason: Optional[str] = None


@dataclass(frozen=True)
class _NormalizedObservation:
    ticker: str
    timestamp: datetime
    targets: tuple[str, ...]
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float]
    source_observation_id: Optional[str]
    source_identity: str
    source_version: str
    fingerprint: str
    identity_key: str


@dataclass(frozen=True)
class _Bucket:
    ticker: str
    timeframe: str
    session_date: date
    bucket_start: datetime
    bucket_end: datetime

    @property
    def bar_id(self) -> str:
        local_start = self.bucket_start.astimezone(ET)
        return (
            f"{self.ticker}|{self.timeframe}|{self.session_date.isoformat()}|"
            f"{local_start.strftime('%H:%M:%S')}"
        )


@dataclass
class _MutableBar:
    bucket: _Bucket
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float]
    first_source_timestamp: datetime
    last_source_timestamp: datetime
    source_identities: set[str] = field(default_factory=set)
    source_versions: set[str] = field(default_factory=set)
    accepted_observation_count: int = 1

    @classmethod
    def from_observation(cls, bucket: _Bucket, observation: _NormalizedObservation) -> "_MutableBar":
        return cls(
            bucket=bucket,
            open=observation.open,
            high=observation.high,
            low=observation.low,
            close=observation.close,
            volume=observation.volume,
            first_source_timestamp=observation.timestamp,
            last_source_timestamp=observation.timestamp,
            source_identities={observation.source_identity},
            source_versions={observation.source_version},
        )

    def apply(self, observation: _NormalizedObservation) -> None:
        self.high = max(self.high, observation.high)
        self.low = min(self.low, observation.low)
        self.close = observation.close
        if observation.volume is not None:
            self.volume = (
                observation.volume
                if self.volume is None
                else self.volume + observation.volume
            )
        self.last_source_timestamp = observation.timestamp
        self.source_identities.add(observation.source_identity)
        self.source_versions.add(observation.source_version)
        self.accepted_observation_count += 1

    def _provenance(self, values: set[str]) -> str:
        return next(iter(values)) if len(values) == 1 else "MIXED"

    def to_bar(self, *, status: str) -> Bar:
        return Bar(
            ticker=self.bucket.ticker,
            timeframe=self.bucket.timeframe,
            session_date=self.bucket.session_date,
            bucket_start=self.bucket.bucket_start,
            bucket_end=self.bucket.bucket_end,
            status=status,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            first_source_timestamp=self.first_source_timestamp,
            last_source_timestamp=self.last_source_timestamp,
            source_identity=self._provenance(self.source_identities),
            source_version=self._provenance(self.source_versions),
        )

    def to_snapshot_dict(self) -> dict[str, Any]:
        result = self.to_bar(status=FORMING).to_dict()
        result.update(
            {
                "_source_identities": sorted(self.source_identities),
                "_source_versions": sorted(self.source_versions),
                "_accepted_observation_count": self.accepted_observation_count,
            }
        )
        return result


def _session_from_calendar(calendar: Any, session_date: date) -> Optional[TradingSession]:
    try:
        provider = getattr(calendar, "session_for", None)
        raw = provider(session_date) if callable(provider) else calendar(session_date)
    except Exception:
        return None
    if raw is None:
        return None
    if isinstance(raw, TradingSession):
        return raw
    if not isinstance(raw, Mapping):
        return None
    open_value = raw.get("open") or raw.get("session_open")
    close_value = raw.get("close") or raw.get("session_close")
    opened = _parse_timestamp(open_value)
    closed = _parse_timestamp(close_value)
    if opened is None or closed is None:
        return None
    try:
        return TradingSession(
            session_date=session_date,
            open=opened.astimezone(ET),
            close=closed.astimezone(ET),
            source=str(raw.get("source") or "injected_session_authority"),
        )
    except (TypeError, ValueError):
        return None


class IntradayBarState:
    """Small deterministic state machine for four RTH intraday timeframes."""

    def __init__(self, *, calendar: Optional[SessionCalendar | Callable[[date], Optional[TradingSession]]] = None) -> None:
        self.calendar: SessionCalendar | Callable[[date], Optional[TradingSession]] = (
            calendar if calendar is not None else NYSESessionCalendar()
        )
        self.universe = BOUNDED_UNIVERSE
        self.timeframes = SUPPORTED_TIMEFRAMES
        self._forming: dict[tuple[str, str], _MutableBar] = {}
        self._completed: dict[tuple[str, str, str], Bar] = {}
        self._latest_completed: dict[tuple[str, str], datetime] = {}
        self._seen_observations: dict[str, str] = {}
        self._seen_by_state: dict[tuple[str, str], set[str]] = {}
        self._seed_attempted = False
        self._seeded = False
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # Input normalization and bucket authority
    # ------------------------------------------------------------------
    @staticmethod
    def _as_mapping(observation: Mapping[str, Any] | MarketObservation) -> Optional[dict[str, Any]]:
        if isinstance(observation, MarketObservation):
            return observation.to_mapping()
        if isinstance(observation, Mapping):
            # Copy only.  The caller's object is never used as mutable state.
            return dict(observation)
        return None

    def _normalize_observation(
        self,
        observation: Mapping[str, Any] | MarketObservation,
        *,
        as_of: Optional[Any] = None,
    ) -> tuple[Optional[_NormalizedObservation], Optional[str]]:
        data = self._as_mapping(observation)
        if data is None:
            return None, "observation_mapping_required"

        raw_ticker = data.get("ticker") or data.get("symbol")
        if raw_ticker is None or not str(raw_ticker).strip():
            return None, "ticker_missing"
        ticker = str(raw_ticker).strip().upper()
        if ticker not in self.universe:
            return None, "ticker_not_in_bounded_universe"

        timestamp = _parse_timestamp(
            data.get("source_timestamp")
            if data.get("source_timestamp") is not None
            else data.get("timestamp")
            if data.get("timestamp") is not None
            else data.get("time")
        )
        if timestamp is None:
            return None, "timezone_aware_source_timestamp_required"

        if as_of is not None:
            cutoff = _parse_timestamp(as_of)
            if cutoff is None:
                return None, "timezone_aware_as_of_required"
            if timestamp > cutoff:
                return None, "future_observation"

        raw_target = data.get("timeframe")
        raw_targets = data.get("timeframes")
        if raw_targets is not None:
            if (
                isinstance(raw_targets, (str, bytes, Mapping))
                or not isinstance(raw_targets, Iterable)
            ):
                return None, "timeframes_malformed"
            candidates = list(raw_targets)
        elif raw_target is not None:
            candidates = [raw_target]
        else:
            candidates = list(self.timeframes)
        targets: list[str] = []
        for value in candidates:
            timeframe = str(value).strip() if value is not None else ""
            if timeframe not in self.timeframes:
                return None, "unsupported_timeframe"
            if timeframe not in targets:
                targets.append(timeframe)
        if not targets:
            return None, "timeframes_empty"
        target_tuple = tuple(tf for tf in self.timeframes if tf in targets)

        ohlc_present = any(key in data for key in ("open", "high", "low", "close", "o", "h", "l", "c"))
        price_present = any(key in data for key in ("price", "last", "value"))
        if ohlc_present and price_present:
            return None, "price_and_ohlc_ambiguous"
        if ohlc_present:
            raw_open = data.get("open") if "open" in data else data.get("o")
            raw_high = data.get("high") if "high" in data else data.get("h")
            raw_low = data.get("low") if "low" in data else data.get("l")
            raw_close = data.get("close") if "close" in data else data.get("c")
            values = (
                _finite_number(raw_open, positive=True),
                _finite_number(raw_high, positive=True),
                _finite_number(raw_low, positive=True),
                _finite_number(raw_close, positive=True),
            )
            if any(value is None for value in values):
                return None, "ohlc_malformed_or_nonfinite"
            opened, high, low, closed = values
        else:
            raw_price = data.get("price") if "price" in data else data.get("last") if "last" in data else data.get("value")
            price = _finite_number(raw_price, positive=True)
            if price is None:
                return None, "positive_finite_price_required"
            opened = high = low = closed = price

        assert opened is not None and high is not None and low is not None and closed is not None
        if high < low or not (low <= opened <= high) or not (low <= closed <= high):
            return None, "impossible_ohlc"

        volume: Optional[float] = None
        if "volume" in data and data.get("volume") is not None:
            volume = _finite_number(data.get("volume"), nonnegative=True)
            if volume is None:
                return None, "volume_malformed_or_negative"
            volume_kind = str(data.get("volume_kind") or "").strip().lower()
            if volume_kind != "incremental":
                return None, "volume_kind_must_be_incremental"

        source_identity = _metadata_string(
            data.get("source_identity") if "source_identity" in data else data.get("source"),
            DEFAULT_SOURCE_IDENTITY,
        )
        source_version = _metadata_string(data.get("source_version"), DEFAULT_SOURCE_VERSION)
        if source_identity is None or source_version is None:
            return None, "source_provenance_malformed"

        source_id: Optional[str] = None
        raw_source_id = (
            data.get("source_observation_id")
            if "source_observation_id" in data
            else data.get("observation_id")
            if "observation_id" in data
            else data.get("source_id")
        )
        if raw_source_id is not None:
            if isinstance(raw_source_id, (Mapping, list, tuple, set)):
                return None, "source_observation_id_malformed"
            source_id = str(raw_source_id).strip()
            if not source_id:
                return None, "source_observation_id_malformed"

        fingerprint_payload = {
            "ticker": ticker,
            "timestamp": _iso_timestamp(timestamp),
            "targets": list(target_tuple),
            "open": opened,
            "high": high,
            "low": low,
            "close": closed,
            "volume": volume,
            "source_identity": source_identity,
            "source_version": source_version,
            "source_observation_id": source_id,
        }
        fingerprint = _canonical_json(fingerprint_payload)
        identity_key = (
            f"source_id|{ticker}|{source_id}"
            if source_id is not None
            else f"canonical|{fingerprint}"
        )
        return (
            _NormalizedObservation(
                ticker=ticker,
                timestamp=timestamp,
                targets=target_tuple,
                open=opened,
                high=high,
                low=low,
                close=closed,
                volume=volume,
                source_observation_id=source_id,
                source_identity=source_identity,
                source_version=source_version,
                fingerprint=fingerprint,
                identity_key=identity_key,
            ),
            None,
        )

    def _bucket_for(self, ticker: str, timeframe: str, timestamp: datetime) -> tuple[Optional[_Bucket], Optional[str]]:
        local = timestamp.astimezone(ET)
        session = _session_from_calendar(self.calendar, local.date())
        if session is None:
            return None, "non_trading_session"
        if local < session.open or local >= session.close:
            return None, "outside_rth"
        elapsed_seconds = (local - session.open).total_seconds()
        if elapsed_seconds < 0:
            return None, "outside_rth"
        bucket_minutes = TIMEFRAME_MINUTES[timeframe]
        bucket_start = session.open + timedelta(
            minutes=int(elapsed_seconds // (bucket_minutes * 60)) * bucket_minutes
        )
        bucket_end = min(bucket_start + timedelta(minutes=bucket_minutes), session.close)
        return (
            _Bucket(
                ticker=ticker,
                timeframe=timeframe,
                session_date=session.session_date,
                bucket_start=bucket_start,
                bucket_end=bucket_end,
            ),
            None,
        )

    # ------------------------------------------------------------------
    # Incremental update path
    # ------------------------------------------------------------------
    def ingest(
        self,
        observation: Mapping[str, Any] | MarketObservation,
        *,
        as_of: Optional[Any] = None,
    ) -> UpdateResult:
        """Validate and apply one source-timestamped observation.

        ``as_of`` is optional explicit processing evidence.  When supplied it
        rejects source timestamps later than that evidence; no worker wall
        clock is consulted when it is omitted.
        """
        with self._lock:
            normalized, error = self._normalize_observation(observation, as_of=as_of)
            if normalized is None:
                return UpdateResult(status=REJECTED, reason=error)

            previous_fingerprint = self._seen_observations.get(normalized.identity_key)
            if previous_fingerprint is not None:
                if previous_fingerprint == normalized.fingerprint:
                    return UpdateResult(
                        status=DUPLICATE,
                        ticker=normalized.ticker,
                        source_timestamp=normalized.timestamp,
                        reason="exact_observation_replay",
                    )
                return UpdateResult(
                    status=CONFLICTING_DUPLICATE,
                    ticker=normalized.ticker,
                    source_timestamp=normalized.timestamp,
                    reason="source_observation_id_reused_with_different_payload",
                )

            buckets: dict[str, _Bucket] = {}
            bucket_errors: dict[str, str] = {}
            for timeframe in normalized.targets:
                bucket, bucket_error = self._bucket_for(
                    normalized.ticker, timeframe, normalized.timestamp
                )
                if bucket is None:
                    bucket_errors[timeframe] = bucket_error or "bucket_unavailable"
                else:
                    buckets[timeframe] = bucket

            if bucket_errors:
                # A source observation outside its official session cannot
                # mutate any timeframe, including timeframes with a valid
                # bucket in a custom calendar.
                return UpdateResult(
                    status=REJECTED,
                    ticker=normalized.ticker,
                    source_timestamp=normalized.timestamp,
                    reason=next(iter(bucket_errors.values())),
                    timeframes=tuple(
                        TimeframeUpdate(tf, REJECTED, reason=bucket_errors.get(tf))
                        for tf in normalized.targets
                    ),
                )

            updates: list[TimeframeUpdate] = []
            accepted_any = False
            rejected_any = False
            for timeframe in normalized.targets:
                bucket = buckets[timeframe]
                state_key = (normalized.ticker, timeframe)
                active = self._forming.get(state_key)
                completed_bar = self._completed.get((normalized.ticker, timeframe, bucket.bar_id))
                if completed_bar is not None:
                    rejected_any = True
                    updates.append(
                        TimeframeUpdate(
                            timeframe,
                            LATE_COMPLETED_BUCKET,
                            bar_id=completed_bar.bar_id,
                            reason="completed_bar_is_immutable",
                        )
                    )
                    continue

                latest_completed_start = self._latest_completed_start(
                    normalized.ticker, timeframe
                )
                if active is None:
                    if latest_completed_start is not None and bucket.bucket_start <= latest_completed_start:
                        rejected_any = True
                        updates.append(
                            TimeframeUpdate(
                                timeframe,
                                LATE_COMPLETED_BUCKET,
                                reason="source_bucket_precedes_frozen_history",
                            )
                        )
                        continue
                    self._forming[state_key] = _MutableBar.from_observation(bucket, normalized)
                    accepted_any = True
                    updates.append(TimeframeUpdate(timeframe, ACCEPTED, bar_id=bucket.bar_id))
                    continue

                if bucket.bucket_start < active.bucket.bucket_start:
                    rejected_any = True
                    updates.append(
                        TimeframeUpdate(
                            timeframe,
                            LATE_COMPLETED_BUCKET,
                            bar_id=active.bucket.bar_id,
                            reason="source_bucket_precedes_current_state",
                        )
                    )
                    continue

                if bucket.bar_id == active.bucket.bar_id:
                    if normalized.timestamp < active.last_source_timestamp:
                        rejected_any = True
                        updates.append(
                            TimeframeUpdate(
                                timeframe,
                                OUT_OF_ORDER_FORMING,
                                bar_id=active.bucket.bar_id,
                                reason="backward_source_timestamp_rejected",
                            )
                        )
                        continue
                    active.apply(normalized)
                    accepted_any = True
                    updates.append(TimeframeUpdate(timeframe, ACCEPTED, bar_id=active.bucket.bar_id))
                    continue

                # The source timestamp belongs to a later bucket.  The prior
                # forming bar has observations, so it is safe to freeze it;
                # empty skipped buckets are never fabricated.
                finalized = self._finalize_active(state_key)
                self._forming[state_key] = _MutableBar.from_observation(bucket, normalized)
                accepted_any = True
                updates.append(
                    TimeframeUpdate(
                        timeframe,
                        ACCEPTED,
                        bar_id=bucket.bar_id,
                        finalized_bar_id=finalized.bar_id if finalized is not None else None,
                    )
                )

            if accepted_any:
                self._seen_observations[normalized.identity_key] = normalized.fingerprint
                for item in updates:
                    if item.status == ACCEPTED:
                        state_key = (normalized.ticker, item.timeframe)
                        self._seen_by_state.setdefault(state_key, set()).add(
                            normalized.identity_key
                        )

            if accepted_any and rejected_any:
                result_status = "PARTIAL"
            elif accepted_any:
                result_status = ACCEPTED
            elif updates and all(item.status == LATE_COMPLETED_BUCKET for item in updates):
                result_status = LATE_COMPLETED_BUCKET
            elif updates and all(item.status == OUT_OF_ORDER_FORMING for item in updates):
                result_status = OUT_OF_ORDER_FORMING
            else:
                result_status = REJECTED
            return UpdateResult(
                status=result_status,
                ticker=normalized.ticker,
                source_timestamp=normalized.timestamp,
                accepted=accepted_any,
                reason=("one_or_more_timeframes_rejected" if accepted_any and rejected_any else None),
                timeframes=tuple(updates),
            )

    def _latest_completed_start(self, ticker: str, timeframe: str) -> Optional[datetime]:
        return self._latest_completed.get((ticker, timeframe))

    def _finalize_active(self, state_key: tuple[str, str]) -> Optional[Bar]:
        active = self._forming.pop(state_key, None)
        if active is None:
            return None
        completed = active.to_bar(status=COMPLETED)
        key = (completed.ticker, completed.timeframe, completed.bar_id)
        # The active map is authoritative.  This guard makes repeated boundary
        # calls idempotent even if a caller restores a previously consistent
        # snapshot and then advances it again.
        self._completed.setdefault(key, completed)
        state_key = (completed.ticker, completed.timeframe)
        prior = self._latest_completed.get(state_key)
        if prior is None or completed.bucket_start > prior:
            self._latest_completed[state_key] = completed.bucket_start
        self._remove_seen_for_state(state_key)
        return self._completed[key]

    def _remove_seen_for_state(self, state_key: tuple[str, str]) -> None:
        """Keep duplicate identities only while their accepted bar is forming."""
        identities = self._seen_by_state.pop(state_key, set())
        if not identities:
            return
        still_active = set().union(*self._seen_by_state.values()) if self._seen_by_state else set()
        for identity_key in identities - still_active:
            self._seen_observations.pop(identity_key, None)

    def advance(self, at: Any, *, ticker: Optional[str] = None) -> AdvanceResult:
        """Finalize observed forming bars whose bucket end is at or before ``at``.

        ``at`` is explicit source/exchange-time evidence.  This method never
        reads the process clock and never creates an empty next bucket.
        """
        timestamp = _parse_timestamp(at)
        if timestamp is None:
            return AdvanceResult(REJECTED, reason="timezone_aware_advance_timestamp_required")
        normalized_ticker: Optional[str] = None
        if ticker is not None:
            normalized_ticker = str(ticker).strip().upper()
            if normalized_ticker not in self.universe:
                return AdvanceResult(REJECTED, reason="ticker_not_in_bounded_universe")

        with self._lock:
            finalized: list[Bar] = []
            for state_key in sorted(self._forming):
                state_ticker, _ = state_key
                if normalized_ticker is not None and state_ticker != normalized_ticker:
                    continue
                active = self._forming.get(state_key)
                if active is None or active.bucket.bucket_end > timestamp:
                    continue
                frozen = self._finalize_active(state_key)
                if frozen is not None:
                    finalized.append(frozen)
            if not finalized:
                return AdvanceResult(NOOP)
            finalized.sort(key=lambda bar: (bar.ticker, bar.timeframe, bar.bucket_start))
            return AdvanceResult(ACCEPTED, finalized=tuple(finalized))

    # ------------------------------------------------------------------
    # One-shot seed and explicit completed-bar loading
    # ------------------------------------------------------------------
    def seed_once(
        self,
        seed_source: Iterable[Mapping[str, Any]] | Callable[[], Iterable[Mapping[str, Any]]],
        *,
        finalize_through: Optional[Any] = None,
    ) -> SeedResult:
        """Consume one supplied seed operation; never acquire history itself.

        A callable is invoked at most once for this state instance.  A caller
        that needs a retry after a failed source must create a new state
        instance or supply a new explicit reconstruction, rather than causing
        an incremental update loop to refetch history.
        """
        cutoff: Optional[datetime] = None
        if finalize_through is not None:
            cutoff = _parse_timestamp(finalize_through)
            if cutoff is None:
                return SeedResult(REJECTED, reason="timezone_aware_finalize_through_required")

        with self._lock:
            if self._seed_attempted:
                return SeedResult(NOOP, reason="seed_already_attempted")
            self._seed_attempted = True
            try:
                rows = seed_source() if callable(seed_source) else seed_source
                if isinstance(rows, Mapping):
                    rows = [rows]
                if rows is None:
                    raise TypeError("seed source returned None")
                rows_list = list(rows)
            except Exception as exc:
                return SeedResult(REJECTED, reason=f"seed_source_error:{type(exc).__name__}")

            results: list[UpdateResult] = []
            accepted_rows = 0
            rejected_rows = 0
            for row in rows_list:
                if self._looks_like_completed_bar(row):
                    loaded = self._load_completed_bar(row)
                    results.append(loaded)
                    if loaded.status == ACCEPTED:
                        accepted_rows += 1
                    else:
                        rejected_rows += 1
                    continue
                result = self.ingest(row)
                results.append(result)
                if result.accepted:
                    accepted_rows += 1
                else:
                    rejected_rows += 1

            if cutoff is not None:
                self.advance(cutoff)
            self._seeded = True
            status = ACCEPTED if rejected_rows == 0 else "PARTIAL"
            return SeedResult(
                status=status,
                rows_seen=len(rows_list),
                accepted_rows=accepted_rows,
                rejected_rows=rejected_rows,
                results=tuple(results),
            )

    @staticmethod
    def _looks_like_completed_bar(row: Any) -> bool:
        return (
            isinstance(row, Mapping)
            and str(row.get("status") or "").upper() == COMPLETED
            and "bucket_start" in row
            and "bucket_end" in row
        )

    def _bar_from_mapping(self, row: Mapping[str, Any], *, expected_status: str) -> tuple[Optional[Bar], Optional[str]]:
        ticker = str(row.get("ticker") or row.get("symbol") or "").strip().upper()
        if ticker not in self.universe:
            return None, "ticker_not_in_bounded_universe"
        timeframe = str(row.get("timeframe") or "").strip()
        if timeframe not in self.timeframes:
            return None, "unsupported_timeframe"
        if str(row.get("status") or "").upper() != expected_status:
            return None, "bar_status_mismatch"
        try:
            session_date = date.fromisoformat(str(row.get("session_date") or ""))
        except ValueError:
            return None, "session_date_malformed"
        bucket_start = _parse_timestamp(row.get("bucket_start"))
        bucket_end = _parse_timestamp(row.get("bucket_end"))
        first_ts = _parse_timestamp(row.get("first_source_timestamp"))
        last_ts = _parse_timestamp(row.get("last_source_timestamp"))
        if bucket_start is None or bucket_end is None or first_ts is None or last_ts is None:
            return None, "bar_timestamps_malformed_or_naive"
        values = tuple(
            _finite_number(row.get(key), positive=True)
            for key in ("open", "high", "low", "close")
        )
        if any(value is None for value in values):
            return None, "bar_ohlc_malformed_or_nonfinite"
        opened, high, low, closed = values
        assert opened is not None and high is not None and low is not None and closed is not None
        if high < low or not (low <= opened <= high) or not (low <= closed <= high):
            return None, "impossible_ohlc"
        volume = row.get("volume")
        if volume is not None:
            volume = _finite_number(volume, nonnegative=True)
            if volume is None:
                return None, "bar_volume_malformed_or_negative"
        source_identity = _metadata_string(row.get("source_identity"), "")
        source_version = _metadata_string(row.get("source_version"), "")
        if source_identity is None or source_version is None:
            return None, "bar_source_provenance_missing"
        if first_ts > last_ts or first_ts < bucket_start or last_ts >= bucket_end:
            return None, "bar_source_timestamp_outside_bucket"
        bucket, bucket_error = self._bucket_for(ticker, timeframe, first_ts)
        if bucket is None:
            return None, bucket_error or "bucket_unavailable"
        if (
            bucket.session_date != session_date
            or bucket.bucket_start != bucket_start
            or bucket.bucket_end != bucket_end
        ):
            return None, "bar_bucket_not_session_anchored"
        return (
            Bar(
                ticker=ticker,
                timeframe=timeframe,
                session_date=session_date,
                bucket_start=bucket_start,
                bucket_end=bucket_end,
                status=expected_status,
                open=opened,
                high=high,
                low=low,
                close=closed,
                volume=volume,
                first_source_timestamp=first_ts,
                last_source_timestamp=last_ts,
                source_identity=source_identity,
                source_version=source_version,
            ),
            None,
        )

    def _load_completed_bar(self, row: Mapping[str, Any]) -> UpdateResult:
        bar, error = self._bar_from_mapping(row, expected_status=COMPLETED)
        if bar is None:
            return UpdateResult(REJECTED, reason=error)
        key = (bar.ticker, bar.timeframe, bar.bar_id)
        existing = self._completed.get(key)
        if existing is not None:
            if existing.to_dict() == bar.to_dict():
                return UpdateResult(DUPLICATE, ticker=bar.ticker, reason="exact_completed_bar_replay")
            return UpdateResult(CONFLICTING_DUPLICATE, ticker=bar.ticker, reason="completed_bar_identity_conflict")
        active = self._forming.get((bar.ticker, bar.timeframe))
        if active is not None and bar.bucket_start <= active.bucket.bucket_start:
            return UpdateResult(REJECTED, ticker=bar.ticker, reason="completed_bar_precedes_current_forming_state")
        latest = self._latest_completed_start(bar.ticker, bar.timeframe)
        if latest is not None and bar.bucket_start <= latest:
            return UpdateResult(REJECTED, ticker=bar.ticker, reason="completed_bar_not_after_frozen_history")
        self._completed[key] = bar
        self._latest_completed[(bar.ticker, bar.timeframe)] = bar.bucket_start
        return UpdateResult(ACCEPTED, ticker=bar.ticker, accepted=True, reason="completed_bar_seeded")

    # ------------------------------------------------------------------
    # Read-only state and restart reconstruction
    # ------------------------------------------------------------------
    def forming_bar(self, ticker: str, timeframe: str) -> Optional[Bar]:
        ticker = str(ticker).strip().upper()
        with self._lock:
            active = self._forming.get((ticker, timeframe))
            return active.to_bar(status=FORMING) if active is not None else None

    def completed_bars(self, ticker: str, timeframe: str) -> tuple[Bar, ...]:
        ticker = str(ticker).strip().upper()
        with self._lock:
            bars = [
                bar
                for (bar_ticker, bar_timeframe, _), bar in self._completed.items()
                if bar_ticker == ticker and bar_timeframe == timeframe
            ]
            return tuple(sorted(bars, key=lambda bar: bar.bucket_start))

    def all_bars(self, ticker: str, timeframe: str) -> tuple[Bar, ...]:
        completed = list(self.completed_bars(ticker, timeframe))
        forming = self.forming_bar(ticker, timeframe)
        if forming is not None:
            completed.append(forming)
        return tuple(sorted(completed, key=lambda bar: bar.bucket_start))

    def snapshot(self) -> dict[str, Any]:
        """Return a deterministic in-memory reconstruction payload."""
        with self._lock:
            forming = [
                self._forming[key].to_snapshot_dict()
                for key in sorted(self._forming)
            ]
            completed = [
                bar.to_dict()
                for bar in sorted(self._completed.values(), key=lambda item: item.bar_id)
            ]
            return {
                "state_schema_version": STATE_SCHEMA_VERSION,
                "universe": sorted(self.universe),
                "timeframes": list(self.timeframes),
                "seed_attempted": self._seed_attempted,
                "seeded": self._seeded,
                "forming": forming,
                "completed": completed,
                "seen_observations": [
                    {
                        "identity_key": key,
                        "fingerprint": self._seen_observations[key],
                        "states": [
                            [state_ticker, state_timeframe]
                            for (state_ticker, state_timeframe), identities in sorted(
                                self._seen_by_state.items()
                            )
                            if key in identities
                        ],
                    }
                    for key in sorted(self._seen_observations)
                ],
            }

    @classmethod
    def from_snapshot(
        cls,
        snapshot: Mapping[str, Any],
        *,
        calendar: Optional[SessionCalendar | Callable[[date], Optional[TradingSession]]] = None,
    ) -> "IntradayBarState":
        if not isinstance(snapshot, Mapping):
            raise ValueError("snapshot_mapping_required")
        if snapshot.get("state_schema_version") != STATE_SCHEMA_VERSION:
            raise ValueError("snapshot_schema_version_mismatch")
        if tuple(snapshot.get("timeframes") or ()) != SUPPORTED_TIMEFRAMES:
            raise ValueError("snapshot_timeframes_mismatch")
        if tuple(sorted(snapshot.get("universe") or ())) != tuple(sorted(BOUNDED_UNIVERSE)):
            raise ValueError("snapshot_universe_mismatch")

        state = cls(calendar=calendar)
        with state._lock:
            state._seed_attempted = bool(snapshot.get("seed_attempted"))
            state._seeded = bool(snapshot.get("seeded"))
            seen_rows = snapshot.get("seen_observations") or []
            if not isinstance(seen_rows, Iterable) or isinstance(seen_rows, (str, bytes, Mapping)):
                raise ValueError("snapshot_seen_observations_malformed")
            for item in seen_rows:
                if not isinstance(item, Mapping):
                    raise ValueError("snapshot_seen_observation_malformed")
                key = str(item.get("identity_key") or "")
                fingerprint = str(item.get("fingerprint") or "")
                if not key or not fingerprint:
                    raise ValueError("snapshot_seen_observation_missing_identity")
                state._seen_observations[key] = fingerprint
                raw_states = item.get("states") or []
                if not isinstance(raw_states, Iterable) or isinstance(raw_states, (str, bytes, Mapping)):
                    raise ValueError("snapshot_seen_observation_states_malformed")
                for raw_state in raw_states:
                    if (
                        not isinstance(raw_state, Iterable)
                        or isinstance(raw_state, (str, bytes, Mapping))
                    ):
                        raise ValueError("snapshot_seen_observation_state_malformed")
                    state_values = list(raw_state)
                    if len(state_values) != 2:
                        raise ValueError("snapshot_seen_observation_state_malformed")
                    state_key = (str(state_values[0]), str(state_values[1]))
                    if state_key[0] not in state.universe or state_key[1] not in state.timeframes:
                        raise ValueError("snapshot_seen_observation_state_out_of_scope")
                    state._seen_by_state.setdefault(state_key, set()).add(key)

            completed_rows = snapshot.get("completed") or []
            if not isinstance(completed_rows, Iterable) or isinstance(completed_rows, (str, bytes, Mapping)):
                raise ValueError("snapshot_completed_malformed")
            for row in completed_rows:
                if not isinstance(row, Mapping):
                    raise ValueError("snapshot_completed_row_malformed")
                bar, error = state._bar_from_mapping(row, expected_status=COMPLETED)
                if bar is None:
                    raise ValueError(error or "snapshot_completed_row_invalid")
                key = (bar.ticker, bar.timeframe, bar.bar_id)
                if key in state._completed:
                    raise ValueError("snapshot_duplicate_completed_bar")
                state._completed[key] = bar
                state_key = (bar.ticker, bar.timeframe)
                prior = state._latest_completed.get(state_key)
                if prior is None or bar.bucket_start > prior:
                    state._latest_completed[state_key] = bar.bucket_start

            forming_rows = snapshot.get("forming") or []
            if not isinstance(forming_rows, Iterable) or isinstance(forming_rows, (str, bytes, Mapping)):
                raise ValueError("snapshot_forming_malformed")
            for row in forming_rows:
                if not isinstance(row, Mapping):
                    raise ValueError("snapshot_forming_row_malformed")
                bar, error = state._bar_from_mapping(
                    {**dict(row), "status": FORMING}, expected_status=FORMING
                )
                if bar is None:
                    raise ValueError(error or "snapshot_forming_row_invalid")
                key = (bar.ticker, bar.timeframe)
                if key in state._forming:
                    raise ValueError("snapshot_duplicate_forming_bar")
                source_ids = row.get("_source_identities")
                source_versions = row.get("_source_versions")
                ids = (
                    {str(value) for value in source_ids}
                    if isinstance(source_ids, Iterable) and not isinstance(source_ids, (str, bytes, Mapping))
                    else {bar.source_identity}
                )
                versions = (
                    {str(value) for value in source_versions}
                    if isinstance(source_versions, Iterable) and not isinstance(source_versions, (str, bytes, Mapping))
                    else {bar.source_version}
                )
                if not ids or not versions:
                    raise ValueError("snapshot_forming_provenance_malformed")
                try:
                    accepted_count = int(row.get("_accepted_observation_count") or 0)
                except (TypeError, ValueError):
                    raise ValueError("snapshot_forming_count_malformed")
                if accepted_count < 1:
                    raise ValueError("snapshot_forming_count_malformed")
                state._forming[key] = _MutableBar(
                    bucket=_Bucket(
                        ticker=bar.ticker,
                        timeframe=bar.timeframe,
                        session_date=bar.session_date,
                        bucket_start=bar.bucket_start,
                        bucket_end=bar.bucket_end,
                    ),
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=bar.volume,
                    first_source_timestamp=bar.first_source_timestamp,
                    last_source_timestamp=bar.last_source_timestamp,
                    source_identities=ids,
                    source_versions=versions,
                    accepted_observation_count=accepted_count,
                )
        return state


__all__ = [
    "ACCEPTED",
    "AdvanceResult",
    "Bar",
    "BOUNDED_UNIVERSE",
    "COMPLETED",
    "CONFLICTING_DUPLICATE",
    "DUPLICATE",
    "ET",
    "FORMING",
    "IntradayBarState",
    "LATE_COMPLETED_BUCKET",
    "MarketObservation",
    "NOOP",
    "NYSESessionCalendar",
    "NYSE_EARLY_CLOSES_ET",
    "OUT_OF_ORDER_FORMING",
    "REJECTED",
    "SeedResult",
    "SUPPORTED_TIMEFRAMES",
    "STATE_SCHEMA_VERSION",
    "SessionCalendar",
    "TimeframeUpdate",
    "TradingSession",
    "UpdateResult",
]
