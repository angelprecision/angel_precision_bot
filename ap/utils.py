import json
from datetime import datetime, timezone
from typing import Any, Mapping
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

BROKER_FILL_TIMESTAMP_SOURCE = "broker_response"
BROKER_FILL_TIMESTAMP_SOURCE_KEY = "exit_fill_timestamp_source"

# These are adapter-normalized execution-time fields only.  Provider lifecycle
# timestamps such as Tradier's ``transaction_date`` are deliberately excluded:
# an order update is not an execution event and must never authorize EXIT P&L
# or proof mutation.
BROKER_FILL_TIMESTAMP_KEYS = (
    "broker_fill_timestamp",
    "broker_execution_timestamp",
    "filled_ts",
    "filled_at",
    "fill_ts",
    "last_fill_date",
)

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_aware_utc_timestamp(value):
    """Parse broker/durable truth without assigning a timezone to naive data.

    Numeric epochs are unambiguous UTC instants and remain supported. ISO
    strings and datetime values must carry an explicit timezone; malformed or
    naive values return ``None`` so callers can fail closed before mutation.
    """
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return None
    try:
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, (int, float)):
            epoch = float(value)
            if epoch > 10_000_000_000:
                epoch /= 1000.0
            parsed = datetime.fromtimestamp(epoch, tz=timezone.utc)
        else:
            text = str(value).strip()
            if not text:
                return None
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def extract_broker_fill_timestamp_with_source(
    payload: Mapping[str, Any] | None,
) -> tuple[datetime | None, str | None]:
    """Resolve one unambiguous, adapter-normalized broker fill timestamp.

    Every supplied candidate is validated.  A malformed candidate or two
    different candidate instants are ambiguous external authority and return
    ``(None, None)`` so callers hold before any lifecycle mutation.
    ``transaction_date`` is intentionally not a candidate; it is an order
    lifecycle/update timestamp for Tradier, not exact execution time.
    """
    if not isinstance(payload, Mapping):
        return None, None

    candidates: list[datetime] = []
    for key in BROKER_FILL_TIMESTAMP_KEYS:
        if key not in payload:
            continue
        parsed = parse_aware_utc_timestamp(payload.get(key))
        if parsed is None:
            return None, None
        candidates.append(parsed)

    if not candidates or any(value != candidates[0] for value in candidates[1:]):
        return None, None
    return candidates[0], BROKER_FILL_TIMESTAMP_SOURCE


def broker_fill_timestamp_source(meta):
    """Return the durable EXIT fill timestamp producer token, if present."""
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except (TypeError, ValueError):
            return None
    if not isinstance(meta, dict):
        return None
    source = meta.get(BROKER_FILL_TIMESTAMP_SOURCE_KEY)
    return str(source).strip() if isinstance(source, str) else None


def has_broker_fill_timestamp_provenance(meta) -> bool:
    return broker_fill_timestamp_source(meta) == BROKER_FILL_TIMESTAMP_SOURCE

def now_et():
    return datetime.now(ET)

def json_dumps(obj) -> str:
    return json.dumps(obj, separators=(",", ":"), default=str)

def json_loads(s: str):
    return json.loads(s)

def pct(x: float) -> float:
    return round(x * 100.0, 2)
