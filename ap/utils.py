import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

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

def now_et():
    return datetime.now(ET)

def json_dumps(obj) -> str:
    return json.dumps(obj, separators=(",", ":"), default=str)

def json_loads(s: str):
    return json.loads(s)

def pct(x: float) -> float:
    return round(x * 100.0, 2)
