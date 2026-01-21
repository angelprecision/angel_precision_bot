import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def now_et():
    return datetime.now(ET)

def json_dumps(obj) -> str:
    return json.dumps(obj, separators=(",", ":"), default=str)

def json_loads(s: str):
    return json.loads(s)

def pct(x: float) -> float:
    return round(x * 100.0, 2)

