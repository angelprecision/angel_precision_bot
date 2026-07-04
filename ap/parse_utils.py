from __future__ import annotations

from typing import Any, Mapping


def safe_float(value: Any, default=None):
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def safe_int(value: Any, default=None):
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def safe_str(value: Any, default=None):
    try:
        if value is None:
            return default
        text = str(value)
        return text if text != "" else default
    except Exception:
        return default


def first_present(mapping: Mapping[str, Any] | None, keys, default=None):
    if not isinstance(mapping, Mapping):
        return default
    for key in keys:
        cur: Any = mapping
        try:
            for part in str(key).split("."):
                if not isinstance(cur, Mapping):
                    cur = None
                    break
                cur = cur.get(part)
            if cur not in (None, ""):
                return cur
        except Exception:
            continue
    return default


def first_float(mapping: Mapping[str, Any] | None, keys, default=None):
    return safe_float(first_present(mapping, keys, default=None), default)
