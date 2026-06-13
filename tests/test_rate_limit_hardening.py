"""
tests/test_rate_limit_hardening.py

Rate limit hardening — confirms:
  1. Default RATE_LIMIT_PER_MIN raised to 240
  2. 429 response includes Retry-After header
  3. 429 response includes retry_after_sec field
  4. RATE_LIMIT_RETRY_AFTER_SEC env var is honored
  5. The 429 path emits a structured log

Pure module-level tests — no Flask boot.
"""
from __future__ import annotations

import os
import re
from pathlib import Path

import pytest


APP_PY = Path(__file__).resolve().parents[1] / "app.py"


def test_rate_limit_default_is_240():
    src = APP_PY.read_text()
    # Default literal "240" appears in the os.getenv call for RATE_LIMIT_PER_MIN
    pattern = r'RATE_LIMIT_PER_MIN\s*=\s*int\(os\.getenv\(\s*"RATE_LIMIT_PER_MIN"\s*,\s*"240"\s*\)\)'
    assert re.search(pattern, src), \
        "RATE_LIMIT_PER_MIN default must be '240' (was 60 — too tight for scanner burst)"


def test_retry_after_env_var_exists():
    src = APP_PY.read_text()
    pattern = r'RATE_LIMIT_RETRY_AFTER_SEC\s*=\s*float\(os\.getenv\('
    assert re.search(pattern, src), \
        "RATE_LIMIT_RETRY_AFTER_SEC env var must be defined for Retry-After header"


def test_signal_429_returns_retry_after_header():
    src = APP_PY.read_text()
    # The /signal 429 path must set Retry-After header
    # Look for the SIGNAL_RATE_LIMITED block
    idx = src.find("SIGNAL_RATE_LIMITED")
    assert idx > 0, "SIGNAL_RATE_LIMITED log marker missing — observability gap"
    # Within ~30 lines after the marker, Retry-After header must be set
    block = src[idx:idx + 2000]
    assert 'resp.headers["Retry-After"]' in block, \
        "/signal 429 path must set Retry-After header"
    assert 'return resp, 429' in block, \
        "/signal 429 path must return tuple (resp, 429)"


def test_scanner_429_returns_retry_after_header():
    src = APP_PY.read_text()
    idx = src.find("SCANNER_RATE_LIMITED")
    assert idx > 0, "SCANNER_RATE_LIMITED log marker missing"
    block = src[idx:idx + 2000]
    assert 'resp.headers["Retry-After"]' in block, \
        "/scanner/discord 429 path must set Retry-After header"


def test_429_response_includes_retry_after_sec_field():
    """JSON body should include retry_after_sec so scanners using JSON
    can extract the value programmatically."""
    src = APP_PY.read_text()
    assert '"retry_after_sec": RATE_LIMIT_RETRY_AFTER_SEC' in src, \
        "429 JSON body must include retry_after_sec field"


def test_429_logs_use_warning_level():
    src = APP_PY.read_text()
    # Both rate-limit logs must be log.warning, not log.debug
    for marker in ("SIGNAL_RATE_LIMITED", "SCANNER_RATE_LIMITED"):
        idx = src.find(marker)
        assert idx > 0
        # Look backward up to 200 chars for log.warning
        backctx = src[max(0, idx - 200):idx]
        assert "log.warning(" in backctx, \
            f"{marker} must be logged at warning level (was debug or missing)"


def test_no_silent_429_remains():
    """Confirm no 429 return path silently drops the request without logging."""
    src = APP_PY.read_text()
    # All occurrences of return ..., 429 must have a log call nearby
    for match in re.finditer(r'return\s+\w+,\s*429', src):
        idx = match.start()
        # Look backward 600 chars for log.warning/error/info
        ctx = src[max(0, idx - 600):idx]
        has_log = any(
            x in ctx
            for x in ("log.warning(", "log.error(", "log.info(", "logger.warning(")
        )
        assert has_log, (
            f"429 return at offset {idx} has no log call in preceding 600 chars — "
            f"silent 429s are an observability gap"
        )
