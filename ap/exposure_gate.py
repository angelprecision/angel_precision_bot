"""
ap/exposure_gate.py — PR #30 live-safety hardening

Same-symbol and same-sector exposure caps applied at entry submit time.

Rules
-----
- Before submit, count open positions for this client.
- If open same-underlying count >= MAX_OPEN_PER_SYMBOL, BLOCK with
  SYMBOL_EXPOSURE_LIMIT.
- If sector mapping is available AND open same-sector count >=
  MAX_OPEN_PER_SECTOR, BLOCK with SECTOR_EXPOSURE_LIMIT.
- If sector mapping is unavailable for the candidate symbol, enforce only
  the same-symbol cap and log sector_unknown.
- Pure entry gate. Does not touch scoring, scanner, exits, force exits,
  reconciler, or positions table.

Env vars
--------
  MAX_OPEN_PER_SYMBOL=1
  MAX_OPEN_PER_SECTOR=2

Public API
----------
  check_exposure(client_id, symbol, *, conn_factory=None) -> CheckResult

  CheckResult is a dict with:
    ok                  : bool — True = entry allowed, False = blocked
    error               : str or None — 'symbol_exposure_limit' /
                          'sector_exposure_limit'
    reason_code         : str or None — 'SYMBOL_EXPOSURE_LIMIT' /
                          'SECTOR_EXPOSURE_LIMIT'
    open_same_symbol    : int
    open_same_sector    : int or None  (None when sector unknown)
    symbol              : str
    sector              : str or None
    max_open_per_symbol : int
    max_open_per_sector : int

Sector mapping
--------------
We use a small, in-repo static map covering the universe the bot trades.
If the candidate symbol is NOT in the map we set sector=None and log
sector_unknown, and we DO NOT block on sector. Same-symbol still enforces.

Operators can override per-client by setting MAX_OPEN_PER_SYMBOL_<CID>
in the future; for now the env defaults apply uniformly.
"""

from __future__ import annotations

import os
from typing import Optional


# ----------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------

MAX_OPEN_PER_SYMBOL = int(os.getenv("MAX_OPEN_PER_SYMBOL", "1"))
MAX_OPEN_PER_SECTOR = int(os.getenv("MAX_OPEN_PER_SECTOR", "2"))


# ----------------------------------------------------------------------
# Sector mapping (static, repo-owned)
# ----------------------------------------------------------------------
# Coverage focuses on the universe the scanners actually trade. Adding a
# new ticker = add a single dict entry below. Unknown tickers default to
# sector=None and the gate only applies symbol cap, not sector cap.
SECTOR_MAP: dict[str, str] = {
    # Mega-cap tech
    "AAPL":  "TECH",      "MSFT":  "TECH",      "GOOGL": "TECH",
    "GOOG":  "TECH",      "AMZN":  "TECH",      "META":  "TECH",
    "NVDA":  "TECH",      "AMD":   "TECH",      "AVGO":  "TECH",
    "ORCL":  "TECH",      "ADBE":  "TECH",      "CRM":   "TECH",
    "INTC":  "TECH",      "QCOM":  "TECH",      "TSM":   "TECH",
    "MU":    "TECH",      "ARM":   "TECH",      "PLTR":  "TECH",
    "SMCI":  "TECH",      "PANW":  "TECH",      "NOW":   "TECH",

    # Autos / EVs
    "TSLA":  "AUTO",      "RIVN":  "AUTO",      "LCID":  "AUTO",
    "F":     "AUTO",      "GM":    "AUTO",

    # Financials
    "JPM":   "FINANCIAL", "BAC":   "FINANCIAL", "WFC":   "FINANCIAL",
    "GS":    "FINANCIAL", "MS":    "FINANCIAL", "C":     "FINANCIAL",
    "V":     "FINANCIAL", "MA":    "FINANCIAL", "SCHW":  "FINANCIAL",
    "AXP":   "FINANCIAL",

    # Healthcare
    "UNH":   "HEALTHCARE", "JNJ":  "HEALTHCARE", "PFE":  "HEALTHCARE",
    "LLY":   "HEALTHCARE", "MRK":  "HEALTHCARE", "ABBV": "HEALTHCARE",
    "TMO":   "HEALTHCARE",

    # Energy
    "XOM":   "ENERGY",    "CVX":   "ENERGY",    "COP":   "ENERGY",
    "SLB":   "ENERGY",    "OXY":   "ENERGY",

    # Consumer
    "WMT":   "CONSUMER",  "TGT":   "CONSUMER",  "COST":  "CONSUMER",
    "HD":    "CONSUMER",  "LOW":   "CONSUMER",  "MCD":   "CONSUMER",
    "SBUX":  "CONSUMER",  "NKE":   "CONSUMER",  "LULU":  "CONSUMER",
    "ULTA":  "CONSUMER",  "UBER":  "CONSUMER",  "ABNB":  "CONSUMER",
    "DIS":   "CONSUMER",  "NFLX":  "CONSUMER",

    # Industrials / defense / aero
    "BA":    "INDUSTRIAL", "CAT":  "INDUSTRIAL", "DE":   "INDUSTRIAL",
    "GE":    "INDUSTRIAL", "RTX":  "INDUSTRIAL", "LMT":  "INDUSTRIAL",

    # Index / broad-market ETFs (separate sector so they don't cluster
    # with TECH or FINANCIAL)
    "SPY":   "INDEX",     "QQQ":   "INDEX",     "DIA":   "INDEX",
    "IWM":   "INDEX",
}


def get_sector(symbol: str) -> Optional[str]:
    """Return the sector for a symbol, or None if unmapped."""
    if not symbol:
        return None
    return SECTOR_MAP.get(symbol.strip().upper())


# ----------------------------------------------------------------------
# Position counting
# ----------------------------------------------------------------------

def _count_open_positions(client_id: str, conn_factory=None) -> list[dict]:
    """Return a list of {symbol, underlying} dicts for this client's
    OPEN/CLOSING positions. conn_factory is overridable for tests.
    """
    if conn_factory is None:
        # Import here so this module stays test-friendly without a DB.
        from ap.db import conn as _conn  # type: ignore
        conn_factory = _conn

    out: list[dict] = []
    with conn_factory() as c:
        # 'underlying' is the canonical stock ticker on positions; some
        # legacy rows may only have 'symbol'. We surface both and prefer
        # underlying when present.
        c.execute(
            """
            SELECT underlying, symbol
            FROM positions
            WHERE client_id = %s
              AND status IN ('OPEN', 'CLOSING')
            """,
            (client_id,),
        )
        for row in c.fetchall():
            out.append({
                "underlying": (row.get("underlying") if isinstance(row, dict) else None)
                              or (row[0] if not isinstance(row, dict) else None),
                "symbol":     (row.get("symbol") if isinstance(row, dict) else None)
                              or (row[1] if not isinstance(row, dict) else None),
            })
    return out


# ----------------------------------------------------------------------
# Public gate
# ----------------------------------------------------------------------

def check_exposure(
    client_id: str,
    symbol: str,
    *,
    conn_factory=None,
    open_positions: Optional[list[dict]] = None,
) -> dict:
    """Apply the exposure caps. Returns a CheckResult dict.

    Args:
      client_id:        the client to check.
      symbol:           the candidate underlying ticker (canonical, uppercase).
      conn_factory:     optional override for tests; otherwise uses ap.db.conn.
      open_positions:   optional pre-loaded list (e.g. from a snapshot the
                        caller already has). When None we query the DB.
    """
    sym = (symbol or "").strip().upper()
    sector = get_sector(sym)

    base = {
        "ok":                  True,
        "error":               None,
        "reason_code":         None,
        "open_same_symbol":    0,
        "open_same_sector":    None if sector is None else 0,
        "symbol":              sym,
        "sector":              sector,
        "max_open_per_symbol": MAX_OPEN_PER_SYMBOL,
        "max_open_per_sector": MAX_OPEN_PER_SECTOR,
    }

    if not sym:
        # Empty symbol: cannot apply caps, fail safe (allow) so we don't
        # accidentally block on a bad input — process_signal will reject
        # missing_symbol upstream anyway.
        return base

    # Pull open positions for this client.
    if open_positions is None:
        try:
            open_positions = _count_open_positions(client_id, conn_factory=conn_factory)
        except Exception:
            # Best-effort: if the DB read fails we DO NOT block. process_signal
            # has its own DB error handling further down.
            open_positions = []

    # Same-symbol count.
    same_symbol = 0
    same_sector = 0
    sectors_seen: set[str] = set()
    for p in open_positions or []:
        u = (p.get("underlying") or p.get("symbol") or "").strip().upper()
        if not u:
            continue
        if u == sym:
            same_symbol += 1
        if sector is not None:
            pos_sector = get_sector(u)
            if pos_sector:
                sectors_seen.add(pos_sector)
                if pos_sector == sector:
                    same_sector += 1

    base["open_same_symbol"] = same_symbol
    if sector is not None:
        base["open_same_sector"] = same_sector

    # Apply caps in order: symbol first (most specific), then sector.
    if same_symbol >= MAX_OPEN_PER_SYMBOL:
        return {
            **base,
            "ok":          False,
            "error":       "symbol_exposure_limit",
            "reason_code": "SYMBOL_EXPOSURE_LIMIT",
        }

    if sector is not None and same_sector >= MAX_OPEN_PER_SECTOR:
        return {
            **base,
            "ok":          False,
            "error":       "sector_exposure_limit",
            "reason_code": "SECTOR_EXPOSURE_LIMIT",
        }

    # If sector is None we surface that for telemetry; not a block.
    return base
