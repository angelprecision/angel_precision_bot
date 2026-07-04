from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import logging
import math
import os
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, Callable, Iterable

log = logging.getLogger("ap.chain_archiver")

ENABLE_CHAIN_ARCHIVER = os.getenv("ENABLE_CHAIN_ARCHIVER", "false").strip().lower() == "true"
CHAIN_ARCHIVER_MAX_ROW_BYTES = int(os.getenv("CHAIN_ARCHIVER_MAX_ROW_BYTES", "150000"))
CHAIN_ARCHIVER_MAX_CONTRACTS_PER_EXP = int(os.getenv("CHAIN_ARCHIVER_MAX_CONTRACTS_PER_EXP", "240"))
CHAIN_ARCHIVER_RECENT_SIGNAL_DAYS = int(os.getenv("CHAIN_ARCHIVER_RECENT_SIGNAL_DAYS", "7"))

WATCHLIST_ENV_KEYS = (
    "CHAIN_ARCHIVER_SCANNER_WATCHLIST",
    "SCANNER_WATCHLIST",
    "WATCHLIST",
    "TICKER_WATCHLIST",
)


@dataclass(frozen=True)
class ChainArchiveResult:
    ok: bool
    enabled: bool
    snapshot_date: str
    tickers_seen: int
    expirations_seen: int
    rows_inserted: int
    rows_existing: int
    errors: list[dict[str, str]]
    storage_estimate_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "enabled": self.enabled,
            "snapshot_date": self.snapshot_date,
            "tickers_seen": self.tickers_seen,
            "expirations_seen": self.expirations_seen,
            "rows_inserted": self.rows_inserted,
            "rows_existing": self.rows_existing,
            "errors": list(self.errors),
            "storage_estimate_bytes": self.storage_estimate_bytes,
        }


def _clean_ticker(value: Any) -> str:
    ticker = str(value or "").strip().upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,9}", ticker):
        return ""
    return ticker


def _split_tickers(raw: str) -> list[str]:
    out: list[str] = []
    for part in re.split(r"[\s,;|]+", str(raw or "")):
        ticker = _clean_ticker(part)
        if ticker and ticker not in out:
            out.append(ticker)
    return out


def _first_float(*values: Any) -> float | None:
    for value in values:
        if value in (None, ""):
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            return number
    return None


def _contract_type(raw: dict[str, Any]) -> str:
    value = str(
        raw.get("type")
        or raw.get("option_type")
        or raw.get("put_call")
        or raw.get("side")
        or ""
    ).strip().lower()
    if value.startswith("c"):
        return "call"
    if value.startswith("p"):
        return "put"
    symbol = str(raw.get("symbol") or raw.get("option_symbol") or "").upper()
    if re.search(r"\d{6}C\d{5,8}", symbol):
        return "call"
    if re.search(r"\d{6}P\d{5,8}", symbol):
        return "put"
    return value or "unknown"


def _greeks(raw: dict[str, Any]) -> dict[str, Any]:
    greeks = raw.get("greeks")
    return greeks if isinstance(greeks, dict) else {}


def _iv_float(*values: Any) -> float | None:
    iv = _first_float(*values)
    if iv is None:
        return None
    # Tradier-style fields are usually decimals, but some providers emit percent.
    return round(iv / 100.0, 6) if iv > 3 else iv


def compact_contract(raw: dict[str, Any]) -> dict[str, Any]:
    greeks = _greeks(raw)
    bid = _first_float(raw.get("bid"))
    ask = _first_float(raw.get("ask"))
    mid = _first_float(raw.get("mid"), raw.get("mark"))
    if mid is None and bid is not None and ask is not None and bid >= 0 and ask >= 0:
        mid = round((bid + ask) / 2.0, 4)
    return {
        "strike": _first_float(raw.get("strike")),
        "type": _contract_type(raw),
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "iv": _iv_float(
            raw.get("iv"),
            raw.get("implied_volatility"),
            raw.get("smv_vol"),
            greeks.get("mid_iv"),
            greeks.get("smv_vol"),
        ),
        "delta": _first_float(raw.get("delta"), greeks.get("delta")),
        "oi": _first_float(raw.get("oi"), raw.get("open_interest")),
        "vol": _first_float(raw.get("vol"), raw.get("volume")),
    }


def compact_chain(
    chain: Iterable[dict[str, Any]],
    *,
    underlying_price: float | None,
    max_contracts: int = CHAIN_ARCHIVER_MAX_CONTRACTS_PER_EXP,
    max_bytes: int = CHAIN_ARCHIVER_MAX_ROW_BYTES,
) -> list[dict[str, Any]]:
    compacted = [c for c in (compact_contract(row) for row in chain) if c.get("strike")]
    if underlying_price and underlying_price > 0:
        compacted.sort(key=lambda c: (abs(float(c["strike"]) - underlying_price), c["type"], c["strike"]))
    else:
        compacted.sort(key=lambda c: (c["strike"], c["type"]))
    compacted = compacted[: max(1, int(max_contracts))]
    while compacted and len(json.dumps(compacted, separators=(",", ":"), sort_keys=True).encode("utf-8")) > max_bytes:
        compacted = compacted[: max(1, int(len(compacted) * 0.8))]
    return compacted


def atm_iv(compact_chain_rows: list[dict[str, Any]], underlying_price: float | None) -> float | None:
    if not compact_chain_rows or not underlying_price or underlying_price <= 0:
        return None
    rows = [r for r in compact_chain_rows if r.get("iv") and r.get("strike")]
    if not rows:
        return None
    nearest_strike = min(rows, key=lambda r: abs(float(r["strike"]) - underlying_price))["strike"]
    ivs = [float(r["iv"]) for r in rows if float(r["strike"]) == float(nearest_strike) and float(r["iv"]) > 0]
    if not ivs:
        return None
    return round(sum(ivs) / len(ivs), 6)


def expected_move(underlying_price: float | None, iv: float | None, days: int | float) -> float | None:
    if not underlying_price or not iv or underlying_price <= 0 or iv <= 0:
        return None
    days = max(float(days), 1.0)
    return round(float(underlying_price) * float(iv) * math.sqrt(days / 365.0), 4)


def select_archive_expirations(expirations: Iterable[str], *, today: date | None = None) -> list[str]:
    today = today or date.today()
    parsed: list[tuple[date, str]] = []
    for raw in expirations or []:
        try:
            exp = datetime.strptime(str(raw)[:10], "%Y-%m-%d").date()
        except ValueError:
            continue
        if exp >= today:
            parsed.append((exp, exp.isoformat()))
    parsed.sort(key=lambda item: item[0])
    selected = [raw for _, raw in parsed[:2]]
    weekly = next((raw for exp, raw in parsed if exp.weekday() == 4 and raw not in selected), None)
    if weekly:
        selected.append(weekly)
    return selected


def _recent_scanner_tickers(conn_factory: Callable[[], Any]) -> list[str]:
    def _q():
        with conn_factory() as c:
            c.execute(
                """
                SELECT DISTINCT UPPER(ticker) AS ticker
                FROM ap_signals
                WHERE ticker IS NOT NULL
                  AND created_at >= NOW() - (%s * INTERVAL '1 day')
                """,
                (CHAIN_ARCHIVER_RECENT_SIGNAL_DAYS,),
            )
            return c.fetchall()

    from ap.db import run_with_retry
    return [_clean_ticker(row.get("ticker")) for row in (run_with_retry(_q) or [])]


def _open_position_tickers(conn_factory: Callable[[], Any]) -> list[str]:
    sqls = (
        """
        SELECT DISTINCT UPPER(COALESCE(underlying, symbol)) AS ticker
        FROM positions
        WHERE status IN ('OPEN', 'CLOSING')
        """,
        """
        SELECT DISTINCT UPPER(symbol) AS ticker
        FROM positions
        WHERE status IN ('OPEN', 'CLOSING')
        """,
    )
    from ap.db import run_with_retry
    for sql in sqls:
        try:
            def _q(sql=sql):
                with conn_factory() as c:
                    c.execute(sql)
                    return c.fetchall()
            return [_clean_ticker(row.get("ticker")) for row in (run_with_retry(_q) or [])]
        except Exception:
            continue
    return []


def build_ticker_universe(conn_factory: Callable[[], Any] | None = None, *, env: dict[str, str] | None = None) -> list[str]:
    env = env or os.environ
    tickers: list[str] = []
    for key in WATCHLIST_ENV_KEYS:
        for ticker in _split_tickers(env.get(key, "")):
            if ticker not in tickers:
                tickers.append(ticker)
    if conn_factory is None:
        from ap.db import conn as conn_factory
    for source in (_recent_scanner_tickers, _open_position_tickers):
        try:
            for ticker in source(conn_factory):
                if ticker and ticker not in tickers:
                    tickers.append(ticker)
        except Exception as exc:
            log.warning("chain_archiver_ticker_source_failed source=%s error=%s", source.__name__, exc)
    for ticker in _split_tickers(env.get("CHAIN_ARCHIVER_TICKERS_EXTRA", "")):
        if ticker not in tickers:
            tickers.append(ticker)
    return tickers


def _underlying_price(broker: Any, ticker: str) -> float | None:
    try:
        quote = broker.get_quote(ticker) or {}
    except Exception:
        return None
    return _first_float(quote.get("last"), quote.get("mark"), quote.get("mid"), quote.get("ask"), quote.get("bid"))


def _archive_row(
    conn_factory: Callable[[], Any],
    *,
    snapshot_date: date,
    ticker: str,
    expiration: str,
    underlying_price: float | None,
    compacted: list[dict[str, Any]],
    atm_iv_value: float | None,
    source: str,
) -> bool:
    exp_date = datetime.strptime(expiration[:10], "%Y-%m-%d").date()
    dte = max((exp_date - snapshot_date).days, 1)
    payload = json.dumps(compacted, separators=(",", ":"), sort_keys=True)
    em_1d = expected_move(underlying_price, atm_iv_value, 1)
    em_exp = expected_move(underlying_price, atm_iv_value, dte)

    def _q():
        with conn_factory() as c:
            c.execute(
                """
                INSERT INTO option_chain_snapshots (
                    snapshot_date, ticker, expiration, underlying_price, chain,
                    atm_iv, expected_move_1d, expected_move_to_exp, source
                )
                VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s)
                ON CONFLICT (snapshot_date, ticker, expiration) DO NOTHING
                RETURNING id
                """,
                (
                    snapshot_date,
                    ticker,
                    exp_date,
                    underlying_price,
                    payload,
                    atm_iv_value,
                    em_1d,
                    em_exp,
                    source,
                ),
            )
            return c.fetchone()

    from ap.db import run_with_retry
    return bool(run_with_retry(_q))


def archive_daily_chains(
    *,
    broker: Any,
    conn_factory: Callable[[], Any] | None = None,
    tickers: list[str] | None = None,
    snapshot_date: date | None = None,
    enabled: bool | None = None,
    source: str = "tradier",
) -> ChainArchiveResult:
    snapshot_date = snapshot_date or datetime.now(timezone.utc).date()
    enabled = ENABLE_CHAIN_ARCHIVER if enabled is None else bool(enabled)
    if conn_factory is None:
        from ap.db import conn as conn_factory
    if not enabled:
        return ChainArchiveResult(True, False, snapshot_date.isoformat(), 0, 0, 0, 0, [], 0)

    tickers = tickers if tickers is not None else build_ticker_universe(conn_factory)
    tickers = [_clean_ticker(t) for t in tickers]
    tickers = [t for i, t in enumerate(tickers) if t and t not in tickers[:i]]
    errors: list[dict[str, str]] = []
    expirations_seen = rows_inserted = rows_existing = storage_bytes = 0

    for ticker in tickers:
        try:
            underlying = _underlying_price(broker, ticker)
            expirations = select_archive_expirations(
                broker.get_option_expirations(ticker),
                today=snapshot_date,
            )
            for expiration in expirations:
                try:
                    raw_chain = broker.get_option_chain(ticker, expiration) or []
                    compacted = compact_chain(raw_chain, underlying_price=underlying)
                    if not compacted:
                        continue
                    iv = atm_iv(compacted, underlying)
                    inserted = _archive_row(
                        conn_factory,
                        snapshot_date=snapshot_date,
                        ticker=ticker,
                        expiration=expiration,
                        underlying_price=underlying,
                        compacted=compacted,
                        atm_iv_value=iv,
                        source=source,
                    )
                    expirations_seen += 1
                    storage_bytes += len(json.dumps(compacted, separators=(",", ":"), sort_keys=True).encode("utf-8"))
                    if inserted:
                        rows_inserted += 1
                    else:
                        rows_existing += 1
                except Exception as exc:
                    log.warning("chain_archive_expiration_failed ticker=%s expiration=%s error=%s", ticker, expiration, exc)
                    errors.append({"ticker": ticker, "expiration": expiration, "error": str(exc)})
        except Exception as exc:
            log.warning("chain_archive_ticker_failed ticker=%s error=%s", ticker, exc)
            errors.append({"ticker": ticker, "expiration": "", "error": str(exc)})

    return ChainArchiveResult(
        ok=True,
        enabled=True,
        snapshot_date=snapshot_date.isoformat(),
        tickers_seen=len(tickers),
        expirations_seen=expirations_seen,
        rows_inserted=rows_inserted,
        rows_existing=rows_existing,
        errors=errors,
        storage_estimate_bytes=storage_bytes,
    )


def build_hmac_headers(secret: str, payload: dict[str, Any] | None = None, *, timestamp: int | None = None) -> tuple[dict[str, str], bytes]:
    body = json.dumps(payload or {}, separators=(",", ":")).encode("utf-8")
    ts = str(timestamp or int(time.time()))
    sig = hmac.new(secret.encode("utf-8"), ts.encode("utf-8") + b"." + body, hashlib.sha256).hexdigest()
    return {
        "Content-Type": "application/json",
        "X-AP-Timestamp": ts,
        "X-AP-Signature": sig,
    }, body


def call_chain_archive_endpoint(*, bot_url: str, signing_secret: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    if not bot_url:
        raise RuntimeError("BOT_URL required")
    if not signing_secret:
        raise RuntimeError("SIGNING_SECRET required")
    url = bot_url.rstrip("/") + "/cron/chain-archive"
    headers, body = build_hmac_headers(signing_secret, payload or {})
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {"ok": True}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace")
        raise RuntimeError(f"chain archive endpoint failed HTTP {exc.code}: {raw}") from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Trigger signed daily option-chain archive capture")
    parser.add_argument("--date", default="", help="Optional snapshot date YYYY-MM-DD")
    args = parser.parse_args(argv)
    if not ENABLE_CHAIN_ARCHIVER:
        log.warning("chain_archiver_disabled")
        return 0
    payload: dict[str, Any] = {}
    if args.date:
        payload["date"] = args.date
    out = call_chain_archive_endpoint(
        bot_url=os.getenv("BOT_URL", ""),
        signing_secret=os.getenv("SIGNING_SECRET", ""),
        payload=payload,
    )
    print(json.dumps(out, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
