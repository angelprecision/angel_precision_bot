"""
ap/exit_context_hydrator.py
PR #175 — P0: Hydrate live exit context before every non-manual exit decision.

Problem this solves:
  The exit engine was making SOFT_LOSS / THESIS_FAIL_SOFT_STOP / NEVER_GREEN_STOP
  decisions while proof_trades had execution_mode=unknown, underlying_entry=0,
  exit_bid/ask/mid=null, and a synthetic broker-repair position_id.

  This caused valid live trades (NKE, RIVN) to be exited because the engine
  treated no_underlying_data as thesis failure.

This module:
  1. Builds a complete ExitContext from DB + fresh market data.
  2. Preserves real positions.id — never replaces with broker-repair-* if a DB row exists.
  3. Preserves execution_mode=live from orders → positions → exits.
  4. Provides all proof_trades fields so proof is never written with nulls.

Usage (in exit engine, before any non-manual exit decision):
    from ap.exit_context_hydrator import ExitContextHydrator, ExitContext

    hydrator = ExitContextHydrator()
    ctx = hydrator.hydrate(position_id=pos_id, client_id=client_id)
    if ctx is None:
        log.error("Could not hydrate exit context — skip cycle")
        return

Author: Angel Precision Intelligence
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import requests

import ap.db as db

log = logging.getLogger(__name__)

POLYGON_API_KEY = "YqXki5W6iwLdft8wKNNFSYvvmGzz8oLG"
POLYGON_SNAPSHOT_URL = (
    "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
    "/{ticker}?apiKey={key}"
)
POLYGON_OPTIONS_SNAPSHOT_URL = (
    "https://api.polygon.io/v3/snapshot/options/{underlying}/{contract}"
    "?apiKey={key}"
)

# Maximum age (seconds) for a quote to be considered "fresh"
FRESH_QUOTE_MAX_AGE_SECS = 30
# Minimum option spread ratio to be considered sane (bid/ask ≤ this × mid)
SANE_SPREAD_MAX_RATIO = 0.50


# ---------------------------------------------------------------------------
# ExitContext — the single source of truth for an exit decision
# ---------------------------------------------------------------------------

@dataclass
class ExitContext:
    """
    Fully-hydrated context for a single exit evaluation.

    All fields that were None at hydration time are explicitly None — the
    LiveExitGuard uses presence/absence to decide whether a live soft exit
    is safe to proceed.
    """

    # ── Identity ────────────────────────────────────────────────────────────
    position_id: str                        # positions.id (never broker-repair-*)
    client_id: str
    client_email: str
    execution_mode: str                     # 'live' | 'paper' — never 'unknown'

    # ── Instrument ──────────────────────────────────────────────────────────
    contract: Optional[str]                 # OCC option symbol
    direction: Optional[str]               # 'CALL' | 'PUT'
    underlying_symbol: Optional[str]

    # ── Entry context (from orders + orders.meta) ────────────────────────────
    underlying_entry: Optional[float]
    entry_price: Optional[float]
    entry_bid: Optional[float]
    entry_ask: Optional[float]
    entry_mid: Optional[float]
    entry_price_source: Optional[str]
    broker_entry_order_id: Optional[str]
    broker_entry_fill_ts: Optional[str]
    watcher_audit: Optional[dict]

    # ── Signal context (from ap_signals) ────────────────────────────────────
    signal_id: Optional[str]
    stop_underlying: Optional[float]        # signal-level hard stop on underlying
    trigger_underlying: Optional[float]     # signal-level entry trigger on underlying

    # ── Current market (fresh quotes, fetched at hydration time) ────────────
    current_underlying: Optional[float]
    underlying_quote_age_secs: Optional[float]
    option_bid: Optional[float]
    option_ask: Optional[float]
    option_mid: Optional[float]
    option_quote_age_secs: Optional[float]
    option_spread_sane: bool = False        # True if spread is within SANE_SPREAD_MAX_RATIO

    # ── Exit context (filled when a broker exit order is known) ─────────────
    broker_exit_order_id: Optional[str] = None
    broker_exit_fill_ts: Optional[str] = None
    exit_price: Optional[float] = None
    exit_bid: Optional[float] = None
    exit_ask: Optional[float] = None
    exit_mid: Optional[float] = None
    exit_price_source: Optional[str] = None
    exit_pricing_tier: Optional[str] = None
    underlying_exit: Optional[float] = None

    # ── Timing ──────────────────────────────────────────────────────────────
    hold_since: Optional[datetime] = None
    hydrated_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    # ── Raw DB rows (for downstream logging / proof writing) ────────────────
    _positions_row: Optional[dict] = field(default=None, repr=False)
    _orders_row: Optional[dict] = field(default=None, repr=False)
    _signals_row: Optional[dict] = field(default=None, repr=False)

    @property
    def is_live(self) -> bool:
        return self.execution_mode == "live"

    @property
    def missing_live_critical_fields(self) -> list[str]:
        """
        Return names of fields that are required for a live soft-exit decision
        but are currently None.
        """
        required = {
            "underlying_entry": self.underlying_entry,
            "current_underlying": self.current_underlying,
            "stop_underlying": self.stop_underlying,
            "contract": self.contract,
            "direction": self.direction,
        }
        # Must have at least one option price dimension
        option_mark = self.option_mid or self.option_bid or self.option_ask
        if option_mark is None:
            required["option_mark"] = None
        return [k for k, v in required.items() if v is None]

    def as_proof_fields(self) -> dict:
        """
        Return the subset of fields needed to update proof_trades.
        Caller merges this dict into the UPDATE statement.
        """
        return {
            "execution_mode": self.execution_mode,
            "underlying_entry": self.underlying_entry,
            "underlying_exit": self.underlying_exit,
            "exit_bid": self.exit_bid,
            "exit_ask": self.exit_ask,
            "exit_mid": self.exit_mid,
            "exit_price_source": self.exit_price_source,
            "exit_pricing_tier": self.exit_pricing_tier,
            "broker_entry_order_id": self.broker_entry_order_id,
            "broker_exit_order_id": self.broker_exit_order_id,
            "broker_entry_fill_ts": self.broker_entry_fill_ts,
            "broker_exit_fill_ts": self.broker_exit_fill_ts,
        }


# ---------------------------------------------------------------------------
# ExitContextHydrator
# ---------------------------------------------------------------------------

class ExitContextHydrator:
    """
    Builds an ExitContext for a given (position_id, client_id) pair by
    querying the DB and fetching fresh market data.

    Hydration order:
        1. positions row → identity, execution_mode, contract, direction,
           underlying_symbol, hold_since
        2. Filled ENTRY order by position_id → entry prices, meta fields,
           broker_entry_order_id
        3. Fallback: filled ENTRY order by client_id + contract (when
           position_id is a broker-repair-* synthetic)
        4. orders.meta → watcher_audit, submit_bid/ask/mid
        5. ap_signals by canonical signal_id → stop_underlying,
           trigger_underlying
        6. Fresh underlying quote from Polygon
        7. Fresh option quote from Polygon options snapshot

    Returns None only if the positions row itself cannot be found.
    All other hydration failures are logged and produce None values in the
    ExitContext; the LiveExitGuard then decides whether to block the exit.
    """

    # ------------------------------------------------------------------ #
    #  Public API                                                          #
    # ------------------------------------------------------------------ #

    def hydrate(
        self,
        position_id: str,
        client_id: str,
    ) -> Optional[ExitContext]:
        """
        Build and return an ExitContext.  Returns None only if the
        positions row is missing entirely.
        """
        log.info(
            "[HYDRATOR] hydrating exit context position_id=%s client_id=%s",
            position_id, client_id,
        )

        # 1 — positions row ───────────────────────────────────────────────
        pos = self._fetch_position(position_id, client_id)
        if pos is None:
            log.error(
                "[HYDRATOR] positions row not found position_id=%s — "
                "cannot build ExitContext",
                position_id,
            )
            return None

        real_position_id = pos["id"]          # always use DB id
        execution_mode = self._resolve_execution_mode(pos)
        contract = pos.get("contract")
        direction = pos.get("direction")
        underlying_symbol = pos.get("underlying_symbol") or (
            self._infer_underlying(contract) if contract else None
        )
        client_email = pos.get("client_email") or self._fetch_client_email(client_id)
        hold_since = pos.get("opened_at") or pos.get("created_at")
        if isinstance(hold_since, str):
            try:
                hold_since = datetime.fromisoformat(hold_since)
            except ValueError:
                hold_since = None

        # 2 & 3 — ENTRY order ──────────────────────────────────────────────
        order = self._fetch_entry_order_by_position(real_position_id)
        if order is None and contract:
            order = self._fetch_entry_order_fallback(client_id, contract)
            if order:
                log.warning(
                    "[HYDRATOR] used fallback ENTRY order for position_id=%s "
                    "contract=%s order_id=%s",
                    real_position_id, contract, order.get("id"),
                )

        # 4 — meta fields ──────────────────────────────────────────────────
        meta = (order or {}).get("meta") or {}
        if isinstance(meta, str):
            import json as _json
            try:
                meta = _json.loads(meta)
            except Exception:
                meta = {}

        watcher_audit = meta.get("watcher_audit")
        entry_bid = _safe_float(
            meta.get("submit_bid") or meta.get("entry_bid")
        )
        entry_ask = _safe_float(
            meta.get("submit_ask") or meta.get("entry_ask")
        )
        entry_mid = _safe_float(
            meta.get("submit_mid") or meta.get("entry_mid")
        )
        entry_price = _safe_float(
            (order or {}).get("fill_price")
            or (order or {}).get("price")
            or entry_mid
        )
        entry_price_source = meta.get("entry_price_source")
        broker_entry_order_id = (order or {}).get("broker_order_id") or (
            (order or {}).get("id")
        )
        broker_entry_fill_ts = str(
            (order or {}).get("filled_at") or ""
        ) or None

        # underlying_entry — from watcher_audit first, then signal, then pos
        underlying_entry = _safe_float(
            (watcher_audit or {}).get("underlying_price_at_trigger")
            or meta.get("underlying_entry")
            or pos.get("underlying_entry")
        )

        # 5 — ap_signals ───────────────────────────────────────────────────
        signal_id = (
            pos.get("signal_id")
            or meta.get("signal_id")
            or (watcher_audit or {}).get("signal_id")
        )
        stop_underlying = None
        trigger_underlying = None
        signals_row = None
        if signal_id:
            signals_row = self._fetch_signal(signal_id)
            if signals_row:
                stop_underlying = _safe_float(
                    signals_row.get("stop_price")
                    or signals_row.get("stop_underlying")
                )
                trigger_underlying = _safe_float(
                    signals_row.get("trigger_price")
                    or signals_row.get("entry_trigger")
                    or signals_row.get("reference_price")
                )

        # Fallback stop from order meta
        if stop_underlying is None:
            stop_underlying = _safe_float(meta.get("stop_underlying"))
        if trigger_underlying is None:
            trigger_underlying = _safe_float(meta.get("trigger_price"))

        # 6 — fresh underlying quote ───────────────────────────────────────
        current_underlying = None
        underlying_quote_age_secs = None
        if underlying_symbol:
            current_underlying, underlying_quote_age_secs = (
                self._fetch_underlying_quote(underlying_symbol)
            )

        # 7 — fresh option quote ───────────────────────────────────────────
        option_bid = option_ask = option_mid = None
        option_quote_age_secs = None
        option_spread_sane = False
        if contract and underlying_symbol:
            (
                option_bid,
                option_ask,
                option_mid,
                option_quote_age_secs,
                option_spread_sane,
            ) = self._fetch_option_quote(underlying_symbol, contract)

        ctx = ExitContext(
            # identity
            position_id=real_position_id,
            client_id=client_id,
            client_email=client_email or "",
            execution_mode=execution_mode,
            # instrument
            contract=contract,
            direction=direction,
            underlying_symbol=underlying_symbol,
            # entry
            underlying_entry=underlying_entry,
            entry_price=entry_price,
            entry_bid=entry_bid,
            entry_ask=entry_ask,
            entry_mid=entry_mid,
            entry_price_source=entry_price_source,
            broker_entry_order_id=str(broker_entry_order_id) if broker_entry_order_id else None,
            broker_entry_fill_ts=broker_entry_fill_ts,
            watcher_audit=watcher_audit,
            # signal
            signal_id=str(signal_id) if signal_id else None,
            stop_underlying=stop_underlying,
            trigger_underlying=trigger_underlying,
            # current market
            current_underlying=current_underlying,
            underlying_quote_age_secs=underlying_quote_age_secs,
            option_bid=option_bid,
            option_ask=option_ask,
            option_mid=option_mid,
            option_quote_age_secs=option_quote_age_secs,
            option_spread_sane=option_spread_sane,
            # timing
            hold_since=hold_since,
            # raw rows for downstream
            _positions_row=dict(pos),
            _orders_row=dict(order) if order else None,
            _signals_row=dict(signals_row) if signals_row else None,
        )

        missing = ctx.missing_live_critical_fields
        if ctx.is_live and missing:
            log.warning(
                "[HYDRATOR] live ExitContext is missing critical fields=%s "
                "position_id=%s contract=%s",
                missing, real_position_id, contract,
            )
        else:
            log.info(
                "[HYDRATOR] ExitContext hydrated OK position_id=%s "
                "execution_mode=%s contract=%s direction=%s "
                "underlying_entry=%s current_underlying=%s stop=%s",
                real_position_id, execution_mode, contract, direction,
                underlying_entry, current_underlying, stop_underlying,
            )

        return ctx

    # ------------------------------------------------------------------ #
    #  DB fetchers                                                         #
    # ------------------------------------------------------------------ #

    def _fetch_position(
        self, position_id: str, client_id: str
    ) -> Optional[dict]:
        """Fetch the positions row, trying both id and client_id for safety."""
        try:
            with db.conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT p.*,
                               c.email AS client_email,
                               c.execution_mode AS client_execution_mode
                        FROM positions p
                        LEFT JOIN clients c ON c.id = p.client_id
                        WHERE p.id = %s
                          AND p.client_id = %s
                        LIMIT 1
                        """,
                        (position_id, client_id),
                    )
                    row = cur.fetchone()
                    if row:
                        return dict(row)
                    # Fallback: broker-repair-* id may not exist; try without it
                    cur.execute(
                        """
                        SELECT p.*,
                               c.email AS client_email,
                               c.execution_mode AS client_execution_mode
                        FROM positions p
                        LEFT JOIN clients c ON c.id = p.client_id
                        WHERE p.client_id = %s
                          AND p.status = 'open'
                        ORDER BY p.opened_at DESC
                        LIMIT 1
                        """,
                        (client_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        log.warning(
                            "[HYDRATOR] position_id=%s not found; "
                            "fell back to latest open position id=%s for client_id=%s",
                            position_id, row["id"], client_id,
                        )
                        return dict(row)
        except Exception as exc:
            log.exception(
                "[HYDRATOR] _fetch_position error position_id=%s: %s",
                position_id, exc,
            )
        return None

    def _fetch_entry_order_by_position(
        self, position_id: str
    ) -> Optional[dict]:
        try:
            with db.conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT *
                        FROM orders
                        WHERE position_id = %s
                          AND kind = 'ENTRY'
                          AND status IN ('filled', 'FILLED', 'partial')
                        ORDER BY filled_at DESC, created_at DESC
                        LIMIT 1
                        """,
                        (position_id,),
                    )
                    row = cur.fetchone()
                    return dict(row) if row else None
        except Exception as exc:
            log.exception(
                "[HYDRATOR] _fetch_entry_order_by_position error "
                "position_id=%s: %s", position_id, exc,
            )
            return None

    def _fetch_entry_order_fallback(
        self, client_id: str, contract: str
    ) -> Optional[dict]:
        """
        Fallback: find the most recent filled ENTRY order for this
        client_id + contract when position_id lookup fails (broker-repair case).
        """
        try:
            with db.conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT *
                        FROM orders
                        WHERE client_id = %s
                          AND contract = %s
                          AND kind = 'ENTRY'
                          AND status IN ('filled', 'FILLED', 'partial')
                        ORDER BY filled_at DESC, created_at DESC
                        LIMIT 1
                        """,
                        (client_id, contract),
                    )
                    row = cur.fetchone()
                    return dict(row) if row else None
        except Exception as exc:
            log.exception(
                "[HYDRATOR] _fetch_entry_order_fallback error "
                "client_id=%s contract=%s: %s", client_id, contract, exc,
            )
            return None

    def _fetch_signal(self, signal_id: str) -> Optional[dict]:
        try:
            with db.conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT * FROM ap_signals WHERE id = %s LIMIT 1",
                        (signal_id,),
                    )
                    row = cur.fetchone()
                    return dict(row) if row else None
        except Exception as exc:
            log.exception(
                "[HYDRATOR] _fetch_signal error signal_id=%s: %s",
                signal_id, exc,
            )
            return None

    def _fetch_client_email(self, client_id: str) -> Optional[str]:
        try:
            with db.conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT email FROM clients WHERE id = %s LIMIT 1",
                        (client_id,),
                    )
                    row = cur.fetchone()
                    return row["email"] if row else None
        except Exception as exc:
            log.exception(
                "[HYDRATOR] _fetch_client_email error client_id=%s: %s",
                client_id, exc,
            )
            return None

    # ------------------------------------------------------------------ #
    #  Market data fetchers                                                #
    # ------------------------------------------------------------------ #

    def _fetch_underlying_quote(
        self, symbol: str
    ) -> tuple[Optional[float], Optional[float]]:
        """
        Returns (price, age_secs).  Uses Polygon v2 snapshot.
        age_secs is seconds since the last trade timestamp.
        """
        url = POLYGON_SNAPSHOT_URL.format(ticker=symbol, key=POLYGON_API_KEY)
        try:
            resp = requests.get(url, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            ticker = data.get("ticker") or {}
            day = ticker.get("day") or {}
            last_trade = ticker.get("lastTrade") or {}
            last_quote = ticker.get("lastQuote") or {}

            # Best price: last trade → mid of last quote → day close
            price = _safe_float(last_trade.get("p")) or _safe_float(
                _mid(
                    last_quote.get("P"),   # ask
                    last_quote.get("p"),   # bid
                )
            ) or _safe_float(day.get("c"))

            # Age from last trade timestamp (nanoseconds)
            ts_ns = last_trade.get("t")
            age_secs = None
            if ts_ns:
                ts_secs = ts_ns / 1e9
                age_secs = time.time() - ts_secs

            log.debug(
                "[HYDRATOR] underlying quote %s price=%s age_secs=%s",
                symbol, price, age_secs,
            )
            return price, age_secs

        except Exception as exc:
            log.warning(
                "[HYDRATOR] Polygon underlying quote failed symbol=%s: %s",
                symbol, exc,
            )
            return None, None

    def _fetch_option_quote(
        self, underlying_symbol: str, contract: str
    ) -> tuple[
        Optional[float], Optional[float], Optional[float],
        Optional[float], bool,
    ]:
        """
        Returns (bid, ask, mid, age_secs, spread_sane).

        Uses Polygon v3 option snapshot.
        Falls back to None values if the call fails; the LiveExitGuard will
        treat this as DATA_DEGRADED for live positions.
        """
        url = POLYGON_OPTIONS_SNAPSHOT_URL.format(
            underlying=underlying_symbol,
            contract=contract,
            key=POLYGON_API_KEY,
        )
        try:
            resp = requests.get(url, timeout=5)
            resp.raise_for_status()
            data = resp.json()
            result = (data.get("results") or {})
            details = result.get("details") or {}
            greeks = result.get("greeks") or {}  # not used for pricing, but available
            day = result.get("day") or {}
            last_quote = result.get("last_quote") or {}

            bid = _safe_float(last_quote.get("bid") or day.get("open"))
            ask = _safe_float(last_quote.get("ask"))
            mid = _safe_float(last_quote.get("midpoint")) or _mid(bid, ask)

            # Age from last_quote.last_updated (nanoseconds)
            ts_ns = last_quote.get("last_updated")
            age_secs = None
            if ts_ns:
                age_secs = time.time() - (ts_ns / 1e9)

            # Spread sanity: bid/ask spread should not exceed 50% of mid
            spread_sane = False
            if bid is not None and ask is not None and mid and mid > 0:
                spread_ratio = (ask - bid) / mid
                spread_sane = spread_ratio <= SANE_SPREAD_MAX_RATIO

            log.debug(
                "[HYDRATOR] option quote %s bid=%s ask=%s mid=%s "
                "age_secs=%s spread_sane=%s",
                contract, bid, ask, mid, age_secs, spread_sane,
            )
            return bid, ask, mid, age_secs, spread_sane

        except Exception as exc:
            log.warning(
                "[HYDRATOR] Polygon option quote failed contract=%s: %s",
                contract, exc,
            )
            return None, None, None, None, False

    # ------------------------------------------------------------------ #
    #  Helpers                                                             #
    # ------------------------------------------------------------------ #

    def _resolve_execution_mode(self, pos: dict) -> str:
        """
        Resolve execution_mode from the positions row.
        Prefers positions.execution_mode → clients.execution_mode.
        Never returns 'unknown'.
        """
        mode = (
            pos.get("execution_mode")
            or pos.get("client_execution_mode")
            or "paper"
        )
        if mode not in ("live", "paper"):
            log.warning(
                "[HYDRATOR] unexpected execution_mode=%s — defaulting to paper",
                mode,
            )
            return "paper"
        return mode

    @staticmethod
    def _infer_underlying(contract: str) -> Optional[str]:
        """
        Derive underlying symbol from OCC contract string.
        OCC format: TICKER + 6-digit date + C/P + 8-digit strike
        E.g. NKE250620P00085000 → NKE
        """
        if not contract:
            return None
        # Find first digit
        for i, ch in enumerate(contract):
            if ch.isdigit():
                ticker = contract[:i]
                return ticker if ticker else None
        return None


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        f = float(value)
        return f if f != 0.0 else None  # treat 0 as missing (common corruption)
    except (TypeError, ValueError):
        return None


def _mid(bid: Any, ask: Any) -> Optional[float]:
    b = _safe_float(bid)
    a = _safe_float(ask)
    if b is not None and a is not None:
        return round((b + a) / 2, 4)
    return b or a or None
