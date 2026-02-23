# ap/contract_pricing.py - PRODUCTION SAFE PRICING

from __future__ import annotations

from ap.logger import get_logger
from ap.broker import BrokerAdapter
from ap.config import Config
from datetime import datetime
import re

log = get_logger("ap.pricing")
cfg = Config()

MIN_BID_ASK = 0.01


def _to_float(x):
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _round_tick(price: float, tick: float = 0.01) -> float:
    try:
        return round(float(price) / tick) * tick
    except Exception:
        return float(price)


def is_contract_expired(symbol: str) -> bool:
    match = re.search(r'(\d{6})[CP]\d+$', symbol)
    if not match:
        return False
    try:
        expiry = datetime.strptime(match.group(1), "%y%m%d").date()
        return expiry < datetime.today().date()
    except Exception:
        return False


def get_contract_price(broker: BrokerAdapter, contract_symbol: str, side: str = "SELL") -> float:
    if is_contract_expired(contract_symbol):
        log.warning(f"Contract {contract_symbol} is expired, skipping")
        return 0.0

    side = (side or "SELL").upper().strip()
    if side not in ("BUY", "SELL"):
        side = "SELL"

    try:
        quote = broker.get_quote(contract_symbol) or {}
        bid = _to_float(quote.get("bid"))
        ask = _to_float(quote.get("ask"))
        last = _to_float(quote.get("last"))

        if bid is not None and bid < MIN_BID_ASK:
            bid = None
        if ask is not None and ask < MIN_BID_ASK:
            ask = None
        if last is not None and last < MIN_BID_ASK:
            last = None

        # =============================================
        # SPREAD CHECK: BUY side only.
        # On SELL (exit) we MUST get out regardless of spread.
        # Never block an exit due to wide spread.
        # =============================================
        if side == "BUY":
            if bid is not None and ask is not None and bid > 0 and ask > 0:
                mid = (bid + ask) / 2.0
                spread = ask - bid
                spread_pct = (spread / mid) if mid > 0 else 1.0
                if spread_pct > float(cfg.MAX_SPREAD_PCT):
                    log.warning(
                        f"Reject price (spread too wide) {contract_symbol} "
                        f"side={side} bid={bid} ask={ask} last={last} spread_pct={spread_pct:.2f}"
                    )
                    return 0.0

        # =============================================
        # PRICE SELECTION
        # SELL (exit): use bid — that's what you get filled at
        # BUY (entry): use ask — that's what you pay
        # =============================================
        if side == "SELL":
            if bid is not None and bid > 0:
                return _round_tick(bid)
            if last is not None and last > 0:
                return _round_tick(last)
            if bid is not None and ask is not None and bid > 0 and ask > 0:
                return _round_tick((bid + ask) / 2.0)
        else:
            if ask is not None and ask > 0:
                return _round_tick(ask)
            if last is not None and last > 0:
                return _round_tick(last)
            if bid is not None and ask is not None and bid > 0 and ask > 0:
                return _round_tick((bid + ask) / 2.0)

        log.warning(
            f"No valid price {contract_symbol} side={side} bid={bid} ask={ask} last={last} raw={quote}"
        )
        return 0.0

    except Exception as e:
        log.error(f"Failed to get price for {contract_symbol} side={side}: {e}")
        return 0.0
