Run this as a GitHub Actions workflow every 5 minutes during market hours.
It EXTERNALLY polls your Render bot to check and trigger exits —
completely bypassing the Render free-tier thread death problem.

HOW IT WORKS:
  1. Pings GET /client/{client_id}/positions → gets all OPEN positions
  2. For each open position, fetches current option price via Tradier
  3. If price hit stop (-25%) or take profit (+50%), posts to /exit endpoint
  4. Sends Discord alert when exit triggered

WHY THIS IS THE CORRECT ARCHITECTURE:
  Render free tier spins down after 15 min idle → daemon threads die →
  your -50% stop loss never fires → options expire at -95%.
  An external GitHub Actions cron is FREE, runs every 5 min, and is reliable.

SETUP:
  1. Add this file to your repo as scripts/exit_poller.py
  2. Create .github/workflows/exit_poller.yml (template below)
  3. Add secrets: BOT_URL, BOT_CLIENT_ID, SIGNING_SECRET, DISCORD_WEBHOOK_URL

GitHub Actions workflow (exit_poller.yml):
  name: Exit Poller
  on:
    schedule:
      - cron: '*/5 9-16 * * 1-5'   # Every 5 min, 9am-4pm ET, Mon-Fri
    workflow_dispatch:
  jobs:
    poll:
      runs-on: ubuntu-latest
      steps:
        - uses: actions/checkout@v3
        - uses: actions/setup-python@v4
          with: { python-version: '3.11' }
        - run: pip install requests yfinance
        - run: python scripts/exit_poller.py
          env:
            BOT_URL: ${{ secrets.BOT_URL }}
            BOT_CLIENT_ID: ${{ secrets.BOT_CLIENT_ID }}
            SIGNING_SECRET: ${{ secrets.SIGNING_SECRET }}
            DISCORD_WEBHOOK_URL: ${{ secrets.DISCORD_WEBHOOK_URL }}
            TRADIER_ACCESS_TOKEN: ${{ secrets.TRADIER_ACCESS_TOKEN }}
            TRADIER_ACCOUNT_ID: ${{ secrets.TRADIER_ACCOUNT_ID }}
"""

import os
import time
import hmac
import hashlib
import json
import requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# ── CONFIG ────────────────────────────────────────────────────────────────
BOT_URL        = (os.getenv("BOT_URL", "https://angel-precision-bot-official-1.onrender.com") or "").strip()
BOT_CLIENT_ID  = (os.getenv("BOT_CLIENT_ID", "default_v4") or "").strip()
_SECRET        = (os.getenv("SIGNING_SECRET", "") or "").strip()
SIGNING_SECRET = _SECRET.encode() if _SECRET else b""
DISCORD_URL    = (os.getenv("DISCORD_WEBHOOK_URL", "") or "").strip()

TRADIER_TOKEN   = (os.getenv("TRADIER_ACCESS_TOKEN", "") or "").strip()
TRADIER_ACCOUNT = (os.getenv("TRADIER_ACCOUNT_ID", "") or "").strip()
TRADIER_BASE    = os.getenv("TRADIER_BASE_URL", "https://sandbox.tradier.com")

STOP_LOSS_PCT   = float(os.getenv("STOP_LOSS_PCT",   "0.25"))  # -25%
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "0.50"))  # +50%

NY = ZoneInfo("America/New_York")


# ── AUTH ──────────────────────────────────────────────────────────────────
def _sign(ts: str, raw: bytes) -> str:
    msg = ts.encode() + b"." + raw
    return hmac.new(SIGNING_SECRET, msg, hashlib.sha256).hexdigest()

def _headers(raw: bytes) -> dict:
    ts  = str(int(time.time()))
    sig = _sign(ts, raw)
    return {
        "Content-Type":   "application/json",
        "X-Client-Id":    BOT_CLIENT_ID,
        "X-AP-Timestamp": ts,
        "X-AP-Signature": sig,
    }

def discord(msg: str):
    if not DISCORD_URL: return
    try:
        requests.post(DISCORD_URL, json={"content": msg}, timeout=10)
    except Exception:
        pass


# ── GET OPEN POSITIONS FROM BOT ───────────────────────────────────────────
def get_open_positions() -> list:
    try:
        r = requests.get(
            f"{BOT_URL}/client/me/positions",
            params={"status": "OPEN"},
            headers={"X-Client-Id": BOT_CLIENT_ID, "X-AP-Timestamp": str(int(time.time())), "X-AP-Signature": "skip"},
            timeout=20,
        )
        data = r.json()
        return data.get("positions", [])
    except Exception as e:
        print(f"⚠️ Failed to get positions: {e}")
        return []


# ── GET OPTION PRICE FROM TRADIER ─────────────────────────────────────────
def get_option_bid(contract: str) -> float:
    """Get current bid price for option contract."""
    if not TRADIER_TOKEN:
        return 0.0
    try:
        r = requests.get(
            f"{TRADIER_BASE}/v1/markets/options/chains",
            params={"symbol": contract[:4].rstrip("0123456789"), "expiration": "auto"},
            headers={"Authorization": f"Bearer {TRADIER_TOKEN}", "Accept": "application/json"},
            timeout=10,
        )
        # Fallback: use quote endpoint directly
        r2 = requests.get(
            f"{TRADIER_BASE}/v1/markets/quotes",
            params={"symbols": contract, "greeks": "false"},
            headers={"Authorization": f"Bearer {TRADIER_TOKEN}", "Accept": "application/json"},
            timeout=10,
        )
        quote = r2.json().get("quotes", {}).get("quote", {})
        bid   = float(quote.get("bid") or quote.get("last") or 0)
        return bid
    except Exception as e:
        print(f"⚠️ Tradier quote failed for {contract}: {e}")
        return 0.0


# ── TRIGGER EXIT VIA BOT API ──────────────────────────────────────────────
def trigger_exit(position_id: str, reason: str, current_price: float) -> bool:
    payload = {
        "position_id":   position_id,
        "reason":        reason,
        "current_price": current_price,
    }
    raw = json.dumps(payload, separators=(",", ":")).encode()
    try:
        r = requests.post(
            f"{BOT_URL}/client/exit",
            data=raw,
            headers=_headers(raw),
            timeout=20,
        )
        return 200 <= r.status_code < 300
    except Exception as e:
        print(f"⚠️ Exit trigger failed: {e}")
        return False


# ── MAIN POLL LOOP ────────────────────────────────────────────────────────
def is_market_open() -> bool:
    now = datetime.now(timezone.utc).astimezone(NY)
    if now.weekday() >= 5: return False
    h, m = now.hour, now.minute
    if (h < 9) or (h == 9 and m < 30): return False
    if h >= 16: return False
    return True

def run_poll():
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n{'='*50}")
    print(f"EXIT POLLER — {now_str}")
    print(f"{'='*50}")

    if not is_market_open():
        print("Market closed — no exits to check.")
        return

    positions = get_open_positions()
    if not positions:
        print("No open positions.")
        return

    print(f"Checking {len(positions)} open position(s)...")

    exits_triggered = 0

    for pos in positions:
        pos_id   = pos.get("id") or pos.get("position_id")
        contract = pos.get("contract", "")
        avg_fill = float(pos.get("avg_fill") or pos.get("entry_price") or 0)

        if not contract or avg_fill <= 0:
            print(f"  ⚠️ Missing data for position {pos_id}")
            continue

        current_bid = get_option_bid(contract)
        if current_bid <= 0:
            print(f"  ⚠️ No price for {contract} — skipping")
            continue

        pnl_pct = (current_bid - avg_fill) / avg_fill

        print(f"  {contract}: bid=${current_bid:.2f} fill=${avg_fill:.2f} pnl={pnl_pct*100:.1f}%")

        # ── STOP LOSS ──
        if pnl_pct <= -STOP_LOSS_PCT:
            reason = f"STOP_LOSS ({pnl_pct*100:.1f}%)"
            print(f"  🛑 STOP LOSS: {contract} {pnl_pct*100:.1f}%")
            ok = trigger_exit(pos_id, reason, current_bid)
            if ok:
                exits_triggered += 1
                discord(
                    f"🛑 **STOP LOSS TRIGGERED** | {contract}\n"
                    f"   Entry: ${avg_fill:.2f} → Now: ${current_bid:.2f} ({pnl_pct*100:.1f}%)\n"
                    f"   Exit poller closed position | {now_str}"
                )
            else:
                discord(f"⚠️ Stop loss trigger FAILED for {contract} — manual review needed")
            continue

        # ── TAKE PROFIT ──
        if pnl_pct >= TAKE_PROFIT_PCT:
            reason = f"TAKE_PROFIT ({pnl_pct*100:.1f}%)"
            print(f"  ✅ TAKE PROFIT: {contract} +{pnl_pct*100:.1f}%")
            ok = trigger_exit(pos_id, reason, current_bid)
            if ok:
                exits_triggered += 1
                discord(
                    f"✅ **TAKE PROFIT** | {contract}\n"
                    f"   Entry: ${avg_fill:.2f} → Now: ${current_bid:.2f} (+{pnl_pct*100:.1f}%)\n"
                    f"   Exit poller closed position | {now_str}"
                )
            continue

        # ── EOD (3:40 PM ET) ──
        now_ny = datetime.now(timezone.utc).astimezone(NY)
        mins_to_close = (16 * 60) - (now_ny.hour * 60 + now_ny.minute)
        if mins_to_close <= 20:
            reason = f"EOD_FLATTEN ({mins_to_close}min to close)"
            print(f"  ⏰ EOD FLATTEN: {contract} ({mins_to_close}min to close)")
            ok = trigger_exit(pos_id, reason, current_bid)
            if ok:
                exits_triggered += 1
                discord(
                    f"⏰ **EOD FLATTEN** | {contract}\n"
                    f"   {mins_to_close}min to close | pnl={pnl_pct*100:.1f}% | {now_str}"
                )

    print(f"\n{'='*50}")
    print(f"Exits triggered: {exits_triggered}/{len(positions)}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    run_poll()
