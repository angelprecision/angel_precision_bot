# ap/config.py -- FIXED
# CHANGES:
#   1. Renamed MAX_DAILY_LOSS_PCT → DAILY_MAX_LOSS_PCT (matches execution.py fix)
#   2. Added DRAWDOWN_KILL and DRAWDOWN_STOP_DAY
#   3. DB_FILE defaults to /tmp/ap_state.db (Render ephemeral disk fix)
#   4. MAX_PREMIUM_PER_SHARE raised to 10.00 (SPY/QQQ ATM options are $5-10)
#   5. Tightened STOP_LOSS_PCT and TAKE_PROFIT_PCT
#   6. Reduced MAX_POSITION_COST, BASE_POSITION_PCT, MAX_TRADES_PER_DAY

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    # ─── DATABASE ───────────────────────────────────────────────────────────
    # FIX: Render free tier has ephemeral disk -- SQLite must live in /tmp
    # Without this, the DB is wiped on every redeploy → disk I/O errors
    DB_FILE: str = os.getenv("BOT_DB_FILE", "/tmp/ap_state.db")

    # ─── MODE ───────────────────────────────────────────────────────────────
    DEFAULT_CLIENT_MODE: str = os.getenv("BOT_MODE", "PAPER").upper()
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

    # ─── POSITION SIZING ────────────────────────────────────────────────────
    BASE_POSITION_PCT: float = min(float(os.getenv("BASE_POSITION_PCT", "0.02")), 0.05)
    MAX_POSITION_COST: float = float(os.getenv("MAX_POSITION_COST", "1000"))

    # ─── PREMIUM RANGE ───────────────────────────────────────────────────────
    # FIX: SPY/QQQ ATM 0DTE options trade at $5-10/share -- old cap was $2.50
    # Raised to $10.00 so we don't reject valid liquid contracts
    MIN_PREMIUM_PER_SHARE: float = float(os.getenv("MIN_PREMIUM_PER_SHARE", "0.50"))
    MAX_PREMIUM_PER_SHARE: float = float(os.getenv("MAX_PREMIUM_PER_SHARE", "10.00"))

    # ─── TRADE LIMITS ───────────────────────────────────────────────────────
    MAX_CONCURRENT_POSITIONS: int = int(os.getenv("MAX_CONCURRENT_POSITIONS", "4"))
    # AUDIT PHASE-2: raised 5 -> 12. With the slot-accounting bug fixed (canceled
    # orders no longer burn slots), and with score-based admission ordering, the
    # daily cap can be lifted so overnight + intraday signals fit in one session.
    # 12 is intentionally roomy: best-12-by-score across the day, not first-12.
    MAX_TRADES_PER_DAY: int = int(os.getenv("MAX_TRADES_PER_DAY", "12"))

    # ─── DAILY LOSS KILL SWITCH ──────────────────────────────────────────────
    # FIX: renamed from MAX_DAILY_LOSS_PCT -- execution.py now uses this name
    DAILY_MAX_LOSS_PCT: float = float(os.getenv("DAILY_MAX_LOSS_PCT", "0.06"))

    # ─── DRAWDOWN KILL ───────────────────────────────────────────────────────
    DRAWDOWN_KILL: float = float(os.getenv("DRAWDOWN_KILL", "0.15"))
    DRAWDOWN_STOP_DAY: float = float(os.getenv("DRAWDOWN_STOP_DAY", "0.08"))

    # ─── TAKE PROFIT / STOP LOSS ─────────────────────────────────────────────
    TAKE_PROFIT_PCT: float = float(os.getenv("TAKE_PROFIT_PCT", "0.20"))
    STOP_LOSS_PCT:   float = float(os.getenv("STOP_LOSS_PCT",   "0.25"))  # 25% -- was 0.35

    # ─── EOD FLATTEN ────────────────────────────────────────────────────────
    EOD_FLATTEN_MINUTES_BEFORE_CLOSE: int = int(os.getenv("EOD_FLATTEN_MINUTES", "15"))

    # ─── ENTRY TIMING ───────────────────────────────────────────────────────
    ENTRY_START_ET: str = os.getenv("ENTRY_START_ET", "09:35")
    ENTRY_END_ET:   str = os.getenv("ENTRY_END_ET",   "13:30")
    NO_ENTRY_LAST_MINUTES: int = int(os.getenv("NO_ENTRY_LAST_MINUTES", "10"))
    COOLDOWN_AFTER_EXIT_MINUTES: int = int(os.getenv("COOLDOWN_AFTER_EXIT_MINUTES", "3"))

    # ─── QUOTE QUALITY ──────────────────────────────────────────────────────
    MAX_SPREAD_PCT: float = float(os.getenv("MAX_SPREAD_PCT", "0.10"))
    MAX_QUOTE_AGE_SECONDS: int = int(os.getenv("MAX_QUOTE_AGE_SECONDS", "5"))

    # ─── GROWTH THROTTLES ───────────────────────────────────────────────────
    GROWTH_THROTTLE_50: float = float(os.getenv("GROWTH_THROTTLE_50", "0.50"))
    GROWTH_THROTTLE_75: float = float(os.getenv("GROWTH_THROTTLE_75", "0.75"))
    GROWTH_THROTTLE_90: float = float(os.getenv("GROWTH_THROTTLE_90", "0.90"))
    REDUCED_POSITION_PCT_50: float = float(os.getenv("REDUCED_POSITION_PCT_50", "0.12"))
    REDUCED_MAX_TRADES_75: int = int(os.getenv("REDUCED_MAX_TRADES_75", "4"))

    # ─── TRADIER ────────────────────────────────────────────────────────────
    TRADIER_ACCOUNT_ID:   str = os.getenv("TRADIER_ACCOUNT_ID", "")
    TRADIER_ACCESS_TOKEN: str = os.getenv("TRADIER_ACCESS_TOKEN", "")
    TRADIER_BASE_URL:     str = os.getenv("TRADIER_BASE_URL", "https://sandbox.tradier.com")

    @property
    def BOT_MODE(self) -> str:
        return self.DEFAULT_CLIENT_MODE
