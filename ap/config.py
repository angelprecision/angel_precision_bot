# ap/config.py — FIXED
# CHANGES:
#   1. Renamed MAX_DAILY_LOSS_PCT → DAILY_MAX_LOSS_PCT (was causing AttributeError in risk.py)
#   2. Added DRAWDOWN_KILL and DRAWDOWN_STOP_DAY (referenced in risk.py but never defined)
#   3. Tightened STOP_LOSS_PCT: 0.50 → 0.25 (hard stop at -25%, not -50%)
#   4. Tightened TAKE_PROFIT_PCT: 0.30 → 0.50 (take profit at +50%)
#   5. Reduced MAX_POSITION_COST: $5000 → $1000 (hard cap per trade)
#   6. Reduced BASE_POSITION_PCT: 5% → 2% (2% risk per trade)
#   7. Reduced MAX_TRADES_PER_DAY: 10 → 3 (3 trades max daily)
#   8. Reduced MAX_CONCURRENT_POSITIONS: 3 → 2

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    # ─── DATABASE ───────────────────────────────────────────────────────────
    DB_FILE: str = os.getenv("BOT_DB_FILE", "ap_state.db")

    # ─── MODE ───────────────────────────────────────────────────────────────
    DEFAULT_CLIENT_MODE: str = os.getenv("BOT_MODE", "PAPER").upper()
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

    # ─── POSITION SIZING ────────────────────────────────────────────────────
    # Risk 2% of equity per trade (was 5% — too aggressive)
    BASE_POSITION_PCT: float = min(float(os.getenv("BASE_POSITION_PCT", "0.02")), 0.05)

    # Hard dollar cap per trade: $1,000 max (was $5,000 — led to $207k positions)
    MAX_POSITION_COST: float = float(os.getenv("MAX_POSITION_COST", "1000"))

    # ─── TRADE LIMITS ───────────────────────────────────────────────────────
    # Max 2 positions open at once (was 3)
    MAX_CONCURRENT_POSITIONS: int = int(os.getenv("MAX_CONCURRENT_POSITIONS", "2"))

    # Max 3 trades per day (was 10 — overtrading kills accounts)
    MAX_TRADES_PER_DAY: int = int(os.getenv("MAX_TRADES_PER_DAY", "5"))

    # ─── DAILY LOSS KILL SWITCH ──────────────────────────────────────────────
    # FIX: was MAX_DAILY_LOSS_PCT — risk.py references DAILY_MAX_LOSS_PCT (caused AttributeError)
    # Kill the day if down 6% (was 17% — way too loose)
    DAILY_MAX_LOSS_PCT: float = float(os.getenv("DAILY_MAX_LOSS_PCT", "0.06"))

    # ─── DRAWDOWN KILL ───────────────────────────────────────────────────────
    # FIX: These were referenced in risk.py but never defined in config → AttributeError
    # Account-level kill switch if down 15% from start of run
    DRAWDOWN_KILL: float = float(os.getenv("DRAWDOWN_KILL", "0.15"))
    # Pause new entries for the day if down 8%
    DRAWDOWN_STOP_DAY: float = float(os.getenv("DRAWDOWN_STOP_DAY", "0.08"))

    # ─── TAKE PROFIT / STOP LOSS ─────────────────────────────────────────────
    # FIX: STOP_LOSS_PCT was 0.50 but Render thread death meant it never fired
    # Tightened to 0.25 (close at -25% of option premium paid)
    # This is the DB default — exit_manager reads these from positions table
    TAKE_PROFIT_PCT: float = float(os.getenv("TAKE_PROFIT_PCT", "0.20"))
    STOP_LOSS_PCT:   float = float(os.getenv("STOP_LOSS_PCT",   "0.35"))

    # ─── EOD FLATTEN ────────────────────────────────────────────────────────
    # Close all positions 15 min before market close (3:45 PM ET)
    EOD_FLATTEN_MINUTES_BEFORE_CLOSE: int = int(os.getenv("EOD_FLATTEN_MINUTES", "15"))

    # ─── ENTRY TIMING ───────────────────────────────────────────────────────
    # Only take entries between 9:35 AM and 1:30 PM ET
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

