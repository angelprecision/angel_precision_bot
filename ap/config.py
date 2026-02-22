# ap/config.py - PRODUCTION CONFIGURATION (CLEAN + SAFE)

import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Config:
    # =====================================================================
    # DATABASE
    # =====================================================================
    DB_FILE: str = os.getenv("BOT_DB_FILE", "ap_state.db")

    # =====================================================================
    # DEFAULT MODE & LOGGING (BOOTSTRAP DEFAULTS ONLY)
    # Real gating uses client_state.mode + client_state.kill_switch
    # =====================================================================
    DEFAULT_CLIENT_MODE: str = os.getenv("BOT_MODE", "PAPER").upper()
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

    # =====================================================================
    # TRADING LIMITS (SAFETY FIRST)
    # =====================================================================

    # Base sizing: default 5% of equity, capped at 15% max
    BASE_POSITION_PCT: float = min(float(os.getenv("BASE_POSITION_PCT", "0.05")), 0.15)

    # Hard maximum cost per trade (absolute cap)
    MAX_POSITION_COST: float = float(os.getenv("MAX_POSITION_COST", "5000"))

    # Hard maximum concurrent positions
    MAX_CONCURRENT_POSITIONS: int = int(os.getenv("MAX_CONCURRENT_POSITIONS", "3"))

    # Hard maximum trades per day
    MAX_TRADES_PER_DAY: int = int(os.getenv("MAX_TRADES_PER_DAY", "10"))

    # Daily loss stop (kill switch at 17% realized loss on the day)
    MAX_DAILY_LOSS_PCT: float = float(os.getenv("MAX_DAILY_LOSS_PCT", "0.17"))

    # =====================================================================
    # TAKE PROFIT / STOP LOSS (used by exits)
    # =====================================================================
    TAKE_PROFIT_PCT: float = float(os.getenv("TAKE_PROFIT_PCT", "0.30"))
    STOP_LOSS_PCT: float = float(os.getenv("STOP_LOSS_PCT", "0.50"))

    # =====================================================================
    # OPTIONAL: GROWTH THROTTLES (only if you use ap.account_growth)
    # =====================================================================
    GROWTH_THROTTLE_50: float = float(os.getenv("GROWTH_THROTTLE_50", "0.50"))
    GROWTH_THROTTLE_75: float = float(os.getenv("GROWTH_THROTTLE_75", "0.75"))
    GROWTH_THROTTLE_90: float = float(os.getenv("GROWTH_THROTTLE_90", "0.90"))

    REDUCED_POSITION_PCT_50: float = float(os.getenv("REDUCED_POSITION_PCT_50", "0.12"))
    REDUCED_MAX_TRADES_75: int = int(os.getenv("REDUCED_MAX_TRADES_75", "4"))

    # =====================================================================
    # TRADING HOURS (Eastern Time) - optional enforcement elsewhere
    # =====================================================================
    ENTRY_START_ET: str = os.getenv("ENTRY_START_ET", "09:35")
    ENTRY_END_ET: str = os.getenv("ENTRY_END_ET", "13:30")
    NO_ENTRY_LAST_MINUTES: int = int(os.getenv("NO_ENTRY_LAST_MINUTES", "10"))
    COOLDOWN_AFTER_EXIT_MINUTES: int = int(os.getenv("COOLDOWN_AFTER_EXIT_MINUTES", "3"))

    # =====================================================================
    # QUOTE QUALITY - optional enforcement elsewhere
    # =====================================================================
    MAX_SPREAD_PCT: float = float(os.getenv("MAX_SPREAD_PCT", "0.10"))
    MAX_QUOTE_AGE_SECONDS: int = int(os.getenv("MAX_QUOTE_AGE_SECONDS", "5"))

    # =====================================================================
    # TRADIER CREDENTIALS
    # =====================================================================
    TRADIER_ACCOUNT_ID: str = os.getenv("TRADIER_ACCOUNT_ID", "")
    TRADIER_ACCESS_TOKEN: str = os.getenv("TRADIER_ACCESS_TOKEN", "")
    TRADIER_BASE_URL: str = os.getenv("TRADIER_BASE_URL", "https://sandbox.tradier.com")

    # Backward compatibility: older code expects cfg.BOT_MODE
    @property
    def BOT_MODE(self) -> str:
        return self.DEFAULT_CLIENT_MODE
