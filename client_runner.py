# client_runner.py -- Multi-client trading loop for Angel Precision Bot
# =============================================================================
# Each active member gets their own isolated trading thread with:
#   - APMasterControl    -- sole decision authority
#   - APPositionManager  -- Postgres-backed position truth
#   - APOrderStateMachine-- enforced order lifecycle
#   - APContractSelector -- real premium-based contract selection
#   - APExecutionCore    -- entry watcher, exit engine, fill monitor
#   - worker_loop()      -- new control path queue dispatch
#
# Signal routing (gunicorn multi-worker safe):
#   route_signal_to_all_clients() → trade_queue (Postgres)
#   worker_loop() in each ClientRunner polls + dispatches via control stack
# =============================================================================

import base64
import hashlib
import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
_ET = ZoneInfo("America/New_York")

from cryptography.fernet import Fernet
from supabase import create_client, Client

from ap.db import conn as ap_conn, run_with_retry
from ap.queue import enqueue_signal, worker_loop
from ap.order_monitor import APOrderMonitor
from ap.position_sizer import APPositionSizer
from ap.market_intelligence import APEarningsGuard, APIVRankFilter
from ap.worker_health import get_monitor, init_monitor
from ap_reconciler import APBrokerReconciler
from ap_recovery   import APStartupRecovery
from ap.self_healing import get_healer, init_self_healing
from ap.utils import now_utc_iso

logger = logging.getLogger("client_runner")
_time_module: object = time   # alias for fan-out fallback cache
_members_cache: dict = {}     # {"emails": ([...], expires_monotonic)}
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_URL         = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

# ── Encryption ────────────────────────────────────────────────────────────────
# Defer ENCRYPTION_KEY validation to call time so supervisor can start even if
# key is not set (it will fail when a specific client tries to decrypt their token).
_raw_key = os.getenv("ENCRYPTION_KEY", "").strip()

def decrypt_token(ciphertext: str) -> str:
    if not _raw_key:
        raise RuntimeError("ENCRYPTION_KEY env var is required and not set")
    _key_bytes = hashlib.sha256(_raw_key.encode()).digest()
    _fernet    = Fernet(base64.urlsafe_b64encode(_key_bytes))
    return _fernet.decrypt(ciphertext.encode()).decode()

# ── Active runner registry ────────────────────────────────────────────────────
_active_runners: dict[str, "ClientRunner"] = {}
_registry_lock  = threading.Lock()


# =============================================================================
# CLIENT RUNNER
# =============================================================================

class ClientRunner(threading.Thread):
    """
    One thread per client. Owns its own:
      - broker connection
      - APMasterControl (with APPositionManager)
      - APOrderStateMachine
      - APContractSelectionEngine
      - APExecutionCore (entry watcher, exit engine, fill monitor, signal tracker)
      - worker_loop thread (queue dispatch via control stack)
    """

    def __init__(self, member: dict):
        super().__init__(daemon=True, name=f"runner-{member['email']}")
        self.member     = member
        self.email      = member["email"]
        self.account_id = member["tradier_account_id"]
        self.base_url   = member.get("tradier_base_url", "https://sandbox.tradier.com")
        self.stopped    = threading.Event()

        # Control stack -- set during run()
        self.core              = None
        self.master_control    = None
        self.reconciler        = None
        self.position_manager  = None
        self.order_state_machine = None
        self.contract_selector = None
        self.order_monitor        = None
        self.fill_monitor_thread  = None
        self.worker_thread        = None
        self.equity_thread        = None
        self.mode              = "PAPER"  # set in run() from AP_MODE env

    def _get_token(self) -> str | None:
        try:
            raw = self.member.get("tradier_access_token", "")
            if not raw:
                return None
            return decrypt_token(raw)
        except Exception as e:
            logger.error(f"[{self.email}] Token decrypt failed: {e}")
            return None

    def run(self):
        logger.info(
            f"[{self.email}] ClientRunner starting -- "
            f"account {self.account_id} @ {self.base_url}"
        )
        token = self._get_token()
        if not token:
            logger.error(f"[{self.email}] No token -- aborting runner")
            return

        # Canonical mode — set once, used everywhere in this runner
        self.mode = os.getenv("AP_MODE", "paper").upper()

        # LIVE assertions validate per-member credentials, not global env vars
        if self.mode == "LIVE":
            _missing = []
            if not token:
                _missing.append("member.tradier_access_token")
            if not str(self.account_id or "").strip():
                _missing.append("member.tradier_account_id")
            if not os.getenv("DATABASE_URL", "").strip():
                _missing.append("DATABASE_URL")
            if _missing:
                logger.error(
                    f"[{self.email}] LIVE mode startup aborted -- "
                    f"missing: {', '.join(_missing)}"
                )
                return
            logger.info(
                f"[{self.email}] LIVE mode assertions PASSED | "
                f"account_id={self.account_id} base_url={self.base_url}"
            )

        # ── Imports ──────────────────────────────────────────────────────────
        try:
            from ap.brokers.tradier import TradierBroker, TradierConfig
            from ap_execution_core  import APExecutionCore
            from ap_master_control  import APMasterControl
            from ap.position_manager import APPositionManager
            from ap.order_state_machine import APOrderStateMachine
            from ap.contract_selector import APContractSelectionEngine
        except Exception as e:
            logger.error(f"[{self.email}] Import failed: {e}")
            return

        try:
            # ── Auto-provision DB rows for this client ────────────────────────
            # Creates clients + client_state rows if missing.
            # Prevents ForeignKeyViolation on first heartbeat for new members.
            # ON CONFLICT DO NOTHING makes this a no-op for existing clients.
            from ap.db import ensure_client_exists
            ensure_client_exists(
                self.email,
                equity=float(os.getenv("ACCOUNT_EQUITY", "25000")),
            )
            logger.info(f"[{self.email}] DB rows provisioned")

            # ── Broker ───────────────────────────────────────────────────────
            broker_cfg = TradierConfig(
                base_url=self.base_url,
                access_token=token,
                account_id=self.account_id,
            )
            broker = TradierBroker(broker_cfg)

            # Startup: cancel phantom orders that survived restart (no broker_id)
            # Prevents them from inflating the position cap on every boot
            try:
                from ap.db import run_with_retry, conn as _conn
                def _clear_phantoms():
                    with _conn() as _c:
                        _c.execute(
                            "UPDATE orders SET status=%s, last_error=%s "
                            "WHERE client_id=%s "
                            "AND status IN ('CREATED','SUBMITTED','ACKNOWLEDGED') "
                            "AND (broker_order_id IS NULL OR broker_order_id = '')",
                            ('CANCELED', 'startup_phantom_clear', self.email)
                        )
                        return _c.rowcount
                _n = run_with_retry(_clear_phantoms)
                if _n:
                    logger.info(f"[{self.email}] Startup: cleared {_n} phantom orders")
            except Exception as _pe:
                logger.warning(f"[{self.email}] Startup phantom clear: {_pe}")

            logger.info(f"[{self.email}] Broker initialized. Starting execution core.")

            sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY) if SUPABASE_URL else None

            # ── Control stack ─────────────────────────────────────────────────
            self.position_manager = APPositionManager(client_id=self.email)

            position_sizer = APPositionSizer(
                throttle_threshold = float(os.getenv("THROTTLE_THRESHOLD", "-200")),
                stop_threshold     = float(os.getenv("STOP_THRESHOLD",     "-500")),
                throttle_factor    = float(os.getenv("THROTTLE_FACTOR",    "0.5")),
                min_history        = int(os.getenv("KELLY_MIN_HISTORY",    "20")),
            )

            # ── Load per-client limits from clients table (overrides env defaults) ──
            _client_cfg = {}
            try:
                from ap.db import run_with_retry, conn as _conn
                def _load_cfg():
                    with _conn() as _c:
                        _c.execute(
                            "SELECT max_trades_per_day, max_concurrent_positions, "
                            "daily_max_loss_pct, initial_equity "
                            "FROM clients WHERE client_id=%s",
                            (self.email,)
                        )
                        return _c.fetchone()
                _client_cfg = run_with_retry(_load_cfg) or {}
            except Exception as _cfg_err:
                logger.warning(f"[{self.email}] Could not load client config: {_cfg_err}")

            _equity      = float(_client_cfg.get("initial_equity", os.getenv("ACCOUNT_EQUITY", "25000")) or 25000)
            _max_trades  = int(_client_cfg.get("max_trades_per_day", os.getenv("MAX_TRADES_TODAY", "10")) or 10)
            _max_pos     = int(_client_cfg.get("max_concurrent_positions", os.getenv("MAX_POSITIONS", "10")) or 10)
            _loss_pct    = float(_client_cfg.get("daily_max_loss_pct", 0.06) or 0.06)
            _max_loss    = -abs(_equity * _loss_pct)

            logger.info(
                f"[{self.email}] Client limits loaded | "
                f"max_trades={_max_trades} max_pos={_max_pos} "
                f"daily_loss=${_max_loss:.0f} ({_loss_pct*100:.0f}%)"
            )

            self.master_control = APMasterControl(
                mode=self.mode.lower(),
                client_id=self.email,
                score_floor=float(os.getenv("SCORE_FLOOR", "65")),
                context_floor=float(os.getenv("CONTEXT_FLOOR", "0.0")),
                max_positions=_max_pos,
                max_capital_pct=float(os.getenv("MAX_CAPITAL_PCT", "0.40")),
                max_sector_pct=float(os.getenv("MAX_SECTOR_PCT", "0.25")),
                max_ticker_pct=float(os.getenv("MAX_TICKER_PCT", "0.10")),
                max_calls=int(os.getenv("MAX_CALLS", "10")),
                max_puts=int(os.getenv("MAX_PUTS", "10")),
                max_trades_today=_max_trades,
                max_daily_loss=_max_loss,
                account_equity=_equity,
                position_manager=self.position_manager,
                position_sizer=position_sizer,
                supabase_client=sb,
            )

            self.order_state_machine = APOrderStateMachine(client_id=self.email)

            # ── Live data broker (optional) ──────────────────────────────────
            # TRADIER_DATA_TOKEN + TRADIER_DATA_BASE_URL point at api.tradier.com
            # for real option chains. The execution broker (above) stays on
            # sandbox / paper so NO real orders are placed.
            _data_token   = os.getenv("TRADIER_DATA_TOKEN", "").strip()
            _data_base_url = os.getenv("TRADIER_DATA_BASE_URL",
                                        "https://api.tradier.com").strip()
            if _data_token:
                data_broker_cfg = TradierConfig(
                    base_url=_data_base_url,
                    access_token=_data_token,
                    account_id=self.account_id,   # account id not used for data calls
                )
                data_broker = TradierBroker(data_broker_cfg)
                logger.info(
                    f"[{self.email}] Live data broker initialized | "
                    f"{_data_base_url} (data-only, no orders)"
                )
            else:
                data_broker = broker   # fallback: same as execution broker
                logger.warning(
                    f"[{self.email}] TRADIER_DATA_TOKEN not set -- "
                    f"using execution broker for data (sandbox chains)"
                )

            # Earnings blackout gate -- blocks trades within N days of earnings
            earnings_guard = APEarningsGuard(
                broker=data_broker,   # use live data broker for earnings calendar
                blackout_days=int(os.getenv("EARNINGS_BLACKOUT_DAYS", "3")),
            )

            # IV rank filter -- blocks buying expensive premium (rank > threshold)
            iv_filter = APIVRankFilter(
                broker=data_broker,   # use live data broker for IV data
                max_iv_rank=float(os.getenv("MAX_IV_RANK", "100")),  # disabled: 85→100 (only blocks rank>100 which is impossible)
            )

            self.contract_selector = APContractSelectionEngine(
                broker=broker,        # execution broker -- gated by BOT_MODE
                data_broker=data_broker,  # live data broker -- quotes + chains only
                mode=self.mode.lower(),
                target_delta=float(os.getenv("TARGET_DELTA", "0.50")),  # ATM
                max_spread_pct=float(os.getenv("MAX_SPREAD_PCT", "0.50")),  # raised: 0.25→0.50 -- let wide spreads through
                min_oi=int(os.getenv("MIN_OI", "1")),          # lowered: 5→1
                min_volume=int(os.getenv("MIN_VOLUME", "0")),   # no floor
                max_dte=int(os.getenv("MAX_DTE", "21")),  # raised: 14→21 -- more expirations available
                earnings_guard=earnings_guard,
                iv_filter=iv_filter,
            )

            # ── Execution core (watcher + exit engine + fill monitor) ─────────
            self.core = APExecutionCore(
                broker=broker,
                supabase_client=sb,
                email=self.email,
                position_manager=self.position_manager,
                order_state_machine=self.order_state_machine,
                data_broker=data_broker,  # live api.tradier.com for exit engine quotes
                master_control=self.master_control,  # Fix: single decision authority per client
            )
            self.core.start()

            # Wire kill switch + mode into master control
            self.master_control.wire(
                kill_switch_fn=lambda: getattr(self.core, "_kill_switch", False),
                mode_fn=lambda: getattr(self.core, "mode", self.mode),
            )

            # Startup recovery: restore positions, verify pending orders, reseed dedup
            try:
                recovery = APStartupRecovery(
                    client_id=self.email,
                    broker=broker,
                    osm=self.order_state_machine,
                    pm=self.position_manager,
                    master_control=self.master_control,
                    exit_engine=getattr(self.core, "exit_eng", None),
                )
                rec_result = recovery.run()
                logger.info(
                    f"[{self.email}] Startup recovery complete: "
                    f"positions={rec_result['positions_recovered']} "
                    f"entries_corrected={rec_result['entries_corrected']} "
                    f"exits={rec_result['exits_reattached']} "
                    f"dedup={rec_result['dedup_seeded']}"
                )
            except Exception as _rec_err:
                logger.error(f"[{self.email}] Startup recovery error: {_rec_err}")
            # Reseed exit engine with open positions after restart
            try:
                _exit_eng = getattr(self.core, "exit_eng", None)
                if _exit_eng:
                    # Register with OSM so transition() calls mark_position_closed etc.
                    try:
                        from ap.order_state_machine import register_exit_engine
                        register_exit_engine(self.email, _exit_eng)
                        logger.info(f"[{self.email}] Exit engine registered with OSM")
                    except Exception as _reg_err:
                        logger.warning(f"[{self.email}] OSM registration: {_reg_err}")
                    if hasattr(_exit_eng, "seed_from_db"):
                        _exit_eng.seed_from_db(self.position_manager)
                        logger.info(f"[{self.email}] Exit engine reseeded from DB")
            except Exception as _seed_err:
                logger.warning(f"[{self.email}] Exit engine seed: {_seed_err}")

            # Broker reconciler: every 3 min, broker truth wins
            try:
                self.reconciler = APBrokerReconciler(
                    broker=broker,
                    client_id=self.email,
                    osm=self.order_state_machine,
                    pm=self.position_manager,
                )
                self.reconciler.start()
                logger.info(f"[{self.email}] Broker reconciler started")
            except Exception as _recon_err:
                logger.error(f"[{self.email}] Reconciler start error: {_recon_err}")
                self.reconciler = None

            # Pull live account equity so all % caps are per-client accurate
            self._sync_account_equity(broker)

            # Register with health monitor (singleton -- watches thread liveness)
            health_mon = get_monitor()
            if health_mon:
                health_mon.register(self)
                health_mon.clear_dashboard_alert(self.email)

            # Register with self-healing system (auto-restart + fast reconcile)
            healer = get_healer()
            if healer:
                healer.register(self)

            # Stale order monitor -- cancel/alert/escalate on timeout
            self.order_monitor = APOrderMonitor(
                client_id=self.email,
                broker=broker,
                order_state_machine=self.order_state_machine,
                position_manager=self.position_manager,
            )
            self.order_monitor.start()

            # Fill monitor -- polls broker and routes fills through OSM
            try:
                from ap.fill_monitor import fill_monitor_loop as _fml
                self.fill_monitor_thread = threading.Thread(
                    target=_fml,
                    kwargs={
                        "broker":       broker,
                        "poll_seconds": 10.0,
                        "osm":          self.order_state_machine,
                    },
                    daemon=True,
                    name=f"fill-monitor-{self.email}",
                )
                self.fill_monitor_thread.start()
                logger.info(f"[{self.email}] Fill monitor started (osm-wired)")
            except Exception as _fm_err:
                logger.error(f"[{self.email}] Fill monitor start error: {_fm_err}")

            # Periodic equity refresh in background (every 15 min)
            self._start_equity_refresh(broker)

            logger.info(
                f"[{self.email}] Control stack initialized | "
                f"mode={self.mode} "
                f"score_floor={os.getenv('SCORE_FLOOR','65')} "
                f"max_pos={_max_pos} "
                f"max_trades={_max_trades} "
                f"daily_loss=${_max_loss:.0f}"
            )

            # ── Queue worker -- NEW control path ───────────────────────────────
            # Runs in its own daemon thread, dispatches via full control stack
            self._start_worker_thread(broker)

            # Keep runner alive — heartbeat so self-healer sees progress
            _healer_ref = get_healer()
            while not self.stopped.wait(60):
                try:
                    if _healer_ref:
                        _healer_ref.heartbeat(self.email, "runner")
                except Exception:
                    pass

        except Exception as e:
            logger.error(f"[{self.email}] Runner crashed: {e}", exc_info=True)
        finally:
            if self.order_monitor:
                self.order_monitor.stop()
            if getattr(self, "reconciler", None):
                try:
                    self.reconciler.stop()
                except Exception:
                    pass
            if self.core:
                self.core.stop()
            # Unregister exit engine from OSM registry to prevent stale references
            try:
                from ap.order_state_machine import unregister_exit_engine
                unregister_exit_engine(self.email)
            except Exception:
                pass
            # Unregister from health monitor
            health_mon = get_monitor()
            if health_mon:
                health_mon.unregister(self.email)
            # Unregister from self-healing system
            healer = get_healer()
            if healer:
                healer.unregister(self.email)
            # Remove from active runner registry so supervisor replaces us promptly
            with _registry_lock:
                _active_runners.pop(self.email, None)
            logger.info(f"[{self.email}] ClientRunner stopped.")

    def stop(self):
        self.stopped.set()
        # Worker loop checks stop_event each poll cycle -- exits cleanly

    def _sync_account_equity(self, broker):
        """Pull live balance from broker and update master control."""
        try:
            balance = None
            # TradierBroker (ap.brokers.tradier) exposes get_account_equity()
            # Fall back to legacy method names for compatibility
            if hasattr(broker, "get_account_equity"):
                balance = broker.get_account_equity()
            elif hasattr(broker, "get_account_balance"):
                balance = broker.get_account_balance()
            elif hasattr(broker, "get_balances"):
                b = broker.get_balances()
                balance = (b.get("equity") or b.get("total_equity")
                           or b.get("net_liquidation") or b.get("cash"))
            if balance and float(balance) > 0:
                self.master_control.set_account_equity(float(balance))
                logger.info(f"[{self.email}] Live equity synced: ${float(balance):.2f}")
            else:
                logger.warning(
                    f"[{self.email}] Could not pull live equity -- "
                    f"using env default ${self.master_control.account_equity:.2f}"
                )
        except Exception as e:
            logger.warning(f"[{self.email}] Equity sync failed: {e} -- using env default")

    def _start_equity_refresh(self, broker):
        """Refresh account equity every 15 minutes.
        Also resets session dedup at 9:30 AM ET daily — prevents the same
        setup being silently blocked all day after a single earlier fill.
        """
        _reset_done_for_date: str = ""  # tracks which date we last reset on

        def _refresh_loop():
            nonlocal _reset_done_for_date
            _eq_healer = get_healer()
            while not self.stopped.wait(900):  # 15 min
                self._sync_account_equity(broker)
                # Heartbeat — must be < STALL_THRESHOLD_SEC so raise threshold in self-healer
                try:
                    if _eq_healer:
                        _eq_healer.heartbeat(self.email, "equity_refresh")
                except Exception:
                    pass

                # Daily session reset at 9:30 AM ET
                # Fires once per calendar date, within one 15-min cycle of open
                try:
                    now_et   = datetime.now(_ET)
                    today_str = now_et.strftime("%Y-%m-%d")
                    at_open   = (now_et.hour == 9 and now_et.minute >= 30) or now_et.hour == 10
                    if at_open and _reset_done_for_date != today_str:
                        if self.master_control and hasattr(self.master_control, "reset_session"):
                            self.master_control.reset_session(client_id=self.email)
                            logger.info(
                                f"[{self.email}] Session dedup reset at market open "
                                f"({now_et.strftime('%H:%M ET')})"
                            )
                        _reset_done_for_date = today_str
                except Exception as _re:
                    logger.debug(f"[{self.email}] Session reset check failed: {_re}")

        self.equity_thread = threading.Thread(
            target=_refresh_loop,
            daemon=True,
            name=f"equity-refresh-{self.email}",
        )
        self.equity_thread.start()

    def _start_worker_thread(self, broker):
        """
        Launch worker_loop in a daemon thread.
        Passes full control stack -- master control is the sole decision authority.
        """
        entry_watcher = getattr(self.core, "entry_watcher", None)

        is_live = self.mode == "LIVE"

        def _run_worker():
            try:
                worker_loop(
                    broker,
                    master_control=self.master_control,
                    contract_selector=self.contract_selector,
                    order_state_machine=self.order_state_machine,
                    entry_watcher=entry_watcher,
                    position_manager=self.position_manager,
                    client_id=self.email,
                    stop_event=self.stopped,
                    live_mode=is_live,
                    exit_eng=getattr(self.core, "exit_eng", None),
                )
            except Exception as e:
                logger.error(f"[{self.email}] worker_loop crashed: {e}", exc_info=True)

        self.worker_thread = threading.Thread(
            target=_run_worker,
            daemon=True,
            name=f"worker-{self.email}",
        )
        self.worker_thread.start()
        logger.info(f"[{self.email}] Queue subscriber started")


# =============================================================================
# SIGNAL ROUTING -- write to Postgres queue (gunicorn multi-worker safe)
# =============================================================================

def route_signal_to_all_clients(signal: dict):
    """
    Called by app.py /signal endpoint.
    Fan-out: enqueues one job per active client so each client's worker
    can claim and process it independently.

    Critical: client_id in trade_queue MUST match the client_id each
    worker polls for -- they are isolated per client.
    """
    signal_id = str(signal.get("signal_id") or uuid.uuid4())
    signal["signal_id"] = signal_id
    ticker = signal.get("ticker", "?")

    with _registry_lock:
        active_emails = list(_active_runners.keys())

    if not active_emails:
        # _active_runners is empty in this gunicorn worker -- the runner lives
        # in the other worker process. Fall back to cached Supabase members.
        # Cache for 60 seconds to avoid a DB hit on every signal during a burst.
        _now = _time_module.monotonic()
        with _registry_lock:
            _cached = _members_cache.get("emails")
        if _cached and _cached[1] > _now:
            active_emails = _cached[0]
            logger.debug(f"Signal {signal_id} [{ticker}] -- using cached member list ({len(active_emails)} clients)")
        else:
            logger.warning(
                f"Signal {signal_id} [{ticker}] -- no active runners in this worker, "
                f"falling back to Supabase members lookup"
            )
            try:
                sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
                members = _fetch_active_members(sb)
                active_emails = [m["email"] for m in members if m.get("email")]
                with _registry_lock:
                    _members_cache["emails"] = (active_emails, _now + 60.0)
            except Exception as e:
                logger.error(f"Supabase members fallback failed: {e}")
                active_emails = []

        if not active_emails:
            # Truly no members -- last resort fallback
            logger.error(f"Signal {signal_id} [{ticker}] -- no members found, dropping")
            return 0

    # Fan-out: one queue entry per active client
    enqueued = 0
    for email in active_emails:
        try:
            # Use signal_id:email as idempotency key -- prevents double-enqueue
            # if this function is called twice (e.g. two gunicorn workers)
            ok = enqueue_signal(
                signal,
                client_id=email,
                idempotency_key=f"{signal_id}:{email}",
            )
            if ok:
                enqueued += 1
                logger.info(f"Signal {signal_id} [{ticker}] → queued for {email}")
            else:
                logger.debug(f"Signal {signal_id} duplicate for {email} -- skipped")
        except Exception as e:
            logger.error(f"Failed to enqueue signal for {email}: {e}")

    logger.info(
        f"Signal {signal_id} [{ticker}] fan-out complete -- "
        f"{enqueued}/{len(active_emails)} clients queued"
    )
    return enqueued


# =============================================================================
# SUPERVISOR -- sync runners from Supabase members table
# =============================================================================

def _fetch_active_members(sb: Client) -> list[dict]:
    try:
        res = (
            sb.table("members")
            .select(
                "id,email,name,tradier_account_id,"
                "tradier_access_token,tradier_base_url,"
                "subscription_active,approved"
            )
            .eq("approved", True)
            .eq("subscription_active", True)
            .not_.is_("tradier_account_id", "null")
            .not_.is_("tradier_access_token", "null")
            .execute()
        )
        return res.data or []
    except Exception as e:
        logger.error(f"Supabase fetch failed: {e}")
        return []


def _sync_runners(sb: Client):
    members       = _fetch_active_members(sb)
    active_emails = {m["email"] for m in members}

    with _registry_lock:
        # Stop runners for removed/inactive members
        to_stop = [e for e in _active_runners if e not in active_emails]
        for email in to_stop:
            logger.info(f"Stopping runner for {email} -- no longer active")
            _active_runners[email].stop()
            del _active_runners[email]

        # Start runners for new/restarted members
        for member in members:
            email = member["email"]
            if email not in _active_runners or not _active_runners[email].is_alive():
                logger.info(f"Starting runner for {email}")
                runner = ClientRunner(member)
                runner.start()
                _active_runners[email] = runner


def start_multi_client_supervisor():
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.warning(
            "No Supabase credentials -- multi-client supervisor not starting"
        )
        return

    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    # Start worker health monitor singleton
    init_monitor(supabase_client=sb)
    logger.info("Worker health monitor initialized")

    # Start self-healing system (auto-restart + fast reconcile)
    init_self_healing(supabase_client=sb)
    logger.info("Self-healing system initialized")

    def _supervisor():
        logger.info("Multi-client supervisor started")
        while True:
            try:
                _sync_runners(sb)
            except Exception as e:
                logger.error(f"Supervisor sync error: {e}")
            time.sleep(300)

    t = threading.Thread(target=_supervisor, daemon=True, name="client-supervisor")
    t.start()
    logger.info("Client supervisor thread launched")


# =============================================================================
# STATUS
# =============================================================================

def get_runner_status() -> list[dict]:
    with _registry_lock:
        return [
            {
                "email":          email,
                "account_id":     r.account_id,
                "base_url":       r.base_url,
                "alive":          r.is_alive(),
                "core_active":    r.core is not None,
                "master_control": r.master_control is not None,
                "position_mgr":   r.position_manager is not None,
                "order_osm":      r.order_state_machine is not None,
                "contract_sel":   r.contract_selector is not None,
                "earnings_guard": r.contract_selector is not None,   # bundled in selector
                "iv_filter":      r.contract_selector is not None,
                "order_monitor":  r.order_monitor is not None,
                "worker_alive":   getattr(r, "worker_thread", None) is not None and r.worker_thread.is_alive(),
                "equity_alive":   getattr(r, "equity_thread", None) is not None and r.equity_thread.is_alive(),
                "mode":           getattr(r, "mode", "PAPER"),
            }
            for email, r in _active_runners.items()
        ]
