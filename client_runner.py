# client_runner.py — Multi-client trading loop for Angel Precision Bot
# =============================================================================
# Each active member gets their own isolated trading thread with:
#   - APMasterControl    — sole decision authority
#   - APPositionManager  — Postgres-backed position truth
#   - APOrderStateMachine— enforced order lifecycle
#   - APContractSelector — real premium-based contract selection
#   - APExecutionCore    — entry watcher, exit engine, fill monitor
#   - worker_loop()      — new control path queue dispatch
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

from cryptography.fernet import Fernet
from supabase import create_client, Client

from ap.db import conn as ap_conn, run_with_retry
from ap.queue import enqueue_signal, worker_loop
from ap.order_monitor import APOrderMonitor
from ap.position_sizer import APPositionSizer
from ap.worker_health import get_monitor, init_monitor
from ap.self_healing import get_healer, init_self_healing
from ap.utils import now_utc_iso

logger = logging.getLogger("client_runner")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_URL         = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")

# ── Encryption ────────────────────────────────────────────────────────────────
_raw_key   = os.getenv("ENCRYPTION_KEY", "angel-precision-encrypt-2026")
_key_bytes = hashlib.sha256(_raw_key.encode()).digest()
_fernet    = Fernet(base64.urlsafe_b64encode(_key_bytes))

def decrypt_token(ciphertext: str) -> str:
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

        # Control stack — set during run()
        self.core              = None
        self.master_control    = None
        self.position_manager  = None
        self.order_state_machine = None
        self.contract_selector = None
        self.order_monitor     = None

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
            f"[{self.email}] ClientRunner starting — "
            f"account {self.account_id} @ {self.base_url}"
        )
        token = self._get_token()
        if not token:
            logger.error(f"[{self.email}] No token — aborting runner")
            return

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
            # ── Broker ───────────────────────────────────────────────────────
            broker_cfg = TradierConfig(
                base_url=self.base_url,
                access_token=token,
                account_id=self.account_id,
            )
            broker = TradierBroker(broker_cfg)
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

            self.master_control = APMasterControl(
                mode=os.getenv("AP_MODE", "paper"),
                score_floor=float(os.getenv("SCORE_FLOOR", "60")),
                context_floor=float(os.getenv("CONTEXT_FLOOR", "6.0")),
                max_positions=int(os.getenv("MAX_POSITIONS", "7")),
                max_capital_pct=float(os.getenv("MAX_CAPITAL_PCT", "0.40")),
                max_sector_pct=float(os.getenv("MAX_SECTOR_PCT", "0.25")),
                max_ticker_pct=float(os.getenv("MAX_TICKER_PCT", "0.10")),
                max_calls=int(os.getenv("MAX_CALLS", "5")),
                max_puts=int(os.getenv("MAX_PUTS", "5")),
                max_trades_today=int(os.getenv("MAX_TRADES_TODAY", "10")),
                max_daily_loss=float(os.getenv("MAX_DAILY_LOSS", "-500")),
                account_equity=float(os.getenv("ACCOUNT_EQUITY", "25000")),
                position_manager=self.position_manager,
                position_sizer=position_sizer,
                supabase_client=sb,
            )

            self.order_state_machine = APOrderStateMachine(client_id=self.email)

            self.contract_selector = APContractSelectionEngine(
                broker=broker,
                mode=os.getenv("AP_MODE", "paper"),
                target_delta=float(os.getenv("TARGET_DELTA", "0.40")),
                max_spread_pct=float(os.getenv("MAX_SPREAD_PCT", "0.20")),
                min_oi=int(os.getenv("MIN_OI", "50")),
                min_volume=int(os.getenv("MIN_VOLUME", "10")),
                max_dte=int(os.getenv("MAX_DTE", "21")),
            )

            # ── Execution core (watcher + exit engine + fill monitor) ─────────
            self.core = APExecutionCore(
                broker=broker,
                supabase_client=sb,
                email=self.email,
                position_manager=self.position_manager,
                order_state_machine=self.order_state_machine,
            )
            self.core.start()

            # Wire kill switch + mode into master control
            self.master_control.wire(
                kill_switch_fn=lambda: getattr(self.core, "_kill_switch", False),
                mode_fn=lambda: getattr(self.core, "mode", "PAPER"),
            )

            # Pull live account equity so all % caps are per-client accurate
            self._sync_account_equity(broker)

            # Register with health monitor (singleton — watches thread liveness)
            health_mon = get_monitor()
            if health_mon:
                health_mon.register(self)
                health_mon.clear_dashboard_alert(self.email)

            # Register with self-healing system (auto-restart + fast reconcile)
            healer = get_healer()
            if healer:
                healer.register(self)

            # Stale order monitor — cancel/alert/escalate on timeout
            self.order_monitor = APOrderMonitor(
                client_id=self.email,
                broker=broker,
                order_state_machine=self.order_state_machine,
                position_manager=self.position_manager,
            )
            self.order_monitor.start()

            # Periodic equity refresh in background (every 15 min)
            self._start_equity_refresh(broker)

            logger.info(
                f"[{self.email}] Control stack initialized | "
                f"mode={os.getenv('AP_MODE','paper').upper()} "
                f"score_floor={os.getenv('SCORE_FLOOR','60')} "
                f"max_pos={os.getenv('MAX_POSITIONS','7')}"
            )

            # ── Queue worker — NEW control path ───────────────────────────────
            # Runs in its own daemon thread, dispatches via full control stack
            self._start_worker_thread(broker)

            # Keep runner alive
            while not self.stopped.wait(60):
                pass

        except Exception as e:
            logger.error(f"[{self.email}] Runner crashed: {e}", exc_info=True)
        finally:
            if self.order_monitor:
                self.order_monitor.stop()
            if self.core:
                self.core.stop()
            # Unregister from health monitor
            health_mon = get_monitor()
            if health_mon:
                health_mon.unregister(self.email)
            # Unregister from self-healing system
            healer = get_healer()
            if healer:
                healer.unregister(self.email)
            logger.info(f"[{self.email}] ClientRunner stopped.")

    def stop(self):
        self.stopped.set()
        # Worker loop checks stop_event each poll cycle — exits cleanly

    def _sync_account_equity(self, broker):
        """Pull live balance from broker and update master control."""
        try:
            balance = None
            if hasattr(broker, "get_account_balance"):
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
                    f"[{self.email}] Could not pull live equity — "
                    f"using env default ${self.master_control.account_equity:.2f}"
                )
        except Exception as e:
            logger.warning(f"[{self.email}] Equity sync failed: {e} — using env default")

    def _start_equity_refresh(self, broker):
        """Refresh account equity every 15 minutes in background."""
        def _refresh_loop():
            while not self.stopped.wait(900):  # 15 min
                self._sync_account_equity(broker)
        t = threading.Thread(
            target=_refresh_loop,
            daemon=True,
            name=f"equity-refresh-{self.email}",
        )
        t.start()

    def _start_worker_thread(self, broker):
        """
        Launch worker_loop in a daemon thread.
        Passes full control stack — master control is the sole decision authority.
        """
        entry_watcher = getattr(self.core, "entry_watcher", None)

        is_live = os.getenv("AP_MODE", "paper").upper() == "LIVE"

        def _run_worker():
            try:
                worker_loop(
                    broker,
                    master_control=self.master_control,
                    contract_selector=self.contract_selector,
                    order_state_machine=self.order_state_machine,
                    entry_watcher=entry_watcher,
                    client_id=self.email,
                    stop_event=self.stopped,    # clean shutdown when runner stops
                    live_mode=is_live,           # disables legacy fallback in live
                )
            except Exception as e:
                logger.error(f"[{self.email}] worker_loop crashed: {e}", exc_info=True)

        t = threading.Thread(
            target=_run_worker,
            daemon=True,
            name=f"worker-{self.email}",
        )
        t.start()
        logger.info(f"[{self.email}] Queue subscriber started")


# =============================================================================
# SIGNAL ROUTING — write to Postgres queue (gunicorn multi-worker safe)
# =============================================================================

def route_signal_to_all_clients(signal: dict):
    """
    Called by app.py /signal endpoint.
    Fan-out: enqueues one job per active client so each client's worker
    can claim and process it independently.

    Critical: client_id in trade_queue MUST match the client_id each
    worker polls for — they are isolated per client.
    """
    signal_id = str(signal.get("signal_id") or uuid.uuid4())
    signal["signal_id"] = signal_id
    ticker = signal.get("ticker", "?")

    with _registry_lock:
        active_emails = list(_active_runners.keys())

    if not active_emails:
        # No runners yet — fallback to "default" so signal isn't lost
        logger.warning(
            f"Signal {signal_id} [{ticker}] — no active runners, "
            f"enqueuing to default"
        )
        try:
            enqueue_signal(signal, client_id="default")
        except Exception as e:
            logger.error(f"Failed to enqueue signal to default: {e}")
        return

    # Fan-out: one queue entry per active client
    enqueued = 0
    for email in active_emails:
        try:
            # Use signal_id:email as idempotency key — prevents double-enqueue
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
                logger.debug(f"Signal {signal_id} duplicate for {email} — skipped")
        except Exception as e:
            logger.error(f"Failed to enqueue signal for {email}: {e}")

    logger.info(
        f"Signal {signal_id} [{ticker}] fan-out complete — "
        f"{enqueued}/{len(active_emails)} clients queued"
    )


# =============================================================================
# SUPERVISOR — sync runners from Supabase members table
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
            logger.info(f"Stopping runner for {email} — no longer active")
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
            "No Supabase credentials — multi-client supervisor not starting"
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
                "order_monitor":  r.order_monitor is not None,
            }
            for email, r in _active_runners.items()
        ]
