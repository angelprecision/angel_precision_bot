# client_runner.py -- Multi-client trading loop for Angel Precision Bot
# =============================================================================
# Each active member gets their own isolated trading thread with:
# - APMasterControl -- sole decision authority
# - APPositionManager -- Postgres-backed position truth
# - APOrderStateMachine -- enforced order lifecycle
# - APContractSelector -- real premium-based contract selection
# - APExecutionCore -- entry watcher, exit engine, signal tracker
# - fill_monitor_loop -- broker reconciliation, OSM-routed
# - worker_loop() -- queue dispatch through control stack
#
# Hardened lifecycle fixes included:
# 25. Runner registry cannot keep half-initialized runners forever.
# 26. Removed stop/delete race; runners are stopped, joined/observed, then removed.
# 27. Phantom order cleanup is age-gated and limited to truly orphaned orders.
# 28. Position sizer thresholds are per-client equity based by default.
# 29. Missing exit engine is fatal.
# 30. Missing/dead fill monitor is fatal.
# 31. Worker thread will not start without entry_watcher.
# 32. Runner owns final cleanup/unregistration; supervisor does not delete live threads prematurely.
# 33. Degraded-mode state freezes new entries without losing shutdown/exit visibility.
# 34. Startup manifest + control-stack validation required before initialized=True.
# 35. Runtime health loop continuously verifies worker/fill-monitor/core integrity.
# =============================================================================

from __future__ import annotations

import base64
import hashlib
import logging
import os
import threading
import time
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo

from cryptography.fernet import Fernet
from supabase import create_client, Client

from ap.db import run_with_retry
from ap.queue import enqueue_signal, worker_loop
from ap.order_monitor import APOrderMonitor
from ap.position_sizer import APPositionSizer
from ap.market_intelligence import APEarningsGuard, APIVRankFilter
from ap.worker_health import get_monitor, init_monitor
from ap_reconciler import APBrokerReconciler
from ap_recovery import APStartupRecovery
from ap.self_healing import get_healer, init_self_healing

logger = logging.getLogger("client_runner")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

_ET = ZoneInfo("America/New_York")
_time_module: object = time
_members_cache: dict = {}

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY", "")
_raw_key = os.getenv("ENCRYPTION_KEY", "").strip()


def _env_bool(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def decrypt_token(ciphertext: str) -> str:
    if not _raw_key:
        raise RuntimeError("ENCRYPTION_KEY env var is required and not set")
    key_bytes = hashlib.sha256(_raw_key.encode()).digest()
    fernet = Fernet(base64.urlsafe_b64encode(key_bytes))
    return fernet.decrypt(ciphertext.encode()).decode()


_active_runners: dict[str, "ClientRunner"] = {}
_registry_lock = threading.RLock()

# Broker reconciler is disabled by default because the rich fill monitor is
# already the primary broker-order reconciliation path.
ENABLE_BROKER_RECONCILER = _env_bool("ENABLE_BROKER_RECONCILER", "0")

# Cross-process fanout fallback can enqueue for members whose runner is not
# initialized in this worker. Keep it explicit so degraded-mode protection is
# not bypassed accidentally.
#
# Production default should stay OFF unless the receiving queue worker/master
# control performs its own client-health/degraded-state revalidation.
ALLOW_SUPABASE_FANOUT_FALLBACK = _env_bool("ALLOW_SUPABASE_FANOUT_FALLBACK", "0")


class ClientRunner(threading.Thread):
    """
    One daemon thread per client. The runner owns all per-client subsystems and
    cleans them up in run(). The supervisor may request stop, but final removal
    from _active_runners is runner-owned to avoid cleanup-order races.
    """

    def __init__(self, member: dict):
        super().__init__(daemon=True, name=f"runner-{member['email']}")
        self.member = member
        self.email = member["email"]
        self.account_id = member["tradier_account_id"]
        self.base_url = member.get("tradier_base_url", "https://sandbox.tradier.com")

        self.stopped = threading.Event()
        self.initialized = threading.Event()
        self.failed = threading.Event()
        self.stopping = threading.Event()
        self.degraded = threading.Event()
        self.entries_allowed = threading.Event()

        self.degraded_reasons: set[str] = set()
        self.startup_manifest: dict = {}

        self.last_health_check_ts = 0.0
        self.last_fill_monitor_heartbeat_ts = 0.0
        self.last_worker_heartbeat_ts = 0.0
        self.last_equity_heartbeat_ts = 0.0

        self.core = None
        self.master_control = None
        self.reconciler = None
        self.position_manager = None
        self.order_state_machine = None
        self.contract_selector = None
        self.order_monitor = None
        self.fill_monitor_thread = None
        self.worker_thread = None
        self.equity_thread = None
        self.health_thread = None
        self.mode = "PAPER"

    def _get_token(self) -> str | None:
        try:
            raw = self.member.get("tradier_access_token", "")
            if not raw:
                return None
            return decrypt_token(raw)
        except Exception as exc:
            logger.error("[%s] Token decrypt failed: %s", self.email, exc)
            return None

    def _mark_failed(self, reason: str):
        reason = str(reason or "failed")
        self.failed.set()
        self.degraded.set()
        self.entries_allowed.clear()
        self.degraded_reasons.add(reason)
        logger.error("[%s] Runner failed: %s", self.email, reason)

    def _enter_degraded_mode(self, reason: str, *, stop_runner: bool = False):
        reason = str(reason or "unknown_degraded_reason")
        self.degraded.set()
        self.entries_allowed.clear()
        self.degraded_reasons.add(reason)
        logger.error("[%s] ENTERING DEGRADED MODE: %s", self.email, reason)

        health_mon = get_monitor()
        if health_mon:
            try:
                if hasattr(health_mon, "raise_dashboard_alert"):
                    health_mon.raise_dashboard_alert(self.email, reason)
            except Exception:
                pass

        healer = get_healer()
        if healer:
            try:
                if hasattr(healer, "request_action"):
                    healer.request_action(self.email, "degraded", reason=reason)
            except Exception:
                pass

        if stop_runner:
            self.stopped.set()

    def _reason_key(self, reason: str) -> str:
        return str(reason or "").split(":", 1)[0]

    def _clear_degraded_reason_key(self, key: str):
        key = str(key or "")
        if not key:
            return
        self.degraded_reasons = {
            r for r in self.degraded_reasons
            if self._reason_key(r) != key
        }

    def _try_recover_degraded_mode(self):
        """
        Clear transient degraded state once the runtime stack is healthy again.

        Critical startup/control-stack failures remain sticky. Runtime worker/fill
        monitor failures are recoverable because those loops are self-restarting.
        """
        if self.failed.is_set() or self.stopping.is_set() or self.stopped.is_set():
            return False

        worker_alive = bool(self.worker_thread and self.worker_thread.is_alive())
        fill_alive = bool(self.fill_monitor_thread and self.fill_monitor_thread.is_alive())
        core_present = self.core is not None
        exit_present = getattr(self.core, "exit_eng", None) is not None if self.core else False

        if not (worker_alive and fill_alive and core_present and exit_present):
            return False

        recoverable = {
            "worker_dead",
            "fill_monitor_dead",
            "worker_loop_crashed",
            "worker_loop_returned",
            "fill_monitor_loop_crashed",
            "fill_monitor_loop_returned",
        }

        remaining = {
            r for r in self.degraded_reasons
            if self._reason_key(r) not in recoverable
        }

        self.degraded_reasons = remaining
        if not self.degraded_reasons:
            if self.degraded.is_set():
                logger.warning("[%s] RECOVERED from transient degraded mode", self.email)
            self.degraded.clear()
            self._set_entry_permission()
            return True

        return False

    def _set_entry_permission(self):
        ready = (
            self.is_alive()
            and self.initialized.is_set()
            and not self.failed.is_set()
            and not self.stopping.is_set()
            and not self.degraded.is_set()
            and self.core is not None
            and getattr(self.core, "exit_eng", None) is not None
            and self.worker_thread is not None
            and self.worker_thread.is_alive()
            and self.fill_monitor_thread is not None
            and self.fill_monitor_thread.is_alive()
        )
        if ready:
            self.entries_allowed.set()
        else:
            self.entries_allowed.clear()
        return ready

    def _build_startup_manifest(
        self,
        *,
        equity: float,
        max_trades: int,
        max_pos: int,
        max_loss: float,
        throttle_threshold: float,
        stop_threshold: float,
        data_broker_is_dedicated: bool,
        exit_eng,
    ):
        self.startup_manifest = {
            "client_id": self.email,
            "account_id": self.account_id,
            "mode": self.mode,
            "base_url": self.base_url,
            "core_present": self.core is not None,
            "exit_engine_present": exit_eng is not None,
            "entry_watcher_present": getattr(self.core, "entry_watcher", None) is not None if self.core else False,
            "position_manager_present": self.position_manager is not None,
            "order_state_machine_present": self.order_state_machine is not None,
            "contract_selector_present": self.contract_selector is not None,
            "order_monitor_present": self.order_monitor is not None,
            "fill_monitor_alive": bool(self.fill_monitor_thread and self.fill_monitor_thread.is_alive()),
            "worker_alive": bool(self.worker_thread and self.worker_thread.is_alive()),
            "equity_alive": bool(self.equity_thread and self.equity_thread.is_alive()),
            "reconciler_enabled": bool(self.reconciler is not None),
            "data_broker_mode": "dedicated" if data_broker_is_dedicated else "shared_execution_broker",
            "equity": float(equity),
            "max_trades": int(max_trades),
            "max_positions": int(max_pos),
            "max_daily_loss": float(max_loss),
            "throttle_threshold": float(throttle_threshold),
            "stop_threshold": float(stop_threshold),
            "score_floor": float(os.getenv("SCORE_FLOOR", "65")),
            "context_floor": float(os.getenv("CONTEXT_FLOOR", "0.0")),
            "max_capital_pct": float(os.getenv("MAX_CAPITAL_PCT", "0.40")),
            "max_sector_pct": float(os.getenv("MAX_SECTOR_PCT", "0.25")),
            "max_ticker_pct": float(os.getenv("MAX_TICKER_PCT", "0.10")),
        }
        logger.info("[%s] Startup manifest: %s", self.email, self.startup_manifest)

    def _validate_control_stack(self):
        problems = []

        if self.core is None:
            problems.append("core_missing")
        if self.master_control is None:
            problems.append("master_control_missing")
        if self.position_manager is None:
            problems.append("position_manager_missing")
        if self.order_state_machine is None:
            problems.append("order_state_machine_missing")
        if self.contract_selector is None:
            problems.append("contract_selector_missing")
        if self.order_monitor is None:
            problems.append("order_monitor_missing")

        exit_eng = getattr(self.core, "exit_eng", None) if self.core else None
        entry_watcher = getattr(self.core, "entry_watcher", None) if self.core else None

        if exit_eng is None:
            problems.append("exit_engine_missing")
        if entry_watcher is None:
            problems.append("entry_watcher_missing")
        if self.fill_monitor_thread is None or not self.fill_monitor_thread.is_alive():
            problems.append("fill_monitor_not_alive")
        if self.worker_thread is None or not self.worker_thread.is_alive():
            problems.append("worker_thread_not_alive")

        if problems:
            reason = "control_stack_validation_failed:" + ",".join(problems)
            self._mark_failed(reason)
            self.stopped.set()
            raise RuntimeError(f"[{self.email}] {reason}")

        logger.info("[%s] Control stack validation PASSED", self.email)
        return True

    def _assert_worker_alive(self):
        time.sleep(float(os.getenv("WORKER_START_GRACE_SEC", "0.5")))
        if not self.worker_thread or not self.worker_thread.is_alive():
            self._mark_failed("worker_thread_failed_to_start")
            self.stopped.set()
            raise RuntimeError(f"[{self.email}] worker_thread failed to start")

    def _start_runtime_health_loop(self):
        interval = float(os.getenv("RUNNER_HEALTH_CHECK_SEC", "20"))

        def _health_loop():
            healer_ref = get_healer()
            while not self.stopped.wait(interval):
                now = time.time()
                self.last_health_check_ts = now

                worker_alive = bool(self.worker_thread and self.worker_thread.is_alive())
                fill_alive = bool(self.fill_monitor_thread and self.fill_monitor_thread.is_alive())
                equity_alive = bool(self.equity_thread and self.equity_thread.is_alive())
                core_present = self.core is not None
                exit_present = getattr(self.core, "exit_eng", None) is not None if self.core else False

                if worker_alive:
                    self.last_worker_heartbeat_ts = now
                if fill_alive:
                    self.last_fill_monitor_heartbeat_ts = now
                if equity_alive:
                    self.last_equity_heartbeat_ts = now

                if not core_present:
                    self._enter_degraded_mode("core_missing_runtime", stop_runner=True)
                    break
                if not exit_present:
                    self._enter_degraded_mode("exit_engine_missing_runtime", stop_runner=True)
                    break
                if not fill_alive:
                    self._enter_degraded_mode("fill_monitor_dead", stop_runner=False)
                else:
                    self._clear_degraded_reason_key("fill_monitor_dead")

                if not worker_alive:
                    self._enter_degraded_mode("worker_dead", stop_runner=False)
                else:
                    self._clear_degraded_reason_key("worker_dead")

                self._try_recover_degraded_mode()
                self._set_entry_permission()

                try:
                    if healer_ref:
                        healer_ref.heartbeat(self.email, "runner_health")
                except Exception:
                    pass

        self.health_thread = threading.Thread(
            target=_health_loop,
            daemon=True,
            name=f"runner-health-{self.email}",
        )
        self.health_thread.start()
        logger.info("[%s] Runtime health loop started", self.email)

    def _validate_execution_core_started(self):
        """
        Fail fast immediately after APExecutionCore.start().

        This catches the exact class of bug where the core object exists but
        watcher/exit/tracker did not actually initialize or start correctly.
        """
        problems = []

        if self.core is None:
            problems.append("core_missing")
            self._mark_failed("execution_core_start_validation_failed:" + ",".join(problems))
            self.stopped.set()
            raise RuntimeError(f"[{self.email}] execution core validation failed: {problems}")

        entry_watcher = getattr(self.core, "entry_watcher", None)
        exit_eng = getattr(self.core, "exit_eng", None)
        tracker = getattr(self.core, "tracker", None)

        if entry_watcher is None:
            problems.append("entry_watcher_missing")
        if exit_eng is None:
            problems.append("exit_engine_missing")
        if tracker is None:
            problems.append("tracker_missing")

        # Thread liveness checks are intentionally conditional because exact
        # attribute names can vary across versions. If a component exposes a
        # thread handle and it is already dead immediately after start(), fail.
        thread_checks = [
            ("entry_watcher_thread_dead", getattr(entry_watcher, "_thread", None) if entry_watcher else None),
            ("exit_engine_thread_dead", getattr(exit_eng, "_thread", None) if exit_eng else None),
            ("tracker_thread_dead", getattr(tracker, "_thread", None) if tracker else None),
        ]
        for reason, thread in thread_checks:
            if thread is not None and not thread.is_alive():
                problems.append(reason)

        # Some watcher/tracker implementations use alternate names.
        alt_thread_checks = [
            ("entry_watcher_loop_dead", getattr(entry_watcher, "thread", None) if entry_watcher else None),
            ("tracker_loop_dead", getattr(tracker, "thread", None) if tracker else None),
        ]
        for reason, thread in alt_thread_checks:
            if thread is not None and not thread.is_alive():
                problems.append(reason)

        if problems:
            reason = "execution_core_start_validation_failed:" + ",".join(problems)
            self._mark_failed(reason)
            self.stopped.set()
            raise RuntimeError(f"[{self.email}] {reason}")

        logger.info("[%s] Execution core startup validation PASSED", self.email)
        return True

    def run(self):
        try:
            self._run_inner()
        except Exception as exc:
            self._mark_failed(str(exc))
            logger.error("[%s] Runner crashed", self.email, exc_info=True)
        finally:
            self._cleanup()
            with _registry_lock:
                current = _active_runners.get(self.email)
                if current is self:
                    _active_runners.pop(self.email, None)
            logger.info("[%s] ClientRunner stopped.", self.email)

    def _run_inner(self):
        logger.info("[%s] ClientRunner starting -- account %s @ %s", self.email, self.account_id, self.base_url)

        token = self._get_token()
        if not token:
            self._mark_failed("no_token")
            return

        if "sandbox" in self.base_url.lower():
            self.mode = "PAPER"
        elif os.getenv("AP_MODE", "paper").upper() == "LIVE":
            self.mode = "LIVE"
        else:
            self.mode = "PAPER"
        logger.info("[%s] Client mode: %s (from tradier_base_url)", self.email, self.mode)

        if self.mode == "LIVE":
            missing = []
            if not token:
                missing.append("member.tradier_access_token")
            if not str(self.account_id or "").strip():
                missing.append("member.tradier_account_id")
            if not os.getenv("DATABASE_URL", "").strip():
                missing.append("DATABASE_URL")
            if missing:
                self._mark_failed("LIVE startup missing: " + ", ".join(missing))
                return
            logger.info("[%s] LIVE mode assertions PASSED | account_id=%s base_url=%s", self.email, self.account_id, self.base_url)

        try:
            from ap.brokers.tradier import TradierBroker, TradierConfig
            from ap_execution_core import APExecutionCore
            from ap_master_control import APMasterControl
            from ap.position_manager import APPositionManager
            from ap.order_state_machine import APOrderStateMachine
            from ap.contract_selector import APContractSelectionEngine
        except Exception as exc:
            self._mark_failed(f"import_failed:{exc}")
            return

        try:
            from ap.db import ensure_client_exists
            ensure_client_exists(self.email, equity=float(os.getenv("ACCOUNT_EQUITY", "25000")))
            logger.info("[%s] DB rows provisioned", self.email)
        except Exception as exc:
            self._mark_failed(f"ensure_client_exists_failed:{exc}")
            return

        broker_cfg = TradierConfig(base_url=self.base_url, access_token=token, account_id=self.account_id)
        broker = TradierBroker(broker_cfg)

        self._clear_old_phantom_orders()
        sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY) if SUPABASE_URL else None

        self.position_manager = APPositionManager(client_id=self.email)

        client_cfg = self._load_client_config()
        equity = float(client_cfg.get("initial_equity", os.getenv("ACCOUNT_EQUITY", "25000")) or 25000)
        max_trades = int(client_cfg.get("max_trades_per_day", os.getenv("MAX_TRADES_TODAY", "10")) or 10)
        max_pos = int(client_cfg.get("max_concurrent_positions", os.getenv("MAX_POSITIONS", "10")) or 10)
        loss_pct = float(client_cfg.get("daily_max_loss_pct", 0.06) or 0.06)
        max_loss = -abs(equity * loss_pct)

        throttle_pct = float(os.getenv("THROTTLE_THRESHOLD_PCT", "0.02"))
        stop_pct = float(os.getenv("STOP_THRESHOLD_PCT", "0.05"))
        throttle_threshold = -abs(float(os.getenv("THROTTLE_THRESHOLD", str(equity * throttle_pct))))
        stop_threshold = -abs(float(os.getenv("STOP_THRESHOLD", str(equity * stop_pct))))

        position_sizer = APPositionSizer(
            throttle_threshold=throttle_threshold,
            stop_threshold=stop_threshold,
            throttle_factor=float(os.getenv("THROTTLE_FACTOR", "0.5")),
            min_history=int(os.getenv("KELLY_MIN_HISTORY", "20")),
        )

        logger.info(
            "[%s] Client limits | equity=$%.2f max_trades=%s max_pos=%s daily_loss=$%.0f sizer(throttle=$%.0f stop=$%.0f)",
            self.email, equity, max_trades, max_pos, max_loss, throttle_threshold, stop_threshold,
        )

        self.master_control = APMasterControl(
            mode=self.mode.lower(),
            client_id=self.email,
            score_floor=float(os.getenv("SCORE_FLOOR", "65")),
            context_floor=float(os.getenv("CONTEXT_FLOOR", "0.0")),
            max_positions=max_pos,
            max_capital_pct=float(os.getenv("MAX_CAPITAL_PCT", "0.40")),
            max_sector_pct=float(os.getenv("MAX_SECTOR_PCT", "0.25")),
            max_ticker_pct=float(os.getenv("MAX_TICKER_PCT", "0.10")),
            max_calls=int(os.getenv("MAX_CALLS", "10")),
            max_puts=int(os.getenv("MAX_PUTS", "10")),
            max_trades_today=max_trades,
            max_daily_loss=max_loss,
            account_equity=equity,
            position_manager=self.position_manager,
            position_sizer=position_sizer,
            supabase_client=sb,
        )

        self.order_state_machine = APOrderStateMachine(client_id=self.email)

        data_token = os.getenv("TRADIER_DATA_TOKEN", "").strip()
        data_base_url = os.getenv("TRADIER_DATA_BASE_URL", "https://api.tradier.com").strip()
        if data_token:
            data_broker_cfg = TradierConfig(base_url=data_base_url, access_token=data_token, account_id=self.account_id)
            data_broker = TradierBroker(data_broker_cfg)
            logger.info("[%s] Live data broker initialized | %s", self.email, data_base_url)
        else:
            data_broker = broker
            logger.warning("[%s] TRADIER_DATA_TOKEN not set -- using execution broker for data", self.email)

        earnings_guard = APEarningsGuard(
            broker=data_broker,
            blackout_days=int(os.getenv("EARNINGS_BLACKOUT_DAYS", "3")),
        )
        iv_filter = APIVRankFilter(
            broker=data_broker,
            max_iv_rank=float(os.getenv("MAX_IV_RANK", "100")),
            hard_cap=float(os.getenv("IV_HARD_CAP", "150")),
            mode=os.getenv("BOT_MODE", "PAPER").upper(),
        )

        self.contract_selector = APContractSelectionEngine(
            broker=broker,
            data_broker=data_broker,
            mode=self.mode.lower(),
            target_delta=float(os.getenv("TARGET_DELTA", "0.50")),
            max_spread_pct=float(os.getenv("MAX_SPREAD_PCT", "0.50")),
            min_oi=int(os.getenv("MIN_OI", "1")),
            min_volume=int(os.getenv("MIN_VOLUME", "0")),
            max_dte=int(os.getenv("MAX_DTE", "21")),
            earnings_guard=earnings_guard,
            iv_filter=iv_filter,
        )

        self.core = APExecutionCore(
            broker=broker,
            supabase_client=sb,
            email=self.email,
            position_manager=self.position_manager,
            order_state_machine=self.order_state_machine,
            data_broker=data_broker,
            master_control=self.master_control,
        )
        self.core.start()
        self._validate_execution_core_started()

        exit_eng = getattr(self.core, "exit_eng", None)
        if exit_eng is None:
            self._mark_failed("exit_engine_missing")
            self.stopped.set()
            return

        self.master_control.wire(
            kill_switch_fn=lambda: getattr(self.core, "_kill_switch", False),
            mode_fn=lambda: getattr(self.core, "mode", self.mode),
        )

        self._register_exit_engine(exit_eng)
        self._run_startup_recovery(broker, exit_eng)
        self._seed_exit_engine_from_db(exit_eng)
        self._start_reconciler(broker, exit_eng)
        self._sync_account_equity(broker)

        health_mon = get_monitor()
        if health_mon:
            health_mon.register(self)
            health_mon.clear_dashboard_alert(self.email)

        healer = get_healer()
        if healer:
            healer.register(self)

        self.order_monitor = APOrderMonitor(
            client_id=self.email,
            broker=broker,
            order_state_machine=self.order_state_machine,
            position_manager=self.position_manager,
            exit_engine=exit_eng,
        )
        self.order_monitor.start()

        self._start_fill_monitor(broker, exit_eng)
        self._assert_fill_monitor_alive()

        self._start_equity_refresh(broker)
        self._start_worker_thread(broker)
        self._assert_worker_alive()

        self._build_startup_manifest(
            equity=equity,
            max_trades=max_trades,
            max_pos=max_pos,
            max_loss=max_loss,
            throttle_threshold=throttle_threshold,
            stop_threshold=stop_threshold,
            data_broker_is_dedicated=bool(data_token),
            exit_eng=exit_eng,
        )

        self._validate_control_stack()

        self.initialized.set()
        self._set_entry_permission()
        self._start_runtime_health_loop()

        logger.info(
            "[%s] Control stack initialized | mode=%s max_pos=%s max_trades=%s daily_loss=$%.0f entries_allowed=%s",
            self.email, self.mode, max_pos, max_trades, max_loss, self.entries_allowed.is_set(),
        )

        healer_ref = get_healer()
        while not self.stopped.wait(60):
            try:
                if healer_ref:
                    healer_ref.heartbeat(self.email, "runner")
            except Exception:
                pass
            self._try_recover_degraded_mode()
            self._set_entry_permission()

    def stop(self):
        self.stopping.set()
        self.entries_allowed.clear()
        self.stopped.set()

    def _cleanup(self):
        self.entries_allowed.clear()
        self.degraded.set()

        if self.order_monitor:
            try:
                self.order_monitor.stop()
            except Exception:
                pass
        if self.reconciler:
            try:
                self.reconciler.stop()
            except Exception:
                pass
        if self.core:
            try:
                self.core.stop()
            except Exception:
                pass

        # Join child threads briefly after stop signals to prevent zombie overlap.
        self._join_child_threads()

        try:
            from ap.order_state_machine import unregister_exit_engine
            unregister_exit_engine(self.email)
        except Exception:
            pass

        health_mon = get_monitor()
        if health_mon:
            try:
                health_mon.unregister(self.email)
            except Exception:
                pass

        healer = get_healer()
        if healer:
            try:
                healer.unregister(self.email)
            except Exception:
                pass

    def _join_child_threads(self):
        timeout = float(os.getenv("RUNNER_CHILD_JOIN_TIMEOUT_SEC", "2"))
        current = threading.current_thread()
        for name in ("fill_monitor_thread", "worker_thread", "equity_thread", "health_thread"):
            thread = getattr(self, name, None)
            if thread and thread is not current and thread.is_alive():
                try:
                    thread.join(timeout=timeout)
                    if thread.is_alive():
                        logger.warning("[%s] %s still alive after %.1fs join", self.email, name, timeout)
                except RuntimeError:
                    pass

    def _clear_old_phantom_orders(self):
        try:
            from ap.db import conn as _conn
            min_age = int(os.getenv("PHANTOM_ORDER_MIN_AGE_MINUTES", "5"))

            def _clear_phantoms():
                with _conn() as c:
                    c.execute(
                        """
                        UPDATE orders
                        SET status = %s,
                            last_error = %s,
                            updated_ts = NOW()
                        WHERE client_id = %s
                          AND status IN ('CREATED', 'SUBMITTED', 'ACKNOWLEDGED')
                          AND created_ts < NOW() - (%s || ' minutes')::interval
                          AND (
                                broker_order_id IS NULL
                             OR TRIM(COALESCE(broker_order_id, '')) = ''
                             OR UPPER(TRIM(COALESCE(broker_order_id, ''))) IN ('N/A', 'NA', 'NONE', 'NULL')
                          )
                        """,
                        ("CANCELED", "startup_phantom_clear", self.email, str(min_age)),
                    )
                    return c.rowcount

            n = run_with_retry(_clear_phantoms)
            if n:
                logger.info("[%s] Startup: cleared %s old phantom orders", self.email, n)
        except Exception as exc:
            logger.warning("[%s] Startup phantom clear: %s", self.email, exc)

    def _load_client_config(self) -> dict:
        try:
            from ap.db import conn as _conn

            def _load_cfg():
                with _conn() as c:
                    c.execute(
                        "SELECT max_trades_per_day, max_concurrent_positions, daily_max_loss_pct, initial_equity "
                        "FROM clients WHERE client_id=%s",
                        (self.email,),
                    )
                    return c.fetchone()

            return run_with_retry(_load_cfg) or {}
        except Exception as exc:
            logger.warning("[%s] Could not load client config: %s", self.email, exc)
            return {}

    def _run_startup_recovery(self, broker, exit_eng):
        try:
            recovery = APStartupRecovery(
                client_id=self.email,
                broker=broker,
                osm=self.order_state_machine,
                pm=self.position_manager,
                master_control=self.master_control,
                exit_engine=exit_eng,
            )
            rec_result = recovery.run()
            logger.info(
                "[%s] Startup recovery complete: positions=%s entries_corrected=%s exits=%s dedup=%s",
                self.email,
                rec_result.get("positions_recovered"),
                rec_result.get("entries_corrected"),
                rec_result.get("exits_reattached"),
                rec_result.get("dedup_seeded"),
            )
        except Exception as exc:
            logger.error("[%s] Startup recovery error: %s", self.email, exc)

    def _register_exit_engine(self, exit_eng):
        try:
            from ap.order_state_machine import register_exit_engine
            register_exit_engine(self.email, exit_eng)
            logger.info("[%s] Exit engine registered with OSM", self.email)
        except Exception as exc:
            logger.warning("[%s] Exit engine registration error: %s", self.email, exc)

    def _seed_exit_engine_from_db(self, exit_eng):
        try:
            if exit_eng and hasattr(exit_eng, "seed_from_db"):
                exit_eng.seed_from_db(self.position_manager)
                logger.info("[%s] Exit engine reseeded from DB", self.email)
        except Exception as exc:
            logger.warning("[%s] Exit engine DB seed error: %s", self.email, exc)

    def _start_reconciler(self, broker, exit_eng):
        if not ENABLE_BROKER_RECONCILER:
            logger.info("[%s] Broker reconciler disabled (ENABLE_BROKER_RECONCILER=0)", self.email)
            self.reconciler = None
            return

        try:
            self.reconciler = APBrokerReconciler(
                broker=broker,
                client_id=self.email,
                osm=self.order_state_machine,
                pm=self.position_manager,
            )
            self.reconciler.exit_engine = exit_eng
            self.reconciler.start()
            logger.info("[%s] Broker reconciler started", self.email)
        except Exception as exc:
            logger.error("[%s] Reconciler start error: %s", self.email, exc)
            self.reconciler = None

    def _start_fill_monitor(self, broker, exit_eng):
        from ap.fill_monitor import fill_monitor_loop

        restart_sleep = float(os.getenv("FILL_MONITOR_RESTART_SLEEP_SEC", "2"))

        def _run_fill_monitor():
            while not self.stopped.is_set():
                try:
                    fill_monitor_loop(
                        broker=broker,
                        poll_seconds=10.0,
                        osm=self.order_state_machine,
                        pm=self.position_manager,
                        exit_engine=exit_eng,
                        stop_event=self.stopped,
                        client_id=self.email,
                    )
                    if self.stopped.is_set():
                        break
                    self._enter_degraded_mode("fill_monitor_loop_returned", stop_runner=False)
                    logger.warning("[%s] fill_monitor_loop returned unexpectedly -- restarting in %.1fs", self.email, restart_sleep)
                    time.sleep(restart_sleep)
                except Exception as exc:
                    if self.stopped.is_set():
                        break
                    self._enter_degraded_mode(f"fill_monitor_loop_crashed:{exc}", stop_runner=False)
                    logger.error("[%s] fill_monitor_loop crashed: %s -- restarting in %.1fs", self.email, exc, restart_sleep, exc_info=True)
                    time.sleep(restart_sleep)

        self.fill_monitor_thread = threading.Thread(
            target=_run_fill_monitor,
            daemon=True,
            name=f"fill-monitor-{self.email}",
        )
        self.fill_monitor_thread.start()
        logger.info("[%s] Fill monitor thread launched", self.email)

    def _assert_fill_monitor_alive(self):
        time.sleep(float(os.getenv("FILL_MONITOR_START_GRACE_SEC", "0.5")))
        if not self.fill_monitor_thread or not self.fill_monitor_thread.is_alive():
            self._mark_failed("fill_monitor_failed_to_start")
            self.stopped.set()
            raise RuntimeError(f"[{self.email}] Fill monitor failed to start")

    def _sync_account_equity(self, broker):
        try:
            balance = None
            if hasattr(broker, "get_account_equity"):
                balance = broker.get_account_equity()
            elif hasattr(broker, "get_account_balance"):
                balance = broker.get_account_balance()
            elif hasattr(broker, "get_balances"):
                b = broker.get_balances()
                balance = b.get("equity") or b.get("total_equity") or b.get("net_liquidation") or b.get("cash")
            if balance and float(balance) > 0:
                self.master_control.set_account_equity(float(balance))
                logger.info("[%s] Live equity synced: $%.2f", self.email, float(balance))
            else:
                logger.warning("[%s] Could not pull live equity -- using default $%.2f", self.email, self.master_control.account_equity)
        except Exception as exc:
            logger.warning("[%s] Equity sync failed: %s -- using default", self.email, exc)

    def _start_equity_refresh(self, broker):
        reset_done_for_date = ""

        def _refresh_loop():
            nonlocal reset_done_for_date
            healer_ref = get_healer()
            while not self.stopped.wait(900):
                self._sync_account_equity(broker)
                self.last_equity_heartbeat_ts = time.time()
                try:
                    if healer_ref:
                        healer_ref.heartbeat(self.email, "equity_refresh")
                except Exception:
                    pass

                try:
                    now_et = datetime.now(_ET)
                    today_str = now_et.strftime("%Y-%m-%d")
                    at_open = (now_et.hour == 9 and now_et.minute >= 30) or now_et.hour == 10
                    if at_open and reset_done_for_date != today_str:
                        if self.master_control and hasattr(self.master_control, "reset_session"):
                            self.master_control.reset_session(client_id=self.email)
                        logger.info("[%s] Session dedup reset at market open (%s)", self.email, now_et.strftime("%H:%M ET"))
                        reset_done_for_date = today_str
                except Exception as exc:
                    logger.debug("[%s] Session reset check failed: %s", self.email, exc)

        self.equity_thread = threading.Thread(target=_refresh_loop, daemon=True, name=f"equity-refresh-{self.email}")
        self.equity_thread.start()

    def _start_worker_thread(self, broker):
        entry_watcher = getattr(self.core, "entry_watcher", None)
        if entry_watcher is None:
            self._mark_failed("entry_watcher_missing")
            self.stopped.set()
            raise RuntimeError(f"[{self.email}] ENTRY WATCHER MISSING — worker startup aborted")

        is_live = self.mode == "LIVE"

        def _run_worker():
            restart_sleep = float(os.getenv("WORKER_RESTART_SLEEP_SEC", "2"))
            while not self.stopped.is_set():
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
                    if self.stopped.is_set():
                        break
                    self._enter_degraded_mode("worker_loop_returned", stop_runner=False)
                    logger.warning("[%s] worker_loop returned unexpectedly -- restarting in %.1fs", self.email, restart_sleep)
                    time.sleep(restart_sleep)
                except Exception as exc:
                    if self.stopped.is_set():
                        break
                    self._enter_degraded_mode(f"worker_loop_crashed:{exc}", stop_runner=False)
                    logger.error("[%s] worker_loop crashed: %s -- restarting in %.1fs", self.email, exc, restart_sleep, exc_info=True)
                    time.sleep(restart_sleep)

        self.worker_thread = threading.Thread(target=_run_worker, daemon=True, name=f"worker-{self.email}")
        self.worker_thread.start()
        if not self.worker_thread.is_alive():
            self._mark_failed("worker_thread_failed_to_start")
            self.stopped.set()
            raise RuntimeError(f"[{self.email}] worker_thread failed to start")

        logger.info("[%s] Queue subscriber started", self.email)


def route_signal_to_all_clients(signal: dict):
    signal_id = str(signal.get("signal_id") or uuid.uuid4())
    signal["signal_id"] = signal_id
    ticker = signal.get("ticker", "?")

    with _registry_lock:
        active_emails = [
            email
            for email, runner in _active_runners.items()
            if (
                runner.is_alive()
                and runner.initialized.is_set()
                and not runner.stopping.is_set()
                and not runner.failed.is_set()
                and not runner.degraded.is_set()
                and runner.entries_allowed.is_set()
            )
        ]

    if not active_emails:
        if not ALLOW_SUPABASE_FANOUT_FALLBACK:
            logger.warning(
                "Signal %s [%s] -- no local entries-allowed runners and Supabase fallback disabled; dropping",
                signal_id,
                ticker,
            )
            return 0

        now = _time_module.monotonic()
        with _registry_lock:
            cached = _members_cache.get("emails")
        if cached and cached[1] > now:
            active_emails = cached[0]
            logger.debug("Signal %s [%s] -- using cached member list (%s clients)", signal_id, ticker, len(active_emails))
        else:
            logger.warning(
                "Signal %s [%s] -- no initialized local runners, using explicit Supabase fallback",
                signal_id,
                ticker,
            )
            try:
                sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
                members = _fetch_active_members(sb)
                active_emails = [m["email"] for m in members if m.get("email")]
                with _registry_lock:
                    _members_cache["emails"] = (active_emails, now + 60.0)
            except Exception as exc:
                logger.error("Supabase members fallback failed: %s", exc)
                active_emails = []

    if not active_emails:
        logger.error("Signal %s [%s] -- no members found, dropping", signal_id, ticker)
        return 0

    enqueued = 0
    for email in active_emails:
        try:
            ok = enqueue_signal(signal, client_id=email, idempotency_key=f"{signal_id}:{email}")
            if ok:
                enqueued += 1
                logger.info("Signal %s [%s] -> queued for %s", signal_id, ticker, email)
            else:
                logger.debug("Signal %s duplicate for %s -- skipped", signal_id, email)
        except Exception as exc:
            logger.error("Failed to enqueue signal for %s: %s", email, exc)

    logger.info("Signal %s [%s] fan-out complete -- %s/%s clients queued", signal_id, ticker, enqueued, len(active_emails))
    return enqueued


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
    except Exception as exc:
        logger.error("Supabase fetch failed: %s", exc)
        return []


def _sync_runners(sb: Client):
    members = _fetch_active_members(sb)
    active_emails = {m["email"] for m in members}
    to_join: list[ClientRunner] = []

    with _registry_lock:
        for email, runner in list(_active_runners.items()):
            if email not in active_emails:
                if not runner.stopping.is_set():
                    logger.info("Stopping runner for %s -- no longer active", email)
                    runner.stop()
                if not runner.is_alive():
                    _active_runners.pop(email, None)
                else:
                    to_join.append(runner)

        for email, runner in list(_active_runners.items()):
            if not runner.is_alive():
                logger.warning("Removing dead runner from registry: %s", email)
                _active_runners.pop(email, None)

        for member in members:
            email = member["email"]
            existing = _active_runners.get(email)
            if existing and existing.is_alive():
                continue
            logger.info("Starting runner for %s", email)
            runner = ClientRunner(member)
            _active_runners[email] = runner
            runner.start()

    join_timeout = float(os.getenv("RUNNER_STOP_JOIN_TIMEOUT_SEC", "10"))
    for runner in to_join:
        runner.join(timeout=join_timeout)
        if not runner.is_alive():
            with _registry_lock:
                if _active_runners.get(runner.email) is runner:
                    _active_runners.pop(runner.email, None)
        else:
            logger.warning("[%s] Runner still stopping after %.1fs; will retry next sync", runner.email, join_timeout)


def start_multi_client_supervisor():
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        logger.warning("No Supabase credentials -- multi-client supervisor not starting")
        return

    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    init_monitor(supabase_client=sb)
    logger.info("Worker health monitor initialized")
    init_self_healing(supabase_client=sb)
    logger.info("Self-healing system initialized")

    def _supervisor():
        logger.info("Multi-client supervisor started")
        while True:
            try:
                _sync_runners(sb)
            except Exception as exc:
                logger.error("Supervisor sync error: %s", exc)
            time.sleep(300)

    thread = threading.Thread(target=_supervisor, daemon=True, name="client-supervisor")
    thread.start()
    logger.info("Client supervisor thread launched")


def get_runner_status() -> list[dict]:
    with _registry_lock:
        return [
            {
                "email": email,
                "account_id": r.account_id,
                "base_url": r.base_url,
                "alive": r.is_alive(),
                "initialized": r.initialized.is_set(),
                "failed": r.failed.is_set(),
                "stopping": r.stopping.is_set(),
                "degraded": r.degraded.is_set(),
                "entries_allowed": r.entries_allowed.is_set(),
                "degraded_reasons": sorted(list(getattr(r, "degraded_reasons", set()))),
                "startup_manifest_present": bool(getattr(r, "startup_manifest", {})),
                "last_health_check_ts": getattr(r, "last_health_check_ts", 0.0),
                "last_fill_monitor_heartbeat_ts": getattr(r, "last_fill_monitor_heartbeat_ts", 0.0),
                "last_worker_heartbeat_ts": getattr(r, "last_worker_heartbeat_ts", 0.0),
                "last_equity_heartbeat_ts": getattr(r, "last_equity_heartbeat_ts", 0.0),
                "core_active": r.core is not None,
                "master_control": r.master_control is not None,
                "position_mgr": r.position_manager is not None,
                "order_osm": r.order_state_machine is not None,
                "contract_sel": r.contract_selector is not None,
                "earnings_guard": r.contract_selector is not None,
                "iv_filter": r.contract_selector is not None,
                "order_monitor": r.order_monitor is not None,
                "worker_alive": getattr(r, "worker_thread", None) is not None and r.worker_thread.is_alive(),
                "fill_monitor_alive": getattr(r, "fill_monitor_thread", None) is not None and r.fill_monitor_thread.is_alive(),
                "equity_alive": getattr(r, "equity_thread", None) is not None and r.equity_thread.is_alive(),
                "mode": getattr(r, "mode", "PAPER"),
                "ready_for_entries": (
                    r.is_alive()
                    and r.initialized.is_set()
                    and not r.failed.is_set()
                    and not r.stopping.is_set()
                    and not r.degraded.is_set()
                    and r.entries_allowed.is_set()
                ),
            }
            for email, r in _active_runners.items()
        ]
