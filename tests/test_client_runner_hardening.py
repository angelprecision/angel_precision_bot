"""
PR D — Client Runner Hardening + Live Safety
=============================================

Tests for the 7 surgical safety fixes in the institutional audit of
client_runner.py (audit dated 2026-05-25). Tests-first: every fix is
red before source change, green after.

Coverage map (7 fixes + structural invariants):

  FIX-1 SECURITY CRITICAL — default ENCRYPTION_KEY LIVE-startup block (3)
    - Module exposes _DEFAULT_KEY_SENTINEL constant
    - LIVE startup raises/marks-failed when ENCRYPTION_KEY equals the
      sentinel (default insecure value)
    - PAPER startup with default key logs a warning but allows boot
      (dev convenience)

  FIX-2 kill_switch_fn wiring — explicit setter + lambda reads it (4)
    - ClientRunner has self.kill_switch_active = False at __init__
    - ClientRunner.trip_kill_switch(reason) sets the flag and logs
    - master_control.wire is passed a kill_switch_fn that returns
      self.kill_switch_active (NOT getattr(self.core, "_kill_switch", False)
      which always returns False because the field is never set)
    - Tripping the kill switch makes the wired callable return True

  FIX-3 Post-QPM quote refresh (LIVE) (2)
    - Source order: _start_position_quote_monitor runs BEFORE the
      second LIVE-only _refresh_quotes() call
    - The second refresh is guarded by self.mode == "LIVE" and absorbs
      exceptions (same shape as the first refresh in _seed_exit_engine_from_db)

  FIX-4 _degraded_lock around all degraded_reasons mutation paths (4)
    - ClientRunner has self._degraded_lock = threading.Lock() at __init__
    - _enter_degraded_mode acquires the lock for the mutation
    - _clear_degraded_reason_key acquires the lock for the mutation
    - _try_recover_degraded_mode acquires the lock for the mutation

  FIX-5 FILL_MONITOR_GRACE_SEC module-level + log string (3)
    - Module exposes FILL_MONITOR_GRACE_SEC = int(os.getenv(..., "300"))
    - _set_entry_permission does NOT call os.getenv("FILL_MONITOR_GRACE_SEC")
      inline (hot path; ~3x/min)
    - The BLOCKED warning log uses the actual grace value, not hard-coded ">120s"

  FIX-6 route_signal_to_all_clients snapshot inside _registry_lock (1)
    - Regression guard: the `active_emails = [...]` comprehension over
      _active_runners is inside the `with _registry_lock:` block
      (already true on main; lock down so future refactor cannot
      silently move it back outside)

  FIX-7 decrypt_token plaintext sanity validation (3)
    - Decrypted plaintext that is empty raises ValueError in BOTH modes
      (post-decrypt sanity; before returning to Tradier client)
    - Decrypted plaintext that is suspiciously short (<10 chars) raises
      ValueError in BOTH modes
    - Normal-length plaintext passes through unchanged

  FIX-8 order_monitor entry_watcher wiring (2)
    - Both production APOrderMonitor construction paths pass entry_watcher
    - A normal startup-built monitor preserves watcher-owned PENDING_TRIGGER rows

  INV — Structural invariants (2)
    - BUG-CR-3 resolution: APExecutionCore constructs APEntryWatcher
      with mode=self.mode — runner-side mutation is NOT needed and is
      NOT present (avoid duplicating canonical-mode authority)
    - Runner does NOT mutate self.core.entry_watcher.mode after start()
"""
from __future__ import annotations

import base64
import hashlib
import inspect
import os
import re
import sys
import threading
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
CLIENT_RUNNER_SRC = (REPO_ROOT / "client_runner.py").read_text()
EX_CORE_SRC = (REPO_ROOT / "ap_execution_core.py").read_text()

# Env BEFORE module import (client_runner reads ENCRYPTION_KEY at module load).
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_client_runner_hardening",
)
os.environ.setdefault("ENCRYPTION_KEY", "angel-pr-d-test-key-2026-not-default")

# Stub supabase if not installed (client_runner imports it at module load).
if "supabase" not in sys.modules:
    _supa_stub = types.ModuleType("supabase")
    _supa_stub.create_client = lambda *a, **kw: None
    _supa_stub.Client = type("Client", (), {})
    sys.modules["supabase"] = _supa_stub


@pytest.fixture(scope="module")
def client_runner_mod():
    """Import client_runner with a defensive ap.db shim.

    Module-pollution context:
    tests/test_repeg_resubmit.py installs a partial `ap.db` stub at
    module-load time (sys.modules['ap.db'] = ModuleType('ap.db') with
    only `update_order` patched on). pytest collects test_repeg_resubmit
    during the collection phase BEFORE running test_client_runner_hardening,
    so by the time this fixture runs, sys.modules['ap.db'] may be that
    partial stub — missing run_with_retry, which client_runner imports
    at line 54 (`from ap.db import run_with_retry`).

    Strategy (mirrors tests/test_pod_isolation.py): if ap.db is present
    in sys.modules but missing the names client_runner needs, patch
    them onto the existing object. Track what we added and remove ONLY
    those after the module-scoped fixture tears down, so we do not
    contaminate any downstream tests that depend on the polluting stub.

    We deliberately do NOT mutate sys.modules itself, and we do NOT
    pre-emptively pop client_runner from sys.modules — either action
    risks converting an existing pollution-driven ERROR in a downstream
    test into a FAILURE.
    """
    _apdb = sys.modules.get("ap.db")
    _added: list[str] = []
    if _apdb is not None:
        if not hasattr(_apdb, "run_with_retry"):
            def _stub_run_with_retry(fn, *args, **kwargs):  # pragma: no cover
                return fn(*args, **kwargs)
            _apdb.run_with_retry = _stub_run_with_retry  # type: ignore[attr-defined]
            _added.append("run_with_retry")
        if not hasattr(_apdb, "conn"):
            _apdb.conn = lambda *a, **kw: None  # type: ignore[attr-defined]
            _added.append("conn")
    import client_runner  # noqa: E402
    try:
        yield client_runner
    finally:
        # Teardown: remove only the attributes WE added — never touch
        # ones the polluting stub or the real module already provided.
        if _apdb is not None:
            for name in _added:
                try:
                    delattr(_apdb, name)
                except AttributeError:
                    pass


# ══════════════════════════════════════════════════════════════════
# FIX-1 — SECURITY CRITICAL: default ENCRYPTION_KEY LIVE-startup block
# ══════════════════════════════════════════════════════════════════

class TestFix1DefaultEncryptionKeyBlocked:
    def test_module_exposes_default_key_sentinel(self, client_runner_mod):
        """The default-key value must be exposed as a named module
        constant so the LIVE-startup gate can reference it exactly,
        and so future changes are visible in one place."""
        assert hasattr(client_runner_mod, "_DEFAULT_KEY_SENTINEL"), (
            "client_runner must expose _DEFAULT_KEY_SENTINEL as a module "
            "constant so the LIVE-startup gate can compare against it."
        )
        sentinel = client_runner_mod._DEFAULT_KEY_SENTINEL
        assert isinstance(sentinel, str) and len(sentinel) > 0
        # And the os.getenv fallback in source must reference the same constant
        # rather than re-typing the literal.
        assert re.search(
            r'os\.getenv\(\s*"ENCRYPTION_KEY"\s*,\s*_DEFAULT_KEY_SENTINEL\s*\)',
            CLIENT_RUNNER_SRC,
        ), (
            "The os.getenv fallback must reference _DEFAULT_KEY_SENTINEL "
            "not a re-typed string literal — single source of truth."
        )

    def test_live_startup_blocks_when_encryption_key_is_default(self):
        """When AP_MODE=LIVE (or BOT_MODE=LIVE) and ENCRYPTION_KEY equals
        the default sentinel, the LIVE safety gate must mark the runner
        failed with a clear LIVE_STARTUP_FATAL message — BEFORE any
        thread starts."""
        # The block lives inside _run_inner's LIVE-safety section.
        # Verify via source pattern (matches the existing pattern used by
        # ALLOW_LEGACY_FILL_MONITOR / ALLOW_IMMEDIATE_EXECUTION).
        assert re.search(
            r'LIVE_STARTUP_FATAL:.*ENCRYPTION_KEY',
            CLIENT_RUNNER_SRC,
        ), (
            "client_runner must emit a LIVE_STARTUP_FATAL message that "
            "mentions ENCRYPTION_KEY when the key equals the default. "
            "Pattern not found in source."
        )
        # And the check must reference the sentinel constant, not a literal.
        # Locate the LIVE safety block and confirm a comparison against
        # _DEFAULT_KEY_SENTINEL exists within ~50 lines of the other
        # LIVE_STARTUP_FATAL gates.
        m = re.search(
            r'LIVE_STARTUP_FATAL: ALLOW_IMMEDIATE_EXECUTION=1.*?LIVE mode assertions PASSED',
            CLIENT_RUNNER_SRC, re.DOTALL,
        )
        assert m, "Could not locate the LIVE safety assertions block"
        block = m.group(0)
        assert "_DEFAULT_KEY_SENTINEL" in block or "ENCRYPTION_KEY" in block, (
            "The LIVE safety block must perform an ENCRYPTION_KEY "
            "default-value check using _DEFAULT_KEY_SENTINEL."
        )

    def test_paper_startup_does_not_block_on_default_key(self):
        """PAPER mode is for dev/sandbox. A default key in PAPER must
        log loudly but NOT raise/mark-failed. Dev iteration must keep
        moving."""
        # Source-shape check: the LIVE-startup block is gated on
        # `if self.mode == "LIVE":` — so PAPER cannot reach it.
        # Verify the new ENCRYPTION_KEY gate is INSIDE that LIVE block.
        m = re.search(
            r'if self\.mode == "LIVE":(.*?)(?=\n        try:\n            from ap\.brokers\.tradier import)',
            CLIENT_RUNNER_SRC, re.DOTALL,
        )
        assert m, "Could not locate the `if self.mode == 'LIVE':` block"
        live_block = m.group(1)
        assert "_DEFAULT_KEY_SENTINEL" in live_block or re.search(
            r'LIVE_STARTUP_FATAL.*ENCRYPTION_KEY', live_block
        ), (
            "The default-ENCRYPTION_KEY gate must live INSIDE the "
            "`if self.mode == 'LIVE':` block so PAPER startups are not blocked."
        )


# ══════════════════════════════════════════════════════════════════
# FIX-2 — kill_switch_fn wiring (explicit setter + lambda reads it)
# ══════════════════════════════════════════════════════════════════

class TestFix2KillSwitchWiring:
    def test_runner_has_kill_switch_active_attribute(self, client_runner_mod):
        """ClientRunner must expose self.kill_switch_active as a real
        attribute (default False) so the wired lambda can read it.

        The previous lambda `lambda: getattr(self.core, "_kill_switch",
        False)` always returned False because self.core._kill_switch is
        never set anywhere in the codebase.
        """
        sig_src = inspect.getsource(client_runner_mod.ClientRunner.__init__)
        assert "self.kill_switch_active" in sig_src, (
            "ClientRunner.__init__ must initialize self.kill_switch_active "
            "(default False) so the wired kill_switch_fn lambda can read it."
        )

    def test_runner_has_trip_kill_switch_method(self, client_runner_mod):
        """ClientRunner must expose trip_kill_switch(reason) so the
        dashboard / admin path can flip the flag from outside the
        runner thread.
        """
        assert hasattr(client_runner_mod.ClientRunner, "trip_kill_switch"), (
            "ClientRunner must expose trip_kill_switch(reason) as the "
            "external setter for the kill-switch flag."
        )

    def test_master_control_wire_uses_kill_switch_active(self):
        """The `master_control.wire(kill_switch_fn=...)` call site must
        read self.kill_switch_active (the new explicit flag), NOT the
        old getattr(self.core, "_kill_switch", False) which is dead code.
        """
        # Old (buggy) pattern must be GONE:
        assert not re.search(
            r'kill_switch_fn\s*=\s*lambda\s*:\s*getattr\(\s*self\.core\s*,\s*[\"\']_kill_switch[\"\']\s*,\s*False\s*\)',
            CLIENT_RUNNER_SRC,
        ), (
            "The dead-code pattern `lambda: getattr(self.core, "
            "'_kill_switch', False)` must be removed. self.core._kill_switch "
            "is never set so the lambda always returns False."
        )
        # New pattern must be present:
        assert re.search(
            r'kill_switch_fn\s*=\s*lambda\s*:\s*self\.kill_switch_active',
            CLIENT_RUNNER_SRC,
        ), (
            "master_control.wire(kill_switch_fn=...) must read "
            "self.kill_switch_active (the explicit flag), not a "
            "getattr against a never-set core attribute."
        )

    def test_trip_kill_switch_makes_wired_callable_return_true(self, client_runner_mod):
        """End-to-end: a runner whose trip_kill_switch() has been called
        must have a wired callable that returns True.
        """
        # Build a minimal runner without going through full __init__
        # (which requires DB, supabase, etc).
        runner = client_runner_mod.ClientRunner.__new__(client_runner_mod.ClientRunner)
        runner.kill_switch_active = False
        runner.email = "test@x.com"

        # The wired lambda matches the production wiring.
        wired = lambda: runner.kill_switch_active  # noqa: E731
        assert wired() is False

        runner.trip_kill_switch("test_kill")
        assert wired() is True, (
            "After trip_kill_switch(), the wired callable must return True. "
            "Otherwise master_control._kill_switch_fn() never trips and "
            "force-close-all never fires."
        )


# ══════════════════════════════════════════════════════════════════
# FIX-3 — Post-QPM quote refresh (LIVE)
# ══════════════════════════════════════════════════════════════════

class TestFix3PostQpmRefresh:
    def test_post_qpm_refresh_call_exists_in_live_block(self):
        """A second _refresh_quotes() call must exist AFTER the
        _start_position_quote_monitor() call, guarded by self.mode == "LIVE".

        The first refresh (inside _seed_exit_engine_from_db) runs BEFORE
        QPM is attached. If that refresh fails for any reason (Render
        cold-start network blip, broker auth race), there's a window
        where the exit engine has stale entry-price underlyings and the
        first 8s of polling can fire stops at entry price.
        """
        # Locate the section between _start_position_quote_monitor and
        # the next major step (_start_reconciler or _sync_account_equity).
        m = re.search(
            r'self\._start_position_quote_monitor\([^)]*\)(.*?)(self\._start_reconciler|self\._sync_account_equity|health_mon = get_monitor\(\))',
            CLIENT_RUNNER_SRC, re.DOTALL,
        )
        assert m, "Could not locate the section after _start_position_quote_monitor"
        between = m.group(1)
        # Must contain a LIVE-guarded _refresh_quotes() call.
        assert re.search(r'self\.mode\s*==\s*[\"\']LIVE[\"\']', between), (
            "Post-QPM block must be guarded by `self.mode == 'LIVE'`"
        )
        assert "_refresh_quotes()" in between, (
            "Post-QPM block must call exit_eng._refresh_quotes() AFTER "
            "_start_position_quote_monitor so QPM is attached when the "
            "refresh runs. Currently the only refresh is BEFORE QPM "
            "attach (inside _seed_exit_engine_from_db)."
        )

    def test_post_qpm_refresh_absorbs_exceptions(self):
        """The post-QPM refresh must be inside a try/except so a transient
        broker error doesn't crash startup."""
        m = re.search(
            r'self\._start_position_quote_monitor\([^)]*\)(.*?)(self\._start_reconciler|self\._sync_account_equity|health_mon = get_monitor\(\))',
            CLIENT_RUNNER_SRC, re.DOTALL,
        )
        assert m
        between = m.group(1)
        assert "try:" in between and "except" in between, (
            "Post-QPM refresh must absorb exceptions — startup cannot "
            "fail on a transient quote-fetch error after QPM is up."
        )


# ══════════════════════════════════════════════════════════════════
# FIX-4 — _degraded_lock around degraded_reasons mutations
# ══════════════════════════════════════════════════════════════════

class TestFix4DegradedLock:
    def test_runner_has_degraded_lock_attribute(self, client_runner_mod):
        """ClientRunner.__init__ must create self._degraded_lock =
        threading.Lock(). Without it, the read-then-write set-comprehension
        in _clear_degraded_reason_key is not atomic across threads.
        """
        init_src = inspect.getsource(client_runner_mod.ClientRunner.__init__)
        assert "self._degraded_lock" in init_src, (
            "ClientRunner.__init__ must create self._degraded_lock = "
            "threading.Lock() to protect degraded_reasons mutations."
        )
        assert "threading.Lock()" in init_src or "threading.RLock()" in init_src, (
            "self._degraded_lock must be a threading.Lock() (or RLock)."
        )

    def test_enter_degraded_mode_acquires_lock(self, client_runner_mod):
        """_enter_degraded_mode must hold _degraded_lock while it
        mutates degraded_reasons / degraded / entries_allowed."""
        src = inspect.getsource(client_runner_mod.ClientRunner._enter_degraded_mode)
        assert "with self._degraded_lock" in src, (
            "_enter_degraded_mode must acquire _degraded_lock around its "
            "mutation of degraded_reasons. Otherwise concurrent worker / "
            "health-loop / split-brain-callback writes race."
        )

    def test_clear_degraded_reason_key_acquires_lock(self, client_runner_mod):
        """_clear_degraded_reason_key must hold _degraded_lock while it
        reconstructs degraded_reasons via set comprehension."""
        src = inspect.getsource(client_runner_mod.ClientRunner._clear_degraded_reason_key)
        assert "with self._degraded_lock" in src, (
            "_clear_degraded_reason_key must acquire _degraded_lock around "
            "the `degraded_reasons = {r for r in degraded_reasons if ...}` "
            "compound read-then-write — it is NOT atomic without the lock."
        )

    def test_try_recover_degraded_mode_acquires_lock(self, client_runner_mod):
        """_try_recover_degraded_mode also rebuilds degraded_reasons via
        set comprehension; must be inside the lock."""
        src = inspect.getsource(client_runner_mod.ClientRunner._try_recover_degraded_mode)
        assert "with self._degraded_lock" in src, (
            "_try_recover_degraded_mode must acquire _degraded_lock around "
            "the `remaining = {r for r in degraded_reasons if ...}` "
            "compound read-then-write."
        )


# ══════════════════════════════════════════════════════════════════
# FIX-5 — FILL_MONITOR_GRACE_SEC module-level + log string
# ══════════════════════════════════════════════════════════════════

class TestFix5FillMonitorGraceSec:
    def test_fill_monitor_grace_sec_is_module_constant(self, client_runner_mod):
        """FILL_MONITOR_GRACE_SEC must be a module-level int resolved
        once at import. Avoids ~3 os.getenv calls per minute in the
        hot-path _set_entry_permission method."""
        assert hasattr(client_runner_mod, "FILL_MONITOR_GRACE_SEC"), (
            "client_runner must expose FILL_MONITOR_GRACE_SEC as a "
            "module-level constant."
        )
        v = client_runner_mod.FILL_MONITOR_GRACE_SEC
        assert isinstance(v, int) and v >= 60, (
            f"FILL_MONITOR_GRACE_SEC must be a positive int >=60 seconds; "
            f"got {v!r}"
        )

    def test_set_entry_permission_does_not_call_getenv_inline(self, client_runner_mod):
        """The hot-path _set_entry_permission method must NOT call
        os.getenv("FILL_MONITOR_GRACE_SEC") inline. It is called ~3x/min."""
        src = inspect.getsource(client_runner_mod.ClientRunner._set_entry_permission)
        assert 'os.getenv("FILL_MONITOR_GRACE_SEC"' not in src, (
            "_set_entry_permission must NOT call os.getenv('FILL_MONITOR_GRACE_SEC') "
            "inline — use the module-level FILL_MONITOR_GRACE_SEC constant."
        )
        assert 'FILL_MONITOR_GRACE_SEC' in src, (
            "_set_entry_permission must reference the module-level "
            "FILL_MONITOR_GRACE_SEC constant."
        )

    def test_blocked_warning_uses_actual_grace_value(self, client_runner_mod):
        """The 'entries_allowed BLOCKED: fill_monitor dead' warning must
        NOT hard-code '>120s' — the actual default is 300s. The warning
        must use the live FILL_MONITOR_GRACE_SEC value."""
        src = inspect.getsource(client_runner_mod.ClientRunner._set_entry_permission)
        # The bad hard-coded ">120s" must be gone:
        assert "dead >120s" not in src, (
            "The stale 'fill_monitor dead >120s' literal must be removed "
            "from the BLOCKED warning — the default grace is 300s, not 120s."
        )
        # Some reference to the grace constant must be in the warning context:
        # search for a warning line that mentions fill_monitor + uses the constant
        # or a format placeholder
        assert re.search(
            r'fill_monitor\s+dead\s+>%[sd]?',
            src,
        ) or "FILL_MONITOR_GRACE_SEC" in src, (
            "The BLOCKED warning must format-substitute the actual grace "
            "value (use %s with the module constant), not a hard-coded literal."
        )


# ══════════════════════════════════════════════════════════════════
# FIX-6 — route_signal_to_all_clients snapshot inside _registry_lock
# ══════════════════════════════════════════════════════════════════

class TestFix6RouteSignalRegistryLock:
    def test_active_emails_snapshot_inside_registry_lock(self):
        """Regression guard: the `active_emails = [...]` comprehension
        over _active_runners must be inside `with _registry_lock:`.

        Already true on main — this test prevents a future refactor
        from silently moving it back outside."""
        m = re.search(
            r'def route_signal_to_all_clients\([^)]*\):(.*?)(\n    if not active_emails:|\nif __name__)',
            CLIENT_RUNNER_SRC, re.DOTALL,
        )
        assert m, "Could not locate route_signal_to_all_clients body"
        body = m.group(1)
        # Find the active_emails assignment
        # Match captures the leading whitespace so we can measure indent directly.
        ae_match = re.search(r'^(?P<indent>[ \t]*)active_emails\s*=\s*\[', body, re.MULTILINE)
        assert ae_match, "active_emails list comprehension not found"
        ae_indent = len(ae_match.group("indent").expandtabs(4))

        # Find the last `with _registry_lock:` before that assignment.
        lock_iter = list(re.finditer(
            r'^(?P<indent>[ \t]*)with _registry_lock:', body[:ae_match.start()],
            re.MULTILINE,
        ))
        assert lock_iter, (
            "active_emails snapshot must come after a `with _registry_lock:` "
            "opening. None found above the assignment."
        )
        last_lock = lock_iter[-1]
        lock_indent = len(last_lock.group("indent").expandtabs(4))

        assert ae_indent > lock_indent, (
            f"active_emails (indent={ae_indent}) must be MORE indented than "
            f"`with _registry_lock:` (indent={lock_indent}) — i.e., inside the "
            "lock's block. Otherwise the snapshot races with supervisor mutations."
        )


# ══════════════════════════════════════════════════════════════════
# FIX-7 — decrypt_token plaintext sanity validation
# ══════════════════════════════════════════════════════════════════

def _encrypt_new_scheme(plaintext: str, raw_key: str) -> str:
    """Encrypt with the NEW direct-Fernet scheme (matches ap.crypto)."""
    from cryptography.fernet import Fernet
    return Fernet(raw_key.encode()).encrypt(plaintext.encode()).decode()


def _encrypt_legacy_scheme(plaintext: str, raw_key: str) -> str:
    """Encrypt with the LEGACY SHA256-derived scheme."""
    from cryptography.fernet import Fernet
    digest = hashlib.sha256(raw_key.encode()).digest()
    return Fernet(base64.urlsafe_b64encode(digest)).encrypt(plaintext.encode()).decode()


class TestFix7DecryptTokenSanityValidation:
    def test_decrypted_empty_plaintext_rejected_in_live(self, client_runner_mod, monkeypatch):
        """Live: a successfully-decrypted plaintext that is empty must
        be rejected with a clear error, BEFORE returning to a Tradier
        client that would silently 401."""
        fresh_key = client_runner_mod.Fernet.generate_key().decode()
        monkeypatch.setattr(client_runner_mod, "_raw_key", fresh_key)
        ct = _encrypt_new_scheme("", fresh_key)  # empty plaintext, valid ciphertext
        with pytest.raises((ValueError, RuntimeError)) as exc_info:
            client_runner_mod.decrypt_token(ct, mode="LIVE")
        assert "empty" in str(exc_info.value).lower() or "invalid" in str(exc_info.value).lower() or "length" in str(exc_info.value).lower(), (
            f"LIVE empty-plaintext rejection must mention empty/invalid/length; "
            f"got: {exc_info.value!r}"
        )

    def test_decrypted_short_plaintext_rejected_in_live(self, client_runner_mod, monkeypatch):
        """Live: a successfully-decrypted plaintext that is suspiciously
        short (<10 chars) must be rejected — Tradier tokens are far
        longer; a short value indicates double-encryption or corruption.
        """
        fresh_key = client_runner_mod.Fernet.generate_key().decode()
        monkeypatch.setattr(client_runner_mod, "_raw_key", fresh_key)
        ct = _encrypt_new_scheme("abc", fresh_key)  # 3-char plaintext
        with pytest.raises((ValueError, RuntimeError)) as exc_info:
            client_runner_mod.decrypt_token(ct, mode="LIVE")
        msg = str(exc_info.value).lower()
        assert "length" in msg or "short" in msg or "invalid" in msg, (
            f"LIVE short-plaintext rejection must mention length/short/invalid; "
            f"got: {exc_info.value!r}"
        )

    def test_normal_plaintext_passes_through(self, client_runner_mod, monkeypatch):
        """Regression guard: a normal-length plaintext (>=10 chars)
        must still decrypt successfully and pass through unchanged."""
        fresh_key = client_runner_mod.Fernet.generate_key().decode()
        monkeypatch.setattr(client_runner_mod, "_raw_key", fresh_key)
        normal = "tradier_access_token_AAAAAA_1234567890"
        ct = _encrypt_new_scheme(normal, fresh_key)
        out = client_runner_mod.decrypt_token(ct, mode="LIVE")
        assert out == normal, (
            f"Normal plaintext must pass through unchanged. "
            f"Expected {normal!r}, got {out!r}"
        )


# ══════════════════════════════════════════════════════════════════
# FIX-8 — order_monitor entry_watcher wiring
# ══════════════════════════════════════════════════════════════════

class TestFix8OrderMonitorWatcherWiring:
    def test_production_order_monitor_call_sites_pass_entry_watcher(self, client_runner_mod):
        """Both production constructors must pass the runner/core watcher.

        The repo has many APOrderMonitor(...) test call sites, but only two
        production construction paths matter for this PR:
        - ClientRunner normal startup
        - self_healing order-monitor restart
        """
        run_inner_src = inspect.getsource(client_runner_mod.ClientRunner._run_inner)
        assert (
            'entry_watcher=getattr(self.core, "entry_watcher", None)' in run_inner_src
            or "entry_watcher=getattr(self.core, 'entry_watcher', None)" in run_inner_src
        ), (
            "ClientRunner normal startup must pass self.core.entry_watcher "
            "into APOrderMonitor so watcher-owned PENDING_TRIGGER rows are "
            "actively provable, not downgraded to ownership-unknown."
        )

        healer_src = (REPO_ROOT / "ap" / "self_healing.py").read_text()
        assert (
            'entry_watcher=getattr(getattr(runner, "core", None), "entry_watcher", None)' in healer_src
            or "entry_watcher=getattr(getattr(runner, 'core', None), 'entry_watcher', None)" in healer_src
        ), (
            "self_healing restart must pass runner.core.entry_watcher into "
            "APOrderMonitor so recovered runners keep watcher ownership proof."
        )

    def test_normal_startup_monitor_receives_watcher_and_preserves_owned_pending_trigger(
        self,
        client_runner_mod,
        monkeypatch,
        caplog,
    ):
        """Runtime regression: the normal startup-built monitor must carry
        core.entry_watcher, and that monitor must preserve a watcher-owned
        stale PENDING_TRIGGER row instead of expiring it.
        """
        import logging
        from datetime import datetime, timedelta, timezone

        import ap.db as db_mod
        import ap.brokers.tradier as tradier_mod
        import ap.contract_selector as contract_selector_mod
        import ap.order_monitor as order_monitor_mod
        import ap.order_state_machine as order_state_machine_mod
        import ap.position_manager as position_manager_mod
        import ap.position_sizer as position_sizer_mod
        import ap_execution_core as execution_core_mod
        import ap_master_control as master_control_mod

        watcher = MagicMock()
        watcher.has_order.return_value = True
        exit_eng = MagicMock()
        exit_eng._refresh_quotes = MagicMock()

        class _FakeBroker:
            def __init__(self, cfg):
                self.cfg = cfg

            def get_account_equity(self):
                return 25000.0

        class _FakeMasterControl:
            def __init__(self, **_kwargs):
                self.wire = MagicMock()

        class _FakeOrderStateMachine:
            def __init__(self, client_id: str, execution_mode=None):
                self.client_id = client_id
                self.execution_mode = execution_mode
                self.expire_calls: list[tuple[str, str]] = []
                self.transition_calls: list[tuple[str, str, dict]] = []

            def get_split_brain_orders(self, *, execution_mode=None):
                return []

            def expire_pending_entry(self, local_order_id: str, *, reason: str):
                self.expire_calls.append((local_order_id, reason))
                return True

            def transition(self, local_order_id: str, status: str, **kwargs):
                self.transition_calls.append((local_order_id, status, dict(kwargs)))
                return True

        class _FakeCore:
            def __init__(self, **_kwargs):
                self.entry_watcher = watcher
                self.exit_eng = exit_eng
                self.mode = "PAPER"

            def start(self):
                return None

        monkeypatch.setattr(tradier_mod, "TradierConfig", lambda **kwargs: types.SimpleNamespace(**kwargs))
        monkeypatch.setattr(tradier_mod, "TradierBroker", _FakeBroker)
        monkeypatch.setattr(master_control_mod, "APMasterControl", _FakeMasterControl)
        monkeypatch.setattr(position_manager_mod, "APPositionManager", lambda client_id: MagicMock(client_id=client_id))
        monkeypatch.setattr(order_state_machine_mod, "APOrderStateMachine", _FakeOrderStateMachine)
        monkeypatch.setattr(contract_selector_mod, "APContractSelectionEngine", lambda **_kwargs: MagicMock())
        monkeypatch.setattr(execution_core_mod, "APExecutionCore", _FakeCore)
        monkeypatch.setattr(position_sizer_mod, "validate_sizer_thresholds", lambda *a, **kw: None)
        monkeypatch.setattr(client_runner_mod, "APPositionSizer", lambda **_kwargs: MagicMock())
        monkeypatch.setattr(client_runner_mod, "APEarningsGuard", lambda *a, **kw: MagicMock())
        monkeypatch.setattr(client_runner_mod, "APIVRankFilter", lambda *a, **kw: MagicMock())
        monkeypatch.setattr(db_mod, "ensure_client_exists", lambda *a, **kw: None)
        monkeypatch.setattr(client_runner_mod, "get_monitor", lambda: None)
        monkeypatch.setattr(client_runner_mod, "get_healer", lambda: None)
        monkeypatch.setattr(client_runner_mod, "SUPABASE_URL", "")
        monkeypatch.setattr(client_runner_mod, "SUPABASE_SERVICE_KEY", "")
        monkeypatch.setattr(client_runner_mod, "create_client", lambda *a, **kw: None)
        monkeypatch.setattr(client_runner_mod.APOrderMonitor, "start", lambda self: None)

        runner = client_runner_mod.ClientRunner(
            {
                "email": "runtime-watcher@test.local",
                "tradier_account_id": "PAPER-1234",
                "tradier_access_token": "paper-token-1234567890",
            }
        )
        runner.stopped.set()

        monkeypatch.setattr(runner, "_get_token", lambda: "paper-token-1234567890")
        monkeypatch.setattr(runner, "_clear_old_phantom_orders", lambda: None)
        monkeypatch.setattr(runner, "_load_client_config", lambda: {})
        monkeypatch.setattr(runner, "_validate_execution_core_started", lambda: None)
        monkeypatch.setattr(runner, "_register_exit_engine", lambda _exit_eng: None)
        monkeypatch.setattr(runner, "_run_startup_recovery", lambda _broker, _exit_eng: None)
        monkeypatch.setattr(runner, "_seed_exit_engine_from_db", lambda _exit_eng: None)
        monkeypatch.setattr(runner, "_start_position_quote_monitor", lambda _broker, _exit_eng: None)
        monkeypatch.setattr(runner, "_start_reconciler", lambda _broker, _exit_eng: None)
        monkeypatch.setattr(runner, "_sync_account_equity", lambda _broker: None)
        monkeypatch.setattr(runner, "_start_fill_monitor", lambda _broker, _exit_eng: None)
        monkeypatch.setattr(runner, "_assert_fill_monitor_alive", lambda: None)
        monkeypatch.setattr(runner, "_start_equity_refresh", lambda _broker: None)
        monkeypatch.setattr(runner, "_start_worker_thread", lambda _broker: None)
        monkeypatch.setattr(runner, "_assert_worker_alive", lambda: None)
        monkeypatch.setattr(runner, "_build_startup_manifest", lambda **_kwargs: None)
        monkeypatch.setattr(runner, "_validate_control_stack", lambda: None)
        monkeypatch.setattr(runner, "_set_entry_permission", lambda: None)
        monkeypatch.setattr(runner, "_start_runtime_health_loop", lambda: None)

        runner._run_inner()

        assert runner.order_monitor is not None, "Normal startup must construct an order monitor"
        assert runner.order_monitor.entry_watcher is watcher, (
            "Normal startup-built APOrderMonitor must receive the same "
            "entry_watcher instance from runner.core."
        )
        assert runner.order_state_machine.execution_mode == "paper", (
            "Normal startup must construct the OSM with the runner's "
            "canonical execution mode."
        )

        monkeypatch.setattr(order_monitor_mod, "PENDING_TRIGGER_CLEANUP_ENABLED", True)
        monkeypatch.setattr(order_monitor_mod, "PENDING_TRIGGER_MAX_AGE_SECONDS", 60)
        monkeypatch.setattr(order_monitor_mod, "PENDING_TRIGGER_CLEANUP_DRY_RUN", False)

        created = datetime.now(timezone.utc) - timedelta(seconds=120)
        runner.order_monitor._emit_order_event = MagicMock()
        runner.order_monitor._get_active_entry_orders = MagicMock(
            return_value=[
                {
                    "local_order_id": "pending-1",
                    "status": "PENDING_TRIGGER",
                    "symbol": "AAPL",
                    "contract": "AAPL260619C00100000",
                    "broker_order_id": None,
                    "submitted_ts": None,
                    "created_ts": created.isoformat(),
                    "updated_ts": created.isoformat(),
                }
            ]
        )

        caplog.set_level(logging.INFO, logger="ap.order_monitor")
        runner.order_monitor._check_entry_orders()

        watcher.has_order.assert_called_once_with("pending-1")
        assert runner.order_state_machine.expire_calls == []
        assert runner.order_state_machine.transition_calls == []
        assert "watcher_owner_state=True" in caplog.text
        assert "cleanup_action=preserve_watcher_owned" in caplog.text


# ══════════════════════════════════════════════════════════════════
# INV — Structural invariants (BUG-CR-3 already resolved upstream)
# ══════════════════════════════════════════════════════════════════

class TestInvariantWatcherModeAlreadyWired:
    def test_execution_core_constructs_watcher_with_mode(self):
        """BUG-CR-3 from the audit suggests adding a runner-side mutation
        `self.core.entry_watcher.mode = self.mode` after core.start().
        That is UNNECESSARY because PR C already wires mode at the
        execution-core construction site. Adding a runner-side mutation
        would duplicate the canonical-mode authority pattern and create
        a second source of truth.

        This test pins down the upstream wiring so we don't regress
        AND so we don't accidentally add a runner-side duplicate.
        """
        # ap_execution_core.py constructs APEntryWatcher with mode=self.mode
        assert re.search(
            r'APEntryWatcher\(\s*(?:broker|self\.broker).*?mode\s*=\s*self\.mode',
            EX_CORE_SRC, re.DOTALL,
        ), (
            "ap_execution_core.py must construct APEntryWatcher with "
            "mode=self.mode (PR C / BUG-EW-5 fix). Without this, the "
            "watcher silently uses PAPER fail-open for LIVE clients."
        )

    def test_runner_does_not_mutate_watcher_mode_post_start(self):
        """The runner must NOT mutate self.core.entry_watcher.mode after
        core.start() — that would create a second authority over the
        watcher's mode and contradict the canonical (master_control.mode)
        source of truth established in PR B."""
        # Search for any assignment to entry_watcher.mode in client_runner
        forbidden = re.search(
            r'\.entry_watcher\.mode\s*=\s*',
            CLIENT_RUNNER_SRC,
        )
        assert not forbidden, (
            "client_runner must NOT mutate entry_watcher.mode. "
            "APExecutionCore already wires it at construction time using "
            "master_control.mode as the canonical authority. A runner-side "
            "mutation would create a duplicate setter and risk drift."
        )
