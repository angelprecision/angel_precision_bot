from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch


_REPO = Path(__file__).resolve().parents[1]
_LOADED_APP_MODULE = None


def _load_flask_client():
    old_env = os.environ.get("APP_ENV")
    old_db = os.environ.get("DATABASE_URL")
    old_fill = os.environ.get("ALLOW_LEGACY_FILL_MONITOR")
    os.environ["APP_ENV"] = "dev"
    os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")
    os.environ["ALLOW_LEGACY_FILL_MONITOR"] = "1"

    psycopg2_mod = MagicMock()
    psycopg2_mod.errors = SimpleNamespace()
    psycopg2_extras_mod = MagicMock()
    psycopg2_pool_mod = MagicMock()
    supabase_mod = MagicMock()
    cryptography_mod = MagicMock()
    fernet_mod = MagicMock()
    fernet_mod.Fernet = MagicMock()
    supabase_mod.create_client = MagicMock()
    supabase_mod.Client = MagicMock()

    with patch.dict(sys.modules, {
        "psycopg2": psycopg2_mod,
        "psycopg2.extras": psycopg2_extras_mod,
        "psycopg2.pool": psycopg2_pool_mod,
        "supabase": supabase_mod,
        "cryptography": cryptography_mod,
        "cryptography.fernet": fernet_mod,
    }):
        try:
            import app as app_mod
            global _LOADED_APP_MODULE
            _LOADED_APP_MODULE = app_mod
            app_mod.app.testing = True
            return app_mod.app.test_client(), app_mod
        finally:
            if old_env is None:
                os.environ.pop("APP_ENV", None)
            else:
                os.environ["APP_ENV"] = old_env
            if old_db is None:
                os.environ.pop("DATABASE_URL", None)
            else:
                os.environ["DATABASE_URL"] = old_db
            if old_fill is None:
                os.environ.pop("ALLOW_LEGACY_FILL_MONITOR", None)
            else:
                os.environ["ALLOW_LEGACY_FILL_MONITOR"] = old_fill


def _fake_runner(mode="live"):
    runner = MagicMock()
    runner.mode = mode
    runner.master_control = SimpleNamespace(mode=mode)
    return runner


class _Lock:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def test_blocked_readiness_marks_lock_failed_and_returns_503():
    client, app_mod = _load_flask_client()
    failed = []
    completed = []

    fake_lock_mod = SimpleNamespace(
        build_run_key=lambda **_: "rk-1",
        try_acquire_run_lock=lambda *a, **k: {
            "acquired": True,
            "run_key": "rk-1",
            "owner_token": "tok-1",
            "reclaimed": False,
        },
        mark_run_lock_completed=lambda *a, **k: completed.append((a, k)),
        mark_run_lock_failed=lambda *a, **k: failed.append((a, k)),
    )
    fake_client_runner = SimpleNamespace(
        _active_runners={"jason@example.com": _fake_runner("live")},
        _registry_lock=_Lock(),
    )

    with patch.dict(sys.modules, {
        "ap_handoff_run_lock": fake_lock_mod,
        "client_runner": fake_client_runner,
    }), patch(
        "ap.morning_handoff.run_morning_handoff_audit",
        return_value={"ok": True, "stage": "manual"},
    ), patch(
        "ap.preopen_readiness.run_preopen_autonomous_readiness",
        return_value={"ok": False, "status": "BLOCKED", "reason": "preopen_blocked"},
    ), patch.object(
        app_mod,
        "_apply_live_preopen_readiness",
        create=True,
    ):
        response = client.post(
            "/admin/morning_handoff_audit",
            json={"dry_run": False, "use_run_lock": True, "execution_mode": "live"},
        )

    assert response.status_code == 503
    body = response.get_json()
    assert body["ok"] is False
    assert body["run_key"] == "rk-1"
    assert completed == []
    assert len(failed) == 1
    assert failed[0][0][0] == "rk-1"
    assert failed[0][0][1] == "tok-1"
    assert failed[0][0][2] == "readiness_blocked"


def test_fail_open_lock_succeeds_without_marking_completion():
    """
    PR #226 amendment — required test.

    When try_acquire_run_lock() fails open (DB/table error on the lock itself,
    not on the handoff work), it returns acquired=True with a real owner_token
    but lock_persisted=False — there is no row in handoff_job_locks to update.

    The route must:
      - still run the handoff and return ok=True on success
      - NOT call mark_run_lock_completed (no row exists for this owner_token)
      - NOT call mark_run_lock_failed (same reason)

    Calling either in this state would hit the `c.rowcount == 0` branch inside
    ap_handoff_run_lock.py and log a misleading HANDOFF_RUN_LOCK_OWNER_MISMATCH
    warning — that's exactly the noise this amendment eliminates.
    """
    client, app_mod = _load_flask_client()
    completed = MagicMock()
    failed = MagicMock()

    fake_lock_mod = SimpleNamespace(
        build_run_key=lambda **_: "rk-fail-open",
        try_acquire_run_lock=lambda *a, **k: {
            "acquired": True,
            "run_key": "rk-fail-open",
            "owner_token": "tok-fail-open",
            "reclaimed": False,
            "reason": "lock_error_fail_open",
            "lock_persisted": False,
        },
        mark_run_lock_completed=completed,
        mark_run_lock_failed=failed,
    )
    fake_client_runner = SimpleNamespace(
        _active_runners={"jose@example.com": _fake_runner("paper")},
        _registry_lock=_Lock(),
    )

    with patch.dict(sys.modules, {
        "ap_handoff_run_lock": fake_lock_mod,
        "client_runner": fake_client_runner,
    }), patch(
        "ap.morning_handoff.run_morning_handoff_audit",
        return_value={"ok": True, "stage": "manual"},
    ), patch(
        "ap.preopen_readiness.run_preopen_autonomous_readiness",
        return_value={"ok": True, "status": "OK"},
    ), patch.object(
        app_mod,
        "_apply_live_preopen_readiness",
        create=True,
    ):
        response = client.post(
            "/admin/morning_handoff_audit",
            json={"dry_run": False, "use_run_lock": True, "execution_mode": "paper"},
        )

    assert response.status_code == 200
    body = response.get_json()
    assert body["ok"] is True
    assert body["run_key"] == "rk-fail-open"

    completed.assert_not_called()
    failed.assert_not_called()


def test_route_uses_acquire_result_object_contract():
    src = (_REPO / "app.py").read_text()
    assert 'run_lock_owner_token = str((run_lock or {}).get("owner_token") or "")' in src
    assert 'if not bool((run_lock or {}).get("acquired")):' in src
    assert 'mark_run_lock_completed(run_lock_key, run_lock_owner_token, summary)' in src
    assert 'mark_run_lock_failed(run_lock_key, run_lock_owner_token, fail_reason, summary)' in src
    # PR #226 amendment: completion/failure marking must be gated on
    # lock_persisted so a fail-open acquire (no DB row) never triggers a
    # misleading HANDOFF_RUN_LOCK_OWNER_MISMATCH warning.
    assert '(run_lock or {}).get("lock_persisted", True)' in src
