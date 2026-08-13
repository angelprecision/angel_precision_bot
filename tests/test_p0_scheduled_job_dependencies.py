"""Scheduled jobs must start without importing the DB-bound trading runtime."""

import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def _workflow_text(name: str) -> str:
    return (ROOT / ".github" / "workflows" / name).read_text()


def test_paper_morning_jobs_install_requirements_before_script():
    text = _workflow_text("paper-morning-jobs.yml")
    install = text.index("python3 -m pip install -r requirements.txt")
    run = text.index("python3 ap/scripts/live_morning_jobs.py")
    assert install < run


def test_backup_morning_jobs_install_requirements_before_script():
    text = _workflow_text("overnight-reeval.yml")
    install = text.index("python3 -m pip install -r requirements.txt")
    run = text.index("python3 ap/scripts/live_morning_jobs.py")
    assert install < run


def test_morning_job_runner_imports_without_database_configuration():
    env = os.environ.copy()
    for key in ("DATABASE_URL", "DATABASE_URI", "SUPABASE_DB_URL", "POSTGRES_URL"):
        env.pop(key, None)

    probe = (
        "import runpy; "
        "ns = runpy.run_path('ap/scripts/live_morning_jobs.py', run_name='not_main'); "
        "assert callable(ns['build_job_calls']); "
        "assert callable(ns['call_admin_endpoint'])"
    )
    result = subprocess.run(
        [sys.executable, "-B", "-c", probe],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
