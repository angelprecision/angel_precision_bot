"""Scheduled jobs must install repo dependencies before importing bot modules."""

from pathlib import Path


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
    run = text.index("python3 -m ap.scripts.live_morning_jobs")
    assert install < run
