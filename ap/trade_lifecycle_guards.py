"""Install optional production trade-lifecycle hardening guards.

Each guard is isolated in its own module and may be absent on branches where its
PR has not merged yet. Import failures are logged and never make ``import ap``
fail. Individual guard installers are idempotent.
"""
from __future__ import annotations

import importlib
import logging

log = logging.getLogger("ap.trade_lifecycle_guards")

_GUARDS: tuple[tuple[str, str], ...] = (
    ("ap.exit_fill_truth_guard", "install_exit_fill_truth_guard"),
    ("ap.exit_decision_idempotency_guard", "install_exit_decision_idempotency_guard"),
    ("ap.proof_taxonomy_guard", "install_proof_taxonomy_guard"),
    ("ap.one_contract_exit_guard", "install_one_contract_exit_guard"),
)


def install_trade_lifecycle_guards() -> None:
    """Install every lifecycle guard currently present in the checkout."""
    for module_name, installer_name in _GUARDS:
        try:
            module = importlib.import_module(module_name)
        except ModuleNotFoundError as exc:
            if exc.name == module_name:
                continue
            log.debug("lifecycle guard dependency unavailable module=%s error=%s", module_name, exc)
            continue
        except Exception as exc:
            log.debug("lifecycle guard import failed module=%s error=%s", module_name, exc)
            continue

        try:
            installer = getattr(module, installer_name)
            installer()
        except Exception as exc:
            log.exception("lifecycle guard install failed module=%s installer=%s error=%s", module_name, installer_name, exc)
