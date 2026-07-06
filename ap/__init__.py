# package marker


def install_entry_metadata_safety_guards() -> None:
    from .master_control_metadata_guard import install_master_control_metadata_guard
    from .entry_metadata_guard import install_entry_metadata_guard

    install_master_control_metadata_guard()
    install_entry_metadata_guard()


def install_underlying_confirmation_safety_guard() -> None:
    from .underlying_confirmation_entry_guard import install_underlying_confirmation_entry_guard

    install_underlying_confirmation_entry_guard()


def install_trade_profile_diagnostics() -> None:
    """Attach observe-only AP profile diagnostics to trade dossiers.

    This installer follows the existing package-level guard pattern: importing the
    package wires diagnostics only. It must never change entry admission, broker
    submit/cancel, queue state, order mutation, position mutation, or execution
    mode. Failures are swallowed so diagnostics cannot block production flow.
    """
    from .trade_profile_diagnostics import install_trade_dossier_profile_diagnostics

    install_trade_dossier_profile_diagnostics()


def install_entry_safety_guards() -> None:
    installers = (
        install_entry_metadata_safety_guards,
        install_underlying_confirmation_safety_guard,
        install_trade_profile_diagnostics,
    )
    for installer in installers:
        try:
            installer()
        except Exception:
            # Keep package import clean. Individual guard modules may be absent on
            # branches where their PR has not merged yet, or partially initialized
            # during import-clean smoke tests. Explicit callers can retry later.
            pass


install_entry_safety_guards()
