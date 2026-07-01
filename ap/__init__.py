# package marker


def install_entry_metadata_safety_guards() -> None:
    from .master_control_metadata_guard import install_master_control_metadata_guard
    from .entry_metadata_guard import install_entry_metadata_guard

    install_master_control_metadata_guard()
    install_entry_metadata_guard()


def install_underlying_confirmation_safety_guard() -> None:
    from .underlying_confirmation_entry_guard import install_underlying_confirmation_entry_guard

    install_underlying_confirmation_entry_guard()


def install_operator_queue_counts_guard() -> None:
    from .operator_queue_counts import install_operator_queue_counts_route

    install_operator_queue_counts_route()


def install_entry_safety_guards() -> None:
    installers = (
        install_entry_metadata_safety_guards,
        install_underlying_confirmation_safety_guard,
        install_operator_queue_counts_guard,
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
