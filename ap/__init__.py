# package marker


def install_entry_metadata_safety_guards() -> None:
    from .master_control_metadata_guard import install_master_control_metadata_guard
    from .entry_metadata_guard import install_entry_metadata_guard

    install_master_control_metadata_guard()
    install_entry_metadata_guard()


def install_underlying_confirmation_safety_guard() -> None:
    from .underlying_confirmation_entry_guard import install_underlying_confirmation_entry_guard

    install_underlying_confirmation_entry_guard()


def install_deferred_materialization_persistence_guard() -> None:
    from .deferred_materializer_persist_guard import install_selected_materialization_persist_guard as install_guard

    install_guard()


def install_selector_cursor_safety_guard() -> None:
    from .selector_cursor_persistence_guard import install_selector_cursor_persistence_guard

    install_selector_cursor_persistence_guard()


def install_trade_lifecycle_safety_guards() -> None:
    from .trade_lifecycle_guards import install_trade_lifecycle_guards

    install_trade_lifecycle_guards()


def install_entry_safety_guards() -> None:
    installers = (
        install_entry_metadata_safety_guards,
        install_underlying_confirmation_safety_guard,
        install_deferred_materialization_persistence_guard,
        install_trade_lifecycle_safety_guards,
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
# PR #401 cursor persistence classification is a deployment invariant, not an
# optional compatibility guard. If it cannot install, fail startup rather than
# silently reverting to the implementation that conflates DB failure with an
# exact-owner CAS miss.
install_selector_cursor_safety_guard()
