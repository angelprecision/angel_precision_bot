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


def install_live_hard_hold_runtime_safety_guard() -> None:
    from .live_hard_hold_runtime_guard import install_live_hard_hold_runtime_guard

    install_live_hard_hold_runtime_guard()


def install_exit_circuit_breaker_broker_truth_safety_guard() -> None:
    from .exit_circuit_breaker_broker_truth_guard import install_exit_circuit_breaker_broker_truth_guard

    install_exit_circuit_breaker_broker_truth_guard()


def install_repair_position_flat_safety_guard() -> None:
    from .repair_position_flat_guard import install_repair_position_flat_guard

    install_repair_position_flat_guard()


def install_entry_safety_guards() -> None:
    installers = (
        install_entry_metadata_safety_guards,
        install_underlying_confirmation_safety_guard,
        install_deferred_materialization_persistence_guard,
        install_live_hard_hold_runtime_safety_guard,
        install_exit_circuit_breaker_broker_truth_safety_guard,
        install_repair_position_flat_safety_guard,
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
