from __future__ import annotations

import inspect

from ap import queue as queue_mod
import ap_master_control as mc_mod


QUEUE_SRC = inspect.getsource(queue_mod._dispatch)
MC_SRC = inspect.getsource(mc_mod.APMasterControl.evaluate)


def test_queued_write_occurs_only_after_create_entry_order():
    create_idx = QUEUE_SRC.find("create_entry_order(")
    queued_idx = QUEUE_SRC.find('decision_status="queued"')

    assert create_idx > 0, "_dispatch() must still create entry orders"
    assert queued_idx > create_idx, (
        "queued status write must happen only after create_entry_order succeeds"
    )


def test_master_control_no_longer_stamps_queued_directly():
    assert 'self._store_update(signal_id, "queued"' not in MC_SRC, (
        "master_control must not mark queued before queue admission is real"
    )


def test_queue_logs_when_post_create_queued_write_fails():
    # PR #225 amendment: the diagnostic must carry every field an operator
    # needs to find the real order when the post-create ap_signals queued
    # write fails — signal_id, client_id, local_order_id, ticker,
    # execution_mode, and an explicit reason_code. Checked on both the
    # not-logged branch and the exception branch.
    assert QUEUE_SRC.count("QUEUED_SIGNAL_WRITE_FAILED_AFTER_ORDER_CREATE") >= 2, (
        "reason_code=QUEUED_SIGNAL_WRITE_FAILED_AFTER_ORDER_CREATE must appear "
        "on both the failed-write branch and the exception branch"
    )
    assert "signal_id=%s" in QUEUE_SRC
    assert "client_id=%s" in QUEUE_SRC
    assert "local_order_id=%s" in QUEUE_SRC
    assert "execution_mode=%s" in QUEUE_SRC
