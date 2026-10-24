"""Startup/timer reads and thread-scheduled directory refresh retain actual owners."""
import asyncio
from contextlib import contextmanager
import threading

import pytest

from gateway import channel_directory, delivery_ledger, run
from gateway.admission import gateway_admission_status
from gateway.config import Platform
from tests.gateway import test_admission_reconnect_and_boot_io

HeldIO = test_admission_reconnect_and_boot_io.HeldIO
native = test_admission_reconnect_and_boot_io.native


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['claim', 'flood'])
@pytest.mark.parametrize('cancel', [False, True])
async def test_direct_boot_claim_and_flood_timer_retain_transaction(native, monkeypatch, kind, cancel):
    runner, held = native.runner, HeldIO()
    # Initialize only disposable native ledger. No historical row or transport input.
    with delivery_ledger._transaction() as conn:
        assert conn.execute('SELECT count(*) FROM delivery_obligations').fetchone()[0] == 0
    transaction = delivery_ledger._transaction

    @contextmanager
    def read():
        try:
            with transaction() as conn:
                conn.execute('SELECT count(*) FROM delivery_obligations').fetchone()
                held.hold()
                yield conn
        finally:
            held.finished.set()

    monkeypatch.setattr(delivery_ledger, '_transaction', read)
    if kind == 'claim':
        task = asyncio.create_task(runner._claim_pending_obligations())
    else:
        runner._schedule_flood_redelivery(Platform.TELEGRAM)
        task = runner._flood_redelivery_tasks[('telegram', 'default')]
    try:
        await held.check_owned(runner, task, cancel)
        if not cancel:
            await asyncio.gather(task)
        assert not native.adapter.sent and not native.effects
    finally:
        await held.cleanup(runner, task)


@pytest.mark.asyncio
@pytest.mark.parametrize('cancel', [False, True])
async def test_actual_thread_scheduled_directory_refresh_retains_child_and_worker(native, monkeypatch, cancel):
    runner, held = native.runner, HeldIO()
    writer = channel_directory.atomic_json_write
    thread_done, errors = threading.Event(), []

    def write(path, value):
        try:
            held.hold()
            return writer(path, value)
        finally:
            held.finished.set()

    def refresh():
        try:
            run._housekeeping_channel_directory(runner.adapters, held.loop, runner)
        except BaseException as exc:
            errors.append(exc)
        finally:
            thread_done.set()

    monkeypatch.setattr(channel_directory, 'atomic_json_write', write)
    thread = threading.Thread(target=refresh)
    thread.start()
    try:
        await asyncio.wait_for(held.entered.wait(), 5)
        child = next(f for f in runner._deferred_agent_workers if isinstance(f, asyncio.Task))
        await held.check_owned(runner, child, cancel)
        assert await asyncio.to_thread(thread_done.wait, 5)
        assert len(errors) == int(cancel)
        assert not native.effects
    finally:
        held.release.set()
        assert await asyncio.to_thread(thread_done.wait, 5)
        thread.join()
        for _ in range(500):
            if not getattr(runner, '_deferred_agent_workers', {}):
                break
            await asyncio.sleep(.01)
    assert gateway_admission_status(runner)['quiescent']
    assert (native.home / 'channel_directory.json').exists()
