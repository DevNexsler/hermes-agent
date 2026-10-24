"""Real reconnect, bounded boot sends and loop liveness retain physical I/O."""
import asyncio
from contextlib import contextmanager
import threading

import pytest

from gateway import admission, channel_directory, delivery_ledger, run, run_adapters, shutdown_watchdog
from gateway.admission import gateway_admission_status, gateway_ingress
from gateway.config import Platform
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native


class HeldIO:
    def __init__(self):
        self.loop = asyncio.get_running_loop()
        self.entered = asyncio.Event()
        self.release = threading.Event()
        self.finished = threading.Event()

    def hold(self):
        self.loop.call_soon_threadsafe(self.entered.set)
        assert self.release.wait(5)

    async def check_owned(self, runner, task, cancel):
        await asyncio.wait_for(self.entered.wait(), 5)
        runner._ingress_barrier.close()
        assert not gateway_admission_status(runner)['quiescent']
        if cancel:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            assert not gateway_admission_status(runner)['quiescent']
        self.release.set()
        assert await asyncio.to_thread(self.finished.wait, 5)

    async def cleanup(self, runner, task):
        self.release.set()
        runner._running = False
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert await asyncio.to_thread(self.finished.wait, 5)
        for _ in range(500):
            if not getattr(runner, '_deferred_agent_workers', {}):
                break
            await asyncio.sleep(.01)
        assert gateway_admission_status(runner)['quiescent']


class ReconnectClock:
    def __init__(self, runner):
        self.runner = runner

    def __getattr__(self, name):
        return self.sleep if name == 'sleep' else getattr(asyncio, name)

    async def sleep(self, delay):
        if delay == 1:
            self.runner._running = False
        await asyncio.sleep(0 if delay in (1, 10) else delay)


@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['sessions', 'writer'])
@pytest.mark.parametrize('cancel', [False, True])
async def test_actual_supervised_reconnect_directory_keeps_physical_io(native, monkeypatch, phase, cancel):
    from hermes_state_registry import acquire
    import hermes_state_registry
    runner, held = native.runner, HeldIO()
    runner.config.platforms[Platform.TELEGRAM].token = 'synthetic-inert-adapter'
    runner._failed_platforms = {Platform.TELEGRAM: {
        'config': runner.config.platforms[Platform.TELEGRAM], 'attempts': 0, 'next_retry': 0}}
    monkeypatch.setattr(runner, '_create_adapter', lambda *_args: native.adapter)
    monkeypatch.setattr(run_adapters, 'asyncio', ReconnectClock(runner))
    writer = channel_directory.atomic_json_write
    discover = channel_directory._build_from_sessions

    def read(*args, **kwargs):
        db = acquire(*args, **kwargs)
        db.list_gateway_sessions(platform='telegram', active_only=False)
        held.hold()
        return db

    def discovery(platform):
        try:
            return discover(platform)
        finally:
            held.finished.set()

    def write(path, value):
        try:
            with (native.home / 'synthetic-directory-input').open('w') as stream:
                stream.write('inert directory input')
                held.hold()
                return writer(path, value)
        finally:
            held.finished.set()

    monkeypatch.setattr(hermes_state_registry, 'acquire', read if phase == 'sessions' else acquire)
    monkeypatch.setattr(channel_directory, '_build_from_sessions', discovery if phase == 'sessions' else discover)
    monkeypatch.setattr(channel_directory, 'atomic_json_write', write if phase == 'writer' else writer)
    task = runner._spawn_reconnect_watcher()
    try:
        await held.check_owned(runner, task, cancel)
        if not cancel:
            await asyncio.gather(task)
        assert not native.effects and not native.adapter.sent
    finally:
        await held.cleanup(runner, task)


@pytest.mark.asyncio
@pytest.mark.parametrize('cancel', [False, True])
async def test_actual_bounded_boot_redelivery_keeps_ledger_transaction(native, monkeypatch, cancel):
    runner, held = native.runner, HeldIO()
    delivery_ledger.record_crash_left_reply(
        obligation_id='synthetic-boot-obligation', session_key='synthetic-absent-session',
        platform='telegram', chat_id='123456', thread_id=None, content='retained synthetic reply', since=0)
    transaction, mark = delivery_ledger._transaction, delivery_ledger.mark_delivered
    local = threading.local()
    inherited_leases = []

    @contextmanager
    def held_transaction():
        with transaction() as conn:
            if getattr(local, 'mark', False):
                conn.execute('SELECT count(*) FROM delivery_obligations').fetchone()
                held.hold()
            yield conn

    def held_mark(oid):
        local.mark = True
        try:
            inherited_leases.append(admission._ingress.get())
            return mark(oid)
        finally:
            held.finished.set()

    async def no_restart_notice():
        return None

    monkeypatch.setattr(delivery_ledger, '_transaction', held_transaction)
    monkeypatch.setattr(delivery_ledger, 'mark_delivered', held_mark)
    monkeypatch.setattr(runner, '_send_restart_notification', no_restart_notice)
    monkeypatch.setattr(run, '_startup_restore_drain_timeout_secs', lambda: .001)
    runner._startup_restore_tasks, runner._startup_restore_queue = [], []
    try:
        with gateway_ingress(runner):
            runner._startup_restore_in_progress = True
            await asyncio.wait_for(runner._await_startup_boot_sends(planned_restart_notification_pending=False), 5)
        task = next(t for t in runner._background_tasks if not t.done())
        await runner._finish_startup_restore()
        assert not runner._startup_restore_in_progress
        await held.check_owned(runner, task, cancel)
        if not cancel:
            await asyncio.gather(task)
        assert native.adapter.sent == [delivery_ledger.RECOVERED_MARKER + 'retained synthetic reply']
        assert inherited_leases == [None]  # Boot producer cannot borrow parent ingress.
        assert not native.effects
    finally:
        if 'task' in locals():
            await held.cleanup(runner, task)
        else:
            held.release.set()
    with delivery_ledger._transaction() as conn:
        row = conn.execute('SELECT state,content FROM delivery_obligations WHERE obligation_id=?',
                           ('synthetic-boot-obligation',)).fetchone()
    assert row == ('delivered', 'retained synthetic reply')


@pytest.mark.asyncio
async def test_bounded_boot_gate_retains_actual_send_before_ledger_worker(native, monkeypatch):
    runner = native.runner
    entered, release = asyncio.Event(), asyncio.Event()
    inherited_leases = []
    original = native.adapter.send
    delivery_ledger.record_crash_left_reply(
        obligation_id='synthetic-async-send', session_key='synthetic-absent-session',
        platform='telegram', chat_id='123456', thread_id=None, content='retained async reply', since=0)

    async def send(*args, **kwargs):
        inherited_leases.append(admission._ingress.get())
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    async def no_restart_notice():
        return None

    monkeypatch.setattr(native.adapter, 'send', send)
    monkeypatch.setattr(runner, '_send_restart_notification', no_restart_notice)
    monkeypatch.setattr(run, '_startup_restore_drain_timeout_secs', lambda: .001)
    runner._startup_restore_in_progress = True
    runner._startup_restore_tasks, runner._startup_restore_queue = [], []
    try:
        await runner._await_startup_boot_sends(planned_restart_notification_pending=False)
        await asyncio.wait_for(entered.wait(), 5)
        await runner._finish_startup_restore()
        runner._ingress_barrier.close()
        assert not gateway_admission_status(runner)['quiescent']
        assert not native.adapter.sent and not native.effects
    finally:
        release.set()
        await asyncio.gather(*runner._background_tasks, return_exceptions=True)
    assert native.adapter.sent == [delivery_ledger.RECOVERED_MARKER + 'retained async reply']
    assert inherited_leases == [None]
    assert gateway_admission_status(runner)['quiescent']


@pytest.mark.asyncio
@pytest.mark.parametrize('cancel', [False, True])
async def test_actual_loop_liveness_keeps_held_file_writer(native, monkeypatch, cancel):
    runner, held = native.runner, HeldIO()
    writer = shutdown_watchdog.atomic_json_write

    def write(path, value, **kwargs):
        try:
            with (native.home / 'synthetic-heartbeat-input').open('w') as stream:
                stream.write('inert heartbeat input')
                held.hold()
                return writer(path, value, **kwargs)
        finally:
            held.finished.set()

    monkeypatch.setattr(shutdown_watchdog, 'atomic_json_write', write)
    runner._start_loop_heartbeat_task()
    task = runner._loop_heartbeat_task
    try:
        await held.check_owned(runner, task, cancel)
    finally:
        await held.cleanup(runner, task)
    assert shutdown_watchdog.get_loop_heartbeat_path(native.home).exists()
    assert not native.effects
