"""Raw observer SQLite, control reads and profile discovery must retain physical workers."""
import asyncio
from contextlib import contextmanager
from pathlib import Path
import queue
import threading

import pytest

from agent.secret_scope import get_secret
from gateway import admission, run, run_notifications, run_shutdown
from gateway.admission import gateway_admission_status
from hermes_constants import get_hermes_home
from hermes_state import SessionDB
from hermes_state_registry import close_all_under
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native


class ObserverClock:
    def __init__(self, runner):
        self.runner = runner

    def __getattr__(self, name):
        return self.sleep if name == 'sleep' else getattr(asyncio, name)

    async def sleep(self, delay):
        if delay == .001:
            self.runner._running = False
        await asyncio.sleep(0 if delay in (3, .001) else delay)


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['ledger', 'control', 'mcp'])
@pytest.mark.parametrize('cancel', [False, True])
async def test_actual_supervised_observer_keeps_raw_worker(native, monkeypatch, kind, cancel):
    from tools import async_delegation, mcp_tool_discovery
    from tools.process_registry import process_registry
    runner = native.runner
    second = native.home.parent / 'profile-B'
    second.mkdir()
    db = SessionDB(db_path=second / 'state.db')
    db.close()
    for home, label in ((native.home, 'A'), (second, 'B')):
        (home / '.env').write_text('NATIVE_SCOPE_PROBE=synthetic-' + label + '\n')
    homes = [native.home] if cancel or kind == 'control' else [native.home, second, native.home]
    entered, stopped = asyncio.Queue(), threading.Event()
    releases, completed, observed = [], threading.Event(), []
    event_loop, original_sleep = asyncio.get_running_loop(), asyncio.sleep

    def hold(home):
        if stopped.is_set():
            return
        release = threading.Event()
        releases.append(release)
        event_loop.call_soon_threadsafe(entered.put_nowait, (Path(home), release))
        assert release.wait(5)

    if kind == 'ledger':
        local = threading.local()
        transaction, sweep = async_delegation._transaction, async_delegation.sweep_orphaned_completions
        @contextmanager
        def held_transaction():
            with transaction() as conn:
                if getattr(local, 'hold', False):
                    local.hold = False
                    conn.execute('SELECT count(*) FROM async_delegations').fetchone()
                    home = Path(get_hermes_home())
                    observed.append(home)
                    hold(home)  # Actual SQLite transaction remains open in worker.
                yield conn
        def marked_sweep(*args, **kwargs):
            local.hold = True
            return sweep(*args, **kwargs)
        original = runner._sweep_orphaned_completion_ledgers
        def actual_sweep():
            try:
                return original()
            finally:
                completed.set()
        runner._served_profile_homes = {} if cancel else {'B': second, 'A-revisit': native.home}
        runner._primary_profile_name = 'launch'
        monkeypatch.setattr(async_delegation, '_transaction', held_transaction)
        monkeypatch.setattr(async_delegation, 'sweep_orphaned_completions', marked_sweep)
        monkeypatch.setattr(runner, '_sweep_orphaned_completion_ledgers', actual_sweep)
        monkeypatch.setattr(process_registry, 'completion_queue', queue.Queue())
        monkeypatch.setattr(run_notifications, 'asyncio', ObserverClock(runner))
        factory = lambda: runner._async_delegation_watcher(interval=.001)
    elif kind == 'control':
        original = admission.read_admission_controls
        def actual_read(home):
            try:
                controls = original(home)
                with (Path(home) / '.env').open() as stream:
                    assert stream.read() == 'NATIVE_SCOPE_PROBE=synthetic-A\n'
                    hold(home)
                return controls
            finally:
                completed.set()
        monkeypatch.setattr(admission, 'read_admission_controls', actual_read)
        monkeypatch.setattr(run_shutdown, 'asyncio', ObserverClock(runner))
        factory = lambda: runner._drain_control_watcher(interval=.001)
    else:
        def discovery():
            try:
                home = Path(get_hermes_home())
                with (home / '.env').open() as stream:
                    expected = 'synthetic-' + ('A' if home == native.home else 'B')
                    assert get_secret('NATIVE_SCOPE_PROBE') == expected
                    assert stream.read() == 'NATIVE_SCOPE_PROBE=' + expected + '\n'
                    observed.append(home)
                    hold(home)
            finally:
                completed.set()
        monkeypatch.setattr(mcp_tool_discovery, 'discover_mcp_tools', discovery)  # Inert discovery boundary; no MCP network.
        monkeypatch.setattr(run, '_enable_multiplex_log_routing', lambda _cfg: None)
        factory = lambda: runner._after_profiles_added([(str(i), home) for i, home in enumerate(homes)])

    task = runner._spawn_supervised(factory, 'synthetic observer physical work', restart=False)
    try:
        for index, home in enumerate(homes):
            actual, release = await asyncio.wait_for(entered.get(), 5)
            assert actual == home
            if cancel or index == len(homes) - 1:
                runner._ingress_barrier.close()
                if kind == 'control':
                    runner._draining = True  # Control reconciliation must not reopen this isolated close.
                assert not gateway_admission_status(runner)['quiescent']
            assert any(not future.done() and not isinstance(future, asyncio.Task)
                       for future in getattr(runner, '_deferred_agent_workers', {}))
            if cancel:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                assert not gateway_admission_status(runner)['quiescent']
            release.set()
        await asyncio.gather(task, return_exceptions=True)
        for _ in range(500):
            if not getattr(runner, '_deferred_agent_workers', {}):
                break
            await original_sleep(.01)
        assert gateway_admission_status(runner)['quiescent'] is True
        if not cancel and kind != 'control':
            assert observed == homes
        assert not native.effects
    finally:
        stopped.set()
        runner._running = False
        task.cancel()
        for release in releases:
            release.set()
        await asyncio.gather(task, return_exceptions=True)
        assert await asyncio.to_thread(completed.wait, 10)
        close_all_under(second)


@pytest.mark.asyncio
@pytest.mark.parametrize('cancel', [False, True])
async def test_actual_catalog_observer_retains_physical_refresh(native, monkeypatch, cancel):
    from gateway import run_watchers
    from hermes_cli import model_catalog
    runner, event_loop = native.runner, asyncio.get_running_loop()
    entered, release, done = asyncio.Event(), threading.Event(), threading.Event()
    path = native.home / 'synthetic-catalog-read.txt'
    path.write_text('EXACT inert catalog input')
    def refresh():
        try:
            with path.open() as stream:
                assert stream.read() == 'EXACT inert catalog input'
                event_loop.call_soon_threadsafe(entered.set)
                assert release.wait(5)
        finally:
            runner._running = False
            done.set()
    class Clock:
        def __getattr__(self, name):
            return self.sleep if name == 'sleep' else getattr(asyncio, name)
        async def sleep(self, _delay):
            await asyncio.sleep(0)
    monkeypatch.setattr(run_watchers, 'asyncio', Clock())
    monkeypatch.setattr(model_catalog, 'refresh_catalogs', refresh)  # Inert remote boundary, actual native observer.
    task = runner._spawn_supervised(runner._model_catalog_refresh_watcher, 'synthetic catalog read', restart=False)
    try:
        await asyncio.wait_for(entered.wait(), 5)
        runner._ingress_barrier.close()
        assert not gateway_admission_status(runner)['quiescent']
        if cancel:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            assert not gateway_admission_status(runner)['quiescent']
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        for _ in range(500):
            if not getattr(runner, '_deferred_agent_workers', {}):
                break
            await asyncio.sleep(.01)
        assert gateway_admission_status(runner)['quiescent'] is True
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert await asyncio.to_thread(done.wait, 5)
