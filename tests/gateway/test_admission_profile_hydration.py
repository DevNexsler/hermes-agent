"""Fresh-context pollers retain actual secret hydration workers before business admission."""
import asyncio
from contextlib import asynccontextmanager
from contextvars import Context
from pathlib import Path
import threading

import pytest

from agent.secret_scope import get_secret
from gateway import run, run_adapters, run_goals
from gateway.admission import GatewayDraining, gateway_admission_status, gateway_ingress, _ingress, _native_runner
from hermes_cli import goals, loops
from hermes_constants import get_hermes_home, set_hermes_home_override, reset_hermes_home_override
from hermes_state_registry import close_all_under
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native


def scoped_homes(native):
    second = native.home.parent / 'profile-B'
    second.mkdir()
    for home, label in ((native.home, 'A'), (second, 'B')):
        (home / '.env').write_text('NATIVE_SCOPE_PROBE=synthetic-' + label + '\n')
        (home / 'config.yaml').write_text('security: {tirith_enabled: false}\n')
        token = set_hermes_home_override(home)
        try:
            db = goals._get_session_db()
            loops.LoopManager('synthetic-loop-' + label).set('EXACT inert unrouted loop', interval_seconds=60)
            db.create_session('synthetic-handoff-' + label, 'cli')
            db.append_message('synthetic-handoff-' + label, 'user', 'EXACT retained ' + label)
            db.request_handoff('synthetic-handoff-' + label, 'telegram')
        finally:
            reset_hermes_home_override(token)
    return second


def poller_clock(monkeypatch, runner, module):
    original = asyncio.sleep
    class Clock:
        def __getattr__(self, name):
            return self.sleep if name == 'sleep' else getattr(asyncio, name)
        async def sleep(self, delay):
            if delay == .001:
                runner._running = False
            await original(0 if delay in (5, .001) else delay)
    monkeypatch.setattr(module, 'asyncio', Clock())


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['loop', 'handoff'])
@pytest.mark.parametrize('cancel', [False, True])
async def test_actual_supervised_profile_hydration_stays_owned(native, monkeypatch, kind, cancel):
    runner = native.runner
    second = scoped_homes(native)
    homes = [second] if cancel else [native.home, second, native.home]
    monkeypatch.setattr(run, '_handoff_watch_scopes', lambda _runner: [('synthetic', home) for home in homes])
    runner._multiplex_profile_homes = {'synthetic': second}
    module = run_goals if kind == 'loop' else run_adapters
    poller_clock(monkeypatch, runner, module)
    loader, scope = run._load_profile_secret_scope, run._async_profile_runtime_scope
    event_loop, entered = asyncio.get_running_loop(), asyncio.Queue()
    releases, finished, scoped, contexts = [], [], [], []

    def held(home):
        contexts.append((_native_runner.get(), _ingress.get()))
        release, done = threading.Event(), threading.Event()
        releases.append(release)
        finished.append(done)
        if len(releases) <= len(homes):
            event_loop.call_soon_threadsafe(entered.put_nowait, (Path(home), release))
            assert release.wait(5)
        try:
            return loader(home)
        finally:
            done.set()

    @asynccontextmanager
    async def observed(home, **kwargs):
        async with scope(home, **kwargs):
            scoped.append((Path(get_hermes_home()), get_secret('NATIVE_SCOPE_PROBE')))
            if runner._ingress_barrier._closed:
                with pytest.raises(GatewayDraining):
                    with gateway_ingress(runner):
                        raise AssertionError('observer identity borrowed closed ingress permission')
            yield

    monkeypatch.setattr(run, '_load_profile_secret_scope', held)
    monkeypatch.setattr(run, '_async_profile_runtime_scope', observed)
    factory = (lambda: runner._loop_wakeup_watcher(interval=.001)) if kind == 'loop' else (
        lambda: runner._handoff_watcher(interval=.001, drain_timeout=.01))
    with gateway_ingress(runner):
        task = runner._spawn_supervised(factory, 'synthetic hydration ownership', restart=False)
    try:
        for index, home in enumerate(homes):
            actual, release = await asyncio.wait_for(entered.get(), 5)
            assert actual == home
            if cancel or index == len(homes) - 1:
                runner._ingress_barrier.close()
                assert not gateway_admission_status(runner)['quiescent']
            assert any(not future.done() and not isinstance(future, asyncio.Task)
                       for future in getattr(runner, '_deferred_agent_workers', {}))
            assert contexts[index] == (runner, None)  # Fresh task drops ingress; I/O owner remains explicit.
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
        if not cancel:
            assert scoped[:3] == [(home, 'synthetic-' + ('A' if home == native.home else 'B')) for home in homes]
        assert not native.effects
    finally:
        runner._running = False
        task.cancel()
        for release in releases:
            release.set()
        await asyncio.gather(task, return_exceptions=True)
        for done in finished:
            assert await asyncio.to_thread(done.wait, 5)  # Own held worker cleanup, including RED source.
        close_all_under(second)


@pytest.mark.asyncio
@pytest.mark.parametrize('operation', ['configuration', 'reconnect'])
@pytest.mark.parametrize('cancel', [False, True])
async def test_direct_profile_hydration_retains_physical_future(native, monkeypatch, operation, cancel):
    from hermes_cli import env_loader, profiles
    second = scoped_homes(native)
    runner, event_loop = native.runner, asyncio.get_running_loop()
    entered, release, finished = asyncio.Event(), threading.Event(), threading.Event()
    original = env_loader.hydrate_profile_secret_sources
    def held(home, *args, **kwargs):
        assert Path(home) == second
        event_loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        try:
            return original(home, *args, **kwargs)
        finally:
            finished.set()
    monkeypatch.setattr(env_loader, 'hydrate_profile_secret_sources', held)
    monkeypatch.setattr(profiles, 'get_profile_dir', lambda _name: second)
    coro = (runner._load_secondary_profile_config('synthetic', second) if operation == 'configuration'
            else runner._secondary_reconnect_attempt('synthetic', run.Platform.TELEGRAM))
    task = Context().run(lambda: asyncio.create_task(coro))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        runner._ingress_barrier.close()
        assert not gateway_admission_status(runner)['quiescent']
        if cancel:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            assert not gateway_admission_status(runner)['quiescent']
        release.set()
        result, = await asyncio.gather(task, return_exceptions=True)
        assert isinstance(result, asyncio.CancelledError) if cancel else not isinstance(result, BaseException)
        for _ in range(500):
            if not getattr(runner, '_deferred_agent_workers', {}):
                break
            await asyncio.sleep(.01)
        assert gateway_admission_status(runner)['quiescent'] is True
        assert not native.effects
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        assert await asyncio.to_thread(finished.wait, 5)
        close_all_under(second)
