"""Real durable loop and CLI handoff work obey admission before database mutation."""
import asyncio
from pathlib import Path
from types import SimpleNamespace
import threading
import time

import pytest
import pytest_asyncio

from gateway.admission import GatewayDraining, gateway_admission_status
from gateway.config import GatewayConfig, HomeChannel, Platform, PlatformConfig
from gateway.run import GatewayRunner
from hermes_cli.loops import LoopManager
from hermes_constants import pin_process_hermes_home
from tests.gateway.restart_test_helpers import RestartTestAdapter


@pytest_asyncio.fixture
async def native(tmp_path, monkeypatch):
    home = tmp_path / '.hermes'
    home.mkdir()
    (home / 'config.yaml').write_text('security: {tirith_enabled: false}\napprovals: {mode: auto}\n')
    monkeypatch.setattr(Path, 'home', lambda: tmp_path)
    monkeypatch.setenv('HERMES_HOME', str(home))
    import hermes_state
    monkeypatch.setattr(hermes_state, 'DEFAULT_DB_PATH', hermes_state._IMPORT_DEFAULT_DB_PATH)
    pin_process_hermes_home(home)
    monkeypatch.setattr(GatewayRunner, '_init_startup_checks', lambda self: None)
    cfg = PlatformConfig(enabled=True, home_channel=HomeChannel(platform=Platform.TELEGRAM, chat_id='123456', name='synthetic'))
    runner = GatewayRunner(GatewayConfig(multiplex_profiles=False, sessions_dir=home / 'sessions',
                                        platforms={Platform.TELEGRAM: cfg}))
    assert runner._session_db._db.db_path.resolve() == home.resolve() / 'state.db'
    adapter = RestartTestAdapter()
    adapter.gateway_runner = runner
    runner.adapters[Platform.TELEGRAM] = adapter
    runner._wire_adapter_handlers(adapter)
    runner._is_user_authorized = lambda _source: True  # Isolated synthetic owner authorization.
    runner._running = True
    entered, finish = asyncio.Event(), asyncio.Event()
    effects = []
    async def inert(event, *_args):
        if hasattr(event, '_heartbeat_execution_started'):
            event._heartbeat_execution_started = True  # Actual inert execution entry, not completion.
        effects.append(event.text)
        entered.set()
        await finish.wait()
        return 'inert completed turn'
    runner._handle_message_with_agent = inert
    try:
        yield SimpleNamespace(runner=runner, adapter=adapter, entered=entered, finish=finish,
                              effects=effects, db=runner._session_db._db, home=home)
    finally:
        finish.set()
        await asyncio.gather(*adapter._background_tasks, return_exceptions=True)
        from hermes_state_registry import close_all_under
        close_all_under(home)
        pin_process_hermes_home(None)


@pytest.mark.asyncio
@pytest.mark.parametrize('prompt', ['inert loop', '/help'])
async def test_closed_loop_never_charges_tick_or_awaiting_and_reopen_retries(native, prompt):
    mgr = LoopManager(session_id='synthetic-loop')
    state = mgr.set(prompt, interval_seconds=60, times=2, route={
        'platform': 'telegram', 'chat_id': '123456', 'chat_type': 'dm', 'user_id': 'u1'})
    before = vars(state).copy()
    native.runner._enter_external_drain()
    try:
        await native.runner._loop_wakeup_fire_one('synthetic-loop', state, time.time(), set())
    except GatewayDraining:
        pass
    persisted = LoopManager(session_id='synthetic-loop').state
    assert persisted.ticks_fired == 0 and not persisted.awaiting_response
    assert vars(persisted) == before and not native.effects
    native.runner._exit_external_drain()
    await native.runner._loop_wakeup_fire_one('synthetic-loop', persisted, time.time(), set())
    native.finish.set()
    await asyncio.gather(*native.adapter._background_tasks)
    assert LoopManager(session_id='synthetic-loop').state.ticks_fired == 1
    assert len(native.effects) == (0 if prompt.startswith('/') else 1)


def fast_watcher_clock(monkeypatch, runner, *, stop_after_tick):
    import gateway.run_adapters as lifecycle
    original_sleep = asyncio.sleep
    async def sleep(seconds):
        if seconds == .001 and stop_after_tick:
            runner._running = False
        await original_sleep(0 if seconds in (5, .001) else seconds)
    class Proxy:
        def __getattr__(self, name):
            return sleep if name == 'sleep' else getattr(asyncio, name)
    monkeypatch.setattr(lifecycle, 'asyncio', Proxy())


@pytest.mark.asyncio
async def test_closed_real_handoff_watcher_preserves_pending_row_and_history(native, monkeypatch):
    native.db.create_session('synthetic-handoff', 'cli')
    native.db.append_message('synthetic-handoff', 'user', 'retained synthetic history')
    assert native.db.request_handoff('synthetic-handoff', 'telegram')
    native.runner._enter_external_drain()
    fast_watcher_clock(monkeypatch, native.runner, stop_after_tick=True)
    await native.runner._handoff_watcher(interval=.001, drain_timeout=1)
    assert native.db.get_handoff_state('synthetic-handoff')['state'] == 'pending'
    assert not native.effects and not native.adapter.sent
    assert native.db.get_messages('synthetic-handoff')[0]['content'] == 'retained synthetic history'


@pytest.mark.asyncio
async def test_cancelled_claim_wait_retains_owned_thread_and_full_handoff_through_closed_core(native, monkeypatch):
    native.db.create_session('synthetic-handoff', 'cli')
    native.db.append_message('synthetic-handoff', 'user', 'retained synthetic history')
    assert native.db.request_handoff('synthetic-handoff', 'telegram')
    claim_entered, claim_release = asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    original = native.db.claim_handoff
    def pending_claim(sid):
        loop.call_soon_threadsafe(claim_entered.set)
        assert claim_release.wait(5)
        return original(sid)
    monkeypatch.setattr(native.db, 'claim_handoff', pending_claim)
    fast_watcher_clock(monkeypatch, native.runner, stop_after_tick=False)
    watcher = asyncio.create_task(native.runner._handoff_watcher(interval=.001, drain_timeout=.01))
    try:
        await asyncio.wait_for(claim_entered.wait(), 5)
        native.runner._enter_external_drain()
        watcher.cancel()
        with pytest.raises(asyncio.CancelledError):
            await watcher
        assert not gateway_admission_status(native.runner)['quiescent']
        assert not native.effects
        claim_release.set()
        await asyncio.wait_for(native.entered.wait(), 5)
        assert native.db.get_handoff_state('synthetic-handoff')['state'] == 'running'
        assert not gateway_admission_status(native.runner)['quiescent']
        native.finish.set()
        for _ in range(200):
            if gateway_admission_status(native.runner)['quiescent']:
                break
            await asyncio.sleep(.01)
        assert native.db.get_handoff_state('synthetic-handoff')['state'] == 'completed'
        assert len(native.effects) == 1 and gateway_admission_status(native.runner)['quiescent']
    finally:
        claim_release.set()
        native.finish.set()
        if not watcher.done():
            watcher.cancel()
        await asyncio.gather(watcher, return_exceptions=True)


@pytest.mark.asyncio
async def test_closed_heartbeat_never_claims_due_state_before_reservation(native, monkeypatch):
    from gateway.session import SessionSource
    from hermes_cli.heartbeat import HeartbeatManager, save_heartbeat
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', chat_type='dm', user_id='u1')
    entry = native.runner.session_store.get_or_create_session(source)
    manager = HeartbeatManager(entry.session_id)
    state = manager.set('inert heartbeat', 60)
    state.created_at -= 120
    save_heartbeat(entry.session_id, state)
    before = state.to_json()
    calls = []
    original = HeartbeatManager.due_prompt
    def claim(self, *args, **kwargs):
        calls.append(self.session_id)
        return original(self, *args, **kwargs)
    monkeypatch.setattr(HeartbeatManager, 'due_prompt', claim)
    watch = {entry.session_key: (source, entry.session_id)}
    native.runner._enter_external_drain()
    await native.runner._heartbeat_poll_once(watch)
    assert not calls and not native.effects
    assert HeartbeatManager(entry.session_id).state.to_json() == before
    native.runner._exit_external_drain()
    await native.runner._heartbeat_poll_once(watch)
    await asyncio.wait_for(native.entered.wait(), 5)
    native.runner._enter_external_drain()
    assert not gateway_admission_status(native.runner)['quiescent']
    native.finish.set()
    await asyncio.gather(*native.adapter._background_tasks)
    await asyncio.sleep(0)
    assert len(native.effects) == 1 and gateway_admission_status(native.runner)['quiescent']
