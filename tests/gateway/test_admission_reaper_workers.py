"""Inert process-reap boundary still owns actual native daemon lifetime."""
import asyncio
from types import SimpleNamespace
import threading

import pytest

from gateway import run
from gateway.admission import gateway_admission_status
from gateway.platforms import api_server
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['gateway', 'api'])
async def test_actual_interrupt_reaper_is_counted_through_physical_settlement(native, monkeypatch, kind):
    runner, loop = native.runner, asyncio.get_running_loop()
    entered, release, finished = asyncio.Event(), threading.Event(), threading.Event()
    agent = SimpleNamespace(_gateway_turn_process_task_id='synthetic-reaper',
                            _gateway_turn_process_baseline=frozenset())

    def reap(*_args, **_kwargs):
        try:
            # Inert signal boundary; actual native caller/thread and real synthetic SQLite write.
            native.db.set_meta('synthetic-reaper-before', 'retained input')
            loop.call_soon_threadsafe(entered.set)
            assert release.wait(5)
            native.db.set_meta('synthetic-reaper-after', 'physical worker settled')
        finally:
            finished.set()

    monkeypatch.setattr(run, '_reap_gateway_turn_processes', reap)
    monkeypatch.setattr(run, 'request_hard_interrupt', lambda *_a, **_k: None)
    if kind == 'api':
        api_server._reap_disconnected_agent_processes(agent)
    else:
        key = 'agent:main:telegram:dm:synthetic-reaper'
        state = runner._session_state(key)
        state.turn.agent = agent
        runner._interrupt_running_turn(key, interrupt_reason='synthetic only', invalidation_reason='synthetic only')
        runner._release_running_agent_state(key)
    try:
        await asyncio.wait_for(entered.wait(), 5)
        runner._ingress_barrier.close()
        assert not gateway_admission_status(runner)['quiescent']
        assert native.db.get_meta('synthetic-reaper-before') == 'retained input'
        assert native.db.get_meta('synthetic-reaper-after') is None
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 5)
        for _ in range(500):
            if gateway_admission_status(runner)['quiescent']:
                break
            await asyncio.sleep(.01)
    assert gateway_admission_status(runner)['quiescent']
    assert native.db.get_meta('synthetic-reaper-after') == 'physical worker settled'
    assert not native.effects
