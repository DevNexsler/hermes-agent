"""Bounded startup gate cannot discard actual launch hydration/probe worker ownership."""
import asyncio
import threading

import pytest

from gateway import run
from gateway.admission import gateway_admission_status
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native


@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['hydration', 'probe'])
@pytest.mark.parametrize('cancel', [False, True])
async def test_actual_startup_gate_release_keeps_physical_warmup_owned(native, monkeypatch, phase, cancel):
    runner = native.runner
    runner.config.multiplex_profiles = True
    runner._startup_restore_in_progress = True
    runner._startup_restore_tasks = []
    runner._startup_restore_queue = []
    entered, release, finished = asyncio.Event(), threading.Event(), threading.Event()
    event_loop, loader = asyncio.get_running_loop(), run._load_profile_secret_scope
    def held(function, *args):
        event_loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        try:
            return function(*args)
        finally:
            finished.set()
    def hydrate(home):
        return held(loader, home) if phase == 'hydration' else loader(home)
    def probe():
        return held(lambda: 1) if phase == 'probe' else 1  # Inert warm-up boundary, no providers/tools.
    monkeypatch.setattr(run, '_load_profile_secret_scope', hydrate)
    monkeypatch.setattr(run, '_warm_turn_machinery_sync', probe)
    monkeypatch.setattr(run, '_startup_warmup_timeout_secs', lambda: .001)
    runner._start_startup_warmup()
    task = runner._startup_warmup_task
    try:
        await asyncio.wait_for(entered.wait(), 5)
        await runner._finish_startup_restore()  # Actual bounded release; no manufactured flag clear.
        assert not runner._startup_restore_in_progress and not task.done()
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
        assert not native.effects
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        assert await asyncio.to_thread(finished.wait, 5)
        runner._startup_restore_in_progress = False
