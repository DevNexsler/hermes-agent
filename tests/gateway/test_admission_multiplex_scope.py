"""Ingress executor work preserves real profile home, secret and terminal scopes A→B→A."""

import asyncio
from pathlib import Path
import threading

import pytest

from agent.secret_scope import get_secret, is_multiplex_active, set_multiplex_active
from gateway.admission import gateway_ingress, ingress_to_thread, initialize_gateway_admission
from gateway.run import _profile_runtime_scope
from hermes_constants import get_hermes_home, get_routing_process_hermes_home, pin_process_hermes_home
from tests.gateway.restart_test_helpers import make_restart_runner
from tools.terminal_scope import terminal_env


@pytest.mark.asyncio
async def test_real_multiplex_thread_scope_survives_handoff_and_cancel(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, 'home', lambda: tmp_path)
    homes = [tmp_path / label for label in ('A', 'B')]
    for home in homes:
        home.mkdir()
        (home / '.env').write_text(f'ADMISSION_TEST_SECRET={home.name}\n', encoding='utf-8')
        (home / 'config.yaml').write_text(
            f'terminal:\n  cwd: {home / "terminal"}\n', encoding='utf-8')
    monkeypatch.setenv('HERMES_HOME', str(homes[0]))
    pin_process_hermes_home(homes[0])
    set_multiplex_active(True)
    runner, _adapter = make_restart_runner()
    expected = []

    def observe():
        assert is_multiplex_active()
        return (get_hermes_home(), get_secret('ADMISSION_TEST_SECRET'), terminal_env('TERMINAL_CWD'))

    try:
        for home in (homes[0], homes[1], homes[0]):
            with _profile_runtime_scope(home):
                assert get_routing_process_hermes_home() == homes[0]
                with gateway_ingress(runner):
                    got = await ingress_to_thread(observe)
                assert got == (home, home.name, str(home / 'terminal'))
                assert runner._ingress_barrier.home == homes[0]
                expected.append(got)
        started, exited, finish = asyncio.Event(), asyncio.Event(), threading.Event()
        after = []
        loop = asyncio.get_running_loop()

        def pending():
            try:
                before = observe()
                loop.call_soon_threadsafe(started.set)
                assert finish.wait(5)
                after.append(observe())
                assert after[-1] == before == expected[1]
            finally:
                loop.call_soon_threadsafe(exited.set)

        async def routed_handler():
            with _profile_runtime_scope(homes[1]), gateway_ingress(runner):
                await ingress_to_thread(pending)

        task = asyncio.create_task(routed_handler())
        try:
            await asyncio.wait_for(started.wait(), 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert runner._ingress_barrier.snapshot()['adopted'] == 1
            with _profile_runtime_scope(homes[0]), gateway_ingress(runner):
                assert await ingress_to_thread(observe) == expected[0]
        finally:
            finish.set()
            await asyncio.wait_for(exited.wait(), 5)
        assert after == [expected[1]]
    finally:
        set_multiplex_active(False)
        pin_process_hermes_home(None)
