"""Actual cache-release worker preserves two-home A→B→A secrets and SQLite writes."""
import asyncio
from pathlib import Path
import sqlite3
import threading

import pytest

from agent import secret_scope
from gateway.admission import gateway_admission_status
from hermes_constants import get_hermes_home
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native


@pytest.mark.asyncio
async def test_real_release_worker_profiles_a_b_a(native):
    runner = native.runner
    runner.config.multiplex_profiles = True
    second = native.home / 'profiles' / 'b'
    second.mkdir(parents=True)
    (second / 'config.yaml').write_text('{}\n')
    for home, label in ((native.home, 'A'), (second, 'B')):
        (home / '.env').write_text('SYNTHETIC_NATIVE_OWNER=' + label + '\n')
    sequence = [(native.home, 'agent:main:telegram:dm:a'),
                (second, 'agent:b:telegram:dm:b'), (native.home, 'agent:main:telegram:dm:a')]
    assert runner.session_store._profile_home_for_key(sequence[1][1]) == second
    entered, seen, releases, workers = asyncio.Queue(), [], [], []
    loop = asyncio.get_running_loop()
    old_active = secret_scope.is_multiplex_active()
    secret_scope.set_multiplex_active(True)

    def write(label, release):
        workers.append(threading.current_thread())
        home = Path(get_hermes_home())
        with sqlite3.connect(home / 'synthetic-release.db') as conn:
            conn.execute('CREATE TABLE IF NOT EXISTS seen (label TEXT)')
            assert secret_scope.get_secret('SYNTHETIC_NATIVE_OWNER') == label
            loop.call_soon_threadsafe(entered.put_nowait, home)
            assert release.wait(5)
            conn.execute('INSERT INTO seen VALUES (?)', (label,))
            seen.append(home)

    try:
        for index, (home, key) in enumerate(sequence):
            label = 'B' if home == second else 'A'
            release = threading.Event()
            releases.append(release)
            runner._spawn_release_thread(write, (label, release), 'synthetic-profile-' + str(index),
                                         inline_fallback=False, session_key=key)
            assert await asyncio.wait_for(entered.get(), 5) == home
            assert gateway_admission_status(runner)['active_work'] > 0
            release.set()
            for _ in range(500):
                if runner._active_deferred_agent_worker_count() == 0:
                    break
                await asyncio.sleep(.01)
        assert seen == [home for home, _ in sequence]
        for home, expected in ((native.home, [('A',), ('A',)]), (second, [('B',)])):
            with sqlite3.connect(home / 'synthetic-release.db') as conn:
                assert conn.execute('SELECT label FROM seen').fetchall() == expected
        assert not native.effects
    finally:
        for release in releases:
            release.set()
        for _ in range(500):
            if runner._active_deferred_agent_worker_count() == 0:
                break
            await asyncio.sleep(.01)
        for worker in workers:
            await asyncio.to_thread(worker.join, 5)
            assert not worker.is_alive()
        secret_scope.set_multiplex_active(old_active)
