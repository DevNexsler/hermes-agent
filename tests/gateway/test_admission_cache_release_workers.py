"""Native TTL/LRU/pressure release owns actual memory commit until physical settlement."""
import asyncio
from collections import OrderedDict
import sqlite3
import threading
import time

import pytest

from agent.memory_manager import MemoryManager
from gateway import agent_cache_pressure, run_watchers
from gateway.admission import gateway_admission_status
from gateway.admission import GatewayDraining
from gateway.agent_cache_pressure import AgentCacheBounds
from hermes_constants import get_hermes_home
from run_agent import AIAgent
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['ttl', 'lru', 'pressure'])
@pytest.mark.parametrize('cancel', [False, True])
async def test_native_eviction_keeps_real_commit_worker(native, monkeypatch, kind, cancel):
    runner, loop = native.runner, asyncio.get_running_loop()
    entered, release, finished = asyncio.Event(), threading.Event(), threading.Event()
    seen, closed = [], threading.Event()

    class Provider:
        name = 'synthetic-sqlite-memory'

        def on_session_end(self, messages):
            home = get_hermes_home()
            try:
                with sqlite3.connect(home / 'synthetic-memory.db') as conn:
                    conn.execute('CREATE TABLE IF NOT EXISTS memory (text TEXT)')
                    loop.call_soon_threadsafe(entered.set)
                    assert release.wait(5)
                    conn.execute('INSERT INTO memory VALUES (?)', (messages[0]['content'],))
                    seen.append(home)
            finally:
                finished.set()

    manager = MemoryManager()
    manager._providers = [Provider()]  # Inert extraction boundary, actual manager/agent/SQLite.
    agent = object.__new__(AIAgent)
    agent._memory_manager = manager
    agent._session_messages = [{'role': 'user', 'content': 'exact retained synthetic memory'}]
    agent._db_flush_scan_prefix = list(agent._session_messages)
    agent._last_flushed_db_idx = len(agent._session_messages)
    agent._last_activity_ts = time.time() - 100
    agent.release_clients = closed.set  # No provider/network/client created.
    runner._agent_cache = OrderedDict([('agent:main:telegram:dm:synthetic', (agent, 'same-prompt'))])
    bounds = AgentCacheBounds(max_size=1, idle_ttl_secs=1,
                              memory_high_mb=1 if kind == 'pressure' else None,
                              max_evictions_per_pass=1, protect_recent=0)
    monkeypatch.setattr(runner, '_agent_cache_bounds', lambda: bounds)
    monkeypatch.setattr(agent_cache_pressure, 'read_anon_rss_mb', lambda: 2)

    class Clock:
        def __getattr__(self, name):
            return self.sleep if name == 'sleep' else getattr(asyncio, name)

        async def sleep(self, delay):
            await asyncio.sleep(0 if delay == 60 else delay)

    monkeypatch.setattr(run_watchers, 'asyncio', Clock())
    if kind == 'pressure':
        agent._last_activity_ts = time.time()  # Pressure, not TTL, must select this row.
    if kind == 'lru':
        runner._agent_cache['agent:main:telegram:dm:new'] = (None, 'new')

        async def cap():
            with runner._agent_cache_lock:
                runner._enforce_agent_cache_cap()
            await asyncio.Event().wait()
        factory = cap
    else:
        factory = runner._session_housekeeping_watcher
    task = runner._spawn_supervised(factory, 'synthetic cache ownership', restart=False)
    try:
        await asyncio.wait_for(entered.wait(), 5)
        runner._ingress_barrier.close()
        assert not gateway_admission_status(runner)['quiescent']
        if cancel:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            assert not gateway_admission_status(runner)['quiescent']
        release.set()
        assert await asyncio.to_thread(closed.wait, 5)
        assert await asyncio.to_thread(finished.wait, 5)
        assert seen == [native.home] and not native.effects
        with sqlite3.connect(native.home / 'synthetic-memory.db') as conn:
            assert conn.execute('SELECT text FROM memory').fetchall() == [('exact retained synthetic memory',)]
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert await asyncio.to_thread(finished.wait, 5)
        assert await asyncio.to_thread(closed.wait, 5)
        for _ in range(500):
            if gateway_admission_status(runner)['quiescent']:
                break
            await asyncio.sleep(.01)
    assert gateway_admission_status(runner)['quiescent']


@pytest.mark.asyncio
async def test_closed_housekeeping_retains_cached_memory_work(native):
    from types import SimpleNamespace
    calls = []
    agent = SimpleNamespace(_last_activity_ts=0, _memory_manager=object(), _session_messages=[],
                            commit_memory_session=lambda _messages: calls.append('commit'),
                            release_clients=lambda: calls.append('release'))
    key = 'agent:main:telegram:dm:retained'
    native.runner._agent_cache = OrderedDict([(key, (agent, 'unchanged prompt'))])
    native.runner._ingress_barrier.close()
    try:
        await native.runner._session_housekeeping()
    except GatewayDraining as exc:
        assert exc.pre_effect is True
    assert key in native.runner._agent_cache
    assert not calls and not native.effects
