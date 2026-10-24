"""Finite native daemon keeps ownership through tail and ambiguous start, without replay."""
import asyncio
import sqlite3
import threading

import pytest

from gateway import admission
from gateway.admission import gateway_admission_status
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native


@pytest.mark.asyncio
async def test_ambiguous_finished_thread_start_never_replays_target(native, monkeypatch):
    original = threading.Thread
    path = native.home / 'synthetic-finished-start.db'

    class FinishedThenError(original):
        def start(self):
            super().start()
            if self.name == 'synthetic-ambiguous-start':
                self.join(5)
                assert not self.is_alive()
                raise RuntimeError('synthetic start reply lost after physical completion')

    def commit():
        with sqlite3.connect(path) as conn:
            conn.execute('CREATE TABLE IF NOT EXISTS attempts (text TEXT)')
            conn.execute('INSERT INTO attempts VALUES (?)', ('exact synthetic attempt',))

    monkeypatch.setattr(admission.threading, 'Thread', FinishedThenError)
    native.runner._spawn_release_thread(commit, (), 'synthetic-ambiguous-start', inline_fallback=True)
    with sqlite3.connect(path) as conn:
        assert conn.execute('SELECT text FROM attempts').fetchall() == [('exact synthetic attempt',)]


@pytest.mark.asyncio
async def test_native_daemon_future_done_cannot_hide_physical_tail(native, monkeypatch):
    original = threading.Thread
    entered, release, completed = threading.Event(), threading.Event(), threading.Event()

    class PhysicalTail(original):
        def run(self):
            try:
                super().run()
                if self.name == 'synthetic-physical-tail':
                    entered.set()
                    assert release.wait(5)
            finally:
                completed.set()

    monkeypatch.setattr(admission.threading, 'Thread', PhysicalTail)
    native.runner._spawn_release_thread(lambda: None, (), 'synthetic-physical-tail', inline_fallback=False)
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        native.runner._ingress_barrier.close()
        assert not gateway_admission_status(native.runner)['quiescent']
        assert any(future.done() for future in native.runner._deferred_agent_workers)
    finally:
        release.set()
        assert await asyncio.to_thread(completed.wait, 5)
        for _ in range(500):
            if gateway_admission_status(native.runner)['quiescent']:
                break
            await asyncio.sleep(.01)
    assert gateway_admission_status(native.runner)['quiescent']


@pytest.mark.asyncio
async def test_lost_start_reply_cannot_settle_still_running_native_future(native, monkeypatch):
    original = threading.Thread
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()

    class StartedThenError(original):
        def start(self):
            super().start()
            if self.name == 'synthetic-running-start':
                assert entered.wait(5)
                raise RuntimeError('synthetic start reply lost while worker remains alive')

    def commit():
        try:
            entered.set()
            assert release.wait(5)
            native.db.set_meta('synthetic-start-commit', 'once')
        finally:
            finished.set()

    monkeypatch.setattr(admission.threading, 'Thread', StartedThenError)
    native.runner._spawn_release_thread(commit, (), 'synthetic-running-start', inline_fallback=True)
    try:
        native.runner._ingress_barrier.close()
        assert not gateway_admission_status(native.runner)['quiescent']
        assert not any(future.done() for future in native.runner._deferred_agent_workers)
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 5)
        for _ in range(500):
            if gateway_admission_status(native.runner)['quiescent']:
                break
            await asyncio.sleep(.01)
    assert native.db.get_meta('synthetic-start-commit') == 'once'
