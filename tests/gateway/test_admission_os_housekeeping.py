"""Actual OS housekeeping loop retains SQLite work and queued coroutine handoff."""
import asyncio
from concurrent.futures import Future
import sqlite3
import threading

import pytest

from gateway import run, run_delivery_queue_watch, run_profile_reconcile
from gateway.admission import gateway_admission_status
from agent.async_utils import safe_schedule_threadsafe
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native


def isolate_chores(monkeypatch, stop, *, ticks=1, keep_directory=False):
    # Keep actual loop/queue watch/profile traversal. All unrelated network/prune boundaries inert.
    names = ['_write_runtime_status_quiet', '_drain_restart_safe_cron_deliveries',
             '_housekeeping_media_caches', '_housekeeping_paste_sweep', '_housekeeping_curator',
             '_housekeeping_skill_sync', '_housekeeping_org_skill_sync',
             '_housekeeping_state_db_maintenance', '_housekeeping_memory_trim',
             '_housekeeping_checkpoint_prune']
    if not keep_directory:
        names.append('_housekeeping_channel_directory')
    for name in names:
        monkeypatch.setattr(run, name, lambda *_a, **_k: None)
    monkeypatch.setattr(run_profile_reconcile, '_mcp_config_reconciler', lambda _r: lambda: None)
    original, count = run_delivery_queue_watch.wait_for_next_tick, 0

    def wait(event, _interval, watch, chore):
        nonlocal count
        count += 1
        if count >= ticks:
            stop.set()
        original(event, .001, watch, chore)

    monkeypatch.setattr(run_delivery_queue_watch, 'wait_for_next_tick', wait)


@pytest.mark.asyncio
@pytest.mark.parametrize('closed_first', [False, True])
async def test_actual_os_tick_fences_and_counts_held_sqlite(native, monkeypatch, closed_first):
    runner, loop = native.runner, asyncio.get_running_loop()
    stop, release, done = threading.Event(), threading.Event(), threading.Event()
    entered, calls = asyncio.Event(), []
    isolate_chores(monkeypatch, stop)

    def retry():
        with sqlite3.connect(native.home / 'state.db') as conn:
            conn.execute('INSERT OR REPLACE INTO state_meta VALUES (?,?)',
                         ('synthetic-housekeeping', 'owned pending SQLite transaction'))
            calls.append('retry')
            loop.call_soon_threadsafe(entered.set)
            assert release.wait(5)
        return False

    monkeypatch.setattr(native.db, 'retry_deferred_fts_recovery', retry)
    if closed_first:
        runner._ingress_barrier.close()
        release.set()

    def housekeeping():
        try:
            run._start_gateway_housekeeping(stop, runner=runner, loop=loop, interval=.001)
        finally:
            done.set()

    thread = threading.Thread(target=housekeeping, name='synthetic-housekeeping-loop')
    thread.start()
    try:
        if closed_first:
            assert await asyncio.to_thread(done.wait, 5)
            assert not calls
        else:
            await asyncio.wait_for(entered.wait(), 5)
            runner._ingress_barrier.close()
            assert not gateway_admission_status(runner)['quiescent']
    finally:
        release.set()
        stop.set()
        assert await asyncio.to_thread(done.wait, 5)
        thread.join()
    assert calls == ([] if closed_first else ['retry'])
    assert not native.effects


@pytest.mark.asyncio
async def test_submitted_handoff_tracking_failure_stays_unknown_without_cancel_or_replay(native, monkeypatch):
    from gateway.admission import native_housekeeping_work
    runner, loop = native.runner, asyncio.get_running_loop()
    track, futures, errors = runner._track_deferred_agent_worker, [], []
    done = threading.Event()

    def fail_handoff(future, agent):
        if isinstance(future, Future) and not future.running():
            futures.append(future)
            raise RuntimeError('synthetic accepted handoff registry refusal')
        return track(future, agent)

    async def accepted():
        assert native.db.get_meta('synthetic-handoff-effect') is None
        native.db.set_meta('synthetic-handoff-effect', 'once')
        return 'owned synthetic effect'

    def submit():
        try:
            with native_housekeeping_work(runner) as admitted:
                assert admitted
                safe_schedule_threadsafe(accepted(), loop)
        except RuntimeError as exc:
            errors.append(str(exc))
        finally:
            done.set()

    monkeypatch.setattr(runner, '_track_deferred_agent_worker', fail_handoff)
    thread = threading.Thread(target=submit)
    thread.start()
    try:
        assert await asyncio.to_thread(done.wait, 5)
        assert errors == ['synthetic accepted handoff registry refusal']
        assert len(futures) == 1 and not futures[0].cancelled()
        assert await asyncio.wrap_future(futures[0]) == 'owned synthetic effect'
        assert native.db.get_meta('synthetic-handoff-effect') == 'once'
        snapshot = gateway_admission_status(runner)
        assert snapshot['known'] is False and snapshot['quiescent'] is None
    finally:
        thread.join()


@pytest.mark.asyncio
async def test_real_queued_coroutine_remains_owned_after_os_parent_timeout(native, monkeypatch):
    runner, loop = native.runner, asyncio.get_running_loop()
    stop, done, futures = threading.Event(), threading.Event(), []
    isolate_chores(monkeypatch, stop, ticks=5, keep_directory=True)
    monkeypatch.setattr(run, '_housekeeping_deferred_fts_retry', lambda: None)
    schedule, refresh = run.safe_schedule_threadsafe, run._housekeeping_channel_directory

    def bounded_schedule(*args, **kwargs):
        future = schedule(*args, **kwargs)  # Actual run_coroutine_threadsafe; loop deliberately occupied below.
        result = future.result

        def short_result(timeout=None):
            return result(timeout=.001)  # Collapse only existing 30s parent wait, no fake coroutine/Future.

        future.result = short_result
        futures.append(future)
        return future

    def bounded_refresh(*args, **kwargs):
        try:
            return refresh(*args, **kwargs)
        finally:
            stop.set()

    monkeypatch.setattr(run, 'safe_schedule_threadsafe', bounded_schedule)
    monkeypatch.setattr(run, '_housekeeping_channel_directory', bounded_refresh)

    def housekeeping():
        try:
            run._start_gateway_housekeeping(stop, runner=runner, adapters=runner.adapters,
                                           loop=loop, interval=.001)
        finally:
            done.set()

    thread = threading.Thread(target=housekeeping, name='synthetic-queued-housekeeping-loop')
    thread.start()
    try:
        assert done.wait(5)  # Actual loop cannot start accepted coroutine before parent has timed out.
        assert len(futures) == 1 and not futures[0].done()
        runner._ingress_barrier.close()
        assert not gateway_admission_status(runner)['quiescent']
    finally:
        stop.set()
        for future in futures:
            await asyncio.wrap_future(future)
        assert await asyncio.to_thread(done.wait, 5)
        thread.join()
    assert (native.home / 'channel_directory.json').exists()
    assert not native.effects
