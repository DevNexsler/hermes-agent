"""Accepted goal preservation has one writer; automatic observers never overwrite controls."""
import asyncio
import json
import threading

import pytest

from gateway.admission import gateway_admission_status
from hermes_cli import goals, loops
from hermes_state import SessionDB
from tests.gateway import test_admission_native_work
from tests.gateway.test_admission_goal_terminal_exit import accepted_goal
from tests.gateway.test_admission_loop_storage import settle
from tests.gateway.test_admission_loop_termination import install_real_turn

native = test_admission_native_work.native


@pytest.mark.asyncio
@pytest.mark.parametrize('stage', ['base', 'agent_start', 'capacity'])
@pytest.mark.parametrize('cancel', [False, True])
async def test_earlier_goal_writer_never_restarts_after_cancel(native, monkeypatch, stage, cancel):
    runner, adapter = native.runner, native.adapter
    _source, entry, event = await accepted_goal(native, monkeypatch)
    sid, key = entry.session_id, entry.session_key
    turn = install_real_turn(native, monkeypatch, 'hook' if stage == 'agent_start' else 'normal')
    entered, physical = asyncio.Event(), threading.Event()
    loop, statements = asyncio.get_running_loop(), []

    def trace(sql):
        if sql.startswith('INSERT INTO state_meta') and 'goal_maintenance_pause:' in sql:
            statements.append(sql)
            loop.call_soon_threadsafe(entered.set)
            assert physical.wait(5)

    native.db._conn.set_trace_callback(trace)
    if stage == 'base':
        original = runner._defer_queued_goal_for_maintenance
        async def close_at_base(*args, **kwargs):
            runner._ingress_barrier.close()
            return await original(*args, **kwargs)
        monkeypatch.setattr(runner, '_defer_queued_goal_for_maintenance', close_at_base)
    elif stage == 'capacity':
        runner._ingress_barrier.close()
    adapter._spawn_drain_task(event, key)
    task = adapter._session_tasks[key]
    try:
        if stage == 'agent_start':
            await asyncio.wait_for(turn.entered.wait(), 5)
            runner._ingress_barrier.close()
            turn.proceed.set()
        await asyncio.wait_for(entered.wait(), 5)
        assert not gateway_admission_status(runner)['quiescent']
        if cancel:
            if stage == 'agent_start':
                await adapter.handle_message(runner._synthetic_prompt_event(event.source, '/stop'))
            else:
                task.cancel()
            await asyncio.sleep(0)
            assert not gateway_admission_status(runner)['quiescent']
            assert (runner._ingress_barrier.snapshot()['adopted']
                    or any(not future.done() and not isinstance(future, asyncio.Task)
                           for future in runner._deferred_agent_workers))
        physical.set()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.gather(*getattr(runner, '_deferred_agent_cleanup_tasks', ()), return_exceptions=True)
        await settle(native)
        assert len(statements) == 1  # One accepted envelope cannot acquire a second metadata attempt.
        row, = native.db.list_meta_prefix('goal_maintenance_pause:' + sid + ':')
        reopened = SessionDB(db_path=native.db.db_path)
        try:
            record = json.loads(reopened.get_meta(row[0]))
            assert record['pause_applied'] is True
            assert record['checkpoint'] == json.loads(event._goal_admission_checkpoint['goal_state'])
            assert record['continuation_prompt'] == event.text
            assert record['input_context']['raw_message'] == event.raw_message
            assert record['final_response'] == 'EXACT prior delivered result'
        finally:
            reopened.close()
        assert not turn.effects and not adapter._pending_messages
        assert gateway_admission_status(runner)['known'] is (not cancel)
    finally:
        turn.proceed.set()
        physical.set()
        await asyncio.gather(task, return_exceptions=True)
        await settle(native)
        native.db._conn.set_trace_callback(None)


@pytest.mark.asyncio
@pytest.mark.parametrize('control', ['pause', 'clear', 'unchanged'])
@pytest.mark.parametrize('waiting', [False, True])
async def test_loop_goal_predicate_cannot_save_over_user_control(native, monkeypatch, control, waiting):
    sid = 'EXACT loop-observed goal'
    mgr = goals.GoalManager(sid)
    mgr.set('EXACT user-owned goal')
    mgr.wait_on_session('EXACT completed observer barrier')
    entered, release = asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    def observe(_session):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        return waiting
    monkeypatch.setattr(goals, '_session_waiting', observe)
    task = asyncio.create_task(native.runner._run_in_executor_with_context(
        lambda: loops.goal_blocks_loop_tick(sid, strict=True)))
    await asyncio.wait_for(entered.wait(), 5)
    other = SessionDB(db_path=native.db.db_path)
    try:
        state = json.loads(other.get_meta('goal:' + sid))
        if control != 'unchanged':
            with monkeypatch.context() as user_scope:
                user_scope.setattr(goals, '_get_session_db', lambda: other)
                user = goals.GoalManager(sid)
                if control == 'pause':
                    user.pause('EXACT authoritative user control')
                else:
                    user.clear()
            assert state['status'] == 'active'
        expected = other.get_meta('goal:' + sid)
        release.set()
        assert await task is (not waiting)
        assert other.get_meta('goal:' + sid) == expected
        assert native.db.get_meta('goal:' + sid) == expected
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        other.close()
