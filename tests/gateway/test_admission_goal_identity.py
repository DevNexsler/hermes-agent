"""Queued goal ownership survives replacement without adopting its successor."""
import asyncio
import json

import pytest

from gateway.admission import gateway_admission_status, gateway_ingress
from gateway.config import Platform
from gateway.platforms.event import MessageEvent
from gateway.session import SessionSource
from hermes_cli.goals import GoalManager
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native


@pytest.mark.asyncio
@pytest.mark.parametrize('enqueue', ['continuation', 'command'])
@pytest.mark.parametrize('replacement', ['different', 'same_text', 'paused', 'malformed'])
@pytest.mark.parametrize('entry_path', ['defer', 'shutdown', 'cancel_before_step'])
async def test_replaced_goal_never_adopts_old_continuation_but_retains_exact_obligation(
        native, monkeypatch, enqueue, replacement, entry_path):
    runner, adapter = native.runner, native.adapter
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    session = runner.session_store.get_or_create_session(source)
    mgr = GoalManager(session.session_id)
    mgr.set('ORIGINAL goal G1')
    if enqueue == 'command':
        mgr.pause()
    monkeypatch.setattr('hermes_cli.goals.judge_goal', lambda *a, **k: ('continue', 'more', False, None, False))
    with gateway_ingress(runner):
        if enqueue == 'continuation':
            await runner._post_turn_goal_continuation(session_entry=session, source=source,
                                                      final_response='EXACT original G1 result')
        else:
            await runner._handle_goal_command(MessageEvent(text='/goal resume', source=source, message_id='exact-owned-G1-message'))
    key = runner._session_key_for_source(source)
    event = adapter._pending_messages[key]
    event.message_id = 'exact-owned-G1-message'
    expected = json.loads(GoalManager(session.session_id).state.to_json())
    original_prompt = event.text
    successor = GoalManager(session.session_id)
    successor.set('ORIGINAL goal G1' if replacement == 'same_text' else 'REPLACEMENT goal G2')
    if replacement == 'paused':
        successor.pause(reason='new-user-owner-pause')
    if replacement == 'malformed':
        native.db.set_meta('goal:' + session.session_id, 'malformed foreign goal')
    replacement_raw = native.db.get_meta('goal:' + session.session_id)
    runner._enter_external_drain()
    try:
        if entry_path == 'defer':
            await runner._defer_queued_goal_for_maintenance(event)
        elif entry_path == 'shutdown':
            await runner._run_agent_drain_pending({'final_response': ''}, adapter, source, key)
        else:
            adapter._pending_messages.pop(key)
            adapter._spawn_drain_task(event, key)
            task = adapter._session_tasks[key]
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await asyncio.gather(*getattr(runner, '_deferred_agent_cleanup_tasks', ()), return_exceptions=True)
    except RuntimeError:
        pass  # Refusal may propagate; durable exact handoff and successor preservation required.
    assert native.db.get_meta('goal:' + session.session_id) == replacement_raw
    rows = native.db.list_meta_prefix('goal_maintenance_pause:' + session.session_id + ':')
    assert len(rows) == 1
    record = json.loads(rows[0][1])
    assert record['checkpoint'] == expected
    assert record['continuation_prompt'] == original_prompt
    assert record['final_response'] == ('EXACT original G1 result' if enqueue == 'continuation' else '')
    assert record['input_context']['message_id'] == 'exact-owned-G1-message'
    assert record['pause_applied'] is False and record['outcome'] == 'UNKNOWN'
    assert record['required_action'] and record['owner']
    assert gateway_admission_status(runner)['known'] is False and not native.effects
    from hermes_state import SessionDB
    reopened = SessionDB(db_path=native.home / 'state.db')
    try:
        assert reopened.get_meta('goal:' + session.session_id) == replacement_raw
        assert json.loads(reopened.get_meta(rows[0][0])) == record
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_new_goal_identity_does_not_use_clock_or_prompt_text(native, monkeypatch):
    import hermes_cli.goals as goals
    real_clock = goals.time
    class FrozenClock:
        time = staticmethod(lambda: 1700000000.0)
        def __getattr__(self, name):
            return getattr(real_clock, name)
    monkeypatch.setattr(goals, 'time', FrozenClock())
    mgr = GoalManager('synthetic-generation')
    first = mgr.set('same goal text').to_json()
    second = mgr.set('same goal text').to_json()
    assert first != second


@pytest.mark.asyncio
async def test_goal_replace_between_lookup_and_pause_transaction_retains_old_obligation(native, monkeypatch):
    import sqlite3
    from hermes_cli.goals import GoalState
    mgr = GoalManager('synthetic-cas-owner')
    original = mgr.set('ORIGINAL G1').to_json()
    successor = GoalState.from_json(original)
    successor.goal = 'NEW G2 committed after lookup'
    successor.generation_id = 'foreign-generation'
    replacement = successor.to_json()
    real_write = native.db._execute_write
    raced = False
    def concurrent_write(callback, *args, **kwargs):
        nonlocal raced
        if not raced:
            raced = True
            with sqlite3.connect(native.home / 'state.db') as other:
                other.execute('UPDATE state_meta SET value=? WHERE key=?', (replacement, 'goal:' + mgr.session_id))
        return real_write(callback, *args, **kwargs)
    monkeypatch.setattr(native.db, '_execute_write', concurrent_write)
    native.runner._enter_external_drain()
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    assert await native.runner._defer_goal_for_maintenance(mgr, source, 'EXACT G1 continuation',
                                                          'EXACT G1 result', expected_goal=original)
    assert native.db.get_meta('goal:' + mgr.session_id) == replacement
    row, = native.db.list_meta_prefix('goal_maintenance_pause:' + mgr.session_id + ':')
    receipt = json.loads(row[1])
    assert receipt['pause_applied'] is False and receipt['checkpoint'] == json.loads(original)
    assert receipt['observed_goal'] == json.loads(replacement)
    assert receipt['continuation_prompt'] == 'EXACT G1 continuation'
    assert not gateway_admission_status(native.runner)['known']


@pytest.mark.asyncio
@pytest.mark.parametrize('boundary', ['open_fifo', 'after_preparation'])
async def test_replaced_goal_never_executes_under_open_fence_or_after_turn_preparation(native, monkeypatch, boundary):
    from gateway.run import GatewayRunner
    runner, adapter = native.runner, native.adapter
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    session = runner.session_store.get_or_create_session(source)
    GoalManager(session.session_id).set('ORIGINAL G1')
    monkeypatch.setattr('hermes_cli.goals.judge_goal', lambda *a, **k: ('continue', 'more', False, None, False))
    with gateway_ingress(runner):
        await runner._post_turn_goal_continuation(session_entry=session, source=source, final_response='EXACT result')
    key = runner._session_key_for_source(source)
    event = adapter._pending_messages.pop(key)
    replacement = None
    def replace_goal():
        nonlocal replacement
        GoalManager(session.session_id).set('NEW G2')
        replacement = native.db.get_meta('goal:' + session.session_id)
    async def prepare(*args):
        replace_goal()
        return runner._PreparedTurn([], '', event.text, event.text, None, None), {}
    async def model(**kwargs):
        native.effects.append(kwargs['message'])
        raise asyncio.CancelledError
    if boundary == 'open_fifo':
        replace_goal()
    else:
        runner._handle_message_with_agent = GatewayRunner._handle_message_with_agent.__get__(runner)
        monkeypatch.setattr(runner, '_hmwa_prepare_turn', prepare)
        monkeypatch.setattr(runner, '_run_agent', model)
    native.finish.set()
    adapter._spawn_drain_task(event, key)
    await asyncio.gather(*adapter._background_tasks, return_exceptions=True)
    assert native.effects == []
    assert native.db.get_meta('goal:' + session.session_id) == replacement
    row, = native.db.list_meta_prefix('goal_maintenance_pause:' + session.session_id + ':')
    record = json.loads(row[1])
    assert record['checkpoint']['goal'] == 'ORIGINAL G1' and record['pause_applied'] is False
    assert record['outcome'] == 'UNKNOWN' and not gateway_admission_status(runner)['known']
