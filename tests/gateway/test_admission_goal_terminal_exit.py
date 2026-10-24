"""Every accepted goal exit retains execution or its original continuation obligation."""
import asyncio
import json
import threading

import pytest

from gateway.admission import GatewayDraining, gateway_admission_status, gateway_ingress, startup_resume_ingress
from hermes_cli import goals, plugins
from hermes_state import SessionDB
from tests.gateway import test_admission_native_work
from tests.gateway.test_admission_goal_nested_and_routing import original_goal
from tests.gateway.test_admission_loop_storage import settle
from tests.gateway.test_admission_loop_termination import install_real_turn

native = test_admission_native_work.native


async def accepted_goal(native, monkeypatch, *, startup=False):
    runner, adapter = native.runner, native.adapter
    source, entry = original_goal(native)
    key = entry.session_key
    monkeypatch.setattr(goals, 'judge_goal', lambda *a, **k: ('continue', 'more', False, None, False))
    owner = runner._synthetic_prompt_event(source, '', internal=True)
    if startup:
        runner._startup_restore_in_progress = True
    with (startup_resume_ingress(runner, owner, key) if startup else gateway_ingress(runner)):
        await runner._post_turn_goal_continuation(session_entry=entry, source=source,
                                                  final_response='EXACT prior delivered result')
    event = adapter._pending_messages.pop(key)
    event.message_id = 'EXACT accepted envelope message'
    event.channel_context = {'obligation': 'EXACT retained continuation inputs'}
    event.raw_message = {'payload': 'EXACT original continuation payload'}
    return source, entry, event


def register_dispatch_hook(monkeypatch, callback):
    manager = plugins.PluginManager()
    manager._discovered = True  # Isolated registry; actual lifecycle/plugin dispatch remains real.
    context = plugins.PluginContext(plugins.PluginManifest(name='goal-terminal-probe', source='user'), manager)
    context.register_hook('pre_gateway_dispatch', callback)
    monkeypatch.setattr(plugins, 'get_plugin_manager', lambda: manager)


@pytest.mark.asyncio
@pytest.mark.parametrize('outcome', ['skip_after_close', 'skip_while_open', 'authorization_after_close',
                                    'preparation_after_close', 'actual_entry', 'startup_entry'])
async def test_real_lifecycle_goal_skip_cannot_drop_accepted_envelope(native, monkeypatch, outcome):
    runner, adapter = native.runner, native.adapter
    startup = outcome == 'startup_entry'
    source, entry, event = await accepted_goal(native, monkeypatch, startup=startup)
    actual_entry = outcome in ('actual_entry', 'startup_entry')
    sid, key = entry.session_id, entry.session_key
    checkpoint, prompt = event._goal_admission_checkpoint['goal_state'], event.text
    preparation = outcome == 'preparation_after_close'
    turn = install_real_turn(native, monkeypatch, 'preflight' if preparation else 'normal')
    monkeypatch.setattr(goals, 'judge_goal', lambda *a, **k: ('done', 'EXACT terminal control', False, None, False))
    entered, release = asyncio.Event(), asyncio.Event()

    async def decision(event, **kwargs):
        if getattr(event, '_goal_admission_checkpoint', None) is None:
            return None
        if preparation:
            return {'action': 'allow'}
        entered.set()
        await release.wait()
        return {'action': 'skip' if outcome.startswith('skip') else 'allow'}

    register_dispatch_hook(monkeypatch, decision)
    if startup:
        with pytest.raises(GatewayDraining):
            with gateway_ingress(runner):
                raise AssertionError('HTTP borrowed native startup permission')
    adapter._spawn_drain_task(event, key)
    task = adapter._session_tasks[key]
    try:
        await asyncio.wait_for((turn.entered if preparation else entered).wait(), 5)
        if not actual_entry and outcome != 'skip_while_open':
            runner._ingress_barrier.close()
            assert not gateway_admission_status(runner)['quiescent']
        if outcome == 'authorization_after_close':
            monkeypatch.setattr(runner, '_is_user_authorized', lambda source: False)
        release.set()
        turn.proceed.set()
        await asyncio.gather(task, return_exceptions=True)
        await settle(native)
        if startup:
            assert not gateway_admission_status(runner)['quiescent']  # Startup restore still owns work.
            runner._startup_restore_in_progress = False
        runner._ingress_barrier.close()
        rows = native.db.list_meta_prefix('goal_maintenance_pause:' + sid + ':')
        if not actual_entry:
            assert len(rows) == 1
            record = json.loads(rows[0][1])
            assert record['checkpoint'] == json.loads(checkpoint)
            assert record['continuation_prompt'] == prompt
            assert record['final_response'] == 'EXACT prior delivered result'
            assert record['input_context']['raw_message'] == event.raw_message
            assert record['input_context']['message_id'] == event.message_id
            assert record['pause_applied'] is True and record['owner'] and record['required_action']
            assert not turn.effects and not adapter._pending_messages
            reopened = SessionDB(db_path=native.db.db_path)
            try:
                assert json.loads(reopened.get_meta(rows[0][0])) == record
                assert [m['content'] for m in reopened.get_messages(sid)][:2] == [
                    'EXACT original inputs', 'EXACT delivered response']
            finally:
                reopened.close()
        else:
            assert not rows and turn.effects == ['EXACT inert core entry']
        status = gateway_admission_status(runner)
        assert status['known'] is (outcome != 'skip_while_open')
        assert bool(status['quiescent']) is (outcome != 'skip_while_open')
    finally:
        if startup:
            runner._startup_restore_in_progress = False
        release.set()
        turn.proceed.set()
        turn.physical.set()
        await asyncio.gather(*adapter._background_tasks, return_exceptions=True)
        await settle(native)


@pytest.mark.asyncio
async def test_actual_goal_cancel_before_step_keeps_owner_until_terminal_callback(native, monkeypatch):
    _source, entry, event = await accepted_goal(native, monkeypatch)
    native.adapter._spawn_drain_task(event, entry.session_key)
    task = native.adapter._session_tasks[entry.session_key]
    native.runner._ingress_barrier.close()
    task.cancel()
    snapshots = []
    asyncio.get_running_loop().call_soon(lambda: snapshots.append(gateway_admission_status(native.runner)))
    await asyncio.sleep(0)  # Cancelled task step, before its queued terminal callback.
    await asyncio.gather(task, return_exceptions=True)
    await asyncio.gather(*native.runner._deferred_agent_cleanup_tasks, return_exceptions=True)
    await settle(native)
    assert snapshots and not snapshots[0]['quiescent']
    row, = native.db.list_meta_prefix('goal_maintenance_pause:' + entry.session_id + ':')
    assert json.loads(row[1])['continuation_prompt'] == event.text
    assert not native.effects and gateway_admission_status(native.runner)['quiescent']


@pytest.mark.asyncio
async def test_ordinary_unleased_human_hook_skip_keeps_native_contract(native, monkeypatch):
    source, entry = original_goal(native)
    expected = native.db.get_meta('goal:' + entry.session_id)
    entered, release = asyncio.Event(), asyncio.Event()
    async def skip(**kwargs):
        entered.set()
        await release.wait()
        return {'action': 'skip'}
    register_dispatch_hook(monkeypatch, skip)
    event = native.runner._synthetic_prompt_event(source, 'EXACT ordinary human input')
    await native.adapter.handle_message(event)
    task = native.adapter._session_tasks[entry.session_key]
    try:
        await asyncio.wait_for(entered.wait(), 5)
        native.runner._ingress_barrier.close()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await settle(native)
    assert native.db.get_meta('goal:' + entry.session_id) == expected
    assert not native.db.list_meta_prefix('goal_maintenance_pause:') and not native.effects
    assert gateway_admission_status(native.runner)['known'] is True


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['reject', 'cancel_writer'])
async def test_actual_terminal_checkpoint_failure_stays_unknown_without_second_attempt(native, monkeypatch, fault):
    runner, adapter = native.runner, native.adapter
    _source, entry, event = await accepted_goal(native, monkeypatch)
    sid, key = entry.session_id, entry.session_key
    expected = native.db.get_meta('goal:' + sid)
    entered, release, writer = asyncio.Event(), asyncio.Event(), asyncio.Event()
    physical = threading.Event()
    loop, statements = asyncio.get_running_loop(), []

    async def skip(**kwargs):
        entered.set()
        await release.wait()
        return {'action': 'skip'}

    def trace(sql):
        if sql.startswith('INSERT INTO state_meta') and 'goal_maintenance_pause:' in sql:
            statements.append(sql)
            if fault == 'cancel_writer':
                loop.call_soon_threadsafe(writer.set)
                assert physical.wait(5)

    register_dispatch_hook(monkeypatch, skip)
    native.db._conn.set_trace_callback(trace)
    if fault == 'reject':
        native.db._conn.execute("CREATE TRIGGER terminal_fault BEFORE INSERT ON state_meta "
                                "WHEN NEW.key LIKE 'goal_maintenance_pause:%' "
                                "BEGIN SELECT RAISE(ABORT, 'EXACT retained receipt fault'); END")
    adapter._spawn_drain_task(event, key)
    task = adapter._session_tasks[key]
    try:
        await asyncio.wait_for(entered.wait(), 5)
        runner._ingress_barrier.close()
        release.set()
        if fault == 'cancel_writer':
            await asyncio.wait_for(writer.wait(), 5)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            assert not gateway_admission_status(runner)['quiescent'] and not native.effects
            assert runner._ingress_barrier.snapshot()['adopted']
            physical.set()
        await asyncio.gather(task, return_exceptions=True)
        await settle(native)
        assert not gateway_admission_status(runner)['known']
        assert not native.effects and not adapter._pending_messages
        if fault == 'reject':
            assert native.db.get_meta('goal:' + sid) == expected
            assert not native.db.list_meta_prefix('goal_maintenance_pause:' + sid + ':')
        else:
            assert len(statements) == 1  # No automatic second settlement after cancellation.
            row, = native.db.list_meta_prefix('goal_maintenance_pause:' + sid + ':')
            reopened = SessionDB(db_path=native.db.db_path)
            try:
                record = json.loads(reopened.get_meta(row[0]))
                assert record['checkpoint'] == json.loads(event._goal_admission_checkpoint['goal_state'])
                assert record['continuation_prompt'] == event.text
                assert record['final_response'] == 'EXACT prior delivered result'
            finally:
                reopened.close()
    finally:
        release.set()
        physical.set()
        await asyncio.gather(task, return_exceptions=True)
        await settle(native)
        native.db._conn.set_trace_callback(None)
        if fault == 'reject':
            native.db._conn.execute('DROP TRIGGER terminal_fault')
