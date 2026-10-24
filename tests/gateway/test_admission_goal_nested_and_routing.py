"""Full post-turn routing and raw nested goal data retain original obligations."""
import asyncio
import json
import threading
from contextlib import contextmanager

import pytest

from gateway.admission import gateway_admission_status, gateway_ingress
from gateway.config import Platform
from gateway.session import SessionSource
from hermes_cli.goals import GoalManager
from hermes_state import SessionDB
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native

BAD = [
    {'gates': 'EXACT must-pass'}, {'gates': ['EXACT retained gate']},
    {'gates': [{'command': 'true', 'foreign_gate_owner': 'EXACT obligation'}]},
    {'gates': [{'command': 'true', 'attempts': 1.5}]},
    {'gates': [{'command': 'true', 'max_retries': True}]},
    {'gates': [{'command': 'true', 'last_output_tail': {'retained': 'EXACT output'}}]},
    {'gates': [None]}, {'gates': [False]}, {'gates': [{'command': ''}]},
    {'subgoals': 'EXACT subgoal'}, {'subgoals': [{'retained': 'EXACT subgoal'}]},
    {'subgoals': [17]}, {'subgoals': ['']},
    {'contract': 'EXACT retained constraint'}, {'contract': ['EXACT constraint']},
    {'contract': {'foreign_constraint': 'EXACT obligation'}},
    {'contract': {'constraints': {'retained': 'EXACT constraint'}}},
    {'last_reason': {'retained': 'EXACT reason'}}, {'generation_id': ['EXACT owner']},
    '{"goal":"EXACT old goal", "goal":"EXACT changed goal"}',
    '{"goal":"EXACT goal", "gates":[{"command":"EXACT old gate", "command":"EXACT changed gate"}]}',
    '{"goal":"EXACT goal", "contract":{"constraints":"EXACT old constraint", "constraints":"EXACT changed constraint"}}',
]
GOOD = [{}, {'gates': [{'command': 'true'}]}, {'contract': {'constraints': 'EXACT keep inputs'}},
        {'subgoals': ['EXACT subgoal']}, {'created_at': 123}]


def original_goal(native):
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = native.runner.session_store.get_or_create_session(source)
    native.db.append_message(entry.session_id, 'user', 'EXACT original inputs')
    native.db.append_message(entry.session_id, 'assistant', 'EXACT delivered response')
    GoalManager(entry.session_id).set('EXACT original goal')
    return source, entry


async def settle(runner):
    for _ in range(500):
        if (not getattr(runner, '_deferred_agent_workers', {})
                and not runner._ingress_barrier.snapshot()['adopted']):
            return
        await asyncio.sleep(.01)
    assert not getattr(runner, '_deferred_agent_workers', {})
    assert not runner._ingress_barrier.snapshot()['adopted']


def assert_history_reopens(native, sid):
    reopened = SessionDB(db_path=native.db.db_path)
    try:
        assert reopened.get_meta('goal:' + sid) == native.db.get_meta('goal:' + sid)
        assert [m['content'] for m in reopened.get_messages(sid)] == [
            'EXACT original inputs', 'EXACT delivered response']
    finally:
        reopened.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('change,valid', [(d, False) for d in BAD] + [(d, True) for d in GOOD])
async def test_full_post_turn_never_normalizes_away_nested_obligation(native, change, valid):
    source, entry = original_goal(native)
    data = json.loads(native.db.get_meta('goal:' + entry.session_id))
    data.pop('generation_id')  # Known legacy missing field remains supported.
    if isinstance(change, str):
        expected = change
    else:
        data.update(change)
        expected = json.dumps(data)
    native.db.set_meta('goal:' + entry.session_id, expected)
    with gateway_ingress(native.runner):
        native.runner._ingress_barrier.close()
        await native.runner._run_post_turn_hooks(agent_result='EXACT delivered response', source=source,
                                                 is_internal=False)
    await settle(native.runner)
    status = gateway_admission_status(native.runner)
    rows = native.db.list_meta_prefix('goal_maintenance_pause:' + entry.session_id + ':')
    assert status['known'] is valid
    if valid:
        assert status['quiescent'] and len(rows) == 1
        assert json.loads(rows[0][1])['pause_applied'] is True
    else:
        assert not status['quiescent'] and not rows
        assert native.db.get_meta('goal:' + entry.session_id) == expected
    assert not native.effects and entry.session_key not in native.adapter._pending_messages
    assert_history_reopens(native, entry.session_id)


@contextmanager
def routing_faults(native, *, sql, mirror, hold=None):
    db, store = native.db, native.runner.session_store
    path, backup = store.sessions_dir / 'sessions.json', store.sessions_dir / 'original-sessions.json'
    statements = []
    def trace(statement):
        statements.append(statement)
        if hold and statement.startswith('INSERT INTO gateway_routing') and not hold[0].is_set():
            hold[0].set()
            assert hold[1].wait(5)
    if sql:
        db._conn.execute("CREATE TRIGGER synthetic_route_refusal BEFORE INSERT ON gateway_routing "
                         "BEGIN SELECT RAISE(ABORT, 'EXACT synthetic routing fault'); END")
    if mirror:
        path.rename(backup)
        path.mkdir()  # Real atomic replace failure, not a mocked store/write callback.
    db._conn.set_trace_callback(trace)
    try:
        yield statements
    finally:
        db._conn.set_trace_callback(None)
        if sql:
            db._conn.execute('DROP TRIGGER synthetic_route_refusal')
        if mirror:
            path.rmdir()
            backup.rename(path)


def assert_route_outcome(native, entry, expected, unknown):
    status = gateway_admission_status(native.runner)
    rows = native.db.list_meta_prefix('goal_maintenance_pause:' + entry.session_id + ':')
    assert status['known'] is (not unknown)
    if unknown:
        assert not status['quiescent'] and not rows
        assert native.db.get_meta('goal:' + entry.session_id) == expected
    else:
        assert status['quiescent'] and len(rows) == 1
    assert not native.effects and entry.session_key not in native.adapter._pending_messages
    assert_history_reopens(native, entry.session_id)


@pytest.mark.asyncio
@pytest.mark.parametrize('sql,mirror', [(False, False), (True, False), (False, True), (True, True)])
async def test_real_routing_primary_and_json_failure_closes_before_post_turn_handoff(native, sql, mirror):
    source, entry = original_goal(native)
    expected = native.db.get_meta('goal:' + entry.session_id)
    with routing_faults(native, sql=sql, mirror=mirror) as statements:
        with gateway_ingress(native.runner):
            native.runner._ingress_barrier.close()
            await native.runner._run_post_turn_hooks(agent_result='EXACT delivered response', source=source,
                                                     is_internal=False)
        await settle(native.runner)
    assert any(s.startswith('INSERT INTO gateway_routing') for s in statements)
    assert_route_outcome(native, entry, expected, sql and mirror)


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', [False, True])
async def test_cancelled_routing_waiter_retains_complete_goal_handoff(native, fault):
    source, entry = original_goal(native)
    expected = native.db.get_meta('goal:' + entry.session_id)
    entered, release = threading.Event(), threading.Event()
    async def request():
        with gateway_ingress(native.runner):
            await native.runner._run_post_turn_hooks(agent_result='EXACT delivered response', source=source,
                                                     is_internal=False)
    with routing_faults(native, sql=fault, mirror=fault, hold=(entered, release)):
        task = asyncio.create_task(request())
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            native.runner._ingress_barrier.close()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            status = gateway_admission_status(native.runner)
            assert not status['quiescent']
            assert status['adopted'] or getattr(native.runner, '_deferred_agent_workers', {})
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
        await settle(native.runner)
    assert_route_outcome(native, entry, expected, fault)
