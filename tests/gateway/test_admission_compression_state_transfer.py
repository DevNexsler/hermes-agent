"""Actual rotating compression cannot sanitize unconfirmed native session state."""
import asyncio
import json
import time

import pytest

from gateway.admission import gateway_admission_status, gateway_ingress
from gateway.config import Platform
from gateway.session import SessionSource
from hermes_cli import goals, heartbeat, loops
from hermes_state import SessionDB
from tests.agent.test_compression_rotation_state import _build_agent_with_db, _msgs
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['goal', 'loop', 'heartbeat'])
@pytest.mark.parametrize('fault', ['none', 'malformed', 'write', 'parent_write', 'readback', 'duplicate'])
async def test_real_rotating_compression_retains_state_and_reports_unknown(native, kind, fault):
    runner, db = native.runner, native.db
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = runner.session_store.get_or_create_session(source)
    parent, key = entry.session_id, entry.session_key
    db.append_message(parent, 'user', 'EXACT retained compression input')
    db.append_message(parent, 'assistant', 'EXACT retained compression reply')
    if kind == 'goal':
        goals.GoalManager(parent).set('EXACT retained goal')
    elif kind == 'loop':
        loops.LoopManager(parent).set('EXACT retained loop', interval_seconds=60)
    else:
        heartbeat.HeartbeatManager(parent).set('EXACT retained heartbeat', interval_seconds=60)
    raw = db.get_meta(kind + ':' + parent)
    data = json.loads(raw)
    data.pop('generation_id', None)  # Valid legacy state remains supported.
    if fault == 'malformed':
        data.update({'gates': 'EXACT must-pass constraint'} if kind == 'goal'
                    else {'route': 'EXACT retained route'} if kind == 'loop'
                    else {'fire_count': 'EXACT retained count'})
    raw = json.dumps(data)
    if fault == 'duplicate':
        raw = '{"status":"EXACT retained conflicting status",' + raw[1:]
    db.set_meta(kind + ':' + parent, raw)
    if fault in ('write', 'parent_write', 'readback'):
        condition = ("NEW.key LIKE '" + kind + ":%' AND NEW.key!='" + kind + ':' + parent + "'"
                     if fault != 'parent_write' else "NEW.key='" + kind + ':' + parent + "'")
        timing, action = ('BEFORE INSERT', "SELECT RAISE(ABORT, 'EXACT transfer refusal')")
        if fault == 'readback':
            timing = 'AFTER INSERT'
            action = "UPDATE state_meta SET value='EXACT unconfirmed readback' WHERE key=NEW.key"
        db._conn.execute('CREATE TRIGGER transfer_fault ' + timing + ' ON state_meta WHEN ' + condition
                         + ' BEGIN ' + action + '; END')
    agent = _build_agent_with_db(db, parent)
    try:
        with gateway_ingress(runner):
            compressed, _prompt = await runner._run_in_executor_with_context(
                lambda: agent._compress_context(_msgs(), 'SYSTEM CACHE PREFIX', approx_tokens=120_000))
            child = agent.session_id
            assert child != parent and db.get_compression_tip(parent) == child
            runner._ingress_barrier.close()
            result = {'session_id': child, 'final_response': 'EXACT compressed turn reply',
                      'messages': compressed, 'completed': True, 'api_calls': 0}
            await runner._hmwa_shape_agent_response(result, source, [], entry, key, key, None,
                                                     parent, 'telegram', time.time())
            await runner._run_post_turn_hooks(agent_result=result, source=source, is_internal=False)
        for _ in range(500):
            if (not runner._ingress_barrier.snapshot()['adopted']
                    and not getattr(runner, '_deferred_agent_workers', {})):
                break
            await asyncio.sleep(.01)
    finally:
        if fault in ('write', 'parent_write', 'readback'):
            db._conn.execute('DROP TRIGGER transfer_fault')
    status = gateway_admission_status(runner)
    assert status['known'] is (fault == 'none')
    if fault != 'none':
        assert not status['quiescent']
        assert db.get_meta(kind + ':' + parent) == raw
        assert db.get_meta(kind + ':' + child) is None
        assert not db.list_meta_prefix('goal_maintenance_pause:' + child + ':')
    else:
        assert status['quiescent']
        assert json.loads(db.get_meta(kind + ':' + parent))['status'] == 'cleared'
        assert db.get_meta(kind + ':' + child) is not None
    reopened = SessionDB(db_path=db.db_path)
    try:
        assert reopened.get_meta(kind + ':' + parent) == db.get_meta(kind + ':' + parent)
        assert any(m['content'] == 'EXACT retained compression input' for m in reopened.get_messages(parent))
    finally:
        reopened.close()
