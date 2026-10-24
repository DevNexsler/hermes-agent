"""Real automatic loop persistence failures cannot settle into known-zero readiness."""
import asyncio
import sqlite3
import threading

import pytest

from gateway.admission import gateway_admission_status, gateway_ingress
from gateway.config import Platform
from gateway.session import SessionSource
from hermes_cli import goals, loops
from hermes_state import SessionDB
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native


def loop_entry(native, *, fired):
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = native.runner.session_store.get_or_create_session(source)
    native.db.append_message(entry.session_id, 'user', 'EXACT loop continuation inputs')
    mgr = loops.LoopManager(entry.session_id)
    state = mgr.set('EXACT native loop', interval_seconds=60, route={
        'platform': 'telegram', 'chat_id': '123456', 'chat_type': 'dm', 'user_id': 'u1'})
    state.next_due_at = 0
    loops.save_loop(entry.session_id, state)
    if fired:
        assert mgr.fire_tick()
    return source, entry


async def settle(native):
    for _ in range(500):
        if (not native.runner._ingress_barrier.snapshot()['adopted']
                and not getattr(native.runner, '_deferred_agent_workers', {})):
            return
        await asyncio.sleep(.01)
    raise AssertionError('owned loop work did not settle')


def storage_fault(db, key, fault):
    if fault == 'reject':
        db._conn.execute("CREATE TRIGGER loop_fault BEFORE INSERT ON state_meta WHEN NEW.key=? "
                         "BEGIN SELECT RAISE(ABORT, 'EXACT synthetic loop failure'); END".replace('?', "'" + key + "'"))
    elif fault == 'readback':
        db._conn.execute("CREATE TRIGGER loop_fault AFTER UPDATE ON state_meta "
                         "WHEN NEW.key='" + key + "' AND NEW.value!='EXACT corrupted readback' "
                         "BEGIN UPDATE state_meta SET value='EXACT corrupted readback' WHERE key=NEW.key; END")
    elif fault == 'read':
        db._conn.set_authorizer(lambda action, table, *a: sqlite3.SQLITE_DENY
                                if action == sqlite3.SQLITE_READ and table == 'state_meta' else sqlite3.SQLITE_OK)


def clear_fault(db, fault):
    if fault == 'read':
        db._conn.set_authorizer(None)
    elif fault != 'none':
        db._conn.execute('DROP TRIGGER loop_fault')


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['none', 'reject', 'readback', 'read'])
@pytest.mark.parametrize('response', ['', 'LOOP_COMPLETE'])
async def test_full_post_turn_loop_storage_is_confirmed(native, fault, response):
    source, entry = loop_entry(native, fired=True)
    key = 'loop:' + entry.session_id
    expected = native.db.get_meta(key)
    storage_fault(native.db, key, fault)
    try:
        with gateway_ingress(native.runner):
            native.runner._ingress_barrier.close()
            await native.runner._run_post_turn_hooks(agent_result=response, source=source, is_internal=True)
        await settle(native)
    finally:
        clear_fault(native.db, fault)
    status = gateway_admission_status(native.runner)
    assert status['known'] is (fault == 'none')
    if fault == 'none':
        assert status['quiescent'] and not loops.load_loop(entry.session_id).awaiting_response
    else:
        assert not status['quiescent'] and native.db.get_meta(key) == expected
    reopened = SessionDB(db_path=native.db.db_path)
    try:
        assert reopened.get_meta(key) == native.db.get_meta(key)
        assert reopened.get_messages(entry.session_id)[0]['content'] == 'EXACT loop continuation inputs'
    finally:
        reopened.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['none', 'reject', 'readback'])
async def test_native_loop_claim_write_precedes_any_adapter_effect(native, fault):
    source, entry = loop_entry(native, fired=False)
    key = 'loop:' + entry.session_id
    expected = native.db.get_meta(key)
    storage_fault(native.db, key, fault)
    native.finish.set()
    try:
        try:
            await native.runner._loop_wakeup_fire_one(entry.session_id, loops.load_loop(entry.session_id),
                                                       10**12, set())
        except Exception:
            assert fault != 'none'
        await asyncio.gather(*native.adapter._background_tasks, return_exceptions=True)
        await settle(native)
    finally:
        clear_fault(native.db, fault)
    native.runner._ingress_barrier.close()
    assert gateway_admission_status(native.runner)['known'] is (fault == 'none')
    if fault != 'none':
        assert not native.effects and native.db.get_meta(key) == expected
    else:
        assert len(native.effects) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('control', ['pause', 'clear'])
async def test_loop_judge_cannot_overwrite_concurrent_user_control(native, monkeypatch, control):
    source, entry = loop_entry(native, fired=True)
    mgr = loops.LoopManager(entry.session_id)
    mgr.state.until = 'EXACT inert until condition'
    loops.save_loop(entry.session_id, mgr.state)
    entered, release = threading.Event(), threading.Event()
    def judge(*a, **k):
        entered.set()
        assert release.wait(5)
        return 'continue', 'EXACT inert result', False, None, False
    monkeypatch.setattr(goals, 'judge_goal', judge)
    async def request():
        with gateway_ingress(native.runner):
            await native.runner._run_post_turn_hooks(agent_result='EXACT completed response', source=source,
                                                     is_internal=True)
    task = asyncio.create_task(request())
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        user = loops.LoopManager(entry.session_id)
        getattr(user, control)()
        expected = native.db.get_meta('loop:' + entry.session_id)
        native.runner._ingress_barrier.close()
    finally:
        release.set()
        await task
        await settle(native)
    assert not gateway_admission_status(native.runner)['known']
    assert native.db.get_meta('loop:' + entry.session_id) == expected
