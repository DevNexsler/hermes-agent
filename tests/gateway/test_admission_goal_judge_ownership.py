"""Real post-turn executors cannot overwrite user goal state while judge/gate waits."""
import asyncio
import json
import sqlite3
import threading
from contextlib import closing

import pytest

from gateway.admission import gateway_admission_status, gateway_ingress
from gateway.config import Platform
from gateway.platforms.event import MessageEvent
from gateway.session import SessionSource
from hermes_cli import goals
from hermes_cli.goals import GoalManager
from hermes_state import SessionDB
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native

CASES = [(change, 'continue', closed)
         for change in ('pause', 'clear', 'replace', 'resume', 'malformed', 'missing', None)
         for closed in (False, True)]
CASES += [(change, verdict, True) for change in ('pause', None)
          for verdict in ('done', 'blocked', 'wait', 'gate_failed', 'gate_pass')]


def held_evaluation(monkeypatch, verdict):
    entered, release = threading.Event(), threading.Event()
    def wait():
        entered.set()
        assert release.wait(5)
    def judge(*args, **kwargs):
        if not verdict.startswith('gate_'):
            wait()
        result = 'continue' if verdict.startswith('gate_') else verdict
        return result, 'EXACT inert judge result', False, ({'seconds': 60} if result == 'wait' else None), False
    def gate(*args, **kwargs):
        wait()
        return verdict == 'gate_pass', 0 if verdict == 'gate_pass' else 1, 'EXACT inert gate output'
    monkeypatch.setattr(goals, 'judge_goal', judge)
    monkeypatch.setattr(goals, 'run_gate', gate)
    return entered, release


@pytest.mark.asyncio
@pytest.mark.parametrize('change,verdict,closed', CASES)
async def test_post_turn_judge_cas_keeps_user_state_and_original_obligation(native, monkeypatch, change, verdict, closed):
    runner, adapter = native.runner, native.adapter
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = runner.session_store.get_or_create_session(source)
    sid, key = entry.session_id, entry.session_key
    native.db.append_message(sid, 'user', 'EXACT original conversation')
    mgr = GoalManager(sid)
    mgr.set('EXACT original goal and input')
    if verdict.startswith('gate_'):
        mgr.add_gate('inert test-owned quality gate')
    original = native.db.get_meta('goal:' + sid)
    entered, release = held_evaluation(monkeypatch, verdict)
    async def evaluate():
        with gateway_ingress(runner):
            await runner._post_turn_goal_continuation(session_entry=entry, source=source,
                                                       final_response='EXACT delivered parent result')
    task = asyncio.create_task(evaluate())
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        user = GoalManager(sid)
        if change in ('pause', 'clear'):
            await runner._handle_goal_command(MessageEvent(text='/goal ' + change, source=source))
        elif change == 'replace':
            user.set('REPLACEMENT independently owned goal')
        elif change == 'resume':
            user.pause()
            user.resume(reset_budget=False)
        elif change == 'malformed':
            native.db.set_meta('goal:' + sid, '{exact malformed foreign goal')
        elif change == 'missing':
            native.db._execute_write(lambda conn: conn.execute('DELETE FROM state_meta WHERE key=?', ('goal:' + sid,)))
        expected = native.db.get_meta('goal:' + sid)
        if closed:
            runner._ingress_barrier.close()
    finally:
        release.set()
        await task
    records = native.db.list_meta_prefix('goal_maintenance_pause:' + sid + ':')
    if change is not None:
        assert native.db.get_meta('goal:' + sid) == expected
        assert key not in adapter._pending_messages and not native.effects
        assert len(records) == 1
        record = json.loads(records[0][1])
        assert record['pause_applied'] is False and record['outcome'] == 'UNKNOWN'
        assert record['checkpoint'] == json.loads(original)
        assert record['final_response'] == 'EXACT delivered parent result'
        assert 'EXACT original goal and input' in record['continuation_prompt']
        assert record['ownership_conflict'] and record['required_action']
        assert not gateway_admission_status(runner)['known']
        reopened = SessionDB(db_path=native.home / 'state.db')
        try:
            assert reopened.get_meta('goal:' + sid) == expected
            assert reopened.get_meta(records[0][0]) == records[0][1]
            assert reopened.get_messages(sid)[0]['content'] == 'EXACT original conversation'
        finally:
            reopened.close()
    elif closed and verdict in ('continue', 'gate_failed', 'gate_pass'):
        assert len(records) == 1 and json.loads(records[0][1])['pause_applied'] is True
        assert GoalManager(sid).state.status == 'paused' and key not in adapter._pending_messages
    else:
        assert not records
        assert (key in adapter._pending_messages) is (verdict == 'continue')


@pytest.mark.asyncio
async def test_cancelled_post_turn_waiter_keeps_judge_conflict_until_durable_handoff(native, monkeypatch):
    runner = native.runner
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = runner.session_store.get_or_create_session(source)
    sid = entry.session_id
    GoalManager(sid).set('EXACT cancelled waiter goal')
    entered, release = held_evaluation(monkeypatch, 'continue')
    async def evaluate():
        with gateway_ingress(runner):
            await runner._post_turn_goal_continuation(session_entry=entry, source=source, final_response='EXACT result')
    task = asyncio.create_task(evaluate())
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        GoalManager(sid).pause('EXACT user pause while judge running')
        expected = native.db.get_meta('goal:' + sid)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        runner._ingress_barrier.close()
        assert gateway_admission_status(runner)['adopted'] > 0 or runner._deferred_agent_workers
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
    for _ in range(500):
        status = gateway_admission_status(runner)
        if not status['adopted'] and not runner._deferred_agent_workers:
            break
        await asyncio.sleep(.01)
    assert native.db.get_meta('goal:' + sid) == expected
    rows = native.db.list_meta_prefix('goal_maintenance_pause:' + sid + ':')
    assert len(rows) == 1 and json.loads(rows[0][1])['outcome'] == 'UNKNOWN'
    assert entry.session_key not in native.adapter._pending_messages and not native.effects


@pytest.mark.asyncio
async def test_same_manager_user_pause_does_not_borrow_automatic_snapshot(native, monkeypatch):
    mgr = GoalManager('same-manager-goal')
    mgr.set('EXACT original same-manager goal')
    entered, release = held_evaluation(monkeypatch, 'continue')
    task = asyncio.create_task(native.runner._run_in_executor_with_context(
        lambda: mgr.evaluate_after_turn('EXACT delivered response')))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        mgr.pause('EXACT ordinary user pause')
        expected = native.db.get_meta('goal:same-manager-goal')
    finally:
        release.set()
        decision = await task
    assert native.db.get_meta('goal:same-manager-goal') == expected
    assert decision['should_continue'] is False and decision['goal_state_conflict']


@pytest.mark.asyncio
async def test_goal_persistence_readback_failure_blocks_native_readiness(native, monkeypatch):
    runner = native.runner
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = runner.session_store.get_or_create_session(source)
    sid = entry.session_id
    GoalManager(sid).set('EXACT persistence ownership')
    expected = native.db.get_meta('goal:' + sid)
    native.db.append_message(sid, 'assistant', 'EXACT durable delivered result')
    db = goals._get_session_db()
    set_meta = db.set_meta
    blocked = []
    def fail_readback(key, value, *, cursor=None):
        if key == 'goal:' + sid and cursor is not None:
            from hermes_cli.sqlite_safe_read import connect_tracked
            with closing(connect_tracked((native.home / 'state.db').as_uri() + '?mode=rw',
                                          uri=True, timeout=0, isolation_level=None)) as other:
                with pytest.raises(sqlite3.OperationalError, match='locked'):
                    other.execute('BEGIN IMMEDIATE')
                blocked.append(True)
            return set_meta(key, 'inert unconfirmed write', cursor=cursor)
        return set_meta(key, value, cursor=cursor)
    monkeypatch.setattr(db, 'set_meta', fail_readback)
    monkeypatch.setattr(goals, 'judge_goal', lambda *a, **k: ('continue', 'inert verdict', False, None, False))
    with pytest.raises(RuntimeError, match='readback unconfirmed'):
        with gateway_ingress(runner):
            await runner._post_turn_goal_continuation(session_entry=entry, source=source,
                                                       final_response='EXACT durable delivered result')
    assert native.db.get_meta('goal:' + sid) == expected
    assert blocked
    assert not gateway_admission_status(runner)['known']
    assert entry.session_key not in native.adapter._pending_messages and not native.effects
    assert native.db.get_messages(sid)[0]['content'] == 'EXACT durable delivered result'


@pytest.mark.asyncio
async def test_goal_evaluation_real_profile_scope_a_b_a(native, monkeypatch):
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override
    from hermes_state_registry import close_all_under
    second_home = native.home.parent / 'profile-B'
    second_home.mkdir()
    observed, stores = [], {}
    def judge(goal, *args, **kwargs):
        observed.append(goal)
        return 'continue', 'EXACT inert verdict', False, None, False
    monkeypatch.setattr(goals, 'judge_goal', judge)
    try:
        for home, text in ((native.home, 'EXACT A'), (second_home, 'EXACT B'), (native.home, 'EXACT A')):
            token = set_hermes_home_override(home)
            try:
                db = await asyncio.to_thread(goals._get_session_db)
                assert db.db_path.resolve() == home.resolve() / 'state.db'
                stores[text] = db
                mgr = GoalManager('same-profile-session-id')
                if mgr.state is None:
                    mgr.set(text)
                result = await native.runner._run_in_executor_with_context(
                    lambda: mgr.evaluate_after_turn('EXACT scoped response'))
                assert result['should_continue'] is True
            finally:
                reset_hermes_home_override(token)
        assert observed == ['EXACT A', 'EXACT B', 'EXACT A']
        assert json.loads(stores['EXACT A'].get_meta('goal:same-profile-session-id'))['turns_used'] == 2
        assert json.loads(stores['EXACT B'].get_meta('goal:same-profile-session-id'))['turns_used'] == 1
    finally:
        close_all_under(second_home)
