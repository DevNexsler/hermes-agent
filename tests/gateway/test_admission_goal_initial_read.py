"""Unconfirmed initial goal reads cannot erase retained work behind a known-zero fence."""
import asyncio
import json
from contextlib import closing

import pytest

from gateway.admission import gateway_admission_status, gateway_ingress
from gateway.config import Platform
from gateway.session import SessionSource
from hermes_cli import goals
from hermes_cli.goals import GoalManager
from hermes_cli.sqlite_safe_read import connect_tracked
from hermes_state import SessionDB
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native


@pytest.fixture(autouse=True)
def rollback_journal(monkeypatch):
    import hermes_state
    def actual_rollback_mode(conn, **_kwargs):
        return conn.execute('PRAGMA journal_mode=DELETE').fetchone()[0]
    monkeypatch.setattr(hermes_state, 'apply_wal_with_fallback', actual_rollback_mode)


CASES = [(state, locked) for state in ('active', 'paused', 'cleared', 'absent')
         for locked in (False, True)]
CASES += [(state, False) for state in ('malformed', 'empty', 'null', 'unknown_status', 'nonfinite',
                                     'string_nonfinite', 'future_field', 'bootstrap', 'legacy')]


@pytest.mark.asyncio
@pytest.mark.parametrize('state,locked', CASES)
async def test_initial_goal_read_is_confirmed_before_known_quiescence(native, monkeypatch, state, locked):
    runner, db = native.runner, native.db
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = runner.session_store.get_or_create_session(source)
    sid = entry.session_id
    db.append_message(sid, 'user', 'EXACT initial-read goal inputs')
    db.append_message(sid, 'assistant', 'EXACT already delivered response')
    mgr = GoalManager(sid)
    if state != 'absent':
        mgr.set('EXACT retained original goal')
        if state == 'paused':
            mgr.pause('EXACT user-owned pause')
        elif state == 'cleared':
            mgr.clear()
        elif state in ('malformed', 'empty'):
            db.set_meta('goal:' + sid, '{EXACT malformed goal' if state == 'malformed' else '')
        elif state == 'null':
            db._execute_write(lambda conn: conn.execute('UPDATE state_meta SET value=NULL WHERE key=?', ('goal:' + sid,)))
        elif state in ('unknown_status', 'nonfinite', 'string_nonfinite', 'future_field', 'legacy'):
            data = json.loads(db.get_meta('goal:' + sid))
            data.update({'status': 'future_owned_state'} if state == 'unknown_status'
                        else {'created_at': float('nan')} if state == 'nonfinite'
                        else {'created_at': 'NaN'} if state == 'string_nonfinite'
                        else {'generation_id': None} if state == 'legacy'
                        else {'future_owner': 'EXACT foreign obligation'})
            if state == 'legacy':
                del data['generation_id']
            db.set_meta('goal:' + sid, json.dumps(data))
    expected = db.get_meta('goal:' + sid)
    row = db._conn.execute('SELECT value FROM state_meta WHERE key=?', ('goal:' + sid,)).fetchone()
    expected_row = None if row is None else tuple(row)
    assert goals._get_session_db() is db
    assert db._conn.execute('PRAGMA journal_mode').fetchone()[0] == 'delete'
    original_timeout = db._conn.execute('PRAGMA busy_timeout').fetchone()[0]
    db._conn.execute('PRAGMA busy_timeout=20')
    if state == 'bootstrap':
        monkeypatch.setattr(goals, '_get_session_db', lambda: None)
    try:
        with closing(connect_tracked(db.db_path.as_uri() + '?mode=rw', uri=True,
                                     timeout=0, isolation_level=None)) as other:
            if locked:
                other.execute('BEGIN EXCLUSIVE')
            try:
                with gateway_ingress(runner):
                    runner._ingress_barrier.close()
                    try:
                        await runner._post_turn_goal_continuation(
                            session_entry=entry, source=source, final_response='EXACT already delivered response')
                        outcome = [None]
                    except Exception as exc:
                        outcome = [exc]
            finally:
                if locked:
                    other.rollback()
    finally:
        db._conn.execute('PRAGMA busy_timeout=' + str(original_timeout))
    for _ in range(200):
        if not getattr(runner, '_deferred_agent_workers', {}):
            break
        await asyncio.sleep(.01)
    status = gateway_admission_status(runner)
    unconfirmed = locked or state not in ('active', 'paused', 'cleared', 'absent', 'legacy')
    assert status['known'] is (not unconfirmed)
    assert not status['quiescent'] if unconfirmed else status['quiescent']
    records = db.list_meta_prefix('goal_maintenance_pause:' + sid + ':')
    if unconfirmed:
        assert isinstance(outcome[0], Exception)
        assert db.get_meta('goal:' + sid) == expected and not records
        row = db._conn.execute('SELECT value FROM state_meta WHERE key=?', ('goal:' + sid,)).fetchone()
        assert (None if row is None else tuple(row)) == expected_row
    elif state in ('active', 'legacy'):
        assert outcome == [None] and len(records) == 1
        assert json.loads(records[0][1])['checkpoint'] == json.loads(goals.GoalState.from_json(expected).to_json())
    else:
        assert outcome == [None] and db.get_meta('goal:' + sid) == expected and not records
    assert not native.effects and entry.session_key not in native.adapter._pending_messages
    reopened = SessionDB(db_path=db.db_path)
    try:
        assert reopened.get_meta('goal:' + sid) == db.get_meta('goal:' + sid)
        assert [m['content'] for m in reopened.get_messages(sid)] == [
            'EXACT initial-read goal inputs', 'EXACT already delivered response']
    finally:
        reopened.close()
