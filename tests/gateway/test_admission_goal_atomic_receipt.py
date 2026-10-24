"""Rejected maintenance receipt/goal writes must roll back one atomic handoff."""
import asyncio
import json

import pytest

from gateway.admission import gateway_admission_status
from hermes_cli.goals import GoalManager
from hermes_state import SessionDB
from tests.gateway import test_admission_native_work
from tests.gateway.test_admission_goal_terminal_exit import accepted_goal
from tests.gateway.test_admission_loop_storage import settle

native = test_admission_native_work.native


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['none', 'receipt_abort', 'receipt_ignore', 'goal_ignore',
                                  'receipt_corrupt', 'goal_corrupt'])
async def test_actual_accepted_goal_pause_checks_rows_before_commit(native, monkeypatch, fault):
    runner, adapter = native.runner, native.adapter
    _source, entry, event = await accepted_goal(native, monkeypatch)
    key, sid = entry.session_key, entry.session_id
    original = native.db.get_meta('goal:' + sid)
    if fault != 'none':
        kind, action = fault.split('_')
        pattern = 'goal_maintenance_pause:%' if kind == 'receipt' else 'goal:%'
        timing = 'AFTER' if action == 'corrupt' else 'BEFORE'
        operation = 'UPDATE' if fault == 'goal_corrupt' else 'INSERT'
        body = ("UPDATE state_meta SET value='{}' WHERE key=NEW.key;" if action == 'corrupt'
                else "SELECT RAISE(IGNORE);" if action == 'ignore' else "SELECT RAISE(ABORT, 'EXACT receipt fault');")
        native.db._conn.execute(f"CREATE TRIGGER handoff_fault {timing} {operation} ON state_meta "
                                f"WHEN NEW.key LIKE '{pattern}' AND NEW.value != '{{}}' "
                                f"BEGIN {body} END")
    runner._ingress_barrier.close()
    adapter._spawn_drain_task(event, key)
    try:
        await asyncio.gather(adapter._session_tasks[key], return_exceptions=True)
        await asyncio.gather(*getattr(runner, '_deferred_agent_cleanup_tasks', ()), return_exceptions=True)
        await settle(native)
        reopened = SessionDB(db_path=native.db.db_path)
        try:
            rows = reopened.list_meta_prefix('goal_maintenance_pause:' + sid + ':')
            if fault != 'none':
                assert reopened.get_meta('goal:' + sid) == original
                assert not rows  # Neither row can commit when either final row fails readback.
                assert gateway_admission_status(runner)['known'] is False
            else:
                row, = rows
                record = json.loads(row[1])
                assert record['checkpoint'] == json.loads(original)
                assert record['input_context']['raw_message'] == event.raw_message
                assert record['continuation_prompt'] == event.text
                assert record['final_response'] == 'EXACT prior delivered result'
                assert json.loads(reopened.get_meta('goal:' + sid))['status'] == 'paused'
                assert gateway_admission_status(runner)['quiescent'] is True
            assert [m['content'] for m in reopened.get_messages(sid)][:2] == [
                'EXACT original inputs', 'EXACT delivered response']
        finally:
            reopened.close()
        assert not native.effects
    finally:
        if fault != 'none':
            native.db._conn.execute('DROP TRIGGER handoff_fault')


@pytest.mark.asyncio
async def test_receipt_fault_cannot_commit_over_foreign_goal(native, monkeypatch):
    _source, entry, event = await accepted_goal(native, monkeypatch)
    sid, runner, adapter = entry.session_id, native.runner, native.adapter
    GoalManager(sid).set('EXACT independently replaced foreign goal')
    foreign = native.db.get_meta('goal:' + sid)
    native.db._conn.execute("CREATE TRIGGER handoff_fault AFTER INSERT ON state_meta "
                            "WHEN NEW.key LIKE 'goal_maintenance_pause:%' BEGIN "
                            "UPDATE state_meta SET value='{}' WHERE key LIKE 'goal:%'; END")
    runner._ingress_barrier.close()
    adapter._spawn_drain_task(event, entry.session_key)
    try:
        await asyncio.gather(adapter._session_tasks[entry.session_key], return_exceptions=True)
        await asyncio.gather(*getattr(runner, '_deferred_agent_cleanup_tasks', ()), return_exceptions=True)
        await settle(native)
        reopened = SessionDB(db_path=native.db.db_path)
        try:
            assert reopened.get_meta('goal:' + sid) == foreign
            assert not reopened.list_meta_prefix('goal_maintenance_pause:' + sid + ':')
        finally:
            reopened.close()
        assert gateway_admission_status(runner)['known'] is False
        assert not native.effects
    finally:
        native.db._conn.execute('DROP TRIGGER handoff_fault')
