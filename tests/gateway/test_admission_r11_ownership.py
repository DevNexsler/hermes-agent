"""Conversation replacement and GC cannot erase accepted native obligations."""
import asyncio
import json
import subprocess
import sys

import pytest

from gateway.admission import gateway_admission_status, gateway_ingress
from gateway.config import Platform
from gateway.platforms.event import MessageEvent
from gateway.run import GatewayRunner
from gateway.session import SessionSource
from hermes_cli.goals import GoalManager, migrate_goal_to_session
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native


@pytest.mark.asyncio
@pytest.mark.parametrize('enqueue', ['continuation', 'command'])
@pytest.mark.parametrize('replacement', ['reset', 'suspend'])
@pytest.mark.parametrize('boundary', ['fifo', 'preparation', 'start_hook'])
async def test_queued_goal_cannot_cross_replaced_conversation(native, monkeypatch, enqueue, replacement, boundary):
    runner, adapter = native.runner, native.adapter
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = runner.session_store.get_or_create_session(source)
    sid, key = entry.session_id, entry.session_key
    native.db.append_message(sid, 'user', 'EXACT original conversation history')
    mgr = GoalManager(sid)
    mgr.set('EXACT original goal')
    if enqueue == 'command':
        mgr.pause()
    monkeypatch.setattr('hermes_cli.goals.judge_goal', lambda *a, **k: ('continue', 'more', False, None, False))
    with gateway_ingress(runner):
        if enqueue == 'command':
            await runner._handle_goal_command(MessageEvent(text='/goal resume', source=source))
        else:
            await runner._post_turn_goal_continuation(session_entry=entry, source=source, final_response='EXACT prior result')
    event = adapter._pending_messages.pop(key)
    original_goal = native.db.get_meta('goal:' + sid)
    observed = []
    def change():
        if replacement == 'reset':
            runner.session_store.reset_session(key)
        else:
            runner.session_store.suspend_session(key)
        current = runner.session_store.lookup_by_session_key(key)
        observed[:] = [current.session_id, current.suspended]
    async def prepare(*args):
        if boundary == 'preparation':
            change()
        return runner._PreparedTurn([], '', event.text, event.text, None, None), {}
    async def hook(name, *args):
        if name == 'agent:start' and boundary == 'start_hook':
            change()
    async def model(**kwargs):
        native.effects.append(kwargs['session_id'])
        raise asyncio.CancelledError
    runner._handle_message_with_agent = GatewayRunner._handle_message_with_agent.__get__(runner)
    monkeypatch.setattr(runner, '_hmwa_prepare_turn', prepare)
    monkeypatch.setattr(runner, '_run_agent', model)
    monkeypatch.setattr(runner.hooks, 'emit', hook)
    if boundary == 'fifo':
        change()
    adapter._spawn_drain_task(event, key)
    await asyncio.gather(*adapter._background_tasks, return_exceptions=True)
    assert native.effects == []
    current = runner.session_store.lookup_by_session_key(key)
    assert [current.session_id, current.suspended] == observed
    assert native.db.get_meta('goal:' + sid) == original_goal
    row, = native.db.list_meta_prefix('goal_maintenance_pause:' + sid + ':')
    record = json.loads(row[1])
    assert record['checkpoint'] == json.loads(original_goal)
    assert record['continuation_prompt'] == event.text
    assert record['pause_applied'] is False and record['outcome'] == 'UNKNOWN'
    assert record['ownership_conflict'] and record['required_action']
    assert not gateway_admission_status(runner)['known']
    from hermes_state import SessionDB
    reopened = SessionDB(db_path=native.home / 'state.db')
    try:
        assert json.loads(reopened.get_meta(row[0])) == record
        assert reopened.get_messages(sid)[0]['content'] == 'EXACT original conversation history'
    finally:
        reopened.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('enqueue', ['continuation', 'command'])
async def test_queued_goal_keeps_verified_compression_lineage(native, monkeypatch, enqueue):
    runner, adapter = native.runner, native.adapter
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = runner.session_store.get_or_create_session(source)
    sid, key = entry.session_id, entry.session_key
    mgr = GoalManager(sid)
    mgr.set('EXACT lineage goal')
    if enqueue == 'command':
        mgr.pause()
    monkeypatch.setattr('hermes_cli.goals.judge_goal', lambda *a, **k: ('continue', 'more', False, None, False))
    with gateway_ingress(runner):
        if enqueue == 'command':
            await runner._handle_goal_command(MessageEvent(text='/goal resume', source=source))
        else:
            await runner._post_turn_goal_continuation(session_entry=entry, source=source, final_response='EXACT prior result')
    event = adapter._pending_messages.pop(key)
    child = sid + '-verified-child'
    native.db.publish_compression_child(parent_session_id=sid, child_session_id=child, source='telegram',
        require_compression_lease=False, model='offline', model_config={}, system_prompt='offline',
        messages=[{'role': 'user', 'content': 'EXACT retained summary'}])
    assert migrate_goal_to_session(sid, child)
    assert runner.session_store.advance_compression_session(key, sid, child)
    async def prepare(*args):
        return runner._PreparedTurn([], '', event.text, event.text, None, None), {}
    async def model(**kwargs):
        native.effects.append(kwargs['session_id'])
        raise asyncio.CancelledError
    runner._handle_message_with_agent = GatewayRunner._handle_message_with_agent.__get__(runner)
    monkeypatch.setattr(runner, '_hmwa_prepare_turn', prepare)
    monkeypatch.setattr(runner, '_run_agent', model)
    adapter._spawn_drain_task(event, key)
    await asyncio.gather(*adapter._background_tasks, return_exceptions=True)
    assert native.effects == [child]
    assert GoalManager(child).is_active()
    assert not native.db.list_meta_prefix('goal_maintenance_pause:')


@pytest.mark.asyncio
@pytest.mark.parametrize('operation', ['events', 'logs', 'workspace', 'completion_cleanup', 'delete', 'delete_archived', 'archive'])
async def test_gc_preserves_fast_terminal_unresolved_launch_and_history_across_restart(tmp_path, monkeypatch, operation):
    from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_dispatch as kbd
    from hermes_cli.kanban_db_spawn import pending_worker_launch
    from gateway.kanban_quiescence import kanban_worker_work_count
    monkeypatch.setenv('HERMES_KANBAN_HOME', str(tmp_path))
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'kanban.db'))
    monkeypatch.setattr(kbd, '_profile_exists_fn', lambda: lambda _: True)
    workers = []
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title='EXACT fast terminal worker', assignee='owner',
                             workspace_kind='dir', workspace_path=str(tmp_path))
        conn.execute('CREATE TRIGGER pid_fault BEFORE UPDATE OF worker_pid ON task_runs BEGIN SELECT RAISE(IGNORE); END')
        conn.commit()
        def spawn(task, *args, **kwargs):
            worker = subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.buffer.read(1)'], stdin=subprocess.PIPE)
            workers.append(worker)
            kb.complete_task(conn, task.id, summary='EXACT retained result', expected_run_id=task.current_run_id)
            return worker.pid
        try:
            with pytest.raises(RuntimeError, match='ownership UNKNOWN'):
                kbd.dispatch_once(conn, spawn_fn=spawn)
            assert len(workers) == 1 and workers[0].poll() is None
            assert kb.get_task(conn, tid).status == 'done'
            assert conn.execute('SELECT worker_pid FROM tasks WHERE id=?', (tid,)).fetchone()[0] is None
            assert pending_worker_launch(conn, tid) is not None
            ordinary = kb.create_task(conn, title='ordinary completed retention control')
            kb.complete_task(conn, ordinary, summary='ordinary result')
            conn.execute('UPDATE task_events SET created_at=1')
            conn.commit()
            history = [tuple(r) for r in conn.execute('SELECT * FROM task_events WHERE task_id=? ORDER BY id', (tid,))]
            if operation == 'events':
                assert kb.gc_events(conn, older_than_seconds=0) > 0
                assert not conn.execute('SELECT 1 FROM task_events WHERE task_id=?', (ordinary,)).fetchone()
            elif operation == 'logs':
                import os
                log = kb.worker_log_path(tid)
                log.parent.mkdir(parents=True, exist_ok=True)
                log.write_text('EXACT uncertain worker output')
                os.utime(log, (1, 1))
                normal = kb.worker_log_path(ordinary)
                normal.write_text('ordinary expired log')
                os.utime(normal, (1, 1))
                assert kb.gc_worker_logs(older_than_seconds=0) == 1
                assert log.read_text() == 'EXACT uncertain worker output' and not normal.exists()
            elif operation in {'workspace', 'completion_cleanup'}:
                import argparse
                from hermes_cli.kanban_ops import _cmd_gc
                workspace = kb.workspaces_root() / tid
                workspace.mkdir(parents=True)
                marker = workspace / 'exact-pending-input.txt'
                marker.write_text('EXACT uncertain worker input')
                conn.execute("UPDATE tasks SET status='archived', workspace_kind='scratch', workspace_path=? WHERE id=?", (str(workspace), tid))
                conn.commit()
                if operation == 'workspace':
                    assert _cmd_gc(argparse.Namespace(event_retention_days=1, log_retention_days=1)) == 0
                else:
                    kb._cleanup_workspace(conn, tid)
                assert marker.read_text() == 'EXACT uncertain worker input'
            else:
                if operation == 'delete_archived':
                    conn.execute("UPDATE tasks SET status='archived' WHERE id=?", (tid,))
                    conn.commit()
                with pytest.raises(RuntimeError, match='ownership UNKNOWN'):
                    getattr(kb, {'delete': 'delete_task', 'delete_archived': 'delete_archived_task', 'archive': 'archive_task'}[operation])(conn, tid)
            assert [tuple(r) for r in conn.execute('SELECT * FROM task_events WHERE task_id=? ORDER BY id', (tid,))] == history
            with kbc.connect() as reopened:
                assert pending_worker_launch(reopened, tid) is not None
            with pytest.raises(RuntimeError, match='ownership UNKNOWN'):
                kanban_worker_work_count()
        finally:
            for worker in workers:
                worker.stdin.write(b'x')
                worker.stdin.close()
                assert worker.wait(timeout=5) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('command', ['kickoff', 'resume'])
@pytest.mark.parametrize('path', ['immediate', 'fifo', 'cancel_before_step', 'capacity_refused'])
async def test_goal_command_preserves_original_inputs_on_every_maintenance_path(native, command, path):
    from hermes_cli import active_sessions as active
    runner, adapter = native.runner, native.adapter
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = runner.session_store.get_or_create_session(source)
    if command == 'resume':
        mgr = GoalManager(entry.session_id)
        mgr.set('EXACT goal')
        mgr.pause()
    raw = {'synthetic_original': ['EXACT original payload']}
    event = MessageEvent(text='/goal EXACT goal' if command == 'kickoff' else '/goal resume', source=source,
        message_id='EXACT original message', channel_prompt='EXACT prompt',
        channel_context='EXACT channel context', raw_message=raw)
    expected = {'message_id': event.message_id, 'channel_prompt': event.channel_prompt,
                'channel_context': event.channel_context, 'raw_message': {'synthetic_original': ['EXACT original payload']}}
    slot = None
    try:
        with gateway_ingress(runner):
            if path == 'immediate':
                runner._enter_external_drain()
            await runner._handle_goal_command(event)
        if path != 'immediate':
            queued = adapter._pending_messages[entry.session_key]
            raw['synthetic_original'][0] = 'MUTATED after enqueue'
            if path == 'capacity_refused':
                runner.config.max_concurrent_sessions = 1
                slot, refusal = active.try_acquire_active_session(session_id='other-owner', surface='fixture',
                    config=runner.config, registry_home=native.home, track_liveness=True)
                assert slot is not None and refusal is None
            else:
                runner._enter_external_drain()
            if path == 'fifo':
                await runner._run_agent_drain_pending({'final_response': ''}, adapter, source, entry.session_key)
            else:
                adapter._pending_messages.pop(entry.session_key)
                adapter._spawn_drain_task(queued, entry.session_key)
                task = adapter._session_tasks[entry.session_key]
                if path == 'cancel_before_step':
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                await asyncio.gather(*getattr(runner, '_deferred_agent_cleanup_tasks', ()), return_exceptions=True)
        row, = native.db.list_meta_prefix('goal_maintenance_pause:' + entry.session_id + ':')
        record = json.loads(row[1])
        for key, value in expected.items():
            assert record['input_context'][key] == value
        assert not native.effects
    finally:
        if slot is not None:
            slot.release()


def test_unresolved_child_retains_ancestor_handoff_artifacts_and_events(tmp_path, monkeypatch):
    import argparse
    from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_dispatch as kbd
    from hermes_cli.kanban_ops import _cmd_gc
    monkeypatch.setenv('HERMES_KANBAN_HOME', str(tmp_path))
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    monkeypatch.setattr(kbd, '_profile_exists_fn', lambda: lambda _: True)
    with kbc.connect() as conn:
        grandparent = kb.create_task(conn, title='original root handoff')
        kb.complete_task(conn, grandparent, summary='EXACT root result')
        parent = kb.create_task(conn, title='original parent handoff', parents=[grandparent])
        kb.complete_task(conn, parent, summary='EXACT parent result')
        child = kb.create_task(conn, title='uncertain descendant', parents=[parent], assignee='owner',
                               workspace_kind='dir', workspace_path=str(tmp_path))
        def uncertain(*args, **kwargs):
            raise RuntimeError('custom launch outcome unknown')
        with pytest.raises(RuntimeError, match='ownership UNKNOWN'):
            kbd.dispatch_once(conn, spawn_fn=uncertain)
        assert kb.get_task(conn, child).status == 'running'
        artifacts = []
        for tid in (grandparent, parent):
            directory = kb.workspaces_root() / tid
            directory.mkdir(parents=True)
            artifact = directory / 'EXACT-handoff.txt'
            artifact.write_text('EXACT pending descendant input')
            artifacts.append(artifact)
            with pytest.raises(RuntimeError, match='ownership UNKNOWN'):
                kb.archive_task(conn, tid)
            conn.execute("UPDATE tasks SET status='archived', workspace_kind='scratch', workspace_path=? WHERE id=?", (str(directory), tid))
        conn.execute('UPDATE task_events SET created_at=1')
        conn.commit()
        before = [tuple(r) for r in conn.execute('SELECT * FROM task_events ORDER BY id')]
        assert _cmd_gc(argparse.Namespace(event_retention_days=1, log_retention_days=1)) == 0
        assert [tuple(r) for r in conn.execute('SELECT * FROM task_events ORDER BY id')] == before
        assert all(path.read_text() == 'EXACT pending descendant input' for path in artifacts)


def test_verified_launch_settlement_keeps_ordinary_gc_and_delete_available(tmp_path, monkeypatch):
    from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_dispatch as kbd
    from hermes_cli.kanban_db_spawn import pending_worker_launch
    monkeypatch.setenv('HERMES_KANBAN_HOME', str(tmp_path))
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    monkeypatch.setattr(kbd, '_profile_exists_fn', lambda: lambda _: True)
    worker = None
    with kbc.connect() as conn:
        tid = kb.create_task(conn, title='verified settled launch control', assignee='owner',
                             workspace_kind='dir', workspace_path=str(tmp_path))
        def spawn(*args, **kwargs):
            nonlocal worker
            worker = subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.buffer.read(1)'], stdin=subprocess.PIPE)
            return worker.pid
        try:
            assert [row[0] for row in kbd.dispatch_once(conn, spawn_fn=spawn).spawned] == [tid]
            assert pending_worker_launch(conn, tid) is None
        finally:
            if worker is not None:
                worker.stdin.write(b'x')
                worker.stdin.close()
                assert worker.wait(timeout=5) == 0
        kb.complete_task(conn, tid, summary='EXACT verified result')
        conn.execute('UPDATE task_events SET created_at=1')
        conn.commit()
        assert kb.gc_events(conn, older_than_seconds=0) > 0
        assert kb.delete_task(conn, tid)
