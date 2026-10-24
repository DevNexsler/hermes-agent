"""Board removal, graph edits, rotated logs and real preparation lineage."""
import asyncio
import json
import os
import sqlite3
import subprocess
import sys
from types import SimpleNamespace

import pytest

from gateway.admission import gateway_admission_status, gateway_ingress
from gateway.config import Platform
from gateway.run import GatewayRunner
from gateway.session import SessionSource
from hermes_cli.goals import GoalManager, migrate_goal_to_session
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native


@pytest.mark.parametrize('state', ['running', 'terminal_live', 'foreign', 'pending', 'unreadable', 'empty', 'settled'])
def test_board_delete_retains_live_or_unverifiable_ownership(tmp_path, monkeypatch, state):
    from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_dispatch as kbd
    from gateway.kanban_quiescence import kanban_worker_work_count
    monkeypatch.setenv('HERMES_KANBAN_HOME', str(tmp_path))
    monkeypatch.delenv('HERMES_KANBAN_DB', raising=False)
    kb.create_board('owned')
    worker = None
    try:
        with kbc.connect_closing(board='owned') as conn:
            if state not in {'empty', 'unreadable'}:
                tid = kb.create_task(conn, title='EXACT board-owned work', assignee='owner')
                kb.claim_task(conn, tid)
                if state in {'terminal_live', 'foreign', 'settled'}:
                    worker = subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.buffer.read(1)'], stdin=subprocess.PIPE)
                    kbd._set_worker_pid(conn, tid, worker.pid)
                    if state == 'foreign':
                        conn.execute("UPDATE task_runs SET claim_lock='unverifiable-foreign-owner' WHERE task_id=?", (tid,))
                        conn.commit()
                    if state == 'settled':
                        worker.stdin.write(b'x')
                        worker.stdin.close()
                        assert worker.wait(timeout=5) == 0
                        worker = None
                    run_id = kb.get_task(conn, tid).current_run_id
                    kb.complete_task(conn, tid, summary='EXACT retained result', expected_run_id=run_id)
                elif state == 'pending':
                    from hermes_cli.kanban_db_spawn import _begin_launch
                    _begin_launch(conn, kb.get_task(conn, tid), str(tmp_path), 'owned')
        kb.set_current_board('owned')
        if state == 'unreadable':
            kb.kanban_db_path('owned').write_bytes(b'unreadable synthetic board ownership')
        directory = kb.board_dir('owned')
        if state in {'empty', 'settled'}:
            assert kb.remove_board('owned', archive=False)['action'] == 'deleted'
            assert not directory.exists() and kb.get_current_board() != 'owned'
        else:
            import sqlite3
            with pytest.raises((RuntimeError, ValueError, sqlite3.Error)):
                kb.remove_board('owned', archive=False)
            assert directory.is_dir() and kb.get_current_board() == 'owned'
            if state in {'pending', 'unreadable'}:
                with pytest.raises((RuntimeError, sqlite3.Error)):
                    kanban_worker_work_count()
            else:
                assert kanban_worker_work_count() > 0
    finally:
        if worker is not None:
            worker.stdin.write(b'x')
            worker.stdin.close()
            assert worker.wait(timeout=5) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('replacement', [False, True])
async def test_goal_real_hygiene_rebinds_only_verified_prepared_lineage(native, monkeypatch, replacement):
    runner, adapter = native.runner, native.adapter
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = runner.session_store.get_or_create_session(source)
    sid, key = entry.session_id, entry.session_key
    GoalManager(sid).set('EXACT goal through preparation')
    monkeypatch.setattr('hermes_cli.goals.judge_goal', lambda *a, **k: ('continue', 'more', False, None, False))
    with gateway_ingress(runner):
        await runner._post_turn_goal_continuation(session_entry=entry, source=source, final_response='EXACT prior result')
    event = adapter._pending_messages.pop(key)
    child = sid + '-hygiene-child'
    async def prepare(event, source, entry, key, quick, generation):
        history = [{'role': 'user', 'content': 'original conversation ' * 100}]
        compressed = [{'role': 'user', 'content': 'EXACT compressed history'}]
        native.db.publish_compression_child(parent_session_id=sid, child_session_id=child, source='telegram',
            require_compression_lease=False, model='offline', model_config={}, system_prompt='offline', messages=compressed)
        assert migrate_goal_to_session(sid, child)
        if replacement:
            GoalManager(child).set('REPLACEMENT independent goal')
        attempt = SimpleNamespace(agent=SimpleNamespace(session_id=child), history=history)
        plan = SimpleNamespace(msg_count=1, approx_tokens=10000, warn_token_threshold=100000)
        rotated, *_ = await runner._hmwa_hygiene_adopt_transcript(attempt, compressed, history, plan,
            session_entry=entry, source=source, _quick_key=quick, run_generation=generation)
        assert rotated and entry.session_id == child
        return runner._PreparedTurn(attempt.history, '', event.text, event.text, None, None, child), {}
    async def model(**kwargs):
        native.effects.append((kwargs['session_id'], kwargs['history']))
        raise asyncio.CancelledError
    runner._handle_message_with_agent = GatewayRunner._handle_message_with_agent.__get__(runner)
    monkeypatch.setattr(runner, '_hmwa_prepare_turn', prepare)
    monkeypatch.setattr(runner, '_run_agent', model)
    adapter._spawn_drain_task(event, key)
    await asyncio.gather(*adapter._background_tasks, return_exceptions=True)
    if replacement:
        assert not native.effects and GoalManager(child).state.goal == 'REPLACEMENT independent goal'
        assert gateway_admission_status(runner)['known'] is False
    else:
        assert len(native.effects) == 1 and native.effects[0][0] == child
        history = native.effects[0][1]
        assert len(history) == 1 and history[0]['role'] == 'user'
        assert history[0]['content'] == 'EXACT compressed history'
        assert isinstance(history[0]['_row_id'], int) and history[0]['timestamp'] > 0
        assert GoalManager(child).is_active()
        assert not native.db.list_meta_prefix('goal_maintenance_pause:')


def test_unlink_cannot_discard_original_unknown_ancestor_inputs(tmp_path, monkeypatch):
    from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_dispatch as kbd
    monkeypatch.setenv('HERMES_KANBAN_HOME', str(tmp_path))
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'board.db'))
    monkeypatch.setattr(kbd, '_profile_exists_fn', lambda: lambda _: True)
    with kbc.connect_closing() as conn:
        parent = kb.create_task(conn, title='EXACT original handoff')
        kb.complete_task(conn, parent, summary='EXACT required parent result')
        child = kb.create_task(conn, title='unknown worker', parents=[parent], assignee='owner',
                               workspace_kind='dir', workspace_path=str(tmp_path))
        def uncertain(*args, **kwargs):
            raise RuntimeError('custom launch outcome unknown')
        with pytest.raises(RuntimeError, match='ownership UNKNOWN'):
            kbd.dispatch_once(conn, spawn_fn=uncertain)
        with pytest.raises(RuntimeError, match='ownership UNKNOWN'):
            kb.unlink_tasks(conn, parent, child)
        assert conn.execute('SELECT 1 FROM task_links WHERE parent_id=? AND child_id=?', (parent, child)).fetchone()
        with pytest.raises(RuntimeError, match='ownership UNKNOWN'):
            kb.delete_task(conn, parent)


def test_gc_keeps_actual_rotated_log_generations_for_unknown_launch(tmp_path, monkeypatch):
    from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_dispatch as kbd
    monkeypatch.setenv('HERMES_KANBAN_HOME', str(tmp_path))
    monkeypatch.setenv('HERMES_KANBAN_DB', str(tmp_path / 'kanban.db'))
    monkeypatch.setattr(kbd, '_profile_exists_fn', lambda: lambda _: True)
    with kbc.connect_closing() as conn:
        tid = kb.create_task(conn, title='EXACT unknown log owner', assignee='owner',
                             workspace_kind='dir', workspace_path=str(tmp_path))
        def uncertain(*args, **kwargs):
            raise RuntimeError('custom launch outcome unknown')
        with pytest.raises(RuntimeError, match='ownership UNKNOWN'):
            kbd.dispatch_once(conn, spawn_fn=uncertain)
    path = kb.worker_log_path(tid)
    path.parent.mkdir(parents=True, exist_ok=True)
    for n in range(3):
        path.write_text('EXACT original generation ' + str(n))
        kbd._rotate_worker_log(path, max_bytes=1, backup_count=2)
    path.write_text('EXACT current generation')
    generations = [path, kbd._rotated_log_path(path, 1), kbd._rotated_log_path(path, 2)]
    before = {p: p.read_bytes() for p in generations}
    for p in generations:
        os.utime(p, (1, 1))
    ordinary = path.parent / 'ordinary.log.1'
    ordinary.write_text('expired normal log')
    os.utime(ordinary, (1, 1))
    assert kb.gc_worker_logs(older_than_seconds=0) == 1
    assert {p: p.read_bytes() for p in generations} == before and not ordinary.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['reset', 'goal', 'suspend'])
async def test_final_executor_snapshot_cannot_authorize_superseded_goal(native, monkeypatch, change):
    import threading
    runner, adapter = native.runner, native.adapter
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = runner.session_store.get_or_create_session(source)
    sid, key = entry.session_id, entry.session_key
    GoalManager(sid).set('EXACT original goal')
    monkeypatch.setattr('hermes_cli.goals.judge_goal', lambda *a, **k: ('continue', 'more', False, None, False))
    with gateway_ingress(runner):
        await runner._post_turn_goal_continuation(session_entry=entry, source=source, final_response='EXACT prior result')
    event = adapter._pending_messages.pop(key)
    snapshot_done = threading.Event()
    changed = asyncio.Event()
    loop = asyncio.get_running_loop()
    original = runner._queued_goal_owner
    def replace():
        if change == 'goal':
            GoalManager(sid).set('REPLACEMENT goal')
        elif change == 'reset':
            runner.session_store.reset_session(key)
        else:
            runner.session_store.suspend_session(key)
        changed.set()
    def final_snapshot(*args):
        result = original(*args)
        if getattr(event, '_goal_resolved_session_id', None) and not snapshot_done.is_set():
            snapshot_done.set()
            # This callback precedes delivery of the real executor's result.
            loop.call_soon_threadsafe(replace)
        return result
    async def prepare(*args):
        return runner._PreparedTurn([], '', event.text, event.text, None, None, sid), {}
    async def model(**kwargs):
        native.effects.append(kwargs['session_id'])
        raise asyncio.CancelledError
    monkeypatch.setattr(runner, '_queued_goal_owner', final_snapshot)
    runner._handle_message_with_agent = GatewayRunner._handle_message_with_agent.__get__(runner)
    monkeypatch.setattr(runner, '_hmwa_prepare_turn', prepare)
    monkeypatch.setattr(runner, '_run_agent', model)
    adapter._spawn_drain_task(event, key)
    await asyncio.gather(*adapter._background_tasks, return_exceptions=True)
    assert changed.is_set() and snapshot_done.is_set() and not native.effects
    assert native.db.list_meta_prefix('goal_maintenance_pause:' + sid + ':')
    assert gateway_admission_status(runner)['known'] is False
    if change == 'goal':
        assert GoalManager(sid).state.goal == 'REPLACEMENT goal'


@pytest.mark.asyncio
@pytest.mark.parametrize('contender', ['route', 'goal', None])
async def test_final_goal_claim_serializes_entry_without_waiting(native, monkeypatch, contender):
    from gateway import admission_capacity
    runner = native.runner
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = runner.session_store.get_or_create_session(source)
    mgr = GoalManager(entry.session_id)
    mgr.set('EXACT final claim goal')
    event = runner._synthetic_prompt_event(source, 'EXACT pending continuation')
    event._goal_admission_checkpoint = {'session_id': entry.session_id, 'goal_state': mgr.state.to_json(),
                                        'final_response': 'EXACT prior result'}
    writer = sqlite3.connect(native.db.db_path, timeout=0, isolation_level=None)
    store = runner.session_store
    marked = []
    original_mark = admission_capacity.mark_event_execution_started
    def check_locks(current):
        assert not store._lock.acquire(blocking=False)
        with pytest.raises(sqlite3.OperationalError, match='locked'):
            writer.execute('BEGIN IMMEDIATE')
        marked.append(current)
        original_mark(current)
    monkeypatch.setattr(admission_capacity, 'mark_event_execution_started', check_locks)
    try:
        if contender == 'route':
            store._lock.acquire()
        elif contender == 'goal':
            writer.execute('BEGIN IMMEDIATE')
        try:
            claimed = runner._claim_goal_entry(event, entry.session_id)
        finally:
            if contender == 'route':
                store._lock.release()
            writer.rollback()
        if contender is None:
            assert claimed and marked == [event] and event._heartbeat_execution_started
        else:
            assert not claimed and not marked
            assert await runner._defer_queued_goal_for_maintenance(event)
            row, = native.db.list_meta_prefix('goal_maintenance_pause:')
            record = json.loads(row[1])
            assert record['outcome'] == 'UNKNOWN' and record['continuation_prompt'] == event.text
            assert record['checkpoint'] == json.loads(event._goal_admission_checkpoint['goal_state'])
            assert GoalManager(entry.session_id).state.to_json() == mgr.state.to_json()
            assert not gateway_admission_status(runner)['known']
    finally:
        writer.close()


@pytest.mark.asyncio
async def test_final_goal_claim_uses_owning_database_a_b_a(native):
    from gateway.run import _profile_runtime_scope
    from gateway.platforms.event import MessageEvent
    from agent.secret_scope import is_multiplex_active, set_multiplex_active
    from hermes_constants import get_hermes_home
    b = native.home / 'profiles' / 'b'
    b.mkdir(parents=True)
    (b / 'config.yaml').write_text('{}\n')
    runner = native.runner
    runner.config.multiplex_profiles = True
    prior = is_multiplex_active()
    set_multiplex_active(True)
    try:
        for index, (profile, home) in enumerate(((None, native.home), ('b', b), (None, native.home))):
            source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm', profile=profile)
            assert runner._resolve_profile_home_for_source(source).resolve() == home.resolve()
            with _profile_runtime_scope(home, {}):
                entry = runner.session_store.get_or_create_session(source)
                mgr = GoalManager(entry.session_id)
                mgr.set('EXACT routed goal ' + str(index))
                event = MessageEvent(text='EXACT routed continuation', source=source)
                event._goal_admission_checkpoint = {'session_id': entry.session_id, 'goal_state': mgr.state.to_json(),
                                                    'final_response': ''}
            assert get_hermes_home() == native.home
            assert runner._claim_goal_entry(event, entry.session_id)
            assert get_hermes_home() == native.home
            db = runner.session_store._db_for_key(entry.session_key)
            assert db.db_path.resolve() == home.resolve() / 'state.db'
            assert json.loads(db.get_meta('goal:' + entry.session_id))['goal'] == 'EXACT routed goal ' + str(index)
    finally:
        set_multiplex_active(prior)
        from hermes_state_registry import close_all_under
        close_all_under(b)


@pytest.mark.parametrize('metadata', ['{"notification_attention_v1":{"owner":"foreign"}}', '{malformed', '[]'])
def test_board_delete_preserves_any_unknown_notification_attention(tmp_path, monkeypatch, metadata):
    from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_notify as notify
    monkeypatch.setenv('HERMES_KANBAN_HOME', str(tmp_path))
    monkeypatch.delenv('HERMES_KANBAN_DB', raising=False)
    kb.create_board('owned')
    with kbc.connect_closing(board='owned') as conn:
        tid = kb.create_task(conn, title='EXACT notification input')
        kb.complete_task(conn, tid, summary='EXACT retained result')
        notify.add_notify_sub(conn, task_id=tid, platform='telegram', chat_id='synthetic')
        conn.execute('UPDATE kanban_notify_subs SET delivery_metadata=?', (metadata,))
        conn.commit()
    with pytest.raises((RuntimeError, ValueError)):
        kb.remove_board('owned', archive=False)
    with kbc.connect_closing(board='owned') as conn:
        assert conn.execute('SELECT delivery_metadata FROM kanban_notify_subs').fetchone()[0] == metadata
        assert kb.get_task(conn, tid).title == 'EXACT notification input'


@pytest.mark.linux_only
def test_board_delete_serializes_preopened_native_claim_writer(tmp_path, monkeypatch):
    import shutil
    from hermes_cli import kanban_db as kb, kanban_db_connect as kbc
    monkeypatch.setenv('HERMES_KANBAN_HOME', str(tmp_path))
    monkeypatch.delenv('HERMES_KANBAN_DB', raising=False)
    kb.create_board('owned')
    with kbc.connect_closing(board='owned') as conn:
        tid = kb.create_task(conn, title='EXACT unclaimed task', assignee='owner', initial_status='blocked')
        assert kb.unblock_task(conn, tid) and kb.get_task(conn, tid).status == 'ready'
        conn.execute('BEGIN IMMEDIATE')
        with pytest.raises(sqlite3.OperationalError, match='locked'):
            kb.remove_board('owned', archive=False)
        conn.rollback()
        original = shutil.rmtree
        observed = []
        def remove(path):
            # Durable closure rejects native admission even after its SQL writer lock releases.
            conn.execute('PRAGMA busy_timeout=0')
            with pytest.raises(RuntimeError, match='removal ownership UNKNOWN'):
                kb.claim_task(conn, tid)
            observed.append(path)
            return original(path)
        monkeypatch.setattr(shutil, 'rmtree', remove)
        assert kb.remove_board('owned', archive=False)['action'] == 'deleted'
        assert observed == [kb.board_dir('owned')]
        # Stale native connection must not launch from the removed board after lock release.
        try:
            claimed = kb.claim_task(conn, tid)
        except (sqlite3.Error, RuntimeError):
            claimed = None
        assert claimed is None
        monkeypatch.setattr(shutil, 'rmtree', original)
        with kbc.connect_closing(board='owned') as fresh:
            fresh_id = kb.create_task(fresh, title='NEW independent work after explicit recreation', assignee='owner')
            assert kb.claim_task(fresh, fresh_id) is not None
        with pytest.raises(RuntimeError, match='board removal ownership UNKNOWN'):
            kb.claim_task(conn, tid)


def test_board_delete_checks_removed_directory_despite_database_override(tmp_path, monkeypatch):
    from hermes_cli import kanban_db as kb, kanban_db_connect as kbc
    monkeypatch.setenv('HERMES_KANBAN_HOME', str(tmp_path))
    monkeypatch.delenv('HERMES_KANBAN_DB', raising=False)
    kb.create_board('owned')
    kb.create_board('unrelated')
    with kbc.connect_closing(board='owned') as conn:
        tid = kb.create_task(conn, title='EXACT live board ownership', assignee='owner')
        assert kb.claim_task(conn, tid) is not None
    monkeypatch.setenv('HERMES_KANBAN_DB', str(kb.board_dir('unrelated') / 'kanban.db'))
    with pytest.raises(RuntimeError, match='physical work'):
        kb.remove_board('owned', archive=False)
    assert kb.board_dir('owned').is_dir() and kb.board_dir('unrelated').is_dir()


@pytest.mark.parametrize('failure', [RuntimeError, KeyboardInterrupt])
def test_board_removal_crash_retains_durable_stop_across_reopen(tmp_path, monkeypatch, failure):
    import shutil
    from hermes_cli import kanban_db as kb, kanban_db_connect as kbc
    from gateway.kanban_quiescence import kanban_worker_work_count
    monkeypatch.setenv('HERMES_KANBAN_HOME', str(tmp_path))
    monkeypatch.delenv('HERMES_KANBAN_DB', raising=False)
    kb.create_board('owned')
    with kbc.connect_closing(board='owned') as conn:
        tid = kb.create_task(conn, title='EXACT not-yet-claimed removal input', initial_status='blocked')
    def failed_remove(path):
        raise failure('inert failure after durable removal intent')
    monkeypatch.setattr(shutil, 'rmtree', failed_remove)
    with pytest.raises(failure):
        kb.remove_board('owned', archive=False)
    path = kb.board_dir('owned') / 'kanban.db'
    from contextlib import closing
    with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)) as reopened:
        reopened.row_factory = sqlite3.Row
        row, = reopened.execute("SELECT payload FROM task_events WHERE kind='board_removal_pending'").fetchall()
        record = json.loads(row[0])
        assert record['version'] == 1 and record['board'] == 'owned'
        assert record['owner'] and record['operation_id'] and record['required_action']
        assert record['outcome'] == 'UNKNOWN' and kb.get_task(reopened, tid).title.startswith('EXACT')
    with pytest.raises(RuntimeError, match='board removal ownership UNKNOWN'):
        kbc.connect(board='owned')
    with pytest.raises(RuntimeError, match='board removal ownership UNKNOWN'):
        kanban_worker_work_count()
    with pytest.raises(RuntimeError, match='board removal ownership UNKNOWN'):
        kb.remove_board('owned', archive=False)
