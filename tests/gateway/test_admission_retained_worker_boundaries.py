"""Physical task deletion, all-board inventory and partial removal preserve ownership."""
import os
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from gateway.kanban_quiescence import kanban_worker_work_count
from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_dispatch as kbd


@pytest.fixture
def shared_home(tmp_path, monkeypatch):
    home = tmp_path / '.hermes'
    home.mkdir()
    monkeypatch.setattr(Path, 'home', lambda: tmp_path)
    monkeypatch.setenv('HERMES_HOME', str(home))
    monkeypatch.setenv('HERMES_KANBAN_HOME', str(home))
    for key in ('HERMES_KANBAN_DB', 'HERMES_KANBAN_BOARD'):
        monkeypatch.delenv(key, raising=False)
    return home


@pytest.mark.parametrize('operation', ['direct', 'archive_purge'])
@pytest.mark.parametrize('state', ['running', 'terminal_live', 'foreign', 'settled'])
def test_task_delete_preserves_verified_physical_worker_and_runs(shared_home, operation, state):
    kb.create_board('owned')
    worker = subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.buffer.read(1)'], stdin=subprocess.PIPE)
    try:
        with kbc.connect_closing(board='owned') as conn:
            tid = kb.create_task(conn, title='EXACT known physical ownership', assignee='owner')
            claimed = kb.claim_task(conn, tid)
            assert claimed is not None
            kbd._set_worker_pid(conn, tid, worker.pid, expected_run_id=claimed.current_run_id)
            if state == 'foreign':
                conn.execute("UPDATE task_runs SET claim_lock='foreign:exact-owner' WHERE task_id=?", (tid,))
                conn.commit()
            if state == 'settled':
                worker.stdin.write(b'x')
                worker.stdin.close()
                assert worker.wait(timeout=5) == 0
            if state != 'running' or operation == 'archive_purge':
                assert kb.complete_task(conn, tid, summary='EXACT retained result', expected_run_id=claimed.current_run_id)
            if operation == 'archive_purge':
                assert kb.archive_task(conn, tid)
            before = [tuple(r) for r in conn.execute('SELECT * FROM task_runs WHERE task_id=?', (tid,))]
            delete = kb.delete_task if operation == 'direct' else kb.delete_archived_task
            if state == 'settled':
                assert delete(conn, tid)
            else:
                with pytest.raises(RuntimeError):
                    delete(conn, tid)
                assert kb.get_task(conn, tid) is not None
                assert [tuple(r) for r in conn.execute('SELECT * FROM task_runs WHERE task_id=?', (tid,))] == before
                with kbc.connect_closing(board='owned') as reopened:
                    assert kb.get_task(reopened, tid) is not None
                assert kanban_worker_work_count() > 0
    finally:
        if worker.poll() is None:
            worker.stdin.write(b'x')
            worker.stdin.close()
            assert worker.wait(timeout=5) == 0


@pytest.mark.parametrize('owner', ['default', 'named', 'archived', 'pending'])
def test_database_override_never_hides_real_shared_board_ownership(shared_home, monkeypatch, owner):
    board = 'default' if owner == 'default' else 'owned'
    kb.init_db(board='default')
    if board != 'default':
        kb.create_board(board)
    kb.create_board('empty-override')
    with kbc.connect_closing(board=board) as conn:
        tid = kb.create_task(conn, title='EXACT board claim hidden by override', assignee='owner')
        claimed = kb.claim_task(conn, tid)
        assert claimed is not None
        if owner == 'pending':
            from hermes_cli.kanban_db_spawn import _begin_launch
            _begin_launch(conn, claimed, str(shared_home), board)
    if owner == 'archived':
        kb.remove_board(board, archive=True)
    override = kb.board_dir('empty-override') / 'kanban.db'
    monkeypatch.setenv('HERMES_KANBAN_DB', str(override))
    if owner == 'pending':
        with pytest.raises(RuntimeError, match='ownership UNKNOWN'):
            kanban_worker_work_count()
    else:
        assert kanban_worker_work_count() > 0


@pytest.mark.parametrize('removed', ['database', 'directory'])
def test_partial_removal_after_database_unlink_never_automatically_reopens(shared_home, monkeypatch, removed):
    import shutil
    original_remove = shutil.rmtree
    kb.create_board('owned')
    directory = kb.board_dir('owned')
    payload = directory / 'exact-unremoved-input.txt'
    payload.write_text('EXACT ambiguous removal payload')
    def partial_remove(path):
        assert path == directory
        if removed == 'directory':
            original_remove(path)
        else:
            for name in ('kanban.db-wal', 'kanban.db-shm', 'kanban.db'):
                (path / name).unlink(missing_ok=True)
        raise OSError('inert filesystem failure after SQLite files removed')
    monkeypatch.setattr(shutil, 'rmtree', partial_remove)
    with pytest.raises((OSError, sqlite3.Error)):
        kb.remove_board('owned', archive=False)
    if removed == 'database':
        assert payload.read_text() == 'EXACT ambiguous removal payload'
        assert (directory / 'board.json').exists()
    assert not (directory / 'kanban.db').exists()
    kbc._INITIALIZED_PATHS.clear()
    with pytest.raises(RuntimeError, match='remov'):
        kbc.connect(board='owned')
    with pytest.raises(RuntimeError, match='remov'):
        kb.create_board('owned')
    assert not (directory / 'kanban.db').exists()
    with pytest.raises(RuntimeError, match='remov'):
        kanban_worker_work_count()


@pytest.mark.parametrize('operation', ['events', 'logs', 'workspace', 'completion_cleanup', 'unlink', 'parent_delete'])
def test_live_verified_worker_preserves_ancestor_inputs_and_retention(shared_home, operation):
    kb.create_board('owned')
    worker = subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.buffer.read(1)'], stdin=subprocess.PIPE)
    try:
        with kbc.connect_closing(board='owned') as conn:
            parent = kb.create_task(conn, title='EXACT original physical-worker ancestor')
            assert kb.complete_task(conn, parent, summary='EXACT original handoff result')
            tid = kb.create_task(conn, title='EXACT live terminal child', assignee='owner', parents=[parent])
            claimed = kb.claim_task(conn, tid)
            assert claimed is not None
            kbd._set_worker_pid(conn, tid, worker.pid, expected_run_id=claimed.current_run_id)
            workspace = kb.workspaces_root('owned') / tid
            workspace.mkdir(parents=True)
            payload = workspace / 'exact-physical-input.txt'
            payload.write_text('EXACT physical input required until worker exit')
            if operation == 'completion_cleanup':
                conn.execute("UPDATE tasks SET workspace_kind='scratch', workspace_path=? WHERE id=?", (str(workspace), tid))
                conn.commit()
            assert kb.complete_task(conn, tid, summary='EXACT result before physical exit', expected_run_id=claimed.current_run_id)
            conn.execute('UPDATE task_events SET created_at=1')
            conn.commit()
            before = [tuple(row) for row in conn.execute('SELECT * FROM task_events ORDER BY id')]
            if operation == 'events':
                assert kb.gc_events(conn, older_than_seconds=1) == 0
            elif operation == 'logs':
                log = kb.worker_log_path(tid, board='owned')
                log.parent.mkdir(parents=True, exist_ok=True)
                rotated = log.with_name(log.name + '.1')
                for path in (log, rotated):
                    path.write_text('EXACT physical worker history')
                    os.utime(path, (1, 1))
                assert kb.gc_worker_logs(older_than_seconds=1, board='owned') == 0
                assert log.exists() and rotated.exists()
            elif operation == 'workspace':
                conn.execute("UPDATE tasks SET workspace_kind='scratch', workspace_path=? WHERE id=?", (str(workspace), tid))
                conn.commit()
                kb._cleanup_workspace(conn, tid)
            elif operation == 'unlink':
                with pytest.raises(RuntimeError):
                    kb.unlink_tasks(conn, parent, tid)
            elif operation == 'parent_delete':
                with pytest.raises(RuntimeError):
                    kb.delete_task(conn, parent)
            assert payload.read_text() == 'EXACT physical input required until worker exit'
            assert [tuple(row) for row in conn.execute('SELECT * FROM task_events ORDER BY id')] == before
            assert worker.poll() is None and kanban_worker_work_count() > 0
    finally:
        worker.stdin.write(b'x')
        worker.stdin.close()
        assert worker.wait(timeout=5) == 0


@pytest.mark.parametrize('contents', ['{"owner":"foreign-exact-owner"}', '{malformed', 'null'])
def test_any_external_removal_marker_blocks_native_recreation_without_mutation(shared_home, contents):
    kb.create_board('owned')
    marker = shared_home / 'kanban' / 'board-removals' / 'owned.json'
    marker.parent.mkdir(parents=True)
    marker.write_text(contents)
    for action in (lambda: kbc.connect(board='owned'), lambda: kb.create_board('owned'),
                   lambda: kb.remove_board('owned', archive=False), kanban_worker_work_count):
        with pytest.raises(RuntimeError, match='remov'):
            action()
        assert marker.read_text() == contents


def test_external_removal_survives_database_intent_commit_failure(shared_home):
    kb.create_board('owned')
    with kbc.connect_closing(board='owned') as conn:
        conn.execute("CREATE TRIGGER removal_fault BEFORE INSERT ON task_events "
                     "WHEN NEW.kind='board_removal_pending' BEGIN SELECT RAISE(ABORT, 'inert commit fault'); END")
    with pytest.raises(sqlite3.Error, match='inert commit fault'):
        kb.remove_board('owned', archive=False)
    marker = shared_home / 'kanban' / 'board-removals' / 'owned.json'
    record = json.loads(marker.read_text())
    assert record['operation_id'] and record['owner'] and record['outcome'] == 'UNKNOWN'
    assert (kb.board_dir('owned') / 'kanban.db').exists()
    with pytest.raises(RuntimeError, match='remov'):
        kbc.connect(board='owned')
    with pytest.raises(RuntimeError, match='remov'):
        kanban_worker_work_count()


@pytest.mark.parametrize('change', ['owner', 'version_bool', 'version_float', 'serialization'])
def test_successful_file_removal_never_clears_foreign_replacement_owner(shared_home, monkeypatch, change):
    import shutil
    kb.create_board('owned')
    marker = shared_home / 'kanban' / 'board-removals' / 'owned.json'
    original_remove = shutil.rmtree
    foreign_bytes = []
    def replace_owner_then_remove(path):
        assert marker.exists()
        foreign = json.loads(marker.read_text())
        if change == 'owner':
            foreign['owner'] = 'foreign-owner'
        elif change == 'version_bool':
            foreign['version'] = True
        elif change == 'version_float':
            foreign['version'] = 1.0
        foreign_bytes[:] = [json.dumps(foreign).encode()]
        marker.write_bytes(foreign_bytes[0])
        original_remove(path)
    monkeypatch.setattr(shutil, 'rmtree', replace_owner_then_remove)
    with pytest.raises(RuntimeError, match='ownership changed'):
        kb.remove_board('owned', archive=False)
    assert marker.read_bytes() == foreign_bytes[0] and not kb.board_dir('owned').exists()
    with pytest.raises(RuntimeError, match='remov'):
        kb.create_board('owned')
    assert marker.read_bytes() == foreign_bytes[0]


@pytest.mark.linux_only
def test_dangling_or_aliased_board_paths_never_erase_ownership(shared_home):
    kb.create_board('owned')
    marker = shared_home / 'kanban' / 'board-removals' / 'owned.json'
    marker.parent.mkdir(parents=True)
    marker.symlink_to(shared_home / 'missing-foreign-owner-record')
    with pytest.raises(RuntimeError, match='remov'):
        kbc.connect(board='owned')
    assert marker.is_symlink()
    marker.unlink()  # Explicit fixture reset; no acquired native ownership.
    original = kb.board_dir('owned')
    alias = kb.board_dir('alias')
    alias.symlink_to(original, target_is_directory=True)
    with pytest.raises(RuntimeError, match='unaliased'):
        kb.remove_board('alias', archive=False)
    assert original.is_dir() and alias.is_symlink()


@pytest.mark.parametrize('operation', ['direct', 'archive_purge', 'events', 'logs', 'workspace', 'inventory'])
@pytest.mark.parametrize('metadata', ['{"notification_attention_v1":"foreign-exact-UNKNOWN"}', '{malformed'])
def test_task_attention_survives_every_destructive_sibling(shared_home, operation, metadata):
    from hermes_cli import kanban_db_notify as notify
    kb.create_board('owned')
    with kbc.connect_closing(board='owned') as conn:
        tid = kb.create_task(conn, title='EXACT ambiguous notification obligation')
        assert kb.complete_task(conn, tid, summary='EXACT result required for review')
        if operation == 'archive_purge':
            assert kb.archive_task(conn, tid)
        notify.add_notify_sub(conn, task_id=tid, platform='telegram', chat_id='exact-owner')
        conn.execute('UPDATE kanban_notify_subs SET delivery_metadata=?', (metadata,))
        conn.execute('UPDATE task_events SET created_at=1')
        conn.commit()
        before = [tuple(row) for row in conn.execute('SELECT * FROM task_events ORDER BY id')]
        if operation in {'direct', 'archive_purge'}:
            delete = kb.delete_task if operation == 'direct' else kb.delete_archived_task
            with pytest.raises(RuntimeError):
                delete(conn, tid)
        elif operation == 'events':
            assert kb.gc_events(conn, older_than_seconds=1) == 0
        elif operation == 'logs':
            log = kb.worker_log_path(tid, board='owned')
            log.parent.mkdir(parents=True, exist_ok=True)
            log.write_text('EXACT ambiguous notification history')
            os.utime(log, (1, 1))
            assert kb.gc_worker_logs(older_than_seconds=1, board='owned') == 0
            assert log.exists()
        elif operation == 'workspace':
            workspace = kb.workspaces_root('owned') / tid
            workspace.mkdir(parents=True)
            payload = workspace / 'exact-unknown-input.txt'
            payload.write_text('EXACT ambiguous notification input')
            conn.execute("UPDATE tasks SET workspace_kind='scratch', workspace_path=? WHERE id=?", (str(workspace), tid))
            conn.commit()
            kb._cleanup_workspace(conn, tid)
            assert payload.exists()
        else:
            with pytest.raises(RuntimeError, match='notification.*UNKNOWN'):
                kanban_worker_work_count()
        assert kb.get_task(conn, tid) is not None
        assert conn.execute('SELECT delivery_metadata FROM kanban_notify_subs').fetchone()[0] == metadata
        assert [tuple(row) for row in conn.execute('SELECT * FROM task_events ORDER BY id')] == before
