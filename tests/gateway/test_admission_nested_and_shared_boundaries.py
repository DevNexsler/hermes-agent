"""Shared input deletion and nested native writers respect exact store ownership."""
import shutil

import pytest

from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_dispatch as kbd
from hermes_cli.kanban_db_spawn import _begin_launch, rotate_settled_worker_log
from tests.gateway import test_admission_retention_siblings as retention

home, child = retention.home, retention.child


@pytest.mark.parametrize('operation', ['delete', 'failed_upload'])
@pytest.mark.parametrize('owner', ['live', 'pending', 'alias', 'unbound', 'settled'])
def test_blob_unlink_keeps_unrelated_retained_workspace_input(home, child, monkeypatch, operation, owner):
    kb.create_board('owned')
    with kbc.connect_closing(board='owned') as conn:
        first = kb.create_task(conn, title='EXACT settled attachment owner')
        assert kb.complete_task(conn, first, summary='EXACT settled result')
        directory = kb.task_attachments_dir(first, board='owned')
        directory.mkdir(parents=True, exist_ok=True)
        shared = directory
        if owner == 'alias':
            shared = home / 'attachment-directory-alias'
            shared.symlink_to(directory, target_is_directory=True)
        second = kb.create_task(conn, title='EXACT input-consuming unrelated worker', assignee='owner',
                                workspace_kind='dir', workspace_path=str(shared))
        claimed = kb.claim_task(conn, second)
        if owner == 'live':
            kbd._set_worker_pid(conn, second, child.pid, expected_run_id=claimed.current_run_id)
        elif owner == 'settled':
            assert kb.complete_task(conn, second, summary='EXACT settled consumer',
                                    expected_run_id=claimed.current_run_id)
        else:
            _begin_launch(conn, claimed, str(directory), 'owned')
            if owner == 'unbound':
                conn.execute('UPDATE tasks SET workspace_path=NULL WHERE id=?', (second,))
                conn.commit()
        blob = directory / 'exact-shared-input.txt'
        if operation == 'delete':
            aid = kb.store_attachment_bytes(conn, first, blob.name, b'EXACT unregistered worker input', board='owned')
            assert kb.delete_attachment(conn, aid) is not None
        else:
            def fail_registration(*args, **kwargs):
                raise RuntimeError('inert metadata registration failure')
            monkeypatch.setattr(kb, 'add_attachment', fail_registration)
            with pytest.raises(RuntimeError, match='metadata registration failure'):
                kb.store_attachment_bytes(conn, first, blob.name, b'EXACT unregistered worker input', board='owned')
        assert kb.list_attachments(conn, second) == []
        if owner == 'settled':
            assert not blob.exists()
        else:
            assert blob.read_bytes() == b'EXACT unregistered worker input'


@pytest.mark.parametrize('operation', ['create', 'comment'])
@pytest.mark.parametrize('state', ['closed', 'open'])
def test_nested_native_writer_checks_closure_and_keeps_outer_transaction(home, monkeypatch, operation, state):
    kb.create_board('owned')
    with kbc.connect_closing(board='owned') as conn:
        first = kb.create_task(conn, title='EXACT original task')
        if state == 'closed':
            def fail_remove(path):
                raise OSError('inert removal failure before any file loss')
            monkeypatch.setattr(shutil, 'rmtree', fail_remove)
            with pytest.raises(OSError, match='before any file loss'):
                kb.remove_board('owned', archive=False)
            marker = kb.boards_root().parent / 'board-removals' / 'owned.json'
            marker_bytes = marker.read_bytes()
        before = [tuple(row) for row in conn.execute('SELECT * FROM task_events ORDER BY id')]
        conn.execute('BEGIN')
        def mutate():
            if operation == 'create':
                return kb.create_task(conn, title='must remain uncommitted', project_id='')
            return kb.add_comment(conn, first, author='test-owned', body='must remain uncommitted')
        if state == 'closed':
            with pytest.raises(RuntimeError, match='remov'):
                mutate()
            assert conn.in_transaction  # Caller still owns its outer transaction.
            conn.commit()
            assert marker.read_bytes() == marker_bytes
            assert [tuple(row) for row in conn.execute('SELECT * FROM task_events ORDER BY id')] == before
        else:
            mutate()
            assert conn.in_transaction
            conn.rollback()
            assert [tuple(row) for row in conn.execute('SELECT * FROM task_events ORDER BY id')] == before


@pytest.mark.parametrize('backups', [0, 1, 2])
def test_implicit_rotation_pins_board_before_supported_current_board_switch(home, monkeypatch, backups):
    kb.create_board('source')
    kb.create_board('other')
    task_id = 't_same_native_identity'
    monkeypatch.setattr(kb, '_new_task_id', lambda: task_id)
    with kbc.connect_closing(board='source') as conn, kbc.connect_closing(board='other') as other:
        assert kb.create_task(conn, title='settled source identity') == task_id
        assert kb.create_task(other, title='EXACT different retained owner', assignee='owner',
                              workspace_kind='dir', workspace_path=str(home)) == task_id
        claimed = kb.claim_task(other, task_id)
        _begin_launch(other, claimed, str(home), 'other')
        originals = {}
        for board in ('source', 'other'):
            path = kb.worker_log_path(task_id, board=board)
            path.parent.mkdir(parents=True, exist_ok=True)
            for generation in range(3):
                log = path if generation == 0 else path.with_name(path.name + f'.{generation}')
                log.write_text(f'EXACT {board} generation {generation}')
                originals[(board, generation)] = log
        before = {path: path.read_bytes() for (board, _), path in originals.items() if board == 'other'}
        kb.set_current_board('source')
        path_fn = kb.kanban_db_path
        switched = []
        def switch_after_validation(board=None):
            path = path_fn(board)
            if not switched:
                switched.append(True)
                kb.set_current_board('other')
            return path
        monkeypatch.setattr(kb, 'kanban_db_path', switch_after_validation)
        monkeypatch.setattr(kbd, 'worker_log_rotation_config', lambda *args: (1, backups))
        rotate_settled_worker_log(conn, task_id, None)
        assert kb.get_current_board() == 'other'
        assert {path: path.read_bytes() for path in before} == before
        source = originals[('source', 0)]
        if backups == 0:
            assert not source.exists()
        else:
            assert originals[('source', 1)].read_text() == 'EXACT source generation 0'
