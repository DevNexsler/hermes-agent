"""The resolving receipt cannot invalidate already-read physical ownership."""
import pytest

from gateway.kanban_quiescence import kanban_worker_work_count
from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_dispatch as kbd
from hermes_cli.kanban_db_spawn import pending_worker_launch
from tests.gateway import test_admission_retention_siblings as retention

home, child = retention.home, retention.child


@pytest.mark.parametrize('fast_complete,fault', [
    (False, 'none'), (True, 'none'), (False, 'erase_run_pid'),
    (True, 'erase_run_pid'), (False, 'erase_task_pid'), (False, 'run_update_erases_task')])
def test_actual_dispatch_receipt_preserves_physical_child(home, child, monkeypatch, fast_complete, fault):
    kb.create_board('owned')
    monkeypatch.setattr(kbd, '_profile_exists_fn', lambda: lambda _: True)
    with kbc.connect_closing(board='owned') as conn:
        tid = kb.create_task(conn, title='EXACT original launch payload', assignee='owner',
                             workspace_kind='dir', workspace_path=str(home))
        if fault == 'run_update_erases_task':
            conn.execute('CREATE TRIGGER receipt_fault AFTER UPDATE OF worker_pid ON task_runs '
                         'BEGIN UPDATE tasks SET worker_pid=NULL,worker_started_at=NULL WHERE id=NEW.task_id; END')
            conn.commit()
        elif fault != 'none':
            target = 'task_runs' if fault == 'erase_run_pid' else 'tasks'
            identity = 'id=NEW.run_id' if target == 'task_runs' else 'id=NEW.task_id'
            conn.execute("CREATE TRIGGER receipt_fault AFTER INSERT ON task_events WHEN NEW.kind='spawned' "
                         f"BEGIN UPDATE {target} SET worker_pid=NULL,worker_started_at=NULL WHERE {identity}; END")
            conn.commit()

        def spawn(task, workspace, board=None):
            assert task.id == tid and child.poll() is None
            if fast_complete:
                assert kb.complete_task(conn, tid, result='EXACT result before physical exit',
                                        expected_run_id=task.current_run_id)
            return child.pid

        if fault == 'none':
            result = kbd.dispatch_once(conn, spawn_fn=spawn, board='owned')
            assert [item[0] for item in result.spawned] == [tid]
            assert pending_worker_launch(conn, tid) is None
            assert kanban_worker_work_count() > 0
        else:
            with pytest.raises(RuntimeError, match='ownership UNKNOWN'):
                kbd.dispatch_once(conn, spawn_fn=spawn, board='owned')
            assert pending_worker_launch(conn, tid) is not None
            assert not conn.execute("SELECT 1 FROM task_events WHERE kind='spawned'").fetchone()
            with pytest.raises(RuntimeError, match='ownership UNKNOWN'):
                kanban_worker_work_count()
            with pytest.raises(RuntimeError, match='ownership'):
                kb.delete_task(conn, tid)
        assert child.poll() is None
        with kbc.connect_closing(board='owned') as reopened:
            task = kb.get_task(reopened, tid)
            assert task.title == 'EXACT original launch payload'
            if fast_complete:
                assert task.result == 'EXACT result before physical exit'
