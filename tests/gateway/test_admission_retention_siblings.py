"""Unreadable identity and alternate retention paths never discard owned evidence."""
import os
from pathlib import Path
import subprocess
import sys
import time

import pytest

from gateway.kanban_quiescence import kanban_worker_work_count
from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_dispatch as kbd
from hermes_cli.kanban_db_spawn import _begin_launch


@pytest.fixture
def home(tmp_path, monkeypatch):
    home = tmp_path / 'hermes'
    home.mkdir()
    monkeypatch.setattr(Path, 'home', lambda: tmp_path)
    monkeypatch.setenv('HERMES_HOME', str(home))
    monkeypatch.setenv('HERMES_KANBAN_HOME', str(home))
    for name in ('HERMES_KANBAN_DB', 'HERMES_KANBAN_BOARD'):
        monkeypatch.delenv(name, raising=False)
    return home


@pytest.fixture
def child():
    proc = subprocess.Popen([sys.executable, '-c', 'import sys; sys.stdin.buffer.read(1)'],
                            stdin=subprocess.PIPE)
    try:
        yield proc
    finally:
        if proc.poll() is None:
            proc.stdin.write(b'x')
            proc.stdin.close()
        assert proc.wait(timeout=5) == 0


@pytest.mark.parametrize('fingerprint', ['epoch', 'legacy'])
@pytest.mark.parametrize('operation', ['terminal', 'runtime', 'crash', 'orphan', 'stale', 'terminate'])
def test_unreadable_identity_keeps_live_worker_evidence(home, child, monkeypatch, fingerprint, operation):
    import gateway.status as status
    kb.create_board('owned')
    with kbc.connect_closing(board='owned') as conn:
        tid = kb.create_task(conn, title='EXACT physical input', assignee='owner', max_runtime_seconds=1)
        claimed = kb.claim_task(conn, tid)
        kbd._set_worker_pid(conn, tid, child.pid, expected_run_id=claimed.current_run_id)
        if fingerprint == 'legacy':
            start = status.get_process_start_time(child.pid)
            assert start is not None
            conn.execute('UPDATE tasks SET worker_started_at=? WHERE id=?', (start, tid))
            conn.execute('UPDATE task_runs SET worker_started_at=? WHERE id=?', (start, claimed.current_run_id))
        if operation == 'terminal':
            assert kb.complete_task(conn, tid, summary='EXACT result', expected_run_id=claimed.current_run_id)
        old = int(time.time()) - 86400
        conn.execute('UPDATE tasks SET started_at=?, claim_expires=?, last_heartbeat_at=? WHERE id=?',
                     (old, old, old, tid))
        conn.execute('UPDATE task_runs SET started_at=?, ended_at=CASE WHEN ended_at IS NULL THEN NULL ELSE ? END '
                     'WHERE id=?', (old, old, claimed.current_run_id))
        if operation == 'orphan':
            conn.execute('UPDATE tasks SET claim_expires=NULL WHERE id=?', (tid,))
        conn.commit()
        before = tuple(conn.execute('SELECT worker_pid,worker_started_at FROM task_runs WHERE id=?',
                                    (claimed.current_run_id,)).fetchone())
        signals = []
        monkeypatch.setattr(status, '_get_process_start_time', lambda pid: None)
        signal_fn = lambda *args: signals.append(args)
        if operation == 'terminal':
            assert kbd.reap_terminal_workers(conn, signal_fn=signal_fn) == []
        elif operation == 'runtime':
            assert kbd.enforce_max_runtime(conn, signal_fn=signal_fn) == []
        elif operation == 'crash':
            assert kbd.detect_crashed_workers(conn) == []
        elif operation == 'orphan':
            assert kbd.reconcile_orphaned_running(conn) == []
        elif operation == 'stale':
            assert kbd.detect_stale_running(conn, stale_timeout_seconds=1, signal_fn=signal_fn) == []
        else:
            info = kbd._terminate_reclaimed_worker(child.pid, claimed.claim_lock, started_at=before[1],
                                                    signal_fn=signal_fn)
            assert info['signal_refused'] and not info['terminated']
        assert signals == [] and child.poll() is None
        assert tuple(conn.execute('SELECT worker_pid,worker_started_at FROM task_runs WHERE id=?',
                                  (claimed.current_run_id,)).fetchone()) == before
        assert kanban_worker_work_count() > 0
        with pytest.raises(RuntimeError, match='ownership'):
            kb.delete_task(conn, tid)
        with kbc.connect_closing(board='owned') as reopened:
            assert kb.get_task(reopened, tid) is not None


@pytest.mark.parametrize('operation', ['terminal', 'runtime'])
def test_identity_lost_after_term_never_escalates_or_discards_owner(home, child, monkeypatch, operation):
    import gateway.status as status
    kb.create_board('owned')
    with kbc.connect_closing(board='owned') as conn:
        tid = kb.create_task(conn, title='EXACT physical ownership during signal', assignee='owner',
                             max_runtime_seconds=1)
        claimed = kb.claim_task(conn, tid)
        kbd._set_worker_pid(conn, tid, child.pid, expected_run_id=claimed.current_run_id)
        if operation == 'terminal':
            assert kb.complete_task(conn, tid, summary='EXACT retained result', expected_run_id=claimed.current_run_id)
        old = int(time.time()) - 86400
        conn.execute('UPDATE tasks SET started_at=? WHERE id=?', (old, tid))
        conn.execute('UPDATE task_runs SET started_at=?,ended_at=CASE WHEN ended_at IS NULL THEN NULL ELSE ? END '
                     'WHERE id=?', (old, old, claimed.current_run_id))
        conn.commit()
        signals = []
        def signal_fn(pid, sig):
            # Inert signal transport: child stays physically alive; identity probe fails afterward.
            signals.append((pid, sig))
            monkeypatch.setattr(status, '_get_process_start_time', lambda pid: None)
        result = (kbd.reap_terminal_workers if operation == 'terminal' else kbd.enforce_max_runtime)(
            conn, signal_fn=signal_fn)
        assert result == [] and len(signals) == 1 and child.poll() is None
        assert conn.execute('SELECT worker_pid FROM task_runs WHERE id=?',
                            (claimed.current_run_id,)).fetchone()[0] == child.pid
        assert kanban_worker_work_count() > 0


def test_local_observed_exit_allows_terminal_cleanup_even_if_fingerprint_unreadable(home, child, monkeypatch):
    import gateway.status as status
    kb.create_board('owned')
    with kbc.connect_closing(board='owned') as conn:
        tid = kb.create_task(conn, title='EXACT settled ownership', assignee='owner')
        claimed = kb.claim_task(conn, tid)
        kbd._set_worker_pid(conn, tid, child.pid, expected_run_id=claimed.current_run_id)
        assert kb.complete_task(conn, tid, summary='EXACT result', expected_run_id=claimed.current_run_id)
        conn.execute('UPDATE task_runs SET ended_at=1 WHERE id=?', (claimed.current_run_id,))
        conn.commit()
        child.stdin.write(b'x')
        child.stdin.close()
        assert child.wait(timeout=5) == 0
        monkeypatch.setattr(status, '_get_process_start_time', lambda pid: None)
        assert kbd.reap_terminal_workers(conn) == []
        assert conn.execute('SELECT worker_pid FROM task_runs WHERE id=?',
                            (claimed.current_run_id,)).fetchone()[0] is None
        assert kanban_worker_work_count() == 0
        assert kb.delete_task(conn, tid)


@pytest.mark.parametrize('board', ['default', 'owned'])
@pytest.mark.parametrize('state', ['pending', 'attention', 'live'])
def test_log_retention_binds_actual_board_despite_override(home, child, monkeypatch, board, state):
    from hermes_cli.kanban_db_notify import add_notify_sub
    kb.init_db(board='default')
    kb.create_board('owned')
    kb.create_board('empty')
    with kbc.connect_closing(board=board) as conn:
        tid = kb.create_task(conn, title='EXACT retained log identity', assignee='owner')
        claimed = kb.claim_task(conn, tid)
        if state == 'pending':
            _begin_launch(conn, claimed, str(home), board)
        elif state == 'live':
            kbd._set_worker_pid(conn, tid, child.pid, expected_run_id=claimed.current_run_id)
            assert kb.complete_task(conn, tid, summary='EXACT completed result', expected_run_id=claimed.current_run_id)
        else:
            assert kb.complete_task(conn, tid, summary='EXACT notification result', expected_run_id=claimed.current_run_id)
            add_notify_sub(conn, task_id=tid, platform='telegram', chat_id='owner')
            conn.execute("UPDATE kanban_notify_subs SET delivery_metadata='{malformed'")
            conn.commit()
        log = kb.worker_log_path(tid, board=board)
        log.parent.mkdir(parents=True, exist_ok=True)
        paths = [log, log.with_name(log.name + '.1')]
        for path in paths:
            path.write_text('EXACT historical physical input')
            os.utime(path, (1, 1))
        # A -> B override -> A; neither scope may collect A's unresolved evidence.
        for override in (None, kb.board_dir('empty') / 'kanban.db', None):
            if override is None:
                monkeypatch.delenv('HERMES_KANBAN_DB', raising=False)
            else:
                monkeypatch.setenv('HERMES_KANBAN_DB', str(override))
            if override is None:
                assert kb.gc_worker_logs(older_than_seconds=1, board=board) == 0
            else:
                with pytest.raises(RuntimeError, match='ownership.*override'):
                    kb.gc_worker_logs(older_than_seconds=1, board=board)
            assert all(path.read_text() == 'EXACT historical physical input' for path in paths)


def test_log_gc_refuses_missing_owner_store_without_creating_empty_replacement(home):
    import sqlite3
    log = kb.worker_log_path('exact-unreadable-owner', board='default')
    log.parent.mkdir(parents=True)
    log.write_text('EXACT retained evidence whose database is missing')
    os.utime(log, (1, 1))
    with pytest.raises(sqlite3.Error):
        kb.gc_worker_logs(older_than_seconds=1, board='default')
    assert log.exists() and not (home / 'kanban.db').exists()


@pytest.mark.parametrize('owner', ['child', 'ancestor'])
@pytest.mark.parametrize('state', ['pending', 'attention', 'live', 'settled'])
def test_attachment_delete_preserves_original_retained_inputs(home, child, owner, state):
    from hermes_cli.kanban_db_notify import add_notify_sub
    kb.create_board('owned')
    with kbc.connect_closing(board='owned') as conn:
        parent = kb.create_task(conn, title='EXACT ancestor handoff')
        assert kb.complete_task(conn, parent, summary='EXACT ancestor result')
        tid = kb.create_task(conn, title='EXACT child input', assignee='owner', parents=[parent])
        claimed = kb.claim_task(conn, tid)
        if state == 'pending':
            _begin_launch(conn, claimed, str(home), 'owned')
        elif state == 'live':
            kbd._set_worker_pid(conn, tid, child.pid, expected_run_id=claimed.current_run_id)
            assert kb.complete_task(conn, tid, summary='EXACT result', expected_run_id=claimed.current_run_id)
        else:
            assert kb.complete_task(conn, tid, summary='EXACT result', expected_run_id=claimed.current_run_id)
            if state == 'attention':
                add_notify_sub(conn, task_id=tid, platform='telegram', chat_id='owner')
                conn.execute("UPDATE kanban_notify_subs SET delivery_metadata='{malformed'")
                conn.commit()
        attachment_owner = tid if owner == 'child' else parent
        blob = home / 'exact-original-payload.txt'
        blob.write_text('EXACT original input before any replay decision')
        aid = kb.add_attachment(conn, attachment_owner, filename=blob.name, stored_path=str(blob))
        if state == 'settled':
            assert kb.delete_attachment(conn, aid) is not None
            assert not blob.exists() and kb.get_attachment(conn, aid) is None
        else:
            before = [tuple(row) for row in conn.execute('SELECT * FROM task_events ORDER BY id')]
            with pytest.raises(RuntimeError, match='ownership'):
                kb.delete_attachment(conn, aid)
            assert blob.read_text() == 'EXACT original input before any replay decision'
            assert kb.get_attachment(conn, aid) is not None
            assert [tuple(row) for row in conn.execute('SELECT * FROM task_events ORDER BY id')] == before
            with kbc.connect_closing(board='owned') as reopened:
                assert kb.get_attachment(reopened, aid) is not None
