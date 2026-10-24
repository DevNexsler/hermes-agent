"""Partial identity and concurrent board/blob changes preserve actual ownership."""
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import sqlite3
import threading
import time
import uuid

import pytest

from gateway import drain_control
from gateway.kanban_quiescence import kanban_worker_work_count
from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_dispatch as kbd
from hermes_cli.kanban_db_spawn import _begin_launch
from tests.gateway import test_admission_retention_siblings as retention

home, child = retention.home, retention.child


@pytest.mark.linux_only
@pytest.mark.parametrize('operation', ['terminal', 'runtime'])
@pytest.mark.parametrize('fault', ['current_boot', 'current_pid1', 'current_both', 'stored_boot',
                                  'stored_pid1', 'stored_both', 'stored_malformed'])
def test_partial_epoch_never_proves_live_worker_replacement(home, child, monkeypatch, operation, fault):
    kb.create_board('owned')
    epoch = drain_control.current_instantiation_epoch()
    boot, pid1 = epoch.rsplit(':', 1)
    assert boot and pid1
    with kbc.connect_closing(board='owned') as conn:
        tid = kb.create_task(conn, title='EXACT live worker despite missing epoch', assignee='owner',
                             max_runtime_seconds=1)
        claimed = kb.claim_task(conn, tid)
        kbd._set_worker_pid(conn, tid, child.pid, expected_run_id=claimed.current_run_id)
        if fault.startswith('stored_'):
            part = {'stored_boot': ':' + pid1, 'stored_pid1': boot + ':', 'stored_both': '',
                    'stored_malformed': 'not-a-boot-id:' + pid1}[fault]
            fingerprint = part + '|' + kbd._process_fingerprint(child.pid).rsplit('|', 1)[1]
            conn.execute('UPDATE tasks SET worker_started_at=? WHERE id=?', (fingerprint, tid))
            conn.execute('UPDATE task_runs SET worker_started_at=? WHERE id=?',
                         (fingerprint, claimed.current_run_id))
        else:
            part = {'current_boot': ':' + pid1, 'current_pid1': boot + ':', 'current_both': ''}[fault]
            monkeypatch.setattr(drain_control, 'current_instantiation_epoch', lambda: part)
        if operation == 'terminal':
            assert kb.complete_task(conn, tid, summary='EXACT terminal result', expected_run_id=claimed.current_run_id)
        old = int(time.time()) - 86400
        conn.execute('UPDATE tasks SET started_at=? WHERE id=?', (old, tid))
        conn.execute('UPDATE task_runs SET started_at=?,ended_at=CASE WHEN ended_at IS NULL THEN NULL ELSE ? END '
                     'WHERE id=?', (old, old, claimed.current_run_id))
        conn.commit()
        before = tuple(conn.execute('SELECT worker_pid,worker_started_at FROM task_runs WHERE id=?',
                                    (claimed.current_run_id,)).fetchone())
        signals = []
        result = (kbd.reap_terminal_workers if operation == 'terminal' else kbd.enforce_max_runtime)(
            conn, signal_fn=lambda *args: signals.append(args))
        assert result == [] and signals == [] and child.poll() is None
        assert tuple(conn.execute('SELECT worker_pid,worker_started_at FROM task_runs WHERE id=?',
                                  (claimed.current_run_id,)).fetchone()) == before
        assert kanban_worker_work_count() > 0
        with pytest.raises(RuntimeError, match='ownership'):
            kb.delete_task(conn, tid)


@pytest.mark.linux_only
@pytest.mark.parametrize('epoch', ['', ':1', '00000000-0000-4000-8000-000000000001:', 'malformed:1'])
def test_partial_epoch_at_spawn_persists_unverified_owner(home, child, monkeypatch, epoch):
    kb.create_board('owned')
    monkeypatch.setattr(drain_control, 'current_instantiation_epoch', lambda: epoch)
    with kbc.connect_closing(board='owned') as conn:
        tid = kb.create_task(conn, title='EXACT unverified spawn', assignee='owner')
        claimed = kb.claim_task(conn, tid)
        kbd._set_worker_pid(conn, tid, child.pid, expected_run_id=claimed.current_run_id)
        assert conn.execute('SELECT worker_started_at FROM task_runs WHERE id=?',
                            (claimed.current_run_id,)).fetchone()[0] == kbd.UNVERIFIED_WORKER_FINGERPRINT
        assert kanban_worker_work_count() > 0


@pytest.mark.linux_only
def test_complete_previous_boot_identity_remains_positive_replacement_control(home, child):
    kb.create_board('owned')
    with kbc.connect_closing(board='owned') as conn:
        tid = kb.create_task(conn, title='EXACT known prior incarnation', assignee='owner')
        claimed = kb.claim_task(conn, tid)
        kbd._set_worker_pid(conn, tid, child.pid, expected_run_id=claimed.current_run_id)
        full = kbd._process_fingerprint(child.pid)
        prior = str(uuid.uuid4()) + ':' + full.split(':', 1)[1]
        conn.execute('UPDATE task_runs SET worker_started_at=? WHERE id=?', (prior, claimed.current_run_id))
        assert kb.complete_task(conn, tid, summary='EXACT prior result', expected_run_id=claimed.current_run_id)
        conn.execute('UPDATE task_runs SET ended_at=1 WHERE id=?', (claimed.current_run_id,))
        conn.commit()
        signals = []
        assert kbd.reap_terminal_workers(conn, signal_fn=lambda *args: signals.append(args)) == []
        assert signals == [] and child.poll() is None
        assert conn.execute('SELECT worker_pid FROM task_runs WHERE id=?',
                            (claimed.current_run_id,)).fetchone()[0] is None


@pytest.mark.parametrize('board', ['default', 'owned'])
def test_current_board_switch_cannot_redirect_log_retention_database(home, monkeypatch, board):
    kb.init_db(board='default')
    kb.create_board('owned')
    kb.create_board('empty')
    kb.set_current_board(board)
    with kbc.connect_closing(board=board) as conn:
        tid = kb.create_task(conn, title='EXACT pending owner before switch', assignee='owner')
        claimed = kb.claim_task(conn, tid)
        _begin_launch(conn, claimed, str(home), board)
    log = kb.worker_log_path(tid, board=board)
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text('EXACT retained log across supported board switch')
    os.utime(log, (1, 1))
    original = kb.worker_logs_dir
    def select_then_switch(board=None):
        path = original(board=board)
        kb.set_current_board('empty')
        return path
    monkeypatch.setattr(kb, 'worker_logs_dir', select_then_switch)
    assert kb.gc_worker_logs(older_than_seconds=1) == 0
    assert log.read_text() == 'EXACT retained log across supported board switch'
    assert kb.get_current_board() == 'empty'


def test_shared_blob_reference_cannot_commit_between_last_reference_and_unlink(home, monkeypatch):
    kb.create_board('owned')
    blob = home / 'exact-shared-input.txt'
    blob.write_text('EXACT original blob that a competing launch needs')
    with kbc.connect_closing(board='owned') as conn:
        first = kb.create_task(conn, title='settled attachment owner')
        assert kb.complete_task(conn, first, summary='settled result')
        next_task = kb.create_task(conn, title='EXACT competing physical launch', assignee='owner')
        aid = kb.add_attachment(conn, first, filename=blob.name, stored_path=str(blob))
    at_unlink, release = threading.Event(), threading.Event()
    unlink = Path.unlink
    def pause_unlink(path, *args, **kwargs):
        if path == blob:
            at_unlink.set()
            assert release.wait(5)
        return unlink(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'unlink', pause_unlink)
    def delete():
        with kbc.connect_closing(board='owned') as deleting:
            return kb.delete_attachment(deleting, aid)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(delete)
        try:
            assert at_unlink.wait(5)
            with kbc.connect_closing(board='owned') as competing:
                competing.execute('PRAGMA busy_timeout=0')
                try:
                    competing.execute('BEGIN IMMEDIATE')
                except sqlite3.OperationalError as exc:
                    assert 'locked' in str(exc)
                    reference = None
                else:
                    competing.execute('ROLLBACK')
                    reference = kb.add_attachment(competing, next_task, filename=blob.name, stored_path=str(blob))
                    claimed = kb.claim_task(competing, next_task)
                    _begin_launch(competing, claimed, str(home), 'owned')
        finally:
            release.set()
        assert future.result(timeout=5) is not None
    with kbc.connect_closing(board='owned') as reopened:
        if reference is not None:
            assert kb.get_attachment(reopened, reference) is not None
            assert blob.read_text() == 'EXACT original blob that a competing launch needs'
        else:
            # A caller queued behind deletion cannot publish a reference to a missing input.
            with pytest.raises(ValueError, match='blob'):
                kb.add_attachment(reopened, next_task, filename=blob.name, stored_path=str(blob))
            assert kb.list_attachments(reopened, next_task) == []


def test_legacy_alias_reference_retains_shared_blob_for_pending_launch(home):
    kb.create_board('owned')
    blob = home / 'exact-shared-input.txt'
    blob.write_text('EXACT shared ancestor input')
    (home / 'sub').mkdir()
    with kbc.connect_closing(board='owned') as conn:
        first = kb.create_task(conn, title='settled original owner')
        assert kb.complete_task(conn, first, summary='settled result')
        next_task = kb.create_task(conn, title='EXACT retained alias owner', assignee='owner')
        aid = kb.add_attachment(conn, first, filename=blob.name, stored_path=str(blob))
        ref = kb.add_attachment(conn, next_task, filename=blob.name, stored_path=str(home / 'sub' / '..' / blob.name))
        claimed = kb.claim_task(conn, next_task)
        _begin_launch(conn, claimed, str(home), 'owned')
        assert kb.delete_attachment(conn, aid) is not None
        assert kb.get_attachment(conn, ref) is not None
        assert blob.read_text() == 'EXACT shared ancestor input'


def test_missing_blob_registration_never_commits_metadata_or_history(home):
    kb.create_board('owned')
    with kbc.connect_closing(board='owned') as conn:
        tid = kb.create_task(conn, title='EXACT missing input must remain unaccepted')
        before = [tuple(row) for row in conn.execute('SELECT * FROM task_events ORDER BY id')]
        with pytest.raises(ValueError, match='blob'):
            kb.add_attachment(conn, tid, filename='missing.txt', stored_path=str(home / 'missing.txt'))
        assert kb.list_attachments(conn, tid) == []
        assert [tuple(row) for row in conn.execute('SELECT * FROM task_events ORDER BY id')] == before
