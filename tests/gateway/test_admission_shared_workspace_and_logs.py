"""Native shared directories, log handoff and inventory keep physical ownership."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest

from gateway.kanban_quiescence import kanban_worker_work_count
from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_dispatch as kbd
from hermes_cli import kanban_db_workspace as kbw, kanban_ops, sqlite_safe_read
from tests.gateway import test_admission_retention_siblings as retention

home, child = retention.home, retention.child


@pytest.mark.parametrize('operation', ['workspace', 'parent', 'gc'])
@pytest.mark.parametrize('overlap', ['same', 'inside', 'outside',
    pytest.param('alias', marks=pytest.mark.require_symlinks), 'settled'])
def test_unrelated_retained_workspace_survives_native_cleanup(home, child, operation, overlap):
    kb.create_board('owned')
    kb.set_current_board('owned')
    with kbc.connect_closing(board='owned') as conn:
        first = kb.create_task(conn, title='EXACT scratch owner')
        workspace = kbw.resolve_workspace(kb.get_task(conn, first), board='owned')
        kbw.set_workspace_path(conn, first, workspace)
        original = workspace / 'exact-unregistered-worker-input.txt'
        original.write_bytes(b'EXACT physical input without attachment metadata')
        parent_child = kb.create_task(conn, title='completed handoff child', parents=[first])
        shared = workspace
        if overlap == 'inside':
            shared = workspace / 'nested'
            shared.mkdir()
        elif overlap == 'outside':
            shared = workspace.parent
        elif overlap == 'alias':
            shared = home / 'workspace-alias'
            shared.symlink_to(workspace, target_is_directory=True)
        second = kb.create_task(conn, title='EXACT unrelated shared workspace', assignee='owner',
                                workspace_kind='dir', workspace_path=str(shared))
        claimed = kb.claim_task(conn, second)
        if overlap == 'settled':
            assert kb.complete_task(conn, second, summary='EXACT settled owner',
                                    expected_run_id=claimed.current_run_id)
        else:
            kbd._set_worker_pid(conn, second, child.pid, expected_run_id=claimed.current_run_id)
        conn.execute("UPDATE tasks SET status='archived' WHERE id IN (?,?)", (first, parent_child))
        conn.commit()
        assert not kb.list_attachments(conn, second)
        if operation == 'workspace':
            kbw._cleanup_workspace(conn, first)
        elif operation == 'parent':
            kbw._try_cleanup_parent_workspaces(conn, parent_child)
        else:
            assert kanban_ops._cmd_gc(SimpleNamespace(event_retention_days=0, log_retention_days=0)) == 0
        if overlap == 'settled':
            assert not workspace.exists()
        else:
            assert child.poll() is None
            assert original.read_bytes() == b'EXACT physical input without attachment metadata'


def test_inventory_blocks_concurrent_raw_header_probe_without_initialization(home, monkeypatch):
    from hermes_cli import kanban_db_spawn
    kb.init_db(board='default')
    database = kb.kanban_db_path(board='default')
    with kbc.connect_closing(board='default') as conn:
        conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        assert conn.execute('PRAGMA journal_mode=DELETE').fetchone()[0] == 'delete'
    assert not sqlite_safe_read.has_live_connection(database)
    before = database.read_bytes()
    entered, finish = threading.Event(), threading.Event()
    count = kanban_db_spawn.worker_ownership_count
    def blocked_count(conn):
        value = count(conn)  # Real read transaction owns rollback-journal snapshot.
        entered.set()
        assert finish.wait(5)
        return value
    monkeypatch.setattr(kanban_db_spawn, 'worker_ownership_count', blocked_count)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(kanban_worker_work_count)
        try:
            assert entered.wait(5)
            # Actual sanctioned probe, concurrent with inventory as sole connection.
            assert sqlite_safe_read.read_header_bytes_preopen(database) is None
        finally:
            finish.set()
        assert future.result(timeout=5) == 0
    assert not sqlite_safe_read.has_live_connection(database)
    assert database.read_bytes() == before
    assert sqlite_safe_read.read_header_bytes_preopen(database).startswith(b'SQLite format 3')


@pytest.mark.parametrize('backups', [0, 1, 2])
@pytest.mark.parametrize('prior', ['live', 'settled', 'competing'])
def test_actual_native_review_spawn_preserves_prior_live_logs(home, child, monkeypatch, backups, prior):
    kb.create_board('owned')
    monkeypatch.setattr(kbd, '_profile_exists_fn', lambda: lambda _: True)
    monkeypatch.setattr(kbd, 'worker_log_rotation_config', lambda *args: (1, backups))
    monkeypatch.setattr(kbd, '_worker_argv', lambda *args: [sys.executable, '-c', 'pass'])
    monkeypatch.setattr(kbd, '_restart_safe_worker_argv', lambda task, argv: argv)
    monkeypatch.setattr(kbd, '_retag_legacy_worker_sessions', lambda *args: None)
    popen, spawned = subprocess.Popen, []
    def capture(*args, **kwargs):
        proc = popen(*args, **kwargs)
        spawned.append(proc)
        return proc
    monkeypatch.setattr(subprocess, 'Popen', capture)
    try:
        with kbc.connect_closing(board='owned') as conn, kbc.connect_closing(board='owned') as other:
            tid = kb.create_task(conn, title='EXACT implementer handoff', assignee='owner',
                                 workspace_kind='dir', workspace_path=str(home))
            claimed = kb.claim_task(conn, tid)
            kbd._set_worker_pid(conn, tid, child.pid, expected_run_id=claimed.current_run_id)
            assert kb.request_review(conn, tid, reviewer='reviewer', summary='EXACT review input',
                                     expected_run_id=claimed.current_run_id)
            if prior != 'live':
                child.stdin.write(b'x')
                child.stdin.close()
                assert child.wait(timeout=5) == 0
            log = kb.worker_log_path(tid, board='owned')
            log.parent.mkdir(parents=True, exist_ok=True)
            paths = [log, log.with_name(log.name + '.1'), log.with_name(log.name + '.2')]
            for index, path in enumerate(paths):
                path.write_bytes(f'EXACT retained generation {index}'.encode())
            before = {path: path.read_bytes() for path in paths}
            blocked = []
            if prior == 'competing':
                other.execute('PRAGMA busy_timeout=0')
                waiting = kb.create_task(conn, title='competing native owner')
                unlink = Path.unlink
                def competing_claim(path, *args, **kwargs):
                    if path in paths:
                        try:
                            other.execute('BEGIN IMMEDIATE')
                        except sqlite3.OperationalError:
                            blocked.append(path)
                        else:
                            other.rollback()
                    return unlink(path, *args, **kwargs)
                monkeypatch.setattr(Path, 'unlink', competing_claim)
            result = kbd.dispatch_once(conn, board='owned', max_spawn=1, max_in_progress=10,
                                        reconcile_orphans=False)
            assert [item[0] for item in result.spawned] == [tid]
            assert len(spawned) == 1 and spawned[0].wait(timeout=5) == 0
            if prior == 'live':
                assert child.poll() is None
                assert {path: path.read_bytes() for path in paths} == before
            elif backups == 0:
                assert log.read_bytes() == b''
            else:
                assert paths[1].read_bytes() == before[log]
            if prior == 'competing':
                assert blocked  # No writer can register ownership across deletion.
                assert kb.claim_task(other, waiting) is not None
    finally:
        for proc in spawned:
            assert proc.wait(timeout=5) == 0
