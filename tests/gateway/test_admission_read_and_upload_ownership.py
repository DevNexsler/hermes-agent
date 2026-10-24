"""Malformed identity, detached reads and failed uploads keep real work/input owners."""
import asyncio
from pathlib import Path
import threading
import time
import sqlite3
from types import SimpleNamespace

import pytest

from gateway.admission import gateway_admission_status, gateway_ingress
from gateway.config import Platform
from gateway.platforms.event import MessageEvent
from gateway.session import SessionSource
from hermes_cli import kanban_db as kb, kanban_db_connect as kbc, kanban_db_dispatch as kbd
from hermes_cli.kanban_db_spawn import _begin_launch
from tests.gateway import test_admission_native_work, test_admission_retention_siblings

native = test_admission_native_work.native
home, child = test_admission_retention_siblings.home, test_admission_retention_siblings.child


@pytest.mark.parametrize('fingerprint', ['NaN', 'Infinity', '-Infinity', '0', '-1', '1e10000'])
@pytest.mark.parametrize('operation', ['terminal', 'runtime'])
def test_invalid_scalar_never_proves_exit_or_authorizes_signal(home, child, fingerprint, operation):
    kb.create_board('owned')
    with kbc.connect_closing(board='owned') as conn:
        tid = kb.create_task(conn, title='EXACT live worker malformed scalar', assignee='owner',
                             max_runtime_seconds=1)
        claimed = kb.claim_task(conn, tid)
        kbd._set_worker_pid(conn, tid, child.pid, expected_run_id=claimed.current_run_id)
        conn.execute('UPDATE tasks SET worker_started_at=?,started_at=1 WHERE id=?', (fingerprint, tid))
        conn.execute('UPDATE task_runs SET worker_started_at=?,started_at=1 WHERE id=?',
                     (fingerprint, claimed.current_run_id))
        if operation == 'terminal':
            assert kb.complete_task(conn, tid, summary='EXACT result', expected_run_id=claimed.current_run_id)
            conn.execute('UPDATE task_runs SET ended_at=1 WHERE id=?', (claimed.current_run_id,))
        conn.commit()
        signals = []
        assert (kbd.reap_terminal_workers if operation == 'terminal' else kbd.enforce_max_runtime)(
            conn, signal_fn=lambda *args: signals.append(args)) == []
        assert signals == [] and child.poll() is None
        assert conn.execute('SELECT worker_pid FROM task_runs WHERE id=?',
                            (claimed.current_run_id,)).fetchone()[0] == child.pid
        with pytest.raises(RuntimeError, match='ownership'):
            kb.delete_task(conn, tid)


@pytest.mark.asyncio
@pytest.mark.parametrize('finish', ['timeout', 'cancel'])
async def test_real_media_transcript_read_remains_owned_after_waiter_ends(native, monkeypatch, finish):
    runner, adapter = native.runner, native.adapter
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = runner.session_store.get_or_create_session(source)
    native.db.append_message(entry.session_id, 'user', 'EXACT prior transcript')
    pdf = native.home / 'exact-current-output.pdf'
    pdf.write_bytes(b'EXACT inert output')
    monkeypatch.setattr('gateway.platforms.base.MEDIA_DELIVERY_SAFE_ROOTS', (native.home,))
    monkeypatch.setattr('gateway.platforms.base._HISTORY_MEDIA_LOOKUP_TIMEOUT_SECONDS', .03)
    started, release, finished = threading.Event(), threading.Event(), threading.Event()
    load = runner.session_store.load_transcript
    transcripts = []
    def blocked_load(sid):
        started.set()
        assert release.wait(5)
        try:
            result = load(sid)
            transcripts.extend(result)
            return result
        finally:
            finished.set()
    monkeypatch.setattr(runner.session_store, 'load_transcript', blocked_load)
    event = MessageEvent(text='EXACT request', source=source)
    try:
        with gateway_ingress(runner):
            task = asyncio.create_task(adapter._extract_response_content(
                f'Generated file: {pdf}', event, entry.session_key, is_ephemeral_response=False))
            assert await asyncio.to_thread(started.wait, 2)
            if finish == 'cancel':
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                extracted = await asyncio.wait_for(task, 2)
                assert str(pdf) in extracted.local_files
        runner._ingress_barrier.close()
        status = gateway_admission_status(runner)
        assert status['known'] and status['closed'] and status['ingress'] == status['adopted'] == 0
        assert status['active_work'] > 0 and not status['quiescent'] and not finished.is_set()
    finally:
        release.set()
        assert await asyncio.to_thread(finished.wait, 2)
        deadline = time.monotonic() + 2
        while runner._active_deferred_agent_worker_count() and time.monotonic() < deadline:
            await asyncio.sleep(0)
    assert transcripts and transcripts[0]['content'] == 'EXACT prior transcript'
    assert gateway_admission_status(runner)['quiescent']


@pytest.mark.asyncio
async def test_media_reader_registration_survives_thread_tail_after_sqlite_completion(native, monkeypatch):
    runner, adapter = native.runner, native.adapter
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='123456', user_id='u1', chat_type='dm')
    entry = runner.session_store.get_or_create_session(source)
    native.db.append_message(entry.session_id, 'user', 'EXACT completed physical read')
    ended_read, release_tail = threading.Event(), threading.Event()
    threads = []
    original = threading.Thread
    class TailThread(original):
        def run(self):
            super().run()
            if self.name == 'media-history-lookup':
                threads.append(self)
                ended_read.set()
                assert release_tail.wait(5)
    monkeypatch.setattr(threading, 'Thread', TailThread)
    monkeypatch.setattr('gateway.platforms.base._HISTORY_MEDIA_LOOKUP_TIMEOUT_SECONDS', .03)
    try:
        with gateway_ingress(runner):
            await adapter._bounded_history_media_paths_for_session(entry.session_key)
        assert await asyncio.to_thread(ended_read.wait, 2)
        assert threads[0].is_alive()
        runner._ingress_barrier.close()
        status = gateway_admission_status(runner)
        assert status['known'] and status['active_work'] > 0 and not status['quiescent']
    finally:
        release_tail.set()
        for thread in threads:
            await asyncio.to_thread(thread.join, 2)
        deadline = time.monotonic() + 2
        while runner._active_deferred_agent_worker_count() and time.monotonic() < deadline:
            await asyncio.sleep(0)
    assert gateway_admission_status(runner)['quiescent']


@pytest.mark.parametrize('refusal', ['missing_task', 'closed_store', 'unreferenced'])
def test_failed_upload_never_unlinks_committed_other_task_reference(home, monkeypatch, refusal):
    kb.create_board('owned')
    path = None
    with kbc.connect_closing(board='owned') as conn:
        first = kb.create_task(conn, title='EXACT failed registration')
        second = kb.create_task(conn, title='EXACT retained shared input owner', assignee='owner')
        add = kb.add_attachment
        def fail_registration(connection, task_id, **kwargs):
            nonlocal path
            path = Path(kwargs['stored_path'])
            if refusal != 'unreferenced':
                with kbc.connect_closing(board='owned') as other:
                    add(other, second, **kwargs)
                    claimed = kb.claim_task(other, second)
                    _begin_launch(other, claimed, str(home), 'owned')
            if refusal == 'closed_store':
                connection.close()
            raise ValueError('inert upload registration refusal')
        monkeypatch.setattr(kb, 'add_attachment', fail_registration)
        with pytest.raises(ValueError, match='registration refusal'):
            kb.store_attachment_bytes(conn, first, 'exact-input.txt', b'EXACT retained upload bytes', board='owned')
    if refusal == 'unreferenced':
        assert not path.exists()
    else:
        assert path.read_bytes() == b'EXACT retained upload bytes'
        with kbc.connect_closing(board='owned') as reopened:
            assert kb.list_attachments(reopened, second)[0].stored_path == str(path)


def test_upload_collision_never_overwrites_or_cleans_foreign_blob(home, monkeypatch):
    kb.create_board('owned')
    foreign = home / 'exact-foreign-input.txt'
    foreign.write_bytes(b'EXACT foreign input must not be overwritten')
    monkeypatch.setattr(kb, '_collision_free_path', lambda *args: foreign)
    with kbc.connect_closing(board='owned') as conn:
        tid = kb.create_task(conn, title='EXACT concurrent filename collision')
        with pytest.raises(FileExistsError):
            kb.store_attachment_bytes(conn, tid, foreign.name, b'new request', board='owned')
        assert kb.list_attachments(conn, tid) == []
    assert foreign.read_bytes() == b'EXACT foreign input must not be overwritten'


@pytest.mark.parametrize('fingerprint', ['null', 'scalar'])
def test_valid_legacy_identity_retains_live_owner(home, child, fingerprint):
    from gateway.status import get_process_start_time
    value = None if fingerprint == 'null' else get_process_start_time(child.pid)
    if fingerprint == 'scalar' and value is None:
        pytest.skip('host cannot supply a valid legacy scalar witness')
    assert kbd._worker_alive(child.pid, value)


@pytest.mark.asyncio
async def test_dashboard_size_refusal_preserves_other_committed_shared_reference(home, monkeypatch):
    from fastapi import HTTPException
    from tests.plugins import test_kanban_attachments
    kb.create_board('owned')
    with kbc.connect_closing(board='owned') as conn:
        first = kb.create_task(conn, title='EXACT oversized upload')
        second = kb.create_task(conn, title='EXACT shared partial input', assignee='owner')
    router = test_kanban_attachments._load_plugin_router()
    endpoint = next(route.endpoint for route in router.routes if route.endpoint.__name__ == 'upload_task_attachment')
    monkeypatch.setitem(endpoint.__globals__, 'KANBAN_ATTACHMENT_MAX_BYTES', 5)
    blob = kb.task_attachments_dir(first, board='owned') / 'exact-partial.txt'
    class Upload:
        filename, content_type = blob.name, 'text/plain'
        reads = 0
        async def read(self, size):
            self.reads += 1
            if self.reads == 2:
                with kbc.connect_closing(board='owned') as conn:
                    kb.add_attachment(conn, second, filename=blob.name, stored_path=str(blob))
                    claimed = kb.claim_task(conn, second)
                    _begin_launch(conn, claimed, str(home), 'owned')
            return b'abc'
    with pytest.raises(HTTPException) as refusal:
        await endpoint(first, Upload(), board='owned', uploaded_by=None)
    assert refusal.value.status_code == 413
    assert blob.read_bytes() == b'abc'
    with kbc.connect_closing(board='owned') as reopened:
        assert kb.list_attachments(reopened, second)[0].stored_path == str(blob)


def test_review_rollback_cleanup_serializes_shared_reference_claim(home, monkeypatch):
    from hermes_cli import kanban_db_workspace as kbw
    kb.create_board('owned')
    with kbc.connect_closing(board='owned') as conn, kbc.connect_closing(board='owned') as other:
        first = kb.create_task(conn, title='EXACT failed review staging', assignee='owner')
        workspace = kbw.resolve_workspace(kb.get_task(conn, first), board='owned')
        kbw.set_workspace_path(conn, first, workspace)
        artifact = workspace / 'exact-staged.txt'
        artifact.write_bytes(b'EXACT original artifact')
        second = kb.create_task(conn, title='EXACT competing artifact owner', assignee='owner')
        claimed = kb.claim_task(conn, first)
        staged = kb.task_attachments_dir(first, board='owned') / artifact.name
        unlink = Path.unlink
        references = []
        def before_unlink(path, *args, **kwargs):
            if path == staged:
                other.execute('PRAGMA busy_timeout=0')
                try:
                    other.execute('BEGIN IMMEDIATE')
                except sqlite3.OperationalError as exc:
                    assert 'locked' in str(exc)
                else:
                    other.execute('ROLLBACK')
                    references.append(kb.add_attachment(other, second, filename=path.name, stored_path=str(path)))
                    owner = kb.claim_task(other, second)
                    _begin_launch(other, owner, str(home), 'owned')
            return unlink(path, *args, **kwargs)
        monkeypatch.setattr(Path, 'unlink', before_unlink)
        def fail(*args, **kwargs):
            raise RuntimeError('inert review bookkeeping failure')
        monkeypatch.setattr(kb, '_end_or_synthesize_run', fail)
        with pytest.raises(RuntimeError, match='bookkeeping failure'):
            kb.request_review(conn, first, reviewer='reviewer', summary='EXACT review payload',
                              metadata={'artifacts': [str(artifact)]}, expected_run_id=claimed.current_run_id)
        assert artifact.read_bytes() == b'EXACT original artifact'
        if references:
            assert kb.get_attachment(other, references[0]) is not None
            assert staged.read_bytes() == b'EXACT original artifact'
        else:
            assert not staged.exists()


@pytest.mark.parametrize('operation', ['workspace', 'parent', 'gc'])
def test_workspace_cleanup_preserves_unrelated_retained_attachment_reference(home, operation):
    from hermes_cli import kanban_db_workspace as kbw, kanban_ops
    kb.create_board('owned')
    kb.set_current_board('owned')
    with kbc.connect_closing(board='owned') as conn:
        first = kb.create_task(conn, title='EXACT scratch owner')
        workspace = kbw.resolve_workspace(kb.get_task(conn, first), board='owned')
        kbw.set_workspace_path(conn, first, workspace)
        blob = workspace / 'exact-shared-handoff.txt'
        blob.write_bytes(b'EXACT retained unrelated handoff')
        child_task = kb.create_task(conn, title='completed child', parents=[first])
        second = kb.create_task(conn, title='EXACT unrelated shared-input launch', assignee='owner')
        ref = kb.add_attachment(conn, second, filename=blob.name, stored_path=str(blob))
        claimed = kb.claim_task(conn, second)
        _begin_launch(conn, claimed, str(home), 'owned')
        conn.execute("UPDATE tasks SET status='archived' WHERE id IN (?,?)", (first, child_task))
        conn.commit()
        if operation == 'workspace':
            kbw._cleanup_workspace(conn, first)
        elif operation == 'parent':
            kbw._try_cleanup_parent_workspaces(conn, child_task)
        else:
            assert kanban_ops._cmd_gc(SimpleNamespace(event_retention_days=0, log_retention_days=0)) == 0
        assert kb.get_attachment(conn, ref) is not None
        assert blob.read_bytes() == b'EXACT retained unrelated handoff'
