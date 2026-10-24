"""Existing task-event ownership spans physical launch and durable PID handoff."""
from dataclasses import asdict
import json
import logging
from pathlib import Path
import uuid

logger = logging.getLogger(__name__)


class WorkerNotStarted(RuntimeError):
    """Native launcher proves Popen never created a child."""


def pending_worker_launch(conn, task_id=None):
    """Unresolved launch, including malformed payload, never grants reclaim/readiness."""
    sql = """SELECT e.* FROM task_events e WHERE e.kind='worker_spawn_pending'
        AND NOT EXISTS (SELECT 1 FROM task_events f WHERE f.task_id=e.task_id
          AND f.run_id IS e.run_id AND f.id>e.id
          AND f.kind IN ('spawned', 'spawn_failed')
          AND json_extract(f.payload, '$.launch_id')=json_extract(e.payload, '$.launch_id'))"""
    if task_id is not None:
        sql += ' AND e.task_id=?'
    return conn.execute(sql + ' LIMIT 1', () if task_id is None else (task_id,)).fetchone()


def _append_checked_event(conn, task_id, kind, payload, run_id):
    from hermes_cli import kanban_db as kb
    kb._append_event(conn, task_id, kind, payload, run_id=run_id)
    saved = conn.execute('SELECT * FROM task_events WHERE id=last_insert_rowid()').fetchone()
    if (saved is None or saved['task_id'] != task_id or saved['run_id'] != run_id
            or saved['kind'] != kind or json.loads(saved['payload']) != payload):
        raise RuntimeError('worker ownership event readback unconfirmed')


def _begin_launch(conn, claimed, workspace, board):
    from hermes_cli import kanban_db as kb
    launch_id = uuid.uuid4().hex
    record = {'version': 1, 'launch_id': launch_id, 'outcome': 'UNKNOWN',
              'owner': f'profile:{claimed.assignee}/kanban-dispatcher',
              'task': asdict(claimed), 'run_id': claimed.current_run_id,
              'workspace': workspace, 'board': board,
              'database_realpath': str(Path(conn.execute('PRAGMA database_list').fetchone()[2]).resolve()),
              'history_event_ids': [r[0] for r in conn.execute(
                  'SELECT id FROM task_events WHERE task_id=? ORDER BY id', (claimed.id,))],
              'required_action': 'Assign accountable operator; inspect exact launch/task/run and physical worker. No automatic retry/refund/completion.'}
    with kb.write_txn(conn):
        row = conn.execute('SELECT status, current_run_id, claim_lock FROM tasks WHERE id=?', (claimed.id,)).fetchone()
        if row is None or tuple(row) != ('running', claimed.current_run_id, claimed.claim_lock):
            raise RuntimeError('worker claim changed before physical launch')
        _append_checked_event(conn, claimed.id, 'worker_spawn_pending', record, claimed.current_run_id)
    return launch_id


def spawn_claimed_worker(conn, claimed, workspace, board, spawn_fn, failure_limit, result):
    """Unknown callback or PID handoff failures retain original claim and launch obligation."""
    from hermes_cli import kanban_db_dispatch as dispatch
    from hermes_cli import kanban_db as kb
    from tools.process_registry import RestartSafeScopeUnavailable
    launch_id = _begin_launch(conn, claimed, workspace, board)
    try:
        pid = dispatch._call_spawn_fn(spawn_fn or dispatch._default_spawn, claimed, workspace, board)
    except Exception as exc:
        if spawn_fn is None and isinstance(exc, (WorkerNotStarted, RestartSafeScopeUnavailable)):
            # Only native pre-Popen failures have this provenance. Custom callbacks may
            # have performed effects before raising any exception, including these types.
            with kb.write_txn(conn):
                _append_checked_event(conn, claimed.id, 'spawn_failed',
                    {'launch_id': launch_id, 'error': str(exc), 'pre_effect': True}, claimed.current_run_id)
            if dispatch._record_task_failure(conn, claimed.id, str(exc), outcome='spawn_failed',
                    failure_limit=failure_limit, release_claim=True, end_run=True,
                    infrastructure=isinstance(exc, RestartSafeScopeUnavailable)):
                result.auto_blocked.append(claimed.id)
            return False
        raise RuntimeError(f'worker ownership UNKNOWN for {claimed.id}; inspect launch {launch_id}') from exc
    try:
        if not pid:
            raise RuntimeError('spawn returned no verifiable PID')
        dispatch._set_worker_pid(conn, claimed.id, int(pid), expected_run_id=claimed.current_run_id,
                                 launch_id=launch_id)
        kb._fire_worker_spawned_hook(conn, claimed, workspace, pid, board=board)
    except Exception as exc:
        try:
            with kb.write_txn(conn):
                _append_checked_event(conn, claimed.id, 'worker_spawn_unknown',
                    {'version': 1, 'launch_id': launch_id, 'pid': pid,
                     'owner': f'profile:{claimed.assignee}/kanban-dispatcher',
                     'error': str(exc), 'required_action': 'Inspect exact launch and physical worker; no automatic retry/refund.'},
                    claimed.current_run_id)
        except Exception:
            logger.exception('Worker PID evidence unconfirmed; durable pending launch %s still blocks reclaim/readiness', launch_id)
        raise RuntimeError(f'worker ownership UNKNOWN for {claimed.id}; pid={pid}; inspect launch {launch_id}') from exc
    return True


def persist_worker_pid(conn, task_id, pid, *, expected_run_id=None, launch_id=None):
    """Record and verify physical ownership on exact run, including a fast-completed worker."""
    from hermes_cli import kanban_db_dispatch as dispatch
    from hermes_cli import kanban_db as kb
    started_at = dispatch._process_fingerprint(pid) or dispatch.UNVERIFIED_WORKER_FINGERPRINT
    with kb.write_txn(conn):
        current = kb._current_run_id(conn, task_id)
        run_id = current if expected_run_id is None else expected_run_id
        if current not in (None, run_id):
            raise RuntimeError('worker run ownership changed')
        if current == run_id:
            conn.execute('UPDATE tasks SET worker_pid=?, worker_started_at=? WHERE id=?',
                         (pid, started_at, task_id))
            row = conn.execute('SELECT worker_pid, worker_started_at FROM tasks WHERE id=?', (task_id,)).fetchone()
            if row is None or tuple(row) != (pid, started_at):
                raise RuntimeError('worker task PID readback unconfirmed')
        if run_id is not None:
            conn.execute('UPDATE task_runs SET worker_pid=?, worker_started_at=? WHERE id=? AND task_id=?',
                         (pid, started_at, run_id, task_id))
            row = conn.execute('SELECT worker_pid, worker_started_at FROM task_runs WHERE id=? AND task_id=?',
                               (run_id, task_id)).fetchone()
            if row is None or tuple(row) != (pid, started_at):
                raise RuntimeError('worker run PID readback unconfirmed')
        payload = {'pid': pid, 'started_at': started_at}
        if launch_id is not None:
            payload['launch_id'] = launch_id
        _append_checked_event(conn, task_id, 'spawned', payload, run_id)
