"""Existing task-event ownership spans physical launch and durable PID handoff."""
from dataclasses import asdict
import json
import logging
import re
from pathlib import Path
import uuid

logger = logging.getLogger(__name__)


class WorkerNotStarted(RuntimeError):
    """Native launcher proves Popen never created a child."""


_PENDING_WORKER_LAUNCH_SQL = """SELECT e.* FROM task_events e WHERE e.kind='worker_spawn_pending'
        AND NOT EXISTS (SELECT 1 FROM task_events f WHERE f.task_id=e.task_id
          AND f.run_id IS e.run_id AND f.id>e.id
          AND f.kind IN ('spawned', 'spawn_failed')
          AND json_extract(f.payload, '$.launch_id')=json_extract(e.payload, '$.launch_id'))"""

_PROTECTED_WORKER_TASKS_SQL = """WITH RECURSIVE protected(id) AS (
    SELECT task_id FROM (""" + _PENDING_WORKER_LAUNCH_SQL + """)
    UNION SELECT l.parent_id FROM task_links l JOIN protected p ON l.child_id=p.id
) SELECT id FROM protected"""


def unresolved_worker_task_ids(conn):
    """Retain an unresolved launch and ancestor handoff inputs until checked settlement."""
    return {row[0] for row in conn.execute(_PROTECTED_WORKER_TASKS_SQL)}


def physical_worker_task_ids(conn):
    """Claims and live/unverifiable task/run PIDs remain owners after result publication."""
    from hermes_cli import kanban_db as kb, kanban_db_dispatch as dispatch
    owners = {row[0] for row in conn.execute(
        "SELECT id FROM tasks WHERE status='running' OR NULLIF(claim_lock, '') IS NOT NULL")}
    local = kb._host_prefix()
    for task_id, pid, owner in conn.execute(
            'SELECT id, worker_pid, claim_lock FROM tasks WHERE worker_pid IS NOT NULL UNION '
            'SELECT task_id, worker_pid, claim_lock FROM task_runs WHERE worker_pid IS NOT NULL'):
        if local == 'unknown:' or not str(owner or '').startswith(local) or dispatch._pid_alive(pid):
            owners.add(task_id)
    return owners


def retained_task_ids(conn):
    """Keep original ancestors until physical and notification ownership both settle."""
    from hermes_cli.kanban_db_notify import notification_attention_task_ids
    protected = unresolved_worker_task_ids(conn) | physical_worker_task_ids(conn) | notification_attention_task_ids(conn)
    links = list(conn.execute('SELECT parent_id, child_id FROM task_links'))
    while True:
        parents = {parent for parent, child in links if child in protected} - protected
        if not parents:
            return protected
        protected.update(parents)


def require_settled_worker_launch(conn, task_id):
    if task_id in unresolved_worker_task_ids(conn):
        raise RuntimeError('physical worker launch ownership UNKNOWN; retain task and evidence')


def pending_worker_launch(conn, task_id=None):
    """Unresolved launch, including malformed payload, never grants reclaim/readiness."""
    sql = _PENDING_WORKER_LAUNCH_SQL
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


def gc_settled_events(conn, cutoff):
    """Keep complete history until launch uncertainty and physical ownership both settle."""
    from hermes_cli import kanban_db as kb
    with kb.write_txn(conn):
        protected = retained_task_ids(conn)
        candidates = conn.execute("SELECT DISTINCT task_id FROM task_events WHERE created_at < ? "
                                 "AND task_id IN (SELECT id FROM tasks WHERE status IN ('done', 'archived'))", (cutoff,))
        rows = [(cutoff, row[0]) for row in candidates if row[0] not in protected]
        cur = conn.executemany("DELETE FROM task_events WHERE created_at < ? AND task_id=? AND kind != 'decomposed'", rows)
    return int(cur.rowcount or 0)


def gc_settled_worker_logs(log_dir, cutoff, board):
    """Serialize retention against launch intent commit; uncertain logs stay actionable."""
    import contextlib
    import sqlite3
    from hermes_cli import kanban_db as kb
    from hermes_cli.sqlite_safe_read import connect_tracked
    removed = 0
    # Logs follow the canonical board, independently of an injected worker DB override.
    database = kb._board_path(None, board, ('kanban.db',), 'kanban.db').resolve()
    if kb.kanban_db_path(board).resolve() != database:
        raise RuntimeError('worker log ownership ambiguous: database override differs from log board')
    with contextlib.closing(connect_tracked(database.as_uri() + '?mode=rw', uri=True,
                                          isolation_level=None)) as conn, kb.write_txn(conn):
        conn.row_factory = sqlite3.Row
        protected = retained_task_ids(conn)
        for path in log_dir.iterdir():
            owner = re.fullmatch(r'(.+)\.log(?:\.\d+)?', path.name)
            if owner is not None and owner.group(1) in protected:
                continue
            with contextlib.suppress(OSError):
                if path.is_file() and path.stat().st_mtime < cutoff:
                    path.unlink()
                    removed += 1
    return removed


def rotate_settled_worker_log(conn, task_id, board):
    """Rotate before this claim exists, serialized against every retained owner."""
    from hermes_cli import kanban_db as kb, kanban_db_dispatch as dispatch
    database = kb._board_path(None, board, ('kanban.db',), 'kanban.db').resolve()
    actual = Path(conn.execute('PRAGMA database_list').fetchone()[2]).resolve()
    if actual != database or kb.kanban_db_path(board).resolve() != database:
        return  # An injected worker DB cannot authorize canonical log deletion.
    with kb.write_txn(conn):
        if task_id not in retained_task_ids(conn):
            rotate_bytes, backups = dispatch.worker_log_rotation_config()
            dispatch._rotate_worker_log(kb.worker_log_path(task_id, board=board), rotate_bytes, backups)


def worker_ownership_count(conn):
    """Shared strict inventory; unreadable/unknown launch ownership raises."""
    require_writable_board(conn)
    from hermes_cli.kanban_db_notify import notification_attention_task_ids
    if notification_attention_task_ids(conn):
        raise RuntimeError('kanban notification outcome UNKNOWN; preserve exact task/attempt/owner attention')
    if pending_worker_launch(conn) is not None:
        raise RuntimeError('physical worker launch ownership UNKNOWN; inspect task events')
    return len(physical_worker_task_ids(conn))


def require_writable_board(conn):
    """A removed WAL file can remain writable through an old connection; intent closes it first."""
    from hermes_cli.kanban_db_removal import require_open_board
    path = conn.execute('PRAGMA database_list').fetchone()[2]
    if path:
        require_open_board(path)
    if conn.execute("SELECT 1 FROM task_events WHERE kind='board_removal_pending' LIMIT 1").fetchone():
        raise RuntimeError('board removal ownership UNKNOWN; preserve remaining store and inspect removal intent')


def require_settled_task_ownership(conn, task_id):
    """Task evidence must outlive physical ownership and every uncertain notification."""
    require_settled_worker_launch(conn, task_id)
    if task_id in retained_task_ids(conn):
        raise RuntimeError('task ownership active or UNKNOWN; preserve task/run/notification evidence')
