"""Board removal ownership survives loss of the database and its directory."""
from contextlib import ExitStack, closing, contextmanager
import json
import os
from pathlib import Path
import sqlite3
import uuid

from utils import atomic_json_write, fsync_directory


def _removal_root():
    from hermes_cli import kanban_db as kb
    return kb.boards_root().parent / 'board-removals'


def require_no_board_removals():
    """Strict machine-shared inventory includes retained removal evidence, even without a DB."""
    root = _removal_root()
    try:
        root.stat()
    except FileNotFoundError:
        return
    if any(path.suffix == '.json' for path in root.iterdir()):
        raise RuntimeError('board removal ownership UNKNOWN; inspect retained board-removals evidence')


def require_open_board(path):
    """Apply to actual database path, independently of the caller's board/DB override."""
    from hermes_cli import kanban_db as kb
    path = Path(path).resolve()
    if path.name != 'kanban.db' or path.parent.parent != kb.boards_root().resolve():
        return
    marker = _removal_root() / (path.parent.name + '.json')
    try:
        marker.lstat()
    except FileNotFoundError:
        return
    raise RuntimeError('board removal ownership UNKNOWN; preserve remaining files and inspect removal intent')


@contextmanager
def _owned_removal(record):
    """Stable lock inode + full-record CAS; foreign/unreadable evidence is never replaced."""
    from hermes_cli.kanban_db_connect import _try_lock_nb, _unlock
    root = _removal_root()
    root.mkdir(parents=True, exist_ok=True)
    marker = root / (record['board'] + '.json')
    fd = os.open(marker.with_suffix('.lock'), os.O_RDWR | os.O_CREAT, 0o600)
    with os.fdopen(fd, 'a+b') as handle:
        if not _try_lock_nb(handle):
            raise RuntimeError('board removal owner lock unavailable')
        try:
            require_open_board(record['database_realpath'])
            atomic_json_write(marker, record, mode=0o600, fsync_dir=True)
            owned_bytes = marker.read_bytes()
            if json.dumps(json.loads(owned_bytes), sort_keys=True, allow_nan=False) != json.dumps(record, sort_keys=True):
                raise RuntimeError('board removal owner readback unconfirmed')
            yield
            if Path(record['database_realpath']).parent.exists() or marker.read_bytes() != owned_bytes:
                raise RuntimeError('board removal unsettled or ownership changed; retained UNKNOWN')
            marker.unlink()
            fsync_directory(root)
        finally:
            _unlock(handle)


@contextmanager
def board_removal_guard(board):
    """Commit closure before unlink; failed removal never automatically reopens on restart."""
    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_db_spawn import _append_checked_event, worker_ownership_count
    from hermes_cli.sqlite_safe_read import connect_tracked
    path = (kb.board_dir(board) / 'kanban.db').resolve()
    if kb.board_dir(board).is_symlink() or (kb.board_dir(board) / 'kanban.db').is_symlink():
        raise RuntimeError('board removal requires an unaliased directory/database ownership path')
    require_open_board(path)
    with ExitStack() as resources:
        conn = resources.enter_context(closing(connect_tracked(
            path.as_uri() + '?mode=rw', uri=True, timeout=0, isolation_level=None)))
        resources.callback(conn.rollback)
        conn.row_factory = sqlite3.Row
        conn.execute('BEGIN IMMEDIATE')
        if worker_ownership_count(conn):
            raise RuntimeError('board still owns active or unverifiable physical work')
        record = {'version': 1, 'operation_id': uuid.uuid4().hex, 'board': board,
                  'database_realpath': str(path), 'owner': 'native-board-removal', 'outcome': 'UNKNOWN',
                  'required_action': 'Inspect exact removal and remaining files; no automatic reopen.'}
        with _owned_removal(record):
            _append_checked_event(conn, '', 'board_removal_pending', record, None)
            conn.commit()
            saved = conn.execute("SELECT payload FROM task_events WHERE kind='board_removal_pending'").fetchall()
            if len(saved) != 1 or json.loads(saved[0][0]) != record:
                raise RuntimeError('board removal intent readback unconfirmed')
            # Closure is now durable outside the deletion set and inside old WAL handles.
            # Release our descriptor before unlink (Windows forbids deleting open databases).
            resources.close()
            yield
