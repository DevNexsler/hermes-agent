"""Read-only work inventory for the machine-shared kanban boards.

An embedded tick owns admission through spawn and durable PID registration. Existing
claims and physical workers then remain visible independently of that tick's lease.
Independent dispatcher processes still require coordinator exclusion/lock ownership.
"""

import sqlite3


def _retained_board_paths(kb):
    """Inventory supported live and archived board locations; unreadability raises."""
    paths = {kb.kanban_db_path(board['slug']).resolve()
             for board in kb.list_boards(include_archived=True)}
    archive_root = kb.boards_root() / '_archived'
    try:
        archive_root.stat()
    except FileNotFoundError:
        return paths
    for directory in archive_root.iterdir():
        # remove_board archives whole directories, including live worker records.
        # stat/iterdir raise on unreadability rather than silently omitting stores.
        if directory.is_dir():
            for entry in directory.iterdir():
                if entry.name == 'kanban.db':
                    paths.add(entry.resolve())
    return paths


def kanban_worker_work_count():
    """Unknown/unreadable stores raise; no migration, reconciliation or signals."""
    from hermes_cli import kanban_db as kb

    root = kb.boards_root()
    try:
        root.stat()
    except FileNotFoundError:
        pass
    else:
        # list_boards' is_dir probe alone can conceal an unreadable directory.
        list(root.iterdir())
    paths = _retained_board_paths(kb)
    total = 0
    for path in paths:
        try:
            path.stat()
        except FileNotFoundError:
            continue
        conn = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=1.0)
        try:
            conn.execute('BEGIN')
            from hermes_cli.kanban_db_spawn import worker_ownership_count
            total += worker_ownership_count(conn)
        finally:
            conn.close()
    return total
