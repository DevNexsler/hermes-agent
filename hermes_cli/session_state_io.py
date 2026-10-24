"""Confirmed I/O for existing automatic goal, loop and heartbeat metadata."""
import json
import logging

logger = logging.getLogger(__name__)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate session state field')
        result[key] = value
    return result


def parse_confirmed_json(raw):
    return json.loads(raw, object_pairs_hook=_unique_object)


def values_preserved(original, decoded):
    if isinstance(original, dict):
        return (isinstance(decoded, dict) and not (original.keys() - decoded.keys())
                and all(values_preserved(v, decoded[k]) for k, v in original.items()))
    if isinstance(original, list):
        return (isinstance(decoded, list) and len(original) == len(decoded)
                and all(values_preserved(a, b) for a, b in zip(original, decoded)))
    if type(original) in (int, float) and type(decoded) in (int, float):
        return original == decoded
    return type(original) is type(decoded) and original == decoded


def _uncertain(key):
    from gateway.admission import mark_native_work_uncertain
    mark_native_work_uncertain()
    logger.error('Automatic session state unconfirmed for %s; inspect retained checkpoint/history before release',
                 key, exc_info=True)


def load_confirmed_state(db, key, decode):
    try:
        if db is None:
            raise RuntimeError('automatic session state store unavailable')
        with db._lock:
            row = db._conn.execute('SELECT value FROM state_meta WHERE key=?', (key,)).fetchone()
        return (None, None) if row is None else (decode(row[0]), row[0])
    except BaseException:
        _uncertain(key)
        raise


def save_confirmed_state(db, key, expected, raw):
    try:
        def commit(conn):
            row = conn.execute('SELECT value FROM state_meta WHERE key=?', (key,)).fetchone()
            if row is None or row[0] != expected:
                raise RuntimeError('automatic session state changed; existing owner preserved')
            db.set_meta(key, raw, cursor=conn)
            if conn.execute('SELECT value FROM state_meta WHERE key=?', (key,)).fetchone()[0] != raw:
                raise RuntimeError('automatic session state readback unconfirmed')
        db._execute_write(commit)
    except BaseException:
        _uncertain(key)
        raise


def migrate_confirmed_state(prefix, old, new, decode, *, db=None, strict=False):
    """Copy exact original bytes and clear parent in one checked writer transaction."""
    if not old or not new or old == new:
        return False
    try:
        if db is None:
            from hermes_cli.goals import _get_session_db
            db = _get_session_db()
        if db is None:
            raise RuntimeError('session transfer store unavailable')
        def commit(conn):
            parent = conn.execute('SELECT value FROM state_meta WHERE key=?', (prefix + old,)).fetchone()
            if parent is None:
                return False
            state = decode(parent[0])
            if state.status == 'cleared':
                return False
            child = conn.execute('SELECT value FROM state_meta WHERE key=?', (prefix + new,)).fetchone()
            if child is not None:
                if strict:
                    raise RuntimeError('session transfer child already owned')
                return False
            cleared = parse_confirmed_json(parent[0])
            cleared['status'] = 'cleared'
            values = ((prefix + new, parent[0]), (prefix + old, json.dumps(cleared)))
            for key, raw in values:
                db.set_meta(key, raw, cursor=conn)
            for key, raw in values:
                row = conn.execute('SELECT value FROM state_meta WHERE key=?', (key,)).fetchone()
                if row is None or row[0] != raw:
                    raise RuntimeError('session transfer readback unconfirmed')
            return True
        result = db._execute_write(commit)
        if type(result) is not bool:
            raise RuntimeError('session transfer completion unconfirmed')
        return result
    except BaseException as exc:
        if strict:
            _uncertain(prefix + old + ' -> ' + new)
            raise
        if not isinstance(exc, Exception):
            raise
        logger.debug('Session transfer unconfirmed: %s%s -> %s', prefix, old, new, exc_info=True)
        return False
