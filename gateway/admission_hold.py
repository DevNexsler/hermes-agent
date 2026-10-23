"""Persistent, token-owned maintenance hold, independent of expiring NAS drain requests.

Only an explicit matching-token release removes a hold. Age, epoch and owner death never
release it; an interrupted deployment remains closed until its owner restores matched assets.
"""

from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets

from utils import atomic_json_write, fsync_directory

HOLD_FILENAME = '.gateway_admission_hold.json'


class AdmissionHoldConflict(RuntimeError):
    """Hold exists, token differs, or control serialization is unavailable. Nothing changed."""


def _home(home):
    from hermes_constants import get_routing_process_hermes_home
    return Path(home if home is not None else get_routing_process_hermes_home()).resolve()


def _read(home):
    try:
        body = json.loads((home / HOLD_FILENAME).read_text(encoding='utf-8'))
    except FileNotFoundError:
        return None
    if (not isinstance(body, dict) or body.get('version') != 1
            or not all(isinstance(body.get(key), str) and body[key]
                       for key in ('hold_id', 'owner', 'token_digest'))):
        raise ValueError('invalid gateway admission hold')
    return body


def inspect_admission_hold(*, home=None):
    """Public readback contains no token/digest. Unreadable/malformed means held and unknown."""
    try:
        body = _read(_home(home))
    except (OSError, ValueError):
        return {'known': False, 'active': True, 'hold_id': None, 'owner': None}
    return {'known': True, 'active': body is not None,
            'hold_id': body['hold_id'] if body else None, 'owner': body['owner'] if body else None}


@contextmanager
def _serialized(home):
    from gateway.status import _release_file_lock, _try_acquire_file_lock
    # Never unlink this inode: all contenders must lock the same object across replace/unlink.
    fd = os.open(home / '.gateway_admission_hold.lock', os.O_RDWR | os.O_CREAT, 0o600)
    with os.fdopen(fd, 'r+', encoding='utf-8') as handle:
        if not _try_acquire_file_lock(handle):
            raise AdmissionHoldConflict('gateway admission hold control busy or unavailable')
        try:
            yield
        finally:
            _release_file_lock(handle)


def acquire_admission_hold(*, owner, home=None, token=None):
    """Acquire exclusively; retry with the SAME persisted token recovers a lost response.

    Caller saves a token generated with secrets.token_urlsafe(32) BEFORE acquisition so a
    crash cannot lose release authority. Omitted token is convenient for in-process owners.
    Never log the returned token. Existing holds, including corrupt ones, are never replaced.
    """
    if not isinstance(owner, str) or not owner.strip():
        raise ValueError('owner required')
    token = token if token is not None else secrets.token_urlsafe(32)
    if not isinstance(token, str) or len(token) < 32:
        raise ValueError('persist a random token of at least 32 characters')
    home = _home(home)
    digest = hashlib.sha256(token.encode('utf-8')).hexdigest()
    with _serialized(home):
        try:
            existing = _read(home)
        except (OSError, ValueError) as exc:
            raise AdmissionHoldConflict('existing hold cannot be verified') from exc
        if existing is not None:
            if existing['owner'] != owner or not hmac.compare_digest(existing['token_digest'], digest):
                raise AdmissionHoldConflict('gateway admission hold belongs to another acquisition')
            return {'hold_id': existing['hold_id'], 'token': token}
        body = {'version': 1, 'hold_id': secrets.token_hex(16), 'owner': owner,
                'token_digest': digest, 'created_at': datetime.now(timezone.utc).isoformat()}
        atomic_json_write(home / HOLD_FILENAME, body, mode=0o600, fsync_dir=True)
        return {'hold_id': body['hold_id'], 'token': token}


def release_admission_hold(*, token, home=None):
    """Compare token while serialized. False when already absent; foreign/unknown holds survive."""
    if not isinstance(token, str) or not token:
        raise AdmissionHoldConflict('release token required')
    home = _home(home)
    digest = hashlib.sha256(token.encode('utf-8')).hexdigest()
    with _serialized(home):
        try:
            body = _read(home)
        except (OSError, ValueError) as exc:
            raise AdmissionHoldConflict('existing hold cannot be verified') from exc
        if body is None:
            return False
        if not hmac.compare_digest(body['token_digest'], digest):
            raise AdmissionHoldConflict('gateway admission hold token differs')
        (home / HOLD_FILENAME).unlink()
        fsync_directory(home)
        return True
