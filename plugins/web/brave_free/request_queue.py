"""Host-wide pacing for calls made with the same Brave API key.

The SQLite write lock covers the wait and HTTP call, so separate gateway,
cron, and CLI processes cannot release a burst of reserved slots at once.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import sqlite3
import time
from typing import Callable, TypeVar

from agent.retry_utils import parse_retry_after_seconds

T = TypeVar("T")


class BraveQueueBusy(RuntimeError):
    """The caller should use another search path instead of adding to a long queue."""


def _queue_path(api_key: str) -> Path:
    digest = hashlib.sha256(api_key.encode("utf-8")).hexdigest()[:24]
    return Path.home() / ".hermes" / "rate_limits" / f"brave-{digest}.sqlite3"


def _retry_delay(header: str | None, default: float) -> float:
    delay = parse_retry_after_seconds(header)
    return max(default, delay) if delay is not None else default


def queued_request(
    api_key: str,
    request: Callable[[], T],
    *,
    interval: float = 2.0,
    max_wait: float = 30.0,
    cooldown: float = 60.0,
) -> T:
    """Run one request after earlier callers finish; honor a 429 cooldown.

    A busy queue fails within ``max_wait`` so an agent can use its browser
    fallback. The database contains only a timestamp; its filename contains a
    digest of the key, never the key itself.
    """
    path = _queue_path(api_key)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        pass
    else:
        os.close(fd)

    deadline = time.monotonic() + max(0.0, max_wait)
    try:
        connection = sqlite3.connect(path, timeout=max(0.1, max_wait))
        connection.execute("CREATE TABLE IF NOT EXISTS queue (id INTEGER PRIMARY KEY, next_at REAL NOT NULL)")
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute("SELECT next_at FROM queue WHERE id=1").fetchone()
        due = row[0] if row else 0.0
        delay = max(0.0, due - time.time())
        if time.monotonic() + delay > deadline:
            raise BraveQueueBusy("Brave Search queue is busy; use the web-usage Chrome browser")
        if delay:
            time.sleep(delay)
        next_delay = max(0.0, interval)
        try:
            response = request()
            if getattr(response, "status_code", None) == 429:
                next_delay = _retry_delay(response.headers.get("Retry-After"), max(interval, cooldown))
            return response
        finally:
            connection.execute(
                "INSERT INTO queue(id,next_at) VALUES(1,?) "
                "ON CONFLICT(id) DO UPDATE SET next_at=excluded.next_at",
                (time.time() + next_delay,),
            )
            connection.commit()
    except sqlite3.OperationalError as exc:
        raise BraveQueueBusy("Brave Search queue is busy; use the web-usage Chrome browser") from exc
    finally:
        if "connection" in locals():
            connection.close()
