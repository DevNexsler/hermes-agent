"""Async/sync bridging helpers.

``asyncio.run_coroutine_threadsafe`` can raise ``RuntimeError`` (loop closed during a
shutdown race); the coroutine is then never awaited or closed, which triggers a
"coroutine was never awaited" RuntimeWarning and leaks its frame. The helpers here
close the coroutine on scheduling failure. ``future.result()`` failures are deliberately
NOT handled: once the loop accepts the coroutine its lifecycle belongs to the loop.
"""
from __future__ import annotations

import asyncio
import logging
from concurrent.futures import Future
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Coroutine, Optional


_DEFAULT_LOGGER = logging.getLogger(__name__)
_threadsafe_future_owner = ContextVar('threadsafe_future_owner', default=None)


@contextmanager
def threadsafe_future_owner(track):
    """Bind an existing caller's lifetime registry without coupling the bridge to a host."""
    token = _threadsafe_future_owner.set(track)
    try:
        yield
    finally:
        _threadsafe_future_owner.reset(token)


def safe_schedule_threadsafe(
    coro: Coroutine[Any, Any, Any], loop: Optional[asyncio.AbstractEventLoop], *,
    logger: Optional[logging.Logger] = None,
    log_message: str = "Failed to schedule coroutine on loop", log_level: int = logging.DEBUG,
) -> Optional[Future]:
    """Schedule ``coro`` on ``loop`` from a sync context, leak-safe.

    Returns the Future on success, or ``None`` if the loop is missing or scheduling
    raised; in every failure path the coroutine is closed. Callers keep full control
    over the returned future (``.result(timeout=...)``, callbacks, fire-and-forget).
    """
    log = logger if logger is not None else _DEFAULT_LOGGER
    try:
        if loop is None:
            raise RuntimeError("loop is None")
        future = asyncio.run_coroutine_threadsafe(coro, loop)
    except Exception as exc:
        if asyncio.iscoroutine(coro):
            coro.close()
        log.log(log_level, "%s: %s", log_message, exc)
        return None
    # Once submitted, a tracking failure is ambiguous: never close/cancel an accepted coroutine.
    track = _threadsafe_future_owner.get()
    if track is not None:
        track(future)
    return future


def consume_detached_task_result(task: "asyncio.Future[Any]") -> None:
    """``add_done_callback`` for cancelled-and-detached tasks: observe the exception so the
    loop does not log "exception was never retrieved"; cancellation and terminal errors
    are swallowed because the task's owner already gave up on it."""
    try:
        task.exception()
    except (asyncio.CancelledError, Exception):
        pass
