"""Coalesced runtime-status persistence and strict pending/writing ownership."""

import copy
from pathlib import Path
import threading
import time
from typing import Any, Callable, Optional


class RuntimeStatusWriter:
    """Persist the latest complete status snapshot on one daemon thread.

    Runtime status is diagnostic state, not an event log. While one write is
    blocked in filesystem I/O, newer submissions replace the single pending
    snapshot. This bounds memory and keeps every status producer off asyncio.
    """

    def __init__(self, write_fn: Optional[Callable[[Path, dict[str, Any]], None]] = None):
        self._write_fn = write_fn
        self._condition = threading.Condition()
        self._pending: Optional[tuple[int, Path, dict[str, Any]]] = None
        self._writing_generation = 0
        self._submitted_generation = 0
        self._completed_generation = 0
        self._successful_generation = 0
        self._last_error: Optional[BaseException] = None
        self._failure_logged = False
        self._thread: Optional[threading.Thread] = None

    def submit(self, path: Path, payload: dict[str, Any]) -> int:
        with self._condition:
            self._submitted_generation += 1
            generation = self._submitted_generation
            self._pending = (generation, path, copy.deepcopy(payload))
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(
                    target=self._run, daemon=True, name="gateway-runtime-status-writer")
                self._thread.start()
            self._condition.notify_all()
            return generation

    def wait(self, generation: int, timeout: Optional[float] = None) -> bool:
        """Block until ``generation`` (or a later snapshot) is persisted; ``False`` on timeout/failure."""
        deadline = None if timeout is None else time.monotonic() + max(timeout, 0.0)
        with self._condition:
            while (state := self.settled(generation)) is None:
                if deadline is None:
                    self._condition.wait()
                    continue
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._condition.wait(timeout=remaining)
            if not state and timeout is None and self._last_error is not None:
                raise self._last_error
            return state

    def flush(self, timeout: float = 2.0) -> bool:
        with self._condition:
            generation = self._submitted_generation
        return generation == 0 or self.wait(generation, timeout=timeout)

    def settled(self, generation: int) -> Optional[bool]:
        """``True`` once ``generation`` persisted, ``False`` once it can no longer, else ``None``."""
        with self._condition:
            if self._successful_generation >= generation:
                return True
            no_more_work = (
                self._completed_generation >= generation
                and self._writing_generation == 0
                and self._pending is None
            )
            return False if no_more_work else None

    def work_count(self) -> int:
        """Count pending/writing snapshots; failed or unreadable ownership is never idle."""
        with self._condition:
            values = (self._submitted_generation, self._completed_generation,
                      self._writing_generation, self._successful_generation)
            if any(type(value) is not int or value < 0 for value in values):
                raise RuntimeError('invalid runtime-status generation')
            if self._completed_generation > self._submitted_generation or self._last_error is not None:
                raise RuntimeError('runtime-status persistence unconfirmed')
            if (self._successful_generation > self._completed_generation
                    or self._writing_generation > self._submitted_generation):
                raise RuntimeError('inconsistent runtime-status generations')
            pending = self._pending is not None
            writing = self._writing_generation != 0
            if pending and (type(self._pending) is not tuple or len(self._pending) != 3
                            or type(self._pending[0]) is not int
                            or not self._completed_generation < self._pending[0] <= self._submitted_generation):
                raise RuntimeError('invalid pending runtime-status ownership')
            if self._submitted_generation > self._completed_generation and not (pending or writing):
                raise RuntimeError('runtime-status work lost its writer')
            if (pending or writing) and (self._thread is None or not self._thread.is_alive()):
                raise RuntimeError('runtime-status writer unavailable')
            return int(pending) + int(writing)

    def _run(self) -> None:
        from gateway import status
        while True:
            with self._condition:
                while self._pending is None:
                    self._condition.wait()
                generation, path, payload = self._pending
                self._pending = None
                self._writing_generation = generation
            error: Optional[BaseException] = None
            try:
                (self._write_fn or status._write_json_file)(path, status._merge_over_on_disk(path, payload))
            except BaseException as exc:
                error = exc
            with self._condition:
                self._writing_generation = 0
                self._completed_generation = max(self._completed_generation, generation)
                if error is None:
                    self._successful_generation = max(self._successful_generation, generation)
                    self._last_error = None
                else:
                    self._last_error = error
                self._condition.notify_all()
            if error is None:
                if self._failure_logged:
                    status.logger.info("Gateway runtime-status persistence recovered")
                    self._failure_logged = False
            elif not self._failure_logged:
                status.logger.warning(
                    "Failed to persist gateway runtime status; later updates will retry: %s", error)
                self._failure_logged = True
            else:
                status.logger.debug("Failed to persist gateway runtime status: %s", error)


def runtime_status_work_count() -> int:
    """Inspect existing writer without creating one or counting its idle thread."""
    from gateway import status
    with status._runtime_status_state_lock:
        writer = status._runtime_status_writer
        return 0 if writer is None else writer.work_count()
