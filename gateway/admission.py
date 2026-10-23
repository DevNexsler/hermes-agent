"""Count transport work before session registration, through its background handoff.

Retryable extension handlers reserve immediately after synchronous authentication, before
their first await or mutation. A closed fence refuses new reservations; existing ones finish.
This process-local capability requires a running gateway built with this module.
"""

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar, copy_context
from functools import partial
import logging
from pathlib import Path
import threading

logger = logging.getLogger(__name__)
_ingress = ContextVar('gateway_ingress', default=None)


class GatewayDraining(RuntimeError):
    """No admission occurred. Retry the same delivery after readiness returns."""

    status = 503
    headers = {'Retry-After': '1'}


class _IngressLease:
    def __init__(self, barrier, owner):
        self.barrier = barrier
        self.owners = {owner}


class GatewayAdmission:
    """Runner-owned fence. Closing and reserving linearize on the same lock."""

    def __init__(self, runner, home):
        self.runner = runner
        self.home = Path(home).resolve()
        self._lock = threading.Lock()
        self._closed = False
        self._ingress = 0
        self._adopted = 0
        self.hold = {'known': True, 'active': False, 'hold_id': None, 'owner': None}

    def close(self):
        with self._lock:
            self._closed = True

    def reopen(self):
        with self._lock:
            if not self.runner._draining and not self.hold['active']:
                self._closed = False

    def _ready(self):
        return (self.runner._running and not self.runner._draining
                and not self.runner._external_drain_active
                and not getattr(self.runner, '_startup_restore_in_progress', False))

    def reserve(self, owner):
        with self._lock:
            if self._closed or not self._ready():
                raise GatewayDraining('gateway admission closed or not ready')
            self._ingress += 1
            return _IngressLease(self, owner)

    def held(self, lease, owner):
        with self._lock:
            return lease.barrier is self and owner in lease.owners

    def adopt(self, lease, owner, task):
        with self._lock:
            if lease.barrier is not self or owner not in lease.owners:
                raise GatewayDraining('ingress reservation ended before handoff')
            if task in lease.owners:
                raise ValueError('ingress task already adopted')
            lease.owners.add(task)
            self._adopted += 1
        # A done callback also releases a task cancelled BEFORE its first coroutine step.
        task.add_done_callback(lambda done: self.release(lease, done, adopted=True))

    def release(self, lease, owner, *, adopted=False):
        with self._lock:
            lease.owners.remove(owner)
            if adopted:
                self._adopted -= 1
            else:
                self._ingress -= 1

    def snapshot(self):
        with self._lock:
            return {'closed': self._closed or not self._ready(),
                    'ingress': self._ingress, 'adopted': self._adopted, 'hold': dict(self.hold)}


def initialize_gateway_admission(runner, *, home=None):
    """Pin the launch home and reconcile its marker BEFORE any adapter can connect."""
    from hermes_constants import get_routing_process_hermes_home
    if getattr(runner, '_ingress_barrier', None) is not None:
        raise RuntimeError('gateway admission already initialized')
    barrier = runner._ingress_barrier = GatewayAdmission(
        runner, home if home is not None else get_routing_process_hermes_home())
    controls = read_admission_controls(barrier.home)
    barrier.hold = controls['hold']
    runner._external_drain_active = controls['drain']
    if runner._external_drain_active:
        barrier.close()


def read_admission_controls(home):
    """Off-loop filesystem read; persistent holds never inherit NAS expiry/epoch semantics."""
    from gateway.admission_hold import inspect_admission_hold
    from gateway.drain_control import drain_requested
    hold = inspect_admission_hold(home=home)
    return {'hold': hold, 'drain': hold['active'] or drain_requested(home=home)}


def reconcile_admission_controls(runner, controls):
    barrier = _barrier(runner)
    with barrier._lock:
        barrier.hold = controls['hold']
    if controls['drain']:
        runner._enter_external_drain()
    else:
        runner._exit_external_drain()


async def wait_for_admission_hold(runner, *, interval=1.0):
    """Boot waits before listeners, cron or restore. Cancellation leaves durable owner hold intact."""
    barrier = _barrier(runner)
    while True:
        controls = await asyncio.to_thread(read_admission_controls, barrier.home)
        reconcile_admission_controls(runner, controls)
        if not controls['hold']['active']:
            return True
        if runner._shutdown_event.is_set():
            return False
        await asyncio.sleep(interval)


def _barrier(runner):
    barrier = getattr(runner, '_ingress_barrier', None)
    if not isinstance(barrier, GatewayAdmission) or barrier.runner is not runner:
        raise GatewayDraining('native gateway admission capability unavailable')
    return barrier


@contextmanager
def gateway_ingress(runner):
    """Reserve one authenticated handler through its response/ack. Refusal has no side effects.

    The base adapter adopts its spawned task automatically. Extension-owned detached tasks must
    be adopted explicitly before this scope exits. Cancellation never releases a still-live task.
    """
    barrier = _barrier(runner)
    owner = asyncio.current_task()
    lease = barrier.reserve(owner)
    token = _ingress.set(lease)
    try:
        yield
    finally:
        _ingress.reset(token)
        barrier.release(lease, owner)


def ingress_admitted(runner):
    """Only this reservation's handler/adopted tasks may finish across an external drain."""
    lease = _ingress.get()
    barrier = getattr(runner, '_ingress_barrier', None)
    return (isinstance(barrier, GatewayAdmission) and lease is not None
            and barrier.held(lease, asyncio.current_task()))


def adopt_ingress_task(runner, task):
    """Carry a live handler reservation across background dispatch; otherwise leave legacy tasks."""
    lease = _ingress.get()
    if lease is not None:
        _barrier(runner).adopt(lease, asyncio.current_task(), task)
    return task


async def ingress_to_thread(function, /, *args, **kwargs):
    """Like to_thread, but cancellation retains admission until the actual worker exits.

    Use for pre-registration SQLite/journal work. Shield the executor future: cancelling an
    asyncio wrapper otherwise marks it done while the underlying thread is still writing.
    """
    lease = _ingress.get()
    if lease is None or not lease.barrier.held(lease, asyncio.current_task()):
        raise GatewayDraining('off-thread work requires a live ingress reservation')
    future = asyncio.get_running_loop().run_in_executor(
        None, copy_context().run, partial(function, *args, **kwargs))
    lease.barrier.adopt(lease, asyncio.current_task(), future)
    # A cancelled handler no longer awaits a worker's exception, but must not cancel its work.
    future.add_done_callback(lambda done: None if done.cancelled() else done.exception())
    return await asyncio.shield(future)


def ingress_work_count(runner):
    """Compatibility aggregate; capability consumers use the strict status below."""
    barrier = getattr(runner, '_ingress_barrier', None)
    if barrier is None:
        return 0
    status = barrier.snapshot()
    return status['ingress'] + status['adopted']


def gateway_admission_status(runner):
    """Strict in-process readback. Unknown counters never mean idle.

    Cron tick locks for EVERY served home must already be held by the coordinator: its
    registration handoff is outside the session counters. This is no business completion receipt.
    """
    try:
        barrier = _barrier(runner)
        status = barrier.snapshot()
        if not status['hold']['known']:
            raise GatewayDraining('persistent admission hold unreadable')
        from gateway.platforms.api_server_runs import api_worker_live_count
        work = (runner._running_agent_count() + runner._running_cron_job_count()
                + runner._api_server_hook('active_agent_work_count') + api_worker_live_count()
                + runner._active_deferred_agent_worker_count())
        # A base task can be waiting on routing/auth before the runner has claimed a session.
        adapters = list(runner.adapters.values())
        for profile_adapters in getattr(runner, '_profile_adapters', {}).values():
            adapters.extend(profile_adapters.values())
        work += sum(not task.done() for adapter in adapters
                    for task in adapter._session_tasks.values())
        status.update(known=True, active_work=work,
                      quiescent=status['closed'] and not (work + status['ingress'] + status['adopted']))
        return status
    except Exception:
        logger.warning('Gateway quiescence readback unavailable', exc_info=True)
        return {'known': False, 'closed': None, 'ingress': None, 'adopted': None,
                'active_work': None, 'quiescent': None}
