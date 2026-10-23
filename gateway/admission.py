"""Count transport work before session registration, through its background handoff.

Retryable extension handlers reserve immediately after synchronous authentication, before
their first await or mutation. A closed fence refuses new reservations; existing ones finish.
This process-local capability requires a running gateway built with this module.
"""

import asyncio
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar, copy_context
from functools import partial
from functools import wraps
import logging
from pathlib import Path
import threading

logger = logging.getLogger(__name__)
_ingress = ContextVar('gateway_ingress', default=None)


class GatewayDraining(RuntimeError):
    """No transport turn accepted. Retry the same delivery after readiness/busy state clears."""

    status = 503
    headers = {'Retry-After': '1'}


class _IngressLease:
    def __init__(self, barrier, owner, startup=None):
        self.startup = startup
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

    def _ready(self, *, startup=False):
        return (self.runner._running and not self.runner._draining
                and not self.runner._external_drain_active
                and (startup or not getattr(self.runner, '_startup_restore_in_progress', False)))

    def reserve(self, owner, *, startup=None):
        with self._lock:
            if self._closed or not self._ready(startup=startup is not None):
                raise GatewayDraining('gateway admission closed or not ready')
            self._ingress += 1
            return _IngressLease(self, owner, startup)

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


def gateway_ingress(runner):
    """Reserve one authenticated handler through its response/ack. Refusal has no side effects.

    The base adapter adopts its spawned task automatically. Extension-owned detached tasks must
    be adopted explicitly before this scope exits. Cancellation never releases a still-live task.
    """
    return _reserve_ingress(runner)


@contextmanager
def _reserve_ingress(runner, *, startup=None):
    barrier = _barrier(runner)
    owner = asyncio.current_task()
    lease = barrier.reserve(owner, startup=startup)
    token = _ingress.set(lease)
    try:
        yield
    finally:
        _ingress.reset(token)
        barrier.release(lease, owner)


def startup_resume_ingress(runner, event, session_key):
    """Native lifecycle owner only: resume its preclaimed slot while HTTP readiness stays closed."""
    return _reserve_ingress(runner, startup=(event, session_key))


def startup_resume_admitted(runner, event, session_key):
    lease = _ingress.get()
    return (ingress_admitted(runner) and lease.startup is not None
            and lease.startup[0] is event and lease.startup[1] == session_key)


async def prepare_ingress_dispatch(adapter, event, session_key):
    """Refuse backlog before Base acknowledges; route another adapter's live turn inline."""
    runner = adapter.gateway_runner
    if not ingress_admitted(runner) or startup_resume_admitted(runner, event, session_key):
        return False
    if runner._draining and (session_key in adapter._active_sessions or runner._is_session_running(session_key)):
        raise GatewayDraining('shutdown cannot accept a busy handoff')
    if adapter._pending_messages.get(session_key) or runner._overflow_queue(session_key):
        raise GatewayDraining('accepted session backlog must finish before retryable ingress')
    if runner._is_session_running(session_key) and session_key not in adapter._active_sessions:
        await adapter._handle_message_while_active(event, session_key)
        return True
    return False


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


def refuse_queued_ingress(runner):
    """Retryable ingress starts/steers directly; its caller retains work instead of a memory queue."""
    if ingress_admitted(runner):
        raise GatewayDraining('retryable ingress cannot enter an in-memory follow-up queue')



def admit_gateway_work(handler):
    """Native lifecycle producer: reserve before mutations; cancellation retains actual work."""
    @wraps(handler)
    async def admitted(runner, *args, **kwargs):
        scope = nullcontext() if ingress_admitted(runner) else gateway_ingress(runner)
        with scope:
            task = adopt_ingress_task(runner, asyncio.create_task(handler(runner, *args, **kwargs)))
            # A cancelled waiter no longer observes the producer's eventual exception.
            task.add_done_callback(lambda done: None if done.cancelled() else done.exception())
            return await asyncio.shield(task)
    return admitted


def admit_gateway_message(handler):
    """Native internal producers acquire the same fence and refund on refusal, before dispatch."""
    @wraps(handler)
    async def admitted(adapter, event):
        runner = adapter.gateway_runner
        if not event.internal or ingress_admitted(runner):
            return await handler(adapter, event)
        event._gateway_accepted = False
        try:
            with gateway_ingress(runner):
                return await handler(adapter, event)
        except GatewayDraining:
            return  # admit_internal_event converts the false receipt to refundable WakeNotAccepted
    return admitted


async def ingress_to_thread(function, /, *args, **kwargs):
    """Like to_thread, but cancellation retains admission until the actual worker exits.

    Use for pre-registration SQLite/journal work. Shield the executor future: cancelling an
    asyncio wrapper otherwise marks it done while the underlying thread is still writing.
    """
    return await _ingress_in_executor(None, function, *args, **kwargs)


async def native_to_thread(function, /, *args, **kwargs):
    """Native I/O policy: retain leased physical work; ordinary observers keep to_thread."""
    lease = _ingress.get()
    if lease is not None and lease.barrier.held(lease, asyncio.current_task()):
        return await ingress_to_thread(function, *args, **kwargs)
    return await asyncio.to_thread(function, *args, **kwargs)


async def native_in_executor(runner, executor, function, /, *args):
    """Preserve the native pool choice and profile context across admission-aware offloads."""
    if ingress_admitted(runner):
        return await _ingress_in_executor(executor, function, *args)
    return await asyncio.get_running_loop().run_in_executor(executor, copy_context().run, function, *args)


async def _ingress_in_executor(executor, function, /, *args, **kwargs):
    lease = _ingress.get()
    if lease is None or not lease.barrier.held(lease, asyncio.current_task()):
        raise GatewayDraining('off-thread work requires a live ingress reservation')
    future = asyncio.get_running_loop().run_in_executor(
        executor, copy_context().run, partial(function, *args, **kwargs))
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


def _observer_work_count(adapters):
    """Optional extension lifecycle counters. A broken declared contract never means idle."""
    total = 0
    for adapter in adapters:
        hook = getattr(adapter, 'admission_observer_status', None)
        if hook is None:
            continue
        status = hook()
        if not isinstance(status, dict) or status.get('known') is not True:
            raise GatewayDraining('extension observer status unavailable')
        for key in ('active_tasks', 'active_threads'):
            value = status.get(key)
            if type(value) is not int or value < 0:
                raise GatewayDraining('invalid extension observer count')
            total += value
    return total


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
        from gateway.config import Platform
        api = runner.adapters.get(Platform.API_SERVER)
        api_count = 0 if api is None else api.active_agent_work_count(strict=True)
        if type(api_count) is not int or api_count < 0:
            raise GatewayDraining('invalid API work count')
        deferred = getattr(runner, '_deferred_agent_workers', {})
        if not isinstance(deferred, dict):
            raise GatewayDraining('deferred worker registry unreadable')
        work = (runner._running_agent_count() + runner._running_cron_job_count()
                + api_count + api_worker_live_count()
                + sum(not future.done() for future in list(deferred))
                + bool(getattr(runner, '_startup_restore_in_progress', False)))
        # A base task can be waiting on routing/auth before the runner has claimed a session.
        adapters = list(runner.adapters.values())
        for profile_adapters in getattr(runner, '_profile_adapters', {}).values():
            adapters.extend(profile_adapters.values())
        observers = _observer_work_count(adapters)
        work += sum(not task.done() for adapter in adapters
                    for task in adapter._session_tasks.values())
        # Accepted memory-only backlog must not disappear behind an idle/orphaned guard.
        work += sum(len(adapter._pending_messages) + len(adapter._text_debounce) for adapter in adapters)
        work += sum(len(state.conversation.queued_events) + bool(state.persistent.pending_command_text)
                    for state in runner.__dict__.get('_sessions', {}).values())
        status.update(known=True, observers=observers, active_work=work + observers,
                      quiescent=status['closed'] and not (work + observers + status['ingress'] + status['adopted']))
        return status
    except Exception:
        logger.warning('Gateway quiescence readback unavailable', exc_info=True)
        return {'known': False, 'closed': None, 'ingress': None, 'adopted': None,
                'active_work': None, 'observers': None, 'quiescent': None}
