"""Own a native loop claim through Base termination and confirmed accounting."""
import asyncio
import logging

logger = logging.getLogger('gateway.run')


def _unknown(runner, attempt):
    runner._ingress_barrier.fail_quiescence()
    logger.error('Loop attempt UNKNOWN for %s; inspect retained loop checkpoint/history before release',
                 attempt['session_id'])


def confirm_loop_attempt(manager, attempt):
    """A compression child may carry this exact claim; replacements never do."""
    if (manager._state_db is not attempt['db'] or manager._expected_raw != attempt['raw']
            or (manager.session_id != attempt['session_id']
                and manager._state_db.get_compression_tip(attempt['session_id']) != manager.session_id)):
        raise RuntimeError('loop attempt ownership changed before completion')


def mark_loop_failure(event, result=None):
    attempt = getattr(event, '_loop_admission_checkpoint', None)
    if attempt is not None and (result is None or result.get('failed')):
        attempt['failed'] = True


async def _settle(runner, manager, event, attempt, task):
    attempt['observer_started'] = True
    try:
        if task is not None:
            await asyncio.shield(task)
        if event.get_command() and attempt.get('command_completed') and not attempt.get('failed'):
            await runner._run_in_executor_with_context(manager.complete_tick, '')
            attempt['accounted'] = True
        if not attempt['accounted']:
            raise RuntimeError('accepted loop ended without confirmed accounting')
    except BaseException:
        _unknown(runner, attempt)
        raise


async def dispatch_loop_attempt(runner, manager, adapter, event, session_key):
    from gateway.admission import GatewayDraining, _ingress, adopt_ingress_task, ingress_admitted
    from gateway.wake import admit_internal_event
    attempt = event._loop_admission_checkpoint = {
        'session_id': manager.session_id, 'db': manager._state_db,
        'raw': manager._expected_raw, 'accounted': False, 'observer_started': False}
    event._heartbeat_execution_started = False  # Existing generic core-entry witness.
    try:
        await admit_internal_event(adapter, event)
    except GatewayDraining as exc:
        if (exc.pre_effect is True and ingress_admitted(runner) and not _ingress.get().effects.started
                and getattr(event, '_gateway_accepted', False) is not True
                and session_key not in getattr(adapter, '_session_tasks', {})):
            await runner._run_in_executor_with_context(manager.abandon_tick)
            return
        _unknown(runner, attempt)
        raise RuntimeError('loop dispatch outcome unknown') from exc
    except BaseException:
        # Missing boolean admission receipt is not positive no-effect proof.
        _unknown(runner, attempt)
        raise
    task = getattr(adapter, '_session_tasks', {}).get(session_key)
    observer = asyncio.create_task(_settle(runner, manager, event, attempt, task))
    observer.add_done_callback(lambda done: _unknown(runner, attempt)
                               if done.cancelled() and not attempt['observer_started'] else None)
    adopt_ingress_task(runner, observer)
    runner._track_deferred_agent_worker(observer, None)
    if task is None:
        await asyncio.shield(observer)
