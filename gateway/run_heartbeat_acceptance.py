"""Accounting at completion of an exact heartbeat admission attempt.

Execution means entering the gateway's agent runner after turn preparation,
not reserving an adapter slot, successful model completion, or outbound delivery.
The done callback retains the watch's profile ContextVars and manager claim.
"""
import asyncio
import logging

logger = logging.getLogger("gateway.run")


async def resolve_heartbeat_owner(runner, event, entry):
    """Keep normal reset/topic resolution, then admit only the original lineage."""
    expected = getattr(event, "_heartbeat_session_id", None)
    if not expected:
        return True
    resolved = entry.session_id
    if resolved != expected:
        def compression_tip():
            return runner.session_store._db_for_key(entry.session_key).get_compression_tip(expected)

        tip = await runner._run_in_executor_with_context(compression_tip)
        if tip != resolved:
            return False
    # Keep a value, not the mutable routing entry: preparation and hooks can yield
    # to /new or /stop before the agent runner starts.
    event._heartbeat_resolved_session_id = resolved
    return heartbeat_owner_is_current(runner, event, entry.session_key)


def heartbeat_owner_is_current(runner, event, session_key):
    expected = getattr(event, "_heartbeat_resolved_session_id", None)
    if not expected:
        return True
    current = runner.session_store.lookup_by_session_key(session_key)
    return current is not None and not current.suspended and current.session_id == expected


def _unknown(runner, manager):
    runner._ingress_barrier.fail_quiescence()
    logger.error('Heartbeat outcome UNKNOWN for %s; inspect retained claim/history before release', manager.session_id)


def settle_heartbeat_attempt(runner, event, manager, *, effects=None, task=None, refund_proven=False):
    """Own actual Base termination and refund only positively proven no-effect work."""
    from gateway.admission import adopt_ingress_task, ingress_admitted
    started = False
    async def settle():
        nonlocal started
        started = True
        refunding = False
        try:
            if task is not None:
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    if not task.cancelled() or asyncio.current_task().cancelling():
                        raise
            if getattr(event, '_heartbeat_execution_started', False):
                return  # A heartbeat charges execution entry, not model completion.
            if effects is None or effects.started or (task is None and not refund_proven):
                raise RuntimeError('heartbeat ended without execution or positive no-effect proof')
            refunding = True
            await runner._run_in_executor_with_context(manager.abandon_fire)
        except asyncio.CancelledError:
            # Confirmed no-effect refund's strict writer retains its physical future and
            # marks storage failures UNKNOWN itself, even after this observer is cancelled.
            if not refunding:
                _unknown(runner, manager)
            raise
        except BaseException:
            _unknown(runner, manager)
            raise
    observer = asyncio.create_task(settle())
    observer.add_done_callback(lambda done: _unknown(runner, manager)
                               if done.cancelled() and not started else None)
    if ingress_admitted(runner):
        adopt_ingress_task(runner, observer)
    runner._track_deferred_agent_worker(observer, None)
    return observer


async def dispatch_heartbeat_attempt(runner, event, manager, adapter, session_key):
    from gateway.admission import GatewayDraining, _ingress, ingress_admitted
    from gateway.wake import admit_internal_event
    effects = _ingress.get().effects
    try:
        await admit_internal_event(adapter, event)
    except GatewayDraining as exc:
        if (exc.pre_effect is True and ingress_admitted(runner) and not effects.started
                and getattr(event, '_gateway_accepted', False) is not True
                and session_key not in getattr(adapter, '_session_tasks', {})):
            observer = settle_heartbeat_attempt(runner, event, manager, effects=effects, refund_proven=True)
            await asyncio.shield(observer)
            return
        _unknown(runner, manager)
        raise RuntimeError('heartbeat dispatch outcome unknown') from exc
    except BaseException:
        _unknown(runner, manager)
        raise
    task = getattr(adapter, '_session_tasks', {}).get(session_key)
    observer = settle_heartbeat_attempt(runner, event, manager, effects=effects, task=task)
    if task is None:
        await asyncio.shield(observer)
