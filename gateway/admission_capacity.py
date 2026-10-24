"""Transfer existing active-session capacity from authenticated ingress to core."""
import asyncio
import logging

logger = logging.getLogger(__name__)


def reserve_ingress_capacity(runner, event, session_key):
    """No await between registry acquisition and Base's task handoff."""
    from gateway.admission import GatewayDraining, _barrier, _ingress
    from hermes_cli.active_sessions import try_acquire_active_session, SESSION_COORDINATION_UNAVAILABLE
    barrier, owner = _barrier(runner), _ingress.get()
    with barrier._lock:
        if session_key in barrier._capacity:
            raise GatewayDraining('session admission already reserved',
                                  pre_effect=not owner.effects.started, reason='busy_backlog')
        limit = runner._active_session_limit_message(session_key)
        if limit is not None:
            raise GatewayDraining(str(limit), pre_effect=not owner.effects.started, reason='busy_backlog')
        try:
            home = runner._resolve_profile_home_for_source(event.source).resolve()
            platform = event.source.platform.value
            slot, refusal = try_acquire_active_session(
                session_id=session_key, surface=f'gateway:{platform}',
                config=getattr(runner, 'config', None), registry_home=home, track_liveness=True,
                metadata={'platform': platform, 'chat_id': event.source.chat_id or '',
                          'user_id': event.source.user_id or '', 'live_session_id': session_key})
        except Exception as exc:
            barrier._closed = barrier._uncertain_work = True
            raise GatewayDraining('active session ownership unreadable') from exc
        if refusal is not None or slot is None:
            if slot is None and (refusal is None or getattr(refusal, 'reason', None) == SESSION_COORDINATION_UNAVAILABLE):
                barrier._closed = barrier._uncertain_work = True
                raise GatewayDraining('active session ownership unreadable')
            raise GatewayDraining(str(refusal or 'active session capacity unavailable'),
                                  pre_effect=not owner.effects.started, reason='busy_backlog')
        barrier._capacity[session_key] = (owner, event, slot)


def take_ingress_capacity(runner, event, session_key):
    """Exact event/lease owner takes slot once; other callers cannot reacquire its writer."""
    from gateway.admission import _ingress
    barrier = getattr(runner, '_ingress_barrier', None)
    if barrier is None:
        return None
    with barrier._lock:
        entry = barrier._capacity.get(session_key)
        if entry is None:
            return None
        owner, reserved_event, slot = entry
        if event is not reserved_event or _ingress.get() is not owner or asyncio.current_task() not in owner.owners:
            return None, 'Session admission belongs to an earlier ingress; wait for its turn.'
        del barrier._capacity[session_key]
        return slot, None


def release_untransferred_capacity(barrier, owner):
    """Cancelled-before-start and pre-core exits cannot leak or transfer capacity."""
    with barrier._lock:
        if owner.owners:
            return
        slots = [entry[2] for entry in barrier._capacity.values() if entry[0] is owner]
        barrier._capacity = {key: entry for key, entry in barrier._capacity.items() if entry[0] is not owner}
    for slot in slots:
        try:
            slot.release()
        except Exception:
            barrier.fail_quiescence()
            logger.exception('Untransferred active session release unconfirmed; readiness blocked')
