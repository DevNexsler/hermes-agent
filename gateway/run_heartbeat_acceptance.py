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


def settle_heartbeat_attempt(runner, event, manager):
    if not getattr(event, "_heartbeat_execution_started", False):
        started = False
        async def settle():
            nonlocal started
            started = True
            try:
                await runner._run_in_executor_with_context(manager.abandon_fire)
            except Exception:
                runner._ingress_barrier.fail_quiescence()
                logger.warning("Unexecuted heartbeat storage unconfirmed; inspect retained claim/history", exc_info=True)
        task = asyncio.create_task(settle())
        task.add_done_callback(lambda done: runner._ingress_barrier.fail_quiescence()
                               if done.cancelled() and not started else None)
        runner._track_deferred_agent_worker(task, None)
