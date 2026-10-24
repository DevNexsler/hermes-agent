"""Cancelled goal handoff stays counted through actual terminal accounting."""
import asyncio

import pytest

from gateway.admission import gateway_admission_status
from hermes_cli import goals
from tests.gateway import test_admission_native_work
from tests.gateway.test_admission_loop_storage import loop_entry

native = test_admission_native_work.native


@pytest.mark.asyncio
async def test_goal_handoff_cancelled_before_first_step_has_no_callback_zero(native):
    source, entry = loop_entry(native, fired=False)
    mgr = goals.GoalManager(entry.session_id)
    mgr.set('EXACT retained goal')
    expected = native.db.get_meta('goal:' + entry.session_id)
    event = native.runner._synthetic_prompt_event(source, mgr.next_continuation_prompt())
    event._goal_admission_checkpoint = {'session_id': entry.session_id, 'goal_state': mgr.state.to_json(),
                                         'final_response': 'EXACT previous response', 'startup_restore': False}
    task = asyncio.create_task(asyncio.sleep(10))
    native.runner._ingress_barrier.close()
    native.runner._track_goal_drain_checkpoint(native.adapter, event, entry.session_key, task)
    task.cancel()
    await asyncio.sleep(0)
    await asyncio.sleep(0)  # Original done callback registered cleanup, before cleanup's first step.
    cleanup = next(iter(native.runner._deferred_agent_cleanup_tasks))
    cleanup.cancel()
    snapshots = []
    asyncio.get_running_loop().call_soon(lambda: snapshots.append(gateway_admission_status(native.runner)))
    await asyncio.sleep(0)
    assert snapshots and not snapshots[0]['quiescent']
    await asyncio.gather(task, cleanup, return_exceptions=True)
    await asyncio.sleep(0)
    assert not gateway_admission_status(native.runner)['known']
    assert native.db.get_meta('goal:' + entry.session_id) == expected and not native.effects


