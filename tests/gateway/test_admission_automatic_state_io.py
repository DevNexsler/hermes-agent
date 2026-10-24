"""Physical native metadata writers retain ownership through refusal and cancellation."""
import asyncio
import json
import threading
from types import SimpleNamespace

import pytest

from gateway.admission import (GatewayAdmission, gateway_admission_status, gateway_ingress,
                               mark_native_work_uncertain, _ingress)
from gateway.run import GatewayRunner
from gateway.run_heartbeat_acceptance import settle_heartbeat_attempt
from hermes_cli import goals, heartbeat, loops
from hermes_state import SessionDB
from tests.gateway import test_admission_native_work
from tests.gateway.test_admission_loop_storage import clear_fault, loop_entry, settle, storage_fault

native = test_admission_native_work.native


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['none', 'reject', 'readback', 'read'])
async def test_real_heartbeat_claim_is_confirmed_before_adapter_effect(native, fault):
    source, entry = loop_entry(native, fired=False)
    key = 'heartbeat:' + entry.session_id
    heartbeat.save_heartbeat(entry.session_id, heartbeat.HeartbeatState('EXACT heartbeat', 60, created_at=1))
    expected = native.db.get_meta(key)
    storage_fault(native.db, key, fault)
    native.finish.set()
    watch = {entry.session_key: (source, entry.session_id)}
    try:
        await native.runner._heartbeat_poll_once(watch)
        await asyncio.gather(*native.adapter._background_tasks, return_exceptions=True)
        await settle(native)
    finally:
        clear_fault(native.db, fault)
    native.runner._ingress_barrier.close()
    assert gateway_admission_status(native.runner)['known'] is (fault == 'none')
    if fault != 'none':
        assert not native.effects and native.db.get_meta(key) == expected
    else:
        assert len(native.effects) == 1 and heartbeat.load_heartbeat(entry.session_id).fire_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['none', 'reject', 'readback'])
async def test_cancelled_heartbeat_refund_keeps_physical_writer_owned(native, monkeypatch, fault):
    source, entry = loop_entry(native, fired=False)
    sid, key = entry.session_id, 'heartbeat:' + entry.session_id
    heartbeat.save_heartbeat(sid, heartbeat.HeartbeatState('EXACT unexecuted heartbeat', 60, created_at=1))
    mgr = heartbeat.HeartbeatManager(sid, strict=True)
    assert mgr.due_prompt()
    charged = native.db.get_meta(key)
    entered, release = asyncio.Event(), threading.Event()
    loop, original = asyncio.get_running_loop(), native.db.set_meta
    def held(*args, **kwargs):
        if args[0] == key:
            loop.call_soon_threadsafe(entered.set)
            assert release.wait(5)
        return original(*args, **kwargs)
    monkeypatch.setattr(native.db, 'set_meta', held)
    storage_fault(native.db, key, fault)
    try:
        with gateway_ingress(native.runner):
            # Explicit no-effect admission-refusal fixture; false core entry alone proves nothing.
            settle_heartbeat_attempt(native.runner, SimpleNamespace(_heartbeat_execution_started=False), mgr,
                                     effects=_ingress.get().effects, refund_proven=True)
        task = next(iter(native.runner._deferred_agent_workers))
        await asyncio.wait_for(entered.wait(), 5)
        native.runner._ingress_barrier.close()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert not gateway_admission_status(native.runner)['quiescent']
        assert not native.effects
    finally:
        release.set()
        await settle(native)
        clear_fault(native.db, fault)
    status = gateway_admission_status(native.runner)
    assert status['known'] is (fault == 'none')
    assert native.db.get_meta(key) == charged if fault != 'none' else heartbeat.load_heartbeat(sid).fire_count == 0


@pytest.mark.asyncio
async def test_explicit_executor_runner_identity_never_borrows_ambient_lease(native):
    other = object.__new__(GatewayRunner)
    other._running, other._draining, other._external_drain_active = True, False, False
    other._ingress_barrier = GatewayAdmission(other, native.home.parent / 'B')
    with gateway_ingress(native.runner):
        await other._run_in_executor_with_context(mark_native_work_uncertain)
        assert other._ingress_barrier._uncertain_work
        assert not native.runner._ingress_barrier._uncertain_work
        mark_native_work_uncertain()
        assert native.runner._ingress_barrier._uncertain_work
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_heartbeat_refund_cancelled_before_first_step_retains_claim_unknown(native):
    _source, entry = loop_entry(native, fired=False)
    sid, key = entry.session_id, 'heartbeat:' + entry.session_id
    heartbeat.save_heartbeat(sid, heartbeat.HeartbeatState('EXACT unexecuted heartbeat', 60, created_at=1))
    mgr = heartbeat.HeartbeatManager(sid, strict=True)
    assert mgr.due_prompt()
    charged = native.db.get_meta(key)
    with gateway_ingress(native.runner):
        settle_heartbeat_attempt(native.runner, SimpleNamespace(_heartbeat_execution_started=False), mgr,
                                 effects=_ingress.get().effects, refund_proven=True)
    task = next(iter(native.runner._deferred_agent_workers))
    native.runner._ingress_barrier.close()
    task.cancel()
    snapshots = []
    asyncio.get_running_loop().call_soon(lambda: snapshots.append(gateway_admission_status(native.runner)))
    await asyncio.sleep(0)  # Probe after task cancellation, before its queued done callbacks.
    assert snapshots and not snapshots[0]['quiescent']
    await asyncio.gather(task, return_exceptions=True)
    await settle(native)
    assert not gateway_admission_status(native.runner)['known']
    assert native.db.get_meta(key) == charged and not native.effects


@pytest.mark.asyncio
async def test_real_state_transfer_profile_scope_a_b_a(native):
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override
    from hermes_state_registry import close_all_under
    second = native.home.parent / 'profile-B'
    second.mkdir()
    stores = {}
    try:
        for index, home in enumerate((native.home, second, native.home)):
            token = set_hermes_home_override(home)
            try:
                db = await native.runner._run_in_executor_with_context(goals._get_session_db)
                assert db.db_path.resolve() == home.resolve() / 'state.db'
                stores[home] = db
                sid, child = 'same-parent-' + str(index), 'same-child-' + str(index)
                loops.LoopManager(sid).set('EXACT ' + home.name, interval_seconds=60)
                expected = db.get_meta('loop:' + sid)
                assert await native.runner._run_in_executor_with_context(
                    lambda: loops.migrate_loop_to_session(sid, child, db=db, strict=True))
                assert db.get_meta('loop:' + child) == expected
                assert json.loads(db.get_meta('loop:' + sid))['status'] == 'cleared'
            finally:
                reset_hermes_home_override(token)
        assert stores[native.home].get_meta('loop:same-child-1') is None
        assert stores[second].get_meta('loop:same-child-0') is None
        reopened = SessionDB(db_path=stores[second].db_path)
        try:
            assert reopened.get_meta('loop:same-child-1') == stores[second].get_meta('loop:same-child-1')
        finally:
            reopened.close()
    finally:
        close_all_under(second)
