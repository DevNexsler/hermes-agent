"""Persistent owner hold survives abandoned/restarted owners and stays profile-explicit."""

import asyncio
from pathlib import Path

import pytest

from gateway.admission import (
    GatewayDraining, gateway_admission_status, gateway_ingress, initialize_gateway_admission,
    read_admission_controls, wait_for_admission_hold,
)
from gateway.admission_hold import (
    AdmissionHoldConflict, HOLD_FILENAME, acquire_admission_hold, inspect_admission_hold,
    release_admission_hold,
)
from tests.gateway.restart_test_helpers import make_restart_runner


@pytest.mark.asyncio
async def test_hold_survives_restart_stale_nas_and_abort_across_two_homes(tmp_path, monkeypatch):
    from gateway import admission
    from gateway import drain_control
    from gateway.run import _profile_runtime_scope

    monkeypatch.setattr(Path, 'home', lambda: tmp_path)
    homes = [tmp_path / name for name in ('A', 'B')]
    for home in homes:
        home.mkdir()
    own = acquire_admission_hold(owner='fixture', home=homes[0])
    original = (homes[0] / HOLD_FILENAME).read_bytes()
    assert acquire_admission_hold(owner='fixture', home=homes[0], token=own['token']) == own
    with pytest.raises(AdmissionHoldConflict):
        acquire_admission_hold(owner='other', home=homes[0])
    with pytest.raises(AdmissionHoldConflict):
        release_admission_hold(token='foreign', home=homes[0])
    assert (homes[0] / HOLD_FILENAME).read_bytes() == original
    # Neither a stale NAS marker nor a new process epoch releases the separate owner hold.
    drain_control.write_drain_request(home=homes[0])
    monkeypatch.setattr(drain_control, 'current_instantiation_epoch', lambda: 'different-epoch')
    monkeypatch.setattr(drain_control, 'DRAIN_REQUEST_MAX_AGE_SECONDS', -1)
    assert not drain_control.drain_requested(home=homes[0])
    for home in (homes[0], homes[1], homes[0]):
        runner, _adapter = make_restart_runner()
        initialize_gateway_admission(runner, home=home)
        with _profile_runtime_scope(homes[1], hydrate_secrets=False):
            controls = read_admission_controls(runner._ingress_barrier.home)
        assert controls['drain'] is (home == homes[0])
        if home == homes[0]:
            with pytest.raises(GatewayDraining):
                with gateway_ingress(runner):
                    pytest.fail('persistent hold admitted work')
    runner._running = False  # fresh boot, before any platform listener
    waiting = asyncio.create_task(wait_for_admission_hold(runner))
    # Run until the real filesystem read finishes and the wait reaches its bounded poll.
    await asyncio.sleep(0)
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert inspect_admission_hold(home=homes[0])['hold_id'] == own['hold_id']
    assert release_admission_hold(token=own['token'], home=homes[0])
    assert not release_admission_hold(token=own['token'], home=homes[0])
    assert await wait_for_admission_hold(runner)
    runner._running = True
    with gateway_ingress(runner):
        pass
    # The real boot entry cannot reach even its first setup hook while an owned hold exists.
    boot_hold = acquire_admission_hold(owner='boot-fixture', home=homes[0])
    boot, _adapter = make_restart_runner()
    boot._running = False
    initialize_gateway_admission(boot, home=homes[0])
    read, reached = asyncio.Event(), []
    loop = asyncio.get_running_loop()
    original_read = admission.read_admission_controls

    def read_controls(home):
        result = original_read(home)
        loop.call_soon_threadsafe(read.set)
        return result

    class BootReached(Exception):
        pass

    def first_setup():
        reached.append(True)
        raise BootReached

    original_wait = admission.wait_for_admission_hold

    async def quick_wait(runner):
        return await original_wait(runner, interval=0)

    monkeypatch.setattr(admission, 'read_admission_controls', read_controls)
    monkeypatch.setattr(admission, 'wait_for_admission_hold', quick_wait)
    boot._start_install_faulthandler = first_setup
    startup = asyncio.create_task(boot._start_impl())
    await asyncio.wait_for(read.wait(), 5)
    assert not reached and not startup.done()
    release_admission_hold(token=boot_hold['token'], home=homes[0])
    with pytest.raises(BootReached):
        await asyncio.wait_for(startup, 5)
    assert reached == [True]


@pytest.mark.parametrize('contents', ['{', '[]', '{"version": 1}', 'directory'])
def test_corrupt_hold_never_looks_absent_or_gets_replaced(tmp_path, contents):
    path = tmp_path / HOLD_FILENAME
    if contents == 'directory':
        path.mkdir()  # a real unreadable marker, independent of host UID/permission semantics
    else:
        path.write_text(contents, encoding='utf-8')
    assert inspect_admission_hold(home=tmp_path) == {
        'known': False, 'active': True, 'hold_id': None, 'owner': None,
    }
    for operation in (
        lambda: acquire_admission_hold(owner='fixture', home=tmp_path),
        lambda: release_admission_hold(token='foreign', home=tmp_path),
    ):
        with pytest.raises(AdmissionHoldConflict):
            operation()
    assert path.is_dir() if contents == 'directory' else path.read_text(encoding='utf-8') == contents
    runner, _adapter = make_restart_runner()
    initialize_gateway_admission(runner, home=tmp_path)
    assert gateway_admission_status(runner)['quiescent'] is None
