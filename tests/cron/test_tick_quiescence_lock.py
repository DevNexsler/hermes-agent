"""Actual per-home tick locks close the predicate-to-registration handoff race."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path
import threading

import pytest

from cron import jobs, scheduler
from cron.executions import finish_execution
from cron.scheduler_provider import _profile_cron_scope
from gateway.config import GatewayConfig
from gateway.run import _cron_tick_profile_homes
from hermes_constants import get_hermes_home


@pytest.mark.linux_only
def test_all_served_home_locks_exclude_ticks_and_preserve_due_work(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, 'home', lambda: tmp_path)
    home_a = tmp_path / '.hermes'
    home_b = home_a / 'profiles' / 'b'
    home_b.mkdir(parents=True)
    (home_b / 'config.yaml').write_text('{}\n', encoding='utf-8')
    monkeypatch.setenv('HERMES_HOME', str(home_a))
    homes = dict(_cron_tick_profile_homes(GatewayConfig(multiplex_profiles=False)))
    assert set(homes.values()) == {home_a, home_b}
    before = {}
    for home in (home_a, home_b, home_a):
        with _profile_cron_scope(home):
            directory, path = scheduler._get_lock_paths()
            assert path.resolve() == (home / 'cron' / '.tick.lock').resolve()
            if home not in before:
                job = jobs.create_job(prompt='offline fixture', schedule='every 1h')
                jobs.update_job(job['id'], {'next_run_at': (
                    datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()})
                before[home] = (directory / 'jobs.json').read_bytes()
    with ExitStack() as locks:
        for home in homes.values():
            with _profile_cron_scope(home):
                _directory, path = scheduler._get_lock_paths()
                handle = scheduler._acquire_tick_lock(path)
                assert handle is not None
                locks.callback(scheduler._release_tick_lock, handle)
                assert scheduler._acquire_tick_lock(path) is None
                # Real tick exits at the actual lock, before due selection or mutation.
                assert scheduler.tick(verbose=False, can_dispatch=lambda: True) == 0
        for home in (home_a, home_b, home_a):
            with _profile_cron_scope(home):
                assert (home / 'cron' / 'jobs.json').read_bytes() == before[home]
    for home in homes.values():
        with _profile_cron_scope(home):
            _directory, path = scheduler._get_lock_paths()
            handle = scheduler._acquire_tick_lock(path)
            assert handle is not None
            scheduler._release_tick_lock(handle)
            assert jobs.get_due_jobs()


@pytest.mark.linux_only
def test_tick_past_predicate_keeps_lock_until_real_registration_handoff(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, 'home', lambda: tmp_path)
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    selected, scan_resume, running, finish = (threading.Event() for _ in range(4))
    drain = threading.Event()
    effects = []
    due = scheduler.get_due_jobs

    def select():
        selected.set()
        assert scan_resume.wait(5)
        return due()

    def inert_job(job, *_args):
        effects.append((get_hermes_home(), job['id']))
        running.set()
        assert finish.wait(5)
        finish_execution(job['execution_id'], success=True)
        return True

    # Isolate side-effecting execution and unrelated periodic maintenance, retain real stores,
    # file lock, due scan/advance, execution rows, running registration and worker handoff.
    monkeypatch.setattr(scheduler, 'get_due_jobs', select)
    monkeypatch.setattr(scheduler, '_process_due_job', inert_job)
    monkeypatch.setattr(scheduler, '_should_yield_tick_to_fresh_gateway', lambda: None)
    for name in ('_maybe_run_worktree_maintenance', '_sweep_mcp_orphans', '_maybe_reap_dead_owners'):
        monkeypatch.setattr(scheduler, name, lambda: None)
    monkeypatch.setattr(scheduler, '_sweep_mcp_orphans_when_all_done', lambda _futures: None)
    from cron import bot_chat_delivery
    monkeypatch.setattr(bot_chat_delivery, 'drain_in_background', lambda: None)
    with _profile_cron_scope(tmp_path):
        job = jobs.create_job(prompt='offline fixture', schedule='every 1h')
        jobs.update_job(job['id'], {'next_run_at': (
            datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()})
        _directory, path = scheduler._get_lock_paths()

    def tick():
        with _profile_cron_scope(tmp_path):
            return scheduler.tick(verbose=False, sync=False, can_dispatch=lambda: not drain.is_set())

    with ThreadPoolExecutor(max_workers=1) as ticker:
        pending = ticker.submit(tick)
        try:
            assert selected.wait(5)  # predicate already passed, no job registered yet
            drain.set()
            assert not scheduler.get_running_job_ids()
            assert scheduler._acquire_tick_lock(path) is None  # zero is NOT quiescence yet
            scan_resume.set()
            assert pending.result(timeout=5) == 1
            assert running.wait(5)
            handle = scheduler._acquire_tick_lock(path)
            assert handle is not None  # handoff finished, worker now visible in real registry
            try:
                assert job['id'] in scheduler.get_running_job_ids()
                with scheduler._running_lock:
                    future = scheduler._running_futures[scheduler._inflight_key(job['id'], tmp_path)]
                finish.set()
                assert future.result(timeout=5)
                assert not scheduler.get_running_job_ids()
                assert tick() == 0  # lock still excludes a second tick after zero
            finally:
                scheduler._release_tick_lock(handle)
            assert effects == [(tmp_path, job['id'])]
            drain.clear()
            assert tick() == 0  # real persisted advance prevents duplicate effects after reopen
        finally:
            scan_resume.set()
            finish.set()
            scheduler._shutdown_parallel_pool()
