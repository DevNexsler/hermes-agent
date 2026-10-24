"""Directory helper carries concrete native owner and actual profile scope A→B→A."""
import asyncio
from pathlib import Path

import pytest

from gateway import admission, channel_directory, run
from gateway.config import Platform
from hermes_constants import get_hermes_home
from hermes_state import SessionDB
from hermes_state_registry import close_all_under
from tests.gateway import test_admission_native_work

native = test_admission_native_work.native


@pytest.mark.asyncio
async def test_actual_directory_discovery_and_persist_keep_scope_and_owner(native, monkeypatch):
    import hermes_state_registry
    runner = native.runner
    runner.config.multiplex_profiles = True
    second = native.home.parent / 'profile-B'
    second.mkdir()
    (second / '.env').write_text('SYNTHETIC_DIRECTORY_PROFILE=B\n')
    db = SessionDB(db_path=second / 'state.db')
    db.close()
    seen, entered, release = [], asyncio.Queue(), asyncio.Event()
    discover, write = channel_directory._build_from_sessions, channel_directory.atomic_json_write
    acquire = hermes_state_registry.acquire

    def inspect(kind):
        assert admission._native_runner.get() is runner
        assert admission._ingress.get() is None
        seen.append((kind, Path(get_hermes_home())))

    def discovery(platform):
        inspect('read')
        return discover(platform)

    def writer(path, data):
        inspect('write')
        assert Path(path).parent == get_hermes_home()
        return write(path, data)

    def scoped_db(*args, **kwargs):
        db = acquire(*args, **kwargs)
        assert Path(db.db_path).parent == get_hermes_home()
        return db

    async def list_channels():
        await entered.put(Path(get_hermes_home()))
        await release.wait()
        release.clear()
        return None

    native.adapter.list_channels = list_channels
    monkeypatch.setattr(channel_directory, '_build_from_sessions', discovery)
    monkeypatch.setattr(channel_directory, 'atomic_json_write', writer)
    monkeypatch.setattr(hermes_state_registry, 'acquire', scoped_db)
    homes = [native.home, second, native.home]

    async def refresh():
        for home in homes:
            async with run._async_profile_runtime_scope(home):
                await channel_directory.build_channel_directory({Platform.TELEGRAM: native.adapter})

    # Actual supervisor must discard unrelated authenticated parent ingress.
    with admission.gateway_ingress(runner):
        task = runner._spawn_supervised(refresh, 'synthetic scoped directory', restart=False)
    try:
        for home in homes:
            assert await asyncio.wait_for(entered.get(), 5) == home
            release.set()
        await task
        assert seen == [(kind, home) for home in homes for kind in ('read', 'write')]
        assert (second / 'channel_directory.json').exists()
        assert (native.home / 'channel_directory.json').exists()
        assert not native.effects
    finally:
        release.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        close_all_under(second)
