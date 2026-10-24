"""Native macOS failed secondary status reads retain an actually live local worker."""
import subprocess
import sys
from types import SimpleNamespace

import pytest

from hermes_cli import kanban_db_dispatch as dispatch


@pytest.mark.macos_only
@pytest.mark.parametrize('failure', ['nonzero', 'timeout', 'empty'])
def test_native_macos_secondary_query_cannot_erase_live_worker(monkeypatch, failure):
    proc = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
    original = subprocess.run
    def failed_status(args, **kwargs):
        if args[:3] == ['ps', '-o', 'stat=']:
            if failure == 'timeout':
                raise subprocess.TimeoutExpired(args, 1)
            return SimpleNamespace(returncode=1 if failure == 'nonzero' else 0, stdout='')
        return original(args, **kwargs)
    try:
        monkeypatch.setattr(subprocess, 'run', failed_status)
        assert proc.poll() is None
        assert dispatch._pid_alive(proc.pid) is True
    finally:
        proc.terminate()
        proc.wait(timeout=5)
    assert dispatch._pid_alive(proc.pid) is False
