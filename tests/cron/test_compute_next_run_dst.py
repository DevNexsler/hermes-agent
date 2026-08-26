"""Cron schedules preserve configured local wall time across DST changes."""
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

pytest.importorskip("croniter")

from cron.jobs import compute_next_run


NY = ZoneInfo("America/New_York")


@pytest.mark.parametrize(
    ("base", "expr", "expected"),
    [
        ("2026-02-10T12:00:00-05:00", "0 8 * * *", "2026-02-11T08:00:00-05:00"),
        ("2026-03-07T12:00:00-05:00", "0 8 * * *", "2026-03-08T08:00:00-04:00"),
        ("2026-03-01T20:31:00-05:00", "30 20 * * 0", "2026-03-08T20:30:00-04:00"),
        ("2026-10-31T12:00:00-04:00", "0 8 * * *", "2026-11-01T08:00:00-05:00"),
        ("2026-10-25T20:31:00-04:00", "30 20 * * 0", "2026-11-01T20:30:00-05:00"),
    ],
)
def test_cron_next_run_preserves_new_york_wall_clock(
    monkeypatch: pytest.MonkeyPatch, base: str, expr: str, expected: str,
) -> None:
    """Catch croniter applying offset changes twice across DST boundaries."""
    now = datetime.fromisoformat(base).astimezone(NY)
    monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)

    assert compute_next_run({"kind": "cron", "expr": expr}) == expected


def test_nonexistent_spring_wall_time_is_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    """A cron wall time inside the spring gap cannot name a real instant."""
    now = datetime.fromisoformat("2026-03-07T12:00:00-05:00").astimezone(NY)
    monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)

    assert compute_next_run({"kind": "cron", "expr": "30 2 * * *"}) == (
        "2026-03-09T02:30:00-04:00"
    )


def test_ambiguous_fall_wall_time_uses_first_fold_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Before fallback, choose first 01:30; after it runs, advance to next day."""
    before = datetime.fromisoformat("2026-10-31T12:00:00-04:00").astimezone(NY)
    monkeypatch.setattr("cron.jobs._hermes_now", lambda: before)
    schedule = {"kind": "cron", "expr": "30 1 * * *"}

    first = compute_next_run(schedule)
    assert first == "2026-11-01T01:30:00-04:00"
    assert compute_next_run(schedule, last_run_at=first) == "2026-11-02T01:30:00-05:00"


def test_once_and_interval_normal_day_contract_is_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.fromisoformat("2026-02-10T12:00:00-05:00").astimezone(NY)
    monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)

    assert compute_next_run({"kind": "interval", "minutes": 60}) == (
        "2026-02-10T13:00:00-05:00"
    )
    run_at = "2026-02-10T12:01:00-05:00"
    assert compute_next_run({"kind": "once", "run_at": run_at}) == run_at
