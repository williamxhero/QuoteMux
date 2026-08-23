from __future__ import annotations

from datetime import datetime, time

from quotemux.store import capture
from quotemux.store.capture import CAPTURE_SUCCESS, CapturePolicy, CaptureRun


def _policy(capability_id: str, cadence: str, run_time: time, **overrides: object) -> CapturePolicy:
    payload: dict[str, object] = {
        "capability_id": capability_id,
        "enabled": True,
        "cadence": cadence,
        "run_time": run_time,
        "timezone": "Asia/Shanghai",
        "weekday": None,
        "month": None,
        "month_day": None,
        "scope_profile": "concepts_recent_trading_days",
        "window_count": 30,
        "batch_size": 100,
        "notes": "",
    }
    payload.update(overrides)
    return CapturePolicy(**payload)


def test_scheduled_time_returns_most_recent_daily_occurrence_before_run_time() -> None:
    policy = _policy("concepts.quotes.daily", capture.CADENCE_DAILY, time(18, 0))

    assert capture._scheduled_time(policy, datetime(2026, 8, 23, 10, 42)) == datetime(2026, 8, 22, 18, 0)
    assert capture._scheduled_time(policy, datetime(2026, 8, 23, 18, 0)) == datetime(2026, 8, 23, 18, 0)


def test_scheduled_time_returns_most_recent_weekly_monthly_and_yearly_occurrence() -> None:
    now = datetime(2026, 8, 23, 10, 42)

    assert capture._scheduled_time(_policy("weekly", capture.CADENCE_WEEKLY, time(18, 0), weekday=6), now) == datetime(2026, 8, 16, 18, 0)
    assert capture._scheduled_time(_policy("monthly", capture.CADENCE_MONTHLY, time(18, 0), month_day=31), now) == datetime(2026, 7, 31, 18, 0)
    assert capture._scheduled_time(_policy("yearly", capture.CADENCE_YEARLY, time(18, 0), month=12, month_day=31), now) == datetime(2025, 12, 31, 18, 0)


def test_concept_membership_capture_precedes_concept_daily_capture() -> None:
    history = _policy("concepts.members.history", capture.CADENCE_DAILY, time(18, 0))
    members = _policy("concepts.members", capture.CADENCE_DAILY, time(18, 0))
    quotes = _policy("concepts.quotes.daily", capture.CADENCE_DAILY, time(18, 0))

    assert capture._due_policy_sort_key(history) < capture._due_policy_sort_key(quotes)
    assert capture._due_policy_sort_key(members) < capture._due_policy_sort_key(quotes)


def test_daily_catchup_is_due_once_for_latest_missed_occurrence() -> None:
    policy = _policy("concepts.quotes.daily", capture.CADENCE_DAILY, time(18, 0))
    now = datetime(2026, 8, 23, 10, 42)
    planned_time = datetime(2026, 8, 22, 18, 0)

    class _Runs:
        def __init__(self, latest: CaptureRun | None) -> None:
            self.latest = latest

        def latest_for_planned_time(self, capability_id: str, actual_planned_time: datetime) -> CaptureRun | None:
            assert actual_planned_time == planned_time
            return self.latest

    completed = CaptureRun(1, policy.capability_id, CAPTURE_SUCCESS, planned_time, planned_time, planned_time, 1, 1, "", {})

    assert capture.is_capture_due(policy, _Runs(None), now) is True
    assert capture.is_capture_due(policy, _Runs(completed), now) is False
