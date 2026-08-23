from __future__ import annotations

from datetime import time

from quotemux.store.capture_batches import CADENCE_DAILY, CADENCE_MONTHLY, CADENCE_WEEKLY, CADENCE_YEARLY, FIRST_BATCH_CAPTURE_POLICIES, first_batch_capability_ids


def test_first_batch_contains_kline_and_daily_capabilities() -> None:
    capability_ids = set(first_batch_capability_ids())

    assert "stocks.quotes.intraday" in capability_ids
    assert "stocks.quotes.daily" in capability_ids
    assert "stocks.quotes.daily_snapshot" in capability_ids
    assert "indexes.quotes.daily" in capability_ids
    assert "concepts.quotes.daily" in capability_ids
    catalog_policy = next(policy for policy in FIRST_BATCH_CAPTURE_POLICIES if policy.capability_id == "stocks.catalog")
    intraday_policy = next(policy for policy in FIRST_BATCH_CAPTURE_POLICIES if policy.capability_id == "stocks.quotes.intraday")
    assert catalog_policy.cadence == CADENCE_DAILY
    assert intraday_policy.batch_size == 20


def test_first_batch_uses_fixed_simplified_schedule() -> None:
    for policy in FIRST_BATCH_CAPTURE_POLICIES:
        expected_run_time = time(20, 0) if policy.capability_id == "stocks.quotes.intraday" else time(0, 0)
        assert policy.run_time == expected_run_time
        if policy.cadence == CADENCE_WEEKLY:
            assert policy.weekday == 6
        if policy.cadence == CADENCE_MONTHLY:
            assert policy.month_day == 31
        if policy.cadence == CADENCE_YEARLY:
            assert policy.month == 12
            assert policy.month_day == 31


def test_first_batch_policy_ids_are_unique() -> None:
    capability_ids = first_batch_capability_ids()

    assert len(capability_ids) == len(set(capability_ids))
