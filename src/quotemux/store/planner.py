from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Sequence


@dataclass(frozen=True)
class CacheMissingRange:
    time_start: datetime
    time_end: datetime


def _merge_missing_points(points: Sequence[datetime], step: timedelta) -> tuple[CacheMissingRange, ...]:
    if points == ():
        return ()
    sorted_points = sorted(dict.fromkeys(points))
    ranges: list[CacheMissingRange] = []
    current_start = sorted_points[0]
    current_end = sorted_points[0]
    for point in sorted_points[1:]:
        if point - current_end <= step:
            current_end = point
            continue
        ranges.append(CacheMissingRange(current_start, current_end))
        current_start = point
        current_end = point
    ranges.append(CacheMissingRange(current_start, current_end))
    return tuple(ranges)


def _daily_points(time_start: datetime, time_end: datetime) -> tuple[datetime, ...]:
    items: list[datetime] = []
    current = datetime(time_start.year, time_start.month, time_start.day)
    end = datetime(time_end.year, time_end.month, time_end.day)
    while current <= end:
        items.append(current)
        current += timedelta(days=1)
    return tuple(items)


def _minute_points(time_start: datetime, time_end: datetime, session_ranges: Sequence[CacheMissingRange] = ()) -> tuple[datetime, ...]:
    items: list[datetime] = []
    if session_ranges == ():
        current = time_start.replace(second=0, microsecond=0)
        end = time_end.replace(second=0, microsecond=0)
        while current <= end:
            items.append(current)
            current += timedelta(minutes=1)
        return tuple(items)
    for session_range in session_ranges:
        current = max(time_start, session_range.time_start).replace(second=0, microsecond=0)
        end = min(time_end, session_range.time_end).replace(second=0, microsecond=0)
        while current <= end:
            items.append(current)
            current += timedelta(minutes=1)
    return tuple(items)


class CacheMissingPlanner:
    def plan(
        self,
        coverage_mode: str,
        time_start: datetime,
        time_end: datetime,
        covered_ranges: Sequence[CacheMissingRange],
        expected_points: Sequence[datetime] = (),
        session_ranges: Sequence[CacheMissingRange] = (),
    ) -> tuple[CacheMissingRange, ...]:
        if coverage_mode == "snapshot":
            return () if self._is_covered(time_start, time_end, covered_ranges) else (CacheMissingRange(time_start, time_end),)
        points = self._expected_points(coverage_mode, time_start, time_end, expected_points, session_ranges)
        missing_points = tuple(point for point in points if not self._is_point_covered(point, covered_ranges))
        return _merge_missing_points(missing_points, self._step_for_mode(coverage_mode))

    def _expected_points(
        self,
        coverage_mode: str,
        time_start: datetime,
        time_end: datetime,
        expected_points: Sequence[datetime],
        session_ranges: Sequence[CacheMissingRange],
    ) -> tuple[datetime, ...]:
        if expected_points != ():
            return tuple(expected_points)
        if coverage_mode == "minute_range":
            return _minute_points(time_start, time_end, session_ranges)
        return _daily_points(time_start, time_end)

    def _step_for_mode(self, coverage_mode: str) -> timedelta:
        if coverage_mode == "minute_range":
            return timedelta(minutes=1)
        return timedelta(days=1)

    def _is_point_covered(self, point: datetime, covered_ranges: Sequence[CacheMissingRange]) -> bool:
        return any(item.time_start <= point <= item.time_end for item in covered_ranges)

    def _is_covered(self, time_start: datetime, time_end: datetime, covered_ranges: Sequence[CacheMissingRange]) -> bool:
        return any(item.time_start <= time_start and item.time_end >= time_end for item in covered_ranges)
