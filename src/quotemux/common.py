from __future__ import annotations

from collections.abc import Sequence

import pandas as pd
from pydantic import BaseModel


DEFAULT_LIMIT = 200
MAX_LIMIT = 5000
MARKET_DAILY_SNAPSHOT_LIMIT = 10000


def ensure_limit(limit: int) -> int:
    if limit < 1:
        raise ValueError("limit 必须大于 0")
    return min(limit, MAX_LIMIT)


def merge_model_lists[T: BaseModel](high_priority: Sequence[T], low_priority: Sequence[T], key_fields: tuple[str, ...]) -> list[T]:
    merged: list[T] = []
    index_map: dict[tuple[object, ...], int] = {}
    for item in high_priority:
        key = tuple(getattr(item, field) for field in key_fields)
        index_map[key] = len(merged)
        merged.append(item.model_copy(deep=True))
    for item in low_priority:
        key = tuple(getattr(item, field) for field in key_fields)
        if key not in index_map:
            index_map[key] = len(merged)
            merged.append(item.model_copy(deep=True))
            continue
        current = merged[index_map[key]]
        payload = current.model_dump()
        for field_name, value in item.model_dump().items():
            if field_name in key_fields:
                continue
            if payload[field_name] in {None, ""} and value not in {None, ""}:
                payload[field_name] = value
        merged[index_map[key]] = type(current)(**payload)
    return merged


def build_missing_expected_date_ranges(expected_dates: list[str], existing_dates: set[str]) -> list[tuple[str, str]]:
    if expected_dates == []:
        return []
    missing_ranges: list[tuple[str, str]] = []
    current_start = ""
    current_end = ""
    for expected_date in expected_dates:
        if expected_date in existing_dates:
            if current_start != "":
                missing_ranges.append((current_start, current_end))
                current_start = ""
                current_end = ""
            continue
        if current_start == "":
            current_start = expected_date
        current_end = expected_date
    if current_start != "":
        missing_ranges.append((current_start, current_end))
    return missing_ranges


def has_enough_stock_quote_rows(items: Sequence[BaseModel], codes: list[str], count: int | None, field_name: str) -> bool:
    if not count:
        return False
    counter = {code: 0 for code in codes}
    for item in items:
        key = str(getattr(item, field_name))
        counter[key] = counter.get(key, 0) + 1
    return all(value >= count for value in counter.values())


def sort_items[T: BaseModel](items: list[T], fields: tuple[str, ...]) -> list[T]:
    return sorted(items, key=lambda item: tuple(getattr(item, field) for field in fields))


def trim_items_per_key[T: BaseModel](items: list[T], key_field: str, time_field: str, count: int | None) -> list[T]:
    if not count:
        return items
    grouped: dict[str, list[T]] = {}
    for item in items:
        grouped.setdefault(str(getattr(item, key_field)), []).append(item)
    trimmed: list[T] = []
    for _, group_items in grouped.items():
        trimmed.extend(sorted(group_items, key=lambda item: str(getattr(item, time_field)))[-count:])
    return trimmed


def frame_to_datetime(frame: pd.DataFrame, column_name: str) -> pd.DataFrame:
    if frame.empty:
        return frame
    work = frame.copy()
    work[column_name] = pd.to_datetime(work[column_name], errors="coerce")
    return work.dropna(subset=[column_name])
