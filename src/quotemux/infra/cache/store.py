from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from quotemux.infra.config import DATA_ROOT
from quotemux.infra.common import format_date_value, parse_date_text


CACHE_ROOT = DATA_ROOT / "type=cache" / "service=integration_api"


def normalize_day_key(value: object) -> str:
    if value is None:
        return ""
    text = format_date_value(value)
    if not text:
        return ""
    day = parse_date_text(text)
    if day is None:
        return ""
    return day.strftime("%Y%m%d")


def build_cache_path(provider: str, namespace: list[str], identity: dict[str, str], file_name: str = "data") -> Path:
    path = CACHE_ROOT / f"provider={provider}"
    for part in namespace:
        path = path / part
    for key, value in identity.items():
        path = path / f"{key}={value}"
    return path / f"{file_name}.parquet"


def read_cache_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def write_cache_frame(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def merge_cache_frame(df_old: pd.DataFrame, df_new: pd.DataFrame, key_columns: list[str], sort_columns: list[str]) -> pd.DataFrame:
    if df_old.empty:
        merged = df_new.copy()
    elif df_new.empty:
        merged = df_old.copy()
    else:
        merged = pd.concat([df_old, df_new], ignore_index=True)
    if merged.empty:
        return merged
    if sort_columns:
        merged = merged.sort_values(sort_columns)
    if key_columns:
        merged = merged.drop_duplicates(subset=key_columns, keep="last")
    if sort_columns:
        merged = merged.sort_values(sort_columns).reset_index(drop=True)
    return merged


def expected_day_points(start_value: str, end_value: str) -> list[str]:
    start_day = parse_date_text(start_value)
    end_day = parse_date_text(end_value)
    if start_day is None or end_day is None or start_day > end_day:
        return []
    points: list[str] = []
    current = start_day
    while current <= end_day:
        points.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return points


def expected_quarter_points(start_value: str, end_value: str) -> list[str]:
    start_day = parse_date_text(start_value)
    end_day = parse_date_text(end_value)
    if start_day is None or end_day is None or start_day > end_day:
        return []
    start_key = start_day.strftime("%Y%m%d")
    end_key = end_day.strftime("%Y%m%d")
    quarter_ends = ("0331", "0630", "0930", "1231")
    points: list[str] = []
    for year in range(start_day.year, end_day.year + 1):
        for quarter_end in quarter_ends:
            day_text = f"{year}{quarter_end}"
            if start_key <= day_text <= end_key:
                points.append(day_text)
    return points


def existing_unit_points(df: pd.DataFrame, column: str, unit: str) -> set[str]:
    if df.empty or column not in df.columns:
        return set()
    values = [normalize_day_key(value) for value in df[column].tolist()]
    return {value for value in values if value}


def group_missing_points(points: list[str]) -> list[tuple[str, str]]:
    if not points:
        return []
    grouped: list[tuple[str, str]] = []
    start_value = points[0]
    previous_value = points[0]
    for value in points[1:]:
        previous_date = parse_date_text(previous_value)
        current_date = parse_date_text(value)
        if current_date is None or previous_date is None:
            grouped.append((start_value, previous_value))
            start_value = value
            previous_value = value
            continue
        if current_date - previous_date == timedelta(days=1):
            previous_value = value
            continue
        grouped.append((start_value, previous_value))
        start_value = value
        previous_value = value
    grouped.append((start_value, previous_value))
    return grouped


def plan_missing_ranges(df: pd.DataFrame, column: str, start_value: str, end_value: str, unit: str) -> list[tuple[str, str]]:
    if not start_value or not end_value:
        return []
    if unit == "quarter":
        expected_points = expected_quarter_points(start_value, end_value)
    else:
        expected_points = expected_day_points(start_value, end_value)
    actual_points = existing_unit_points(df, column, unit)
    missing_points = [point for point in expected_points if point not in actual_points]
    return group_missing_points(missing_points)


def filter_frame_by_date_range(df: pd.DataFrame, column: str, start_value: str, end_value: str) -> pd.DataFrame:
    if df.empty or column not in df.columns:
        return df
    work = df.copy()
    dates = pd.to_datetime(work[column], format="mixed", errors="coerce").dt.strftime("%Y%m%d").fillna("")
    start_key = normalize_day_key(start_value)
    end_key = normalize_day_key(end_value)
    if start_key:
        work = work[dates >= start_key]
        dates = pd.to_datetime(work[column], format="mixed", errors="coerce").dt.strftime("%Y%m%d").fillna("")
    if end_key:
        work = work[dates <= end_key]
    return work


def filter_frame_by_datetime_range(df: pd.DataFrame, column: str, start_value: datetime | None, end_value: datetime | None) -> pd.DataFrame:
    if df.empty or column not in df.columns:
        return df
    work = df.copy()
    times = pd.to_datetime(work[column])
    if getattr(times.dt, "tz", None) is not None:
        times = times.dt.tz_localize(None)
    if start_value is not None:
        work = work[times >= start_value]
        times = pd.to_datetime(work[column])
        if getattr(times.dt, "tz", None) is not None:
            times = times.dt.tz_localize(None)
    if end_value is not None:
        work = work[times <= end_value]
    return work


def latest_n_rows(df: pd.DataFrame, sort_column: str, count: int | None) -> pd.DataFrame:
    if df.empty or not count:
        return df
    return df.sort_values(sort_column).tail(count)


