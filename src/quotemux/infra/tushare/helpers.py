from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

from quotemux.infra.cache.store import build_cache_path, merge_cache_frame, plan_missing_ranges, read_cache_frame, write_cache_frame
from quotemux.infra.common import format_date_value
from quotemux.infra.config import DATE_FORMAT
from quotemux.infra.tushare.rate_limit import call_tushare_api


def query_frame(api_name: str, **kwargs: object) -> pd.DataFrame:
    from quotemux_packages.tushare.source import get_ts_pro

    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    fetcher = getattr(pro, api_name, None)
    try:
        if callable(fetcher):
            df = call_tushare_api(api_name, fetcher, **kwargs)
        else:
            df = call_tushare_api(api_name, pro.query, api_name, **kwargs)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    return df.copy()


def normalize_date_range(trade_date: str, start_date: str, end_date: str, default_days: int = 30) -> tuple[str, str]:
    actual_start = trade_date or start_date
    actual_end = trade_date or end_date
    if not actual_start and not actual_end:
        actual_end = datetime.now().strftime(DATE_FORMAT)
        actual_start = (datetime.now() - timedelta(days=default_days)).strftime(DATE_FORMAT)
    elif not actual_start:
        actual_start = actual_end
    elif not actual_end:
        actual_end = actual_start
    return format_date_value(actual_start).replace("-", ""), format_date_value(actual_end).replace("-", "")


def normalize_period_range(report_period: str, start_period: str, end_period: str, default_years: int = 2) -> tuple[str, str]:
    actual_start = report_period or start_period
    actual_end = report_period or end_period
    if not actual_start and not actual_end:
        year = datetime.now().year
        actual_start = f"{year - default_years}0101"
        actual_end = f"{year}1231"
    elif not actual_start:
        actual_start = actual_end
    elif not actual_end:
        actual_end = actual_start
    return format_date_value(actual_start).replace("-", ""), format_date_value(actual_end).replace("-", "")


def plan_days(start_value: str, end_value: str) -> list[str]:
    if not start_value or not end_value:
        return []
    start_dt = datetime.strptime(start_value, DATE_FORMAT)
    end_dt = datetime.strptime(end_value, DATE_FORMAT)
    items: list[str] = []
    current = start_dt
    while current <= end_dt:
        items.append(current.strftime(DATE_FORMAT))
        current += timedelta(days=1)
    return items


def read_cached_ranges(
    namespace: list[str],
    identity: dict[str, str],
    column: str,
    start_value: str,
    end_value: str,
    unit: str,
    fetcher,
) -> pd.DataFrame:
    cache_path = build_cache_path("tushare", namespace, identity)
    cache_df = read_cache_frame(cache_path)
    fetched_frames: list[pd.DataFrame] = []
    for missing_start, missing_end in plan_missing_ranges(cache_df, column, start_value, end_value, unit):
        fetched_df = fetcher(missing_start, missing_end)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if cache_df.empty and not fetched_frames:
        fetched_df = fetcher(start_value, end_value)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if not fetched_frames:
        return cache_df
    merged_frame = pd.concat(fetched_frames, ignore_index=True)
    key_columns = [key for key in identity.keys() if key in merged_frame.columns or (not cache_df.empty and key in cache_df.columns)]
    key_columns.append(column)
    merged_cache = merge_cache_frame(cache_df, merged_frame, key_columns, [column])
    write_cache_frame(cache_path, merged_cache)
    return merged_cache


def read_cached_once(namespace: list[str], identity: dict[str, str], fetcher) -> pd.DataFrame:
    cache_path = build_cache_path("tushare", namespace, identity)
    cache_df = read_cache_frame(cache_path)
    if not cache_df.empty:
        return cache_df
    fetched_df = fetcher()
    if fetched_df.empty:
        return cache_df
    write_cache_frame(cache_path, fetched_df)
    return fetched_df


