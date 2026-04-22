from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path

import pandas as pd

from quotemux.infra.config import DATE_FORMAT, DATETIME_FORMAT


INTRADAY_RULES = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "60m": "60min",
}
PRICE_COLUMNS = ("open", "high", "low", "close")
PUBLIC_DATE_FORMAT = "%Y-%m-%d"
PUBLIC_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def parse_date_text(value: str) -> date | None:
    if not value:
        return None
    if "-" in value:
        return datetime.strptime(value, "%Y-%m-%d").date()
    return datetime.strptime(value, DATE_FORMAT).date()


def parse_datetime_text(value: str) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if "T" in text:
        return datetime.fromisoformat(text)
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, DATE_FORMAT)
    if "-" in text and len(text) == 10:
        return datetime.strptime(text, "%Y-%m-%d")
    if "-" in text:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    return datetime.strptime(text, DATETIME_FORMAT)


def format_date_value(value: object) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    if isinstance(value, pd.Timestamp):
        return value.strftime(PUBLIC_DATE_FORMAT)
    if isinstance(value, datetime):
        return value.strftime(PUBLIC_DATE_FORMAT)
    if isinstance(value, date):
        return value.strftime(PUBLIC_DATE_FORMAT)
    text = str(value)
    if not text:
        return ""
    try:
        parsed = parse_date_text(text)
    except Exception:
        return text
    return parsed.strftime(PUBLIC_DATE_FORMAT)


def format_datetime_value(value: object, freq: str) -> str:
    if value is None:
        return ""
    if freq in {"1d", "1w", "1mo"}:
        return format_date_value(value)
    if isinstance(value, pd.Timestamp):
        return value.strftime(PUBLIC_DATETIME_FORMAT)
    if isinstance(value, datetime):
        return value.strftime(PUBLIC_DATETIME_FORMAT)
    text = str(value)
    if not text:
        return ""
    try:
        parsed = parse_datetime_text(text)
    except Exception:
        return text
    return parsed.strftime(PUBLIC_DATETIME_FORMAT)


def split_csv(value: str) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def normalize_stock_code(code: str) -> str:
    text = code.strip().upper()
    if not text:
        return ""
    if "." in text:
        left, right = text.split(".", 1)
        if left in {"SHSE", "SZSE", "BJSE"}:
            return right
        return left
    return text


def normalize_index_code(code: str) -> str:
    text = code.strip().upper()
    if not text:
        return ""
    if "." in text:
        left, right = text.split(".", 1)
        if left in {"SHSE", "SZSE", "BJSE"}:
            return right
        return left
    return text


def index_code_to_ts(code: str) -> str:
    text = code.strip().upper()
    if not text:
        return ""
    if "." in text:
        left, right = text.split(".", 1)
        if left in {"SHSE", "SZSE"}:
            suffix = "SH" if left == "SHSE" else "SZ"
            return f"{right}.{suffix}"
        return text
    normalized = normalize_index_code(text)
    if normalized.startswith("399"):
        return f"{normalized}.SZ"
    if normalized.startswith("8"):
        return f"{normalized}.SI"
    return f"{normalized}.SH"


def index_code_to_gm(code: str) -> str:
    text = code.strip().upper()
    if not text:
        return ""
    if "." in text:
        left, right = text.split(".", 1)
        if right in {"SH", "SZ", "SI"}:
            prefix = "SZSE" if right == "SZ" else "SHSE"
            return f"{prefix}.{left}"
        return text
    normalized = normalize_index_code(text)
    if normalized.startswith("399"):
        return f"SZSE.{normalized}"
    return f"SHSE.{normalized}"


def stock_code_to_ts(code: str) -> str:
    normalized = normalize_stock_code(code)
    if not normalized:
        return ""
    if normalized.startswith(("4", "8")):
        return f"{normalized}.BJ"
    if normalized.startswith(("5", "6", "9")):
        return f"{normalized}.SH"
    return f"{normalized}.SZ"


def stock_code_to_gm(code: str) -> str:
    normalized = normalize_stock_code(code)
    if not normalized:
        return ""
    if normalized.startswith(("4", "8")):
        return f"BJSE.{normalized}"
    if normalized.startswith(("5", "6", "9")):
        return f"SHSE.{normalized}"
    return f"SZSE.{normalized}"


def stock_market_name(code: str) -> str:
    normalized = normalize_stock_code(code)
    if normalized.startswith(("4", "8")):
        return "BJSE"
    if normalized.startswith(("5", "6", "9")):
        return "SHSE"
    return "SZSE"


@lru_cache(maxsize=256)
def read_parquet_cached(path_text: str) -> pd.DataFrame:
    return pd.read_parquet(Path(path_text))


@lru_cache(maxsize=64)
def read_json_cached(path_text: str) -> dict[str, object]:
    path = Path(path_text)
    return json.loads(path.read_text(encoding="utf-8"))


def build_time_bounds(
    trade_date: str,
    start_date: str,
    end_date: str,
    start_time: str,
    end_time: str,
    count: int | None,
    intraday: bool,
) -> tuple[datetime | None, datetime | None]:
    if trade_date:
        day = parse_date_text(trade_date)
        return datetime.combine(day, datetime.min.time()), datetime.combine(day, datetime.max.time())
    start_dt = parse_datetime_text(start_time)
    end_dt = parse_datetime_text(end_time)
    if start_dt or end_dt:
        return start_dt, end_dt
    start_day = parse_date_text(start_date)
    end_day = parse_date_text(end_date)
    if start_day or end_day:
        start_value = datetime.combine(start_day, datetime.min.time()) if start_day else None
        end_value = datetime.combine(end_day, datetime.max.time()) if end_day else None
        return start_value, end_value
    if count:
        end_value = datetime.now()
        delta_days = 30 if intraday else 400
        return end_value - timedelta(days=delta_days), end_value
    return None, None


def aggregate_ohlc(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    if df.empty:
        return df
    if freq == "1m":
        return df.copy()
    work = df.sort_values("trade_time").set_index("trade_time")
    if freq in INTRADAY_RULES and freq != "1m":
        rule = INTRADAY_RULES[freq]
    elif freq == "1d":
        rule = "1D"
    elif freq == "1w":
        rule = "W-FRI"
    elif freq == "1mo":
        rule = "ME"
    else:
        return pd.DataFrame()
    grouped = work.resample(rule, label="left", closed="left").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "amount": "sum",
        }
    )
    return grouped[grouped["close"].notna()].reset_index()


def add_quote_metrics(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.sort_values("trade_time").copy()
    work["pre_close"] = work["close"].shift(1)
    work["change"] = work["close"] - work["pre_close"]
    work["pct_chg"] = work["change"] / work["pre_close"] * 100
    return work


