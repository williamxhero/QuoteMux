from __future__ import annotations

from datetime import date, datetime
import math

import pandas as pd

from quotemux.infra.common import normalize_index_code, normalize_stock_code


EXPECTED_MINUTE_BAR_COUNT = 242


def normalize_index_code_full(code: str) -> str:
    text = code.strip().upper()
    if not text:
        return ""
    if "." in text:
        left, right = text.split(".", 1)
        if left in {"SHSE", "SZSE", "BJSE"}:
            return f"{left}.{right}"
        if right in {"SH", "SZ", "BJ"}:
            exchange = {"SH": "SHSE", "SZ": "SZSE", "BJ": "BJSE"}[right]
            return f"{exchange}.{left}"
        return text
    normalized = normalize_index_code(text)
    if normalized.startswith("399"):
        return f"SZSE.{normalized}"
    if normalized.startswith(("43", "83", "87")):
        return f"BJSE.{normalized}"
    return f"SHSE.{normalized}"


def normalize_index_provider_code(code: str) -> str:
    normalized = normalize_index_code_full(code)
    if normalized == "":
        return ""
    return normalized.split(".", 1)[1]


def normalize_stock_provider_code(code: str) -> str:
    return normalize_stock_code(code).zfill(6)


def build_akshare_index_symbol(code: str) -> str:
    full_code = normalize_index_code_full(code)
    if full_code == "":
        return ""
    exchange, bare_code = full_code.split(".", 1)
    prefix = {"SHSE": "sh", "SZSE": "sz", "BJSE": "bj"}.get(exchange, "sh")
    return f"{prefix}{bare_code}"


def validate_quote_frame(df: pd.DataFrame, key_columns: list[str], time_column: str) -> dict[str, int]:
    if df.empty:
        return {
            "row_count": 0,
            "duplicate_key_count": 0,
            "invalid_ohlc_count": 0,
            "negative_volume_count": 0,
            "negative_amount_count": 0,
            "missing_time_count": 0,
        }
    work = df.copy()
    duplicate_key_count = int(work.duplicated(subset=key_columns, keep=False).sum())
    missing_time_count = int(pd.to_datetime(work[time_column], errors="coerce").isna().sum())
    open_value = pd.to_numeric(work["open"], errors="coerce")
    high_value = pd.to_numeric(work["high"], errors="coerce")
    low_value = pd.to_numeric(work["low"], errors="coerce")
    close_value = pd.to_numeric(work["close"], errors="coerce")
    invalid_ohlc_mask = (
        open_value.notna()
        & high_value.notna()
        & low_value.notna()
        & close_value.notna()
        & ((high_value < open_value) | (high_value < close_value) | (low_value > open_value) | (low_value > close_value))
    )
    volume_value = pd.to_numeric(work.get("volume"), errors="coerce")
    amount_value = pd.to_numeric(work.get("amount"), errors="coerce")
    return {
        "row_count": int(len(work)),
        "duplicate_key_count": duplicate_key_count,
        "invalid_ohlc_count": int(invalid_ohlc_mask.sum()),
        "negative_volume_count": int((volume_value < 0).sum()),
        "negative_amount_count": int((amount_value < 0).sum()),
        "missing_time_count": missing_time_count,
    }


def calibrate_quote_units(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, float]]:
    if df.empty or "volume" not in df.columns or "amount" not in df.columns:
        return df, {"volume_factor": 1.0, "amount_factor": 1.0}
    work = df.copy()
    work["volume"] = pd.to_numeric(work["volume"], errors="coerce")
    work["amount"] = pd.to_numeric(work["amount"], errors="coerce")
    price_reference = pd.to_numeric(work["close"], errors="coerce")
    valid_mask = work["volume"].gt(0) & work["amount"].gt(0) & price_reference.gt(0)
    if not bool(valid_mask.any()):
        return work, {"volume_factor": 1.0, "amount_factor": 1.0}
    volume_candidates = (1.0, 0.01, 0.1, 10.0, 100.0)
    amount_candidates = (1.0, 0.01, 0.1, 10.0, 100.0, 1000.0)
    target = price_reference.loc[valid_mask] * 100.0
    best_score = math.inf
    best_volume_factor = 1.0
    best_amount_factor = 1.0
    volume_series = work.loc[valid_mask, "volume"]
    amount_series = work.loc[valid_mask, "amount"]
    for volume_factor in volume_candidates:
        scaled_volume = volume_series * volume_factor
        if not bool(scaled_volume.gt(0).all()):
            continue
        for amount_factor in amount_candidates:
            ratio = amount_series * amount_factor / scaled_volume
            if not bool(ratio.gt(0).all()):
                continue
            score = float((ratio / target - 1.0).abs().median())
            if score < best_score:
                best_score = score
                best_volume_factor = volume_factor
                best_amount_factor = amount_factor
    work["volume"] = work["volume"] * best_volume_factor
    work["amount"] = work["amount"] * best_amount_factor
    return work, {"volume_factor": best_volume_factor, "amount_factor": best_amount_factor}


def summarize_minute_completeness(df: pd.DataFrame, trade_date: date) -> dict[str, int]:
    if df.empty or "bar_time" not in df.columns:
        return {
            "expected_bar_count": EXPECTED_MINUTE_BAR_COUNT,
            "actual_bar_count": 0,
            "missing_bar_count": EXPECTED_MINUTE_BAR_COUNT,
        }
    times = pd.to_datetime(df["bar_time"], errors="coerce")
    actual_bar_count = int((times.dt.date == trade_date).sum())
    return {
        "expected_bar_count": EXPECTED_MINUTE_BAR_COUNT,
        "actual_bar_count": actual_bar_count,
        "missing_bar_count": max(0, EXPECTED_MINUTE_BAR_COUNT - actual_bar_count),
    }

