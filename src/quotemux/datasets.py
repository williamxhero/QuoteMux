from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Union

import pandas as pd

from platform_models import IndexQuoteItem, StockQuoteItem
from quotemux.infra.common import normalize_index_code, normalize_stock_code
from quotemux.runtime_core.executor import SourceInstanceExecutor, run_fallback_chain_with_report
from quotemux.runtime_core.quality import summarize_minute_completeness, validate_quote_frame
from quotemux.markets import QuoteMuxMarkets
from quotemux.reports import ContractReport
from quotemux.requests.markets import TradingCalendarRequest
from quotemux.requests.datasets import IndexBar1dRequest, StockBar1mRequest, StockDailyOhlcvaRepairRequest
from quotemux.source_packages.registry import get_default_source_package_registry
from quotemux.settings import QuoteMuxSettings


CONTRACT_STOCK_INTRADAY = "stocks.quotes.intraday"
CONTRACT_STOCK_DAILY = "stocks.quotes.daily"
CONTRACT_INDEX_DAILY = "indexes.quotes.daily"


def _source_package_call(package_id: str, handler_name: str, *args: object) -> object:
    handler = get_default_source_package_registry().get_handler(package_id, handler_name)
    return handler(*args)


def _stock_items_to_frame(items: list[StockQuoteItem]) -> pd.DataFrame:
    if items == []:
        return pd.DataFrame(columns=["code", "bar_time", "trade_date", "open", "high", "low", "close", "volume", "amount"])
    frame = pd.DataFrame([item.model_dump() for item in items])
    frame["code"] = frame["code"].astype(str).str.zfill(6)
    frame["bar_time"] = pd.to_datetime(frame["trade_time"], errors="coerce")
    frame["trade_date"] = frame["bar_time"].dt.date
    for column in ["open", "high", "low", "close", "volume", "amount"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["bar_time"])
    frame = frame[["code", "bar_time", "trade_date", "open", "high", "low", "close", "volume", "amount"]]
    frame = frame.drop_duplicates(subset=["code", "bar_time"], keep="last")
    return frame.sort_values(["code", "bar_time"]).reset_index(drop=True)


def _index_items_to_frame(items: list[IndexQuoteItem]) -> pd.DataFrame:
    if items == []:
        return pd.DataFrame(columns=["index_code", "trade_date", "open", "high", "low", "close", "amount"])
    frame = pd.DataFrame([item.model_dump() for item in items])
    frame["trade_date"] = pd.to_datetime(frame["trade_time"], errors="coerce").dt.date
    for column in ["open", "high", "low", "close", "amount"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame = frame.dropna(subset=["trade_date"])
    frame = frame[["index_code", "trade_date", "open", "high", "low", "close", "amount"]]
    frame = frame.drop_duplicates(subset=["index_code", "trade_date"], keep="last")
    return frame.sort_values(["index_code", "trade_date"]).reset_index(drop=True)


def _trade_date_text(day: date) -> str:
    return day.strftime("%Y-%m-%d")


def _expected_trade_dates(start_date: date, end_date: date, settings: QuoteMuxSettings) -> set[date]:
    items = QuoteMuxMarkets(settings).get_trading_calendar(
        TradingCalendarRequest(exchange="SSE", start_date=_trade_date_text(start_date), end_date=_trade_date_text(end_date), is_open=True)
    )
    return {pd.to_datetime(item.trade_date).date() for item in items}


def _base_stock_items(code: str, base_df: pd.DataFrame) -> list[StockQuoteItem]:
    normalized_base = base_df.copy() if not base_df.empty else pd.DataFrame(columns=["bar_time", "open", "high", "low", "close", "volume", "amount"])
    if not normalized_base.empty:
        normalized_base["bar_time"] = pd.to_datetime(normalized_base["bar_time"], errors="coerce")
        normalized_base = normalized_base.dropna(subset=["bar_time"])
    base_items: list[StockQuoteItem] = []
    if not normalized_base.empty:
        for _, row in normalized_base.sort_values("bar_time").iterrows():
            base_items.append(
                StockQuoteItem(
                    code=str(code).zfill(6),
                    trade_time=str(pd.Timestamp(row["bar_time"]).strftime("%Y-%m-%d %H:%M:%S")),
                    freq="1m",
                    open=float(row["open"]) if pd.notna(row["open"]) else None,
                    high=float(row["high"]) if pd.notna(row["high"]) else None,
                    low=float(row["low"]) if pd.notna(row["low"]) else None,
                    close=float(row["close"]) if pd.notna(row["close"]) else None,
                    volume=float(row["volume"]) if pd.notna(row["volume"]) else None,
                    amount=float(row["amount"]) if pd.notna(row["amount"]) else None,
                    adjust="none",
                )
            )
    return base_items


def _base_index_items(index_code: str, base_df: pd.DataFrame) -> list[IndexQuoteItem]:
    normalized_base = base_df.copy() if not base_df.empty else pd.DataFrame(columns=["trade_date", "open", "high", "low", "close", "amount"])
    if not normalized_base.empty:
        normalized_base["trade_date"] = pd.to_datetime(normalized_base["trade_date"], errors="coerce").dt.date
        normalized_base = normalized_base.dropna(subset=["trade_date"])
    base_items: list[IndexQuoteItem] = []
    if not normalized_base.empty:
        for _, row in normalized_base.sort_values("trade_date").iterrows():
            base_items.append(
                IndexQuoteItem(
                    index_code=index_code,
                    trade_time=_trade_date_text(row["trade_date"]),
                    freq="1d",
                    open=float(row["open"]) if pd.notna(row["open"]) else None,
                    high=float(row["high"]) if pd.notna(row["high"]) else None,
                    low=float(row["low"]) if pd.notna(row["low"]) else None,
                    close=float(row["close"]) if pd.notna(row["close"]) else None,
                    amount=float(row["amount"]) if pd.notna(row["amount"]) else None,
                )
            )
    return base_items


class QuoteMuxDatasets:
    def __init__(self, settings: QuoteMuxSettings) -> None:
        self._settings = settings
        self._markets = QuoteMuxMarkets(settings)

    def get_open_trade_dates(self, start_dt: Union[date, datetime, str], end_dt: Union[date, datetime, str], exchange: str = "SSE") -> list[date]:
        start_date = pd.to_datetime(start_dt).date() if not isinstance(start_dt, str) else pd.to_datetime(start_dt).date()
        end_date = pd.to_datetime(end_dt).date() if not isinstance(end_dt, str) else pd.to_datetime(end_dt).date()
        items = self._markets.get_trading_calendar(
            TradingCalendarRequest(
                exchange=exchange,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                is_open=True,
            )
        )
        return [pd.to_datetime(item.trade_date).date() for item in items]

    def get_effective_end_trade_date(self, now: datetime | None = None, exchange: str = "SSE", cutoff_hour: int = 16) -> date:
        current = now or datetime.now()
        today = current.date()
        lookback_start = today - timedelta(days=120)
        open_dates = self.get_open_trade_dates(lookback_start, today, exchange=exchange)
        if not open_dates:
            return today
        if today in open_dates and current.hour >= cutoff_hour:
            return today
        previous_open_dates = [item for item in open_dates if item < today]
        if previous_open_dates:
            return previous_open_dates[-1]
        return open_dates[0]

    def fetch_stock_minute_bars_seed(self, code: str, start_date: date, end_date: date) -> pd.DataFrame:
        if start_date > end_date:
            return pd.DataFrame(columns=["bar_time", "open", "high", "low", "close", "volume", "amount"])
        handlers = {
            "get_stock_quotes": lambda instance: lambda codes, start_text, end_text: _source_package_call(instance.package_id, "get_stock_quotes", codes, "1m", "", start_text, end_text, "", "", None, "none"),
        }
        items, _ = run_fallback_chain_with_report(
            CONTRACT_STOCK_INTRADAY,
            [],
            ("code", "trade_time", "freq"),
            lambda current_items: [([normalize_stock_code(code)], _trade_date_text(start_date), _trade_date_text(end_date))] if current_items == [] else [],
            SourceInstanceExecutor(self._settings).build_steps(CONTRACT_STOCK_INTRADAY, handlers, ("opentdx", "efinance", "mootdx", "akshare")),
            self._settings.get_contract_source_order(CONTRACT_STOCK_INTRADAY, ("opentdx", "efinance", "mootdx", "akshare")),
        )
        frame = _stock_items_to_frame(items)
        if frame.empty:
            return pd.DataFrame(columns=["bar_time", "open", "high", "low", "close", "volume", "amount"])
        frame = frame[(frame["trade_date"] >= start_date) & (frame["trade_date"] <= end_date)].copy()
        return frame[["bar_time", "open", "high", "low", "close", "volume", "amount"]].reset_index(drop=True)

    def fetch_index_daily_bars_seed(self, index_code: str, start_date: date, end_date: date) -> pd.DataFrame:
        if start_date > end_date:
            return pd.DataFrame(columns=["trade_date", "open", "high", "low", "close", "amount"])
        handlers = {
            "get_index_quotes": lambda instance: lambda index_codes, start_text, end_text: _source_package_call(instance.package_id, "get_index_quotes", index_codes, "1d", "", start_text, end_text, None),
        }
        items, _ = run_fallback_chain_with_report(
            CONTRACT_INDEX_DAILY,
            [],
            ("index_code", "trade_time", "freq"),
            lambda current_items: [([normalize_index_code(index_code)], _trade_date_text(start_date), _trade_date_text(end_date))] if current_items == [] else [],
            SourceInstanceExecutor(self._settings).build_steps(CONTRACT_INDEX_DAILY, handlers, ("akshare", "mootdx", "opentdx")),
            self._settings.get_contract_source_order(CONTRACT_INDEX_DAILY, ("akshare", "mootdx", "opentdx")),
        )
        frame = _index_items_to_frame(items)
        if frame.empty:
            return pd.DataFrame(columns=["trade_date", "open", "high", "low", "close", "amount"])
        frame = frame[(frame["trade_date"] >= start_date) & (frame["trade_date"] <= end_date)].copy()
        return frame[["trade_date", "open", "high", "low", "close", "amount"]].reset_index(drop=True)

    def get_stock_bar_1m(self, request: StockBar1mRequest, base_df: pd.DataFrame) -> tuple[pd.DataFrame, ContractReport]:
        base_items = _base_stock_items(request.code, base_df)
        expected_dates = _expected_trade_dates(request.start_date, request.end_date, self._settings)

        def _needs_more(items: list[StockQuoteItem]) -> bool:
            if items == []:
                return True
            frame = _stock_items_to_frame(items)
            if frame.empty:
                return True
            actual_dates = set(frame["trade_date"].dropna().tolist())
            if not expected_dates.issubset(actual_dates):
                return True
            latest_trade_date = max(expected_dates) if expected_dates else request.end_date
            completeness = summarize_minute_completeness(frame[frame["trade_date"] == latest_trade_date], latest_trade_date)
            return completeness["missing_bar_count"] > 0

        handlers = {
            "get_stock_quotes": lambda instance: lambda codes, start_text, end_text: _source_package_call(instance.package_id, "get_stock_quotes", codes, "1m", "", start_text, end_text, "", "", None, "none"),
        }
        merged_items, fallback_report = run_fallback_chain_with_report(
            CONTRACT_STOCK_INTRADAY,
            base_items,
            ("code", "trade_time", "freq"),
            lambda items: [([str(request.code).zfill(6)], _trade_date_text(request.start_date), _trade_date_text(request.end_date))] if _needs_more(items) else [],
            SourceInstanceExecutor(self._settings).build_steps(CONTRACT_STOCK_INTRADAY, handlers, ("opentdx", "efinance", "mootdx", "akshare")),
            self._settings.get_contract_source_order(CONTRACT_STOCK_INTRADAY, ("opentdx", "efinance", "mootdx", "akshare")),
        )
        out = _stock_items_to_frame(merged_items)
        out = out[["bar_time", "open", "high", "low", "close", "volume", "amount"]]
        quality = validate_quote_frame(out.rename(columns={"bar_time": "trade_time"}), ["trade_time"], "trade_time")
        completeness = summarize_minute_completeness(_stock_items_to_frame(merged_items), request.end_date)
        report = ContractReport.from_fallback_report(CONTRACT_STOCK_INTRADAY, fallback_report, "seed", base_items != [])
        return out, ContractReport(
            contract_name=report.contract_name,
            profile_id=report.profile_id,
            profile_version=report.profile_version,
            source_hit_counts=report.source_hit_counts,
            source_request_counts=report.source_request_counts,
            source_instance_reports=report.source_instance_reports,
            source_error_count=report.source_error_count,
            source_skipped_count=report.source_skipped_count,
            conflict_count=report.conflict_count + int(quality["duplicate_key_count"]),
            quarantine_count=report.quarantine_count,
            degraded=report.degraded or completeness["missing_bar_count"] > 0,
        )

    def get_index_bar_1d(self, request: IndexBar1dRequest, base_df: pd.DataFrame) -> tuple[pd.DataFrame, ContractReport]:
        base_items = _base_index_items(request.index_code, base_df)
        expected_dates = _expected_trade_dates(request.start_date, request.end_date, self._settings)

        def _needs_more(items: list[IndexQuoteItem]) -> bool:
            if items == []:
                return True
            frame = _index_items_to_frame(items)
            actual_dates = set(frame["trade_date"].dropna().tolist())
            return not expected_dates.issubset(actual_dates)

        handlers = {
            "get_index_quotes": lambda instance: lambda index_codes, start_text, end_text: _source_package_call(instance.package_id, "get_index_quotes", index_codes, "1d", "", start_text, end_text, None),
        }
        merged_items, fallback_report = run_fallback_chain_with_report(
            CONTRACT_INDEX_DAILY,
            base_items,
            ("index_code", "trade_time", "freq"),
            lambda items: [([request.index_code], _trade_date_text(request.start_date), _trade_date_text(request.end_date))] if _needs_more(items) else [],
            SourceInstanceExecutor(self._settings).build_steps(CONTRACT_INDEX_DAILY, handlers, ("akshare", "mootdx", "opentdx")),
            self._settings.get_contract_source_order(CONTRACT_INDEX_DAILY, ("akshare", "mootdx", "opentdx")),
        )
        out = _index_items_to_frame(merged_items)
        quality = validate_quote_frame(out.rename(columns={"trade_date": "trade_time"}), ["index_code", "trade_time"], "trade_time")
        report = ContractReport.from_fallback_report(CONTRACT_INDEX_DAILY, fallback_report, "seed", base_items != [])
        return out, ContractReport(
            contract_name=report.contract_name,
            profile_id=report.profile_id,
            profile_version=report.profile_version,
            source_hit_counts=report.source_hit_counts,
            source_request_counts=report.source_request_counts,
            source_instance_reports=report.source_instance_reports,
            source_error_count=report.source_error_count,
            source_skipped_count=report.source_skipped_count,
            conflict_count=report.conflict_count + int(quality["duplicate_key_count"]),
            quarantine_count=report.quarantine_count,
            degraded=report.degraded,
        )

    def repair_stock_daily_ohlcva(self, request: StockDailyOhlcvaRepairRequest, df_day: pd.DataFrame) -> tuple[pd.DataFrame, ContractReport]:
        if df_day.empty:
            return df_day, ContractReport.empty(CONTRACT_STOCK_DAILY, "seed", False)
        work = df_day.copy()
        quote_columns = ["open", "high", "low", "close", "volume", "amount"]
        active_mask = ~work["is_suspended"].fillna(False).astype(bool)
        missing_mask = active_mask & work[quote_columns].isna().any(axis=1)
        missing_codes = work.loc[missing_mask, "code"].astype(str).str.zfill(6).tolist()
        if missing_codes == []:
            return work, ContractReport.empty(CONTRACT_STOCK_DAILY, "seed", True)
        base_items: list[StockQuoteItem] = []
        existing_rows = work.loc[~missing_mask, ["code", *quote_columns]].copy()
        for _, row in existing_rows.iterrows():
            base_items.append(
                StockQuoteItem(
                    code=str(row["code"]).zfill(6),
                    trade_time=_trade_date_text(request.trade_date),
                    freq="1d",
                    open=float(row["open"]) if pd.notna(row["open"]) else None,
                    high=float(row["high"]) if pd.notna(row["high"]) else None,
                    low=float(row["low"]) if pd.notna(row["low"]) else None,
                    close=float(row["close"]) if pd.notna(row["close"]) else None,
                    volume=float(row["volume"]) if pd.notna(row["volume"]) else None,
                    amount=float(row["amount"]) if pd.notna(row["amount"]) else None,
                    adjust="none",
                )
            )

        def _remaining_missing_codes(items: list[StockQuoteItem]) -> list[str]:
            if items == []:
                return missing_codes
            frame = _stock_items_to_frame(items)
            if frame.empty:
                return missing_codes
            frame = frame[frame["trade_date"] == request.trade_date]
            if frame.empty:
                return missing_codes
            available = frame[["code", *quote_columns]].drop_duplicates(subset=["code"], keep="last")
            ready_codes = set(available.loc[available[quote_columns].notna().all(axis=1), "code"].tolist())
            return [code for code in missing_codes if code not in ready_codes]

        handlers = {
            "get_stock_quotes": lambda instance: lambda codes, start_text, end_text: _source_package_call(instance.package_id, "get_stock_quotes", codes, "1d", start_text, "", "", "", "", None, "none"),
        }
        merged_items, fallback_report = run_fallback_chain_with_report(
            CONTRACT_STOCK_DAILY,
            base_items,
            ("code", "trade_time", "freq"),
            lambda items: [(_remaining_missing_codes(items), _trade_date_text(request.trade_date), _trade_date_text(request.trade_date))] if _remaining_missing_codes(items) else [],
            SourceInstanceExecutor(self._settings).build_steps(CONTRACT_STOCK_DAILY, handlers, ("tushare", "efinance", "mootdx", "akshare")),
            self._settings.get_contract_source_order(CONTRACT_STOCK_DAILY, ("tushare", "efinance", "mootdx", "akshare")),
        )
        filled_frame = _stock_items_to_frame(merged_items)
        if not filled_frame.empty:
            filled_frame = filled_frame[filled_frame["trade_date"] == request.trade_date]
            filled_frame = filled_frame[["code", *quote_columns]].drop_duplicates(subset=["code"], keep="last")
            work["code"] = work["code"].astype(str).str.zfill(6)
            work = work.merge(filled_frame, on="code", how="left", suffixes=("", "_fallback"))
            for column in quote_columns:
                fallback_column = f"{column}_fallback"
                work[column] = work[column].where(work[column].notna(), work[fallback_column])
                work = work.drop(columns=[fallback_column])
        report = ContractReport.from_fallback_report(CONTRACT_STOCK_DAILY, fallback_report, "seed", base_items != [])
        return work, report



