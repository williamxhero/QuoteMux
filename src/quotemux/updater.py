from __future__ import annotations

from datetime import date, datetime, time, timedelta
from functools import lru_cache
from typing import Union

import pandas as pd

from platform_models import IndexQuoteItem, StockQuoteItem
from quotemux.infra.common import index_code_to_ts, normalize_index_code, normalize_stock_code, stock_code_to_ts
from quotemux.runtime_core.executor import ProviderStep, run_fallback_chain_with_report
from quotemux.runtime_core.quality import summarize_minute_completeness, validate_quote_frame
from quotemux.markets import QuoteMuxMarkets
from quotemux.reports import ContractReport
from quotemux.requests.markets import TradingCalendarRequest
from quotemux.requests.updater import IndexBar1dRequest, StockBar1mRequest, StockDailyOhlcvaRepairRequest
from quotemux.runtime_core.registry import SourceProxy
from quotemux.settings import QuoteMuxSettings
from quotemux.sources.tushare.source import call_tushare_api, get_ts_pro

try:
    from opentdx import MARKET, PERIOD, TdxClient
except Exception:
    MARKET = None
    PERIOD = None
    TdxClient = None


DEFAULT_TS_DAILY_CLOSE_TIME = time(15, 0)
akshare_provider = SourceProxy("akshare")
datalake_reference = SourceProxy("datalake_reference")
efinance_provider = SourceProxy("efinance")
mootdx_provider = SourceProxy("mootdx")
opentdx_provider = SourceProxy("opentdx")
tushare_provider = SourceProxy("tushare")
INDEX_NAME_MAP = {
    "SHSE.000001": "涓婅瘉鎸囨暟",
    "SZSE.399107": "娣辫瘉锛℃寚",
    "SHSE.000680": "绉戝垱50",
    "SZSE.399006": "鍒涗笟鏉挎寚",
}
INDEX_KLINE_CODE_MAP = {
    "SHSE.000001": "999999",
    "SZSE.399107": "399107",
    "SHSE.000680": "000680",
    "SZSE.399006": "399006",
}


def _normalize_datetime(frame: pd.DataFrame, column_name: str) -> pd.DataFrame:
    if frame.empty:
        return frame
    out = frame.copy()
    out[column_name] = pd.to_datetime(out[column_name], errors="coerce")
    try:
        if getattr(out[column_name].dt, "tz", None) is not None:
            out[column_name] = out[column_name].dt.tz_localize(None)
    except Exception:
        pass
    return out.dropna(subset=[column_name])


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


def _to_yyyymmdd(value: Union[str, date, datetime, None]) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        text = value.strip()
        if text == "":
            return ""
        if len(text) == 8 and text.isdigit():
            return text
        try:
            return pd.to_datetime(text).strftime("%Y%m%d")
        except Exception:
            return text
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    return value.strftime("%Y%m%d")


def _ts_code_to_market(ts_code: str) -> str:
    suffix = str(ts_code).split(".")[-1]
    if suffix == "SH":
        return "SHSE"
    if suffix == "SZ":
        return "SZSE"
    if suffix == "BJ":
        return "BJSE"
    return ""


def _map_stock_board_type(market_name: str) -> str:
    text = str(market_name)
    if text == "涓绘澘":
        return "ZB"
    if text == "创业板":
        return "CYB"
    if text == "科创板":
        return "KCB"
    if text == "鍖椾氦鎵€":
        return "BSE"
    return ""


def _normalize_stock_basic_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["market", "code", "name", "board_type", "listed_date", "delisted_date", "is_active"])
    out = df.copy()
    out["board_market_name"] = out["market"].astype(str)
    out["market"] = out["ts_code"].map(_ts_code_to_market)
    out["code"] = out["symbol"].astype(str).str.zfill(6)
    out["name"] = out["name"].astype(str)
    out["board_type"] = out["board_market_name"].map(_map_stock_board_type)
    out["listed_date"] = pd.to_datetime(out["list_date"], format="%Y%m%d", errors="coerce").dt.date
    out["delisted_date"] = pd.to_datetime(out["delist_date"], format="%Y%m%d", errors="coerce").dt.date
    out["is_active"] = out["list_status"].astype(str) == "L"
    out = out[["market", "code", "name", "board_type", "listed_date", "delisted_date", "is_active"]]
    out = out.dropna(subset=["market", "listed_date"])
    out = out[out["code"] != "000000"]
    out = out.drop_duplicates(subset=["market", "code"], keep="last")
    return out.sort_values(["market", "code"]).reset_index(drop=True)


def _normalize_kline_records(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=["bar_time", "open", "high", "low", "close", "volume", "amount"])
    out = pd.DataFrame(records)
    if out.empty:
        return pd.DataFrame(columns=["bar_time", "open", "high", "low", "close", "volume", "amount"])
    time_column = "date_time" if "date_time" in out.columns else "datetime"
    out["bar_time"] = pd.to_datetime(out[time_column], errors="coerce")
    try:
        if getattr(out["bar_time"].dt, "tz", None) is not None:
            out["bar_time"] = out["bar_time"].dt.tz_localize(None)
    except Exception:
        pass
    out["open"] = pd.to_numeric(out["open"], errors="coerce")
    out["high"] = pd.to_numeric(out["high"], errors="coerce")
    out["low"] = pd.to_numeric(out["low"], errors="coerce")
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out["volume"] = pd.to_numeric(out["vol"], errors="coerce")
    out["amount"] = pd.to_numeric(out["amount"], errors="coerce")
    out = out[["bar_time", "open", "high", "low", "close", "volume", "amount"]]
    out = out.dropna(subset=["bar_time"])
    out = out.drop_duplicates(subset=["bar_time"], keep="last")
    return out.sort_values("bar_time").reset_index(drop=True)


def _quote_market_from_index_code(index_code: str):
    if MARKET is None:
        raise RuntimeError("缂哄皯 OpenTDX 渚濊禆")
    if index_code.startswith("SHSE."):
        return MARKET.SH
    if index_code.startswith("SZSE."):
        return MARKET.SZ
    if index_code.startswith("BJSE."):
        return MARKET.BJ
    raise ValueError(f"涓嶆敮鎸佺殑鎸囨暟浠ｇ爜: {index_code}")


def _stock_market_from_code(code: str):
    if MARKET is None:
        raise RuntimeError("缂哄皯 OpenTDX 渚濊禆")
    normalized = str(code).zfill(6)
    if normalized.startswith(("4", "8")):
        return MARKET.BJ
    if normalized.startswith(("5", "6", "9")):
        return MARKET.SH
    return MARKET.SZ


def _call_opentdx(callback):
    if TdxClient is None:
        raise RuntimeError("缂哄皯 OpenTDX 渚濊禆")
    with TdxClient() as client:
        return callback(client)


def _expected_trade_dates(start_date: date, end_date: date) -> set[date]:
    items = datalake_reference.get_trading_calendar("SSE", _trade_date_text(start_date), _trade_date_text(end_date), True)
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


class QuoteMuxUpdater:
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

    def fetch_stock_basic_snapshot(self, list_status: str = "L") -> pd.DataFrame:
        pro = get_ts_pro()
        if pro is None:
            return pd.DataFrame(columns=["market", "code", "name", "board_type", "listed_date", "delisted_date", "is_active"])
        df = call_tushare_api(
            "stock_basic",
            pro.stock_basic,
            exchange="",
            list_status=list_status,
            fields="ts_code,symbol,name,market,list_date,delist_date,list_status",
        )
        return _normalize_stock_basic_snapshot(df)

    def fetch_stock_basic_snapshot_by_statuses(self, list_statuses: tuple[str, ...]) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for list_status in list_statuses:
            normalized = self.fetch_stock_basic_snapshot(list_status)
            if not normalized.empty:
                frames.append(normalized)
        if not frames:
            return pd.DataFrame(columns=["market", "code", "name", "board_type", "listed_date", "delisted_date", "is_active"])
        out = pd.concat(frames, ignore_index=True)
        out = out.drop_duplicates(subset=["market", "code"], keep="last")
        return out.sort_values(["market", "code"]).reset_index(drop=True)

    def fetch_name_change_history(self, code: str) -> pd.DataFrame:
        pro = get_ts_pro()
        ts_code = stock_code_to_ts(code)
        if pro is None or ts_code == "":
            return pd.DataFrame(columns=["code", "date_start", "date_end", "name"])
        df = call_tushare_api(
            "namechange",
            pro.namechange,
            ts_code=ts_code,
            fields="ts_code,name,start_date,end_date,ann_date,change_reason",
        )
        if df is None or df.empty:
            return pd.DataFrame(columns=["code", "date_start", "date_end", "name"])
        out = df.copy()
        out["code"] = str(code).zfill(6)
        out["date_start"] = pd.to_datetime(out["start_date"], format="%Y%m%d", errors="coerce")
        out["date_end"] = pd.to_datetime(out["end_date"], format="%Y%m%d", errors="coerce")
        out["name"] = out["name"].astype(str)
        out = out[["code", "date_start", "date_end", "name"]]
        out = out.dropna(subset=["date_start"])
        out = out[out["name"] != ""]
        out = out.drop_duplicates(subset=["code", "date_start", "name"], keep="last")
        return out.sort_values(["code", "date_start"]).reset_index(drop=True)

    def fetch_ths_index(self, index_type: str = "N") -> pd.DataFrame:
        pro = get_ts_pro()
        if pro is None:
            return pd.DataFrame(columns=["ts_code", "name", "count", "list_date", "type"])
        df = call_tushare_api("ths_index", pro.ths_index, exchange="A", type=index_type)
        if df is None or df.empty:
            return pd.DataFrame(columns=["ts_code", "name", "count", "list_date", "type"])
        out = df.copy()
        out["list_date"] = pd.to_datetime(out["list_date"], format="%Y%m%d", errors="coerce")
        return out[["ts_code", "name", "count", "list_date", "type"]]

    def fetch_ths_member(self, ts_code: str) -> pd.DataFrame:
        pro = get_ts_pro()
        if pro is None:
            return pd.DataFrame(columns=["ts_code", "con_code", "con_name", "in_date", "out_date"])
        df = call_tushare_api("ths_member", pro.ths_member, ts_code=ts_code)
        if df is None or df.empty:
            return pd.DataFrame(columns=["ts_code", "con_code", "con_name", "in_date", "out_date"])
        out = df.copy()
        out["in_date"] = pd.to_datetime(out["in_date"], format="%Y%m%d", errors="coerce") if "in_date" in out.columns else pd.NaT
        out["out_date"] = pd.to_datetime(out["out_date"], format="%Y%m%d", errors="coerce") if "out_date" in out.columns else pd.NaT
        return out

    def fetch_dc_index(
        self,
        trade_date: Union[date, datetime, str, None] = None,
        start_date: Union[date, datetime, str, None] = None,
        end_date: Union[date, datetime, str, None] = None,
    ) -> pd.DataFrame:
        pro = get_ts_pro()
        if pro is None:
            return pd.DataFrame(columns=["ts_code", "name", "trade_date", "idx_type", "level"])
        kwargs: dict[str, str] = {}
        trade_date_text = _to_yyyymmdd(trade_date)
        start_date_text = _to_yyyymmdd(start_date)
        end_date_text = _to_yyyymmdd(end_date)
        if trade_date_text != "":
            kwargs["trade_date"] = trade_date_text
        if start_date_text != "":
            kwargs["start_date"] = start_date_text
        if end_date_text != "":
            kwargs["end_date"] = end_date_text
        df = call_tushare_api("dc_index", pro.dc_index, **kwargs, fields="ts_code,name,trade_date,idx_type,level")
        if df is None or df.empty:
            return pd.DataFrame(columns=["ts_code", "name", "trade_date", "idx_type", "level"])
        out = df.copy()
        out["ts_code"] = out["ts_code"].astype(str)
        out["name"] = out["name"].astype(str)
        out["trade_date"] = pd.to_datetime(out["trade_date"], format="%Y%m%d", errors="coerce")
        out["idx_type"] = out["idx_type"].fillna("").astype(str)
        out["level"] = out["level"].fillna("").astype(str)
        out = out.dropna(subset=["ts_code", "name"])
        out = out.drop_duplicates(subset=["ts_code", "trade_date"], keep="last")
        return out[["ts_code", "name", "trade_date", "idx_type", "level"]].reset_index(drop=True)

    def fetch_dc_member(
        self,
        ts_code: str = "",
        con_code: str = "",
        trade_date: Union[date, datetime, str, None] = None,
        start_date: Union[date, datetime, str, None] = None,
        end_date: Union[date, datetime, str, None] = None,
    ) -> pd.DataFrame:
        pro = get_ts_pro()
        if pro is None:
            return pd.DataFrame(columns=["trade_date", "ts_code", "con_code", "name"])
        kwargs: dict[str, str] = {}
        if ts_code != "":
            kwargs["ts_code"] = ts_code
        if con_code != "":
            kwargs["con_code"] = con_code
        trade_date_text = _to_yyyymmdd(trade_date)
        start_date_text = _to_yyyymmdd(start_date)
        end_date_text = _to_yyyymmdd(end_date)
        if trade_date_text != "":
            kwargs["trade_date"] = trade_date_text
        if start_date_text != "":
            kwargs["start_date"] = start_date_text
        if end_date_text != "":
            kwargs["end_date"] = end_date_text
        df = call_tushare_api("dc_member", pro.dc_member, **kwargs, fields="trade_date,ts_code,con_code,name")
        if df is None or df.empty:
            return pd.DataFrame(columns=["trade_date", "ts_code", "con_code", "name"])
        out = df.copy()
        out["trade_date"] = pd.to_datetime(out["trade_date"], format="%Y%m%d", errors="coerce")
        out["ts_code"] = out["ts_code"].astype(str)
        out["con_code"] = out["con_code"].astype(str)
        out["name"] = out["name"].fillna("").astype(str)
        out = out.dropna(subset=["trade_date", "ts_code", "con_code"])
        out = out.drop_duplicates(subset=["trade_date", "ts_code", "con_code"], keep="last")
        return out[["trade_date", "ts_code", "con_code", "name"]].reset_index(drop=True)

    def fetch_stock_daily_by_trade_date(self, trade_date: Union[date, datetime, str]) -> pd.DataFrame:
        pro = get_ts_pro()
        td = _to_yyyymmdd(trade_date)
        if pro is None or td == "":
            return pd.DataFrame(columns=["market", "code", "trade_date", "open", "high", "low", "close", "volume", "amount"])
        df = call_tushare_api("daily", pro.daily, trade_date=td)
        if df is None or df.empty:
            return pd.DataFrame(columns=["market", "code", "trade_date", "open", "high", "low", "close", "volume", "amount"])
        out = df.copy()
        out["market"] = out["ts_code"].map(_ts_code_to_market)
        out["code"] = out["ts_code"].astype(str).str[:6]
        out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.tz_localize(None)
        out["open"] = pd.to_numeric(out["open"], errors="coerce")
        out["high"] = pd.to_numeric(out["high"], errors="coerce")
        out["low"] = pd.to_numeric(out["low"], errors="coerce")
        out["close"] = pd.to_numeric(out["close"], errors="coerce")
        out["volume"] = pd.to_numeric(out["vol"], errors="coerce")
        out["amount"] = pd.to_numeric(out["amount"], errors="coerce")
        out = out[["market", "code", "trade_date", "open", "high", "low", "close", "volume", "amount"]]
        out = out.dropna(subset=["market", "code", "trade_date"])
        out = out.drop_duplicates(subset=["market", "code", "trade_date"], keep="last")
        return out

    def fetch_stock_flows_by_trade_date(self, trade_date: Union[date, datetime, str]) -> pd.DataFrame:
        pro = get_ts_pro()
        td = _to_yyyymmdd(trade_date)
        if pro is None or td == "":
            return pd.DataFrame(columns=["code", "trade_date", "labi_buy", "labi_sell", "mism_buy", "mism_sell"])
        df = call_tushare_api("moneyflow", pro.moneyflow, trade_date=td)
        if df is None or df.empty:
            return pd.DataFrame(columns=["code", "trade_date", "labi_buy", "labi_sell", "mism_buy", "mism_sell"])
        out = df.copy()
        out["code"] = out["ts_code"].astype(str).str[:6]
        out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.tz_localize(None)
        out["labi_buy"] = pd.to_numeric(out["buy_lg_amount"], errors="coerce") + pd.to_numeric(out["buy_elg_amount"], errors="coerce")
        out["labi_sell"] = pd.to_numeric(out["sell_lg_amount"], errors="coerce") + pd.to_numeric(out["sell_elg_amount"], errors="coerce")
        out["mism_buy"] = pd.to_numeric(out["buy_sm_amount"], errors="coerce") + pd.to_numeric(out["buy_md_amount"], errors="coerce")
        out["mism_sell"] = pd.to_numeric(out["sell_sm_amount"], errors="coerce") + pd.to_numeric(out["sell_md_amount"], errors="coerce")
        out = out[["code", "trade_date", "labi_buy", "labi_sell", "mism_buy", "mism_sell"]].dropna(subset=["code", "trade_date"])
        out = out.drop_duplicates(subset=["code", "trade_date"], keep="last")
        return out

    def fetch_stock_flows(self, code: str, start_dt: date, end_dt: date) -> pd.DataFrame:
        pro = get_ts_pro()
        ts_code = stock_code_to_ts(code)
        if pro is None or ts_code == "":
            return pd.DataFrame(columns=["date", "labi_buy", "labi_sell", "mism_buy", "mism_sell"])
        df = call_tushare_api("moneyflow", pro.moneyflow, ts_code=ts_code, start_date=_to_yyyymmdd(start_dt), end_date=_to_yyyymmdd(end_dt))
        if df is None or df.empty:
            return pd.DataFrame(columns=["date", "labi_buy", "labi_sell", "mism_buy", "mism_sell"])
        out = df.copy()
        out["labi_buy"] = pd.to_numeric(out["buy_lg_amount"], errors="coerce") + pd.to_numeric(out["buy_elg_amount"], errors="coerce")
        out["labi_sell"] = pd.to_numeric(out["sell_lg_amount"], errors="coerce") + pd.to_numeric(out["sell_elg_amount"], errors="coerce")
        out["mism_buy"] = pd.to_numeric(out["buy_sm_amount"], errors="coerce") + pd.to_numeric(out["buy_md_amount"], errors="coerce")
        out["mism_sell"] = pd.to_numeric(out["sell_sm_amount"], errors="coerce") + pd.to_numeric(out["sell_md_amount"], errors="coerce")
        out["date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.tz_localize(None)
        out = out[["date", "labi_buy", "labi_sell", "mism_buy", "mism_sell"]].dropna(subset=["date"])
        return out.sort_values("date").reset_index(drop=True)

    def fetch_adj_factors_by_trade_date(self, trade_date: Union[date, datetime, str]) -> pd.DataFrame:
        pro = get_ts_pro()
        td = _to_yyyymmdd(trade_date)
        if pro is None or td == "":
            return pd.DataFrame(columns=["code", "date", "adj_factor"])
        df = call_tushare_api("adj_factor", pro.query, "adj_factor", trade_date=td)
        if df is None or df.empty:
            return pd.DataFrame(columns=["code", "date", "adj_factor"])
        out = df.copy()
        out["code"] = out["ts_code"].astype(str).str[:6]
        out["date"] = pd.to_datetime(out["trade_date"]).dt.tz_localize(None)
        out["adj_factor"] = pd.to_numeric(out["adj_factor"], errors="coerce")
        out = out[["code", "date", "adj_factor"]].dropna(subset=["code", "date"])
        return out

    def fetch_stock_st_by_trade_date(self, trade_date: Union[date, datetime, str]) -> pd.DataFrame:
        pro = get_ts_pro()
        td = _to_yyyymmdd(trade_date)
        if pro is None or td == "":
            return pd.DataFrame(columns=["code", "trade_date"])
        df = call_tushare_api("stock_st", pro.stock_st, trade_date=td)
        if df is None or df.empty:
            return pd.DataFrame(columns=["code", "trade_date"])
        out = df.copy()
        out["code"] = out["ts_code"].astype(str).str[:6]
        out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.tz_localize(None)
        out = out[["code", "trade_date"]].dropna(subset=["code", "trade_date"])
        out = out.drop_duplicates(subset=["code", "trade_date"], keep="last")
        return out

    def fetch_limit_list_by_trade_date(self, trade_date: Union[date, datetime, str]) -> pd.DataFrame:
        pro = get_ts_pro()
        td = _to_yyyymmdd(trade_date)
        if pro is None or td == "":
            return pd.DataFrame(columns=["code", "trade_date", "limit", "fd_amount"])
        df = call_tushare_api("limit_list_d", pro.limit_list_d, trade_date=td)
        if df is None or df.empty:
            return pd.DataFrame(columns=["code", "trade_date", "limit", "fd_amount"])
        out = df.copy()
        out["code"] = out["ts_code"].astype(str).str[:6]
        out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.tz_localize(None)
        out["fd_amount"] = pd.to_numeric(out["fd_amount"], errors="coerce")
        out = out[["code", "trade_date", "limit", "fd_amount"]].dropna(subset=["code", "trade_date"])
        out = out.drop_duplicates(subset=["code", "trade_date"], keep="last")
        return out

    def fetch_stock_price_limits_by_trade_date(self, trade_date: Union[date, datetime, str]) -> pd.DataFrame:
        pro = get_ts_pro()
        td = _to_yyyymmdd(trade_date)
        if pro is None or td == "":
            return pd.DataFrame(columns=["code", "trade_date", "pre_close", "upper_limit", "lower_limit"])
        df = call_tushare_api("stk_limit", pro.stk_limit, trade_date=td)
        if df is None or df.empty:
            return pd.DataFrame(columns=["code", "trade_date", "pre_close", "upper_limit", "lower_limit"])
        out = df.copy()
        out["code"] = out["ts_code"].astype(str).str[:6]
        out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.tz_localize(None)
        out["pre_close"] = pd.to_numeric(out["pre_close"], errors="coerce")
        out["upper_limit"] = pd.to_numeric(out["up_limit"], errors="coerce")
        out["lower_limit"] = pd.to_numeric(out["down_limit"], errors="coerce")
        out = out[["code", "trade_date", "pre_close", "upper_limit", "lower_limit"]]
        out = out.dropna(subset=["code", "trade_date"])
        out = out.drop_duplicates(subset=["code", "trade_date"], keep="last")
        return out

    def fetch_open_auction_by_trade_date(self, trade_date: Union[date, datetime, str]) -> pd.DataFrame:
        pro = get_ts_pro()
        td = _to_yyyymmdd(trade_date)
        if pro is None or td == "":
            return pd.DataFrame(columns=["code", "trade_date", "open_auction_volume"])
        df = call_tushare_api("stk_auction", pro.stk_auction, trade_date=td, fields="ts_code,trade_date,vol")
        if df is None or df.empty:
            return pd.DataFrame(columns=["code", "trade_date", "open_auction_volume"])
        out = df.copy()
        out["code"] = out["ts_code"].astype(str).str[:6]
        out["trade_date"] = pd.to_datetime(out["trade_date"]).dt.tz_localize(None)
        out["open_auction_volume"] = pd.to_numeric(out["vol"], errors="coerce").round().astype("Int64")
        out = out[["code", "trade_date", "open_auction_volume"]].dropna(subset=["code", "trade_date"])
        out = out.drop_duplicates(subset=["code", "trade_date"], keep="last")
        return out

    def fetch_stock_minute_bars_seed(self, code: str, start_date: date, end_date: date) -> pd.DataFrame:
        if start_date > end_date or not self._settings.is_source_enabled("opentdx"):
            return pd.DataFrame(columns=["bar_time", "open", "high", "low", "close", "volume", "amount"])
        items = opentdx_provider.get_stock_quotes([normalize_stock_code(code)], "1m", "", _trade_date_text(start_date), _trade_date_text(end_date), "", "", None, "none")
        frame = _stock_items_to_frame(items)
        if frame.empty:
            return pd.DataFrame(columns=["bar_time", "open", "high", "low", "close", "volume", "amount"])
        frame = frame[(frame["trade_date"] >= start_date) & (frame["trade_date"] <= end_date)].copy()
        return frame[["bar_time", "open", "high", "low", "close", "volume", "amount"]].reset_index(drop=True)

    def fetch_index_daily_bars_seed(self, index_code: str, start_date: date, end_date: date) -> pd.DataFrame:
        if start_date > end_date or not self._settings.is_source_enabled("opentdx"):
            return pd.DataFrame(columns=["trade_date", "open", "high", "low", "close", "amount"])
        items = opentdx_provider.get_index_quotes([normalize_index_code(index_code)], "1d", "", _trade_date_text(start_date), _trade_date_text(end_date), None)
        frame = _index_items_to_frame(items)
        if frame.empty:
            return pd.DataFrame(columns=["trade_date", "open", "high", "low", "close", "amount"])
        frame = frame[(frame["trade_date"] >= start_date) & (frame["trade_date"] <= end_date)].copy()
        return frame[["trade_date", "open", "high", "low", "close", "amount"]].reset_index(drop=True)

    def fetch_index_quote_info(self, index_code: str) -> dict[str, object]:
        if TdxClient is None:
            return {"index_code": index_code, "name": INDEX_NAME_MAP.get(index_code, index_code.split(".", 1)[-1]), "pre_close": None}
        normalized_code = index_code.split(".", 1)[1]
        market = _quote_market_from_index_code(index_code)
        records = _call_opentdx(lambda client: client.index_info([(market, normalized_code)]))
        if not records:
            return {"index_code": index_code, "name": INDEX_NAME_MAP.get(index_code, normalized_code), "pre_close": None}
        frame = pd.DataFrame(records)
        if frame.empty:
            return {"index_code": index_code, "name": INDEX_NAME_MAP.get(index_code, normalized_code), "pre_close": None}
        row = frame.iloc[0]
        pre_close = pd.to_numeric(row.get("pre_close"), errors="coerce")
        return {
            "index_code": index_code,
            "name": INDEX_NAME_MAP.get(index_code, normalized_code),
            "pre_close": float(pre_close) if pd.notna(pre_close) else None,
        }

    def get_stock_bar_1m(self, request: StockBar1mRequest, base_df: pd.DataFrame) -> tuple[pd.DataFrame, ContractReport]:
        base_items = _base_stock_items(request.code, base_df)
        expected_dates = _expected_trade_dates(request.start_date, request.end_date)

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

        steps: list[ProviderStep[StockQuoteItem]] = []
        if self._settings.is_source_enabled("efinance"):
            steps.append(ProviderStep("efinance", lambda codes, start_text, end_text: efinance_provider.get_stock_quotes(codes, "1m", "", start_text, end_text, "", "", None, "none")))
        if self._settings.is_source_enabled("mootdx"):
            steps.append(ProviderStep("mootdx", lambda codes, start_text, end_text: mootdx_provider.get_stock_quotes(codes, "1m", "", start_text, end_text, "", "", None, "none")))
        if self._settings.is_source_enabled("akshare"):
            steps.append(ProviderStep("akshare", lambda codes, start_text, end_text: akshare_provider.get_stock_quotes(codes, "1m", "", start_text, end_text, "", "", None, "none")))
        merged_items, fallback_report = run_fallback_chain_with_report(
            "updater.stock_bar_1m",
            base_items,
            ("code", "trade_time", "freq"),
            lambda items: [([str(request.code).zfill(6)], _trade_date_text(request.start_date), _trade_date_text(request.end_date))] if _needs_more(items) else [],
            tuple(steps),
        )
        out = _stock_items_to_frame(merged_items)
        out = out[["bar_time", "open", "high", "low", "close", "volume", "amount"]]
        quality = validate_quote_frame(out.rename(columns={"bar_time": "trade_time"}), ["trade_time"], "trade_time")
        completeness = summarize_minute_completeness(_stock_items_to_frame(merged_items), request.end_date)
        report = ContractReport.from_fallback_report("updater.stock_bar_1m", fallback_report, "seed", base_items != [])
        return out, ContractReport(
            contract_name=report.contract_name,
            source_hit_counts=report.source_hit_counts,
            source_request_counts=report.source_request_counts,
            source_error_count=report.source_error_count,
            source_skipped_count=report.source_skipped_count,
            conflict_count=report.conflict_count + int(quality["duplicate_key_count"]),
            quarantine_count=report.quarantine_count,
            degraded=report.degraded or completeness["missing_bar_count"] > 0,
        )

    def get_index_bar_1d(self, request: IndexBar1dRequest, base_df: pd.DataFrame) -> tuple[pd.DataFrame, ContractReport]:
        base_items = _base_index_items(request.index_code, base_df)
        expected_dates = _expected_trade_dates(request.start_date, request.end_date)

        def _needs_more(items: list[IndexQuoteItem]) -> bool:
            if items == []:
                return True
            frame = _index_items_to_frame(items)
            actual_dates = set(frame["trade_date"].dropna().tolist())
            return not expected_dates.issubset(actual_dates)

        steps: list[ProviderStep[IndexQuoteItem]] = []
        if self._settings.is_source_enabled("efinance"):
            steps.append(ProviderStep("efinance", lambda index_codes, start_text, end_text: efinance_provider.get_index_quotes(index_codes, "1d", "", start_text, end_text, None)))
        if self._settings.is_source_enabled("mootdx"):
            steps.append(ProviderStep("mootdx", lambda index_codes, start_text, end_text: mootdx_provider.get_index_quotes(index_codes, "1d", "", start_text, end_text, None)))
        if self._settings.is_source_enabled("akshare"):
            steps.append(ProviderStep("akshare", lambda index_codes, start_text, end_text: akshare_provider.get_index_quotes(index_codes, "1d", "", start_text, end_text, None)))
        merged_items, fallback_report = run_fallback_chain_with_report(
            "updater.index_bar_1d",
            base_items,
            ("index_code", "trade_time", "freq"),
            lambda items: [([request.index_code], _trade_date_text(request.start_date), _trade_date_text(request.end_date))] if _needs_more(items) else [],
            tuple(steps),
        )
        out = _index_items_to_frame(merged_items)
        quality = validate_quote_frame(out.rename(columns={"trade_date": "trade_time"}), ["index_code", "trade_time"], "trade_time")
        report = ContractReport.from_fallback_report("updater.index_bar_1d", fallback_report, "seed", base_items != [])
        return out, ContractReport(
            contract_name=report.contract_name,
            source_hit_counts=report.source_hit_counts,
            source_request_counts=report.source_request_counts,
            source_error_count=report.source_error_count,
            source_skipped_count=report.source_skipped_count,
            conflict_count=report.conflict_count + int(quality["duplicate_key_count"]),
            quarantine_count=report.quarantine_count,
            degraded=report.degraded,
        )

    def repair_stock_daily_ohlcva(self, request: StockDailyOhlcvaRepairRequest, df_day: pd.DataFrame) -> tuple[pd.DataFrame, ContractReport]:
        if df_day.empty:
            return df_day, ContractReport.empty("updater.stock_daily_1d.ohlcva", "seed", False)
        work = df_day.copy()
        quote_columns = ["open", "high", "low", "close", "volume", "amount"]
        active_mask = ~work["is_suspended"].fillna(False).astype(bool)
        missing_mask = active_mask & work[quote_columns].isna().any(axis=1)
        missing_codes = work.loc[missing_mask, "code"].astype(str).str.zfill(6).tolist()
        if missing_codes == []:
            return work, ContractReport.empty("updater.stock_daily_1d.ohlcva", "seed", True)
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

        steps: list[ProviderStep[StockQuoteItem]] = []
        if self._settings.is_source_enabled("tushare"):
            steps.append(ProviderStep("tushare", lambda codes, start_text, end_text: tushare_provider.get_stock_quotes(codes, "1d", start_text, "", "", "", "", None, "none")))
        if self._settings.is_source_enabled("efinance"):
            steps.append(ProviderStep("efinance", lambda codes, start_text, end_text: efinance_provider.get_stock_quotes(codes, "1d", start_text, "", "", "", "", None, "none")))
        if self._settings.is_source_enabled("mootdx"):
            steps.append(ProviderStep("mootdx", lambda codes, start_text, end_text: mootdx_provider.get_stock_quotes(codes, "1d", start_text, "", "", "", "", None, "none")))
        if self._settings.is_source_enabled("akshare"):
            steps.append(ProviderStep("akshare", lambda codes, start_text, end_text: akshare_provider.get_stock_quotes(codes, "1d", start_text, "", "", "", "", None, "none")))
        merged_items, fallback_report = run_fallback_chain_with_report(
            "updater.stock_daily_1d.ohlcva",
            base_items,
            ("code", "trade_time", "freq"),
            lambda items: [(_remaining_missing_codes(items), _trade_date_text(request.trade_date), _trade_date_text(request.trade_date))] if _remaining_missing_codes(items) else [],
            tuple(steps),
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
        report = ContractReport.from_fallback_report("updater.stock_daily_1d.ohlcva", fallback_report, "seed", base_items != [])
        return work, report

