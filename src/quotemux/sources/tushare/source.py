from __future__ import annotations

from datetime import datetime, timedelta
from functools import lru_cache

import pandas as pd

from quotemux.infra.cache.store import build_cache_path, filter_frame_by_date_range, filter_frame_by_datetime_range, latest_n_rows, merge_cache_frame, plan_missing_ranges, read_cache_frame, write_cache_frame
from quotemux.infra.config import DATE_FORMAT, TS_TOKEN
from platform_models import AdjFactorItem, BoardCatalogItem, BoardCategoryItem, BoardMemberHistoryItem, BoardMemberItem, BoardMoneyFlowItem, BoardQuoteItem, IndexCatalogItem, IndexMemberItem, IndexQuoteItem, MarketCapitalFlowItem, NameHistoryItem, ShareholderChangeItem, StockBasicInfo, StockFinancialStatementItem, StockMoneyFlowItem, StockQuoteItem, TechnicalFactorItem, TradingCalendarItem, TradingSessionItem
from quotemux.infra.common import INTRADAY_RULES, aggregate_ohlc, add_quote_metrics, build_time_bounds, format_date_value, format_datetime_value, index_code_to_ts, normalize_index_code, normalize_stock_code, stock_code_to_ts
from quotemux.infra.tushare.rate_limit import call_tushare_api

try:
    import tushare as ts
except Exception:
    ts = None


TS_FREQ_MAP = {
    "1m": "1min",
    "5m": "5min",
    "15m": "15min",
    "30m": "30min",
    "60m": "60min",
    "1d": "D",
    "1w": "W",
    "1mo": "M",
}
TS_INDEX_MARKETS = ("CSI", "SSE", "SZSE", "SW", "CICC", "OTH")
TS_STOCK_LIST_STATUS = ("L", "D", "P")


@lru_cache(maxsize=1)
def get_ts_pro():
    if ts is None or not TS_TOKEN:
        return None
    return ts.pro_api(TS_TOKEN)


def _normalize_index_market(market: str) -> str:
    if not market:
        return ""
    return market.strip().lower()


def _resolve_index_markets(market: str) -> list[str]:
    text = market.strip().upper()
    if text == "":
        return list(TS_INDEX_MARKETS)
    if text == "A_SHARE":
        return ["CSI", "SSE", "SZSE", "SW", "CICC"]
    if text in TS_INDEX_MARKETS:
        return [text]
    return []


def _to_tushare_date(value: str) -> str:
    return format_date_value(value).replace("-", "")


def _stock_exchange_from_ts_code(ts_code: str) -> str:
    text = str(ts_code).upper()
    if text.endswith(".SH"):
        return "SSE"
    if text.endswith(".SZ"):
        return "SZSE"
    if text.endswith(".BJ"):
        return "BSE"
    return ""


def _stock_market_from_row(market_text: str, exchange: str, code: str) -> str:
    text = str(market_text).lower()
    if exchange == "BSE" or code.startswith(("4", "8")):
        return "beijing"
    if "科创" in text or code.startswith("688"):
        return "star_market"
    if "创业" in text or code.startswith(("300", "301")):
        return "chi_next"
    return "main_board"


def _stock_list_status(status: str) -> str:
    if status == "D":
        return "delisted"
    if status == "P":
        return "pending"
    return "listed"


def _stock_statuses(list_status: str, include_delisted: bool) -> tuple[str, ...]:
    if list_status == "listed":
        return ("L",)
    if list_status == "delisted":
        return ("D",)
    if list_status == "pending":
        return ("P",)
    if include_delisted:
        return TS_STOCK_LIST_STATUS
    return ("L",)


def _fetch_stock_basic_frame(status: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    try:
        df = call_tushare_api(
            "stock_basic",
            pro.stock_basic,
            exchange="",
            list_status=status,
            fields="ts_code,symbol,name,area,industry,market,list_date,delist_date,list_status",
        )
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    for column in ["ts_code", "symbol", "name", "area", "industry", "market", "list_date", "delist_date", "list_status"]:
        if column not in work.columns:
            work[column] = ""
    work["code"] = work["symbol"].fillna("").astype(str).str.zfill(6)
    work["exchange"] = work["ts_code"].map(_stock_exchange_from_ts_code)
    work["market2"] = work.apply(lambda row: _stock_market_from_row(str(row["market"]), str(row["exchange"]), str(row["code"])), axis=1)
    work["list_status2"] = work["list_status"].fillna("").astype(str).map(_stock_list_status)
    return work[["code", "name", "exchange", "market2", "list_status2", "list_date", "delist_date", "industry", "area"]]


def _load_stock_basic_frame(status: str) -> pd.DataFrame:
    cache_path = build_cache_path("tushare", ["stocks", "catalog"], {"status": status})
    cache_df = read_cache_frame(cache_path)
    if cache_df.empty:
        fetched_df = _fetch_stock_basic_frame(status)
        if not fetched_df.empty:
            write_cache_frame(cache_path, fetched_df)
            cache_df = fetched_df
    return cache_df


def get_stock_catalog(codes: list[str], name: str, exchange: str, list_status: str, include_delisted: bool, limit: int, offset: int) -> list[StockBasicInfo]:
    frames = [_load_stock_basic_frame(status) for status in _stock_statuses(list_status, include_delisted)]
    frames = [frame for frame in frames if not frame.empty]
    if frames == []:
        return []
    work = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["code"], keep="last")
    normalized_codes = [normalize_stock_code(code) for code in codes if normalize_stock_code(code)]
    if normalized_codes:
        work = work[work["code"].isin(normalized_codes)]
    if name:
        work = work[work["name"].fillna("").astype(str).str.contains(name, case=False, na=False)]
    if exchange:
        work = work[work["exchange"] == exchange]
    if list_status:
        work = work[work["list_status2"] == list_status]
    work = work.sort_values("code").iloc[offset: offset + limit]
    items: list[StockBasicInfo] = []
    for _, row in work.iterrows():
        items.append(
            StockBasicInfo(
                code=str(row["code"]),
                name=str(row["name"]),
                exchange=str(row["exchange"]),
                market=str(row["market2"]),
                list_status=str(row["list_status2"]),
                list_date=format_date_value(row["list_date"]),
                delist_date=format_date_value(row["delist_date"]),
                industry=str(row["industry"] or ""),
                area=str(row["area"] or ""),
            )
        )
    return items


def get_stock_basic(code: str) -> StockBasicInfo | None:
    items = get_stock_catalog([normalize_stock_code(code)], "", "", "", True, 1, 0)
    return items[0] if items else None


def get_stock_name_history(code: str, start_date: str, end_date: str) -> list[NameHistoryItem]:
    pro = get_ts_pro()
    ts_code = stock_code_to_ts(code)
    if pro is None or ts_code == "":
        return []
    try:
        df = call_tushare_api("namechange", pro.namechange, ts_code=ts_code, start_date=_to_tushare_date(start_date), end_date=_to_tushare_date(end_date))
    except Exception:
        return []
    if df is None or df.empty:
        return []
    work = df.copy()
    for column in ["ts_code", "name", "start_date", "end_date", "ann_date"]:
        if column not in work.columns:
            work[column] = ""
    items: list[NameHistoryItem] = []
    normalized = normalize_stock_code(code)
    for _, row in work.sort_values("start_date").iterrows():
        items.append(
            NameHistoryItem(
                code=normalized,
                name=str(row["name"]),
                start_date=format_date_value(row["start_date"]),
                end_date=format_date_value(row["end_date"]),
                ann_date=format_date_value(row["ann_date"]),
            )
        )
    return items


def get_adj_factors(code: str, start_date: str, end_date: str, base_date: str) -> list[AdjFactorItem]:
    del base_date
    pro = get_ts_pro()
    ts_code = stock_code_to_ts(code)
    if pro is None or ts_code == "":
        return []
    try:
        df = call_tushare_api("adj_factor", pro.adj_factor, ts_code=ts_code, start_date=_to_tushare_date(start_date), end_date=_to_tushare_date(end_date))
    except Exception:
        return []
    if df is None or df.empty:
        return []
    work = df.copy()
    for column in ["trade_date", "adj_factor"]:
        if column not in work.columns:
            work[column] = None
    normalized = normalize_stock_code(code)
    items: list[AdjFactorItem] = []
    for _, row in work.sort_values("trade_date").iterrows():
        items.append(
            AdjFactorItem(
                code=normalized,
                trade_date=format_date_value(row["trade_date"]).replace("-", ""),
                adj_factor=float(row["adj_factor"]) if pd.notna(row["adj_factor"]) else None,
            )
        )
    return items


def _board_code_to_ts(board_code: str) -> str:
    text = str(board_code).strip().upper()
    if text == "":
        return ""
    if "." in text:
        return text
    return f"{text}.TI"


def _board_category_from_code(board_code: str) -> str:
    text = str(board_code).upper()
    if text.startswith(("881", "877")):
        return "industry"
    if text.startswith(("885", "886", "BK")):
        return "concept"
    return ""


def _fetch_board_catalog_frame(index_type: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    try:
        df = call_tushare_api("ths_index", pro.ths_index, type=index_type)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    for column in ["ts_code", "name", "list_date", "type"]:
        if column not in work.columns:
            work[column] = ""
    work["board_code"] = work["ts_code"].fillna("").astype(str).str.split(".").str[0]
    work["category"] = work["board_code"].map(_board_category_from_code)
    work["status"] = "active"
    return work[["board_code", "name", "category", "status"]]


def _load_board_catalog_frame(index_type: str) -> pd.DataFrame:
    cache_path = build_cache_path("tushare", ["boards", "catalog"], {"type": index_type})
    cache_df = read_cache_frame(cache_path)
    if cache_df.empty:
        fetched_df = _fetch_board_catalog_frame(index_type)
        if not fetched_df.empty:
            write_cache_frame(cache_path, fetched_df)
            cache_df = fetched_df
    return cache_df


def get_board_catalog(category: str, market: str, status: str, limit: int, offset: int) -> list[BoardCatalogItem]:
    if market and market != "a_share":
        return []
    frames = [_load_board_catalog_frame(index_type) for index_type in ("N", "I")]
    frames = [frame for frame in frames if not frame.empty]
    if frames == []:
        return []
    work = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["board_code"], keep="last")
    if category:
        work = work[work["category"] == category]
    if status:
        work = work[work["status"] == status]
    work = work.sort_values("board_code").iloc[offset: offset + limit]
    return [
        BoardCatalogItem(
            board_code=str(row["board_code"]),
            board_name=str(row["name"]),
            category=str(row["category"]),
            market="a_share",
            status=str(row["status"]),
        )
        for _, row in work.iterrows()
    ]


def get_board_profile(board_code: str) -> BoardCatalogItem | None:
    for item in get_board_catalog("", "a_share", "", 100000, 0):
        if item.board_code == board_code:
            return item
    return None


def get_board_categories(parent_code: str, level: int | None) -> list[BoardCategoryItem]:
    items = [
        BoardCategoryItem(category_code="concept", category_name="概念板块", parent_code="", level=1, sort_order=1),
        BoardCategoryItem(category_code="industry", category_name="行业板块", parent_code="", level=1, sort_order=2),
    ]
    if parent_code:
        items = [item for item in items if item.parent_code == parent_code]
    if level is not None:
        items = [item for item in items if item.level == level]
    return items


def _fetch_board_members_frame(board_code: str) -> pd.DataFrame:
    pro = get_ts_pro()
    ts_code = _board_code_to_ts(board_code)
    if pro is None or ts_code == "":
        return pd.DataFrame()
    try:
        df = call_tushare_api("ths_member", pro.ths_member, ts_code=ts_code)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    for column in ["ts_code", "con_code", "con_name", "weight", "in_date", "out_date"]:
        if column not in work.columns:
            work[column] = ""
    work["board_code"] = ts_code.split(".", 1)[0]
    work["code"] = work["con_code"].map(normalize_stock_code)
    work["name"] = work["con_name"].fillna("").astype(str)
    return work[["board_code", "code", "name", "weight", "in_date", "out_date"]]


def _load_board_members_frame(board_code: str) -> pd.DataFrame:
    normalized = _board_code_to_ts(board_code).split(".", 1)[0]
    cache_path = build_cache_path("tushare", ["boards", "members"], {"board_code": normalized})
    cache_df = read_cache_frame(cache_path)
    if cache_df.empty:
        fetched_df = _fetch_board_members_frame(normalized)
        if not fetched_df.empty:
            write_cache_frame(cache_path, fetched_df)
            cache_df = fetched_df
    return cache_df


def get_board_members(board_code: str, trade_date: str) -> list[BoardMemberItem]:
    work = _load_board_members_frame(board_code)
    if work.empty:
        return []
    target_date = _to_tushare_date(trade_date)
    if target_date:
        work = work[
            (work["in_date"].fillna("").astype(str) <= target_date)
            & ((work["out_date"].fillna("").astype(str) == "") | (work["out_date"].fillna("").astype(str) >= target_date))
        ]
    items: list[BoardMemberItem] = []
    for _, row in work.sort_values("code").iterrows():
        items.append(
            BoardMemberItem(
                board_code=str(row["board_code"]),
                code=str(row["code"]),
                name=str(row["name"]),
                weight=float(row["weight"]) if pd.notna(row["weight"]) and str(row["weight"]) != "" else None,
                join_date=format_date_value(row["in_date"]),
            )
        )
    return items


def get_board_member_history(board_code: str, start_date: str, end_date: str) -> list[BoardMemberHistoryItem]:
    work = _load_board_members_frame(board_code)
    if work.empty:
        return []
    start_text = _to_tushare_date(start_date)
    end_text = _to_tushare_date(end_date)
    items: list[BoardMemberHistoryItem] = []
    for _, row in work.iterrows():
        in_date = str(row["in_date"] or "")
        out_date = str(row["out_date"] or "")
        if in_date and (start_text == "" or in_date >= start_text) and (end_text == "" or in_date <= end_text):
            items.append(BoardMemberHistoryItem(board_code=str(row["board_code"]), code=str(row["code"]), name=str(row["name"]), effective_date=format_date_value(in_date), action="add"))
        if out_date and (start_text == "" or out_date >= start_text) and (end_text == "" or out_date <= end_text):
            items.append(BoardMemberHistoryItem(board_code=str(row["board_code"]), code=str(row["code"]), name=str(row["name"]), effective_date=format_date_value(out_date), action="remove"))
    return sorted(items, key=lambda item: (item.effective_date, item.code, item.action))


def _fetch_board_quotes_frame(board_code: str, start_value: str, end_value: str) -> pd.DataFrame:
    pro = get_ts_pro()
    ts_code = _board_code_to_ts(board_code)
    if pro is None or ts_code == "":
        return pd.DataFrame()
    fetcher = getattr(pro, "ths_daily", None)
    if fetcher is None:
        return pd.DataFrame()
    try:
        df = call_tushare_api("ths_daily", fetcher, ts_code=ts_code, start_date=start_value, end_date=end_value)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    for column in ["trade_date", "open", "high", "low", "close", "pre_close", "pct_change", "vol", "amount"]:
        if column not in work.columns:
            work[column] = None
    work["board_code"] = ts_code.split(".", 1)[0]
    work["trade_time"] = pd.to_datetime(work["trade_date"], errors="coerce")
    work["volume"] = pd.to_numeric(work["vol"], errors="coerce") if "vol" in work.columns else None
    return work[["board_code", "trade_time", "open", "high", "low", "close", "pre_close", "pct_change", "volume", "amount"]]


def get_board_quotes(board_codes: list[str], freq: str, trade_date: str, start_date: str, end_date: str, start_time: str, end_time: str, count: int | None) -> list[BoardQuoteItem]:
    del start_time
    del end_time
    request_start_dt, request_end_dt = build_time_bounds(trade_date, start_date, end_date, "", "", count, False)
    request_start = request_start_dt.strftime(DATE_FORMAT) if request_start_dt is not None else ""
    request_end = request_end_dt.strftime(DATE_FORMAT) if request_end_dt is not None else ""
    if request_start == "" and request_end == "":
        request_end = datetime.now().strftime(DATE_FORMAT)
        request_start = (datetime.now() - timedelta(days=400)).strftime(DATE_FORMAT)
    elif request_start == "":
        request_start = request_end
    elif request_end == "":
        request_end = request_start
    items: list[BoardQuoteItem] = []
    for board_code in board_codes:
        normalized = _board_code_to_ts(board_code).split(".", 1)[0]
        cache_path = build_cache_path("tushare", ["boards", "quotes"], {"board_code": normalized})
        cache_df = read_cache_frame(cache_path)
        missing_ranges = plan_missing_ranges(cache_df, "trade_time", request_start, request_end, "day")
        fetched_frames = [_fetch_board_quotes_frame(normalized, missing_start, missing_end) for missing_start, missing_end in missing_ranges]
        fetched_frames = [frame for frame in fetched_frames if not frame.empty]
        if cache_df.empty and fetched_frames == []:
            fetched_df = _fetch_board_quotes_frame(normalized, request_start, request_end)
            if not fetched_df.empty:
                fetched_frames.append(fetched_df)
        if fetched_frames:
            cache_df = merge_cache_frame(cache_df, pd.concat(fetched_frames, ignore_index=True), ["board_code", "trade_time"], ["trade_time"])
            write_cache_frame(cache_path, cache_df)
        filtered_df = filter_frame_by_date_range(cache_df, "trade_time", request_start, request_end)
        if filtered_df.empty:
            continue
        filtered_df["trade_time"] = pd.to_datetime(filtered_df["trade_time"])
        agg_df = add_quote_metrics(aggregate_ohlc(filtered_df.drop(columns=["board_code"]), freq))
        if count:
            agg_df = agg_df.tail(count)
        for _, row in agg_df.iterrows():
            items.append(
                BoardQuoteItem(
                    board_code=normalized,
                    trade_time=format_datetime_value(row["trade_time"], freq),
                    freq=freq,
                    open=float(row["open"]) if pd.notna(row["open"]) else None,
                    high=float(row["high"]) if pd.notna(row["high"]) else None,
                    low=float(row["low"]) if pd.notna(row["low"]) else None,
                    close=float(row["close"]) if pd.notna(row["close"]) else None,
                    pre_close=float(row["pre_close"]) if pd.notna(row["pre_close"]) else None,
                    change=float(row["change"]) if pd.notna(row["change"]) else None,
                    pct_chg=float(row["pct_chg"]) if pd.notna(row["pct_chg"]) else None,
                    volume=float(row["volume"]) if pd.notna(row["volume"]) else None,
                    amount=float(row["amount"]) if pd.notna(row["amount"]) else None,
                )
            )
    return items


def get_board_daily_money_flow_snapshot(trade_date: str, scope: str, limit: int, offset: int) -> list[BoardMoneyFlowItem]:
    catalog_items = get_board_catalog(scope if scope in {"industry", "concept"} else "", "a_share", "active", limit, offset)
    items: list[BoardMoneyFlowItem] = []
    for catalog_item in catalog_items:
        items.extend(get_board_money_flow(catalog_item.board_code, trade_date, "", "", scope))
    return sorted(items, key=lambda item: (item.board_code, item.trade_date))


def get_market_sessions(codes: str) -> list[TradingSessionItem]:
    items: list[TradingSessionItem] = []
    for code in [normalize_stock_code(item) for item in codes.split(",") if normalize_stock_code(item)]:
        items.append(TradingSessionItem(code=code, session_name="pre_open", start_time="09:15:00", end_time="09:25:00", timezone="Asia/Shanghai"))
        items.append(TradingSessionItem(code=code, session_name="continuous", start_time="09:30:00", end_time="11:30:00", timezone="Asia/Shanghai"))
        items.append(TradingSessionItem(code=code, session_name="continuous", start_time="13:00:00", end_time="14:57:00", timezone="Asia/Shanghai"))
        items.append(TradingSessionItem(code=code, session_name="closing_call", start_time="14:57:00", end_time="15:00:00", timezone="Asia/Shanghai"))
        items.append(TradingSessionItem(code=code, session_name="after_hours", start_time="15:00:00", end_time="15:30:00", timezone="Asia/Shanghai"))
    return items


def _fetch_index_catalog_frame(market: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    try:
        df = call_tushare_api("index_basic", pro.index_basic, market=market)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    for column in ["ts_code", "name", "category", "market", "publisher", "list_date", "exp_date"]:
        if column not in work.columns:
            work[column] = ""
    work["index_code"] = work["ts_code"].map(normalize_index_code)
    work["index_name"] = work["name"].fillna("").astype(str)
    work["category"] = work["category"].fillna("").astype(str)
    work["market2"] = work["market"].fillna("").astype(str).map(_normalize_index_market)
    work["publisher2"] = work["publisher"].fillna("").astype(str)
    work["list_date2"] = work["list_date"].fillna("").astype(str)
    work["status"] = work["exp_date"].fillna("").astype(str).map(lambda value: "inactive" if value else "active")
    return work[["index_code", "index_name", "category", "market2", "publisher2", "list_date2", "status"]]


def get_index_catalog(index_code: str, category: str, market: str, publisher: str, status: str) -> list[IndexCatalogItem]:
    selected_markets = _resolve_index_markets(market)
    if market and not selected_markets:
        return []
    frames: list[pd.DataFrame] = []
    for market_code in selected_markets:
        cache_path = build_cache_path("tushare", ["indexes", "catalog"], {"market": market_code.lower()})
        cache_df = read_cache_frame(cache_path)
        if cache_df.empty:
            fetched_df = _fetch_index_catalog_frame(market_code)
            if not fetched_df.empty:
                write_cache_frame(cache_path, fetched_df)
                cache_df = fetched_df
        if not cache_df.empty:
            frames.append(cache_df)
    if not frames:
        return []
    work = merge_cache_frame(pd.DataFrame(), pd.concat(frames, ignore_index=True), ["index_code"], ["index_code"])
    normalized_code = normalize_index_code(index_code)
    if normalized_code:
        work = work[work["index_code"] == normalized_code]
    if category:
        work = work[work["category"] == category]
    if publisher:
        work = work[work["publisher2"] == publisher]
    if status:
        work = work[work["status"] == status]
    items: list[IndexCatalogItem] = []
    for _, row in work.sort_values("index_code").iterrows():
        items.append(
            IndexCatalogItem(
                index_code=str(row["index_code"]),
                index_name=str(row["index_name"]),
                category=str(row["category"]),
                market=str(row["market2"]),
                publisher=str(row["publisher2"]),
                list_date=format_date_value(row["list_date2"]),
                status=str(row["status"]),
            )
        )
    return items


from quotemux.sources.tushare.market_topics import get_block_trades, get_connect_active_top10, get_connect_capital_flow, get_connect_quotas, get_dragon_tiger, get_dragon_tiger_institutions, get_hot_money_details, get_hot_money_profiles, get_market_open_auctions
from quotemux.sources.tushare.stock_chips import get_chip_distribution, get_chip_performance
from quotemux.sources.tushare.stock_finance import get_audits, get_disclosure_dates, get_dividends, get_express, get_forecasts, get_main_business, get_repurchases, get_rights_issues, get_share_changes, get_unlock_schedules
from quotemux.sources.tushare.stock_ownership import get_ccass_holding_details, get_ccass_holdings, get_hk_connect_holdings, get_pledge_details, get_pledge_stats, get_shareholder_count, get_shareholder_top10
from quotemux.sources.tushare.stocks import get_auctions, get_bse_code_mappings, get_company_profile, get_hk_connect_targets, get_management_rewards, get_managers, get_nine_turn, get_premarket, get_rank_broker_monthly_picks, get_rank_research_reports, get_research_reports, get_stock_ah_comparisons, get_stock_archive, get_stock_daily_basic, get_stock_daily_market_value, get_stock_daily_valuation, get_stock_finance_indicators, get_stock_risk_flags, get_surveys


def _fetch_index_quotes_frame(index_code: str, start_value: str, end_value: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    try:
        df = call_tushare_api("index_daily", pro.index_daily, ts_code=index_code_to_ts(index_code), start_date=start_value, end_date=end_value)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy().sort_values("trade_date")
    for column in ["ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount"]:
        if column not in work.columns:
            work[column] = None
    work["index_code"] = work["ts_code"].map(normalize_index_code)
    work["trade_time"] = pd.to_datetime(work["trade_date"])
    work["volume2"] = pd.to_numeric(work["vol"], errors="coerce") if "vol" in work.columns else None
    return work[["index_code", "trade_time", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "volume2", "amount"]]


def get_index_quotes(
    index_codes: list[str],
    freq: str,
    trade_date: str,
    start_date: str,
    end_date: str,
    count: int | None,
) -> list[IndexQuoteItem]:
    request_start_dt, request_end_dt = build_time_bounds(trade_date, start_date, end_date, "", "", count, False)
    request_start = request_start_dt.strftime(DATE_FORMAT) if request_start_dt is not None else ""
    request_end = request_end_dt.strftime(DATE_FORMAT) if request_end_dt is not None else ""
    if request_start == "" and request_end == "":
        request_end = datetime.now().strftime(DATE_FORMAT)
        request_start = (datetime.now() - timedelta(days=400)).strftime(DATE_FORMAT)
    elif request_start == "":
        request_start = request_end
    elif request_end == "":
        request_end = request_start
    items: list[IndexQuoteItem] = []
    for index_code in index_codes:
        normalized = normalize_index_code(index_code)
        cache_path = build_cache_path("tushare", ["indexes", "quotes"], {"index_code": normalized})
        cache_df = read_cache_frame(cache_path)
        missing_ranges = plan_missing_ranges(cache_df, "trade_time", request_start, request_end, "day")
        fetched_frames: list[pd.DataFrame] = []
        for missing_start, missing_end in missing_ranges:
            fetched_df = _fetch_index_quotes_frame(normalized, missing_start, missing_end)
            if not fetched_df.empty:
                fetched_frames.append(fetched_df)
        if cache_df.empty and not fetched_frames:
            fetched_df = _fetch_index_quotes_frame(normalized, request_start, request_end)
            if not fetched_df.empty:
                fetched_frames.append(fetched_df)
        if fetched_frames:
            merged_cache = merge_cache_frame(cache_df, pd.concat(fetched_frames, ignore_index=True), ["index_code", "trade_time"], ["trade_time"])
            write_cache_frame(cache_path, merged_cache)
            cache_df = merged_cache
        filtered_df = filter_frame_by_date_range(cache_df, "trade_time", request_start, request_end)
        if filtered_df.empty:
            continue
        filtered_df["trade_time"] = pd.to_datetime(filtered_df["trade_time"])
        agg_df = add_quote_metrics(aggregate_ohlc(filtered_df.rename(columns={"volume2": "volume"}), freq))
        if count:
            agg_df = agg_df.tail(count)
        for _, row in agg_df.iterrows():
            items.append(
                IndexQuoteItem(
                    index_code=normalized,
                    trade_time=format_datetime_value(row["trade_time"], freq),
                    freq=freq,
                    open=float(row["open"]) if pd.notna(row["open"]) else None,
                    high=float(row["high"]) if pd.notna(row["high"]) else None,
                    low=float(row["low"]) if pd.notna(row["low"]) else None,
                    close=float(row["close"]) if pd.notna(row["close"]) else None,
                    pre_close=float(row["pre_close"]) if pd.notna(row["pre_close"]) else None,
                    change=float(row["change"]) if pd.notna(row["change"]) else None,
                    pct_chg=float(row["pct_chg"]) if pd.notna(row["pct_chg"]) else None,
                    volume=float(row["volume"]) if "volume" in row and pd.notna(row["volume"]) else None,
                    amount=float(row["amount"]) if pd.notna(row["amount"]) else None,
                )
            )
    return items


def _fetch_index_members_frame(index_code: str, start_value: str, end_value: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    try:
        df = call_tushare_api("index_weight", pro.index_weight, index_code=index_code_to_ts(index_code), start_date=start_value, end_date=end_value)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    for column in ["index_code", "con_code", "trade_date", "weight"]:
        if column not in work.columns:
            work[column] = None
    work["index_code2"] = work["index_code"].map(normalize_index_code)
    work["code"] = work["con_code"].map(normalize_stock_code)
    work["trade_date2"] = work["trade_date"].fillna("").astype(str)
    return work[["index_code2", "code", "trade_date2", "weight"]]


def get_index_members(index_code: str, trade_date: str) -> list[IndexMemberItem]:
    normalized = normalize_index_code(index_code)
    actual_trade_date = format_date_value(trade_date)
    if actual_trade_date:
        target_day = datetime.strptime(actual_trade_date, "%Y-%m-%d")
        start_value = target_day.replace(day=1).strftime(DATE_FORMAT)
        end_value = (target_day.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        end_text = end_value.strftime(DATE_FORMAT)
    else:
        end_text = datetime.now().strftime(DATE_FORMAT)
        start_value = (datetime.now() - timedelta(days=370)).strftime(DATE_FORMAT)
    cache_path = build_cache_path("tushare", ["indexes", "members"], {"index_code": normalized})
    cache_df = read_cache_frame(cache_path)
    missing_ranges = plan_missing_ranges(cache_df, "trade_date2", start_value, end_text, "day")
    fetched_frames: list[pd.DataFrame] = []
    for missing_start, missing_end in missing_ranges:
        fetched_df = _fetch_index_members_frame(normalized, missing_start, missing_end)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if cache_df.empty and not fetched_frames:
        fetched_df = _fetch_index_members_frame(normalized, start_value, end_text)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if fetched_frames:
        merged_cache = merge_cache_frame(cache_df, pd.concat(fetched_frames, ignore_index=True), ["index_code2", "code", "trade_date2"], ["trade_date2", "code"])
        write_cache_frame(cache_path, merged_cache)
        cache_df = merged_cache
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date2", start_value, end_text)
    if filtered_df.empty:
        return []
    if actual_trade_date:
        exact_trade_date = actual_trade_date.replace("-", "")
        exact_df = filtered_df[filtered_df["trade_date2"] == exact_trade_date]
        if exact_df.empty:
            candidate_df = filtered_df[filtered_df["trade_date2"] <= exact_trade_date]
            if candidate_df.empty:
                filtered_df = pd.DataFrame()
            else:
                latest_trade_date = candidate_df["trade_date2"].max()
                filtered_df = candidate_df[candidate_df["trade_date2"] == latest_trade_date]
        else:
            filtered_df = exact_df
    else:
        latest_trade_date = filtered_df["trade_date2"].max()
        filtered_df = filtered_df[filtered_df["trade_date2"] == latest_trade_date]
    if filtered_df.empty:
        return []
    items: list[IndexMemberItem] = []
    for _, row in filtered_df.sort_values(["trade_date2", "code"]).iterrows():
        items.append(
            IndexMemberItem(
                index_code=str(row["index_code2"]),
                code=str(row["code"]),
                name="",
                weight=float(row["weight"]) if pd.notna(row["weight"]) else None,
                trade_date=format_date_value(str(row["trade_date2"])),
            )
        )
    return items


def _fetch_stock_quotes_frame(code: str, freq: str, start_dt: datetime | None, end_dt: datetime | None, adjust: str) -> pd.DataFrame:
    if ts is None or not TS_TOKEN or freq == "tick":
        return pd.DataFrame()
    ts.set_token(TS_TOKEN)
    try:
        df = call_tushare_api(
            "pro_bar",
            ts.pro_bar,
            ts_code=stock_code_to_ts(code),
            start_date=start_dt.strftime(DATE_FORMAT) if start_dt else "",
            end_date=end_dt.strftime(DATE_FORMAT) if end_dt else "",
            asset="E",
            adj=None if adjust == "none" else adjust,
            freq=TS_FREQ_MAP.get(freq, "D"),
        )
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    time_column = "trade_time" if "trade_time" in df.columns else "trade_date"
    volume_column = "vol" if "vol" in df.columns else "volume"
    work = df.copy().sort_values(time_column)
    work["code"] = normalize_stock_code(code)
    work["trade_time"] = pd.to_datetime(work[time_column])
    work["freq"] = freq
    work["adjust"] = adjust
    work["volume2"] = work[volume_column] if volume_column in work.columns else None
    return work[["code", "trade_time", "freq", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "volume2", "amount", "adjust"]]


def _fetch_stock_daily_snapshot_frame(trade_date: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    try:
        df = call_tushare_api("daily", pro.daily, trade_date=trade_date.replace("-", ""))
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    work["code"] = work["ts_code"].astype(str).str.split(".").str[0]
    work["trade_time"] = pd.to_datetime(work["trade_date"])
    work["freq"] = "1d"
    work["adjust"] = "none"
    work["volume2"] = pd.to_numeric(work["vol"], errors="coerce") if "vol" in work.columns else None
    for column in ["open", "high", "low", "close", "pre_close", "change", "pct_chg", "amount"]:
        if column not in work.columns:
            work[column] = None
    return work[["code", "trade_time", "freq", "open", "high", "low", "close", "pre_close", "change", "pct_chg", "volume2", "amount", "adjust"]]


def _frame_to_stock_quotes(df: pd.DataFrame, freq: str) -> list[StockQuoteItem]:
    if df.empty:
        return []
    items: list[StockQuoteItem] = []
    for _, row in df.sort_values("trade_time").iterrows():
        items.append(
            StockQuoteItem(
                code=str(row["code"]),
                trade_time=format_datetime_value(row["trade_time"], freq),
                freq=str(row["freq"]),
                open=float(row["open"]) if pd.notna(row["open"]) else None,
                high=float(row["high"]) if pd.notna(row["high"]) else None,
                low=float(row["low"]) if pd.notna(row["low"]) else None,
                close=float(row["close"]) if pd.notna(row["close"]) else None,
                pre_close=float(row["pre_close"]) if pd.notna(row["pre_close"]) else None,
                change=float(row["change"]) if pd.notna(row["change"]) else None,
                pct_chg=float(row["pct_chg"]) if pd.notna(row["pct_chg"]) else None,
                volume=float(row["volume2"]) if pd.notna(row["volume2"]) else None,
                amount=float(row["amount"]) if pd.notna(row["amount"]) else None,
                adjust=str(row["adjust"]),
            )
        )
    return items


def get_stock_quotes(
    codes: list[str],
    freq: str,
    trade_date: str,
    start_date: str,
    end_date: str,
    start_time: str,
    end_time: str,
    count: int | None,
    adjust: str,
) -> list[StockQuoteItem]:
    if freq == "tick":
        return []
    request_start_dt, request_end_dt = build_time_bounds(trade_date, start_date, end_date, start_time, end_time, count, freq in INTRADAY_RULES)
    items: list[StockQuoteItem] = []
    for code in codes:
        cache_path = build_cache_path("tushare", ["stocks", "quotes"], {"code": normalize_stock_code(code), "freq": freq, "adjust": adjust})
        cache_df = read_cache_frame(cache_path)
        fetch_start_dt = request_start_dt
        fetch_end_dt = request_end_dt
        if fetch_start_dt is None and fetch_end_dt is None:
            fetch_end_dt = datetime.now()
            fetch_start_dt = fetch_end_dt - timedelta(days=30)
        range_start = fetch_start_dt.strftime("%Y%m%d") if fetch_start_dt else ""
        range_end = fetch_end_dt.strftime("%Y%m%d") if fetch_end_dt else ""
        missing_ranges = plan_missing_ranges(cache_df, "trade_time", range_start, range_end, "day")
        fetched_frames: list[pd.DataFrame] = []
        for missing_start, missing_end in missing_ranges:
            start_dt = datetime.strptime(missing_start, "%Y%m%d")
            end_dt = datetime.strptime(missing_end, "%Y%m%d") + timedelta(hours=23, minutes=59, seconds=59)
            fetched_df = _fetch_stock_quotes_frame(code, freq, start_dt, end_dt, adjust)
            if not fetched_df.empty:
                fetched_frames.append(fetched_df)
        if cache_df.empty and not fetched_frames:
            fetched_df = _fetch_stock_quotes_frame(code, freq, fetch_start_dt, fetch_end_dt, adjust)
            if not fetched_df.empty:
                fetched_frames.append(fetched_df)
        if fetched_frames:
            merged_cache = merge_cache_frame(cache_df, pd.concat(fetched_frames, ignore_index=True), ["code", "trade_time", "freq"], ["trade_time"])
            write_cache_frame(cache_path, merged_cache)
            cache_df = merged_cache
        filtered_df = filter_frame_by_datetime_range(cache_df, "trade_time", request_start_dt, request_end_dt)
        filtered_df = latest_n_rows(filtered_df, "trade_time", count)
        items.extend(_frame_to_stock_quotes(filtered_df, freq))
    return items


def get_stock_daily_snapshot(trade_date: str) -> list[StockQuoteItem]:
    actual_trade_date = format_date_value(trade_date)
    if actual_trade_date == "":
        return []
    cache_path = build_cache_path("tushare", ["stocks", "quotes", "daily-snapshot"], {"trade_date": actual_trade_date.replace("-", "")})
    cache_df = read_cache_frame(cache_path)
    if cache_df.empty:
        fetched_df = _fetch_stock_daily_snapshot_frame(actual_trade_date)
        if not fetched_df.empty:
            write_cache_frame(cache_path, fetched_df)
            cache_df = fetched_df
    if cache_df.empty:
        return []
    filtered_df = filter_frame_by_date_range(cache_df, "trade_time", actual_trade_date, actual_trade_date)
    return _frame_to_stock_quotes(filtered_df, "1d")


def get_stock_daily_snapshot_full(trade_date: str) -> list[StockQuoteItem]:
    return get_stock_daily_snapshot(trade_date)


def _rsi(series: pd.Series, period: int) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(period, min_periods=period).mean()
    avg_loss = loss.rolling(period, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    return 100 - 100 / (1 + rs)


def get_technical_factors(code: str, trade_date: str, start_date: str, end_date: str, adjust: str) -> list[TechnicalFactorItem]:
    quote_items = get_stock_quotes([code], "1d", trade_date, start_date, end_date, "", "", None, adjust)
    if quote_items == []:
        return []
    frame = pd.DataFrame([item.model_dump() for item in quote_items])
    frame["trade_date"] = frame["trade_time"].astype(str)
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame["high"] = pd.to_numeric(frame["high"], errors="coerce")
    frame["low"] = pd.to_numeric(frame["low"], errors="coerce")
    frame = frame.sort_values("trade_date").reset_index(drop=True)
    frame["ma5"] = frame["close"].rolling(5, min_periods=5).mean()
    frame["ma10"] = frame["close"].rolling(10, min_periods=10).mean()
    frame["ma20"] = frame["close"].rolling(20, min_periods=20).mean()
    frame["ma60"] = frame["close"].rolling(60, min_periods=60).mean()
    frame["ema12"] = frame["close"].ewm(span=12, adjust=False).mean()
    frame["ema26"] = frame["close"].ewm(span=26, adjust=False).mean()
    frame["dif"] = frame["ema12"] - frame["ema26"]
    frame["dea"] = frame["dif"].ewm(span=9, adjust=False).mean()
    frame["macd"] = (frame["dif"] - frame["dea"]) * 2
    frame["rsi6"] = _rsi(frame["close"], 6)
    frame["rsi12"] = _rsi(frame["close"], 12)
    frame["rsi24"] = _rsi(frame["close"], 24)
    low_n = frame["low"].rolling(9, min_periods=9).min()
    high_n = frame["high"].rolling(9, min_periods=9).max()
    rsv = (frame["close"] - low_n) / (high_n - low_n).replace(0, pd.NA) * 100
    frame["kdj_k"] = rsv.ewm(com=2, adjust=False).mean()
    frame["kdj_d"] = frame["kdj_k"].ewm(com=2, adjust=False).mean()
    frame["kdj_j"] = 3 * frame["kdj_k"] - 2 * frame["kdj_d"]
    boll_mid = frame["close"].rolling(20, min_periods=20).mean()
    boll_std = frame["close"].rolling(20, min_periods=20).std()
    frame["boll_upper"] = boll_mid + 2 * boll_std
    frame["boll_mid"] = boll_mid
    frame["boll_lower"] = boll_mid - 2 * boll_std
    return [
        TechnicalFactorItem(
            code=str(row["code"]),
            trade_date=str(row["trade_date"]),
            adjust=adjust,
            ma5=float(row["ma5"]) if pd.notna(row["ma5"]) else None,
            ma10=float(row["ma10"]) if pd.notna(row["ma10"]) else None,
            ma20=float(row["ma20"]) if pd.notna(row["ma20"]) else None,
            ma60=float(row["ma60"]) if pd.notna(row["ma60"]) else None,
            ema12=float(row["ema12"]) if pd.notna(row["ema12"]) else None,
            ema26=float(row["ema26"]) if pd.notna(row["ema26"]) else None,
            dif=float(row["dif"]) if pd.notna(row["dif"]) else None,
            dea=float(row["dea"]) if pd.notna(row["dea"]) else None,
            macd=float(row["macd"]) if pd.notna(row["macd"]) else None,
            rsi6=float(row["rsi6"]) if pd.notna(row["rsi6"]) else None,
            rsi12=float(row["rsi12"]) if pd.notna(row["rsi12"]) else None,
            rsi24=float(row["rsi24"]) if pd.notna(row["rsi24"]) else None,
            kdj_k=float(row["kdj_k"]) if pd.notna(row["kdj_k"]) else None,
            kdj_d=float(row["kdj_d"]) if pd.notna(row["kdj_d"]) else None,
            kdj_j=float(row["kdj_j"]) if pd.notna(row["kdj_j"]) else None,
            boll_upper=float(row["boll_upper"]) if pd.notna(row["boll_upper"]) else None,
            boll_mid=float(row["boll_mid"]) if pd.notna(row["boll_mid"]) else None,
            boll_lower=float(row["boll_lower"]) if pd.notna(row["boll_lower"]) else None,
        )
        for _, row in frame.iterrows()
    ]


def get_shareholder_changes(code: str, trade_date: str, start_date: str, end_date: str) -> list[ShareholderChangeItem]:
    count_items = get_shareholder_count(code, trade_date, start_date, end_date)
    rows: list[ShareholderChangeItem] = []
    previous_count: int | None = None
    for item in sorted(count_items, key=lambda value: value.trade_date):
        change_count = item.holder_count - previous_count if item.holder_count is not None and previous_count is not None else None
        change_pct = None
        if change_count is not None and previous_count not in {None, 0}:
            change_pct = change_count / previous_count * 100
        rows.append(ShareholderChangeItem(code=item.code, trade_date=item.trade_date, holder_count=item.holder_count, change_count=change_count, change_pct=change_pct))
        previous_count = item.holder_count
    return rows


def _fetch_money_flow_frame(code: str, start_value: str, end_value: str, view: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    try:
        df = call_tushare_api("moneyflow", pro.moneyflow, ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["view"] = view
    work["main_inflow"] = (work["buy_lg_amount"].fillna(0) + work["buy_elg_amount"].fillna(0)).astype(float)
    work["main_outflow"] = (work["sell_lg_amount"].fillna(0) + work["sell_elg_amount"].fillna(0)).astype(float)
    work["net_inflow"] = work["net_mf_amount"]
    return work[["code", "trade_date", "view", "main_inflow", "main_outflow", "net_inflow"]]


def get_stock_money_flow(code: str, trade_date: str, start_date: str, end_date: str, view: str) -> list[StockMoneyFlowItem]:
    actual_start = trade_date or start_date
    actual_end = trade_date or end_date
    if not actual_start and not actual_end:
        actual_end = datetime.now().strftime(DATE_FORMAT)
        actual_start = (datetime.now() - timedelta(days=30)).strftime(DATE_FORMAT)
    elif not actual_start:
        actual_start = actual_end
    elif not actual_end:
        actual_end = actual_start
    cache_path = build_cache_path("tushare", ["stocks", "indicators", "money-flow"], {"code": normalize_stock_code(code), "view": view})
    cache_df = read_cache_frame(cache_path)
    missing_ranges = plan_missing_ranges(cache_df, "trade_date", actual_start, actual_end, "day")
    fetched_frames: list[pd.DataFrame] = []
    for missing_start, missing_end in missing_ranges:
        fetched_df = _fetch_money_flow_frame(code, missing_start, missing_end, view)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if cache_df.empty and not fetched_frames:
        fetched_df = _fetch_money_flow_frame(code, actual_start, actual_end, view)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if fetched_frames:
        merged_cache = merge_cache_frame(cache_df, pd.concat(fetched_frames, ignore_index=True), ["code", "trade_date", "view"], ["trade_date"])
        write_cache_frame(cache_path, merged_cache)
        cache_df = merged_cache
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    items: list[StockMoneyFlowItem] = []
    for _, row in filtered_df.sort_values("trade_date").iterrows():
        items.append(
            StockMoneyFlowItem(
                code=str(row["code"]),
                trade_date=str(row["trade_date"]),
                view=str(row["view"]),
                main_inflow=float(row["main_inflow"]) if pd.notna(row["main_inflow"]) else None,
                main_outflow=float(row["main_outflow"]) if pd.notna(row["main_outflow"]) else None,
                net_inflow=float(row["net_inflow"]) if pd.notna(row["net_inflow"]) else None,
            )
        )
    return items


def board_code_to_ts(board_code: str) -> str:
    text = board_code.strip().upper()
    if not text:
        return ""
    if "." in text:
        return text
    return f"{text}.TI"


def _fetch_board_money_flow_frame(board_code: str, start_value: str, end_value: str, scope: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    fetch_name = "moneyflow_ind_ths" if scope == "industry" else "moneyflow_cnt_ths"
    fetcher = getattr(pro, fetch_name, None)
    if fetcher is None:
        return pd.DataFrame()
    try:
        df = call_tushare_api(fetch_name, fetcher, ts_code=board_code_to_ts(board_code), start_date=start_value, end_date=end_value)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    code_column = "ts_code" if "ts_code" in work.columns else "code"
    work["board_code"] = work[code_column].astype(str).str.split(".").str[0]
    work["scope"] = scope
    work["inflow"] = work["net_buy_amount"] if "net_buy_amount" in work.columns else None
    work["outflow"] = work["net_sell_amount"] if "net_sell_amount" in work.columns else None
    work["net_inflow"] = work["net_amount"] if "net_amount" in work.columns else None
    return work[["board_code", "trade_date", "scope", "inflow", "outflow", "net_inflow"]]


def get_board_money_flow(board_code: str, trade_date: str, start_date: str, end_date: str, scope: str) -> list[BoardMoneyFlowItem]:
    actual_start = trade_date or start_date
    actual_end = trade_date or end_date
    if not actual_start and not actual_end:
        actual_end = datetime.now().strftime(DATE_FORMAT)
        actual_start = (datetime.now() - timedelta(days=30)).strftime(DATE_FORMAT)
    elif not actual_start:
        actual_start = actual_end
    elif not actual_end:
        actual_end = actual_start
    cache_path = build_cache_path("tushare", ["boards", "indicators", "money-flow"], {"board_code": board_code, "scope": scope})
    cache_df = read_cache_frame(cache_path)
    missing_ranges = plan_missing_ranges(cache_df, "trade_date", actual_start, actual_end, "day")
    fetched_frames: list[pd.DataFrame] = []
    for missing_start, missing_end in missing_ranges:
        fetched_df = _fetch_board_money_flow_frame(board_code, missing_start, missing_end, scope)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if cache_df.empty and not fetched_frames:
        fetched_df = _fetch_board_money_flow_frame(board_code, actual_start, actual_end, scope)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if fetched_frames:
        merged_cache = merge_cache_frame(cache_df, pd.concat(fetched_frames, ignore_index=True), ["board_code", "trade_date", "scope"], ["trade_date"])
        write_cache_frame(cache_path, merged_cache)
        cache_df = merged_cache
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    items: list[BoardMoneyFlowItem] = []
    for _, row in filtered_df.sort_values("trade_date").iterrows():
        items.append(
            BoardMoneyFlowItem(
                board_code=str(row["board_code"]),
                trade_date=str(row["trade_date"]),
                scope=str(row["scope"]),
                inflow=float(row["inflow"]) if pd.notna(row["inflow"]) else None,
                outflow=float(row["outflow"]) if pd.notna(row["outflow"]) else None,
                net_inflow=float(row["net_inflow"]) if pd.notna(row["net_inflow"]) else None,
            )
        )
    return items


def _fetch_market_capital_flow_frame(start_value: str, end_value: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    fetcher = getattr(pro, "moneyflow_mkt_dc", None)
    if fetcher is None:
        return pd.DataFrame()
    try:
        df = call_tushare_api("moneyflow_mkt_dc", fetcher, start_date=start_value, end_date=end_value)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    work["market"] = "all"
    work["main_inflow"] = None
    work["main_outflow"] = None
    if "buy_elg_amount" in work.columns and "buy_lg_amount" in work.columns:
        net_large = work["buy_elg_amount"].fillna(0) + work["buy_lg_amount"].fillna(0)
        work["main_inflow"] = net_large.where(net_large > 0)
        work["main_outflow"] = (-net_large).where(net_large < 0)
    work["net_inflow"] = work["net_amount"] if "net_amount" in work.columns else None
    return work[["trade_date", "market", "main_inflow", "main_outflow", "net_inflow"]]


def get_market_capital_flow(trade_date: str, start_date: str, end_date: str) -> list[MarketCapitalFlowItem]:
    actual_start = trade_date or start_date
    actual_end = trade_date or end_date
    if not actual_start and not actual_end:
        actual_end = datetime.now().strftime(DATE_FORMAT)
        actual_start = (datetime.now() - timedelta(days=30)).strftime(DATE_FORMAT)
    elif not actual_start:
        actual_start = actual_end
    elif not actual_end:
        actual_end = actual_start
    cache_path = build_cache_path("tushare", ["markets", "indicators", "main-capital-flow"], {"market": "all"})
    cache_df = read_cache_frame(cache_path)
    missing_ranges = plan_missing_ranges(cache_df, "trade_date", actual_start, actual_end, "day")
    fetched_frames: list[pd.DataFrame] = []
    for missing_start, missing_end in missing_ranges:
        fetched_df = _fetch_market_capital_flow_frame(missing_start, missing_end)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if cache_df.empty and not fetched_frames:
        fetched_df = _fetch_market_capital_flow_frame(actual_start, actual_end)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if fetched_frames:
        merged_cache = merge_cache_frame(cache_df, pd.concat(fetched_frames, ignore_index=True), ["trade_date", "market"], ["trade_date"])
        write_cache_frame(cache_path, merged_cache)
        cache_df = merged_cache
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    items: list[MarketCapitalFlowItem] = []
    for _, row in filtered_df.sort_values("trade_date").iterrows():
        items.append(
            MarketCapitalFlowItem(
                trade_date=str(row["trade_date"]),
                market=str(row["market"]),
                main_inflow=float(row["main_inflow"]) if pd.notna(row["main_inflow"]) else None,
                main_outflow=float(row["main_outflow"]) if pd.notna(row["main_outflow"]) else None,
                net_inflow=float(row["net_inflow"]) if pd.notna(row["net_inflow"]) else None,
            )
        )
    return items


def trade_calendar_fetch_exchange(exchange: str) -> str:
    if exchange == "BSE":
        return "SSE"
    if exchange == "HKEX":
        return ""
    return exchange


def _fetch_trading_calendar_frame(exchange: str, start_value: str, end_value: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    fetch_exchange = trade_calendar_fetch_exchange(exchange)
    if not fetch_exchange:
        return pd.DataFrame()
    try:
        df = call_tushare_api("trade_cal", pro.trade_cal, exchange=fetch_exchange, start_date=start_value, end_date=end_value)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    work["exchange"] = exchange
    work["trade_date"] = work["cal_date"]
    return work[["exchange", "trade_date", "is_open"]]


def get_trading_calendar(exchange: str, start_date: str, end_date: str, is_open: bool | None) -> list[TradingCalendarItem]:
    actual_end = end_date or datetime.now().strftime(DATE_FORMAT)
    actual_start = start_date or (datetime.now() - timedelta(days=365)).strftime(DATE_FORMAT)
    cache_path = build_cache_path("tushare", ["markets", "calendar", "trading"], {"exchange": exchange})
    cache_df = read_cache_frame(cache_path)
    missing_ranges = plan_missing_ranges(cache_df, "trade_date", actual_start, actual_end, "day")
    fetched_frames: list[pd.DataFrame] = []
    for missing_start, missing_end in missing_ranges:
        fetched_df = _fetch_trading_calendar_frame(exchange, missing_start, missing_end)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if cache_df.empty and not fetched_frames:
        fetched_df = _fetch_trading_calendar_frame(exchange, actual_start, actual_end)
        if not fetched_df.empty:
            fetched_frames.append(fetched_df)
    if fetched_frames:
        merged_cache = merge_cache_frame(cache_df, pd.concat(fetched_frames, ignore_index=True), ["exchange", "trade_date"], ["trade_date"])
        write_cache_frame(cache_path, merged_cache)
        cache_df = merged_cache
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    if is_open is not None:
        filtered_df = filtered_df[filtered_df["is_open"].astype(str) == ("1" if is_open else "0")]
    items: list[TradingCalendarItem] = []
    for _, row in filtered_df.sort_values("trade_date").iterrows():
        items.append(
            TradingCalendarItem(
                exchange=str(row["exchange"]),
                trade_date=str(row["trade_date"]),
                is_open=str(row["is_open"]) == "1",
            )
        )
    return items


def _fetch_financial_frame(code: str, start_value: str, end_value: str, report_type: str) -> pd.DataFrame:
    pro = get_ts_pro()
    if pro is None:
        return pd.DataFrame()
    if report_type == "income_statement":
        fetch_name = "income"
        fields = "ts_code,ann_date,end_date,total_revenue,operate_profit,total_profit,n_income"
    elif report_type == "balance_sheet":
        fetch_name = "balancesheet"
        fields = "ts_code,ann_date,end_date,total_assets,total_liab,total_hldr_eqy_exc_min_int"
    else:
        fetch_name = "cashflow"
        fields = "ts_code,ann_date,end_date"
    fetcher = getattr(pro, fetch_name, None)
    if fetcher is None:
        return pd.DataFrame()
    try:
        df = call_tushare_api(fetch_name, fetcher, ts_code=stock_code_to_ts(code), start_date=start_value, end_date=end_value, fields=fields)
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    work = df.copy()
    work["code"] = normalize_stock_code(code)
    work["report_period"] = work["end_date"]
    work["report_type"] = report_type
    work["announce_date"] = work["ann_date"]
    work["revenue"] = work["total_revenue"] if "total_revenue" in work.columns else None
    work["operating_profit"] = work["operate_profit"] if "operate_profit" in work.columns else None
    work["total_profit"] = work["total_profit"] if "total_profit" in work.columns else None
    work["net_profit"] = work["n_income"] if "n_income" in work.columns else None
    work["total_assets2"] = work["total_assets"] if "total_assets" in work.columns else None
    work["total_liabilities2"] = work["total_liab"] if "total_liab" in work.columns else None
    work["equity2"] = work["total_hldr_eqy_exc_min_int"] if "total_hldr_eqy_exc_min_int" in work.columns else None
    return work[["code", "report_period", "report_type", "announce_date", "revenue", "operating_profit", "total_profit", "net_profit", "total_assets2", "total_liabilities2", "equity2"]]


def get_stock_financial_statements(
    codes: list[str],
    report_period: str,
    start_period: str,
    end_period: str,
    report_type: str,
) -> list[StockFinancialStatementItem]:
    start_value = start_period or report_period
    end_value = end_period or report_period
    if not start_value and not end_value:
        end_value = datetime.now().strftime("%Y1231")
        start_value = f"{datetime.now().year - 2}0101"
    elif not start_value:
        start_value = end_value
    elif not end_value:
        end_value = start_value
    items: list[StockFinancialStatementItem] = []
    for code in codes:
        cache_path = build_cache_path("tushare", ["stocks", "finance", "statements"], {"code": normalize_stock_code(code), "report_type": report_type})
        cache_df = read_cache_frame(cache_path)
        missing_ranges = plan_missing_ranges(cache_df, "report_period", start_value, end_value, "quarter")
        fetched_frames: list[pd.DataFrame] = []
        for missing_start, missing_end in missing_ranges:
            fetched_df = _fetch_financial_frame(code, missing_start, missing_end, report_type)
            if not fetched_df.empty:
                fetched_frames.append(fetched_df)
        if cache_df.empty and not fetched_frames:
            fetched_df = _fetch_financial_frame(code, start_value, end_value, report_type)
            if not fetched_df.empty:
                fetched_frames.append(fetched_df)
        if fetched_frames:
            merged_cache = merge_cache_frame(cache_df, pd.concat(fetched_frames, ignore_index=True), ["code", "report_period", "report_type", "announce_date"], ["report_period", "announce_date"])
            write_cache_frame(cache_path, merged_cache)
            cache_df = merged_cache
        filtered_df = filter_frame_by_date_range(cache_df, "report_period", start_value, end_value)
        required_columns = {"code", "report_period", "report_type", "announce_date"}
        if filtered_df.empty or not required_columns.issubset(set(filtered_df.columns)):
            continue
        for _, row in filtered_df.sort_values(["report_period", "announce_date"]).iterrows():
            items.append(
                StockFinancialStatementItem(
                    code=str(row["code"]),
                    report_period=str(row["report_period"]),
                    report_type=str(row["report_type"]),
                    announce_date=str(row["announce_date"]),
                    revenue=float(row["revenue"]) if pd.notna(row["revenue"]) else None,
                    operating_profit=float(row["operating_profit"]) if pd.notna(row["operating_profit"]) else None,
                    total_profit=float(row["total_profit"]) if pd.notna(row["total_profit"]) else None,
                    net_profit=float(row["net_profit"]) if pd.notna(row["net_profit"]) else None,
                    total_assets=float(row["total_assets2"]) if pd.notna(row["total_assets2"]) else None,
                    total_liabilities=float(row["total_liabilities2"]) if pd.notna(row["total_liabilities2"]) else None,
                    equity=float(row["equity2"]) if pd.notna(row["equity2"]) else None,
                )
            )
    return items


