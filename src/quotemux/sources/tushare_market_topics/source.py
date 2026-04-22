from __future__ import annotations

import pandas as pd

from quotemux.infra.cache.store import filter_frame_by_date_range
from platform_models import AuctionItem, BlockTradeItem, ConnectActiveTop10Item, ConnectCapitalFlowItem, ConnectQuotaItem, DragonTigerInstitutionItem, DragonTigerItem, HotMoneyDetailItem, HotMoneyProfileItem
from quotemux.infra.common import normalize_stock_code, split_csv, stock_code_to_ts
from quotemux.infra.tushare.helpers import normalize_date_range, plan_days, query_frame, read_cached_once, read_cached_ranges
from quotemux.sources.tushare_stocks.source import get_auctions


def _fetch_connect_flow_frame(start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("moneyflow_hsgt", start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    rows: list[dict[str, object]] = []
    for _, row in df.iterrows():
        trade_date = str(row["trade_date"])
        rows.extend(
            [
                {"trade_date": trade_date, "market": "northbound", "buy_amount": None, "sell_amount": None, "net_amount": row.get("north_money")},
                {"trade_date": trade_date, "market": "southbound", "buy_amount": None, "sell_amount": None, "net_amount": row.get("south_money")},
                {"trade_date": trade_date, "market": "sh_hk", "buy_amount": None, "sell_amount": None, "net_amount": row.get("ggt_ss")},
                {"trade_date": trade_date, "market": "sz_hk", "buy_amount": None, "sell_amount": None, "net_amount": row.get("ggt_sz")},
            ]
        )
    return pd.DataFrame(rows)


def get_connect_capital_flow(trade_date: str, start_date: str, end_date: str) -> list[ConnectCapitalFlowItem]:
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date, 120)
    cache_df = read_cached_ranges(["markets", "connect", "capital-flow"], {"scope": "all"}, "trade_date", actual_start, actual_end, "day", _fetch_connect_flow_frame)
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    return [
        ConnectCapitalFlowItem(
            trade_date=str(row["trade_date"]),
            market=str(row["market"]),
            buy_amount=float(row["buy_amount"]) if pd.notna(row["buy_amount"]) else None,
            sell_amount=float(row["sell_amount"]) if pd.notna(row["sell_amount"]) else None,
            net_amount=float(row["net_amount"]) if pd.notna(row["net_amount"]) else None,
        )
        for _, row in filtered_df.sort_values(["trade_date", "market"]).iterrows()
    ]


def get_connect_quotas(trade_date: str, start_date: str, end_date: str, market_type: str) -> list[ConnectQuotaItem]:
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date, 120)
    cache_df = read_cached_ranges(["markets", "connect", "quotas"], {"scope": "all"}, "trade_date", actual_start, actual_end, "day", _fetch_connect_quota_frame)
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    if filtered_df.empty or "trade_date" not in filtered_df.columns:
        return []
    if market_type:
        filtered_df = filtered_df[filtered_df["market"] == market_type]
    return [
        ConnectQuotaItem(
            trade_date=str(row["trade_date"]),
            market=str(row["market"]),
            quota_total=float(row["quota_total"]) if pd.notna(row["quota_total"]) else None,
            quota_balance=float(row["quota_balance"]) if pd.notna(row["quota_balance"]) else None,
            quota_used=float(row["quota_used"]) if pd.notna(row["quota_used"]) else None,
        )
        for _, row in filtered_df.sort_values(["trade_date", "market"]).iterrows()
    ]


def _fetch_connect_quota_frame(start_value: str, end_value: str) -> pd.DataFrame:
    df = query_frame("moneyflow_hsgt", start_date=start_value, end_date=end_value)
    if df.empty:
        return df
    quota_map = {
        "northbound": 1040.0,
        "southbound": 840.0,
        "sh_hk": 420.0,
        "sz_hk": 420.0,
    }
    rows: list[dict[str, object]] = []
    for _, row in df.iterrows():
        trade_date = str(row["trade_date"])
        values = {
            "northbound": row.get("north_money"),
            "southbound": row.get("south_money"),
            "sh_hk": row.get("ggt_ss"),
            "sz_hk": row.get("ggt_sz"),
        }
        for market, quota_used in values.items():
            quota_total = quota_map[market]
            quota_balance = None
            if pd.notna(quota_used):
                quota_balance = quota_total - float(quota_used)
            rows.append(
                {
                    "trade_date": trade_date,
                    "market": market,
                    "quota_total": quota_total,
                    "quota_balance": quota_balance,
                    "quota_used": quota_used,
                }
            )
    return pd.DataFrame(rows)


def _fetch_connect_top10_day(trade_date: str, market_type: str) -> pd.DataFrame:
    df = query_frame("hsgt_top10", trade_date=trade_date, market_type=market_type)
    if df.empty:
        df = query_frame("ggt_top10", trade_date=trade_date, market_type=market_type)
    if df.empty:
        return df
    work = df.copy()
    work["trade_date"] = trade_date
    work["market"] = market_type
    work["code"] = work["ts_code"].astype(str).str.split(".").str[0] if "ts_code" in work.columns else ""
    work["rank"] = work["rank"] if "rank" in work.columns else range(1, len(work) + 1)
    work["buy_amount"] = work["buy"] if "buy" in work.columns else work["buy_amount"] if "buy_amount" in work.columns else None
    work["sell_amount"] = work["sell"] if "sell" in work.columns else work["sell_amount"] if "sell_amount" in work.columns else None
    work["net_amount"] = work["net_amount"] if "net_amount" in work.columns else None
    return work[["trade_date", "market", "code", "name", "rank", "buy_amount", "sell_amount", "net_amount"]]


def get_connect_active_top10(trade_date: str, start_date: str, end_date: str, market_type: str, limit: int) -> list[ConnectActiveTop10Item]:
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date, 30)
    type_values = [market_type] if market_type else ["northbound", "southbound", "sh_hk", "sz_hk"]
    items: list[ConnectActiveTop10Item] = []
    for current_type in type_values:
        cache_df = read_cached_ranges(
            ["markets", "connect", "active-top10"],
            {"type": current_type},
            "trade_date",
            actual_start,
            actual_end,
            "day",
            lambda start_value, end_value: pd.concat([_fetch_connect_top10_day(day, current_type) for day in plan_days(start_value, end_value)], ignore_index=True) if plan_days(start_value, end_value) else pd.DataFrame(),
        )
        filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
        if filtered_df.empty or "trade_date" not in filtered_df.columns:
            continue
        for _, row in filtered_df.sort_values(["trade_date", "rank"]).head(limit).iterrows():
            items.append(
                ConnectActiveTop10Item(
                    trade_date=str(row["trade_date"]),
                    market=str(row["market"]),
                    code=str(row["code"]),
                    name=str(row["name"]) if pd.notna(row["name"]) else "",
                    rank=int(row["rank"]) if pd.notna(row["rank"]) else None,
                    buy_amount=float(row["buy_amount"]) if pd.notna(row["buy_amount"]) else None,
                    sell_amount=float(row["sell_amount"]) if pd.notna(row["sell_amount"]) else None,
                    net_amount=float(row["net_amount"]) if pd.notna(row["net_amount"]) else None,
                )
            )
    return items[:limit]


def _fetch_block_trade_frame(start_value: str, end_value: str, code: str) -> pd.DataFrame:
    kwargs: dict[str, object] = {"start_date": start_value, "end_date": end_value}
    if code:
        kwargs["ts_code"] = stock_code_to_ts(code)
    df = query_frame("block_trade", **kwargs)
    if df.empty:
        return df
    work = df.copy()
    work["trade_date"] = work["trade_date"].astype(str)
    work["code"] = work["ts_code"].astype(str).str.split(".").str[0] if "ts_code" in work.columns else normalize_stock_code(code)
    if "name" not in work.columns:
        work["name"] = ""
    work["buyer"] = work["buyer"] if "buyer" in work.columns else ""
    work["seller"] = work["seller"] if "seller" in work.columns else ""
    return work[["trade_date", "code", "name", "price", "vol", "amount", "buyer", "seller"]].rename(columns={"vol": "volume"})


def get_block_trades(trade_date: str, start_date: str, end_date: str, code: str, limit: int) -> list[BlockTradeItem]:
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date, 30)
    cache_df = read_cached_ranges(
        ["markets", "events", "block-trades"],
        {"code": normalize_stock_code(code) or "all"},
        "trade_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: _fetch_block_trade_frame(start_value, end_value, code),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    if filtered_df.empty or "trade_date" not in filtered_df.columns:
        return []
    return [
        BlockTradeItem(
            trade_date=str(row["trade_date"]),
            code=str(row["code"]),
            name=str(row["name"]) if pd.notna(row["name"]) else "",
            price=float(row["price"]) if pd.notna(row["price"]) else None,
            volume=float(row["volume"]) if pd.notna(row["volume"]) else None,
            amount=float(row["amount"]) if pd.notna(row["amount"]) else None,
            buyer=str(row["buyer"]) if pd.notna(row["buyer"]) else "",
            seller=str(row["seller"]) if pd.notna(row["seller"]) else "",
        )
        for _, row in filtered_df.sort_values(["trade_date", "code"]).head(limit).iterrows()
    ]


def _fetch_dragon_tiger_frame(start_value: str, end_value: str, code: str) -> pd.DataFrame:
    kwargs: dict[str, object] = {"start_date": start_value, "end_date": end_value}
    if code:
        kwargs["ts_code"] = stock_code_to_ts(code)
    df = query_frame("top_list", **kwargs)
    if df.empty:
        return df
    work = df.copy()
    work["trade_date"] = work["trade_date"].astype(str)
    work["code"] = work["ts_code"].astype(str).str.split(".").str[0] if "ts_code" in work.columns else normalize_stock_code(code)
    work["buy_amount"] = work["l_buy"] if "l_buy" in work.columns else None
    work["sell_amount"] = work["l_sell"] if "l_sell" in work.columns else None
    work["net_amount"] = work["net_amount"] if "net_amount" in work.columns else None
    return work[["trade_date", "code", "name", "reason", "buy_amount", "sell_amount", "net_amount"]]


def get_dragon_tiger(trade_date: str, start_date: str, end_date: str, code: str, limit: int) -> list[DragonTigerItem]:
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date, 30)
    cache_df = read_cached_ranges(
        ["markets", "participants", "dragon-tiger"],
        {"code": normalize_stock_code(code) or "all"},
        "trade_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: _fetch_dragon_tiger_frame(start_value, end_value, code),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    if filtered_df.empty or "trade_date" not in filtered_df.columns:
        return []
    return [
        DragonTigerItem(
            trade_date=str(row["trade_date"]),
            code=str(row["code"]),
            name=str(row["name"]) if pd.notna(row["name"]) else "",
            reason=str(row["reason"]) if pd.notna(row["reason"]) else "",
            buy_amount=float(row["buy_amount"]) if pd.notna(row["buy_amount"]) else None,
            sell_amount=float(row["sell_amount"]) if pd.notna(row["sell_amount"]) else None,
            net_amount=float(row["net_amount"]) if pd.notna(row["net_amount"]) else None,
        )
        for _, row in filtered_df.sort_values(["trade_date", "code"]).head(limit).iterrows()
    ]


def _fetch_dragon_tiger_inst_frame(start_value: str, end_value: str, code: str) -> pd.DataFrame:
    kwargs: dict[str, object] = {"start_date": start_value, "end_date": end_value}
    if code:
        kwargs["ts_code"] = stock_code_to_ts(code)
    df = query_frame("top_inst", **kwargs)
    if df.empty:
        return df
    work = df.copy()
    work["trade_date"] = work["trade_date"].astype(str)
    work["code"] = work["ts_code"].astype(str).str.split(".").str[0] if "ts_code" in work.columns else normalize_stock_code(code)
    work["buy_amount"] = work["buy"] if "buy" in work.columns else None
    work["sell_amount"] = work["sell"] if "sell" in work.columns else None
    work["net_amount"] = work["net_buy"] if "net_buy" in work.columns else work["net_amount"] if "net_amount" in work.columns else None
    work["institution_count"] = 1
    return work[["trade_date", "code", "name", "buy_amount", "sell_amount", "net_amount", "institution_count"]]


def get_dragon_tiger_institutions(trade_date: str, start_date: str, end_date: str, code: str, limit: int) -> list[DragonTigerInstitutionItem]:
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date, 30)
    cache_df = read_cached_ranges(
        ["markets", "participants", "dragon-tiger-institutions"],
        {"code": normalize_stock_code(code) or "all"},
        "trade_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: _fetch_dragon_tiger_inst_frame(start_value, end_value, code),
    )
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    if filtered_df.empty or "trade_date" not in filtered_df.columns:
        return []
    return [
        DragonTigerInstitutionItem(
            trade_date=str(row["trade_date"]),
            code=str(row["code"]),
            name=str(row["name"]) if pd.notna(row["name"]) else "",
            buy_amount=float(row["buy_amount"]) if pd.notna(row["buy_amount"]) else None,
            sell_amount=float(row["sell_amount"]) if pd.notna(row["sell_amount"]) else None,
            net_amount=float(row["net_amount"]) if pd.notna(row["net_amount"]) else None,
            institution_count=int(row["institution_count"]) if pd.notna(row["institution_count"]) else None,
        )
        for _, row in filtered_df.sort_values(["trade_date", "code"]).head(limit).iterrows()
    ]


def get_hot_money_profiles(name: str) -> list[HotMoneyProfileItem]:
    cache_df = read_cached_once(["markets", "participants", "hot-money"], {"scope": "all"}, lambda: query_frame("hm_list"))
    if cache_df.empty:
        return []
    work = cache_df.copy()
    if name:
        work = work[work["name"].astype(str).str.contains(name, na=False)]
    return [
        HotMoneyProfileItem(
            name=str(row["name"]) if pd.notna(row["name"]) else "",
            alias="",
            tag="",
            style=str(row["desc"]) if "desc" in work.columns and pd.notna(row["desc"]) else "",
        )
        for _, row in work.sort_values("name").iterrows()
    ]


def get_hot_money_details(trade_date: str, start_date: str, end_date: str, name: str, limit: int) -> list[HotMoneyDetailItem]:
    actual_start, actual_end = normalize_date_range(trade_date, start_date, end_date, 30)
    cache_df = read_cached_ranges(
        ["markets", "participants", "hot-money-details"],
        {"scope": "all"},
        "trade_date",
        actual_start,
        actual_end,
        "day",
        lambda start_value, end_value: query_frame("hm_detail", start_date=start_value, end_date=end_value),
    )
    if cache_df.empty:
        return []
    filtered_df = filter_frame_by_date_range(cache_df, "trade_date", actual_start, actual_end)
    if filtered_df.empty or "trade_date" not in filtered_df.columns or "name" not in filtered_df.columns:
        return []
    if name and "name" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["name"].astype(str).str.contains(name, na=False)]
    return [
        HotMoneyDetailItem(
            trade_date=str(row["trade_date"]),
            name=str(row["name"]) if "name" in filtered_df.columns and pd.notna(row["name"]) else "",
            code=str(row["ts_code"]).split(".")[0] if "ts_code" in filtered_df.columns and pd.notna(row["ts_code"]) else "",
            stock_name=str(row["stock_name"]) if "stock_name" in filtered_df.columns and pd.notna(row["stock_name"]) else "",
            buy_amount=float(row["buy_amount"]) if "buy_amount" in filtered_df.columns and pd.notna(row["buy_amount"]) else None,
            sell_amount=float(row["sell_amount"]) if "sell_amount" in filtered_df.columns and pd.notna(row["sell_amount"]) else None,
            net_amount=float(row["net_amount"]) if "net_amount" in filtered_df.columns and pd.notna(row["net_amount"]) else None,
        )
        for _, row in filtered_df.sort_values(["trade_date", "name"]).head(limit).iterrows()
    ]


def get_market_open_auctions(codes: str, trade_date: str) -> list[AuctionItem]:
    items: list[AuctionItem] = []
    for code in split_csv(codes):
        items.extend(get_auctions(code, "open", trade_date, "", ""))
    return items


