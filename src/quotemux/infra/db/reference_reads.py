from __future__ import annotations

import pandas as pd

from quotemux.infra.db.client import query_dataframe


def load_stock_catalog_frame(codes: list[str], name: str, market: str, listed_filter: str) -> pd.DataFrame:
    where_clauses = ["code <> '000000'"]
    params: list[object] = []
    if codes:
        where_clauses.append("code = any(%s)")
        params.append(codes)
    if name:
        where_clauses.append("name ilike %s")
        params.append(f"%{name}%")
    if market:
        where_clauses.append("market = %s")
        params.append(market)
    if listed_filter == "listed":
        where_clauses.append("(delisted_date is null or delisted_date >= current_date)")
    elif listed_filter == "delisted":
        where_clauses.append("delisted_date < current_date")
    query = f"""
        select
            market,
            code,
            name,
            board_type,
            listed_date::text as listed_date,
            delisted_date::text as delisted_date
        from ref.stock
        where {' and '.join(where_clauses)}
        order by code
    """
    return query_dataframe(query, tuple(params))


def load_stock_active_codes_frame(trade_date: str) -> pd.DataFrame:
    if not trade_date:
        return pd.DataFrame()
    query = """
        select
            code
        from ref.stock
        where code <> '000000'
          and listed_date <= %s
          and (delisted_date is null or delisted_date >= %s)
        order by code
    """
    return query_dataframe(query, (trade_date, trade_date))


def load_stock_name_history_frame(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    where_clauses = ["code = %s"]
    params: list[object] = [code]
    if start_date:
        where_clauses.append("valid_from >= %s")
        params.append(start_date)
    if end_date:
        where_clauses.append("valid_from <= %s")
        params.append(end_date)
    query = f"""
        select
            code,
            name,
            valid_from::text as valid_from,
            valid_to::text as valid_to
        from ref.stock_name_history
        where {' and '.join(where_clauses)}
        order by valid_from
    """
    return query_dataframe(query, tuple(params))


def load_stock_hl_frame(code: str, trade_date: str, start_date: str, end_date: str) -> pd.DataFrame:
    where_clauses = ["code = %s"]
    params: list[object] = [code]
    if trade_date:
        where_clauses.append("trade_date = %s")
        params.append(trade_date)
    else:
        if start_date:
            where_clauses.append("trade_date >= %s")
            params.append(start_date)
        if end_date:
            where_clauses.append("trade_date <= %s")
            params.append(end_date)
    query = f"""
        select
            code,
            trade_date::text as trade_date,
            h_time::text as h_time,
            l_time::text as l_time
        from fact.stock_daily_1d
        where {' and '.join(where_clauses)}
        order by trade_date
    """
    return query_dataframe(query, tuple(params))


def load_board_catalog_frame(status_filter: str) -> pd.DataFrame:
    where_clauses = ["board_code <> '000000'"]
    if status_filter == "active":
        where_clauses.append("(delisted_date is null or delisted_date >= current_date)")
    elif status_filter == "inactive":
        where_clauses.append("delisted_date < current_date")
    query = f"""
        select
            board_code,
            board_type,
            name,
            listed_date::text as listed_date,
            delisted_date::text as delisted_date
        from ref.board
        where {' and '.join(where_clauses)}
        order by board_code
    """
    return query_dataframe(query)


def load_board_members_frame(board_code: str, trade_date: str) -> pd.DataFrame:
    query = """
        select
            m.board_code,
            m.stock_code as code,
            coalesce(s.name, '') as name,
            m.valid_from::text as join_date
        from ref.board_stock_membership m
        left join ref.stock s on s.market = m.stock_market and s.code = m.stock_code
        where m.board_code = %s
          and m.valid_from <= %s
          and (m.valid_to is null or m.valid_to >= %s)
        order by m.stock_code
    """
    return query_dataframe(query, (board_code, trade_date, trade_date))


def load_index_catalog_frame(index_codes: list[str]) -> pd.DataFrame:
    where_clauses = []
    params: list[object] = []
    if index_codes:
        where_clauses.append("s.index_code = any(%s)")
        params.append(index_codes)
    where_sql = f"where {' and '.join(where_clauses)}" if where_clauses else ""
    query = f"""
        with summary as (
            select
                index_code,
                min(trade_date) as first_trade_date,
                max(trade_date) as last_trade_date
            from fact.index_bar_1d
            group by index_code
        ),
        latest as (
            select distinct on (index_code)
                index_code,
                name
            from fact.index_bar_1d
            order by index_code, trade_date desc
        )
        select
            s.index_code,
            coalesce(l.name, s.index_code) as index_name,
            s.first_trade_date::text as list_date,
            s.last_trade_date::text as last_trade_date
        from summary s
        left join latest l on l.index_code = s.index_code
        {where_sql}
        order by s.index_code
    """
    return query_dataframe(query, tuple(params))


def load_trade_calendar_frame(start_date: str, end_date: str, is_open: bool | None) -> pd.DataFrame:
    where_clauses = ["exchange = 'SHSE'"]
    params: list[object] = []
    if start_date:
        where_clauses.append("trade_date >= %s")
        params.append(start_date)
    if end_date:
        where_clauses.append("trade_date <= %s")
        params.append(end_date)
    if is_open is not None:
        where_clauses.append("is_open = %s")
        params.append(is_open)
    query = f"""
        select
            trade_date::text as trade_date,
            is_open
        from ref.trade_calendar
        where {' and '.join(where_clauses)}
        order by trade_date
    """
    return query_dataframe(query, tuple(params))


def load_board_member_history_frame(board_code: str) -> pd.DataFrame:
    query = """
        select
            m.board_code,
            m.stock_code as code,
            coalesce(s.name, '') as name,
            m.valid_from::text as valid_from,
            m.valid_to::text as valid_to
        from ref.board_stock_membership m
        left join ref.stock s on s.market = m.stock_market and s.code = m.stock_code
        where m.board_code = %s
        order by m.stock_code, m.valid_from
    """
    return query_dataframe(query, (board_code,))

