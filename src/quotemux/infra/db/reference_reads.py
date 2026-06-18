from __future__ import annotations

import pandas as pd

from quotemux.infra.db.client import query_dataframe


def _normalize_exchange(value: str) -> str:
    text = value.upper()
    if text in {"SSE", "SH", "SHSE"}:
        return "SHSE"
    if text in {"SZSE", "SZ"}:
        return "SZSE"
    if text in {"BSE", "BJ", "BJSE"}:
        return "BJSE"
    return value


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
        with target_rows as (
            select
                m.board_code,
                m.stock_market,
                m.stock_code,
                m.valid_from,
                m.valid_to
            from ref.board_stock_membership m
            where m.board_code = %s
              and m.valid_from <= %s
              and (m.valid_to is null or m.valid_to >= %s)
        ),
        fallback_date as (
            select max(m.valid_from) as valid_from
            from ref.board_stock_membership m
            where m.board_code = %s
              and m.valid_from <= %s
        ),
        fallback_rows as (
            select
                m.board_code,
                m.stock_market,
                m.stock_code,
                m.valid_from,
                m.valid_to
            from ref.board_stock_membership m
            join fallback_date latest on latest.valid_from = m.valid_from
            where m.board_code = %s
              and not exists (select 1 from target_rows)
        )
        select
            m.board_code,
            m.stock_code as code,
            coalesce(s.name, '') as name,
            m.valid_from::text as join_date
        from (
            select * from target_rows
            union all
            select * from fallback_rows
        ) m
        left join ref.stock s on s.market = m.stock_market and s.code = m.stock_code
        order by m.stock_code
    """
    return query_dataframe(query, (board_code, trade_date, trade_date, board_code, trade_date, board_code))


def load_index_catalog_frame(index_codes: list[str]) -> pd.DataFrame:
    where_clauses = []
    params: list[object] = []
    if index_codes:
        where_clauses.append("index_code = any(%s)")
        params.append(index_codes)
    where_sql = f"where {' and '.join(where_clauses)}" if where_clauses else ""
    query = f"""
        select
            index_code,
            index_name,
            category,
            market,
            publisher,
            list_date::text as list_date,
            status
        from ref.index
        {where_sql}
        order by index_code
    """
    return query_dataframe(query, tuple(params))


def load_trade_calendar_frame(exchange: str, start_date: str, end_date: str, is_open: bool | None) -> pd.DataFrame:
    where_clauses = ["exchange = %s"]
    params: list[object] = [_normalize_exchange(exchange or "SSE")]
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

