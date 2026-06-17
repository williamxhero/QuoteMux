from __future__ import annotations

import pandas as pd

from quotemux.infra.db.client import execute_many, query_dataframe


def load_stock_daily_frame(codes: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """读取正式股票日线表。"""
    if not codes:
        return pd.DataFrame()
    where_clauses = ["day_rows.code = any(%s)"]
    params: list[object] = [codes]
    if start_date:
        where_clauses.append("day_rows.trade_date >= %s")
        params.append(start_date)
    if end_date:
        where_clauses.append("day_rows.trade_date <= %s")
        params.append(end_date)
    query = f"""
        select
            day_rows.code,
            day_rows.trade_date::text as trade_time,
            day_rows.open,
            day_rows.high,
            day_rows.low,
            day_rows.close,
            previous_rows.close as pre_close,
            day_rows.volume,
            day_rows.amount,
            day_rows.is_suspended,
            day_rows.is_st,
            day_rows.adj_factor,
            day_rows.labi_buy,
            day_rows.labi_sell,
            day_rows.mism_buy,
            day_rows.mism_sell
        from fact.stock_daily_1d day_rows
        left join lateral (
            select close
            from fact.stock_daily_1d previous_rows
            where previous_rows.code = day_rows.code
              and previous_rows.trade_date < day_rows.trade_date
            order by previous_rows.trade_date desc
            limit 1
        ) previous_rows on true
        where {' and '.join(where_clauses)}
        order by day_rows.code, day_rows.trade_date
    """
    return query_dataframe(query, tuple(params))


def load_stock_daily_previous_frame(codes: list[str], before_date: str) -> pd.DataFrame:
    """读取每只股票在指定日期前最近的一条日线。"""
    if not codes or not before_date:
        return pd.DataFrame()
    query = """
        select distinct on (day_rows.code)
            day_rows.code,
            day_rows.trade_date::text as trade_time,
            day_rows.open,
            day_rows.high,
            day_rows.low,
            day_rows.close,
            previous_rows.close as pre_close,
            day_rows.volume,
            day_rows.amount,
            day_rows.is_suspended,
            day_rows.is_st,
            day_rows.adj_factor,
            day_rows.labi_buy,
            day_rows.labi_sell,
            day_rows.mism_buy,
            day_rows.mism_sell
        from fact.stock_daily_1d day_rows
        left join lateral (
            select close
            from fact.stock_daily_1d previous_rows
            where previous_rows.code = day_rows.code
              and previous_rows.trade_date < day_rows.trade_date
            order by previous_rows.trade_date desc
            limit 1
        ) previous_rows on true
        where day_rows.code = any(%s)
          and day_rows.trade_date < %s
        order by day_rows.code, day_rows.trade_date desc
    """
    return query_dataframe(query, (codes, before_date))


def _stock_daily_snapshot_query() -> str:
    return """
        with day_rows as (
            select
                code,
                trade_date,
                open,
                high,
                low,
                close,
                volume,
                amount,
                is_suspended,
                is_st
            from fact.stock_daily_1d
            where trade_date = %s
        )
        select
            day_rows.code,
            day_rows.trade_date::text as trade_time,
            day_rows.open,
            day_rows.high,
            day_rows.low,
            day_rows.close,
            previous_rows.close as pre_close,
            day_rows.volume,
            day_rows.amount,
            day_rows.is_suspended,
            day_rows.is_st
        from day_rows
        left join lateral (
            select close
            from fact.stock_daily_1d previous_rows
            where previous_rows.code = day_rows.code
              and previous_rows.trade_date < day_rows.trade_date
            order by previous_rows.trade_date desc
            limit 1
        ) previous_rows on true
        order by day_rows.code
    """


def load_stock_daily_snapshot_full_frame(trade_date: str) -> pd.DataFrame:
    """读取单个交易日的全市场股票日线快照。"""
    if not trade_date:
        return pd.DataFrame()
    return query_dataframe(_stock_daily_snapshot_query(), (trade_date,))


def load_stock_daily_snapshot_frame(trade_date: str, limit: int, offset: int) -> pd.DataFrame:
    if not trade_date:
        return pd.DataFrame()
    query = f"""
        select *
        from ({_stock_daily_snapshot_query()}) as snapshot_rows
        limit %s
        offset %s
    """
    return query_dataframe(query, (trade_date, limit, offset))


def load_stock_daily_local_window_frame(start_date: str, end_date: str, limit: int | None, offset: int) -> pd.DataFrame:
    """????????????"""
    if not start_date or not end_date:
        return pd.DataFrame()
    query = """
        select
            day_rows.code,
            day_rows.trade_date::text as trade_time,
            day_rows.open,
            day_rows.high,
            day_rows.low,
            day_rows.close,
            previous_rows.close as pre_close,
            day_rows.volume,
            day_rows.amount,
            day_rows.is_suspended,
            day_rows.is_st
        from fact.stock_daily_1d day_rows
        left join lateral (
            select close
            from fact.stock_daily_1d previous_rows
            where previous_rows.code = day_rows.code
              and previous_rows.trade_date < day_rows.trade_date
            order by previous_rows.trade_date desc
            limit 1
        ) previous_rows on true
        where day_rows.trade_date >= %s
          and day_rows.trade_date <= %s
        order by day_rows.trade_date, day_rows.code
    """
    if limit is None:
        return query_dataframe(query, (start_date, end_date))
    paged_query = query + "\n        limit %s\n        offset %s"
    return query_dataframe(paged_query, (start_date, end_date, limit, offset))


def load_stock_intraday_frame(codes: list[str], start_time: object, end_time: object, freq: str = "1m") -> pd.DataFrame:
    if not codes:
        return pd.DataFrame()
    if freq == "30m":
        frame = load_stock_bar_30m_frame(codes, start_time, end_time)
        if not frame.empty:
            return frame
    where_clauses = ["code = any(%s)"]
    params: list[object] = [codes]
    if start_time is not None:
        where_clauses.append("bar_time >= %s")
        params.append(start_time)
    if end_time is not None:
        where_clauses.append("bar_time <= %s")
        params.append(end_time)
    query = f"""
        select
            code,
            bar_time as trade_time,
            open,
            high,
            low,
            close,
            volume,
            amount
        from fact.stock_bar_1m
        where {' and '.join(where_clauses)}
        order by code, bar_time
    """
    return query_dataframe(query, tuple(params))


def load_stock_bar_30m_frame(codes: list[str], start_time: object, end_time: object) -> pd.DataFrame:
    if not codes:
        return pd.DataFrame()
    where_clauses = ["code = any(%s)"]
    params: list[object] = [codes]
    if start_time is not None:
        where_clauses.append("bar_time >= %s")
        params.append(start_time)
    if end_time is not None:
        where_clauses.append("bar_time <= %s")
        params.append(end_time)
    query = f"""
        select
            code,
            bar_time as trade_time,
            open,
            high,
            low,
            close,
            volume,
            amount
        from fact.stock_bar_30m
        where {' and '.join(where_clauses)}
        order by code, bar_time
    """
    return query_dataframe(query, tuple(params))


def _stock_market(code: str) -> str:
    if code.startswith("6"):
        return "SHSE"
    if code.startswith(("4", "8")):
        return "BJSE"
    return "SZSE"


def upsert_stock_bar_30m_rows(rows: list[dict[str, object]]) -> bool:
    params: list[tuple[object, ...]] = []
    for row in rows:
        code = str(row["code"])
        params.append(
            (
                _stock_market(code),
                code,
                row["trade_time"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                int(float(row["volume"])),
                row["amount"],
            )
        )
    return execute_many(
        """
        insert into fact.stock_bar_30m (market, code, bar_time, open, high, low, close, volume, amount)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        on conflict (market, code, bar_time) do update set
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume,
            amount = excluded.amount,
            loaded_at = now()
        """,
        params,
    )


def load_board_daily_frame(board_codes: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    if not board_codes and not start_date and not end_date:
        return pd.DataFrame()
    where_clauses: list[str] = []
    params: list[object] = []
    if board_codes:
        where_clauses.append("board_code = any(%s)")
        params.append(board_codes)
    if start_date:
        where_clauses.append("trade_date >= %s")
        params.append(start_date)
    if end_date:
        where_clauses.append("trade_date <= %s")
        params.append(end_date)
    query = f"""
        select
            day_rows.board_code,
            day_rows.trade_date::text as trade_time,
            day_rows.open,
            day_rows.high,
            day_rows.low,
            day_rows.close,
            previous_rows.close as pre_close,
            day_rows.volume,
            day_rows.amount,
            day_rows.labi_buy,
            day_rows.labi_sell,
            day_rows.mism_buy,
            day_rows.mism_sell
        from fact.board_daily_1d day_rows
        left join lateral (
            select close
            from fact.board_daily_1d previous_rows
            where previous_rows.board_code = day_rows.board_code
              and previous_rows.trade_date < day_rows.trade_date
            order by previous_rows.trade_date desc
            limit 1
        ) previous_rows on true
        where {' and '.join(where_clauses)}
        order by day_rows.board_code, day_rows.trade_date
    """
    return query_dataframe(query, tuple(params))


def load_board_daily_snapshot_frame(trade_date: str, limit: int, offset: int) -> pd.DataFrame:
    if not trade_date:
        return pd.DataFrame()
    query = """
        with day_rows as (
            select
                board_code,
                trade_date,
                open,
                high,
                low,
                close,
                volume,
                amount,
                labi_buy,
                labi_sell,
                mism_buy,
                mism_sell
            from fact.board_daily_1d
            where trade_date = %s
        )
        select
            day_rows.board_code,
            day_rows.trade_date::text as trade_time,
            day_rows.open,
            day_rows.high,
            day_rows.low,
            day_rows.close,
            previous_rows.close as pre_close,
            day_rows.volume,
            day_rows.amount,
            day_rows.labi_buy,
            day_rows.labi_sell,
            day_rows.mism_buy,
            day_rows.mism_sell
        from day_rows
        left join lateral (
            select close
            from fact.board_daily_1d previous_rows
            where previous_rows.board_code = day_rows.board_code
              and previous_rows.trade_date < day_rows.trade_date
            order by previous_rows.trade_date desc
            limit 1
        ) previous_rows on true
        order by day_rows.board_code
        limit %s
        offset %s
    """
    return query_dataframe(query, (trade_date, limit, offset))


def load_index_daily_frame(index_codes: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    if not index_codes:
        return pd.DataFrame()
    where_clauses = ["day_rows.index_code = any(%s)"]
    params: list[object] = [index_codes]
    if start_date:
        where_clauses.append("day_rows.trade_date >= %s")
        params.append(start_date)
    if end_date:
        where_clauses.append("day_rows.trade_date <= %s")
        params.append(end_date)
    query = f"""
        select
            day_rows.index_code,
            day_rows.trade_date::text as trade_time,
            day_rows.open,
            day_rows.high,
            day_rows.low,
            day_rows.close,
            coalesce(day_rows.pre_close, previous_rows.close) as pre_close,
            day_rows.pct_chg,
            day_rows.volume,
            day_rows.amount
        from fact.index_bar_1d day_rows
        left join lateral (
            select close
            from fact.index_bar_1d previous_rows
            where previous_rows.index_code = day_rows.index_code
              and previous_rows.trade_date < day_rows.trade_date
            order by previous_rows.trade_date desc
            limit 1
        ) previous_rows on true
        where {' and '.join(where_clauses)}
        order by day_rows.index_code, day_rows.trade_date
    """
    return query_dataframe(query, tuple(params))

