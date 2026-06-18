from __future__ import annotations

import pandas as pd

from quotemux.infra.db.client import execute_many, query_dataframe


def _existing_columns(table_schema: str, table_name: str) -> set[str]:
    frame = query_dataframe(
        """
        select column_name
        from information_schema.columns
        where table_schema = %s
          and table_name = %s
        """,
        (table_schema, table_name),
    )
    if frame.empty:
        return set()
    return {str(row["column_name"]) for _, row in frame.iterrows()}


def _optional_column(existing_columns: set[str], column_name: str) -> str:
    if column_name in existing_columns:
        return f"day_rows.{column_name}"
    return f"null as {column_name}"


def _daily_metric_selects(existing_columns: set[str], row_alias: str) -> str:
    pre_close_value = f"{row_alias}.pre_close" if "pre_close" in existing_columns else "null"
    change_value = f"{row_alias}.change" if "change" in existing_columns else "null"
    pct_chg_value = f"{row_alias}.pct_chg" if "pct_chg" in existing_columns else "null"
    previous_close = f"coalesce({pre_close_value}, {row_alias}.previous_close)"
    change_expr = f"coalesce({change_value}, {row_alias}.close - {previous_close})"
    pct_chg_expr = f"coalesce({pct_chg_value}, {change_expr} / nullif({previous_close}, 0) * 100)"
    return f"""
            {previous_close} as pre_close,
            {change_expr} as change,
            {pct_chg_expr} as pct_chg,
    """


def load_stock_daily_frame(codes: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    """读取正式股票日线表。"""
    if not codes:
        return pd.DataFrame()
    existing_columns = _existing_columns("fact", "stock_daily_1d")
    source_where_clauses = ["day_rows.code = any(%s)"]
    source_params: list[object] = [codes]
    if end_date:
        source_where_clauses.append("day_rows.trade_date <= %s")
        source_params.append(end_date)
    outer_where_clauses: list[str] = []
    outer_params: list[object] = []
    if start_date:
        outer_where_clauses.append("raw_rows.trade_date >= %s")
        outer_params.append(start_date)
    if end_date:
        outer_where_clauses.append("raw_rows.trade_date <= %s")
        outer_params.append(end_date)
    outer_where_sql = f"where {' and '.join(outer_where_clauses)}" if outer_where_clauses else ""
    query = f"""
        with raw_rows as (
            select
                day_rows.code,
                day_rows.trade_date,
                day_rows.open,
                day_rows.high,
                day_rows.low,
                day_rows.close,
                {_optional_column(existing_columns, "pre_close")},
                {_optional_column(existing_columns, "change")},
                {_optional_column(existing_columns, "pct_chg")},
                day_rows.volume,
                day_rows.amount,
                {_optional_column(existing_columns, "is_suspended")},
                {_optional_column(existing_columns, "is_st")},
                {_optional_column(existing_columns, "adj_factor")},
                {_optional_column(existing_columns, "labi_buy")},
                {_optional_column(existing_columns, "labi_sell")},
                {_optional_column(existing_columns, "mism_buy")},
                {_optional_column(existing_columns, "mism_sell")},
                lag(day_rows.close) over (partition by day_rows.code order by day_rows.trade_date) as previous_close
            from fact.stock_daily_1d day_rows
            where {' and '.join(source_where_clauses)}
        )
        select
            raw_rows.code,
            raw_rows.trade_date::text as trade_time,
            raw_rows.open,
            raw_rows.high,
            raw_rows.low,
            raw_rows.close,
            {_daily_metric_selects(existing_columns, "raw_rows")}
            raw_rows.volume,
            raw_rows.amount,
            raw_rows.is_suspended,
            raw_rows.is_st,
            raw_rows.adj_factor,
            raw_rows.labi_buy,
            raw_rows.labi_sell,
            raw_rows.mism_buy,
            raw_rows.mism_sell
        from raw_rows
        {outer_where_sql}
        order by raw_rows.code, raw_rows.trade_date
    """
    return query_dataframe(query, tuple([*source_params, *outer_params]))


def _stock_daily_snapshot_query() -> str:
    existing_columns = _existing_columns("fact", "stock_daily_1d")
    return f"""
        with day_rows as (
            select
                code,
                trade_date,
                open,
                high,
                low,
                close,
                {_optional_column(existing_columns, "pre_close")},
                {_optional_column(existing_columns, "change")},
                {_optional_column(existing_columns, "pct_chg")},
                volume,
                amount,
                {_optional_column(existing_columns, "is_suspended")},
                {_optional_column(existing_columns, "is_st")},
                lag(day_rows.close) over (partition by day_rows.code order by day_rows.trade_date) as previous_close
            from fact.stock_daily_1d day_rows
            where trade_date <= %s
        )
        select
            day_rows.code,
            day_rows.trade_date::text as trade_time,
            day_rows.open,
            day_rows.high,
            day_rows.low,
            day_rows.close,
            {_daily_metric_selects(existing_columns, "day_rows")}
            day_rows.volume,
            day_rows.amount,
            day_rows.is_suspended,
            day_rows.is_st
        from day_rows
        where day_rows.trade_date = %s
        order by day_rows.code
    """


def load_stock_daily_snapshot_full_frame(trade_date: str) -> pd.DataFrame:
    """读取单个交易日的全市场股票日线快照。"""
    if not trade_date:
        return pd.DataFrame()
    return query_dataframe(_stock_daily_snapshot_query(), (trade_date, trade_date))


def load_stock_daily_snapshot_frame(trade_date: str, limit: int, offset: int) -> pd.DataFrame:
    if not trade_date:
        return pd.DataFrame()
    query = f"""
        select *
        from ({_stock_daily_snapshot_query()}) as snapshot_rows
        limit %s
        offset %s
    """
    return query_dataframe(query, (trade_date, trade_date, limit, offset))


def load_stock_daily_local_window_frame(start_date: str, end_date: str, limit: int | None, offset: int) -> pd.DataFrame:
    """读取本地日线窗口。"""
    if not start_date or not end_date:
        return pd.DataFrame()
    existing_columns = _existing_columns("fact", "stock_daily_1d")
    query = f"""
        with raw_rows as (
            select
                day_rows.code,
                day_rows.trade_date,
                day_rows.open,
                day_rows.high,
                day_rows.low,
                day_rows.close,
                {_optional_column(existing_columns, "pre_close")},
                {_optional_column(existing_columns, "change")},
                {_optional_column(existing_columns, "pct_chg")},
                day_rows.volume,
                day_rows.amount,
                {_optional_column(existing_columns, "is_suspended")},
                {_optional_column(existing_columns, "is_st")},
                lag(day_rows.close) over (partition by day_rows.code order by day_rows.trade_date) as previous_close
            from fact.stock_daily_1d day_rows
            where day_rows.trade_date <= %s
        )
        select
            raw_rows.code,
            raw_rows.trade_date::text as trade_time,
            raw_rows.open,
            raw_rows.high,
            raw_rows.low,
            raw_rows.close,
            {_daily_metric_selects(existing_columns, "raw_rows")}
            raw_rows.volume,
            raw_rows.amount,
            raw_rows.is_suspended,
            raw_rows.is_st
        from raw_rows
        where raw_rows.trade_date >= %s
          and raw_rows.trade_date <= %s
        order by raw_rows.trade_date, raw_rows.code
    """
    if limit is None:
        return query_dataframe(query, (end_date, start_date, end_date))
    paged_query = query + "\n        limit %s\n        offset %s"
    return query_dataframe(paged_query, (end_date, start_date, end_date, limit, offset))


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
    if code.startswith(("4", "8", "9")):
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
    existing_columns = _existing_columns("fact", "board_daily_1d")
    where_clauses: list[str] = []
    params: list[object] = []
    if board_codes:
        where_clauses.append("day_rows.board_code = any(%s)")
        params.append(board_codes)
    if start_date:
        where_clauses.append("day_rows.trade_date >= %s")
        params.append(start_date)
    if end_date:
        where_clauses.append("day_rows.trade_date <= %s")
        params.append(end_date)
    query = f"""
        with scoped_rows as (
            select
                day_rows.*,
                lag(day_rows.close) over (partition by day_rows.board_code order by day_rows.trade_date) as previous_close
            from fact.board_daily_1d day_rows
            where {' and '.join(where_clauses)}
        )
        select
            day_rows.board_code,
            coalesce(board_ref.name, '') as board_name,
            day_rows.trade_date::text as trade_time,
            day_rows.open,
            day_rows.high,
            day_rows.low,
            day_rows.close,
            {_daily_metric_selects(existing_columns, "day_rows")}
            day_rows.volume,
            day_rows.amount,
            {_optional_column(existing_columns, "labi_buy")},
            {_optional_column(existing_columns, "labi_sell")},
            {_optional_column(existing_columns, "mism_buy")},
            {_optional_column(existing_columns, "mism_sell")}
        from scoped_rows day_rows
        left join ref.board board_ref on board_ref.board_code = day_rows.board_code
        order by day_rows.board_code, day_rows.trade_date
    """
    return query_dataframe(query, tuple(params))


def load_board_daily_snapshot_frame(trade_date: str, limit: int, offset: int) -> pd.DataFrame:
    if not trade_date:
        return pd.DataFrame()
    existing_columns = _existing_columns("fact", "board_daily_1d")
    query = f"""
        with scoped_rows as (
            select
                day_rows.*,
                lag(day_rows.close) over (partition by day_rows.board_code order by day_rows.trade_date) as previous_close
            from fact.board_daily_1d day_rows
            where day_rows.trade_date <= %s
        ),
        latest_rows as (
            select *
            from scoped_rows
            where trade_date = %s
        )
        select
            day_rows.board_code,
            coalesce(board_ref.name, '') as board_name,
            day_rows.trade_date::text as trade_time,
            day_rows.open,
            day_rows.high,
            day_rows.low,
            day_rows.close,
            {_daily_metric_selects(existing_columns, "day_rows")}
            day_rows.volume,
            day_rows.amount,
            {_optional_column(existing_columns, "labi_buy")},
            {_optional_column(existing_columns, "labi_sell")},
            {_optional_column(existing_columns, "mism_buy")},
            {_optional_column(existing_columns, "mism_sell")}
        from latest_rows day_rows
        left join ref.board board_ref on board_ref.board_code = day_rows.board_code
        order by day_rows.board_code
        limit %s
        offset %s
    """
    return query_dataframe(query, (trade_date, trade_date, limit, offset))


def _latest_complete_board_daily_date_cte(existing_columns: set[str]) -> str:
    pre_close_value = "day_rows.pre_close" if "pre_close" in existing_columns else "null"
    pct_chg_value = "day_rows.pct_chg" if "pct_chg" in existing_columns else "null"
    previous_close = f"coalesce({pre_close_value}, day_rows.previous_close)"
    pct_chg_expr = f"coalesce({pct_chg_value}, (day_rows.close - {previous_close}) / nullif({previous_close}, 0) * 100)"
    return f"""
        historical_rows as (
            select
                day_rows.board_code,
                day_rows.trade_date,
                day_rows.close,
                {pre_close_value} as pre_close,
                {pct_chg_value} as pct_chg,
                lag(day_rows.close) over (partition by day_rows.board_code order by day_rows.trade_date) as previous_close
            from fact.board_daily_1d day_rows
            where day_rows.trade_date < %s
        ),
        complete_dates as (
            select
                day_rows.trade_date,
                count(*) as row_count,
                count(*) filter (where {previous_close} is not null and {pct_chg_expr} is not null) as complete_count
            from historical_rows day_rows
            group by day_rows.trade_date
        ),
        latest_complete_date as (
            select trade_date
            from complete_dates
            where row_count = complete_count and row_count > 0
            order by trade_date desc
            limit 1
        )
    """


def load_latest_complete_board_daily_snapshot_codes(trade_date: str, limit: int, offset: int) -> list[str]:
    if not trade_date:
        return []
    existing_columns = _existing_columns("fact", "board_daily_1d")
    query = f"""
        with {_latest_complete_board_daily_date_cte(existing_columns)}
        select day_rows.board_code
        from fact.board_daily_1d day_rows
        join latest_complete_date latest on latest.trade_date = day_rows.trade_date
        order by day_rows.board_code
        limit %s
        offset %s
    """
    frame = query_dataframe(query, (trade_date, limit, offset))
    if frame.empty:
        return []
    return [str(row["board_code"]) for _, row in frame.iterrows()]


def load_latest_complete_board_daily_snapshot_frame(trade_date: str, limit: int, offset: int) -> pd.DataFrame:
    if not trade_date:
        return pd.DataFrame()
    existing_columns = _existing_columns("fact", "board_daily_1d")
    query = f"""
        with {_latest_complete_board_daily_date_cte(existing_columns)},
        scoped_rows as (
            select
                day_rows.*,
                lag(day_rows.close) over (partition by day_rows.board_code order by day_rows.trade_date) as previous_close
            from fact.board_daily_1d day_rows
            where day_rows.trade_date <= (select trade_date from latest_complete_date)
        ),
        snapshot_rows as (
            select *
            from scoped_rows
            where trade_date = (select trade_date from latest_complete_date)
        )
        select
            day_rows.board_code,
            coalesce(board_ref.name, '') as board_name,
            day_rows.trade_date::text as trade_time,
            day_rows.open,
            day_rows.high,
            day_rows.low,
            day_rows.close,
            {_daily_metric_selects(existing_columns, "day_rows")}
            day_rows.volume,
            day_rows.amount,
            {_optional_column(existing_columns, "labi_buy")},
            {_optional_column(existing_columns, "labi_sell")},
            {_optional_column(existing_columns, "mism_buy")},
            {_optional_column(existing_columns, "mism_sell")}
        from snapshot_rows day_rows
        left join ref.board board_ref on board_ref.board_code = day_rows.board_code
        order by day_rows.board_code
        limit %s
        offset %s
    """
    return query_dataframe(query, (trade_date, limit, offset))


def load_index_daily_frame(index_codes: list[str], start_date: str, end_date: str) -> pd.DataFrame:
    if not index_codes:
        return pd.DataFrame()
    existing_columns = _existing_columns("fact", "index_bar_1d")
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
            {_optional_column(existing_columns, "pre_close")},
            {_optional_column(existing_columns, "pct_chg")},
            {_optional_column(existing_columns, "volume")},
            {_optional_column(existing_columns, "amount")}
        from fact.index_bar_1d day_rows
        where {' and '.join(where_clauses)}
        order by day_rows.index_code, day_rows.trade_date
    """
    return query_dataframe(query, tuple(params))
