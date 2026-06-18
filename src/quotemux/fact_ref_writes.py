from __future__ import annotations

from typing import Callable, Sequence

from pydantic import BaseModel

from platform_models import BoardCatalogItem, BoardMemberHistoryItem, BoardMemberItem, BoardQuoteItem, IndexCatalogItem, IndexQuoteItem, NameHistoryItem, StockBasicInfo, StockQuoteItem, TradingCalendarItem
from quotemux.infra.common import format_date_value, format_datetime_value, normalize_index_code, normalize_stock_code
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


def _optional_update_assignments(existing_columns: set[str], column_names: tuple[str, ...]) -> str:
    assignments = [f"{column_name} = excluded.{column_name}" for column_name in column_names if column_name in existing_columns]
    if assignments == []:
        return ""
    return ",\n            " + ",\n            ".join(assignments)


def _stock_market(code: str) -> str:
    if code.startswith("6"):
        return "SHSE"
    if code.startswith(("4", "8", "9")):
        return "BJSE"
    return "SZSE"


def _exchange_to_ref(value: str) -> str:
    text = value.upper()
    if text in {"SSE", "SH", "SHSE", "STAR_MARKET"}:
        return "SHSE"
    if text in {"SZSE", "SZ", "CHI_NEXT"}:
        return "SZSE"
    if text in {"BSE", "BJ", "BJSE", "BEIJING"}:
        return "BJSE"
    return value


def _stock_status_to_delisted_date(item: StockBasicInfo) -> str:
    if item.delist_date:
        return format_date_value(item.delist_date)
    if item.list_status.upper() in {"D", "DELISTED", "INACTIVE"}:
        return format_date_value(item.delist_date)
    return ""


def _upsert_stock_daily(items: Sequence[StockQuoteItem]) -> bool:
    existing_columns = _existing_columns("fact", "stock_daily_1d")
    optional_columns = tuple(column_name for column_name in ("pre_close", "change", "pct_chg") if column_name in existing_columns)
    params: list[tuple[object, ...]] = []
    for item in items:
        if item.freq != "1d":
            continue
        code = normalize_stock_code(item.code).zfill(6)
        trade_date = format_date_value(item.trade_time)
        if code == "" or trade_date == "":
            continue
        optional_values = tuple(getattr(item, column_name) for column_name in optional_columns)
        params.append((
            _stock_market(code),
            code,
            trade_date,
            item.open,
            item.high,
            item.low,
            item.close,
            int(item.volume) if item.volume is not None else 0,
            item.amount,
            item.is_suspended,
            item.is_st,
            *optional_values,
        ))
    optional_column_sql = "".join(f", {column_name}" for column_name in optional_columns)
    optional_placeholder_sql = "".join(", %s" for _ in optional_columns)
    return execute_many(
        f"""
        insert into fact.stock_daily_1d (market, code, trade_date, open, high, low, close, volume, amount, is_suspended, is_st{optional_column_sql})
        values (%s, %s, %s::date, %s, %s, %s, %s, %s, %s, %s, %s{optional_placeholder_sql})
        on conflict (market, code, trade_date) do update set
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume,
            amount = excluded.amount,
            is_suspended = excluded.is_suspended,
            is_st = excluded.is_st{_optional_update_assignments(existing_columns, optional_columns)},
            loaded_at = now()
        """,
        params,
    )


def _upsert_stock_intraday(items: Sequence[StockQuoteItem]) -> bool:
    params_1m: list[tuple[object, ...]] = []
    params_30m: list[tuple[object, ...]] = []
    for item in items:
        if item.freq not in {"1m", "30m"}:
            continue
        code = normalize_stock_code(item.code).zfill(6)
        trade_time = format_datetime_value(item.trade_time, item.freq)
        if code == "" or trade_time == "":
            continue
        params = (
            _stock_market(code),
            code,
            trade_time,
            item.open,
            item.high,
            item.low,
            item.close,
            int(item.volume) if item.volume is not None else 0,
            item.amount,
        )
        if item.freq == "1m":
            params_1m.append(params)
        else:
            params_30m.append(params)
    query_1m = """
        insert into fact.stock_bar_1m (market, code, bar_time, open, high, low, close, volume, amount)
        values (%s, %s, %s::timestamp, %s, %s, %s, %s, %s, %s)
        on conflict (market, code, bar_time) do update set
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume,
            amount = excluded.amount,
            loaded_at = now()
    """
    query_30m = query_1m.replace("fact.stock_bar_1m", "fact.stock_bar_30m")
    return execute_many(query_1m, params_1m) and execute_many(query_30m, params_30m)


def _upsert_index_daily(items: Sequence[IndexQuoteItem]) -> bool:
    existing_columns = _existing_columns("fact", "index_bar_1d")
    optional_columns = tuple(column_name for column_name in ("pre_close", "change", "pct_chg") if column_name in existing_columns)
    params: list[tuple[object, ...]] = []
    for item in items:
        if item.freq != "1d":
            continue
        index_code = normalize_index_code(item.index_code)
        trade_date = format_date_value(item.trade_time)
        if index_code == "" or trade_date == "":
            continue
        optional_values = tuple(getattr(item, column_name) for column_name in optional_columns)
        params.append((index_code, trade_date, item.open, item.high, item.low, item.close, item.volume, item.amount, *optional_values))
    optional_column_sql = "".join(f", {column_name}" for column_name in optional_columns)
    optional_placeholder_sql = "".join(", %s" for _ in optional_columns)
    return execute_many(
        f"""
        insert into fact.index_bar_1d (index_code, trade_date, open, high, low, close, volume, amount{optional_column_sql})
        values (%s, %s::date, %s, %s, %s, %s, %s, %s{optional_placeholder_sql})
        on conflict (index_code, trade_date) do update set
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume,
            amount = excluded.amount{_optional_update_assignments(existing_columns, optional_columns)},
            loaded_at = now()
        """,
        params,
    )


def _upsert_board_daily(items: Sequence[BoardQuoteItem]) -> bool:
    existing_columns = _existing_columns("fact", "board_daily_1d")
    optional_columns = tuple(column_name for column_name in ("pre_close", "change", "pct_chg") if column_name in existing_columns)
    params: list[tuple[object, ...]] = []
    for item in items:
        if item.freq != "1d":
            continue
        trade_date = format_date_value(item.trade_time)
        if item.board_code == "" or trade_date == "":
            continue
        optional_values = tuple(getattr(item, column_name) for column_name in optional_columns)
        params.append((item.board_code, trade_date, item.open, item.high, item.low, item.close, item.volume, item.amount, *optional_values))
    optional_column_sql = "".join(f", {column_name}" for column_name in optional_columns)
    optional_placeholder_sql = "".join(", %s" for _ in optional_columns)
    return execute_many(
        f"""
        insert into fact.board_daily_1d (board_code, trade_date, open, high, low, close, volume, amount{optional_column_sql})
        values (%s, %s::date, %s, %s, %s, %s, %s, %s{optional_placeholder_sql})
        on conflict (board_code, trade_date) do update set
            open = excluded.open,
            high = excluded.high,
            low = excluded.low,
            close = excluded.close,
            volume = excluded.volume,
            amount = excluded.amount{_optional_update_assignments(existing_columns, optional_columns)},
            loaded_at = now()
        """,
        params,
    )


def _upsert_trading_calendar(items: Sequence[TradingCalendarItem]) -> bool:
    params: list[tuple[object, ...]] = []
    for item in items:
        trade_date = format_date_value(item.trade_date)
        if trade_date == "":
            continue
        params.append((_exchange_to_ref(item.exchange), trade_date, item.is_open))
    return execute_many(
        """
        insert into ref.trade_calendar (exchange, trade_date, is_open)
        values (%s, %s::date, %s)
        on conflict (exchange, trade_date) do update set
            is_open = excluded.is_open
        """,
        params,
    )


def _upsert_stock_catalog(items: Sequence[StockBasicInfo]) -> bool:
    params: list[tuple[object, ...]] = []
    for item in items:
        code = normalize_stock_code(item.code).zfill(6)
        if code == "":
            continue
        market = _exchange_to_ref(item.exchange or item.market or _stock_market(code))
        params.append((market, code, item.name, item.industry, format_date_value(item.list_date), _stock_status_to_delisted_date(item), item.area))
    return execute_many(
        """
        insert into ref.stock (market, code, name, board_type, listed_date, delisted_date, area)
        values (%s, %s, %s, %s, nullif(%s, '')::date, nullif(%s, '')::date, %s)
        on conflict (market, code) do update set
            name = excluded.name,
            board_type = excluded.board_type,
            listed_date = excluded.listed_date,
            delisted_date = excluded.delisted_date,
            area = excluded.area,
            updated_at = now()
        """,
        params,
    )


def _upsert_stock_name_history(items: Sequence[NameHistoryItem]) -> bool:
    params: list[tuple[object, ...]] = []
    for item in items:
        code = normalize_stock_code(item.code).zfill(6)
        valid_from = format_date_value(item.start_date)
        if code == "" or valid_from == "":
            continue
        params.append((_stock_market(code), code, item.name, valid_from, format_date_value(item.end_date), format_date_value(item.ann_date)))
    return execute_many(
        """
        insert into ref.stock_name_history (market, code, name, valid_from, valid_to, ann_date)
        values (%s, %s, %s, %s::date, nullif(%s, '')::date, nullif(%s, '')::date)
        on conflict (market, code, name, valid_from) do update set
            valid_to = excluded.valid_to,
            ann_date = excluded.ann_date,
            updated_at = now()
        """,
        params,
    )


def _upsert_board_catalog(items: Sequence[BoardCatalogItem]) -> bool:
    params: list[tuple[object, ...]] = []
    for item in items:
        if item.board_code == "":
            continue
        params.append((item.board_code, item.category, item.board_name, item.market, item.status))
    return execute_many(
        """
        insert into ref.board (board_code, board_type, name, market, status)
        values (%s, %s, %s, %s, %s)
        on conflict (board_code) do update set
            board_type = excluded.board_type,
            name = excluded.name,
            market = excluded.market,
            status = excluded.status,
            updated_at = now()
        """,
        params,
    )


def _upsert_board_members(items: Sequence[BoardMemberItem]) -> bool:
    params: list[tuple[object, ...]] = []
    stock_params: list[tuple[object, ...]] = []
    for item in items:
        code = normalize_stock_code(item.code).zfill(6)
        if item.board_code == "" or code == "":
            continue
        market = _stock_market(code)
        valid_from = format_date_value(item.join_date) or "1900-01-01"
        params.append((item.board_code, market, code, valid_from, item.weight))
        if item.name != "":
            stock_params.append((market, code, item.name))
    members_ok = execute_many(
        """
        insert into ref.board_stock_membership (board_code, stock_market, stock_code, valid_from, valid_to, weight)
        values (%s, %s, %s, %s::date, null, %s)
        on conflict (board_code, stock_market, stock_code, valid_from) do update set
            valid_to = excluded.valid_to,
            weight = excluded.weight,
            updated_at = now()
        """,
        params,
    )
    names_ok = execute_many(
        """
        insert into ref.stock (market, code, name)
        values (%s, %s, %s)
        on conflict (market, code) do update set
            name = case when ref.stock.name = '' then excluded.name else ref.stock.name end,
            updated_at = now()
        """,
        stock_params,
    )
    return members_ok and names_ok


def _upsert_board_member_history(items: Sequence[BoardMemberHistoryItem]) -> bool:
    in_params: list[tuple[object, ...]] = []
    out_params: list[tuple[object, ...]] = []
    for item in items:
        code = normalize_stock_code(item.code).zfill(6)
        effective_date = format_date_value(item.effective_date)
        if item.board_code == "" or code == "" or effective_date == "":
            continue
        if item.action == "out":
            out_params.append((effective_date, item.board_code, _stock_market(code), code))
        else:
            in_params.append((item.board_code, _stock_market(code), code, effective_date))
    insert_ok = execute_many(
        """
        insert into ref.board_stock_membership (board_code, stock_market, stock_code, valid_from, valid_to)
        values (%s, %s, %s, %s::date, null)
        on conflict (board_code, stock_market, stock_code, valid_from) do nothing
        """,
        in_params,
    )
    update_ok = execute_many(
        """
        update ref.board_stock_membership
        set valid_to = %s::date,
            updated_at = now()
        where board_code = %s
          and stock_market = %s
          and stock_code = %s
          and valid_to is null
        """,
        out_params,
    )
    return insert_ok and update_ok


def _upsert_index_catalog(items: Sequence[IndexCatalogItem]) -> bool:
    params: list[tuple[object, ...]] = []
    for item in items:
        index_code = normalize_index_code(item.index_code)
        if index_code == "":
            continue
        params.append((index_code, item.index_name, item.category, item.market, item.publisher, format_date_value(item.list_date), item.status))
    return execute_many(
        """
        insert into ref.index (index_code, index_name, category, market, publisher, list_date, status)
        values (%s, %s, %s, %s, %s, nullif(%s, '')::date, %s)
        on conflict (index_code) do update set
            index_name = excluded.index_name,
            category = excluded.category,
            market = excluded.market,
            publisher = excluded.publisher,
            list_date = excluded.list_date,
            status = excluded.status,
            updated_at = now()
        """,
        params,
    )


def get_fact_ref_writer(capability_id: str) -> Callable[[list[BaseModel]], bool] | None:
    writers: dict[str, Callable[[Sequence[object]], bool]] = {
        "stocks.quotes.daily": _upsert_stock_daily,
        "stocks.quotes.intraday": _upsert_stock_intraday,
        "stocks.quotes.daily_snapshot": _upsert_stock_daily,
        "indexes.quotes.daily": _upsert_index_daily,
        "boards.quotes.daily": _upsert_board_daily,
        "markets.calendar.trading": _upsert_trading_calendar,
        "stocks.catalog": _upsert_stock_catalog,
        "stocks.profile.basic": _upsert_stock_catalog,
        "stocks.profile.name_history": _upsert_stock_name_history,
        "boards.catalog": _upsert_board_catalog,
        "boards.profile": _upsert_board_catalog,
        "boards.members": _upsert_board_members,
        "boards.members.history": _upsert_board_member_history,
        "indexes.catalog": _upsert_index_catalog,
        "indexes.profile": _upsert_index_catalog,
    }
    writer = writers.get(capability_id)
    if writer is None:
        return None
    return lambda items: writer(items)
