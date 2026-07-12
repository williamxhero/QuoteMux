from __future__ import annotations

from typing import Callable, Sequence

from pydantic import BaseModel

from platform_models import BoardQuoteItem, ConceptCatalogItem, ConceptMemberHistoryItem, ConceptMemberItem, ConceptQuoteItem, IndexCatalogItem, IndexQuoteItem, NameHistoryItem, StockBasicInfo, StockQuoteItem, TradingCalendarItem
from quotemux.infra.common import format_date_value, format_datetime_value, normalize_index_code, normalize_stock_code, stock_market_name
from quotemux.infra.db.client import execute_many, execute_sql, query_dataframe


KNOWN_INDEX_CATALOG: dict[str, IndexCatalogItem] = {
    "000001": IndexCatalogItem(index_code="000001", index_name="上证指数", category="broad_market", market="SHSE", publisher="SSE", status="active"),
    "000300": IndexCatalogItem(index_code="000300", index_name="沪深300", category="broad_market", market="A_SHARE", publisher="CSI", status="active"),
    "000688": IndexCatalogItem(index_code="000688", index_name="科创50", category="broad_market", market="SHSE", publisher="SSE", status="active"),
    "000852": IndexCatalogItem(index_code="000852", index_name="中证1000", category="broad_market", market="A_SHARE", publisher="CSI", status="active"),
    "000905": IndexCatalogItem(index_code="000905", index_name="中证500", category="broad_market", market="A_SHARE", publisher="CSI", status="active"),
    "399001": IndexCatalogItem(index_code="399001", index_name="深证成指", category="broad_market", market="SZSE", publisher="SZSE", status="active"),
    "399006": IndexCatalogItem(index_code="399006", index_name="创业板指", category="broad_market", market="SZSE", publisher="SZSE", status="active"),
    "899050": IndexCatalogItem(index_code="899050", index_name="北证50", category="broad_market", market="BJSE", publisher="BSE", status="active"),
}


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


def _table_exists(table_schema: str, table_name: str) -> bool:
    frame = query_dataframe(
        """
        select 1
        from information_schema.tables
        where table_schema = %s
          and table_name = %s
        limit 1
        """,
        (table_schema, table_name),
    )
    return not frame.empty


def _optional_update_assignments(existing_columns: set[str], column_names: tuple[str, ...]) -> str:
    assignments = [f"{column_name} = excluded.{column_name}" for column_name in column_names if column_name in existing_columns]
    if assignments == []:
        return ""
    return ",\n            " + ",\n            ".join(assignments)


def _ensure_concept_membership_table() -> bool:
    statements = (
        "create schema if not exists ref",
        """
        create table if not exists ref.concept_stock_membership (
            concept_id character varying not null,
            stock_market character varying not null,
            stock_code character varying not null,
            valid_from date not null,
            valid_to date,
            weight double precision,
            updated_at timestamp with time zone not null default now(),
            primary key (concept_id, stock_market, stock_code, valid_from)
        )
        """,
        "create index if not exists concept_stock_membership_stock_idx on ref.concept_stock_membership (stock_code, valid_from, valid_to)",
    )
    for statement in statements:
        if not execute_sql(statement):
            return False
    return True


def _stock_market(code: str) -> str:
    return stock_market_name(code)


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


def _fallback_index_market(index_code: str) -> str:
    if index_code.startswith(("0", "9")):
        return "SHSE"
    if index_code.startswith("3"):
        return "SZSE"
    if index_code.startswith("8"):
        return "BJSE"
    return ""


def _index_catalog_from_quotes(items: Sequence[IndexQuoteItem]) -> list[IndexCatalogItem]:
    index_codes = []
    for item in items:
        if item.freq != "1d":
            continue
        index_code = normalize_index_code(item.index_code)
        if index_code != "":
            index_codes.append(index_code)
    catalog_items: list[IndexCatalogItem] = []
    for index_code in dict.fromkeys(index_codes):
        known_item = KNOWN_INDEX_CATALOG.get(index_code)
        if known_item is not None:
            catalog_items.append(known_item)
            continue
        catalog_items.append(
            IndexCatalogItem(
                index_code=index_code,
                index_name=index_code,
                category="",
                market=_fallback_index_market(index_code),
                publisher="",
                status="active",
            )
        )
    return catalog_items


def _upsert_stock_daily(items: Sequence[StockQuoteItem]) -> bool:
    existing_columns = _existing_columns("fact", "stock_daily_1d")
    optional_columns = tuple(column_name for column_name in ("pre_close", "change", "pct_chg") if column_name in existing_columns)
    params: list[tuple[object, ...]] = []
    daily_codes: list[str] = []
    for item in items:
        if item.freq != "1d":
            continue
        code = normalize_stock_code(item.code).zfill(6)
        trade_date = format_date_value(item.trade_time)
        if code == "" or trade_date == "":
            continue
        daily_codes.append(code)
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
    upsert_ok = execute_many(
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
    if not upsert_ok:
        return False
    return _repair_stock_listed_dates_from_daily(list(dict.fromkeys(daily_codes)))


def _repair_stock_listed_dates_from_daily(codes: Sequence[str]) -> bool:
    if not codes:
        return True
    return execute_sql(
        """
        with target_codes as (
            select unnest(%s::text[]) as code
        ),
        first_daily as (
            select day_rows.market, day_rows.code, min(day_rows.trade_date) as listed_date
            from fact.stock_daily_1d day_rows
            join target_codes target on target.code = day_rows.code
            group by day_rows.market, day_rows.code
        )
        update ref.stock stock_ref
        set listed_date = first_daily.listed_date,
            updated_at = now()
        from first_daily
        where stock_ref.market = first_daily.market
          and stock_ref.code = first_daily.code
          and stock_ref.listed_date is null
        """,
        (list(codes),),
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
        open_price, high_price, low_price, close_price = item.open, item.high, item.low, item.close
        if open_price is not None and high_price is not None and low_price is not None and close_price is not None:
            open_price, high_price, low_price, close_price = _normalized_ohlc(
                float(open_price),
                float(high_price),
                float(low_price),
                float(close_price),
                int(item.volume) if item.volume is not None else 0,
                float(item.amount) if item.amount is not None else 0,
            )
        params = (
            _stock_market(code),
            code,
            trade_time,
            open_price,
            high_price,
            low_price,
            close_price,
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


def _normalized_ohlc(open_price: float, high_price: float, low_price: float, close_price: float, volume: int, amount: float) -> tuple[float, float, float, float]:
    if volume == 0 and amount == 0 and open_price == high_price == low_price and close_price > high_price:
        return open_price, high_price, low_price, open_price
    if open_price == 0 and low_price <= close_price <= high_price and close_price > 0:
        return close_price, high_price, low_price, close_price
    return open_price, high_price, low_price, close_price


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
    daily_ok = execute_many(
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
    if not daily_ok:
        return False
    return _upsert_index_catalog(_index_catalog_from_quotes(items))


def _upsert_concept_daily(items: Sequence[ConceptQuoteItem]) -> bool:
    if not _table_exists("fact", "concept_daily_1d"):
        return False
    existing_columns = _existing_columns("fact", "concept_daily_1d")
    optional_columns = tuple(column_name for column_name in ("pre_close", "change", "pct_chg") if column_name in existing_columns)
    params: list[tuple[object, ...]] = []
    for item in items:
        if item.freq != "1d":
            continue
        trade_date = format_date_value(item.trade_time)
        if item.concept_id == "" or trade_date == "":
            continue
        optional_values = tuple(getattr(item, column_name) for column_name in optional_columns)
        params.append((item.concept_id, trade_date, item.open, item.high, item.low, item.close, item.volume, item.amount, *optional_values))
    optional_column_sql = "".join(f", {column_name}" for column_name in optional_columns)
    optional_placeholder_sql = "".join(", %s" for _ in optional_columns)
    return execute_many(
        f"""
        insert into fact.concept_daily_1d (concept_id, trade_date, open, high, low, close, volume, amount{optional_column_sql})
        values (%s, %s::date, %s, %s, %s, %s, %s, %s{optional_placeholder_sql})
        on conflict (concept_id, trade_date) do update set
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
    if not _table_exists("fact", "board_daily_1d"):
        return False
    existing_columns = _existing_columns("fact", "board_daily_1d")
    optional_columns = tuple(column_name for column_name in ("pre_close", "change", "pct_chg") if column_name in existing_columns)
    params: list[tuple[object, ...]] = []
    board_params: list[tuple[object, ...]] = []
    trade_dates: set[str] = set()
    for item in items:
        if item.freq != "1d":
            continue
        trade_date = format_date_value(item.trade_time)
        if item.board_code == "" or trade_date == "":
            continue
        trade_dates.add(trade_date)
        optional_values = tuple(getattr(item, column_name) for column_name in optional_columns)
        params.append((item.board_code, trade_date, item.open, item.high, item.low, item.close, item.volume, item.amount, *optional_values))
        board_params.append((item.board_code, item.board_name, "industry"))
    for trade_date in sorted(trade_dates):
        if not execute_sql(
            "delete from fact.board_daily_1d where trade_date = %s::date and left(board_code, 9) = 'INDUSTRY:'",
            (trade_date,),
        ):
            return False
    if not execute_many(
        """
        insert into ref.board (board_code, name, board_type)
        values (%s, %s, %s)
        on conflict (board_code) do update set
            name = excluded.name,
            board_type = excluded.board_type,
            updated_at = now()
        """,
        list(dict.fromkeys(board_params)),
    ):
        return False
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
    existing_columns = _existing_columns("ref", "stock")
    has_board_type = "board_type" in existing_columns
    for item in items:
        code = normalize_stock_code(item.code).zfill(6)
        if code == "":
            continue
        market = _exchange_to_ref(item.exchange or item.market or _stock_market(code))
        listing_board = item.listing_board or item.market
        params.append((market, code, item.name, item.industry, listing_board, format_date_value(item.list_date), _stock_status_to_delisted_date(item), item.area))
    board_type_column_sql = ", board_type" if has_board_type else ""
    board_type_value_sql = ", %s" if has_board_type else ""
    update_board_type_sql = ",\n            board_type = excluded.board_type" if has_board_type else ""
    if has_board_type:
        params = [(*item, item[4]) for item in params]
    return execute_many(
        f"""
        insert into ref.stock (market, code, name, industry, listing_board, listed_date, delisted_date, area{board_type_column_sql})
        values (%s, %s, %s, %s, %s, nullif(%s, '')::date, nullif(%s, '')::date, %s{board_type_value_sql})
        on conflict (market, code) do update set
            name = excluded.name,
            industry = excluded.industry,
            listing_board = excluded.listing_board,
            listed_date = excluded.listed_date,
            delisted_date = excluded.delisted_date,
            area = excluded.area{update_board_type_sql},
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


def _upsert_concept_catalog(items: Sequence[ConceptCatalogItem]) -> bool:
    params: list[tuple[object, ...]] = []
    for item in items:
        if item.concept_id == "":
            continue
        params.append((item.concept_id, item.category, item.concept_name, item.market, item.status))
    return execute_many(
        """
        insert into ref.concept (concept_id, concept_type, name, market, status)
        values (%s, %s, %s, %s, %s)
        on conflict (concept_id) do update set
            concept_type = excluded.concept_type,
            name = excluded.name,
            market = excluded.market,
            status = excluded.status,
            updated_at = now()
        """,
        params,
    )


def _upsert_concept_members(items: Sequence[ConceptMemberItem]) -> bool:
    if not _ensure_concept_membership_table():
        return False
    params: list[tuple[object, ...]] = []
    stock_params: list[tuple[object, ...]] = []
    for item in items:
        code = normalize_stock_code(item.code).zfill(6)
        if item.concept_id == "" or code == "":
            continue
        market = _stock_market(code)
        valid_from = format_date_value(item.join_date) or "1900-01-01"
        params.append((item.concept_id, market, code, valid_from, item.weight))
        if item.name != "":
            stock_params.append((market, code, item.name))
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
    if not names_ok:
        return False
    valid_params = _filter_concept_member_params(params)
    members_ok = execute_many(
        """
        insert into ref.concept_stock_membership (concept_id, stock_market, stock_code, valid_from, valid_to, weight)
        values (%s, %s, %s, %s::date, null, %s)
        on conflict (concept_id, stock_market, stock_code, valid_from) do update set
            valid_to = excluded.valid_to,
            weight = excluded.weight,
            updated_at = now()
        """,
        valid_params,
    )
    return members_ok


def _filter_concept_member_params(params: list[tuple[object, ...]]) -> list[tuple[object, ...]]:
    if params == []:
        return []
    concept_ids = [str(item[0]) for item in params]
    markets = [str(item[1]) for item in params]
    codes = [str(item[2]) for item in params]
    valid_froms = [str(item[3]) for item in params]
    weights = [item[4] for item in params]
    frame = query_dataframe(
        """
        with incoming as (
            select *
            from unnest(%s::text[], %s::text[], %s::text[], %s::date[], %s::double precision[])
              as rows(concept_id, stock_market, stock_code, valid_from, weight)
        )
        select incoming.concept_id,
               incoming.stock_market,
               incoming.stock_code,
               incoming.valid_from::text as valid_from,
               incoming.weight
        from incoming
        where exists (
            select 1
            from ref.stock stock_ref
            where stock_ref.market = incoming.stock_market
              and stock_ref.code = incoming.stock_code
        )
          and (
              incoming.valid_from = date '1900-01-01'
              or exists (
                  select 1
                  from fact.stock_daily_1d daily_rows
                  where daily_rows.market = incoming.stock_market
                    and daily_rows.code = incoming.stock_code
                    and daily_rows.trade_date = incoming.valid_from
              )
          )
          and (
              incoming.valid_from = date '1900-01-01'
              or exists (
                  select 1
                  from fact.concept_daily_1d concept_rows
                  where concept_rows.concept_id = incoming.concept_id
                    and concept_rows.trade_date = incoming.valid_from
              )
          )
        """,
        (concept_ids, markets, codes, valid_froms, weights),
    )
    if frame.empty:
        return []
    return [
        (row["concept_id"], row["stock_market"], row["stock_code"], row["valid_from"], row["weight"])
        for row in frame.to_dict("records")
    ]


def _upsert_concept_member_history(items: Sequence[ConceptMemberHistoryItem]) -> bool:
    if not _ensure_concept_membership_table():
        return False
    in_params: list[tuple[object, ...]] = []
    out_params: list[tuple[object, ...]] = []
    for item in items:
        code = normalize_stock_code(item.code).zfill(6)
        effective_date = format_date_value(item.effective_date)
        if item.concept_id == "" or code == "" or effective_date == "":
            continue
        if item.action == "out":
            out_params.append((effective_date, item.concept_id, _stock_market(code), code))
        else:
            in_params.append((item.concept_id, _stock_market(code), code, effective_date))
    insert_ok = execute_many(
        """
        insert into ref.concept_stock_membership (concept_id, stock_market, stock_code, valid_from, valid_to)
        values (%s, %s, %s, %s::date, null)
        on conflict (concept_id, stock_market, stock_code, valid_from) do nothing
        """,
        in_params,
    )
    update_ok = execute_many(
        """
        update ref.concept_stock_membership
        set valid_to = %s::date,
            updated_at = now()
        where concept_id = %s
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
        "boards.quotes.daily": _upsert_board_daily,
        "indexes.quotes.daily": _upsert_index_daily,
        "concepts.quotes.daily": _upsert_concept_daily,
        "markets.calendar.trading": _upsert_trading_calendar,
        "stocks.catalog": _upsert_stock_catalog,
        "stocks.profile.basic": _upsert_stock_catalog,
        "stocks.profile.name_history": _upsert_stock_name_history,
        "concepts.catalog": _upsert_concept_catalog,
        "concepts.profile": _upsert_concept_catalog,
        "concepts.members": _upsert_concept_members,
        "concepts.members.history": _upsert_concept_member_history,
        "indexes.catalog": _upsert_index_catalog,
        "indexes.profile": _upsert_index_catalog,
    }
    writer = writers.get(capability_id)
    if writer is None:
        return None
    return lambda items: writer(items)
