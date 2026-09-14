from __future__ import annotations

import os
from datetime import UTC, date, datetime

import pandas as pd
import psycopg
import pytest
from psycopg.rows import dict_row
from quotemux.infra.db import reference_reads
from quotemux.models import StockBasicInfo

from quotemux import fact_ref_writes
from quotemux import stock_reference_authority as authority

pytestmark = pytest.mark.skipif(
    os.getenv("QUOTEMUX_TEST_POSTGRES_DSN", "") == "",
    reason="QUOTEMUX_TEST_POSTGRES_DSN is required for the PostgreSQL L2 contract",
)


BASE_SCHEMA_SQL = """
create schema ref;
create schema fact;
create table ref.stock (
    market text not null,
    code text not null,
    name text not null default '',
    industry text not null default '',
    listing_board text not null default '',
    listed_date date,
    delisted_date date,
    area text not null default '',
    board_type text not null default '',
    updated_at timestamp with time zone not null default now(),
    primary key (market, code)
);
create table ref.trade_calendar (
    exchange text not null,
    trade_date date not null,
    is_open boolean not null,
    primary key (exchange, trade_date)
);
create table fact.stock_daily_1d (
    market text not null,
    code text not null,
    trade_date date not null,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume double precision,
    amount double precision,
    is_suspended boolean not null default false,
    is_st boolean not null default false,
    pre_close double precision,
    change double precision,
    pct_chg double precision,
    loaded_at timestamp with time zone not null default now(),
    primary key (market, code, trade_date),
    foreign key (market, code) references ref.stock (market, code)
);
"""


def _stock(
    code: str,
    name: str,
    status: str,
    exchange: str,
    *,
    delist_date: str = "",
) -> StockBasicInfo:
    return StockBasicInfo(
        code=code,
        name=name,
        exchange=exchange,
        market="",
        list_status=status,
        list_date="2020-01-01",
        delist_date=delist_date,
    )


def _shards(listed_name: str = " 浦发银行 ") -> dict[str, list[StockBasicInfo]]:
    return {
        "listed": [_stock("600000", listed_name, "listed", "SSE")],
        "pending": [_stock("301699", "创业板样例", "pending", "SZSE")],
        "delisted": [
            _stock(
                "920268",
                "北交样例",
                "delisted",
                "BSE",
                delist_date="2026-09-01",
            )
        ],
    }


def _daily_params(market: str, code: str) -> tuple[object, ...]:
    return (
        market,
        code,
        "2026-09-09",
        10.0,
        11.0,
        9.0,
        10.5,
        100,
        1000.0,
        False,
        False,
    )


def test_postgres_authority_lifecycle_is_atomic_and_replay_safe(monkeypatch) -> None:
    dsn = os.environ["QUOTEMUX_TEST_POSTGRES_DSN"]
    with psycopg.connect(dsn) as connection:
        connection.execute(BASE_SCHEMA_SQL)
        connection.commit()

        authority.apply_stock_reference_authority_migration(lambda: connection)
        authority.apply_stock_reference_authority_migration(lambda: connection)
        monkeypatch.setattr(authority, "ensure_stock_reference_authority_schema", lambda: None)
        monkeypatch.setattr(
            fact_ref_writes,
            "ensure_stock_reference_authority_schema",
            lambda: None,
        )

        def query_dataframe(query: str, params: tuple[object, ...] = ()) -> pd.DataFrame:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(query, params)
                rows = cursor.fetchall()
            connection.rollback()
            return pd.DataFrame.from_records(rows)

        monkeypatch.setattr(reference_reads, "query_dataframe", query_dataframe)

        connection.execute(
            """
            insert into ref.trade_calendar(exchange, trade_date, is_open)
            values ('SSE', '2026-09-04', true),
                   ('SSE', '2026-09-07', false),
                   ('SSE', '2026-09-08', true),
                   ('SSE', '2026-09-09', true)
            """
        )
        connection.execute(
            """
            insert into ref.stock(market, code, name)
            values ('SHSE', '601999', '历史非空名')
            """
        )
        connection.commit()

        assert fact_ref_writes._write_stock_daily_transaction(
            [_daily_params("SZSE", "301699"), _daily_params("BJSE", "920268")],
            ["301699", "920268"],
            set(),
            (),
            connection_factory=lambda: connection,
        )
        provisional_rows = connection.execute(
            """
            select code, name, identity_status, identity_source
            from ref.stock
            where code in ('301699', '920268', '601999')
            order by code
            """
        ).fetchall()
        assert provisional_rows == [
            ("301699", "", "provisional", "stock_daily_1d"),
            ("601999", "历史非空名", "provisional", "legacy"),
            ("920268", "", "provisional", "stock_daily_1d"),
        ]
        assert connection.execute("select count(*) from fact.stock_daily_1d").fetchone() == (2,)
        assert reference_reads.load_stock_catalog_frame([], "", "", "").empty

        refreshed_at = datetime(2026, 9, 9, 9, tzinfo=UTC)
        assert authority.load_latest_completed_trading_day(
            refreshed_at,
            connection_factory=lambda: connection,
        ) == date(2026, 9, 9)
        frozen = authority.freeze_stock_authority_input(
            _shards(),
            source_refreshed_at_utc=refreshed_at,
            fresh_through=date(2026, 9, 9),
            connection_factory=lambda: connection,
        )
        first = authority.reconcile_stock_authority_input(
            frozen,
            run_key="production-replay-2026-09-09",
            connection_factory=lambda: connection,
        )
        replay = authority.reconcile_stock_authority_input(
            frozen,
            run_key="production-replay-2026-09-09",
            connection_factory=lambda: connection,
        )

        assert first.status == "committed"
        assert first.provisional_count == 1
        assert replay.status == "idempotent"
        assert replay.provisional_count == first.provisional_count
        assert replay.normalized_output_sha256 == first.normalized_output_sha256
        assert replay.audit_content_sha256 == first.audit_content_sha256
        assert connection.execute(
            "select count(*) from audit.stock_reference_reconciliation"
        ).fetchone() == (1,)
        assert connection.execute(
            "select provisional_count from audit.stock_reference_reconciliation"
        ).fetchone() == (1,)
        assert connection.execute(
            """
            select code, name, identity_status, identity_source,
                   authority_provider, authority_input_id is not null
            from ref.stock
            order by code
            """
        ).fetchall() == [
            ("301699", "创业板样例", "authoritative", "tushare_catalog", "tushare", True),
            ("600000", "浦发银行", "authoritative", "tushare_catalog", "tushare", True),
            ("601999", "历史非空名", "provisional", "legacy", None, False),
            ("920268", "北交样例", "authoritative", "tushare_catalog", "tushare", True),
        ]
        assert list(reference_reads.load_stock_catalog_frame([], "", "", "")["code"]) == [
            "301699",
            "600000",
            "920268",
        ]

        connection.execute(
            """
            create function audit.fail_authority_test() returns trigger
            language plpgsql as $$ begin
              if new.run_key = 'forced-failure' then
                raise exception 'forced authority audit failure';
              end if;
              return new;
            end $$;
            create trigger fail_authority_test
            before insert on audit.stock_reference_reconciliation
            for each row execute function audit.fail_authority_test();
            """
        )
        connection.commit()
        changed = authority.freeze_stock_authority_input(
            _shards("不应提交的名称"),
            source_refreshed_at_utc=refreshed_at,
            fresh_through=date(2026, 9, 9),
            connection_factory=lambda: connection,
        )
        with pytest.raises(psycopg.Error, match="forced authority audit failure"):
            authority.reconcile_stock_authority_input(
                changed,
                run_key="forced-failure",
                connection_factory=lambda: connection,
            )
        assert connection.execute(
            "select name from ref.stock where market = 'SHSE' and code = '600000'"
        ).fetchone() == ("浦发银行",)
        assert connection.execute(
            "select count(*) from audit.stock_reference_reconciliation "
            "where run_key = 'forced-failure'"
        ).fetchone() == (0,)

        connection.execute(
            "insert into ref.stock(market, code, name) values ('SHSE', '600001', '')"
        )
        connection.commit()
        assert connection.execute(
            "select identity_status, identity_source from ref.stock where code = '600001'"
        ).fetchone() == ("provisional", "legacy")

        connection.execute(
            """
            insert into ref.stock (
                market, code, name, identity_status, identity_source,
                authority_provider, authority_input_id, authority_verified_at
            )
            values (
                'SHSE', '600010', '已验证名称', 'authoritative', 'tushare_catalog',
                'tushare', %s, %s
            )
            """,
            (frozen.input_id, refreshed_at),
        )
        connection.commit()
        assert fact_ref_writes._write_stock_daily_transaction(
            [_daily_params("SHSE", "600010")],
            ["600010"],
            set(),
            (),
            connection_factory=lambda: connection,
        )
        assert connection.execute(
            """
            select name, listed_date, identity_status, authority_input_id
            from ref.stock where code = '600010'
            """
        ).fetchone() == ("已验证名称", None, "authoritative", frozen.input_id)
