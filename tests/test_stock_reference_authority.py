from __future__ import annotations

import re
from dataclasses import replace
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest
from quotemux.models import StockBasicInfo
from quotemux.settings import QuoteMuxSettings
from quotemux.stock_reference_authority import (
    STOCK_REFERENCE_AUTHORITY_SCHEMA_SQL,
    StockAuthorityInputError,
    StockReferenceMigrationError,
    apply_stock_reference_authority_migration,
    freeze_stock_authority_input,
    latest_completed_trading_day,
    prepare_stock_authority_input,
    reconcile_stock_authority_input,
)

from quotemux import fact_ref_writes
from quotemux import stock_reference_authority as authority

LEGACY_COLUMNS = {
    "market",
    "code",
    "name",
    "industry",
    "listing_board",
    "listed_date",
    "delisted_date",
    "area",
    "board_type",
    "updated_at",
}


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.query = ""

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, _params: object = None) -> None:
        self.query = " ".join(query.split())
        self.connection.statements.append(self.query)
        if self.connection.fail_on and self.connection.fail_on in self.query:
            raise RuntimeError("injected migration failure")
        match = re.match(
            r"alter table ref\.stock add column if not exists ([a-z_]+)",
            self.query,
            re.IGNORECASE,
        )
        if match:
            self.connection.columns.add(match.group(1).lower())

    def fetchone(self) -> tuple[object, ...] | None:
        if "to_regclass('ref.stock')" in self.query:
            return ("ref.stock",) if self.connection.stock_exists else (None,)
        if "to_regclass('audit.stock_authority_input')" in self.query:
            return (
                "audit.stock_authority_input",
                "audit.stock_authority_input_item",
                "audit.stock_reference_reconciliation",
            )
        return None

    def fetchall(self) -> list[tuple[str]]:
        if "information_schema.columns" in self.query:
            return [(column,) for column in sorted(self.connection.columns)]
        return []


class FakeConnection:
    def __init__(
        self,
        *,
        stock_exists: bool = True,
        columns: set[str] | None = None,
        fail_on: str = "",
    ) -> None:
        self.stock_exists = stock_exists
        self.columns = set(LEGACY_COLUMNS if columns is None else columns)
        self.fail_on = fail_on
        self.statements: list[str] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, **_kwargs: object) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class DailyCursor:
    def __init__(self, connection: DailyConnection) -> None:
        self.connection = connection

    def __enter__(self) -> DailyCursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def executemany(self, query: str, params: list[tuple[object, ...]]) -> None:
        normalized = " ".join(query.split())
        self.connection.calls.append(("many", normalized, params))
        if self.connection.fail_on and self.connection.fail_on in normalized:
            raise RuntimeError("injected daily transaction failure")

    def execute(self, query: str, params: tuple[object, ...]) -> None:
        normalized = " ".join(query.split())
        self.connection.calls.append(("one", normalized, params))
        if self.connection.fail_on and self.connection.fail_on in normalized:
            raise RuntimeError("injected daily transaction failure")


class DailyConnection:
    def __init__(self, fail_on: str = "") -> None:
        self.fail_on = fail_on
        self.calls: list[tuple[str, str, object]] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> DailyCursor:
        return DailyCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class AuthorityCursor:
    def __init__(self, connection: AuthorityConnection) -> None:
        self.connection = connection

    def __enter__(self) -> AuthorityCursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, params: object = None) -> None:
        normalized = " ".join(query.split())
        self.connection.calls.append(("one", normalized, params))

    def executemany(self, query: str, params: list[tuple[object, ...]]) -> None:
        normalized = " ".join(query.split())
        self.connection.calls.append(("many", normalized, params))

    def fetchone(self) -> tuple[object, ...] | None:
        return self.connection.existing_input


class AuthorityConnection:
    def __init__(self, existing_input: tuple[object, ...] | None = None) -> None:
        self.existing_input = existing_input
        self.calls: list[tuple[str, str, object]] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, **_kwargs: object) -> AuthorityCursor:
        return AuthorityCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class ReconciliationCursor:
    def __init__(self, connection: ReconciliationConnection) -> None:
        self.connection = connection
        self.query = ""

    def __enter__(self) -> ReconciliationCursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, query: str, params: object = None) -> None:
        self.query = " ".join(query.split())
        self.connection.calls.append(("one", self.query, params))
        if self.connection.fail_on and self.connection.fail_on in self.query:
            raise RuntimeError("injected reconciliation failure")

    def executemany(self, query: str, params: list[tuple[object, ...]]) -> None:
        self.query = " ".join(query.split())
        self.connection.calls.append(("many", self.query, params))
        if self.connection.fail_on and self.connection.fail_on in self.query:
            raise RuntimeError("injected reconciliation failure")

    def fetchone(self) -> tuple[object, ...] | None:
        if "from audit.stock_reference_reconciliation" in self.query:
            return self.connection.prior_run
        if "from audit.stock_authority_input" in self.query:
            return self.connection.accepted_input
        return None

    def fetchall(self) -> list[tuple[object, ...]]:
        if "where identity_status = 'authoritative'" in self.query:
            return self.connection.output_rows
        if "from ref.stock" in self.query and "for update" in self.query:
            return self.connection.existing_rows
        return []


class ReconciliationConnection:
    def __init__(
        self,
        *,
        accepted_input: tuple[object, ...],
        existing_rows: list[tuple[object, ...]],
        output_rows: list[tuple[object, ...]],
        prior_run: tuple[object, ...] | None = None,
        fail_on: str = "",
    ) -> None:
        self.accepted_input = accepted_input
        self.existing_rows = existing_rows
        self.output_rows = output_rows
        self.prior_run = prior_run
        self.fail_on = fail_on
        self.calls: list[tuple[str, str, object]] = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, **_kwargs: object) -> ReconciliationCursor:
        return ReconciliationCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_expand_migration_is_repeatable_and_keeps_legacy_inserts_compatible() -> None:
    connection = FakeConnection()

    apply_stock_reference_authority_migration(lambda: connection)
    apply_stock_reference_authority_migration(lambda: connection)

    assert connection.commits == 2
    assert connection.rollbacks == 0
    assert {
        "identity_status",
        "identity_source",
        "authority_provider",
        "authority_input_id",
        "authority_verified_at",
    } <= connection.columns
    schema_text = "\n".join(STOCK_REFERENCE_AUTHORITY_SCHEMA_SQL)
    assert "identity_status text not null default 'provisional'" in schema_text
    assert "identity_source text not null default 'legacy'" in schema_text
    assert "alter column name" not in schema_text.lower()
    assert "authoritative stock identity cannot be downgraded" in schema_text


def test_expand_migration_rolls_back_all_statements_after_failure() -> None:
    connection = FakeConnection(
        fail_on="create table if not exists audit.stock_authority_input_item"
    )

    with pytest.raises(RuntimeError, match="injected migration failure"):
        apply_stock_reference_authority_migration(lambda: connection)

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_expand_migration_rejects_missing_or_incompatible_legacy_table() -> None:
    missing_table = FakeConnection(stock_exists=False)
    with pytest.raises(StockReferenceMigrationError, match="must exist"):
        apply_stock_reference_authority_migration(lambda: missing_table)
    assert missing_table.rollbacks == 1

    missing_name = FakeConnection(columns=LEGACY_COLUMNS - {"name"})
    with pytest.raises(StockReferenceMigrationError, match="name"):
        apply_stock_reference_authority_migration(lambda: missing_name)
    assert missing_name.rollbacks == 1


def test_expand_migration_constrains_provenance_and_audit_permissions() -> None:
    schema_text = "\n".join(STOCK_REFERENCE_AUTHORITY_SCHEMA_SQL)

    assert "identity_status = 'provisional'" in schema_text
    assert "identity_status = 'authoritative'" in schema_text
    assert "identity_source = 'tushare_catalog'" in schema_text
    assert "authority_provider = 'tushare'" in schema_text
    assert "authority_input_id ~ '^[0-9a-f]{64}$'" in schema_text
    assert "constraint stock_authority_input_fk" in schema_text
    assert "references audit.stock_authority_input(input_id)" in schema_text
    assert "stock authority audit records are immutable" in schema_text
    assert schema_text.count("revoke insert, update, delete, truncate") == 3


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


def test_daily_writer_commits_facts_and_generic_provisional_references_together(
    monkeypatch,
) -> None:
    connection = DailyConnection()
    monkeypatch.setattr(
        fact_ref_writes,
        "ensure_stock_reference_authority_schema",
        lambda: None,
    )

    result = fact_ref_writes._write_stock_daily_transaction(
        [_daily_params("SZSE", "301699"), _daily_params("BJSE", "920268")],
        ["301699", "920268", "301699"],
        set(),
        (),
        connection_factory=lambda: connection,
    )

    assert result is True
    assert connection.commits == 1
    assert connection.rollbacks == 0
    provisional_call, fact_call, listed_date_call, metrics_call = connection.calls
    assert provisional_call[0] == "many"
    assert "'provisional', 'stock_daily_1d'" in provisional_call[1]
    assert "on conflict (market, code) do nothing" in provisional_call[1]
    assert provisional_call[2] == [("SZSE", "301699"), ("BJSE", "920268")]
    assert "insert into fact.stock_daily_1d" in fact_call[1]
    assert "update ref.stock stock_ref" in listed_date_call[1]
    assert "stock_ref.identity_status = 'provisional'" in listed_date_call[1]
    assert "update fact.stock_daily_1d target" in metrics_call[1]


def test_daily_writer_rolls_back_reference_and_fact_on_failure(monkeypatch) -> None:
    connection = DailyConnection(fail_on="insert into fact.stock_daily_1d")
    monkeypatch.setattr(
        fact_ref_writes,
        "ensure_stock_reference_authority_schema",
        lambda: None,
    )

    result = fact_ref_writes._write_stock_daily_transaction(
        [_daily_params("SHSE", "600000")],
        ["600000"],
        set(),
        (),
        connection_factory=lambda: connection,
    )

    assert result is False
    assert connection.commits == 0
    assert connection.rollbacks == 1


def _stock(
    code: str,
    name: str,
    status: str,
    *,
    exchange: str = "",
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


def _complete_shards() -> dict[str, list[StockBasicInfo]]:
    return {
        "listed": [_stock("600000", " 浦发银行\u3000", "listed", exchange="SSE")],
        "pending": [_stock("301699", "新股样例", "pending", exchange="SZSE")],
        "delisted": [
            _stock(
                "920268",
                "北交样例",
                "delisted",
                exchange="BSE",
                delist_date="2026-09-01",
            )
        ],
    }


def test_latest_completed_day_is_calendar_and_shanghai_session_aware() -> None:
    open_dates = [date(2026, 9, 4), date(2026, 9, 8), date(2026, 9, 9)]

    saturday = datetime(2026, 9, 5, 4, tzinfo=UTC)
    before_close = datetime(2026, 9, 9, 6, tzinfo=UTC)
    after_close = datetime(2026, 9, 9, 8, tzinfo=UTC)

    assert latest_completed_trading_day(saturday, open_dates) == date(2026, 9, 4)
    assert latest_completed_trading_day(before_close, open_dates) == date(2026, 9, 8)
    assert latest_completed_trading_day(after_close, open_dates) == date(2026, 9, 9)


def test_authority_input_requires_all_shard_keys_and_fresh_source() -> None:
    refreshed_at = datetime(2026, 9, 9, 9, tzinfo=UTC)
    shards = _complete_shards()
    shards["pending"] = []

    result = prepare_stock_authority_input(
        shards,
        source_refreshed_at_utc=refreshed_at,
        fresh_through=date(2026, 9, 9),
    )
    assert result.shard_counts == {"listed": 1, "pending": 0, "delisted": 1}

    del shards["pending"]
    with pytest.raises(StockAuthorityInputError, match="missing required shards: pending"):
        prepare_stock_authority_input(
            shards,
            source_refreshed_at_utc=refreshed_at,
            fresh_through=date(2026, 9, 9),
        )

    empty_listed = _complete_shards()
    empty_listed["listed"] = []
    with pytest.raises(StockAuthorityInputError, match="listed shard is empty"):
        prepare_stock_authority_input(
            empty_listed,
            source_refreshed_at_utc=refreshed_at,
            fresh_through=date(2026, 9, 9),
        )

    malformed = _complete_shards()
    malformed["pending"] = None
    with pytest.raises(StockAuthorityInputError, match="malformed required shards: pending"):
        prepare_stock_authority_input(
            malformed,
            source_refreshed_at_utc=refreshed_at,
            fresh_through=date(2026, 9, 9),
        )

    with pytest.raises(StockAuthorityInputError, match="is stale"):
        prepare_stock_authority_input(
            _complete_shards(),
            source_refreshed_at_utc=datetime(2026, 9, 8, 9, tzinfo=UTC),
            fresh_through=date(2026, 9, 9),
        )


def test_authority_input_trims_only_name_edges_and_rejects_invalid_names() -> None:
    refreshed_at = datetime(2026, 9, 9, 9, tzinfo=UTC)
    result = prepare_stock_authority_input(
        _complete_shards(),
        source_refreshed_at_utc=refreshed_at,
        fresh_through=date(2026, 9, 9),
    )

    assert next(item.name for item in result.items if item.code == "600000") == "浦发银行"

    blank = _complete_shards()
    blank["listed"] = [_stock("600000", "\u3000\t", "listed", exchange="SSE")]
    with pytest.raises(StockAuthorityInputError, match="name is blank"):
        prepare_stock_authority_input(
            blank,
            source_refreshed_at_utc=refreshed_at,
            fresh_through=date(2026, 9, 9),
        )

    invalid_type = _complete_shards()
    invalid_type["listed"] = [
        SimpleNamespace(
            code="600000",
            name=123,
            exchange="SSE",
            market="",
            list_status="listed",
            list_date="2020-01-01",
            delist_date="",
            industry="",
            listing_board="",
            area="",
        )
    ]
    with pytest.raises(StockAuthorityInputError, match="name must be text"):
        prepare_stock_authority_input(
            invalid_type,
            source_refreshed_at_utc=refreshed_at,
            fresh_through=date(2026, 9, 9),
        )


def test_authority_input_rejects_cross_shard_code_conflicts() -> None:
    shards = _complete_shards()
    shards["delisted"] = [
        _stock("600000", "旧名", "delisted", exchange="SSE", delist_date="2020-01-01")
    ]

    with pytest.raises(StockAuthorityInputError, match="conflicting stock authority identity"):
        prepare_stock_authority_input(
            shards,
            source_refreshed_at_utc=datetime(2026, 9, 9, 9, tzinfo=UTC),
            fresh_through=date(2026, 9, 9),
        )


def test_authority_input_does_not_normalize_nonstandard_provider_codes() -> None:
    shards = _complete_shards()
    shards["listed"] = [_stock("T600000", "异常代码", "listed", exchange="SSE")]

    with pytest.raises(StockAuthorityInputError, match="invalid stock authority code"):
        prepare_stock_authority_input(
            shards,
            source_refreshed_at_utc=datetime(2026, 9, 9, 9, tzinfo=UTC),
            fresh_through=date(2026, 9, 9),
        )


def test_authority_input_hash_and_freeze_are_deterministic(monkeypatch) -> None:
    refreshed_at = datetime(2026, 9, 9, 9, tzinfo=UTC)
    first = prepare_stock_authority_input(
        _complete_shards(),
        source_refreshed_at_utc=refreshed_at,
        fresh_through=date(2026, 9, 9),
    )
    second = prepare_stock_authority_input(
        {key: list(reversed(value)) for key, value in _complete_shards().items()},
        source_refreshed_at_utc=refreshed_at,
        fresh_through=date(2026, 9, 9),
    )
    assert first.content_sha256 == second.content_sha256
    assert first.input_id == second.input_id

    later_refresh = prepare_stock_authority_input(
        _complete_shards(),
        source_refreshed_at_utc=datetime(2026, 9, 9, 10, tzinfo=UTC),
        fresh_through=date(2026, 9, 9),
    )
    assert later_refresh.content_sha256 == first.content_sha256
    assert later_refresh.input_id != first.input_id

    connection = AuthorityConnection()
    monkeypatch.setattr(authority, "ensure_stock_reference_authority_schema", lambda: None)
    frozen = freeze_stock_authority_input(
        _complete_shards(),
        source_refreshed_at_utc=refreshed_at,
        fresh_through=date(2026, 9, 9),
        connection_factory=lambda: connection,
    )

    assert frozen.input_id == first.input_id
    assert connection.commits == 1
    assert any("request_status" in call[1] for call in connection.calls)
    item_call = next(call for call in connection.calls if call[0] == "many")
    assert len(item_call[2]) == 3


def test_reconciliation_rejects_tampered_frozen_input(monkeypatch) -> None:
    authority_input = prepare_stock_authority_input(
        _complete_shards(),
        source_refreshed_at_utc=datetime(2026, 9, 9, 9, tzinfo=UTC),
        fresh_through=date(2026, 9, 9),
    )
    tampered_item = replace(authority_input.items[0], name="篡改名称")
    tampered_input = replace(
        authority_input,
        items=(tampered_item, *authority_input.items[1:]),
    )
    monkeypatch.setattr(
        authority,
        "ensure_stock_reference_authority_schema",
        lambda: pytest.fail("tampered input must fail before database access"),
    )

    with pytest.raises(StockAuthorityInputError, match="integrity check failed"):
        reconcile_stock_authority_input(tampered_input)


def test_invalid_authority_input_is_audited_without_items(monkeypatch) -> None:
    connection = AuthorityConnection()
    monkeypatch.setattr(authority, "ensure_stock_reference_authority_schema", lambda: None)
    shards = _complete_shards()
    del shards["delisted"]

    with pytest.raises(StockAuthorityInputError, match="missing required shards"):
        freeze_stock_authority_input(
            shards,
            source_refreshed_at_utc=datetime(2026, 9, 9, 9, tzinfo=UTC),
            fresh_through=date(2026, 9, 9),
            connection_factory=lambda: connection,
        )

    assert connection.commits == 1
    assert not any(call[0] == "many" for call in connection.calls)
    rejected_call = next(call for call in connection.calls if "'rejected'" in call[1])
    assert "on conflict (input_id) do nothing" in rejected_call[1]


def test_tushare_authority_fetch_uses_three_explicit_status_shards(monkeypatch) -> None:
    from quotemux import stocks

    calls: list[tuple[object, ...]] = []

    def fake_source_call(package_id: str, handler_name: str, *args: object):
        calls.append((package_id, handler_name, *args))
        status = str(args[3])
        return _complete_shards()[status]

    monkeypatch.setattr(stocks, "_source_package_call", fake_source_call)

    def fake_provider_request(
        _capability_id: str,
        _provider: str,
        _source_instance_id: str,
        _handler: str,
        _source_instance: object,
        fetcher,
        _remaining: object,
    ):
        return fetcher()

    monkeypatch.setattr(
        stocks,
        "run_provider_request",
        fake_provider_request,
    )

    result = stocks._fetch_tushare_authority_shards(QuoteMuxSettings(enabled_sources=("tushare",)))

    assert tuple(result) == ("listed", "pending", "delisted")
    assert [call[5] for call in calls] == ["listed", "pending", "delisted"]
    assert all(call[0] == "tushare" for call in calls)
    assert all(call[-1] is True for call in calls)


def _reference_row(
    market: str,
    code: str,
    name: str,
    identity_status: str,
    *,
    identity_source: str = "stock_daily_1d",
    input_id: str = "",
    verified_at: datetime | None = None,
    delisted_date: str = "",
) -> tuple[object, ...]:
    return (
        market,
        code,
        name,
        "",
        "",
        date(2020, 1, 1),
        date.fromisoformat(delisted_date) if delisted_date else None,
        "",
        identity_status,
        identity_source,
        "tushare" if identity_status == "authoritative" else None,
        input_id or ("a" * 64 if identity_status == "authoritative" else None),
        verified_at
        or (datetime(2026, 9, 8, 9, tzinfo=UTC) if identity_status == "authoritative" else None),
    )


def _reconciliation_connection(
    authority_input,
    *,
    prior_run: tuple[object, ...] | None = None,
    fail_on: str = "",
) -> ReconciliationConnection:
    existing_rows = [
        _reference_row("SZSE", "301699", "", "provisional"),
        _reference_row("BJSE", "920268", "", "provisional"),
        _reference_row(
            "SHSE",
            "600000",
            "浦发旧名",
            "authoritative",
            identity_source="tushare_catalog",
        ),
        _reference_row(
            "SZSE",
            "000001",
            "平安银行",
            "authoritative",
            identity_source="tushare_catalog",
        ),
        _reference_row("SHSE", "601999", "历史非空名", "provisional", identity_source="legacy"),
    ]
    updated_by_code = {item.code: item for item in authority_input.items}
    output_rows = [
        _reference_row(
            "SZSE",
            "000001",
            "平安银行",
            "authoritative",
            identity_source="tushare_catalog",
        )
    ]
    for code in ("301699", "600000", "920268"):
        item = updated_by_code[code]
        output_rows.append(
            _reference_row(
                item.market,
                item.code,
                item.name,
                "authoritative",
                identity_source="tushare_catalog",
                input_id=authority_input.input_id,
                verified_at=authority_input.source_refreshed_at_utc,
                delisted_date=item.delisted_date,
            )
        )
    return ReconciliationConnection(
        accepted_input=(
            authority_input.provider,
            authority_input.content_sha256,
            "accepted",
        ),
        existing_rows=existing_rows,
        output_rows=output_rows,
        prior_run=prior_run,
        fail_on=fail_on,
    )


def test_reconciliation_promotes_updates_and_retains_missing_authority(monkeypatch) -> None:
    authority_input = prepare_stock_authority_input(
        _complete_shards(),
        source_refreshed_at_utc=datetime(2026, 9, 9, 9, tzinfo=UTC),
        fresh_through=date(2026, 9, 9),
    )
    connection = _reconciliation_connection(authority_input)
    monkeypatch.setattr(authority, "ensure_stock_reference_authority_schema", lambda: None)

    result = reconcile_stock_authority_input(
        authority_input,
        run_key="catalog-2026-09-09",
        connection_factory=lambda: connection,
    )

    assert result.status == "committed"
    assert result.existing_count == 5
    assert result.provisional_count == 1
    assert result.inserted_count == 0
    assert result.promoted_count == 2
    assert result.renamed_count == 1
    assert result.missing_count == 1
    assert result.conflict_count == 0
    assert len(result.normalized_output_sha256) == 64
    assert len(result.audit_content_sha256) == 64
    assert connection.commits == 1
    assert connection.rollbacks == 0

    upsert_call = next(
        call
        for call in connection.calls
        if call[0] == "many" and "insert into ref.stock" in call[1]
    )
    assert "identity_status = excluded.identity_status" in upsert_call[1]
    params_by_code = {str(params[1]): params for params in upsert_call[2]}
    assert params_by_code["301699"][2] == "新股样例"
    assert params_by_code["920268"][2] == "北交样例"
    assert params_by_code["920268"][6] == "2026-09-01"
    assert all(params[8] == authority_input.input_id for params in upsert_call[2])
    audit_call = next(
        call
        for call in connection.calls
        if "insert into audit.stock_reference_reconciliation" in call[1]
    )
    assert audit_call[2][-2:] == (
        result.normalized_output_sha256,
        result.audit_content_sha256,
    )


def test_reconciliation_rolls_back_reference_and_audit_together(monkeypatch) -> None:
    authority_input = prepare_stock_authority_input(
        _complete_shards(),
        source_refreshed_at_utc=datetime(2026, 9, 9, 9, tzinfo=UTC),
        fresh_through=date(2026, 9, 9),
    )
    connection = _reconciliation_connection(
        authority_input,
        fail_on="insert into audit.stock_reference_reconciliation",
    )
    monkeypatch.setattr(authority, "ensure_stock_reference_authority_schema", lambda: None)

    with pytest.raises(RuntimeError, match="injected reconciliation failure"):
        reconcile_stock_authority_input(
            authority_input,
            run_key="failed-run",
            connection_factory=lambda: connection,
        )

    assert connection.commits == 0
    assert connection.rollbacks == 1


def test_reconciliation_same_run_key_is_idempotent(monkeypatch) -> None:
    authority_input = prepare_stock_authority_input(
        _complete_shards(),
        source_refreshed_at_utc=datetime(2026, 9, 9, 9, tzinfo=UTC),
        fresh_through=date(2026, 9, 9),
    )
    monkeypatch.setattr(authority, "ensure_stock_reference_authority_schema", lambda: None)
    first_connection = _reconciliation_connection(authority_input)
    first = reconcile_stock_authority_input(
        authority_input,
        run_key="stable-run",
        connection_factory=lambda: first_connection,
    )
    prior_run = (
        authority_input.input_id,
        authority_input.content_sha256,
        first.candidate_count,
        first.existing_count,
        first.provisional_count,
        first.inserted_count,
        first.promoted_count,
        first.renamed_count,
        first.missing_count,
        first.conflict_count,
        first.normalized_output_sha256,
        first.audit_content_sha256,
    )
    replay_connection = _reconciliation_connection(authority_input, prior_run=prior_run)

    replay = reconcile_stock_authority_input(
        authority_input,
        run_key="stable-run",
        connection_factory=lambda: replay_connection,
    )

    assert replay.status == "idempotent"
    assert replay.provisional_count == first.provisional_count
    assert replay.normalized_output_sha256 == first.normalized_output_sha256
    assert replay.audit_content_sha256 == first.audit_content_sha256
    assert replay_connection.commits == 0
    assert replay_connection.rollbacks == 1
    assert not any(call[0] == "many" for call in replay_connection.calls)
