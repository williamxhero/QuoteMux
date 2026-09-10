from __future__ import annotations

import re

import pytest

from quotemux.stock_reference_authority import (
    STOCK_REFERENCE_AUTHORITY_SCHEMA_SQL,
    StockReferenceMigrationError,
    apply_stock_reference_authority_migration,
)


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


def test_expand_migration_is_repeatable_and_keeps_legacy_inserts_compatible() -> None:
    connection = FakeConnection()

    apply_stock_reference_authority_migration(lambda: connection)
    apply_stock_reference_authority_migration(lambda: connection)

    assert connection.commits == 2
    assert connection.rollbacks == 0
    assert {
        "identity_status",
        "authority_provider",
        "authority_input_id",
        "authority_verified_at",
    } <= connection.columns
    schema_text = "\n".join(STOCK_REFERENCE_AUTHORITY_SCHEMA_SQL)
    assert "identity_status text not null default 'provisional'" in schema_text
    assert "alter column name" not in schema_text.lower()
    assert "authoritative stock identity cannot be downgraded" in schema_text


def test_expand_migration_rolls_back_all_statements_after_failure() -> None:
    connection = FakeConnection(fail_on="create table if not exists audit.stock_authority_input_item")

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
    assert "authority_provider = 'tushare'" in schema_text
    assert "authority_input_id ~ '^[0-9a-f]{64}$'" in schema_text
    assert "stock authority audit records are immutable" in schema_text
    assert schema_text.count("revoke insert, update, delete, truncate") == 3
