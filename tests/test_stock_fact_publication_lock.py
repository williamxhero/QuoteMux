from __future__ import annotations

from quotemux.infra.db.locks import STOCK_FACT_PUBLICATION_LOCK, acquire_stock_fact_publication_lock


class _Cursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    def execute(self, query: str, params: tuple[str, ...]) -> None:
        self.calls.append((query, params))


def test_stock_fact_publication_lock_uses_transaction_advisory_lock() -> None:
    cursor = _Cursor()

    acquire_stock_fact_publication_lock(cursor)

    assert cursor.calls == [
        (
            "select pg_advisory_xact_lock(hashtext(%s))",
            (STOCK_FACT_PUBLICATION_LOCK,),
        )
    ]
