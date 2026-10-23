from __future__ import annotations


STOCK_FACT_PUBLICATION_LOCK = "markethub:stock-fact-publication"


def acquire_stock_fact_publication_lock(cursor: object) -> None:
    """Serialize fact/reference writers with the daily coverage read model."""
    cursor.execute(
        "select pg_advisory_xact_lock(hashtext(%s))",
        (STOCK_FACT_PUBLICATION_LOCK,),
    )
