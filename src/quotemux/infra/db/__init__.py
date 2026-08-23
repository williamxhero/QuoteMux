from __future__ import annotations

from importlib import import_module

__all__ = [
    "QueryBatch",
    "ReadOnlyClient",
    "ReadOnlySnapshot",
    "close_pool",
    "close_read_pool",
    "get_read_pool_metrics",
    "query_dataframe",
    "stream_query_batches",
]


def __getattr__(name: str):
    if name not in __all__:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name = "quotemux.infra.db.read_client" if name in {
        "QueryBatch",
        "ReadOnlyClient",
        "ReadOnlySnapshot",
        "close_read_pool",
        "get_read_pool_metrics",
    } else "quotemux.infra.db.client"
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value
