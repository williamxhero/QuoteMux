from __future__ import annotations

from dataclasses import dataclass


DEFAULT_ENABLED_SOURCES = (
    "datalake",
    "datalake_news",
    "datalake_reference",
    "local_topics",
    "tushare_market_topics",
    "tushare",
    "tushare_stock_chips",
    "tushare_stock_finance",
    "tushare_stock_ownership",
    "tushare_stocks",
    "opentdx",
    "efinance",
    "mootdx",
    "akshare",
)


@dataclass(frozen=True)
class QuoteMuxSettings:
    enabled_sources: tuple[str, ...] = DEFAULT_ENABLED_SOURCES

    def is_source_enabled(self, source_name: str) -> bool:
        return source_name in self.enabled_sources
