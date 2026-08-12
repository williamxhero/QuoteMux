from __future__ import annotations


MIGRATION_SOURCE_BY_CAPABILITY = {
    "stocks.finance.forecasts": "eastmoney_datacenter_financial_events",
    "stocks.finance.express": "eastmoney_datacenter_financial_events",
    "funds.etf.profile": "eastmoney_fundf10_profile",
    "indexes.members": "tushare_index_weight",
}

MIGRATION_CACHE_TOTAL_BYTES = 64 * 1024 * 1024
MIGRATION_CACHE_BYTES_BY_CAPABILITY = {
    "stocks.finance.forecasts": 16 * 1024 * 1024,
    "stocks.finance.express": 16 * 1024 * 1024,
    "funds.etf.profile": 4 * 1024 * 1024,
    "indexes.members": 32 * 1024 * 1024,
}
MIGRATION_CACHE_TTL_SECONDS_BY_CAPABILITY = {
    "stocks.finance.forecasts": 30 * 86400,
    "stocks.finance.express": 30 * 86400,
    "funds.etf.profile": 30 * 86400,
    "indexes.members": 30 * 86400,
}
