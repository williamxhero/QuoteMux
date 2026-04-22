from __future__ import annotations

from dataclasses import dataclass


FALLBACK_MODE_AUTO = "auto"
FALLBACK_MODE_DEGRADED = "degraded"
FALLBACK_MODE_A2_ONLY = "a2_only"


@dataclass(frozen=True)
class ContractPolicy:
    name: str
    mode: str
    source_order: tuple[str, ...]
    stage_namespace: tuple[str, ...]


CONTRACT_POLICIES = {
    "stocks.quotes.intraday": ContractPolicy(
        name="stocks.quotes.intraday",
        mode=FALLBACK_MODE_AUTO,
        source_order=("opentdx", "efinance", "mootdx", "akshare"),
        stage_namespace=("stocks", "quotes", "intraday"),
    ),
    "stocks.quotes.daily": ContractPolicy(
        name="stocks.quotes.daily",
        mode=FALLBACK_MODE_AUTO,
        source_order=("tushare", "efinance", "mootdx", "akshare"),
        stage_namespace=("stocks", "quotes", "daily"),
    ),
    "stocks.daily_snapshot": ContractPolicy(
        name="stocks.daily_snapshot",
        mode=FALLBACK_MODE_AUTO,
        source_order=("tushare", "efinance", "mootdx", "akshare"),
        stage_namespace=("stocks", "daily-snapshot"),
    ),
    "indexes.quotes.daily": ContractPolicy(
        name="indexes.quotes.daily",
        mode=FALLBACK_MODE_AUTO,
        source_order=("tushare", "opentdx", "efinance", "mootdx", "akshare"),
        stage_namespace=("indexes", "quotes", "daily"),
    ),
    "indexes.members": ContractPolicy(
        name="indexes.members",
        mode=FALLBACK_MODE_DEGRADED,
        source_order=("tushare", "efinance", "mootdx", "akshare"),
        stage_namespace=("indexes", "members"),
    ),
    "markets.trading_calendar": ContractPolicy(
        name="markets.trading_calendar",
        mode=FALLBACK_MODE_DEGRADED,
        source_order=("tushare", "akshare"),
        stage_namespace=("markets", "trading-calendar"),
    ),
    "updater.stock_bar_1m": ContractPolicy(
        name="updater.stock_bar_1m",
        mode=FALLBACK_MODE_AUTO,
        source_order=("opentdx", "efinance", "mootdx", "akshare"),
        stage_namespace=("updater", "stock-bar-1m"),
    ),
    "updater.index_bar_1d": ContractPolicy(
        name="updater.index_bar_1d",
        mode=FALLBACK_MODE_AUTO,
        source_order=("opentdx", "efinance", "mootdx", "akshare"),
        stage_namespace=("updater", "index-bar-1d"),
    ),
    "updater.stock_daily_1d.ohlcva": ContractPolicy(
        name="updater.stock_daily_1d.ohlcva",
        mode=FALLBACK_MODE_AUTO,
        source_order=("tushare", "efinance", "mootdx", "akshare"),
        stage_namespace=("updater", "stock-daily-1d", "ohlcva"),
    ),
    "stocks.indicators.daily_basic": ContractPolicy(
        name="stocks.indicators.daily_basic",
        mode=FALLBACK_MODE_A2_ONLY,
        source_order=("tushare",),
        stage_namespace=("stocks", "indicators", "daily-basic"),
    ),
    "stocks.indicators.daily_valuation": ContractPolicy(
        name="stocks.indicators.daily_valuation",
        mode=FALLBACK_MODE_A2_ONLY,
        source_order=("tushare",),
        stage_namespace=("stocks", "indicators", "daily-valuation"),
    ),
    "stocks.indicators.daily_market_value": ContractPolicy(
        name="stocks.indicators.daily_market_value",
        mode=FALLBACK_MODE_A2_ONLY,
        source_order=("tushare",),
        stage_namespace=("stocks", "indicators", "daily-market-value"),
    ),
    "stocks.finance.statements": ContractPolicy(
        name="stocks.finance.statements",
        mode=FALLBACK_MODE_A2_ONLY,
        source_order=("tushare",),
        stage_namespace=("stocks", "finance", "statements"),
    ),
    "reference.stock_basic": ContractPolicy(
        name="reference.stock_basic",
        mode=FALLBACK_MODE_A2_ONLY,
        source_order=("tushare",),
        stage_namespace=("reference", "stock-basic"),
    ),
}


AUTO_FALLBACK_CONTRACTS = {name for name, policy in CONTRACT_POLICIES.items() if policy.mode == FALLBACK_MODE_AUTO}
DEGRADED_FALLBACK_CONTRACTS = {name for name, policy in CONTRACT_POLICIES.items() if policy.mode == FALLBACK_MODE_DEGRADED}
A2_ONLY_CONTRACTS = {name for name, policy in CONTRACT_POLICIES.items() if policy.mode == FALLBACK_MODE_A2_ONLY}


def get_contract_policy(contract_name: str) -> ContractPolicy:
    policy = CONTRACT_POLICIES.get(contract_name)
    if policy is None:
        raise KeyError(f"未知 contract: {contract_name}")
    return policy


def is_auto_fallback_contract(contract_name: str) -> bool:
    return get_contract_policy(contract_name).mode == FALLBACK_MODE_AUTO


def is_degraded_fallback_contract(contract_name: str) -> bool:
    return get_contract_policy(contract_name).mode == FALLBACK_MODE_DEGRADED


def is_a2_only_contract(contract_name: str) -> bool:
    return get_contract_policy(contract_name).mode == FALLBACK_MODE_A2_ONLY
