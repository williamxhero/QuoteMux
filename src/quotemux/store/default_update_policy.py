from __future__ import annotations

from dataclasses import dataclass


SECONDS_PER_DAY = 86400
CACHE_NEVER_EXPIRE_TTL_DAYS = -1


@dataclass(frozen=True)
class CapabilityUpdatePolicyDefault:
    capability_id: str
    capture_enabled: bool
    capture_cadence: str
    cache_ttl_days: int


def _policy(capability_id: str, capture_enabled: bool, capture_cadence: str, cache_ttl_days: int) -> CapabilityUpdatePolicyDefault:
    return CapabilityUpdatePolicyDefault(capability_id, capture_enabled, capture_cadence, cache_ttl_days)


CAPABILITY_UPDATE_POLICY_DEFAULTS = (
    _policy("boards.catalog", True, "monthly", 365),
    _policy("boards.indicators.money_flow", True, "daily", 180),
    _policy("boards.indicators.money_flow.snapshot", True, "daily", 180),
    _policy("boards.members", True, "weekly", 365),
    _policy("boards.members.history", False, "daily", 365),
    _policy("boards.profile", False, "daily", 365),
    _policy("boards.quotes.daily", True, "daily", 30),
    _policy("boards.reference.categories", True, "monthly", CACHE_NEVER_EXPIRE_TTL_DAYS),
    _policy("indexes.catalog", True, "monthly", 365),
    _policy("indexes.members", True, "weekly", 365),
    _policy("indexes.profile", False, "daily", 365),
    _policy("indexes.quotes.daily", True, "daily", 30),
    _policy("markets.calendar.trading", True, "monthly", CACHE_NEVER_EXPIRE_TTL_DAYS),
    _policy("markets.calendar.trading.next", False, "daily", 30),
    _policy("markets.calendar.trading.previous", False, "daily", 365),
    _policy("markets.calendar.trading.yearly", False, "daily", 3650),
    _policy("markets.connect.active_top10", True, "daily", 180),
    _policy("markets.connect.capital_flow", True, "daily", 180),
    _policy("markets.connect.quotas", True, "daily", 180),
    _policy("markets.events.block_trades", True, "daily", 180),
    _policy("markets.events.news", True, "daily", 30),
    _policy("markets.indicators.main_capital_flow", True, "daily", 180),
    _policy("markets.participants.dragon_tiger", True, "daily", 180),
    _policy("markets.participants.dragon_tiger.institutions", True, "daily", 180),
    _policy("markets.participants.hot_money", True, "monthly", 365),
    _policy("markets.participants.hot_money.details", True, "daily", 180),
    _policy("markets.trading.open_auctions", True, "daily", 30),
    _policy("markets.trading.sessions", False, "daily", CACHE_NEVER_EXPIRE_TTL_DAYS),
    _policy("rankings.research.broker_monthly_picks", True, "weekly", 180),
    _policy("rankings.research.reports", True, "daily", 90),
    _policy("stocks.catalog", True, "monthly", 365),
    _policy("stocks.catalog.archive", True, "monthly", 365),
    _policy("stocks.corporate_actions.dividends", False, "daily", 365),
    _policy("stocks.corporate_actions.repurchases", False, "daily", 365),
    _policy("stocks.corporate_actions.rights_issues", False, "daily", 365),
    _policy("stocks.corporate_actions.share_changes", False, "daily", 365),
    _policy("stocks.corporate_actions.unlock_schedules", False, "daily", 365),
    _policy("stocks.factors.adj", False, "daily", 365),
    _policy("stocks.factors.technical", True, "daily", 30),
    _policy("stocks.finance.audits", False, "daily", 365),
    _policy("stocks.finance.disclosure_dates", False, "daily", 180),
    _policy("stocks.finance.express", False, "daily", 180),
    _policy("stocks.finance.forecasts", False, "daily", 180),
    _policy("stocks.finance.indicators", False, "daily", 365),
    _policy("stocks.finance.main_business", False, "daily", 365),
    _policy("stocks.finance.statements", False, "daily", 365),
    _policy("stocks.indicators.ah_comparisons", True, "daily", 180),
    _policy("stocks.indicators.chip_distribution", True, "daily", 180),
    _policy("stocks.indicators.chip_performance", True, "daily", 180),
    _policy("stocks.indicators.daily_basic", True, "daily", 180),
    _policy("stocks.indicators.daily_market_value", True, "daily", 180),
    _policy("stocks.indicators.daily_valuation", True, "daily", 180),
    _policy("stocks.indicators.money_flow", True, "daily", 180),
    _policy("stocks.indicators.premarket", True, "daily", 30),
    _policy("stocks.indicators.risk_flags", True, "daily", 180),
    _policy("stocks.ownership.ccass_holding_details", False, "daily", 180),
    _policy("stocks.ownership.ccass_holdings", False, "daily", 180),
    _policy("stocks.ownership.hk_connect_holdings", False, "daily", 180),
    _policy("stocks.ownership.pledges.details", False, "daily", 365),
    _policy("stocks.ownership.pledges.stats", False, "daily", 365),
    _policy("stocks.ownership.shareholders.changes", True, "weekly", 365),
    _policy("stocks.ownership.shareholders.count", False, "daily", 365),
    _policy("stocks.ownership.shareholders.top10", False, "daily", 365),
    _policy("stocks.ownership.shareholders.top10_float", False, "daily", 365),
    _policy("stocks.profile.basic", False, "daily", 365),
    _policy("stocks.profile.company", False, "daily", 365),
    _policy("stocks.profile.management_rewards", True, "monthly", 365),
    _policy("stocks.profile.managers", True, "monthly", 365),
    _policy("stocks.profile.name_history", False, "daily", 365),
    _policy("stocks.quotes.auctions", True, "daily", 30),
    _policy("stocks.quotes.daily", True, "daily", 30),
    _policy("stocks.quotes.daily_snapshot", True, "daily", 30),
    _policy("stocks.quotes.intraday", False, "daily", 1),
    _policy("stocks.reference.bse_code_mappings", True, "monthly", CACHE_NEVER_EXPIRE_TTL_DAYS),
    _policy("stocks.reference.hk_connect_targets", True, "monthly", 365),
    _policy("stocks.research.reports", False, "daily", 180),
    _policy("stocks.research.surveys", False, "daily", 180),
    _policy("stocks.signals.hl", True, "daily", 1),
    _policy("stocks.signals.nine_turn", True, "daily", 30),
)

_CAPABILITY_UPDATE_POLICY_DEFAULT_BY_ID = {item.capability_id: item for item in CAPABILITY_UPDATE_POLICY_DEFAULTS}


def get_capability_update_policy_default(capability_id: str) -> CapabilityUpdatePolicyDefault:
    return _CAPABILITY_UPDATE_POLICY_DEFAULT_BY_ID[capability_id]


def cache_enabled_from_ttl_days(ttl_days: int) -> bool:
    return ttl_days == CACHE_NEVER_EXPIRE_TTL_DAYS or ttl_days > 0


def ttl_seconds_from_days(ttl_days: int) -> int:
    if ttl_days == CACHE_NEVER_EXPIRE_TTL_DAYS:
        return CACHE_NEVER_EXPIRE_TTL_DAYS
    return ttl_days * SECONDS_PER_DAY
