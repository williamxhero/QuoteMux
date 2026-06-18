from __future__ import annotations

from dataclasses import dataclass


MODE_AUTO = "auto"
MODE_DEGRADED = "degraded"
MODE_A2_ONLY = "a2_only"

RESULT_SHAPE_SINGLE_RECORD = "single_record"
RESULT_SHAPE_KEYED_RECORDS = "keyed_records"
RESULT_SHAPE_TIME_SERIES = "time_series"
RESULT_SHAPE_REFERENCE_TABLE = "reference_table"
RESULT_SHAPE_EVENT_STREAM = "event_stream"
RESULT_SHAPE_RAW_PASSTHROUGH = "raw_passthrough"

MERGE_STRATEGY_FIRST_SUCCESS = "first_success"
MERGE_STRATEGY_PRIORITY_FALLBACK = "priority_fallback"
MERGE_STRATEGY_FRESHEST_WINS = "freshest_wins"
MERGE_STRATEGY_FIELD_CONSENSUS = "field_consensus"
MERGE_STRATEGY_APPEND_DEDUPE = "append_dedupe"

SUPPORT_LEVEL_NATIVE = "native"
SUPPORT_LEVEL_DERIVED = "derived"
SUPPORT_LEVEL_STATIC = "static"
SUPPORT_LEVEL_LOCAL_DB = "local_db"
SUPPORT_LEVEL_IMPORT_ONLY = "import_only"
DEFAULT_FRESHNESS_SECONDS = 365 * 86400


@dataclass(frozen=True)
class CapabilityDefinition:
    capability_id: str
    api_paths: tuple[str, ...]
    result_shape: str
    key_fields: tuple[str, ...]
    default_merge_strategy: str
    allowed_packages: tuple[str, ...]
    default_source_order: tuple[str, ...]
    policy_mode: str
    freshness_seconds: int
    store_enabled: bool


@dataclass(frozen=True)
class PublicApiCapabilityBinding:
    api_path: str
    capability_ids: tuple[str, ...]


PUBLIC_API_CAPABILITY_BINDINGS = (
    PublicApiCapabilityBinding("/api/stocks/quotes", ("stocks.quotes.intraday", "stocks.quotes.daily")),
    PublicApiCapabilityBinding("/api/stocks/quotes/daily-snapshot", ("stocks.quotes.daily_snapshot",)),
    PublicApiCapabilityBinding("/api/stocks/quotes/daily-local-window", ("stocks.quotes.daily",)),
    PublicApiCapabilityBinding("/api/stocks/catalog", ("stocks.catalog",)),
    PublicApiCapabilityBinding("/api/stocks/catalog/archive", ("stocks.catalog.archive",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/profile/basic", ("stocks.profile.basic",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/profile", ("stocks.profile.company",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/profile/name-history", ("stocks.profile.name_history",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/profile/managers", ("stocks.profile.managers",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/profile/management-rewards", ("stocks.profile.management_rewards",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/signals/hl", ("stocks.signals.hl",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/signals/nine-turn", ("stocks.signals.nine_turn",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/factors/adj", ("stocks.factors.adj",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/factors/technical", ("stocks.factors.technical",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/indicators/money-flow", ("stocks.indicators.money_flow",)),
    PublicApiCapabilityBinding("/api/stocks/indicators/money-flow/batch", ("stocks.indicators.money_flow.batch",)),
    PublicApiCapabilityBinding("/api/stocks/indicators/ah-comparisons", ("stocks.indicators.ah_comparisons",)),
    PublicApiCapabilityBinding("/api/stocks/indicators/daily-basic", ("stocks.indicators.daily_basic",)),
    PublicApiCapabilityBinding("/api/stocks/indicators/daily-valuation", ("stocks.indicators.daily_valuation",)),
    PublicApiCapabilityBinding("/api/stocks/indicators/daily-market-value", ("stocks.indicators.daily_market_value",)),
    PublicApiCapabilityBinding("/api/stocks/indicators/risk-flags", ("stocks.indicators.risk_flags",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/indicators/premarket", ("stocks.indicators.premarket",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/indicators/chip-distribution", ("stocks.indicators.chip_distribution",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/indicators/chip-performance", ("stocks.indicators.chip_performance",)),
    PublicApiCapabilityBinding("/api/stocks/finance/statements", ("stocks.finance.statements",)),
    PublicApiCapabilityBinding("/api/stocks/finance/indicators", ("stocks.finance.indicators",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/finance/audits", ("stocks.finance.audits",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/finance/disclosure-dates", ("stocks.finance.disclosure_dates",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/finance/express", ("stocks.finance.express",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/finance/forecasts", ("stocks.finance.forecasts",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/finance/main-business", ("stocks.finance.main_business",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/corporate-actions/dividends", ("stocks.corporate_actions.dividends",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/corporate-actions/repurchases", ("stocks.corporate_actions.repurchases",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/corporate-actions/rights-issues", ("stocks.corporate_actions.rights_issues",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/corporate-actions/share-changes", ("stocks.corporate_actions.share_changes",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/corporate-actions/unlock-schedules", ("stocks.corporate_actions.unlock_schedules",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/ownership/ccass-holdings", ("stocks.ownership.ccass_holdings",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/ownership/ccass-holding-details", ("stocks.ownership.ccass_holding_details",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/ownership/hk-connect-holdings", ("stocks.ownership.hk_connect_holdings",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/ownership/pledges/stats", ("stocks.ownership.pledges.stats",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/ownership/pledges/details", ("stocks.ownership.pledges.details",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/ownership/shareholders/count", ("stocks.ownership.shareholders.count",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/ownership/shareholders/changes", ("stocks.ownership.shareholders.changes",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/ownership/shareholders/top10", ("stocks.ownership.shareholders.top10",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/ownership/shareholders/top10-float", ("stocks.ownership.shareholders.top10_float",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/research/reports", ("stocks.research.reports",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/research/surveys", ("stocks.research.surveys",)),
    PublicApiCapabilityBinding("/api/stocks/reference/bse-code-mappings", ("stocks.reference.bse_code_mappings",)),
    PublicApiCapabilityBinding("/api/stocks/reference/hk-connect-targets", ("stocks.reference.hk_connect_targets",)),
    PublicApiCapabilityBinding("/api/stocks/{code}/quotes/auctions", ("stocks.quotes.auctions",)),
    PublicApiCapabilityBinding("/api/boards/quotes", ("boards.quotes.daily",)),
    PublicApiCapabilityBinding("/api/boards/quotes/daily-snapshot", ("boards.quotes.daily",)),
    PublicApiCapabilityBinding("/api/boards/catalog", ("boards.catalog",)),
    PublicApiCapabilityBinding("/api/boards/{board_code}/profile", ("boards.profile",)),
    PublicApiCapabilityBinding("/api/boards/{board_code}/members", ("boards.members",)),
    PublicApiCapabilityBinding("/api/boards/{board_code}/members/history", ("boards.members.history",)),
    PublicApiCapabilityBinding("/api/boards/{board_code}/indicators/money-flow", ("boards.indicators.money_flow",)),
    PublicApiCapabilityBinding("/api/boards/indicators/money-flow", ("boards.indicators.money_flow.snapshot",)),
    PublicApiCapabilityBinding("/api/boards/reference/categories", ("boards.reference.categories",)),
    PublicApiCapabilityBinding("/api/indexes/catalog", ("indexes.catalog",)),
    PublicApiCapabilityBinding("/api/indexes/{index_code}/profile", ("indexes.profile",)),
    PublicApiCapabilityBinding("/api/indexes/quotes", ("indexes.quotes.daily",)),
    PublicApiCapabilityBinding("/api/indexes/{index_code}/members", ("indexes.members",)),
    PublicApiCapabilityBinding("/api/markets/calendar/trading", ("markets.calendar.trading",)),
    PublicApiCapabilityBinding("/api/markets/calendar/trading/previous", ("markets.calendar.trading.previous",)),
    PublicApiCapabilityBinding("/api/markets/calendar/trading/next", ("markets.calendar.trading.next",)),
    PublicApiCapabilityBinding("/api/markets/calendar/trading/yearly", ("markets.calendar.trading.yearly",)),
    PublicApiCapabilityBinding("/api/markets/indicators/main-capital-flow", ("markets.indicators.main_capital_flow",)),
    PublicApiCapabilityBinding("/api/markets/connect/capital-flow", ("markets.connect.capital_flow",)),
    PublicApiCapabilityBinding("/api/markets/connect/quotas", ("markets.connect.quotas",)),
    PublicApiCapabilityBinding("/api/markets/connect/active-top10", ("markets.connect.active_top10",)),
    PublicApiCapabilityBinding("/api/markets/events/block-trades", ("markets.events.block_trades",)),
    PublicApiCapabilityBinding("/api/markets/participants/dragon-tiger", ("markets.participants.dragon_tiger",)),
    PublicApiCapabilityBinding("/api/markets/participants/dragon-tiger/institutions", ("markets.participants.dragon_tiger.institutions",)),
    PublicApiCapabilityBinding("/api/markets/participants/hot-money", ("markets.participants.hot_money",)),
    PublicApiCapabilityBinding("/api/markets/participants/hot-money/details", ("markets.participants.hot_money.details",)),
    PublicApiCapabilityBinding("/api/markets/trading/open-auctions", ("markets.trading.open_auctions",)),
    PublicApiCapabilityBinding("/api/markets/trading/sessions", ("markets.trading.sessions",)),
    PublicApiCapabilityBinding("/api/markets/events/news", ("markets.events.news",)),
    PublicApiCapabilityBinding("/api/rankings/research/reports", ("rankings.research.reports",)),
    PublicApiCapabilityBinding("/api/rankings/research/broker-monthly-picks", ("rankings.research.broker_monthly_picks",)),
)

LEGACY_CAPABILITY_ALIASES = {
    "boards.money_flow": "boards.indicators.money_flow",
    "boards.quotes": "boards.quotes.daily",
    "boards.reference": "boards.reference.categories",
    "indexes.quotes": "indexes.quotes.daily",
    "stocks.quotes": "stocks.quotes.daily",
    "markets.trading_calendar": "markets.calendar.trading",
    "reference.stock_basic": "stocks.profile.basic",
    "stocks.daily_snapshot": "stocks.quotes.daily_snapshot",
    "stocks.money_flow": "stocks.indicators.money_flow",
}

# ?????????? capability ??????????????????????
DERIVED_CAPABILITY_BASE_IDS = {
    "markets.calendar.trading.next": "markets.calendar.trading",
    "markets.calendar.trading.previous": "markets.calendar.trading",
    "markets.calendar.trading.yearly": "markets.calendar.trading",
}

STORE_TARGET_CAPABILITIES = {
    "stocks.quotes.intraday",
    "stocks.quotes.daily",
    "stocks.quotes.daily_snapshot",
    "indexes.quotes.daily",
    "markets.calendar.trading",
    "markets.events.news",
}

_API_PATHS_BY_CAPABILITY: dict[str, list[str]] = {}
for binding in PUBLIC_API_CAPABILITY_BINDINGS:
    for capability_id in binding.capability_ids:
        _API_PATHS_BY_CAPABILITY.setdefault(capability_id, []).append(binding.api_path)


def _default_merge_strategy(result_shape: str) -> str:
    if result_shape == RESULT_SHAPE_EVENT_STREAM:
        return MERGE_STRATEGY_APPEND_DEDUPE
    if result_shape == RESULT_SHAPE_TIME_SERIES:
        return MERGE_STRATEGY_APPEND_DEDUPE
    if result_shape == RESULT_SHAPE_REFERENCE_TABLE:
        return MERGE_STRATEGY_PRIORITY_FALLBACK
    if result_shape == RESULT_SHAPE_SINGLE_RECORD:
        return MERGE_STRATEGY_PRIORITY_FALLBACK
    return MERGE_STRATEGY_FIELD_CONSENSUS


def _infer_result_shape(capability_id: str) -> str:
    if capability_id == "markets.events.news":
        return RESULT_SHAPE_EVENT_STREAM
    if capability_id.endswith(".basic") or capability_id.endswith(".company") or capability_id in {"boards.profile", "indexes.profile"}:
        return RESULT_SHAPE_SINGLE_RECORD
    if capability_id.endswith(".catalog") or capability_id.endswith(".archive") or capability_id.startswith("stocks.reference.") or capability_id in {"boards.reference.categories", "markets.trading.sessions", "indexes.catalog"}:
        return RESULT_SHAPE_REFERENCE_TABLE
    if (
        ".quotes." in capability_id
        or capability_id.endswith(".quotes.daily")
        or capability_id.endswith(".money_flow")
        or capability_id.endswith(".snapshot")
        or capability_id.startswith("stocks.finance.")
        or capability_id.startswith("stocks.ownership.")
        or capability_id.startswith("stocks.research.")
        or capability_id.startswith("stocks.corporate_actions.")
        or capability_id.startswith("markets.calendar.")
        or capability_id.startswith("markets.indicators.")
        or capability_id.startswith("markets.connect.")
        or capability_id.startswith("markets.events.block_trades")
        or capability_id.startswith("markets.participants.")
        or capability_id.startswith("markets.trading.open_auctions")
    ):
        return RESULT_SHAPE_TIME_SERIES
    return RESULT_SHAPE_KEYED_RECORDS


def _infer_key_fields(capability_id: str) -> tuple[str, ...]:
    if capability_id.startswith("stocks.quotes."):
        return ("code", "trade_time", "freq")
    if capability_id == "stocks.indicators.money_flow":
        return ("code", "trade_date", "view")
    if capability_id.startswith("stocks.profile."):
        return ("code",)
    if capability_id.startswith("stocks.finance.statements"):
        return ("code", "report_period", "report_type")
    if capability_id.startswith("stocks.finance.") or capability_id.startswith("stocks.ownership.") or capability_id.startswith("stocks.corporate_actions.") or capability_id.startswith("stocks.research.") or capability_id.startswith("stocks.indicators.") or capability_id.startswith("stocks.signals.") or capability_id.startswith("stocks.factors."):
        return ("code", "trade_date")
    if capability_id.startswith("boards.quotes."):
        return ("board_code", "trade_time", "freq")
    if capability_id == "boards.indicators.money_flow":
        return ("board_code", "trade_date", "scope")
    if capability_id == "boards.indicators.money_flow.snapshot":
        return ("board_code", "trade_date", "scope")
    if capability_id.startswith("boards.members"):
        return ("board_code", "code")
    if capability_id.startswith("boards."):
        return ("board_code",)
    if capability_id.startswith("indexes.quotes."):
        return ("index_code", "trade_time", "freq")
    if capability_id == "indexes.members":
        return ("index_code", "code")
    if capability_id.startswith("indexes."):
        return ("index_code",)
    if capability_id.startswith("markets.calendar."):
        return ("exchange", "trade_date")
    if capability_id.startswith("markets.events.news"):
        return ("event_id",)
    if capability_id.startswith("markets.") or capability_id.startswith("rankings."):
        return ("trade_date",)
    if capability_id.startswith("stocks.catalog"):
        return ("code",)
    if capability_id.startswith("stocks.reference."):
        return ("code",)
    return ("id",)


def _infer_allowed_packages(capability_id: str) -> tuple[str, ...]:
    if capability_id in DERIVED_CAPABILITY_BASE_IDS:
        return ("derived_core",)
    if capability_id == "stocks.quotes.intraday":
        return ("opentdx", "efinance", "mootdx", "akshare")
    if capability_id == "stocks.quotes.daily":
        return ("tushare", "efinance", "mootdx", "akshare", "opentdx")
    if capability_id == "indexes.quotes.daily":
        return ("tushare", "akshare", "mootdx", "opentdx")
    if capability_id == "stocks.quotes.daily_snapshot":
        return ("tushare", "efinance", "akshare", "mootdx")
    if capability_id in {"indexes.members"}:
        return ("tushare", "efinance", "mootdx", "akshare")
    if capability_id.startswith("markets.calendar."):
        return ("tushare", "akshare")
    if capability_id in {"stocks.indicators.money_flow", "boards.indicators.money_flow"}:
        return ("tushare", "akshare")
    if capability_id in {"boards.catalog", "boards.profile", "boards.members", "boards.indicators.money_flow.snapshot", "boards.reference.categories"}:
        return ("tushare", "akshare")
    if capability_id == "boards.quotes.daily":
        return ("tushare", "efinance", "akshare")
    if capability_id in {"markets.connect.capital_flow", "markets.events.block_trades", "markets.indicators.main_capital_flow", "markets.participants.dragon_tiger.institutions"}:
        return ("tushare", "akshare")
    if capability_id == "markets.participants.dragon_tiger":
        return ("tushare", "akshare", "efinance")
    if capability_id in {"stocks.finance.express", "stocks.finance.indicators"}:
        return ("tushare", "akshare", "efinance")
    if capability_id in {"stocks.finance.disclosure_dates", "stocks.finance.forecasts", "stocks.finance.main_business", "stocks.finance.statements", "stocks.profile.company", "stocks.research.reports", "stocks.research.surveys"}:
        return ("tushare", "akshare")
    if capability_id in {
        "stocks.corporate_actions.dividends",
        "stocks.corporate_actions.repurchases",
        "stocks.corporate_actions.rights_issues",
        "stocks.corporate_actions.share_changes",
        "stocks.corporate_actions.unlock_schedules",
        "stocks.ownership.hk_connect_holdings",
        "stocks.ownership.pledges.details",
        "stocks.ownership.pledges.stats",
        "stocks.ownership.shareholders.top10",
        "stocks.ownership.shareholders.top10_float",
    }:
        return ("tushare", "akshare")
    if capability_id == "stocks.ownership.shareholders.changes":
        return ("derived_core", "tushare", "akshare")
    if capability_id == "stocks.ownership.shareholders.count":
        return ("tushare", "akshare", "efinance")
    if capability_id == "stocks.signals.hl":
        return ("derived_core", "tushare", "opentdx", "efinance", "mootdx", "akshare")
    if capability_id == "stocks.factors.technical":
        return ("derived_core", "tushare")
    if capability_id == "markets.connect.quotas":
        return ("tushare",)
    if capability_id == "markets.events.news":
        return ()
    if capability_id in {
        "stocks.catalog",
        "stocks.profile.basic",
        "stocks.profile.name_history",
        "stocks.factors.adj",
        "boards.quotes.daily",
        "boards.catalog",
        "boards.profile",
        "boards.members",
        "boards.members.history",
        "boards.indicators.money_flow.snapshot",
        "boards.reference.categories",
        "indexes.catalog",
        "indexes.profile",
        "markets.trading.sessions",
    }:
        return ("tushare",)
    return ("tushare",)


def _infer_source_order(capability_id: str) -> tuple[str, ...]:
    if capability_id in DERIVED_CAPABILITY_BASE_IDS or capability_id in {"stocks.factors.technical", "stocks.ownership.shareholders.changes", "stocks.signals.hl"}:
        return ("derived_core",)
    return _infer_allowed_packages(capability_id)


def _infer_policy_mode(capability_id: str) -> str:
    if capability_id in {"indexes.members", "markets.calendar.trading"}:
        return MODE_DEGRADED
    if capability_id.startswith("stocks.finance.") or capability_id.startswith("stocks.ownership.") or capability_id.startswith("stocks.research.") or capability_id.startswith("stocks.corporate_actions.") or capability_id.startswith("stocks.reference.") or capability_id in {"rankings.research.reports", "rankings.research.broker_monthly_picks"}:
        return MODE_A2_ONLY
    return MODE_AUTO


def _infer_freshness_seconds(capability_id: str) -> int:
    return DEFAULT_FRESHNESS_SECONDS


def _build_capability_definitions() -> tuple[CapabilityDefinition, ...]:
    definitions: list[CapabilityDefinition] = []
    for capability_id in sorted(_API_PATHS_BY_CAPABILITY):
        result_shape = _infer_result_shape(capability_id)
        definitions.append(
            CapabilityDefinition(
                capability_id=capability_id,
                api_paths=tuple(_API_PATHS_BY_CAPABILITY[capability_id]),
                result_shape=result_shape,
                key_fields=_infer_key_fields(capability_id),
                default_merge_strategy=_default_merge_strategy(result_shape),
                allowed_packages=_infer_allowed_packages(capability_id),
                default_source_order=_infer_source_order(capability_id),
                policy_mode=_infer_policy_mode(capability_id),
                freshness_seconds=_infer_freshness_seconds(capability_id),
                store_enabled=capability_id in STORE_TARGET_CAPABILITIES,
            )
        )
    return tuple(definitions)


CAPABILITY_DEFINITIONS = _build_capability_definitions()
_CAPABILITY_BY_ID = {definition.capability_id: definition for definition in CAPABILITY_DEFINITIONS}
_PUBLIC_API_BY_PATH = {binding.api_path: binding for binding in PUBLIC_API_CAPABILITY_BINDINGS}


def normalize_capability_id(capability_id: str) -> str:
    return LEGACY_CAPABILITY_ALIASES.get(capability_id, capability_id)


def get_capability_config_root(capability_id: str) -> str:
    normalized = normalize_capability_id(capability_id)
    return DERIVED_CAPABILITY_BASE_IDS.get(normalized, normalized)


def is_derived_capability_id(capability_id: str) -> bool:
    return normalize_capability_id(capability_id) in DERIVED_CAPABILITY_BASE_IDS


def is_independently_configurable_capability_id(capability_id: str) -> bool:
    return not is_derived_capability_id(capability_id)


def get_capability_definition(capability_id: str) -> CapabilityDefinition:
    normalized = normalize_capability_id(capability_id)
    definition = _CAPABILITY_BY_ID.get(normalized)
    if definition is None:
        raise KeyError(f"?? capability: {capability_id}")
    return definition


def list_capability_definitions() -> tuple[CapabilityDefinition, ...]:
    return CAPABILITY_DEFINITIONS


def list_capability_ids() -> tuple[str, ...]:
    return tuple(definition.capability_id for definition in CAPABILITY_DEFINITIONS)


def is_known_capability_id(capability_id: str) -> bool:
    return normalize_capability_id(capability_id) in _CAPABILITY_BY_ID


def get_public_api_binding(api_path: str) -> PublicApiCapabilityBinding:
    binding = _PUBLIC_API_BY_PATH.get(api_path)
    if binding is None:
        raise KeyError(f"?? Public API: {api_path}")
    return binding


def list_public_api_bindings() -> tuple[PublicApiCapabilityBinding, ...]:
    return PUBLIC_API_CAPABILITY_BINDINGS
