from __future__ import annotations

from functools import lru_cache

from quotemux.sources.akshare import source as akshare_source
from quotemux.sources.base import SourceDefinition
from quotemux.sources.datalake import reference as datalake_reference_source
from quotemux.sources.datalake import source as datalake_source
from quotemux.sources.datalake_news import source as datalake_news_source
from quotemux.sources.efinance import source as efinance_source
from quotemux.sources.local_topics import source as local_topics_source
from quotemux.sources.mootdx import source as mootdx_source
from quotemux.sources.opentdx import source as opentdx_source
from quotemux.sources.tushare import source as tushare_source
from quotemux.sources.tushare_market_topics import source as tushare_market_topics_source
from quotemux.sources.tushare_stock_chips import source as tushare_stock_chips_source
from quotemux.sources.tushare_stock_finance import source as tushare_stock_finance_source
from quotemux.sources.tushare_stock_ownership import source as tushare_stock_ownership_source
from quotemux.sources.tushare_stocks import source as tushare_stocks_source


class SourceRegistry:
    def __init__(self, definitions: tuple[SourceDefinition, ...]) -> None:
        self._definitions = {definition.name: definition for definition in definitions}

    def get_source(self, source_name: str) -> SourceDefinition:
        definition = self._definitions.get(source_name)
        if definition is None:
            raise KeyError(f"未知 source: {source_name}")
        return definition

    def get_handler(self, source_name: str, handler_name: str):
        return self.get_source(source_name).get_handler(handler_name)

    def has_handler(self, source_name: str, handler_name: str) -> bool:
        definition = self._definitions.get(source_name)
        if definition is None:
            return False
        return definition.has_handler(handler_name)

    def list_sources(self) -> tuple[str, ...]:
        return tuple(self._definitions.keys())


class SourceProxy:
    def __init__(self, source_name: str) -> None:
        self._source_name = source_name

    def __getattr__(self, handler_name: str):
        try:
            return get_default_source_registry().get_handler(self._source_name, handler_name)
        except KeyError as exc:
            raise AttributeError(str(exc)) from exc


def _definition(name: str, module: object, handler_names: tuple[str, ...]) -> SourceDefinition:
    handlers: dict[str, object] = {}
    for handler_name in handler_names:
        if hasattr(module, handler_name):
            handlers[handler_name] = getattr(module, handler_name)
    return SourceDefinition(name=name, handlers=handlers)


@lru_cache(maxsize=1)
def get_default_source_registry() -> SourceRegistry:
    return SourceRegistry(
        (
            _definition(
                "datalake",
                datalake_source,
                (
                    "get_adj_factors",
                    "get_board_daily_money_flow_snapshot",
                    "get_board_money_flow",
                    "get_board_quotes",
                    "get_index_quotes",
                    "get_stock_daily_snapshot",
                    "get_stock_daily_snapshot_full",
                    "get_stock_money_flow",
                    "get_stock_quotes",
                ),
            ),
            _definition(
                "datalake_reference",
                datalake_reference_source,
                (
                    "get_board_catalog",
                    "get_board_categories",
                    "get_board_member_history",
                    "get_board_members",
                    "get_board_profile",
                    "get_hl_signal",
                    "get_index_catalog",
                    "get_index_profile",
                    "get_stock_active_codes",
                    "get_stock_basic",
                    "get_stock_catalog",
                    "get_stock_name_history",
                    "get_stock_names",
                    "get_trading_calendar",
                ),
            ),
            _definition("datalake_news", datalake_news_source, ("get_news_event_sources", "get_news_events")),
            _definition("local_topics", local_topics_source, ("get_market_sessions",)),
            _definition(
                "tushare",
                tushare_source,
                (
                    "get_board_money_flow",
                    "get_index_catalog",
                    "get_index_members",
                    "get_index_quotes",
                    "get_market_capital_flow",
                    "get_stock_financial_statements",
                    "get_stock_money_flow",
                    "get_stock_quotes",
                    "get_trading_calendar",
                ),
            ),
            _definition(
                "tushare_stocks",
                tushare_stocks_source,
                (
                    "get_auctions",
                    "get_bse_code_mappings",
                    "get_company_profile",
                    "get_hk_connect_targets",
                    "get_management_rewards",
                    "get_managers",
                    "get_nine_turn",
                    "get_premarket",
                    "get_rank_broker_monthly_picks",
                    "get_rank_research_reports",
                    "get_research_reports",
                    "get_stock_ah_comparisons",
                    "get_stock_archive",
                    "get_stock_daily_basic",
                    "get_stock_daily_market_value",
                    "get_stock_daily_valuation",
                    "get_stock_finance_indicators",
                    "get_stock_risk_flags",
                    "get_surveys",
                ),
            ),
            _definition(
                "tushare_stock_finance",
                tushare_stock_finance_source,
                (
                    "get_audits",
                    "get_disclosure_dates",
                    "get_dividends",
                    "get_express",
                    "get_forecasts",
                    "get_main_business",
                    "get_repurchases",
                    "get_rights_issues",
                    "get_share_changes",
                    "get_unlock_schedules",
                ),
            ),
            _definition(
                "tushare_stock_ownership",
                tushare_stock_ownership_source,
                (
                    "get_ccass_holding_details",
                    "get_ccass_holdings",
                    "get_hk_connect_holdings",
                    "get_pledge_details",
                    "get_pledge_stats",
                    "get_shareholder_count",
                    "get_shareholder_top10",
                ),
            ),
            _definition("tushare_stock_chips", tushare_stock_chips_source, ("get_chip_distribution", "get_chip_performance")),
            _definition(
                "tushare_market_topics",
                tushare_market_topics_source,
                (
                    "get_block_trades",
                    "get_connect_active_top10",
                    "get_connect_capital_flow",
                    "get_connect_quotas",
                    "get_dragon_tiger",
                    "get_dragon_tiger_institutions",
                    "get_hot_money_details",
                    "get_hot_money_profiles",
                    "get_market_open_auctions",
                ),
            ),
            _definition("efinance", efinance_source, ("get_index_members", "get_index_quotes", "get_stock_quotes")),
            _definition("mootdx", mootdx_source, ("get_index_members", "get_index_quotes", "get_stock_quotes")),
            _definition("opentdx", opentdx_source, ("get_index_quotes", "get_stock_quotes")),
            _definition("akshare", akshare_source, ("get_index_members", "get_index_quotes", "get_stock_quotes", "get_trading_calendar")),
        )
    )
