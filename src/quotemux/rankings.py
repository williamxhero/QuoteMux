from __future__ import annotations

from platform_models import RankingBrokerPickItem, RankingResearchReportItem
from quotemux.common import ensure_limit, merge_model_lists
from quotemux.reports import ContractReport
from quotemux.runtime_core.executor import SourceInstanceExecutor, run_fallback_chain_with_report
from quotemux.source_packages.registry import get_default_source_package_registry
from quotemux.settings import QuoteMuxSettings
from quotemux.store import load_store_result, store_result


def _source_package_call(package_id: str, handler_name: str, *args: object) -> object:
    handler = get_default_source_package_registry().get_handler(package_id, handler_name)
    return handler(*args)


class QuoteMuxRankings:
    def __init__(self, settings: QuoteMuxSettings) -> None:
        self._settings = settings

    def _source_list(self, capability_id: str, handlers: dict[str, object], source_order: tuple[str, ...], key_fields: tuple[str, ...]) -> list[object]:
        items, _ = run_fallback_chain_with_report(
            capability_id,
            [],
            key_fields,
            lambda current_items: [()] if current_items == [] else [],
            SourceInstanceExecutor(self._settings).build_steps(capability_id, handlers, source_order),
            self._settings.get_contract_source_order(capability_id, source_order),
        )
        return items

    def get_research_reports(self, trade_date: str, start_date: str, end_date: str, limit: int) -> list[RankingResearchReportItem]:
        store_identity = {"trade_date": trade_date, "start_date": start_date, "end_date": end_date, "limit": limit}
        store_items, store_read = load_store_result("rankings.research.reports", store_identity, RankingResearchReportItem)
        if store_read.hit:
            return list(store_items)[: ensure_limit(limit)]
        handlers = {
            "get_rank_research_reports": lambda instance: lambda: _source_package_call(instance.package_id, "get_rank_research_reports", trade_date, start_date, end_date, ensure_limit(limit)),
        }
        items = self._source_list("rankings.research.reports", handlers, ("tushare",), ("trade_date", "code", "institution", "title"))
        if store_read.partial_hit:
            items = merge_model_lists(store_items, items, ("trade_date", "code", "institution", "title"))
        items = sorted(items, key=lambda item: (item.trade_date, item.code, item.institution, item.title))
        store_result("rankings.research.reports", store_identity, items, ContractReport(contract_name="rankings.research.reports"))
        return items[: ensure_limit(limit)]

    def get_broker_monthly_picks(self, trade_month: str, limit: int) -> list[RankingBrokerPickItem]:
        store_identity = {"trade_month": trade_month, "limit": limit}
        store_items, store_read = load_store_result("rankings.research.broker_monthly_picks", store_identity, RankingBrokerPickItem)
        if store_read.hit:
            return list(store_items)[: ensure_limit(limit)]
        handlers = {
            "get_rank_broker_monthly_picks": lambda instance: lambda: _source_package_call(instance.package_id, "get_rank_broker_monthly_picks", trade_month, ensure_limit(limit)),
        }
        items = self._source_list("rankings.research.broker_monthly_picks", handlers, ("tushare",), ("trade_month", "code", "institution"))
        if store_read.partial_hit:
            items = merge_model_lists(store_items, items, ("trade_month", "code", "institution"))
        items = sorted(items, key=lambda item: (item.trade_month, item.rank or 0, item.code, item.institution))
        store_result("rankings.research.broker_monthly_picks", store_identity, items, ContractReport(contract_name="rankings.research.broker_monthly_picks"))
        return items[: ensure_limit(limit)]
