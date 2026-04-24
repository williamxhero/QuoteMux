from __future__ import annotations

from platform_models import NewsEventItem, NewsEventQueryResult
from quotemux.reports import ContractReport
from quotemux.runtime_core.registry import SourceProxy
from quotemux.settings import QuoteMuxSettings
from quotemux.store import load_store_result, store_result


_news_store = SourceProxy("news_store")


class QuoteMuxNews:
    def __init__(self, settings: QuoteMuxSettings) -> None:
        self._settings = settings

    def get_events(
        self,
        trade_date: str,
        announcement_date: str,
        crawl_date: str,
        stock_code: str,
        event_type: str,
        min_importance_score: int | None,
        sort_by: str,
        limit: int,
        offset: int,
        include_sources: bool,
        include_content_text: bool,
    ) -> NewsEventQueryResult:
        store_identity = {
            "trade_date": trade_date,
            "announcement_date": announcement_date,
            "crawl_date": crawl_date,
            "stock_code": stock_code,
            "event_type": event_type,
            "min_importance_score": min_importance_score,
            "sort_by": sort_by,
            "limit": limit,
            "offset": offset,
            "include_sources": include_sources,
            "include_content_text": include_content_text,
        }
        store_items, store_read = load_store_result("markets.events.news", store_identity, NewsEventItem)
        if store_read.hit:
            return NewsEventQueryResult(events=list(store_items))
        if not self._settings.is_source_enabled("news_store"):
            return NewsEventQueryResult(events=[])
        items = _news_store.get_news_events(
            trade_date,
            announcement_date,
            crawl_date,
            stock_code,
            event_type,
            min_importance_score,
            sort_by,
            limit,
            offset,
            include_content_text,
        )
        if include_sources and items != []:
            sources_by_event_id = _news_store.get_news_event_sources([item.event_id for item in items])
            items = [item.model_copy(update={"sources": sources_by_event_id.get(item.event_id, [])}) for item in items]
        store_result("markets.events.news", store_identity, items, ContractReport(contract_name="markets.events.news"))
        return NewsEventQueryResult(events=items)
