from __future__ import annotations

from platform_models import NewsEventItem, NewsEventQueryResult
from quotemux.reports import ContractReport
from quotemux.settings import QuoteMuxSettings
from quotemux.sources.datalake.news import get_news_event_sources, get_news_events
from quotemux.store import load_store_result, store_result


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
        if (store_read.hit or store_read.partial_hit) and store_items != []:
            return NewsEventQueryResult(events=list(store_items))
        items = get_news_events(trade_date, announcement_date, crawl_date, stock_code, event_type, min_importance_score, sort_by, limit, offset, include_content_text)
        if include_sources and items != []:
            sources = get_news_event_sources([item.event_id for item in items])
            items = [item.model_copy(update={"sources": sources.get(item.event_id, [])}) for item in items]
        store_result("markets.events.news", store_identity, items, ContractReport(contract_name="markets.events.news"))
        return NewsEventQueryResult(events=items)

    def update_events_capture(
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
    ) -> tuple[list[NewsEventItem], ContractReport]:
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
        items = get_news_events(trade_date, announcement_date, crawl_date, stock_code, event_type, min_importance_score, sort_by, limit, offset, include_content_text)
        if include_sources and items != []:
            sources = get_news_event_sources([item.event_id for item in items])
            items = [item.model_copy(update={"sources": sources.get(item.event_id, [])}) for item in items]
        write_result = store_result("markets.events.news", store_identity, items, ContractReport(contract_name="markets.events.news"))
        return items, ContractReport(contract_name="markets.events.news").with_store_stats(write=write_result.status == "write")
