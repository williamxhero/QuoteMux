from __future__ import annotations

from platform_models import NewsEventQueryResult
from quotemux.runtime_core.registry import SourceProxy
from quotemux.settings import QuoteMuxSettings


_datalake_news = SourceProxy("datalake_news")


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
        if not self._settings.is_source_enabled("datalake_news"):
            return NewsEventQueryResult(events=[])
        items = _datalake_news.get_news_events(
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
            sources_by_event_id = _datalake_news.get_news_event_sources([item.event_id for item in items])
            items = [item.model_copy(update={"sources": sources_by_event_id.get(item.event_id, [])}) for item in items]
        return NewsEventQueryResult(events=items)



