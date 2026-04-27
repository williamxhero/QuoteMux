from __future__ import annotations

import pandas as pd

from quotemux.infra.db.news_reads import load_news_event_frame, load_news_event_source_frame
from platform_models import NewsEventItem, NewsEventSourceItem
from quotemux.infra.common import format_date_value


def _to_text_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item) != ""]
    if isinstance(value, tuple):
        return [str(item) for item in value if str(item) != ""]
    try:
        if pd.isna(value):
            return []
    except TypeError:
        pass
    text = str(value)
    return [text] if text != "" else []


def get_news_events(
    trade_date: str,
    announcement_date: str,
    crawl_date: str,
    stock_code: str,
    event_type: str,
    min_importance_score: int | None,
    sort_by: str,
    limit: int,
    offset: int,
    include_content_text: bool,
) -> list[NewsEventItem]:
    frame = load_news_event_frame(
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
    if frame.empty:
        return []
    items: list[NewsEventItem] = []
    for _, row in frame.iterrows():
        items.append(
            NewsEventItem(
                event_id=str(row["event_id"]),
                trade_date=format_date_value(row["trade_date"]),
                announcement_time=str(row["announcement_time"]),
                crawl_time=str(row["crawl_time"]),
                session_tag=str(row["session_tag"]),
                event_type=str(row["event_type"]),
                title=str(row["title"]),
                summary=str(row["summary"]),
                content_text=str(row["content_text"]) if include_content_text and "content_text" in row else "",
                importance_score=int(row["importance_score"]) if pd.notna(row["importance_score"]) else 0,
                sentiment=str(row["sentiment"]),
                source_name=str(row["source_name"]),
                primary_detail_url=str(row["primary_detail_url"]),
                related_stock_codes=_to_text_list(row["related_stock_codes"]),
                related_stock_names=_to_text_list(row["related_stock_names"]),
                related_board_codes=_to_text_list(row["related_board_codes"]),
                related_board_names=_to_text_list(row["related_board_names"]),
                topic_tags=_to_text_list(row["topic_tags"]),
                mentioned_stock_codes=_to_text_list(row["mentioned_stock_codes"]),
                mentioned_stock_names=_to_text_list(row["mentioned_stock_names"]),
                mentioned_board_names=_to_text_list(row["mentioned_board_names"]),
            )
        )
    return items


def get_news_event_sources(event_ids: list[str]) -> dict[str, list[NewsEventSourceItem]]:
    frame = load_news_event_source_frame(event_ids)
    if frame.empty:
        return {}
    sources_by_event_id: dict[str, list[NewsEventSourceItem]] = {}
    for _, row in frame.iterrows():
        event_id = str(row["event_id"])
        sources_by_event_id.setdefault(event_id, []).append(
            NewsEventSourceItem(
                source_table=str(row["source_table"]),
                source_record_id=str(row["source_record_id"]),
                source_name=str(row["source_name"]),
                source_type=str(row["source_type"]),
                detail_url=str(row["detail_url"]),
                announcement_time=str(row["announcement_time"]),
                crawl_time=str(row["crawl_time"]),
            )
        )
    return sources_by_event_id

