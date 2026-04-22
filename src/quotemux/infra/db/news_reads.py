from __future__ import annotations

import pandas as pd

from quotemux.infra.db.client import query_dataframe


def _coalesce_row_text(row_alias: str, alias: str, field_names: list[str], fallback_sql: str = "''") -> str:
    parts = [f"to_jsonb({row_alias})->>'{field_name}'" for field_name in field_names]
    parts.append(fallback_sql)
    return f"coalesce({', '.join(parts)}) as {alias}"


def load_news_event_frame(
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
) -> pd.DataFrame:
    if trade_date == "":
        return pd.DataFrame()
    announcement_time_sql = _coalesce_row_text(
        "source_row",
        "announcement_time",
        ["announcement_time", "announcement_at", "announcement_date"],
        "source_row.published_at::text",
    )
    crawl_time_sql = _coalesce_row_text(
        "source_row",
        "crawl_time",
        ["crawl_time", "crawled_at", "captured_at", "first_crawled_at", "first_seen_at"],
    )
    select_columns = [
        "event_id",
        "trade_date::text as trade_date",
        announcement_time_sql,
        crawl_time_sql,
        "session_tag",
        "event_type",
        "title",
        "summary",
        "importance_score",
        "sentiment",
        "source_name",
        "primary_detail_url",
        "related_stock_codes",
        "related_stock_names",
        "related_board_codes",
        "related_board_names",
        "topic_tags",
        "mentioned_stock_codes",
        "mentioned_stock_names",
        "mentioned_board_names",
    ]
    if include_content_text:
        select_columns.insert(7, "content_text")
    where_clauses = ["source_row.trade_date = %s"]
    params: list[object] = [trade_date]
    if stock_code != "":
        where_clauses.append("%s = any(related_stock_codes)")
        params.append(stock_code)
    if event_type != "":
        where_clauses.append("event_type = %s")
        params.append(event_type)
    if min_importance_score is not None:
        where_clauses.append("importance_score >= %s")
        params.append(min_importance_score)
    outer_where_clauses: list[str] = []
    if announcement_date != "":
        outer_where_clauses.append("left(announcement_time, 10) = %s")
        params.append(announcement_date)
    if crawl_date != "":
        outer_where_clauses.append("left(crawl_time, 10) = %s")
        params.append(crawl_date)
    order_by = "nullif(announcement_time, '') desc, importance_score desc, event_id desc"
    if sort_by == "crawl_time":
        order_by = "nullif(crawl_time, '') desc, nullif(announcement_time, '') desc, importance_score desc, event_id desc"
    params.extend([limit, offset])
    outer_where_sql = ""
    if outer_where_clauses != []:
        outer_where_sql = f"where {' and '.join(outer_where_clauses)}"
    query = f"""
        with base as (
            select
                {", ".join(select_columns)}
            from fact.news_event_agent_view as source_row
            where {' and '.join(where_clauses)}
        )
        select *
        from base
        {outer_where_sql}
        order by {order_by}
        limit %s
        offset %s
    """
    return query_dataframe(query, tuple(params))


def load_news_event_source_frame(event_ids: list[str]) -> pd.DataFrame:
    if event_ids == []:
        return pd.DataFrame()
    announcement_time_sql = _coalesce_row_text(
        "source_row",
        "announcement_time",
        ["announcement_time", "announcement_at", "announcement_date"],
        "source_row.published_at::text",
    )
    crawl_time_sql = _coalesce_row_text(
        "source_row",
        "crawl_time",
        ["crawl_time", "crawled_at", "captured_at", "first_crawled_at", "first_seen_at"],
    )
    query = """
        select
            event_id,
            source_table,
            source_record_id,
            source_name,
            source_type,
            detail_url,
            """
    query += announcement_time_sql
    query += """,
            """
    query += crawl_time_sql
    query += """
        from fact.news_event_source as source_row
        where event_id = any(%s)
        order by event_id, nullif(announcement_time, '') desc, source_table, source_record_id
    """
    return query_dataframe(query, (event_ids,))

