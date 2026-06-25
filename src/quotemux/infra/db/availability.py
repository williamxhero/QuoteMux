from __future__ import annotations

from dataclasses import dataclass

from quotemux.infra.db.client import query_dataframe


@dataclass(frozen=True)
class FactRefObjectSpec:
    schema_name: str
    table_name: str
    required_indexes: tuple[str, ...]
    coverage_column: str

    @property
    def full_name(self) -> str:
        return f"{self.schema_name}.{self.table_name}"


OBJECT_SPECS = (
    FactRefObjectSpec("fact", "stock_daily_1d", ("stock_daily_1d_pkey", "stock_daily_1d_code_date_idx"), "trade_date"),
    FactRefObjectSpec("fact", "stock_bar_1m", ("stock_bar_1m_pkey", "stock_bar_1m_code_time_idx", "stock_bar_1m_time_idx"), "bar_time"),
    FactRefObjectSpec("fact", "stock_bar_30m", ("stock_bar_30m_pkey", "stock_bar_30m_code_time_idx", "stock_bar_30m_time_idx"), "bar_time"),
    FactRefObjectSpec("fact", "index_bar_1d", ("index_bar_1d_pkey", "index_bar_1d_date_idx"), "trade_date"),
    FactRefObjectSpec("fact", "concept_daily_1d", ("concept_daily_1d_pkey", "concept_daily_1d_date_idx"), "trade_date"),
    FactRefObjectSpec("ref", "trade_calendar", ("trade_calendar_pkey",), "trade_date"),
    FactRefObjectSpec("ref", "stock", ("stock_pkey", "stock_code_idx"), "listed_date"),
    FactRefObjectSpec("ref", "stock_name_history", ("stock_name_history_pkey",), "valid_from"),
    FactRefObjectSpec("ref", "concept", ("concept_pkey",), "listed_date"),
    FactRefObjectSpec("ref", "concept_stock_membership", ("concept_stock_membership_pkey", "concept_stock_membership_stock_idx"), "valid_from"),
    FactRefObjectSpec("ref", "index", ("index_pkey",), "list_date"),
)


def _existing_tables() -> set[str]:
    frame = query_dataframe(
        """
        select table_schema || '.' || table_name as full_name
        from information_schema.tables
        where table_schema in ('fact', 'ref')
        """
    )
    if frame.empty:
        return set()
    return {str(row["full_name"]) for _, row in frame.iterrows()}


def _existing_indexes() -> set[str]:
    frame = query_dataframe(
        """
        select indexname
        from pg_indexes
        where schemaname in ('fact', 'ref')
        """
    )
    if frame.empty:
        return set()
    return {str(row["indexname"]) for _, row in frame.iterrows()}


def _estimated_row_count(spec: FactRefObjectSpec) -> int:
    frame = query_dataframe("select greatest(reltuples::bigint, 0) as row_count from pg_class where oid = %s::regclass", (spec.full_name,))
    if frame.empty:
        return 0
    return int(frame.iloc[0]["row_count"])


def _boundary_value(spec: FactRefObjectSpec, direction: str) -> str:
    query = f"select {spec.coverage_column}::text as value from {spec.full_name} where {spec.coverage_column} is not null order by {spec.coverage_column} {direction} limit 1"
    frame = query_dataframe(query)
    if frame.empty:
        return ""
    value = frame.iloc[0]["value"]
    return "" if value is None else str(value)


def _coverage(spec: FactRefObjectSpec) -> dict[str, object]:
    return {
        "row_count": _estimated_row_count(spec),
        "min_value": _boundary_value(spec, "asc"),
        "max_value": _boundary_value(spec, "desc"),
    }


def get_fact_ref_availability() -> dict[str, object]:
    tables = _existing_tables()
    indexes = _existing_indexes()
    objects: list[dict[str, object]] = []
    warnings: list[str] = []
    for spec in OBJECT_SPECS:
        exists = spec.full_name in tables
        missing_indexes = [index_name for index_name in spec.required_indexes if index_name not in indexes]
        coverage = _coverage(spec) if exists else {"row_count": 0, "min_value": "", "max_value": ""}
        if not exists:
            warnings.append(f"缺少本地表 {spec.full_name}")
        if exists and missing_indexes != []:
            warnings.append(f"{spec.full_name} 缺少索引: {', '.join(missing_indexes)}")
        objects.append({"name": spec.full_name, "exists": exists, "missing_indexes": missing_indexes, **coverage})
    return {"status": "ok" if warnings == [] else "warning", "warnings": warnings, "objects": objects}
