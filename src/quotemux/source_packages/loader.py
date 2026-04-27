from __future__ import annotations

import json
from pathlib import Path

from quotemux.capabilities import list_capability_definitions
from quotemux.capabilities.inventory import SUPPORT_LEVEL_NATIVE
from quotemux.source_packages.manifest import ConfigFieldSchema, SourcePackageCapability, SourcePackageManifest, _guess_handler_name


MANIFEST_FILE_NAME = "quotemux_package.json"


def _field(name: str, title: str, description: str, required: bool = False, default_value: str = "") -> ConfigFieldSchema:
    return ConfigFieldSchema(
        name=name,
        field_type="string",
        title=title,
        description=description,
        required=required,
        default_value=default_value,
    )


def _capabilities(package_id: str, support_level: str, handler_names: tuple[str, ...]) -> tuple[SourcePackageCapability, ...]:
    available_handlers = set(handler_names)
    items: list[SourcePackageCapability] = []
    for definition in list_capability_definitions():
        if package_id not in definition.allowed_packages:
            continue
        handler_name = _guess_handler_name(definition.capability_id, available_handlers)
        if handler_name == "":
            continue
        items.append(
            SourcePackageCapability(
                capability_id=definition.capability_id,
                support_level=support_level,
                handler_name=handler_name,
            )
        )
    return tuple(items)


def _manifest(
    package_id: str,
    display_name: str,
    description: str,
    support_level: str,
    capability_tags: tuple[str, ...],
    handler_names: tuple[str, ...],
    module_path: str,
    config_schema: tuple[ConfigFieldSchema, ...],
    secret_fields: tuple[str, ...],
    supports_multi_instance: bool,
) -> SourcePackageManifest:
    return SourcePackageManifest(
        package_id=package_id,
        version="2026.4.23",
        source_name=package_id,
        display_name=display_name,
        description=description,
        capabilities=_capabilities(package_id, support_level, handler_names),
        capability_tags=capability_tags,
        config_schema=config_schema,
        secret_fields=secret_fields,
        supports_multi_instance=supports_multi_instance,
        handler_targets=tuple((handler_name, f"{module_path}:{handler_name}") for handler_name in handler_names),
    )


def _build_builtin_manifests() -> tuple[SourcePackageManifest, ...]:
    tushare_schema = (
        _field("token", "Tushare Token", "Tushare API token", True),
        _field("timeout_seconds", "瓒呮椂绉掓暟", "Tushare 璇锋眰瓒呮椂绉掓暟", False, "15"),
    )
    opentdx_schema = (
        _field("host", "涓绘満", "OpenTDX 涓绘満", False),
        _field("port", "绔彛", "OpenTDX 绔彛", False),
    )
    http_schema = (
        _field("timeout_seconds", "瓒呮椂绉掓暟", "HTTP 璇锋眰瓒呮椂绉掓暟", False, "15"),
    )
    return (
        _manifest(
            "tushare",
            "Tushare",
            "Tushare 数据源。",
            SUPPORT_LEVEL_NATIVE,
            ("provider", "http", "tushare"),
            (
                "get_adj_factors",
                "get_auctions",
                "get_audits",
                "get_block_trades",
                "get_board_catalog",
                "get_board_categories",
                "get_board_daily_money_flow_snapshot",
                "get_board_member_history",
                "get_board_members",
                "get_board_money_flow",
                "get_board_profile",
                "get_board_quotes",
                "get_bse_code_mappings",
                "get_ccass_holding_details",
                "get_ccass_holdings",
                "get_chip_distribution",
                "get_chip_performance",
                "get_company_profile",
                "get_connect_active_top10",
                "get_connect_capital_flow",
                "get_connect_quotas",
                "get_disclosure_dates",
                "get_dividends",
                "get_dragon_tiger",
                "get_dragon_tiger_institutions",
                "get_express",
                "get_forecasts",
                "get_hk_connect_holdings",
                "get_hk_connect_targets",
                "get_hot_money_details",
                "get_hot_money_profiles",
                "get_index_catalog",
                "get_index_members",
                "get_index_quotes",
                "get_main_business",
                "get_market_capital_flow",
                "get_market_open_auctions",
                "get_management_rewards",
                "get_managers",
                "get_market_sessions",
                "get_nine_turn",
                "get_pledge_details",
                "get_pledge_stats",
                "get_premarket",
                "get_rank_broker_monthly_picks",
                "get_rank_research_reports",
                "get_repurchases",
                "get_research_reports",
                "get_rights_issues",
                "get_share_changes",
                "get_shareholder_changes",
                "get_shareholder_count",
                "get_shareholder_top10",
                "get_stock_ah_comparisons",
                "get_stock_archive",
                "get_stock_basic",
                "get_stock_catalog",
                "get_stock_daily_basic",
                "get_stock_daily_market_value",
                "get_stock_daily_snapshot",
                "get_stock_daily_snapshot_full",
                "get_stock_daily_valuation",
                "get_stock_financial_statements",
                "get_stock_finance_indicators",
                "get_stock_money_flow",
                "get_stock_name_history",
                "get_stock_quotes",
                "get_stock_risk_flags",
                "get_surveys",
                "get_technical_factors",
                "get_trading_calendar",
                "get_unlock_schedules",
            ),
            "quotemux.sources.tushare.source",
            tushare_schema,
            ("token",),
            True,
        ),
        _manifest(
            "efinance",
            "EFinance",
            "EFinance 数据源。",
            SUPPORT_LEVEL_NATIVE,
            ("provider", "http"),
            ("get_index_members", "get_index_quotes", "get_stock_quotes"),
            "quotemux.sources.efinance.source",
            http_schema,
            (),
            True,
        ),
        _manifest(
            "mootdx",
            "Mootdx",
            "Mootdx 数据源。",
            SUPPORT_LEVEL_NATIVE,
            ("provider", "socket"),
            ("get_index_members", "get_index_quotes", "get_stock_quotes"),
            "quotemux.sources.mootdx.source",
            opentdx_schema,
            (),
            True,
        ),
        _manifest(
            "opentdx",
            "OpenTDX",
            "OpenTDX 数据源。",
            SUPPORT_LEVEL_NATIVE,
            ("provider", "socket"),
            ("get_stock_quotes",),
            "quotemux.sources.opentdx.source",
            opentdx_schema,
            (),
            True,
        ),
        _manifest(
            "akshare",
            "AKShare",
            "AKShare 数据源。",
            SUPPORT_LEVEL_NATIVE,
            ("provider", "http"),
            ("get_index_members", "get_index_quotes", "get_stock_quotes", "get_trading_calendar"),
            "quotemux.sources.akshare.source",
            http_schema,
            (),
            True,
        ),
    )


def load_builtin_manifests() -> tuple[SourcePackageManifest, ...]:
    return _build_builtin_manifests()


def _iter_manifest_candidates(import_root: Path) -> list[Path]:
    if not import_root.exists():
        return []
    direct_file = import_root / MANIFEST_FILE_NAME
    if direct_file.is_file():
        return [direct_file]
    return sorted(import_root.glob(f"*/{MANIFEST_FILE_NAME}"))


def load_external_manifests(import_roots: tuple[str, ...]) -> tuple[SourcePackageManifest, ...]:
    manifests: list[SourcePackageManifest] = []
    for root_text in import_roots:
        root_path = Path(root_text)
        for manifest_path in _iter_manifest_candidates(root_path):
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifests.append(SourcePackageManifest.from_dict(payload, package_root=str(manifest_path.parent)))
    return tuple(manifests)
