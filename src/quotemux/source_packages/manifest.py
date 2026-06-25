from __future__ import annotations

from dataclasses import dataclass

from quotemux.capabilities import normalize_capability_id


@dataclass(frozen=True)
class ConfigFieldSchema:
    name: str
    field_type: str
    title: str
    description: str = ""
    required: bool = False
    default_value: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "field_type": self.field_type,
            "title": self.title,
            "description": self.description,
            "required": self.required,
            "default_value": self.default_value,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> ConfigFieldSchema:
        return cls(
            name=str(payload.get("name", "")),
            field_type=str(payload.get("field_type", "string")),
            title=str(payload.get("title", "")),
            description=str(payload.get("description", "")),
            required=bool(payload.get("required", False)),
            default_value=str(payload.get("default_value", "")),
        )


@dataclass(frozen=True)
class SourcePackageCapability:
    capability_id: str
    support_level: str
    handler_name: str
    mergeable: bool = True
    notes: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "capability_id": self.capability_id,
            "support_level": self.support_level,
            "handler_name": self.handler_name,
            "mergeable": self.mergeable,
            "notes": self.notes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> SourcePackageCapability:
        return cls(
            capability_id=normalize_capability_id(str(payload.get("capability_id", ""))),
            support_level=str(payload.get("support_level", "native")),
            handler_name=str(payload.get("handler_name", "")),
            mergeable=bool(payload.get("mergeable", True)),
            notes=str(payload.get("notes", "")),
        )


@dataclass(frozen=True)
class SourcePackageManifest:
    package_id: str
    version: str
    source_name: str
    display_name: str
    description: str
    capabilities: tuple[SourcePackageCapability, ...]
    capability_tags: tuple[str, ...]
    config_schema: tuple[ConfigFieldSchema, ...]
    secret_fields: tuple[str, ...]
    supports_multi_instance: bool
    handler_targets: tuple[tuple[str, str], ...]
    origin: str = "builtin"
    package_root: str = ""

    @property
    def contract_names(self) -> tuple[str, ...]:
        return tuple(item.capability_id for item in self.capabilities)

    def get_handler_target(self, handler_name: str) -> str:
        for current_name, target in self.handler_targets:
            if current_name == handler_name:
                return target
        raise KeyError(f"package {self.package_id} 未注册 handler: {handler_name}")

    def get_handler_name_for_capability(self, capability_id: str) -> str:
        normalized = normalize_capability_id(capability_id)
        for capability in self.capabilities:
            if capability.capability_id == normalized:
                return capability.handler_name
        raise KeyError(f"package {self.package_id} 未声明 capability: {capability_id}")

    def supports_capability(self, capability_id: str) -> bool:
        normalized = normalize_capability_id(capability_id)
        return any(item.capability_id == normalized for item in self.capabilities)

    def capability_support_level(self, capability_id: str) -> str:
        normalized = normalize_capability_id(capability_id)
        for capability in self.capabilities:
            if capability.capability_id == normalized:
                return capability.support_level
        return ""

    def list_handler_names(self) -> tuple[str, ...]:
        return tuple(handler_name for handler_name, _ in self.handler_targets)

    def to_dict(self) -> dict[str, object]:
        return {
            "package_id": self.package_id,
            "version": self.version,
            "source_name": self.source_name,
            "display_name": self.display_name,
            "description": self.description,
            "capabilities": [item.to_dict() for item in self.capabilities],
            "capability_tags": list(self.capability_tags),
            "config_schema": [field.to_dict() for field in self.config_schema],
            "secret_fields": list(self.secret_fields),
            "supports_multi_instance": self.supports_multi_instance,
            "handler_targets": {handler_name: target for handler_name, target in self.handler_targets},
            "origin": self.origin,
            "package_root": self.package_root,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object], package_root: str = "") -> SourcePackageManifest:
        handler_targets_payload = payload.get("handler_targets", {})
        handler_targets: list[tuple[str, str]] = []
        if isinstance(handler_targets_payload, dict):
            for handler_name, target in handler_targets_payload.items():
                handler_targets.append((str(handler_name), str(target)))
        capabilities = _load_capabilities(payload)
        return cls(
            package_id=str(payload.get("package_id", "")),
            version=str(payload.get("version", "")),
            source_name=str(payload.get("source_name", "")),
            display_name=str(payload.get("display_name", "")),
            description=str(payload.get("description", "")),
            capabilities=capabilities,
            capability_tags=tuple(str(item) for item in payload.get("capability_tags", [])),
            config_schema=tuple(ConfigFieldSchema.from_dict(item) for item in payload.get("config_schema", []) if isinstance(item, dict)),
            secret_fields=tuple(str(item) for item in payload.get("secret_fields", [])),
            supports_multi_instance=bool(payload.get("supports_multi_instance", False)),
            handler_targets=tuple(handler_targets),
            origin=str(payload.get("origin", "external")),
            package_root=package_root or str(payload.get("package_root", "")),
        )


def _load_capabilities(payload: dict[str, object]) -> tuple[SourcePackageCapability, ...]:
    capabilities_payload = payload.get("capabilities", [])
    if isinstance(capabilities_payload, list) and capabilities_payload != []:
        return tuple(SourcePackageCapability.from_dict(item) for item in capabilities_payload if isinstance(item, dict))
    legacy_contract_names = payload.get("contract_names", [])
    if not isinstance(legacy_contract_names, list):
        return ()
    handler_targets_payload = payload.get("handler_targets", {})
    available_handlers = set(handler_targets_payload) if isinstance(handler_targets_payload, dict) else set()
    capabilities: list[SourcePackageCapability] = []
    for capability_id in legacy_contract_names:
        handler_name = _guess_handler_name(str(capability_id), available_handlers)
        capabilities.append(
            SourcePackageCapability(
                capability_id=normalize_capability_id(str(capability_id)),
                support_level="native",
                handler_name=handler_name,
            )
        )
    return tuple(capabilities)


def _guess_handler_name(capability_id: str, available_handlers: set[str]) -> str:
    normalized = normalize_capability_id(capability_id)
    handler_map = {
        "concepts.catalog": "get_concept_catalog",
        "concepts.indicators.money_flow": "get_concept_money_flow",
        "concepts.indicators.money_flow.snapshot": "get_concept_daily_money_flow_snapshot",
        "concepts.members": "get_concept_members",
        "concepts.members.history": "get_concept_member_history",
        "concepts.profile": "get_concept_profile",
        "concepts.quotes.daily": "get_concept_quotes",
        "concepts.reference.categories": "get_concept_categories",
        "indexes.catalog": "get_index_catalog",
        "indexes.members": "get_index_members",
        "indexes.profile": "get_index_profile",
        "indexes.quotes.daily": "get_index_quotes",
        "markets.calendar.trading": "get_trading_calendar",
        "markets.connect.active_top10": "get_connect_active_top10",
        "markets.connect.capital_flow": "get_connect_capital_flow",
        "markets.connect.quotas": "get_connect_quotas",
        "markets.events.block_trades": "get_block_trades",
        "markets.events.news": "get_news_events",
        "markets.indicators.main_capital_flow": "get_market_capital_flow",
        "markets.participants.dragon_tiger": "get_dragon_tiger",
        "markets.participants.dragon_tiger.institutions": "get_dragon_tiger_institutions",
        "markets.participants.hot_money": "get_hot_money_profiles",
        "markets.participants.hot_money.details": "get_hot_money_details",
        "markets.trading.open_auctions": "get_market_open_auctions",
        "markets.trading.sessions": "get_market_sessions",
        "rankings.research.broker_monthly_picks": "get_rank_broker_monthly_picks",
        "rankings.research.reports": "get_rank_research_reports",
        "stocks.catalog": "get_stock_catalog",
        "stocks.catalog.archive": "get_stock_archive",
        "stocks.corporate_actions.dividends": "get_dividends",
        "stocks.corporate_actions.repurchases": "get_repurchases",
        "stocks.corporate_actions.rights_issues": "get_rights_issues",
        "stocks.corporate_actions.share_changes": "get_share_changes",
        "stocks.corporate_actions.unlock_schedules": "get_unlock_schedules",
        "stocks.factors.adj": "get_adj_factors",
        "stocks.factors.technical": "get_technical_factors",
        "stocks.finance.audits": "get_audits",
        "stocks.finance.disclosure_dates": "get_disclosure_dates",
        "stocks.finance.express": "get_express",
        "stocks.finance.forecasts": "get_forecasts",
        "stocks.finance.indicators": "get_stock_finance_indicators",
        "stocks.finance.main_business": "get_main_business",
        "stocks.finance.statements": "get_stock_financial_statements",
        "stocks.indicators.ah_comparisons": "get_stock_ah_comparisons",
        "stocks.indicators.chip_distribution": "get_chip_distribution",
        "stocks.indicators.chip_performance": "get_chip_performance",
        "stocks.indicators.daily_basic": "get_stock_daily_basic",
        "stocks.indicators.daily_market_value": "get_stock_daily_market_value",
        "stocks.indicators.daily_valuation": "get_stock_daily_valuation",
        "stocks.indicators.money_flow": "get_stock_money_flow",
        "stocks.indicators.money_flow.batch": "get_stock_money_flow_batch",
        "stocks.indicators.premarket": "get_premarket",
        "stocks.indicators.risk_flags": "get_stock_risk_flags",
        "stocks.ownership.ccass_holding_details": "get_ccass_holding_details",
        "stocks.ownership.ccass_holdings": "get_ccass_holdings",
        "stocks.ownership.hk_connect_holdings": "get_hk_connect_holdings",
        "stocks.ownership.pledges.details": "get_pledge_details",
        "stocks.ownership.pledges.stats": "get_pledge_stats",
        "stocks.ownership.shareholders.count": "get_shareholder_count",
        "stocks.ownership.shareholders.changes": "get_shareholder_changes",
        "stocks.ownership.shareholders.top10": "get_shareholder_top10",
        "stocks.ownership.shareholders.top10_float": "get_shareholder_top10",
        "stocks.profile.basic": "get_stock_basic",
        "stocks.profile.company": "get_company_profile",
        "stocks.profile.management_rewards": "get_management_rewards",
        "stocks.profile.managers": "get_managers",
        "stocks.profile.name_history": "get_stock_name_history",
        "stocks.quotes.auctions": "get_auctions",
        "stocks.quotes.daily": "get_stock_quotes",
        "stocks.quotes.daily_snapshot": "get_stock_daily_snapshot_full",
        "stocks.quotes.intraday": "get_stock_quotes",
        "stocks.reference.bse_code_mappings": "get_bse_code_mappings",
        "stocks.reference.hk_connect_targets": "get_hk_connect_targets",
        "stocks.research.reports": "get_research_reports",
        "stocks.research.surveys": "get_surveys",
        "stocks.signals.hl": "get_hl_signal",
        "stocks.signals.limit_order_amount": "get_limit_order_amount",
        "stocks.signals.nine_turn": "get_nine_turn",
    }
    handler_name = handler_map.get(normalized, "")
    if normalized == "stocks.quotes.daily_snapshot" and handler_name not in available_handlers and "get_stock_quotes" in available_handlers:
        return "get_stock_quotes"
    if handler_name in available_handlers:
        return handler_name
    if len(available_handlers) == 1:
        return next(iter(available_handlers))
    return handler_name
