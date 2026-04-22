from __future__ import annotations

from dataclasses import dataclass, field

from quotemux.config_runtime.models import SourceInstanceConfig
from quotemux.config_runtime.runtime import get_config_runtime


DEFAULT_ENABLED_SOURCES = (
    "datalake",
    "datalake_news",
    "datalake_reference",
    "local_topics",
    "tushare_market_topics",
    "tushare",
    "tushare_stock_chips",
    "tushare_stock_finance",
    "tushare_stock_ownership",
    "tushare_stocks",
    "opentdx",
    "efinance",
    "mootdx",
    "akshare",
)


@dataclass(frozen=True)
class QuoteMuxSettings:
    enabled_sources: tuple[str, ...] = ()
    contract_source_orders: dict[str, tuple[str, ...]] = field(default_factory=dict)

    def is_source_enabled(self, source_name: str) -> bool:
        if self.enabled_sources != ():
            return source_name in self.enabled_sources
        snapshot = get_config_runtime().get_active_snapshot()
        enabled_packages = snapshot.list_enabled_package_ids()
        if enabled_packages == ():
            return source_name in DEFAULT_ENABLED_SOURCES
        return source_name in enabled_packages

    def get_contract_source_order(self, contract_name: str, fallback: tuple[str, ...]) -> tuple[str, ...]:
        override = self.contract_source_orders.get(contract_name)
        if override is not None:
            return override
        snapshot = get_config_runtime().get_active_snapshot()
        return snapshot.get_contract_source_order(contract_name, fallback)

    def get_contract_mode(self, contract_name: str, fallback: str) -> str:
        snapshot = get_config_runtime().get_active_snapshot()
        return snapshot.get_contract_mode(contract_name, fallback)

    def get_contract_source_instances(self, contract_name: str, fallback: tuple[str, ...]) -> tuple[SourceInstanceConfig, ...]:
        if self.enabled_sources != ():
            return tuple(
                SourceInstanceConfig(
                    instance_id=f"{source_name}-default",
                    package_id=source_name,
                    display_name=source_name,
                    enabled=True,
                    priority=index + 1,
                    config_values={},
                    secret_values={},
                    tags=(),
                )
                for index, source_name in enumerate(self.get_contract_source_order(contract_name, fallback))
                if source_name in self.enabled_sources
            )
        snapshot = get_config_runtime().get_active_snapshot()
        instances = snapshot.get_contract_source_instances(contract_name, fallback)
        if instances == ():
            return tuple(
                SourceInstanceConfig(
                    instance_id=f"{source_name}-default",
                    package_id=source_name,
                    display_name=source_name,
                    enabled=True,
                    priority=index + 1,
                    config_values={},
                    secret_values={},
                    tags=(),
                )
                for index, source_name in enumerate(fallback)
                if source_name in DEFAULT_ENABLED_SOURCES
            )
        return instances

    def list_enabled_sources(self) -> tuple[str, ...]:
        if self.enabled_sources != ():
            return self.enabled_sources
        snapshot = get_config_runtime().get_active_snapshot()
        enabled_packages = snapshot.list_enabled_package_ids()
        if enabled_packages == ():
            return DEFAULT_ENABLED_SOURCES
        return enabled_packages
