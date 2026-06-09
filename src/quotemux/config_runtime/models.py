from __future__ import annotations

from dataclasses import dataclass

from quotemux.capabilities import get_capability_config_root


@dataclass(frozen=True)
class SourceInstanceConfig:
    instance_id: str
    package_id: str
    display_name: str
    enabled: bool
    priority: int
    timeout_seconds: int | None
    config_values: dict[str, str]
    secret_values: dict[str, str]
    tags: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "instance_id": self.instance_id,
            "package_id": self.package_id,
            "display_name": self.display_name,
            "enabled": self.enabled,
            "priority": self.priority,
            "timeout_seconds": self.timeout_seconds,
            "config_values": dict(self.config_values),
            "secret_values": dict(self.secret_values),
            "tags": list(self.tags),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> SourceInstanceConfig:
        config_values = payload.get("config_values", {})
        secret_values = payload.get("secret_values", {})
        timeout_value = payload.get("timeout_seconds", None)
        return cls(
            instance_id=str(payload.get("instance_id", "")),
            package_id=str(payload.get("package_id", "")),
            display_name=str(payload.get("display_name", "")),
            enabled=bool(payload.get("enabled", False)),
            priority=int(payload.get("priority", 100)),
            timeout_seconds=int(timeout_value) if timeout_value not in {None, ""} else None,
            config_values={str(key): str(value) for key, value in config_values.items()} if isinstance(config_values, dict) else {},
            secret_values={str(key): str(value) for key, value in secret_values.items()} if isinstance(secret_values, dict) else {},
            tags=tuple(str(item) for item in payload.get("tags", [])),
        )


@dataclass(frozen=True)
class ContractPolicyOverride:
    contract_name: str
    mode: str
    source_order: tuple[str, ...]
    merge_strategy: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "contract_name": self.contract_name,
            "mode": self.mode,
            "source_order": list(self.source_order),
            "merge_strategy": self.merge_strategy,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> ContractPolicyOverride:
        return cls(
            contract_name=str(payload.get("contract_name", "")),
            mode=str(payload.get("mode", "")),
            source_order=tuple(str(item) for item in payload.get("source_order", [])),
            merge_strategy=str(payload.get("merge_strategy", "")),
        )


@dataclass(frozen=True)
class RuntimeProfile:
    profile_id: str
    display_name: str
    version: str
    created_at: str
    published_at: str
    note: str
    source_instances: tuple[SourceInstanceConfig, ...]
    contract_policy_overrides: tuple[ContractPolicyOverride, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "profile_id": self.profile_id,
            "display_name": self.display_name,
            "version": self.version,
            "created_at": self.created_at,
            "published_at": self.published_at,
            "note": self.note,
            "source_instances": [item.to_dict() for item in self.source_instances],
            "contract_policy_overrides": [item.to_dict() for item in self.contract_policy_overrides],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> RuntimeProfile:
        return cls(
            profile_id=str(payload.get("profile_id", "")),
            display_name=str(payload.get("display_name", "")),
            version=str(payload.get("version", "")),
            created_at=str(payload.get("created_at", "")),
            published_at=str(payload.get("published_at", "")),
            note=str(payload.get("note", "")),
            source_instances=tuple(SourceInstanceConfig.from_dict(item) for item in payload.get("source_instances", []) if isinstance(item, dict)),
            contract_policy_overrides=tuple(ContractPolicyOverride.from_dict(item) for item in payload.get("contract_policy_overrides", []) if isinstance(item, dict)),
        )


@dataclass(frozen=True)
class RuntimeSnapshot:
    profile_id: str
    version: str
    published_at: str
    source_instances: tuple[SourceInstanceConfig, ...]
    contract_policy_overrides: tuple[ContractPolicyOverride, ...]

    def is_source_enabled(self, package_id: str) -> bool:
        return any(item.enabled and item.package_id == package_id for item in self.source_instances)

    def list_enabled_package_ids(self) -> tuple[str, ...]:
        sorted_instances = sorted(self.source_instances, key=lambda item: (item.priority, item.instance_id))
        ordered_packages: list[str] = []
        for instance in sorted_instances:
            if not instance.enabled:
                continue
            if instance.package_id not in ordered_packages:
                ordered_packages.append(instance.package_id)
        return tuple(ordered_packages)

    def list_enabled_source_instances(self) -> tuple[SourceInstanceConfig, ...]:
        return tuple(
            item
            for item in sorted(self.source_instances, key=lambda current: (current.priority, current.instance_id))
            if item.enabled
        )

    def _contract_override(self, contract_name: str) -> ContractPolicyOverride | None:
        config_root = get_capability_config_root(contract_name)
        return next((item for item in self.contract_policy_overrides if get_capability_config_root(item.contract_name) == config_root), None)

    def get_contract_source_order(self, contract_name: str, fallback: tuple[str, ...]) -> tuple[str, ...]:
        del fallback
        override = self._contract_override(contract_name)
        if override is None:
            return ()
        enabled_instance_ids = {item.instance_id for item in self.list_enabled_source_instances()}
        return tuple(source_id for source_id in override.source_order if source_id in enabled_instance_ids)

    def get_contract_source_instances(self, contract_name: str, fallback: tuple[str, ...]) -> tuple[SourceInstanceConfig, ...]:
        del fallback
        override = self._contract_override(contract_name)
        enabled_instances = self.list_enabled_source_instances()
        if enabled_instances == () or override is None:
            return ()
        ordered: list[SourceInstanceConfig] = []
        for source_id in override.source_order:
            for instance in enabled_instances:
                if instance.instance_id == source_id and instance not in ordered:
                    ordered.append(instance)
        return tuple(ordered)

    def get_contract_mode(self, contract_name: str, fallback: str) -> str:
        override = self._contract_override(contract_name)
        if override is None or override.mode == "":
            return fallback
        return override.mode

    def get_contract_merge_strategy(self, contract_name: str, fallback: str) -> str:
        override = self._contract_override(contract_name)
        if override is None or override.merge_strategy == "":
            return fallback
        return override.merge_strategy


def build_runtime_snapshot(profile: RuntimeProfile) -> RuntimeSnapshot:
    return RuntimeSnapshot(
        profile_id=profile.profile_id,
        version=profile.version,
        published_at=profile.published_at,
        source_instances=profile.source_instances,
        contract_policy_overrides=profile.contract_policy_overrides,
    )
