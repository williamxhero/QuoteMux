from __future__ import annotations

import json
import os
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path

from quotemux.config_runtime.models import ContractPolicyOverride, RuntimeProfile, SourceInstanceConfig
from quotemux.source_packages.manifest import SourcePackageManifest


LEGACY_PACKAGE_ALIASES: dict[str, str] = {}
LEGACY_CONTRACT_ALIASES = {
    "updater.stock_bar_1m": "stocks.quotes.intraday",
    "updater.index_bar_1d": "indexes.quotes.daily",
    "updater.stock_daily_1d.ohlcva": "stocks.quotes.daily",
    "updater": "",
    "boards.money_flow": "boards.indicators.money_flow",
    "boards.quotes": "boards.quotes.daily",
    "boards.reference": "boards.reference.categories",
    "indexes.quotes": "indexes.quotes.daily",
    "markets.trading_calendar": "markets.calendar.trading",
    "reference.stock_basic": "stocks.profile.basic",
    "stocks.daily_snapshot": "stocks.quotes.daily_snapshot",
    "stocks.money_flow": "stocks.indicators.money_flow",
}


def _runtime_root() -> Path:
    root_text = os.getenv("QUOTEMUX_RUNTIME_ROOT", "")
    if root_text != "":
        return Path(root_text)
    return Path.home() / ".quotemux" / "runtime"


def read_import_roots() -> tuple[str, ...]:
    store = RuntimeConfigStore(_runtime_root())
    return store.read_import_roots()


@dataclass(frozen=True)
class RuntimeState:
    active_profile_id: str
    previous_profile_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "active_profile_id": self.active_profile_id,
            "previous_profile_ids": list(self.previous_profile_ids),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> RuntimeState:
        return cls(
            active_profile_id=str(payload.get("active_profile_id", "")),
            previous_profile_ids=tuple(str(item) for item in payload.get("previous_profile_ids", [])),
        )


class RuntimeConfigStore:
    def __init__(self, root: Path) -> None:
        self._root = root
        self._instances_path = self._root / "instances.json"
        self._draft_policies_path = self._root / "draft_policies.json"
        self._profiles_path = self._root / "profiles.json"
        self._state_path = self._root / "state.json"
        self._imports_path = self._root / "imports.json"
        self._profile_transitions_path = self._root / "profile_transitions.json"

    def ensure_initialized(
        self,
        manifests: tuple[SourcePackageManifest, ...],
        default_policies: tuple[ContractPolicyOverride, ...],
    ) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        instances = self.read_instances()
        if instances == ():
            instances = self._build_default_instances(manifests)
            self.write_instances(instances)
        else:
            instances = self._reconcile_instances(instances, manifests)
            self.write_instances(instances)
        default_policies = self._resolve_policy_source_orders(instances, default_policies)
        if self.read_draft_policies() == ():
            self.write_draft_policies(default_policies)
        else:
            draft_policies = self._resolve_policy_source_orders(instances, self.read_draft_policies())
            self.write_draft_policies(self._merge_missing_default_policies(draft_policies, default_policies))
        profiles = self.read_profiles()
        if profiles == ():
            default_profile = self._build_default_profile(instances, default_policies)
            self.write_profiles((default_profile,))
            self.write_state(RuntimeState(active_profile_id=default_profile.profile_id, previous_profile_ids=()))
        else:
            profiles = tuple(self._resolve_profile_source_order(profile, instances, default_policies) for profile in profiles)
            self.write_profiles(profiles)
            if self.read_state().active_profile_id == "":
                self.write_state(RuntimeState(active_profile_id=profiles[-1].profile_id, previous_profile_ids=()))

    def read_instances(self) -> tuple[SourceInstanceConfig, ...]:
        payload = self._read_json(self._instances_path, [])
        return tuple(SourceInstanceConfig.from_dict(item) for item in payload if isinstance(item, dict))

    def write_instances(self, instances: tuple[SourceInstanceConfig, ...]) -> None:
        self._write_json(self._instances_path, [item.to_dict() for item in instances])

    def read_draft_policies(self) -> tuple[ContractPolicyOverride, ...]:
        payload = self._read_json(self._draft_policies_path, [])
        return tuple(ContractPolicyOverride.from_dict(item) for item in payload if isinstance(item, dict))

    def write_draft_policies(self, policies: tuple[ContractPolicyOverride, ...]) -> None:
        self._write_json(self._draft_policies_path, [item.to_dict() for item in policies])

    def read_profiles(self) -> tuple[RuntimeProfile, ...]:
        payload = self._read_json(self._profiles_path, [])
        return tuple(RuntimeProfile.from_dict(item) for item in payload if isinstance(item, dict))

    def write_profiles(self, profiles: tuple[RuntimeProfile, ...]) -> None:
        self._write_json(self._profiles_path, [item.to_dict() for item in profiles])

    def read_state(self) -> RuntimeState:
        payload = self._read_json(self._state_path, {})
        if not isinstance(payload, dict):
            return RuntimeState(active_profile_id="", previous_profile_ids=())
        return RuntimeState.from_dict(payload)

    def write_state(self, state: RuntimeState) -> None:
        self._write_json(self._state_path, state.to_dict())

    def read_import_roots(self) -> tuple[str, ...]:
        payload = self._read_json(self._imports_path, [])
        return tuple(str(item) for item in payload if str(item) != "")

    def write_import_roots(self, import_roots: tuple[str, ...]) -> None:
        self._write_json(self._imports_path, list(import_roots))

    def read_profile_transitions(self) -> tuple[dict[str, str], ...]:
        payload = self._read_json(self._profile_transitions_path, [])
        return tuple(
            {str(key): str(value) for key, value in item.items()}
            for item in payload
            if isinstance(item, dict)
        )

    def append_profile_transition(self, action: str, from_profile_id: str, to_profile_id: str) -> None:
        transitions = list(self.read_profile_transitions())
        transitions.append(
            {
                "action": action,
                "from_profile_id": from_profile_id,
                "to_profile_id": to_profile_id,
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
        self._write_json(self._profile_transitions_path, transitions[-200:])

    def _build_default_instances(self, manifests: tuple[SourcePackageManifest, ...]) -> tuple[SourceInstanceConfig, ...]:
        return tuple(self._build_default_instance(index, manifest) for index, manifest in enumerate(manifests))

    def _build_default_instance(self, index: int, manifest: SourcePackageManifest) -> SourceInstanceConfig:
        return SourceInstanceConfig(
            instance_id=f"{manifest.package_id}-default",
            package_id=manifest.package_id,
            display_name=f"{manifest.display_name} 默认实例",
            enabled=True,
            priority=index + 1,
            timeout_seconds=None,
            config_values={field.name: field.default_value for field in manifest.config_schema},
            secret_values={field_name: "" for field_name in manifest.secret_fields},
            tags=("builtin",) if manifest.origin == "builtin" else (),
        )

    def _reconcile_instances(
        self,
        instances: tuple[SourceInstanceConfig, ...],
        manifests: tuple[SourcePackageManifest, ...],
    ) -> tuple[SourceInstanceConfig, ...]:
        manifests_by_id = {manifest.package_id: manifest for manifest in manifests}
        resolved = [instance for instance in instances if instance.package_id in manifests_by_id]
        existing_packages = {instance.package_id for instance in resolved}
        for index, manifest in enumerate(manifests):
            if manifest.package_id not in existing_packages:
                resolved.append(self._build_default_instance(index, manifest))
        return tuple(sorted(resolved, key=lambda item: (item.priority, item.instance_id)))

    def _build_default_profile(
        self,
        instances: tuple[SourceInstanceConfig, ...],
        policies: tuple[ContractPolicyOverride, ...],
    ) -> RuntimeProfile:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return RuntimeProfile(
            profile_id="profile-default",
            display_name="默认 Profile",
            version="v1",
            created_at=timestamp,
            published_at=timestamp,
            note="系统初始化生成的默认 profile。",
            source_instances=instances,
            contract_policy_overrides=policies,
        )

    def _resolve_profile_source_order(
        self,
        profile: RuntimeProfile,
        instances: tuple[SourceInstanceConfig, ...],
        default_policies: tuple[ContractPolicyOverride, ...],
    ) -> RuntimeProfile:
        if profile.profile_id == "profile-default":
            return replace(profile, source_instances=instances, contract_policy_overrides=default_policies)
        policies = self._resolve_policy_source_orders(profile.source_instances, profile.contract_policy_overrides)
        policies = self._merge_missing_default_policies(policies, default_policies)
        return replace(profile, contract_policy_overrides=policies)

    def _resolve_policy_source_orders(
        self,
        instances: tuple[SourceInstanceConfig, ...],
        policies: tuple[ContractPolicyOverride, ...],
    ) -> tuple[ContractPolicyOverride, ...]:
        resolved: dict[str, ContractPolicyOverride] = {}
        for policy in policies:
            contract_name = LEGACY_CONTRACT_ALIASES.get(policy.contract_name, policy.contract_name)
            if contract_name == "":
                continue
            updated = replace(
                policy,
                contract_name=contract_name,
                source_order=self._resolve_source_order(contract_name, instances, policy.source_order),
            )
            if contract_name in resolved and policy.contract_name != contract_name:
                continue
            resolved[contract_name] = updated
        return tuple(sorted(resolved.values(), key=lambda item: item.contract_name))

    def _merge_missing_default_policies(
        self,
        policies: tuple[ContractPolicyOverride, ...],
        default_policies: tuple[ContractPolicyOverride, ...],
    ) -> tuple[ContractPolicyOverride, ...]:
        policy_by_name = {policy.contract_name: policy for policy in policies}
        for default_policy in default_policies:
            if default_policy.contract_name not in policy_by_name:
                policy_by_name[default_policy.contract_name] = default_policy
        return tuple(sorted(policy_by_name.values(), key=lambda item: item.contract_name))

    def _resolve_source_order(
        self,
        contract_name: str,
        instances: tuple[SourceInstanceConfig, ...],
        source_order: tuple[str, ...],
    ) -> tuple[str, ...]:
        instance_ids = {item.instance_id for item in instances}
        ordered: list[str] = []
        for source_id in source_order:
            source_id = self._normalize_source_id(contract_name, source_id)
            if source_id in instance_ids and source_id not in ordered:
                ordered.append(source_id)
                continue
            for instance in instances:
                if instance.package_id == source_id and instance.instance_id not in ordered:
                    ordered.append(instance.instance_id)
        return tuple(ordered)

    def _normalize_source_id(self, contract_name: str, source_id: str) -> str:
        if source_id == "datalake":
            return "tushare"
        return LEGACY_PACKAGE_ALIASES.get(source_id, source_id)

    def _read_json(self, path: Path, default: object):
        if not path.exists():
            return default
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default

    def _write_json(self, path: Path, payload: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_suffix(f"{path.suffix}.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(path)
