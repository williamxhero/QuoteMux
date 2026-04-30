from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from functools import lru_cache
from pathlib import Path

from quotemux.config_runtime.models import ContractPolicyOverride, RuntimeProfile, RuntimeSnapshot, SourceInstanceConfig, build_runtime_snapshot
from quotemux.config_runtime.store import RuntimeConfigStore, RuntimeState, _runtime_root
from quotemux.config_runtime.validation import ConfigValidationError, validate_instance, validate_profile
from quotemux.contracts.policies import list_default_contract_policies
from quotemux.source_packages.installer import install_source_package_directory
from quotemux.source_packages.registry import build_source_package_registry, clear_loaded_source_package_modules, refresh_default_source_package_registry


class QuoteMuxConfigRuntime:
    def __init__(self, root: Path | None = None) -> None:
        actual_root = Path(root) if root is not None else _runtime_root()
        self._store = RuntimeConfigStore(actual_root)

    def ensure_initialized(self) -> None:
        registry = build_source_package_registry(self._store.read_import_roots())
        self._store.ensure_initialized(registry.list_packages(), list_default_contract_policies())

    def list_source_packages(self):
        self.ensure_initialized()
        return build_source_package_registry(self._store.read_import_roots()).list_packages()

    def refresh_source_packages(self):
        self.ensure_initialized()
        refresh_default_source_package_registry()
        return self.list_source_packages()

    def list_package_health(self):
        self.ensure_initialized()
        return build_source_package_registry(self._store.read_import_roots()).list_package_health()

    def get_package_health(self, package_id: str):
        self.ensure_initialized()
        return build_source_package_registry(self._store.read_import_roots()).check_package_health(package_id)

    def list_import_roots(self) -> tuple[str, ...]:
        self.ensure_initialized()
        return self._store.read_import_roots()

    def add_import_root(self, path_text: str) -> tuple[str, ...]:
        installed_root = install_source_package_directory(Path(path_text), self._store.package_install_root())
        current = list(self._store.read_import_roots())
        if installed_root not in current:
            current.insert(0, installed_root)
            self._store.write_import_roots(tuple(current))
        clear_loaded_source_package_modules()
        refresh_default_source_package_registry()
        self.ensure_initialized()
        return self._store.read_import_roots()

    def list_source_instances(self) -> tuple[SourceInstanceConfig, ...]:
        self.ensure_initialized()
        return self._store.read_instances()

    def save_source_instance(self, instance: SourceInstanceConfig) -> SourceInstanceConfig:
        self.ensure_initialized()
        registry = build_source_package_registry(self._store.read_import_roots())
        validate_instance(instance, registry, self._store.read_instances())
        current = {item.instance_id: item for item in self._store.read_instances()}
        current[instance.instance_id] = instance
        instances = tuple(sorted(current.values(), key=lambda item: (item.priority, item.instance_id)))
        self._store.write_instances(instances)
        return instance

    def delete_source_instance(self, instance_id: str) -> None:
        self.ensure_initialized()
        instances = tuple(item for item in self._store.read_instances() if item.instance_id != instance_id)
        self._store.write_instances(instances)

    def update_source_instance_enabled(self, instance_id: str, enabled: bool) -> SourceInstanceConfig:
        self.ensure_initialized()
        current = {item.instance_id: item for item in self._store.read_instances()}
        instance = current.get(instance_id)
        if instance is None:
            raise KeyError(f"未知 source instance: {instance_id}")
        updated = replace(instance, enabled=enabled)
        current[instance_id] = updated
        self._store.write_instances(tuple(sorted(current.values(), key=lambda item: (item.priority, item.instance_id))))
        return updated

    def list_draft_policies(self) -> tuple[ContractPolicyOverride, ...]:
        self.ensure_initialized()
        return self._store.read_draft_policies()

    def save_draft_policy(self, policy: ContractPolicyOverride) -> ContractPolicyOverride:
        self.ensure_initialized()
        profile = RuntimeProfile(
            profile_id="draft",
            display_name="draft",
            version="draft",
            created_at="",
            published_at="",
            note="",
            source_instances=self._store.read_instances(),
            contract_policy_overrides=(policy,),
        )
        validate_profile(profile, build_source_package_registry(self._store.read_import_roots()))
        current = {item.contract_name: item for item in self._store.read_draft_policies()}
        current[policy.contract_name] = policy
        policies = tuple(sorted(current.values(), key=lambda item: item.contract_name))
        self._store.write_draft_policies(policies)
        return policy

    def list_profiles(self) -> tuple[RuntimeProfile, ...]:
        self.ensure_initialized()
        return self._store.read_profiles()

    def validate_draft_profile(self) -> dict[str, object]:
        self.ensure_initialized()
        profile = self._build_draft_profile()
        try:
            validate_profile(profile, build_source_package_registry(self._store.read_import_roots()))
        except ConfigValidationError as exc:
            return {
                "valid": False,
                "issues": [{"field": issue.field, "message": issue.message} for issue in exc.issues],
            }
        return {"valid": True, "issues": []}

    def diff_draft_profile(self) -> dict[str, object]:
        self.ensure_initialized()
        active_profile = self.get_active_profile()
        draft_profile = self._build_draft_profile()
        active_instances = {item.instance_id: item for item in active_profile.source_instances}
        draft_instances = {item.instance_id: item for item in draft_profile.source_instances}
        active_policies = {item.contract_name: item for item in active_profile.contract_policy_overrides}
        draft_policies = {item.contract_name: item for item in draft_profile.contract_policy_overrides}
        added_instances = tuple(instance_id for instance_id in draft_instances if instance_id not in active_instances)
        removed_instances = tuple(instance_id for instance_id in active_instances if instance_id not in draft_instances)
        changed_instances = tuple(instance_id for instance_id, instance in draft_instances.items() if instance_id in active_instances and instance != active_instances[instance_id])
        policy_changes = tuple(contract_name for contract_name, policy in draft_policies.items() if active_policies.get(contract_name) != policy)
        return {
            "from_profile_id": active_profile.profile_id,
            "added_instances": list(added_instances),
            "removed_instances": list(removed_instances),
            "changed_instances": list(changed_instances),
            "policy_changes": list(policy_changes),
        }

    def publish_profile(self, display_name: str, note: str) -> RuntimeProfile:
        self.ensure_initialized()
        profiles = list(self._store.read_profiles())
        state = self._store.read_state()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        profile_id = f"profile-{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
        next_version = f"v{len(profiles) + 1}"
        profile = RuntimeProfile(
            profile_id=profile_id,
            display_name=display_name or profile_id,
            version=next_version,
            created_at=timestamp,
            published_at=timestamp,
            note=note,
            source_instances=self._store.read_instances(),
            contract_policy_overrides=self._store.read_draft_policies(),
        )
        validate_profile(profile, build_source_package_registry(self._store.read_import_roots()))
        profiles.append(profile)
        self._store.write_profiles(tuple(profiles))
        previous_ids = list(state.previous_profile_ids)
        if state.active_profile_id != "":
            previous_ids.append(state.active_profile_id)
        self._store.write_state(RuntimeState(active_profile_id=profile.profile_id, previous_profile_ids=tuple(previous_ids[-20:])))
        return profile

    def rollback_profile(self, profile_id: str) -> RuntimeProfile:
        self.ensure_initialized()
        profiles = {item.profile_id: item for item in self._store.read_profiles()}
        profile = profiles.get(profile_id)
        if profile is None:
            raise KeyError(f"未知 runtime profile: {profile_id}")
        state = self._store.read_state()
        previous_ids = list(state.previous_profile_ids)
        if state.active_profile_id != "":
            previous_ids.append(state.active_profile_id)
        self._store.write_state(RuntimeState(active_profile_id=profile_id, previous_profile_ids=tuple(previous_ids[-20:])))
        self._store.append_profile_transition("rollback", state.active_profile_id, profile_id)
        return profile

    def get_active_profile(self) -> RuntimeProfile:
        self.ensure_initialized()
        profiles = {item.profile_id: item for item in self._store.read_profiles()}
        state = self._store.read_state()
        profile = profiles.get(state.active_profile_id)
        if profile is None:
            return next(iter(profiles.values()))
        return profile

    def get_active_snapshot(self) -> RuntimeSnapshot:
        return build_runtime_snapshot(self.get_active_profile())

    def list_profile_transitions(self) -> tuple[dict[str, str], ...]:
        self.ensure_initialized()
        return self._store.read_profile_transitions()

    def _build_draft_profile(self) -> RuntimeProfile:
        return RuntimeProfile(
            profile_id="draft",
            display_name="draft",
            version="draft",
            created_at="",
            published_at="",
            note="",
            source_instances=self._store.read_instances(),
            contract_policy_overrides=self._store.read_draft_policies(),
        )


@lru_cache(maxsize=1)
def get_config_runtime() -> QuoteMuxConfigRuntime:
    return QuoteMuxConfigRuntime()


def reset_config_runtime_cache() -> None:
    get_config_runtime.cache_clear()
