from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from re import fullmatch

from quotemux.capabilities import is_known_capability_id, normalize_capability_id
from quotemux.config_runtime.models import ContractPolicyOverride, RuntimeProfile, SourceInstanceConfig
from quotemux.contracts.registry import get_contract_allowed_merge_strategies
from quotemux.contracts.policies import CONTRACT_POLICIES
from quotemux.source_packages.environment import package_uses_isolated_environment
from quotemux.source_packages.manifest import SourcePackageManifest
from quotemux.source_packages.registry import SourcePackageRegistry


_HANDLER_CAPABILITY_PREFIXES = (
    ("get_stock", ("stocks.", "reference")),
    ("get_index", ("indexes.",)),
    ("get_board", ("boards.",)),
    ("get_trading_calendar", ("markets.calendar.trading",)),
    ("get_market_sessions", ("markets.trading.sessions",)),
    ("get_news", ("markets.events.news",)),
)
_INTERNAL_HANDLER_NAMES = {"get_news_event_sources", "get_stock_active_codes"}


@dataclass(frozen=True)
class ValidationIssue:
    field: str
    message: str


class ConfigValidationError(ValueError):
    def __init__(self, issues: tuple[ValidationIssue, ...]) -> None:
        self.issues = issues
        message = "; ".join(f"{issue.field}: {issue.message}" for issue in issues)
        super().__init__(message)


def validate_manifests(manifests: tuple[SourcePackageManifest, ...]) -> None:
    issues: list[ValidationIssue] = []
    seen_package_ids: set[str] = set()
    for manifest in manifests:
        issues.extend(_validate_manifest_required_fields(manifest))
        if manifest.package_id in seen_package_ids:
            issues.append(ValidationIssue("package_id", f"重复 source package: {manifest.package_id}"))
        seen_package_ids.add(manifest.package_id)
        issues.extend(_validate_package_version(manifest))
        issues.extend(_validate_capabilities(manifest))
        issues.extend(_validate_handler_targets(manifest))
        issues.extend(_validate_config_schema(manifest))
    _raise_if_needed(issues)


def validate_instance(instance: SourceInstanceConfig, registry: SourcePackageRegistry, existing_instances: tuple[SourceInstanceConfig, ...]) -> None:
    issues: list[ValidationIssue] = []
    if instance.instance_id == "":
        issues.append(ValidationIssue("instance_id", "不能为空"))
    if instance.package_id == "":
        issues.append(ValidationIssue("package_id", "不能为空"))
    try:
        manifest = registry.get_manifest(instance.package_id)
    except KeyError:
        issues.append(ValidationIssue("package_id", f"未知 source package: {instance.package_id}"))
        _raise_if_needed(issues)
        return
    if not manifest.supports_multi_instance:
        for current in existing_instances:
            if current.instance_id != instance.instance_id and current.package_id == instance.package_id:
                issues.append(ValidationIssue("package_id", f"{instance.package_id} 不支持多实例"))
                break
    known_config_fields = {field.name for field in manifest.config_schema}
    for field_name in instance.config_values:
        if field_name not in known_config_fields:
            issues.append(ValidationIssue("config_values", f"未知配置字段: {field_name}"))
    known_secret_fields = set(manifest.secret_fields)
    for field_name in instance.secret_values:
        if field_name not in known_secret_fields:
            issues.append(ValidationIssue("secret_values", f"未知密钥字段: {field_name}"))
    _raise_if_needed(issues)


def validate_profile(profile: RuntimeProfile, registry: SourcePackageRegistry) -> None:
    issues: list[ValidationIssue] = []
    instance_ids = {item.instance_id for item in profile.source_instances}
    for instance in profile.source_instances:
        try:
            registry.get_manifest(instance.package_id)
        except KeyError:
            issues.append(ValidationIssue("source_instances", f"{instance.instance_id} 绑定未知 package: {instance.package_id}"))
    for policy in profile.contract_policy_overrides:
        issues.extend(_validate_policy(policy, instance_ids, {item.package_id for item in profile.source_instances}))
    _raise_if_needed(issues)


def _validate_manifest_required_fields(manifest: SourcePackageManifest) -> tuple[ValidationIssue, ...]:
    issues: list[ValidationIssue] = []
    for field_name in ["package_id", "version", "source_name", "display_name"]:
        if str(getattr(manifest, field_name)) == "":
            issues.append(ValidationIssue(field_name, "不能为空"))
    if manifest.handler_targets == ():
        issues.append(ValidationIssue("handler_targets", "不能为空"))
    return tuple(issues)


def _validate_package_version(manifest: SourcePackageManifest) -> tuple[ValidationIssue, ...]:
    if manifest.version == "":
        return ()
    if fullmatch(r"\d+\.\d+\.\d+", manifest.version) is None:
        return (ValidationIssue("version", f"{manifest.package_id} 版本不兼容: {manifest.version}"),)
    return ()


def _validate_capabilities(manifest: SourcePackageManifest) -> tuple[ValidationIssue, ...]:
    issues: list[ValidationIssue] = []
    seen_names: set[str] = set()
    for capability in manifest.capabilities:
        capability_id = normalize_capability_id(capability.capability_id)
        if capability_id == "":
            issues.append(ValidationIssue("capabilities", f"{manifest.package_id} capability 不能为空"))
            continue
        if capability_id in seen_names:
            issues.append(ValidationIssue("capabilities", f"{manifest.package_id} 重复 capability: {capability_id}"))
        seen_names.add(capability_id)
        if not is_known_capability_id(capability_id):
            issues.append(ValidationIssue("capabilities", f"{manifest.package_id} 未知 capability: {capability_id}"))
        if capability.handler_name == "":
            issues.append(ValidationIssue("capabilities", f"{manifest.package_id} capability 未声明 handler_name: {capability_id}"))
        if capability.support_level == "":
            issues.append(ValidationIssue("capabilities", f"{manifest.package_id} capability 未声明 support_level: {capability_id}"))
    return tuple(issues)


def _validate_handler_targets(manifest: SourcePackageManifest) -> tuple[ValidationIssue, ...]:
    issues: list[ValidationIssue] = []
    seen_handler_names: set[str] = set()
    for handler_name, target in manifest.handler_targets:
        module_name, _, attr_name = target.partition(":")
        if handler_name == "" or module_name == "" or attr_name == "":
            issues.append(ValidationIssue("handler_targets", f"非法 handler: {handler_name} -> {target}"))
            continue
        if handler_name in seen_handler_names:
            issues.append(ValidationIssue("handler_targets", f"重复 handler: {handler_name}"))
        seen_handler_names.add(handler_name)
        if not _handler_matches_capabilities(handler_name, manifest):
            issues.append(ValidationIssue("handler_targets", f"{handler_name} 不属于 manifest 声明的 capability 能力"))
        if package_uses_isolated_environment(manifest):
            continue
        try:
            handler = getattr(import_module(module_name), attr_name)
        except (AttributeError, ImportError) as exc:
            issues.append(ValidationIssue("handler_targets", f"{handler_name} 无法加载: {exc}"))
            continue
        if not callable(handler):
            issues.append(ValidationIssue("handler_targets", f"{handler_name} 不是可调用对象"))
    return tuple(issues)


def _handler_matches_capabilities(handler_name: str, manifest: SourcePackageManifest) -> bool:
    if handler_name in _INTERNAL_HANDLER_NAMES:
        return True
    capability_names = tuple(item.capability_id for item in manifest.capabilities if item.handler_name == handler_name)
    if capability_names != ():
        return True
    for handler_prefix, capability_prefixes in _HANDLER_CAPABILITY_PREFIXES:
        if not handler_name.startswith(handler_prefix):
            continue
        for capability_name in manifest.contract_names:
            for capability_prefix in capability_prefixes:
                if capability_name == capability_prefix.removesuffix(".") or capability_name.startswith(capability_prefix):
                    return True
        return False
    return True


def _validate_config_schema(manifest: SourcePackageManifest) -> tuple[ValidationIssue, ...]:
    issues: list[ValidationIssue] = []
    seen_names: set[str] = set()
    for field in manifest.config_schema:
        if field.name == "":
            issues.append(ValidationIssue("config_schema", "字段名不能为空"))
        if field.name in seen_names:
            issues.append(ValidationIssue("config_schema", f"重复字段: {field.name}"))
        seen_names.add(field.name)
        if field.field_type not in {"string", "int", "float", "bool"}:
            issues.append(ValidationIssue("config_schema", f"{field.name} 类型不合法: {field.field_type}"))
        if not isinstance(field.required, bool):
            issues.append(ValidationIssue("config_schema", f"{field.name} required 必须是 bool"))
        if field.default_value != "" and not _default_value_matches_type(field.default_value, field.field_type):
            issues.append(ValidationIssue("config_schema", f"{field.name} 默认值不符合类型: {field.field_type}"))
    schema_names = {field.name for field in manifest.config_schema}
    for secret_field in manifest.secret_fields:
        if secret_field not in schema_names:
            issues.append(ValidationIssue("secret_fields", f"密钥字段未在 config_schema 声明: {secret_field}"))
    return tuple(issues)


def _default_value_matches_type(default_value: str, field_type: str) -> bool:
    if field_type == "string":
        return True
    if field_type == "int":
        try:
            int(default_value)
        except ValueError:
            return False
        return True
    if field_type == "float":
        try:
            float(default_value)
        except ValueError:
            return False
        return True
    if field_type == "bool":
        return default_value in {"true", "false", "1", "0"}
    return False


def _validate_policy(policy: ContractPolicyOverride, instance_ids: set[str], package_ids: set[str]) -> tuple[ValidationIssue, ...]:
    del package_ids
    issues: list[ValidationIssue] = []
    if policy.contract_name not in CONTRACT_POLICIES:
        issues.append(ValidationIssue("contract_name", f"未知 contract: {policy.contract_name}"))
    if policy.merge_strategy != "":
        try:
            allowed_strategies = get_contract_allowed_merge_strategies(policy.contract_name)
        except KeyError:
            allowed_strategies = ()
        if policy.merge_strategy not in allowed_strategies:
            issues.append(ValidationIssue("merge_strategy", f"{policy.contract_name} 不支持合并策略: {policy.merge_strategy}"))
    for source_id in policy.source_order:
        if source_id not in instance_ids:
            issues.append(ValidationIssue("source_order", f"未知 source instance: {source_id}"))
    return tuple(issues)


def _raise_if_needed(issues: list[ValidationIssue]) -> None:
    if issues != []:
        raise ConfigValidationError(tuple(issues))
