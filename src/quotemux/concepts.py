from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
import hashlib
import re
from typing import Callable, Sequence

from platform_models import BoardCatalogItem, BoardMemberItem, ConceptAliasGroupItem, ConceptAliasGroupMemberItem, ConceptAliasResolveItem
from quotemux.settings import QuoteMuxSettings
from quotemux.source_packages.instance_context import use_source_instance
from quotemux.source_packages.registry import get_default_source_package_registry


CONCEPT_CATEGORY = "concept"
CONCEPT_ALIAS_PROVIDERS = ("tushare", "akshare")
HIGH_CONFIDENCE_THRESHOLD = 0.82
REVIEW_CONFIDENCE_THRESHOLD = 0.62


@dataclass(frozen=True)
class ConceptBoardSnapshot:
    provider: str
    board_code: str
    board_name: str
    category: str
    member_stock_codes: frozenset[str]
    trade_date: str


@dataclass(frozen=True)
class ConceptAliasCandidate:
    left_provider: str
    left_board_code: str
    right_provider: str
    right_board_code: str
    confidence: float
    status: str


@dataclass(frozen=True)
class ConceptAliasAsset:
    groups: tuple[ConceptAliasGroupItem, ...]
    candidates: tuple[ConceptAliasCandidate, ...]


@dataclass(frozen=True)
class ManualConceptAliasDecision:
    provider: str
    board_code: str
    concept_id: str
    canonical_name: str
    decision: str


FetchCatalog = Callable[[str, str, str, int, int], list[BoardCatalogItem]]
FetchMembers = Callable[[str, str], list[BoardMemberItem]]


@dataclass(frozen=True)
class ConceptProviderSource:
    provider: str
    fetch_catalog: FetchCatalog
    fetch_members: FetchMembers


class QuoteMuxConcepts:
    def __init__(self, settings: QuoteMuxSettings | None = None) -> None:
        self._settings = settings or QuoteMuxSettings()

    def resolve_alias(self, provider: str, board_code: str, trade_date: str) -> ConceptAliasResolveItem:
        provider_text = provider.strip()
        board_code_text = board_code.strip()
        if provider_text == "" or board_code_text == "":
            return ConceptAliasResolveItem(concept_id="", canonical_name="", confidence=None)
        asset = self.build_alias_asset(trade_date)
        for group in asset.groups:
            for member in group.members:
                if member.provider == provider_text and member.board_code == board_code_text:
                    return ConceptAliasResolveItem(concept_id=group.concept_id, canonical_name=group.canonical_name, confidence=1.0)
        return ConceptAliasResolveItem(concept_id="", canonical_name="", confidence=None)

    def get_alias_group(self, concept_id: str, trade_date: str) -> ConceptAliasGroupItem:
        concept_id_text = concept_id.strip()
        if concept_id_text == "":
            return ConceptAliasGroupItem(concept_id="", canonical_name="")
        asset = self.build_alias_asset(trade_date)
        for group in asset.groups:
            if group.concept_id == concept_id_text:
                return group
        return ConceptAliasGroupItem(concept_id="", canonical_name="")

    def build_alias_asset(self, trade_date: str) -> ConceptAliasAsset:
        sources = self._build_provider_sources()
        return build_concept_alias_asset(sources, trade_date, ())

    def _build_provider_sources(self) -> tuple[ConceptProviderSource, ...]:
        registry = get_default_source_package_registry()
        sources: list[ConceptProviderSource] = []
        for provider in CONCEPT_ALIAS_PROVIDERS:
            if not self._settings.is_source_enabled(provider):
                continue
            if not _provider_has_concept_contracts(provider):
                continue
            try:
                catalog_handler = registry.get_handler(provider, "get_board_catalog")
                members_handler = registry.get_handler(provider, "get_board_members")
            except KeyError:
                continue
            catalog_instance = _source_instance(self._settings, "boards.catalog", provider)
            members_instance = _source_instance(self._settings, "boards.members", provider)
            if catalog_instance is None or members_instance is None:
                continue
            sources.append(
                ConceptProviderSource(
                    provider=provider,
                    fetch_catalog=_with_instance(catalog_handler, catalog_instance),
                    fetch_members=_with_instance(members_handler, members_instance),
                )
            )
        return tuple(sources)


def build_concept_alias_asset(
    sources: Sequence[ConceptProviderSource],
    trade_date: str,
    manual_decisions: Sequence[ManualConceptAliasDecision],
) -> ConceptAliasAsset:
    snapshots = _collect_snapshots(sources, trade_date)
    decision_groups = _build_manual_groups(snapshots, manual_decisions)
    auto_groups, candidates = _build_auto_groups(snapshots, decision_groups)
    groups = tuple(sorted([*decision_groups, *auto_groups], key=lambda item: item.concept_id))
    return ConceptAliasAsset(groups=groups, candidates=tuple(sorted(candidates, key=lambda item: (item.left_provider, item.left_board_code, item.right_provider, item.right_board_code))))


def _collect_snapshots(sources: Sequence[ConceptProviderSource], trade_date: str) -> tuple[ConceptBoardSnapshot, ...]:
    snapshots: list[ConceptBoardSnapshot] = []
    for source in sources:
        catalog_items = source.fetch_catalog(CONCEPT_CATEGORY, "", "active", 10000, 0)
        for catalog in catalog_items:
            if catalog.category != CONCEPT_CATEGORY:
                continue
            if catalog.board_code == "":
                continue
            members = source.fetch_members(catalog.board_code, trade_date)
            member_codes = frozenset(_member_code(item) for item in members if _member_code(item) != "")
            if member_codes == frozenset():
                continue
            snapshots.append(
                ConceptBoardSnapshot(
                    provider=source.provider,
                    board_code=catalog.board_code,
                    board_name=catalog.board_name,
                    category=catalog.category,
                    member_stock_codes=member_codes,
                    trade_date=trade_date,
                )
            )
    return tuple(snapshots)


def _build_manual_groups(
    snapshots: Sequence[ConceptBoardSnapshot],
    manual_decisions: Sequence[ManualConceptAliasDecision],
) -> list[ConceptAliasGroupItem]:
    snapshots_by_key = {(item.provider, item.board_code): item for item in snapshots}
    groups_by_id: dict[str, list[ConceptBoardSnapshot]] = {}
    names_by_id: dict[str, str] = {}
    for decision in manual_decisions:
        if decision.decision != "confirmed":
            continue
        snapshot = snapshots_by_key.get((decision.provider, decision.board_code))
        if snapshot is None:
            continue
        groups_by_id.setdefault(decision.concept_id, []).append(snapshot)
        names_by_id[decision.concept_id] = decision.canonical_name
    groups: list[ConceptAliasGroupItem] = []
    for concept_id, members in groups_by_id.items():
        groups.append(_to_group(concept_id, names_by_id.get(concept_id, ""), members))
    return groups


def _build_auto_groups(
    snapshots: Sequence[ConceptBoardSnapshot],
    existing_groups: Sequence[ConceptAliasGroupItem],
) -> tuple[list[ConceptAliasGroupItem], list[ConceptAliasCandidate]]:
    assigned = {(member.provider, member.board_code) for group in existing_groups for member in group.members}
    candidates: list[ConceptAliasCandidate] = []
    union = _UnionFind(tuple(index for index, item in enumerate(snapshots) if (item.provider, item.board_code) not in assigned))
    for left_index, left in enumerate(snapshots):
        if (left.provider, left.board_code) in assigned:
            continue
        for right_index in range(left_index + 1, len(snapshots)):
            right = snapshots[right_index]
            if (right.provider, right.board_code) in assigned:
                continue
            if left.provider == right.provider:
                continue
            confidence = _match_confidence(left, right)
            status = "confirmed" if confidence >= HIGH_CONFIDENCE_THRESHOLD else "review" if confidence >= REVIEW_CONFIDENCE_THRESHOLD else "ignored"
            if status != "ignored":
                candidates.append(
                    ConceptAliasCandidate(
                        left_provider=left.provider,
                        left_board_code=left.board_code,
                        right_provider=right.provider,
                        right_board_code=right.board_code,
                        confidence=confidence,
                        status=status,
                    )
                )
            if status == "confirmed":
                union.union(left_index, right_index)
    groups: list[ConceptAliasGroupItem] = []
    for members in union.groups().values():
        if len(members) < 2:
            continue
        group_snapshots = [snapshots[index] for index in members]
        concept_id = _concept_id(group_snapshots)
        groups.append(_to_group(concept_id, "", group_snapshots))
    return groups, candidates


def _match_confidence(left: ConceptBoardSnapshot, right: ConceptBoardSnapshot) -> float:
    member_score = _jaccard(left.member_stock_codes, right.member_stock_codes)
    name_score = _name_similarity(left.board_name, right.board_name)
    if member_score == 1.0:
        return 1.0
    return round(member_score * 0.78 + name_score * 0.22, 6)


def _jaccard(left: frozenset[str], right: frozenset[str]) -> float:
    if left == frozenset() or right == frozenset():
        return 0.0
    return len(left & right) / len(left | right)


def _name_similarity(left: str, right: str) -> float:
    left_name = _normalize_board_name(left)
    right_name = _normalize_board_name(right)
    if left_name == "" or right_name == "":
        return 0.0
    if left_name == right_name:
        return 1.0
    if left_name in right_name or right_name in left_name:
        return 0.86
    return SequenceMatcher(None, left_name, right_name).ratio()


def _normalize_board_name(value: str) -> str:
    upper_value = value.upper()
    normalized = re.sub(r"[\s·・（）()\[\]【】\-_/]+", "", upper_value)
    for suffix in ("概念", "板块", "行业"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
    return normalized


def _member_code(item: BoardMemberItem) -> str:
    code = item.code.strip()
    if code == "":
        return ""
    return code.zfill(6)


def _concept_id(snapshots: Sequence[ConceptBoardSnapshot]) -> str:
    canonical_name = _canonical_name(snapshots)
    normalized = _normalize_board_name(canonical_name).lower()
    if normalized == "":
        seed = "|".join(f"{item.provider}:{item.board_code}" for item in snapshots)
        normalized = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:10]
    ascii_text = re.sub(r"[^a-z0-9]+", "_", normalized)
    if ascii_text != "":
        return f"concept_{ascii_text.strip('_')}"
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:10]
    return f"concept_{digest}"


def _canonical_name(snapshots: Sequence[ConceptBoardSnapshot]) -> str:
    names = [item.board_name for item in snapshots if item.board_name != ""]
    if names == []:
        return ""
    return sorted(names, key=lambda item: (len(_normalize_board_name(item)), item))[0]


def _to_group(concept_id: str, canonical_name: str, snapshots: Sequence[ConceptBoardSnapshot]) -> ConceptAliasGroupItem:
    actual_name = canonical_name if canonical_name != "" else _canonical_name(snapshots)
    members = [
        ConceptAliasGroupMemberItem(provider=item.provider, board_code=item.board_code, board_name=item.board_name)
        for item in sorted(snapshots, key=lambda value: (value.provider, value.board_code))
    ]
    return ConceptAliasGroupItem(concept_id=concept_id, canonical_name=actual_name, members=members)


def _provider_has_concept_contracts(provider: str) -> bool:
    registry = get_default_source_package_registry()
    try:
        manifest = registry.get_manifest(provider)
    except KeyError:
        return False
    return manifest.supports_capability("boards.catalog") and manifest.supports_capability("boards.members")


def _source_instance(settings: QuoteMuxSettings, contract_name: str, provider: str):
    for instance in settings.get_contract_source_instances(contract_name, (provider,)):
        if instance.package_id == provider and instance.enabled:
            return instance
    return None


def _with_instance(handler, source_instance):
    def fetcher(*args):
        with use_source_instance(source_instance):
            return handler(*args)

    return fetcher


class _UnionFind:
    def __init__(self, values: Sequence[int]) -> None:
        self._parent = {value: value for value in values}

    def find(self, value: int) -> int:
        parent = self._parent[value]
        if parent != value:
            parent = self.find(parent)
            self._parent[value] = parent
        return parent

    def union(self, left: int, right: int) -> None:
        if left not in self._parent or right not in self._parent:
            return
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root != right_root:
            self._parent[right_root] = left_root

    def groups(self) -> dict[int, list[int]]:
        groups: dict[int, list[int]] = {}
        for value in self._parent:
            groups.setdefault(self.find(value), []).append(value)
        return groups
