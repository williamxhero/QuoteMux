from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import datetime
from difflib import SequenceMatcher
import os
import re
from typing import Callable, Sequence

import psycopg
from psycopg.rows import dict_row

from platform_models import BoardCatalogItem, BoardMemberItem, ConceptAliasGroupItem, ConceptAliasGroupMemberItem, ConceptAliasResolveItem
from quotemux.config_runtime import SourceInstanceConfig, get_config_runtime
from quotemux.infra.db.client import execute_sql, query_dataframe
from quotemux.infra.db.config import DB_CONNECT_TIMEOUT, DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
from quotemux.settings import QuoteMuxSettings
from quotemux.source_packages.instance_context import use_source_instance
from quotemux.source_packages.registry import get_default_source_package_registry


CONCEPT_CATEGORY = "concept"
CONCEPT_ALIAS_PROVIDERS = ("tushare", "akshare")
DEFAULT_CONCEPT_START_DATE = "20000101"
HIGH_CONFIDENCE_THRESHOLD = 0.70
REVIEW_CONFIDENCE_THRESHOLD = 0.62
EXHAUSTIVE_SNAPSHOT_BOARD_LIMIT = 80
CATALOG_NAME_CANDIDATE_THRESHOLD = 0.62
CONCEPT_MEMBER_FETCH_WORKERS = 8
MAX_CANDIDATE_PAIRS = 20
MEMBER_VERIFY_PAIR_LIMIT = 80
CONCEPT_ID_PATTERN = re.compile(r"C[1-9][0-9]*")
CONCEPT_TYPE_ORDER = ("ths", "dc", "tdx", "kpl", "em")

CONCEPT_ALIAS_SCHEMA_SQL = (
    "create schema if not exists derived",
    """
    create table if not exists derived.concept_alias_groups (
        concept_id text primary key,
        canonical_name text not null,
        start_date text not null,
        end_date text not null,
        updated_at timestamp without time zone not null default now()
    )
    """,
    """
    create table if not exists derived.concept_alias_members (
        concept_id text not null references derived.concept_alias_groups(concept_id) on update cascade on delete cascade,
        provider text not null,
        provider_concept_type text not null,
        provider_concept_code text not null,
        provider_concept_name text not null,
        start_date text not null,
        end_date text not null,
        updated_at timestamp without time zone not null default now(),
        primary key (concept_id, provider, provider_concept_type, provider_concept_code)
    )
    """,
    "alter table derived.concept_alias_members add column if not exists provider_concept_type text",
    "alter table derived.concept_alias_members add column if not exists provider_concept_code text",
    "alter table derived.concept_alias_members add column if not exists provider_concept_name text",
    """
    do $$
    begin
        if exists (
            select 1 from information_schema.columns
            where table_schema = 'derived' and table_name = 'concept_alias_members' and column_name = 'board_type'
        ) then
            execute 'update derived.concept_alias_members set provider_concept_type = board_type where provider_concept_type is null';
        end if;
        if exists (
            select 1 from information_schema.columns
            where table_schema = 'derived' and table_name = 'concept_alias_members' and column_name = 'board_code'
        ) then
            execute 'update derived.concept_alias_members set provider_concept_code = board_code where provider_concept_code is null';
        end if;
        if exists (
            select 1 from information_schema.columns
            where table_schema = 'derived' and table_name = 'concept_alias_members' and column_name = 'board_name'
        ) then
            execute 'update derived.concept_alias_members set provider_concept_name = board_name where provider_concept_name is null';
        end if;
    end $$;
    """,
    "update derived.concept_alias_members set provider_concept_type = '' where provider_concept_type is null",
    "update derived.concept_alias_members set provider_concept_code = '' where provider_concept_code is null",
    "update derived.concept_alias_members set provider_concept_name = '' where provider_concept_name is null",
    "alter table derived.concept_alias_members alter column provider_concept_type set not null",
    "alter table derived.concept_alias_members alter column provider_concept_code set not null",
    "alter table derived.concept_alias_members alter column provider_concept_name set not null",
    "create unique index if not exists ux_concept_alias_members_provider_concept on derived.concept_alias_members (provider, provider_concept_type, provider_concept_code)",
    """
    create table if not exists derived.concept_alias_candidates (
        left_provider text not null,
        left_board_type text not null,
        left_board_code text not null,
        right_provider text not null,
        right_board_type text not null,
        right_board_code text not null,
        confidence double precision not null,
        status text not null,
        updated_at timestamp without time zone not null default now(),
        primary key (left_provider, left_board_type, left_board_code, right_provider, right_board_type, right_board_code)
    )
    """,
    """
    create table if not exists derived.concept_alias_registry (
        registry_key text primary key,
        registry_value text not null,
        updated_at timestamp without time zone not null default now()
    )
    """,
    """
    create table if not exists derived.concept_alias_registry_signatures (
        signature text primary key,
        concept_id text not null,
        updated_at timestamp without time zone not null default now()
    )
    """,
)

_ALIAS_SCHEMA_READY = False


@dataclass(frozen=True)
class ConceptBoardCatalog:
    provider: str
    board_type: str
    board_code: str
    board_name: str
    category: str
    start_date: str
    end_date: str


@dataclass(frozen=True)
class ConceptBoardSnapshot:
    provider: str
    board_type: str
    board_code: str
    board_name: str
    category: str
    member_stock_codes: frozenset[str]
    trade_date: str
    start_date: str
    end_date: str


@dataclass(frozen=True)
class ConceptAliasCandidate:
    left_provider: str
    left_board_type: str
    left_board_code: str
    right_provider: str
    right_board_type: str
    right_board_code: str
    confidence: float
    status: str


@dataclass(frozen=True)
class ConceptAliasAsset:
    groups: tuple[ConceptAliasGroupItem, ...]
    candidates: tuple[ConceptAliasCandidate, ...]


@dataclass(frozen=True)
class ConceptBoardAlias:
    concept_id: str
    canonical_name: str
    provider: str
    board_type: str
    board_code: str
    board_name: str


@dataclass
class ConceptIdRegistry:
    next_id: int
    signature_to_id: dict[str, str]


@dataclass(frozen=True)
class ManualConceptAliasDecision:
    provider: str
    board_type: str
    board_code: str
    concept_id: str
    canonical_name: str
    decision: str


FetchCatalog = Callable[[str, str, str, int, int], list[BoardCatalogItem]]
FetchMembers = Callable[[str, str], list[BoardMemberItem]]


@dataclass(frozen=True)
class ConceptProviderSource:
    provider: str
    board_type: str
    fetch_catalog: FetchCatalog
    fetch_members: FetchMembers


class QuoteMuxConcepts:
    def __init__(self, settings: QuoteMuxSettings | None = None) -> None:
        self._settings = settings or QuoteMuxSettings()

    def resolve_alias(self, provider: str, provider_concept_type: str, provider_concept_code: str, trade_date: str) -> ConceptAliasResolveItem:
        provider_text = provider.strip()
        concept_type_text = provider_concept_type.strip()
        concept_code_text = provider_concept_code.strip()
        if provider_text == "" or concept_code_text == "":
            return ConceptAliasResolveItem(concept_id="", canonical_name="", confidence=None)
        for group in self.list_alias_groups(trade_date):
            for member in group.members:
                if member.provider == provider_text and member.provider_concept_code == concept_code_text and (concept_type_text == "" or member.provider_concept_type == concept_type_text):
                    return ConceptAliasResolveItem(concept_id=group.concept_id, canonical_name=group.canonical_name, confidence=1.0)
        return ConceptAliasResolveItem(concept_id="", canonical_name="", confidence=None)

    def get_alias_group(self, concept_id: str, trade_date: str) -> ConceptAliasGroupItem:
        concept_id_text = concept_id.strip()
        if concept_id_text == "":
            return ConceptAliasGroupItem(concept_id="", canonical_name="")
        for group in self.list_alias_groups(trade_date):
            if group.concept_id == concept_id_text:
                return group
        return ConceptAliasGroupItem(concept_id="", canonical_name="")

    def list_alias_groups(self, trade_date: str) -> list[ConceptAliasGroupItem]:
        return _read_alias_groups(trade_date)

    def list_concept_aliases(self, concept_id: str, trade_date: str, source_order: tuple[str, ...]) -> tuple[ConceptBoardAlias, ...]:
        group = self.get_alias_group(concept_id, trade_date)
        if group.concept_id == "":
            return ()
        return tuple(
            ConceptBoardAlias(
                concept_id=group.concept_id,
                canonical_name=group.canonical_name,
                provider=member.provider,
                board_type=member.provider_concept_type,
                board_code=member.provider_concept_code,
                board_name=member.provider_concept_name,
            )
            for member in _sort_alias_members(group.members, source_order)
        )

    def build_alias_asset(self, trade_date: str) -> ConceptAliasAsset:
        actual_trade_date = _actual_trade_date(trade_date)
        sources = self._build_provider_sources(actual_trade_date)
        return build_concept_alias_asset(sources, actual_trade_date, ())

    def refresh_alias_asset(self, trade_date: str) -> ConceptAliasAsset:
        actual_trade_date = _actual_trade_date(trade_date)
        sources = self._build_provider_sources(actual_trade_date)
        registry = _read_concept_id_registry()
        asset = _build_concept_alias_asset(sources, actual_trade_date, (), registry)
        _write_alias_asset(asset, registry)
        return asset

    def _build_provider_sources(self, trade_date: str) -> tuple[ConceptProviderSource, ...]:
        registry = get_default_source_package_registry()
        sources: list[ConceptProviderSource] = []
        for provider in CONCEPT_ALIAS_PROVIDERS:
            if not self._settings.is_source_enabled(provider):
                continue
            if not _provider_has_concept_contracts(provider):
                continue
            try:
                catalog_handler = registry.get_handler(provider, "get_concept_catalog")
                members_handler = registry.get_handler(provider, "get_concept_members")
            except KeyError:
                continue
            catalog_instance = _source_instance(self._settings, "concepts.catalog", provider)
            members_instance = _source_instance(self._settings, "concepts.members", provider)
            if catalog_instance is None or members_instance is None:
                continue
            if not _provider_has_member_runtime(provider, members_instance):
                continue
            sources.extend(_typed_sources(provider, catalog_handler, members_handler, catalog_instance, members_instance, trade_date))
        return tuple(sources)


def build_concept_alias_asset(
    sources: Sequence[ConceptProviderSource],
    trade_date: str,
    manual_decisions: Sequence[ManualConceptAliasDecision],
) -> ConceptAliasAsset:
    registry = ConceptIdRegistry(next_id=1, signature_to_id={})
    return _build_concept_alias_asset(sources, trade_date, manual_decisions, registry)


def _build_concept_alias_asset(
    sources: Sequence[ConceptProviderSource],
    trade_date: str,
    manual_decisions: Sequence[ManualConceptAliasDecision],
    registry: ConceptIdRegistry,
) -> ConceptAliasAsset:
    snapshots = _collect_snapshots(sources, trade_date)
    decision_groups = _build_manual_groups(snapshots, manual_decisions)
    auto_groups, candidates = _build_auto_groups(snapshots, decision_groups)
    groups = _assign_concept_ids(tuple([*decision_groups, *auto_groups]), registry)
    return ConceptAliasAsset(groups=groups, candidates=tuple(sorted(candidates, key=lambda item: (item.left_provider, item.left_board_type, item.left_board_code, item.right_provider, item.right_board_type, item.right_board_code))))


def _collect_snapshots(sources: Sequence[ConceptProviderSource], trade_date: str) -> tuple[ConceptBoardSnapshot, ...]:
    catalogs = _collect_catalogs(sources, trade_date)
    if catalogs == ():
        return ()
    catalog_keys = _candidate_catalog_keys(catalogs)
    snapshots_by_key = {
        _snapshot_key(item): ConceptBoardSnapshot(
            provider=item.provider,
            board_type=item.board_type,
            board_code=item.board_code,
            board_name=item.board_name,
            category=item.category,
            member_stock_codes=frozenset(),
            trade_date=trade_date,
            start_date=item.start_date,
            end_date=item.end_date,
        )
        for item in catalogs
    }
    candidate_catalogs = [item for item in catalogs if _snapshot_key(item) in catalog_keys]
    sources_by_key = {(source.provider, source.board_type): source for source in sources}
    with ThreadPoolExecutor(max_workers=CONCEPT_MEMBER_FETCH_WORKERS) as executor:
        future_map = {
            executor.submit(_build_snapshot, sources_by_key[(catalog.provider, catalog.board_type)], catalog, trade_date): catalog
            for catalog in candidate_catalogs
            if (catalog.provider, catalog.board_type) in sources_by_key
        }
        for future in as_completed(future_map):
            snapshot = future.result()
            if snapshot is not None:
                snapshots_by_key[_snapshot_key(snapshot)] = snapshot
    return tuple(sorted(snapshots_by_key.values(), key=lambda item: (item.provider, item.board_type, item.board_code)))


def _collect_catalogs(sources: Sequence[ConceptProviderSource], trade_date: str) -> tuple[ConceptBoardCatalog, ...]:
    catalogs: list[ConceptBoardCatalog] = []
    for source in sources:
        catalog_items = source.fetch_catalog(CONCEPT_CATEGORY, "", "active", 10000, 0)
        for catalog in catalog_items:
            if catalog.category != CONCEPT_CATEGORY:
                continue
            if catalog.board_code == "":
                continue
            catalogs.append(
                ConceptBoardCatalog(
                    provider=source.provider,
                    board_type=source.board_type,
                    board_code=catalog.board_code,
                    board_name=catalog.board_name,
                    category=catalog.category,
                    start_date=_concept_start_date(catalog.start_date, catalog.board_name),
                    end_date=_normalize_date_text(catalog.end_date),
                )
            )
    return tuple(sorted(catalogs, key=lambda item: (item.provider, item.board_type, item.board_code)))


def _candidate_catalog_keys(catalogs: Sequence[ConceptBoardCatalog]) -> set[tuple[str, str, str]]:
    keys = {_snapshot_key(item) for item in catalogs}
    if len(keys) <= EXHAUSTIVE_SNAPSHOT_BOARD_LIMIT:
        return keys
    pairs: list[tuple[float, str, str, ConceptBoardCatalog, ConceptBoardCatalog]] = []
    candidate_keys: set[tuple[str, str]] = set()
    for left_index, left in enumerate(catalogs):
        for right_index in range(left_index + 1, len(catalogs)):
            right = catalogs[right_index]
            if _snapshot_key(left) == _snapshot_key(right):
                continue
            score = _name_similarity(left.board_name, right.board_name)
            if score < CATALOG_NAME_CANDIDATE_THRESHOLD:
                continue
            pairs.append((score, left.board_name, right.board_name, left, right))
    pairs = sorted(pairs, key=lambda item: (-item[0], item[1], item[2]))[:MEMBER_VERIFY_PAIR_LIMIT]
    for _, _, _, left, right in pairs:
        candidate_keys.add(_snapshot_key(left))
        candidate_keys.add(_snapshot_key(right))
    return candidate_keys


def _build_snapshot(source: ConceptProviderSource, catalog: ConceptBoardCatalog, trade_date: str) -> ConceptBoardSnapshot | None:
    try:
        members = source.fetch_members(catalog.board_code, trade_date)
    except Exception:
        return None
    member_codes = frozenset(_member_code(item) for item in members if _member_code(item) != "")
    if member_codes == frozenset():
        return None
    return ConceptBoardSnapshot(
        provider=source.provider,
        board_type=catalog.board_type,
        board_code=catalog.board_code,
        board_name=catalog.board_name,
        category=catalog.category,
        member_stock_codes=member_codes,
        trade_date=trade_date,
        start_date=catalog.start_date,
        end_date=catalog.end_date,
    )


def _build_manual_groups(
    snapshots: Sequence[ConceptBoardSnapshot],
    manual_decisions: Sequence[ManualConceptAliasDecision],
) -> list[ConceptAliasGroupItem]:
    snapshots_by_key = {_snapshot_key(item): item for item in snapshots}
    groups_by_id: dict[str, list[ConceptBoardSnapshot]] = {}
    names_by_id: dict[str, str] = {}
    for decision in manual_decisions:
        if decision.decision != "confirmed":
            continue
        snapshot = snapshots_by_key.get((decision.provider, decision.board_type, decision.board_code))
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
    assigned = {(member.provider, member.board_type, member.board_code) for group in existing_groups for member in group.members}
    candidates: list[ConceptAliasCandidate] = []
    union = _UnionFind(tuple(index for index, item in enumerate(snapshots) if _snapshot_key(item) not in assigned))
    for left_index, left in enumerate(snapshots):
        if _snapshot_key(left) in assigned:
            continue
        for right_index in range(left_index + 1, len(snapshots)):
            right = snapshots[right_index]
            if _snapshot_key(right) in assigned:
                continue
            if _snapshot_key(left) == _snapshot_key(right):
                continue
            confidence = _match_confidence(left, right)
            if confidence < HIGH_CONFIDENCE_THRESHOLD and _name_similarity(left.board_name, right.board_name) == 1.0:
                confidence = HIGH_CONFIDENCE_THRESHOLD
            status = "confirmed" if confidence >= HIGH_CONFIDENCE_THRESHOLD else "review" if confidence >= REVIEW_CONFIDENCE_THRESHOLD else "ignored"
            if status != "ignored":
                candidates.append(
                    ConceptAliasCandidate(
                        left_provider=left.provider,
                        left_board_type=left.board_type,
                        left_board_code=left.board_code,
                        right_provider=right.provider,
                        right_board_type=right.board_type,
                        right_board_code=right.board_code,
                        confidence=confidence,
                        status=status,
                    )
                )
            if status == "confirmed":
                union.union(left_index, right_index)
    groups: list[ConceptAliasGroupItem] = []
    for members in union.groups().values():
        group_snapshots = [snapshots[index] for index in members]
        groups.append(_to_group("", "", group_snapshots))
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
    normalized = re.sub(r"[\s·・（�?)\[\]【】\-_/]+", "", upper_value)
    for suffix in ("概念", "板块", "行业"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
    return normalized


def _member_code(item: BoardMemberItem) -> str:
    code = item.code.strip()
    if code == "":
        return ""
    return code.zfill(6)


def _canonical_name(snapshots: Sequence[ConceptBoardSnapshot]) -> str:
    names = [item.board_name for item in snapshots if item.board_name != ""]
    if names == []:
        return ""
    return sorted(names, key=lambda item: (len(_normalize_board_name(item)), item))[0]


def _to_group(concept_id: str, canonical_name: str, snapshots: Sequence[ConceptBoardSnapshot]) -> ConceptAliasGroupItem:
    actual_name = canonical_name if canonical_name != "" else _canonical_name(snapshots)
    members = [
        ConceptAliasGroupMemberItem(provider=item.provider, provider_concept_type=item.board_type, provider_concept_code=item.board_code, provider_concept_name=item.board_name, start_date=item.start_date, end_date=item.end_date)
        for item in sorted(snapshots, key=lambda value: (value.provider, value.board_type, value.board_code))
    ]
    return ConceptAliasGroupItem(concept_id=concept_id, canonical_name=actual_name, start_date=_group_start_date(members), end_date=_group_end_date(members), members=members)


def _snapshot_key(item: ConceptBoardCatalog | ConceptBoardSnapshot) -> tuple[str, str, str]:
    return item.provider, item.board_type, item.board_code


def _assign_concept_ids(groups: Sequence[ConceptAliasGroupItem], registry: ConceptIdRegistry) -> tuple[ConceptAliasGroupItem, ...]:
    assigned_groups: list[ConceptAliasGroupItem] = []
    for group in sorted(groups, key=lambda item: (_canonical_group_sort_name(item), _group_signature(item))):
        signature = _group_signature(group)
        concept_id = registry.signature_to_id.get(signature, "")
        if concept_id == "":
            concept_id = f"C{registry.next_id}"
            registry.next_id += 1
            registry.signature_to_id[signature] = concept_id
        assigned_groups.append(group.model_copy(update={"concept_id": concept_id}))
    return tuple(sorted(assigned_groups, key=lambda item: _concept_id_number(item.concept_id)))


def _canonical_group_sort_name(group: ConceptAliasGroupItem) -> str:
    return _normalize_board_name(group.canonical_name)


def _group_signature(group: ConceptAliasGroupItem) -> str:
    keys = sorted(f"{member.provider}|{member.board_type}|{member.board_code}" for member in group.members)
    return "\n".join(keys)


def _concept_id_number(concept_id: str) -> int:
    if CONCEPT_ID_PATTERN.fullmatch(concept_id):
        return int(concept_id[1:])
    return 0


def is_concept_id(value: str) -> bool:
    return CONCEPT_ID_PATTERN.fullmatch(value.strip()) is not None


def _sort_alias_members(members: Sequence[ConceptAliasGroupMemberItem], source_order: tuple[str, ...]) -> list[ConceptAliasGroupMemberItem]:
    package_rank = {source_id.split("-", 1)[0]: index for index, source_id in enumerate(source_order)}
    type_rank = {concept_type: index for index, concept_type in enumerate(CONCEPT_TYPE_ORDER)}
    return sorted(
        members,
        key=lambda member: (
            package_rank.get(member.provider, len(package_rank)),
            type_rank.get(member.board_type, len(type_rank)),
            member.provider,
            member.board_type,
            member.board_code,
        ),
    )


def _read_concept_id_registry() -> ConceptIdRegistry:
    if not _ensure_alias_schema():
        return ConceptIdRegistry(next_id=1, signature_to_id={})
    signature_frame = query_dataframe(
        """
        select signature, concept_id
        from derived.concept_alias_registry_signatures
        order by concept_id asc
        """,
        (),
    )
    signature_to_id: dict[str, str] = {}
    if not signature_frame.empty:
        for row in signature_frame.to_dict("records"):
            signature = str(row.get("signature", ""))
            concept_id = str(row.get("concept_id", ""))
            if signature != "" and re.fullmatch(r"C[1-9][0-9]*", concept_id):
                signature_to_id[signature] = concept_id
    registry_frame = query_dataframe(
        """
        select registry_value
        from derived.concept_alias_registry
        where registry_key = %s
        """,
        ("next_id",),
    )
    next_id_value: object = 1
    if not registry_frame.empty:
        next_id_text = str(registry_frame.iloc[0].to_dict().get("registry_value", "1"))
        try:
            next_id_value = int(next_id_text)
        except ValueError:
            next_id_value = 1
    next_id = _next_registry_id(next_id_value, signature_to_id)
    return ConceptIdRegistry(next_id=next_id, signature_to_id=signature_to_id)


def _next_registry_id(value: object, signature_to_id: dict[str, str]) -> int:
    assigned_numbers = [_concept_id_number(item) for item in signature_to_id.values()]
    minimum_next = max(assigned_numbers, default=0) + 1
    if isinstance(value, int) and value >= minimum_next:
        return value
    return minimum_next


def _group_start_date(members: Sequence[ConceptAliasGroupMemberItem]) -> str:
    values = [member.start_date for member in members if member.start_date != ""]
    if values == []:
        return ""
    real_values = [value for value in values if value != DEFAULT_CONCEPT_START_DATE]
    if real_values != []:
        return max(real_values)
    return min(values)


def _concept_start_date(value: str, board_name: str) -> str:
    name_start_date = _concept_name_start_date(board_name)
    if name_start_date != "":
        return name_start_date
    normalized = _normalize_date_text(value)
    if normalized != "":
        return normalized
    return DEFAULT_CONCEPT_START_DATE


def _concept_name_start_date(board_name: str) -> str:
    match = re.search(r"(20[0-9]{2}).*?(年报|四季|一季|中报|二季|三季)", board_name)
    if match is None:
        return ""
    year = int(match.group(1))
    period = match.group(2)
    if period in {"年报", "四季"}:
        return f"{year + 1}0101"
    if period == "一�?:
        return f"{year}0401"
    if period in {"中报", "二季"}:
        return f"{year}0701"
    if period == "三季":
        return f"{year}1001"
    return ""


def _group_end_date(members: Sequence[ConceptAliasGroupMemberItem]) -> str:
    if any(member.end_date == "" for member in members):
        return ""
    values = [member.end_date for member in members]
    if values == []:
        return ""
    return max(values)


def _read_alias_groups(trade_date: str) -> list[ConceptAliasGroupItem]:
    if not _ensure_alias_schema():
        return []
    groups_frame = query_dataframe(
        """
        select concept_id, canonical_name, start_date, end_date
        from derived.concept_alias_groups
        order by cast(substring(concept_id from 2) as integer) asc
        """,
        (),
    )
    if groups_frame.empty:
        return []
    members_frame = query_dataframe(
        """
        select concept_id, provider, provider_concept_type, provider_concept_code, provider_concept_name, start_date, end_date
        from derived.concept_alias_members
        order by concept_id asc, provider asc, provider_concept_type asc, provider_concept_code asc
        """,
        (),
    )
    members_by_id: dict[str, list[ConceptAliasGroupMemberItem]] = {}
    if not members_frame.empty:
        for row in members_frame.to_dict("records"):
            concept_id = str(row.get("concept_id", ""))
            members_by_id.setdefault(concept_id, []).append(
                ConceptAliasGroupMemberItem(
                    provider=str(row.get("provider", "")),
                    provider_concept_type=str(row.get("provider_concept_type", "")),
                    provider_concept_code=str(row.get("provider_concept_code", "")),
                    provider_concept_name=str(row.get("provider_concept_name", "")),
                    start_date=str(row.get("start_date", "")),
                    end_date=str(row.get("end_date", "")),
                )
            )
    groups = [
        ConceptAliasGroupItem(
            concept_id=str(row.get("concept_id", "")),
            canonical_name=str(row.get("canonical_name", "")),
            start_date=str(row.get("start_date", "")),
            end_date=str(row.get("end_date", "")),
            members=members_by_id.get(str(row.get("concept_id", "")), []),
        )
        for row in groups_frame.to_dict("records")
    ]
    return _filter_alias_groups(groups, trade_date)


def _filter_alias_groups(groups: Sequence[ConceptAliasGroupItem], trade_date: str) -> list[ConceptAliasGroupItem]:
    actual_trade_date = _normalize_date_text(trade_date)
    if actual_trade_date == "":
        return list(groups)
    filtered_groups: list[ConceptAliasGroupItem] = []
    for group in groups:
        members = [member for member in group.members if _member_active_on(member, actual_trade_date)]
        if members == []:
            continue
        filtered_groups.append(group.model_copy(update={"members": members}))
    return filtered_groups


def _member_active_on(member: ConceptAliasGroupMemberItem, trade_date: str) -> bool:
    start_date = _normalize_date_text(member.start_date)
    end_date = _normalize_date_text(member.end_date)
    if start_date != "" and trade_date < start_date:
        return False
    if end_date != "" and trade_date > end_date:
        return False
    return True


def _write_alias_asset(asset: ConceptAliasAsset, registry: ConceptIdRegistry) -> None:
    if not _ensure_alias_schema():
        return
    group_rows = [(item.concept_id, item.canonical_name, item.start_date, item.end_date) for item in asset.groups]
    member_rows = [
        (group.concept_id, member.provider, member.provider_concept_type, member.provider_concept_code, member.provider_concept_name, member.start_date, member.end_date)
        for group in asset.groups
        for member in group.members
    ]
    candidate_rows = [
        (
            item.left_provider,
            item.left_board_type,
            item.left_board_code,
            item.right_provider,
            item.right_board_type,
            item.right_board_code,
            item.confidence,
            item.status,
        )
        for item in asset.candidates
    ]
    signature_rows = [(signature, concept_id) for signature, concept_id in sorted(registry.signature_to_id.items(), key=lambda item: _concept_id_number(item[1]))]
    _write_alias_tables(group_rows, member_rows, candidate_rows, registry.next_id, signature_rows)


def _ensure_alias_schema() -> bool:
    global _ALIAS_SCHEMA_READY
    if _ALIAS_SCHEMA_READY:
        return True
    for statement in CONCEPT_ALIAS_SCHEMA_SQL:
        if not execute_sql(statement, ()):
            return False
    _ALIAS_SCHEMA_READY = True
    return True


def _write_alias_tables(
    group_rows: list[tuple[str, str, str, str]],
    member_rows: list[tuple[str, str, str, str, str, str, str]],
    candidate_rows: list[tuple[str, str, str, str, str, str, float, str]],
    next_id: int,
    signature_rows: list[tuple[str, str]],
) -> None:
    connection = psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        connect_timeout=DB_CONNECT_TIMEOUT,
        row_factory=dict_row,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("delete from derived.concept_alias_members")
            cursor.execute("delete from derived.concept_alias_groups")
            cursor.execute("delete from derived.concept_alias_candidates")
            cursor.execute("delete from derived.concept_alias_registry")
            cursor.execute("delete from derived.concept_alias_registry_signatures")
            cursor.executemany(
                """
                insert into derived.concept_alias_groups (concept_id, canonical_name, start_date, end_date)
                values (%s, %s, %s, %s)
                """,
                group_rows,
            )
            cursor.executemany(
                """
                insert into derived.concept_alias_members (concept_id, provider, provider_concept_type, provider_concept_code, provider_concept_name, start_date, end_date)
                values (%s, %s, %s, %s, %s, %s, %s)
                """,
                member_rows,
            )
            cursor.executemany(
                """
                insert into derived.concept_alias_candidates (
                    left_provider, left_board_type, left_board_code,
                    right_provider, right_board_type, right_board_code,
                    confidence, status
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                candidate_rows,
            )
            cursor.execute(
                """
                insert into derived.concept_alias_registry (registry_key, registry_value)
                values (%s, %s)
                """,
                ("next_id", str(next_id)),
            )
            cursor.executemany(
                """
                insert into derived.concept_alias_registry_signatures (signature, concept_id)
                values (%s, %s)
                """,
                signature_rows,
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _provider_has_concept_contracts(provider: str) -> bool:
    registry = get_default_source_package_registry()
    try:
        manifest = registry.get_manifest(provider)
    except KeyError:
        return False
    return manifest.supports_capability("concepts.catalog") and manifest.supports_capability("concepts.members")


def _typed_sources(
    provider: str,
    catalog_handler,
    members_handler,
    catalog_instance: SourceInstanceConfig,
    members_instance: SourceInstanceConfig,
    trade_date: str,
) -> tuple[ConceptProviderSource, ...]:
    if provider == "tushare":
        return _tushare_typed_sources(catalog_instance, trade_date)
    if provider == "akshare":
        return _akshare_typed_sources(catalog_instance, members_instance)
    return (
        ConceptProviderSource(
            provider=provider,
            board_type="default",
            fetch_catalog=_with_instance(catalog_handler, catalog_instance),
            fetch_members=_with_instance(members_handler, members_instance),
        ),
    )


def _tushare_typed_sources(catalog_instance: SourceInstanceConfig, trade_date: str) -> tuple[ConceptProviderSource, ...]:
    from quotemux_packages.tushare import source as tushare_source

    source_types = (
        ("ths", "ths_index", "ths_member", _tushare_ths_catalog, _tushare_standard_members),
        ("dc", "dc_index", "dc_member", _tushare_dc_catalog, _tushare_standard_members),
        ("tdx", "tdx_index", "tdx_member", _tushare_tdx_catalog, _tushare_standard_members),
        ("kpl", "kpl_concept_cons", "kpl_concept_cons", _tushare_kpl_catalog, _tushare_kpl_members),
    )
    sources: list[ConceptProviderSource] = []
    for board_type, catalog_api, members_api, catalog_builder, members_builder in source_types:
        sources.append(
            ConceptProviderSource(
                provider="tushare",
                board_type=board_type,
                fetch_catalog=_tushare_fetch_catalog(tushare_source, catalog_instance, board_type, catalog_api, catalog_builder, trade_date),
                fetch_members=_tushare_fetch_members(tushare_source, catalog_instance, board_type, members_api, members_builder, trade_date),
            )
        )
    return tuple(sources)


def _tushare_fetch_catalog(tushare_source, source_instance: SourceInstanceConfig, board_type: str, api_name: str, catalog_builder, trade_date: str):
    def fetcher(category: str, market: str, status: str, limit: int, offset: int) -> list[BoardCatalogItem]:
        del market
        if category not in {"", CONCEPT_CATEGORY}:
            return []
        with use_source_instance(source_instance):
            frame = catalog_builder(tushare_source, api_name, trade_date)
        if frame.empty:
            return []
        frame = _catalog_frame_with_dates(frame, trade_date)
        if status:
            frame = frame[frame["status"] == status]
        work = frame.sort_values("board_code").iloc[offset: offset + limit]
        return [
            BoardCatalogItem(
                board_code=str(row["board_code"]),
                board_name=str(row["board_name"]),
                category=CONCEPT_CATEGORY,
                market=board_type,
                status=str(row["status"]),
                start_date=_normalize_date_text(str(row["start_date"])),
                end_date=_normalize_date_text(str(row["end_date"])),
            )
            for _, row in work.iterrows()
        ]

    return fetcher


def _tushare_fetch_members(tushare_source, source_instance: SourceInstanceConfig, board_type: str, api_name: str, members_builder, trade_date: str):
    def fetcher(board_code: str, request_trade_date: str) -> list[BoardMemberItem]:
        actual_trade_date = _actual_trade_date(request_trade_date or trade_date)
        with use_source_instance(source_instance):
            frame = members_builder(tushare_source, api_name, board_code, actual_trade_date)
        if frame.empty:
            return []
        return [
            BoardMemberItem(board_code=str(row["board_code"]), code=str(row["code"]), name=str(row["name"]))
            for _, row in frame.sort_values("code").iterrows()
        ]

    return fetcher


def _tushare_ths_catalog(tushare_source, api_name: str, trade_date: str):
    del trade_date
    frames = [tushare_source._load_board_catalog_frame(index_type) for index_type in ("N", "I")]
    frames = [frame for frame in frames if not frame.empty]
    if frames == []:
        return tushare_source.pd.DataFrame()
    work = tushare_source.pd.concat(frames, ignore_index=True).drop_duplicates(subset=["board_code"], keep="last")
    work = work[work["category"] == CONCEPT_CATEGORY].copy()
    work = work.rename(columns={"name": "board_name"})
    for column in ("start_date", "list_date", "end_date"):
        if column not in work.columns:
            work[column] = ""
    work["start_date"] = work["start_date"].fillna("").astype(str)
    work.loc[work["start_date"] == "", "start_date"] = work["list_date"].fillna("").astype(str)
    work["end_date"] = work["end_date"].fillna("").astype(str)
    return work[["board_code", "board_name", "status", "start_date", "end_date"]]


def _tushare_dc_catalog(tushare_source, api_name: str, trade_date: str):
    pro = tushare_source.get_ts_pro()
    if pro is None:
        return tushare_source.pd.DataFrame()
    try:
        frame = tushare_source.call_tushare_api(api_name, getattr(pro, api_name), trade_date=_to_tushare_date(trade_date))
    except Exception:
        return tushare_source.pd.DataFrame()
    return _standard_tushare_catalog_frame(tushare_source, frame, "DC", ("概念板块", "题材�?))


def _tushare_tdx_catalog(tushare_source, api_name: str, trade_date: str):
    pro = tushare_source.get_ts_pro()
    if pro is None:
        return tushare_source.pd.DataFrame()
    try:
        frame = tushare_source.call_tushare_api(api_name, getattr(pro, api_name), trade_date=_to_tushare_date(trade_date))
    except Exception:
        return tushare_source.pd.DataFrame()
    return _standard_tushare_catalog_frame(tushare_source, frame, "TDX", ("概念板块",))


def _tushare_kpl_catalog(tushare_source, api_name: str, trade_date: str):
    frame = _tushare_kpl_raw_frame(tushare_source, api_name, trade_date)
    list_frame = _tushare_kpl_list_theme_frame(tushare_source, trade_date)
    frames = []
    if not frame.empty:
        work = frame[["ts_code", "name"]].drop_duplicates(subset=["ts_code"], keep="last").copy()
        work["board_code"] = work["ts_code"].fillna("").astype(str).str.upper()
        work["board_name"] = work["name"].fillna("").astype(str)
        work["status"] = "active"
        work["start_date"] = ""
        work["end_date"] = ""
        frames.append(work[["board_code", "board_name", "status", "start_date", "end_date"]])
    if not list_frame.empty:
        frames.append(list_frame[["board_code", "board_name", "status"]].drop_duplicates(subset=["board_code"], keep="last"))
    if frames == []:
        return tushare_source.pd.DataFrame()
    work = tushare_source.pd.concat(frames, ignore_index=True).drop_duplicates(subset=["board_code"], keep="last")
    work = work[(work["board_code"] != "") & (work["board_name"] != "")]
    return work[["board_code", "board_name", "status", "start_date", "end_date"]]


def _catalog_frame_with_dates(frame, trade_date: str):
    work = frame.copy()
    for column in ("start_date", "end_date"):
        if column not in work.columns:
            work[column] = ""
    work["start_date"] = work["start_date"].fillna("").astype(str).map(_normalize_date_text)
    work["end_date"] = work["end_date"].fillna("").astype(str).map(_normalize_date_text)
    return work


def _standard_tushare_catalog_frame(tushare_source, frame, suffix: str, concept_types: tuple[str, ...]):
    if frame is None or frame.empty:
        return tushare_source.pd.DataFrame()
    work = frame.copy()
    for column in ("ts_code", "name", "idx_type"):
        if column not in work.columns:
            work[column] = ""
    if concept_types != ():
        work = work[work["idx_type"].fillna("").astype(str).isin(concept_types)]
    work["board_code"] = work["ts_code"].fillna("").astype(str).str.upper().str.replace(f".{suffix}", "", regex=False)
    work["board_name"] = work["name"].fillna("").astype(str)
    work["status"] = "active"
    work["start_date"] = ""
    work["end_date"] = ""
    work = work[(work["board_code"] != "") & (work["board_name"] != "")]
    return work[["board_code", "board_name", "status", "start_date", "end_date"]].drop_duplicates(subset=["board_code"], keep="last")


def _tushare_standard_members(tushare_source, api_name: str, board_code: str, trade_date: str):
    pro = tushare_source.get_ts_pro()
    if pro is None:
        return tushare_source.pd.DataFrame()
    ts_code = _typed_tushare_code(board_code, api_name)
    try:
        frame = tushare_source.call_tushare_api(api_name, getattr(pro, api_name), ts_code=ts_code, trade_date=_to_tushare_date(trade_date))
    except Exception:
        return tushare_source.pd.DataFrame()
    if frame is None or frame.empty:
        return tushare_source.pd.DataFrame()
    work = frame.copy()
    code_column = "con_code"
    name_column = "con_name" if "con_name" in work.columns else "name"
    for column in (code_column, name_column):
        if column not in work.columns:
            work[column] = ""
    work["board_code"] = board_code
    work["code"] = work[code_column].map(tushare_source.normalize_stock_code)
    work["name"] = work[name_column].fillna("").astype(str)
    work = work[work["code"] != ""]
    return work[["board_code", "code", "name"]].drop_duplicates(subset=["code"], keep="last")


def _tushare_kpl_members(tushare_source, api_name: str, board_code: str, trade_date: str):
    frame = _tushare_kpl_raw_frame(tushare_source, api_name, trade_date)
    frames = []
    if not frame.empty:
        work = frame[frame["ts_code"].fillna("").astype(str).str.upper() == board_code.upper()].copy()
        for column in ("con_code", "con_name"):
            if column not in work.columns:
                work[column] = ""
        work["board_code"] = board_code
        work["code"] = work["con_code"].map(tushare_source.normalize_stock_code)
        work["name"] = work["con_name"].fillna("").astype(str)
        frames.append(work[["board_code", "code", "name"]])
    list_frame = _tushare_kpl_list_member_frame(tushare_source, board_code, trade_date)
    if not list_frame.empty:
        frames.append(list_frame)
    if frames == []:
        return tushare_source.pd.DataFrame()
    result = tushare_source.pd.concat(frames, ignore_index=True)
    result = result[result["code"] != ""]
    return result[["board_code", "code", "name"]].drop_duplicates(subset=["code"], keep="last")


def _tushare_kpl_raw_frame(tushare_source, api_name: str, trade_date: str):
    pro = tushare_source.get_ts_pro()
    if pro is None:
        return tushare_source.pd.DataFrame()
    try:
        frame = tushare_source.call_tushare_api(api_name, getattr(pro, api_name), trade_date=_to_tushare_date(trade_date))
    except Exception:
        return tushare_source.pd.DataFrame()
    if frame is None:
        return tushare_source.pd.DataFrame()
    return frame


def _tushare_kpl_list_frame(tushare_source, trade_date: str):
    pro = tushare_source.get_ts_pro()
    if pro is None:
        return tushare_source.pd.DataFrame()
    try:
        frame = tushare_source.call_tushare_api("kpl_list", getattr(pro, "kpl_list"), trade_date=_to_tushare_date(trade_date))
    except Exception:
        return tushare_source.pd.DataFrame()
    if frame is None:
        return tushare_source.pd.DataFrame()
    return frame


def _tushare_kpl_list_theme_frame(tushare_source, trade_date: str):
    frame = _tushare_kpl_list_frame(tushare_source, trade_date)
    if frame.empty or "theme" not in frame.columns:
        return tushare_source.pd.DataFrame()
    rows: list[dict[str, str]] = []
    for value in frame["theme"].fillna("").astype(str):
        for name in _split_kpl_themes(value):
            rows.append({"board_code": name, "board_name": name, "status": "active", "start_date": "", "end_date": ""})
    if rows == []:
        return tushare_source.pd.DataFrame()
    return tushare_source.pd.DataFrame(rows)


def _tushare_kpl_list_member_frame(tushare_source, board_code: str, trade_date: str):
    frame = _tushare_kpl_list_frame(tushare_source, trade_date)
    if frame.empty or "theme" not in frame.columns:
        return tushare_source.pd.DataFrame()
    for column in ("ts_code", "name"):
        if column not in frame.columns:
            frame[column] = ""
    rows: list[dict[str, str]] = []
    for _, row in frame.iterrows():
        themes = _split_kpl_themes(str(row["theme"]))
        if board_code not in themes:
            continue
        rows.append({"board_code": board_code, "code": tushare_source.normalize_stock_code(row["ts_code"]), "name": str(row["name"])})
    if rows == []:
        return tushare_source.pd.DataFrame()
    return tushare_source.pd.DataFrame(rows)


def _split_kpl_themes(value: str) -> list[str]:
    names: list[str] = []
    for item in re.split(r"[�?�?]+", value):
        name = item.strip()
        if name != "" and name not in names:
            names.append(name)
    return names


def _typed_tushare_code(board_code: str, api_name: str) -> str:
    text = board_code.upper()
    if "." in text:
        return text
    if api_name.startswith("dc_"):
        return f"{text}.DC"
    if api_name.startswith("tdx_"):
        return f"{text}.TDX"
    return f"{text}.TI"


def _akshare_typed_sources(catalog_instance: SourceInstanceConfig, members_instance: SourceInstanceConfig) -> tuple[ConceptProviderSource, ...]:
    from quotemux_packages.akshare import source as akshare_source

    return (
        ConceptProviderSource(
            provider="akshare",
            board_type="em",
            fetch_catalog=_akshare_fetch_catalog(akshare_source, catalog_instance, "em"),
            fetch_members=_akshare_fetch_members(akshare_source, members_instance),
        ),
        ConceptProviderSource(
            provider="akshare",
            board_type="ths",
            fetch_catalog=_akshare_fetch_catalog(akshare_source, catalog_instance, "ths"),
            fetch_members=_akshare_fetch_members(akshare_source, members_instance),
        ),
    )


def _akshare_fetch_catalog(akshare_source, source_instance: SourceInstanceConfig, board_type: str):
    def fetcher(category: str, market: str, status: str, limit: int, offset: int) -> list[BoardCatalogItem]:
        del market
        if category not in {"", CONCEPT_CATEGORY}:
            return []
        with use_source_instance(source_instance):
            frame = _akshare_catalog_frame(akshare_source, board_type)
        if frame.empty:
            return []
        if status:
            frame = frame[frame["status"] == status]
        work = frame.sort_values("board_code").iloc[offset: offset + limit]
        return [
            BoardCatalogItem(board_code=str(row["board_code"]), board_name=str(row["board_name"]), category=CONCEPT_CATEGORY, market=board_type, status=str(row["status"]))
            for _, row in work.iterrows()
        ]

    return fetcher


def _akshare_fetch_members(akshare_source, source_instance: SourceInstanceConfig):
    def fetcher(board_code: str, trade_date: str) -> list[BoardMemberItem]:
        del trade_date
        with use_source_instance(source_instance):
            return akshare_source.get_board_members(board_code, "")

    return fetcher


def _akshare_catalog_frame(akshare_source, board_type: str):
    api_map = {
        "em": ("stock_board_concept_name_em", akshare_source.ak.stock_board_concept_name_em),
        "ths": ("stock_board_concept_name_ths", akshare_source.ak.stock_board_concept_name_ths),
    }
    api_item = api_map.get(board_type)
    if api_item is None:
        return akshare_source.pd.DataFrame()
    api_name, fetcher = api_item
    cache_path = akshare_source.build_cache_path("akshare", ["boards", "catalog", f"type={board_type}"], {"category": CONCEPT_CATEGORY})
    cache_df = akshare_source.read_cache_frame(cache_path)
    if cache_df.empty:
        try:
            cache_df = akshare_source._normalize_board_catalog_frame(akshare_source._call_ak(api_name, fetcher), CONCEPT_CATEGORY)
        except Exception:
            return akshare_source.pd.DataFrame()
        if not cache_df.empty:
            akshare_source.write_cache_frame(cache_path, cache_df)
    return cache_df


def _source_instance(settings: QuoteMuxSettings, contract_name: str, provider: str):
    for instance in settings.get_contract_source_instances(contract_name, (provider,)):
        if instance.package_id == provider and instance.enabled:
            return _with_provider_env(instance)
    for instance in _enabled_runtime_instances(provider):
        return _with_provider_env(instance)
    return None


def _enabled_runtime_instances(provider: str) -> tuple[SourceInstanceConfig, ...]:
    snapshot = get_config_runtime().get_active_snapshot()
    return tuple(instance for instance in snapshot.list_enabled_source_instances() if instance.package_id == provider)


def _provider_has_member_runtime(provider: str, source_instance: SourceInstanceConfig) -> bool:
    if provider != "tushare":
        return True
    if source_instance.secret_values.get("api_key", "") != "":
        return True
    if source_instance.secret_values.get("token", "") != "":
        return True
    return os.getenv("TS_TOKEN", "") != ""


def _with_provider_env(source_instance: SourceInstanceConfig) -> SourceInstanceConfig:
    if source_instance.package_id != "tushare":
        return source_instance
    if source_instance.secret_values.get("api_key", "") != "":
        return source_instance
    token = os.getenv("TS_TOKEN", "")
    if token == "":
        return source_instance
    secret_values = dict(source_instance.secret_values)
    secret_values["api_key"] = token
    secret_values["token"] = token
    return replace(source_instance, secret_values=secret_values)


def _actual_trade_date(trade_date: str) -> str:
    text = _normalize_date_text(trade_date)
    if text != "":
        return text
    return datetime.now().strftime("%Y%m%d")


def _normalize_date_text(value: str) -> str:
    text = value.strip().replace("-", "")
    if text == "":
        return ""
    if re.fullmatch(r"[0-9]{8}", text):
        return text
    return ""


def _to_tushare_date(trade_date: str) -> str:
    return _actual_trade_date(trade_date)


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
