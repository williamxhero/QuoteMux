from __future__ import annotations

import quotemux  # noqa: F401
from platform_models import BoardMemberItem as PlatformBoardMemberItem, BoardMoneyFlowItem, BoardQuoteItem, ConceptAliasGroupItem, ConceptAliasGroupMemberItem
from quotemux.concept_runtime import QuoteMuxConceptRuntime
from quotemux.config_runtime.models import SourceInstanceConfig
from quotemux.concepts import ConceptIdRegistry, ConceptProviderSource, _assign_concept_ids, _concept_name_start_date, _group_signature, _typed_sources, build_concept_alias_asset
from quotemux.models import BoardCatalogItem, BoardMemberItem
from quotemux.settings import QuoteMuxSettings


def test_concept_name_start_date_uses_report_period() -> None:
    rows = [
        ("A001", "2025年报预增", "20260101"),
        ("A002", "2025四季报预增", "20260101"),
        ("A003", "2026一季报预增", "20260401"),
        ("A004", "2026中报预增", "20260701"),
        ("A005", "2026二季报预增", "20260701"),
        ("A006", "2026三季报预增", "20261001"),
    ]
    assert {_name: _concept_name_start_date(_name) for _, _name, _ in rows} == {name: start_date for _, name, start_date in rows}


def test_concept_alias_asset_excludes_financial_report_period_topics() -> None:
    rows = [
        ("886108", "AI应用"),
        ("886109", "2026一季报预增"),
    ]

    def fetch_catalog(category: str, market: str, status: str, limit: int, offset: int) -> list[BoardCatalogItem]:
        del market, status, limit, offset
        return [BoardCatalogItem(board_code=code, board_name=name, category=category) for code, name in rows]

    def fetch_members(board_code: str, trade_date: str) -> list[BoardMemberItem]:
        del trade_date
        return [BoardMemberItem(board_code=board_code, code="000001", name="")]

    source = ConceptProviderSource(provider="crawler_provider", board_type="ths", fetch_catalog=fetch_catalog, fetch_members=fetch_members)
    asset = build_concept_alias_asset((source,), "20260708", ())

    assert [group.canonical_name for group in asset.groups] == ["AI应用"]


def test_concept_runtime_reads_crawler_provider_with_existing_alias(monkeypatch) -> None:
    alias_group = ConceptAliasGroupItem(
        concept_id="C1",
        canonical_name="测试题材",
        members=[ConceptAliasGroupMemberItem(provider="akshare", provider_concept_type="em", provider_concept_code="BK1184", provider_concept_name="测试题材")],
    )
    calls: list[tuple[str, str]] = []

    def fake_list_concept_aliases(self, concept_id: str, trade_date: str, source_order: tuple[str, ...]):
        del self, trade_date, source_order
        if concept_id != "C1":
            return ()
        from quotemux.concepts import ConceptBoardAlias

        return (
            ConceptBoardAlias(concept_id="C1", canonical_name="测试题材", provider="akshare", board_type="em", board_code="BK1184", board_name="测试题材"),
        )

    def fake_source_package_call(package_id: str, handler_name: str, *args: object):
        calls.append((package_id, handler_name))
        if package_id == "crawler_provider" and handler_name == "get_concept_quotes":
            return [BoardQuoteItem(board_code="BK1184", trade_time="2026-06-18", freq="1d", close=1.1)]
        if package_id == "crawler_provider" and handler_name == "get_concept_members":
            return [PlatformBoardMemberItem(board_code="BK1184", code="600000", name="")]
        return []

    monkeypatch.setattr("quotemux.concepts.QuoteMuxConcepts.list_concept_aliases", fake_list_concept_aliases)
    monkeypatch.setattr("quotemux.concept_runtime._source_package_call", fake_source_package_call)
    monkeypatch.setattr("quotemux.concept_runtime._timed_source_package_call", lambda settings, capability_id, package_id, handler_name, *args: fake_source_package_call(package_id, handler_name, *args))
    runtime = QuoteMuxConceptRuntime(QuoteMuxSettings(enabled_sources=("crawler_provider",)))

    quote_items = runtime.get_quotes([alias_group.concept_id], "1d", "2026-06-18", "", "", "", "", None, 10)
    member_items = runtime.get_members(alias_group.concept_id, "2026-06-18")

    assert [(item.concept_id, item.concept_name, item.close) for item in quote_items] == [("C1", "测试题材", 1.1)]
    assert [(item.concept_id, item.code) for item in member_items] == [("C1", "600000")]
    assert ("crawler_provider", "get_concept_quotes") in calls
    assert ("crawler_provider", "get_concept_members") in calls


def test_concept_money_flow_provider_call_uses_timeout_wrapper(monkeypatch) -> None:
    from quotemux.concepts import ConceptBoardAlias

    runtime = QuoteMuxConceptRuntime(QuoteMuxSettings(enabled_sources=("akshare",)))
    calls: list[tuple[str, str, str]] = []
    alias = ConceptBoardAlias(
        concept_id="C1",
        canonical_name="测试题材",
        provider="akshare",
        board_type="em",
        board_code="BK1184",
        board_name="测试题材",
    )
    monkeypatch.setattr(runtime, "_concept_aliases", lambda *args: (alias,))
    monkeypatch.setattr(runtime, "_get_concept_money_flow_from_stock_flows", lambda *args: [])
    monkeypatch.setattr("quotemux.concept_runtime._load_money_flow_snapshot_item", lambda *args: [])
    monkeypatch.setattr("quotemux.concept_runtime._load_money_flow_snapshot_range_items", lambda *args: [])
    monkeypatch.setattr("quotemux.concept_runtime.load_store_result", lambda *args: ([], type("Read", (), {"hit": False})()))

    def timed_call(settings, capability_id: str, package_id: str, handler_name: str, *args: object):
        del settings, args
        calls.append((capability_id, package_id, handler_name))
        return [BoardMoneyFlowItem(board_code="BK1184", trade_date="2026-07-17", scope="concept", net_inflow=1.0)]

    monkeypatch.setattr("quotemux.concept_runtime._timed_source_package_call", timed_call)

    items = runtime.get_money_flow("C1", "2026-07-17", "", "", "concept")

    assert len(items) == 1
    assert calls == [
        ("concepts.indicators.money_flow.snapshot", "akshare", "get_concept_daily_money_flow_snapshot"),
        ("concepts.indicators.money_flow", "akshare", "get_concept_money_flow"),
    ]


def test_concept_alias_asset_uses_crawler_provider_members() -> None:
    source_instance = SourceInstanceConfig(
        instance_id="crawler_provider-default",
        package_id="crawler_provider",
        display_name="Crawler Provider",
        enabled=True,
        priority=1,
        timeout_seconds=None,
        config_values={},
        secret_values={},
        tags=(),
    )
    catalog_calls: list[tuple[str, str, str, int, int]] = []
    member_calls: list[tuple[str, str]] = []

    def crawler_catalog(category: str, market: str, status: str, limit: int, offset: int) -> list[BoardCatalogItem]:
        catalog_calls.append((category, market, status, limit, offset))
        return [BoardCatalogItem(board_code="A001", board_name="robotics", category="concept", market=market, status="active")]

    def crawler_members(concept_id: str, trade_date: str) -> list[BoardMemberItem]:
        member_calls.append((concept_id, trade_date))
        return [BoardMemberItem(board_code=concept_id, code="000001", name=""), BoardMemberItem(board_code=concept_id, code="000002", name="")]

    def akshare_catalog(category: str, market: str, status: str, limit: int, offset: int) -> list[BoardCatalogItem]:
        del market, status, limit, offset
        return [BoardCatalogItem(board_code="B001", board_name="robotics", category=category)]

    def akshare_members(board_code: str, trade_date: str) -> list[BoardMemberItem]:
        del trade_date
        return [BoardMemberItem(board_code=board_code, code="000001", name=""), BoardMemberItem(board_code=board_code, code="000002", name="")]

    crawler_sources = _typed_sources("crawler_provider", crawler_catalog, crawler_members, source_instance, source_instance, "20260708")
    akshare_source = ConceptProviderSource(provider="akshare", board_type="em", fetch_catalog=akshare_catalog, fetch_members=akshare_members)
    asset = build_concept_alias_asset((*crawler_sources, akshare_source), "20260708", ())

    merged_groups = [
        group
        for group in asset.groups
        if {member.provider for member in group.members} == {"crawler_provider", "akshare"}
    ]

    assert [source.board_type for source in crawler_sources] == ["ths", "em"]
    assert ("concept", "ths", "active", 10000, 0) in catalog_calls
    assert ("concept", "em", "active", 10000, 0) in catalog_calls
    assert ("ths:A001", "20260708") in member_calls
    assert ("em:A001", "20260708") in member_calls
    assert len(merged_groups) == 1
    assert [(member.provider, member.provider_concept_type) for member in merged_groups[0].members] == [("akshare", "em"), ("crawler_provider", "em"), ("crawler_provider", "ths")]


def test_concept_alias_asset_uses_manual_confirmed_review_pairs() -> None:
    def em_catalog(category: str, market: str, status: str, limit: int, offset: int) -> list[BoardCatalogItem]:
        del market, status, limit, offset
        return [BoardCatalogItem(board_code="BK1156", board_name="PEEK Material", category=category)]

    def ths_catalog(category: str, market: str, status: str, limit: int, offset: int) -> list[BoardCatalogItem]:
        del market, status, limit, offset
        return [BoardCatalogItem(board_code="309105", board_name="PEEK Resin", category=category)]

    def fetch_members(board_code: str, trade_date: str) -> list[BoardMemberItem]:
        del trade_date
        codes_by_board = {
            "BK1156": ("000001", "000002", "000003", "000004", "000005"),
            "309105": ("000001", "000002", "000003", "000006", "000007"),
        }
        return [BoardMemberItem(board_code=board_code, code=code, name="") for code in codes_by_board[board_code]]

    sources = (
        ConceptProviderSource(provider="crawler_provider", board_type="em", fetch_catalog=em_catalog, fetch_members=fetch_members),
        ConceptProviderSource(provider="crawler_provider", board_type="ths", fetch_catalog=ths_catalog, fetch_members=fetch_members),
    )
    asset = build_concept_alias_asset(sources, "20260708", ())

    assert len(asset.groups) == 1
    assert [(member.provider_concept_type, member.provider_concept_code) for member in asset.groups[0].members] == [("em", "BK1156"), ("ths", "309105")]
    assert [candidate.status for candidate in asset.candidates] == ["confirmed"]
    assert asset.candidates[0].confidence < 0.70


def test_concept_alias_asset_uses_crawler_ths_dual_ids() -> None:
    def crawler_catalog(category: str, market: str, status: str, limit: int, offset: int) -> list[BoardCatalogItem]:
        del market, status, limit, offset
        return [BoardCatalogItem(board_code="886071", board_name="AI PC", category=category, market="ths", url_id="309121")]

    def akshare_catalog(category: str, market: str, status: str, limit: int, offset: int) -> list[BoardCatalogItem]:
        del market, status, limit, offset
        return [BoardCatalogItem(board_code="309121", board_name="AI PC", category=category, market="ths")]

    def tushare_catalog(category: str, market: str, status: str, limit: int, offset: int) -> list[BoardCatalogItem]:
        del market, status, limit, offset
        return [BoardCatalogItem(board_code="886071", board_name="AI PC", category=category, market="ths")]

    def crawler_members(board_code: str, trade_date: str) -> list[BoardMemberItem]:
        del trade_date
        return [BoardMemberItem(board_code=board_code, code="000001", name=""), BoardMemberItem(board_code=board_code, code="000002", name="")]

    def akshare_members(board_code: str, trade_date: str) -> list[BoardMemberItem]:
        del trade_date
        return [BoardMemberItem(board_code=board_code, code="000003", name=""), BoardMemberItem(board_code=board_code, code="000004", name="")]

    def tushare_members(board_code: str, trade_date: str) -> list[BoardMemberItem]:
        del trade_date
        return [BoardMemberItem(board_code=board_code, code="000005", name=""), BoardMemberItem(board_code=board_code, code="000006", name="")]

    sources = (
        ConceptProviderSource(provider="crawler_provider", board_type="ths", fetch_catalog=crawler_catalog, fetch_members=crawler_members),
        ConceptProviderSource(provider="akshare", board_type="ths", fetch_catalog=akshare_catalog, fetch_members=akshare_members),
        ConceptProviderSource(provider="tushare", board_type="ths", fetch_catalog=tushare_catalog, fetch_members=tushare_members),
    )
    asset = build_concept_alias_asset(sources, "20260708", ())

    assert len(asset.groups) == 1
    assert {(member.provider, member.provider_concept_code) for member in asset.groups[0].members} == {("crawler_provider", "886071"), ("akshare", "309121"), ("tushare", "886071")}
    direct_candidates = [candidate for candidate in asset.candidates if "crawler_provider" in {candidate.left_provider, candidate.right_provider}]

    assert [candidate.status for candidate in direct_candidates] == ["confirmed", "confirmed"]
    assert [candidate.confidence for candidate in direct_candidates] == [1.0, 1.0]


def test_concept_alias_asset_uses_same_type_provider_id_first() -> None:
    def left_catalog(category: str, market: str, status: str, limit: int, offset: int) -> list[BoardCatalogItem]:
        del market, status, limit, offset
        return [BoardCatalogItem(board_code="A001", board_name="left topic", category=category, market="ths")]

    def right_catalog(category: str, market: str, status: str, limit: int, offset: int) -> list[BoardCatalogItem]:
        del market, status, limit, offset
        return [BoardCatalogItem(board_code="B001", board_name="right topic", category=category, market="ths", url_id="A001")]

    def left_members(board_code: str, trade_date: str) -> list[BoardMemberItem]:
        del trade_date
        return [BoardMemberItem(board_code=board_code, code="000001", name="")]

    def right_members(board_code: str, trade_date: str) -> list[BoardMemberItem]:
        del trade_date
        return [BoardMemberItem(board_code=board_code, code="000002", name="")]

    sources = (
        ConceptProviderSource(provider="left_provider", board_type="ths", fetch_catalog=left_catalog, fetch_members=left_members),
        ConceptProviderSource(provider="right_provider", board_type="ths", fetch_catalog=right_catalog, fetch_members=right_members),
    )
    asset = build_concept_alias_asset(sources, "20260708", ())

    assert len(asset.groups) == 1
    assert asset.candidates[0].status == "confirmed"
    assert asset.candidates[0].confidence == 1.0


def test_concept_alias_asset_does_not_use_id_first_across_types() -> None:
    def left_catalog(category: str, market: str, status: str, limit: int, offset: int) -> list[BoardCatalogItem]:
        del market, status, limit, offset
        return [BoardCatalogItem(board_code="A001", board_name="alpha", category=category, market="ths")]

    def right_catalog(category: str, market: str, status: str, limit: int, offset: int) -> list[BoardCatalogItem]:
        del market, status, limit, offset
        return [BoardCatalogItem(board_code="A001", board_name="omega", category=category, market="em")]

    def left_members(board_code: str, trade_date: str) -> list[BoardMemberItem]:
        del trade_date
        return [BoardMemberItem(board_code=board_code, code="000001", name="")]

    def right_members(board_code: str, trade_date: str) -> list[BoardMemberItem]:
        del trade_date
        return [BoardMemberItem(board_code=board_code, code="000002", name="")]

    sources = (
        ConceptProviderSource(provider="left_provider", board_type="ths", fetch_catalog=left_catalog, fetch_members=left_members),
        ConceptProviderSource(provider="right_provider", board_type="em", fetch_catalog=right_catalog, fetch_members=right_members),
    )
    asset = build_concept_alias_asset(sources, "20260708", ())

    assert len(asset.groups) == 2
    assert asset.candidates == ()


def test_assign_concept_ids_reuses_existing_signatures_after_reorder() -> None:
    first_group = _test_alias_group("", "3D camera", (("akshare", "ths", "885001"),))
    second_group = _test_alias_group("", "AI office", (("tushare", "ths", "309001"),))
    registry = ConceptIdRegistry(
        next_id=3,
        signature_to_id={
            _group_signature(second_group): "C1",
            _group_signature(first_group): "C2",
        },
    )

    assigned = _assign_concept_ids((first_group, second_group), registry)
    ids_by_name = {group.canonical_name: group.concept_id for group in assigned}

    assert ids_by_name == {"AI office": "C1", "3D camera": "C2"}
    assert registry.next_id == 3


def test_assign_concept_ids_reuses_old_id_when_group_expands() -> None:
    old_group = _test_alias_group("", "AI office", (("tushare", "ths", "309001"),))
    expanded_group = _test_alias_group(
        "",
        "AI office",
        (("akshare", "ths", "885001"), ("crawler_provider", "ths", "309001"), ("tushare", "ths", "309001")),
    )
    new_group = _test_alias_group("", "new concept", (("akshare", "em", "BK9001"),))
    registry = ConceptIdRegistry(next_id=8, signature_to_id={_group_signature(old_group): "C7"})

    assigned = _assign_concept_ids((new_group, expanded_group), registry)
    ids_by_name = {group.canonical_name: group.concept_id for group in assigned}

    assert ids_by_name["AI office"] == "C7"
    assert ids_by_name["new concept"] == "C8"
    assert registry.signature_to_id[_group_signature(old_group)] == "C7"
    assert registry.signature_to_id[_group_signature(expanded_group)] == "C7"
    assert registry.next_id == 9


def test_assign_concept_ids_reuses_old_id_when_group_shrinks() -> None:
    old_group = _test_alias_group("", "AI office", (("akshare", "ths", "885001"), ("tushare", "ths", "309001")))
    current_group = _test_alias_group("", "AI office", (("tushare", "ths", "309001"),))
    registry = ConceptIdRegistry(next_id=8, signature_to_id={_group_signature(old_group): "C7"})

    assigned = _assign_concept_ids((current_group,), registry)

    assert assigned[0].concept_id == "C7"
    assert registry.signature_to_id[_group_signature(old_group)] == "C7"
    assert registry.signature_to_id[_group_signature(current_group)] == "C7"
    assert registry.next_id == 8


def _test_alias_group(concept_id: str, canonical_name: str, members: tuple[tuple[str, str, str], ...]) -> ConceptAliasGroupItem:
    return ConceptAliasGroupItem(
        concept_id=concept_id,
        canonical_name=canonical_name,
        members=[
            ConceptAliasGroupMemberItem(provider=provider, provider_concept_type=board_type, provider_concept_code=board_code, provider_concept_name=canonical_name)
            for provider, board_type, board_code in members
        ],
    )
