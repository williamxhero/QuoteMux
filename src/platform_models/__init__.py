from __future__ import annotations

import math

from pydantic import BaseModel, Field

from quotemux.infra.common import format_date_value, format_datetime_value


def format_api_temporal_value(field_name: str, value: str) -> str:
    if not value:
        return value
    if field_name == "report_period" or field_name.endswith("_date"):
        return format_date_value(value)
    if field_name.endswith("_time"):
        if value.count(":") == 2 and len(value) <= 8:
            return value
        if ":" not in value:
            return format_date_value(value)
        return format_datetime_value(value, "1m")
    return value


def format_api_dump_value(field_name: str, value: object) -> object:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, str):
        return format_api_temporal_value(field_name, value)
    if isinstance(value, dict):
        return {str(key): format_api_dump_value(str(key), item) for key, item in value.items()}
    if isinstance(value, list):
        return [format_api_dump_value(field_name, item) for item in value]
    return value


class ApiModel(BaseModel):
    def model_dump(self, *args, **kwargs):
        payload = super().model_dump(*args, **kwargs)
        return {field_name: format_api_dump_value(field_name, value) for field_name, value in payload.items()}


class ApiError(ApiModel):
    code: str
    message: str
    details: str = ""


class StockQuoteItem(ApiModel):
    code: str
    trade_time: str
    freq: str
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    pre_close: float | None = None
    change: float | None = None
    pct_chg: float | None = None
    volume: float | None = None
    amount: float | None = None
    adjust: str = "none"
    is_suspended: bool = False
    is_st: bool = False


class StockQuoteCodeSummary(ApiModel):
    code: str
    row_count: int
    expected_bar_count: int = 0
    actual_bar_count: int = 0
    first_trade_time: str = ""
    last_trade_time: str = ""
    complete: bool
    truncated: bool
    missing_trade_dates: list[str] = Field(default_factory=list)
    missing_trade_times: list[str] = Field(default_factory=list)


class StockQuotesMeta(ApiModel):
    total_rows: int
    returned_rows: int
    complete: bool
    truncated: bool
    codes: list[StockQuoteCodeSummary] = Field(default_factory=list)


class StockQuotesQueryResult(ApiModel):
    items: list[StockQuoteItem] = Field(default_factory=list)
    meta: StockQuotesMeta


class BoardQuoteItem(ApiModel):
    board_code: str
    board_name: str = ""
    trade_time: str
    freq: str
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    pre_close: float | None = None
    change: float | None = None
    pct_chg: float | None = None
    volume: float | None = None
    amount: float | None = None


class IndexQuoteItem(ApiModel):
    index_code: str
    trade_time: str
    freq: str
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    pre_close: float | None = None
    change: float | None = None
    pct_chg: float | None = None
    volume: float | None = None
    amount: float | None = None


class StockMoneyFlowItem(ApiModel):
    code: str
    trade_date: str
    view: str
    main_inflow: float | None = None
    main_outflow: float | None = None
    net_inflow: float | None = None


class NewsEventSourceItem(ApiModel):
    source_table: str
    source_record_id: str
    source_name: str
    source_type: str
    detail_url: str
    announcement_time: str
    crawl_time: str


class NewsEventItem(ApiModel):
    event_id: str
    trade_date: str
    announcement_time: str
    crawl_time: str
    session_tag: str
    event_type: str
    title: str
    summary: str
    content_text: str = ""
    importance_score: int
    sentiment: str
    source_name: str
    primary_detail_url: str
    related_stock_codes: list[str] = Field(default_factory=list)
    related_stock_names: list[str] = Field(default_factory=list)
    related_board_codes: list[str] = Field(default_factory=list)
    related_board_names: list[str] = Field(default_factory=list)
    topic_tags: list[str] = Field(default_factory=list)
    mentioned_stock_codes: list[str] = Field(default_factory=list)
    mentioned_stock_names: list[str] = Field(default_factory=list)
    mentioned_board_names: list[str] = Field(default_factory=list)
    sources: list[NewsEventSourceItem] = Field(default_factory=list)


class NewsEventQueryResult(ApiModel):
    events: list[NewsEventItem] = Field(default_factory=list)


class StockFinancialStatementItem(ApiModel):
    code: str
    report_period: str
    report_type: str
    announce_date: str
    revenue: float | None = None
    operating_profit: float | None = None
    total_profit: float | None = None
    net_profit: float | None = None
    total_assets: float | None = None
    total_liabilities: float | None = None
    equity: float | None = None


class AdjFactorItem(ApiModel):
    code: str
    trade_date: str
    adj_factor: float | None = None


class BoardRankingItem(ApiModel):
    board_code: str
    board_name: str
    trade_date: str
    rank: int | None = None
    change_pct: float | None = None
    turnover_rate: float | None = None
    net_inflow: float | None = None


class BoardMoneyFlowItem(ApiModel):
    board_code: str
    trade_date: str
    scope: str
    inflow: float | None = None
    outflow: float | None = None
    net_inflow: float | None = None


class StockBasicInfo(ApiModel):
    code: str
    name: str
    exchange: str
    market: str
    list_status: str
    list_date: str
    delist_date: str
    industry: str = ""
    area: str = ""


class NameHistoryItem(ApiModel):
    code: str
    name: str
    start_date: str
    end_date: str
    ann_date: str


class HLSignalItem(ApiModel):
    code: str
    trade_date: str
    first_extreme: str
    high_time: str = ""
    low_time: str = ""
    signal: str


class BoardCatalogItem(ApiModel):
    board_code: str
    board_name: str
    category: str = ""
    market: str = "a_share"
    status: str = "active"


class IndexCatalogItem(ApiModel):
    index_code: str
    index_name: str
    category: str = ""
    market: str = ""
    publisher: str = ""
    list_date: str = ""
    status: str = "active"


class BoardMemberItem(ApiModel):
    board_code: str
    code: str
    name: str
    weight: float | None = None
    join_date: str = ""


class BoardMemberHistoryItem(ApiModel):
    board_code: str
    code: str
    name: str
    effective_date: str
    action: str


class IndexMemberItem(ApiModel):
    index_code: str
    code: str
    name: str
    weight: float | None = None
    trade_date: str = ""


class BoardCategoryItem(ApiModel):
    category_code: str
    category_name: str
    parent_code: str = ""
    level: int | None = None
    sort_order: int | None = None


class MarketTemperatureItem(ApiModel):
    trade_date: str
    market: str
    temperature: float | None = None
    hot_count: int | None = None
    cold_count: int | None = None


class FengdanItem(ApiModel):
    trade_date: str
    code: str
    name: str
    side: str
    queue_volume: float | None = None
    queue_amount: float | None = None


class MarketCapitalFlowItem(ApiModel):
    trade_date: str
    market: str
    main_inflow: float | None = None
    main_outflow: float | None = None
    net_inflow: float | None = None


class LimitPoolItem(ApiModel):
    trade_date: str
    code: str
    name: str
    limit_type: str
    first_time: str = ""
    last_time: str = ""
    open_count: int | None = None
    reason: str = ""


class LimitLadderItem(ApiModel):
    trade_date: str
    code: str
    name: str
    streak_count: int | None = None
    latest_limit_time: str = ""


class TradingCalendarItem(ApiModel):
    exchange: str
    trade_date: str
    is_open: bool


class BoardShockItem(ApiModel):
    board_code: str
    trade_time: str
    shock_type: str
    change_pct: float | None = None
    description: str = ""


class ConnectCapitalFlowItem(ApiModel):
    trade_date: str
    market: str
    buy_amount: float | None = None
    sell_amount: float | None = None
    net_amount: float | None = None


class ConnectQuotaItem(ApiModel):
    trade_date: str
    market: str
    quota_total: float | None = None
    quota_balance: float | None = None
    quota_used: float | None = None


class ConnectActiveTop10Item(ApiModel):
    trade_date: str
    market: str
    code: str
    name: str
    rank: int | None = None
    buy_amount: float | None = None
    sell_amount: float | None = None
    net_amount: float | None = None


class BlockTradeItem(ApiModel):
    trade_date: str
    code: str
    name: str
    price: float | None = None
    volume: float | None = None
    amount: float | None = None
    buyer: str = ""
    seller: str = ""


class DragonTigerItem(ApiModel):
    trade_date: str
    code: str
    name: str
    reason: str = ""
    buy_amount: float | None = None
    sell_amount: float | None = None
    net_amount: float | None = None


class DragonTigerInstitutionItem(ApiModel):
    trade_date: str
    code: str
    name: str
    buy_amount: float | None = None
    sell_amount: float | None = None
    net_amount: float | None = None
    institution_count: int | None = None


class HotMoneyProfileItem(ApiModel):
    name: str
    alias: str = ""
    tag: str = ""
    style: str = ""


class HotMoneyDetailItem(ApiModel):
    trade_date: str
    name: str
    code: str
    stock_name: str = ""
    buy_amount: float | None = None
    sell_amount: float | None = None
    net_amount: float | None = None


class TradingSessionItem(ApiModel):
    code: str
    session_name: str
    start_time: str
    end_time: str
    timezone: str


class AuctionItem(ApiModel):
    code: str
    trade_date: str
    auction_time: str
    price: float | None = None
    volume: float | None = None
    amount: float | None = None
    session: str = ""


class RankingResearchReportItem(ApiModel):
    trade_date: str
    code: str
    name: str
    institution: str = ""
    rating: str = ""
    target_price: float | None = None
    title: str = ""


class RankingBrokerPickItem(ApiModel):
    trade_month: str
    code: str
    name: str
    institution: str = ""
    rank: int | None = None
    recommend_count: int | None = None
    rating: str = ""


class StockArchiveItem(ApiModel):
    trade_date: str
    code: str
    name: str
    exchange: str
    market: str
    list_status: str
    industry: str = ""
    area: str = ""


class StockFinanceIndicatorItem(ApiModel):
    code: str
    report_period: str
    roe: float | None = None
    roa: float | None = None
    gross_margin: float | None = None
    net_margin: float | None = None
    asset_turnover: float | None = None
    current_ratio: float | None = None
    debt_to_asset: float | None = None


class StockAHComparisonItem(ApiModel):
    code: str
    name: str
    h_code: str = ""
    trade_date: str
    a_close: float | None = None
    h_close: float | None = None
    premium_ratio: float | None = None


class StockDailyBasicItem(ApiModel):
    code: str
    trade_date: str
    turnover_rate: float | None = None
    volume_ratio: float | None = None
    pe: float | None = None
    pb: float | None = None
    total_share: float | None = None
    float_share: float | None = None


class StockDailyValuationItem(ApiModel):
    code: str
    trade_date: str
    pe: float | None = None
    pb: float | None = None
    ps: float | None = None
    pcf: float | None = None
    dv_ratio: float | None = None


class StockDailyMarketValueItem(ApiModel):
    code: str
    trade_date: str
    total_mv: float | None = None
    float_mv: float | None = None
    free_mv: float | None = None


class StockRiskFlagItem(ApiModel):
    code: str
    name: str
    flag_type: str
    start_date: str
    end_date: str
    status: str


class StockPremarketItem(ApiModel):
    code: str
    trade_date: str
    total_share: float | None = None
    float_share: float | None = None
    limit_up: float | None = None
    limit_down: float | None = None


class ChipDistributionItem(ApiModel):
    code: str
    trade_date: str
    price: float | None = None
    chip_ratio: float | None = None


class ChipPerformanceItem(ApiModel):
    code: str
    trade_date: str
    profit_ratio: float | None = None
    avg_cost: float | None = None
    cost_70: float | None = None
    cost_90: float | None = None


class TechnicalFactorItem(ApiModel):
    code: str
    trade_date: str
    adjust: str
    ma5: float | None = None
    ma10: float | None = None
    ma20: float | None = None
    ma60: float | None = None
    ema12: float | None = None
    ema26: float | None = None
    dif: float | None = None
    dea: float | None = None
    macd: float | None = None
    rsi6: float | None = None
    rsi12: float | None = None
    rsi24: float | None = None
    kdj_k: float | None = None
    kdj_d: float | None = None
    kdj_j: float | None = None
    boll_upper: float | None = None
    boll_mid: float | None = None
    boll_lower: float | None = None


class AuditItem(ApiModel):
    code: str
    report_period: str
    audit_result: str = ""
    auditor: str = ""
    sign_accountant: str = ""
    announce_date: str = ""


class DisclosureDateItem(ApiModel):
    code: str
    report_period: str
    plan_date: str = ""
    actual_date: str = ""
    change_reason: str = ""


class ExpressItem(ApiModel):
    code: str
    report_period: str
    announce_date: str
    revenue: float | None = None
    operating_profit: float | None = None
    total_profit: float | None = None
    net_profit: float | None = None
    eps: float | None = None
    roe: float | None = None


class ForecastItem(ApiModel):
    code: str
    report_period: str
    forecast_type: str = ""
    forecast_summary: str = ""
    net_profit_min: float | None = None
    net_profit_max: float | None = None
    pct_chg_min: float | None = None
    pct_chg_max: float | None = None


class MainBusinessItem(ApiModel):
    code: str
    report_period: str
    classification: str
    segment_name: str
    revenue: float | None = None
    cost: float | None = None
    profit: float | None = None
    revenue_ratio: float | None = None


class DividendItem(ApiModel):
    code: str
    announce_date: str
    record_date: str = ""
    ex_date: str = ""
    pay_date: str = ""
    cash_dividend_per_share: float | None = None
    stock_dividend_per_share: float | None = None
    capital_reserve_per_share: float | None = None


class RepurchaseItem(ApiModel):
    code: str
    announce_date: str
    progress: str = ""
    repurchase_volume: float | None = None
    repurchase_amount: float | None = None
    highest_price: float | None = None
    lowest_price: float | None = None


class RightsIssueItem(ApiModel):
    code: str
    announce_date: str
    rights_ratio: float | None = None
    rights_price: float | None = None
    record_date: str = ""
    ex_date: str = ""


class ShareChangeItem(ApiModel):
    code: str
    change_date: str
    reason: str = ""
    total_share: float | None = None
    float_share: float | None = None
    restricted_share: float | None = None


class UnlockScheduleItem(ApiModel):
    code: str
    unlock_date: str
    holder_type: str = ""
    unlock_volume: float | None = None
    unlock_ratio: float | None = None
    share_type: str = ""


class CcassHoldingItem(ApiModel):
    code: str
    trade_date: str
    participant_count: int | None = None
    holding_volume: float | None = None
    holding_ratio: float | None = None


class CcassHoldingDetailItem(ApiModel):
    code: str
    trade_date: str
    participant_id: str = ""
    participant_name: str = ""
    holding_volume: float | None = None
    holding_ratio: float | None = None


class HKConnectHoldingItem(ApiModel):
    code: str
    trade_date: str
    holding_volume: float | None = None
    holding_ratio: float | None = None
    change_volume: float | None = None


class PledgeStatItem(ApiModel):
    code: str
    trade_date: str
    pledge_volume: float | None = None
    pledge_ratio: float | None = None
    unrestricted_pledge_volume: float | None = None


class PledgeDetailItem(ApiModel):
    code: str
    holder_name: str = ""
    start_date: str
    end_date: str = ""
    pledge_volume: float | None = None
    pledge_ratio: float | None = None
    status: str = ""


class ShareholderCountItem(ApiModel):
    code: str
    trade_date: str
    holder_count: int | None = None
    avg_holding: float | None = None


class ShareholderChangeItem(ApiModel):
    code: str
    trade_date: str
    holder_count: int | None = None
    change_count: int | None = None
    change_pct: float | None = None


class ShareholderTop10Item(ApiModel):
    code: str
    report_period: str
    rank: int | None = None
    shareholder_name: str = ""
    holding_volume: float | None = None
    holding_ratio: float | None = None
    change_volume: float | None = None


class StockProfileItem(ApiModel):
    code: str
    company_name: str = ""
    full_name: str = ""
    chairman: str = ""
    manager: str = ""
    website: str = ""
    employee_count: int | None = None
    main_business: str = ""
    office: str = ""


class StockManagerItem(ApiModel):
    code: str
    name: str
    title: str = ""
    gender: str = ""
    education: str = ""
    begin_date: str = ""
    end_date: str = ""


class ManagementRewardItem(ApiModel):
    code: str
    ann_date: str
    name: str
    title: str = ""
    reward_amount: float | None = None
    hold_amount: float | None = None


class ResearchReportItem(ApiModel):
    code: str
    report_date: str
    institution: str = ""
    analyst: str = ""
    rating: str = ""
    target_price: float | None = None
    title: str = ""


class SurveyItem(ApiModel):
    code: str
    survey_date: str
    org_name: str = ""
    survey_method: str = ""
    topic: str = ""
    announcement_date: str = ""


class NineTurnItem(ApiModel):
    code: str
    trade_time: str
    freq: str
    setup_index: int | None = None
    countdown_index: int | None = None
    signal: str = ""


class HKConnectTargetItem(ApiModel):
    code: str
    name: str
    direction: str
    status: str
    effective_date: str


class BSECodeMappingItem(ApiModel):
    old_code: str
    new_code: str
    effective_date: str
    status: str

