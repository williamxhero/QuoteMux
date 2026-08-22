from __future__ import annotations

from datetime import date, datetime
import math

from pydantic import BaseModel, Field


def _format_date_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    text = str(value).strip()
    if text == "":
        return ""
    try:
        if len(text) == 8 and text.isdigit():
            return datetime.strptime(text, "%Y%m%d").strftime("%Y-%m-%d")
        return datetime.fromisoformat(text.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except ValueError:
        return text


def _format_datetime_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    text = str(value).strip()
    if text == "":
        return ""
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return text


def format_api_temporal_value(field_name: str, value: str) -> str:
    if not value:
        return value
    if field_name == "report_period" or field_name.endswith("_date"):
        return _format_date_value(value)
    if field_name.endswith("_time"):
        if value.count(":") == 2 and len(value) <= 8:
            return value
        if ":" not in value:
            return _format_date_value(value)
        return _format_datetime_value(value)
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
    missing_count: int = 0
    first_trade_time: str = ""
    last_trade_time: str = ""
    complete: bool
    truncated: bool
    missing_trade_dates: list[str] = Field(default_factory=list)
    missing_trade_times: list[str] = Field(default_factory=list)


class StockQuotesMeta(ApiModel):
    data_version: str = ""
    total_rows: int
    returned_rows: int
    complete: bool
    truncated: bool
    codes: list[StockQuoteCodeSummary] = Field(default_factory=list)


class StockQuotesQueryResult(ApiModel):
    items: list[StockQuoteItem] = Field(default_factory=list)
    meta: StockQuotesMeta


class EtfCatalogItem(ApiModel):
    ts_code: str
    code: str
    market: str
    name: str
    fund_type: str = ""
    management: str = ""
    custodian: str = ""
    list_date: str = ""
    delist_date: str = ""


class EtfDailyQuoteItem(ApiModel):
    ts_code: str
    trade_date: str
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    pre_close: float | None = None
    change: float | None = None
    pct_chg: float | None = None
    volume: float | None = None
    amount: float | None = None


class EtfDailyQuoteCodeSummary(ApiModel):
    ts_code: str
    row_count: int
    expected_bar_count: int = 0
    actual_bar_count: int = 0
    missing_count: int = 0
    first_trade_date: str = ""
    last_trade_date: str = ""
    complete: bool
    truncated: bool
    missing_trade_dates: list[str] = Field(default_factory=list)


class EtfDailyQuotesMeta(ApiModel):
    total_rows: int
    returned_rows: int
    complete: bool
    truncated: bool
    codes: list[EtfDailyQuoteCodeSummary] = Field(default_factory=list)


class EtfDailyQuotesQueryResult(ApiModel):
    items: list[EtfDailyQuoteItem] = Field(default_factory=list)
    meta: EtfDailyQuotesMeta


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


class ConceptQuoteItem(ApiModel):
    concept_id: str
    concept_name: str = ""
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
    active_buy_amount: float | None = None


class StockMarginItem(ApiModel):
    code: str
    trade_date: str
    financing_balance: float | None = None
    financing_buy: float | None = None
    financing_repay: float | None = None
    securities_lending_balance: float | None = None
    securities_lending_volume: float | None = None
    securities_lending_repay: float | None = None
    securities_lending_sell: float | None = None
    total_margin_balance: float | None = None


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
    related_concept_ids: list[str] = Field(default_factory=list)
    related_concept_names: list[str] = Field(default_factory=list)
    topic_tags: list[str] = Field(default_factory=list)
    mentioned_stock_codes: list[str] = Field(default_factory=list)
    mentioned_stock_names: list[str] = Field(default_factory=list)
    mentioned_concept_names: list[str] = Field(default_factory=list)
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


class ConceptMoneyFlowItem(ApiModel):
    concept_id: str
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
    listing_board: str = ""
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
    url_id: str = ""
    start_date: str = ""
    end_date: str = ""


class ConceptCatalogItem(ApiModel):
    concept_id: str
    concept_name: str
    category: str = ""
    market: str = "a_share"
    status: str = "active"
    start_date: str = ""
    end_date: str = ""


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


class ConceptMemberItem(ApiModel):
    concept_id: str
    code: str
    name: str
    weight: float | None = None
    join_date: str = ""


class ConceptAliasResolveItem(ApiModel):
    concept_id: str
    canonical_name: str
    confidence: float | None = None


class ConceptAliasGroupMemberItem(ApiModel):
    provider: str
    provider_concept_type: str = ""
    provider_concept_code: str
    provider_concept_name: str
    start_date: str = ""
    end_date: str = ""

    @property
    def board_type(self) -> str:
        return self.provider_concept_type

    @property
    def board_code(self) -> str:
        return self.provider_concept_code

    @property
    def board_name(self) -> str:
        return self.provider_concept_name


class ConceptAliasGroupItem(ApiModel):
    concept_id: str
    canonical_name: str
    start_date: str = ""
    end_date: str = ""
    members: list[ConceptAliasGroupMemberItem] = Field(default_factory=list)


class BoardMemberHistoryItem(ApiModel):
    board_code: str
    code: str
    name: str
    effective_date: str
    action: str


class ConceptMemberHistoryItem(ApiModel):
    concept_id: str
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


class ConceptCategoryItem(ApiModel):
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
    free_share: float | None = None


class StockDailyValuationItem(ApiModel):
    code: str
    trade_date: str
    pe: float | None = None
    pb: float | None = None
    ps: float | None = None
    pcf: float | None = None
    dv_ratio: float | None = None
    dv_ttm: float | None = None


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


class StockStrategyFactorItem(ApiModel):
    trade_date: str
    code: str
    listing_board: str = ""
    close: float | None = None
    dividend_yield_pct: float | None = None
    dividend_yield_ttm_pct: float | None = None
    float_market_cap: float | None = None
    circulating_shares: float | None = None
    free_float_shares: float | None = None
    active_buy_amount: float | None = None
    active_buy_amount_proportion_all: float | None = None
    volume_shares: float | None = None
    mean_volume_5d_shares: float | None = None
    mean_volume_5d_to_free_float_shares: float | None = None
    ma10: float | None = None
    ma20: float | None = None
    ma40: float | None = None
    ma10_qfq: float | None = None
    ma20_qfq: float | None = None
    ma40_qfq: float | None = None
    is_st: bool = False
    is_suspended: bool = False
    upper_limit: float | None = None
    lower_limit: float | None = None
    price_band_state: str = ""
    financial_formula_version: str = ""
    financial_announcement_date: str = ""
    financial_report_period: str = ""
    gross_profit_ttm_yoy_pct: float | None = None
    total_operating_revenue_ttm_yoy_pct: float | None = None
    gross_profit_ttm_yoy_3y_avg_pct: float | None = None
    total_operating_revenue_ttm_yoy_3y_avg_pct: float | None = None
    ebit_per_share_latest_quarter_cny: float | None = None
    attributable_net_profit_per_latest_share_cny: float | None = None
    attributable_net_profit_per_latest_share_qoq_pct: float | None = None
    deducted_net_profit_ttm_cny: float | None = None
    deducted_net_profit_ttm_qoq_pct: float | None = None
    ebit_ps_lf_consec_min_3q: float | None = None
    basic_eps_latest_capital_lf_qoq_consec_min_3q: float | None = None
    net_profit_deducted_ttm_qoq_consec_min_3q: float | None = None


class FutureBar1mItem(ApiModel):
    product_code: str
    exchange: str
    series_type: str
    bar_time: str
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: float | None = None
    open_interest: float | None = None
    adjustment_offset: float | None = None


class FutureSeriesCoverageItem(ApiModel):
    product_code: str
    exchange: str
    series_type: str
    row_count: int
    first_bar_time: str = ""
    last_bar_time: str = ""


class FutureRealtimeQuoteItem(ApiModel):
    product_code: str
    exchange: str
    provider_symbol: str
    contract_symbol: str = ""
    quote_time: str = ""
    last_price: float | None = None
    open: float | None = None
    high: float | None = None
    low: float | None = None
    pre_close: float | None = None
    volume: float | None = None
    open_interest: float | None = None
    bid_price1: float | None = None
    ask_price1: float | None = None
    trading_status: str = ""


class FutureContractCatalogItem(ApiModel):
    provider_symbol: str = Field(description="TqSdk 原始合约标识；不可按交易所代码自行拼接。", examples=["SHFE.rb2610"])
    contract_symbol: str = Field(default="", description="交易所交割合约代码；provider 未提供时为空。", examples=["SHFE.rb2610"])
    product_code: str = Field(default="", description="QuoteMux 品种代码；仅在可识别时填写。", examples=["rb"])
    exchange: str = Field(default="", description="QuoteMux 交易所代码；仅在可识别时填写。", examples=["SHFE"])
    name: str = Field(default="", description="TqSdk 合约名称。", examples=["螺纹钢2610"])
    ins_class: str = Field(default="", description="TqSdk instrument class 原始分类。", examples=["FUTURE"])
    underlying_symbol: str = Field(default="", description="TqSdk 标的合约标识。", examples=["SHFE.rb2610"])
    expired: bool | None = Field(default=None, description="TqSdk 是否标记为到期；未知为 null。", examples=[False])
    expire_datetime: str = Field(default="", description="TqSdk 到期时间，Asia/Shanghai 格式；未提供为空。", examples=["2026-10-15 15:00:00"])
    last_exercise_datetime: str = Field(default="", description="TqSdk 最后行权时间，Asia/Shanghai 格式；未提供为空。", examples=[""])
    delivery_year: int | None = Field(default=None, description="交割年份；非交割合约或未知为 null。", examples=[2026])
    delivery_month: int | None = Field(default=None, description="交割月份；非交割合约或未知为 null。", examples=[10])
    volume_multiple: float | None = Field(default=None, description="每手合约乘数。", examples=[10.0])
    price_tick: float | None = Field(default=None, description="最小变动价位。", examples=[1.0])
    price_decs: int | None = Field(default=None, description="报价小数位数。", examples=[0])
    max_limit_order_volume: int | None = Field(default=None, description="限价单最大下单量；未知为 null。", examples=[500])
    max_market_order_volume: int | None = Field(default=None, description="市价单最大下单量；未知为 null。", examples=[500])
    min_limit_order_volume: int | None = Field(default=None, description="限价单最小下单量；未知为 null。", examples=[1])
    min_market_order_volume: int | None = Field(default=None, description="市价单最小下单量；未知为 null。", examples=[1])
    metadata_time: str = Field(default="", description="本条 TqSdk 元数据的提供或采集时间，Asia/Shanghai；未知为空。", examples=["2026-08-20 09:31:00"])
    raw_metadata: dict[str, object] = Field(default_factory=dict, description="未进入稳定字段的 TqSdk 原始 metadata，保持 provider 口径。", examples=[{"exchange_id": "SHFE"}])


class FutureMainContractMappingItem(ApiModel):
    product_code: str = Field(description="QuoteMux 主力映射请求的品种代码。", examples=["rb"])
    exchange: str = Field(description="品种所属交易所代码。", examples=["SHFE"])
    provider_symbol: str = Field(description="TqSdk 主力连续订阅标识；不可按规则自行生成。", examples=["KQ.m@SHFE.rb"])
    contract_symbol: str = Field(default="", description="当前映射到的具体交割合约；无映射时为空。", examples=["SHFE.rb2610"])
    updated_time: str = Field(default="", description="provider 返回或读取映射的时间，Asia/Shanghai；未知为空。", examples=["2026-08-20 09:31:00"])


class FutureContractRealtimeQuoteItem(ApiModel):
    provider_symbol: str = Field(description="请求的 TqSdk 精确交割合约标识。", examples=["SHFE.rb2610"])
    contract_symbol: str = Field(description="provider 确认的精确交割合约标识。", examples=["SHFE.rb2610"])
    product_code: str = Field(default="", description="QuoteMux 品种代码；无法识别时为空。", examples=["rb"])
    exchange: str = Field(default="", description="QuoteMux 交易所代码；无法识别时为空。", examples=["SHFE"])
    quote_time: str = Field(default="", description="行情时间，Asia/Shanghai；交易所未给时间时为空。", examples=["2026-08-20 09:31:00"])
    last_price: float | None = Field(default=None, description="最新价。", examples=[3400.0])
    open: float | None = Field(default=None, description="今开价。", examples=[3380.0])
    high: float | None = Field(default=None, description="最高价。", examples=[3420.0])
    low: float | None = Field(default=None, description="最低价。", examples=[3375.0])
    pre_close: float | None = Field(default=None, description="昨收价。", examples=[3388.0])
    pre_settlement: float | None = Field(default=None, description="昨结算价。", examples=[3390.0])
    settlement: float | None = Field(default=None, description="今结算价；盘中未提供时为 null。", examples=[3402.0])
    average: float | None = Field(default=None, description="成交均价。", examples=[3398.0])
    volume: float | None = Field(default=None, description="累计成交量，单位为手。", examples=[12345.0])
    amount: float | None = Field(default=None, description="累计成交额，货币单位遵循交易所口径。", examples=[419000000.0])
    open_interest: float | None = Field(default=None, description="持仓量，单位为手。", examples=[1800000.0])
    bid_price1: float | None = Field(default=None, description="买一价。", examples=[3399.0])
    bid_price2: float | None = Field(default=None, description="买二价。", examples=[3398.0])
    bid_price3: float | None = Field(default=None, description="买三价。", examples=[3397.0])
    bid_price4: float | None = Field(default=None, description="买四价。", examples=[3396.0])
    bid_price5: float | None = Field(default=None, description="买五价。", examples=[3395.0])
    ask_price1: float | None = Field(default=None, description="卖一价。", examples=[3400.0])
    ask_price2: float | None = Field(default=None, description="卖二价。", examples=[3401.0])
    ask_price3: float | None = Field(default=None, description="卖三价。", examples=[3402.0])
    ask_price4: float | None = Field(default=None, description="卖四价。", examples=[3403.0])
    ask_price5: float | None = Field(default=None, description="卖五价。", examples=[3404.0])
    bid_volume1: float | None = Field(default=None, description="买一量，单位为手。", examples=[10.0])
    bid_volume2: float | None = Field(default=None, description="买二量，单位为手。", examples=[8.0])
    bid_volume3: float | None = Field(default=None, description="买三量，单位为手。", examples=[6.0])
    bid_volume4: float | None = Field(default=None, description="买四量，单位为手。", examples=[4.0])
    bid_volume5: float | None = Field(default=None, description="买五量，单位为手。", examples=[2.0])
    ask_volume1: float | None = Field(default=None, description="卖一量，单位为手。", examples=[10.0])
    ask_volume2: float | None = Field(default=None, description="卖二量，单位为手。", examples=[8.0])
    ask_volume3: float | None = Field(default=None, description="卖三量，单位为手。", examples=[6.0])
    ask_volume4: float | None = Field(default=None, description="卖四量，单位为手。", examples=[4.0])
    ask_volume5: float | None = Field(default=None, description="卖五量，单位为手。", examples=[2.0])
    upper_limit: float | None = Field(default=None, description="涨停价；CFFEX 等 provider 未提供时为 null。", examples=[3729.0])
    lower_limit: float | None = Field(default=None, description="跌停价；CFFEX 等 provider 未提供时为 null。", examples=[3051.0])
    trading_status: str = Field(default="", description="TqSdk 原始交易状态。", examples=["TRADING"])
    expired: bool | None = Field(default=None, description="TqSdk 是否标记为到期；未知为 null。", examples=[False])


class StockFinancialPitRawItem(ApiModel):
    code: str
    report_period: str
    announcement_date: str
    actual_announcement_date: str = ""
    report_type: str = "1"
    company_type: str = ""
    update_flag: str = ""
    gross_profit_cumulative_cny: float | None = None
    total_operating_revenue_cumulative_cny: float | None = None
    ebit_cumulative_cny: float | None = None
    attributable_net_profit_cumulative_cny: float | None = None
    basic_eps_cumulative_cny_per_share: float | None = None
    deducted_net_profit_single_quarter_cny: float | None = None


class StockFinancialPitFactorItem(ApiModel):
    code: str
    announcement_date: str
    report_period: str
    formula_version: str
    revision_source: str
    gross_profit_ttm_cny: float | None = None
    gross_profit_ttm_yoy_pct: float | None = None
    total_operating_revenue_ttm_cny: float | None = None
    total_operating_revenue_ttm_yoy_pct: float | None = None
    ebit_latest_quarter_cny: float | None = None
    total_shares_latest: float | None = None
    ebit_per_share_latest_quarter_cny: float | None = None
    attributable_net_profit_per_latest_share_cny: float | None = None
    attributable_net_profit_per_latest_share_qoq_pct: float | None = None
    deducted_net_profit_ttm_cny: float | None = None
    deducted_net_profit_ttm_qoq_pct: float | None = None
    ebit_ps_lf_consec_min_3q: float | None = None
    basic_eps_latest_capital_lf_qoq_consec_min_3q: float | None = None
    net_profit_deducted_ttm_qoq_consec_min_3q: float | None = None


class LimitOrderAmountItem(ApiModel):
    code: str
    trade_date: str
    limit_side: str
    market: str
    close: float | None = None
    limit_price: float | None = None
    order_price: float | None = None
    order_volume: float | None = None
    order_amount: float | None = None
    captured_at: str


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
    announce_date: str = ""
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


class CorporateActionCoverage(ApiModel):
    data_version: str = ""
    complete: bool
    missing_date_ranges: list[str]


class DividendPage(ApiModel):
    """按公告日筛选的分红事件分页结果。"""

    items: list[DividendItem]
    total: int
    offset: int
    limit: int
    coverage: CorporateActionCoverage


class RightsIssuePage(ApiModel):
    """按公告日筛选的配股事件分页结果。"""

    items: list[RightsIssueItem]
    total: int
    offset: int
    limit: int
    coverage: CorporateActionCoverage


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

