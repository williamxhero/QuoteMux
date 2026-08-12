from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Annotated, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from platform_models.provider_contracts import AuditedPage, AuditedRecord


class MigrationContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _RequestBase(MigrationContractModel):
    capability_id: str
    provider: str = Field(min_length=1)
    provider_instance_id: str = Field(min_length=1)
    cursor: str
    data_version: str
    source_version: str


class _StockRequestBase(_RequestBase):
    code: str
    market: Literal["SH", "SZ", "BJ"]

    @field_validator("code")
    @classmethod
    def _validate_code(cls, value: str) -> str:
        if len(value) != 6 or not value.isdigit():
            raise ValueError("code 必须是 6 位数字")
        return value


class _FinanceEventRequest(_StockRequestBase):
    provider: Literal["eastmoney_official"]
    range_start: str
    range_end: str

    @model_validator(mode="after")
    def _validate_range(self) -> _FinanceEventRequest:
        try:
            start = date.fromisoformat(self.range_start)
            end = date.fromisoformat(self.range_end)
        except ValueError as exc:
            raise ValueError("range_start/range_end 必须是 YYYY-MM-DD") from exc
        if end < start:
            raise ValueError("range_end 不得早于 range_start")
        if (end - start).days > 366 * 40:
            raise ValueError("财务事件报告期范围不得超过 40 年")
        return self


class ForecastEventsRequest(_FinanceEventRequest):
    capability_id: Literal["stocks.finance.forecasts"]
    data_version: Literal["quotemux.stocks.finance.forecasts.v2"]
    source_version: Literal["eastmoney.datacenter.financial_forecast.v1"]


class ExpressEventsRequest(_FinanceEventRequest):
    capability_id: Literal["stocks.finance.express"]
    data_version: Literal["quotemux.stocks.finance.express.v2"]
    source_version: Literal["eastmoney.datacenter.financial_express.v1"]


class EtfProfileRequest(_StockRequestBase):
    capability_id: Literal["funds.etf.profile"]
    provider: Literal["eastmoney_official"]
    market: Literal["SH", "SZ"]
    cursor: Literal[""]
    data_version: Literal["quotemux.funds.etf.profile.v1"]
    source_version: Literal["eastmoney.fundf10.profile.v1"]


class IndexMembersAuditRequest(_RequestBase):
    capability_id: Literal["indexes.members"]
    provider: Literal["tushare"]
    index_code: str = Field(min_length=1)
    query_mode: Literal["current", "history"]
    as_of_date: str
    range_start: str
    range_end: str
    data_version: Literal["quotemux.indexes.members.v2"]
    source_version: Literal["tushare.index_weight.v1"]

    @model_validator(mode="after")
    def _validate_dates(self) -> IndexMembersAuditRequest:
        try:
            as_of = date.fromisoformat(self.as_of_date)
            start = date.fromisoformat(self.range_start)
            end = date.fromisoformat(self.range_end)
        except ValueError as exc:
            raise ValueError("指数日期必须是 YYYY-MM-DD") from exc
        if end < start:
            raise ValueError("range_end 不得早于 range_start")
        if self.query_mode == "current" and not (start == end == as_of):
            raise ValueError("current 查询必须锁定单一 as_of_date")
        if self.query_mode == "history" and as_of != end:
            raise ValueError("history 查询的 as_of_date 必须等于 range_end")
        if (end - start).days > 366:
            raise ValueError("指数历史单次查询范围不得超过 366 天")
        return self


MigrationRequest: TypeAlias = Annotated[
    ForecastEventsRequest
    | ExpressEventsRequest
    | EtfProfileRequest
    | IndexMembersAuditRequest,
    Field(discriminator="capability_id"),
]


class FinancialEventData(MigrationContractModel):
    code: str
    market: Literal["SH", "SZ", "BJ"]
    security_code: str
    report_period: str
    notice_date: str
    notice_time: str
    event_type: Literal["forecast", "forecast_revision", "express", "express_revision"]
    event_subtype: str
    is_revision: bool
    notice_title: str
    notice_url: str
    notice_summary: str
    forecast_metric_code: str
    forecast_metric_name: str
    forecast_summary: str
    forecast_direction: str
    forecast_amount_lower: Decimal | None
    forecast_amount_upper: Decimal | None
    forecast_yoy_lower: Decimal | None
    forecast_yoy_upper: Decimal | None
    net_profit_lower: Decimal | None
    net_profit_upper: Decimal | None
    net_profit_yoy_lower: Decimal | None
    net_profit_yoy_upper: Decimal | None
    net_profit_excl_nonrecurring_lower: Decimal | None
    net_profit_excl_nonrecurring_upper: Decimal | None
    net_profit_excl_nonrecurring_yoy_lower: Decimal | None
    net_profit_excl_nonrecurring_yoy_upper: Decimal | None
    operating_revenue_lower: Decimal | None
    operating_revenue_upper: Decimal | None
    operating_revenue_yoy_lower: Decimal | None
    operating_revenue_yoy_upper: Decimal | None
    forecast_amount_unit: str
    operating_revenue: Decimal | None
    operating_revenue_yoy: Decimal | None
    net_profit: Decimal | None
    net_profit_parent: Decimal | None
    net_profit_yoy: Decimal | None
    basic_eps: Decimal | None
    bps: Decimal | None
    roe: Decimal | None
    data_quality_flags: list[str]


class EtfProfileData(MigrationContractModel):
    code: str
    market: Literal["SH", "SZ"]
    security_code: str
    name: str
    full_name: str
    fund_type: str
    found_date: str
    listing_date: str
    field_quality_flags: list[str]


class IndexMemberAuditData(MigrationContractModel):
    index_code: str
    code: str
    as_of_date: str
    weight: Decimal = Field(max_digits=20, decimal_places=8)
    weight_unit: Literal["percent"]


MigrationData: TypeAlias = FinancialEventData | EtfProfileData | IndexMemberAuditData
MigrationRecord = AuditedRecord
MigrationPage = AuditedPage
