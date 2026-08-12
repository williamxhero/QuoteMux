from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Annotated, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from platform_models.provider_contracts import (
    AuditedPage,
    AuditedRecord,
    canonical_json_sha256,
)


EASTMONEY_OFFICIAL_PROVIDER = "eastmoney_official"
CNINFO_EVIDENCE_PROVIDER = "cninfo_evidence"
ORIGINAL_VALUE_UNIT = "eastmoney_source_original_unscaled"


class P0ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _P0RequestBase(P0ContractModel):
    provider: str = Field(min_length=1)
    code: str
    market: Literal["SH", "SZ", "BJ"]
    cursor: str

    @field_validator("code")
    @classmethod
    def _validate_code(cls, value: str) -> str:
        if len(value) != 6 or not value.isdigit():
            raise ValueError("code 必须是 6 位数字")
        return value


class CompanyP0Request(_P0RequestBase):
    provider: Literal["eastmoney_official"]
    capability_id: Literal["stocks.profile.company"]
    range_start: Literal[""]
    range_end: Literal[""]
    cursor: Literal[""]
    data_version: Literal["quotemux.stocks.profile.company.v1"]
    source_version: Literal["eastmoney.hsf10.company_survey.v1"]


class _RangedP0Request(_P0RequestBase):
    range_start: str
    range_end: str

    @model_validator(mode="after")
    def _validate_range(self) -> _RangedP0Request:
        try:
            start = date.fromisoformat(self.range_start)
            end = date.fromisoformat(self.range_end)
        except ValueError as exc:
            raise ValueError("range_start/range_end 必须是 YYYY-MM-DD") from exc
        if end < start:
            raise ValueError("range_end 不得早于 range_start")
        return self


class CapitalP0Request(_RangedP0Request):
    provider: Literal["eastmoney_official"]
    capability_id: Literal["stocks.corporate_actions.share_changes"]
    data_version: Literal["quotemux.stocks.corporate_actions.share_changes.v1"]
    source_version: Literal["eastmoney.hsf10.capital_structure.v1"]


class ReportDisclosuresP0Request(_P0RequestBase):
    provider: Literal["cninfo_evidence"]
    capability_id: Literal["stocks.finance.report_disclosures"]
    report_period: str
    document_kind: Literal["annual", "quarter1", "semiannual", "quarter3"]
    range_start: str
    range_end: str
    cursor: Literal[""]
    data_version: Literal["quotemux.stocks.finance.report_disclosures.v2"]
    source_version: Literal["cninfo_disclosure/v1"]

    @model_validator(mode="after")
    def _validate_evidence_scope(self) -> ReportDisclosuresP0Request:
        try:
            period = date.fromisoformat(self.report_period)
        except ValueError as exc:
            raise ValueError("report_period 必须是 YYYY-MM-DD") from exc
        expected_suffix = {
            "annual": (12, 31),
            "quarter1": (3, 31),
            "semiannual": (6, 30),
            "quarter3": (9, 30),
        }[self.document_kind]
        if (period.month, period.day) != expected_suffix:
            raise ValueError("report_period 与 document_kind 不匹配")
        if self.range_start != self.report_period or self.range_end != self.report_period:
            raise ValueError("CNInfo evidence 请求必须锁定单一 report_period")
        return self


class LegacyEastmoneyReportDisclosuresRequest(_RangedP0Request):
    """静态迁移对照；不属于正式 P0Request，也不得进入 Provider policy。"""

    provider: Literal["eastmoney_official"]
    capability_id: Literal["stocks.finance.report_disclosures"]
    data_version: Literal["quotemux.stocks.finance.report_disclosures.v1"]
    source_version: Literal["eastmoney.notice.security_ann.v1"]


class StatementsP0Request(_RangedP0Request):
    provider: Literal["eastmoney_official"]
    capability_id: Literal["stocks.finance.statements"]
    statement_type: Literal["balance", "income", "cashflow"]
    data_version: Literal["quotemux.stocks.finance.statements.v1"]
    source_version: Literal["eastmoney.datacenter.financial_statements.v1"]


P0Request: TypeAlias = Annotated[
    CompanyP0Request
    | CapitalP0Request
    | ReportDisclosuresP0Request
    | StatementsP0Request,
    Field(discriminator="capability_id"),
]


class CompanyP0Data(P0ContractModel):
    code: str
    market: Literal["SH", "SZ", "BJ"]
    security_code: str
    company_name: str
    company_full_name: str
    security_type: str
    trade_market: str
    industry_system: Literal["eastmoney_em2016"]
    industry_code: str
    industry_name: str
    industry_path: str
    industry_csrc_path: str
    listing_date: str
    found_date: str


class CapitalP0Data(P0ContractModel):
    code: str
    market: Literal["SH", "SZ", "BJ"]
    security_code: str
    change_date: str
    total_shares: int | None
    unlimited_shares: int | None
    free_shares: int | None
    listed_a_shares: int | None
    limited_shares: int | None
    change_reason: str


class ReportDisclosureP0Data(P0ContractModel):
    code: str
    market: Literal["SH", "SZ", "BJ"]
    security_code: str
    report_period: str
    report_kind: Literal["q1", "h1", "q3", "annual"]
    notice_date: str
    notice_title: str
    article_code: str
    evidence_id: str = Field(min_length=1)
    published_at: str = Field(min_length=1)
    source_url: str = Field(min_length=1)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class LegacyEastmoneyReportDisclosureData(P0ContractModel):
    """旧 Eastmoney ann 投影，仅保留作重复抓取候选的静态对照。"""

    code: str
    market: Literal["SH", "SZ", "BJ"]
    security_code: str
    report_period: str
    report_kind: Literal["q1", "h1", "q3", "annual"]
    notice_date: str
    notice_title: str
    article_code: str


class StatementP0Data(P0ContractModel):
    code: str
    market: Literal["SH", "SZ", "BJ"]
    security_code: str
    statement_type: Literal["balance", "income", "cashflow"]
    report_period: str
    announce_date: str
    unit_identity: Literal["eastmoney_source_original_unscaled"]
    total_assets: Decimal | None
    total_liabilities: Decimal | None
    total_equity: Decimal | None
    cash_and_equivalents: Decimal | None
    accounts_receivable: Decimal | None
    inventory: Decimal | None
    operating_revenue: Decimal | None
    operating_profit: Decimal | None
    total_profit: Decimal | None
    net_profit: Decimal | None
    net_profit_parent: Decimal | None
    basic_eps: Decimal | None
    net_operating_cash_flow: Decimal | None
    net_investing_cash_flow: Decimal | None
    net_financing_cash_flow: Decimal | None
    cash_flow_net_increase: Decimal | None


P0Data: TypeAlias = (
    CompanyP0Data | CapitalP0Data | ReportDisclosureP0Data | StatementP0Data
)
P0Record = AuditedRecord
P0Page = AuditedPage
