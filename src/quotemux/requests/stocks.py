from __future__ import annotations

from pydantic import BaseModel, Field, field_validator

from quotemux.infra.common import normalize_stock_code


class StockQuotesRequest(BaseModel):
    codes: list[str] = Field(default_factory=list)
    freq: str = "1d"
    trade_date: str = ""
    start_date: str = ""
    end_date: str = ""
    start_time: str = ""
    end_time: str = ""
    count: int | None = None
    adjust: str = "none"
    limit: int | None = None
    skip_suspended: bool = True
    skip_st: bool = False
    fill_missing: bool = False

    @field_validator("codes", mode="before")
    @classmethod
    def _normalize_codes(cls, value: object) -> list[str]:
        if value is None:
            return []
        items = value if isinstance(value, list) else [value]
        normalized: list[str] = []
        for item in items:
            code = normalize_stock_code(str(item))
            if code:
                normalized.append(code)
        return list(dict.fromkeys(normalized))


class StockDailySnapshotRequest(BaseModel):
    trade_date: str
    limit: int = 200
    offset: int = 0
    skip_suspended: bool = True
    skip_st: bool = False


class StockDailyLocalWindowRequest(BaseModel):
    start_date: str
    end_date: str
    limit: int = 50000
    offset: int = 0
    skip_suspended: bool = True
    skip_st: bool = False

