from __future__ import annotations

from datetime import date

from pydantic import BaseModel, field_validator

from quotemux.infra.common import normalize_index_code, normalize_stock_code


class StockBar1mRequest(BaseModel):
    code: str
    start_date: date
    end_date: date

    @field_validator("code", mode="before")
    @classmethod
    def _normalize_code(cls, value: object) -> str:
        return normalize_stock_code(str(value or ""))


class IndexBar1dRequest(BaseModel):
    index_code: str
    start_date: date
    end_date: date

    @field_validator("index_code", mode="before")
    @classmethod
    def _normalize_index_code(cls, value: object) -> str:
        return normalize_index_code(str(value or ""))


class StockDailyOhlcvaRepairRequest(BaseModel):
    trade_date: date

