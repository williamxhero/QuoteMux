from __future__ import annotations

from pydantic import BaseModel


class TradingCalendarRequest(BaseModel):
    exchange: str = "SSE"
    start_date: str = ""
    end_date: str = ""
    is_open: bool | None = None


class PreviousTradingDaysRequest(BaseModel):
    exchange: str = "SSE"
    trade_date: str
    n: int


class NextTradingDaysRequest(BaseModel):
    exchange: str = "SSE"
    trade_date: str
    n: int


class YearlyTradingCalendarRequest(BaseModel):
    exchange: str = "SSE"
    start_year: int
    end_year: int
