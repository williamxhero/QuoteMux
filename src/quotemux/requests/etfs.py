from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


def normalize_etf_ts_code(value: object) -> str:
    text = str(value or "").strip().upper()
    if len(text) != 9 or text[6] != "." or not text[:6].isdigit() or text[7:] not in {"SH", "SZ"}:
        return ""
    return text


class EtfDailyQuotesRequest(BaseModel):
    ts_codes: list[str] = Field(default_factory=list)
    trade_date: str = ""
    start_date: str = ""
    end_date: str = ""
    limit: int | None = None
    meta_detail: str = "summary"

    @field_validator("ts_codes", mode="before")
    @classmethod
    def _normalize_ts_codes(cls, value: object) -> list[str]:
        if value is None:
            return []
        items = value if isinstance(value, list) else [value]
        normalized = [normalize_etf_ts_code(item) for item in items]
        return list(dict.fromkeys(item for item in normalized if item != ""))
