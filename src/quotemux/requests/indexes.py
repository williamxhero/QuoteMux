from __future__ import annotations

from pydantic import BaseModel, Field, field_validator

from quotemux.infra.common import normalize_index_code


class IndexQuotesRequest(BaseModel):
    index_codes: list[str] = Field(default_factory=list)
    freq: str = "1d"
    trade_date: str = ""
    start_date: str = ""
    end_date: str = ""
    count: int | None = None
    limit: int = 200

    @field_validator("index_codes", mode="before")
    @classmethod
    def _normalize_codes(cls, value: object) -> list[str]:
        if value is None:
            return []
        items = value if isinstance(value, list) else [value]
        normalized: list[str] = []
        for item in items:
            code = normalize_index_code(str(item))
            if code:
                normalized.append(code)
        return list(dict.fromkeys(normalized))


class IndexMembersRequest(BaseModel):
    index_code: str
    trade_date: str = ""

    @field_validator("index_code", mode="before")
    @classmethod
    def _normalize_index_code(cls, value: object) -> str:
        return normalize_index_code(str(value or ""))

