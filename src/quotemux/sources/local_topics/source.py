from __future__ import annotations

from platform_models import TradingSessionItem
from quotemux.infra.common import normalize_stock_code


def get_market_sessions(codes: str) -> list[TradingSessionItem]:
    items: list[TradingSessionItem] = []
    for code in [normalize_stock_code(item) for item in codes.split(",") if item.strip()]:
        items.append(TradingSessionItem(code=code, session_name="pre_open", start_time="09:15:00", end_time="09:25:00", timezone="Asia/Shanghai"))
        items.append(TradingSessionItem(code=code, session_name="continuous", start_time="09:30:00", end_time="11:30:00", timezone="Asia/Shanghai"))
        items.append(TradingSessionItem(code=code, session_name="continuous", start_time="13:00:00", end_time="14:57:00", timezone="Asia/Shanghai"))
        items.append(TradingSessionItem(code=code, session_name="closing_call", start_time="14:57:00", end_time="15:00:00", timezone="Asia/Shanghai"))
        items.append(TradingSessionItem(code=code, session_name="after_hours", start_time="15:00:00", end_time="15:30:00", timezone="Asia/Shanghai"))
    return items


