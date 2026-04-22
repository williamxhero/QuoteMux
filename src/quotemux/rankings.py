from __future__ import annotations

from platform_models import RankingBrokerPickItem, RankingResearchReportItem
from quotemux.common import ensure_limit
from quotemux.runtime_core.registry import SourceProxy
from quotemux.settings import QuoteMuxSettings


tushare_stocks = SourceProxy("tushare_stocks")


class QuoteMuxRankings:
    def __init__(self, settings: QuoteMuxSettings) -> None:
        self._settings = settings

    def get_research_reports(self, trade_date: str, start_date: str, end_date: str, limit: int) -> list[RankingResearchReportItem]:
        if not self._settings.is_source_enabled("tushare_stocks"):
            return []
        return tushare_stocks.get_rank_research_reports(trade_date, start_date, end_date, ensure_limit(limit))

    def get_broker_monthly_picks(self, trade_month: str, limit: int) -> list[RankingBrokerPickItem]:
        if not self._settings.is_source_enabled("tushare_stocks"):
            return []
        return tushare_stocks.get_rank_broker_monthly_picks(trade_month, ensure_limit(limit))
