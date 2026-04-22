from __future__ import annotations

from quotemux.boards import QuoteMuxBoards
from quotemux.indexes import QuoteMuxIndexes
from quotemux.markets import QuoteMuxMarkets
from quotemux.news import QuoteMuxNews
from quotemux.rankings import QuoteMuxRankings
from quotemux.settings import QuoteMuxSettings
from quotemux.stocks import QuoteMuxStocks
from quotemux.updater import QuoteMuxUpdater


class QuoteMux:
    def __init__(self, settings: QuoteMuxSettings | None = None) -> None:
        self.settings = settings or QuoteMuxSettings()
        self.stocks = QuoteMuxStocks(self.settings)
        self.indexes = QuoteMuxIndexes(self.settings)
        self.markets = QuoteMuxMarkets(self.settings)
        self.boards = QuoteMuxBoards(self.settings)
        self.news = QuoteMuxNews(self.settings)
        self.rankings = QuoteMuxRankings(self.settings)
        self.updater = QuoteMuxUpdater(self.settings)

    @classmethod
    def from_settings(cls, settings: QuoteMuxSettings | None = None) -> QuoteMux:
        return cls(settings=settings)
