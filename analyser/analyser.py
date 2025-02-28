import logging
from typing import Iterator

from analyser import interfaces
from analyser.reconcilers.close_weight_abs import PortfolioDataReconcilerCloseWeightAbs
from analyser.reconcilers.closing_weight import PortfolioDataReconcilerClosingWeight
from analyser.reconcilers.dollar_pnl import PortfolioDataReconcilerDollarPnL
from analyser.reconcilers.market_cap import PortfolioDataReconcilerMarketCap
from analyser.reconcilers.opening_weight import PortfolioDataReconcilerOpeningWeight
from analyser.reconcilers.price_fluctuation import (
    PortfolioDataReconcilerPriceFluctuation,
)
from analyser.reconcilers.return_adjustments import (
    PortfolioDataReconcilerReturnAdjustments,
)
from analyser.reconcilers.total_return import PortfolioDataReconcilerTotalReturn
from analyser.reconcilers.trade_day_move import PortfolioDataReconcilerTradeDayMove
from analyser.reconcilers.trade_weight import PortfolioDataReconcilerTradeWeight
from analyser.reconcilers.traded_today import PortfolioDataReconcilerTradedToday
from analyser.reconcilers.value_in_usd import PortfolioDataReconcilerValueInUSD

logger = logging.getLogger(__name__)


class PortfolioAnalyzer(interfaces.PortfolioAnalyser):
    def __init__(
        self,
        data_feed: interfaces.PortfolioDataFeed,
        error_exporter: interfaces.PortfolioErrorExporter,
    ):
        self.data_feed = data_feed
        self.error_exporter = error_exporter
        self._data_iterator = self.data_feed.get_data()
        self._data_headers = None
        self.reconcilers = [
            PortfolioDataReconcilerOpeningWeight(),
            PortfolioDataReconcilerClosingWeight(),
            PortfolioDataReconcilerValueInUSD(),
            PortfolioDataReconcilerTradedToday(),
            PortfolioDataReconcilerTradeDayMove(),
            PortfolioDataReconcilerTradeWeight(),
            PortfolioDataReconcilerReturnAdjustments(),
            PortfolioDataReconcilerTotalReturn(),
            PortfolioDataReconcilerCloseWeightAbs(),
            PortfolioDataReconcilerDollarPnL(),
            PortfolioDataReconcilerMarketCap(),
            PortfolioDataReconcilerPriceFluctuation(),
        ]

    def analyse(self) -> Iterator[interfaces.PortfolioError]:
        for data_chunk in self.get_data():
            for reconciler in self.reconcilers:
                yield from reconciler.reconcile(data_chunk)

    def export_errors(self, errors: Iterator[interfaces.PortfolioError]) -> None:
        self.error_exporter.export(errors)
