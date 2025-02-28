import logging
from decimal import Decimal
from typing import Iterator

import pandas as pd

from analyser.enums import PortfolioDataHeader
from analyser.errors import PortfolioErrorCalculation
from analyser.interfaces import PortfolioError
from analyser.reconcilers.base import PortfolioDataReconcilerBase

logger = logging.getLogger(__name__)


class PortfolioDataReconcilerMarketCap(PortfolioDataReconcilerBase):
    """Reconcile portfolio data for market cap."""

    ERROR_TOLERANCE = Decimal("1.0")

    def __init__(self):
        super().__init__()

        self._diff_column = self._difference_column_name_factory(
            PortfolioDataHeader.MARKET_CAP.value
        )
        self._recalc_column = self._recalculate_column_name_factory(
            PortfolioDataHeader.MARKET_CAP.value
        )

    def reconcile(self, data: pd.DataFrame) -> Iterator[PortfolioError]:
        """Reconcile the data for market cap."""

        logger.info("Reconciling 'market cap'")
        self.recalculate(data)

        data[self._diff_column] = (
            data[self._recalc_column] - data[PortfolioDataHeader.MARKET_CAP.value]
        )
        # find the rows where the difference is greater than the tolerance
        mask = data[self._diff_column].abs() > self.ERROR_TOLERANCE
        for _, row in data[mask].iterrows():
            error = PortfolioErrorCalculation(
                ticker=row[PortfolioDataHeader.P_TICKER.value],
                location=PortfolioDataHeader.MARKET_CAP.value,
                date=row[PortfolioDataHeader.DATE.value].date(),
                value=row[PortfolioDataHeader.MARKET_CAP.value],
                correct_value=row[self._recalc_column],
            )
            yield error

        logger.info("'market cap' reconciled")

    def recalculate(self, data):
        """Recalculate the data for market cap."""

        if self._recalc_column in data:
            return

        logger.debug("Recalculating 'market cap'")
        data[self._recalc_column] = (
            data[PortfolioDataHeader.SHARESOUT.value]
            * data[PortfolioDataHeader.PRICE.value]
            * data[PortfolioDataHeader.EXCHANGE_RATE.value]
        )
        logger.debug("Recalculated 'market cap'")
