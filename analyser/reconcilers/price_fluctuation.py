import logging
from decimal import Decimal
from typing import Iterator

import pandas as pd

from analyser.enums import PortfolioDataHeader
from analyser.errors import PortfolioErrorHighVolatility
from analyser.interfaces import PortfolioError
from analyser.reconcilers.base import PortfolioDataReconcilerBase

logger = logging.getLogger(__name__)


class PortfolioDataReconcilerPriceFluctuation(PortfolioDataReconcilerBase):
    """Reconcile portfolio data for price fluctuation."""

    ERROR_TOLERANCE = Decimal("0.10")  # detech price changes greater than 10%

    def __init__(self):
        super().__init__()

        self._recalc_column = "price_volatility"

    def reconcile(self, data: pd.DataFrame) -> Iterator[PortfolioError]:
        """Reconcile the data for price fluctuation."""

        logger.info("Reconciling 'price fluctuation'")
        self.recalculate(data)

        mask = data[self._recalc_column].abs() > self.ERROR_TOLERANCE
        for _, row in data[mask].iterrows():
            error = PortfolioErrorHighVolatility(
                ticker=row[PortfolioDataHeader.P_TICKER.value],
                location=self._recalc_column,
                date=row[PortfolioDataHeader.DATE.value].date(),
                value=row[PortfolioDataHeader.PRICE.value],
            )
            yield error

        logger.info("'price fluctuation' reconciled")

    def recalculate(self, data):
        """Recalculate the data for price fluctuation."""

        if self._recalc_column in data:
            return

        logger.debug("Calculating 'price fluctuation'")
        data[self._recalc_column] = (
            data[PortfolioDataHeader.PRICE_YESTERDAY.value]
            / data[PortfolioDataHeader.PRICE.value]
            - 1
        )
        logger.debug("'price fluctuation' calculated")
