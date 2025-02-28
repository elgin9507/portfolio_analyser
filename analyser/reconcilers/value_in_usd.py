import logging
from decimal import Decimal
from typing import Iterator

import pandas as pd

from analyser.enums import PortfolioDataHeader
from analyser.errors import PortfolioErrorCalculation
from analyser.interfaces import PortfolioError
from analyser.reconcilers.base import PortfolioDataReconcilerBase

logger = logging.getLogger(__name__)


class PortfolioDataReconcilerValueInUSD(PortfolioDataReconcilerBase):
    """Reconcile portfolio data for value in USD."""

    def __init__(self):
        super().__init__()

        self._diff_column = self._difference_column_name_factory(
            PortfolioDataHeader.VALUE_IN_USD.value
        )
        self._recalc_column = self._recalculate_column_name_factory(
            PortfolioDataHeader.VALUE_IN_USD.value
        )

    ERROR_TOLERANCE = Decimal("1.00")

    def reconcile(self, data: pd.DataFrame) -> Iterator[PortfolioError]:
        """Reconcile the data for value in USD."""

        logger.info("Reconciling value in USD")
        self.recalculate(data)

        data[self._diff_column] = (
            data[self._recalc_column] - data[PortfolioDataHeader.VALUE_IN_USD.value]
        )
        # find the rows where the difference is greater than the tolerance
        mask = data[self._diff_column].abs() > self.ERROR_TOLERANCE
        for _, row in data[mask].iterrows():
            error = PortfolioErrorCalculation(
                ticker=row[PortfolioDataHeader.P_TICKER.value],
                location=PortfolioDataHeader.VALUE_IN_USD.value,
                date=row[PortfolioDataHeader.DATE.value].date(),
                value=row[PortfolioDataHeader.VALUE_IN_USD.value],
                correct_value=row[self._recalc_column],
            )
            yield error

        logger.info("Value in USD reconciled")

    def recalculate(self, data: pd.DataFrame) -> None:
        """Recalculate the data for value in USD."""

        if self._recalc_column in data:
            return

        logger.debug("Recalculating value in USD")
        data[self._recalc_column] = (
            data[PortfolioDataHeader.CLOSE_QUANTITY.value]
            * data[PortfolioDataHeader.EXCHANGE_RATE.value]
            * data[PortfolioDataHeader.PRICE.value]
        )
        logger.debug("Value in USD recalculated")
