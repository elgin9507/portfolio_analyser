import logging
from decimal import Decimal
from typing import Iterator

import pandas as pd

from analyser.enums import PortfolioDataHeader
from analyser.errors import PortfolioErrorCalculation
from analyser.interfaces import PortfolioError
from analyser.reconcilers.base import PortfolioDataReconcilerBase

logger = logging.getLogger(__name__)


class PortfolioDataReconcilerClosingWeight(PortfolioDataReconcilerBase):
    """Reconcile portfolio data for closing weight."""

    ERROR_TOLERANCE = Decimal("0.01")

    def __init__(self):
        super().__init__()

        self._diff_column = self._difference_column_name_factory(
            PortfolioDataHeader.CLOSING_WEIGHTS.value
        )
        self._recalc_column = self._recalculate_column_name_factory(
            PortfolioDataHeader.CLOSING_WEIGHTS.value
        )

    def reconcile(self, data: pd.DataFrame) -> Iterator[PortfolioError]:
        """Reconcile the closing weights calculation."""

        logger.info("Reconciling closing weights")
        self.recalculate(data)

        data[self._diff_column] = (
            data[self._recalc_column] - data[PortfolioDataHeader.CLOSING_WEIGHTS.value]
        )
        # find the rows where the difference is greater than the tolerance
        mask = data[self._diff_column].abs() > self.ERROR_TOLERANCE
        for _, row in data[mask].iterrows():
            error = PortfolioErrorCalculation(
                ticker=row[PortfolioDataHeader.P_TICKER.value],
                location=PortfolioDataHeader.CLOSING_WEIGHTS.value,
                date=row[PortfolioDataHeader.DATE.value].date(),
                value=row[PortfolioDataHeader.CLOSING_WEIGHTS.value],
                correct_value=row[self._recalc_column],
            )
            yield error

        logger.info("Closing weights reconciled")

    def recalculate(self, data: pd.DataFrame):
        """Recalculate the closing weights."""

        if self._recalc_column in data:
            return

        logger.debug("Recalculating closing weights")
        data[self._recalc_column] = (
            data[PortfolioDataHeader.CLOSE_QUANTITY.value]
            * data[PortfolioDataHeader.EXCHANGE_RATE.value]
            * data[PortfolioDataHeader.PRICE.value]
            / data[PortfolioDataHeader.CALCULATED_NAV.value]
        )
        logger.debug("Closing weights recalculated")
