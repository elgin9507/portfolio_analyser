import logging
from decimal import Decimal
from typing import Iterator

import pandas as pd

from analyser.enums import PortfolioDataHeader
from analyser.errors import PortfolioErrorCalculation
from analyser.interfaces import PortfolioError
from analyser.reconcilers.base import PortfolioDataReconcilerBase

logger = logging.getLogger(__name__)


class PortfolioDataReconcilerTradeDayMove(PortfolioDataReconcilerBase):
    """Reconcile portfolio data for trade day move."""

    ERROR_TOLERANCE = Decimal("0.01")

    def __init__(self):
        super().__init__()

        self._diff_column = self._difference_column_name_factory(
            PortfolioDataHeader.TRADE_DAY_MOVE.value
        )
        self._recalc_column = self._recalculate_column_name_factory(
            PortfolioDataHeader.TRADE_DAY_MOVE.value
        )

    def reconcile(self, data: pd.DataFrame) -> Iterator[PortfolioError]:
        """Reconcile the data for trade day move."""

        logger.info("Reconciling 'trade day move'")
        self.recalculate(data)

        data[self._diff_column] = (
            data[self._recalc_column] - data[PortfolioDataHeader.TRADE_DAY_MOVE.value]
        )
        # find the rows where the difference is greater than the tolerance
        mask = data[self._diff_column].abs() > self.ERROR_TOLERANCE
        for _, row in data[mask].iterrows():
            error = PortfolioErrorCalculation(
                ticker=row[PortfolioDataHeader.P_TICKER.value],
                location=PortfolioDataHeader.TRADE_DAY_MOVE.value,
                date=row[PortfolioDataHeader.DATE.value].date(),
                value=row[PortfolioDataHeader.TRADE_DAY_MOVE.value],
                correct_value=row[self._recalc_column],
            )
            yield error

        logger.info("'trade day move' reconciled")

    def recalculate(self, data):
        """Recalculate the data for trade day move."""

        if self._recalc_column in data:
            return

        logger.debug("Recalculating 'trade day move'")
        data[self._recalc_column] = (
            data[PortfolioDataHeader.PRICE.value]
            - data[PortfolioDataHeader.TRADE_PRICE.value]
        ) / data[PortfolioDataHeader.TRADE_PRICE.value]
        logger.debug("Recalculated 'trade day move'")
