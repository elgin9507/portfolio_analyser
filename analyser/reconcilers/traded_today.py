import logging
from decimal import Decimal
from typing import Iterator

import pandas as pd

from analyser.enums import PortfolioDataHeader
from analyser.errors import PortfolioErrorCalculation
from analyser.interfaces import PortfolioError
from analyser.reconcilers.base import PortfolioDataReconcilerBase

logger = logging.getLogger(__name__)


class PortfolioDataReconcilerTradedToday(PortfolioDataReconcilerBase):
    """Reconcile portfolio data for traded today."""

    ERROR_TOLERANCE = Decimal("0.0")

    def __init__(self):
        super().__init__()

        self._diff_column = self._difference_column_name_factory(
            PortfolioDataHeader.TRADED_TODAY.value
        )
        self._recalc_column = self._recalculate_column_name_factory(
            PortfolioDataHeader.TRADED_TODAY.value
        )

    def reconcile(self, data: pd.DataFrame) -> Iterator[PortfolioError]:
        """Reconcile the data for traded today."""

        logger.info("Reconciling 'traded today'")
        self.recalculate(data)

        data[self._diff_column] = (
            data[self._recalc_column] - data[PortfolioDataHeader.TRADED_TODAY.value]
        )
        # find the rows where the difference is greater than the tolerance
        mask = data[self._diff_column].abs() > self.ERROR_TOLERANCE
        for _, row in data[mask].iterrows():
            error = PortfolioErrorCalculation(
                ticker=row[PortfolioDataHeader.P_TICKER.value],
                location=PortfolioDataHeader.TRADED_TODAY.value,
                date=row[PortfolioDataHeader.DATE.value].date(),
                value=row[PortfolioDataHeader.TRADED_TODAY.value],
                correct_value=row[self._recalc_column],
            )
            yield error

        logger.info("'traded today' reconciled")

    def recalculate(self, data):
        """Recalculate the data for traded today."""

        if self._recalc_column in data:
            return

        logger.debug("Recalculating 'traded today'")
        data[self._recalc_column] = (
            data[PortfolioDataHeader.CLOSE_QUANTITY.value]
            - data[PortfolioDataHeader.OPEN_QUANTITY.value]
        )
        logger.debug("'traded today' recalculated")
