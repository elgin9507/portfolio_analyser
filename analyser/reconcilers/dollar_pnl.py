import logging
from decimal import Decimal
from typing import Iterator

import pandas as pd

from analyser.enums import PortfolioDataHeader
from analyser.errors import PortfolioErrorCalculation
from analyser.interfaces import PortfolioError
from analyser.reconcilers.base import PortfolioDataReconcilerBase

logger = logging.getLogger(__name__)


class PortfolioDataReconcilerDollarPnL(PortfolioDataReconcilerBase):
    """Reconcile portfolio data for dollar PnL."""

    ERROR_TOLERANCE = Decimal("0.01")

    def __init__(self):
        super().__init__()

        self._diff_column = self._difference_column_name_factory(
            PortfolioDataHeader.DOLLAR_PNL.value
        )
        self._recalc_column = self._recalculate_column_name_factory(
            PortfolioDataHeader.DOLLAR_PNL.value
        )

    def reconcile(self, data: pd.DataFrame) -> Iterator[PortfolioError]:
        """Reconcile the data for dollar PnL."""

        logger.info("Reconciling 'dollar PnL'")
        self.recalculate(data)

        data[self._diff_column] = (
            data[self._recalc_column] - data[PortfolioDataHeader.DOLLAR_PNL.value]
        )
        # find the rows where the difference is greater than the tolerance
        mask = data[self._diff_column].abs() > self.ERROR_TOLERANCE
        for _, row in data[mask].iterrows():
            error = PortfolioErrorCalculation(
                ticker=row[PortfolioDataHeader.P_TICKER.value],
                location=PortfolioDataHeader.DOLLAR_PNL.value,
                date=row[PortfolioDataHeader.DATE.value].date(),
                value=row[PortfolioDataHeader.DOLLAR_PNL.value],
                correct_value=row[self._recalc_column],
            )
            yield error

        logger.info("'dollar PnL' reconciled")

    def recalculate(self, data):
        """Recalculate the data for dollar PnL."""

        if self._recalc_column in data:
            return

        logger.debug("Recalculating 'dollar PnL'")
        data[self._recalc_column] = (
            data[PortfolioDataHeader.TOTAL_RETURN]
            * data[PortfolioDataHeader.NAV_YESTERDAY.value]
        )
        logger.debug("Recalculated 'dollar PnL'")
