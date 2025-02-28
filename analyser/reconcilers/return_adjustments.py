import logging
from decimal import Decimal
from typing import Iterator

import pandas as pd

from analyser.enums import PortfolioDataHeader
from analyser.errors import PortfolioErrorCalculation
from analyser.interfaces import PortfolioError
from analyser.reconcilers.base import PortfolioDataReconcilerBase

logger = logging.getLogger(__name__)


class PortfolioDataReconcilerReturnAdjustments(PortfolioDataReconcilerBase):
    """Reconcile portfolio data for return adjustments."""

    ERROR_TOLERANCE = Decimal("0.01")

    def __init__(self):
        super().__init__()

        self._diff_column = self._difference_column_name_factory(
            PortfolioDataHeader.RETURN_ADJUSTMENTS.value
        )
        self._recalc_column = self._recalculate_column_name_factory(
            PortfolioDataHeader.RETURN_ADJUSTMENTS.value
        )

    def reconcile(self, data: pd.DataFrame) -> Iterator[PortfolioError]:
        """Reconcile the data for return adjustments."""

        logger.info("Reconciling 'return adjustments'")
        self.recalculate(data)

        data[self._diff_column] = (
            data[self._recalc_column]
            - data[PortfolioDataHeader.RETURN_ADJUSTMENTS.value]
        )
        # find the rows where the difference is greater than the tolerance
        mask = data[self._diff_column].abs() > self.ERROR_TOLERANCE
        for _, row in data[mask].iterrows():
            error = PortfolioErrorCalculation(
                ticker=row[PortfolioDataHeader.P_TICKER.value],
                location=PortfolioDataHeader.RETURN_ADJUSTMENTS.value,
                date=row[PortfolioDataHeader.DATE.value].date(),
                value=row[PortfolioDataHeader.RETURN_ADJUSTMENTS.value],
                correct_value=row[self._recalc_column],
            )
            yield error

        logger.info("'return adjustments' reconciled")

    def recalculate(self, data):
        """Recalculate the data for return adjustments."""

        if self._recalc_column in data:
            return

        logger.debug("Recalculating 'return adjustments'")
        short_pos_data = data.loc[data[PortfolioDataHeader.SHORT_POS.value] == True]
        data.loc[
            data[PortfolioDataHeader.SHORT_POS.value] == True, self._recalc_column
        ] = (
            -short_pos_data[PortfolioDataHeader.TRADED_TODAY.value]
            * (
                short_pos_data[PortfolioDataHeader.TRADE_PRICE.value]
                - short_pos_data[PortfolioDataHeader.PRICE.value]
            )
            / short_pos_data[PortfolioDataHeader.CALCULATED_NAV.value]
        )
        long_pos_data = data.loc[data[PortfolioDataHeader.SHORT_POS.value] == False]
        data.loc[
            data[PortfolioDataHeader.SHORT_POS.value] == False, self._recalc_column
        ] = (
            long_pos_data[PortfolioDataHeader.TRADED_TODAY.value]
            * (
                long_pos_data[PortfolioDataHeader.PRICE.value]
                - long_pos_data[PortfolioDataHeader.TRADE_PRICE.value]
            )
            / long_pos_data[PortfolioDataHeader.CALCULATED_NAV.value]
        )
        logger.debug("Recalculated 'return adjustments'")
