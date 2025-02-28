import logging
from decimal import Decimal
from typing import Iterator

import pandas as pd

from analyser.enums import PortfolioDataHeader
from analyser.errors import PortfolioErrorCalculation
from analyser.interfaces import PortfolioError
from analyser.reconcilers.base import PortfolioDataReconcilerBase

logger = logging.getLogger(__name__)


class PortfolioDataReconcilerTotalReturn(PortfolioDataReconcilerBase):
    """Reconcile portfolio data for total return."""

    ERROR_TOLERANCE = Decimal("0.01")

    def __init__(self):
        super().__init__()

        self._diff_column = self._difference_column_name_factory(
            PortfolioDataHeader.TOTAL_RETURN.value
        )
        self._recalc_column = self._recalculate_column_name_factory(
            PortfolioDataHeader.TOTAL_RETURN.value
        )
        self._recalc_column_return_adjustments = self._recalculate_column_name_factory(
            PortfolioDataHeader.RETURN_ADJUSTMENTS.value
        )

    def reconcile(self, data: pd.DataFrame) -> Iterator[PortfolioError]:
        """Reconcile the data for total return."""

        logger.info("Reconciling 'total return'")
        self.recalculate(data)

        data[self._diff_column] = (
            data[self._recalc_column] - data[PortfolioDataHeader.TOTAL_RETURN.value]
        )
        # find the rows where the difference is greater than the tolerance
        mask = data[self._diff_column].abs() > self.ERROR_TOLERANCE
        for _, row in data[mask].iterrows():
            error = PortfolioErrorCalculation(
                ticker=row[PortfolioDataHeader.P_TICKER.value],
                location=PortfolioDataHeader.TOTAL_RETURN.value,
                date=row[PortfolioDataHeader.DATE.value].date(),
                value=row[PortfolioDataHeader.TOTAL_RETURN.value],
                correct_value=row[self._recalc_column],
            )
            yield error

        logger.info("'total return' reconciled")

    def recalculate(self, data):
        """Recalculate the data for total return."""

        if self._recalc_column in data:
            return

        logger.debug("Recalculating 'total return'")
        short_pos_data = data.loc[data[PortfolioDataHeader.SHORT_POS.value] == True]
        data.loc[
            data[PortfolioDataHeader.SHORT_POS.value] == True, self._recalc_column
        ] = (
            (
                short_pos_data[PortfolioDataHeader.PRICE_YESTERDAY.value]
                - short_pos_data[PortfolioDataHeader.PRICE.value]
            )
            / short_pos_data[PortfolioDataHeader.PRICE_YESTERDAY.value]
            * (
                (
                    short_pos_data[PortfolioDataHeader.CLOSE_QUANTITY.value]
                    - short_pos_data[PortfolioDataHeader.TRADED_TODAY.value]
                )
                * short_pos_data[PortfolioDataHeader.PRICE.value]
                * short_pos_data[PortfolioDataHeader.EXCHANGE_RATE.value]
            ).abs()
            / short_pos_data[PortfolioDataHeader.CALCULATED_NAV.value]
        ) + short_pos_data[
            self._recalc_column_return_adjustments
        ]
        long_pos_data = data.loc[data[PortfolioDataHeader.SHORT_POS.value] == False]
        data.loc[
            data[PortfolioDataHeader.SHORT_POS.value] == False, self._recalc_column
        ] = (
            (
                long_pos_data[PortfolioDataHeader.PRICE.value]
                - long_pos_data[PortfolioDataHeader.PRICE_YESTERDAY.value]
            )
            / long_pos_data[PortfolioDataHeader.PRICE_YESTERDAY.value]
            * (
                (
                    long_pos_data[PortfolioDataHeader.CLOSE_QUANTITY.value]
                    - long_pos_data[PortfolioDataHeader.TRADED_TODAY.value]
                )
                * long_pos_data[PortfolioDataHeader.PRICE.value]
                * long_pos_data[PortfolioDataHeader.EXCHANGE_RATE.value]
            ).abs()
            / long_pos_data[PortfolioDataHeader.CALCULATED_NAV.value]
        ) + long_pos_data[
            self._recalc_column_return_adjustments
        ]
        logger.debug("'total return' recalculated")
