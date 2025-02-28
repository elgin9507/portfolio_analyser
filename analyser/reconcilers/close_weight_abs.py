import logging
from decimal import Decimal
from typing import Iterator

import pandas as pd

from analyser.enums import PortfolioDataHeader
from analyser.errors import PortfolioErrorCalculation
from analyser.interfaces import PortfolioError
from analyser.reconcilers.base import PortfolioDataReconcilerBase

logger = logging.getLogger(__name__)


class PortfolioDataReconcilerCloseWeightAbs(PortfolioDataReconcilerBase):
    """Reconcile portfolio data for close weight abs."""

    ERROR_TOLERANCE = Decimal("0.01")

    def __init__(self):
        super().__init__()

        self._diff_column = self._difference_column_name_factory(
            PortfolioDataHeader.CLOSE_WEIGHT_ABS.value
        )
        self._recalc_column = self._recalculate_column_name_factory(
            PortfolioDataHeader.CLOSE_WEIGHT_ABS.value
        )
        self._recalc_column_close_weights = self._recalculate_column_name_factory(
            PortfolioDataHeader.CLOSING_WEIGHTS.value
        )

    def reconcile(self, data: pd.DataFrame) -> Iterator[PortfolioError]:
        """Reconcile the data for close weight abs."""

        logger.info("Reconciling 'close weight abs'")
        self.recalculate(data)

        data[self._diff_column] = (
            data[self._recalc_column] - data[PortfolioDataHeader.CLOSE_WEIGHT_ABS.value]
        )
        # find the rows where the difference is greater than the tolerance
        mask = data[self._diff_column].abs() > self.ERROR_TOLERANCE
        for _, row in data[mask].iterrows():
            error = PortfolioErrorCalculation(
                ticker=row[PortfolioDataHeader.P_TICKER.value],
                location=PortfolioDataHeader.CLOSE_WEIGHT_ABS.value,
                date=row[PortfolioDataHeader.DATE.value].date(),
                value=row[PortfolioDataHeader.CLOSE_WEIGHT_ABS.value],
                correct_value=row[self._recalc_column],
            )
            yield error

        logger.info("'close weight abs' reconciled")

    def recalculate(self, data):
        """Recalculate the data for close weight abs."""

        if self._recalc_column in data:
            return

        logger.debug("Recalculating 'close weight abs'")
        data[self._recalc_column] = data[self._recalc_column_close_weights].abs()
        data[self._recalc_column] = data[self._recalc_column].fillna(0)
