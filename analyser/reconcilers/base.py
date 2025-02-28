from enum import Enum

from analyser.interfaces import PortfolioDataReconciler


class AddedColumnSuffix(Enum):
    """Suffixes for added columns during reconciliation."""

    RECALC = "_recalc"
    DIFF = "_diff"


class PortfolioDataReconcilerBase(PortfolioDataReconciler):
    """Base class for portfolio data reconcilers."""

    def _recalculate_column_name_factory(self, column_name: str) -> str:
        """Return the recalculated column name."""
        return f"{column_name}{AddedColumnSuffix.RECALC.value}"

    def _difference_column_name_factory(self, column_name: str) -> str:
        """Return the difference column name."""
        return f"{column_name}{AddedColumnSuffix.DIFF.value}"
