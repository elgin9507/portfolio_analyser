from datetime import date
from decimal import Decimal
from typing import Any

from analyser.interfaces import PortfolioError, PortfolioErrorContext


class PortfolioErrorBase(PortfolioError):
    """Base class for portfolio errors."""

    error_code: str = "ERR_UNKNOWN"

    def __init__(self, ticker: str, date: date, location: str, value: Any):
        if isinstance(value, Decimal):
            value_converted = self._convert_decimal(value)

        self._context = PortfolioErrorContext(
            ticker=ticker,
            date=date,
            location=location,
            value=value_converted,
        )

    def to_dict(self) -> dict:
        """Convert error to dictionary."""

        context_data = self._context.__dict__
        error_data = {
            "error_code": self.error_code,
            "description": self.describe(),
            "context": context_data,
        }

        return error_data

    @staticmethod
    def _convert_decimal(value: Decimal) -> Decimal:
        """Convert Decimal 4 precision for better readability."""

        return value.quantize(Decimal("0.0001"))


class PortfolioErrorHighVolatility(PortfolioErrorBase):
    """Error in high volatility of portfolio data."""

    error_code = "ERR_HIGH_VOLATILITY"

    def describe(self) -> str:
        """Describe the error."""

        return f"High volatility detected on date '{self._context.date}'."


class PortfolioErrorCalculation(PortfolioErrorBase):
    """Error in calculation of portfolio data."""

    error_code = "ERR_CALCULATION"

    def __init__(
        self, ticker: str, date: date, location: str, value: Any, correct_value: Any
    ):
        super().__init__(ticker, date, location, value)

        self.correct_value = self._convert_decimal(correct_value)

    def to_dict(self):
        """Convert error to dictionary."""

        data = super().to_dict()
        data.update(correct_value=self.correct_value)

        return data

    def describe(self) -> str:
        """Describe the error."""

        if self._context.value > self.correct_value:
            description = "Value is greater than expected"
        else:
            description = "Value is less than expected"

        difference = self._convert_decimal(
            abs(self._context.value - self.correct_value)
        )
        description += f" by amount '{difference}'."

        return description
