from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any, Iterator

import pandas as pd

from analyser import enums


class PortfolioAnalyser(ABC):
    """Portfolio Analyser interface."""

    data_feed: "PortfolioDataFeed"
    error_exporter: "PortfolioErrorExporter"
    reconcilers: Iterator["PortfolioDataReconciler"]

    def get_headers(self) -> Iterator[str]:
        """Get the headers for portfolio data."""

        if self._data_headers is not None:
            return self._data_headers

        headers = next(self._data_iterator)
        assert headers is not None, "Headers not found in data feed."
        assert list(headers) == [
            header.value for header in enums.PortfolioDataHeader
        ], f"Headers do not match expected headers."

        return headers

    def get_data(self) -> Iterator[pd.DataFrame]:
        """Get the data for portfolio."""

        if self._data_headers is None:
            self._data_headers = self.get_headers()

        yield from self._data_iterator

    @abstractmethod
    def analyse(self) -> Iterator["PortfolioError"]:
        """Analyse the portfolio data."""

        ...

    @abstractmethod
    def export_errors(self, errors: Iterator["PortfolioError"]) -> None:
        """Export the errors."""

        ...


class PortfolioDataReconciler(ABC):
    """Portfolio Data Reconciler interface."""

    ERROR_TOLERANCE: Decimal

    @abstractmethod
    def reconcile(self, data: pd.DataFrame) -> Iterator["PortfolioError"]:
        """Reconcile the data."""

        ...

    @abstractmethod
    def recalculate(self, data: pd.DataFrame) -> None:
        """Recalculate the data."""

        ...


class PortfolioDataFeed(ABC):
    """Portfolio Data Feed interface."""

    @abstractmethod
    def get_data(self) -> Iterator[pd.DataFrame]:
        """Get the data for portfolio."""

        ...

    def normalize_header(self, header: str) -> str:
        return header.strip().lower().replace(" ", "_")


class PortfolioError(ABC):
    """Portfolio Error interface."""

    @abstractmethod
    def describe(self) -> str:
        """Describe the error."""

        ...

    @abstractmethod
    def to_dict(self) -> dict:
        """Convert error to dictionary."""

        ...


@dataclass
class PortfolioErrorContext:
    """Context for Portfolio Error."""

    ticker: str
    date: date
    location: str
    value: Any


class PortfolioErrorExporter(ABC):
    """Portfolio Error Export interface."""

    chunk_size: int

    @abstractmethod
    def export(self, errors: Iterator[PortfolioError]):
        """Export the errors."""

        ...
