import logging
from decimal import Decimal
from typing import Iterator

import pandas as pd

from analyser.interfaces import PortfolioDataFeed

logger = logging.getLogger(__name__)


class PortfolioDataFeedExcel(PortfolioDataFeed):
    def __init__(self, file_path: str, chunk_size: int = 1000):
        self.file_path = file_path
        self.chunk_size = chunk_size

    def get_data(self) -> Iterator[pd.DataFrame]:
        """Get the data for portfolio."""

        first_row = pd.read_excel(self.file_path, nrows=0)
        headers = [self.normalize_header(header) for header in first_row.columns]
        yield headers
        logger.debug(f"Headers: {headers}")
        skip_rows = 1

        while True:
            logger.debug(f"Reading chunk from row {skip_rows}")
            df = pd.read_excel(
                self.file_path,
                skiprows=skip_rows,
                header=None,
                names=headers,
                nrows=self.chunk_size,
            )

            if df.empty:
                break

            # Convert all float values to Decimal
            for col in df.select_dtypes(include=["float"]).columns:
                df[col] = df[col].apply(Decimal)

            skip_rows += self.chunk_size
            yield df

        logger.debug("No more data to read")
