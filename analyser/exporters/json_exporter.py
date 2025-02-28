import io
import json
import logging
from datetime import date
from decimal import Decimal
from typing import Iterable

from analyser.interfaces import PortfolioError, PortfolioErrorExporter
from analyser.utils import chunked_iterable

logger = logging.getLogger(__name__)


class PortfolioErrorEncoder(json.JSONEncoder):
    """Custom JSON encoder for PortfolioError."""

    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, date):
            return obj.isoformat()
        return super().default(obj)


class PortfolioErrorExporterJSON(PortfolioErrorExporter):
    """Export portfolio errors to JSON."""

    def __init__(self, output_file: io.TextIOWrapper, chunk_size: int = 1000):
        self.output_file = output_file
        self.chunk_size = chunk_size

    def export(self, errors: Iterable[PortfolioError]):
        """Export the errors."""

        logger.info("Exporting errors to JSON")
        chunk_start = 0
        error_counter = 0

        for error_chunk in chunked_iterable(errors, self.chunk_size):
            for error in error_chunk:
                self.output_file.write(
                    json.dumps(error.to_dict(), cls=PortfolioErrorEncoder)
                )
                self.output_file.write("\n")
                error_counter += 1

            chunk_start += self.chunk_size
            logger.debug(f"Exported errors from {chunk_start} rows")
            self.output_file.flush()

        logger.info("Detected %d errors in total", error_counter)
        logger.info("Errors exported to JSON")
