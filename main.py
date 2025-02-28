import logging
import uuid

from analyser.analyser import PortfolioAnalyzer
from analyser.data_feeds.excel import PortfolioDataFeedExcel
from analyser.exporters.json_exporter import PortfolioErrorExporterJSON

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

DATA_FEED_CHUNK_SIZE = 1000
ERROR_EXPORT_CHUNK_SIZE = 1000
error_file_folder = "results"
reference = str(uuid.uuid4())
error_file_path = f"{error_file_folder}/{reference}.jsonl"

with open(error_file_path, "w") as error_output_file:
    data_feed = PortfolioDataFeedExcel(
        "data/Test.xlsx", chunk_size=DATA_FEED_CHUNK_SIZE
    )
    error_exporter = PortfolioErrorExporterJSON(
        error_output_file, chunk_size=ERROR_EXPORT_CHUNK_SIZE
    )
    portfolio_analyzer = PortfolioAnalyzer(data_feed, error_exporter)
    logger.info("Starting portfolio analysis. Reference: %s", reference)
    errors = portfolio_analyzer.analyse()
    portfolio_analyzer.export_errors(errors)
    logger.info("Portfolio analysis completed")
