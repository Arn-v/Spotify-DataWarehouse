"""Abstract base class for all data extractors."""

import logging
from abc import ABC, abstractmethod

import pandas as pd


class BaseExtractor(ABC):
    """Base class for data extraction from any source.

    Subclasses must implement extract() and validate_source().
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Pull raw data from the source and return as a DataFrame."""

    @abstractmethod
    def validate_source(self) -> bool:
        """Check if the data source is available and accessible."""

    def _log_extraction(self, count: int) -> None:
        """Log extraction results in a standardized format."""
        self.logger.info(
            "Extraction complete",
            extra={"extractor": self.__class__.__name__, "rows_extracted": count},
        )
