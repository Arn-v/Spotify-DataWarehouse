"""Abstract base class for all analytics modules."""

import logging
from abc import ABC, abstractmethod

import pandas as pd


class BaseAnalyzer(ABC):
    """Base class for marketing analytics.

    Subclasses must implement analyze().
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def analyze(self, df: pd.DataFrame) -> pd.DataFrame:
        """Run the analysis and return results as a DataFrame."""

    def _validate_input(self, df: pd.DataFrame, required_columns: list[str]) -> bool:
        """Check that the input DataFrame has the required columns."""
        missing = set(required_columns) - set(df.columns)
        if missing:
            self.logger.error(f"Missing required columns: {missing}")
            return False
        if df.empty:
            self.logger.warning("Empty input DataFrame")
            return False
        return True

    def _export(self, df: pd.DataFrame, path: str, format: str = "csv") -> None:
        """Export results to a file."""
        if format == "csv":
            df.to_csv(path, index=False)
        elif format == "json":
            df.to_json(path, orient="records", indent=2)
        else:
            raise ValueError(f"Unsupported export format: {format}")
        self.logger.info(f"Exported {len(df)} rows to {path}")
