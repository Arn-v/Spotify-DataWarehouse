"""Abstract base class for all data loaders."""

import logging
from abc import ABC, abstractmethod

import pandas as pd


class BaseLoader(ABC):
    """Base class for loading data into a target store.

    Subclasses must implement load().
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def load(self, df: pd.DataFrame) -> int:
        """Write the DataFrame to the target and return the number of rows loaded."""

    def _pre_load_checks(self, df: pd.DataFrame) -> bool:
        """Validate data before loading. Returns True if OK."""
        if df.empty:
            self.logger.warning("Empty DataFrame, skipping load")
            return False
        return True

    def _post_load_verify(self, expected_count: int, actual_count: int) -> bool:
        """Verify the load succeeded by comparing expected vs actual row counts."""
        if actual_count != expected_count:
            self.logger.warning(
                "Row count mismatch",
                extra={"expected": expected_count, "actual": actual_count},
            )
            return False
        return True
