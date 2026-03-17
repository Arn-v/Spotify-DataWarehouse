"""Abstract base class for all data transformers."""

import logging
from abc import ABC, abstractmethod

import pandas as pd


class BaseTransformer(ABC):
    """Base class for data transformation and cleaning.

    Subclasses must implement transform().
    """

    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and reshape the input DataFrame."""

    def _validate_schema(self, df: pd.DataFrame, expected_columns: list[str]) -> None:
        """Raise ValueError if expected columns are missing."""
        missing = set(expected_columns) - set(df.columns)
        if missing:
            raise ValueError(f"Missing columns: {missing}")

    def _handle_nulls(self, df: pd.DataFrame, strategy: str = "drop", columns: list[str] | None = None) -> pd.DataFrame:
        """Handle null values using the specified strategy.

        Args:
            df: Input DataFrame.
            strategy: 'drop' to drop rows, 'fill_default' to fill with type defaults.
            columns: Columns to apply strategy to. None means all columns.
        """
        subset = columns if columns else None

        if strategy == "drop":
            return df.dropna(subset=subset)
        elif strategy == "fill_default":
            df = df.copy()
            if columns:
                for col in columns:
                    if df[col].dtype in ("float64", "int64"):
                        df[col] = df[col].fillna(0)
                    else:
                        df[col] = df[col].fillna("")
            return df
        else:
            raise ValueError(f"Unknown null strategy: {strategy}")
