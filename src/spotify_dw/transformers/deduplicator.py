"""Cross-source deduplicator for merging Kaggle and API data."""

import pandas as pd

from spotify_dw.transformers.base import BaseTransformer


class Deduplicator(BaseTransformer):
    """Deduplicates tracks across data sources.

    When the same spotify_track_id exists in both sources, API data is
    preferred (fresher). Tracks only in one source are kept as-is.
    """

    REQUIRED_COLUMNS = ["spotify_track_id"]

    def __init__(self, existing_ids: set[str] | None = None) -> None:
        """
        Args:
            existing_ids: Set of spotify_track_ids already in the warehouse.
                          If provided, only new/updated tracks pass through.
        """
        super().__init__()
        self.existing_ids = existing_ids or set()

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self._validate_schema(df, self.REQUIRED_COLUMNS)
        df = df.copy()

        before = len(df)

        # Drop duplicates within the batch itself
        df = df.drop_duplicates(subset=["spotify_track_id"], keep="first")

        # Filter out tracks already in the warehouse
        if self.existing_ids:
            df = df[~df["spotify_track_id"].isin(self.existing_ids)]

        after = len(df)
        skipped = before - after
        if skipped > 0:
            self.logger.info(f"Deduplication: {skipped} tracks skipped, {after} new tracks remaining")

        return df
