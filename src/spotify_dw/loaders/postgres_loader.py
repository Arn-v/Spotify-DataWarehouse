"""PostgreSQL loader with upsert (INSERT ON CONFLICT UPDATE) support."""

import logging
from typing import Any

import pandas as pd
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from spotify_dw.loaders.base import BaseLoader
from spotify_dw.models.base import Base

logger = logging.getLogger(__name__)


class PostgresLoader(BaseLoader):
    """Loads DataFrames into PostgreSQL tables using upsert logic.

    Uses INSERT ... ON CONFLICT DO UPDATE for dimension tables (idempotent),
    and INSERT ... ON CONFLICT DO NOTHING for fact tables (append-only).

    Args:
        session: SQLAlchemy session.
        model: The ORM model class for the target table.
        conflict_columns: Column(s) that define uniqueness for upsert.
        update_on_conflict: If True, update existing rows. If False, skip duplicates.
        batch_size: Number of rows per batch insert.
    """

    def __init__(
        self,
        session: Session,
        model: type[Base],
        conflict_columns: list[str],
        update_on_conflict: bool = True,
        batch_size: int = 1000,
    ) -> None:
        super().__init__()
        self.session = session
        self.model = model
        self.conflict_columns = conflict_columns
        self.update_on_conflict = update_on_conflict
        self.batch_size = batch_size
        self.table = model.__table__

    def load(self, df: pd.DataFrame) -> int:
        """Upsert the DataFrame into the target table. Returns rows affected."""
        if not self._pre_load_checks(df):
            return 0

        # Get valid columns (intersection of DataFrame cols and table cols)
        table_columns = {col.name for col in self.table.columns}
        valid_columns = [c for c in df.columns if c in table_columns]

        if not valid_columns:
            self.logger.warning(f"No matching columns between DataFrame and {self.table.name}")
            return 0

        records = df[valid_columns].to_dict(orient="records")
        total_loaded = 0

        for i in range(0, len(records), self.batch_size):
            batch = records[i : i + self.batch_size]
            total_loaded += self._upsert_batch(batch)

        self.logger.info(
            "Load complete",
            extra={"table": self.table.name, "rows_loaded": total_loaded},
        )
        return total_loaded

    def _upsert_batch(self, records: list[dict[str, Any]]) -> int:
        """Execute a single batch upsert. Returns rows affected."""
        if not records:
            return 0

        stmt = pg_insert(self.table).values(records)

        if self.update_on_conflict and self.conflict_columns:
            # Build SET clause: update all columns except the conflict columns and PKs
            update_cols = {
                col.name: stmt.excluded[col.name]
                for col in self.table.columns
                if col.name not in self.conflict_columns and not col.primary_key and col.name in records[0]
            }
            if update_cols:
                stmt = stmt.on_conflict_do_update(
                    index_elements=self.conflict_columns,
                    set_=update_cols,
                )
            else:
                stmt = stmt.on_conflict_do_nothing(index_elements=self.conflict_columns)
        elif self.conflict_columns:
            stmt = stmt.on_conflict_do_nothing(index_elements=self.conflict_columns)

        result = self.session.execute(stmt)
        return result.rowcount or 0

    def load_simple(self, df: pd.DataFrame) -> int:
        """Simple bulk insert without upsert — for tables without conflict columns (e.g., facts).

        Falls back to SQLAlchemy ORM bulk_save_objects for SQLite compatibility in tests.
        """
        if not self._pre_load_checks(df):
            return 0

        table_columns = {col.name for col in self.table.columns}
        valid_columns = [c for c in df.columns if c in table_columns]
        records = df[valid_columns].to_dict(orient="records")

        objects = [self.model(**record) for record in records]
        self.session.bulk_save_objects(objects)
        self.session.flush()

        self.logger.info(
            "Simple load complete",
            extra={"table": self.table.name, "rows_loaded": len(objects)},
        )
        return len(objects)
