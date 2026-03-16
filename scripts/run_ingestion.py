"""CLI script to run a one-off Spotify API ingestion."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.logging_config import setup_logging
from spotify_dw.db.session import SessionFactory
from spotify_dw.pipelines.ingestion import IngestionPipeline


def main() -> None:
    setup_logging()

    factory = SessionFactory()
    with factory.session() as session:
        pipeline = IngestionPipeline(session)
        result = pipeline.execute()

    print(f"\nIngestion Complete:")
    print(f"  Status: {result.status}")
    print(f"  Rows extracted: {result.rows_extracted}")
    print(f"  Rows loaded: {result.rows_loaded}")
    print(f"  Duration: {result.duration_seconds:.2f}s")
    print(f"  Endpoint statuses: {result.endpoint_statuses}")
    if result.errors:
        print(f"  Errors: {result.errors}")


if __name__ == "__main__":
    main()
