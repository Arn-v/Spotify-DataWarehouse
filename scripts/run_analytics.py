"""CLI script to run the analytics pipeline."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.logging_config import setup_logging
from spotify_dw.db.session import SessionFactory
from spotify_dw.pipelines.analytics import AnalyticsPipeline


def main() -> None:
    setup_logging()

    factory = SessionFactory()
    with factory.session() as session:
        pipeline = AnalyticsPipeline(session)
        result = pipeline.execute()

    print(f"\nAnalytics Complete:")
    print(f"  Status: {result.status}")
    print(f"  Rows loaded: {result.rows_loaded}")
    print(f"  Duration: {result.duration_seconds:.2f}s")
    if result.errors:
        print(f"  Errors: {result.errors}")


if __name__ == "__main__":
    main()
