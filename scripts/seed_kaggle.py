"""CLI script to load Kaggle historical data into the data warehouse."""

import argparse
import sys
from pathlib import Path

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.logging_config import setup_logging
from spotify_dw.db.session import SessionFactory
from spotify_dw.pipelines.kaggle_load import KaggleLoadPipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Load Kaggle Spotify data into the warehouse")
    parser.add_argument("--file", required=True, help="Path to the Kaggle CSV file")
    args = parser.parse_args()

    setup_logging()

    file_path = Path(args.file)
    if not file_path.exists():
        print(f"Error: File not found: {file_path}")
        sys.exit(1)

    factory = SessionFactory()
    with factory.session() as session:
        pipeline = KaggleLoadPipeline(session, file_path)
        result = pipeline.execute()

    print(f"\nKaggle Load Complete:")
    print(f"  Status: {result.status}")
    print(f"  Rows extracted: {result.rows_extracted}")
    print(f"  Rows loaded: {result.rows_loaded}")
    print(f"  Duration: {result.duration_seconds:.2f}s")
    if result.errors:
        print(f"  Errors: {result.errors}")


if __name__ == "__main__":
    main()
