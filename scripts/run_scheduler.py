"""CLI script to start the APScheduler daemon for periodic ingestion + analytics."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.logging_config import setup_logging
from spotify_dw.scheduler.jobs import create_scheduler


def main() -> None:
    setup_logging()
    print("Starting Spotify DW Scheduler...")
    print("Press Ctrl+C to stop.\n")

    scheduler = create_scheduler()
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\nScheduler stopped.")


if __name__ == "__main__":
    main()
