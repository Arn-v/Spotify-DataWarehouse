"""Token-bucket rate limiter for Spotify API calls."""

import logging
import threading
import time

logger = logging.getLogger(__name__)


class RateLimiter:
    """Simple token-bucket rate limiter.

    Ensures no more than `max_calls` are made within `period` seconds.
    Thread-safe for use with APScheduler.

    Args:
        max_calls: Maximum number of calls allowed per period.
        period: Time window in seconds.
    """

    def __init__(self, max_calls: int = 30, period: float = 30.0) -> None:
        self.max_calls = max_calls
        self.period = period
        self._calls: list[float] = []
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block until a call is allowed."""
        while True:
            with self._lock:
                now = time.monotonic()
                # Remove expired timestamps
                self._calls = [t for t in self._calls if now - t < self.period]

                if len(self._calls) < self.max_calls:
                    self._calls.append(now)
                    return

                # Calculate sleep time outside the lock
                sleep_time = self.period - (now - self._calls[0])

            if sleep_time > 0:
                logger.debug("Rate limited, sleeping %.2fs", sleep_time)
                time.sleep(sleep_time)

    def __enter__(self) -> "RateLimiter":
        self.acquire()
        return self

    def __exit__(self, *args) -> None:
        pass
