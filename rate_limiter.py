"""
Shared rate limiter to stay within the server's 20 requests/minute limit.

All API calls (enrichment, webhooks) go through this single limiter
so we never exceed the server's global rate limit.
"""
import time
import threading
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    """Token-bucket-style rate limiter that tracks request timestamps."""

    def __init__(self, max_requests: int = 18, window_seconds: int = 60):
        """
        Args:
            max_requests: Max requests allowed per window (set below server limit for safety)
            window_seconds: Time window in seconds
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._timestamps: list = []
        self._lock = threading.Lock()

    def wait_if_needed(self):
        """Block until we have capacity to make another request."""
        with self._lock:
            now = time.time()

            # Remove timestamps older than the window
            self._timestamps = [t for t in self._timestamps if t > now - self.window_seconds]

            if len(self._timestamps) >= self.max_requests:
                # Wait until the oldest request in our window expires
                oldest = self._timestamps[0]
                wait_time = oldest + self.window_seconds - now + 0.5  # small buffer
                if wait_time > 0:
                    logger.info("Rate limiter: at capacity, waiting %.1fs", wait_time)
                    time.sleep(wait_time)

                    # Clean up again after sleeping
                    now = time.time()
                    self._timestamps = [t for t in self._timestamps if t > now - self.window_seconds]

            self._timestamps.append(time.time())
