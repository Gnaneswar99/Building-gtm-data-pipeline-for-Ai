"""
Data enrichment service for firmographic and contact data.

Handles real-world API challenges:
- Rate limiting (429 with Retry-After)
- Random server errors (500)
- Schema inconsistencies (num_lawyers vs lawyer_count)
- Missing/partial data in responses
"""
import time
import logging
from typing import Dict, Any, Optional

import httpx

logger = logging.getLogger(__name__)


class Enricher:
    """Handles data enrichment for firms via external APIs."""

    def __init__(self, base_url: str, timeout: int = 30, max_retries: int = 3, rate_limiter=None):
        """
        Initialize enricher with API configuration.

        Args:
            base_url: Base URL for enrichment API
            timeout: Request timeout in seconds
            max_retries: Max retry attempts for failed requests
            rate_limiter: Shared RateLimiter instance to respect global rate limits
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.rate_limiter = rate_limiter
        self.client = httpx.Client(timeout=self.timeout)

    def _request_with_retry(self, method: str, url: str, **kwargs) -> Optional[httpx.Response]:
        """
        Make an HTTP request with exponential backoff retry logic.

        Uses the shared rate limiter to proactively wait before each request,
        and handles 429/500 responses as a fallback safety net.
        """
        for attempt in range(self.max_retries + 2):
            # Proactively wait for rate limit capacity before making the request
            if self.rate_limiter:
                self.rate_limiter.wait_if_needed()

            try:
                response = self.client.request(method, url, **kwargs)

                if response.status_code == 200:
                    return response

                if response.status_code == 429:
                    # Shouldn't hit this often with the rate limiter, but handle it
                    wait_time = min(int(response.headers.get("retry-after", "5")), 5)
                    logger.warning(
                        "Rate limited on %s, waiting %ds (attempt %d)",
                        url, wait_time, attempt + 1,
                    )
                    time.sleep(wait_time)
                    continue

                if response.status_code == 500:
                    backoff = 2 ** attempt
                    logger.warning(
                        "Server error on %s, retrying in %ds (attempt %d/%d)",
                        url, backoff, attempt + 1, self.max_retries,
                    )
                    time.sleep(backoff)
                    continue

                if response.status_code == 404:
                    logger.info("Resource not found: %s", url)
                    return None

                logger.error("Unexpected status %d from %s", response.status_code, url)

            except (httpx.ConnectError, httpx.TimeoutException) as exc:
                backoff = 2 ** attempt
                logger.warning(
                    "Connection error on %s: %s, retrying in %ds (attempt %d/%d)",
                    url, exc, backoff, attempt + 1, self.max_retries,
                )
                time.sleep(backoff)

        logger.error("All retries exhausted for %s", url)
        return None

    def _normalize_firmographic(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize schema inconsistencies in firmographic data.

        The API sometimes returns 'lawyer_count' instead of 'num_lawyers',
        and may drop optional fields. We handle both cases here.
        """
        if "lawyer_count" in data and "num_lawyers" not in data:
            data["num_lawyers"] = data.pop("lawyer_count")

        data.setdefault("region", None)
        data.setdefault("practice_areas", [])
        data.setdefault("domain", None)
        data.setdefault("country", None)
        data.setdefault("num_lawyers", 0)

        return data

    def fetch_firmographic(self, firm_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch firmographic data for a firm.

        Args:
            firm_id: Unique identifier for the firm

        Returns:
            Normalized firmographic data or None if unavailable
        """
        url = f"{self.base_url}/firms/{firm_id}/firmographic"
        response = self._request_with_retry("GET", url)

        if response is None:
            return None

        data = response.json()
        return self._normalize_firmographic(data)

    def fetch_contact(self, firm_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch contact information for a firm.

        Args:
            firm_id: Unique identifier for the firm

        Returns:
            Contact data dict or None if unavailable.
            Note: email and linkedin_url may be None even in successful responses.
        """
        url = f"{self.base_url}/firms/{firm_id}/contact"
        response = self._request_with_retry("GET", url)

        if response is None:
            return None

        return response.json()

    def close(self):
        """Close the underlying HTTP client."""
        self.client.close()