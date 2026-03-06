"""
Webhook client for firing events to downstream systems.

Sends lead data to CRM and email campaign endpoints with
retry logic to handle the ~5% failure rate from each service.
"""
import time
import logging
from typing import Dict, Any

import httpx

logger = logging.getLogger(__name__)


class WebhookClient:
    """Handles webhook delivery to CRM and email platforms."""

    def __init__(self, config: Dict[str, Any], rate_limiter=None):
        """
        Initialize webhook client with configuration.

        Args:
            config: Webhook config with crm_endpoint, email_endpoint, timeout, max_retries
            rate_limiter: Shared RateLimiter instance to respect global rate limits
        """
        webhooks_cfg = config.get("webhooks", config)
        self.crm_endpoint = webhooks_cfg.get("crm_endpoint", "http://localhost:8000/webhooks/crm")
        self.email_endpoint = webhooks_cfg.get("email_endpoint", "http://localhost:8000/webhooks/email")
        self.timeout = webhooks_cfg.get("timeout", 10)
        self.max_retries = webhooks_cfg.get("max_retries", 2)
        self.rate_limiter = rate_limiter
        self.client = httpx.Client(timeout=self.timeout)

    def _post_with_retry(self, url: str, payload: Dict[str, Any]) -> bool:
        """
        POST a JSON payload to a URL with retry on failure.

        Uses the shared rate limiter before each request, and retries on
        500 errors and connection issues with exponential backoff.
        """
        for attempt in range(self.max_retries + 3):
            # Proactively wait for rate limit capacity
            if self.rate_limiter:
                self.rate_limiter.wait_if_needed()

            try:
                response = self.client.post(url, json=payload)

                if response.status_code == 200:
                    result = response.json()
                    logger.info("Webhook delivered to %s: %s", url, result.get("id", ""))
                    return True

                if response.status_code == 429:
                    wait_time = min(int(response.headers.get("retry-after", "5")), 5)
                    logger.warning("Webhook rate limited, waiting %ds", wait_time)
                    time.sleep(wait_time)
                    continue

                if response.status_code >= 500:
                    backoff = 2 ** attempt
                    logger.warning(
                        "Webhook %s returned %d, retrying in %ds (attempt %d)",
                        url, response.status_code, backoff, attempt + 1,
                    )
                    time.sleep(backoff)
                    continue

                logger.error("Webhook %s returned unexpected status %d", url, response.status_code)
                return False

            except (httpx.ConnectError, httpx.TimeoutException) as exc:
                backoff = 2 ** attempt
                logger.warning("Webhook connection error: %s, retrying in %ds", exc, backoff)
                time.sleep(backoff)

        logger.error("All retries exhausted for webhook %s", url)
        return False

    def fire(self, payload: Dict[str, Any], target: str = "both") -> bool:
        """
        Fire webhook with payload to configured endpoints.

        Args:
            payload: Data to send in webhook
            target: Which endpoint(s) to hit - "crm", "email", or "both"

        Returns:
            True if at least one delivery succeeded, False if all failed
        """
        results = []

        if target in ("crm", "both"):
            crm_ok = self._post_with_retry(self.crm_endpoint, payload)
            results.append(crm_ok)

        if target in ("email", "both"):
            email_ok = self._post_with_retry(self.email_endpoint, payload)
            results.append(email_ok)

        return any(results)

    def close(self):
        """Close the underlying HTTP client."""
        self.client.close()