"""
Experiment assignment system for A/B testing.

Uses deterministic hashing so the same lead always gets the same variant,
which is important for consistency if the pipeline is re-run. This avoids
the problem of random assignment shifting leads between variants across runs.
"""
import hashlib
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class ExperimentAssigner:
    """Assigns leads to experiment variants using deterministic hashing."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize experiment assigner with configuration.

        Args:
            config: Experiment config containing email_variants
        """
        experiments_cfg = config.get("experiments", config)
        variants_cfg = experiments_cfg.get("email_variants", {})
        self.variants: List[str] = sorted(variants_cfg.keys())
        self.variant_details = variants_cfg

        if not self.variants:
            logger.warning("No experiment variants configured, defaulting to variant_a/variant_b")
            self.variants = ["variant_a", "variant_b"]

    def assign_variant(self, lead_id: str) -> str:
        """
        Assign a lead to an experiment variant using consistent hashing.

        The same lead_id will always map to the same variant, ensuring
        deterministic assignment across pipeline re-runs.

        Args:
            lead_id: Unique lead identifier (typically the firm_id)

        Returns:
            Experiment variant identifier (e.g. "variant_a" or "variant_b")
        """
        hash_val = hashlib.md5(lead_id.encode()).hexdigest()
        bucket = int(hash_val, 16) % len(self.variants)
        variant = self.variants[bucket]

        logger.debug("Lead %s assigned to %s (hash bucket %d)", lead_id, variant, bucket)
        return variant

    def get_variant_subject(self, variant: str) -> str:
        """Get the email subject line for a given variant."""
        details = self.variant_details.get(variant, {})
        return details.get("subject", "")