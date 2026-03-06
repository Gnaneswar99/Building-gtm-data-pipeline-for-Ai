"""
ICP scoring system for evaluating firm fit.

Scores each firm on three dimensions:
1. Firm size (number of lawyers within the ideal range)
2. Practice area overlap with preferred areas
3. Geographic match with target regions

Each dimension produces a 0-1 sub-score, combined via configurable weights
into a final score between 0.0 and 1.0.
"""
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


class ICPScorer:
    """Scores firms against ideal customer profile criteria."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize scorer with ICP configuration.

        Args:
            config: ICP scoring config with firm_size, practice_areas, geography sections
        """
        size_cfg = config.get("firm_size", {})
        self.min_lawyers = size_cfg.get("min_lawyers", 50)
        self.max_lawyers = size_cfg.get("max_lawyers", 500)
        self.size_weight = size_cfg.get("weight", 0.35)

        pa_cfg = config.get("practice_areas", {})
        self.preferred_areas: List[str] = pa_cfg.get("preferred", [])
        self.area_weight = pa_cfg.get("weight", 0.40)

        geo_cfg = config.get("geography", {})
        self.preferred_regions: List[str] = geo_cfg.get("preferred_regions", [])
        self.geo_weight = geo_cfg.get("weight", 0.25)

    def _score_firm_size(self, num_lawyers: int) -> float:
        """
        Score based on whether the firm falls within the ideal lawyer count range.

        Returns 1.0 if within range, with a gradual falloff outside.
        Firms very far outside the range get close to 0.
        """
        if num_lawyers <= 0:
            return 0.0

        if self.min_lawyers <= num_lawyers <= self.max_lawyers:
            return 1.0

        # Partial credit for firms close to the range boundaries
        if num_lawyers < self.min_lawyers:
            # e.g., firm with 40 lawyers vs min 50 -> 40/50 = 0.8
            return max(0.0, num_lawyers / self.min_lawyers)
        else:
            # e.g., firm with 600 lawyers vs max 500 -> 500/600 = 0.83
            return max(0.0, self.max_lawyers / num_lawyers)

    def _score_practice_areas(self, practice_areas: List[str]) -> float:
        """
        Score based on overlap between firm's practice areas and our preferred ones.

        Returns the fraction of the firm's areas that match our preferences.
        If the firm has no listed areas, returns 0.
        """
        if not practice_areas or not self.preferred_areas:
            return 0.0

        preferred_set = set(self.preferred_areas)
        firm_set = set(practice_areas)
        overlap = firm_set & preferred_set

        # Fraction of the firm's areas that are in our preferred list
        return len(overlap) / len(firm_set)

    def _score_geography(self, country: str, region: str) -> float:
        """
        Score based on whether the firm is in a preferred geographic region.

        Checks country name against preferred_regions list.
        Returns 1.0 for a match, 0.0 otherwise.
        """
        if not country:
            return 0.0

        # The config uses country-level names like "US", "Australia", etc.
        if country in self.preferred_regions:
            return 1.0

        # Also check region code in case it maps to a preferred entry
        if region and region in self.preferred_regions:
            return 1.0

        return 0.0

    def score(self, firm: Dict[str, Any]) -> float:
        """
        Calculate ICP score for a firm.

        Args:
            firm: Firm data with enriched information (must have num_lawyers,
                  practice_areas, country fields at minimum)

        Returns:
            ICP score between 0.0 and 1.0
        """
        num_lawyers = firm.get("num_lawyers", 0)
        practice_areas = firm.get("practice_areas", [])
        country = firm.get("country", "")
        region = firm.get("region", "")

        size_score = self._score_firm_size(num_lawyers)
        area_score = self._score_practice_areas(practice_areas)
        geo_score = self._score_geography(country, region)

        total = (
            size_score * self.size_weight
            + area_score * self.area_weight
            + geo_score * self.geo_weight
        )

        logger.debug(
            "Firm %s scores: size=%.2f, areas=%.2f, geo=%.2f -> total=%.2f",
            firm.get("name", "unknown"), size_score, area_score, geo_score, total,
        )

        return round(total, 4)