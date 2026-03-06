"""
Lead routing system for qualified prospects.

Routes firms into three categories based on their ICP score:
- high_priority: Score >= high threshold -> immediate sales outreach
- nurture: Score >= nurture threshold -> drip campaign / warm outreach
- disqualified: Score below nurture threshold -> not a fit right now
"""
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class LeadRouter:
    """Routes qualified leads to appropriate categories."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize router with routing configuration.

        Args:
            config: Routing config with threshold values
        """
        routing_cfg = config.get("routing", {})
        self.high_priority_threshold = routing_cfg.get("high_priority_threshold", 0.70)
        self.nurture_threshold = routing_cfg.get("nurture_threshold", 0.40)

    def route(self, firm: Dict[str, Any], score: float) -> str:
        """
        Route a lead based on its ICP score and firm data.

        Args:
            firm: Firm data dict
            score: ICP score (0.0 to 1.0)

        Returns:
            Route category: "high_priority", "nurture", or "disqualified"
        """
        firm_name = firm.get("name", "unknown")

        if score >= self.high_priority_threshold:
            logger.info("Firm '%s' (score=%.2f) -> high_priority", firm_name, score)
            return "high_priority"

        if score >= self.nurture_threshold:
            logger.info("Firm '%s' (score=%.2f) -> nurture", firm_name, score)
            return "nurture"

        logger.info("Firm '%s' (score=%.2f) -> disqualified", firm_name, score)
        return "disqualified"