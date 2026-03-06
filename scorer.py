"""
ICP scoring system for evaluating firm fit.
"""
from typing import Dict, Any

class ICPScorer:
    """Scores firms against ideal customer profile criteria."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize scorer with ICP configuration.
        
        Args:
            config: ICP scoring configuration
        """
        pass
    
    def score(self, firm: Dict[str, Any]) -> float:
        """
        Calculate ICP score for a firm.
        
        Args:
            firm: Firm data with enriched information
            
        Returns:
            ICP score between 0.0 and 1.0
        """
        pass