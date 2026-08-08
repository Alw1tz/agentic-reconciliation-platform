"""
Data Validation Utilities for Financial Risk Assessment

Provides validation functions to ensure data integrity before processing.
"""

from typing import Dict, Any
from pydantic import BaseModel, ValidationError
import logging

logger = logging.getLogger(__name__)

def validate_input_data(input_data: BaseModel) -> bool:
    """
    Validate input data for risk assessment.
    
    Args:
        input_data (BaseModel): Input data to validate
        
    Returns:
        bool: True if valid, raises exception if invalid
        
    Raises:
        ValueError: If validation fails
    """
    try:
        # Basic validation checks
        if not input_data.customer_id or len(input_data.customer_id.strip()) == 0:
            raise ValueError("Customer ID is required")
        
        if input_data.transaction_amount <= 0:
            raise ValueError("Transaction amount must be positive")
            
        if not input_data.product_type:
            raise ValueError("Product type is required")
        
        logger.info(f"Input data validation passed for customer {input_data.customer_id}")
        return True
        
    except Exception as e:
        logger.error(f"Input validation failed: {str(e)}")
        raise

def validate_risk_score(score: float) -> bool:
    """
    Validate that risk score is within acceptable range.
    
    Args:
        score (float): Risk score to validate
        
    Returns:
        bool: True if valid
    """
    if not isinstance(score, (int, float)):
        raise ValueError("Risk score must be numeric")
        
    if score < 0 or score > 100:
        raise ValueError("Risk score must be between 0 and 100")
        
    return True