"""
Financial Risk Assessment MCP Server

This server provides a unified interface for risk assessment across multiple 
financial domains (credit, market, operational). It coordinates between various 
risk models and data sources to provide comprehensive risk scoring.

Usage:
    python mcp_server.py
"""

import os
from typing import Dict, List, Any
from pydantic import BaseModel
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import components
from data_sources.database_client import DatabaseClient
from data_sources.api_client import APIClient
from risk_models.credit_risk import CreditRiskModel
from risk_models.market_risk import MarketRiskModel
from risk_models.operational_risk import OperationalRiskModel
from utils.validation import validate_input_data
from utils.logging_config import setup_logger

logger = setup_logger(__name__)

# Initialize components
db_client = DatabaseClient()
api_client = APIClient()
credit_model = CreditRiskModel()
market_model = MarketRiskModel()
operational_model = OperationalRiskModel()

class RiskAssessmentInput(BaseModel):
    """Input data model for risk assessment requests"""
    customer_id: str
    transaction_amount: float
    product_type: str
    time_period: str  # e.g., "monthly", "quarterly"
    additional_data: Dict[str, Any] = None

class RiskAssessmentOutput(BaseModel):
    """Output data model for risk assessment results"""
    customer_id: str
    overall_risk_score: float
    risk_categories: Dict[str, float]
    recommendations: List[str]
    confidence_level: float
    timestamp: str

# Initialize MCP server
from mcp.server.mcpserver import MCPServer

mcp = MCPServer("financial_risk")

@mcp.tool()
def get_customer_data(customer_id: str) -> dict:
    """
    Retrieve customer data from database for risk assessment.
    
    Args:
        customer_id (str): Unique identifier for the customer
        
    Returns:
        dict: Customer information including credit history, transaction patterns
    """
    try:
        logger.info(f"Retrieving data for customer {customer_id}")
        
        # Get customer profile from database
        customer_profile = db_client.get_customer_profile(customer_id)
        
        # Get transaction history
        transactions = db_client.get_transaction_history(customer_id)
        
        # Get external credit reports (if available)
        credit_reports = api_client.get_credit_report(customer_id)
        
        return {
            "customer_profile": customer_profile,
            "transactions": transactions,
            "credit_reports": credit_reports
        }
    except Exception as e:
        logger.error(f"Error retrieving customer data: {str(e)}")
        raise

@mcp.tool()
def assess_credit_risk(customer_id: str, transaction_amount: float) -> dict:
    """
    Assess credit risk using internal models and external data.
    
    Args:
        customer_id (str): Customer identifier
        transaction_amount (float): Amount of the transaction
        
    Returns:
        dict: Credit risk score and factors
    """
    try:
        logger.info(f"Assessing credit risk for customer {customer_id}")
        
        # Get customer data
        customer_data = get_customer_data(customer_id)
        
        # Run credit risk model
        risk_score = credit_model.calculate_risk(
            customer_data=customer_data,
            transaction_amount=transaction_amount
        )
        
        return {
            "risk_score": risk_score,
            "factors": credit_model.get_factors(),
            "recommendations": credit_model.get_recommendations()
        }
    except Exception as e:
        logger.error(f"Error in credit risk assessment: {str(e)}")
        raise

@mcp.tool()
def assess_market_risk(transaction_amount: float, product_type: str) -> dict:
    """
    Assess market risk for the transaction.
    
    Args:
        transaction_amount (float): Transaction amount
        product_type (str): Type of financial product
        
    Returns:
        dict: Market risk score and exposure details
    """
    try:
        logger.info(f"Assessing market risk for {product_type} transaction")
        
        # Get current market data
        market_data = api_client.get_market_data(product_type)
        
        # Run market risk model
        risk_score = market_model.calculate_risk(
            transaction_amount=transaction_amount,
            product_type=product_type,
            market_data=market_data
        )
        
        return {
            "risk_score": risk_score,
            "exposure_details": market_model.get_exposure_details(),
            "recommendations": market_model.get_recommendations()
        }
    except Exception as e:
        logger.error(f"Error in market risk assessment: {str(e)}")
        raise

@mcp.tool()
def assess_operational_risk(customer_id: str) -> dict:
    """
    Assess operational risk factors.
    
    Args:
        customer_id (str): Customer identifier
        
    Returns:
        dict: Operational risk score and mitigation strategies
    """
    try:
        logger.info(f"Assessing operational risk for customer {customer_id}")
        
        # Get operational data
        operational_data = db_client.get_operational_metrics(customer_id)
        
        # Run operational risk model
        risk_score = operational_model.calculate_risk(operational_data)
        
        return {
            "risk_score": risk_score,
            "mitigation_strategies": operational_model.get_mitigation_strategies(),
            "alerts": operational_model.get_alerts()
        }
    except Exception as e:
        logger.error(f"Error in operational risk assessment: {str(e)}")
        raise

@mcp.tool()
def comprehensive_risk_assessment(input_data: RiskAssessmentInput) -> RiskAssessmentOutput:
    """
    Perform comprehensive risk assessment across all domains.
    
    Args:
        input_data (RiskAssessmentInput): Input parameters for risk assessment
        
    Returns:
        RiskAssessmentOutput: Unified risk assessment with scores and recommendations
    """
    try:
        logger.info(f"Starting comprehensive risk assessment for {input_data.customer_id}")
        
        # Validate input data
        validate_input_data(input_data)
        
        # Get customer data
        customer_data = get_customer_data(input_data.customer_id)
        
        # Run individual risk assessments
        credit_result = assess_credit_risk(
            input_data.customer_id, 
            input_data.transaction_amount
        )
        
        market_result = assess_market_risk(
            input_data.transaction_amount, 
            input_data.product_type
        )
        
        operational_result = assess_operational_risk(input_data.customer_id)
        
        # Calculate overall risk score (weighted average)
        overall_score = (
            0.4 * credit_result["risk_score"] +
            0.3 * market_result["risk_score"] +
            0.3 * operational_result["risk_score"]
        )
        
        # Aggregate recommendations
        recommendations = []
        recommendations.extend(credit_result.get("recommendations", []))
        recommendations.extend(market_result.get("recommendations", []))
        recommendations.extend(operational_result.get("mitigation_strategies", []))
        
        # Create comprehensive output
        result = RiskAssessmentOutput(
            customer_id=input_data.customer_id,
            overall_risk_score=round(overall_score, 3),
            risk_categories={
                "credit": credit_result["risk_score"],
                "market": market_result["risk_score"],
                "operational": operational_result["risk_score"]
            },
            recommendations=list(set(recommendations)),  # Remove duplicates
            confidence_level=0.95,  # Example confidence level
            timestamp=api_client.get_current_timestamp()
        )
        
        logger.info(f"Risk assessment completed for {input_data.customer_id}")
        return result.dict()
        
    except Exception as e:
        logger.error(f"Error in comprehensive risk assessment: {str(e)}")
        raise

if __name__ == "__main__":
    mcp.run(transport="stdio")