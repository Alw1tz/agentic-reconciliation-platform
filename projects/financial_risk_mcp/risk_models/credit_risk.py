"""
Credit Risk Assessment Model

Implements credit risk scoring based on customer history, transaction patterns,
and external data sources.
"""

from typing import Dict, List, Any
from pydantic import BaseModel
import numpy as np

class CreditRiskModel:
    """Credit risk assessment model implementation"""
    
    def __init__(self):
        self.factors = []
        self.recommendations = []
        
    def calculate_risk(self, customer_data: Dict[str, Any], transaction_amount: float) -> float:
        """
        Calculate credit risk score based on customer data.
        
        Args:
            customer_data (dict): Customer profile and history
            transaction_amount (float): Transaction amount
            
        Returns:
            float: Credit risk score (0-100)
        """
        # Extract key metrics from customer data
        credit_score = customer_data.get("credit_reports", {}).get("score", 700)
        transaction_history = customer_data.get("transactions", [])
        
        # Calculate basic factors
        payment_history_score = self._calculate_payment_history(transaction_history)
        debt_to_income_ratio = self._calculate_debt_to_income(customer_data)
        transaction_frequency = len(transaction_history) / 30 if transaction_history else 0
        
        # Weighted score calculation
        risk_score = (
            0.4 * (850 - credit_score) +  # Lower credit score = higher risk
            0.3 * debt_to_income_ratio +  # Higher DTI = higher risk
            0.2 * (100 - payment_history_score) +  # Poor payment history = higher risk
            0.1 * transaction_frequency  # Frequent transactions = lower risk
        )
        
        # Clamp score between 0 and 100
        return max(0, min(100, risk_score))
    
    def _calculate_payment_history(self, transactions: List[Dict]) -> float:
        """Calculate payment history score from transaction data."""
        if not transactions:
            return 50
            
        on_time_payments = sum(1 for t in transactions if t.get('on_time', True))
        total_transactions = len(transactions)
        
        return (on_time_payments / total_transactions) * 100 if total_transactions > 0 else 0
    
    def _calculate_debt_to_income(self, customer_data: Dict[str, Any]) -> float:
        """Calculate debt-to-income ratio."""
        income = customer_data.get("customer_profile", {}).get("income", 1000)
        total_debt = customer_data.get("customer_profile", {}).get("total_debt", 0)
        
        return (total_debt / income) * 100 if income > 0 else 0
    
    def get_factors(self) -> List[str]:
        """Get contributing factors to the risk assessment."""
        return self.factors
    
    def get_recommendations(self) -> List[str]:
        """Get recommendations based on risk assessment."""
        return self.recommendations