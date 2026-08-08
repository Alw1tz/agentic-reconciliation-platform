"""
Database Client for Financial Data Access

Provides secure, efficient access to financial data in the database.
"""

import os
from typing import Dict, List, Any
from sqlalchemy import create_engine, text
from contextlib import contextmanager
import logging

logger = logging.getLogger(__name__)

class DatabaseClient:
    """Secure database client with connection pooling"""
    
    def __init__(self):
        self.connection_string = os.getenv("DATABASE_URL")
        self.engine = None
        self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize database connection with proper configuration."""
        try:
            self.engine = create_engine(
                self.connection_string,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            logger.info("Database connection initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize database connection: {str(e)}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Get database connection with proper context management."""
        connection = None
        try:
            connection = self.engine.connect()
            yield connection
        except Exception as e:
            logger.error(f"Database connection error: {str(e)}")
            raise
        finally:
            if connection:
                connection.close()
    
    def get_customer_profile(self, customer_id: str) -> Dict[str, Any]:
        """Retrieve customer profile information."""
        try:
            with self.get_connection() as conn:
                query = text("""
                    SELECT 
                        c.customer_id,
                        c.name,
                        c.income,
                        c.credit_score,
                        c.total_debt,
                        c.risk_category
                    FROM customers c
                    WHERE c.customer_id = :customer_id
                """)
                
                result = conn.execute(query, {"customer_id": customer_id})
                row = result.fetchone()
                
                if not row:
                    raise ValueError(f"Customer {customer_id} not found")
                
                return {
                    "customer_id": row.customer_id,
                    "name": row.name,
                    "income": row.income,
                    "credit_score": row.credit_score,
                    "total_debt": row.total_debt,
                    "risk_category": row.risk_category
                }
        except Exception as e:
            logger.error(f"Error retrieving customer profile: {str(e)}")
            raise
    
    def get_transaction_history(self, customer_id: str) -> List[Dict[str, Any]]:
        """Retrieve customer transaction history."""
        try:
            with self.get_connection() as conn:
                query = text("""
                    SELECT 
                        t.transaction_id,
                        t.amount,
                        t.date,
                        t.status,
                        t.on_time
                    FROM transactions t
                    WHERE t.customer_id = :customer_id
                    ORDER BY t.date DESC
                    LIMIT 100
                """)
                
                result = conn.execute(query, {"customer_id": customer_id})
                return [
                    {
                        "transaction_id": row.transaction_id,
                        "amount": row.amount,
                        "date": row.date.isoformat() if row.date else None,
                        "status": row.status,
                        "on_time": row.on_time
                    }
                    for row in result.fetchall()
                ]
        except Exception as e:
            logger.error(f"Error retrieving transaction history: {str(e)}")
            raise
    
    def get_operational_metrics(self, customer_id: str) -> Dict[str, Any]:
        """Retrieve operational metrics for risk assessment."""
        try:
            with self.get_connection() as conn:
                query = text("""
                    SELECT 
                        o.customer_id,
                        o.compliance_score,
                        o.suspicious_activity_count,
                        o.verification_status
                    FROM operational_metrics o
                    WHERE o.customer_id = :customer_id
                """)
                
                result = conn.execute(query, {"customer_id": customer_id})
                row = result.fetchone()
                
                if not row:
                    return {}
                
                return {
                    "compliance_score": row.compliance_score,
                    "suspicious_activity_count": row.suspicious_activity_count,
                    "verification_status": row.verification_status
                }
        except Exception as e:
            logger.error(f"Error retrieving operational metrics: {str(e)}")
            raise