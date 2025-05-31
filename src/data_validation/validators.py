import re
from datetime import datetime
from typing import Dict, Any, List
import pandas as pd
from pydantic import BaseModel, validator
import logging

logger = logging.getLogger(__name__)

class Transaction(BaseModel):
    """Pydantic model for transaction validation"""
    transaction_id: str
    user_id: str
    country: str
    currency: str
    amount: float
    product_category: str
    transaction_time: datetime
    batch_id: str

    @validator('transaction_id')
    def validate_transaction_id(cls, v):
        if not v or not isinstance(v, str):
            raise ValueError('Transaction ID must be a non-empty string')
        if not re.match(r'^TX-\d{10,}-\d{4}$', v):
            raise ValueError('Invalid transaction ID format')
        return v

    @validator('country')
    def validate_country(cls, v):
        if not v or not isinstance(v, str):
            raise ValueError('Country must be a non-empty string')
        if not re.match(r'^[A-Z]{2,3}$', v):
            raise ValueError('Country must be a 2 or 3 letter code')
        return v

    @validator('currency')
    def validate_currency(cls, v):
        if not v or not isinstance(v, str):
            raise ValueError('Currency must be a non-empty string')
        if not re.match(r'^[A-Z]{3}$', v):
            raise ValueError('Currency must be a 3 letter code')
        return v

    @validator('amount')
    def validate_amount(cls, v):
        if v <= 0:
            raise ValueError('Amount must be positive')
        if v > 1000000:  # Example threshold
            raise ValueError('Amount exceeds maximum allowed')
        return v

class TransactionValidator:
    """Validates transaction data"""
    def __init__(self):
        self.validation_errors = []
        
    def validate_transaction(self, transaction: Dict[str, Any]) -> bool:
        """Validate a single transaction"""
        try:
            Transaction(**transaction)
            return True
        except Exception as e:
            self.validation_errors.append({
                'transaction_id': transaction.get('transaction_id', 'unknown'),
                'error': str(e)
            })
            return False

    def validate_batch(self, transactions: List[Dict[str, Any]]) -> pd.DataFrame:
        """Validate a batch of transactions"""
        valid_transactions = []
        
        for tx in transactions:
            try:
                validated_tx = Transaction(**tx)
                valid_transactions.append(validated_tx.dict())
            except Exception as e:
                logger.warning(f"Invalid transaction {tx.get('transaction_id', 'unknown')}: {e}")
                self.validation_errors.append({
                    'transaction_id': tx.get('transaction_id', 'unknown'),
                    'error': str(e)
                })
        
        return pd.DataFrame(valid_transactions) if valid_transactions else pd.DataFrame()

    def get_validation_report(self) -> Dict:
        """Get validation error report"""
        return {
            'total_errors': len(self.validation_errors),
            'errors': self.validation_errors
        }

class FXRateValidator:
    """Validates FX rate data"""
    def __init__(self):
        self.validation_errors = []
        self.known_currencies = set(['USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD', 'CHF', 'CNY', 'INR'])
        
    def validate_rate(self, currency: str, rate: float, timestamp: datetime) -> bool:
        """Validate a single FX rate"""
        try:
            if not currency in self.known_currencies:
                raise ValueError(f"Unknown currency: {currency}")
                
            if rate <= 0:
                raise ValueError(f"Invalid rate: {rate}")
                
            if timestamp > datetime.now():
                raise ValueError(f"Future timestamp not allowed: {timestamp}")
                
            return True
            
        except Exception as e:
            self.validation_errors.append({
                'currency': currency,
                'error': str(e)
            })
            return False
    
    def validate_rates_batch(self, rates: Dict[str, float]) -> Dict[str, float]:
        """Validate a batch of FX rates"""
        valid_rates = {}
        
        for currency, rate in rates.items():
            try:
                if self.validate_rate(currency, rate, datetime.now()):
                    valid_rates[currency] = rate
            except Exception as e:
                logger.warning(f"Invalid FX rate for {currency}: {e}")
                
        return valid_rates

    def get_validation_report(self) -> Dict:
        """Get validation error report"""
        return {
            'total_errors': len(self.validation_errors),
            'errors': self.validation_errors
        } 