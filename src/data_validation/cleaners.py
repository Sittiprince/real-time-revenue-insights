from typing import Dict, Any, List
import pandas as pd
import numpy as np
from datetime import datetime
import pytz
import logging
import re

logger = logging.getLogger(__name__)

class DataCleaner:
    """Handles data cleaning and standardization"""
    
    def __init__(self):
        self.country_code_map = {
            'USA': 'US', 'United States': 'US', 'America': 'US',
            'UK': 'GB', 'United Kingdom': 'GB', 'Britain': 'GB',
            'Germany': 'DE', 'Deutschland': 'DE',
            'France': 'FR', 'Francia': 'FR',
            'Japan': 'JP', 'Japon': 'JP',
            # Add more mappings as needed
        }
        
        self.currency_code_map = {
            'USD': 'USD', 'US Dollar': 'USD', '$': 'USD',
            'EUR': 'EUR', 'Euro': 'EUR', '€': 'EUR',
            'GBP': 'GBP', 'British Pound': 'GBP', '£': 'GBP',
            'JPY': 'JPY', 'Japanese Yen': 'JPY', '¥': 'JPY',
            # Add more mappings as needed
        }
    
    def clean_transaction(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """Clean a single transaction"""
        cleaned = transaction.copy()
        
        # Standardize country codes
        if 'country' in cleaned:
            cleaned['country'] = self._standardize_country_code(cleaned['country'])
            
        # Standardize currency codes
        if 'currency' in cleaned:
            cleaned['currency'] = self._standardize_currency_code(cleaned['currency'])
            
        # Clean amount
        if 'amount' in cleaned:
            cleaned['amount'] = self._clean_amount(cleaned['amount'])
            
        # Ensure proper timestamp
        if 'transaction_time' in cleaned:
            cleaned['transaction_time'] = self._clean_timestamp(cleaned['transaction_time'])
            
        # Clean product category
        if 'product_category' in cleaned:
            cleaned['product_category'] = self._clean_product_category(cleaned['product_category'])
            
        # Clean user_id
        if 'user_id' in cleaned:
            cleaned['user_id'] = self._clean_user_id(cleaned['user_id'])
            
        return cleaned
    
    def clean_batch(self, transactions: List[Dict[str, Any]]) -> pd.DataFrame:
        """Clean a batch of transactions"""
        cleaned_transactions = [self.clean_transaction(tx) for tx in transactions]
        df = pd.DataFrame(cleaned_transactions)
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['transaction_id'], keep='first')
        
        # Sort by timestamp
        if 'transaction_time' in df.columns:
            df = df.sort_values('transaction_time')
            
        return df
    
    def _standardize_country_code(self, country: str) -> str:
        """Standardize country codes to ISO format"""
        if not country:
            return 'UNKNOWN'
            
        country = str(country).strip()
        # First try direct lookup
        mapped = self.country_code_map.get(country.upper())
        if mapped:
            return mapped
            
        # Try lookup with original case
        mapped = self.country_code_map.get(country)
        if mapped:
            return mapped
            
        # If it's already a 2-3 letter code, return it
        if re.match(r'^[A-Z]{2,3}$', country.upper()):
            return country.upper()
            
        return country.upper()
    
    def _standardize_currency_code(self, currency: str) -> str:
        """Standardize currency codes to ISO format"""
        if not currency:
            return 'UNKNOWN'
            
        currency = str(currency).strip()
        # First try direct lookup
        mapped = self.currency_code_map.get(currency.upper())
        if mapped:
            return mapped
            
        # Try lookup with original case
        mapped = self.currency_code_map.get(currency)
        if mapped:
            return mapped
            
        # If it's already a 3 letter code, return it
        if re.match(r'^[A-Z]{3}$', currency.upper()):
            return currency.upper()
            
        return currency.upper()
    
    def _clean_amount(self, amount: Any) -> float:
        """Clean and validate amount"""
        try:
            if isinstance(amount, str):
                # Remove currency symbols and commas
                amount = amount.replace(',', '').strip('$€£¥')
            
            amount = float(amount)
            
            # Handle negative amounts
            amount = abs(amount)
            
            # Handle obviously wrong amounts (e.g., too many zeros)
            if amount > 1e8:  # 100 million threshold
                amount = amount / 100  # Assume wrong decimal placement
                
            return round(amount, 2)
            
        except (ValueError, TypeError):
            logger.warning(f"Invalid amount value: {amount}, defaulting to 0")
            return 0.0
    
    def _clean_timestamp(self, timestamp: Any) -> datetime:
        """Clean and standardize timestamp"""
        try:
            if isinstance(timestamp, str):
                # Try multiple formats
                for fmt in [
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%dT%H:%M:%S',
                    '%Y-%m-%d %H:%M:%S.%f',
                    '%Y-%m-%dT%H:%M:%S.%f'
                ]:
                    try:
                        timestamp = datetime.strptime(timestamp, fmt)
                        break
                    except ValueError:
                        continue
                        
            if isinstance(timestamp, datetime):
                # Ensure UTC timezone
                if timestamp.tzinfo is None:
                    timestamp = pytz.UTC.localize(timestamp)
                return timestamp
                
            raise ValueError(f"Unable to parse timestamp: {timestamp}")
            
        except Exception as e:
            logger.warning(f"Invalid timestamp: {timestamp}, using current time. Error: {e}")
            return datetime.now(pytz.UTC)
    
    def _clean_product_category(self, category: str) -> str:
        """Clean and standardize product category"""
        if not category:
            return 'UNKNOWN'
            
        category = str(category).strip().title()
        
        # Map common variations
        category_map = {
            'Electronics': ['Electronic', 'Electron', 'Tech'],
            'Clothing': ['Clothes', 'Apparel', 'Fashion'],
            'Food': ['Grocery', 'Groceries', 'Foods'],
            'Books': ['Book', 'Reading', 'Literature'],
            # Add more mappings as needed
        }
        
        for standard, variations in category_map.items():
            if category in variations or standard in category:
                return standard
                
        return category
    
    def _clean_user_id(self, user_id: str) -> str:
        """Clean and standardize user ID"""
        if not user_id:
            return 'UNKNOWN'
            
        # Remove any whitespace and special characters
        user_id = str(user_id).strip().replace(' ', '_')
        user_id = ''.join(c for c in user_id if c.isalnum() or c in '_-')
        
        return user_id 