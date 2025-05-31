import unittest
from datetime import datetime
import pytz
from src.data_validation import TransactionValidator, FXRateValidator, DataCleaner

class TestDataValidation(unittest.TestCase):
    def setUp(self):
        self.tx_validator = TransactionValidator()
        self.fx_validator = FXRateValidator()
        self.cleaner = DataCleaner()
        
        # Sample valid transaction
        self.valid_transaction = {
            'transaction_id': 'TX-1234567890-1234',
            'user_id': 'user123',
            'country': 'US',
            'currency': 'USD',
            'amount': 100.00,
            'product_category': 'Electronics',
            'transaction_time': datetime.now(pytz.UTC),
            'batch_id': 'batch123'
        }
        
        # Sample invalid transaction
        self.invalid_transaction = {
            'transaction_id': 'invalid-id',
            'user_id': '',
            'country': 'United States',  # Invalid format
            'currency': '$',  # Invalid format
            'amount': -100,  # Negative amount
            'product_category': '',
            'transaction_time': 'invalid-date',
            'batch_id': ''
        }
    
    def test_transaction_validation_valid(self):
        """Test validation of a valid transaction"""
        result = self.tx_validator.validate_transaction(self.valid_transaction)
        self.assertTrue(result)
        self.assertEqual(len(self.tx_validator.validation_errors), 0)
    
    def test_transaction_validation_invalid(self):
        """Test validation of an invalid transaction"""
        result = self.tx_validator.validate_transaction(self.invalid_transaction)
        self.assertFalse(result)
        self.assertGreater(len(self.tx_validator.validation_errors), 0)
    
    def test_transaction_batch_validation(self):
        """Test batch validation of transactions"""
        transactions = [
            self.valid_transaction,
            self.invalid_transaction,
            self.valid_transaction.copy()
        ]
        df = self.tx_validator.validate_batch(transactions)
        self.assertEqual(len(df), 2)  # Only valid transactions should remain
    
    def test_fx_rate_validation_valid(self):
        """Test validation of valid FX rates"""
        valid_rates = {
            'USD': 1.0,
            'EUR': 1.1,
            'GBP': 1.25
        }
        result = self.fx_validator.validate_rates_batch(valid_rates)
        self.assertEqual(len(result), 3)
    
    def test_fx_rate_validation_invalid(self):
        """Test validation of invalid FX rates"""
        invalid_rates = {
            'USD': 1.0,
            'INVALID': 1.1,  # Invalid currency code
            'GBP': -1.25     # Invalid rate
        }
        result = self.fx_validator.validate_rates_batch(invalid_rates)
        self.assertEqual(len(result), 1)  # Only USD should be valid
    
    def test_data_cleaning_country_codes(self):
        """Test country code standardization"""
        countries = {
            'USA': 'US',
            'United States': 'US',
            'UK': 'GB',
            'Britain': 'GB',
            'Deutschland': 'DE'
        }
        for input_code, expected in countries.items():
            cleaned = self.cleaner._standardize_country_code(input_code)
            self.assertEqual(cleaned, expected)
    
    def test_data_cleaning_currency_codes(self):
        """Test currency code standardization"""
        currencies = {
            '$': 'USD',
            'USD': 'USD',
            '€': 'EUR',
            'Euro': 'EUR',
            '£': 'GBP'
        }
        for input_code, expected in currencies.items():
            cleaned = self.cleaner._standardize_currency_code(input_code)
            self.assertEqual(cleaned, expected)
    
    def test_data_cleaning_amounts(self):
        """Test amount cleaning"""
        amounts = {
            '$100.00': 100.00,
            '1,000.50': 1000.50,
            '-50.25': 50.25,  # Should convert to positive
            '1000000000': 10000000.00  # Should handle large numbers
        }
        for input_amount, expected in amounts.items():
            cleaned = self.cleaner._clean_amount(input_amount)
            self.assertEqual(cleaned, expected)
    
    def test_data_cleaning_timestamps(self):
        """Test timestamp cleaning"""
        # Test valid timestamp
        valid_ts = '2025-01-01 12:00:00'
        cleaned_ts = self.cleaner._clean_timestamp(valid_ts)
        self.assertTrue(isinstance(cleaned_ts, datetime))
        self.assertEqual(cleaned_ts.year, 2025)
        
        # Test invalid timestamp
        invalid_ts = 'invalid'
        cleaned_ts = self.cleaner._clean_timestamp(invalid_ts)
        self.assertTrue(isinstance(cleaned_ts, datetime))
        self.assertEqual(cleaned_ts.tzinfo, pytz.UTC)
    
    def test_full_transaction_cleaning(self):
        """Test complete transaction cleaning process"""
        dirty_transaction = {
            'transaction_id': 'TX-1234567890-1234',
            'user_id': 'user 123!@#',  # Contains special chars
            'country': 'United States',  # Full name instead of code
            'currency': '$',  # Symbol instead of code
            'amount': '$1,234.56',  # Formatted amount
            'product_category': 'electronic',  # Uncategorized
            'transaction_time': '2025-01-01 12:00:00',
            'batch_id': 'batch123'
        }
        
        cleaned = self.cleaner.clean_transaction(dirty_transaction)
        
        self.assertEqual(cleaned['country'], 'US')
        self.assertEqual(cleaned['currency'], 'USD')
        self.assertEqual(cleaned['amount'], 1234.56)
        self.assertEqual(cleaned['user_id'], 'user_123')
        self.assertEqual(cleaned['product_category'], 'Electronics')
        self.assertTrue(isinstance(cleaned['transaction_time'], datetime))
    
    def test_batch_cleaning(self):
        """Test batch cleaning with duplicates"""
        transactions = [
            self.valid_transaction,
            self.valid_transaction.copy(),  # Duplicate
            self.invalid_transaction
        ]
        
        cleaned_df = self.cleaner.clean_batch(transactions)
        
        # Should remove duplicates
        self.assertEqual(len(cleaned_df), 2)
        
        # Should be sorted by timestamp
        if 'transaction_time' in cleaned_df.columns:
            self.assertTrue(cleaned_df['transaction_time'].is_monotonic_increasing)

if __name__ == '__main__':
    unittest.main() 