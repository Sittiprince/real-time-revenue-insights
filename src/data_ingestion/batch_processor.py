import logging
from datetime import datetime, timedelta
import pandas as pd
from typing import Optional
from .postgres_connector import PostgreSQLConnector
from ..storage.duckdb_manager import DuckDBManager
from ..data_validation import TransactionValidator, DataCleaner

logger = logging.getLogger(__name__)

class BatchProcessor:
    def __init__(
        self,
        postgres_connector: Optional[PostgreSQLConnector] = None,
        duckdb_manager: Optional[DuckDBManager] = None,
        test_mode: bool = False
    ):
        """Initialize the batch processor with database connections
        
        Args:
            postgres_connector: Optional PostgreSQLConnector instance
            duckdb_manager: Optional DuckDBManager instance
            test_mode: If True, use SQLite for testing
        """
        self.pg_conn = postgres_connector or PostgreSQLConnector(test_mode=test_mode)
        self.duckdb = duckdb_manager or DuckDBManager(
            "tests/data/test_analytics.duckdb" if test_mode else "data/analytics.duckdb"
        )
        self.test_mode = test_mode
        self.validator = TransactionValidator()
        self.cleaner = DataCleaner()
        
    def process_hourly_batch(self, batch_hour: Optional[datetime] = None) -> bool:
        """Process a single hourly batch of transactions
        
        Args:
            batch_hour: The hour to process. If None, processes the previous hour
            
        Returns:
            bool: True if processing was successful
        """
        try:
            # Default to previous hour if not specified
            if batch_hour is None:
                batch_hour = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
            
            logger.info(f"Processing batch for hour: {batch_hour}")
            
            # Fetch transactions from PostgreSQL
            transactions_df = self.pg_conn.fetch_hourly_batch(batch_hour)
            
            if transactions_df.empty:
                logger.info("No transactions found for this hour")
                return True
            
            # Convert DataFrame to list of dictionaries for processing
            transactions = transactions_df.to_dict('records')
            
            # Clean the data
            logger.info("Cleaning transaction data...")
            cleaned_df = self.cleaner.clean_batch(transactions)
            
            if cleaned_df.empty:
                logger.warning("No transactions remained after cleaning")
                return True
            
            # Validate the cleaned data
            logger.info("Validating transactions...")
            valid_df = self.validator.validate_batch(cleaned_df.to_dict('records'))
            
            if valid_df.empty:
                logger.error("No valid transactions after validation")
                logger.error(f"Validation report: {self.validator.get_validation_report()}")
                return False
            
            # Convert to DuckDB format
            processed_transactions = []
            now = datetime.now()
            
            for _, row in valid_df.iterrows():
                currency = row['currency']
                amount = float(row['amount'])
                
                # Calculate USD amount
                if self.test_mode:
                    test_fx_rates = {
                        'USD': 1.0,
                        'EUR': 1.1,
                        'GBP': 1.25,
                        'JPY': 0.0067
                    }
                    fx_rate = test_fx_rates.get(currency, 1.0)
                    usd_amount = amount * fx_rate
                else:
                    fx_rate = None
                    usd_amount = None
                
                processed_transactions.append({
                    'transaction_id': row['transaction_id'],
                    'user_id': row['user_id'],
                    'country': row['country'],
                    'original_currency': currency,
                    'original_amount': amount,
                    'usd_amount': usd_amount,
                    'fx_rate': fx_rate,
                    'transaction_date': row['transaction_time'],
                    'processed_at': now
                })
            
            # Insert into DuckDB
            if processed_transactions:
                self.duckdb.insert_revenue_batch(processed_transactions)
                logger.info(f"Successfully processed {len(processed_transactions)} transactions")
                return True
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing hourly batch: {e}")
            logger.exception("Full traceback:")
            return False
    
    def process_backlog(self, hours: int = 24) -> bool:
        """Process multiple hours of historical data
        
        Args:
            hours: Number of past hours to process
            
        Returns:
            bool: True if all batches were processed successfully
        """
        try:
            end_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
            start_hour = end_hour - timedelta(hours=hours)
            
            current_hour = start_hour
            success = True
            
            while current_hour < end_hour:
                batch_success = self.process_hourly_batch(current_hour)
                if not batch_success:
                    logger.error(f"Failed to process batch for hour: {current_hour}")
                    success = False
                current_hour += timedelta(hours=1)
            
            return success
            
        except Exception as e:
            logger.error(f"Error processing backlog: {e}")
            logger.exception("Full traceback:")
            return False
    
    def close(self):
        """Close all database connections"""
        try:
            self.pg_conn.close()
            self.duckdb.close()
        except Exception as e:
            logger.error(f"Error closing connections: {e}")

# Test the batch processor
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    processor = BatchProcessor()
    try:
        # Process last 24 hours of data
        processor.process_backlog(hours=24)
    finally:
        processor.close() 