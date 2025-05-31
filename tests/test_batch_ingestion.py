import unittest
import logging
from datetime import datetime, timedelta
import pandas as pd
import os
import sys
import sqlite3
import time

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_ingestion.batch_processor import BatchProcessor
from src.data_ingestion.batch_scheduler import BatchScheduler
from src.storage.duckdb_manager import DuckDBManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestBatchIngestion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test databases and sample data"""
        cls.setup_sqlite()
        cls.setup_duckdb()
        
    @classmethod
    def tearDownClass(cls):
        """Clean up test databases"""
        cls.cleanup_sqlite()
        cls.cleanup_duckdb()
    
    @classmethod
    def setup_sqlite(cls):
        """Set up SQLite test database with sample data"""
        try:
            # Create test database directory
            os.makedirs('tests/data', exist_ok=True)
            
            # Connect to SQLite
            conn = sqlite3.connect('tests/data/test.db')
            cursor = conn.cursor()
            
            # Create test transactions
            cursor.execute("DROP TABLE IF EXISTS transactions")
            cursor.execute("""
                CREATE TABLE transactions (
                    transaction_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    amount DECIMAL(10,2) NOT NULL,
                    currency TEXT NOT NULL,
                    country TEXT NOT NULL,
                    transaction_time TIMESTAMP NOT NULL,
                    batch_id TEXT,
                    product_category TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Insert sample data for the last 24 hours
            now = datetime.now()
            sample_data = []
            currencies = ['USD', 'EUR', 'GBP', 'JPY']
            countries = ['US', 'UK', 'DE', 'JP']
            
            for hour in range(24):
                timestamp = now - timedelta(hours=hour)
                for i in range(5):  # 5 transactions per hour
                    sample_data.append((
                        f'TX{hour}{i}',
                        f'USER{i}',
                        100 + i * 10,
                        currencies[i % len(currencies)],
                        countries[i % len(countries)],
                        timestamp.isoformat(),
                        f'BATCH{hour}',
                        'Electronics'
                    ))
            
            # Insert sample data
            cursor.executemany("""
                INSERT INTO transactions (
                    transaction_id, user_id, amount, currency,
                    country, transaction_time, batch_id, product_category
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, sample_data)
            
            conn.commit()
            conn.close()
            logger.info("SQLite test database setup complete")
            
        except Exception as e:
            logger.error(f"Failed to setup SQLite test database: {e}")
            raise
    
    @classmethod
    def cleanup_sqlite(cls):
        """Clean up SQLite test database"""
        try:
            os.remove('tests/data/test.db')
            logger.info("SQLite test database cleaned up")
        except Exception as e:
            logger.error(f"Failed to cleanup SQLite test database: {e}")
    
    @classmethod
    def setup_duckdb(cls):
        """Set up DuckDB test database"""
        cls.duckdb = DuckDBManager("tests/data/test_analytics.duckdb")
        logger.info("DuckDB test database setup complete")
    
    @classmethod
    def cleanup_duckdb(cls):
        """Clean up DuckDB test database"""
        try:
            cls.duckdb.close()
            os.remove("tests/data/test_analytics.duckdb")
            logger.info("DuckDB test database cleaned up")
        except Exception as e:
            logger.error(f"Failed to cleanup DuckDB test database: {e}")
    
    def test_data_ingestion(self):
        """Test the entire data ingestion pipeline"""
        # Create a batch processor
        processor = BatchProcessor(test_mode=True)
        
        try:
            # Process a single batch
            batch_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
            success = processor.process_hourly_batch(batch_hour)
            self.assertTrue(success)
            
            # Verify data in DuckDB
            metrics = processor.duckdb.get_revenue_metrics(hours=1)
            self.assertGreater(len(metrics['revenue_by_country']), 0)
            self.assertGreater(metrics['total_revenue'], 0)
            
            # Test backlog processing
            success = processor.process_backlog(hours=24)
            self.assertTrue(success)
            
            # Verify 24-hour data
            metrics = processor.duckdb.get_revenue_metrics(hours=24)
            self.assertEqual(len(metrics['revenue_by_country']), 4)  # US, UK, DE, JP
            self.assertEqual(len(metrics['revenue_by_currency']), 4)  # USD, EUR, GBP, JPY
            
        finally:
            processor.close()
    
    def test_batch_scheduler(self):
        """Test batch scheduler functionality"""
        scheduler = BatchScheduler(BatchProcessor(test_mode=True))
        
        try:
            # Start scheduler
            scheduler.start()
            
            # Let it run for a few seconds
            time.sleep(5)
            
            # Verify data in DuckDB
            duckdb = DuckDBManager("tests/data/test_analytics.duckdb")
            metrics = duckdb.get_revenue_metrics(hours=24)
            self.assertGreater(len(metrics['revenue_by_country']), 0)
            duckdb.close()
            
        finally:
            scheduler.stop()

if __name__ == '__main__':
    unittest.main() 