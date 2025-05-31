import unittest
import logging
import time
from datetime import datetime, timedelta
import os
import json
import threading
import pandas as pd
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import uuid
import shutil

from src.data_ingestion.postgres_connector import PostgreSQLConnector
from src.data_ingestion.batch_processor import BatchProcessor
from src.data_ingestion.batch_scheduler import BatchScheduler
from src.data_ingestion.fx_rates_consumer import FXRatesConsumer
from src.storage.duckdb_manager import DuckDBManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestSystemIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment with all components"""
        # Create test directories
        cls.test_dir = 'tests/data'
        if os.path.exists(cls.test_dir):
            shutil.rmtree(cls.test_dir)
        os.makedirs(cls.test_dir, exist_ok=True)
        
        # Initialize test topic for FX rates
        cls.fx_topic = f'fx-rates-test-{uuid.uuid4().hex[:8]}'
        admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
        try:
            new_topic = NewTopic(
                name=cls.fx_topic,
                num_partitions=1,
                replication_factor=1
            )
            admin_client.create_topics([new_topic])
            logger.info(f"Created test topic: {cls.fx_topic}")
        finally:
            admin_client.close()
        
        # Create Kafka producer for FX rates
        cls.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Test data
        cls.test_fx_rates = {
            'USD': 1.0,
            'EUR': 1.1,
            'GBP': 1.25,
            'JPY': 0.0067,
            'timestamp': datetime.now().isoformat()
        }
    
    def setUp(self):
        """Start components for each test"""
        # Initialize DuckDB for analytics with unique file per test
        self.test_id = uuid.uuid4().hex[:8]
        self.duckdb = DuckDBManager(os.path.join(self.test_dir, f'test_{self.test_id}.duckdb'))
        
        # Initialize PostgreSQL connector in test mode (uses SQLite)
        self.pg_conn = PostgreSQLConnector(test_mode=True)
        self.pg_conn._connect()  # Ensure connection is established
        
        # Create test transactions table and add sample data
        cursor = self.pg_conn.get_connection().cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id TEXT PRIMARY KEY,
                user_id TEXT,
                amount DECIMAL(10,2),
                currency TEXT,
                country TEXT,
                transaction_time TIMESTAMP,
                product_category TEXT,
                batch_id TEXT
            )
        """)
        
        # Insert test transactions
        current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
        test_transactions = [
            (
                f"tx-{i}",
                f"user-{i}",
                100.00 + i,
                "EUR",
                "FR",
                current_hour,
                "electronics",  # Added product category
                f"batch-{self.test_id}"  # Added batch ID
            )
            for i in range(5)
        ]
        
        cursor.executemany("""
            INSERT OR REPLACE INTO transactions 
            (transaction_id, user_id, amount, currency, country, transaction_time, 
             product_category, batch_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, test_transactions)
        self.pg_conn.get_connection().commit()
        cursor.close()
        
        # Initialize components
        self.batch_processor = BatchProcessor(
            postgres_connector=self.pg_conn,
            duckdb_manager=self.duckdb,
            test_mode=True
        )
        
        self.fx_consumer = FXRatesConsumer(
            topic=self.fx_topic,
            duckdb_manager=self.duckdb,
            auto_offset_reset='earliest',
            group_id=f'fx-rates-consumer-{self.test_id}'  # Unique consumer group per test
        )
        
        self.scheduler = BatchScheduler(
            batch_processor=self.batch_processor
        )
        
        # Start FX consumer
        self.fx_consumer.start()
        time.sleep(1)  # Wait for consumer to start
        
        # Send initial FX rates
        self.producer.send(self.fx_topic, self.test_fx_rates)
        self.producer.flush()
        time.sleep(1)  # Wait for processing
        
        # Start batch scheduler
        self.scheduler.start()
        time.sleep(1)  # Wait for scheduler to start
    
    def tearDown(self):
        """Stop components after each test"""
        # Stop scheduler first
        if hasattr(self, 'scheduler'):
            self.scheduler.stop()
            time.sleep(1)  # Wait for cleanup
        
        # Stop FX consumer
        if hasattr(self, 'fx_consumer'):
            self.fx_consumer.stop()
            time.sleep(1)  # Wait for cleanup
            
        # Close connections
        if hasattr(self, 'pg_conn'):
            self.pg_conn.close()
            
        if hasattr(self, 'duckdb'):
            self.duckdb.close()
            
        # Clean up test file
        try:
            os.remove(os.path.join(self.test_dir, f'test_{self.test_id}.duckdb'))
        except:
            pass
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment"""
        # Close producer
        try:
            if hasattr(cls, 'producer'):
                cls.producer.close()
        except:
            pass
        
        # Delete test topic
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
            admin_client.delete_topics([cls.fx_topic])
            admin_client.close()
        except:
            pass
        
        # Clean up test directory
        try:
            shutil.rmtree(cls.test_dir)
        except:
            pass
    
    def test_end_to_end_flow(self):
        """Test complete data flow from ingestion to analytics"""
        # Wait for initial processing
        time.sleep(2)
        
        # Verify FX rates are received
        latest_rates = self.fx_consumer.get_latest_rates()
        self.assertTrue(len(latest_rates) > 0)
        self.assertEqual(latest_rates['EUR'], self.test_fx_rates['EUR'])
        
        # Get analytics metrics
        metrics = self.duckdb.get_revenue_metrics(hours=1)
        
        # Verify metrics structure
        self.assertIn('total_revenue', metrics)
        self.assertIn('revenue_by_country', metrics)
        self.assertIn('revenue_by_currency', metrics)
        self.assertIn('hourly_trends', metrics)
        
        # Verify FX rate history
        fx_trends = self.duckdb.get_fx_trends('EUR', hours=1)
        self.assertTrue(len(fx_trends) > 0)
        self.assertEqual(fx_trends[0]['rate'], self.test_fx_rates['EUR'])
    
    def test_transaction_processing(self):
        """Test transaction processing with FX conversion"""
        logger.info("Starting test_transaction_processing")
        
        # Wait for initial setup
        time.sleep(2)
        
        # Process a batch manually
        batch_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
        logger.info(f"Processing batch for hour: {batch_hour}")
        success = self.batch_processor.process_hourly_batch(batch_hour)
        self.assertTrue(success)
        
        # Wait for processing
        time.sleep(2)
        
        # Verify processed transactions
        metrics = self.duckdb.get_revenue_metrics(hours=1)
        logger.info(f"Revenue metrics: {metrics}")
        
        # We expect 5 transactions of ~100 EUR each, converted to USD at rate 1.1
        # So total revenue should be around 550 USD
        self.assertGreater(metrics['total_revenue'], 0)
        self.assertEqual(len(metrics['revenue_by_country']), 1)  # Only FR
        self.assertEqual(len(metrics['revenue_by_currency']), 1)  # Only EUR
        
        # Verify transaction count
        total_transactions = sum(trend['transactions'] for trend in metrics['hourly_trends'])
        self.assertEqual(total_transactions, 5)  # We inserted 5 test transactions
    
    def test_fx_rate_updates(self):
        """Test FX rate updates affect transaction processing"""
        logger.info("Starting test_fx_rate_updates")
        
        # Initial FX rates - wait longer for setup
        time.sleep(3)
        logger.info("Initial setup complete")
        
        # Update FX rates
        new_rates = self.test_fx_rates.copy()
        new_rates['EUR'] = 1.2  # Changed EUR rate
        new_rates['timestamp'] = datetime.now().isoformat()
        logger.info(f"Sending new FX rates: {new_rates}")
        
        # Send multiple times to ensure delivery
        for _ in range(3):
            self.producer.send(self.fx_topic, new_rates)
        self.producer.flush()
        logger.info("Sent new FX rates to Kafka")
        
        # Wait longer for processing
        time.sleep(3)
        
        # Verify new rates are received with retries
        max_retries = 3
        retry_delay = 1
        latest_rates = None
        
        for attempt in range(max_retries):
            latest_rates = self.fx_consumer.get_latest_rates()
            logger.info(f"Attempt {attempt + 1}: Latest rates = {latest_rates}")
            if latest_rates.get('EUR') == 1.2:
                logger.info("Found expected EUR rate")
                break
            logger.info(f"EUR rate not found, waiting {retry_delay}s before retry")
            time.sleep(retry_delay)
            retry_delay *= 2
        
        self.assertEqual(latest_rates.get('EUR'), 1.2, 
                        f"FX rate not updated after {max_retries} attempts. Got {latest_rates}")
        
        # Process transactions with new rates
        batch_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
        logger.info(f"Processing batch for hour: {batch_hour}")
        success = self.batch_processor.process_hourly_batch(batch_hour)
        self.assertTrue(success)
        logger.info("Batch processing complete")
        
        # Wait for processing
        time.sleep(2)
        
        # Verify FX history shows rate change with retries
        max_retries = 3
        retry_delay = 1
        fx_trends = None
        
        for attempt in range(max_retries):
            fx_trends = self.duckdb.get_fx_trends('EUR', hours=1)
            logger.info(f"Attempt {attempt + 1}: FX trends = {fx_trends}")
            if any(trend['rate'] == 1.2 for trend in fx_trends):
                logger.info("Found new rate in FX trends")
                break
            logger.info(f"New rate not found in trends, waiting {retry_delay}s before retry")
            time.sleep(retry_delay)
            retry_delay *= 2
        
        self.assertTrue(
            any(trend['rate'] == 1.2 for trend in fx_trends),
            f"New FX rate not found in history after {max_retries} attempts. Trends: {fx_trends}"
        )
        logger.info("Test completed successfully")

if __name__ == '__main__':
    unittest.main() 