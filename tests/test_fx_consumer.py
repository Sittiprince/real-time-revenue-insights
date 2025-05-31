import unittest
import json
import threading
import time
from datetime import datetime
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from src.data_ingestion.fx_rates_consumer import FXRatesConsumer
from src.storage.duckdb_manager import DuckDBManager
import logging
import os
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestFXRatesConsumer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment"""
        # Ensure test data directory exists
        os.makedirs('tests/data', exist_ok=True)
        
        # Generate unique test topic
        cls.test_topic = f'fx-rates-test-{uuid.uuid4().hex[:8]}'
        
        # Create admin client to create test topic
        admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
        
        try:
            # Create test topic
            new_topic = NewTopic(
                name=cls.test_topic,
                num_partitions=1,
                replication_factor=1
            )
            admin_client.create_topics([new_topic])
            logger.info(f"Created test topic: {cls.test_topic}")
        except Exception as e:
            logger.error(f"Error creating topic: {e}")
        finally:
            admin_client.close()
        
        # Initialize DuckDB manager for testing
        cls.duckdb = DuckDBManager('tests/data/test_fx.duckdb')
        
        # Create test producer
        cls.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Test data
        cls.test_rates = {
            'USD': 1.0,
            'EUR': 1.1,
            'GBP': 1.25,
            'JPY': 0.0067,
            'timestamp': datetime.now().isoformat()
        }
    
    def setUp(self):
        """Set up test case"""
        self.received_data = []
        self.consumer = FXRatesConsumer(
            topic=self.test_topic,
            duckdb_manager=self.duckdb,
            auto_offset_reset='earliest'
        )
    
    def tearDown(self):
        """Clean up after test"""
        if hasattr(self, 'consumer'):
            self.consumer.stop()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment"""
        try:
            # Delete test topic
            admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
            admin_client.delete_topics([cls.test_topic])
            admin_client.close()
        except:
            pass
            
        try:
            cls.producer.close()
        except:
            pass
        try:
            cls.duckdb.close()
        except:
            pass
        try:
            os.remove('tests/data/test_fx.duckdb')
        except:
            pass
    
    def test_consumer_receives_messages(self):
        """Test that consumer can receive and process messages"""
        def callback(data):
            self.received_data.append(data)
        
        # Start consumer
        self.consumer.start(callback=callback)
        time.sleep(2)  # Wait for consumer to start
        
        # Send test message
        future = self.producer.send(self.test_topic, self.test_rates)
        self.producer.flush()
        
        # Wait for message to be processed
        max_wait = 10
        start_time = time.time()
        while len(self.received_data) == 0 and time.time() - start_time < max_wait:
            time.sleep(0.1)
        
        # Verify message was received
        self.assertTrue(len(self.received_data) > 0)
        received = self.received_data[0]
        self.assertEqual(received['EUR'], self.test_rates['EUR'])
        self.assertEqual(received['USD'], self.test_rates['USD'])
    
    def test_latest_rates_updated(self):
        """Test that latest rates are updated correctly"""
        # Start consumer
        self.consumer.start()
        time.sleep(2)  # Wait for consumer to start
        
        # Send test message
        self.producer.send(self.test_topic, self.test_rates)
        self.producer.flush()
        
        # Wait for processing
        max_wait = 10
        start_time = time.time()
        while len(self.consumer.get_latest_rates()) == 0 and time.time() - start_time < max_wait:
            time.sleep(0.1)
        
        # Verify rates were updated
        latest_rates = self.consumer.get_latest_rates()
        self.assertEqual(latest_rates['EUR'], self.test_rates['EUR'])
        self.assertEqual(latest_rates['USD'], self.test_rates['USD'])
    
    def test_duckdb_storage(self):
        """Test that FX rates are stored in DuckDB"""
        # Start consumer
        self.consumer.start()
        time.sleep(2)  # Wait for consumer to start
        
        # Send test message
        self.producer.send(self.test_topic, self.test_rates)
        self.producer.flush()
        
        # Wait for processing
        time.sleep(2)
        
        # Query DuckDB
        trends = self.duckdb.get_fx_trends('EUR', hours=1)
        self.assertTrue(len(trends) > 0)
        self.assertEqual(trends[0]['rate'], self.test_rates['EUR'])

if __name__ == '__main__':
    unittest.main() 