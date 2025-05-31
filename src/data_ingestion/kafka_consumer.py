import json
from kafka import KafkaConsumer
import logging
import os
from typing import Dict, Optional
from datetime import datetime
from dotenv import load_dotenv
import threading
import time

load_dotenv()

logger = logging.getLogger(__name__)

class FXRateConsumer:
    def __init__(self, bootstrap_servers: str = 'localhost:9092', 
                 topic: str = 'fx-rates'):
        """Initialize Kafka consumer for FX rates"""
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.latest_rates = {'USD': 1.0}  # USD base rate
        self.is_consuming = False
        self.consumer_thread = None
        self.consumer = None
        self._initialize_consumer()
        
    def _initialize_consumer(self):
        """Initialize or reinitialize the Kafka consumer"""
        try:
            if self.consumer:
                self.consumer.close()
            
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True,
                group_id='fx_rate_processor',
                session_timeout_ms=10000,
                heartbeat_interval_ms=3000,
                max_poll_interval_ms=300000,
                request_timeout_ms=30000
            )
            logger.info(f"FX Rate consumer initialized for topic: {self.topic}")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            return False
    
    def start_consuming(self):
        """Start consuming FX rates in background thread"""
        if self.is_consuming:
            logger.info("Consumer already running")
            return
            
        self.is_consuming = True
        self.consumer_thread = threading.Thread(target=self._consume_loop)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        logger.info("Started FX rate consumer")
    
    def _consume_loop(self):
        """Main consumption loop"""
        reconnect_delay = 1  # Start with 1 second delay
        max_reconnect_delay = 60  # Maximum delay between reconnection attempts
        
        while self.is_consuming:
            try:
                if not self.consumer:
                    if not self._initialize_consumer():
                        logger.error(f"Failed to initialize consumer, retrying in {reconnect_delay}s...")
                        time.sleep(reconnect_delay)
                        reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
                        continue
                    reconnect_delay = 1  # Reset delay after successful connection
                
                for message in self.consumer:
                    if not self.is_consuming:
                        break
                        
                    fx_data = message.value
                    # Check if data has required fields
                    if isinstance(fx_data, dict) and 'timestamp' in fx_data and any(k for k in fx_data.keys() if k not in ['timestamp']):
                        # Remove timestamp from rates
                        rates = fx_data.copy()
                        rates.pop('timestamp', None)
                        self.latest_rates.update(rates)  # Update instead of replace
                        
                        logger.info(f"Updated FX rates: EUR={self.latest_rates.get('EUR', 'N/A'):.4f}, GBP={self.latest_rates.get('GBP', 'N/A'):.4f}")
                    else:
                        logger.warning(f"Received invalid FX data format: {fx_data}")
                        
            except Exception as e:
                if "timeout" in str(e).lower():
                    continue
                    
                logger.error(f"Error in FX consumer: {e}")
                
                # Close the consumer on error
                try:
                    if self.consumer:
                        self.consumer.close()
                except:
                    pass
                self.consumer = None
                
                # Wait before attempting to reconnect
                time.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
    
    def get_latest_rates(self) -> Optional[Dict]:
        """Get latest FX rates from cache"""
        return self.latest_rates if self.latest_rates else None
    
    def get_rate(self, currency):
        """Get specific currency rate to USD"""
        return self.latest_rates.get(currency, None)
    
    def stop_consuming(self):
        """Stop the consumer"""
        self.is_consuming = False
        if self.consumer_thread:
            self.consumer_thread.join(timeout=5)
        if self.consumer:
            try:
                self.consumer.close()
            except:
                pass
        logger.info("Stopped FX rate consumer")

    def close(self):
        """Close the Kafka consumer"""
        try:
            self.consumer.close()
            logger.info("Kafka consumer closed")
        except Exception as e:
            logger.error(f"Error closing Kafka consumer: {e}")

    def start(self):
        """Start consuming FX rates"""
        self.start_consuming()  # Start background consumer thread

# Test the consumer
if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Test the consumer
    consumer = FXRateConsumer()
    
    try:
        logger.info("Waiting for FX rate messages...")
        
        while True:
            rates = consumer.get_latest_rates()
            if rates:
                logger.info(f"Received rates for {len(rates)} currencies")
                logger.debug(f"Rates: {rates}")
    
    except KeyboardInterrupt:
        logger.info("\nStopping consumer...")
        consumer.stop_consuming()
        consumer.close()