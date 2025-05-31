import json
import logging
from kafka import KafkaConsumer
from typing import Dict, Optional, Callable
from datetime import datetime
import threading
import time
from ..storage.duckdb_manager import DuckDBManager

logger = logging.getLogger(__name__)

class FXRatesConsumer:
    def __init__(
        self,
        bootstrap_servers: str = 'localhost:9092',
        topic: str = 'fx-rates',
        group_id: str = 'fx-rates-consumer',
        duckdb_manager: Optional[DuckDBManager] = None,
        auto_offset_reset: str = 'latest'
    ):
        """Initialize FX rates consumer
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic to consume from
            group_id: Consumer group ID
            duckdb_manager: Optional DuckDBManager instance
            auto_offset_reset: Where to start consuming messages from
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.duckdb = duckdb_manager or DuckDBManager()
        self.consumer = None
        self.running = False
        self.consumer_thread = None
        self._last_rates: Dict[str, float] = {}
        self._lock = threading.Lock()
        
    def _setup_consumer(self) -> None:
        """Set up the Kafka consumer"""
        try:
            if self.consumer:
                try:
                    self.consumer.close()
                except:
                    pass
                self.consumer = None

            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset=self.auto_offset_reset,
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                enable_auto_commit=True,
                consumer_timeout_ms=1000,  # 1 second timeout
                max_poll_interval_ms=10000,  # 10 seconds
                session_timeout_ms=10000,  # 10 seconds
                request_timeout_ms=11000,  # 11 seconds
                heartbeat_interval_ms=3000  # 3 seconds
            )
            logger.info(f"Connected to Kafka topic: {self.topic}")
        except Exception as e:
            logger.error(f"Failed to setup Kafka consumer: {e}")
            raise
    
    def start(self, callback: Optional[Callable[[Dict], None]] = None) -> None:
        """Start consuming FX rates
        
        Args:
            callback: Optional callback function to process rates
        """
        if self.running:
            logger.warning("Consumer is already running")
            return
            
        self.running = True
        
        def consume_messages():
            try:
                self._setup_consumer()
                
                while self.running:
                    try:
                        # Poll for messages with timeout
                        messages = self.consumer.poll(timeout_ms=1000)
                        
                        if not messages:
                            continue
                            
                        for topic_partition, records in messages.items():
                            for record in records:
                                if not self.running:
                                    return
                                    
                                try:
                                    fx_data = record.value
                                    
                                    # Store rates
                                    with self._lock:
                                        self._last_rates = {
                                            k: float(v) for k, v in fx_data.items() 
                                            if k != 'timestamp'
                                        }
                                    
                                    # Store in DuckDB
                                    self.duckdb.insert_fx_rates(fx_data)
                                    
                                    # Call callback if provided
                                    if callback:
                                        callback(fx_data)
                                        
                                    logger.debug(f"Processed FX rates: {fx_data}")
                                    
                                except Exception as e:
                                    logger.error(f"Error processing FX rate message: {e}")
                                    continue
                                    
                    except Exception as e:
                        if self.running:  # Only log if we're still supposed to be running
                            logger.error(f"Error polling messages: {e}")
                            time.sleep(1)  # Brief pause before retry
                        
            except Exception as e:
                if self.running:  # Only log if we're still supposed to be running
                    logger.error(f"Consumer thread error: {e}")
            finally:
                if self.consumer:
                    try:
                        self.consumer.close(autocommit=False)
                    except:
                        pass
                    self.consumer = None
        
        # Start consumer thread
        self.consumer_thread = threading.Thread(target=consume_messages)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        logger.info("FX rates consumer started")
    
    def stop(self) -> None:
        """Stop consuming FX rates"""
        logger.info("Stopping FX rates consumer...")
        self.running = False
        
        if self.consumer:
            try:
                # Close consumer first to stop network operations
                self.consumer.close(autocommit=False)
            except:
                pass
            self.consumer = None
        
        if self.consumer_thread:
            try:
                self.consumer_thread.join(timeout=5)  # Wait up to 5 seconds
            except:
                pass
            
        logger.info("FX rates consumer stopped")
    
    def get_latest_rates(self) -> Dict[str, float]:
        """Get the latest FX rates
        
        Returns:
            Dict[str, float]: Currency code to rate mapping
        """
        with self._lock:
            return self._last_rates.copy()
    
    def __enter__(self):
        """Context manager entry"""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop() 