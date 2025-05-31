import asyncio
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import time
import json
import os
from dotenv import load_dotenv
import pytz

from src.data_ingestion.postgres_connector import PostgreSQLConnector
from src.data_ingestion.kafka_consumer import FXRateConsumer
from src.data_processing.currency_converter import CurrencyConverter
from src.storage.duckdb_manager import DuckDBManager
from src.data_validation import FXRateValidator, TransactionValidator, DataCleaner

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Configure logging handlers if not already configured
if not logger.handlers:
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler
    os.makedirs('logs', exist_ok=True)
    file_handler = logging.FileHandler('logs/pipeline.log', encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

class RealTimeProcessor:
    def __init__(self):
        # Ensure data directory exists
        os.makedirs('data', exist_ok=True)
        os.makedirs('logs', exist_ok=True)
        
        # Get database configuration from environment variables
        db_host = os.getenv('POSTGRES_HOST', 'localhost')
        db_port = int(os.getenv('POSTGRES_PORT', '5432'))
        db_name = os.getenv('POSTGRES_DB', 'globemart')
        db_user = os.getenv('POSTGRES_USER', 'dataeng')
        db_password = os.getenv('POSTGRES_PASSWORD', 'pipeline123')
        
        logger.info(f"Connecting to PostgreSQL at {db_host}")
        self.postgres = PostgreSQLConnector(
            host=db_host,
            port=db_port,
            database=db_name,
            user=db_user,
            password=db_password
        )
        
        self.fx_consumer = FXRateConsumer()
        self.fx_consumer.start()  # Start consuming FX rates immediately
        self.converter = CurrencyConverter()
        
        # Initialize DuckDB with proper configuration
        self.analytics_db = DuckDBManager(
            db_path="data/analytics.duckdb"
        )
        
        self.running = False
        self.last_batch_time = datetime.now(pytz.UTC) - timedelta(hours=1)
        
        # Cache for latest FX rates
        self.latest_fx_rates = {}
        
        # Initialize validators and cleaner
        self.fx_validator = FXRateValidator()
        self.tx_validator = TransactionValidator()
        self.cleaner = DataCleaner()
        
        logger.info("RealTime Processor initialized")
    
    def start(self):
        """Start the real-time processor"""
        logger.info("Starting real-time processor...")
        self.running = True
        
        # Start processing threads
        self.fx_thread = threading.Thread(target=self._process_fx_rates)
        self.tx_thread = threading.Thread(target=self._process_transactions)
        self.agg_thread = threading.Thread(target=self._update_aggregations)
        
        self.fx_thread.daemon = True
        self.tx_thread.daemon = True
        self.agg_thread.daemon = True
        
        self.fx_thread.start()
        self.tx_thread.start()
        self.agg_thread.start()
        
        logger.info("All processor threads started")
        
    def stop(self):
        """Stop the processor"""
        logger.info("Stopping processor...")
        self.running = False
        
        # Wait for threads to finish
        self.fx_thread.join(timeout=5)
        self.tx_thread.join(timeout=5)
        self.agg_thread.join(timeout=5)
        
        # Close connections
        self.fx_consumer.stop()
        self.postgres.close()
        self.analytics_db.close()
        
        logger.info("Processor stopped")
    
    def _process_fx_rates(self):
        """Process incoming FX rates"""
        logger.info("Starting FX rate consumer...")
        
        while self.running:
            try:
                # Get latest rates
                rates = self.fx_consumer.get_latest_rates()
                
                if rates:
                    # Validate rates
                    valid_rates = self.fx_validator.validate_rates_batch(rates)
                    
                    if valid_rates:
                        self.latest_fx_rates.update(valid_rates)
                        self.analytics_db.update_fx_rates(valid_rates)
                        logger.info(f"Updated FX rates: {len(valid_rates)} currencies")
                    else:
                        logger.warning("No valid FX rates in batch")
                        logger.warning(f"Validation report: {self.fx_validator.get_validation_report()}")
                
                time.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                logger.error(f"Error consuming FX rates: {e}")
                time.sleep(5)
    
    def _process_transactions(self):
        """Process transactions in hourly batches"""
        logger.info("Starting transaction processor...")
        
        while self.running:
            try:
                current_time = datetime.now(pytz.UTC)
                logger.info("Checking for new transactions...")
                
                # Process recent transactions first (last 10 minutes)
                self._process_recent_transactions()
                
                # Check if we need to process a new batch (every hour)
                if current_time - self.last_batch_time >= timedelta(hours=1):
                    logger.info("Starting hourly batch processing...")
                    self._process_hourly_batch()
                    self.last_batch_time = current_time
                
                time.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"Error processing transactions: {e}")
                logger.exception("Full traceback:")
                time.sleep(60)
    
    def _process_hourly_batch(self):
        """Process transactions from the last hour"""
        try:
            # Get transactions from last hour
            end_time = datetime.now(pytz.UTC)
            start_time = end_time - timedelta(hours=1)
            
            logger.info(f"Fetching transactions between {start_time} and {end_time}")
            transactions = self.postgres.get_transactions_by_timerange(
                start_time, end_time
            )
            
            if not transactions:
                logger.info("No new transactions to process in hourly batch")
                return
            
            logger.info(f"Processing {len(transactions)} transactions from last hour")
            
            # Clean and validate transactions
            cleaned_transactions = self.cleaner.clean_batch(transactions)
            valid_transactions = self.tx_validator.validate_batch(cleaned_transactions.to_dict('records'))
            
            if valid_transactions.empty:
                logger.error("No valid transactions after validation")
                logger.error(f"Validation report: {self.tx_validator.get_validation_report()}")
                return
            
            # Convert to USD
            processed_transactions = []
            for _, tx in valid_transactions.iterrows():
                try:
                    # Convert to USD using latest rates
                    if not self.latest_fx_rates:
                        logger.warning("No FX rates available, waiting for rates...")
                        return
                        
                    usd_amount, fx_rate = self.converter.convert_to_usd(
                        tx['amount'], tx['currency']
                    )
                    
                    processed_tx = {
                        'transaction_id': tx['transaction_id'],
                        'user_id': tx['user_id'],
                        'country': tx['country'],
                        'original_currency': tx['currency'],
                        'original_amount': float(tx['amount']),
                        'usd_amount': usd_amount,
                        'fx_rate': fx_rate,
                        'transaction_date': tx['transaction_time'],
                        'processed_at': datetime.now(pytz.UTC)
                    }
                    
                    processed_transactions.append(processed_tx)
                    logger.debug(f"Processed transaction {tx['transaction_id']}: {tx['amount']} {tx['currency']} = ${usd_amount:.2f}")
                    
                except Exception as e:
                    logger.error(f"Error processing transaction {tx['transaction_id']}: {e}")
                    continue
            
            # Insert into analytics DB
            if processed_transactions:
                logger.info(f"Inserting {len(processed_transactions)} processed transactions into analytics DB")
                self.analytics_db.insert_revenue_batch(processed_transactions)
                logger.info(f"Successfully stored {len(processed_transactions)} processed transactions")
            
        except Exception as e:
            logger.error(f"Error in hourly batch processing: {e}")
            logger.exception("Full traceback:")
    
    def _process_recent_transactions(self):
        """Process transactions from the last 10 minutes"""
        try:
            # Get transactions from last 10 minutes
            end_time = datetime.now(pytz.UTC)
            start_time = end_time - timedelta(minutes=10)
            
            logger.info(f"Fetching recent transactions between {start_time} and {end_time}")
            transactions = self.postgres.get_transactions_by_timerange(
                start_time, end_time
            )
            
            if not transactions:
                logger.info("No recent transactions found")
                return
            
            logger.info(f"Processing {len(transactions)} recent transactions")
            
            # Clean and validate transactions
            cleaned_transactions = self.cleaner.clean_batch(transactions)
            valid_transactions = self.tx_validator.validate_batch(cleaned_transactions.to_dict('records'))
            
            if valid_transactions.empty:
                logger.warning("No valid transactions after validation")
                return
            
            # Convert to USD
            processed_transactions = []
            for _, tx in valid_transactions.iterrows():
                try:
                    # Convert to USD using latest rates
                    if not self.latest_fx_rates:
                        logger.warning("No FX rates available, waiting for rates...")
                        return
                        
                    usd_amount, fx_rate = self.converter.convert_to_usd(
                        tx['amount'], tx['currency']
                    )
                    
                    processed_tx = {
                        'transaction_id': tx['transaction_id'],
                        'user_id': tx['user_id'],
                        'country': tx['country'],
                        'original_currency': tx['currency'],
                        'original_amount': float(tx['amount']),
                        'usd_amount': usd_amount,
                        'fx_rate': fx_rate,
                        'transaction_date': tx['transaction_time'],
                        'processed_at': datetime.now(pytz.UTC)
                    }
                    
                    processed_transactions.append(processed_tx)
                    
                except Exception as e:
                    logger.error(f"Error processing transaction {tx['transaction_id']}: {e}")
                    continue
            
            if processed_transactions:
                # Store in analytics DB
                self.analytics_db.insert_revenue_batch(processed_transactions)
                logger.info(f"Processed and stored {len(processed_transactions)} transactions")
            
        except Exception as e:
            logger.error(f"Error processing recent transactions: {e}")
            logger.exception("Full traceback:")
    
    def _update_aggregations(self):
        """Update hourly aggregations periodically"""
        logger.info("Starting aggregation updater...")
        
        while self.running:
            try:
                self.analytics_db.update_hourly_aggregations()
                logger.info("Updated hourly aggregations")
                
                # Update every 5 minutes
                time.sleep(300)
                
            except Exception as e:
                logger.error(f"Error updating aggregations: {e}")
                time.sleep(60)
    
    def get_current_metrics(self) -> Dict:
        """Get current revenue metrics"""
        try:
            metrics = self.analytics_db.get_revenue_metrics(24)
            if metrics is None:
                logger.warning("No metrics available, returning empty dict")
                return {
                    'total_revenue': 0,
                    'revenue_by_country': [],
                    'revenue_by_currency': [],
                    'hourly_trends': [],
                    'last_update': datetime.now(pytz.UTC).isoformat(),
                    'fx_rates_count': len(self.latest_fx_rates),
                    'processor_status': 'running' if self.running else 'stopped'
                }
            
            # Add real-time info
            metrics['last_update'] = datetime.now(pytz.UTC).isoformat()
            metrics['fx_rates_count'] = len(self.latest_fx_rates)
            metrics['processor_status'] = 'running' if self.running else 'stopped'
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error getting metrics: {e}")
            return {}
    
    def get_fx_trends(self, currency: str = 'EUR') -> List[Dict]:
        """Get FX rate trends for a currency"""
        try:
            return self.analytics_db.get_fx_trends(currency)
        except Exception as e:
            logger.error(f"Error getting FX trends: {e}")
            return []
    
    def get_top_users(self) -> List[Dict]:
        """Get top spending users"""
        try:
            return self.analytics_db.get_top_users()
        except Exception as e:
            logger.error(f"Error getting top users: {e}")
            return []

# Standalone runner for testing
if __name__ == "__main__":
    # Configure root logger for testing
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    processor = RealTimeProcessor()
    
    try:
        threads = processor.start()
        
        # Keep running
        while True:
            time.sleep(10)
            metrics = processor.get_current_metrics()
            logger.info(f"Current total revenue: ${metrics.get('total_revenue', 0):,.2f}")
            
    except KeyboardInterrupt:
        logger.info("Shutting down processor...")
        processor.stop()
        logger.info("Processor stopped.")