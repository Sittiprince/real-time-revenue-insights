import psycopg2
import random
from datetime import datetime, timedelta
import time
import logging
import os
from dotenv import load_dotenv
import socket
import uuid
import pytz

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/transaction_simulator.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Sample data
COUNTRIES = {
    'US': 'USA',
    'GB': 'UK', 
    'DE': 'Germany',
    'FR': 'France',
    'JP': 'Japan',
    'CA': 'Canada',
    'AU': 'Australia',
    'CN': 'China',
    'BR': 'Brazil',
    'IN': 'India'
}

CURRENCIES = {
    'US': 'USD',
    'GB': 'GBP',
    'DE': 'EUR',
    'FR': 'EUR',
    'JP': 'JPY',
    'CA': 'CAD',
    'AU': 'AUD',
    'CN': 'CNY',
    'BR': 'BRL',
    'IN': 'INR'
}

PRODUCT_CATEGORIES = ['Electronics', 'Fashion', 'Home', 'Books', 'Sports', 'Beauty', 'Food', 'Toys']

# Get database configuration from environment variables
DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
DB_NAME = os.getenv('POSTGRES_DB', 'globemart')
DB_USER = os.getenv('POSTGRES_USER', 'dataeng')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'pipeline123')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
UPDATE_INTERVAL = int(os.getenv('UPDATE_INTERVAL', '10'))

def create_tables():
    """Create necessary tables in PostgreSQL"""
    logger.info(f"Connecting to PostgreSQL at {DB_HOST}")
    
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    
    try:
        with conn.cursor() as cur:
            # Create transactions table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id VARCHAR PRIMARY KEY,
                    user_id VARCHAR NOT NULL,
                    country VARCHAR NOT NULL,
                    currency VARCHAR NOT NULL,
                    amount DECIMAL(10,2) NOT NULL,
                    product_category VARCHAR NOT NULL,
                    transaction_time TIMESTAMP WITH TIME ZONE NOT NULL,
                    batch_id VARCHAR NOT NULL
                )
            """)
            
            conn.commit()
            logger.info("Tables created successfully")
            
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        logger.exception("Full traceback:")
        conn.rollback()
        
    finally:
        conn.close()

def generate_transaction():
    """Generate a single random transaction"""
    country_code = random.choice(list(COUNTRIES.keys()))
    currency = CURRENCIES[country_code]
    current_time = datetime.now(pytz.UTC)
    
    # Generate transaction ID in the format TX-XXXXXXXXXX-YYYY
    tx_number = random.randint(1000000000, 9999999999)
    tx_year = current_time.strftime("%Y")
    
    return {
        'transaction_id': f"TX-{tx_number}-{tx_year}",
        'user_id': f"USER-{random.randint(1, 1000)}",
        'country': country_code,
        'currency': currency,
        'amount': round(random.uniform(10, 1000), 2),
        'product_category': random.choice(PRODUCT_CATEGORIES),
        'transaction_time': current_time,
        'batch_id': f"BATCH-{current_time.strftime('%Y%m%d-%H')}"
    }

def insert_transactions(num_transactions=10):
    """Insert a batch of random transactions"""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    
    try:
        transactions = [generate_transaction() for _ in range(num_transactions)]
        
        with conn.cursor() as cur:
            for tx in transactions:
                cur.execute("""
                    INSERT INTO transactions (
                        transaction_id, user_id, country, currency,
                        amount, product_category, transaction_time, batch_id
                    ) VALUES (
                        %(transaction_id)s, %(user_id)s, %(country)s, %(currency)s,
                        %(amount)s, %(product_category)s, %(transaction_time)s, %(batch_id)s
                    )
                """, tx)
            
            conn.commit()
            logger.info(f"Inserted {num_transactions} transactions")
            
    except Exception as e:
        logger.error(f"Error inserting transactions: {e}")
        logger.exception("Full traceback:")
        conn.rollback()
        
    finally:
        conn.close()

def run_generator():
    """Run continuous transaction generation"""
    logger.info(f"Starting transaction generator... (Host: {DB_HOST}, BatchSize: {BATCH_SIZE}, Interval: {UPDATE_INTERVAL}s)")
    
    try:
        create_tables()
        
        while True:
            insert_transactions(BATCH_SIZE)
            time.sleep(UPDATE_INTERVAL)
            
    except KeyboardInterrupt:
        logger.info("Stopping transaction generator...")
    except Exception as e:
        logger.error(f"Generator error: {e}")
        logger.exception("Full traceback:")

if __name__ == "__main__":
    run_generator() 