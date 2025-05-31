from kafka import KafkaProducer
import json
import random
from datetime import datetime
import time
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Base currency rates (relative to USD)
BASE_RATES = {
    'EUR': 0.85,  # Euro
    'GBP': 0.73,  # British Pound
    'JPY': 110.0, # Japanese Yen
    'CAD': 1.25,  # Canadian Dollar
    'AUD': 1.35,  # Australian Dollar
    'CNY': 6.45,  # Chinese Yuan
    'BRL': 5.20,  # Brazilian Real
    'INR': 74.5,  # Indian Rupee
    'CHF': 0.92   # Swiss Franc
}

def generate_fx_rates():
    """Generate FX rates with random fluctuations"""
    current_time = datetime.now().isoformat()
    
    # Add random fluctuations to base rates (±1%)
    rates = {
        currency: round(rate * (1 + random.uniform(-0.01, 0.01)), 4)
        for currency, rate in BASE_RATES.items()
    }
    
    # Always include USD base rate
    rates['USD'] = 1.0
    
    # Add timestamp
    rates['timestamp'] = current_time
    
    return rates

def run_generator(bootstrap_servers='localhost:9092', topic='fx-rates', interval_seconds=15):
    """Run continuous FX rate generation"""
    logger.info(f"Starting FX rate generator (publishing to topic: {topic})...")
    
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    try:
        while True:
            # Generate and send FX rates
            rates = generate_fx_rates()
            
            producer.send(topic, rates)
            producer.flush()
            
            logger.info(f"Published FX rates for {len(rates)-1} currencies")  # -1 for timestamp
            
            # Wait for next update
            time.sleep(interval_seconds)
            
    except KeyboardInterrupt:
        logger.info("Stopping FX rate generator...")
    except Exception as e:
        logger.error(f"Generator error: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    run_generator() 