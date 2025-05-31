import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import os
from dotenv import load_dotenv
import logging

load_dotenv()

logger = logging.getLogger(__name__)

class FXRateProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Base exchange rates (realistic values)
        self.base_rates = {
            'EUR': 0.85,
            'GBP': 0.73,
            'CAD': 1.35,
            'JPY': 110.0,
            'AUD': 1.45,
            'CHF': 0.92,
            'SEK': 10.50,
            'NOK': 10.80
        }
    
    def generate_fx_rates(self):
        """Generate realistic FX rates with small variations"""
        current_time = datetime.now()
        
        rates = {}
        
        for currency, base_rate in self.base_rates.items():
            # Add small random variation (±2%)
            variation = random.uniform(-0.02, 0.02)
            new_rate = base_rate * (1 + variation)
            rates[currency] = round(new_rate, 6)
        
        rates['timestamp'] = current_time.isoformat()
        rates['USD'] = 1.0  # Add base currency
        
        return rates
    
    def start_streaming(self, interval=30):
        """Start streaming FX rates to Kafka"""
        print(f"Starting FX rate producer. Publishing every {interval} seconds...")
        print("Press Ctrl+C to stop")
        
        try:
            while True:
                fx_data = self.generate_fx_rates()
                
                self.producer.send(
                    os.getenv('KAFKA_FX_TOPIC', 'fx-rates'),
                    value=fx_data
                )
                
                print(f"Published FX rates at {fx_data['timestamp'][:19]}")
                print(f"   EUR: {fx_data.get('EUR', 'N/A'):.4f}, GBP: {fx_data.get('GBP', 'N/A'):.4f}")
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("\nStopping FX rate producer...")
        finally:
            self.producer.close()

if __name__ == "__main__":
    producer = FXRateProducer()
    producer.start_streaming(interval=15)  # Update every 15 seconds for testing