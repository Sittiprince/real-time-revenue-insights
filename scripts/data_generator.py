import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_batch
import os
from dotenv import load_dotenv

load_dotenv()

fake = Faker()

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'globemart',
    'user': 'dataeng',
    'password': 'pipeline123'
}

class TransactionGenerator:
    def __init__(self):
        self.countries = {
            'US': 'USD', 'UK': 'GBP', 'DE': 'EUR', 'FR': 'EUR', 
            'CA': 'CAD', 'JP': 'JPY', 'AU': 'AUD', 'CH': 'CHF',
            'SE': 'SEK', 'NO': 'NOK'
        }
        self.categories = [
            'Electronics', 'Clothing', 'Books', 'Home & Garden',
            'Sports', 'Toys', 'Health', 'Automotive', 'Food'
        ]
    
    def generate_transactions(self, count=1000, hours_back=24):
        """Generate realistic transaction data"""
        transactions = []
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours_back)
        
        for i in range(count):
            country = random.choice(list(self.countries.keys()))
            currency = self.countries[country]
            
            # Generate realistic amounts based on currency
            if currency == 'JPY':
                amount = round(random.uniform(1000, 50000), 0)
            elif currency == 'USD':
                amount = round(random.uniform(10, 500), 2)
            elif currency == 'EUR':
                amount = round(random.uniform(15, 400), 2)
            else:
                amount = round(random.uniform(5, 300), 2)
            
            transaction_time = fake.date_time_between(
                start_date=start_time, 
                end_date=end_time
            )
            
            # Create batch_id based on hour
            batch_id = f"batch_{transaction_time.strftime('%Y%m%d_%H')}"
            
            transaction = {
                'transaction_id': f"tx_{transaction_time.strftime('%Y%m%d%H%M%S')}_{i:04d}",
                'user_id': f"user_{random.randint(1000, 9999)}",
                'country': country,
                'currency': currency,
                'amount': amount,
                'product_category': random.choice(self.categories),
                'transaction_time': transaction_time,
                'batch_id': batch_id
            }
            transactions.append(transaction)
        
        return pd.DataFrame(transactions)
    
    def save_to_postgres(self, df):
        """Save transactions to PostgreSQL"""
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Prepare data for insertion
        data_tuples = [
            (row.transaction_id, row.user_id, row.country, row.currency, 
             float(row.amount), row.product_category, row.transaction_time, 
             row.batch_id)
            for row in df.itertuples()
        ]
        
        insert_query = """
        INSERT INTO transactions 
        (transaction_id, user_id, country, currency, amount, 
         product_category, transaction_time, batch_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        execute_batch(cursor, insert_query, data_tuples, page_size=1000)
        conn.commit()
        
        print(f"Inserted {len(df)} transactions into PostgreSQL")
        
        cursor.close()
        conn.close()

def main():
    generator = TransactionGenerator()
    
    # Generate sample data for last 48 hours
    print("Generating sample transactions...")
    transactions_df = generator.generate_transactions(count=5000, hours_back=48)
    
    # Save to CSV for backup
    transactions_df.to_csv('./data/sample_transactions.csv', index=False)
    print("Saved sample data to CSV")
    
    # Display sample data
    print("\nSample transactions:")
    print(transactions_df.head())
    print(f"\nCountries: {transactions_df['country'].unique()}")
    print(f"Currencies: {transactions_df['currency'].unique()}")
    print(f"Date range: {transactions_df['transaction_time'].min()} to {transactions_df['transaction_time'].max()}")
    
    # Save to PostgreSQL
    try:
        generator.save_to_postgres(transactions_df)
        print("Sample data loaded successfully to PostgreSQL!")
    except Exception as e:
        print(f"Error loading data to PostgreSQL: {e}")

if __name__ == "__main__":
    main()