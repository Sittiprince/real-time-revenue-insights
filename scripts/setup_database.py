# scripts/setup_database.py
import psycopg2
import logging
import sys
import os
from datetime import datetime, timedelta
import random

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'globemart',
    'user': 'dataeng',
    'password': 'pipeline123'
}

def setup_database():
    """Setup PostgreSQL database with tables and initial data"""
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("Connected to PostgreSQL")
        
        # Drop existing table if it exists
        cursor.execute("DROP TABLE IF EXISTS transactions CASCADE")
        print("Dropped existing table")
        
        # Create transactions table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id VARCHAR(50) PRIMARY KEY,
                user_id VARCHAR(50) NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                currency VARCHAR(3) NOT NULL,
                country VARCHAR(50) NOT NULL,
                transaction_time TIMESTAMP NOT NULL,
                batch_id VARCHAR(50),
                product_category VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        print("Created transactions table")
        
        # Create index for faster queries
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_transactions_time 
            ON transactions(transaction_time)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_transactions_country 
            ON transactions(country)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_transactions_currency 
            ON transactions(currency)
        """)
        
        print("Created database indexes")
        
        # Check if we already have data
        cursor.execute("SELECT COUNT(*) FROM transactions")
        count = cursor.fetchone()[0]
        
        if count > 0:
            print(f"Database already has {count} transactions")
        else:
            print("Generating initial transaction data...")
            generate_sample_transactions(cursor, 5000)
        
        # Commit changes
        conn.commit()
        cursor.close()
        conn.close()
        
        print("Database setup completed successfully")
        
    except Exception as e:
        print(f"Database setup failed: {e}")
        sys.exit(1)

def generate_sample_transactions(cursor, num_transactions=5000):
    """Generate sample transaction data"""
    
    # Sample data
    countries = ['USA', 'UK', 'Germany', 'France', 'Japan', 'Canada', 'Australia', 'Brazil', 'India', 'China']
    currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'CNY', 'INR', 'BRL']
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Toys', 'Health', 'Automotive', 'Food']
    
    country_currency_map = {
        'USA': 'USD',
        'UK': 'GBP', 
        'Germany': 'EUR',
        'France': 'EUR',
        'Japan': 'JPY',
        'Canada': 'CAD',
        'Australia': 'AUD',
        'Brazil': 'BRL',
        'India': 'INR',
        'China': 'CNY'
    }
    
    transactions = []
    
    # Generate transactions over the last 48 hours
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=48)
    
    for i in range(num_transactions):
        # Random timestamp within last 48 hours
        random_time = start_time + timedelta(
            seconds=random.randint(0, int((end_time - start_time).total_seconds()))
        )
        
        # Pick a country and its currency
        country = random.choice(countries)
        currency = country_currency_map[country]
        
        # Create batch_id based on hour
        batch_id = f"batch_{random_time.strftime('%Y%m%d_%H')}"
        
        # Generate transaction
        transaction = (
            f"tx_{i+1:06d}",  # transaction_id
            f"user_{random.randint(1, 1000):04d}",  # user_id
            round(random.uniform(10.0, 2000.0), 2),  # amount
            currency,  # currency
            country,  # country
            random_time,  # transaction_time
            batch_id,  # batch_id
            random.choice(categories)  # product_category
        )
        
        transactions.append(transaction)
    
    # Batch insert
    cursor.executemany("""
        INSERT INTO transactions 
        (transaction_id, user_id, amount, currency, country, transaction_time, batch_id, product_category)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, transactions)
    
    print(f"Generated {num_transactions} sample transactions")

def test_connection():
    """Test database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("SELECT version()")
        version = cursor.fetchone()[0]
        print(f"PostgreSQL connection successful: {version}")
        
        cursor.execute("SELECT COUNT(*) FROM transactions")
        count = cursor.fetchone()[0]
        print(f"Total transactions in database: {count}")
        
        # Show recent transactions
        cursor.execute("""
            SELECT country, currency, COUNT(*), SUM(amount)
            FROM transactions 
            GROUP BY country, currency
            ORDER BY COUNT(*) DESC
            LIMIT 5
        """)
        
        print("\nTop transaction groups:")
        for row in cursor.fetchall():
            print(f"  {row[0]} ({row[1]}): {row[2]} transactions, ${row[3]:,.2f}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False
    
    return True

def main():
    print("GlobeMart Database Setup")
    print("========================")
    
    # Test connection first
    if not test_connection():
        print("Failed to connect to database. Please check your configuration.")
        return
    
    # Setup database
    setup_database()
    
    print("Database setup complete!")

if __name__ == "__main__":
    main()