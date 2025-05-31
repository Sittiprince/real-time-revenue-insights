import duckdb
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Optional
import os
import time
import tempfile
import shutil
import uuid
import threading

logger = logging.getLogger(__name__)

class DuckDBManager:
    def __init__(self, db_path: str = "data/analytics.duckdb"):
        self.db_path = db_path
        self.conn = None
        self._ensure_data_dir()
        self.temp_dir = os.path.join(os.path.dirname(db_path), "temp")
        os.makedirs(self.temp_dir, exist_ok=True)
        self._lock = threading.Lock()
        self._setup_connection()
    
    def _ensure_data_dir(self):
        """Ensure data directory exists"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
    
    def _setup_connection(self):
        """Setup connection to DuckDB"""
        retries = 3
        retry_delay = 2
        
        while retries > 0:
            try:
                with self._lock:
                    # Close existing connection if any
                    if self.conn:
                        try:
                            self.conn.close()
                        except:
                            pass
                        self.conn = None
                    
                    # Connect to DuckDB with proper configuration
                    self.conn = duckdb.connect(
                        database=self.db_path,
                        read_only=False,
                        config={
                            'access_mode': 'READ_WRITE',
                            'threads': '4',
                            'memory_limit': '512MB'
                        }
                    )
                    
                    # Test connection by creating tables
                    self._create_tables()
                    logger.info(f"Successfully connected to DuckDB at {self.db_path}")
                    return True
                    
            except Exception as e:
                logger.error(f"Failed to connect to DuckDB (attempt {4-retries}/3): {e}")
                retries -= 1
                if retries > 0:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
        
        if retries == 0:
            logger.error("Failed to establish DuckDB connection after 3 attempts")
            return False
    
    def _create_tables(self):
        """Create analytics tables"""
        if not self.conn:
            raise Exception("No DuckDB connection available")
            
        try:
            # Revenue transactions table
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS revenue_analytics (
                    transaction_id VARCHAR,
                    user_id VARCHAR,
                    country VARCHAR,
                    original_currency VARCHAR,
                    original_amount DECIMAL(10,2),
                    usd_amount DECIMAL(10,2),
                    fx_rate DECIMAL(8,4),
                    transaction_date TIMESTAMP,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (transaction_id)
                )
            """)
            
            # Hourly aggregations
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS hourly_revenue (
                    hour_timestamp TIMESTAMP,
                    country VARCHAR,
                    currency VARCHAR,
                    total_usd DECIMAL(12,2),
                    transaction_count INTEGER,
                    avg_fx_rate DECIMAL(8,4),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (hour_timestamp, country, currency)
                )
            """)
            
            # FX rate history
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS fx_rates_history (
                    currency VARCHAR,
                    rate DECIMAL(8,4),
                    timestamp TIMESTAMP,
                    PRIMARY KEY (currency, timestamp)
                )
            """)
            
            self.conn.commit()
            logger.info("DuckDB tables created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            # Try to reconnect
            self._setup_connection()
            return False
    
    def _ensure_connection(self):
        """Ensure we have a valid connection"""
        try:
            with self._lock:
                if self.conn:
                    try:
                        # Test the connection
                        self.conn.execute("SELECT 1")
                        return True
                    except:
                        # Connection failed, close it and try to reconnect
                        try:
                            self.conn.close()
                        except:
                            pass
                        self.conn = None
                
                # No connection or connection failed, try to reconnect
                return self._setup_connection()
        except Exception as e:
            logger.error(f"Error ensuring connection: {e}")
            return False
    
    def insert_revenue_batch(self, transactions: List[Dict]):
        """Insert processed transactions"""
        if not transactions:
            return
        
        if not self._ensure_connection():
            logger.error("Cannot insert revenue batch: no database connection")
            return
        
        try:
            with self._lock:
                df = pd.DataFrame(transactions)
                self.conn.execute("BEGIN TRANSACTION")
                self.conn.execute("""
                    INSERT OR REPLACE INTO revenue_analytics (
                        transaction_id, user_id, country, original_currency,
                        original_amount, usd_amount, fx_rate, transaction_date,
                        processed_at
                    ) SELECT 
                        transaction_id, user_id, country, original_currency,
                        original_amount, usd_amount, fx_rate, transaction_date,
                        processed_at
                    FROM df
                """)
                self.conn.commit()
                logger.info(f"Inserted {len(transactions)} revenue records")
        except Exception as e:
            logger.error(f"Failed to insert revenue batch: {e}")
            with self._lock:
                self.conn.rollback()
            # Try to reconnect on failure
            self._setup_connection()
    
    def insert_fx_rates(self, fx_data: Dict):
        """Insert FX rate snapshot"""
        if not self._ensure_connection():
            logger.error("Cannot insert FX rates: no database connection")
            return
            
        try:
            records = []
            timestamp = datetime.now()
            
            for currency, rate in fx_data.items():
                if currency != 'timestamp':
                    records.append({
                        'currency': currency,
                        'rate': float(rate),
                        'timestamp': timestamp
                    })
            
            if records:
                with self._lock:
                    df = pd.DataFrame(records)
                    self.conn.execute("BEGIN TRANSACTION")
                    self.conn.execute("INSERT INTO fx_rates_history SELECT * FROM df")
                    self.conn.commit()
                    logger.info(f"Inserted FX rates for {len(records)} currencies")
        except Exception as e:
            logger.error(f"Failed to insert FX rates: {e}")
            with self._lock:
                self.conn.rollback()
            self._setup_connection()
    
    def update_hourly_aggregations(self):
        """Update hourly revenue aggregations"""
        if not self._ensure_connection():
            logger.error("Cannot update aggregations: no database connection")
            return

        try:
            with self._lock:
                self.conn.execute("BEGIN TRANSACTION")
                self.conn.execute("""
                    INSERT OR REPLACE INTO hourly_revenue
                    SELECT 
                        DATE_TRUNC('hour', transaction_date::TIMESTAMP) as hour_timestamp,
                        country,
                        original_currency as currency,
                        SUM(usd_amount) as total_usd,
                        COUNT(*) as transaction_count,
                        AVG(fx_rate) as avg_fx_rate,
                        CURRENT_TIMESTAMP::TIMESTAMP as created_at
                    FROM revenue_analytics
                    WHERE transaction_date::TIMESTAMP >= (CURRENT_TIMESTAMP::TIMESTAMP - INTERVAL '48 HOURS')
                    GROUP BY 1, 2, 3
                """)
                self.conn.commit()
                logger.info("Updated hourly aggregations")
        except Exception as e:
            logger.error(f"Failed to update aggregations: {e}")
            with self._lock:
                try:
                    self.conn.rollback()
                except:
                    pass
            # Try to reconnect on failure
            self._setup_connection()
    
    def get_revenue_metrics(self, hours: int = 24) -> Dict:
        """Get key revenue metrics"""
        if not self._ensure_connection():
            logger.error("No database connection available")
            return {
                'total_revenue': 0.0,
                'revenue_by_country': [],
                'revenue_by_currency': [],
                'hourly_trends': []
            }

        try:
            # Total revenue last N hours
            total_result = self.conn.execute(f"""
                SELECT COALESCE(SUM(usd_amount), 0) as total_revenue
                FROM revenue_analytics 
                WHERE transaction_date::TIMESTAMP >= (CURRENT_TIMESTAMP::TIMESTAMP - INTERVAL '{hours} HOURS')
            """).fetchone()
            
            # Revenue by country
            country_result = self.conn.execute(f"""
                SELECT country, COALESCE(SUM(usd_amount), 0) as revenue
                FROM revenue_analytics 
                WHERE transaction_date::TIMESTAMP >= (CURRENT_TIMESTAMP::TIMESTAMP - INTERVAL '{hours} HOURS')
                GROUP BY country
                ORDER BY revenue DESC
            """).fetchall()
            
            # Revenue by currency
            currency_result = self.conn.execute(f"""
                SELECT original_currency, COALESCE(SUM(usd_amount), 0) as revenue
                FROM revenue_analytics 
                WHERE transaction_date::TIMESTAMP >= (CURRENT_TIMESTAMP::TIMESTAMP - INTERVAL '{hours} HOURS')
                GROUP BY original_currency
                ORDER BY revenue DESC
            """).fetchall()
            
            # Hourly trends
            hourly_result = self.conn.execute(f"""
                SELECT 
                    DATE_TRUNC('hour', transaction_date::TIMESTAMP) as hour,
                    COALESCE(SUM(usd_amount), 0) as revenue,
                    COUNT(*) as transactions
                FROM revenue_analytics 
                WHERE transaction_date::TIMESTAMP >= (CURRENT_TIMESTAMP::TIMESTAMP - INTERVAL '{hours} HOURS')
                GROUP BY 1
                ORDER BY 1 DESC
            """).fetchall()
            
            return {
                'total_revenue': float(total_result[0]) if total_result else 0.0,
                'revenue_by_country': [{'country': r[0], 'revenue': float(r[1])} for r in (country_result or [])],
                'revenue_by_currency': [{'currency': r[0], 'revenue': float(r[1])} for r in (currency_result or [])],
                'hourly_trends': [{'hour': r[0], 'revenue': float(r[1]), 'transactions': r[2]} for r in (hourly_result or [])]
            }
        except Exception as e:
            logger.error(f"Failed to get revenue metrics: {e}")
            return {
                'total_revenue': 0.0,
                'revenue_by_country': [],
                'revenue_by_currency': [],
                'hourly_trends': []
            }
    
    def get_fx_trends(self, currency: str = 'EUR', hours: int = 24) -> List[Dict]:
        """Get FX rate trends"""
        if not self._ensure_connection():
            logger.error("No database connection available")
            return []

        try:
            result = self.conn.execute(f"""
                SELECT timestamp::TIMESTAMP, rate
                FROM fx_rates_history 
                WHERE currency = ? 
                AND timestamp::TIMESTAMP >= (CURRENT_TIMESTAMP::TIMESTAMP - INTERVAL '{hours} HOURS')
                ORDER BY timestamp DESC
                LIMIT 100
            """, [currency]).fetchall()
            
            return [{'timestamp': r[0], 'rate': float(r[1])} for r in result]
        except Exception as e:
            logger.error(f"Failed to get FX trends: {e}")
            return []
    
    def get_top_users(self, hours: int = 24, limit: int = 10) -> List[Dict]:
        """Get top spending users"""
        if not self._ensure_connection():
            logger.error("No database connection available")
            return []

        try:
            result = self.conn.execute(f"""
                SELECT 
                    user_id,
                    SUM(usd_amount) as total_spent,
                    COUNT(*) as transaction_count,
                    COUNT(DISTINCT country) as countries_shopped
                FROM revenue_analytics 
                WHERE transaction_date::TIMESTAMP >= (CURRENT_TIMESTAMP::TIMESTAMP - INTERVAL '{hours} HOURS')
                GROUP BY user_id
                ORDER BY total_spent DESC
                LIMIT {limit}
            """).fetchall()
            
            return [{
                'user_id': r[0], 
                'total_spent': float(r[1]), 
                'transactions': r[2],
                'countries': r[3]
            } for r in result]
        except Exception as e:
            logger.error(f"Failed to get top users: {e}")
            return []
    
    def close(self):
        """Close database connection"""
        if self.conn:
            try:
                self.conn.close()
                self.conn = None
                logger.info("DuckDB connection closed")
            except Exception as e:
                logger.error(f"Error closing DuckDB connection: {e}")
    
    def update_fx_rates(self, fx_rates: Dict):
        """Update FX rates in the database
        
        Args:
            fx_rates: Dictionary of currency codes to rates
        """
        if not fx_rates:
            return
            
        if not self._ensure_connection():
            logger.error("Cannot update FX rates: no database connection")
            return
            
        try:
            # First insert into history
            self.insert_fx_rates(fx_rates)
            
            logger.info(f"Updated FX rates for {len(fx_rates)} currencies")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update FX rates: {e}")
            return False